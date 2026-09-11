use super::*;
use crate::state::OpencodeDatabaseState;
use crate::state::checkpoint::{CheckpointDelta, CheckpointWriter, PendingChange};

pub(super) struct CheckpointSession {
    writer: CheckpointWriter,
    pub loaded: HashMap<String, Option<FileState>>,
    delta: CheckpointDelta,
    original_next_doc_id: u64,
    original_opencode_databases: HashMap<String, OpencodeDatabaseState>,
    pub next_doc_id: u64,
    pub opencode_databases: HashMap<String, OpencodeDatabaseState>,
    pub pending: Option<PendingIngest>,
    pub scan_cache: ScanCache,
}

impl CheckpointSession {
    pub fn open(
        path: &Path,
        lease: &IngestLease,
        allow_initialize: bool,
        header: Option<CheckpointHeader>,
    ) -> Result<Self> {
        let writer = CheckpointWriter::open(path, lease, allow_initialize)?;
        let header = match header {
            Some(header) => header,
            None => writer.reader().header()?,
        };
        Ok(Self {
            writer,
            loaded: HashMap::new(),
            delta: CheckpointDelta::default(),
            original_next_doc_id: header.next_doc_id,
            original_opencode_databases: header.opencode_databases.clone(),
            next_doc_id: header.next_doc_id,
            opencode_databases: header.opencode_databases,
            pending: header.pending,
            scan_cache: header.scan_cache,
        })
    }

    pub fn preload(&mut self, paths: &[String]) -> Result<()> {
        let missing = paths
            .iter()
            .filter(|path| !self.loaded.contains_key(*path))
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if self.delta.clear_files {
            self.loaded
                .extend(missing.into_iter().map(|path| (path, None)));
        } else if !missing.is_empty() {
            self.loaded
                .extend(self.writer.reader().load_files(&missing)?);
        }
        Ok(())
    }

    pub fn file(&self, path: &str) -> Option<&FileState> {
        self.loaded
            .get(path)
            .expect("checkpoint path must be preloaded")
            .as_ref()
    }

    pub fn contains_file(&self, path: &str) -> Result<bool> {
        if let Some(file) = self.loaded.get(path) {
            return Ok(file.is_some());
        }
        if self.delta.clear_files {
            return Ok(false);
        }
        self.writer.reader().contains_file(path)
    }

    pub fn file_keys(&self) -> Result<Vec<String>> {
        let mut keys = if self.delta.clear_files {
            HashSet::new()
        } else {
            self.writer
                .reader()
                .file_keys()?
                .into_iter()
                .collect::<HashSet<_>>()
        };
        keys.retain(|key| !self.delta.deletes.contains(key));
        keys.extend(self.delta.upserts.keys().cloned());
        Ok(keys.into_iter().collect())
    }

    pub fn has_files(&self) -> Result<bool> {
        if !self.delta.upserts.is_empty() {
            return Ok(true);
        }
        if self.delta.clear_files {
            return Ok(false);
        }
        self.writer
            .reader()
            .has_files_excluding(&self.delta.deletes)
    }

    pub fn delete_file(&mut self, path: &str) {
        self.loaded.insert(path.to_owned(), None);
        self.delta.upserts.remove(path);
        if !self.delta.clear_files {
            self.delta.deletes.insert(path.to_owned());
        }
    }

    pub fn clear_files(&mut self) {
        self.loaded.clear();
        self.delta.upserts.clear();
        self.delta.deletes.clear();
        self.delta.clear_files = true;
    }

    pub fn upsert_file(&mut self, path: String, file: FileState) {
        if self
            .loaded
            .get(&path)
            .is_some_and(|previous| previous.as_ref() == Some(&file))
        {
            return;
        }
        self.delta.deletes.remove(&path);
        self.loaded.insert(path.clone(), Some(file.clone()));
        self.delta.upserts.insert(path, file);
    }

    pub fn commit_intent(&mut self, pending: &PendingIngest) -> Result<()> {
        self.writer.commit_intent(pending)?;
        self.pending = Some(pending.clone());
        Ok(())
    }

    pub fn commit_final(
        &mut self,
        cache: Option<ScanCache>,
        pending: PendingChange,
    ) -> Result<bool> {
        crate::profiling::span!("state.checkpoint.commit_final");
        self.delta.scan_cache = cache;
        self.delta.pending = pending;
        self.delta.next_doc_id =
            (self.next_doc_id != self.original_next_doc_id).then_some(self.next_doc_id);
        self.delta.opencode_databases = (self.opencode_databases
            != self.original_opencode_databases)
            .then(|| self.opencode_databases.clone());
        let changed = self.writer.commit_delta(&self.delta)?;
        if let Some(cache) = self.delta.scan_cache.take() {
            self.scan_cache = cache;
        }
        match std::mem::take(&mut self.delta.pending) {
            PendingChange::Keep => {}
            PendingChange::Replace(pending) => self.pending = Some(pending),
            PendingChange::Clear => self.pending = None,
        }
        self.delta = CheckpointDelta::default();
        self.original_next_doc_id = self.next_doc_id;
        self.original_opencode_databases = self.opencode_databases.clone();
        Ok(changed)
    }
}
