#[cfg(test)]
mod benchmark;
#[cfg(test)]
mod cleanup_tests;
mod storage;

use crate::state::SessionScope;
use crate::types::{Record, RecordLinks, SourceFilter};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::ops::Bound;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};
use tantivy::collector::{Collector, Count, SegmentCollector, TopDocs};
use tantivy::columnar::StrColumn;
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    Directory, DirectoryLock, FileHandle, Lock, MmapDirectory, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::merge_policy::{LogMergePolicy, NoMergePolicy};
use tantivy::query::{AllQuery, BooleanQuery, EmptyQuery, Occur, Query, RangeQuery, TermQuery};
use tantivy::schema::Value;
use tantivy::schema::{
    FAST, Field, INDEXED, IndexRecordOption, STORED, STRING, Schema, SchemaBuilder,
    TextFieldIndexing, TextOptions,
};
use tantivy::store::StoreReader;
use tantivy::{
    DocId, Index, IndexReader, IndexWriter, Order, ReloadPolicy, Score, SegmentReader,
    TantivyDocument, Term,
};

#[derive(Clone)]
pub struct IndexFields {
    /// Optional for reading generations built before transcript presentation metadata.
    pub reader_metadata: Option<Field>,
    /// Present in indexes created after canonical record lookup was introduced. Older indexes
    /// remain readable and use a scoped stored-record fallback until they are rebuilt.
    pub canonical_record_id: Option<Field>,
    pub doc_id: Field,
    pub ts: Field,
    pub project: Field,
    pub session_id: Field,
    pub turn_id: Field,
    pub role: Field,
    pub text: Field,
    pub source: Option<Field>,
    pub tool_name: Field,
    pub tool_input: Field,
    pub tool_output: Field,
    pub event_id: Field,
    pub parent_event_id: Field,
    pub logical_parent_event_id: Field,
    pub parent_session_id: Field,
    pub thread_source: Field,
    pub conversation_kind: Field,
    pub parent_tool_use_id: Field,
    pub source_tool_use_id: Field,
    pub source_tool_assistant_uuid: Field,
    pub source_path: Field,
}

#[derive(Clone)]
pub struct SearchIndex {
    pub index: Index,
    pub fields: IndexFields,
    snapshot_version: String,
    writable: bool,
    pending_generation: Option<Arc<PendingGeneration>>,
    _generation_lease: Option<Arc<GenerationLease>>,
    incremental_merge_policy: bool,
    defer_merges: bool,
    shared_reader: Arc<OnceLock<IndexReader>>,
}

const GENERATIONS_DIR: &str = "generations";
const CURRENT_FILE: &str = "CURRENT";
const GENERATION_LEASE_FILE: &str = ".lease";
const SMALL_INGEST_MAX_BYTES: u64 = 1024 * 1024;
const CONTINUOUS_MAX_SEGMENTS: usize = 4096;
/// Segment count above which a search-triggered refresh schedules background compaction.
pub const SEARCH_REFRESH_COMPACTION_SEGMENTS: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerationGcReport {
    pub generations_removed: usize,
    pub abandoned_workdirs_removed: usize,
    pub legacy_files_removed: usize,
    pub shared_files_removed: usize,
    pub dry_run: bool,
}

#[derive(Debug)]
struct GenerationLease {
    #[allow(dead_code)]
    file: Option<File>,
}

#[derive(Debug)]
struct PendingGeneration {
    index_root: PathBuf,
    staging_dir: PathBuf,
    generation_name: String,
    requires_initial_publication: bool,
    published: AtomicBool,
    _staging_lease: Arc<GenerationLease>,
    directory: storage::SharedDirectory,
}

impl Drop for PendingGeneration {
    fn drop(&mut self) {
        if !self.published.load(AtomicOrdering::Acquire) {
            let _ = fs::remove_dir_all(&self.staging_dir);
        }
    }
}

/// Tantivy normally takes a metadata lock every time it opens segment readers so its own
/// garbage collector cannot remove a segment concurrently. Published generations are immutable,
/// so Tantivy cannot remove their segments and the lock is unnecessary for sealed readers.
#[derive(Clone, Debug)]
struct SealedDirectory {
    directory: MmapDirectory,
    _generation_lease: Option<Arc<GenerationLease>>,
}

impl Directory for SealedDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.directory.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.directory.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.directory.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        self.directory.open_write(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.directory.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.directory.atomic_write(path, data)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.directory.sync_directory()
    }

    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(())))
    }

    fn watch(&self, callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.directory.watch(callback)
    }
}

#[derive(Debug, Clone)]
pub struct QueryOptions {
    pub query: String,
    pub project: Option<String>,
    pub role: Option<String>,
    pub tool: Option<String>,
    pub session_id: Option<String>,
    /// Exact session identities allowed by an external scope such as `--cwd`.
    /// `Some([])` intentionally matches no records.
    pub session_scope: Option<Vec<SessionScopeKey>>,
    pub source: Option<crate::types::SourceFilter>,
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampOrder {
    Newest,
    Oldest,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionScopeKey {
    pub source: crate::types::SourceKind,
    pub session_id: String,
    pub source_path: String,
}

type SessionScopeIdentity = (crate::types::SourceKind, String, String);

struct SessionScopeCollector {
    fields: IndexFields,
    fast_session_identity: bool,
}

enum SessionScopeSegmentCollector {
    Fast {
        source: StrColumn,
        session_id: StrColumn,
        source_path: StrColumn,
        scope_ords: HashSet<(Option<u64>, Option<u64>, Option<u64>)>,
    },
    Stored {
        store: StoreReader,
        fields: IndexFields,
        scopes: HashSet<SessionScopeIdentity>,
        error: Option<tantivy::TantivyError>,
    },
}

impl Collector for SessionScopeCollector {
    type Fruit = std::result::Result<HashSet<SessionScopeIdentity>, tantivy::TantivyError>;
    type Child = SessionScopeSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        if !self.fast_session_identity {
            return Ok(SessionScopeSegmentCollector::Stored {
                store: segment.get_store_reader(1)?,
                fields: self.fields.clone(),
                scopes: HashSet::new(),
                error: None,
            });
        }
        let fast_fields = segment.fast_fields();
        let source = self
            .fields
            .source
            .map(|_| fast_fields.str("source"))
            .transpose()?
            .flatten();
        let session_id = fast_fields.str("session_id")?;
        let source_path = fast_fields.str("source_path")?;
        if let (Some(source), Some(session_id), Some(source_path)) =
            (source, session_id, source_path)
        {
            Ok(SessionScopeSegmentCollector::Fast {
                source,
                session_id,
                source_path,
                scope_ords: HashSet::new(),
            })
        } else {
            Ok(SessionScopeSegmentCollector::Stored {
                store: segment.get_store_reader(1)?,
                fields: self.fields.clone(),
                scopes: HashSet::new(),
                error: None,
            })
        }
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut scopes = HashSet::new();
        for fruit in segment_fruits {
            scopes.extend(fruit?);
        }
        Ok(Ok(scopes))
    }
}

impl SegmentCollector for SessionScopeSegmentCollector {
    type Fruit = std::result::Result<HashSet<SessionScopeIdentity>, tantivy::TantivyError>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        match self {
            Self::Fast {
                source,
                session_id,
                source_path,
                scope_ords,
            } => {
                scope_ords.insert((
                    source.ords().first(doc),
                    session_id.ords().first(doc),
                    source_path.ords().first(doc),
                ));
            }
            Self::Stored {
                store,
                fields,
                scopes,
                error,
            } if error.is_none() => {
                if let Err(found) = collect_stored_scope(store, fields, scopes, doc) {
                    *error = Some(found);
                }
            }
            Self::Stored { .. } => {}
        }
    }

    fn harvest(self) -> Self::Fruit {
        match self {
            Self::Fast {
                source,
                session_id,
                source_path,
                scope_ords,
            } => {
                let mut scopes = HashSet::with_capacity(scope_ords.len());
                for (source_ord, session_id_ord, source_path_ord) in scope_ords {
                    let source_path = fast_string(&source_path, source_path_ord)?;
                    let session_id = fast_string(&session_id, session_id_ord)?;
                    let source_label = fast_string(&source, source_ord)?;
                    let source = crate::types::SourceKind::from_label(&source_label)
                        .unwrap_or_else(|| crate::types::SourceKind::from_path(&source_path));
                    scopes.insert((source, session_id, source_path));
                }
                Ok(scopes)
            }
            Self::Stored { scopes, error, .. } => error.map_or(Ok(scopes), Err),
        }
    }
}

fn collect_stored_scope(
    store: &StoreReader,
    fields: &IndexFields,
    scopes: &mut HashSet<SessionScopeIdentity>,
    doc_id: DocId,
) -> tantivy::Result<()> {
    let doc = store.get::<TantivyDocument>(doc_id)?;
    let source_path = doc
        .get_first(fields.source_path)
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();
    let session_id = doc
        .get_first(fields.session_id)
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string();
    let source = fields
        .source
        .and_then(|field| doc.get_first(field))
        .and_then(|value| value.as_str())
        .and_then(crate::types::SourceKind::from_label)
        .unwrap_or_else(|| crate::types::SourceKind::from_path(&source_path));
    scopes.insert((source, session_id, source_path));
    Ok(())
}

fn fast_string(column: &StrColumn, ord: Option<u64>) -> tantivy::Result<String> {
    let Some(ord) = ord else {
        return Ok(String::new());
    };
    let mut value = String::new();
    column.ord_to_str(ord, &mut value)?;
    Ok(value)
}

impl SearchIndex {
    pub fn exists(dir: &Path) -> bool {
        resolve_current_generation(dir)
            .is_some_and(|generation| generation.join("meta.json").exists())
            || dir.join("meta.json").exists()
    }

    pub fn garbage_collect_generations_offline(
        dir: &Path,
        dry_run: bool,
    ) -> Result<GenerationGcReport> {
        let _store_guard = if dry_run {
            storage::lock_existing_store(dir)?
        } else {
            Some(storage::lock_store(dir)?)
        };
        let source = resolve_current_generation(dir).unwrap_or_else(|| dir.to_path_buf());
        if !source.join("meta.json").is_file() {
            bail!("no committed index exists at {}", dir.display());
        }

        let generations = dir.join(GENERATIONS_DIR);
        if !dry_run {
            fs::create_dir_all(&generations)?;
        }
        let entries = match fs::read_dir(&generations) {
            Ok(entries) => entries.collect::<io::Result<Vec<_>>>()?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let old_generations = entries
            .iter()
            .filter(|entry| {
                entry.file_type().is_ok_and(|kind| kind.is_dir())
                    && !entry.file_name().to_string_lossy().starts_with('.')
            })
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        let abandoned_workdirs = entries
            .iter()
            .filter(|entry| {
                entry.file_type().is_ok_and(|kind| kind.is_dir())
                    && is_abandoned_generation_workdir(&entry.file_name())
            })
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        let legacy_files = fs::read_dir(dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.file_type().is_ok_and(|kind| kind.is_file())
                    && entry.file_name() != CURRENT_FILE
            })
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        let mut report = GenerationGcReport {
            generations_removed: old_generations.len(),
            abandoned_workdirs_removed: abandoned_workdirs.len(),
            legacy_files_removed: legacy_files.len(),
            shared_files_removed: 0,
            dry_run,
        };
        if dry_run {
            let doomed = old_generations
                .iter()
                .chain(abandoned_workdirs.iter())
                .cloned()
                .collect::<Vec<_>>();
            report.shared_files_removed =
                storage::collect_unreachable_excluding(dir, true, &doomed)?;
            return Ok(report);
        }

        // New-format readers hold shared leases for their generation. Refuse before changing
        // CURRENT if any such reader is still active. Pre-lease readers are why this operation is
        // explicitly offline.
        let mut exclusive_leases = Vec::new();
        for generation in &old_generations {
            if generation.join(GENERATION_LEASE_FILE).is_file() {
                let lease = try_lock_generation_exclusive(generation)?.ok_or_else(|| {
                    anyhow!(
                        "index generation {} is still in use; close all Memex readers and retry",
                        generation.display()
                    )
                })?;
                exclusive_leases.push(lease);
            }
        }
        for workdir in &abandoned_workdirs {
            if workdir.join(GENERATION_LEASE_FILE).is_file() {
                let lease = try_lock_generation_exclusive(workdir)?.ok_or_else(|| {
                    anyhow!(
                        "index generation work directory {} is still in use; close all Memex \
                         readers and writers and retry",
                        workdir.display()
                    )
                })?;
                exclusive_leases.push(lease);
            }
        }

        let expected = validate_committed_generation(&source)?;
        let temp = tempfile::Builder::new()
            .prefix(".gc-")
            .tempdir_in(&generations)?;
        let generation_name = new_generation_name();
        create_generation_lease_file(temp.path())?;
        let shared =
            storage::SharedDirectory::stage(dir, temp.path(), Some(&source), &generation_name)?;
        rewrite_managed_files_to_committed_set(temp.path())?;
        create_generation_lease_file(temp.path())?;
        shared.prepare_publication(
            dir,
            &generation_name,
            &committed_generation_files(temp.path())?,
        )?;
        let actual = validate_committed_generation(temp.path())?;
        if actual != expected {
            bail!(
                "clean index validation changed document count from {expected} to {actual}; \
                 existing index was left untouched"
            );
        }

        let final_dir = generations.join(&generation_name);
        let staging = temp.keep();
        fs::rename(&staging, &final_dir)?;
        shared.seal_at(&final_dir)?;
        sync_directory(&generations)?;
        atomic_write_current(dir, &generation_name)?;

        for generation in old_generations {
            fs::remove_dir_all(&generation).with_context(|| {
                format!(
                    "remove unreachable index generation {}",
                    generation.display()
                )
            })?;
        }
        for workdir in abandoned_workdirs {
            fs::remove_dir_all(&workdir).with_context(|| {
                format!(
                    "remove abandoned index generation work directory {}",
                    workdir.display()
                )
            })?;
        }
        for file in legacy_files {
            fs::remove_file(&file)
                .with_context(|| format!("remove unreachable index file {}", file.display()))?;
        }
        drop(exclusive_leases);
        sync_directory(&generations)?;
        sync_directory(dir)?;
        report.shared_files_removed = storage::collect_unreachable(dir, false)?;
        Ok(report)
    }

    pub fn open_or_create(dir: &Path) -> Result<Self> {
        loop {
            let Some(generation) = resolve_current_generation(dir) else {
                return Self::open_or_create_legacy(dir);
            };
            match open_sealed_generation(&generation) {
                Ok(index) => return Ok(index),
                Err(error) => {
                    if resolve_current_generation(dir).as_ref() == Some(&generation) {
                        return Err(error);
                    }
                }
            }
        }
    }

    pub fn open_or_create_for_ingest(dir: &Path) -> Result<Self> {
        Self::open_or_create_for_ingest_with_merge_policy(dir, false)
    }

    pub fn open_or_create_for_continuous_ingest(dir: &Path) -> Result<Self> {
        Self::open_or_create_for_ingest_with_merge_policy(dir, true)
    }

    /// Search-triggered refreshes never merge in the foreground. Compaction runs in a
    /// separate `memex index` process once the segment count passes
    /// [`SEARCH_REFRESH_COMPACTION_SEGMENTS`].
    pub fn open_or_create_for_search_refresh(dir: &Path) -> Result<Self> {
        let mut index = Self::open_or_create_for_ingest_with_merge_policy(dir, true)?;
        index.defer_merges = true;
        Ok(index)
    }

    pub fn segment_count(&self) -> Result<usize> {
        Ok(self.index.searchable_segment_metas()?.len())
    }

    fn open_or_create_for_ingest_with_merge_policy(
        dir: &Path,
        incremental_merge_policy: bool,
    ) -> Result<Self> {
        crate::profiling::span!("lexical.stage");
        fs::create_dir_all(dir)?;
        let generations = dir.join(GENERATIONS_DIR);
        fs::create_dir_all(&generations)?;
        let generation_name = new_generation_name();
        let staging_dir = generations.join(format!(".{generation_name}.tmp"));

        let _store_guard = storage::lock_store(dir)?;
        let current = resolve_current_generation(dir);
        let source = current
            .as_deref()
            .or_else(|| dir.join("meta.json").is_file().then_some(dir));
        fs::create_dir(&staging_dir)?;
        #[cfg(target_os = "macos")]
        let durability = storage::StagingDurability::prepare(dir, &staging_dir)?;
        #[cfg(not(target_os = "macos"))]
        create_generation_lease_file(&staging_dir)?;
        let staging_lease = Arc::new(acquire_generation_lease(&staging_dir)?);
        let directory =
            match storage::SharedDirectory::stage(dir, &staging_dir, source, &generation_name) {
                Ok(directory) => directory,
                Err(error) => {
                    let _ = fs::remove_dir_all(&staging_dir);
                    return Err(error);
                }
            };
        #[cfg(target_os = "macos")]
        directory.set_durability(durability);
        directory.pin_generation(Arc::clone(&staging_lease));
        let pending = Arc::new(PendingGeneration {
            index_root: dir.to_path_buf(),
            staging_dir: staging_dir.clone(),
            generation_name,
            requires_initial_publication: current.is_none()
                || source.is_some_and(|path| !path.join(storage::FORMAT).exists()),
            published: AtomicBool::new(false),
            _staging_lease: staging_lease,
            directory: directory.clone(),
        });
        let index = if staging_dir.join("meta.json").exists() {
            let existing = Index::open(directory.clone())?;
            // A staged generation is what future writes go into, so it must also gain
            // the optional reader_metadata field that published generations may lack.
            if schema_is_current(&existing.schema())
                && existing.schema().get_field("reader_metadata").is_ok()
            {
                existing
            } else {
                drop(existing);
                directory.reset()?;
                Index::create(directory, build_schema()?, Default::default())?
            }
        } else {
            Index::create(directory, build_schema()?, Default::default())?
        };
        Ok(Self {
            fields: load_fields(index.schema())?,
            index,
            snapshot_version: snapshot_version_for_path(&staging_dir),
            writable: true,
            pending_generation: Some(pending),
            _generation_lease: None,
            incremental_merge_policy,
            defer_merges: false,
            shared_reader: Arc::new(OnceLock::new()),
        })
    }

    fn open_or_create_legacy(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let meta_path = dir.join("meta.json");
        if meta_path.exists() {
            let index = Index::open_in_dir(dir)?;
            if !schema_is_current(&index.schema()) {
                return Err(stale_schema_error(dir));
            }
            let fields = load_fields(index.schema())?;
            Ok(Self {
                index,
                fields,
                snapshot_version: snapshot_version_for_path(dir),
                writable: true,
                pending_generation: None,
                _generation_lease: None,
                incremental_merge_policy: false,
                defer_merges: false,
                shared_reader: Arc::new(OnceLock::new()),
            })
        } else {
            create_index_in_dir(dir)
        }
    }

    pub(crate) fn is_writable(&self) -> bool {
        self.writable
    }

    pub fn writer(&self) -> Result<IndexWriter> {
        self.writer_for_ingest(None)
    }

    pub(crate) fn writer_for_ingest(&self, input_bytes: Option<u64>) -> Result<IndexWriter> {
        crate::profiling::span!("lexical.writer_open");
        if !self.writable
            || self
                .pending_generation
                .as_ref()
                .is_some_and(|pending| pending.published.load(AtomicOrdering::Acquire))
        {
            bail!("cannot create a writer for a sealed index generation");
        }
        let writer = if input_bytes.is_some_and(|bytes| bytes <= SMALL_INGEST_MAX_BYTES) {
            crate::profiling::count!("lexical.single_thread_batches", 1);
            self.index.writer_with_num_threads(1, 64_000_000)?
        } else {
            self.index.writer(256_000_000)?
        };
        if self.defer_merges {
            writer.set_merge_policy(Box::new(NoMergePolicy));
        } else if self.incremental_merge_policy {
            let mut policy = LogMergePolicy::default();
            policy.set_min_layer_size(1);
            writer.set_merge_policy(Box::new(policy));
        }
        Ok(writer)
    }

    /// One reader per instance. Sealed generations never change; writable instances reload
    /// the shared reader so committed segments become visible without reopening every file.
    pub fn reader(&self) -> Result<IndexReader> {
        if let Some(reader) = self.shared_reader.get() {
            if self.writable {
                crate::profiling::span!("lexical.reader_reload");
                reader.reload()?;
            }
            return Ok(reader.clone());
        }
        crate::profiling::span!("lexical.reader_open");
        let reader: IndexReader = self
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        crate::profiling::count!("lexical.readers_opened", 1);
        let _ = self.shared_reader.set(reader.clone());
        Ok(reader)
    }

    /// Open-time snapshot identity. It remains stable for this instance; writable callers must
    /// reopen after committing if they need the newly committed identity.
    pub fn snapshot_version(&self) -> &str {
        &self.snapshot_version
    }

    pub(crate) fn check_continuous_segment_limit(&self) -> Result<()> {
        if !self.incremental_merge_policy || self.pending_generation.is_none() {
            return Ok(());
        }
        let segments = self.index.searchable_segment_metas()?;
        if segments.len() > CONTINUOUS_MAX_SEGMENTS {
            bail!(
                "refusing to continue indexing: {} continuous index segments exceed the safety \
                 limit of {CONTINUOUS_MAX_SEGMENTS}; run `memex index rebuild`",
                segments.len()
            );
        }
        Ok(())
    }

    pub(crate) fn publish_generation(&self) -> Result<()> {
        crate::profiling::span!("lexical.publish");
        let Some(pending) = &self.pending_generation else {
            return Ok(());
        };
        if pending.published.load(AtomicOrdering::Acquire) {
            return Ok(());
        }

        let _store_guard = storage::lock_store(&pending.index_root)?;
        if pending.staging_dir.exists() {
            let committed = committed_files(&self.index)?;
            pending.directory.prepare_publication(
                &pending.index_root,
                &pending.generation_name,
                &committed,
            )?;
        }
        let final_dir = pending
            .index_root
            .join(GENERATIONS_DIR)
            .join(&pending.generation_name);
        if pending.staging_dir.exists() {
            pending.directory.sync_for_publication()?;
            fs::rename(&pending.staging_dir, &final_dir)
                .with_context(|| format!("publish index generation {}", pending.generation_name))?;
        } else if !final_dir.exists() {
            bail!(
                "index generation {} has neither staging nor published data",
                pending.generation_name
            );
        }
        pending.directory.seal_at(&final_dir)?;
        fsync_directory(&pending.index_root.join(GENERATIONS_DIR))?;
        atomic_write_current(&pending.index_root, &pending.generation_name)?;
        pending.published.store(true, AtomicOrdering::Release);
        prune_superseded_generations(&pending.index_root, &pending.generation_name)?;
        prune_legacy_index_files(&pending.index_root)?;
        storage::collect_unreachable(&pending.index_root, false)?;
        Ok(())
    }

    pub(crate) fn publish_generation_if_uninitialized(&self) -> Result<()> {
        if self
            .pending_generation
            .as_ref()
            .is_some_and(|pending| pending.requires_initial_publication)
        {
            self.publish_generation()?;
        }
        Ok(())
    }

    pub fn delete_by_source_path(&self, writer: &mut IndexWriter, path: &str) {
        let term = Term::from_field_text(self.fields.source_path, path);
        writer.delete_term(term);
    }

    pub fn doc_ids_by_source_scope(&self, scope: &SessionScope) -> Result<Vec<u64>> {
        self.doc_ids_matching_query(Box::new(source_scope_query(&self.fields, scope)))
    }

    pub(crate) fn source_paths_with_records(
        &self,
        candidates: &HashSet<String>,
    ) -> Result<HashSet<String>> {
        crate::profiling::span!("lexical.source_presence");
        if candidates.is_empty() {
            return Ok(HashSet::new());
        }
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let mut present = HashSet::new();
        for path in candidates {
            let query = TermQuery::new(
                Term::from_field_text(self.fields.source_path, path),
                IndexRecordOption::Basic,
            );
            crate::profiling::count!("lexical.source_presence_queries", 1);
            if searcher.search(&query, &Count)? > 0 {
                present.insert(path.clone());
            }
        }
        Ok(present)
    }

    pub fn doc_ids_by_source_path(&self, path: &str) -> Result<Vec<u64>> {
        let query = TermQuery::new(
            Term::from_field_text(self.fields.source_path, path),
            IndexRecordOption::Basic,
        );
        self.doc_ids_matching_query(Box::new(query))
    }

    fn doc_ids_matching_query(&self, query: Box<dyn Query>) -> Result<Vec<u64>> {
        crate::profiling::span!("lexical.source_ids");
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let limit = (searcher.num_docs() as usize).max(1);
        let top_docs = searcher.search(query.as_ref(), &TopDocs::with_limit(limit))?;
        let mut doc_ids = Vec::with_capacity(top_docs.len());
        for (_, address) in top_docs {
            let doc = searcher.doc::<TantivyDocument>(address)?;
            if let Some(doc_id) = doc
                .get_first(self.fields.doc_id)
                .and_then(|value| value.as_u64())
            {
                doc_ids.push(doc_id);
            }
        }
        doc_ids.sort_unstable();
        crate::profiling::count!("lexical.source_id_queries", 1);
        crate::profiling::count!("lexical.source_ids_found", doc_ids.len());
        Ok(doc_ids)
    }

    pub fn delete_by_source_scope(
        &self,
        writer: &mut IndexWriter,
        scope: &SessionScope,
    ) -> Result<()> {
        writer.delete_query(Box::new(source_scope_query(&self.fields, scope)))?;
        Ok(())
    }

    pub fn add_record(&self, writer: &mut IndexWriter, record: &Record) -> Result<()> {
        let mut doc = TantivyDocument::default();
        if let Some(field) = self.fields.canonical_record_id {
            doc.add_text(field, crate::retrieval::canonical_record_id(record));
        }
        doc.add_u64(self.fields.doc_id, record.doc_id);
        doc.add_u64(self.fields.ts, record.ts);
        doc.add_text(self.fields.project, &record.project);
        doc.add_text(self.fields.session_id, &record.session_id);
        doc.add_u64(self.fields.turn_id, record.turn_id as u64);
        doc.add_text(self.fields.role, &record.role);
        doc.add_text(self.fields.text, &record.text);
        if let Some(field) = self.fields.source {
            doc.add_text(field, record.source.storage_label());
        }
        if let Some(tool_name) = &record.tool_name {
            doc.add_text(self.fields.tool_name, tool_name);
        }
        if let Some(tool_input) = &record.tool_input {
            doc.add_text(self.fields.tool_input, tool_input);
        }
        if let Some(tool_output) = &record.tool_output {
            doc.add_text(self.fields.tool_output, tool_output);
        }
        add_optional_text(&mut doc, self.fields.event_id, &record.links.event_id);
        add_optional_text(
            &mut doc,
            self.fields.parent_event_id,
            &record.links.parent_event_id,
        );
        add_optional_text(
            &mut doc,
            self.fields.logical_parent_event_id,
            &record.links.logical_parent_event_id,
        );
        add_optional_text(
            &mut doc,
            self.fields.parent_session_id,
            &record.links.parent_session_id,
        );
        add_optional_text(
            &mut doc,
            self.fields.thread_source,
            &record.links.thread_source,
        );
        add_optional_text(
            &mut doc,
            self.fields.conversation_kind,
            &record.links.conversation_kind,
        );
        add_optional_text(
            &mut doc,
            self.fields.parent_tool_use_id,
            &record.links.parent_tool_use_id,
        );
        add_optional_text(
            &mut doc,
            self.fields.source_tool_use_id,
            &record.links.source_tool_use_id,
        );
        add_optional_text(
            &mut doc,
            self.fields.source_tool_assistant_uuid,
            &record.links.source_tool_assistant_uuid,
        );
        if let Some(field) = self.fields.reader_metadata {
            doc.add_text(field, serde_json::to_string(&record.links)?);
        }
        doc.add_text(self.fields.source_path, &record.source_path);
        writer.add_document(doc)?;
        Ok(())
    }

    pub fn get_by_doc_id(&self, doc_id: u64) -> Result<Option<Record>> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let term = Term::from_field_u64(self.fields.doc_id, doc_id);
        let query = TermQuery::new(term, IndexRecordOption::Basic);
        let top = searcher.search(&query, &TopDocs::with_limit(1))?;
        let Some((_, addr)) = top.first() else {
            return Ok(None);
        };
        let doc = searcher.doc::<TantivyDocument>(*addr)?;
        Ok(Some(record_from_doc(&self.fields, &doc)))
    }

    pub(crate) fn records_by_doc_id(&self, doc_id: u64) -> Result<Vec<Record>> {
        self.records_matching_query(Box::new(TermQuery::new(
            Term::from_field_u64(self.fields.doc_id, doc_id),
            IndexRecordOption::Basic,
        )))
    }

    pub(crate) fn records_by_event_id(&self, event_id: &str) -> Result<Vec<Record>> {
        self.records_matching_query(Box::new(TermQuery::new(
            Term::from_field_text(self.fields.event_id, event_id),
            IndexRecordOption::Basic,
        )))
    }

    /// Returns `None` when this index predates the canonical ID field. Callers can then use a
    /// scoped fallback; rebuilding the index enables direct canonical-ID lookup.
    pub(crate) fn records_by_canonical_id(&self, record_id: &str) -> Result<Option<Vec<Record>>> {
        let Some(field) = self.fields.canonical_record_id else {
            return Ok(None);
        };
        self.records_matching_query(Box::new(TermQuery::new(
            Term::from_field_text(field, record_id),
            IndexRecordOption::Basic,
        )))
        .map(Some)
    }

    /// Resolve up to two canonical-ID matches within one session scope. Current indexes use the
    /// canonical-ID term directly. Legacy indexes must inspect stored records, but the fallback is
    /// constrained to the supplied session/source/path scope and preserves session order.
    pub fn records_by_canonical_id_in_session_scope(
        &self,
        record_id: &str,
        session_id: &str,
        source_path: Option<&str>,
        source: Option<crate::types::SourceKind>,
    ) -> Result<Vec<Record>> {
        let reader = self.reader()?;
        records_by_canonical_id_in_session_scope(
            &reader.searcher(),
            &self.fields,
            record_id,
            session_id,
            source_path,
            source,
        )
    }

    pub(crate) fn records_by_context_scope(
        &self,
        session_id: Option<&str>,
        source: Option<crate::types::SourceKind>,
    ) -> Result<Vec<Record>> {
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        if let Some(session_id) = session_id {
            clauses.push((
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(self.fields.session_id, session_id),
                    IndexRecordOption::Basic,
                )),
            ));
        }
        if let Some(source) = source
            && let Some(query) = exact_source_query(&self.fields, source)
        {
            clauses.push((Occur::Must, query));
        }
        let query: Box<dyn Query> = if clauses.is_empty() {
            Box::new(AllQuery)
        } else {
            Box::new(BooleanQuery::new(clauses))
        };
        self.records_matching_query(query)
    }

    pub(crate) fn records_by_session_path(
        &self,
        source: crate::types::SourceKind,
        session_id: &str,
        source_path: &str,
    ) -> Result<Vec<Record>> {
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = vec![
            (
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(self.fields.session_id, session_id),
                    IndexRecordOption::Basic,
                )),
            ),
            (
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(self.fields.source_path, source_path),
                    IndexRecordOption::Basic,
                )),
            ),
        ];
        if let Some(query) = exact_source_query(&self.fields, source) {
            clauses.push((Occur::Must, query));
        }
        self.records_matching_query(Box::new(BooleanQuery::new(clauses)))
    }

    fn records_matching_query(&self, query: Box<dyn Query>) -> Result<Vec<Record>> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let count = searcher.search(query.as_ref(), &Count)?;
        if count == 0 {
            return Ok(Vec::new());
        }
        let top_docs = searcher.search(query.as_ref(), &TopDocs::with_limit(count))?;
        top_docs
            .into_iter()
            .map(|(_, address)| {
                let doc = searcher.doc::<TantivyDocument>(address)?;
                Ok(record_from_doc(&self.fields, &doc))
            })
            .collect()
    }

    pub fn search(&self, options: &QueryOptions) -> Result<Vec<(f32, Record)>> {
        crate::profiling::span!("lexical.search");
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let query = build_query(&self.fields, options, &self.index)?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(options.limit))?;
        let mut results = Vec::with_capacity(top_docs.len());
        for (score, addr) in top_docs {
            let doc = searcher.doc::<TantivyDocument>(addr)?;
            results.push((score, record_from_doc(&self.fields, &doc)));
        }
        Ok(results)
    }

    /// Search matching records in timestamp order without changing the relevance-ordered search
    /// used by the CLI and TUI. `doc_id` makes equal timestamps deterministic across pages.
    pub fn search_by_timestamp(
        &self,
        options: &QueryOptions,
        order: TimestampOrder,
    ) -> Result<Vec<Record>> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let query = build_query(&self.fields, options, &self.index)?;
        let addresses: Vec<tantivy::DocAddress> = match order {
            TimestampOrder::Newest => searcher
                .search(
                    &query,
                    &TopDocs::with_limit(options.limit.max(1))
                        .custom_score(TimestampDescendingScorerFactory),
                )?
                .into_iter()
                .map(|(_, address)| address)
                .collect(),
            TimestampOrder::Oldest => searcher
                .search(
                    &query,
                    &TopDocs::with_limit(options.limit.max(1))
                        .custom_score(TimestampAscendingScorerFactory),
                )?
                .into_iter()
                .map(|(_, address)| address)
                .collect(),
        };
        addresses
            .into_iter()
            .map(|address| {
                let doc = searcher.doc::<TantivyDocument>(address)?;
                Ok(record_from_doc(&self.fields, &doc))
            })
            .collect()
    }

    /// Collect every exact session identity matching a lexical query without retaining scores,
    /// document addresses, or full records. Current indexes read identity fast fields while
    /// older compatible indexes fall back to hydrating one stored document at a time.
    pub fn session_scopes_matching_query(
        &self,
        options: &QueryOptions,
    ) -> Result<HashSet<(crate::types::SourceKind, String, String)>> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let query = build_query(&self.fields, options, &self.index)?;
        let schema = self.index.schema();
        let fast_session_identity = self.fields.source.is_some_and(|source| {
            [source, self.fields.session_id, self.fields.source_path]
                .into_iter()
                .all(|field| schema.get_field_entry(field).is_fast())
        });
        Ok(searcher.search(
            &query,
            &SessionScopeCollector {
                fields: self.fields.clone(),
                fast_session_identity,
            },
        )??)
    }

    /// Interactive exact counts must never hydrate legacy stored records.
    pub fn fast_session_scopes_matching_query(
        &self,
        options: &QueryOptions,
    ) -> Result<Option<HashSet<SessionScopeIdentity>>> {
        let schema = self.index.schema();
        if !self.fields.source.is_some_and(|source| {
            [source, self.fields.session_id, self.fields.source_path]
                .into_iter()
                .all(|field| schema.get_field_entry(field).is_fast())
        }) {
            return Ok(None);
        }
        let reader = self.reader()?;
        let searcher = reader.searcher();
        // A compatible schema alone is insufficient for mixed/older segments.
        for segment in searcher.segment_readers() {
            for field in ["source", "session_id", "source_path"] {
                if segment.fast_fields().str(field)?.is_none() {
                    return Ok(None);
                }
            }
        }
        let query = build_query(&self.fields, options, &self.index)?;
        Ok(Some(searcher.search(
            &query,
            &SessionScopeCollector {
                fields: self.fields.clone(),
                fast_session_identity: true,
            },
        )??))
    }

    pub fn session_scope_has_matching_conversation_kind(
        &self,
        options: &QueryOptions,
        scope: &SessionScopeKey,
        kind: &str,
    ) -> Result<bool> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let mut scoped_options = options.clone();
        scoped_options.session_scope = Some(vec![scope.clone()]);
        let query = BooleanQuery::new(vec![
            (
                Occur::Must,
                build_query(&self.fields, &scoped_options, &self.index)?,
            ),
            (
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(self.fields.conversation_kind, kind),
                    IndexRecordOption::Basic,
                )),
            ),
        ]);
        Ok(!searcher.search(&query, &TopDocs::with_limit(1))?.is_empty())
    }

    pub(crate) fn doc_ids_matching_filters(&self, options: &QueryOptions) -> Result<HashSet<u64>> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let mut filter_options = options.clone();
        filter_options.query.clear();
        let query = build_query(&self.fields, &filter_options, &self.index)?;
        let count = searcher.search(query.as_ref(), &Count)?;
        if count == 0 {
            return Ok(HashSet::new());
        }

        // This query-scoped set trades memory proportional to the filtered lexical matches for
        // native filtered traversal without requesting or re-querying the entire vector corpus.
        let collector = TopDocs::with_limit(count).order_by_fast_field::<u64>("doc_id", Order::Asc);
        let doc_ids: Vec<(u64, tantivy::DocAddress)> =
            searcher.search(query.as_ref(), &collector)?;
        Ok(doc_ids.into_iter().map(|(doc_id, _)| doc_id).collect())
    }

    pub fn records_by_session_id(&self, session_id: &str) -> Result<Vec<Record>> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let term = Term::from_field_text(self.fields.session_id, session_id);
        let query = TermQuery::new(term, IndexRecordOption::Basic);
        let limit = searcher.num_docs() as usize;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;
        let mut records = Vec::with_capacity(top_docs.len());
        for (_score, addr) in top_docs {
            let doc = searcher.doc::<TantivyDocument>(addr)?;
            records.push(record_from_doc(&self.fields, &doc));
        }
        Ok(records)
    }

    /// Resolve up to two exact identities for a session with bounded document hydration.
    /// Two records are sufficient for callers to reject ambiguous legacy deep links.
    pub fn matching_session_scopes(
        &self,
        session_id: &str,
        source_path: Option<&str>,
        source: Option<crate::types::SourceKind>,
    ) -> Result<Vec<(crate::types::SourceKind, String)>> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let partial = session_query(&self.fields, session_id, source_path, source, None);
        let representative = searcher.search(&partial, &TopDocs::with_limit(1))?;
        let Some((_, representative_address)) = representative.first() else {
            return Ok(Vec::new());
        };
        let representative_doc = searcher.doc::<TantivyDocument>(*representative_address)?;
        let representative = record_from_doc(&self.fields, &representative_doc);
        let mut matches = vec![(representative.source, representative.source_path.clone())];

        let mut identity: Vec<(Occur, Box<dyn Query>)> = vec![(
            Occur::Must,
            Box::new(TermQuery::new(
                Term::from_field_text(self.fields.source_path, &representative.source_path),
                IndexRecordOption::Basic,
            )),
        )];
        if let Some(source_query) = exact_source_query(&self.fields, representative.source) {
            identity.push((Occur::Must, source_query));
        }
        let other_query = BooleanQuery::new(vec![
            (
                Occur::Must,
                Box::new(session_query(
                    &self.fields,
                    session_id,
                    source_path,
                    source,
                    None,
                )),
            ),
            (Occur::MustNot, Box::new(BooleanQuery::new(identity))),
        ]);
        if let Some((_, other_address)) = searcher
            .search(&other_query, &TopDocs::with_limit(1))?
            .first()
        {
            let other_doc = searcher.doc::<TantivyDocument>(*other_address)?;
            let other = record_from_doc(&self.fields, &other_doc);
            matches.push((other.source, other.source_path));
        }
        Ok(matches)
    }

    pub fn records_by_session_id_page(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<Record>, usize)> {
        self.records_by_session_path_page(session_id, None, offset, limit)
    }

    /// Find a canonical record in an exact session scope and return a page centered on it.
    /// Current indexes hydrate only the matching anchor and returned page. Legacy indexes scan
    /// stored records within the supplied scope to reconstruct canonical IDs before paging.
    pub fn records_by_session_path_around(
        &self,
        session_id: &str,
        source_path: Option<&str>,
        record_id: &str,
        limit: usize,
    ) -> Result<Option<(Vec<Record>, usize, usize)>> {
        self.records_by_session_scope_around(session_id, source_path, None, record_id, limit)
    }

    pub fn records_by_session_scope_around(
        &self,
        session_id: &str,
        source_path: Option<&str>,
        source: Option<crate::types::SourceKind>,
        record_id: &str,
        limit: usize,
    ) -> Result<Option<(Vec<Record>, usize, usize)>> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let anchors = records_by_canonical_id_in_session_scope(
            &searcher,
            &self.fields,
            record_id,
            session_id,
            source_path,
            source,
        )?;
        let Some(anchor) = anchors.into_iter().next() else {
            return Ok(None);
        };

        let predecessors = BooleanQuery::new(vec![
            (
                Occur::Should,
                Box::new(RangeQuery::new_u64_bounds(
                    "turn_id".to_string(),
                    Bound::Unbounded,
                    Bound::Excluded(u64::from(anchor.turn_id)),
                )) as Box<dyn Query>,
            ),
            (
                Occur::Should,
                Box::new(BooleanQuery::new(vec![
                    (
                        Occur::Must,
                        Box::new(TermQuery::new(
                            Term::from_field_u64(self.fields.turn_id, u64::from(anchor.turn_id)),
                            IndexRecordOption::Basic,
                        )) as Box<dyn Query>,
                    ),
                    (
                        Occur::Must,
                        Box::new(RangeQuery::new_u64_bounds(
                            "ts".to_string(),
                            Bound::Unbounded,
                            Bound::Excluded(anchor.ts),
                        )) as Box<dyn Query>,
                    ),
                ])) as Box<dyn Query>,
            ),
            (
                Occur::Should,
                Box::new(BooleanQuery::new(vec![
                    (
                        Occur::Must,
                        Box::new(TermQuery::new(
                            Term::from_field_u64(self.fields.turn_id, u64::from(anchor.turn_id)),
                            IndexRecordOption::Basic,
                        )) as Box<dyn Query>,
                    ),
                    (
                        Occur::Must,
                        Box::new(TermQuery::new(
                            Term::from_field_u64(self.fields.ts, anchor.ts),
                            IndexRecordOption::Basic,
                        )) as Box<dyn Query>,
                    ),
                    (
                        Occur::Must,
                        Box::new(RangeQuery::new_u64_bounds(
                            "doc_id".to_string(),
                            Bound::Unbounded,
                            Bound::Excluded(anchor.doc_id),
                        )) as Box<dyn Query>,
                    ),
                ])) as Box<dyn Query>,
            ),
        ]);
        let before_query = session_query(
            &self.fields,
            session_id,
            source_path,
            source,
            Some(Box::new(predecessors)),
        );
        let anchor_offset = searcher.search(&before_query, &Count)?;
        let whole_session_query =
            session_query(&self.fields, session_id, source_path, source, None);
        let total = searcher.search(&whole_session_query, &Count)?;
        let limit = limit.max(1).min(total);
        let mut before_count = anchor_offset.min(limit / 2);
        let after_available = total - anchor_offset;
        let after_count = after_available.min(limit - before_count);
        before_count = anchor_offset.min(limit - after_count);
        let offset = anchor_offset - before_count;

        let mut addresses = searcher
            .search(
                &before_query,
                &TopDocs::with_limit(before_count.max(1))
                    .custom_score(SessionReverseOrderScorerFactory),
            )?
            .into_iter()
            .take(before_count)
            .map(|(_, address)| address)
            .collect::<Vec<_>>();

        let successors = BooleanQuery::new(vec![
            (
                Occur::Should,
                Box::new(RangeQuery::new_u64_bounds(
                    "turn_id".to_string(),
                    Bound::Excluded(u64::from(anchor.turn_id)),
                    Bound::Unbounded,
                )) as Box<dyn Query>,
            ),
            (
                Occur::Should,
                Box::new(BooleanQuery::new(vec![
                    (
                        Occur::Must,
                        Box::new(TermQuery::new(
                            Term::from_field_u64(self.fields.turn_id, u64::from(anchor.turn_id)),
                            IndexRecordOption::Basic,
                        )) as Box<dyn Query>,
                    ),
                    (
                        Occur::Must,
                        Box::new(RangeQuery::new_u64_bounds(
                            "ts".to_string(),
                            Bound::Excluded(anchor.ts),
                            Bound::Unbounded,
                        )) as Box<dyn Query>,
                    ),
                ])) as Box<dyn Query>,
            ),
            (
                Occur::Should,
                Box::new(BooleanQuery::new(vec![
                    (
                        Occur::Must,
                        Box::new(TermQuery::new(
                            Term::from_field_u64(self.fields.turn_id, u64::from(anchor.turn_id)),
                            IndexRecordOption::Basic,
                        )) as Box<dyn Query>,
                    ),
                    (
                        Occur::Must,
                        Box::new(TermQuery::new(
                            Term::from_field_u64(self.fields.ts, anchor.ts),
                            IndexRecordOption::Basic,
                        )) as Box<dyn Query>,
                    ),
                    (
                        Occur::Must,
                        Box::new(RangeQuery::new_u64_bounds(
                            "doc_id".to_string(),
                            Bound::Included(anchor.doc_id),
                            Bound::Unbounded,
                        )) as Box<dyn Query>,
                    ),
                ])) as Box<dyn Query>,
            ),
        ]);
        let after_query = session_query(
            &self.fields,
            session_id,
            source_path,
            source,
            Some(Box::new(successors)),
        );
        addresses.extend(
            searcher
                .search(
                    &after_query,
                    &TopDocs::with_limit(after_count.max(1))
                        .custom_score(SessionOrderScorerFactory),
                )?
                .into_iter()
                .take(after_count)
                .map(|(_, address)| address),
        );
        let mut records = addresses
            .into_iter()
            .map(|address| {
                let doc = searcher.doc::<TantivyDocument>(address)?;
                Ok(record_from_doc(&self.fields, &doc))
            })
            .collect::<Result<Vec<_>>>()?;
        records.sort_by(|a, b| {
            a.turn_id
                .cmp(&b.turn_id)
                .then_with(|| a.ts.cmp(&b.ts))
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        Ok(Some((records, total, offset)))
    }

    /// Read one globally ordered session page without materializing record content outside the
    /// requested offset and limit. `source_path` is an exact indexed filter when supplied.
    pub fn records_by_session_path_page(
        &self,
        session_id: &str,
        source_path: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<Record>, usize)> {
        self.records_by_session_scope_page(session_id, source_path, None, offset, limit)
    }

    pub fn records_by_session_scope_page(
        &self,
        session_id: &str,
        source_path: Option<&str>,
        source: Option<crate::types::SourceKind>,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<Record>, usize)> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let query = session_query(&self.fields, session_id, source_path, source, None);
        let total = searcher.search(&query, &Count)?;
        if offset >= total {
            return Ok((Vec::new(), total));
        }
        let page_limit = limit.max(1).min(total - offset);
        let remaining_after = total - offset - page_limit;
        let addresses = if offset <= remaining_after {
            searcher
                .search(
                    &query,
                    &TopDocs::with_limit(page_limit)
                        .and_offset(offset)
                        .custom_score(SessionOrderScorerFactory),
                )?
                .into_iter()
                .map(|(_, address)| address)
                .collect::<Vec<_>>()
        } else {
            searcher
                .search(
                    &query,
                    &TopDocs::with_limit(page_limit)
                        .and_offset(remaining_after)
                        .custom_score(SessionReverseOrderScorerFactory),
                )?
                .into_iter()
                .map(|(_, address)| address)
                .collect::<Vec<_>>()
        };
        let mut records = Vec::with_capacity(addresses.len());
        for addr in addresses {
            let doc = searcher.doc::<TantivyDocument>(addr)?;
            records.push(record_from_doc(&self.fields, &doc));
        }
        records.sort_by(|a, b| {
            a.turn_id
                .cmp(&b.turn_id)
                .then_with(|| a.ts.cmp(&b.ts))
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        Ok((records, total))
    }

    /// Return timestamp bounds for the complete exact session scope without hydrating content.
    pub fn session_time_bounds(
        &self,
        session_id: &str,
        source_path: Option<&str>,
    ) -> Result<Option<(u64, u64)>> {
        self.session_scope_time_bounds(session_id, source_path, None)
    }

    pub fn session_scope_time_bounds(
        &self,
        session_id: &str,
        source_path: Option<&str>,
        source: Option<crate::types::SourceKind>,
    ) -> Result<Option<(u64, u64)>> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let query = session_query(&self.fields, session_id, source_path, source, None);
        let oldest: Vec<(u64, tantivy::DocAddress)> = searcher.search(
            &query,
            &TopDocs::with_limit(1).order_by_fast_field::<u64>("ts", Order::Asc),
        )?;
        let newest: Vec<(u64, tantivy::DocAddress)> = searcher.search(
            &query,
            &TopDocs::with_limit(1).order_by_fast_field::<u64>("ts", Order::Desc),
        )?;
        Ok(oldest
            .first()
            .zip(newest.first())
            .map(|((oldest, _), (newest, _))| (*oldest, *newest)))
    }

    pub fn recent_records(&self, limit: usize) -> Result<Vec<Record>> {
        self.recent_records_filtered(limit, None, None)
    }

    pub fn recent_records_for_source(
        &self,
        limit: usize,
        source: Option<SourceFilter>,
    ) -> Result<Vec<Record>> {
        self.recent_records_filtered(limit, source, None)
    }

    pub fn recent_records_filtered(
        &self,
        limit: usize,
        source: Option<SourceFilter>,
        project: Option<&str>,
    ) -> Result<Vec<Record>> {
        self.recent_records_filtered_since(limit, source, project, None)
    }

    pub fn recent_records_filtered_since(
        &self,
        limit: usize,
        source: Option<SourceFilter>,
        project: Option<&str>,
        since: Option<u64>,
    ) -> Result<Vec<Record>> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        if let Some(source) = source
            && let Some(field) = self.fields.source
        {
            let terms = source
                .storage_labels()
                .iter()
                .map(|label| {
                    (
                        Occur::Should,
                        Box::new(TermQuery::new(
                            Term::from_field_text(field, label),
                            IndexRecordOption::Basic,
                        )) as Box<dyn Query>,
                    )
                })
                .collect();
            clauses.push((Occur::Must, Box::new(BooleanQuery::new(terms))));
        }
        if let Some(project) = project {
            clauses.push((
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(self.fields.project, project),
                    IndexRecordOption::Basic,
                )),
            ));
        }
        if let Some(since) = since {
            clauses.push((
                Occur::Must,
                Box::new(RangeQuery::new_u64_bounds(
                    "ts".to_string(),
                    Bound::Included(since),
                    Bound::Unbounded,
                )),
            ));
        }
        let query: Box<dyn Query> = if clauses.is_empty() {
            Box::new(AllQuery)
        } else {
            Box::new(BooleanQuery::new(clauses))
        };
        let collector =
            TopDocs::with_limit(limit.max(1)).order_by_fast_field::<u64>("ts", Order::Desc);
        let top_docs: Vec<(u64, tantivy::DocAddress)> =
            searcher.search(query.as_ref(), &collector)?;
        let mut records = Vec::with_capacity(top_docs.len());
        for (_ts, addr) in top_docs {
            let doc = searcher.doc::<TantivyDocument>(addr)?;
            records.push(record_from_doc(&self.fields, &doc));
        }
        Ok(records)
    }

    pub fn doc_count(&self) -> Result<usize> {
        let reader = self.reader()?;
        Ok(reader.searcher().num_docs() as usize)
    }

    pub fn for_each_record<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(Record) -> Result<()>,
    {
        crate::profiling::span!("lexical.walk_records");
        let reader = self.reader()?;
        let searcher = reader.searcher();
        for segment_reader in searcher.segment_readers() {
            let store = segment_reader.get_store_reader(0)?;
            for doc in store.iter::<TantivyDocument>(segment_reader.alive_bitset()) {
                let doc = doc?;
                let record = record_from_doc(&self.fields, &doc);
                crate::profiling::count!("lexical.records_walked", 1);
                f(record)?;
            }
        }
        Ok(())
    }
}

fn source_scope_query(fields: &IndexFields, scope: &SessionScope) -> BooleanQuery {
    BooleanQuery::new(vec![
        (
            Occur::Must,
            Box::new(TermQuery::new(
                Term::from_field_text(fields.source_path, &scope.source_path),
                IndexRecordOption::Basic,
            )),
        ),
        (
            Occur::Must,
            Box::new(TermQuery::new(
                Term::from_field_text(fields.session_id, &scope.session_id),
                IndexRecordOption::Basic,
            )),
        ),
    ])
}

fn exact_source_query(
    fields: &IndexFields,
    source: crate::types::SourceKind,
) -> Option<Box<dyn Query>> {
    let field = fields.source?;
    let labels: &[&str] = match source {
        // Historical Codex projections used these labels. `record_from_doc` maps all three to
        // one source identity, so exact context lookup must preserve the same compatibility.
        crate::types::SourceKind::Codex => &["codex", "codex-session", "codex-history"],
        _ => &[source.storage_label()],
    };
    let queries = labels
        .iter()
        .map(|label| {
            (
                Occur::Should,
                Box::new(TermQuery::new(
                    Term::from_field_text(field, label),
                    IndexRecordOption::Basic,
                )) as Box<dyn Query>,
            )
        })
        .collect();
    Some(Box::new(BooleanQuery::new(queries)))
}

fn records_by_canonical_id_in_session_scope(
    searcher: &tantivy::Searcher,
    fields: &IndexFields,
    record_id: &str,
    session_id: &str,
    source_path: Option<&str>,
    source: Option<crate::types::SourceKind>,
) -> Result<Vec<Record>> {
    let scope = if let Some(canonical_record_id) = fields.canonical_record_id {
        session_query(
            fields,
            session_id,
            source_path,
            source,
            Some(Box::new(TermQuery::new(
                Term::from_field_text(canonical_record_id, record_id),
                IndexRecordOption::Basic,
            ))),
        )
    } else {
        session_query(fields, session_id, source_path, source, None)
    };
    let limit = if fields.canonical_record_id.is_some() {
        2
    } else {
        searcher.search(&scope, &Count)?
    };
    if limit == 0 {
        return Ok(Vec::new());
    }
    let addresses = searcher.search(
        &scope,
        &TopDocs::with_limit(limit).custom_score(SessionOrderScorerFactory),
    )?;
    let mut matches = Vec::new();
    for (_, address) in addresses {
        let doc = searcher.doc::<TantivyDocument>(address)?;
        let record = record_from_doc(fields, &doc);
        if crate::retrieval::canonical_record_id(&record) == record_id {
            matches.push(record);
            if matches.len() == 2 {
                break;
            }
        }
    }
    matches.sort_by(|a, b| {
        a.turn_id
            .cmp(&b.turn_id)
            .then_with(|| a.ts.cmp(&b.ts))
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    Ok(matches)
}

fn session_query(
    fields: &IndexFields,
    session_id: &str,
    source_path: Option<&str>,
    source: Option<crate::types::SourceKind>,
    extra: Option<Box<dyn Query>>,
) -> BooleanQuery {
    let mut clauses: Vec<(Occur, Box<dyn Query>)> = vec![(
        Occur::Must,
        Box::new(TermQuery::new(
            Term::from_field_text(fields.session_id, session_id),
            IndexRecordOption::Basic,
        )),
    )];
    if let Some(source_path) = source_path {
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(
                Term::from_field_text(fields.source_path, source_path),
                IndexRecordOption::Basic,
            )),
        ));
    }
    if let Some(source) = source
        && let Some(query) = exact_source_query(fields, source)
    {
        clauses.push((Occur::Must, query));
    }
    if let Some(extra) = extra {
        clauses.push((Occur::Must, extra));
    }
    BooleanQuery::new(clauses)
}

type SessionOrder = std::cmp::Reverse<(u64, u64, u64)>;
type SessionReverseOrder = (u64, u64, u64);
type TimestampAscending = std::cmp::Reverse<(u64, u64)>;
type TimestampDescending = (u64, u64);

struct SessionOrderScorerFactory;
struct SessionReverseOrderScorerFactory;
struct TimestampAscendingScorerFactory;
struct TimestampDescendingScorerFactory;

struct SessionOrderScorer {
    turn_ids: Arc<dyn tantivy::columnar::ColumnValues<u64>>,
    timestamps: Arc<dyn tantivy::columnar::ColumnValues<u64>>,
    doc_ids: Arc<dyn tantivy::columnar::ColumnValues<u64>>,
}

struct TimestampScorer {
    timestamps: Arc<dyn tantivy::columnar::ColumnValues<u64>>,
    doc_ids: Arc<dyn tantivy::columnar::ColumnValues<u64>>,
}

impl TimestampScorer {
    fn for_segment(segment_reader: &tantivy::SegmentReader) -> tantivy::Result<Self> {
        Ok(Self {
            timestamps: segment_reader
                .fast_fields()
                .u64("ts")?
                .first_or_default_col(0),
            doc_ids: segment_reader
                .fast_fields()
                .u64("doc_id")?
                .first_or_default_col(0),
        })
    }
}

impl tantivy::collector::CustomScorer<TimestampAscending> for TimestampAscendingScorerFactory {
    type Child = TimestampScorer;

    fn segment_scorer(
        &self,
        segment_reader: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        TimestampScorer::for_segment(segment_reader)
    }
}

impl tantivy::collector::CustomScorer<TimestampDescending> for TimestampDescendingScorerFactory {
    type Child = TimestampScorer;

    fn segment_scorer(
        &self,
        segment_reader: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        TimestampScorer::for_segment(segment_reader)
    }
}

impl tantivy::collector::CustomSegmentScorer<TimestampAscending> for TimestampScorer {
    fn score(&mut self, doc: tantivy::DocId) -> TimestampAscending {
        std::cmp::Reverse((self.timestamps.get_val(doc), self.doc_ids.get_val(doc)))
    }
}

impl tantivy::collector::CustomSegmentScorer<TimestampDescending> for TimestampScorer {
    fn score(&mut self, doc: tantivy::DocId) -> TimestampDescending {
        (self.timestamps.get_val(doc), self.doc_ids.get_val(doc))
    }
}

impl tantivy::collector::CustomScorer<SessionOrder> for SessionOrderScorerFactory {
    type Child = SessionOrderScorer;

    fn segment_scorer(
        &self,
        segment_reader: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(SessionOrderScorer {
            turn_ids: segment_reader
                .fast_fields()
                .u64("turn_id")?
                .first_or_default_col(0),
            timestamps: segment_reader
                .fast_fields()
                .u64("ts")?
                .first_or_default_col(0),
            doc_ids: segment_reader
                .fast_fields()
                .u64("doc_id")?
                .first_or_default_col(0),
        })
    }
}

impl tantivy::collector::CustomScorer<SessionReverseOrder> for SessionReverseOrderScorerFactory {
    type Child = SessionOrderScorer;

    fn segment_scorer(
        &self,
        segment_reader: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(SessionOrderScorer {
            turn_ids: segment_reader
                .fast_fields()
                .u64("turn_id")?
                .first_or_default_col(0),
            timestamps: segment_reader
                .fast_fields()
                .u64("ts")?
                .first_or_default_col(0),
            doc_ids: segment_reader
                .fast_fields()
                .u64("doc_id")?
                .first_or_default_col(0),
        })
    }
}

impl tantivy::collector::CustomSegmentScorer<SessionOrder> for SessionOrderScorer {
    fn score(&mut self, doc: tantivy::DocId) -> SessionOrder {
        std::cmp::Reverse((
            self.turn_ids.get_val(doc),
            self.timestamps.get_val(doc),
            self.doc_ids.get_val(doc),
        ))
    }
}

impl tantivy::collector::CustomSegmentScorer<SessionReverseOrder> for SessionOrderScorer {
    fn score(&mut self, doc: tantivy::DocId) -> SessionReverseOrder {
        (
            self.turn_ids.get_val(doc),
            self.timestamps.get_val(doc),
            self.doc_ids.get_val(doc),
        )
    }
}

fn stale_schema_error(dir: &Path) -> anyhow::Error {
    anyhow!(
        "index schema at {} is stale; run `memex index` or `memex index rebuild` to rebuild it",
        dir.display()
    )
}

fn create_index_in_dir(dir: &Path) -> Result<SearchIndex> {
    let schema = build_schema()?;
    let index = Index::create_in_dir(dir, schema.clone())?;
    let fields = load_fields(schema)?;
    Ok(SearchIndex {
        index,
        fields,
        snapshot_version: snapshot_version_for_path(dir),
        writable: true,
        pending_generation: None,
        _generation_lease: None,
        incremental_merge_policy: false,
        defer_merges: false,
        shared_reader: Arc::new(OnceLock::new()),
    })
}

fn open_sealed_generation(dir: &Path) -> Result<SearchIndex> {
    let generation_lease = Arc::new(acquire_generation_lease(dir)?);
    let directory: Box<dyn Directory> = if let Some(shared) =
        storage::SharedDirectory::open(storage::index_root(dir), dir, true)?
    {
        shared.pin_generation(Arc::clone(&generation_lease));
        Box::new(shared)
    } else {
        Box::new(SealedDirectory {
            directory: MmapDirectory::open(dir)?,
            _generation_lease: Some(Arc::clone(&generation_lease)),
        })
    };
    let index = Index::open(directory)?;
    if !schema_is_current(&index.schema()) {
        return Err(stale_schema_error(dir));
    }
    let fields = load_fields(index.schema())?;
    Ok(SearchIndex {
        index,
        fields,
        snapshot_version: snapshot_version_for_path(dir),
        writable: false,
        pending_generation: None,
        _generation_lease: Some(generation_lease),
        incremental_merge_policy: false,
        defer_merges: false,
        shared_reader: Arc::new(OnceLock::new()),
    })
}

fn resolve_current_generation(index_root: &Path) -> Option<PathBuf> {
    let name = fs::read_to_string(index_root.join(CURRENT_FILE)).ok()?;
    let name = name.trim();
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return None;
    }
    Some(index_root.join(GENERATIONS_DIR).join(name))
}

fn snapshot_version_for_path(path: &Path) -> String {
    if path
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == GENERATIONS_DIR)
    {
        return path
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("unknown")
            .trim_start_matches('.')
            .trim_end_matches(".tmp")
            .to_string();
    }
    let metadata = fs::read(path.join("meta.json")).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(&metadata);
    format!("legacy-{:x}", hasher.finalize())
}

fn new_generation_name() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("{nanos:032x}-{:08x}", std::process::id())
}

fn is_abandoned_generation_workdir(name: &std::ffi::OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    if name.starts_with(".gc-") {
        return true;
    }
    let Some(generation) = name
        .strip_prefix('.')
        .and_then(|name| name.strip_suffix(".tmp"))
    else {
        return false;
    };
    let Some((timestamp, pid)) = generation.split_once('-') else {
        return false;
    };
    timestamp.len() == 32
        && pid.len() == 8
        && timestamp.bytes().all(|byte| byte.is_ascii_hexdigit())
        && pid.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
fn clone_generation(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)?;
    let directory = storage::open_directory(storage::index_root(source), source)?;
    for name in committed_generation_files(source)? {
        if directory.exists(&name)? {
            fs::write(destination.join(&name), directory.atomic_read(&name)?)?;
        }
    }
    Ok(())
}

fn committed_generation_files(source: &Path) -> Result<HashSet<PathBuf>> {
    let index = Index::open(storage::open_directory(
        storage::index_root(source),
        source,
    )?)?;
    committed_files(&index)
}

fn committed_files(index: &Index) -> Result<HashSet<PathBuf>> {
    let mut files: HashSet<PathBuf> = index
        .searchable_segment_metas()?
        .into_iter()
        .flat_map(|segment| segment.list_files())
        .collect();
    files.insert(PathBuf::from("meta.json"));
    files.insert(PathBuf::from(".managed.json"));
    Ok(files)
}

fn rewrite_managed_files_to_committed_set(generation: &Path) -> Result<()> {
    let directory = storage::open_directory(storage::index_root(generation), generation)?;
    let mut managed = HashSet::new();
    for path in committed_generation_files(generation)? {
        if directory.exists(&path)?
            && path
                .file_name()
                .is_none_or(|name| !name.to_string_lossy().starts_with('.'))
        {
            managed.insert(path);
        }
    }
    let mut encoded = serde_json::to_vec(&managed)?;
    encoded.push(b'\n');
    fs::write(generation.join(".managed.json"), encoded)?;
    Ok(())
}

fn validate_committed_generation(generation: &Path) -> Result<u64> {
    let index = Index::open(storage::open_directory(
        storage::index_root(generation),
        generation,
    )?)
    .with_context(|| format!("validate committed generation {}", generation.display()))?;
    let damaged = index.validate_checksum()?;
    if !damaged.is_empty() {
        bail!(
            "index generation {} has {} damaged files",
            generation.display(),
            damaged.len()
        );
    }
    let reader = index.reader()?;
    Ok(reader.searcher().num_docs())
}

fn create_generation_lease_file(generation: &Path) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(generation.join(GENERATION_LEASE_FILE))?;
    fsync_file(&file)?;
    Ok(())
}

fn acquire_generation_lease(generation: &Path) -> Result<GenerationLease> {
    let path = generation.join(GENERATION_LEASE_FILE);
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(GenerationLease { file: None });
        }
        Err(error) => return Err(error.into()),
    };
    lock_generation_shared(&file)?;
    Ok(GenerationLease { file: Some(file) })
}

fn prune_superseded_generations(index_root: &Path, current: &str) -> Result<()> {
    prune_superseded_generations_with_sync(index_root, current, fsync_directory)
}

fn prune_superseded_generations_with_sync(
    index_root: &Path,
    current: &str,
    synchronize: impl FnOnce(&Path) -> io::Result<()>,
) -> Result<()> {
    crate::profiling::span!("lexical.cleanup.generations");
    crate::profiling::count!("lexical.cleanup.generations.calls", 1);
    let generations = index_root.join(GENERATIONS_DIR);
    let mut removal_attempted = false;
    for entry in fs::read_dir(&generations)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name_text = name.to_string_lossy();
        if name_text == current {
            continue;
        }
        if is_abandoned_generation_workdir(&name) {
            // Only automatically remove workdirs created by lease-aware versions. An older
            // Memex process can retain a live writable staging index after releasing its ingest
            // lease, so an unleased workdir is not proof that the owner is dead. Explicit offline
            // GC may remove those after the user confirms all readers and writers are stopped.
            let lease_path = entry.path().join(GENERATION_LEASE_FILE);
            if !lease_path.is_file() || name_text.starts_with(".gc-") {
                continue;
            }
            let Some(_lease) = try_lock_generation_exclusive(&entry.path())? else {
                continue;
            };
            removal_attempted = true;
            fs::remove_dir_all(entry.path()).with_context(|| {
                format!(
                    "remove abandoned index generation work directory {}",
                    entry.path().display()
                )
            })?;
            continue;
        }
        if name_text.starts_with('.') {
            continue;
        }

        let lease_path = entry.path().join(GENERATION_LEASE_FILE);
        let _lease = if lease_path.is_file() {
            let Some(lease) = try_lock_generation_exclusive(&entry.path())? else {
                continue;
            };
            Some(lease)
        } else {
            // Pre-lease generations cannot advertise readers. On Unix, removing their directory
            // is safe even if an older process still has segment files open or memory-mapped. On
            // platforms that prohibit deleting open files, leave the generation for a later pass.
            None
        };
        removal_attempted = true;
        if let Err(error) = fs::remove_dir_all(entry.path())
            && error.kind() != io::ErrorKind::PermissionDenied
        {
            return Err(error).with_context(|| {
                format!(
                    "prune superseded index generation {}",
                    entry.path().display()
                )
            });
        }
    }
    if removal_attempted {
        crate::profiling::count!("lexical.cleanup.generations.sync_requests", 1);
        synchronize(&generations)?;
    } else {
        crate::profiling::count!("lexical.cleanup.generations.sync_skips", 1);
    }
    Ok(())
}

fn prune_legacy_index_files(index_root: &Path) -> Result<()> {
    prune_legacy_index_files_with_sync(index_root, fsync_directory)
}

fn prune_legacy_index_files_with_sync(
    index_root: &Path,
    synchronize: impl FnOnce(&Path) -> io::Result<()>,
) -> Result<()> {
    crate::profiling::span!("lexical.cleanup.legacy");
    crate::profiling::count!("lexical.cleanup.legacy.calls", 1);
    let mut removal_attempted = false;
    for entry in fs::read_dir(index_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() || entry.file_name() == CURRENT_FILE {
            continue;
        }
        removal_attempted = true;
        if let Err(error) = fs::remove_file(entry.path())
            && error.kind() != io::ErrorKind::PermissionDenied
        {
            return Err(error)
                .with_context(|| format!("prune legacy index file {}", entry.path().display()));
        }
    }
    if removal_attempted {
        crate::profiling::count!("lexical.cleanup.legacy.sync_requests", 1);
        synchronize(index_root)?;
    } else {
        crate::profiling::count!("lexical.cleanup.legacy.sync_skips", 1);
    }
    Ok(())
}

#[cfg(unix)]
fn lock_generation_shared(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_SH) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn lock_generation_shared(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn try_lock_generation_exclusive(generation: &Path) -> Result<Option<File>> {
    use std::os::fd::AsRawFd;
    let path = generation.join(GENERATION_LEASE_FILE);
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
        return Ok(Some(file));
    }
    let error = io::Error::last_os_error();
    let raw_error = error.raw_os_error();
    if raw_error == Some(libc::EWOULDBLOCK) || raw_error == Some(libc::EAGAIN) {
        Ok(None)
    } else {
        Err(error.into())
    }
}

#[cfg(not(unix))]
fn try_lock_generation_exclusive(_generation: &Path) -> Result<Option<File>> {
    Ok(None)
}

fn atomic_write_current(index_root: &Path, generation_name: &str) -> Result<()> {
    let mut temp = tempfile::NamedTempFile::new_in(index_root)?;
    temp.write_all(format!("{generation_name}\n").as_bytes())?;
    fsync_file(temp.as_file())?;
    temp.persist(index_root.join(CURRENT_FILE))?;
    // The one drive-cache flush of publication: everything written before it lands with it.
    sync_directory(index_root)?;
    Ok(())
}

#[cfg(unix)]
fn sync_directory(dir: &Path) -> io::Result<()> {
    use std::fs::File;
    File::open(dir)?.sync_all()
}

/// Orders writes without a drive-cache flush; the next full sync makes them durable.
#[cfg(unix)]
fn fsync_directory(dir: &Path) -> io::Result<()> {
    fsync_file(&File::open(dir)?)
}

#[cfg(unix)]
fn fsync_file(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    if unsafe { libc::fsync(file.as_raw_fd()) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn fsync_directory(dir: &Path) -> io::Result<()> {
    sync_directory(dir)
}

#[cfg(not(unix))]
fn fsync_file(file: &File) -> io::Result<()> {
    file.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_dir: &Path) -> io::Result<()> {
    Ok(())
}

fn build_schema() -> Result<Schema> {
    build_schema_with_canonical_record_id(true)
}

pub(crate) fn build_schema_with_canonical_record_id(
    include_canonical_record_id: bool,
) -> Result<Schema> {
    build_schema_with_options(include_canonical_record_id, true)
}

fn build_schema_with_options(
    include_canonical_record_id: bool,
    fast_session_identity: bool,
) -> Result<Schema> {
    let mut builder = SchemaBuilder::default();
    let session_identity_options = if fast_session_identity {
        STRING | STORED | FAST
    } else {
        STRING | STORED
    };

    if include_canonical_record_id {
        builder.add_text_field("canonical_record_id", STRING | STORED);
    }
    builder.add_u64_field("doc_id", INDEXED | STORED | FAST);
    builder.add_u64_field("ts", INDEXED | STORED | FAST);
    builder.add_text_field("project", STRING | STORED);
    builder.add_text_field("session_id", session_identity_options.clone());
    builder.add_u64_field("turn_id", INDEXED | STORED | FAST);
    builder.add_text_field("role", STRING | STORED);
    builder.add_text_field("source", session_identity_options.clone());

    // `en_stem` lowercases and applies the English Snowball stemmer, so a query for
    // "migration" also matches "migrations" and "migrated". Existing indexes keep the
    // tokenizer recorded in their on-disk schema until `memex index rebuild`.
    let text_indexing = TextFieldIndexing::default()
        .set_tokenizer("en_stem")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_options = TextOptions::default()
        .set_indexing_options(text_indexing)
        .set_stored();
    builder.add_text_field("text", text_options);

    builder.add_text_field("tool_name", STRING | STORED);
    // Queries only parse against `text`, which already carries a tool result's content;
    // indexing these too roughly doubled each segment's vocabulary. Existing indexes keep
    // their on-disk schema until `memex index rebuild`.
    builder.add_text_field("tool_input", STORED);
    builder.add_text_field("tool_output", STORED);
    builder.add_text_field("event_id", STRING | STORED);
    builder.add_text_field("parent_event_id", STRING | STORED);
    builder.add_text_field("logical_parent_event_id", STRING | STORED);
    builder.add_text_field("parent_session_id", STRING | STORED);
    builder.add_text_field("thread_source", STRING | STORED);
    builder.add_text_field("conversation_kind", STRING | STORED);
    builder.add_text_field("parent_tool_use_id", STRING | STORED);
    builder.add_text_field("source_tool_use_id", STRING | STORED);
    builder.add_text_field("source_tool_assistant_uuid", STRING | STORED);
    builder.add_text_field("reader_metadata", STORED);
    builder.add_text_field("source_path", session_identity_options);

    Ok(builder.build())
}

fn schema_is_current(schema: &Schema) -> bool {
    [
        "doc_id",
        "ts",
        "project",
        "session_id",
        "turn_id",
        "role",
        "source",
        "text",
        "tool_name",
        "tool_input",
        "tool_output",
        "event_id",
        "parent_event_id",
        "logical_parent_event_id",
        "parent_session_id",
        "thread_source",
        "conversation_kind",
        "parent_tool_use_id",
        "source_tool_use_id",
        "source_tool_assistant_uuid",
        "source_path",
    ]
    .into_iter()
    .all(|field| schema.get_field(field).is_ok())
}

fn load_fields(schema: Schema) -> Result<IndexFields> {
    let get = |name: &str| {
        schema
            .get_field(name)
            .map_err(|_| anyhow!(format!("missing field {name}")))
    };
    Ok(IndexFields {
        reader_metadata: schema.get_field("reader_metadata").ok(),
        canonical_record_id: schema.get_field("canonical_record_id").ok(),
        doc_id: get("doc_id")?,
        ts: get("ts")?,
        project: get("project")?,
        session_id: get("session_id")?,
        turn_id: get("turn_id")?,
        role: get("role")?,
        text: get("text")?,
        source: schema.get_field("source").ok(),
        tool_name: get("tool_name")?,
        tool_input: get("tool_input")?,
        tool_output: get("tool_output")?,
        event_id: get("event_id")?,
        parent_event_id: get("parent_event_id")?,
        logical_parent_event_id: get("logical_parent_event_id")?,
        parent_session_id: get("parent_session_id")?,
        thread_source: get("thread_source")?,
        conversation_kind: get("conversation_kind")?,
        parent_tool_use_id: get("parent_tool_use_id")?,
        source_tool_use_id: get("source_tool_use_id")?,
        source_tool_assistant_uuid: get("source_tool_assistant_uuid")?,
        source_path: get("source_path")?,
    })
}

fn build_query(
    fields: &IndexFields,
    options: &QueryOptions,
    index: &Index,
) -> Result<Box<dyn Query>> {
    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

    if options.query.trim().is_empty() {
        clauses.push((Occur::Must, Box::new(AllQuery)));
    } else {
        let parser = tantivy::query::QueryParser::for_index(index, vec![fields.text]);
        let text_query = parser.parse_query(&options.query)?;
        clauses.push((Occur::Must, text_query));
    }

    if let Some(project) = &options.project {
        let term = Term::from_field_text(fields.project, project);
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }

    if let Some(role) = &options.role {
        let term = Term::from_field_text(fields.role, role);
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }

    if let Some(tool) = &options.tool {
        let term = Term::from_field_text(fields.tool_name, tool);
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }

    if let Some(source) = options.source
        && let Some(field) = fields.source
    {
        let source_terms = source
            .storage_labels()
            .iter()
            .map(|label| {
                (
                    Occur::Should,
                    Box::new(TermQuery::new(
                        Term::from_field_text(field, label),
                        IndexRecordOption::Basic,
                    )) as Box<dyn Query>,
                )
            })
            .collect::<Vec<_>>();
        clauses.push((Occur::Must, Box::new(BooleanQuery::new(source_terms))));
    }

    if let Some(session_id) = &options.session_id {
        let term = Term::from_field_text(fields.session_id, session_id);
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }

    if let Some(scope) = &options.session_scope {
        if scope.is_empty() {
            clauses.push((Occur::Must, Box::new(EmptyQuery)));
        } else {
            let alternatives = scope
                .iter()
                .map(|key| {
                    let mut identity: Vec<(Occur, Box<dyn Query>)> = vec![
                        (
                            Occur::Must,
                            Box::new(TermQuery::new(
                                Term::from_field_text(fields.session_id, &key.session_id),
                                IndexRecordOption::Basic,
                            )),
                        ),
                        (
                            Occur::Must,
                            Box::new(TermQuery::new(
                                Term::from_field_text(fields.source_path, &key.source_path),
                                IndexRecordOption::Basic,
                            )),
                        ),
                    ];
                    if let Some(source_field) = fields.source {
                        identity.push((
                            Occur::Must,
                            Box::new(TermQuery::new(
                                Term::from_field_text(source_field, key.source.storage_label()),
                                IndexRecordOption::Basic,
                            )),
                        ));
                    }
                    (
                        Occur::Should,
                        Box::new(BooleanQuery::new(identity)) as Box<dyn Query>,
                    )
                })
                .collect();
            clauses.push((Occur::Must, Box::new(BooleanQuery::new(alternatives))));
        }
    }

    if options.since.is_some() || options.until.is_some() {
        let start = options.since.unwrap_or(0);
        let end = options.until.unwrap_or(u64::MAX);
        let range = RangeQuery::new_u64_bounds(
            "ts".to_string(),
            Bound::Included(start),
            Bound::Included(end),
        );
        clauses.push((Occur::Must, Box::new(range)));
    }

    Ok(Box::new(BooleanQuery::new(clauses)))
}

fn record_from_doc(fields: &IndexFields, doc: &TantivyDocument) -> Record {
    let get_str = |field: Field| -> Option<String> {
        doc.get_first(field)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    };

    let get_u64 =
        |field: Field| -> u64 { doc.get_first(field).and_then(|v| v.as_u64()).unwrap_or(0) };

    let source_path = get_str(fields.source_path).unwrap_or_default();
    let source = fields
        .source
        .and_then(&get_str)
        .and_then(|label| crate::types::SourceKind::from_label(&label))
        .unwrap_or_else(|| crate::types::SourceKind::from_path(&source_path));
    Record {
        source,
        doc_id: get_u64(fields.doc_id),
        ts: get_u64(fields.ts),
        project: get_str(fields.project).unwrap_or_default(),
        session_id: get_str(fields.session_id).unwrap_or_default(),
        turn_id: get_u64(fields.turn_id) as u32,
        role: get_str(fields.role).unwrap_or_default(),
        text: get_str(fields.text).unwrap_or_default(),
        tool_name: get_str(fields.tool_name),
        tool_input: get_str(fields.tool_input),
        tool_output: get_str(fields.tool_output),
        links: RecordLinks {
            event_id: get_str(fields.event_id),
            parent_event_id: get_str(fields.parent_event_id),
            logical_parent_event_id: get_str(fields.logical_parent_event_id),
            parent_session_id: get_str(fields.parent_session_id),
            thread_source: get_str(fields.thread_source),
            conversation_kind: get_str(fields.conversation_kind),
            parent_tool_use_id: get_str(fields.parent_tool_use_id),
            source_tool_use_id: get_str(fields.source_tool_use_id),
            source_tool_assistant_uuid: get_str(fields.source_tool_assistant_uuid),
            ..fields
                .reader_metadata
                .and_then(&get_str)
                .and_then(|json| serde_json::from_str(&json).ok())
                .unwrap_or_default()
        },
        source_path,
    }
}

fn add_optional_text(doc: &mut TantivyDocument, field: Field, value: &Option<String>) {
    if let Some(value) = value {
        doc.add_text(field, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_payload_fields_are_stored_without_indexing() {
        let schema = build_schema().unwrap();
        for name in ["tool_input", "tool_output"] {
            let field = schema.get_field(name).unwrap();
            let entry = schema.get_field_entry(field);
            assert!(entry.is_stored(), "{name} must remain retrievable");
            assert!(
                !entry.is_indexed(),
                "{name} must not duplicate the text vocabulary"
            );
        }
    }
    use tantivy::schema::TEXT;

    fn test_record(doc_id: u64, text: &str) -> Record {
        Record {
            source: crate::types::SourceKind::Codex,
            doc_id,
            ts: doc_id,
            project: "memex".to_string(),
            session_id: "session".to_string(),
            turn_id: doc_id as u32,
            role: "user".to_string(),
            text: text.to_string(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks::default(),
            source_path: "session.jsonl".to_string(),
        }
    }

    #[test]
    fn small_ingest_produces_one_segment() {
        let temp = tempfile::tempdir().unwrap();
        let index = SearchIndex::open_or_create_for_continuous_ingest(temp.path()).unwrap();
        let mut writer = index.writer_for_ingest(Some(1024)).unwrap();
        for id in 0..32 {
            index
                .add_record(&mut writer, &test_record(id, "small update"))
                .unwrap();
        }
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        assert_eq!(index.index.searchable_segment_metas().unwrap().len(), 1);
        assert_eq!(index.doc_count().unwrap(), 32);
    }

    #[test]
    fn search_refresh_never_merges_and_the_shared_reader_sees_each_commit() {
        let temp = tempfile::tempdir().unwrap();
        let index = SearchIndex::open_or_create_for_search_refresh(temp.path()).unwrap();
        let mut writer = index.writer_for_ingest(Some(1024)).unwrap();
        for id in 0..12 {
            index
                .add_record(&mut writer, &test_record(id, "deferred"))
                .unwrap();
            writer.commit().unwrap();
            assert_eq!(index.doc_count().unwrap(), id as usize + 1);
        }
        writer.wait_merging_threads().unwrap();
        assert_eq!(index.segment_count().unwrap(), 12);
        assert!(index.segment_count().unwrap() < SEARCH_REFRESH_COMPACTION_SEGMENTS);
    }

    #[test]
    fn failed_shared_file_publication_keeps_the_previous_current_generation() {
        let temp = tempfile::tempdir().unwrap();
        let first = SearchIndex::open_or_create_for_ingest(temp.path()).unwrap();
        let mut writer = first.writer().unwrap();
        first
            .add_record(&mut writer, &test_record(1, "original"))
            .unwrap();
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        first.publish_generation().unwrap();
        let current = fs::read(temp.path().join(CURRENT_FILE)).unwrap();
        let update = SearchIndex::open_or_create_for_ingest(temp.path()).unwrap();
        let mut writer = update.writer().unwrap();
        update
            .add_record(&mut writer, &test_record(2, "replacement"))
            .unwrap();
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        let staging = &update.pending_generation.as_ref().unwrap().staging_dir;
        let file = fs::read_dir(staging)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "store")
            })
            .unwrap();
        fs::remove_file(file).unwrap();
        assert!(update.publish_generation().is_err());
        assert_eq!(fs::read(temp.path().join(CURRENT_FILE)).unwrap(), current);
        assert_eq!(
            SearchIndex::open_or_create(temp.path())
                .unwrap()
                .doc_count()
                .unwrap(),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn index_reader_keeps_its_generation_lease_after_search_index_is_dropped() {
        let temp = tempfile::tempdir().unwrap();
        let first = SearchIndex::open_or_create_for_ingest(temp.path()).unwrap();
        let mut writer = first.writer().unwrap();
        first
            .add_record(&mut writer, &test_record(1, "old snapshot"))
            .unwrap();
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        first.publish_generation().unwrap();
        drop(first);
        let snapshot = SearchIndex::open_or_create(temp.path()).unwrap();
        let reader = snapshot.reader().unwrap();
        drop(snapshot);
        let update = SearchIndex::open_or_create_for_ingest(temp.path()).unwrap();
        let mut writer = update.writer().unwrap();
        update
            .add_record(&mut writer, &test_record(2, "new snapshot"))
            .unwrap();
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        update.publish_generation().unwrap();
        drop(update);
        reader.reload().unwrap();
        assert_eq!(reader.searcher().num_docs(), 1);
        assert!(SearchIndex::garbage_collect_generations_offline(temp.path(), false).is_err());
        drop(reader);
        SearchIndex::garbage_collect_generations_offline(temp.path(), false).unwrap();
        assert_eq!(
            SearchIndex::open_or_create(temp.path())
                .unwrap()
                .doc_count()
                .unwrap(),
            2
        );
    }

    fn create_stale_schema_index(dir: &Path) {
        let mut builder = SchemaBuilder::default();
        builder.add_u64_field("doc_id", INDEXED | STORED);
        builder.add_u64_field("ts", FAST | STORED | INDEXED);
        builder.add_text_field("project", STRING | STORED);
        builder.add_text_field("session_id", STRING | STORED);
        builder.add_u64_field("turn_id", FAST | STORED);
        builder.add_text_field("role", STRING | STORED);
        builder.add_text_field("source", STRING | STORED);
        builder.add_text_field("text", TEXT | STORED);
        builder.add_text_field("tool_name", STRING | STORED);
        builder.add_text_field("tool_input", TEXT | STORED);
        builder.add_text_field("tool_output", TEXT | STORED);
        builder.add_text_field("source_path", STRING | STORED);

        let index = Index::create_in_dir(dir, builder.build()).expect("create stale index");
        drop(index);
        std::fs::write(dir.join("sentinel"), "keep").expect("write sentinel");
    }

    #[test]
    fn read_only_open_preserves_stale_schema_index() {
        let tmp = tempfile::tempdir().expect("tempdir");
        create_stale_schema_index(tmp.path());

        let err = match SearchIndex::open_or_create(tmp.path()) {
            Ok(_) => panic!("stale index unexpectedly opened"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("index schema"));
        assert!(tmp.path().join("meta.json").exists());
        assert!(tmp.path().join("sentinel").exists());
    }

    #[test]
    fn reader_metadata_round_trips_without_changing_record_sequence() {
        let tmp = tempfile::tempdir().unwrap();
        let index = SearchIndex::open_or_create(tmp.path()).unwrap();
        let mut writer = index.writer().unwrap();
        let mut record = test_record(1, "Turn completed");
        record.links.source_turn_id = Some("provider-turn".to_string());
        record.links.legacy_turn_id = Some(0);
        record.links.source_record_offset = Some(123);
        record.links.tool_result_is_error = Some(true);
        record.links.assistant_phase = Some("final_answer".to_string());
        record.links.lifecycle_event = Some("task_complete".to_string());
        record.links.source_record_type = Some("event_msg/task_complete".to_string());
        record.links.source_content =
            Some(r#"[{"type":"input_image","image_url":"/tmp/photo.png"}]"#.to_string());
        index.add_record(&mut writer, &record).unwrap();
        writer.commit().unwrap();
        let rows = index
            .records_by_context_scope(Some("session"), Some(crate::types::SourceKind::Codex))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].turn_id, record.turn_id);
        assert_eq!(
            serde_json::to_value(&rows[0].links).unwrap(),
            serde_json::to_value(&record.links).unwrap()
        );
    }

    #[test]
    fn reader_metadata_upgrade_keeps_old_generation_readable_until_publish() {
        let tmp = tempfile::tempdir().unwrap();
        let mut schema =
            serde_json::to_value(build_schema_with_canonical_record_id(true).unwrap()).unwrap();
        schema
            .as_array_mut()
            .unwrap()
            .retain(|field| field["name"] != "reader_metadata");
        let schema: Schema = serde_json::from_value(schema).unwrap();
        drop(Index::create_in_dir(tmp.path(), schema).unwrap());
        let legacy = SearchIndex::open_or_create(tmp.path()).unwrap();
        assert!(legacy.fields.reader_metadata.is_none());
        let mut writer = legacy.writer().unwrap();
        legacy
            .add_record(&mut writer, &test_record(1, "legacy record"))
            .unwrap();
        writer.commit().unwrap();
        drop(writer);
        let fresh = SearchIndex::open_or_create_for_ingest(tmp.path()).unwrap();
        assert!(fresh.fields.reader_metadata.is_some());
        assert_eq!(fresh.doc_count().unwrap(), 0);
        // Ingest detects the empty private generation and reparses all source files;
        // the published generation remains intact throughout that rebuild.
        assert_eq!(
            SearchIndex::open_or_create(tmp.path())
                .unwrap()
                .doc_count()
                .unwrap(),
            1
        );
        let mut reparsed = test_record(1, "legacy record");
        reparsed.links.source_turn_id = Some("recovered-turn".to_string());
        let mut writer = fresh.writer().unwrap();
        fresh.add_record(&mut writer, &reparsed).unwrap();
        writer.commit().unwrap();
        drop(writer);
        fresh.publish_generation().unwrap();
        let reopened = SearchIndex::open_or_create(tmp.path()).unwrap();
        let rows = reopened
            .records_by_context_scope(Some("session"), Some(crate::types::SourceKind::Codex))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].links.source_turn_id.as_deref(),
            Some("recovered-turn")
        );
    }

    #[test]
    fn legacy_current_schema_remains_readable_without_canonical_record_id() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let schema = build_schema_with_canonical_record_id(false).expect("legacy schema");
        let raw = Index::create_in_dir(tmp.path(), schema).expect("legacy index");
        drop(raw);

        let index = SearchIndex::open_or_create(tmp.path()).expect("open legacy index");
        assert!(index.fields.canonical_record_id.is_none());
        let mut writer = index.writer().expect("legacy writer");
        let record = test_record(1, "legacy");
        index
            .add_record(&mut writer, &record)
            .expect("add legacy record");
        writer.commit().expect("commit legacy record");
        assert_eq!(
            index
                .records_by_context_scope(Some("session"), Some(crate::types::SourceKind::Codex))
                .expect("scoped legacy fallback")
                .len(),
            1
        );
        assert!(
            index
                .records_by_canonical_id("rid1_missing")
                .expect("legacy canonical lookup")
                .is_none()
        );
        let selector = crate::retrieval::ContextSelector::record_id(
            crate::retrieval::canonical_record_id(&record),
        )
        .with_scope(Some(record.session_id.clone()), Some(record.source));
        assert_eq!(
            crate::retrieval::resolve_record(&index, &selector)
                .expect("legacy scoped record-ID fallback")
                .doc_id,
            record.doc_id
        );
    }

    #[test]
    fn legacy_session_around_resolves_scoped_stored_record_ids() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let schema = build_schema_with_canonical_record_id(false).expect("legacy schema");
        let raw = Index::create_in_dir(tmp.path(), schema).expect("legacy index");
        drop(raw);

        let index = SearchIndex::open_or_create(tmp.path()).expect("open legacy index");
        let mut writer = index.writer().expect("legacy writer");
        let before = test_record(1, "before");
        let mut anchor = test_record(2, "anchor");
        anchor.links.event_id = Some("shared-native-event".to_string());
        let after = test_record(3, "after");
        let mut other_path = anchor.clone();
        other_path.doc_id = 4;
        other_path.source_path = "other.jsonl".to_string();
        for record in [&before, &anchor, &after, &other_path] {
            index
                .add_record(&mut writer, record)
                .expect("add legacy record");
        }
        writer.commit().expect("commit legacy records");
        let anchor_id = crate::retrieval::canonical_record_id(&anchor);

        let resolved = index
            .records_by_canonical_id_in_session_scope(
                &anchor_id,
                "session",
                Some("session.jsonl"),
                Some(crate::types::SourceKind::Codex),
            )
            .expect("resolve legacy anchor");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].doc_id, anchor.doc_id);

        let (records, total, offset) = index
            .records_by_session_scope_around(
                "session",
                Some("session.jsonl"),
                Some(crate::types::SourceKind::Codex),
                &anchor_id,
                3,
            )
            .expect("legacy around read")
            .expect("legacy anchor");
        assert_eq!(total, 3);
        assert_eq!(offset, 0);
        assert_eq!(
            records
                .into_iter()
                .map(|record| record.doc_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        for (record_id, path, source) in [
            (
                "rid1_missing",
                "session.jsonl",
                crate::types::SourceKind::Codex,
            ),
            (
                anchor_id.as_str(),
                "missing.jsonl",
                crate::types::SourceKind::Codex,
            ),
            (
                anchor_id.as_str(),
                "session.jsonl",
                crate::types::SourceKind::Claude,
            ),
        ] {
            assert!(
                index
                    .records_by_session_scope_around(
                        "session",
                        Some(path),
                        Some(source),
                        record_id,
                        3,
                    )
                    .expect("scoped legacy miss")
                    .is_none()
            );
        }
    }

    #[test]
    fn canonical_and_session_path_lookups_use_exact_index_fields() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index = SearchIndex::open_or_create(tmp.path()).expect("index");
        let first = test_record(1, "first");
        let mut other_path = test_record(2, "other path");
        other_path.source_path = "other.jsonl".to_string();
        let mut writer = index.writer().expect("writer");
        index.add_record(&mut writer, &first).expect("add first");
        index
            .add_record(&mut writer, &other_path)
            .expect("add other path");
        writer.commit().expect("commit");

        let record_id = crate::retrieval::canonical_record_id(&first);
        let exact = index
            .records_by_canonical_id(&record_id)
            .expect("canonical lookup")
            .expect("canonical field");
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0].doc_id, first.doc_id);

        let scoped = index
            .records_by_session_path(first.source, &first.session_id, &first.source_path)
            .expect("session path lookup");
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].doc_id, first.doc_id);
    }

    #[test]
    fn recent_records_since_uses_inclusive_indexed_timestamp_filter() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index = SearchIndex::open_or_create(tmp.path()).expect("index");
        let mut writer = index.writer().expect("writer");
        let mut records = [
            test_record(1, "before"),
            test_record(2, "boundary"),
            test_record(3, "after"),
            test_record(4, "other project"),
            test_record(5, "other source"),
        ];
        for (record, ts) in records.iter_mut().zip([99, 100, 101, 102, 103]) {
            record.ts = ts;
        }
        records[3].project = "other".to_string();
        records[4].source = crate::types::SourceKind::Claude;
        for record in &records {
            index.add_record(&mut writer, record).expect("add record");
        }
        writer.commit().expect("commit records");

        let recent = index
            .recent_records_filtered_since(10, Some(SourceFilter::Codex), Some("memex"), Some(100))
            .expect("recent range");
        assert_eq!(
            recent
                .into_iter()
                .map(|record| (record.ts, record.text))
                .collect::<Vec<_>>(),
            vec![(101, "after".to_string()), (100, "boundary".to_string())]
        );
    }

    #[test]
    fn timestamp_search_orders_ties_by_doc_id_across_segments() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index = SearchIndex::open_or_create(tmp.path()).expect("index");
        let mut writer = index.writer().expect("writer");
        for doc_id in [1, 3, 2] {
            let mut record = test_record(doc_id, "shared needle");
            record.session_id = format!("session-{doc_id}");
            record.source_path = format!("session-{doc_id}.jsonl");
            record.ts = 42;
            index.add_record(&mut writer, &record).expect("add record");
            writer.commit().expect("commit segment");
        }

        let options = QueryOptions {
            query: "needle".to_string(),
            project: Some("memex".to_string()),
            role: None,
            tool: None,
            session_id: None,
            session_scope: None,
            source: None,
            since: None,
            until: None,
            limit: 3,
        };
        let doc_ids = |order| {
            index
                .search_by_timestamp(&options, order)
                .expect("timestamp search")
                .into_iter()
                .map(|record| record.doc_id)
                .collect::<Vec<_>>()
        };

        assert_eq!(doc_ids(TimestampOrder::Newest), vec![3, 2, 1]);
        assert_eq!(doc_ids(TimestampOrder::Oldest), vec![1, 2, 3]);
    }

    #[test]
    fn session_path_pages_preserve_global_tie_order_across_boundaries() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index = SearchIndex::open_or_create(tmp.path()).expect("index");
        let mut writer = index.writer().expect("writer");
        for doc_id in (1..=1_200).rev() {
            let mut record = test_record(doc_id, "same turn");
            record.turn_id = 7;
            record.source_path = if doc_id % 2 == 0 {
                "other.jsonl".to_string()
            } else {
                "selected.jsonl".to_string()
            };
            index
                .add_record(&mut writer, &record)
                .expect("add tied record");
        }
        writer.commit().expect("commit tied records");

        let (first, total) = index
            .records_by_session_path_page("session", Some("selected.jsonl"), 0, 500)
            .expect("first page");
        let (second, second_total) = index
            .records_by_session_path_page("session", Some("selected.jsonl"), 500, 500)
            .expect("second page");
        let actual = first
            .into_iter()
            .chain(second)
            .map(|record| record.doc_id)
            .collect::<Vec<_>>();
        let expected = (1..=1_200)
            .filter(|doc_id| doc_id % 2 == 1)
            .collect::<Vec<_>>();

        assert_eq!(total, 600);
        assert_eq!(second_total, total);
        assert_eq!(actual, expected);
    }

    #[test]
    fn session_around_uses_doc_id_to_rank_tied_records() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index = SearchIndex::open_or_create(tmp.path()).expect("index");
        let mut writer = index.writer().expect("writer");
        let mut anchor_id = String::new();
        for doc_id in (1..=1_200).rev() {
            let mut record = test_record(doc_id, "same turn and timestamp");
            record.turn_id = 7;
            record.ts = 42;
            record.links.event_id = Some(format!("event-{doc_id}"));
            if doc_id == 600 {
                anchor_id = crate::retrieval::canonical_record_id(&record);
            }
            index.add_record(&mut writer, &record).expect("add record");
        }
        writer.commit().expect("commit tied records");

        let (records, total, offset) = index
            .records_by_session_path_around("session", Some("session.jsonl"), &anchor_id, 5)
            .expect("around page")
            .expect("anchor");

        assert_eq!(total, 1_200);
        assert_eq!(offset, 597);
        assert_eq!(
            records
                .into_iter()
                .map(|record| record.doc_id)
                .collect::<Vec<_>>(),
            vec![598, 599, 600, 601, 602]
        );
    }

    #[test]
    fn legacy_snapshot_version_changes_after_commit() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index = SearchIndex::open_or_create(tmp.path()).expect("index");
        let mut writer = index.writer().expect("writer");
        index
            .add_record(&mut writer, &test_record(1, "first"))
            .expect("add first");
        writer.commit().expect("commit first");
        drop(writer);
        drop(index);
        let first = SearchIndex::open_or_create(tmp.path())
            .expect("first snapshot")
            .snapshot_version()
            .to_string();

        let index = SearchIndex::open_or_create(tmp.path()).expect("index");
        let mut writer = index.writer().expect("writer");
        index
            .add_record(&mut writer, &test_record(2, "second"))
            .expect("add second");
        writer.commit().expect("commit second");
        drop(writer);
        drop(index);
        let second = SearchIndex::open_or_create(tmp.path())
            .expect("second snapshot")
            .snapshot_version()
            .to_string();

        assert_ne!(first, second);
    }

    #[test]
    fn session_scope_filters_exact_source_session_and_path() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index = SearchIndex::open_or_create(tmp.path()).expect("index");
        let mut first = test_record(1, "shared needle");
        first.session_id = "first".to_string();
        first.source_path = "first.jsonl".to_string();
        let mut second = test_record(2, "shared needle");
        second.session_id = "second".to_string();
        second.source_path = "second.jsonl".to_string();
        let mut writer = index.writer().expect("writer");
        index.add_record(&mut writer, &first).expect("first");
        index.add_record(&mut writer, &second).expect("second");
        writer.commit().expect("commit");

        let options = QueryOptions {
            query: "shared needle".to_string(),
            project: None,
            role: None,
            tool: None,
            session_id: None,
            session_scope: Some(vec![SessionScopeKey {
                source: first.source,
                session_id: first.session_id.clone(),
                source_path: first.source_path.clone(),
            }]),
            source: None,
            since: None,
            until: None,
            limit: 10,
        };
        let scoped = index.search(&options).expect("scoped search");
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].1.doc_id, first.doc_id);

        let empty = index
            .search(&QueryOptions {
                session_scope: Some(Vec::new()),
                ..options
            })
            .expect("empty scope");
        assert!(empty.is_empty());
    }

    #[test]
    fn session_scope_collector_deduplicates_all_matches_with_fast_and_legacy_fields() {
        let tmp = tempfile::tempdir().expect("tempdir");
        for fast_session_identity in [true, false] {
            let dir = tmp.path().join(if fast_session_identity {
                "fast"
            } else {
                "legacy"
            });
            std::fs::create_dir(&dir).expect("create index dir");
            let schema = build_schema_with_options(true, fast_session_identity).expect("schema");
            drop(Index::create_in_dir(&dir, schema).expect("create index"));
            let index = SearchIndex::open_or_create(&dir).expect("open index");
            let mut writer = index.writer().expect("writer");
            for doc_id in 1..=257 {
                index
                    .add_record(&mut writer, &test_record(doc_id, "shared needle"))
                    .expect("add repeated session record");
            }
            let mut other = test_record(258, "shared needle");
            other.source = crate::types::SourceKind::Claude;
            other.session_id = "other-session".to_string();
            other.source_path = "other.jsonl".to_string();
            index
                .add_record(&mut writer, &other)
                .expect("add other session record");
            let mut deleted = test_record(259, "shared needle");
            deleted.session_id = "deleted-session".to_string();
            deleted.source_path = "deleted.jsonl".to_string();
            index
                .add_record(&mut writer, &deleted)
                .expect("add deleted session record");
            if !fast_session_identity {
                let mut inferred = TantivyDocument::default();
                inferred.add_text(index.fields.text, "shared needle");
                inferred.add_text(index.fields.session_id, "inferred-session");
                inferred.add_text(
                    index.fields.source_path,
                    "/tmp/.claude/projects/inferred.jsonl",
                );
                writer
                    .add_document(inferred)
                    .expect("add legacy record without source field");
            }
            writer.commit().expect("commit");
            writer.delete_term(Term::from_field_u64(index.fields.doc_id, deleted.doc_id));
            writer.commit().expect("commit deletion");

            let options = QueryOptions {
                query: "shared needle".to_string(),
                project: None,
                role: None,
                tool: None,
                session_id: None,
                session_scope: None,
                source: None,
                since: None,
                until: None,
                limit: 1,
            };
            let scopes = index
                .session_scopes_matching_query(&options)
                .expect("collect session scopes");

            let fast_scopes = index.fast_session_scopes_matching_query(&options).unwrap();
            if fast_session_identity {
                assert_eq!(fast_scopes.as_ref(), Some(&scopes));
            } else {
                assert!(fast_scopes.is_none());
            }
            assert_eq!(scopes.len(), if fast_session_identity { 2 } else { 3 });
            assert!(scopes.contains(&(
                crate::types::SourceKind::Codex,
                "session".to_string(),
                "session.jsonl".to_string(),
            )));
            assert!(scopes.contains(&(
                crate::types::SourceKind::Claude,
                "other-session".to_string(),
                "other.jsonl".to_string(),
            )));
            assert!(!scopes.iter().any(|scope| scope.1 == "deleted-session"));
            if !fast_session_identity {
                assert!(scopes.contains(&(
                    crate::types::SourceKind::Claude,
                    "inferred-session".to_string(),
                    "/tmp/.claude/projects/inferred.jsonl".to_string(),
                )));
            }
            assert!(
                index
                    .session_scopes_matching_query(&QueryOptions {
                        query: "missing phrase".to_string(),
                        ..options
                    })
                    .expect("collect empty result")
                    .is_empty()
            );
        }
    }

    #[test]
    fn ingest_open_recreates_stale_schema_index() {
        let tmp = tempfile::tempdir().expect("tempdir");
        create_stale_schema_index(tmp.path());

        let index =
            SearchIndex::open_or_create_for_ingest(tmp.path()).expect("recreate stale index");

        assert_eq!(index.doc_count().expect("doc count"), 0);
        index.publish_generation().expect("publish generation");
        assert!(SearchIndex::exists(tmp.path()));
        assert_eq!(
            SearchIndex::open_or_create(tmp.path())
                .expect("open published generation")
                .doc_count()
                .expect("published doc count"),
            0
        );
    }

    #[test]
    fn publishing_generation_atomically_advances_new_readers() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let first = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("first generation");
        let mut writer = first.writer().expect("first writer");
        first
            .add_record(&mut writer, &test_record(1, "first"))
            .expect("add first");
        writer.commit().expect("commit first");
        writer.wait_merging_threads().expect("finish first writer");
        first.publish_generation().expect("publish first");

        let old_reader = SearchIndex::open_or_create(tmp.path()).expect("old reader");
        assert_eq!(old_reader.doc_count().expect("old count"), 1);

        let second = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("second generation");
        let mut writer = second.writer().expect("second writer");
        second
            .add_record(&mut writer, &test_record(2, "second"))
            .expect("add second");
        writer.commit().expect("commit second");
        writer.wait_merging_threads().expect("finish second writer");

        assert_eq!(
            SearchIndex::open_or_create(tmp.path())
                .expect("reader before publish")
                .doc_count()
                .expect("count before publish"),
            1
        );
        second.publish_generation().expect("publish second");
        assert_eq!(
            SearchIndex::open_or_create(tmp.path())
                .expect("reader after publish")
                .doc_count()
                .expect("count after publish"),
            2
        );
        assert_eq!(old_reader.doc_count().expect("old reader remains valid"), 1);
    }

    #[test]
    fn generation_clone_excludes_uncommitted_files() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let first = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("first generation");
        let mut writer = first.writer().expect("first writer");
        first
            .add_record(&mut writer, &test_record(1, "committed"))
            .expect("add record");
        writer.commit().expect("commit first");
        writer.wait_merging_threads().expect("finish first writer");
        first.publish_generation().expect("publish first");

        let first_dir = resolve_current_generation(tmp.path()).expect("first current generation");
        fs::write(first_dir.join("orphan.store"), b"stale").expect("write stale file");

        let second = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("second generation");
        let staging = &second
            .pending_generation
            .as_ref()
            .expect("pending generation")
            .staging_dir;
        assert!(!staging.join("orphan.store").exists());
        assert_eq!(second.doc_count().expect("cloned doc count"), 1);
    }

    #[test]
    fn offline_generation_gc_reuses_live_segments_without_reindexing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let first = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("first generation");
        let mut writer = first.writer().expect("first writer");
        first
            .add_record(&mut writer, &test_record(1, "preserved"))
            .expect("add record");
        writer.commit().expect("commit first");
        writer.wait_merging_threads().expect("finish first writer");
        first.publish_generation().expect("publish first");
        let original = resolve_current_generation(tmp.path()).expect("current generation");
        let original_segment = SearchIndex::open_or_create(tmp.path())
            .expect("published index")
            .index
            .searchable_segment_ids()
            .expect("segment ids")[0];
        drop(first);

        let stale_generation = tmp.path().join(GENERATIONS_DIR).join("stale-generation");
        clone_generation(&original, &stale_generation).expect("clone stale generation");
        let abandoned_staging = tmp
            .path()
            .join(GENERATIONS_DIR)
            .join(".00000000000000000000000000000001-00000002.tmp");
        clone_generation(&original, &abandoned_staging)
            .expect("clone abandoned staging generation");
        let abandoned_gc = tmp.path().join(GENERATIONS_DIR).join(".gc-abandoned");
        fs::create_dir(&abandoned_gc).expect("create abandoned GC work directory");
        fs::write(original.join("orphan.store"), b"unreachable").expect("write orphan");
        fs::write(tmp.path().join("legacy.store"), b"legacy").expect("write legacy file");

        let dry_run =
            SearchIndex::garbage_collect_generations_offline(tmp.path(), true).expect("dry-run gc");
        assert!(dry_run.dry_run);
        assert_eq!(dry_run.generations_removed, 2);
        assert_eq!(dry_run.abandoned_workdirs_removed, 2);
        assert_eq!(dry_run.legacy_files_removed, 1);
        assert!(original.exists());
        assert!(abandoned_staging.exists());
        assert!(abandoned_gc.exists());

        let active_reader = SearchIndex::open_or_create(tmp.path()).expect("active reader");
        let error = SearchIndex::garbage_collect_generations_offline(tmp.path(), false)
            .expect_err("active reader must block offline gc");
        assert!(error.to_string().contains("still in use"));
        drop(active_reader);

        let report = SearchIndex::garbage_collect_generations_offline(tmp.path(), false)
            .expect("offline gc");
        assert!(!report.dry_run);
        assert!(!tmp.path().join("legacy.store").exists());
        assert!(!original.exists());
        assert!(!stale_generation.exists());
        assert!(!abandoned_staging.exists());
        assert!(!abandoned_gc.exists());

        let cleaned = SearchIndex::open_or_create(tmp.path()).expect("cleaned index");
        assert_eq!(cleaned.doc_count().expect("document count"), 1);
        assert_eq!(search_text_count(&cleaned, "preserved"), 1);
        assert_eq!(
            cleaned
                .index
                .searchable_segment_ids()
                .expect("cleaned segment ids")[0],
            original_segment,
            "GC must retain the existing Tantivy segment instead of rebuilding it"
        );
        let generation_count = fs::read_dir(tmp.path().join(GENERATIONS_DIR))
            .expect("generation directory")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .count();
        assert_eq!(generation_count, 1);
    }

    #[test]
    fn legacy_gc_dry_run_does_not_create_storage() {
        let temp = tempfile::tempdir().unwrap();
        let index = Index::create_in_dir(temp.path(), build_schema().unwrap()).unwrap();
        drop(index);
        let before = fs::read_dir(temp.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<HashSet<_>>();
        let report = SearchIndex::garbage_collect_generations_offline(temp.path(), true).unwrap();
        assert!(report.dry_run);
        let after = fs::read_dir(temp.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<HashSet<_>>();
        assert_eq!(before, after);
    }

    #[test]
    fn normal_indexing_reclaims_pre_lease_generations_automatically() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let first = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("first generation");
        let mut writer = first.writer().expect("first writer");
        first
            .add_record(&mut writer, &test_record(1, "preserved"))
            .expect("add record");
        writer.commit().expect("commit first");
        writer.wait_merging_threads().expect("finish first writer");
        first.publish_generation().expect("publish first");
        let current = resolve_current_generation(tmp.path()).expect("current generation");
        let abandoned_staging = tmp
            .path()
            .join(GENERATIONS_DIR)
            .join(".00000000000000000000000000000001-00000002.tmp");
        clone_generation(&current, &abandoned_staging).expect("clone abandoned staging generation");
        create_generation_lease_file(&abandoned_staging)
            .expect("create abandoned staging generation lease");

        for generation in 0..300 {
            let stale = tmp
                .path()
                .join(GENERATIONS_DIR)
                .join(format!("pre-lease-{generation:03}"));
            clone_generation(&current, &stale).expect("clone pre-lease generation");
            let lease = stale.join(GENERATION_LEASE_FILE);
            if lease.exists() {
                fs::remove_file(lease).expect("remove generation lease");
            }
        }
        drop(first);

        let refresh = SearchIndex::open_or_create_for_continuous_ingest(tmp.path())
            .expect("continuous indexing must not require manual GC");
        refresh
            .publish_generation()
            .expect("normal publication must reclaim pre-lease generations");

        let published = SearchIndex::open_or_create(tmp.path()).expect("published index");
        assert_eq!(published.doc_count().expect("document count"), 1);
        assert_eq!(search_text_count(&published, "preserved"), 1);
        assert!(!abandoned_staging.exists());
        let generations = fs::read_dir(tmp.path().join(GENERATIONS_DIR))
            .expect("generation directory")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .count();
        assert_eq!(generations, 1);
    }

    #[test]
    fn normal_indexing_preserves_staging_generation_with_live_owner() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let live = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("live staging index");
        let live_staging = live
            .pending_generation
            .as_ref()
            .expect("pending generation")
            .staging_dir
            .clone();
        let mut writer = live.writer().expect("live writer");
        live.add_record(&mut writer, &test_record(1, "still searchable"))
            .expect("add live record");
        writer.commit().expect("commit live staging index");

        let refresh =
            SearchIndex::open_or_create_for_ingest(tmp.path()).expect("background refresh");
        refresh
            .publish_generation()
            .expect("publish background refresh");

        assert!(live_staging.exists(), "live staging generation was pruned");
        assert_eq!(live.doc_count().expect("live staging document count"), 1);
        assert_eq!(search_text_count(&live, "still searchable"), 1);

        drop(refresh);
        let error = SearchIndex::garbage_collect_generations_offline(tmp.path(), false)
            .expect_err("offline GC must preserve a staging generation with a live owner");
        assert!(error.to_string().contains("work directory"));
        assert!(live_staging.exists(), "offline GC pruned live staging");

        drop(writer);
        drop(live);
        assert!(
            !live_staging.exists(),
            "released staging generation was retained"
        );
    }

    #[test]
    fn normal_indexing_migrates_flat_legacy_index_automatically() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let legacy = SearchIndex::open_or_create_legacy(tmp.path()).expect("legacy flat index");
        let mut writer = legacy.writer().expect("legacy writer");
        legacy
            .add_record(&mut writer, &test_record(1, "preserved"))
            .expect("add record");
        writer.commit().expect("commit legacy index");
        writer.wait_merging_threads().expect("finish legacy writer");
        drop(legacy);

        let refresh = SearchIndex::open_or_create_for_continuous_ingest(tmp.path())
            .expect("continuous indexing must migrate a flat index");
        refresh
            .publish_generation()
            .expect("publish migrated generation");

        assert!(!tmp.path().join("meta.json").exists());
        let published = SearchIndex::open_or_create(tmp.path()).expect("published index");
        assert_eq!(published.doc_count().expect("document count"), 1);
        assert_eq!(search_text_count(&published, "preserved"), 1);
    }

    #[test]
    fn continuous_refreshes_compact_small_peers_and_retain_large_inherited_segment() {
        const SEED_RECORDS: u64 = 1024;
        const REFRESHES: u64 = 129;

        let tmp = tempfile::tempdir().expect("tempdir");
        let first = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("first generation");
        let mut writer = first.writer_for_ingest(Some(1024)).expect("first writer");
        for doc_id in 1..=SEED_RECORDS {
            first
                .add_record(
                    &mut writer,
                    &test_record(doc_id, &format!("record{doc_id}")),
                )
                .expect("add baseline");
        }
        writer.commit().expect("commit baseline");
        writer.wait_merging_threads().expect("finish baseline");
        first.publish_generation().expect("publish baseline");
        let seed_segments = first.index.searchable_segment_ids().expect("seed segments");
        assert_eq!(seed_segments.len(), 1);
        let seed_segment = seed_segments[0];
        drop(first);

        for refresh_number in 1..=REFRESHES {
            let refresh = SearchIndex::open_or_create_for_continuous_ingest(tmp.path())
                .expect("continuous refresh");
            let mut writer = refresh
                .writer_for_ingest(Some(1024))
                .expect("continuous writer");
            let doc_id = SEED_RECORDS + refresh_number;
            refresh
                .add_record(
                    &mut writer,
                    &test_record(doc_id, &format!("record{doc_id}")),
                )
                .expect("add refresh record");
            writer.commit().expect("commit refresh");
            writer.wait_merging_threads().expect("finish refresh");
            refresh
                .check_continuous_segment_limit()
                .expect("segment safety limit");
            refresh.publish_generation().expect("publish refresh");

            let segments = refresh
                .index
                .searchable_segment_metas()
                .expect("searchable segments");
            assert!(
                segments.iter().any(|segment| segment.id() == seed_segment),
                "small peer merges must retain the large inherited segment"
            );
            if refresh_number >= 8 {
                assert!(
                    segments.len() < refresh_number as usize + 1,
                    "small peers must compact once eight peers accumulate"
                );
                assert!(
                    segments
                        .iter()
                        .any(|segment| { segment.id() != seed_segment && segment.num_docs() > 1 })
                );
            }
        }

        let published = SearchIndex::open_or_create(tmp.path()).expect("published index");
        assert_eq!(
            published.doc_count().expect("document count"),
            (SEED_RECORDS + REFRESHES) as usize
        );
        let mut records = Vec::new();
        published
            .for_each_record(|record| {
                records.push((record.doc_id, record.text));
                Ok(())
            })
            .expect("read every surviving record");
        records.sort_unstable();
        assert_eq!(
            records,
            (1..=SEED_RECORDS + REFRESHES)
                .map(|doc_id| (doc_id, format!("record{doc_id}")))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn deleting_in_new_generation_does_not_mutate_old_reader_snapshot() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let first = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("first generation");
        let mut writer = first.writer().expect("first writer");
        first
            .add_record(&mut writer, &test_record(1, "beforeupdate"))
            .expect("add old record");
        writer.commit().expect("commit old record");
        writer.wait_merging_threads().expect("finish first writer");
        first.publish_generation().expect("publish first");
        let old_reader = SearchIndex::open_or_create(tmp.path()).expect("old reader");

        let second = SearchIndex::open_or_create_for_continuous_ingest(tmp.path())
            .expect("second generation");
        let mut writer = second.writer().expect("second writer");
        second.delete_by_source_path(&mut writer, "session.jsonl");
        second
            .add_record(&mut writer, &test_record(2, "afterupdate"))
            .expect("add replacement record");
        writer.commit().expect("commit replacement");
        writer.wait_merging_threads().expect("finish second writer");
        second.publish_generation().expect("publish second");

        assert_eq!(search_text_count(&old_reader, "beforeupdate"), 1);
        let new_reader = SearchIndex::open_or_create(tmp.path()).expect("new reader");
        assert_eq!(search_text_count(&new_reader, "beforeupdate"), 0);
        assert_eq!(search_text_count(&new_reader, "afterupdate"), 1);
    }

    #[test]
    fn text_search_matches_inflected_forms_through_stemming() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index = SearchIndex::open_or_create(tmp.path()).expect("index");
        let mut writer = index.writer().expect("writer");
        index
            .add_record(&mut writer, &test_record(1, "ran the database migrations"))
            .expect("add record");
        writer.commit().expect("commit");

        assert_eq!(search_text_count(&index, "migration"), 1);
        assert_eq!(search_text_count(&index, "migrated"), 1);
        assert_eq!(search_text_count(&index, "Databases"), 1);
        assert_eq!(search_text_count(&index, "rollback"), 0);
    }

    fn search_text_count(index: &SearchIndex, query: &str) -> usize {
        index
            .search(&QueryOptions {
                query: query.to_string(),
                project: None,
                role: None,
                tool: None,
                session_id: None,
                session_scope: None,
                source: None,
                since: None,
                until: None,
                limit: 10,
            })
            .expect("search")
            .len()
    }

    #[cfg(unix)]
    #[test]
    fn superseded_generations_are_pruned_after_readers_release_their_leases() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let first = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("first generation");
        let mut writer = first.writer().expect("first writer");
        first
            .add_record(&mut writer, &test_record(1, "first"))
            .expect("add first");
        writer.commit().expect("commit first");
        writer.wait_merging_threads().expect("finish first writer");
        first.publish_generation().expect("publish first");
        let first_dir = resolve_current_generation(tmp.path()).expect("first current generation");
        let old_reader = SearchIndex::open_or_create(tmp.path()).expect("lease first generation");

        let second = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("second generation");
        second.publish_generation().expect("publish second");
        assert!(
            first_dir.exists(),
            "leased generation must remain available"
        );
        assert_eq!(old_reader.doc_count().expect("old reader count"), 1);

        drop(old_reader);
        drop(first);
        drop(second);
        let third = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("third generation");
        third.publish_generation().expect("publish third");
        assert!(
            !first_dir.exists(),
            "released superseded generation must be reclaimed"
        );
        let generation_count = fs::read_dir(tmp.path().join(GENERATIONS_DIR))
            .expect("generation directory")
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .count();
        assert_eq!(generation_count, 1);
    }

    #[test]
    fn publishing_waits_for_merges_without_losing_segments() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("generation");
        let mut writer = index.writer().expect("writer");

        for doc_id in 1..=4 {
            index
                .add_record(
                    &mut writer,
                    &test_record(doc_id, &format!("unique{doc_id}")),
                )
                .expect("add record");
            writer.commit().expect("commit segment");
        }

        let segment_ids = index
            .index
            .searchable_segment_ids()
            .expect("segments before merge");
        assert!(segment_ids.len() > 1);
        writer.merge(&segment_ids).wait().expect("merge segments");
        writer.wait_merging_threads().expect("finish writer");
        index.publish_generation().expect("publish generation");

        let published = SearchIndex::open_or_create(tmp.path()).expect("published generation");
        assert_eq!(published.doc_count().expect("published count"), 4);
        assert_eq!(
            published
                .index
                .searchable_segment_ids()
                .expect("published segments")
                .len(),
            1
        );
        for doc_id in 1..=4 {
            assert_eq!(
                published
                    .search(&QueryOptions {
                        query: format!("unique{doc_id}"),
                        project: None,
                        role: None,
                        tool: None,
                        session_id: None,
                        session_scope: None,
                        source: None,
                        since: None,
                        until: None,
                        limit: 10,
                    })
                    .expect("search merged segment")
                    .len(),
                1
            );
        }
    }

    #[test]
    fn legacy_index_is_adopted_without_rebuilding() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let legacy = SearchIndex::open_or_create(tmp.path()).expect("legacy index");
        let mut writer = legacy.writer().expect("legacy writer");
        legacy
            .add_record(&mut writer, &test_record(1, "preserved"))
            .expect("add legacy record");
        writer.commit().expect("commit legacy index");
        writer.wait_merging_threads().expect("finish legacy writer");

        let adopted =
            SearchIndex::open_or_create_for_ingest(tmp.path()).expect("adopt legacy index");
        assert_eq!(adopted.doc_count().expect("adopted count"), 1);
        adopted.publish_generation().expect("publish adoption");

        let published = SearchIndex::open_or_create(tmp.path()).expect("published generation");
        assert_eq!(published.doc_count().expect("published count"), 1);
        assert_eq!(
            published
                .search(&QueryOptions {
                    query: "preserved".to_string(),
                    project: None,
                    role: None,
                    tool: None,
                    session_id: None,
                    session_scope: None,
                    source: None,
                    since: None,
                    until: None,
                    limit: 10,
                })
                .expect("search adopted generation")
                .len(),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn sealed_generation_can_be_searched_without_directory_write_access() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().expect("tempdir");
        let writable = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("generation");
        let mut writer = writable.writer().expect("writer");
        writable
            .add_record(&mut writer, &test_record(1, "needle"))
            .expect("add record");
        writer.commit().expect("commit");
        writer.wait_merging_threads().expect("finish writer");
        writable.publish_generation().expect("publish");

        let generation = resolve_current_generation(tmp.path()).expect("current generation");
        let original_permissions = fs::metadata(&generation).expect("metadata").permissions();
        fs::set_permissions(&generation, fs::Permissions::from_mode(0o555))
            .expect("seal directory");
        let result = SearchIndex::open_or_create(tmp.path())
            .expect("open sealed generation")
            .search(&QueryOptions {
                query: "needle".to_string(),
                project: None,
                role: None,
                tool: None,
                session_id: None,
                session_scope: None,
                source: None,
                since: None,
                until: None,
                limit: 10,
            })
            .expect("search sealed generation");
        fs::set_permissions(&generation, original_permissions).expect("restore permissions");
        assert_eq!(result.len(), 1);
    }
}
