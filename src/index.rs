use crate::types::{Record, RecordLinks, SourceFilter};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::ops::Bound;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use tantivy::collector::{Count, TopDocs};
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    Directory, DirectoryLock, FileHandle, Lock, MmapDirectory, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::merge_policy::NoMergePolicy;
use tantivy::query::{AllQuery, BooleanQuery, EmptyQuery, Occur, Query, RangeQuery, TermQuery};
use tantivy::schema::Value;
use tantivy::schema::{
    FAST, Field, INDEXED, IndexRecordOption, STORED, STRING, Schema, SchemaBuilder, TEXT,
    TextFieldIndexing, TextOptions,
};
use tantivy::{Index, IndexReader, IndexWriter, Order, ReloadPolicy, TantivyDocument, Term};

#[derive(Clone)]
pub struct IndexFields {
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
    writable: bool,
    pending_generation: Option<Arc<PendingGeneration>>,
    _generation_lease: Option<Arc<GenerationLease>>,
    suppress_automatic_merges: bool,
}

const GENERATIONS_DIR: &str = "generations";
const CURRENT_FILE: &str = "CURRENT";
const GENERATION_LEASE_FILE: &str = ".lease";
const CONTINUOUS_MERGE_BATCH_SEGMENTS: usize = 128;
const CONTINUOUS_MERGE_MAX_INPUT_BYTES: u64 = 256 * 1024 * 1024;
const CONTINUOUS_MAX_SEGMENTS: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerationGcReport {
    pub generations_removed: usize,
    pub abandoned_workdirs_removed: usize,
    pub legacy_files_removed: usize,
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
    replaces_published_generation: bool,
    published: AtomicBool,
    _staging_lease: GenerationLease,
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
struct SealedDirectory(MmapDirectory);

impl Directory for SealedDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.0.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.0.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.0.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        self.0.open_write(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.0.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.0.atomic_write(path, data)
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.0.sync_directory()
    }

    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(())))
    }

    fn watch(&self, callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.0.watch(callback)
    }
}

const REASONING_ROLE: &str = "reasoning";

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
    pub include_reasoning: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionScopeKey {
    pub source: crate::types::SourceKind,
    pub session_id: String,
    pub source_path: String,
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
        let source = resolve_current_generation(dir).unwrap_or_else(|| dir.to_path_buf());
        if !source.join("meta.json").is_file() {
            bail!("no committed index exists at {}", dir.display());
        }

        let generations = dir.join(GENERATIONS_DIR);
        fs::create_dir_all(&generations)?;
        let old_generations = fs::read_dir(&generations)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.file_type().is_ok_and(|kind| kind.is_dir())
                    && !entry.file_name().to_string_lossy().starts_with('.')
            })
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        let abandoned_workdirs = fs::read_dir(&generations)?
            .filter_map(|entry| entry.ok())
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
        let report = GenerationGcReport {
            generations_removed: old_generations.len(),
            abandoned_workdirs_removed: abandoned_workdirs.len(),
            legacy_files_removed: legacy_files.len(),
            dry_run,
        };
        if dry_run {
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
        clone_generation(&source, temp.path())?;
        rewrite_managed_files_to_committed_set(temp.path())?;
        create_generation_lease_file(temp.path())?;
        let actual = validate_committed_generation(temp.path())?;
        if actual != expected {
            bail!(
                "clean index validation changed document count from {expected} to {actual}; \
                 existing index was left untouched"
            );
        }

        let generation_name = new_generation_name();
        let final_dir = generations.join(&generation_name);
        let staging = temp.keep();
        fs::rename(&staging, &final_dir)?;
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
        Ok(report)
    }

    pub fn open_or_create(dir: &Path) -> Result<Self> {
        loop {
            let Some(generation) = resolve_current_generation(dir) else {
                return Self::open_or_create_with_policy(dir, StaleSchemaPolicy::Error);
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

    fn open_or_create_for_ingest_with_merge_policy(
        dir: &Path,
        suppress_automatic_merges: bool,
    ) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let generations = dir.join(GENERATIONS_DIR);
        fs::create_dir_all(&generations)?;
        let generation_name = new_generation_name();
        let staging_dir = generations.join(format!(".{generation_name}.tmp"));

        let current = resolve_current_generation(dir);
        if let Some(current) = &current {
            clone_generation(current, &staging_dir)?;
        } else if dir.join("meta.json").exists() {
            clone_generation(dir, &staging_dir)?;
        } else {
            fs::create_dir_all(&staging_dir)?;
        }
        create_generation_lease_file(&staging_dir)?;
        let staging_lease = acquire_generation_lease(&staging_dir)?;

        let mut index =
            Self::open_or_create_with_policy(&staging_dir, StaleSchemaPolicy::Recreate)?;
        index.pending_generation = Some(Arc::new(PendingGeneration {
            index_root: dir.to_path_buf(),
            staging_dir,
            generation_name,
            replaces_published_generation: current.is_some(),
            published: AtomicBool::new(false),
            _staging_lease: staging_lease,
        }));
        index.suppress_automatic_merges = suppress_automatic_merges;
        Ok(index)
    }

    fn open_or_create_with_policy(
        dir: &Path,
        stale_schema_policy: StaleSchemaPolicy,
    ) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let meta_path = dir.join("meta.json");
        if meta_path.exists() {
            let index = Index::open_in_dir(dir)?;
            if !schema_is_current(&index.schema()) {
                return match stale_schema_policy {
                    StaleSchemaPolicy::Error => Err(stale_schema_error(dir)),
                    StaleSchemaPolicy::Recreate => {
                        drop(index);
                        recreate_index_dir(dir)
                    }
                };
            }
            let fields = load_fields(index.schema())?;
            Ok(Self {
                index,
                fields,
                writable: true,
                pending_generation: None,
                _generation_lease: None,
                suppress_automatic_merges: false,
            })
        } else {
            create_index_in_dir(dir)
        }
    }

    pub fn writer(&self) -> Result<IndexWriter> {
        if !self.writable {
            bail!("cannot create a writer for a sealed index generation");
        }
        let writer = self.index.writer(256_000_000)?;
        if self.suppress_automatic_merges {
            writer.set_merge_policy(Box::new(NoMergePolicy));
        }
        Ok(writer)
    }

    pub fn reader(&self) -> Result<IndexReader> {
        Ok(self
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?)
    }

    pub(crate) fn maybe_compact_continuous_segments(&self, writer: &mut IndexWriter) -> Result<()> {
        if !self.suppress_automatic_merges {
            return Ok(());
        }
        let Some(pending) = &self.pending_generation else {
            return Ok(());
        };
        let segments = self.index.searchable_segment_metas()?;
        if segments.len() > CONTINUOUS_MAX_SEGMENTS {
            bail!(
                "refusing to continue indexing: {} continuous index segments exceed the safety \
                 limit of {CONTINUOUS_MAX_SEGMENTS}; run an explicit compaction or reindex",
                segments.len()
            );
        }
        if segments.len() < CONTINUOUS_MERGE_BATCH_SEGMENTS {
            return Ok(());
        }

        let mut sized_segments = segments
            .into_iter()
            .map(|segment| {
                let bytes = segment
                    .list_files()
                    .into_iter()
                    .try_fold(0u64, |total, file| {
                        let path = pending.staging_dir.join(file);
                        match fs::metadata(path) {
                            Ok(metadata) => Ok(total.saturating_add(metadata.len())),
                            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(total),
                            Err(error) => Err(error),
                        }
                    })?;
                Ok((bytes, segment.id()))
            })
            .collect::<io::Result<Vec<_>>>()?;
        sized_segments.sort_unstable_by_key(|(bytes, _)| *bytes);
        let candidates = &sized_segments[..CONTINUOUS_MERGE_BATCH_SEGMENTS];
        let input_bytes = candidates
            .iter()
            .fold(0u64, |total, (bytes, _)| total.saturating_add(*bytes));
        if input_bytes > CONTINUOUS_MERGE_MAX_INPUT_BYTES {
            return Ok(());
        }
        let candidate_ids: Vec<_> = candidates.iter().map(|(_, id)| *id).collect();
        writer
            .merge(&candidate_ids)
            .wait()
            .context("compact bounded continuous index segment batch")?;
        Ok(())
    }

    pub(crate) fn publish_generation(&self) -> Result<()> {
        let Some(pending) = &self.pending_generation else {
            return Ok(());
        };
        if pending.published.load(AtomicOrdering::Acquire) {
            return Ok(());
        }

        let final_dir = pending
            .index_root
            .join(GENERATIONS_DIR)
            .join(&pending.generation_name);
        if pending.staging_dir.exists() {
            create_generation_lease_file(&pending.staging_dir)?;
            fs::rename(&pending.staging_dir, &final_dir)
                .with_context(|| format!("publish index generation {}", pending.generation_name))?;
        } else if !final_dir.exists() {
            bail!(
                "index generation {} has neither staging nor published data",
                pending.generation_name
            );
        }
        sync_directory(&pending.index_root.join(GENERATIONS_DIR))?;
        atomic_write_current(&pending.index_root, &pending.generation_name)?;
        pending.published.store(true, AtomicOrdering::Release);
        prune_superseded_generations(&pending.index_root, &pending.generation_name)?;
        prune_legacy_index_files(&pending.index_root)?;
        Ok(())
    }

    pub(crate) fn publish_generation_if_uninitialized(&self) -> Result<()> {
        if self
            .pending_generation
            .as_ref()
            .is_some_and(|pending| !pending.replaces_published_generation)
        {
            self.publish_generation()?;
        }
        Ok(())
    }

    pub fn delete_by_source_path(&self, writer: &mut IndexWriter, path: &str) {
        let term = Term::from_field_text(self.fields.source_path, path);
        writer.delete_term(term);
    }

    pub fn add_record(&self, writer: &mut IndexWriter, record: &Record) -> Result<()> {
        let mut doc = TantivyDocument::default();
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

    pub fn search(&self, options: &QueryOptions) -> Result<Vec<(f32, Record)>> {
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

    pub fn records_by_session_id_page(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<Record>, usize)> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let term = Term::from_field_text(self.fields.session_id, session_id);
        let query = TermQuery::new(term, IndexRecordOption::Basic);
        let total = searcher.search(&query, &Count)?;
        if offset >= total {
            return Ok((Vec::new(), total));
        }
        let page_limit = limit.max(1).min(total - offset);
        let collector = TopDocs::with_limit(page_limit)
            .and_offset(offset)
            .order_by_fast_field::<u64>("turn_id", Order::Asc);
        let top_docs: Vec<(u64, tantivy::DocAddress)> = searcher.search(&query, &collector)?;
        let mut records = Vec::with_capacity(top_docs.len());
        for (_turn_id, addr) in top_docs {
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
        let reader = self.reader()?;
        let searcher = reader.searcher();
        for segment_reader in searcher.segment_readers() {
            let store = segment_reader.get_store_reader(0)?;
            for doc in store.iter::<TantivyDocument>(segment_reader.alive_bitset()) {
                let doc = doc?;
                let record = record_from_doc(&self.fields, &doc);
                f(record)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum StaleSchemaPolicy {
    Error,
    Recreate,
}

fn stale_schema_error(dir: &Path) -> anyhow::Error {
    anyhow!(
        "index schema at {} is stale; run `memex index` or `memex reindex` to rebuild it",
        dir.display()
    )
}

fn recreate_index_dir(dir: &Path) -> Result<SearchIndex> {
    std::fs::remove_dir_all(dir)?;
    std::fs::create_dir_all(dir)?;
    create_index_in_dir(dir)
}

fn create_index_in_dir(dir: &Path) -> Result<SearchIndex> {
    let schema = build_schema()?;
    let index = Index::create_in_dir(dir, schema.clone())?;
    let fields = load_fields(schema)?;
    Ok(SearchIndex {
        index,
        fields,
        writable: true,
        pending_generation: None,
        _generation_lease: None,
        suppress_automatic_merges: false,
    })
}

fn open_sealed_generation(dir: &Path) -> Result<SearchIndex> {
    let generation_lease = acquire_generation_lease(dir)?;
    let directory = MmapDirectory::open(dir)
        .with_context(|| format!("open sealed index generation {}", dir.display()))?;
    let index = Index::open(SealedDirectory(directory))?;
    if !schema_is_current(&index.schema()) {
        return Err(stale_schema_error(dir));
    }
    let fields = load_fields(index.schema())?;
    Ok(SearchIndex {
        index,
        fields,
        writable: false,
        pending_generation: None,
        _generation_lease: Some(Arc::new(generation_lease)),
        suppress_automatic_merges: false,
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

fn clone_generation(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)?;
    for name in committed_generation_files(source)? {
        let source_file = source.join(&name);
        if !source_file.is_file() {
            continue;
        }
        let target = destination.join(&name);
        let name_text = name.to_string_lossy();
        if should_copy_generation_file(&name_text) || fs::hard_link(&source_file, &target).is_err()
        {
            fs::copy(&source_file, &target).with_context(|| {
                format!(
                    "copy index generation file {} to {}",
                    source_file.display(),
                    target.display()
                )
            })?;
        }
    }
    Ok(())
}

fn committed_generation_files(source: &Path) -> Result<HashSet<PathBuf>> {
    let index = Index::open_in_dir(source)
        .with_context(|| format!("open committed index generation {}", source.display()))?;
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
    let managed: HashSet<PathBuf> = committed_generation_files(generation)?
        .into_iter()
        .filter(|path| {
            generation.join(path).is_file()
                && path
                    .file_name()
                    .is_none_or(|name| !name.to_string_lossy().starts_with('.'))
        })
        .collect();
    let mut encoded = serde_json::to_vec(&managed)?;
    encoded.push(b'\n');
    fs::write(generation.join(".managed.json"), encoded)?;
    Ok(())
}

fn validate_committed_generation(generation: &Path) -> Result<u64> {
    let index = Index::open_in_dir(generation)
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

fn should_copy_generation_file(name: &str) -> bool {
    matches!(name, "meta.json" | ".managed.json")
}

fn create_generation_lease_file(generation: &Path) -> Result<()> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(generation.join(GENERATION_LEASE_FILE))?
        .sync_all()?;
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
    let generations = index_root.join(GENERATIONS_DIR);
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
    sync_directory(&generations)?;
    Ok(())
}

fn prune_legacy_index_files(index_root: &Path) -> Result<()> {
    for entry in fs::read_dir(index_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() || entry.file_name() == CURRENT_FILE {
            continue;
        }
        if let Err(error) = fs::remove_file(entry.path())
            && error.kind() != io::ErrorKind::PermissionDenied
        {
            return Err(error)
                .with_context(|| format!("prune legacy index file {}", entry.path().display()));
        }
    }
    sync_directory(index_root)?;
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
    temp.as_file_mut().sync_all()?;
    temp.persist(index_root.join(CURRENT_FILE))?;
    sync_directory(index_root)?;
    Ok(())
}

#[cfg(unix)]
fn sync_directory(dir: &Path) -> io::Result<()> {
    use std::fs::File;
    File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_dir: &Path) -> io::Result<()> {
    Ok(())
}

fn build_schema() -> Result<Schema> {
    let mut builder = SchemaBuilder::default();

    builder.add_u64_field("doc_id", INDEXED | STORED | FAST);
    builder.add_u64_field("ts", INDEXED | STORED | FAST);
    builder.add_text_field("project", STRING | STORED);
    builder.add_text_field("session_id", STRING | STORED);
    builder.add_u64_field("turn_id", INDEXED | STORED | FAST);
    builder.add_text_field("role", STRING | STORED);
    builder.add_text_field("source", STRING | STORED);

    let text_indexing = TextFieldIndexing::default()
        .set_tokenizer("default")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_options = TextOptions::default()
        .set_indexing_options(text_indexing)
        .set_stored();
    builder.add_text_field("text", text_options);

    builder.add_text_field("tool_name", STRING | STORED);
    builder.add_text_field("tool_input", TEXT | STORED);
    builder.add_text_field("tool_output", TEXT | STORED);
    builder.add_text_field("event_id", STRING | STORED);
    builder.add_text_field("parent_event_id", STRING | STORED);
    builder.add_text_field("logical_parent_event_id", STRING | STORED);
    builder.add_text_field("parent_session_id", STRING | STORED);
    builder.add_text_field("thread_source", STRING | STORED);
    builder.add_text_field("conversation_kind", STRING | STORED);
    builder.add_text_field("parent_tool_use_id", STRING | STORED);
    builder.add_text_field("source_tool_use_id", STRING | STORED);
    builder.add_text_field("source_tool_assistant_uuid", STRING | STORED);
    builder.add_text_field("source_path", STRING | STORED);

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

    let reasoning_requested = options.role.as_deref() == Some(REASONING_ROLE);
    if !options.include_reasoning && !reasoning_requested {
        let term = Term::from_field_text(fields.role, REASONING_ROLE);
        clauses.push((
            Occur::MustNot,
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
            include_reasoning: false,
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
    fn reasoning_records_are_excluded_unless_requested() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index = SearchIndex::open_or_create(tmp.path()).expect("index");
        let spoken = test_record(1, "shared needle");
        let mut thought = test_record(2, "shared needle");
        thought.role = REASONING_ROLE.to_string();
        let mut writer = index.writer().expect("writer");
        index.add_record(&mut writer, &spoken).expect("spoken");
        index.add_record(&mut writer, &thought).expect("thought");
        writer.commit().expect("commit");

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
            limit: 10,
            include_reasoning: false,
        };

        let default = index.search(&options).expect("default search");
        assert_eq!(
            default.iter().map(|(_, r)| r.doc_id).collect::<Vec<_>>(),
            vec![spoken.doc_id],
        );

        let included = index
            .search(&QueryOptions {
                include_reasoning: true,
                ..options.clone()
            })
            .expect("include reasoning");
        let mut ids = included.iter().map(|(_, r)| r.doc_id).collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, vec![spoken.doc_id, thought.doc_id]);

        let only = index
            .search(&QueryOptions {
                role: Some(REASONING_ROLE.to_string()),
                ..options
            })
            .expect("explicit role overrides the exclusion");
        assert_eq!(
            only.iter().map(|(_, r)| r.doc_id).collect::<Vec<_>>(),
            vec![thought.doc_id],
        );
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
        let legacy = SearchIndex::open_or_create_with_policy(tmp.path(), StaleSchemaPolicy::Error)
            .expect("legacy flat index");
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
    fn continuous_refreshes_do_not_automatically_merge_existing_segments() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let first = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("first generation");
        let mut writer = first.writer().expect("first writer");
        first
            .add_record(&mut writer, &test_record(1, "baseline"))
            .expect("add baseline");
        writer.commit().expect("commit baseline");
        writer
            .wait_merging_threads()
            .expect("finish baseline writer");
        first.publish_generation().expect("publish baseline");

        for doc_id in 2..=21 {
            let refresh = SearchIndex::open_or_create_for_continuous_ingest(tmp.path())
                .expect("continuous refresh");
            let mut writer = refresh.writer().expect("continuous writer");
            refresh
                .add_record(
                    &mut writer,
                    &test_record(doc_id, &format!("refresh-{doc_id}")),
                )
                .expect("add refresh record");
            writer.commit().expect("commit refresh");
            refresh
                .maybe_compact_continuous_segments(&mut writer)
                .expect("bounded compaction");
            writer
                .wait_merging_threads()
                .expect("finish refresh writer");
            refresh.publish_generation().expect("publish refresh");
        }

        let published = SearchIndex::open_or_create(tmp.path()).expect("published index");
        assert_eq!(
            published
                .index
                .searchable_segment_ids()
                .expect("searchable segments")
                .len(),
            21,
            "continuous refresh must not rewrite prior segments through automatic merging"
        );
    }

    #[test]
    fn continuous_compaction_batches_many_small_segments_without_major_rewrites() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let first = SearchIndex::open_or_create_for_ingest(tmp.path()).expect("first generation");
        let mut writer = first.writer().expect("first writer");
        first
            .add_record(&mut writer, &test_record(1, "baseline"))
            .expect("add baseline");
        writer.commit().expect("commit baseline");
        writer.wait_merging_threads().expect("finish baseline");
        first.publish_generation().expect("publish baseline");

        for doc_id in 2..=130 {
            let refresh = SearchIndex::open_or_create_for_continuous_ingest(tmp.path())
                .expect("continuous refresh");
            let mut writer = refresh.writer().expect("continuous writer");
            refresh
                .add_record(
                    &mut writer,
                    &test_record(doc_id, &format!("refresh{doc_id}")),
                )
                .expect("add refresh record");
            writer.commit().expect("commit refresh");
            refresh
                .maybe_compact_continuous_segments(&mut writer)
                .expect("bounded compaction");
            writer.wait_merging_threads().expect("finish refresh");
            refresh.publish_generation().expect("publish refresh");
        }

        let published = SearchIndex::open_or_create(tmp.path()).expect("published index");
        assert_eq!(
            published
                .index
                .searchable_segment_ids()
                .expect("searchable segments")
                .len(),
            3,
            "128 small segments should compact once, leaving two subsequent segments"
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
                include_reasoning: false,
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
                        include_reasoning: false,
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
                    include_reasoning: false,
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
                include_reasoning: false,
            })
            .expect("search sealed generation");
        fs::set_permissions(&generation, original_permissions).expect("restore permissions");
        assert_eq!(result.len(), 1);
    }
}
