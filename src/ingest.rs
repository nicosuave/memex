use crate::analytics::{AnalyticsStore, AnalyticsWriter, analytics_path, backfill_from_index};
use crate::config::{IndexedToolContentLimits, Paths};
use crate::embed::{EmbedRuntimeConfig, EmbedderHandle, ModelChoice};
use crate::index::SearchIndex;
use crate::lease::IngestLease;
use crate::progress::{Progress, SOURCE_COUNT};
use crate::state::{
    FileIdentity, FileState, IngestState, PendingIngest, PendingToolCall, ScanCache, SessionScope,
};
#[cfg(test)]
use crate::types::RecordLinks;
use crate::types::{Record, SourceKind};
use anyhow::{Context, Result, anyhow};
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

mod selection;

const EMBED_BATCH_SIZE: usize = 64;
const EMBED_MAX_CHARS: usize = 8192;
const RETAINED_HEAD_PERCENT: usize = 75;
const INDEX_PROGRESS_BATCH: u64 = 1;
// Keep a small amount of parser/writer overlap without retaining an unbounded transcript backlog.
const RECORD_CHANNEL_CAPACITY: usize = 8;

#[derive(Debug, Clone)]
pub struct IngestOptions {
    pub claude_sources: Vec<PathBuf>,
    pub include_agents: bool,
    pub include_reasoning: bool,
    pub include_codex: bool,
    pub include_opencode: bool,
    pub include_cursor: bool,
    pub include_pi: bool,
    pub include_omp: bool,
    pub include_openclaw: bool,
    pub include_copilot: bool,
    pub include_grok: bool,
    pub include_jcode: bool,
    pub include_muse: bool,
    pub include_antigravity: bool,
    pub exclude_patterns: Vec<String>,
    pub embeddings: bool,
    pub backfill_embeddings: bool,
    pub model: ModelChoice,
    pub embed_runtime: EmbedRuntimeConfig,
    pub tool_content_limits: IndexedToolContentLimits,
}

#[derive(Debug)]
pub struct IngestReport {
    pub records_added: usize,
    pub records_embedded: usize,
    pub files_scanned: usize,
    pub files_skipped: usize,
    pub diagnostics: crate::sources::ParseDiagnostics,
}

/// Whether a dirty batch stayed targeted or required a complete reconciliation.
/// Only a full scan may advance the daemon's reconciliation deadline.
pub(crate) struct DirtyIngestReport {
    pub report: IngestReport,
    pub full_scan: bool,
}

#[derive(Debug)]
struct FileTask {
    path: PathBuf,
    source: SourceKind,
    offset: u64,
    turn_id: u32,
    size: u64,
    mtime: i64,
    delete_first: bool,
    parser_version_invalidated: bool,
    pending_tool_calls: HashMap<String, PendingToolCall>,
    identity: FileIdentity,
    parser_version: u32,
}

#[derive(Debug)]
struct FileUpdate {
    path: String,
    state: FileState,
    session_id: Option<String>,
    diagnostics: crate::sources::ParseDiagnostics,
}

#[derive(Debug, Clone)]
struct PlannedOpencodeDatabase {
    path: PathBuf,
    scan: crate::sources::opencode::DatabaseScan,
}

struct PreparedOpencodeDatabase {
    path: PathBuf,
    scan: crate::sources::opencode::DatabaseScan,
    spool: tempfile::NamedTempFile,
    diagnostics: crate::sources::ParseDiagnostics,
}

const OPENCODE_SPOOL_PREFIX: &str = ".memex-opencode-spool-";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpencodeDatabaseOutcome {
    Planned,
    Ready,
    ConfirmedAbsent,
    Failed,
}

fn classify_opencode_database_outcome(
    outcome: Option<OpencodeDatabaseOutcome>,
    discovered: bool,
    inventory_completed: bool,
) -> OpencodeDatabaseOutcome {
    outcome.unwrap_or(if inventory_completed && !discovered {
        OpencodeDatabaseOutcome::ConfirmedAbsent
    } else {
        OpencodeDatabaseOutcome::Failed
    })
}

fn claim_opencode_session_owner(
    owners: &mut HashMap<String, String>,
    session_id: String,
    database_path: &str,
) {
    owners
        .entry(session_id)
        .or_insert_with(|| database_path.to_string());
}

const FILE_IDENTITY_PREFIX_BYTES: usize = 4096;

fn file_identity(path: &Path, metadata: &std::fs::Metadata, prefix_bytes: usize) -> FileIdentity {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;

    let prefix_sha256 = if metadata.is_file() {
        File::open(path).ok().and_then(|mut file| {
            let mut bytes = vec![0; prefix_bytes];
            let read = file.read(&mut bytes).ok()?;
            bytes.truncate(read);
            Some(format!("{:x}", Sha256::digest(&bytes)))
        })
    } else {
        None
    };

    FileIdentity {
        sqlite_wal: None,
        #[cfg(unix)]
        device: Some(metadata.dev()),
        #[cfg(not(unix))]
        device: None,
        #[cfg(unix)]
        inode: Some(metadata.ino()),
        #[cfg(not(unix))]
        inode: None,
        prefix_sha256,
        prefix_bytes: prefix_bytes as u64,
        modified_ns: metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64),
    }
}

fn file_was_replaced(previous: &FileIdentity, current: &FileIdentity) -> bool {
    let prefix_matches = previous
        .prefix_sha256
        .as_ref()
        .zip(current.prefix_sha256.as_ref())
        .is_some_and(|(old, new)| {
            previous.prefix_bytes > 0 && previous.prefix_bytes == current.prefix_bytes && old == new
        });
    let prefix_changed = previous
        .prefix_sha256
        .as_ref()
        .zip(current.prefix_sha256.as_ref())
        .is_some_and(|(old, new)| previous.prefix_bytes == current.prefix_bytes && old != new);
    let filesystem_identity_changed = previous
        .device
        .zip(previous.inode)
        .zip(current.device.zip(current.inode))
        .is_some_and(|((old_device, old_inode), (new_device, new_inode))| {
            old_inode != new_inode || (old_device != new_device && !prefix_matches)
        });

    filesystem_identity_changed || prefix_changed
}

fn prepare_file_task(
    path: PathBuf,
    source: SourceKind,
    include_reasoning: bool,
    metadata: &std::fs::Metadata,
    previous: Option<&FileState>,
) -> (FileTask, bool) {
    let size = metadata.len();
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0);
    let prefix_bytes = previous
        .map(|state| {
            if state.identity.prefix_bytes > 0 {
                state.identity.prefix_bytes
            } else {
                state.size.min(FILE_IDENTITY_PREFIX_BYTES as u64)
            }
        })
        .unwrap_or_else(|| size.min(FILE_IDENTITY_PREFIX_BYTES as u64))
        .min(size) as usize;
    let mut identity = file_identity(&path, metadata, prefix_bytes);
    if source == SourceKind::Antigravity && crate::sources::antigravity::is_db_path(&path) {
        identity.sqlite_wal = Some(crate::state::SqliteWalIdentity::read(&path));
    }
    let parser_version = crate::sources::index_state_version_for(source, include_reasoning);
    let parser_version_invalidated =
        previous.is_some_and(|previous| previous.parser_version != parser_version);
    let (offset, turn_id, delete_first, pending_tool_calls, skip) = match previous {
        None => (0, 0, false, HashMap::new(), false),
        Some(previous)
            if size < previous.size
                || mtime < previous.mtime
                || previous.parser_version != parser_version
                || file_was_replaced(&previous.identity, &identity)
                || previous.identity.sqlite_wal != identity.sqlite_wal
                || (size == previous.size
                    && previous
                        .identity
                        .modified_ns
                        .zip(identity.modified_ns)
                        .is_some_and(|(old, new)| old != new))
                || (size == previous.size && mtime != previous.mtime) =>
        {
            (0, 0, true, HashMap::new(), false)
        }
        Some(previous) if size == previous.size && mtime == previous.mtime => (
            previous.offset,
            previous.turn_id,
            false,
            previous.pending_tool_calls.clone(),
            true,
        ),
        // A jcode session is one JSON object, so a byte offset cannot resume
        // mid-file and the parser always emits from message zero. Reparse
        // atomically instead: delete_first purges the stale rows first, so
        // growing files can neither duplicate records nor inflate counts.
        Some(_) if source == SourceKind::Jcode => (0, 0, true, HashMap::new(), false),
        // An antigravity .db/overview.txt file is rewritten wholesale as the
        // conversation grows (SQLite stores the WAL separately); a byte offset
        // cannot resume mid-file, so reparse atomically instead.
        Some(_) if source == SourceKind::Antigravity => (0, 0, true, HashMap::new(), false),
        Some(previous) => (
            previous.offset,
            previous.turn_id,
            false,
            previous.pending_tool_calls.clone(),
            false,
        ),
    };

    (
        FileTask {
            path,
            source,
            offset,
            turn_id,
            size,
            mtime,
            delete_first,
            parser_version_invalidated,
            pending_tool_calls,
            identity,
            parser_version,
        },
        skip,
    )
}

fn discovered_metadata(path: &Path) -> Result<Option<std::fs::Metadata>> {
    match path.metadata() {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read metadata for {}", path.display())),
    }
}

fn is_not_found(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    })
}

fn finish_file_task(
    task: &FileTask,
    progress: &Progress,
    skipped: &AtomicUsize,
    result: Result<()>,
) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if is_not_found(&error) => {
            // Active agent clients may rotate or delete a transcript after discovery. Treat
            // that filesystem race as a skipped file instead of discarding the whole ingest.
            progress.add_files_done(task.source, 1);
            skipped.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to parse {} transcript {}",
                task.source.label(),
                task.path.display()
            )
        }),
    }
}

fn completed_file_state(
    task: &FileTask,
    offset: u64,
    turn_id: u32,
    pending_tool_calls: HashMap<String, PendingToolCall>,
) -> FileState {
    FileState {
        size: task.size,
        mtime: task.mtime,
        offset,
        turn_id,
        parser_version: task.parser_version,
        pending_tool_calls,
        identity: task.identity.clone(),
    }
}

#[derive(Clone)]
struct RecordSender {
    sender: Sender<Record>,
    limits: IndexedToolContentLimits,
    diagnostics: Arc<Mutex<crate::sources::ParseDiagnostics>>,
}

impl RecordSender {
    #[cfg(test)]
    fn new(sender: Sender<Record>, limits: IndexedToolContentLimits) -> Self {
        Self::with_diagnostics(
            sender,
            limits,
            Arc::new(Mutex::new(crate::sources::ParseDiagnostics::default())),
        )
    }

    fn with_diagnostics(
        sender: Sender<Record>,
        limits: IndexedToolContentLimits,
        diagnostics: Arc<Mutex<crate::sources::ParseDiagnostics>>,
    ) -> Self {
        Self {
            sender,
            limits,
            diagnostics,
        }
    }

    fn send(&self, mut record: Record) -> Result<()> {
        let (input_truncated, output_truncated) =
            limit_record_tool_content(&mut record, self.limits);
        if input_truncated || output_truncated {
            let mut diagnostics = self.diagnostics.lock().unwrap();
            diagnostics.truncated_tool_inputs += u64::from(input_truncated);
            diagnostics.truncated_tool_outputs += u64::from(output_truncated);
        }
        self.sender.send(record)?;
        Ok(())
    }
}

fn prehydrate_opencode_database(
    path: &Path,
    scan: &crate::sources::opencode::DatabaseScan,
    session_ids: &[String],
    next_doc_id: &AtomicU64,
    state_dir: &Path,
) -> Result<PreparedOpencodeDatabase> {
    let mut spool = tempfile::Builder::new()
        .prefix(OPENCODE_SPOOL_PREFIX)
        .tempfile_in(state_dir)
        .with_context(|| format!("create OpenCode hydration spool in {}", state_dir.display()))?;
    let mut diagnostics = crate::sources::ParseDiagnostics::default();
    for session_id in session_ids {
        let output = crate::sources::opencode::parse_database_records(
            path,
            session_id,
            crate::sources::IndexParseState::default(),
            next_doc_id,
            |record| {
                serde_json::to_writer(spool.as_file_mut(), &record)?;
                spool.as_file_mut().write_all(b"\n")?;
                Ok(())
            },
        )
        .with_context(|| {
            format!(
                "hydrate OpenCode session `{session_id}` from {}",
                path.display()
            )
        })?;
        diagnostics.merge(output.diagnostics);
    }
    spool.as_file_mut().flush()?;
    Ok(PreparedOpencodeDatabase {
        path: path.to_path_buf(),
        scan: scan.clone(),
        spool,
        diagnostics,
    })
}

fn cleanup_opencode_spools(state_dir: &Path) -> Result<()> {
    let entries = match std::fs::read_dir(state_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(OPENCODE_SPOOL_PREFIX)
            && entry.file_type()?.is_file()
        {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn replay_opencode_spool(
    spool: &mut tempfile::NamedTempFile,
    tx_record: &RecordSender,
    database_path: &Path,
    owned_session_ids: &HashSet<String>,
    progress: &Progress,
) -> Result<()> {
    spool.as_file_mut().seek(SeekFrom::Start(0))?;
    let reader = BufReader::new(spool.as_file_mut());
    for line in reader.lines() {
        let line = line.with_context(|| {
            format!(
                "read OpenCode hydration spool for {}",
                database_path.display()
            )
        })?;
        let record = serde_json::from_str::<Record>(&line).with_context(|| {
            format!(
                "decode OpenCode hydration spool for {}",
                database_path.display()
            )
        })?;
        if owned_session_ids.contains(&record.session_id) {
            progress.add_produced(SourceKind::Opencode, 1);
            tx_record.send(record)?;
        }
    }
    Ok(())
}

struct WriterContext {
    embeddings: bool,
    do_backfill_embeddings: bool,
    reset_vector_store: bool,
    vector_dir: PathBuf,
    analytics_path: PathBuf,
    progress: Arc<Progress>,
    model: ModelChoice,
    embed_runtime: EmbedRuntimeConfig,
    tool_content_limits: IndexedToolContentLimits,
    reconcile_vector_ids: bool,
    scope_targets: Vec<SessionScope>,
    opencode_session_cwds: HashMap<SessionScope, String>,
    vector_delete_paths: HashSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriterDecision {
    Commit,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriterOutcome {
    Published {
        records_added: usize,
        records_embedded: usize,
    },
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VectorMigration {
    rebuild: bool,
    model: ModelChoice,
}

fn vector_migration(
    vector_dir: &Path,
    tasks: &[FileTask],
    configured_model: ModelChoice,
) -> VectorMigration {
    let rebuild = tasks.iter().any(|task| task.parser_version_invalidated)
        && crate::vector::VectorIndex::exists(vector_dir);
    let model = if rebuild {
        crate::vector::VectorIndex::open(vector_dir)
            .ok()
            .and_then(|index| {
                index
                    .model()
                    .and_then(|model| ModelChoice::parse(model).ok())
            })
            .unwrap_or(configured_model)
    } else {
        configured_model
    };
    VectorMigration { rebuild, model }
}

fn record_channel() -> (Sender<Record>, Receiver<Record>) {
    bounded(RECORD_CHANNEL_CAPACITY)
}

fn parser_thread_pool() -> Result<rayon::ThreadPool> {
    build_parser_thread_pool(rayon::current_num_threads().max(1))
}

fn build_parser_thread_pool(num_threads: usize) -> Result<rayon::ThreadPool> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads.max(1))
        .thread_name(|index| format!("memex-parser-{index}"))
        .build()
        .context("build parser thread pool")
}

/// Check if scan cache is fresh and vector state is usable; if so, skip indexing entirely.
/// Returns Ok(None) if skipped due to fresh cache, Ok(Some(report)) if indexing ran.
pub fn ingest_if_stale(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    ttl_seconds: u64,
    lease: &IngestLease,
) -> Result<Option<IngestReport>> {
    let cache_path = paths.state.join("scan_cache.json");
    let cache = ScanCache::load(&cache_path)?;

    if can_skip_fresh_scan(&cache, paths, index, options, ttl_seconds)? {
        // The transcript scan cache cannot detect edits in a Markdown memory
        // file. Refresh these small documents even when transcript discovery is
        // still within its TTL, under the same ingestion lease.
        refresh_memories(paths, options)?;
        return Ok(None);
    }

    let report = ingest_all(paths, index, options, lease)?;
    Ok(Some(report))
}

/// Glob-based path exclusion applied at discovery time so matched
/// transcripts never enter the index. Empty pattern sets disable matching.
#[derive(Debug, Clone)]
pub(crate) struct PathExcluder {
    set: Option<globset::GlobSet>,
}

impl PathExcluder {
    pub(crate) fn build(patterns: &[String]) -> Result<Self> {
        if patterns.is_empty() {
            return Ok(Self { set: None });
        }
        let mut builder = globset::GlobSetBuilder::new();
        for pattern in patterns {
            builder.add(
                globset::GlobBuilder::new(pattern)
                    .literal_separator(false)
                    .build()
                    .with_context(|| format!("invalid exclude pattern: {pattern}"))?,
            );
        }
        let set = builder
            .build()
            .context("failed to compile exclude patterns")?;
        Ok(Self { set: Some(set) })
    }

    pub(crate) fn is_excluded(&self, path: &Path) -> bool {
        let Some(set) = &self.set else {
            return false;
        };
        set.is_match(path)
            || path
                .canonicalize()
                .is_ok_and(|canonical| canonical != path && set.is_match(&canonical))
    }
}

pub(crate) fn build_path_excluder(options: &IngestOptions) -> Result<PathExcluder> {
    let expanded = crate::config::expand_exclude_patterns(options.exclude_patterns.clone());
    PathExcluder::build(&expanded)
}

fn pending_ingest_path(paths: &Paths) -> PathBuf {
    paths.state.join("ingest.pending.json")
}

fn finalize_pending_ingest(
    pending_path: &Path,
    deferred_scopes: &[SessionScope],
    next_doc_id: u64,
) -> Result<()> {
    if deferred_scopes.is_empty() {
        return PendingIngest::clear(pending_path);
    }
    PendingIngest {
        next_doc_id,
        source_paths: Vec::new(),
        session_scopes: deferred_scopes.to_vec(),
        vector_publication: false,
    }
    .save(pending_path)
}

fn pending_scope_union(
    active_scopes: &[SessionScope],
    deferred_scopes: &[SessionScope],
) -> Vec<SessionScope> {
    let mut scopes = active_scopes
        .iter()
        .chain(deferred_scopes)
        .cloned()
        .collect::<Vec<_>>();
    scopes.sort_by(|left, right| {
        left.source_path
            .cmp(&right.source_path)
            .then_with(|| left.session_id.cmp(&right.session_id))
    });
    scopes.dedup();
    scopes
}

fn prepare_pending_ingest_recovery(
    paths: &Paths,
    state: &mut IngestState,
) -> Result<Option<PendingIngest>> {
    let pending_path = pending_ingest_path(paths);
    let Some(pending) = PendingIngest::load(&pending_path)
        .with_context(|| format!("load pending ingest at {}", pending_path.display()))?
    else {
        return Ok(None);
    };

    for source_path in &pending.source_paths {
        state.files.remove(source_path);
        if crate::sources::opencode::is_database_path(source_path) {
            state.opencode_databases.remove(source_path);
        }
    }
    state.next_doc_id = state.next_doc_id.max(pending.next_doc_id);
    Ok(Some(pending))
}

pub fn ingest_all(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    lease: &IngestLease,
) -> Result<IngestReport> {
    ingest_selected(paths, index, options, lease, None).map(|result| result.report)
}

/// Ingest a filesystem-event batch through the normal publication pipeline.
/// Missing/ambiguous inputs and interrupted publication fall back to full
/// reconciliation; a partial inventory never establishes global absence.
pub(crate) fn ingest_dirty(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    lease: &IngestLease,
    dirty: &HashSet<PathBuf>,
) -> Result<DirtyIngestReport> {
    ingest_selected(paths, index, options, lease, Some(dirty))
}

fn ingest_selected(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    _lease: &IngestLease,
    dirty: Option<&HashSet<PathBuf>>,
) -> Result<DirtyIngestReport> {
    // Apply additive analytics migrations even when the scan finds no changed files.
    drop(AnalyticsStore::open(analytics_path(&paths.state))?);
    let state_path = paths.state.join("ingest.json");
    let mut state = IngestState::load(&state_path)?;
    let pending_recovery = prepare_pending_ingest_recovery(paths, &mut state)?;
    let recovering_pending_ingest = pending_recovery.is_some();
    let mut deferred_pending_scopes = pending_recovery
        .as_ref()
        .map(|pending| pending.session_scopes.clone())
        .unwrap_or_default();
    cleanup_opencode_spools(&paths.state)?;
    let mut empty_index_rebuild = false;
    if index.doc_count()? == 0 && (!state.files.is_empty() || !state.opencode_databases.is_empty())
    {
        empty_index_rebuild = true;
        state.files.clear();
        state.opencode_databases.clear();
        if paths.vectors.exists() {
            std::fs::remove_dir_all(&paths.vectors)?;
            std::fs::create_dir_all(&paths.vectors)?;
        }
    }

    let selected = if let Some(dirty) = dirty
        && !recovering_pending_ingest
        && !empty_index_rebuild
        && state_path.exists()
    {
        match selection::resolve_dirty(options, dirty, &state)? {
            selection::DirtySelection::Paths { files, databases } => Some((files, databases)),
            selection::DirtySelection::Resync => None,
        }
    } else {
        None
    };
    let full_scan = selected.is_none();
    if full_scan {
        // Memory discovery is independent of transcript file events.
        refresh_memories(paths, options)?;
    }

    // Index-time exclusion: matched transcripts never enter the index, and
    // records previously indexed from now-excluded paths are removed.
    let excluder = build_path_excluder(options)?;
    let mut excluded_state_paths: Vec<String> = Vec::new();
    if full_scan {
        state.files.retain(|key, _| {
            if excluder.is_excluded(Path::new(key)) {
                excluded_state_paths.push(key.clone());
                false
            } else {
                true
            }
        });
    }
    let next_doc_id = Arc::new(AtomicU64::new(state.next_doc_id));

    let mut tasks = Vec::new();
    let mut files_scanned = 0usize;
    let mut files_skipped = 0usize;
    let mut total_bytes = 0u64;
    let mut opencode_ready_databases = Vec::new();
    let mut opencode_ready_owned_sessions = HashMap::new();
    let mut opencode_diagnostics = crate::sources::ParseDiagnostics::default();
    let mut opencode_scope_targets = Vec::new();
    let mut opencode_session_cwds = HashMap::new();
    let mut opencode_database_states = HashMap::new();
    let mut opencode_database_paths_to_delete = Vec::new();
    let mut opencode_database_outcomes = HashMap::new();
    let mut opencode_discovered_database_paths = HashSet::new();
    let mut opencode_legacy_paths_to_delete = Vec::new();

    if let Some((files, _)) = &selected {
        for file in files {
            let Some(meta) = discovered_metadata(&file.path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = file.path.to_string_lossy();
            let (task, skip) = prepare_file_task(
                file.path.clone(),
                file.source,
                options.include_reasoning,
                &meta,
                state.files.get(key.as_ref()),
            );
            if skip {
                files_skipped += 1;
            } else {
                tasks.push(task);
            }
        }
    }

    for claude_source in options.claude_sources.iter().filter(|_| full_scan) {
        if !claude_source.exists() {
            continue;
        }
        let claude_files = crate::sources::claude::discover(claude_source, options.include_agents)?;
        for source_file in claude_files {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let Some(meta) = discovered_metadata(&path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                path,
                SourceKind::Claude,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(task);
        }
    }

    let mut session_ids = selected.as_ref().map_or_else(HashSet::new, |(files, _)| {
        if files.iter().any(|file| {
            file.source == SourceKind::Codex && crate::sources::codex::is_history_path(&file.path)
        }) {
            selection::codex_session_ids(options, &state, files)
        } else {
            HashSet::new()
        }
    });
    if options.include_codex && full_scan {
        let codex_files = crate::sources::codex::discover_rollouts();
        for source_file in codex_files {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            if let Some(id) = crate::sources::codex::session_id_from_path(&path) {
                session_ids.insert(id);
            }
            let Some(meta) = discovered_metadata(&path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                path,
                SourceKind::Codex,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(task);
        }
    }

    if options.include_codex && full_scan {
        for history_path in crate::sources::codex::history_paths() {
            if excluder.is_excluded(&history_path) {
                files_skipped += 1;
                continue;
            }
            let Some(meta) = discovered_metadata(&history_path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = history_path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                history_path,
                SourceKind::Codex,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                files_skipped += 1;
            } else {
                tasks.push(task);
            }
        }
    }

    if options.include_opencode
        && (full_scan
            || selected
                .as_ref()
                .is_some_and(|(_, databases)| !databases.is_empty()))
    {
        let database_files = if let Some((_, databases)) = &selected {
            databases.clone()
        } else {
            crate::sources::opencode::discover_databases()?
        };
        let mut planned_databases = Vec::new();
        for source_file in database_files {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let key = path.to_string_lossy().to_string();
            opencode_discovered_database_paths.insert(key.clone());
            let Some(meta) = (match discovered_metadata(&path) {
                Ok(meta) => meta,
                Err(error) => {
                    if !full_scan {
                        return Err(error)
                            .with_context(|| format!("stat changed database {}", path.display()));
                    }
                    opencode_database_outcomes.insert(key, OpencodeDatabaseOutcome::Failed);
                    files_skipped += 1;
                    continue;
                }
            }) else {
                if !full_scan {
                    return ingest_selected(paths, index, options, _lease, None);
                }
                opencode_database_outcomes.insert(key, OpencodeDatabaseOutcome::Failed);
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let previous = state.opencode_databases.get(&key);
            match crate::sources::opencode::scan_database(&path, previous) {
                Ok(scan) => {
                    if !full_scan
                        && previous.is_none_or(|previous| {
                            let sessions = scan
                                .sessions
                                .iter()
                                .map(|session| session.id.clone())
                                .collect::<HashSet<_>>();
                            sessions != previous.owned_session_ids
                        })
                    {
                        // Adding/removing session ownership can affect other
                        // databases or the legacy JSON store. Ordinary WAL
                        // updates retain the inventory and stay targeted.
                        return ingest_selected(paths, index, options, _lease, None);
                    }
                    opencode_database_outcomes.insert(key, OpencodeDatabaseOutcome::Planned);
                    planned_databases.push(PlannedOpencodeDatabase { path, scan });
                }
                Err(error) => {
                    if !full_scan {
                        return Err(error)
                            .with_context(|| format!("scan changed database {}", path.display()));
                    }
                    // A bad/locked modern database must not hide the compatible JSON store.
                    opencode_database_outcomes.insert(key, OpencodeDatabaseOutcome::Failed);
                    files_skipped += 1;
                }
            }
        }
        planned_databases.sort_by(|left, right| left.path.cmp(&right.path));

        for database in planned_databases {
            let path = database.path.to_string_lossy().to_string();
            let mut hydration_session_ids = database.scan.dirty_session_ids.clone();
            if let Some(pending) = &pending_recovery {
                hydration_session_ids.extend(
                    pending
                        .session_scopes
                        .iter()
                        .filter(|scope| scope.source_path == path)
                        .map(|scope| scope.session_id.clone()),
                );
                hydration_session_ids.sort();
                hydration_session_ids.dedup();
            }
            match prehydrate_opencode_database(
                &database.path,
                &database.scan,
                &hydration_session_ids,
                &next_doc_id,
                &paths.state,
            ) {
                Ok(prepared) => {
                    opencode_database_outcomes.insert(path, OpencodeDatabaseOutcome::Ready);
                    opencode_diagnostics.merge(prepared.diagnostics.clone());
                    opencode_ready_databases.push(prepared);
                }
                Err(error) => {
                    if !full_scan {
                        return Err(error).with_context(|| {
                            format!("parse changed database {}", database.path.display())
                        });
                    }
                    opencode_database_outcomes.insert(path, OpencodeDatabaseOutcome::Failed);
                    files_skipped += 1;
                }
            }
        }
        opencode_ready_databases.sort_by(|left, right| left.path.cmp(&right.path));

        let mut owner_by_session = HashMap::<String, String>::new();
        let mut failed_previous = state
            .opencode_databases
            .iter()
            .filter(|(path, _)| {
                matches!(
                    classify_opencode_database_outcome(
                        opencode_database_outcomes.get(*path).copied(),
                        opencode_discovered_database_paths.contains(*path),
                        full_scan,
                    ),
                    OpencodeDatabaseOutcome::Failed
                )
            })
            .collect::<Vec<_>>();
        failed_previous.sort_by_key(|(path, _)| *path);
        for (path, previous) in failed_previous {
            for session_id in &previous.owned_session_ids {
                claim_opencode_session_owner(&mut owner_by_session, session_id.clone(), path);
            }
        }
        for database in &opencode_ready_databases {
            let path = database.path.to_string_lossy().to_string();
            for session in &database.scan.sessions {
                claim_opencode_session_owner(&mut owner_by_session, session.id.clone(), &path);
            }
        }
        for (path, previous) in &state.opencode_databases {
            let outcome = classify_opencode_database_outcome(
                opencode_database_outcomes.get(path).copied(),
                opencode_discovered_database_paths.contains(path),
                full_scan,
            );
            if outcome != OpencodeDatabaseOutcome::Ready {
                if outcome == OpencodeDatabaseOutcome::ConfirmedAbsent {
                    opencode_database_paths_to_delete.push(path.clone());
                }
                continue;
            }
            for session_id in &previous.owned_session_ids {
                if owner_by_session.get(session_id) != Some(path) {
                    opencode_scope_targets.push(SessionScope {
                        source_path: path.clone(),
                        session_id: session_id.clone(),
                    });
                }
            }
        }

        for database in &opencode_ready_databases {
            let path = database.path.to_string_lossy().to_string();
            let owned_sessions = database
                .scan
                .sessions
                .iter()
                .filter(|session| owner_by_session.get(&session.id) == Some(&path))
                .collect::<Vec<_>>();
            let owned_session_ids = owned_sessions
                .iter()
                .map(|session| session.id.clone())
                .collect::<HashSet<_>>();
            opencode_ready_owned_sessions.insert(path.clone(), owned_session_ids.clone());
            for session in &owned_sessions {
                opencode_session_cwds.insert(
                    SessionScope {
                        source_path: path.clone(),
                        session_id: session.id.clone(),
                    },
                    session.directory.clone(),
                );
            }
            for session_id in &database.scan.dirty_session_ids {
                if owner_by_session.get(session_id) == Some(&path) {
                    opencode_scope_targets.push(SessionScope {
                        source_path: path.clone(),
                        session_id: session_id.clone(),
                    });
                }
            }
            opencode_database_states.insert(
                path,
                crate::state::OpencodeDatabaseState {
                    parser_version: crate::sources::opencode::DATABASE_STATE_VERSION,
                    event_rowid: database.scan.cursor.event_rowid,
                    event_id: database.scan.cursor.event_id.clone(),
                    owned_session_ids,
                },
            );
        }
        if let Some(pending) = &pending_recovery {
            deferred_pending_scopes = pending
                .session_scopes
                .iter()
                .filter(|scope| {
                    classify_opencode_database_outcome(
                        opencode_database_outcomes.get(&scope.source_path).copied(),
                        opencode_discovered_database_paths.contains(&scope.source_path),
                        full_scan,
                    ) == OpencodeDatabaseOutcome::Failed
                })
                .cloned()
                .collect();
            for scope in &pending.session_scopes {
                if matches!(
                    opencode_database_outcomes.get(&scope.source_path),
                    Some(OpencodeDatabaseOutcome::Ready)
                ) {
                    opencode_scope_targets.push(scope.clone());
                }
            }
        }
        opencode_scope_targets.sort_by(|left, right| {
            left.source_path
                .cmp(&right.source_path)
                .then_with(|| left.session_id.cmp(&right.session_id))
        });
        opencode_scope_targets.dedup();
        opencode_database_paths_to_delete.sort();
        opencode_database_paths_to_delete.dedup();

        let opencode_files = if full_scan {
            crate::sources::opencode::discover_sessions()?
        } else {
            Vec::new()
        };
        for source_file in opencode_files {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let session_id = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            if owner_by_session.contains_key(session_id) {
                let path_key = path.to_string_lossy().to_string();
                state.files.remove(&path_key);
                opencode_legacy_paths_to_delete.push(path_key);
                files_skipped += 1;
                continue;
            }
            let Some(meta) = discovered_metadata(&path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                path,
                SourceKind::Opencode,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(task);
        }
    }

    if options.include_cursor && full_scan {
        let cursor_files = crate::sources::cursor::discover_transcripts();
        for source_file in cursor_files {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let Some(meta) = discovered_metadata(&path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                path,
                SourceKind::Cursor,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(task);
        }
    }

    if options.include_pi && full_scan {
        let pi_files = crate::sources::pi::discover();
        for source_file in pi_files {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let Some(meta) = discovered_metadata(&path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                path,
                SourceKind::Pi,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(task);
        }
    }

    if options.include_omp && full_scan {
        let omp_files = crate::sources::omp::discover();
        for source_file in omp_files {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let Some(meta) = discovered_metadata(&path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                path,
                SourceKind::Omp,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(task);
        }
    }

    if options.include_openclaw && full_scan {
        for source_file in crate::sources::openclaw::discover() {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let Some(meta) = discovered_metadata(&path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                path,
                SourceKind::OpenClaw,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(task);
        }
    }

    if options.include_copilot && full_scan {
        let copilot_files = crate::sources::copilot::discover_sessions();
        for source_file in copilot_files {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let Some(meta) = discovered_metadata(&path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                path,
                SourceKind::Copilot,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(task);
        }
    }

    if options.include_grok && full_scan {
        for source_file in crate::sources::grok::discover_sessions() {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let Some(meta) = discovered_metadata(&path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                path,
                SourceKind::Grok,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(task);
        }
    }

    if options.include_jcode && full_scan {
        let jcode_files = crate::sources::jcode::discover();
        for source_file in jcode_files {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let Some(meta) = discovered_metadata(&path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                path,
                SourceKind::Jcode,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(task);
        }
    }

    if options.include_muse && full_scan {
        let muse_files = crate::sources::muse::discover();
        for source_file in muse_files {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let Some(meta) = discovered_metadata(&path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                path,
                SourceKind::Muse,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(task);
        }
    }

    if options.include_antigravity && full_scan {
        let antigravity_files = crate::sources::antigravity::discover();
        for source_file in antigravity_files {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let Some(meta) = discovered_metadata(&path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                path,
                SourceKind::Antigravity,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(task);
        }
    }

    // Previously indexed records under now-excluded paths must be deleted even
    // when there is no ingest state entry for them (e.g. state loss or legacy runs).
    let mut excluded_index_paths: Vec<String> = Vec::new();
    if full_scan && excluder.set.is_some() {
        index.for_each_record(|record| {
            if excluder.is_excluded(Path::new(&record.source_path)) {
                excluded_index_paths.push(record.source_path.clone());
            }
            Ok(())
        })?;
        excluded_index_paths.sort();
        excluded_index_paths.dedup();
    }
    files_skipped += excluded_state_paths.len();

    let opencode_session_links = if tasks.iter().any(|task| task.source == SourceKind::Opencode) {
        crate::sources::opencode::session_links_by_id()
    } else {
        HashMap::new()
    };

    let recover_vectors = pending_recovery
        .as_ref()
        .is_some_and(|pending| pending.vector_publication);
    let pending_ready_scope_recovery = pending_recovery.as_ref().is_some_and(|pending| {
        pending.session_scopes.iter().any(|scope| {
            matches!(
                opencode_database_outcomes.get(&scope.source_path),
                Some(OpencodeDatabaseOutcome::Ready)
            )
        })
    });
    let recover_vector_cleanup = pending_ready_scope_recovery
        || pending_recovery.as_ref().is_some_and(|pending| {
            pending
                .source_paths
                .iter()
                .any(|path| crate::sources::opencode::is_database_path(path))
        });
    let mut delete_paths = pending_recovery
        .as_ref()
        .map(|pending| pending.source_paths.clone())
        .unwrap_or_default();
    delete_paths.extend(opencode_database_paths_to_delete.clone());
    delete_paths.extend(opencode_legacy_paths_to_delete.clone());
    delete_paths.extend(excluded_state_paths);
    delete_paths.extend(excluded_index_paths);
    delete_paths.sort();
    delete_paths.dedup();
    let pending_database_paths_to_delete = pending_recovery
        .as_ref()
        .map(|pending| {
            pending
                .source_paths
                .iter()
                .filter(|path| crate::sources::opencode::is_database_path(path))
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut installed_opencode_states = state.opencode_databases.clone();
    for path in &opencode_database_paths_to_delete {
        installed_opencode_states.remove(path);
    }
    installed_opencode_states.extend(opencode_database_states.clone());
    let opencode_database_state_changed = installed_opencode_states != state.opencode_databases;

    let totals = compute_totals(&tasks);
    let file_totals = compute_file_totals(&tasks);
    let analytics_db = analytics_path(&paths.state);
    let analytics_needs_backfill =
        !AnalyticsStore::is_complete(&analytics_db) && index.doc_count()? > 0;
    if !recover_vectors
        && tasks.is_empty()
        && delete_paths.is_empty()
        && opencode_scope_targets.is_empty()
        && opencode_ready_databases
            .iter()
            .all(|database| database.scan.dirty_session_ids.is_empty())
        && (!full_scan || can_skip_noop_index(paths, index, options)?)
    {
        if analytics_needs_backfill {
            backfill_from_index(&analytics_db, index)?;
        }
        index.publish_generation_if_uninitialized()?;
        state.opencode_databases = installed_opencode_states;
        if recovering_pending_ingest || empty_index_rebuild {
            state.save(&state_path)?;
        }
        if opencode_database_state_changed {
            state.save(&state_path)?;
        }
        if full_scan {
            update_scan_cache(paths, files_scanned, total_bytes)?;
        }
        if recovering_pending_ingest {
            finalize_pending_ingest(
                &pending_ingest_path(paths),
                &deferred_pending_scopes,
                state.next_doc_id,
            )?;
        }
        return Ok(DirtyIngestReport {
            full_scan,
            report: IngestReport {
                records_added: 0,
                records_embedded: 0,
                files_scanned,
                files_skipped,
                diagnostics: Default::default(),
            },
        });
    }

    let mut vector_migration = vector_migration(&paths.vectors, &tasks, options.model);
    if recover_vectors
        && !options.embeddings
        && crate::vector::VectorIndex::exists(&paths.vectors)
        && let Some(model) = crate::vector::VectorIndex::open(&paths.vectors)?
            .model()
            .and_then(|model| ModelChoice::parse(model).ok())
    {
        vector_migration.model = model;
    }
    let embeddings = options.embeddings || vector_migration.rebuild || recover_vectors;
    let vector_publication = embeddings
        || ((recover_vector_cleanup
            || !opencode_scope_targets.is_empty()
            || !opencode_database_paths_to_delete.is_empty())
            && crate::vector::VectorIndex::exists(&paths.vectors));
    let progress = Arc::new(Progress::new(totals, file_totals, embeddings));

    let (raw_tx_record, rx_record) = record_channel();
    let shared_diagnostics = Arc::new(Mutex::new(crate::sources::ParseDiagnostics::default()));
    shared_diagnostics
        .lock()
        .unwrap()
        .merge(opencode_diagnostics);
    let tx_record = RecordSender::with_diagnostics(
        raw_tx_record,
        options.tool_content_limits,
        shared_diagnostics.clone(),
    );
    let (tx_update, rx_update) = unbounded::<FileUpdate>();

    delete_paths.extend(
        tasks
            .iter()
            .filter(|t| t.delete_first)
            .map(|t| t.path.to_string_lossy().to_string()),
    );
    delete_paths.sort();
    delete_paths.dedup();

    let mut affected_paths = delete_paths.clone();
    affected_paths.extend(
        tasks
            .iter()
            .map(|task| task.path.to_string_lossy().to_string()),
    );
    affected_paths.sort();
    affected_paths.dedup();
    let pending_scopes = pending_scope_union(&opencode_scope_targets, &deferred_pending_scopes);
    let mut pending_ingest = PendingIngest {
        next_doc_id: state.next_doc_id,
        source_paths: affected_paths,
        session_scopes: pending_scopes,
        vector_publication,
    };
    let pending_path = pending_ingest_path(paths);
    pending_ingest
        .save(&pending_path)
        .with_context(|| format!("save pending ingest at {}", pending_path.display()))?;

    let writer = index
        .writer()
        .context("failed to initialize the Tantivy index writer")?;
    let writer_index = index.clone();
    let writer_ctx = WriterContext {
        embeddings,
        do_backfill_embeddings: options.backfill_embeddings
            || vector_migration.rebuild
            || recover_vectors,
        reset_vector_store: vector_migration.rebuild,
        vector_dir: paths.vectors.clone(),
        analytics_path: analytics_db.clone(),
        progress: progress.clone(),
        model: vector_migration.model,
        embed_runtime: options.embed_runtime.clone(),
        tool_content_limits: options.tool_content_limits,
        reconcile_vector_ids: recover_vectors,
        scope_targets: opencode_scope_targets.clone(),
        opencode_session_cwds: opencode_session_cwds.clone(),
        vector_delete_paths: opencode_database_paths_to_delete
            .iter()
            .chain(pending_database_paths_to_delete.iter())
            .chain(opencode_legacy_paths_to_delete.iter())
            .cloned()
            .collect(),
    };
    let (decision_tx, decision_rx) = bounded(1);
    let writer_handle = std::thread::spawn(move || {
        writer_loop(
            writer_index,
            writer,
            rx_record,
            decision_rx,
            delete_paths,
            writer_ctx,
        )
    });

    let tasks_arc = Arc::new(tasks);
    let parse_skipped = AtomicUsize::new(0);
    let parser_pool = parser_thread_pool()?;
    let parser_result = parser_pool.install(|| {
        tasks_arc.par_iter().try_for_each(|task| -> Result<()> {
            let result = match task.source {
                SourceKind::Claude => parse_claude_file(
                    task,
                    options.include_reasoning,
                    &tx_record,
                    &tx_update,
                    &next_doc_id,
                    &progress,
                ),
                SourceKind::Codex => {
                    if crate::sources::codex::is_history_path(&task.path) {
                        parse_codex_history(
                            task,
                            &tx_record,
                            &tx_update,
                            &next_doc_id,
                            &session_ids,
                            &progress,
                        )
                    } else {
                        parse_codex_session(
                            task,
                            options.include_reasoning,
                            &tx_record,
                            &tx_update,
                            &next_doc_id,
                            &progress,
                        )
                    }
                }
                SourceKind::Opencode => parse_opencode_file(
                    task,
                    &tx_record,
                    &tx_update,
                    &next_doc_id,
                    &progress,
                    &opencode_session_links,
                ),
                SourceKind::Omp => parse_omp_file(
                    task,
                    options.include_reasoning,
                    &tx_record,
                    &tx_update,
                    &next_doc_id,
                    &progress,
                ),
                SourceKind::Cursor => {
                    parse_cursor_file(task, &tx_record, &tx_update, &next_doc_id, &progress)
                }
                SourceKind::Pi => parse_pi_file(
                    task,
                    options.include_reasoning,
                    &tx_record,
                    &tx_update,
                    &next_doc_id,
                    &progress,
                ),
                SourceKind::OpenClaw => parse_openclaw_file(
                    task,
                    options.include_reasoning,
                    &tx_record,
                    &tx_update,
                    &next_doc_id,
                    &progress,
                ),
                SourceKind::Copilot => {
                    parse_copilot_session(task, &tx_record, &tx_update, &next_doc_id, &progress)
                }
                SourceKind::Grok => parse_grok_session(
                    task,
                    options.include_reasoning,
                    &tx_record,
                    &tx_update,
                    &next_doc_id,
                    &progress,
                ),
                SourceKind::Hermes => Err(anyhow!("Hermes indexing is not supported")),
                SourceKind::Jcode => parse_jcode_file(
                    task,
                    options.include_reasoning,
                    &tx_record,
                    &tx_update,
                    &next_doc_id,
                    &progress,
                ),
                SourceKind::Muse => parse_muse_file(
                    task,
                    options.include_reasoning,
                    &tx_record,
                    &tx_update,
                    &next_doc_id,
                    &progress,
                ),
                SourceKind::Antigravity => parse_antigravity_file(
                    task,
                    options.include_reasoning,
                    &tx_record,
                    &tx_update,
                    &next_doc_id,
                    &progress,
                ),
            };
            finish_file_task(task, &progress, &parse_skipped, result)
        })
    });
    let parser_result = parser_result.and_then(|_| {
        for database in &mut opencode_ready_databases {
            let path = database.path.to_string_lossy().to_string();
            let owned_session_ids = opencode_ready_owned_sessions
                .get(&path)
                .cloned()
                .unwrap_or_default();
            replay_opencode_spool(
                &mut database.spool,
                &tx_record,
                &database.path,
                &owned_session_ids,
                &progress,
            )?;
        }
        Ok(())
    });

    drop(tx_record);
    drop(tx_update);

    // Parsers are done; what follows is commit/merge/publish plus analytics
    // and state writes, none of which is per-source work. Keep one spinner
    // visible so the tail doesn't read as hung.
    let tail = progress.tail_spinner("committing index…");
    pending_ingest.next_doc_id = next_doc_id.load(Ordering::SeqCst);
    let pending_update_error = pending_ingest
        .save(&pending_path)
        .with_context(|| format!("update pending ingest at {}", pending_path.display()))
        .err();
    let decision = if parser_result.is_ok() && pending_update_error.is_none() {
        WriterDecision::Commit
    } else {
        WriterDecision::Cancel
    };
    // A failed writer may have already closed the channel. Joining below keeps that root cause.
    let _ = decision_tx.send(decision);
    let writer_result = writer_handle.join().map_err(|_| {
        tail.finish_and_clear();
        anyhow!("writer thread panicked")
    })?;
    progress.finish();
    tail.set_message("updating analytics…");
    let outcome = (|| -> Result<DirtyIngestReport> {
        let writer_outcome =
            writer_result.context("index writer stopped before ingestion completed")?;
        parser_result?;
        if let Some(error) = pending_update_error {
            return Err(error);
        }
        let (records_added, records_embedded) = match writer_outcome {
            WriterOutcome::Published {
                records_added,
                records_embedded,
            } => (records_added, records_embedded),
            WriterOutcome::Cancelled => {
                return Err(anyhow!(
                    "index writer cancelled a successful ingest publication"
                ));
            }
        };
        if analytics_needs_backfill {
            let published_index = SearchIndex::open_or_create(&paths.index)?;
            backfill_from_index(&analytics_db, &published_index)?;
        } else {
            AnalyticsStore::open(&analytics_db)?.mark_complete()?;
        }

        let mut diagnostics = shared_diagnostics.lock().unwrap().clone();
        let mut updated_files = HashMap::new();
        while let Ok(update) = rx_update.recv() {
            updated_files.insert(update.path.clone(), update.state.clone());
            diagnostics.merge(update.diagnostics);
            let _ = update.session_id;
        }

        for (path, update) in updated_files {
            state.files.insert(path, update);
        }
        state.opencode_databases = installed_opencode_states;
        state.next_doc_id = next_doc_id.load(Ordering::SeqCst);
        state.save(&state_path)?;

        if full_scan {
            update_scan_cache(paths, files_scanned, total_bytes)?;
        }
        finalize_pending_ingest(&pending_path, &deferred_pending_scopes, state.next_doc_id)?;

        Ok(DirtyIngestReport {
            full_scan,
            report: IngestReport {
                records_added,
                records_embedded,
                files_scanned,
                files_skipped: files_skipped + parse_skipped.load(Ordering::Relaxed),
                diagnostics,
            },
        })
    })();
    tail.finish_and_clear();
    outcome
}

fn refresh_memories(paths: &Paths, options: &IngestOptions) -> Result<()> {
    let mut enabled_sources = HashSet::new();
    if !options.claude_sources.is_empty() {
        enabled_sources.insert(SourceKind::Claude);
    }
    if options.include_codex {
        enabled_sources.insert(SourceKind::Codex);
    }
    let discovery = crate::memory::MemoryDiscoveryOptions {
        claude_project_roots: options.claude_sources.clone(),
        codex_homes: if options.include_codex {
            crate::sources::codex::homes()
        } else {
            Vec::new()
        },
        enabled_sources,
        exclude_patterns: options.exclude_patterns.clone(),
    };
    let store = crate::memory::MemoryStore::new(paths.root.join("memory/documents.json"));
    let report = store.refresh(&discovery)?;
    if report.changed && (report.document_count > 0 || report.deleted > 0) {
        eprintln!(
            "memory index: {} documents, {} sections ({} updated, {} deleted, {} stale)",
            report.document_count,
            report.section_count,
            report.parsed,
            report.deleted,
            report.stale_count,
        );
    }
    for failure in &report.failures {
        eprintln!(
            "memory index: {}: {}",
            failure.path.display(),
            failure.error
        );
    }
    if options.embeddings {
        let count =
            crate::memory_search::embed_memory(paths, options.model, &options.embed_runtime)?;
        if count > 0 {
            eprintln!("memory index: embedded {count} sections");
        }
    }
    Ok(())
}

fn update_scan_cache(paths: &Paths, files_scanned: usize, total_bytes: u64) -> Result<()> {
    let cache_path = paths.state.join("scan_cache.json");
    let mut cache = ScanCache::load(&cache_path)?;
    cache.update(files_scanned, total_bytes);
    cache.save(&cache_path)
}

fn can_skip_fresh_scan(
    cache: &ScanCache,
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    ttl_seconds: u64,
) -> Result<bool> {
    let pending_path = pending_ingest_path(paths);
    if pending_path
        .try_exists()
        .with_context(|| format!("check pending ingest at {}", pending_path.display()))?
    {
        return Ok(false);
    }
    if options.include_opencode {
        let databases = match crate::sources::opencode::discover_databases() {
            Ok(databases) => databases,
            Err(_) => return Ok(false),
        };
        if !databases.is_empty() {
            return Ok(false);
        }
    }
    if index.doc_count()? == 0 {
        return Ok(false);
    }
    if !cache.is_fresh(ttl_seconds) {
        return Ok(false);
    }
    let analytics = AnalyticsStore::open(analytics_path(&paths.state))?;
    if !analytics.complete()? && index.doc_count()? > 0 {
        return Ok(false);
    }
    can_skip_noop_index(paths, index, options)
}

fn can_skip_noop_index(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
) -> Result<bool> {
    if !options.embeddings {
        return Ok(true);
    }
    let Some(dimensions) = options.model.known_dimensions() else {
        return Ok(false);
    };
    if !crate::vector::VectorIndex::exists(&paths.vectors) {
        return Ok(false);
    }
    let vector_index = crate::vector::VectorIndex::open(&paths.vectors)?;
    if vector_index.model() != Some(options.model.as_str())
        || vector_index.dimensions() != dimensions
    {
        return Ok(false);
    }
    vector_index_covers_embeddable_records(index, &vector_index)
}

fn vector_index_covers_embeddable_records(
    index: &SearchIndex,
    vector_index: &crate::vector::VectorIndex,
) -> Result<bool> {
    let mut covers_all = true;
    index.for_each_record(|record| {
        if record_needs_embedding(&record) && !vector_index.contains(record.doc_id) {
            covers_all = false;
        }
        Ok(())
    })?;
    Ok(covers_all)
}

fn record_needs_embedding(record: &Record) -> bool {
    is_embedding_role(&record.role) && !record.text.is_empty()
}

fn open_vector_index_for_ingest(
    vector_dir: &Path,
    dimensions: usize,
    model: ModelChoice,
    replace: bool,
) -> Result<crate::vector::VectorIndex> {
    if replace {
        crate::vector::VectorIndex::empty_replacement(vector_dir, dimensions, Some(model.as_str()))
    } else {
        crate::vector::VectorIndex::open_or_create(vector_dir, dimensions, Some(model.as_str()))
    }
}

fn writer_loop(
    index: SearchIndex,
    mut writer: tantivy::IndexWriter,
    rx: Receiver<Record>,
    decision_rx: Receiver<WriterDecision>,
    delete_paths: Vec<String>,
    ctx: WriterContext,
) -> Result<WriterOutcome> {
    let WriterContext {
        embeddings,
        do_backfill_embeddings,
        reset_vector_store,
        vector_dir,
        analytics_path,
        progress,
        model,
        embed_runtime,
        tool_content_limits,
        reconcile_vector_ids,
        scope_targets,
        opencode_session_cwds,
        vector_delete_paths,
    } = ctx;
    let mut analytics = AnalyticsWriter::open(&analytics_path)?;
    let mut scoped_doc_ids = HashSet::new();
    for scope in &scope_targets {
        for doc_id in index.doc_ids_by_source_scope(scope)? {
            scoped_doc_ids.insert(doc_id);
        }
        index.delete_by_source_scope(&mut writer, scope)?;
    }
    for path in &delete_paths {
        if vector_delete_paths.contains(path) {
            for doc_id in index.doc_ids_by_source_path(path)? {
                scoped_doc_ids.insert(doc_id);
            }
        }
        index.delete_by_source_path(&mut writer, path);
    }
    for (scope, cwd) in &opencode_session_cwds {
        analytics.set_session_cwd(
            SourceKind::Opencode,
            &scope.source_path,
            &scope.session_id,
            cwd,
        );
    }

    let mut count = 0usize;
    let mut embedded_count = 0usize;
    let mut vector_index = None;
    let mut embedder: Option<EmbedderHandle> = None;
    let mut embed_buffer: Vec<(u64, String, SourceKind)> = Vec::new();
    let mut index_pending = [0u64; SOURCE_COUNT];
    if embeddings {
        let handle = EmbedderHandle::with_model_and_runtime(model, &embed_runtime)?;
        let dims = handle.dims;
        vector_index = Some(open_vector_index_for_ingest(
            &vector_dir,
            dims,
            model,
            reset_vector_store,
        )?);
        embedder = Some(handle);
        progress.set_embed_ready();
    } else if (reconcile_vector_ids || !scoped_doc_ids.is_empty())
        && crate::vector::VectorIndex::exists(&vector_dir)
    {
        vector_index = Some(crate::vector::VectorIndex::open(&vector_dir)?);
    }
    if let Some(vindex) = vector_index.as_mut() {
        vindex.remove_doc_ids(&scoped_doc_ids)?;
    }

    for mut record in rx.iter() {
        // Parsers apply the limit before queueing; enforce it here as a defensive boundary too.
        let _ = limit_record_tool_content(&mut record, tool_content_limits);
        analytics.record(&record)?;
        index.add_record(&mut writer, &record)?;
        let source_idx = record.source.idx();
        index_pending[source_idx] += 1;
        if index_pending[source_idx] >= INDEX_PROGRESS_BATCH {
            progress.add_indexed(record.source, index_pending[source_idx]);
            index_pending[source_idx] = 0;
        }
        if embeddings
            && !reset_vector_store
            && is_embedding_role(&record.role)
            && !record.text.is_empty()
        {
            let text = truncate_for_embedding(std::mem::take(&mut record.text));
            if let Some(vindex) = vector_index.as_ref()
                && !vindex.contains(record.doc_id)
            {
                progress.add_embed_total(record.source, 1);
                progress.add_embed_pending(record.source, 1);
                embed_buffer.push((record.doc_id, text, record.source));
            }
            if let Some(emb) = embedder.as_mut()
                && embed_buffer.len() >= EMBED_BATCH_SIZE
            {
                embedded_count += flush_embeddings(
                    &mut embed_buffer,
                    emb,
                    vector_index.as_mut().unwrap(),
                    &progress,
                )?;
            }
        }
        count += 1;
    }

    // Flush any remaining index progress
    for (idx, &pending) in index_pending.iter().enumerate() {
        if pending > 0
            && let Some(source) = SourceKind::from_idx(idx)
        {
            progress.add_indexed(source, pending);
        }
    }

    match decision_rx.recv() {
        Ok(WriterDecision::Commit) => {}
        Ok(WriterDecision::Cancel) | Err(_) => {
            writer.rollback()?;
            return Ok(WriterOutcome::Cancelled);
        }
    }

    for scope in &scope_targets {
        analytics.delete_session_scope(scope)?;
    }
    for path in delete_paths {
        analytics.delete_source_path(&path)?;
    }
    analytics.flush()?;
    writer.commit()?;
    index.maybe_compact_continuous_segments(&mut writer)?;
    let mut staged_vectors = None;
    if reconcile_vector_ids {
        let mut live_doc_ids = HashSet::new();
        index.for_each_record(|record| {
            if is_embedding_role(&record.role) && !record.text.is_empty() {
                live_doc_ids.insert(record.doc_id);
            }
            Ok(())
        })?;
        if let Some(vindex) = vector_index.as_mut() {
            vindex.retain_doc_ids(&live_doc_ids)?;
        }
    }
    if embeddings {
        if !embed_buffer.is_empty() {
            embedded_count += flush_embeddings(
                &mut embed_buffer,
                embedder.as_mut().unwrap(),
                vector_index.as_mut().unwrap(),
                &progress,
            )?;
        }

        let needs_vector_backfill = match vector_index.as_ref() {
            Some(vindex) => {
                vindex.needs_backfill() || !vector_index_covers_embeddable_records(&index, vindex)?
            }
            None => false,
        };
        if do_backfill_embeddings || needs_vector_backfill {
            embedded_count += backfill_embeddings(
                &index,
                embedder.as_mut().unwrap(),
                vector_index.as_mut().unwrap(),
                &progress,
            )?;
        }
    }
    if let Some(vindex) = vector_index.as_ref() {
        staged_vectors = Some(vindex.stage()?);
    }
    if let Some(handle) = embedder.take() {
        std::mem::forget(handle);
    }
    writer.wait_merging_threads()?;
    index.publish_generation()?;
    if let Some(staged) = staged_vectors {
        staged.publish()?;
    }
    Ok(WriterOutcome::Published {
        records_added: count,
        records_embedded: embedded_count,
    })
}

fn backfill_embeddings(
    index: &SearchIndex,
    embedder: &mut EmbedderHandle,
    vector_index: &mut crate::vector::VectorIndex,
    progress: &Arc<Progress>,
) -> Result<usize> {
    use std::cell::Cell;
    let embedded_count = Cell::new(0usize);
    let mut embed_buffer: Vec<(u64, String, SourceKind)> = Vec::new();
    index.for_each_record(|record| {
        if record.text.is_empty()
            || !is_embedding_role(&record.role)
            || vector_index.contains(record.doc_id)
        {
            return Ok(());
        }
        progress.add_embed_total(record.source, 1);
        progress.add_embed_pending(record.source, 1);
        embed_buffer.push((
            record.doc_id,
            truncate_for_embedding(record.text),
            record.source,
        ));
        if embed_buffer.len() >= EMBED_BATCH_SIZE {
            let n = flush_embeddings(&mut embed_buffer, embedder, vector_index, progress)?;
            embedded_count.set(embedded_count.get() + n);
        }
        Ok(())
    })?;
    if !embed_buffer.is_empty() {
        let n = flush_embeddings(&mut embed_buffer, embedder, vector_index, progress)?;
        embedded_count.set(embedded_count.get() + n);
    }
    Ok(embedded_count.get())
}

fn parse_claude_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::claude::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Claude, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Claude,
        source_path,
        parsed,
    )
}

fn parse_codex_session(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::codex::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Codex, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Codex,
        source_path,
        parsed,
    )
}

fn parse_codex_history(
    task: &FileTask,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    session_ids: &HashSet<String>,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::codex::parse_history_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        session_ids,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Codex, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Codex,
        source_path,
        parsed,
    )
}

fn finish_source_parse(
    task: &FileTask,
    tx_update: &Sender<FileUpdate>,
    progress: &Arc<Progress>,
    source: SourceKind,
    source_path: String,
    parsed: crate::sources::IndexParseOutput,
) -> Result<()> {
    progress.add_parsed_bytes(source, parsed.offset.saturating_sub(task.offset));
    progress.add_files_done(source, 1);
    let state = completed_file_state(
        task,
        parsed.offset,
        parsed.turn_id,
        parsed.pending_tool_calls,
    );
    tx_update.send(FileUpdate {
        path: source_path,
        state,
        session_id: parsed.session_id,
        diagnostics: parsed.diagnostics,
    })?;
    Ok(())
}
fn parse_opencode_file(
    task: &FileTask,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
    opencode_session_links: &HashMap<String, crate::sources::opencode::SessionLinks>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::opencode::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        opencode_session_links,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Opencode, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Opencode,
        source_path,
        parsed,
    )
}

fn parse_jcode_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::jcode::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Jcode, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Jcode,
        source_path,
        parsed,
    )
}

fn parse_muse_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::muse::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Muse, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Muse,
        source_path,
        parsed,
    )
}

fn parse_antigravity_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::antigravity::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Antigravity, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Antigravity,
        source_path,
        parsed,
    )
}

fn parse_cursor_file(
    task: &FileTask,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::cursor::parse_index_records(
        &task.path,
        task.mtime,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Cursor, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Cursor,
        source_path,
        parsed,
    )
}
fn parse_pi_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::pi::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Pi, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Pi,
        source_path,
        parsed,
    )
}
fn parse_omp_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::omp::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Omp, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Omp,
        source_path,
        parsed,
    )
}
fn parse_openclaw_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::openclaw::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::OpenClaw, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::OpenClaw,
        source_path,
        parsed,
    )
}
fn parse_copilot_session(
    task: &FileTask,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::copilot::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Copilot, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Copilot,
        source_path,
        parsed,
    )
}

fn parse_grok_session(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::grok::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Grok, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Grok,
        source_path,
        parsed,
    )
}
fn flush_embeddings(
    buffer: &mut Vec<(u64, String, SourceKind)>,
    embedder: &mut EmbedderHandle,
    vindex: &mut crate::vector::VectorIndex,
    progress: &Arc<Progress>,
) -> Result<usize> {
    if buffer.is_empty() {
        return Ok(0);
    }

    // Prepare texts for batch embedding
    let items: Vec<(u64, String, SourceKind)> = buffer
        .drain(..)
        .map(|(doc_id, text, source)| (doc_id, truncate_for_embedding(text), source))
        .filter(|(_, text, _)| !text.is_empty())
        .collect();

    if items.is_empty() {
        return Ok(0);
    }

    // Batch embed all texts at once (ONNX Runtime handles internal parallelism)
    let texts: Vec<&str> = items.iter().map(|(_, text, _)| text.as_str()).collect();
    let embeddings = embedder.embed_texts(&texts)?;

    // Add embeddings to index
    let mut count = 0;
    for ((doc_id, _, source), vec) in items.iter().zip(embeddings.iter()) {
        vindex.add(*doc_id, vec)?;
        progress.sub_embed_pending(*source, 1);
        progress.add_embedded(*source, 1);
        count += 1;
    }
    Ok(count)
}

fn compute_totals(tasks: &[FileTask]) -> [u64; SOURCE_COUNT] {
    let mut totals = [0u64; SOURCE_COUNT];
    for task in tasks {
        let remaining = task.size.saturating_sub(task.offset);
        totals[task.source.idx()] += remaining;
    }
    totals
}

fn compute_file_totals(tasks: &[FileTask]) -> [u64; SOURCE_COUNT] {
    let mut totals = [0u64; SOURCE_COUNT];
    for task in tasks {
        totals[task.source.idx()] += 1;
    }
    totals
}

fn truncate_for_embedding(mut text: String) -> String {
    if text.len() <= EMBED_MAX_CHARS {
        return text;
    }
    let mut end = EMBED_MAX_CHARS.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text
}

fn limit_record_tool_content(
    record: &mut Record,
    limits: IndexedToolContentLimits,
) -> (bool, bool) {
    let original_input_len = record.tool_input.as_ref().map(String::len);
    let original_output_len = record.tool_output.as_ref().map(String::len);
    let text_limit = match record.role.as_str() {
        "tool_use" => Some(limits.input_bytes),
        "tool_result" => Some(limits.output_bytes),
        _ if record.tool_output.is_some() => Some(limits.output_bytes),
        _ if record.tool_input.is_some() => Some(limits.input_bytes),
        _ => None,
    };
    if let Some(max_bytes) = text_limit {
        truncate_for_index(&mut record.text, max_bytes);
    }
    if let Some(tool_input) = record.tool_input.as_mut() {
        truncate_for_index(tool_input, limits.input_bytes);
    }
    if let Some(tool_output) = record.tool_output.as_mut() {
        truncate_for_index(tool_output, limits.output_bytes);
    }
    (
        original_input_len
            .zip(record.tool_input.as_ref().map(String::len))
            .is_some_and(|(before, after)| after < before),
        original_output_len
            .zip(record.tool_output.as_ref().map(String::len))
            .is_some_and(|(before, after)| after < before),
    )
}

fn truncate_for_index(text: &mut String, max_bytes: usize) {
    if text.len() <= max_bytes {
        return;
    }

    let original_len = text.len();
    let mut marker = truncation_marker(original_len);
    let (head_end, tail_start) = loop {
        let retained_bytes = max_bytes.saturating_sub(marker.len());
        let head_target = retained_bytes.saturating_mul(RETAINED_HEAD_PERCENT) / 100;
        let head_end = char_boundary_at_or_before(text, head_target);
        let tail_target = retained_bytes.saturating_sub(head_end);
        let tail_start = char_boundary_at_or_after(text, original_len.saturating_sub(tail_target));
        let omitted_bytes = tail_start.saturating_sub(head_end);
        let updated_marker = truncation_marker(omitted_bytes);
        if updated_marker.len() == marker.len() {
            marker = updated_marker;
            break (head_end, tail_start);
        }
        marker = updated_marker;
    };

    let tail = text[tail_start..].to_string();
    text.truncate(head_end);
    text.push_str(&marker);
    text.push_str(&tail);
}

fn char_boundary_at_or_before(text: &str, mut position: usize) -> usize {
    position = position.min(text.len());
    while position > 0 && !text.is_char_boundary(position) {
        position -= 1;
    }
    position
}

fn char_boundary_at_or_after(text: &str, mut position: usize) -> usize {
    position = position.min(text.len());
    while position < text.len() && !text.is_char_boundary(position) {
        position += 1;
    }
    position
}

fn truncation_marker(omitted_bytes: usize) -> String {
    format!("\n\n[... {omitted_bytes} bytes truncated ...]\n\n")
}

fn is_embedding_role(role: &str) -> bool {
    role == "user" || role == "assistant"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{IndexedToolContentLimits, Paths};
    use crate::embed::{EmbedRuntimeConfig, ModelChoice};
    use crate::index::SearchIndex;
    use crate::test_support::{EnvVarGuard, env_lock};
    use crate::vector::VectorIndex;
    use std::fs;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn ingest_options(embeddings: bool, model: ModelChoice) -> IngestOptions {
        IngestOptions {
            claude_sources: vec![PathBuf::from("/does/not/exist")],
            exclude_patterns: Vec::new(),
            include_agents: false,
            include_reasoning: false,
            include_codex: false,
            include_opencode: false,
            include_cursor: false,
            include_pi: false,
            include_omp: false,
            include_openclaw: false,
            include_copilot: false,
            include_grok: false,
            include_jcode: false,
            include_muse: false,
            include_antigravity: false,
            embeddings,
            backfill_embeddings: false,
            model,
            embed_runtime: EmbedRuntimeConfig::default(),
            tool_content_limits: IndexedToolContentLimits::default(),
        }
    }

    #[test]
    fn exclusion_filters_new_and_previously_indexed_transcripts() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let claude_root = tmp.path().join("claude-projects");
        let keep_dir = claude_root.join("-Users-nico-Code-personal");
        let drop_dir = claude_root.join("-Users-nico-Code-client-x");
        fs::create_dir_all(&keep_dir).expect("create keep dir");
        fs::create_dir_all(&drop_dir).expect("create drop dir");
        let keep_file = keep_dir.join("keep.jsonl");
        let drop_file = drop_dir.join("drop.jsonl");
        let line = br#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hello"}]},"uuid":"u1","timestamp":"2024-01-01T00:00:00Z"}
"#;
        fs::write(&keep_file, line).expect("write keep");
        fs::write(&drop_file, line).expect("write drop");

        let paths = Paths::new(Some(tmp.path().join("memex-root"))).expect("paths");
        paths.ensure_dirs().expect("ensure dirs");
        let index = open_search_index(&paths);

        // First run with no exclusions indexes both transcripts.
        let mut options = ingest_options(false, ModelChoice::Gemma);
        options.claude_sources = vec![claude_root.clone()];
        let lease = ingest_lease(&paths);
        let report = ingest_all(&paths, &index, &options, &lease).expect("first ingest");
        assert_eq!(report.records_added, 2);
        assert!(index.doc_count().expect("doc count") >= 2);

        // Adding an exclusion removes the previously indexed transcript and
        // never indexes newly discovered files under matched paths.
        let drop_pattern = format!("{}/*-client-*/*.jsonl", claude_root.to_string_lossy());
        options.exclude_patterns = vec![drop_pattern];
        let new_drop = drop_dir.join("new-drop.jsonl");
        fs::write(&new_drop, line).expect("write new drop");
        let report = ingest_all(&paths, &index, &options, &lease).expect("second ingest");
        assert_eq!(
            report.records_added, 0,
            "excluded files must not be indexed"
        );

        let mut remaining = Vec::new();
        index
            .for_each_record(|record| {
                remaining.push(record.source_path.clone());
                Ok(())
            })
            .expect("collect remaining records");
        assert!(
            remaining.iter().all(|p| !p.contains("-client-")),
            "excluded transcripts must be purged from the index, got: {remaining:?}"
        );
        assert!(
            remaining.iter().any(|p| p.contains("keep.jsonl")),
            "non-excluded transcripts must remain indexed, got: {remaining:?}"
        );

        // Ingest state must not retain entries for excluded paths.
        let state = IngestState::load(&paths.state.join("ingest.json")).expect("load state");
        assert!(
            state.files.keys().all(|k| !k.contains("-client-")),
            "excluded paths must be pruned from ingest state"
        );
    }

    #[test]
    fn memory_edits_refresh_inside_transcript_scan_ttl_without_creating_sessions() {
        let tmp = tempfile::tempdir().unwrap();
        let claude = tmp.path().join("claude/projects");
        let project = claude.join("-work-project");
        let memory = project.join("memory/MEMORY.md");
        fs::create_dir_all(memory.parent().unwrap()).unwrap();
        fs::write(project.join("session.jsonl"),
            "{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"a conversation\"},\"uuid\":\"u1\"}\n"
        ).unwrap();
        fs::write(
            &memory,
            "# Decisions\n\nOriginal middle paragraph.\n\n## Retired\nOld decision.\n",
        )
        .unwrap();
        let paths = Paths::new(Some(tmp.path().join("data"))).unwrap();
        paths.ensure_dirs().unwrap();
        let mut options = ingest_options(false, ModelChoice::Gemma);
        options.claude_sources = vec![claude];
        let lease = ingest_lease(&paths);
        ingest_all(&paths, &open_search_index(&paths), &options, &lease).unwrap();
        let store = crate::memory::MemoryStore::new(paths.root.join("memory/documents.json"));
        let original = store.load().unwrap();
        assert_eq!(original.documents.len(), 1);
        let original_id = original.documents[0].stable_id.clone();
        let original_version = original.documents[0].version_sha256.clone();
        let published = SearchIndex::open_or_create(&paths.index).unwrap();
        assert_eq!(
            published.doc_count().unwrap(),
            1,
            "memories must not become transcript records"
        );

        fs::write(&memory, "# Decisions\n\nRevised middle paragraph.\n").unwrap();
        assert!(
            can_skip_fresh_scan(
                &ScanCache::load(&paths.state.join("scan_cache.json")).unwrap(),
                &paths,
                &published,
                &options,
                3600,
            )
            .unwrap()
        );
        assert!(
            ingest_if_stale(&paths, &published, &options, 3600, &lease)
                .unwrap()
                .is_none()
        );
        let updated = store.load().unwrap();
        assert_eq!(updated.documents[0].stable_id, original_id);
        assert_ne!(updated.documents[0].version_sha256, original_version);
        assert!(!updated.documents[0].content.contains("Retired"));
        assert!(updated.documents[0].content.contains("Revised middle"));
        assert_eq!(published.doc_count().unwrap(), 1);

        fs::remove_file(memory).unwrap();
        ingest_if_stale(&paths, &published, &options, 3600, &lease).unwrap();
        assert!(store.load().unwrap().documents.is_empty());
    }

    #[test]
    fn ingest_discovers_claude_transcripts_across_multiple_roots() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let first_root = tmp.path().join("claude-one").join("projects");
        let second_root = tmp.path().join("claude-two").join("projects");
        let first_project = first_root.join("-Users-nico-Code-first");
        let second_project = second_root.join("-Users-nico-Code-second");
        fs::create_dir_all(&first_project).expect("create first project");
        fs::create_dir_all(&second_project).expect("create second project");
        fs::write(
            first_project.join("first.jsonl"),
            r#"{"type":"user","message":{"role":"user","content":"first root"},"uuid":"u1"}
"#,
        )
        .expect("write first transcript");
        fs::write(
            second_project.join("second.jsonl"),
            r#"{"type":"user","message":{"role":"user","content":"second root"},"uuid":"u2"}
"#,
        )
        .expect("write second transcript");

        let paths = Paths::new(Some(tmp.path().join("memex-root"))).expect("paths");
        paths.ensure_dirs().expect("ensure dirs");
        let index = open_search_index(&paths);
        let mut options = ingest_options(false, ModelChoice::Gemma);
        options.claude_sources = vec![first_root, second_root];

        let lease = ingest_lease(&paths);
        let report = ingest_all(&paths, &index, &options, &lease).expect("ingest");

        assert_eq!(report.files_scanned, 2);
        assert_eq!(report.records_added, 2);
    }

    fn append_claude_message(path: &Path, text: &str) {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        writeln!(
            file,
            "{}",
            serde_json::json!({
                "type": "user", "uuid": text,
                "message": {"role": "user", "content": text},
            })
        )
        .unwrap();
    }

    fn indexed_texts(paths: &Paths) -> Vec<String> {
        let index = SearchIndex::open_or_create(&paths.index).unwrap();
        let mut texts = Vec::new();
        index
            .for_each_record(|record| {
                texts.push(record.text);
                Ok(())
            })
            .unwrap();
        texts.sort();
        texts
    }

    #[test]
    fn targeted_ingest_only_updates_selected_files_until_reconciliation() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("claude");
        fs::create_dir_all(&source).unwrap();
        let first = source.join("first.jsonl");
        let second = source.join("second.jsonl");
        append_claude_message(&first, "first original");
        append_claude_message(&second, "second original");
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let mut options = ingest_options(false, ModelChoice::Gemma);
        options.claude_sources = vec![source];
        let lease = ingest_lease(&paths);
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        assert_eq!(
            ingest_all(&paths, &index, &options, &lease)
                .unwrap()
                .files_scanned,
            2
        );
        let cache_path = paths.state.join("scan_cache.json");
        let original_cache = fs::read(&cache_path).unwrap();
        let state_before = IngestState::load(&paths.state.join("ingest.json")).unwrap();
        append_claude_message(&first, "first appended");
        append_claude_message(&second, "second missed event");
        // The watcher emits canonical paths; ingestion must retain lexical
        // discovery keys rather than create duplicate state/index entries.
        let dirty = HashSet::from([fs::canonicalize(&first).unwrap()]);
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        let result = ingest_dirty(&paths, &index, &options, &lease, &dirty).unwrap();
        assert!(!result.full_scan);
        assert_eq!(result.report.files_scanned, 1);
        assert_eq!(result.report.records_added, 1);
        assert_eq!(
            indexed_texts(&paths),
            ["first appended", "first original", "second original"]
        );
        let state_after = IngestState::load(&paths.state.join("ingest.json")).unwrap();
        let key = second.to_string_lossy();
        assert_eq!(
            state_after.files[key.as_ref()].offset,
            state_before.files[key.as_ref()].offset
        );
        assert_eq!(state_after.files.len(), 2);
        assert_eq!(fs::read(&cache_path).unwrap(), original_cache);

        // A no-op batch must not mark a complete scan fresh either.
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        let result = ingest_dirty(&paths, &index, &options, &lease, &dirty).unwrap();
        assert!(!result.full_scan);
        assert_eq!(result.report.records_added, 0);
        assert_eq!(fs::read(&cache_path).unwrap(), original_cache);

        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        let report = ingest_all(&paths, &index, &options, &lease).unwrap();
        assert_eq!(report.files_scanned, 2);
        assert_eq!(report.records_added, 1);
        assert_eq!(
            indexed_texts(&paths),
            [
                "first appended",
                "first original",
                "second missed event",
                "second original"
            ]
        );
    }

    #[test]
    fn targeted_ingest_creates_and_replaces_files_without_unrelated_discovery() {
        let _guard = env_lock();
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("claude");
        fs::create_dir_all(&source).unwrap();
        let first = source.join("first.jsonl");
        append_claude_message(&first, "existing");
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let mut options = ingest_options(false, ModelChoice::Gemma);
        options.claude_sources = vec![source.clone()];
        let lease = ingest_lease(&paths);
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        ingest_all(&paths, &index, &options, &lease).unwrap();
        // A full discovery would fail on this unrelated configured source.
        let bad_root = tmp.path().join("not-a-directory");
        fs::write(&bad_root, "not a directory").unwrap();
        let _env = EnvVarGuard::set_os(&[("OPENCODE_DATA_DIR", Some(bad_root.as_os_str()))]);
        options.include_opencode = true;
        let new_file = source.join("new.jsonl");
        append_claude_message(&new_file, "created");
        for expected in ["created", "rewritten"] {
            if expected == "rewritten" {
                fs::write(&new_file, []).unwrap();
                append_claude_message(&new_file, expected);
            }
            let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
            let result = ingest_dirty(
                &paths,
                &index,
                &options,
                &lease,
                &HashSet::from([new_file.clone()]),
            )
            .unwrap();
            assert!(!result.full_scan);
            assert_eq!(result.report.files_scanned, 1);
            assert_eq!(result.report.records_added, 1);
            let mut expected_texts = vec!["existing".to_string(), expected.to_string()];
            expected_texts.sort();
            assert_eq!(indexed_texts(&paths), expected_texts);
        }
    }

    #[test]
    fn targeted_ingest_escalates_pending_publication_to_full_recovery() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("claude");
        fs::create_dir_all(&source).unwrap();
        let first = source.join("first.jsonl");
        let second = source.join("second.jsonl");
        append_claude_message(&first, "first original");
        append_claude_message(&second, "second original");
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let mut options = ingest_options(false, ModelChoice::Gemma);
        options.claude_sources = vec![source];
        let lease = ingest_lease(&paths);
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        ingest_all(&paths, &index, &options, &lease).unwrap();
        append_claude_message(&first, "first appended");
        append_claude_message(&second, "second missed event");
        let state = IngestState::load(&paths.state.join("ingest.json")).unwrap();
        PendingIngest {
            next_doc_id: state.next_doc_id,
            source_paths: vec![second.to_string_lossy().into_owned()],
            session_scopes: Vec::new(),
            vector_publication: false,
        }
        .save(&pending_ingest_path(&paths))
        .unwrap();
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        let result =
            ingest_dirty(&paths, &index, &options, &lease, &HashSet::from([first])).unwrap();
        assert!(result.full_scan);
        assert_eq!(result.report.files_scanned, 2);
        assert_eq!(
            indexed_texts(&paths),
            [
                "first appended",
                "first original",
                "second missed event",
                "second original"
            ]
        );
        assert!(!pending_ingest_path(&paths).exists());
    }

    #[test]
    fn targeted_ingest_preserves_unselected_database_ownership_and_wal_updates() {
        let _guard = env_lock();
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("opencode");
        fs::create_dir_all(&source).unwrap();
        let first = source.join("opencode.db");
        let second = source.join("opencode-work.db");
        let create = |path: &Path, id: &str| {
            let writer = rusqlite::Connection::open(path).unwrap();
            writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
                CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT, time_created INTEGER, time_updated INTEGER);
                CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
                CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, data TEXT);
                CREATE TABLE event (id TEXT NOT NULL, aggregate_id TEXT NOT NULL);").unwrap();
            writer
                .execute("INSERT INTO session VALUES (?1, NULL, '/tmp', 1, 2)", [id])
                .unwrap();
            writer
                .execute(
                    "INSERT INTO message VALUES ('message', ?1, 3, '{\"role\":\"assistant\"}')",
                    [id],
                )
                .unwrap();
            writer
                .execute(
                    "INSERT INTO part VALUES ('part', 'message', ?1)",
                    [
                        serde_json::json!({"type": "text", "text": format!("{id} original")})
                            .to_string(),
                    ],
                )
                .unwrap();
            writer
                .execute("INSERT INTO event VALUES ('event-1', ?1)", [id])
                .unwrap();
            writer
        };
        let first_writer = create(&first, "first");
        let second_writer = create(&second, "second");
        let _env = EnvVarGuard::set_os(&[("OPENCODE_DATA_DIR", Some(source.as_os_str()))]);
        let mut options = ingest_options(false, ModelChoice::Gemma);
        options.include_opencode = true;
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let lease = ingest_lease(&paths);
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        assert_eq!(
            ingest_all(&paths, &index, &options, &lease)
                .unwrap()
                .records_added,
            2
        );
        let second_key = second.to_string_lossy().into_owned();
        let prior = IngestState::load(&paths.state.join("ingest.json")).unwrap();
        for (writer, id) in [(&first_writer, "first"), (&second_writer, "second")] {
            writer
                .execute(
                    "UPDATE part SET data = ?1",
                    [
                        serde_json::json!({"type": "text", "text": format!("{id} updated")})
                            .to_string(),
                    ],
                )
                .unwrap();
            writer
                .execute("INSERT INTO event VALUES ('event-2', ?1)", [id])
                .unwrap();
        }
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        let dirty = HashSet::from([source.join("opencode.db-wal")]);
        let result = ingest_dirty(&paths, &index, &options, &lease, &dirty).unwrap();
        assert!(!result.full_scan);
        assert_eq!(result.report.files_scanned, 1);
        assert_eq!(indexed_texts(&paths), ["first updated", "second original"]);
        let updated = IngestState::load(&paths.state.join("ingest.json")).unwrap();
        assert_eq!(
            updated.opencode_databases[&second_key],
            prior.opencode_databases[&second_key]
        );
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        ingest_all(&paths, &index, &options, &lease).unwrap();
        assert_eq!(indexed_texts(&paths), ["first updated", "second updated"]);

        // Ownership changes require a complete inventory, including the
        // unaffected database and any compatible legacy session store.
        first_writer
            .execute("DELETE FROM session WHERE id = 'first'", [])
            .unwrap();
        first_writer
            .execute("INSERT INTO event VALUES ('event-3', 'first')", [])
            .unwrap();
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        let result = ingest_dirty(&paths, &index, &options, &lease, &dirty).unwrap();
        assert!(result.full_scan);
        assert_eq!(indexed_texts(&paths), ["second updated"]);
    }

    #[test]
    fn antigravity_ingest_tracks_wal_updates_and_checkpoint_without_duplicates() {
        let _guard = env_lock();
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("gemini");
        let database = source.join("antigravity-ide/conversations/session.db");
        fs::create_dir_all(database.parent().unwrap()).unwrap();
        let _env = EnvVarGuard::set_os(&[("ANTIGRAVITY_HOME", Some(source.as_os_str()))]);
        let writer = rusqlite::Connection::open(&database).unwrap();
        writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
            CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, status INTEGER, step_payload BLOB);").unwrap();
        // Protobuf user step: field 19 { field 2: text }.
        let put = |text: &str| {
            let mut payload = vec![0x9a, 0x01, (text.len() + 2) as u8, 0x12, text.len() as u8];
            payload.extend_from_slice(text.as_bytes());
            writer
                .execute(
                    "INSERT OR REPLACE INTO steps VALUES (0, 14, 3, ?1)",
                    [payload],
                )
                .unwrap();
        };
        put("original");
        let mut options = ingest_options(false, ModelChoice::Gemma);
        options.include_antigravity = true;
        let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let lease = ingest_lease(&paths);
        let full = || {
            let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
            ingest_all(&paths, &index, &options, &lease).unwrap()
        };
        assert_eq!(full().records_added, 1);
        assert_eq!(indexed_texts(&paths), ["original"]);
        assert_eq!(full().records_added, 0);
        let before = database.metadata().unwrap();
        put("updated");
        assert_eq!(
            database.metadata().unwrap().modified().unwrap(),
            before.modified().unwrap()
        );
        assert_eq!(database.metadata().unwrap().len(), before.len());
        assert!(
            crate::watch::dirty_needs_ingest(&paths, &HashSet::from([database.clone()])).unwrap()
        );
        let wal = database.with_file_name("session.db-wal");
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        let result = ingest_dirty(&paths, &index, &options, &lease, &HashSet::from([wal])).unwrap();
        assert!(!result.full_scan);
        assert_eq!(result.report.records_added, 1);
        assert_eq!(indexed_texts(&paths), ["updated"]);
        assert!(
            !crate::watch::dirty_needs_ingest(&paths, &HashSet::from([database.clone()])).unwrap()
        );
        put("full scan update");
        assert_eq!(full().records_added, 1);
        assert_eq!(indexed_texts(&paths), ["full scan update"]);
        writer
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        full();
        assert_eq!(indexed_texts(&paths), ["full scan update"]);
        drop(writer);
        full();
        assert_eq!(indexed_texts(&paths), ["full scan update"]);
        assert_eq!(full().records_added, 0);
    }

    #[test]
    fn targeted_ingest_codex_history_uses_known_rollouts_without_discovery() {
        let _guard = env_lock();
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("custom-codex");
        let sessions = home.join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let id = "11111111-1111-1111-1111-111111111111";
        let rollout = sessions.join(format!("rollout-{id}.jsonl"));
        fs::write(&rollout, format!("{}\n{}\n",
            serde_json::json!({"type": "session_meta", "payload": {"id": id, "cwd": "/tmp"}}),
            serde_json::json!({"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "rollout original"}]}}),
        )).unwrap();
        let history = home.join("history.jsonl");
        fs::write(
            &history,
            format!(
                "{}\n",
                serde_json::json!({"session_id": id, "text": "duplicate history", "ts": 1})
            ),
        )
        .unwrap();
        let _env = EnvVarGuard::set_os(&[("CODEX_HOME", Some(home.as_os_str()))]);
        let mut options = ingest_options(false, ModelChoice::Gemma);
        options.include_codex = true;
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let lease = ingest_lease(&paths);
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        ingest_all(&paths, &index, &options, &lease).unwrap();
        assert_eq!(indexed_texts(&paths), ["rollout original"]);
        let mut file = fs::OpenOptions::new().append(true).open(&history).unwrap();
        for (session_id, text) in [(id, "duplicate append"), ("history-only", "history only")] {
            writeln!(
                file,
                "{}",
                serde_json::json!({"session_id": session_id, "text": text, "ts": 2})
            )
            .unwrap();
        }
        drop(file);
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        let result =
            ingest_dirty(&paths, &index, &options, &lease, &HashSet::from([history])).unwrap();
        assert!(!result.full_scan);
        assert_eq!(result.report.files_scanned, 1);
        assert_eq!(indexed_texts(&paths), ["history only", "rollout original"]);
    }

    #[test]
    fn exclusion_glob_star_matches_path_separators() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let nested = tmp.path().join("work/deep/project/session.jsonl");
        let patterns = vec![format!("{}/work/**", tmp.path().to_string_lossy())];
        let excluder = PathExcluder::build(&patterns).expect("build excluder");
        assert!(excluder.is_excluded(&nested));
        assert!(!excluder.is_excluded(&tmp.path().join("other/session.jsonl")));
    }

    #[test]
    fn exclusion_invalid_pattern_is_rejected() {
        let result = PathExcluder::build(&["[unclosed".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn exclusion_empty_patterns_match_nothing() {
        let excluder = PathExcluder::build(&[]).expect("build excluder");
        assert!(!excluder.is_excluded(Path::new("/anything/at/all.jsonl")));
    }

    fn save_vector_store(paths: &Paths, model: &str, dimensions: usize) {
        let mut vector = VectorIndex::open_or_create(&paths.vectors, dimensions, Some(model))
            .expect("open vector store");
        vector.add(1, &vec![0.0; dimensions]).expect("add vector");
        vector.save().unwrap();
    }

    fn open_search_index(paths: &Paths) -> SearchIndex {
        fs::create_dir_all(&paths.index).expect("create index dir");
        SearchIndex::open_or_create(&paths.index).expect("open search index")
    }

    fn ingest_lease(paths: &Paths) -> IngestLease {
        IngestLease::acquire(paths, "test ingest", Duration::from_secs(1))
            .expect("acquire ingest lease")
    }

    fn save_search_records(paths: &Paths, records: &[Record]) -> SearchIndex {
        let index = open_search_index(paths);
        let mut writer = index.writer().expect("open index writer");
        for record in records {
            index.add_record(&mut writer, record).expect("add record");
        }
        writer.commit().expect("commit records");
        index
    }

    fn mark_analytics_complete(paths: &Paths) {
        AnalyticsStore::open(analytics_path(&paths.state))
            .expect("open analytics")
            .mark_complete()
            .expect("mark analytics complete");
    }

    fn assert_recovers_cross_store_crash(lexical_advanced: bool) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let claude_root = tmp.path().join("claude-projects");
        let project = claude_root.join("-Users-nico-Code-memex");
        fs::create_dir_all(&project).expect("create project");
        let transcript = project.join("session.jsonl");
        fs::write(
            &transcript,
            r#"{"type":"user","uuid":"original","sessionId":"original-session","timestamp":"2026-08-08T10:00:00Z","message":{"content":"original"}}
"#,
        )
        .expect("write original transcript");

        let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
        paths.ensure_dirs().expect("ensure dirs");
        let mut options = ingest_options(false, ModelChoice::default());
        options.claude_sources = vec![claude_root];
        {
            let index =
                SearchIndex::open_or_create_for_ingest(&paths.index).expect("initial generation");
            let lease = ingest_lease(&paths);
            ingest_all(&paths, &index, &options, &lease).expect("initial ingest");
        }
        save_vector_store(&paths, "unavailable-test-model", 4);
        let untouched_vector_pointer =
            fs::read(paths.vectors.join("current.json")).expect("vector pointer");

        fs::write(
            &transcript,
            r#"{"type":"user","uuid":"recovered","sessionId":"recovered-session","timestamp":"2026-08-08T11:00:00Z","message":{"content":"recovered"}}
"#,
        )
        .expect("write recovered transcript");
        let source_path = transcript.to_string_lossy().to_string();
        PendingIngest {
            next_doc_id: 100,
            source_paths: vec![source_path.clone()],
            session_scopes: Vec::new(),
            vector_publication: false,
        }
        .save(&pending_ingest_path(&paths))
        .expect("save interrupted ingest marker");

        let mut interrupted = record(99, "user", "interrupted");
        interrupted.session_id = "interrupted-session".to_string();
        interrupted.source_path = source_path.clone();
        if lexical_advanced {
            let index = SearchIndex::open_or_create_for_ingest(&paths.index)
                .expect("interrupted lexical generation");
            let mut writer = index.writer().expect("lexical writer");
            index.delete_by_source_path(&mut writer, &source_path);
            index
                .add_record(&mut writer, &interrupted)
                .expect("stage interrupted record");
            writer.commit().expect("commit interrupted lexical state");
            writer.wait_merging_threads().expect("finish lexical write");
            index
                .publish_generation()
                .expect("publish interrupted lexical state");
        } else {
            let mut analytics = AnalyticsWriter::open(analytics_path(&paths.state))
                .expect("interrupted analytics writer");
            analytics
                .delete_source_path(&source_path)
                .expect("delete old analytics row");
            analytics
                .record(&interrupted)
                .expect("stage interrupted analytics row");
            analytics
                .flush()
                .expect("commit interrupted analytics state");
        }

        {
            let index =
                SearchIndex::open_or_create_for_ingest(&paths.index).expect("recovery generation");
            let lease = ingest_lease(&paths);
            ingest_all(&paths, &index, &options, &lease).expect("recover interrupted ingest");
        }

        let index = SearchIndex::open_or_create(&paths.index).expect("published recovery index");
        let mut records = Vec::new();
        index
            .for_each_record(|record| {
                if record.source_path == source_path {
                    records.push(record);
                }
                Ok(())
            })
            .expect("collect recovered records");
        assert_eq!(records.len(), 1, "source path must not be duplicated");
        assert_eq!(records[0].session_id, "session");
        assert_eq!(records[0].text, "recovered");
        assert!(records[0].doc_id >= 100);

        let analytics = AnalyticsStore::open(analytics_path(&paths.state)).expect("analytics");
        let sessions = analytics
            .query_sessions_detailed(None, None, None, None, None)
            .expect("query recovered analytics");
        assert_eq!(sessions.len(), 1, "analytics must not retain stale rows");
        assert_eq!(sessions[0].session_id, "session");
        assert_eq!(sessions[0].source_path, source_path);
        assert_eq!(sessions[0].message_count, 1);

        let state = IngestState::load(&paths.state.join("ingest.json")).expect("ingest state");
        assert!(state.next_doc_id > records[0].doc_id);
        assert!(state.files.contains_key(&source_path));
        assert_eq!(
            PendingIngest::load(&pending_ingest_path(&paths)).expect("pending marker"),
            None
        );
        assert_eq!(
            fs::read(paths.vectors.join("current.json")).expect("untouched vector pointer"),
            untouched_vector_pointer,
            "lexical-only recovery must not initialize or publish vectors"
        );
    }

    #[test]
    fn recovery_reconciles_analytics_commit_before_lexical_publish() {
        assert_recovers_cross_store_crash(false);
    }

    #[test]
    fn recovery_reconciles_lexical_publish_before_analytics_commit() {
        assert_recovers_cross_store_crash(true);
    }

    fn assert_recovers_vector_crash(publish_interrupted_vectors: bool) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let claude_root = tmp.path().join("claude-projects");
        let project = claude_root.join("-Users-nico-Code-memex");
        fs::create_dir_all(&project).expect("create project");
        let transcript = project.join("session.jsonl");
        fs::write(
            &transcript,
            r#"{"type":"user","uuid":"original","sessionId":"session","timestamp":"2026-08-08T10:00:00Z","message":{"content":"original"}}
"#,
        )
        .expect("write original transcript");

        let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
        paths.ensure_dirs().expect("ensure dirs");
        let mut options = ingest_options(true, ModelChoice::Potion);
        options.claude_sources = vec![claude_root];
        {
            let index =
                SearchIndex::open_or_create_for_ingest(&paths.index).expect("initial generation");
            let lease = ingest_lease(&paths);
            ingest_all(&paths, &index, &options, &lease).expect("initial ingest");
        }
        let original_vector_pointer =
            fs::read(paths.vectors.join("current.json")).expect("original vector pointer");
        let original_vectors = VectorIndex::inventory(&paths.vectors)
            .expect("original vector inventory")
            .expect("original vectors");
        assert_eq!(original_vectors.doc_ids.len(), 1);

        fs::write(
            &transcript,
            r#"{"type":"user","uuid":"recovered","sessionId":"session","timestamp":"2026-08-08T11:00:00Z","message":{"content":"recovered"}}
"#,
        )
        .expect("write recovered transcript");
        let source_path = transcript.to_string_lossy().to_string();
        PendingIngest {
            next_doc_id: 100,
            source_paths: vec![source_path.clone()],
            session_scopes: Vec::new(),
            vector_publication: true,
        }
        .save(&pending_ingest_path(&paths))
        .expect("save interrupted ingest marker");

        let mut interrupted = record(99, "user", "interrupted");
        interrupted.session_id = "interrupted-session".to_string();
        interrupted.source_path = source_path.clone();

        // The writer stages vector files before publishing either pointer.
        let mut interrupted_vectors = VectorIndex::open(&paths.vectors).expect("active vectors");
        interrupted_vectors
            .add(99, &vec![0.0; original_vectors.dimensions])
            .expect("add interrupted vector");
        let staged_vectors = interrupted_vectors
            .stage()
            .expect("stage interrupted vectors");
        assert_eq!(
            fs::read_dir(paths.vectors.join("generations"))
                .expect("vector generations")
                .count(),
            2,
            "a process crash can leave one unpublished generation"
        );

        let interrupted_index = SearchIndex::open_or_create_for_ingest(&paths.index)
            .expect("interrupted lexical generation");
        let mut writer = interrupted_index.writer().expect("lexical writer");
        interrupted_index.delete_by_source_path(&mut writer, &source_path);
        interrupted_index
            .add_record(&mut writer, &interrupted)
            .expect("stage interrupted record");
        writer.commit().expect("commit interrupted lexical state");
        writer.wait_merging_threads().expect("finish lexical write");
        interrupted_index
            .publish_generation()
            .expect("publish interrupted lexical state");

        if publish_interrupted_vectors {
            staged_vectors
                .publish()
                .expect("publish interrupted vectors");
            let active = VectorIndex::inventory(&paths.vectors)
                .expect("interrupted vector inventory")
                .expect("interrupted vectors");
            assert!(active.doc_ids.contains(&99));
        } else {
            assert_eq!(
                fs::read(paths.vectors.join("current.json")).expect("active vector pointer"),
                original_vector_pointer,
                "staging vector files must not expose them before vector publication"
            );
            assert_eq!(
                VectorIndex::inventory(&paths.vectors)
                    .expect("active vector inventory")
                    .expect("active vectors")
                    .doc_ids,
                original_vectors.doc_ids
            );
            std::mem::forget(staged_vectors);
        }

        {
            let index =
                SearchIndex::open_or_create_for_ingest(&paths.index).expect("recovery generation");
            let lease = ingest_lease(&paths);
            ingest_all(&paths, &index, &options, &lease).expect("recover interrupted ingest");
        }

        let index = SearchIndex::open_or_create(&paths.index).expect("published recovery index");
        let mut records = Vec::new();
        index
            .for_each_record(|record| {
                if record.source_path == source_path {
                    records.push(record);
                }
                Ok(())
            })
            .expect("collect recovered records");
        assert_eq!(records.len(), 1, "source path must not be duplicated");
        assert_eq!(records[0].text, "recovered");
        assert!(records[0].doc_id >= 100);

        let vectors = VectorIndex::inventory(&paths.vectors)
            .expect("recovered vector inventory")
            .expect("recovered vectors");
        assert_eq!(vectors.doc_ids, HashSet::from([records[0].doc_id]));
        assert!(!vectors.doc_ids.contains(&99));
        assert_eq!(
            fs::read_dir(paths.vectors.join("generations"))
                .expect("recovered vector generations")
                .count(),
            1,
            "successful recovery must collect the unpublished generation"
        );

        let analytics = AnalyticsStore::open(analytics_path(&paths.state)).expect("analytics");
        let sessions = analytics
            .query_sessions_detailed(None, None, None, None, None)
            .expect("query recovered analytics");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].source_path, source_path);
        assert_eq!(sessions[0].message_count, 1);
        assert_eq!(
            PendingIngest::load(&pending_ingest_path(&paths)).expect("pending marker"),
            None
        );
    }

    #[test]
    fn recovery_reconciles_lexical_publish_before_vector_publish() {
        assert_recovers_vector_crash(false);
    }

    #[test]
    fn recovery_removes_vectors_published_before_marker_clear() {
        assert_recovers_vector_crash(true);
    }

    #[test]
    fn vector_only_pending_ingest_is_completed_when_embeddings_are_disabled() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
        paths.ensure_dirs().expect("ensure dirs");
        let index = save_search_records(
            &paths,
            &[record(1, "user", "first"), record(2, "assistant", "second")],
        );
        index
            .publish_generation_if_uninitialized()
            .expect("publish initial lexical generation");
        save_vector_store(&paths, "potion", 256);
        PendingIngest {
            next_doc_id: 3,
            source_paths: Vec::new(),
            session_scopes: Vec::new(),
            vector_publication: true,
        }
        .save(&pending_ingest_path(&paths))
        .expect("save vector-only pending marker");

        let index =
            SearchIndex::open_or_create_for_ingest(&paths.index).expect("recovery generation");
        let lease = ingest_lease(&paths);
        let options = ingest_options(false, ModelChoice::Potion);
        ingest_all(&paths, &index, &options, &lease).expect("finish vector recovery");

        let vectors = VectorIndex::inventory(&paths.vectors)
            .expect("vector inventory")
            .expect("vectors");
        assert_eq!(vectors.doc_ids, HashSet::from([1, 2]));
        assert_eq!(
            PendingIngest::load(&pending_ingest_path(&paths)).expect("pending marker"),
            None
        );
    }

    #[test]
    fn vector_recovery_runs_when_pending_session_scopes_are_present() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
        paths.ensure_dirs().expect("ensure dirs");
        let index = save_search_records(
            &paths,
            &[record(1, "user", "first"), record(2, "assistant", "second")],
        );
        index
            .publish_generation_if_uninitialized()
            .expect("publish initial lexical generation");
        let mut vectors = VectorIndex::open_or_create(&paths.vectors, 256, Some("potion"))
            .expect("create interrupted vectors");
        vectors.add(1, &vec![0.0; 256]).expect("add live vector");
        vectors.add(99, &vec![0.0; 256]).expect("add stale vector");
        vectors.save().expect("save interrupted vectors");
        PendingIngest {
            next_doc_id: 3,
            source_paths: Vec::new(),
            session_scopes: vec![SessionScope {
                source_path: "/unavailable/opencode.db".to_string(),
                session_id: "deferred-session".to_string(),
            }],
            vector_publication: true,
        }
        .save(&pending_ingest_path(&paths))
        .expect("save pending vector publication with deferred scope");

        let index =
            SearchIndex::open_or_create_for_ingest(&paths.index).expect("recovery generation");
        let lease = ingest_lease(&paths);
        let options = ingest_options(false, ModelChoice::Potion);
        ingest_all(&paths, &index, &options, &lease).expect("finish vector recovery");

        let vectors = VectorIndex::inventory(&paths.vectors)
            .expect("vector inventory")
            .expect("vectors");
        assert_eq!(vectors.doc_ids, HashSet::from([1, 2]));
    }

    #[test]
    fn parser_pool_leaves_global_rayon_available_under_backpressure() {
        let parser_pool = build_parser_thread_pool(2).expect("build parser pool");
        let (tx, rx) = bounded::<usize>(1);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let consumer = std::thread::spawn(move || {
            let first = rx.recv().expect("receive first parser result");
            let sum: usize = (0..1_000usize).into_par_iter().sum();
            let count = 1 + rx.iter().count();
            done_tx.send((first, sum, count)).expect("report result");
        });

        parser_pool.install(|| {
            (0..4usize)
                .into_par_iter()
                .for_each(|value| tx.send(value).expect("send parser result"));
        });
        drop(tx);

        let (_first, sum, count) = done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("global Rayon work should not deadlock behind parser backpressure");
        consumer.join().expect("join consumer");
        assert_eq!(sum, (0..1_000usize).sum::<usize>());
        assert_eq!(count, 4);
    }

    fn incremental_task(
        path: &Path,
        source: SourceKind,
        offset: u64,
        turn_id: u32,
        pending_tool_calls: HashMap<String, PendingToolCall>,
    ) -> FileTask {
        let metadata = path.metadata().expect("transcript metadata");
        FileTask {
            path: path.to_path_buf(),
            source,
            offset,
            turn_id,
            size: metadata.len(),
            mtime: metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs() as i64)
                .unwrap_or(0),
            delete_first: false,
            parser_version_invalidated: false,
            pending_tool_calls,
            identity: file_identity(
                path,
                &metadata,
                metadata.len().min(FILE_IDENTITY_PREFIX_BYTES as u64) as usize,
            ),
            parser_version: crate::sources::index_state_version(source),
        }
    }

    fn parser_channels() -> (
        RecordSender,
        Receiver<Record>,
        Sender<FileUpdate>,
        Receiver<FileUpdate>,
    ) {
        let (raw_tx_record, rx_record) = unbounded();
        let (tx_update, rx_update) = unbounded();
        (
            RecordSender::new(raw_tx_record, IndexedToolContentLimits::default()),
            rx_record,
            tx_update,
            rx_update,
        )
    }

    fn record(doc_id: u64, role: &str, text: &str) -> Record {
        Record {
            source: SourceKind::Claude,
            doc_id,
            ts: doc_id,
            project: "project".to_string(),
            session_id: "session".to_string(),
            turn_id: doc_id as u32,
            role: role.to_string(),
            text: text.to_string(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks::default(),
            source_path: format!("source-{doc_id}.jsonl"),
        }
    }

    #[test]
    fn record_channel_applies_backpressure_at_capacity() {
        let (tx_record, _rx_record) = record_channel();
        for doc_id in 0..RECORD_CHANNEL_CAPACITY {
            tx_record
                .try_send(record(doc_id as u64, "assistant", "text"))
                .expect("record within channel capacity");
        }

        let result =
            tx_record.try_send(record(RECORD_CHANNEL_CAPACITY as u64, "assistant", "text"));
        assert!(matches!(
            result,
            Err(crossbeam_channel::TrySendError::Full(_))
        ));
    }

    #[test]
    fn transcript_removed_after_discovery_is_skipped() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("removed.jsonl");
        fs::write(&path, "{}\n").expect("seed transcript");
        let metadata = path.metadata().expect("transcript metadata");
        let (task, skip) =
            prepare_file_task(path.clone(), SourceKind::Claude, false, &metadata, None);
        assert!(!skip);
        fs::remove_file(&path).expect("remove transcript after discovery");
        assert!(
            discovered_metadata(&path)
                .expect("missing metadata should not fail")
                .is_none()
        );

        let (raw_tx_record, _rx_record) = unbounded();
        let tx_record = RecordSender::new(raw_tx_record, IndexedToolContentLimits::default());
        let (tx_update, _rx_update) = unbounded();
        let next_doc_id = AtomicU64::new(1);
        let progress = Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], false));
        let skipped = AtomicUsize::new(0);

        let parse_result = parse_claude_file(
            &task,
            false,
            &tx_record,
            &tx_update,
            &next_doc_id,
            &progress,
        );
        finish_file_task(&task, &progress, &skipped, parse_result)
            .expect("removed transcript should be skipped");

        assert_eq!(skipped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn writer_initialization_error_is_not_masked_by_disconnected_channel() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(temp.path().join("memex"))).expect("paths");
        paths.ensure_dirs().expect("ensure dirs");
        let claude_root = temp.path().join("claude");
        let project = claude_root.join("-tmp-project");
        fs::create_dir_all(&project).expect("create project");
        fs::write(
            project.join("session.jsonl"),
            r#"{"type":"user","uuid":"u1","sessionId":"session","timestamp":"2026-07-26T17:00:00Z","message":{"content":"hello"}}"#,
        )
        .expect("write transcript");
        let index = SearchIndex::open_or_create(&paths.index).expect("index");
        let _existing_writer = index.writer().expect("existing writer");
        let lease = ingest_lease(&paths);
        let mut options = ingest_options(false, ModelChoice::default());
        options.claude_sources = vec![claude_root];

        let error = ingest_all(&paths, &index, &options, &lease).expect_err("writer collision");
        let message = format!("{error:#}");

        assert!(message.contains("failed to initialize the Tantivy index writer"));
        assert!(!message.contains("disconnected channel"));
    }

    #[test]
    fn cancelled_writer_does_not_publish_staged_records() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
        paths.ensure_dirs().expect("ensure dirs");
        let index = save_search_records(&paths, &[record(1, "user", "existing")]);
        backfill_from_index(analytics_path(&paths.state), &index).expect("seed analytics");
        let (tx_record, rx_record) = unbounded();
        tx_record
            .send(record(2, "user", "staged"))
            .expect("send staged record");
        drop(tx_record);
        let (decision_tx, decision_rx) = bounded(1);
        decision_tx.send(WriterDecision::Cancel).expect("cancel");
        let ctx = WriterContext {
            embeddings: false,
            do_backfill_embeddings: false,
            reset_vector_store: false,
            vector_dir: paths.vectors.clone(),
            analytics_path: analytics_path(&paths.state),
            progress: Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], false)),
            model: ModelChoice::default(),
            embed_runtime: EmbedRuntimeConfig::default(),
            tool_content_limits: IndexedToolContentLimits::default(),
            reconcile_vector_ids: false,
            scope_targets: Vec::new(),
            opencode_session_cwds: HashMap::new(),
            vector_delete_paths: HashSet::new(),
        };
        let writer = index.writer().expect("writer");

        let outcome = writer_loop(
            index.clone(),
            writer,
            rx_record,
            decision_rx,
            vec!["source-1.jsonl".to_string()],
            ctx,
        )
        .expect("cancel writer");

        assert_eq!(outcome, WriterOutcome::Cancelled);
        assert_eq!(index.doc_count().expect("document count"), 1);
        let existing = index
            .get_by_doc_id(1)
            .expect("existing lexical lookup")
            .expect("existing lexical record");
        assert_eq!(existing.source_path, "source-1.jsonl");
        let analytics = AnalyticsStore::open(analytics_path(&paths.state)).expect("analytics");
        let sessions = analytics
            .query_sessions_detailed(None, None, None, None, None)
            .expect("analytics sessions");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].source_path, "source-1.jsonl");
    }

    #[test]
    fn parser_cancellation_preserves_active_vectors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
        paths.ensure_dirs().expect("ensure dirs");
        save_vector_store(&paths, "potion", 256);
        let original_pointer =
            fs::read(paths.vectors.join("current.json")).expect("original vector pointer");
        let original_inventory = VectorIndex::inventory(&paths.vectors)
            .expect("original inventory")
            .expect("original vectors");

        let index = open_search_index(&paths);
        let (tx_record, rx_record) = unbounded();
        for offset in 0..EMBED_BATCH_SIZE {
            tx_record
                .send(record(100 + offset as u64, "user", "staged replacement"))
                .expect("send staged embedding record");
        }
        drop(tx_record);
        let (decision_tx, decision_rx) = bounded(1);
        // This is the decision ingest_all sends when any parser fails.
        decision_tx
            .send(WriterDecision::Cancel)
            .expect("cancel after parser failure");
        let ctx = WriterContext {
            embeddings: true,
            do_backfill_embeddings: false,
            reset_vector_store: true,
            vector_dir: paths.vectors.clone(),
            analytics_path: analytics_path(&paths.state),
            progress: Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], true)),
            model: ModelChoice::Potion,
            embed_runtime: EmbedRuntimeConfig::default(),
            tool_content_limits: IndexedToolContentLimits::default(),
            reconcile_vector_ids: false,
            scope_targets: Vec::new(),
            opencode_session_cwds: HashMap::new(),
            vector_delete_paths: HashSet::new(),
        };
        let writer = index.writer().expect("writer");

        let outcome = writer_loop(index, writer, rx_record, decision_rx, Vec::new(), ctx)
            .expect("cancel writer");

        assert_eq!(outcome, WriterOutcome::Cancelled);
        assert_eq!(
            fs::read(paths.vectors.join("current.json")).expect("active vector pointer"),
            original_pointer
        );
        let published = VectorIndex::inventory(&paths.vectors)
            .expect("published inventory")
            .expect("published vectors");
        assert_eq!(published.doc_ids, original_inventory.doc_ids);
        assert_eq!(published.vector_count, original_inventory.vector_count);
    }

    #[test]
    fn record_sender_caps_tool_payloads_but_keeps_plain_text() {
        let limits = IndexedToolContentLimits {
            input_bytes: 1024,
            output_bytes: 2048,
        };
        let plain_text = format!("plain-begin{}plain-end", "w".repeat(4096));
        let plain = record(1, "assistant", &plain_text);

        let mut tool_use = record(
            2,
            "tool_use",
            &format!("input-begin{}input-end", "🦀".repeat(2048)),
        );
        tool_use.tool_input = Some(tool_use.text.clone());
        let mut tool_result = record(
            3,
            "tool_result",
            &format!("output-begin{}output-end", "y".repeat(4096)),
        );
        tool_result.tool_output = Some(tool_result.text.clone());
        let role_only_tool_result = record(
            4,
            "tool_result",
            &format!("role-output-begin{}role-output-end", "z".repeat(4096)),
        );

        let (raw_tx, rx) = unbounded();
        let tx = RecordSender::new(raw_tx, limits);
        tx.send(plain).expect("queue plain record");
        tx.send(tool_use).expect("queue tool-use record");
        tx.send(tool_result).expect("queue tool-result record");
        tx.send(role_only_tool_result)
            .expect("queue role-only tool-result record");
        drop(tx);
        let records = rx.iter().collect::<Vec<_>>();

        assert_eq!(records[0].text, plain_text);
        assert_truncated_content(
            &records[1].text,
            limits.input_bytes,
            "input-begin",
            "input-end",
        );
        assert_truncated_content(
            records[1].tool_input.as_deref().expect("tool input"),
            limits.input_bytes,
            "input-begin",
            "input-end",
        );
        assert_truncated_content(
            &records[2].text,
            limits.output_bytes,
            "output-begin",
            "output-end",
        );
        assert_truncated_content(
            records[2].tool_output.as_deref().expect("tool output"),
            limits.output_bytes,
            "output-begin",
            "output-end",
        );
        assert_truncated_content(
            &records[3].text,
            limits.output_bytes,
            "role-output-begin",
            "role-output-end",
        );
    }

    fn assert_truncated_content(content: &str, max_bytes: usize, prefix: &str, suffix: &str) {
        assert!(content.len() <= max_bytes);
        assert!(content.starts_with(prefix));
        assert!(content.contains("bytes truncated"));
        assert!(content.ends_with(suffix));
    }

    fn fresh_scan_cache() -> ScanCache {
        let last_scan_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_secs();
        ScanCache {
            last_scan_ts,
            file_count: 0,
            total_bytes: 0,
        }
    }

    #[test]
    fn database_races_are_failed_but_successful_inventory_absence_is_confirmed() {
        assert_eq!(
            classify_opencode_database_outcome(None, true, true),
            OpencodeDatabaseOutcome::Failed
        );
        assert_eq!(
            classify_opencode_database_outcome(None, false, true),
            OpencodeDatabaseOutcome::ConfirmedAbsent
        );
        assert_eq!(
            classify_opencode_database_outcome(None, false, false),
            OpencodeDatabaseOutcome::Failed
        );
    }

    #[test]
    fn failed_database_owner_has_priority_over_ready_duplicate() {
        let mut owners = HashMap::new();
        claim_opencode_session_owner(&mut owners, "session".to_string(), "/failed.db");
        claim_opencode_session_owner(&mut owners, "session".to_string(), "/ready.db");
        assert_eq!(
            owners.get("session").map(String::as_str),
            Some("/failed.db")
        );
    }

    #[test]
    fn prepublication_pending_intent_keeps_active_and_deferred_scopes() {
        let active = SessionScope {
            source_path: "/ready.db".to_string(),
            session_id: "active".to_string(),
        };
        let deferred = SessionScope {
            source_path: "/failed.db".to_string(),
            session_id: "deferred".to_string(),
        };
        let scopes = pending_scope_union(
            std::slice::from_ref(&active),
            &[deferred.clone(), active.clone()],
        );
        assert_eq!(scopes, vec![deferred, active]);
    }

    #[test]
    fn pending_session_scopes_retain_database_state_for_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let database_path = temp
            .path()
            .join("opencode.db")
            .to_string_lossy()
            .to_string();
        let scope = SessionScope {
            source_path: database_path.clone(),
            session_id: "session".to_string(),
        };
        let mut state = IngestState::default();
        state.opencode_databases.insert(
            database_path.clone(),
            crate::state::OpencodeDatabaseState {
                owned_session_ids: ["session".to_string()].into_iter().collect(),
                ..Default::default()
            },
        );
        PendingIngest {
            next_doc_id: 1,
            source_paths: Vec::new(),
            session_scopes: vec![scope],
            vector_publication: false,
        }
        .save(&pending_ingest_path(&paths))
        .unwrap();
        prepare_pending_ingest_recovery(&paths, &mut state)
            .unwrap()
            .expect("pending recovery");
        assert!(state.opencode_databases.contains_key(&database_path));

        PendingIngest::clear(&pending_ingest_path(&paths)).unwrap();
        PendingIngest {
            next_doc_id: 1,
            source_paths: vec![database_path.clone()],
            session_scopes: Vec::new(),
            vector_publication: false,
        }
        .save(&pending_ingest_path(&paths))
        .unwrap();
        prepare_pending_ingest_recovery(&paths, &mut state)
            .unwrap()
            .expect("full-path pending recovery");
        assert!(!state.opencode_databases.contains_key(&database_path));
    }

    #[test]
    fn stale_opencode_spools_are_cleaned_without_touching_other_state() {
        let temp = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let stale = paths.state.join(format!("{OPENCODE_SPOOL_PREFIX}stale"));
        let unrelated = paths.state.join("unrelated");
        fs::write(&stale, b"stale").unwrap();
        fs::write(&unrelated, b"keep").unwrap();
        let index = SearchIndex::open_or_create(&paths.index).unwrap();
        ingest_all(
            &paths,
            &index,
            &ingest_options(false, ModelChoice::default()),
            &ingest_lease(&paths),
        )
        .unwrap();
        assert!(!stale.exists());
        assert_eq!(fs::read(unrelated).unwrap(), b"keep");
    }

    #[test]
    fn empty_index_rebuild_persists_cleared_database_state() {
        let temp = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let database_path = temp
            .path()
            .join("opencode.db")
            .to_string_lossy()
            .to_string();
        let mut state = IngestState::default();
        state.opencode_databases.insert(
            database_path,
            crate::state::OpencodeDatabaseState::default(),
        );
        state.save(&paths.state.join("ingest.json")).unwrap();
        let index = SearchIndex::open_or_create(&paths.index).unwrap();
        ingest_all(
            &paths,
            &index,
            &ingest_options(false, ModelChoice::default()),
            &ingest_lease(&paths),
        )
        .unwrap();
        let state = IngestState::load(&paths.state.join("ingest.json")).unwrap();
        assert!(state.opencode_databases.is_empty());
    }

    #[test]
    fn modern_opencode_database_ingests_once_and_skips_noop_hydration() {
        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let db = rusqlite::Connection::open(&db_path).expect("open OpenCode fixture");
        db.execute_batch(
            "CREATE TABLE session (
                id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT,
                time_created INTEGER, time_updated INTEGER
             );
             CREATE TABLE message (
                id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT
             );
             CREATE TABLE part (
                id TEXT PRIMARY KEY, message_id TEXT, data TEXT
             );
             CREATE TABLE event (id TEXT NOT NULL, aggregate_id TEXT NOT NULL);
             INSERT INTO session VALUES ('ses_db-session', NULL, '/tmp', 1, 2);
             INSERT INTO message VALUES ('db-message', 'ses_db-session', 3, '{\"role\":\"assistant\"}');
             INSERT INTO part VALUES ('db-part', 'db-message', '{\"type\":\"text\",\"text\":\"before\"}');
             INSERT INTO event VALUES ('db-event-1', 'ses_db-session');",
        )
        .expect("write OpenCode fixture");
        drop(db);
        let _env = EnvVarGuard::set_os(&[("OPENCODE_DATA_DIR", Some(tmp.path().as_os_str()))]);
        let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
        paths.ensure_dirs().expect("ensure paths");
        let index = open_search_index(&paths);
        let legacy_path = tmp.path().join("storage/message/ses_db-session");
        fs::create_dir_all(&legacy_path).expect("create legacy session");
        let mut legacy_record = record(900, "user", "legacy copy");
        legacy_record.source = SourceKind::Opencode;
        legacy_record.session_id = "ses_db-session".to_string();
        legacy_record.source_path = legacy_path.to_string_lossy().to_string();
        let mut seed_writer = index.writer().expect("open seed writer");
        index
            .add_record(&mut seed_writer, &legacy_record)
            .expect("seed legacy record");
        seed_writer.commit().expect("commit legacy record");
        drop(seed_writer);
        let mut vectors = VectorIndex::open_or_create(&paths.vectors, 4, Some("fixture"))
            .expect("create legacy vector");
        vectors
            .add(legacy_record.doc_id, &[1.0, 0.0, 0.0, 0.0])
            .expect("seed legacy vector");
        vectors.save().expect("save legacy vector");
        let mut options = ingest_options(false, ModelChoice::Gemma);
        options.include_opencode = true;

        let first = ingest_all(&paths, &index, &options, &ingest_lease(&paths));
        assert_eq!(first.expect("initial database ingest").records_added, 1);
        let source_path = db_path.to_string_lossy().to_string();
        let records = index
            .records_by_session_id("ses_db-session")
            .expect("database records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_path, source_path);
        assert_eq!(records[0].text, "before");
        assert!(
            !VectorIndex::inventory(&paths.vectors)
                .expect("inspect legacy vector removal")
                .expect("legacy vectors")
                .doc_ids
                .contains(&legacy_record.doc_id)
        );
        let analytics =
            crate::analytics::AnalyticsStore::open_read_only(analytics_path(&paths.state))
                .expect("open analytics");
        let sessions = analytics
            .query_sessions(
                Some(crate::types::SourceFilter::Opencode),
                None,
                None,
                crate::analytics::ProjectGrouping::Flat,
                None,
            )
            .expect("query analytics");
        assert_eq!(sessions[0].cwd.as_deref(), Some("/tmp"));

        let second = ingest_all(&paths, &index, &options, &ingest_lease(&paths));
        assert_eq!(second.expect("no-op database ingest").records_added, 0);

        let db = rusqlite::Connection::open(&db_path).expect("reopen OpenCode fixture");
        db.execute(
            "UPDATE part SET data = '{\"type\":\"text\",\"text\":\"without event\"}'",
            [],
        )
        .expect("update part without event");
        drop(db);
        let third = ingest_all(&paths, &index, &options, &ingest_lease(&paths));
        assert_eq!(
            third.expect("unchanged event cursor ingest").records_added,
            0
        );
        assert_eq!(
            index.records_by_session_id("ses_db-session").unwrap()[0].text,
            "before"
        );
        let doc_id = index.records_by_session_id("ses_db-session").unwrap()[0].doc_id;
        let mut vectors = VectorIndex::open_or_create(&paths.vectors, 4, Some("fixture"))
            .expect("create fixture vectors");
        vectors
            .add(doc_id, &[1.0, 0.0, 0.0, 0.0])
            .expect("seed fixture vector");
        vectors.save().expect("save fixture vector");

        let db = rusqlite::Connection::open(&db_path).expect("reopen OpenCode fixture");
        db.execute_batch(
            "UPDATE part SET data = '{\"type\":\"text\",\"text\":\"after event\"}';
             INSERT INTO event VALUES ('db-event-2', 'ses_db-session');",
        )
        .expect("update OpenCode fixture");
        drop(db);
        let fourth = ingest_all(&paths, &index, &options, &ingest_lease(&paths));
        assert_eq!(fourth.expect("event database ingest").records_added, 1);
        let records = index
            .records_by_session_id("ses_db-session")
            .expect("updated records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].text, "after event");
        assert!(
            VectorIndex::inventory(&paths.vectors)
                .expect("inspect updated vectors")
                .expect("fixture vectors")
                .doc_ids
                .is_empty()
        );

        fs::remove_dir_all(&legacy_path).expect("remove legacy fallback");
        let unrelated_source = tmp.path().join("claude");
        fs::create_dir_all(&unrelated_source).expect("create unrelated source");
        fs::write(
            unrelated_source.join("unrelated.jsonl"),
            r#"{"type":"user","uuid":"unrelated","sessionId":"unrelated-session","timestamp":"2026-08-08T11:00:00Z","message":{"content":"unrelated source"}}
"#,
        )
        .expect("write unrelated source");
        options.claude_sources = vec![unrelated_source];
        PendingIngest {
            next_doc_id: 1,
            source_paths: Vec::new(),
            session_scopes: vec![SessionScope {
                source_path: source_path.clone(),
                session_id: "ses_db-session".to_string(),
            }],
            vector_publication: false,
        }
        .save(&pending_ingest_path(&paths))
        .expect("save pending database scope");
        fs::write(&db_path, b"corrupt OpenCode database").expect("corrupt OpenCode database");
        ingest_all(&paths, &index, &options, &ingest_lease(&paths))
            .expect("failed database falls back safely");
        assert_eq!(index.doc_count().expect("remaining document count"), 2);
        assert_eq!(
            index.records_by_session_id("ses_db-session").unwrap()[0].text,
            "after event"
        );
        let state = IngestState::load(&paths.state.join("ingest.json")).expect("load ingest state");
        assert!(
            state.opencode_databases.contains_key(&source_path),
            "failed database state must be preserved"
        );
        let pending = PendingIngest::load(&pending_ingest_path(&paths))
            .expect("load deferred scope")
            .expect("deferred scope marker");
        assert_eq!(pending.source_paths, Vec::<String>::new());
        assert_eq!(pending.session_scopes.len(), 1);

        fs::remove_file(&db_path).expect("remove corrupt database");
        let db = rusqlite::Connection::open(&db_path).expect("recreate OpenCode fixture");
        db.execute_batch(
            "CREATE TABLE session (
                id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT,
                time_created INTEGER, time_updated INTEGER
             );
             CREATE TABLE message (
                id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT
             );
             CREATE TABLE part (
                id TEXT PRIMARY KEY, message_id TEXT, data TEXT
             );
             CREATE TABLE event (id TEXT NOT NULL, aggregate_id TEXT NOT NULL);
             INSERT INTO session VALUES ('ses_db-session', NULL, '/recovered', 2, 3);
             INSERT INTO message VALUES ('recovered-message', 'ses_db-session', 4, '{\"role\":\"assistant\"}');
             INSERT INTO part VALUES ('recovered-part', 'recovered-message', '{\"type\":\"text\",\"text\":\"recovered\"}');
             INSERT INTO session VALUES ('ses_reappeared', NULL, '/reappeared', 4, 5);
             INSERT INTO message VALUES ('reappeared-message', 'ses_reappeared', 6, '{\"role\":\"assistant\"}');
             INSERT INTO part VALUES ('reappeared-part', 'reappeared-message', '{\"type\":\"text\",\"text\":\"reappeared\"}');
             INSERT INTO event VALUES ('reappeared-event', 'ses_reappeared');",
        )
        .expect("write reappeared OpenCode fixture");
        drop(db);
        ingest_all(&paths, &index, &options, &ingest_lease(&paths))
            .expect("reappeared database cold hydration");
        let records = index
            .records_by_session_id("ses_reappeared")
            .expect("reappeared database records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].text, "reappeared");
        assert_eq!(
            index.records_by_session_id("ses_db-session").unwrap()[0].text,
            "recovered"
        );
        let state = IngestState::load(&paths.state.join("ingest.json")).expect("load ingest state");
        assert!(state.opencode_databases.contains_key(&source_path));
        assert!(
            PendingIngest::load(&pending_ingest_path(&paths))
                .expect("load resolved scope")
                .is_none()
        );

        fs::remove_file(&db_path).expect("remove unavailable database");
        ingest_all(&paths, &index, &options, &ingest_lease(&paths))
            .expect("confirmed-absent database cleanup");
        assert_eq!(index.doc_count().expect("remaining document count"), 1);
        let state = IngestState::load(&paths.state.join("ingest.json")).expect("load ingest state");
        assert!(!state.opencode_databases.contains_key(&source_path));
    }

    #[test]
    fn cursor_session_id_uses_agent_transcripts_session_directory() {
        let path = Path::new(
            "/Users/nico/.cursor/projects/-Users-nico-Code-memex/agent-transcripts/\
             11111111-1111-1111-1111-111111111111/\
             11111111-1111-1111-1111-111111111111.jsonl",
        );

        assert_eq!(
            crate::sources::cursor::session_id_from_path(path),
            "11111111-1111-1111-1111-111111111111"
        );
    }

    #[test]
    fn cursor_session_id_strips_direct_transcript_extension() {
        let path = Path::new(
            "/Users/nico/.cursor/projects/-Users-nico-Code-memex/agent-transcripts/\
             11111111-1111-1111-1111-111111111111.jsonl",
        );

        assert_eq!(
            crate::sources::cursor::session_id_from_path(path),
            "11111111-1111-1111-1111-111111111111"
        );
    }

    #[test]
    fn cursor_session_id_uses_parent_session_for_subagent_transcripts() {
        let path = Path::new(
            "/Users/nico/.cursor/projects/-Users-nico-Code-memex/agent-transcripts/\
             11111111-1111-1111-1111-111111111111/subagents/\
             22222222-2222-2222-2222-222222222222.jsonl",
        );

        assert_eq!(
            crate::sources::cursor::session_id_from_path(path),
            "11111111-1111-1111-1111-111111111111"
        );
    }

    #[test]
    fn cursor_parent_transcripts_start_at_cached_turn_id() {
        let path = Path::new(
            "/Users/nico/.cursor/projects/-Users-nico-Code-memex/agent-transcripts/\
             11111111-1111-1111-1111-111111111111/\
             11111111-1111-1111-1111-111111111111.jsonl",
        );

        assert_eq!(crate::sources::cursor::initial_turn_id(path, 0), 0);
        assert_eq!(crate::sources::cursor::initial_turn_id(path, 42), 42);
    }

    #[test]
    fn cursor_subagent_transcripts_use_reserved_turn_range() {
        let path = Path::new(
            "/Users/nico/.cursor/projects/-Users-nico-Code-memex/agent-transcripts/\
             11111111-1111-1111-1111-111111111111/subagents/\
             22222222-2222-2222-2222-222222222222.jsonl",
        );

        let initial = crate::sources::cursor::initial_turn_id(path, 0);
        assert!(initial >= 1_000_000_000);
        assert_eq!(
            crate::sources::cursor::initial_turn_id(path, initial + 3),
            initial + 3
        );
    }

    #[test]
    fn cursor_record_links_mark_subagent_parent_session() {
        let path = Path::new(
            "/Users/nico/.cursor/projects/-Users-nico-Code-memex/agent-transcripts/\
             11111111-1111-1111-1111-111111111111/subagents/\
             22222222-2222-2222-2222-222222222222.jsonl",
        );

        let links =
            crate::sources::cursor::record_links(path, "11111111-1111-1111-1111-111111111111", 42);

        assert_eq!(
            links.event_id.as_deref(),
            Some("22222222-2222-2222-2222-222222222222:42")
        );
        assert_eq!(
            links.parent_session_id.as_deref(),
            Some("11111111-1111-1111-1111-111111111111")
        );
        assert_eq!(links.thread_source.as_deref(), Some("subagent"));
        assert_eq!(links.conversation_kind.as_deref(), Some("subagent"));
    }

    #[test]
    fn opencode_session_links_preserve_parent_id() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let project = tmp.path().join("project");
        fs::create_dir_all(&project).expect("create opencode project");
        fs::write(
            project.join("ses_child.json"),
            r#"{"id":"ses_child","parentID":"ses_parent","projectID":"global"}"#,
        )
        .expect("write opencode session");

        let links = crate::sources::opencode::session_links_by_id_from_root(tmp.path())
            .remove("ses_child")
            .expect("child links");

        assert_eq!(links.parent_session_id.as_deref(), Some("ses_parent"));
        assert_eq!(links.thread_source.as_deref(), Some("fork"));
        assert_eq!(links.conversation_kind.as_deref(), Some("fork"));
    }

    #[test]
    fn opencode_session_links_by_id_caches_metadata_tree() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let project = tmp.path().join("project");
        fs::create_dir_all(&project).expect("create opencode project");
        fs::write(
            project.join("ses_child.json"),
            r#"{"id":"ses_child","parentID":"ses_parent","projectID":"global"}"#,
        )
        .expect("write child session");
        fs::write(
            project.join("ses_main.json"),
            r#"{"id":"ses_main","projectID":"global"}"#,
        )
        .expect("write main session");

        let links_by_id = crate::sources::opencode::session_links_by_id_from_root(tmp.path());
        let child_links = links_by_id.get("ses_child").expect("child links");
        let main_links = links_by_id.get("ses_main").expect("main links");

        assert_eq!(links_by_id.len(), 2);
        assert_eq!(child_links.parent_session_id.as_deref(), Some("ses_parent"));
        assert_eq!(child_links.thread_source.as_deref(), Some("fork"));
        assert_eq!(child_links.conversation_kind.as_deref(), Some("fork"));
        assert_eq!(main_links.parent_session_id, None);
        assert_eq!(main_links.thread_source, None);
        assert_eq!(main_links.conversation_kind.as_deref(), Some("main"));
    }

    #[test]
    fn codex_session_meta_preserves_fork_and_subagent_links() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp
            .path()
            .join("rollout-2026-05-22T13-17-11-019e5155-b507-7d83-8c3d-9ecee5f93f12.jsonl");
        fs::write(
            &path,
            r#"{"timestamp":"2026-05-22T20:17:12.595Z","type":"session_meta","payload":{"id":"019e5155-b507-7d83-8c3d-9ecee5f93f12","forked_from_id":"019e5117-c673-7660-b218-af0489416e0f","cwd":"/tmp/project","source":{"subagent":{"thread_spawn":{"parent_thread_id":"019e5117-c673-7660-b218-af0489416e0f","depth":1}}},"thread_source":"subagent"}}"#
                .to_string()
                + "\n",
        )
        .expect("write codex session");

        let meta = crate::sources::codex::probe(&path).expect("read codex meta");

        assert_eq!(
            meta.session.session_id,
            "019e5155-b507-7d83-8c3d-9ecee5f93f12"
        );
        assert_eq!(meta.project.as_deref(), Some("project"));
        assert_eq!(
            meta.session.parent_session_id.as_deref(),
            Some("019e5117-c673-7660-b218-af0489416e0f")
        );
        assert_eq!(
            meta.session.conversation_kind,
            crate::sources::ConversationKind::Subagent
        );
    }

    #[test]
    fn claude_incremental_results_use_persisted_calls_out_of_order() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let project = tmp.path().join("-Users-nico-Code-memex");
        fs::create_dir_all(&project).expect("project dir");
        let path = project.join("claude-incremental.jsonl");
        let calls = concat!(
            "{\"type\":\"assistant\",\"uuid\":\"assistant-1\",\"sessionId\":\"claude-incremental\",\"timestamp\":\"2026-07-20T10:00:00Z\",\"message\":{\"content\":[",
            "{\"type\":\"tool_use\",\"id\":\"call-a\",\"name\":\"Read\",\"input\":{\"path\":\"a\"}},",
            "{\"type\":\"tool_use\",\"id\":\"call-b\",\"name\":\"Grep\",\"input\":{\"pattern\":\"b\"}}]}}\n"
        );
        let results = concat!(
            "{\"type\":\"user\",\"uuid\":\"result-b\",\"parentUuid\":\"assistant-1\",\"sessionId\":\"claude-incremental\",\"timestamp\":\"2026-07-20T10:00:01Z\",\"message\":{\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"call-b\",\"content\":\"B\"}]}}\n",
            "{\"type\":\"user\",\"uuid\":\"result-a\",\"parentUuid\":\"result-b\",\"sessionId\":\"claude-incremental\",\"timestamp\":\"2026-07-20T10:00:02Z\",\"message\":{\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"call-a\",\"content\":\"A\"}]}}\n"
        );
        fs::write(&path, calls).expect("write calls");

        let progress = Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], false));
        let next_doc_id = AtomicU64::new(1);
        let (tx_record, rx_record, tx_update, rx_update) = parser_channels();
        let first = incremental_task(&path, SourceKind::Claude, 0, 0, HashMap::new());
        parse_claude_file(
            &first,
            false,
            &tx_record,
            &tx_update,
            &next_doc_id,
            &progress,
        )
        .expect("parse calls");
        let first_records: Vec<_> = rx_record.try_iter().collect();
        let first_state = rx_update.try_recv().expect("first state").state;
        assert_eq!(first_records.len(), 2);
        assert_eq!(first_state.pending_tool_calls.len(), 2);
        let pending_a = first_state
            .pending_tool_calls
            .get("call-a")
            .expect("pending call a");
        assert_eq!(pending_a.tool_name.as_deref(), Some("Read"));
        assert_eq!(pending_a.tool_use_event_id.as_deref(), Some("call-a"));
        assert_eq!(pending_a.tool_use_doc_id, Some(first_records[0].doc_id));
        assert!(pending_a.argument_sha256.is_some());
        assert!(pending_a.argument_bytes.is_some_and(|bytes| bytes > 0));

        use std::io::Write;
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open append")
            .write_all(results.as_bytes())
            .expect("append results");
        let second = incremental_task(
            &path,
            SourceKind::Claude,
            first_state.offset,
            first_state.turn_id,
            first_state.pending_tool_calls,
        );
        parse_claude_file(
            &second,
            false,
            &tx_record,
            &tx_update,
            &next_doc_id,
            &progress,
        )
        .expect("parse results");
        let second_records: Vec<_> = rx_record.try_iter().collect();
        let second_state = rx_update.try_recv().expect("second state").state;

        assert_eq!(second_records.len(), 2);
        assert_eq!(second_records[0].tool_name.as_deref(), Some("Grep"));
        assert_eq!(
            second_records[0].links.parent_tool_use_id.as_deref(),
            Some("call-b")
        );
        assert_eq!(second_records[1].tool_name.as_deref(), Some("Read"));
        assert_eq!(
            second_records[1].links.parent_event_id.as_deref(),
            Some("call-a")
        );
        assert!(
            second_records
                .iter()
                .all(|record| record.session_id == "claude-incremental"
                    && record.source == SourceKind::Claude
                    && record.source_path == path.to_string_lossy())
        );
        assert!(second_state.pending_tool_calls.is_empty());
    }

    #[test]
    fn codex_incremental_result_uses_persisted_call_metadata() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp
            .path()
            .join("rollout-2026-07-20T10-00-00-11111111-1111-4111-8111-111111111111.jsonl");
        let call = concat!(
            "{\"timestamp\":\"2026-07-20T10:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"11111111-1111-4111-8111-111111111111\",\"cwd\":\"/Users/nico/Code/memex\"}}\n",
            "{\"timestamp\":\"2026-07-20T10:00:01Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"id\":\"fc-item\",\"call_id\":\"call-1\",\"name\":\"shell\",\"arguments\":\"{\\\"cmd\\\":\\\"pwd\\\"}\"}}\n"
        );
        let result = "{\"timestamp\":\"2026-07-20T10:00:02Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"call-1\",\"output\":\"/Users/nico/Code/memex\"}}\n";
        fs::write(&path, call).expect("write call");

        let progress = Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], false));
        let next_doc_id = AtomicU64::new(10);
        let (tx_record, rx_record, tx_update, rx_update) = parser_channels();
        let first = incremental_task(&path, SourceKind::Codex, 0, 0, HashMap::new());
        parse_codex_session(
            &first,
            false,
            &tx_record,
            &tx_update,
            &next_doc_id,
            &progress,
        )
        .expect("parse call");
        let call_record = rx_record.try_recv().expect("call record");
        let first_state = rx_update.try_recv().expect("first state").state;
        assert_eq!(call_record.tool_name.as_deref(), Some("shell"));
        assert_eq!(
            first_state
                .pending_tool_calls
                .get("call-1")
                .and_then(|call| call.tool_use_doc_id),
            Some(call_record.doc_id)
        );

        use std::io::Write;
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open append")
            .write_all(result.as_bytes())
            .expect("append result");
        let second = incremental_task(
            &path,
            SourceKind::Codex,
            first_state.offset,
            first_state.turn_id,
            first_state.pending_tool_calls,
        );
        parse_codex_session(
            &second,
            false,
            &tx_record,
            &tx_update,
            &next_doc_id,
            &progress,
        )
        .expect("parse result");
        let result_record = rx_record.try_recv().expect("result record");
        let second_state = rx_update.try_recv().expect("second state").state;

        assert_eq!(result_record.tool_name.as_deref(), Some("shell"));
        assert_eq!(
            result_record.links.parent_tool_use_id.as_deref(),
            Some("call-1")
        );
        assert_eq!(
            result_record.links.parent_event_id.as_deref(),
            Some("call-1")
        );
        assert_eq!(
            result_record.session_id,
            "11111111-1111-4111-8111-111111111111"
        );
        assert_eq!(result_record.project, "memex");
        assert_eq!(result_record.source, SourceKind::Codex);
        assert_eq!(result_record.source_path, path.to_string_lossy());
        assert!(second_state.pending_tool_calls.is_empty());
    }

    #[test]
    fn truncation_and_replacement_clear_stale_pending_calls() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("session.jsonl");
        fs::write(&path, "original transcript with a pending call\n").expect("write original");
        let metadata = path.metadata().expect("original metadata");
        let (mut original, _) =
            prepare_file_task(path.clone(), SourceKind::Claude, false, &metadata, None);
        original.pending_tool_calls.insert(
            "stale".to_string(),
            PendingToolCall {
                tool_name: Some("StaleTool".to_string()),
                ..PendingToolCall::default()
            },
        );
        let prior = completed_file_state(
            &original,
            metadata.len(),
            1,
            original.pending_tool_calls.clone(),
        );

        fs::write(&path, "short\n").expect("truncate");
        let truncated_meta = path.metadata().expect("truncated metadata");
        let (truncated, skip) = prepare_file_task(
            path.clone(),
            SourceKind::Claude,
            false,
            &truncated_meta,
            Some(&prior),
        );
        assert!(!skip);
        assert!(truncated.delete_first);
        assert_eq!(truncated.offset, 0);
        assert!(truncated.pending_tool_calls.is_empty());

        let replacement = tmp.path().join("replacement.jsonl");
        fs::write(
            &replacement,
            "replacement transcript that is longer than the original\n",
        )
        .expect("write replacement");
        fs::rename(&replacement, &path).expect("replace path");
        let replacement_meta = path.metadata().expect("replacement metadata");
        let (replaced, skip) = prepare_file_task(
            path,
            SourceKind::Claude,
            false,
            &replacement_meta,
            Some(&prior),
        );
        assert!(!skip);
        assert!(replaced.delete_first);
        assert_eq!(replaced.offset, 0);
        assert!(replaced.pending_tool_calls.is_empty());
    }

    #[test]
    fn device_renumbering_preserves_append_continuity() {
        let previous = FileIdentity {
            device: Some(1),
            inode: Some(2),
            prefix_sha256: Some("same".to_string()),
            prefix_bytes: 4,
            modified_ns: Some(3),
            sqlite_wal: None,
        };
        let current = FileIdentity {
            device: Some(4),
            ..previous.clone()
        };

        assert!(!file_was_replaced(&previous, &current));
    }

    #[test]
    fn device_renumbering_does_not_hide_file_replacement() {
        let previous = FileIdentity {
            device: Some(1),
            inode: Some(2),
            prefix_sha256: Some("original".to_string()),
            prefix_bytes: 8,
            modified_ns: Some(3),
            sqlite_wal: None,
        };
        let different_inode = FileIdentity {
            device: Some(4),
            inode: Some(5),
            ..previous.clone()
        };
        let different_prefix = FileIdentity {
            device: Some(4),
            prefix_sha256: Some("replaced".to_string()),
            ..previous.clone()
        };

        assert!(file_was_replaced(&previous, &different_inode));
        assert!(file_was_replaced(&previous, &different_prefix));
    }

    #[test]
    fn short_file_append_preserves_pending_calls_and_offset() {
        use std::io::Write;

        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("session.jsonl");
        fs::write(&path, "tool call\n").expect("write call");
        let metadata = path.metadata().expect("call metadata");
        let (mut first, _) =
            prepare_file_task(path.clone(), SourceKind::Claude, false, &metadata, None);
        first.pending_tool_calls.insert(
            "call-1".to_string(),
            PendingToolCall {
                tool_name: Some("Read".to_string()),
                ..PendingToolCall::default()
            },
        );
        let previous =
            completed_file_state(&first, metadata.len(), 1, first.pending_tool_calls.clone());

        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open append")
            .write_all(b"tool result\n")
            .expect("append result");
        let appended_metadata = path.metadata().expect("appended metadata");
        let (appended, skip) = prepare_file_task(
            path,
            SourceKind::Claude,
            false,
            &appended_metadata,
            Some(&previous),
        );

        assert!(!skip);
        assert!(!appended.delete_first);
        assert_eq!(appended.offset, metadata.len());
        assert_eq!(
            appended
                .pending_tool_calls
                .get("call-1")
                .and_then(|call| call.tool_name.as_deref()),
            Some("Read")
        );
    }

    #[test]
    fn jcode_append_forces_whole_file_replacement() {
        use std::io::Write;

        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("session_test.json");
        fs::write(&path, r#"{"id":"s1","messages":[]}"#).expect("write session");
        let metadata = path.metadata().expect("session metadata");
        let (first, _) = prepare_file_task(path.clone(), SourceKind::Jcode, false, &metadata, None);
        let previous =
            completed_file_state(&first, metadata.len(), 1, first.pending_tool_calls.clone());

        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open append")
            .write_all(br#", "appended": true}"#)
            .expect("append");
        let appended_metadata = path.metadata().expect("appended metadata");
        let (appended, skip) = prepare_file_task(
            path,
            SourceKind::Jcode,
            false,
            &appended_metadata,
            Some(&previous),
        );

        assert!(!skip);
        assert!(appended.delete_first);
        assert_eq!(appended.offset, 0);
        assert!(appended.pending_tool_calls.is_empty());
    }

    #[test]
    fn index_parser_version_change_rebuilds_and_clears_pending_state() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("session.jsonl");
        fs::write(&path, "{}\n").expect("transcript");
        let metadata = path.metadata().expect("metadata");
        let identity = file_identity(
            &path,
            &metadata,
            metadata.len().min(FILE_IDENTITY_PREFIX_BYTES as u64) as usize,
        );
        let previous = FileState {
            size: metadata.len(),
            mtime: metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs() as i64)
                .unwrap_or(0),
            offset: metadata.len(),
            turn_id: 1,
            parser_version: crate::sources::index_state_version(SourceKind::Claude)
                .saturating_sub(1),
            pending_tool_calls: HashMap::from([(
                "stale".to_string(),
                PendingToolCall {
                    tool_name: Some("Old".to_string()),
                    ..PendingToolCall::default()
                },
            )]),
            identity,
        };
        let (task, skip) =
            prepare_file_task(path, SourceKind::Claude, false, &metadata, Some(&previous));
        assert!(!skip);
        assert!(task.delete_first);
        assert!(task.parser_version_invalidated);
        assert_eq!(task.offset, 0);
        assert!(task.pending_tool_calls.is_empty());
    }

    #[test]
    fn grown_jcode_file_reparses_atomically_instead_of_resuming() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("session_jcode.json");
        // Fabricate prior state for a smaller file with identical identity
        // so only the jcode whole-object arm (not replacement detection)
        // can explain atomic reparse.
        fs::write(&path, "A".repeat(5100)).expect("transcript");
        let metadata = path.metadata().expect("metadata");
        let identity = file_identity(
            &path,
            &metadata,
            metadata.len().min(FILE_IDENTITY_PREFIX_BYTES as u64) as usize,
        );
        let version = crate::sources::index_state_version(SourceKind::Jcode);
        let mtime = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or(0);
        let previous = FileState {
            size: metadata.len() - 100,
            mtime,
            offset: metadata.len() - 100,
            turn_id: 3,
            parser_version: version,
            pending_tool_calls: HashMap::new(),
            identity,
        };
        let (task, skip) =
            prepare_file_task(path, SourceKind::Jcode, false, &metadata, Some(&previous));
        // Byte offsets cannot resume a single-JSON-object file: the parser
        // re-emits from message zero, so the stale rows must go first.
        assert!(!skip);
        assert!(task.delete_first);
        assert_eq!(task.offset, 0);
        assert_eq!(task.turn_id, 0);
    }

    #[test]
    fn parser_version_migration_rebuilds_vectors_with_the_existing_model() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        save_vector_store(&paths, "bge", 384);
        let transcript = tmp.path().join("session.jsonl");
        fs::write(&transcript, "{}\n").expect("transcript");
        let mut task = incremental_task(&transcript, SourceKind::Claude, 0, 0, HashMap::new());
        task.parser_version_invalidated = true;

        let migration = vector_migration(&paths.vectors, &[task], ModelChoice::Gemma);

        assert!(migration.rebuild);
        assert_eq!(migration.model, ModelChoice::BGESmall);
    }

    #[test]
    fn ordinary_file_replacement_does_not_rebuild_the_vector_store() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        save_vector_store(&paths, "bge", 384);
        let transcript = tmp.path().join("session.jsonl");
        fs::write(&transcript, "{}\n").expect("transcript");
        let task = incremental_task(&transcript, SourceKind::Claude, 0, 0, HashMap::new());

        let migration = vector_migration(&paths.vectors, &[task], ModelChoice::Gemma);

        assert!(!migration.rebuild);
        assert_eq!(migration.model, ModelChoice::Gemma);
    }

    #[test]
    fn parser_rebuild_keeps_published_vectors_until_replacement_is_saved() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        save_vector_store(&paths, "bge", 384);

        let replacement =
            open_vector_index_for_ingest(&paths.vectors, 384, ModelChoice::BGESmall, true)
                .expect("start replacement");

        assert!(replacement.is_empty());
        assert_eq!(
            crate::vector::VectorIndex::open(&paths.vectors)
                .expect("published vectors")
                .len(),
            1
        );
    }

    #[test]
    fn ingest_claude_records_preserve_sidechain_and_tool_links() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let claude_root = tmp.path().join("claude-projects");
        let project_root = claude_root.join("-Users-nico-Code-memex");
        fs::create_dir_all(&project_root).expect("create claude project");
        let session_file = project_root.join("sess-claude.jsonl");
        fs::write(
            &session_file,
            r#"{"type":"user","uuid":"u1","parentUuid":null,"sessionId":"sess-claude","isSidechain":false,"timestamp":"2026-03-11T01:23:43.844Z","message":{"content":"question"}}
{"type":"assistant","uuid":"a1","parentUuid":"u1","logicalParentUuid":"u0","sessionId":"sess-claude","isSidechain":true,"sourceToolUseID":"source-tool","sourceToolAssistantUUID":"source-assistant","timestamp":"2026-03-11T01:23:44.844Z","message":{"content":[{"type":"text","text":"answer"},{"type":"tool_use","id":"tool-claude","name":"Read","input":{"file_path":"Cargo.toml"}}]}}
{"type":"user","uuid":"r1","parentUuid":"a1","sessionId":"sess-claude","isSidechain":true,"timestamp":"2026-03-11T01:23:45.844Z","message":{"content":[{"type":"tool_result","tool_use_id":"tool-claude","content":"ok"}]}}
"#,
        )
        .expect("write claude fixture");

        let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
        paths.ensure_dirs().expect("ensure dirs");
        let index = SearchIndex::open_or_create(&paths.index).expect("index");
        let options = IngestOptions {
            claude_sources: vec![claude_root],
            exclude_patterns: Vec::new(),
            include_agents: false,
            include_reasoning: false,
            include_codex: false,
            include_opencode: false,
            include_cursor: false,
            include_pi: false,
            include_omp: false,
            include_openclaw: false,
            include_copilot: false,
            include_grok: false,
            include_jcode: false,
            include_muse: false,
            include_antigravity: false,
            embeddings: false,
            backfill_embeddings: false,
            model: ModelChoice::default(),
            embed_runtime: EmbedRuntimeConfig::default(),
            tool_content_limits: IndexedToolContentLimits::default(),
        };

        let lease = ingest_lease(&paths);
        let report = ingest_all(&paths, &index, &options, &lease).expect("ingest");
        assert_eq!(report.records_added, 4);

        let mut records = index
            .records_by_session_id("sess-claude")
            .expect("records by session");
        records.sort_by_key(|record| record.turn_id);

        assert_eq!(records.len(), 4);
        assert_eq!(records[0].role, "user");
        assert_eq!(records[0].links.event_id.as_deref(), Some("u1"));
        assert_eq!(records[0].links.conversation_kind.as_deref(), Some("main"));
        assert_eq!(records[1].role, "tool_use");
        assert_eq!(records[1].links.event_id.as_deref(), Some("tool-claude"));
        assert_eq!(records[1].links.parent_event_id.as_deref(), Some("a1"));
        assert_eq!(
            records[1].links.logical_parent_event_id.as_deref(),
            Some("u0")
        );
        assert_eq!(
            records[1].links.source_tool_use_id.as_deref(),
            Some("source-tool")
        );
        assert_eq!(
            records[1].links.source_tool_assistant_uuid.as_deref(),
            Some("source-assistant")
        );
        assert_eq!(records[1].links.thread_source.as_deref(), Some("sidechain"));
        assert_eq!(
            records[1].links.conversation_kind.as_deref(),
            Some("sidechain")
        );
        assert_eq!(records[2].role, "assistant");
        assert_eq!(records[2].links.event_id.as_deref(), Some("a1"));
        assert_eq!(records[2].links.parent_event_id.as_deref(), Some("u1"));
        assert_eq!(records[2].links.thread_source.as_deref(), Some("sidechain"));
        assert_eq!(
            records[2].links.conversation_kind.as_deref(),
            Some("sidechain")
        );
        assert_eq!(records[3].role, "tool_result");
        assert_eq!(
            records[3].links.event_id.as_deref(),
            Some("r1:tool_result:tool-claude")
        );
        assert_eq!(
            records[3].links.parent_event_id.as_deref(),
            Some("tool-claude")
        );
        assert_eq!(
            records[3].links.parent_tool_use_id.as_deref(),
            Some("tool-claude")
        );
        assert_eq!(records[3].tool_name.as_deref(), Some("Read"));
    }

    #[test]
    fn collect_codex_session_files_includes_archived_sessions() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let sessions_root = tmp.path().join("sessions");
        let archived_root = tmp.path().join("archived_sessions");

        let sessions_day = sessions_root.join("2026/02/11");
        fs::create_dir_all(&sessions_day).expect("create sessions day");
        fs::create_dir_all(archived_root.join("state")).expect("create archived state");

        let live = sessions_day.join("session-live.jsonl");
        let archived = archived_root.join("rollout-archive.jsonl");
        let ignored = archived_root.join("state/ingest.json");

        fs::write(&live, "{}\n").expect("write live");
        fs::write(&archived, "{}\n").expect("write archived");
        fs::write(&ignored, "{}\n").expect("write ignored");

        let files = crate::sources::common::jsonl_files([sessions_root, archived_root]);

        assert_eq!(files, vec![archived, live]);
    }

    #[test]
    fn can_skip_noop_index_when_embeddings_are_disabled() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        let index = open_search_index(&paths);
        let options = ingest_options(false, ModelChoice::BGESmall);

        assert!(can_skip_noop_index(&paths, &index, &options).unwrap());
    }

    #[test]
    fn can_skip_fresh_scan_when_embeddings_are_disabled() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        let index = save_search_records(&paths, &[record(1, "user", "hello")]);
        let options = ingest_options(false, ModelChoice::BGESmall);
        let cache = fresh_scan_cache();
        mark_analytics_complete(&paths);

        assert!(can_skip_fresh_scan(&cache, &paths, &index, &options, 60).unwrap());
    }

    #[test]
    fn can_skip_fresh_scan_with_compatible_vectors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        save_vector_store(&paths, "bge", 384);
        let index = save_search_records(&paths, &[record(1, "user", "hello")]);
        let options = ingest_options(true, ModelChoice::BGESmall);
        let cache = fresh_scan_cache();
        mark_analytics_complete(&paths);

        assert!(can_skip_fresh_scan(&cache, &paths, &index, &options, 60).unwrap());
    }

    #[test]
    fn cannot_skip_fresh_scan_when_vectors_are_missing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        let index = open_search_index(&paths);
        let options = ingest_options(true, ModelChoice::BGESmall);
        let cache = fresh_scan_cache();

        assert!(!can_skip_fresh_scan(&cache, &paths, &index, &options, 60).unwrap());
    }

    #[test]
    fn cannot_skip_fresh_scan_with_pending_ingest() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        let index = save_search_records(&paths, &[record(1, "user", "hello")]);
        let options = ingest_options(false, ModelChoice::BGESmall);
        let cache = fresh_scan_cache();
        mark_analytics_complete(&paths);
        PendingIngest {
            next_doc_id: 2,
            source_paths: vec!["source-1.jsonl".to_string()],
            session_scopes: Vec::new(),
            vector_publication: false,
        }
        .save(&pending_ingest_path(&paths))
        .expect("save pending ingest");

        assert!(!can_skip_fresh_scan(&cache, &paths, &index, &options, 60).unwrap());
    }

    #[test]
    fn database_discovery_error_never_allows_fresh_scan_skip() {
        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let bad_root = tmp.path().join("not-a-directory");
        fs::write(&bad_root, "fixture").expect("write invalid data root");
        let _env = EnvVarGuard::set_os(&[("OPENCODE_DATA_DIR", Some(bad_root.as_os_str()))]);
        let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
        let index = save_search_records(&paths, &[record(1, "user", "hello")]);
        let options = ingest_options(false, ModelChoice::BGESmall);
        let mut options = options;
        options.include_opencode = true;
        mark_analytics_complete(&paths);

        assert!(!can_skip_fresh_scan(&fresh_scan_cache(), &paths, &index, &options, 60).unwrap());
        assert!(ingest_all(&paths, &index, &options, &ingest_lease(&paths)).is_err());
        assert_eq!(index.doc_count().expect("preserved documents"), 1);
    }

    #[test]
    fn cannot_skip_fresh_scan_with_incompatible_vectors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        save_vector_store(&paths, "minilm", 384);
        let index = open_search_index(&paths);
        let options = ingest_options(true, ModelChoice::BGESmall);
        let cache = fresh_scan_cache();

        assert!(!can_skip_fresh_scan(&cache, &paths, &index, &options, 60).unwrap());
    }

    #[test]
    fn cannot_skip_fresh_scan_when_cache_is_stale() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        let index = open_search_index(&paths);
        let options = ingest_options(false, ModelChoice::BGESmall);
        let cache = ScanCache {
            last_scan_ts: 0,
            file_count: 0,
            total_bytes: 0,
        };

        assert!(!can_skip_fresh_scan(&cache, &paths, &index, &options, 60).unwrap());
    }

    #[test]
    fn updating_scan_cache_replaces_malformed_cache() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        paths.ensure_dirs().expect("dirs");
        let cache_path = paths.state.join("scan_cache.json");
        fs::write(&cache_path, "{\"last_scan_ts\":").expect("seed malformed cache");

        update_scan_cache(&paths, 7, 42).expect("update scan cache");

        let cache = ScanCache::load(&cache_path).expect("load replaced cache");
        assert_eq!(cache.file_count, 7);
        assert_eq!(cache.total_bytes, 42);
    }

    #[test]
    fn can_skip_noop_index_with_compatible_vectors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        save_vector_store(&paths, "bge", 384);
        let index = save_search_records(&paths, &[record(1, "user", "hello")]);
        let options = ingest_options(true, ModelChoice::BGESmall);

        assert!(can_skip_noop_index(&paths, &index, &options).unwrap());
    }

    #[test]
    fn cannot_skip_noop_index_with_partial_compatible_vectors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        save_vector_store(&paths, "bge", 384);
        let index = save_search_records(
            &paths,
            &[
                record(1, "user", "embedded"),
                record(2, "assistant", "missing vector"),
            ],
        );
        let options = ingest_options(true, ModelChoice::BGESmall);

        assert!(!can_skip_noop_index(&paths, &index, &options).unwrap());
    }

    #[test]
    fn can_skip_noop_index_ignores_records_that_do_not_need_embeddings() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        save_vector_store(&paths, "bge", 384);
        let index = save_search_records(
            &paths,
            &[
                record(1, "user", "embedded"),
                record(2, "tool_result", "not embedded"),
                record(3, "assistant", ""),
            ],
        );
        let options = ingest_options(true, ModelChoice::BGESmall);

        assert!(can_skip_noop_index(&paths, &index, &options).unwrap());
    }

    #[test]
    fn cannot_skip_noop_index_when_vectors_are_missing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        let index = open_search_index(&paths);
        let options = ingest_options(true, ModelChoice::BGESmall);

        assert!(!can_skip_noop_index(&paths, &index, &options).unwrap());
    }

    #[test]
    fn cannot_skip_noop_index_with_incompatible_vectors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        save_vector_store(&paths, "minilm", 384);
        let index = open_search_index(&paths);
        let options = ingest_options(true, ModelChoice::BGESmall);

        assert!(!can_skip_noop_index(&paths, &index, &options).unwrap());
    }

    #[test]
    fn cannot_skip_noop_index_with_wrong_vector_dimensions() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        save_vector_store(&paths, "bge", 768);
        let index = open_search_index(&paths);
        let options = ingest_options(true, ModelChoice::BGESmall);

        assert!(!can_skip_noop_index(&paths, &index, &options).unwrap());
    }

    #[test]
    fn cannot_skip_noop_index_when_model_dimensions_are_dynamic() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
        save_vector_store(&paths, "potion", 256);
        let index = open_search_index(&paths);
        let options = ingest_options(true, ModelChoice::Potion);

        assert!(!can_skip_noop_index(&paths, &index, &options).unwrap());
    }
    #[test]
    fn collect_pi_files_recurses_under_sessions_root() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let sessions_root = tmp.path().join("sessions");
        let project_root = sessions_root.join("--Users-nico-Code-memex--");
        fs::create_dir_all(&project_root).expect("create pi session dir");

        let session =
            project_root.join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");
        let ignored = project_root.join("notes.json");
        fs::write(&session, "{}\n").expect("write pi session");
        fs::write(&ignored, "{}\n").expect("write ignored");

        let files = crate::sources::common::jsonl_files([sessions_root]);

        assert_eq!(files, vec![session]);
    }

    #[test]
    fn pi_sessions_root_honors_session_dir_override() {
        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let custom_sessions = tmp.path().join("custom-sessions");
        let _env = EnvVarGuard::set_os(&[
            (
                "PI_CODING_AGENT_SESSION_DIR",
                Some(custom_sessions.as_os_str()),
            ),
            ("PI_CODING_AGENT_DIR", None),
        ]);

        assert_eq!(crate::sources::pi::sessions_root(), custom_sessions);
    }

    #[test]
    fn pi_sessions_root_honors_settings_session_dir() {
        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let pi_root = tmp.path().join("pi-agent");
        fs::create_dir_all(&pi_root).expect("create pi root");
        fs::write(
            pi_root.join("settings.json"),
            r#"{ "sessionDir": ".pi/sessions" }"#,
        )
        .expect("write settings");
        let _env = EnvVarGuard::set_os(&[
            ("PI_CODING_AGENT_SESSION_DIR", None),
            ("PI_CODING_AGENT_DIR", Some(pi_root.as_os_str())),
        ]);

        assert_eq!(
            crate::sources::pi::sessions_root(),
            pi_root.join(".pi/sessions")
        );
    }

    #[test]
    fn pi_session_path_fallback_preserves_project_name() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let home_path = tmp
            .path()
            .join("sessions")
            .join("--home-alice-code-memex--")
            .join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");
        let users_path = tmp
            .path()
            .join("sessions")
            .join("--Users-nico-Code-memex--")
            .join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");
        let windows_path = tmp
            .path()
            .join("sessions")
            .join("--C--Users-alice-Code-memex--")
            .join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");
        let nested_path = tmp
            .path()
            .join("sessions")
            .join("--home-alice-code-acme-memex--")
            .join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");

        assert_eq!(crate::sources::pi::project_from_path(&home_path), "memex");
        assert_eq!(crate::sources::pi::project_from_path(&users_path), "memex");
        assert_eq!(
            crate::sources::pi::project_from_path(&windows_path),
            "memex"
        );
        assert_eq!(crate::sources::pi::project_from_path(&nested_path), "memex");
    }

    #[test]
    fn ingest_pi_session_records_supported_message_shapes() {
        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let pi_root = tmp.path().join("pi-agent");
        let omp_root = tmp.path().join("omp");
        let sessions_root = pi_root.join("sessions").join("--Users-nico-Code-memex--");
        fs::create_dir_all(&sessions_root).expect("create pi sessions");
        let session_file =
            sessions_root.join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");
        fs::write(
            &session_file,
            r#"{"type":"session","version":3,"id":"11111111-1111-1111-1111-111111111111","timestamp":"2026-07-03T01:02:03Z","cwd":"/Users/nico/Code/memex"}
{"type":"message","id":"u1","timestamp":"2026-07-03T01:02:04Z","message":{"role":"user","content":[{"type":"text","text":"hello pi"}]}}
{"type":"message","id":"a1","parentId":"u1","timestamp":"2026-07-03T01:02:05Z","message":{"role":"assistant","content":[{"type":"thinking","thinking":"considering options"},{"type":"text","text":"I will run a command"},{"type":"toolCall","id":"tc1","name":"Read","arguments":{"file_path":"README.md"}}]}}
{"type":"message","id":"tr1","parentId":"a1","timestamp":"2026-07-03T01:02:06Z","message":{"role":"toolResult","toolCallId":"tc1","toolName":"Read","content":[{"type":"text","text":"README contents"}],"isError":false}}
{"type":"message","id":"b1","parentId":"tr1","timestamp":"2026-07-03T01:02:07Z","message":{"role":"bashExecution","command":"cargo test","output":"ok","exitCode":0,"cancelled":false,"truncated":false}}
{"type":"message","id":"bh1","parentId":"b1","timestamp":"2026-07-03T01:02:07Z","message":{"role":"bashExecution","command":"echo secret","output":"secret output","exitCode":0,"excludeFromContext":true}}
{"type":"compaction","id":"c1","parentId":"b1","timestamp":"2026-07-03T01:02:08Z","summary":"compacted top-level summary","firstKeptEntryId":"tr1","tokensBefore":50000}
{"type":"branch_summary","id":"br1","parentId":"u1","timestamp":"2026-07-03T01:02:09Z","fromId":"c1","summary":"branch top-level summary"}
{"type":"custom_message","id":"cm1","parentId":"br1","timestamp":"2026-07-03T01:02:10Z","customType":"memex","content":[{"type":"text","text":"extension context"}],"display":true}
{"type":"message","id":"mcs1","parentId":"cm1","timestamp":"2026-07-03T01:02:11Z","message":{"role":"compactionSummary","content":"summary text"}}
{"type":"message","id":"mbs1","parentId":"mcs1","timestamp":"2026-07-03T01:02:12Z","message":{"role":"branchSummary","summary":"message summary text"}}
"#,
        )
        .expect("write pi fixture");
        let _env = EnvVarGuard::set_os(&[
            ("PI_CODING_AGENT_DIR", Some(pi_root.as_os_str())),
            ("PI_CODING_AGENT_SESSION_DIR", None),
            ("PI_CONFIG_DIR", Some(omp_root.as_os_str())),
            ("XDG_DATA_HOME", None),
        ]);

        let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
        paths.ensure_dirs().expect("ensure dirs");
        let index = SearchIndex::open_or_create(&paths.index).expect("index");
        let options = IngestOptions {
            claude_sources: vec![tmp.path().join("missing-claude")],
            exclude_patterns: Vec::new(),
            include_agents: false,
            include_reasoning: false,
            include_codex: false,
            include_opencode: false,
            include_cursor: false,
            include_pi: true,
            include_omp: false,
            include_openclaw: false,
            include_copilot: false,
            include_grok: false,
            include_jcode: false,
            include_muse: false,
            include_antigravity: false,
            embeddings: false,
            backfill_embeddings: false,
            model: ModelChoice::default(),
            embed_runtime: EmbedRuntimeConfig::default(),
            tool_content_limits: IndexedToolContentLimits::default(),
        };

        let lease = ingest_lease(&paths);
        let report = ingest_all(&paths, &index, &options, &lease).expect("ingest");
        assert_eq!(report.records_added, 10);

        let mut records = index
            .records_by_session_id("11111111-1111-1111-1111-111111111111")
            .expect("records by session");
        records.sort_by_key(|record| record.turn_id);

        assert_eq!(records.len(), 10);
        assert!(records.iter().all(|record| record.source == SourceKind::Pi));
        assert!(records.iter().all(|record| record.project == "memex"));
        let source_path = session_file.to_string_lossy().to_string();
        assert!(
            records
                .iter()
                .all(|record| record.source_path == source_path)
        );
        assert_eq!(records[0].role, "user");
        assert_eq!(records[0].text, "hello pi");
        assert_eq!(records[0].links.event_id.as_deref(), Some("u1"));
        assert_eq!(records[0].links.conversation_kind.as_deref(), Some("main"));
        assert_eq!(records[1].role, "tool_use");
        assert_eq!(records[1].tool_name.as_deref(), Some("Read"));
        assert!(records[1].text.contains("README.md"));
        assert_eq!(records[1].links.event_id.as_deref(), Some("tc1"));
        assert_eq!(records[1].links.parent_event_id.as_deref(), Some("a1"));
        assert_eq!(records[2].role, "assistant");
        assert!(records[2].text.contains("I will run a command"));
        assert!(!records[2].text.contains("considering options"));
        assert_eq!(records[2].links.event_id.as_deref(), Some("a1"));
        assert_eq!(records[2].links.parent_event_id.as_deref(), Some("u1"));
        assert_eq!(records[3].role, "tool_result");
        assert_eq!(records[3].tool_name.as_deref(), Some("Read"));
        assert_eq!(records[3].text, "README contents");
        assert_eq!(records[3].links.event_id.as_deref(), Some("tr1"));
        assert_eq!(records[3].links.parent_event_id.as_deref(), Some("a1"));
        assert_eq!(records[3].links.parent_tool_use_id.as_deref(), Some("tc1"));
        assert_eq!(records[4].role, "tool_result");
        assert_eq!(records[4].tool_name.as_deref(), Some("Bash"));
        assert!(records[4].text.contains("$ cargo test"));
        assert!(records[4].text.contains("exit code: 0"));
        assert_eq!(records[5].role, "assistant");
        assert_eq!(records[5].links.event_id.as_deref(), Some("c1"));
        assert_eq!(
            records[5].links.thread_source.as_deref(),
            Some("compaction")
        );
        assert_eq!(
            records[5].links.conversation_kind.as_deref(),
            Some("compaction")
        );
        assert_eq!(records[5].text, "compaction: compacted top-level summary");
        assert_eq!(records[6].role, "assistant");
        assert_eq!(records[6].text, "branch_summary: branch top-level summary");
        assert_eq!(records[6].links.event_id.as_deref(), Some("br1"));
        assert_eq!(records[6].links.parent_event_id.as_deref(), Some("u1"));
        assert_eq!(
            records[6].links.logical_parent_event_id.as_deref(),
            Some("c1")
        );
        assert_eq!(records[6].links.thread_source.as_deref(), Some("branch"));
        assert_eq!(
            records[6].links.conversation_kind.as_deref(),
            Some("branch")
        );
        assert_eq!(records[7].role, "assistant");
        assert_eq!(records[7].text, "custom_message(memex): extension context");
        assert_eq!(records[8].role, "assistant");
        assert_eq!(records[8].text, "compactionSummary: summary text");
        assert_eq!(
            records[8].links.conversation_kind.as_deref(),
            Some("compaction")
        );
        assert_eq!(records[9].role, "assistant");
        assert_eq!(records[9].text, "branchSummary: message summary text");
        assert_eq!(
            records[9].links.conversation_kind.as_deref(),
            Some("branch")
        );
        assert!(!records.iter().any(|record| record.text.contains("secret")));
    }

    #[test]
    fn ingest_omp_session_from_agent_root_override() {
        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let omp_agent_root = tmp.path().join("omp-agent");
        let pi_sessions = tmp.path().join("pi-sessions");
        let session_dir = omp_agent_root
            .join("sessions")
            .join("--Users-nico-Code-omp--");
        fs::create_dir_all(&session_dir).expect("create omp session dir");
        let session_file = session_dir.join("omp-session.jsonl");
        fs::write(
            &session_file,
            include_str!("../fixtures/trajectory_parity/omp.jsonl"),
        )
        .expect("write omp fixture");
        let _env = EnvVarGuard::set_os(&[
            ("PI_CONFIG_DIR", None),
            ("PI_CODING_AGENT_SESSION_DIR", Some(pi_sessions.as_os_str())),
            ("PI_CODING_AGENT_DIR", Some(omp_agent_root.as_os_str())),
            ("XDG_DATA_HOME", None),
        ]);

        let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
        paths.ensure_dirs().expect("ensure dirs");
        let index = SearchIndex::open_or_create(&paths.index).expect("index");
        let mut options = ingest_options(false, ModelChoice::default());
        options.include_omp = true;
        let lease = ingest_lease(&paths);
        let report = ingest_all(&paths, &index, &options, &lease).expect("ingest");
        assert_eq!(report.files_scanned, 1);
        assert_eq!(report.records_added, 4);

        let records = index
            .records_by_session_id("omp-session")
            .expect("records by session");
        assert_eq!(records.len(), 4);
        assert!(
            records
                .iter()
                .all(|record| record.source == SourceKind::Omp)
        );
        assert!(records.iter().all(|record| record.project == "omp-project"));
        assert!(
            records
                .iter()
                .any(|record| record.text == "Inspect the project")
        );
        assert!(
            records
                .iter()
                .any(|record| record.text == "project contents")
        );
        assert!(
            records
                .iter()
                .all(|record| record.source_path == session_file.to_string_lossy())
        );
    }

    #[test]
    fn ingest_grok_session_from_grok_home_override() {
        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let grok_home = tmp.path().join("grok-home");
        let session_dir = grok_home
            .join("sessions")
            .join("%2Fworkspace%2Fgrok-project")
            .join("grok-session");
        fs::create_dir_all(&session_dir).expect("create grok session dir");
        fs::write(
            session_dir.join("summary.json"),
            r#"{"info":{"id":"grok-session","cwd":"/workspace/grok-project"},"git_root_dir":"/workspace/grok-project/"}"#,
        )
        .expect("write grok summary");
        let session_file = session_dir.join("updates.jsonl");
        fs::write(
            &session_file,
            include_str!("../fixtures/trajectory_parity/grok.jsonl"),
        )
        .expect("write grok fixture");
        let _env = EnvVarGuard::set_os(&[("GROK_HOME", Some(grok_home.as_os_str()))]);

        let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
        paths.ensure_dirs().expect("ensure dirs");
        let index = SearchIndex::open_or_create(&paths.index).expect("index");
        let mut options = ingest_options(false, ModelChoice::default());
        options.include_grok = true;
        let lease = ingest_lease(&paths);
        let report = ingest_all(&paths, &index, &options, &lease).expect("ingest");
        assert_eq!(report.files_scanned, 1);
        // Reasoning is off, the pending tool update and the unknown event are skipped.
        assert_eq!(report.records_added, 5);

        let records = index
            .records_by_session_id("grok-session")
            .expect("records by session");
        assert_eq!(records.len(), 5);
        assert!(
            records
                .iter()
                .all(|record| record.source == SourceKind::Grok)
        );
        assert!(
            records
                .iter()
                .all(|record| record.project == "grok-project")
        );
        assert!(!records.iter().any(|record| record.role == "reasoning"));
        assert!(
            records
                .iter()
                .any(|record| record.text == "Inspect the project")
        );
        assert!(
            records
                .iter()
                .any(|record| record.text == "project contents")
        );
        assert!(
            records
                .iter()
                .any(|record| record.tool_name.as_deref() == Some("read_file")
                    && record.role == "tool_use")
        );
        assert!(records.iter().any(|record| {
            record.links.conversation_kind.as_deref() == Some("compaction")
                && record.text.starts_with("session_recap: Read the README")
        }));
        assert!(
            records
                .iter()
                .all(|record| record.source_path == session_file.to_string_lossy())
        );
    }

    #[test]
    fn ingest_pi_incremental_records_keep_header_project() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let sessions_root = tmp
            .path()
            .join("sessions")
            .join("--home-alice-code-my-project--");
        fs::create_dir_all(&sessions_root).expect("create pi sessions");
        let session_file =
            sessions_root.join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");
        let existing = r#"{"type":"session","version":3,"id":"22222222-2222-2222-2222-222222222222","timestamp":"2026-07-03T01:02:03Z","cwd":"/home/alice/code/my-project"}
{"type":"message","id":"u1","timestamp":"2026-07-03T01:02:04Z","message":{"role":"user","content":"first"}}
"#;
        let appended = r#"{"type":"message","id":"a1","timestamp":"2026-07-03T01:02:05Z","message":{"role":"assistant","content":"second"}}
"#;
        fs::write(&session_file, format!("{existing}{appended}")).expect("write pi fixture");

        let (raw_tx_record, rx_record) = unbounded();
        let tx_record = RecordSender::new(raw_tx_record, IndexedToolContentLimits::default());
        let (tx_update, _rx_update) = unbounded();
        let task = FileTask {
            path: session_file,
            source: SourceKind::Pi,
            offset: existing.len() as u64,
            turn_id: 1,
            size: (existing.len() + appended.len()) as u64,
            mtime: 0,
            delete_first: false,
            parser_version_invalidated: false,
            pending_tool_calls: HashMap::new(),
            identity: FileIdentity::default(),
            parser_version: crate::sources::index_state_version(SourceKind::Pi),
        };
        let progress = Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], false));
        let next_doc_id = AtomicU64::new(1);

        parse_pi_file(
            &task,
            false,
            &tx_record,
            &tx_update,
            &next_doc_id,
            &progress,
        )
        .expect("parse pi");
        drop(tx_record);
        let records: Vec<_> = rx_record.try_iter().collect();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].project, "my-project");
        assert_eq!(records[0].text, "second");
    }
    #[test]
    fn collect_copilot_files_finds_session_events() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let session_dir = tmp
            .path()
            .join("session-state")
            .join("11111111-1111-4111-8111-111111111111");
        fs::create_dir_all(&session_dir).expect("create session dir");

        let events = session_dir.join("events.jsonl");
        let ignored = session_dir.join("workspace.yaml");
        fs::write(&events, "{}\n").expect("write events");
        fs::write(&ignored, "cwd: /tmp/project\n").expect("write workspace");

        let files =
            crate::sources::copilot::discover_sessions_from_root(&tmp.path().join("session-state"))
                .into_iter()
                .map(|file| file.path)
                .collect::<Vec<_>>();

        assert_eq!(files, vec![events]);
    }

    #[test]
    fn parse_copilot_session_extracts_messages_tools_and_workspace() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let session_id = "11111111-1111-4111-8111-111111111111";
        let session_dir = tmp.path().join("session-state").join(session_id);
        fs::create_dir_all(&session_dir).expect("create session dir");
        fs::write(
            session_dir.join("workspace.yaml"),
            "cwd: /Users/nico/Code/memex\ngitRoot: /Users/nico/Code/memex\nrepository: nicosuave/memex\nbranch: main\n",
        )
        .expect("write workspace");
        let events = session_dir.join("events.jsonl");
        fs::write(
            &events,
            concat!(
                "{\"type\":\"session.start\",\"timestamp\":\"2026-06-01T12:00:00Z\",\"data\":{\"sessionId\":\"11111111-1111-4111-8111-111111111111\",\"context\":{\"cwd\":\"/Users/nico/Code/memex\",\"repository\":\"nicosuave/memex\"}}}\n",
                "{\"type\":\"user.message\",\"timestamp\":\"2026-06-01T12:00:01Z\",\"data\":{\"content\":\"Find the parser\"}}\n",
                "{\"type\":\"assistant.message\",\"timestamp\":\"2026-06-01T12:00:02Z\",\"data\":{\"content\":\"I will inspect ingestion.\"}}\n",
                "{\"type\":\"tool.execution_start\",\"timestamp\":\"2026-06-01T12:00:03Z\",\"data\":{\"toolCallId\":\"call-1\",\"toolName\":\"grep\",\"arguments\":{\"pattern\":\"parse_copilot\"}}}\n",
                "{\"type\":\"tool.execution_complete\",\"timestamp\":\"2026-06-01T12:00:04Z\",\"data\":{\"toolCallId\":\"call-1\",\"success\":true,\"result\":{\"content\":\"src/ingest.rs\"}}}\n"
            ),
        )
        .expect("write events");
        let meta = events.metadata().expect("metadata");
        let task = FileTask {
            path: events.clone(),
            source: SourceKind::Copilot,
            offset: 0,
            turn_id: 0,
            size: meta.len(),
            mtime: 0,
            delete_first: false,
            parser_version_invalidated: false,
            pending_tool_calls: HashMap::new(),
            identity: FileIdentity::default(),
            parser_version: crate::sources::index_state_version(SourceKind::Copilot),
        };
        let (raw_tx_record, rx_record) = unbounded();
        let tx_record = RecordSender::new(raw_tx_record, IndexedToolContentLimits::default());
        let (tx_update, rx_update) = unbounded();
        let next_doc_id = AtomicU64::new(1);
        let mut total_bytes = [0; SOURCE_COUNT];
        total_bytes[SourceKind::Copilot.idx()] = meta.len();
        let mut files_total = [0; SOURCE_COUNT];
        files_total[SourceKind::Copilot.idx()] = 1;
        let progress = Arc::new(Progress::new(total_bytes, files_total, false));

        parse_copilot_session(&task, &tx_record, &tx_update, &next_doc_id, &progress)
            .expect("parse copilot session");
        drop(tx_record);
        drop(tx_update);

        let records: Vec<Record> = rx_record.try_iter().collect();
        assert_eq!(records.len(), 4);
        assert!(records.iter().all(|r| r.source == SourceKind::Copilot));
        assert!(records.iter().all(|r| r.project == "memex"));
        assert!(records.iter().all(|r| r.session_id == session_id));
        assert_eq!(records[0].role, "user");
        assert_eq!(
            records[0].links.event_id.as_deref(),
            Some("11111111-1111-4111-8111-111111111111:0")
        );
        assert_eq!(records[0].links.conversation_kind.as_deref(), Some("main"));
        assert_eq!(records[1].role, "assistant");
        assert_eq!(
            records[1].links.event_id.as_deref(),
            Some("11111111-1111-4111-8111-111111111111:1")
        );
        assert_eq!(records[2].role, "tool_use");
        assert_eq!(records[2].tool_name.as_deref(), Some("grep"));
        assert!(records[2].text.contains("parse_copilot"));
        assert_eq!(records[2].links.event_id.as_deref(), Some("call-1"));
        assert_eq!(records[3].role, "tool_result");
        assert_eq!(records[3].tool_name.as_deref(), Some("grep"));
        assert_eq!(records[3].tool_output.as_deref(), Some("src/ingest.rs"));
        assert_eq!(records[3].links.parent_event_id.as_deref(), Some("call-1"));
        assert_eq!(
            records[3].links.parent_tool_use_id.as_deref(),
            Some("call-1")
        );

        let update = rx_update.try_recv().expect("file update");
        assert_eq!(update.state.offset, meta.len());
        assert_eq!(update.state.turn_id, 4);
    }

    #[test]
    fn writer_loop_accepts_copilot_source_progress() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let index_dir = tmp.path().join("index");
        let vector_dir = tmp.path().join("vectors");
        fs::create_dir_all(&index_dir).expect("create index dir");
        fs::create_dir_all(&vector_dir).expect("create vector dir");
        let index = SearchIndex::open_or_create(&index_dir).expect("open index");
        let (tx_record, rx_record) = unbounded();
        tx_record
            .send(Record {
                source: SourceKind::Copilot,
                doc_id: 1,
                ts: 1_780_291_200_000,
                project: "memex".to_string(),
                session_id: "11111111-1111-4111-8111-111111111111".to_string(),
                turn_id: 0,
                role: "user".to_string(),
                text: "Find the parser".to_string(),
                tool_name: None,
                tool_input: None,
                tool_output: None,
                links: RecordLinks::default(),
                source_path: tmp
                    .path()
                    .join(
                        ".copilot/session-state/11111111-1111-4111-8111-111111111111/events.jsonl",
                    )
                    .to_string_lossy()
                    .to_string(),
            })
            .expect("send record");
        drop(tx_record);

        let progress = Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], false));
        let ctx = WriterContext {
            embeddings: false,
            do_backfill_embeddings: false,
            reset_vector_store: false,
            vector_dir,
            analytics_path: tmp.path().join("state").join("analytics.sqlite"),
            progress,
            model: ModelChoice::default(),
            embed_runtime: EmbedRuntimeConfig::default(),
            tool_content_limits: IndexedToolContentLimits::default(),
            reconcile_vector_ids: false,
            scope_targets: Vec::new(),
            opencode_session_cwds: HashMap::new(),
            vector_delete_paths: HashSet::new(),
        };

        let writer = index.writer().expect("open writer");
        let (decision_tx, decision_rx) = bounded(1);
        decision_tx.send(WriterDecision::Commit).expect("commit");
        let outcome = writer_loop(index, writer, rx_record, decision_rx, Vec::new(), ctx)
            .expect("write copilot record");
        let WriterOutcome::Published {
            records_added,
            records_embedded,
        } = outcome
        else {
            panic!("writer cancelled")
        };

        assert_eq!(records_added, 1);
        assert_eq!(records_embedded, 0);
    }
}
