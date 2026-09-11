mod discovery;
mod execution;
mod plan;
mod publication;
mod selection;

pub(crate) use discovery::{PathExcluder, build_path_excluder};
use discovery::{
    can_skip_fresh_scan, can_skip_noop_index, is_not_found, vector_index_covers_embeddable_records,
    vector_migration,
};
use execution::{
    cleanup_opencode_spools, flush_embeddings, is_embedding_role, limit_record_tool_content,
    parser_thread_pool, prehydrate_opencode_database, refresh_memories, truncate_for_embedding,
};
use publication::{
    finalize_pending_ingest, pending_ingest_path, pending_scope_union, update_scan_cache,
    writer_loop,
};

use crate::analytics::{
    AnalyticsStore, AnalyticsWriter, analytics_path, backfill_from_index_with_repositories,
};
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
#[cfg(test)]
use plan::file_was_replaced;
use plan::{FileChange, claim_opencode_session_owner, classify_opencode_database_outcome};
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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
    legacy_turn_id: Option<u32>,
    size: u64,
    mtime: i64,
    change: FileChange,
    pending_tool_calls: HashMap<String, PendingToolCall>,
    identity: FileIdentity,
    parser_version: u32,
    codex_metadata_offsets: Option<Vec<u64>>,
    claude_background: Option<bool>,
}

impl FileTask {
    fn delete_first(&self) -> bool {
        self.change.replaces_records()
    }
    fn parser_version_invalidated(&self) -> bool {
        self.change == FileChange::ParserChanged
    }
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

struct WriterContext {
    index_root: PathBuf,
    input_bytes: Option<u64>,
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
    repositories: Arc<crate::repository::RepositoryResolver>,
    codex_metadata_checkpoints: HashMap<String, (u64, Vec<u64>)>,
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
    CheckpointsOnly,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VectorMigration {
    rebuild: bool,
    model: ModelChoice,
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
    crate::profiling::span!("ingest.freshness");
    let cache_path = paths.state.join("scan_cache.json");
    let cache = ScanCache::load(&cache_path)?;

    if can_skip_fresh_scan(&cache, paths, index, options, ttl_seconds)? {
        crate::profiling::count!("ingest.fresh_cache_hits", 1);
        // The transcript scan cache cannot detect edits in a Markdown memory
        // file. Refresh these small documents even when transcript discovery is
        // still within its TTL, under the same ingestion lease.
        refresh_memories(
            paths,
            options,
            &crate::repository::RepositoryResolver::default(),
        )?;
        return Ok(None);
    }

    crate::profiling::count!("ingest.fresh_cache_misses", 1);
    let report = ingest_all(paths, index, options, lease)?;
    Ok(Some(report))
}

pub fn ingest_all(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    lease: &IngestLease,
) -> Result<IngestReport> {
    ingest_selected(paths, index, options, lease, None).map(|result| result.report)
}

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
    lease: &IngestLease,
    dirty: Option<&HashSet<PathBuf>>,
) -> Result<DirtyIngestReport> {
    crate::profiling::span!("ingest.all");
    let repositories = Arc::new(crate::repository::RepositoryResolver::default());
    let pool = parser_thread_pool()?;
    let recovered = publication::recover_checkpoint(paths, index, lease)?;
    let prepared = discovery::prepare_refresh(paths, index, options, &pool, recovered, dirty)?;
    let full_scan = prepared.full_scan;
    if full_scan {
        refresh_memories(paths, options, &repositories)?;
    }
    let report = execution::execute_refresh(prepared, paths, index, options, repositories, &pool)?;
    Ok(DirtyIngestReport { report, full_scan })
}

#[cfg(test)]
mod tests;
