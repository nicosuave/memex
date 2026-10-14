mod checkpoint;
pub mod directories;
pub(crate) mod discovery;
mod execution;
pub mod journal;
mod plan;
mod publication;
mod selection;

use checkpoint::CheckpointSession;
pub(crate) use discovery::{PathExcluder, build_path_excluder};
use discovery::{
    can_skip_fresh_scan, can_skip_noop_index, is_not_found, record_needs_embedding,
    vector_index_covers_embeddable_records, vector_migration,
};
use execution::{
    cleanup_opencode_spools, flush_embeddings, is_embedding_role, limit_record_tool_content,
    parser_thread_pool, prehydrate_opencode_database, refresh_memories, truncate_for_embedding,
};
#[cfg(test)]
use publication::pending_ingest_path;
use publication::{finalized_pending_ingest, pending_scope_union, updated_scan_cache, writer_loop};

use crate::analytics::{
    AnalyticsStore, AnalyticsWriter, analytics_path, backfill_from_index_with_repositories,
};
use crate::config::{IndexedToolContentLimits, Paths};
use crate::embed::{EmbedRuntimeConfig, EmbedderHandle, ModelChoice};
use crate::index::SearchIndex;
use crate::lease::IngestLease;
use crate::progress::{Progress, SOURCE_COUNT};
use crate::state::checkpoint::{CheckpointHeader, CheckpointReader, PendingChange};
use crate::state::{
    FileIdentity, FileState, PendingIngest, PendingToolCall, ScanCache, SessionScope,
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
    pub include_hermes: bool,
    pub include_jcode: bool,
    pub include_muse: bool,
    pub include_antigravity: bool,
    pub include_bob: bool,
    pub include_zcode: bool,
    pub include_kilocode: bool,
    pub include_kiro: bool,
    pub include_kimi: bool,
    pub exclude_patterns: Vec<String>,
    pub embeddings: bool,
    pub backfill_embeddings: bool,
    pub model: ModelChoice,
    pub embed_runtime: EmbedRuntimeConfig,
    pub tool_content_limits: IndexedToolContentLimits,
    /// Search-triggered refreshes append without foreground merges; compaction is scheduled
    /// separately once segments accumulate.
    pub defer_merges: bool,
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
    claude_agent_copies: Vec<crate::sources::claude::AgentCopy>,
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
    source: SourceKind,
    session_cwd: Option<String>,
}

/// A session's working directory as its transcript recorded it, handed to analytics so it
/// never re-reads the transcript to find it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SessionCwd {
    source: SourceKind,
    source_path: String,
    session_id: String,
    cwd: String,
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
    defer_merges: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WriterDecision {
    Commit { session_cwds: Vec<SessionCwd> },
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct VectorMigration {
    rebuild: bool,
    model: ModelChoice,
}

/// How far a read may trail the transcripts on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    /// Reads serve the index as it stands.
    None,
    /// Reads take in what the journal names as changed. When it cannot name the changes, the
    /// index is walked only once the last walk is older than `ttl_seconds`.
    Bounded { ttl_seconds: u64 },
    /// Reads take in what the journal names as changed, and walk whenever it cannot.
    Strict,
}

/// Bring the index up to date for a read, as far as `freshness` asks. Returns Ok(None) when
/// nothing was refreshed, Ok(Some(report)) when a refresh ran.
pub fn ingest_if_stale(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    freshness: Freshness,
    lease: &IngestLease,
    journal: Option<journal::ReplayHandle>,
) -> Result<Option<IngestReport>> {
    crate::profiling::span!("ingest.freshness");
    let header = CheckpointReader::open(&paths.state.join("ingest.json"))?.header()?;
    let walk = match freshness {
        Freshness::None => return Ok(None),
        Freshness::Strict => true,
        Freshness::Bounded { ttl_seconds } => {
            !can_skip_fresh_scan(&header, paths, index, options, ttl_seconds)?
        }
    };
    if !walk && journal.is_none() {
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
    let narrowing = match journal {
        Some(replay) => discovery::Narrowing::Journal { replay, walk },
        None => discovery::Narrowing::None,
    };
    let report = ingest_selected(paths, index, options, lease, narrowing, Some(header))?.report;
    Ok(Some(report))
}

/// The transcripts among `sources` whose size, identity or timestamps moved since the
/// checkpoint last recorded them, or that it has never recorded. A path that cannot be
/// stat'ed is left to the refreshes that discover sources.
pub(crate) fn changed_sources<'a>(
    paths: &Paths,
    sources: impl IntoIterator<Item = &'a str>,
) -> Result<HashSet<PathBuf>> {
    let sources: Vec<String> = sources
        .into_iter()
        .filter(|source| std::fs::metadata(source).is_ok_and(|metadata| metadata.is_file()))
        .map(str::to_owned)
        .collect();
    let state_path = paths.state.join("ingest.json");
    if sources.is_empty() || !state_path.exists() {
        return Ok(sources.into_iter().map(PathBuf::from).collect());
    }
    let recorded = CheckpointReader::open(&state_path)?
        .load_files(&sources, crate::state::checkpoint::FileLoadScope::Targeted)?;
    Ok(sources
        .into_iter()
        .filter(|source| {
            let current = std::fs::metadata(source);
            !matches!(
                (recorded.get(source), current),
                (Some(Some(previous)), Ok(metadata))
                    if discovery::unchanged_file_metadata(
                        previous,
                        &metadata,
                        previous.parser_version,
                    )
            )
        })
        .map(PathBuf::from)
        .collect())
}

pub fn ingest_all(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    lease: &IngestLease,
) -> Result<IngestReport> {
    ingest_selected(
        paths,
        index,
        options,
        lease,
        discovery::Narrowing::None,
        None,
    )
    .map(|result| result.report)
}

pub(crate) fn ingest_dirty(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    lease: &IngestLease,
    dirty: &HashSet<PathBuf>,
) -> Result<DirtyIngestReport> {
    ingest_selected(
        paths,
        index,
        options,
        lease,
        discovery::Narrowing::Dirty(dirty),
        None,
    )
}

fn ingest_selected(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    lease: &IngestLease,
    narrowing: discovery::Narrowing<'_>,
    checkpoint_header: Option<CheckpointHeader>,
) -> Result<DirtyIngestReport> {
    crate::profiling::span!("ingest.all");
    let repositories = Arc::new(crate::repository::RepositoryResolver::default());
    let dirty = matches!(narrowing, discovery::Narrowing::Dirty(_));
    let pool = parser_thread_pool()?;
    let recovered = publication::recover_checkpoint(paths, index, lease, checkpoint_header)?;
    if !dirty {
        refresh_memories(paths, options, &repositories)?;
    }
    let prepared =
        discovery::prepare_refresh(paths, index, options, &pool, recovered, narrowing, None)?;
    let full_scan = prepared.full_scan;
    if full_scan && dirty {
        refresh_memories(paths, options, &repositories)?;
    }
    let report = execution::execute_refresh(prepared, paths, index, options, repositories, &pool)?;
    Ok(DirtyIngestReport { report, full_scan })
}

#[cfg(test)]
mod tests;
