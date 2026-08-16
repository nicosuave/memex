use crate::analytics::{AnalyticsStore, AnalyticsWriter, analytics_path, backfill_from_index};
use crate::config::{IndexedToolContentLimits, Paths};
use crate::embed::{EmbedRuntimeConfig, EmbedderHandle, ModelChoice};
use crate::index::SearchIndex;
use crate::lease::IngestLease;
use crate::progress::{Progress, SOURCE_COUNT};
use crate::state::{FileIdentity, FileState, IngestState, PendingToolCall, ScanCache};
#[cfg(test)]
use crate::types::RecordLinks;
use crate::types::{Record, SourceKind};
use anyhow::{Context, Result, anyhow};
use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
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
    pub claude_source: PathBuf,
    pub include_agents: bool,
    pub include_reasoning: bool,
    pub include_codex: bool,
    pub include_opencode: bool,
    pub include_cursor: bool,
    pub include_pi: bool,
    pub include_omp: bool,
    pub include_openclaw: bool,
    pub include_copilot: bool,
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
    previous
        .device
        .zip(previous.inode)
        .zip(current.device.zip(current.inode))
        .is_some_and(|((old_device, old_inode), (new_device, new_inode))| {
            old_device != new_device || old_inode != new_inode
        })
        || previous
            .prefix_sha256
            .as_ref()
            .zip(current.prefix_sha256.as_ref())
            .is_some_and(|(old, new)| previous.prefix_bytes == current.prefix_bytes && old != new)
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
    let identity = file_identity(&path, metadata, prefix_bytes);
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
        && vector_dir.join("usearch.index").exists();
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
        return Ok(None);
    }

    let report = ingest_all(paths, index, options, lease)?;
    Ok(Some(report))
}

pub fn ingest_all(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    _lease: &IngestLease,
) -> Result<IngestReport> {
    // Apply additive analytics migrations even when the scan finds no changed files.
    drop(AnalyticsStore::open(analytics_path(&paths.state))?);
    let state_path = paths.state.join("ingest.json");
    let mut state = IngestState::load(&state_path)?;
    if index.doc_count()? == 0 && !state.files.is_empty() {
        state = IngestState::default();
        if paths.vectors.exists() {
            std::fs::remove_dir_all(&paths.vectors)?;
            std::fs::create_dir_all(&paths.vectors)?;
        }
    }
    let next_doc_id = Arc::new(AtomicU64::new(state.next_doc_id));

    let mut tasks = Vec::new();
    let mut files_scanned = 0usize;
    let mut files_skipped = 0usize;
    let mut total_bytes = 0u64;

    if options.claude_source.exists() {
        let claude_files =
            crate::sources::claude::discover(&options.claude_source, options.include_agents)?;
        for source_file in claude_files {
            let path = source_file.path;
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

    let mut session_ids = HashSet::new();
    if options.include_codex {
        let codex_files = crate::sources::codex::discover_rollouts();
        for source_file in codex_files {
            let path = source_file.path;
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

    if options.include_codex {
        for history_path in crate::sources::codex::history_paths() {
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

    if options.include_opencode {
        let opencode_files = crate::sources::opencode::discover_sessions()?;
        for source_file in opencode_files {
            let path = source_file.path;
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

    if options.include_cursor {
        let cursor_files = crate::sources::cursor::discover_transcripts();
        for source_file in cursor_files {
            let path = source_file.path;
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

    if options.include_pi {
        let pi_files = crate::sources::pi::discover();
        for source_file in pi_files {
            let path = source_file.path;
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

    if options.include_omp {
        let omp_files = crate::sources::omp::discover();
        for source_file in omp_files {
            let path = source_file.path;
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

    if options.include_openclaw {
        for source_file in crate::sources::openclaw::discover() {
            let path = source_file.path;
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

    if options.include_copilot {
        let copilot_files = crate::sources::copilot::discover_sessions();
        for source_file in copilot_files {
            let path = source_file.path;
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

    let opencode_session_links = if tasks.iter().any(|task| task.source == SourceKind::Opencode) {
        crate::sources::opencode::session_links_by_id()
    } else {
        HashMap::new()
    };

    let totals = compute_totals(&tasks);
    let file_totals = compute_file_totals(&tasks);
    let analytics_db = analytics_path(&paths.state);
    let analytics_needs_backfill =
        !AnalyticsStore::is_complete(&analytics_db) && index.doc_count()? > 0;
    if tasks.is_empty() && can_skip_noop_index(paths, index, options)? {
        if analytics_needs_backfill {
            backfill_from_index(&analytics_db, index)?;
        }
        update_scan_cache(paths, files_scanned, total_bytes)?;
        return Ok(IngestReport {
            records_added: 0,
            records_embedded: 0,
            files_scanned,
            files_skipped,
            diagnostics: Default::default(),
        });
    }

    let vector_migration = vector_migration(&paths.vectors, &tasks, options.model);
    let embeddings = options.embeddings || vector_migration.rebuild;
    let progress = Arc::new(Progress::new(totals, file_totals, embeddings));

    let (raw_tx_record, rx_record) = record_channel();
    let shared_diagnostics = Arc::new(Mutex::new(crate::sources::ParseDiagnostics::default()));
    let tx_record = RecordSender::with_diagnostics(
        raw_tx_record,
        options.tool_content_limits,
        shared_diagnostics.clone(),
    );
    let (tx_update, rx_update) = unbounded::<FileUpdate>();

    let delete_paths: Vec<String> = tasks
        .iter()
        .filter(|t| t.delete_first)
        .map(|t| t.path.to_string_lossy().to_string())
        .collect();
    let writer = index
        .writer()
        .context("failed to initialize the Tantivy index writer")?;
    let writer_index = index.clone();
    let writer_ctx = WriterContext {
        embeddings,
        do_backfill_embeddings: options.backfill_embeddings || vector_migration.rebuild,
        reset_vector_store: vector_migration.rebuild,
        vector_dir: paths.vectors.clone(),
        analytics_path: analytics_db.clone(),
        progress: progress.clone(),
        model: vector_migration.model,
        embed_runtime: options.embed_runtime.clone(),
        tool_content_limits: options.tool_content_limits,
    };
    let writer_handle = std::thread::spawn(move || {
        writer_loop(writer_index, writer, rx_record, delete_paths, writer_ctx)
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
                SourceKind::Hermes => Err(anyhow!("Hermes indexing is not supported")),
            };
            finish_file_task(task, &progress, &parse_skipped, result)
        })
    });

    drop(tx_record);
    drop(tx_update);

    let writer_result = writer_handle
        .join()
        .map_err(|_| anyhow!("writer thread panicked"))?;
    progress.finish();
    let (records_added, records_embedded) =
        writer_result.context("index writer stopped before ingestion completed")?;
    parser_result?;
    if analytics_needs_backfill {
        backfill_from_index(&analytics_db, index)?;
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
    state.next_doc_id = next_doc_id.load(Ordering::SeqCst);
    state.save(&state_path)?;

    update_scan_cache(paths, files_scanned, total_bytes)?;

    Ok(IngestReport {
        records_added,
        records_embedded,
        files_scanned,
        files_skipped: files_skipped + parse_skipped.load(Ordering::Relaxed),
        diagnostics,
    })
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
    if !paths.vectors.join("usearch.index").exists() {
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

fn writer_loop(
    index: SearchIndex,
    mut writer: tantivy::IndexWriter,
    rx: Receiver<Record>,
    delete_paths: Vec<String>,
    ctx: WriterContext,
) -> Result<(usize, usize)> {
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
    } = ctx;
    let mut analytics = AnalyticsWriter::open(&analytics_path)?;
    for path in delete_paths {
        index.delete_by_source_path(&mut writer, &path);
        analytics.delete_source_path(&path)?;
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
        if reset_vector_store {
            crate::vector::VectorIndex::reset(&vector_dir)?;
        }
        vector_index = Some(crate::vector::VectorIndex::open_or_create(
            &vector_dir,
            dims,
            Some(model.as_str()),
        )?);
        embedder = Some(handle);
        progress.set_embed_ready();
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

    analytics.flush()?;
    writer.commit()?;
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
        if let Some(vindex) = vector_index.as_mut() {
            vindex.save()?;
        }
        if let Some(handle) = embedder.take() {
            std::mem::forget(handle);
        }
    }
    Ok((count, embedded_count))
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
            claude_source: PathBuf::from("/does/not/exist"),
            include_agents: false,
            include_reasoning: false,
            include_codex: false,
            include_opencode: false,
            include_cursor: false,
            include_pi: false,
            include_omp: false,
            include_openclaw: false,
            include_copilot: false,
            embeddings,
            backfill_embeddings: false,
            model,
            embed_runtime: EmbedRuntimeConfig::default(),
            tool_content_limits: IndexedToolContentLimits::default(),
        }
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
        options.claude_source = claude_root;

        let error = ingest_all(&paths, &index, &options, &lease).expect_err("writer collision");
        let message = format!("{error:#}");

        assert!(message.contains("failed to initialize the Tantivy index writer"));
        assert!(!message.contains("disconnected channel"));
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
            claude_source: claude_root,
            include_agents: false,
            include_reasoning: false,
            include_codex: false,
            include_opencode: false,
            include_cursor: false,
            include_pi: false,
            include_omp: false,
            include_openclaw: false,
            include_copilot: false,
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
            claude_source: tmp.path().join("missing-claude"),
            include_agents: false,
            include_reasoning: false,
            include_codex: false,
            include_opencode: false,
            include_cursor: false,
            include_pi: true,
            include_omp: false,
            include_openclaw: false,
            include_copilot: false,
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
        let progress = Arc::new(Progress::new(
            [0, 0, 0, 0, 0, 0, meta.len(), 0, 0],
            [0, 0, 0, 0, 0, 0, 1, 0, 0],
            false,
        ));

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
        };

        let writer = index.writer().expect("open writer");
        let (records_added, records_embedded) =
            writer_loop(index, writer, rx_record, Vec::new(), ctx).expect("write copilot record");

        assert_eq!(records_added, 1);
        assert_eq!(records_embedded, 0);
    }
}
