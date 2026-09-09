//! Event-driven index triggering.
//!
//! The daemon historically re-scanned every source root on a fixed sleep
//! (`poll_interval`, default 30s). This module replaces the sleep with OS
//! filesystem events — FSEventStream on macOS, inotify on Linux, both behind
//! [`notify::RecommendedWatcher`] — while keeping full scans as the
//! correctness backstop.
//!
//! Design rules (see `docs/event-driven-indexing-spec.md`):
//!
//! - Events are **hints only**. Dirty fires select affected inputs for the
//!   normal incremental ingest, whose `IngestState` comparison (`size`/`mtime`/identity) stays
//!   the source of truth. A spurious event costs one cheap stat check.
//! - Fires are debounced and settled: a transcript being actively appended to
//!   must go quiet before it is parsed. This matters because the JSONL
//!   parsers treat a torn trailing line (no newline yet) as a complete line,
//!   advance the byte offset past it, and would permanently lose that record
//!   once the writer completes the line.
//! - Anything suspicious — watcher errors, [`notify`] rescan flags
//!   (`mustScanSubDirs`, `IN_Q_OVERFLOW`), a full dirty set, a newly appeared
//!   watch root — escalates to a full resync ingest, exactly like the old
//!   poll cycle. A daemon that missed *every* event still converges on the
//!   periodic resync timer.

use crate::config::Paths;
use crate::ingest::{IngestOptions, PathExcluder, build_path_excluder};
use crate::state::IngestState;
use anyhow::{Context, Result, anyhow};
use clap::ValueEnum;
use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Daemon refresh strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub(crate) enum WatchMode {
    /// React to filesystem events; full rescan on a (long) timer as backstop.
    #[default]
    Events,
    /// Legacy fixed-interval full rescan.
    Poll,
}

impl std::str::FromStr for WatchMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "events" => Ok(Self::Events),
            "poll" => Ok(Self::Poll),
            other => Err(anyhow!(
                "invalid watch mode: {other} (expected \"events\" or \"poll\")"
            )),
        }
    }
}

impl std::fmt::Display for WatchMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Events => write!(formatter, "events"),
            Self::Poll => write!(formatter, "poll"),
        }
    }
}

/// Timing knobs for the event pipeline. Defaults are chosen so an actively
/// written transcript settles before parsing, while bursts coalesce.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WatchConfig {
    /// Per-batch quiet period: fire only once no event arrived for this long.
    pub debounce: Duration,
    /// Minimum file-mtime age before a fire (torn-write settle).
    pub settle: Duration,
    /// Upper bound a dirty batch waits before firing anyway.
    pub max_batch_age: Duration,
    /// Minimum spacing between two ingests.
    pub min_spacing: Duration,
    /// Periodic full-resync interval (the missed-event backstop).
    pub resync_interval: Duration,
    /// Dirty-set capacity; overflow degrades to a full resync, never OOM.
    pub dirty_cap: usize,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            debounce: Duration::from_millis(1500),
            settle: Duration::from_millis(500),
            max_batch_age: Duration::from_secs(8),
            min_spacing: Duration::from_secs(5),
            resync_interval: Duration::from_secs(600),
            dirty_cap: 10_000,
        }
    }
}

/// Why the scheduler wants an ingest run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FireCause {
    /// Debounced, settled file changes.
    Dirty,
    /// Startup-equivalent full convergence (timer, overflow, error, new root).
    Resync,
}

/// Counters for `daemon status` and for spotting a silent watcher: events at
/// zero across resyncs that keep finding changes means broken watches.
#[derive(Debug, Clone, Default)]
pub(crate) struct WatchStats {
    pub(crate) events_total: u64,
    pub(crate) events_filtered: u64,
    pub(crate) fires_dirty: u64,
    pub(crate) fires_resync: u64,
    pub(crate) noop_skips: u64,
    pub(crate) watcher_errors: u64,
    pub(crate) resync_requests: u64,
    pub(crate) hot_sweeps: u64,
    pub(crate) hot_hits: u64,
}

/// Compute the exact directory set that transcript discovery walks, so the
/// watcher covers every source `ingest_all` reads — and nothing else.
///
/// Sources without an ingest discovery block (Hermes) are deliberately
/// excluded; watching them could only trigger useless ingests.
pub(crate) fn watch_roots(options: &IngestOptions) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if !options.claude_sources.is_empty() {
        roots.extend(options.claude_sources.iter().cloned());
    }
    if options.include_codex {
        roots.extend(crate::sources::codex::homes());
    }
    if options.include_opencode {
        roots.extend(crate::sources::opencode::data_roots());
    }
    if options.include_cursor {
        roots.push(crate::sources::cursor::projects_root());
    }
    if options.include_pi {
        roots.push(crate::sources::pi::sessions_root());
        roots.push(crate::sources::pi::agent_root());
    }
    if options.include_omp {
        roots.extend(crate::sources::omp::session_roots());
    }
    if options.include_openclaw {
        roots.extend(crate::sources::openclaw::state_dirs());
    }
    if options.include_copilot {
        roots.push(crate::sources::copilot::root());
    }
    if options.include_grok {
        roots.push(crate::sources::grok::root());
    }
    if options.include_jcode {
        roots.push(crate::sources::jcode::sessions_root());
    }
    if options.include_muse {
        roots.push(crate::sources::muse::sessions_root());
    }
    if options.include_antigravity {
        roots.push(crate::sources::antigravity::sessions_root());
    }
    roots.sort();
    roots.dedup();
    roots
}

/// Build the event filter from the same exclusion patterns discovery uses,
/// so an event for a path ingest would ignore never fires an ingest.
pub(crate) fn watch_excluder(options: &IngestOptions) -> Result<PathExcluder> {
    build_path_excluder(options)
}

/// Narrow deny-list applied before anything else. Deliberately a deny-list,
/// not an allow-list: unknown-but-real transcript extensions must still
/// trigger (the ingest state comparison will no-op them cheaply if they are
/// irrelevant), while over-filtering would silently miss records.
fn ignorable_file_name(name: &str) -> bool {
    name == ".DS_Store"
        || name.ends_with(".tmp")
        || name.ends_with(".swp")
        || name.ends_with('~')
        || name.ends_with("-wal")
        || name.ends_with("-shm")
        || name.ends_with("-journal")
        || name.starts_with(".memex-opencode-spool-")
        || matches!(
            name,
            "ingest.json" | "ingest.pending.json" | "scan_cache.json"
        )
}

/// Classify one watcher event. Returns the paths worth tracking, and whether
/// the event signals possibly-missed history ([`Event::need_rescan`]).
fn interesting_event_paths(event: &Event, excluder: &PathExcluder) -> (Vec<PathBuf>, bool) {
    if matches!(event.kind, EventKind::Access(_)) {
        return (Vec::new(), event.need_rescan());
    }
    let mut paths = Vec::with_capacity(event.paths.len());
    for path in &event.paths {
        // SQLite commits can live entirely in the WAL until checkpoint.
        // Route the hint to the database that ingestion knows how to read.
        if let Some(name) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix("-wal"))
            .filter(|name| {
                let database = path.with_file_name(name);
                crate::sources::opencode::is_database_path(name)
                    || (crate::sources::antigravity::is_db_path(&database)
                        && crate::sources::antigravity::matches_path(&database.to_string_lossy()))
            })
        {
            let database = path.with_file_name(name);
            if !excluder.is_excluded(path) && !excluder.is_excluded(&database) {
                paths.push(database);
            }
            continue;
        }
        let ignored = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(ignorable_file_name);
        if ignored || excluder.is_excluded(path) {
            continue;
        }
        paths.push(path.clone());
    }
    (paths, event.need_rescan())
}

/// Compare live filesystem metadata against stored ingest state. This is the
/// same predicate `prepare_file_task` uses to skip unchanged files (equal
/// size+mtime means skip), tightened with nanosecond mtime so same-second
/// rewrites still trigger.
fn stat_differs(
    metadata: &std::fs::Metadata,
    size: u64,
    mtime: i64,
    modified_ns: Option<i64>,
) -> bool {
    if metadata.len() != size {
        return true;
    }
    let modified = metadata.modified().ok();
    let current_ns = modified
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64);
    match (current_ns, modified_ns) {
        (Some(current), Some(stored)) if current != stored => true,
        _ => {
            let current_mtime = modified
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs() as i64)
                .unwrap_or(0);
            current_mtime != mtime
        }
    }
}
/// Fast no-op check: is there any dirty path the ingest state comparison
/// would actually act on?
///
/// Conservative by construction: unknown paths, vanished paths, and stat
/// failures all report "needs ingest" and let the full ingest sort it out.
pub(crate) fn dirty_needs_ingest(paths: &Paths, dirty: &HashSet<PathBuf>) -> Result<bool> {
    if dirty.is_empty() {
        return Ok(false);
    }
    let state = IngestState::load(&paths.state.join("ingest.json"))?;
    for path in dirty {
        let key = path.to_string_lossy().into_owned();
        let Some(previous) = state.files.get(&key) else {
            return Ok(true);
        };
        if let Some(wal) = &previous.identity.sqlite_wal
            && *wal != crate::state::SqliteWalIdentity::read(path)
        {
            return Ok(true);
        }
        let metadata = match std::fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
            Err(error) => {
                return Err(error).with_context(|| format!("stat dirty path {}", path.display()));
            }
        };
        if stat_differs(
            &metadata,
            previous.size,
            previous.mtime,
            previous.identity.modified_ns,
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Resolve symlinks in an existing watch root. Missing roots are returned
/// unchanged so the tick retry keeps waiting for them.
fn canonical_root(root: &PathBuf) -> PathBuf {
    std::fs::canonicalize(root).unwrap_or_else(|_| root.clone())
}

/// The event-driven scheduler. Owns the [`RecommendedWatcher`], debounces the
/// raw stream into dirty batches, and decides when the caller should run an
/// ingest. Ingest execution itself stays with the caller so this type never
/// touches the index, the lease, or source parsers.
pub(crate) struct WatchService {
    watcher: RecommendedWatcher,
    receiver: Receiver<notify::Result<Event>>,
    _sender: Sender<notify::Result<Event>>,
    queue_overflow: Arc<AtomicBool>,
    watched: Vec<PathBuf>,
    pending: Vec<PathBuf>,
    dirty: HashMap<PathBuf, Instant>,
    batch_start: Option<Instant>,
    last_fire: Option<Instant>,
    resync_due: Instant,
    resync_needed: bool,
    excluder: PathExcluder,
    config: WatchConfig,
    stats: WatchStats,
    hot_snapshot: Option<HashMap<String, StateSnapshot>>,
    hot_state_mtime: Option<SystemTime>,
    hot_databases: HashMap<PathBuf, Option<DatabaseSnapshot>>,
}

/// Compact copy of the ingest state needed to spot changed files without
/// re-reading `ingest.json` on every sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StateSnapshot {
    size: u64,
    mtime: i64,
    modified_ns: Option<i64>,
}

impl StateSnapshot {
    fn read(path: &Path) -> Option<Self> {
        let metadata = std::fs::metadata(path).ok()?;
        let modified = metadata.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
        Some(Self {
            size: metadata.len(),
            mtime: modified.as_secs() as i64,
            modified_ns: Some(modified.as_nanos().min(i64::MAX as u128) as i64),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DatabaseSnapshot {
    database: Option<StateSnapshot>,
    wal: Option<StateSnapshot>,
}

const EVENT_QUEUE_CAPACITY: usize = 8192;

fn enqueue_event(
    sender: &Sender<notify::Result<Event>>,
    overflow: &AtomicBool,
    event: notify::Result<Event>,
) {
    // Never block the OS watcher during ingestion. One sticky bit preserves
    // the need to reconcile when bounded buffering cannot retain all hints.
    if let Err(TrySendError::Full(_)) = sender.try_send(event) {
        overflow.store(true, Ordering::Release);
    }
}

/// How often the macOS hot sweep re-stats recently active files (see
/// [`WatchService::hot_sweep_dirty`]).
pub(crate) const HOT_SWEEP_INTERVAL: Duration = Duration::from_secs(5);
/// Files whose ingested mtime is older than this are not sweep candidates;
/// events and periodic resync discover resumed cold sessions.
pub(crate) const HOT_WINDOW: Duration = Duration::from_secs(6 * 60 * 60);

impl WatchService {
    pub(crate) fn new(
        roots: Vec<PathBuf>,
        excluder: PathExcluder,
        config: WatchConfig,
    ) -> Result<Self> {
        let (sender, receiver) = bounded(EVENT_QUEUE_CAPACITY);
        let callback_sender = sender.clone();
        let queue_overflow = Arc::new(AtomicBool::new(false));
        let callback_overflow = Arc::clone(&queue_overflow);
        let watcher = RecommendedWatcher::new(
            move |result| {
                enqueue_event(&callback_sender, &callback_overflow, result);
            },
            Config::default(),
        )
        .context("create filesystem watcher")?;
        let resync_due = Instant::now() + config.resync_interval;
        let mut service = Self {
            watcher,
            receiver,
            _sender: sender,
            queue_overflow,
            watched: Vec::new(),
            pending: roots,
            dirty: HashMap::new(),
            batch_start: None,
            last_fire: None,
            resync_due,
            resync_needed: false,
            excluder,
            config,
            stats: WatchStats::default(),
            hot_snapshot: None,
            hot_state_mtime: None,
            hot_databases: HashMap::new(),
        };
        service.sort_pending();
        service.activate_pending();
        Ok(service)
    }

    /// Non-blocking scheduler tick. Drain the event queue, then report
    /// whether the caller should ingest now. A returned fire does not clear
    /// state; the caller must invoke [`Self::mark_complete`] on success,
    /// [`Self::mark_skipped`] when the no-op check vetoes, or nothing at all
    /// (keeping the dirty set for retry) when the ingest itself failed.
    pub(crate) fn poll(&mut self) -> Option<FireCause> {
        let now = Instant::now();
        self.activate_pending();
        self.drain();
        self.fire_decision(now)
    }

    /// Snapshot of currently dirty paths, for the no-op stat check and logs.
    pub(crate) fn dirty_paths(&self) -> HashSet<PathBuf> {
        self.dirty.keys().cloned().collect()
    }

    pub(crate) fn stats(&self) -> &WatchStats {
        &self.stats
    }

    pub(crate) fn watched_roots(&self) -> &[PathBuf] {
        &self.watched
    }

    pub(crate) fn pending_roots(&self) -> &[PathBuf] {
        &self.pending
    }

    /// Record a completed ingest. Partial batches clear their hints without
    /// postponing full reconciliation of sources that had no delivered events.
    pub(crate) fn mark_complete(&mut self, cause: FireCause) {
        self.dirty.clear();
        self.batch_start = None;
        self.last_fire = Some(Instant::now());
        match cause {
            FireCause::Dirty => self.stats.fires_dirty += 1,
            FireCause::Resync => {
                self.resync_due = self.last_fire.unwrap() + self.config.resync_interval;
                self.resync_needed = false;
                self.stats.fires_resync += 1;
            }
        }
    }

    /// Record a vetoed fire (no-op check): drop the noise, keep the timers —
    /// no scan happened, so the resync deadline must not move.
    pub(crate) fn mark_skipped(&mut self) {
        self.dirty.clear();
        self.batch_start = None;
        self.stats.noop_skips += 1;
    }

    /// Reconcile watches with freshly resolved roots (cheap; call on resync).
    /// Added roots activate on the next tick and force a resync ingest, which
    /// closes the gap between the root appearing and the watch arming.
    pub(crate) fn ensure_roots(&mut self, roots: Vec<PathBuf>) {
        let desired: HashSet<PathBuf> = roots
            .into_iter()
            .map(|root| canonical_root(&root))
            .collect();
        self.watched.retain(|root| {
            if desired.contains(root) {
                return true;
            }
            let _ = self.watcher.unwatch(root);
            false
        });
        for root in desired {
            if !self.watched.contains(&root) && !self.pending.contains(&root) {
                self.pending.push(root);
            }
        }
        self.sort_pending();
    }

    fn sort_pending(&mut self) {
        self.pending.sort();
        self.pending.dedup();
    }

    /// Try to arm watches for not-yet-existing roots. Runs every tick; each
    /// attempt is one `stat` per missing root. A newly armed root forces a
    /// resync so files created in the blind window converge immediately.
    ///
    /// Roots are canonicalized before watching: backends (notably FSEvents)
    /// silently mis-deliver for paths containing symlinks, and the temp
    /// directories tests use are symlinked on macOS.
    fn activate_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let mut still_pending = Vec::with_capacity(self.pending.len());
        for root in std::mem::take(&mut self.pending) {
            if !root.is_dir() {
                still_pending.push(root);
                continue;
            }
            let canonical = canonical_root(&root);
            match self.watcher.watch(&canonical, RecursiveMode::Recursive) {
                Ok(()) => {
                    self.watched.push(canonical);
                    self.request_resync();
                }
                Err(error) => {
                    self.stats.watcher_errors += 1;
                    eprintln!("watch: cannot watch {}: {error:#}", root.display());
                    still_pending.push(root);
                }
            }
        }
        self.pending = still_pending;
        self.watched.sort();
        self.watched.dedup();
    }

    fn request_resync(&mut self) {
        if !self.resync_needed {
            self.resync_needed = true;
            self.stats.resync_requests += 1;
        }
    }

    fn drain(&mut self) {
        if self.queue_overflow.swap(false, Ordering::AcqRel) {
            self.request_resync();
        }
        let now = Instant::now();
        // Bound work as well as memory: a producer that never goes quiet
        // must not keep the scheduler draining forever.
        for _ in 0..EVENT_QUEUE_CAPACITY {
            let Ok(result) = self.receiver.try_recv() else {
                break;
            };
            let event = match result {
                Ok(event) => event,
                Err(error) => {
                    self.stats.watcher_errors += 1;
                    eprintln!("watch: watcher error: {error:#}");
                    self.request_resync();
                    continue;
                }
            };
            self.stats.events_total += 1;
            let (paths, rescan) = interesting_event_paths(&event, &self.excluder);
            if rescan {
                eprintln!("watch: backend requested rescan; scheduling full ingest");
                self.request_resync();
            }
            if paths.is_empty() {
                self.stats.events_filtered += 1;
                continue;
            }
            for path in paths {
                if self.dirty.len() >= self.config.dirty_cap && !self.dirty.contains_key(&path) {
                    self.dirty.clear();
                    self.request_resync();
                    break;
                }
                self.dirty.insert(path, now);
            }
            if self.batch_start.is_none() && !self.dirty.is_empty() {
                self.batch_start = Some(now);
            }
        }
    }

    fn fire_decision(&self, now: Instant) -> Option<FireCause> {
        if let Some(last) = self.last_fire
            && now.duration_since(last) < self.config.min_spacing
        {
            return None;
        }
        if self.resync_needed || now >= self.resync_due {
            return Some(FireCause::Resync);
        }
        if self.dirty.is_empty() {
            return None;
        }
        let newest = self.dirty.values().copied().max().unwrap_or(now);
        let aged = self
            .batch_start
            .is_some_and(|start| now.duration_since(start) >= self.config.max_batch_age);
        if !aged {
            if now.duration_since(newest) < self.config.debounce {
                return None;
            }
            if !self.settled() {
                return None;
            }
        }
        Some(FireCause::Dirty)
    }

    /// No dirty file may have been modified within the settle window.
    /// Missing paths count as settled: deletions need the ingest (tombstone
    /// sweep), and a path that no longer exists cannot tear.
    fn settled(&self) -> bool {
        let now = SystemTime::now();
        self.dirty.keys().all(|path| {
            if !path.is_file() {
                return true;
            }
            match std::fs::metadata(path)
                .ok()
                .and_then(|metadata| metadata.modified().ok())
            {
                Some(mtime) => now
                    .duration_since(mtime)
                    .is_ok_and(|age| age >= self.config.settle),
                None => true,
            }
        })
    }

    /// Record externally observed changes (e.g. the hot sweep below) as if
    /// they arrived as events: same capacity backpressure, same batch window.
    pub(crate) fn note_dirty(&mut self, paths: Vec<PathBuf>) {
        self.note_dirty_at(paths, Instant::now());
    }

    pub(crate) fn note_dirty_at(&mut self, paths: Vec<PathBuf>, now: Instant) {
        for path in paths {
            if self.dirty.len() >= self.config.dirty_cap && !self.dirty.contains_key(&path) {
                self.dirty.clear();
                self.request_resync();
                break;
            }
            self.dirty.insert(path, now);
        }
        if self.batch_start.is_none() && !self.dirty.is_empty() {
            self.batch_start = Some(now);
        }
    }

    /// Re-stat recently active transcripts and report the ones whose content
    /// moved since the last ingest.
    ///
    /// This exists for a platform gap, not as polling-by-another-name:
    /// FSEvents defers content-modification events for files held open for
    /// writing (verified: zero events in 8s with the fd open, immediate
    /// delivery on close), and agents stream transcripts through a single
    /// held-open fd for the whole session. Without this sweep, an active
    /// macOS session would only index on close. inotify reports `MODIFY` on
    /// every write regardless of open handles, so callers only need this on
    /// macOS; everywhere else events plus the resync timer suffice.
    ///
    /// Cost is bounded: `ingest.json` is re-parsed only when it changed, and
    /// ordinary files are stat-compared only within `window`. Cold file history
    /// is rediscovered through events/resync. Each tracked OpenCode or Antigravity
    /// database also needs two stats regardless of age: its main file and WAL.
    pub(crate) fn hot_sweep_dirty(
        &mut self,
        paths: &Paths,
        window: Duration,
    ) -> Result<Vec<PathBuf>> {
        self.stats.hot_sweeps += 1;
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(window.as_secs()) as i64;
        let state_path = paths.state.join("ingest.json");
        let state_mtime = std::fs::metadata(&state_path)
            .ok()
            .and_then(|metadata| metadata.modified().ok());
        if self.hot_snapshot.is_none() || state_mtime != self.hot_state_mtime {
            let state = IngestState::load(&state_path)?;
            self.hot_snapshot = Some(
                state
                    .files
                    .iter()
                    .filter(|(_, file)| file.mtime >= cutoff && file.identity.sqlite_wal.is_none())
                    .map(|(key, file)| {
                        (
                            key.clone(),
                            StateSnapshot {
                                size: file.size,
                                mtime: file.mtime,
                                modified_ns: file.identity.modified_ns,
                            },
                        )
                    })
                    .collect(),
            );
            // Keep databases even when their main-file mtime is old: active
            // writers may commit exclusively to the WAL for the whole session.
            let databases = state
                .opencode_databases
                .keys()
                .chain(
                    state
                        .files
                        .iter()
                        .filter(|(_, file)| file.identity.sqlite_wal.is_some())
                        .map(|(key, _)| key),
                )
                .map(PathBuf::from)
                .collect::<HashSet<_>>();
            self.hot_databases
                .retain(|path, _| databases.contains(path));
            for path in databases {
                self.hot_databases.entry(path).or_insert(None);
            }
            self.hot_state_mtime = state_mtime;
        }
        let mut changed = Vec::new();
        if let Some(snapshot) = &mut self.hot_snapshot {
            // Expire candidates even when the ingest state has not changed.
            snapshot.retain(|_, previous| previous.mtime >= cutoff);
            for (key, previous) in snapshot {
                let path = Path::new(key);
                let metadata = match std::fs::metadata(path) {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        changed.push(PathBuf::from(key));
                        continue;
                    }
                    Err(_) => continue,
                };
                if stat_differs(
                    &metadata,
                    previous.size,
                    previous.mtime,
                    previous.modified_ns,
                ) {
                    changed.push(PathBuf::from(key));
                }
            }
        }
        for (path, previous) in &mut self.hot_databases {
            let mut wal = path.as_os_str().to_os_string();
            wal.push("-wal");
            let current = DatabaseSnapshot {
                database: StateSnapshot::read(path),
                wal: StateSnapshot::read(Path::new(&wal)),
            };
            if previous.as_ref() != Some(&current) {
                changed.push(path.clone());
                *previous = Some(current);
            }
        }
        self.stats.hot_hits += changed.len() as u64;
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::UserConfig;
    use crate::state::{FileIdentity, FileState};
    use crate::test_support::{EnvVarGuard, env_lock};
    use crossbeam_channel::unbounded;
    use std::collections::{HashMap, HashSet};
    use std::io::Write;
    use std::path::Path;

    fn test_options() -> IngestOptions {
        IngestOptions {
            claude_sources: vec![PathBuf::from("/tmp/memex-watch-test/claude")],
            include_agents: false,
            include_reasoning: false,
            include_codex: true,
            include_opencode: true,
            include_cursor: true,
            include_pi: true,
            include_omp: true,
            include_openclaw: true,
            include_copilot: true,
            include_grok: true,
            include_jcode: true,
            include_muse: true,
            include_antigravity: true,
            exclude_patterns: Vec::new(),
            embeddings: false,
            backfill_embeddings: false,
            model: crate::embed::ModelChoice::default(),
            embed_runtime: UserConfig::default()
                .resolve_embed_runtime()
                .expect("default embed runtime"),
            tool_content_limits: crate::config::IndexedToolContentLimits::default(),
        }
    }

    fn test_service() -> WatchService {
        let options = test_options();
        let excluder = watch_excluder(&options).expect("excluder");
        WatchService::new(Vec::new(), excluder, WatchConfig::default()).expect("service")
    }

    #[test]
    fn watch_mode_parses_and_displays() {
        assert_eq!(
            "events".parse::<WatchMode>().expect("parse events"),
            WatchMode::Events
        );
        assert_eq!(
            "POLL".parse::<WatchMode>().expect("parse poll"),
            WatchMode::Poll
        );
        assert!("fsevents".parse::<WatchMode>().is_err());
        assert_eq!(WatchMode::Events.to_string(), "events");
        assert_eq!(WatchMode::Poll.to_string(), "poll");
        assert_eq!(WatchMode::default(), WatchMode::Events);
    }

    #[test]
    fn watch_roots_cover_every_ingested_source() {
        let _guard = env_lock();
        let temp = tempfile::tempdir().expect("tempdir");
        let home = temp.path().join("home");
        std::fs::create_dir_all(&home).expect("home");
        let claude_projects = temp.path().join("claude-projects");
        std::fs::create_dir_all(&claude_projects).expect("claude projects");
        let _env = EnvVarGuard::set(&[
            ("HOME", Some(home.to_str().expect("home utf8"))),
            (
                "CLAUDE_CONFIG_DIR",
                Some(claude_projects.to_str().expect("claude utf8")),
            ),
            ("CODEX_HOME", Some("/tmp/memex-watch-test-codex")),
            ("OPENCODE_DATA_DIR", Some("/tmp/memex-watch-test-opencode")),
            ("PI_CODING_AGENT_DIR", None),
            ("PI_CODING_AGENT_SESSION_DIR", None),
            ("PI_CONFIG_DIR", None),
            ("OMP_PROFILE", None),
            ("PI_PROFILE", None),
            ("OPENCLAW_STATE_DIR", Some("/tmp/memex-watch-test-openclaw")),
            ("CLAWDBOT_STATE_DIR", None),
            ("COPILOT_HOME", Some("/tmp/memex-watch-test-copilot")),
            ("GROK_HOME", Some("/tmp/memex-watch-test-grok")),
            ("JCODE_HOME", Some("/tmp/memex-watch-test-jcode")),
            ("MUSE_HOME", Some("/tmp/memex-watch-test-muse")),
            ("XDG_DATA_HOME", None),
        ]);

        let mut options = test_options();
        options.claude_sources = vec![claude_projects.join("projects")];
        let roots = watch_roots(&options);

        let rendered: Vec<String> = roots
            .iter()
            .map(|root| root.to_string_lossy().into_owned())
            .collect();
        for expected in [
            "claude-projects/projects",
            "memex-watch-test-codex",
            "memex-watch-test-opencode",
            ".cursor/projects",
            ".pi/agent",
            ".omp/agent/sessions",
            "memex-watch-test-openclaw",
            "memex-watch-test-copilot",
            "memex-watch-test-grok",
            "jcode/sessions",
            "muse/sessions",
        ] {
            assert!(
                rendered.iter().any(|root| root.contains(expected)),
                "missing watch root containing {expected}: {rendered:?}"
            );
        }
        // Sorted and deduplicated.
        let mut sorted = rendered.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(rendered, sorted);
    }

    #[test]
    fn watch_roots_respect_disabled_sources() {
        let _guard = env_lock();
        let mut options = test_options();
        options.include_codex = false;
        options.include_opencode = false;
        options.include_cursor = false;
        options.include_pi = false;
        options.include_omp = false;
        options.include_openclaw = false;
        options.include_copilot = false;
        options.include_grok = false;
        options.include_jcode = false;
        options.include_muse = false;
        options.include_antigravity = false;
        let roots = watch_roots(&options);
        assert_eq!(roots, options.claude_sources);
    }

    #[test]
    fn event_filter_drops_access_noise_and_sidecars() {
        let _guard = env_lock();
        let options = test_options();
        let excluder = watch_excluder(&options).expect("excluder");
        let event = |kind: EventKind, name: &str| Event {
            kind,
            paths: vec![PathBuf::from(format!("/tmp/sessions/{name}"))],
            attrs: Default::default(),
        };

        // Access events never fire, even for real transcripts.
        let (paths, _) = interesting_event_paths(
            &event(EventKind::Access(notify::event::AccessKind::Any), "s.jsonl"),
            &excluder,
        );
        assert!(paths.is_empty());

        // Real transcript writes pass.
        let (paths, _) = interesting_event_paths(
            &event(
                EventKind::Modify(notify::event::ModifyKind::Any),
                "session.jsonl",
            ),
            &excluder,
        );
        assert_eq!(paths.len(), 1);

        // Unrelated SQLite sidecars and editor debris do not.
        for junk in [
            "other.db-wal",
            "opencode.db-shm",
            "opencode.db-journal",
            "session.jsonl.tmp",
            "session.jsonl.swp",
            "session.jsonl~",
            ".DS_Store",
        ] {
            let (paths, _) = interesting_event_paths(
                &event(EventKind::Modify(notify::event::ModifyKind::Any), junk),
                &excluder,
            );
            assert!(paths.is_empty(), "{junk} should be filtered");
        }

        // The main database still fires.
        let (paths, _) = interesting_event_paths(
            &event(
                EventKind::Modify(notify::event::ModifyKind::Any),
                "opencode.db",
            ),
            &excluder,
        );
        assert_eq!(paths.len(), 1);
    }

    #[test]
    fn wal_events_target_database_and_honor_database_exclusions() {
        let _guard = env_lock();
        let mut options = test_options();
        for name in [
            "opencode.db",
            "opencode-work.db",
            "antigravity-ide/conversations/session.db",
        ] {
            let database = PathBuf::from(format!("/tmp/sessions/{name}"));
            let wal = PathBuf::from(format!("/tmp/sessions/{name}-wal"));
            let event = Event {
                kind: EventKind::Modify(notify::event::ModifyKind::Any),
                paths: vec![wal.clone()],
                attrs: Default::default(),
            };
            options.exclude_patterns.clear();
            let excluder = watch_excluder(&options).unwrap();
            assert_eq!(
                interesting_event_paths(&event, &excluder).0,
                vec![database.clone()]
            );
            for excluded in [&database, &wal] {
                options.exclude_patterns = vec![excluded.to_string_lossy().into_owned()];
                let excluder = watch_excluder(&options).unwrap();
                assert!(interesting_event_paths(&event, &excluder).0.is_empty());
            }
        }
    }

    #[test]
    fn event_queue_overflow_is_nonblocking_and_survives_inflight_ingest() {
        let _guard = env_lock();
        let mut service = test_service();
        service.config.min_spacing = Duration::ZERO;
        let sender = service._sender.clone();
        let overflow = Arc::clone(&service.queue_overflow);
        let (finished_tx, finished_rx) = bounded(1);
        let producer = std::thread::spawn(move || {
            for _ in 0..EVENT_QUEUE_CAPACITY + 1 {
                enqueue_event(
                    &sender,
                    &overflow,
                    Ok(Event {
                        kind: EventKind::Modify(notify::event::ModifyKind::Any),
                        paths: vec![PathBuf::from("/tmp/queue-session.jsonl")],
                        attrs: Default::default(),
                    }),
                );
            }
            finished_tx.send(()).unwrap();
        });
        // No consumer runs until the producer finishes, just like a slow ingest.
        finished_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("callback blocked on full queue");
        producer.join().unwrap();
        assert_eq!(service.receiver.len(), EVENT_QUEUE_CAPACITY);
        service.mark_complete(FireCause::Dirty);
        assert_eq!(service.poll(), Some(FireCause::Resync));
        service.mark_complete(FireCause::Resync);
        assert_eq!(service.poll(), None);
        // Recovery does not leave a permanent overflow condition.
        enqueue_event(
            &service._sender,
            &service.queue_overflow,
            Ok(Event {
                kind: EventKind::Modify(notify::event::ModifyKind::Any),
                paths: vec![PathBuf::from("/tmp/after-overflow.jsonl")],
                attrs: Default::default(),
            }),
        );
        service.config.debounce = Duration::ZERO;
        assert_eq!(service.poll(), Some(FireCause::Dirty));
    }

    #[test]
    fn event_filter_honors_exclude_globs() {
        let _guard = env_lock();
        let mut options = test_options();
        options.exclude_patterns = vec!["/tmp/sessions/*-client-*".to_string()];
        let excluder = watch_excluder(&options).expect("excluder");
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Any),
            paths: vec![PathBuf::from("/tmp/sessions/work-client-1/session.jsonl")],
            attrs: Default::default(),
        };
        let (paths, _) = interesting_event_paths(&event, &excluder);
        assert!(paths.is_empty());
    }

    #[test]
    fn debounce_holds_fire_until_quiet_then_settled() {
        let _guard = env_lock();
        let mut service = test_service();
        let start = Instant::now();
        let path = PathBuf::from("/tmp/memex-watch-test/session.jsonl");

        service.note_dirty_at(vec![path.clone()], start);
        assert_eq!(service.fire_decision(start), None);

        // Still inside the quiet period.
        service.note_dirty_at(vec![path.clone()], start + Duration::from_millis(1400));
        assert_eq!(
            service.fire_decision(start + Duration::from_millis(2000)),
            None
        );

        // Quiet for longer than debounce, but the file is missing so settle
        // treats it as deletion-needing-ingest: fires.
        assert_eq!(
            service.fire_decision(start + Duration::from_millis(3000)),
            Some(FireCause::Dirty)
        );

        service.mark_complete(FireCause::Dirty);
        assert_eq!(
            service.fire_decision(start + Duration::from_millis(3100)),
            None
        );
        // Min spacing holds back an immediate re-fire.
        service.note_dirty_at(vec![path], start + Duration::from_millis(3200));
        assert_eq!(
            service.fire_decision(start + Duration::from_millis(4000)),
            None
        );
        assert_eq!(
            service.fire_decision(start + Duration::from_secs(9)),
            Some(FireCause::Dirty)
        );
        assert_eq!(service.stats().fires_dirty, 1);
    }

    #[test]
    fn max_batch_age_forces_fire_under_continuous_writes() {
        let _guard = env_lock();
        let mut service = test_service();
        let start = Instant::now();
        let path = PathBuf::from("/tmp/memex-watch-test/session.jsonl");
        // A writer that never goes quiet: every tick resets debounce, but the
        // batch age cap still forces a fire.
        for millis in (0..9000).step_by(200) {
            service.note_dirty_at(vec![path.clone()], start + Duration::from_millis(millis));
            if service.fire_decision(start + Duration::from_millis(millis))
                == Some(FireCause::Dirty)
            {
                assert!(
                    millis >= 8000,
                    "fired at {millis}ms, before the max batch age"
                );
                return;
            }
        }
        panic!("continuous writes never fired");
    }

    #[test]
    fn dirty_cap_escalates_to_resync() {
        let _guard = env_lock();
        let options = test_options();
        let excluder = watch_excluder(&options).expect("excluder");
        let config = WatchConfig {
            dirty_cap: 4,
            ..WatchConfig::default()
        };
        let mut service = WatchService::new(Vec::new(), excluder, config).expect("service");
        let start = Instant::now();
        let paths: Vec<PathBuf> = (0..8)
            .map(|index| PathBuf::from(format!("/tmp/sessions/{index}.jsonl")))
            .collect();
        service.note_dirty_at(paths, start);
        assert_eq!(
            service.fire_decision(start + Duration::from_secs(30)),
            Some(FireCause::Resync)
        );
        service.mark_complete(FireCause::Resync);
        assert_eq!(service.stats().fires_resync, 1);
        assert!(service.dirty_paths().is_empty());
    }

    #[test]
    fn resync_timer_fires_when_idle() {
        let _guard = env_lock();
        let options = test_options();
        let excluder = watch_excluder(&options).expect("excluder");
        let config = WatchConfig {
            resync_interval: Duration::from_millis(100),
            ..WatchConfig::default()
        };
        let mut service = WatchService::new(Vec::new(), excluder, config).expect("service");
        assert_eq!(service.poll(), None);
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(service.poll(), Some(FireCause::Resync));
    }

    #[test]
    fn targeted_ingests_cannot_postpone_full_reconciliation() {
        let _guard = env_lock();
        let mut service = test_service();
        service.config.min_spacing = Duration::ZERO;
        let deadline = service.resync_due;
        for _ in 0..3 {
            service.mark_complete(FireCause::Dirty);
            assert_eq!(service.fire_decision(deadline), Some(FireCause::Resync));
        }
        service.request_resync();
        service.mark_complete(FireCause::Dirty);
        assert_eq!(service.poll(), Some(FireCause::Resync));
        service.mark_complete(FireCause::Resync);
        assert_eq!(service.poll(), None);
    }

    /// FSEvents defers content-modification events for a file held open for
    /// writing and flushes them on close (verified empirically: 0 events in
    /// 8s with the fd open, immediate delivery after `drop`). Agents stream
    /// transcripts through a single held-open fd (see `lsof` on any live
    /// session), so on macOS events alone can never index an active session —
    /// the hot sweep (`hot_sweep_dirty`) exists for exactly this gap.
    /// If this test ever fails by observing events while open, FSEvents
    /// improved and the sweep may be redundant.
    #[cfg(target_os = "macos")]
    #[test]
    fn fsevents_defers_modify_until_close() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().to_path_buf();
        std::fs::write(root.join("seed.txt"), "x").expect("seed");
        let canon: PathBuf = std::fs::canonicalize(&root).expect("canon");
        let (tx, rx) = unbounded::<notify::Result<Event>>();
        let mut watcher = RecommendedWatcher::new(
            move |result| {
                let _ = tx.send(result);
            },
            Config::default(),
        )
        .expect("watcher");
        watcher
            .watch(&canon, RecursiveMode::Recursive)
            .expect("watch");
        std::thread::sleep(Duration::from_secs(2));
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(root.join("seed.txt"))
            .expect("open");
        std::io::Write::write_all(&mut file, b"held-open\n").expect("write");
        file.sync_all().expect("sync");
        let mut open_count = 0;
        let deadline = Instant::now() + Duration::from_secs(4);
        while Instant::now() < deadline {
            if rx.try_recv().is_ok() {
                open_count += 1;
            } else {
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        assert_eq!(open_count, 0, "modify events arrived while fd open");
        drop(file);
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut closed = false;
        while Instant::now() < deadline {
            if rx.try_recv().is_ok() {
                closed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(closed, "no modify event after close");
    }

    #[test]
    fn real_watcher_event_fires_dirty() {
        let _guard = env_lock();
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().to_path_buf();
        std::fs::write(root.join("session.jsonl"), "{}\n").expect("seed");
        let options = test_options();
        let excluder = watch_excluder(&options).expect("excluder");
        let config = WatchConfig {
            debounce: Duration::from_millis(100),
            settle: Duration::from_millis(0),
            min_spacing: Duration::from_millis(0),
            resync_interval: Duration::from_secs(3600),
            ..WatchConfig::default()
        };
        let mut service = WatchService::new(vec![root.clone()], excluder, config).expect("service");
        assert!(!service.watched_roots().is_empty());
        // Drain the creation burst so the append below is the only dirty input.
        // Sleeps are generous on purpose: fseventsd delivery lag on a loaded
        // machine is measured in seconds, not milliseconds.
        for _ in 0..20 {
            let _ = service.poll();
            std::thread::sleep(Duration::from_millis(100));
        }
        service.mark_complete(FireCause::Resync);

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(root.join("session.jsonl"))
            .expect("open");
        writeln!(file, "{{\"appended\": true}}").expect("append");
        file.sync_all().expect("sync");
        drop(file);

        let deadline = Instant::now() + Duration::from_secs(30);
        let mut fired = false;
        while Instant::now() < deadline {
            if service.poll() == Some(FireCause::Dirty) {
                fired = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(fired, "append was not observed: {:?}", service.stats());
    }

    fn write_ingest_state(dir: &Path, files: HashMap<String, FileState>) {
        let state = IngestState {
            next_doc_id: 2,
            files,
            opencode_databases: HashMap::new(),
        };
        state
            .save(&dir.join("state").join("ingest.json"))
            .expect("save state");
    }

    fn file_state_for(path: &Path) -> FileState {
        let metadata = std::fs::metadata(path).expect("metadata");
        let mtime = metadata
            .modified()
            .expect("mtime")
            .duration_since(UNIX_EPOCH)
            .expect("epoch")
            .as_secs() as i64;
        let modified_ns = metadata
            .modified()
            .expect("mtime")
            .duration_since(UNIX_EPOCH)
            .expect("epoch")
            .as_nanos()
            .min(i64::MAX as u128) as i64;
        FileState {
            size: metadata.len(),
            mtime,
            offset: metadata.len(),
            turn_id: 1,
            legacy_turn_id: None,
            parser_version: 1,
            pending_tool_calls: HashMap::new(),
            identity: FileIdentity {
                sqlite_wal: None,
                device: None,
                inode: None,
                prefix_sha256: None,
                prefix_bytes: 0,
                modified_ns: Some(modified_ns),
            },
        }
    }

    #[test]
    fn dirty_check_skips_unchanged_but_fires_on_change() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("memex");
        std::fs::create_dir_all(root.join("state")).expect("state dir");
        let paths = Paths::new(Some(root.clone())).expect("paths");
        let transcript = temp.path().join("session.jsonl");
        std::fs::write(&transcript, "{}\n").expect("seed");

        let key = transcript.to_string_lossy().into_owned();
        write_ingest_state(
            &root,
            HashMap::from([(key.clone(), file_state_for(&transcript))]),
        );

        let unchanged: HashSet<PathBuf> = HashSet::from([transcript.clone()]);
        assert!(!dirty_needs_ingest(&paths, &unchanged).expect("check unchanged"));
        assert!(!dirty_needs_ingest(&paths, &HashSet::new()).expect("check empty"));

        // Append: size change fires.
        std::fs::write(&transcript, "{}\n{}\n").expect("append");
        assert!(dirty_needs_ingest(&paths, &unchanged).expect("check appended"));

        // Unknown path fires.
        let unknown: HashSet<PathBuf> = HashSet::from([temp.path().join("brand-new.jsonl")]);
        assert!(dirty_needs_ingest(&paths, &unknown).expect("check unknown"));

        // Deleted path fires (tombstone sweep needed).
        std::fs::remove_file(&transcript).expect("remove");
        assert!(dirty_needs_ingest(&paths, &unchanged).expect("check deleted"));
    }

    #[cfg(unix)]
    fn backdate(path: &Path) {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let current = CString::new(path.as_os_str().as_bytes()).expect("cstring");
        let old = libc::timespec {
            tv_sec: 1_000_000,
            tv_nsec: 0,
        };
        let times = [old, old];
        let result =
            unsafe { libc::utimensat(libc::AT_FDCWD, current.as_ptr(), times.as_ptr(), 0) };
        assert_eq!(result, 0, "utimensat failed");
    }

    #[cfg(unix)]
    #[test]
    fn hot_sweep_reports_only_fresh_changes() {
        let _guard = env_lock();
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("memex");
        std::fs::create_dir_all(root.join("state")).expect("state dir");
        let paths = Paths::new(Some(root.clone())).expect("paths");
        let hot = temp.path().join("hot.jsonl");
        let cold = temp.path().join("cold.jsonl");
        std::fs::write(&hot, "{}\n").expect("seed hot");
        std::fs::write(&cold, "{}\n").expect("seed cold");
        backdate(&cold);
        let mut files = HashMap::new();
        files.insert(hot.to_string_lossy().into_owned(), file_state_for(&hot));
        files.insert(cold.to_string_lossy().into_owned(), file_state_for(&cold));
        write_ingest_state(&root, files);

        let mut service = test_service();
        // Both unchanged: nothing to do.
        assert!(
            service
                .hot_sweep_dirty(&paths, HOT_WINDOW)
                .expect("sweep")
                .is_empty()
        );
        // Append to both. Only the fresh one is a candidate; the 1970 file
        // is outside the window even though its bytes changed.
        std::fs::write(&hot, "{}\n{}\n").expect("append hot");
        std::fs::write(&cold, "{}\n{}\n").expect("append cold");
        backdate(&cold);
        let changed = service.hot_sweep_dirty(&paths, HOT_WINDOW).expect("sweep");
        assert_eq!(changed, vec![hot.clone()]);
        assert_eq!(service.stats().hot_hits, 1);

        // note_dirty feeds the normal debounce path.
        service.note_dirty(changed);
        assert!(!service.dirty_paths().is_empty());

        // Deleted hot file is reported for tombstone handling.
        std::fs::remove_file(&hot).expect("remove");
        let changed = service.hot_sweep_dirty(&paths, HOT_WINDOW).expect("sweep");
        assert!(changed.contains(&hot));
    }

    #[test]
    fn hot_sweep_does_not_revisit_cold_history() {
        let _guard = env_lock();
        let temp = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
        let hot = temp.path().join("hot.jsonl");
        std::fs::write(&hot, "{}\n").unwrap();
        let mut files = HashMap::from([(hot.to_string_lossy().into_owned(), file_state_for(&hot))]);
        // Missing historical files would be reported as deletions if the
        // hot sweep tried to stat them. Resync owns those cold tombstones.
        for index in 0..256 {
            let mut cold = file_state_for(&hot);
            cold.mtime = 1_000_000;
            files.insert(
                temp.path()
                    .join(format!("cold-{index}.jsonl"))
                    .to_string_lossy()
                    .into_owned(),
                cold,
            );
        }
        write_ingest_state(&paths.root, files);
        let mut service = test_service();
        for _ in 0..2 {
            assert!(
                service
                    .hot_sweep_dirty(&paths, HOT_WINDOW)
                    .unwrap()
                    .is_empty()
            );
        }
        std::fs::write(&hot, "{}\n{}\n").unwrap();
        assert_eq!(
            service.hot_sweep_dirty(&paths, HOT_WINDOW).unwrap(),
            vec![hot]
        );
    }

    #[test]
    fn hot_sweep_observes_held_open_database_wal_commits() {
        let _guard = env_lock();
        for name in [
            "opencode.db",
            "opencode-work.db",
            "antigravity-ide/conversations/session.db",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
            let database = temp.path().join(name);
            std::fs::create_dir_all(database.parent().unwrap()).unwrap();
            let writer = rusqlite::Connection::open(&database).unwrap();
            writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE messages (body TEXT);").unwrap();
            let mut state = IngestState::default();
            if name.starts_with("antigravity") {
                let mut file = file_state_for(&database);
                file.identity.sqlite_wal = Some(crate::state::SqliteWalIdentity::read(&database));
                // Main DB age must not remove WAL-backed databases from the sweep.
                file.mtime = 1;
                state
                    .files
                    .insert(database.to_string_lossy().into_owned(), file);
            } else {
                state
                    .opencode_databases
                    .insert(database.to_string_lossy().into_owned(), Default::default());
            }
            state.save(&paths.state.join("ingest.json")).unwrap();
            let mut service = test_service();
            // A newly tracked database gets one conservative reconciliation.
            assert_eq!(
                service.hot_sweep_dirty(&paths, HOT_WINDOW).unwrap(),
                vec![database.clone()]
            );
            assert!(
                service
                    .hot_sweep_dirty(&paths, HOT_WINDOW)
                    .unwrap()
                    .is_empty()
            );
            let main_before = StateSnapshot::read(&database);
            writer
                .execute("INSERT INTO messages VALUES ('committed while open')", [])
                .unwrap();
            assert_eq!(StateSnapshot::read(&database), main_before);
            // Reloading ingest state must not reset the WAL observation baseline.
            state.save(&paths.state.join("ingest.json")).unwrap();
            let changed = service.hot_sweep_dirty(&paths, HOT_WINDOW).unwrap();
            assert_eq!(changed, vec![database.clone()]);
            assert!(dirty_needs_ingest(&paths, &changed.into_iter().collect()).unwrap());
            assert!(
                service
                    .hot_sweep_dirty(&paths, HOT_WINDOW)
                    .unwrap()
                    .is_empty()
            );
            writer
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
            assert_eq!(
                service.hot_sweep_dirty(&paths, HOT_WINDOW).unwrap(),
                vec![database.clone()]
            );
            drop(writer);
            assert_eq!(
                service.hot_sweep_dirty(&paths, HOT_WINDOW).unwrap(),
                vec![database]
            );
        }
    }
}
