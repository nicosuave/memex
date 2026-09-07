# Event-driven indexing: FSEvents / inotify spec

Started from latest `main` (`04adaca`). This spec replaces the daemon's
polling loop with OS filesystem events, with periodic reconciliation as the
correctness backstop. Events are **hints only** — `ingest.json` file-state
stays the source of truth.

## 1. Problem

`run_index_loop` (`src/cli.rs:1756`) sleeps `poll_interval` (default 30s,
`src/config.rs:424`) then calls `run_index_args` → `ingest_all`
(`src/ingest.rs:668`), which walks **every** source root, stats every file,
and diffs against `IngestState` (`src/state.rs:122`).

Costs:

- Worst-case ~30s index latency for an active agent session.
- A full walk + stat storm every 30s even when idle (battery, SSD, CPU).
- Walk cost grows with history size; parse cost is already incremental via
  `offset`/`turn_id`, but discovery is not.
- Lease contention with search-time `ingest_if_stale` (`src/ingest.rs:543`)
  and TUI auto-index (`src/tui.rs:1118`): the daemon wakes up just to find
  nothing changed and fight for the lease.

Goal: index a changed transcript in ~1–3s of settle time, do ~zero work when
idle, and never miss/duplicate records even when events are lost, coalesced,
or reordered.

Non-goals: changing the record format, the Tantivy writer protocol, the
`PendingIngest` crash-recovery protocol, or the vector/analytics pipelines.
Those are reused untouched.

## 2. Platform answer: yes, Linux has an equivalent — use `notify`, not raw APIs

| OS    | Native API              | Properties |
| ----- | ----------------------- | ---------- |
| macOS | FSEventStream (FSEvents) | Device-scoped, path-scoped stream, coalesced, persistent `since` event ID, directory-granularity history, `mustScanSubDirs` resync flag, latency parameter. Misses nothing unless the stream overflows, but tells you only *something under this dir changed*. |
| Linux | inotify                 | Per-watch-descriptor, **non-recursive**, fixed-size kernel queue. Reports file-granular create/modify/delete/move cookies, but drops events with `IN_Q_OVERFLOW` under load and requires the watcher to manage recursive watches itself (watch new subdirs on create, drop watches on delete/move, watch parents of not-yet-existing roots). |

Do **not** hand-roll `fsevents-sys` on macOS plus `inotify` on Linux.
Depend on the `notify` crate (`RecommendedWatcher`): FSEvents backend on
macOS, inotify backend on Linux, correct recursive-watch management included,
single API for both. This is the standard choice and the only way to keep the
two platforms' wildly different semantics behind one tested abstraction.
(`notify-debouncer-mini` or `notify-debouncer-full` supplies the coalescing
layer; prefer `debouncer-mini` — smaller, fewer surprises — unless the
`full` variant's rename-cookie tracking proves necessary in testing.)

Windows (`ReadDirectoryChangesW`) comes free via `notify` but is out of scope
for testing; the code must compile there, nothing more.

## 3. Requirements

### Functional

- R1. Daemon indexes appended/created transcripts within seconds, without any
  polling sleep in the hot path.
- R2. All currently polled sources are covered (section 5). Adding a source
  later means adding its roots in one place.
- R3. CLI/config stays backward compatible: `--watch` / `--watch-interval` /
  `--poll-interval` / `index_service_poll_interval` keep working, reinterpreted
  as event mode + resync interval (section 9).

### Correctness (no ghetto)

- C1. Events never advance trust directly. Every event path is re-`stat`ed and
  run through the existing `prepare_file_task` comparison (`src/ingest.rs:191`:
  size, mtime, `FileIdentity` device/inode/prefix-hash/modified-ns,
  parser-version). An event for an unchanged file is a no-op.
- C2. Full reconciliation always converges: startup scan, periodic resync,
  overflow resync, error resync, and delete/rename tombstone sweep (section 8).
  A daemon that missed *every* event still converges on the next resync.
- C3. No torn-read corruption: a file actively being appended is only parsed
  after settle (section 7). A partial trailing JSONL line must never advance
  `offset` past committed data — verify the parsers already skip/retain it;
  if any parser advances on error, fix that first.
- C4. No duplicate records on rename/rotation/truncation: reuse the exact
  `delete_first` paths (`size < previous.size`, mtime regression,
  `file_was_replaced`, parser-version change, Jcode atomic-reparse rule).
- C5. Crash safety preserved: the `PendingIngest` write-ahead marker
  (`src/state.rs:141`, `src/ingest.rs:1452`) and `prepare_pending_ingest_recovery`
  flow stay exactly as-is. Event mode must not clear/skip recovery.
- C6. Single-flight ingest under the existing `IngestLease` (`src/lease.rs`).
  Never run two ingests concurrently; never bypass the lease because "events
  are fast".

### Performance

- P1. Idle daemon does no walks, no stats, no wakeups except the resync timer.
- P2. A one-line append to one file stats ~1 file and parses the tail — never
  a full walk.
- P3. Bursty agent output (dozens of appends/second) coalesces into at most ~1
  ingest per debounce window per batch.
- P4. Dirty-set memory is bounded; overload degrades to "full resync needed",
  never to unbounded queues or OOM.
- P5. Embedding/vector work batching (`EMBED_BATCH_SIZE`, writer thread in
  `ingest_all`) is unchanged; event mode must not lower the batching that
  makes bulk ingest efficient.

## 4. Architecture

New `src/watch.rs` (name TBD, ~600–900 lines + tests), wired into the daemon
only:

```
OS events (notify) ──> filter ──> debounce/settle ──> dirty set ──> scheduler ──> ingest
                              │                                              │
                              └── resync timer / overflow / error ────────────┘
```

- **Root resolver**: pure function `watch_roots(options: &IngestOptions, config) -> Vec<PathBuf>`
  computed from the same inputs as `ingest_all` discovery. Canonicalizes,
  dedups, drops disabled sources, keeps not-yet-existing roots as "pending"
  (watch nearest existing ancestor).
- **Watcher**: one `RecommendedWatcher` held for the daemon lifetime.
  Recursive watches where supported; on Linux `notify` manages subdir watches.
- **Pipeline**: synchronous filter → debouncer → dirty-set insert. The ingest
  trigger fires on: debounce quiet-period expiry, max-batch-age expiry, or
  resync timer/overflow/error.
- **Scheduler**: single-flight. Holds `IngestLease` via `try_acquire`; on
  contention keeps the dirty set and retries with backoff instead of dropping.
  Runs the ingest on a dedicated thread so the watcher callback never blocks.
- **Ingest**: two tiers (section 7). Both tiers funnel into the existing
  `prepare_file_task` → parse → `writer_loop` machinery. No forked parse logic.

`memex daemon run` / `memex index --watch` construct this instead of
`run_index_loop`'s `sleep` loop. One-shot `memex index`, `search` auto-index,
TUI auto-index, RPC, MCP paths are untouched except that they benefit from
fresher state.

## 5. Watch roots (must all be covered)

Resolver output derives from the enabled-source flags in `IngestOptions`:

| Source | Roots to watch | Notes |
| ------ | -------------- | ----- |
| Claude | `default_claude_sources()` (`CLAUDE_CONFIG_DIR` or `~/.claude/projects`, `~/.config/claude/projects`) + explicit `--claude-path` | Watch the `projects` dirs, not `$HOME`. |
| Codex | `CODEX_HOME` (`~/.codex`): `sessions/`, rollout roots, history file parents | History files are single files — watch parent dir. |
| OpenCode | `OPENCODE_DATA_DIR` / `~/.local/share/opencode`: storage root, message/parts roots, `*.sqlite` parents | SQLite: trigger on main DB path only; ignore `-wal`/`-shm`/`-journal` sidecar events (they always accompany a DB write; coalesce, don't double-fire). Cursor logic (`event_rowid`/`event_id` in `scan_database`) stays authoritative. |
| Cursor | `~/.cursor/projects` | |
| Pi | `PI_CODING_AGENT_DIR` / `~/.pi/agent/sessions` (+ configured session root) | Env-driven; also watch `config.toml` (see below) and re-resolve roots when it changes. |
| OMP | `~/.omp/agent/sessions`, profiles root, `XDG_DATA_HOME` variant | |
| OpenClaw | `~/.openclaw`, `~/.clawdbot` state dirs | |
| Copilot | `COPILOT_HOME` / `~/.copilot` session root | |
| Grok | `GROK_HOME` / `~/.grok/sessions` | |
| Jcode | `JCODE_HOME` / `~/.jcode/sessions` | Single-JSON files: atomic reparse rule already exists; event just triggers it sooner. |
| Muse | `MUSE_HOME` / `~/.local/share/muse/sessions` | |
| Hermes | `HERMES_HOME` profiles | Currently discovery-only; include for free via `profile_roots()`. |
| Memory docs | Whatever `refresh_memories` (`src/ingest.rs:1701`) reads: project `memory/` dirs are per-project (unbounded) — **do not** watch the world. Watch only `~/.memex/memory/` outputs? No: inputs live in user repos. Decision: memory refresh stays on the ingest path (it runs inside every ingest, including event-triggered ones) and is *not* independently watched in v1. Document this explicitly. |
| Config | `~/.memex/config.toml` | Change → re-resolve roots (add/drop watches), re-read debounce/resync settings. Debounce this harder (5s); never trigger an ingest by itself. |

Nonexistent roots: watch the nearest existing ancestor so first-run creation is
observed, then escalate to the real root once it appears. Never `watch("/")`
or `$HOME` as a fallback — too broad, too many events, violates P1.

## 6. Event filtering (before debounce)

Drop in the watcher callback, in order:

1. Non-regular files: directories themselves (except create/delete, which mean
   "re-discover the subtree"), symlinks followed sideways (discovery uses
   `follow_links(false)` — the watcher must match that), sockets/fifos.
2. Sidecars and temp files: `*.tmp`, `*.swp`, `*~`, `.DS_Store`,
   `.memex-opencode-spool-*` (`OPENCODE_SPOOL_PREFIX`), Tantivy generation
   workdirs under `~/.memex/index` (never watch `~/.memex` itself), SQLite
   `-wal`/`-shm`/`-journal` (coalesced into the main-DB event).
3. Paths matching the existing `PathExcluder` (config `exclude_paths` + CLI
   `--exclude`). Canonicalize before matching, exactly like discovery does.
4. Events from our own state writes (`ingest.json`, `scan_cache.json`,
   `ingest.pending.json`) — these live under `~/.memex/state`, which is not a
   watch root, so this is defense-in-depth; assert it in a test.

What survives: create/modify/rename affecting `*.jsonl`, `*.json`, codex
history files, opencode `*.sqlite` main files, cursor/grok/copilot session
files. When in doubt, keep the event — `prepare_file_task` will no-op it
cheaply (one stat). Over-filtering causes silent misses; under-filtering costs
one stat.

## 7. Debounce, settle, batch, schedule

- **Per-path debounce**: 1–2s quiet period (configurable, default 1.5s).
  Streaming agents append many lines/second; each append resets the path timer.
- **Max batch age**: 5–10s. A continuously-appended file still gets indexed at
  least every max-age even without a quiet period.
- **Global batch window**: collect all settled paths; fire one ingest covering
  the whole dirty set. Minimum ingest spacing ~2s (prevents ingest thrash when
  5 agents write concurrently).
- **Settle check at fire time**: re-stat each dirty path; require size+mtime
  stable across the debounce window (two consecutive stats agree) *or* the
  quiet period fully elapsed with no new event. If still churning, keep it in
  the dirty set for the next batch — do not parse a file that grew in the last
  ~500ms. This plus the trailing-line rule (C3) eliminates torn reads.
- **Opencode SQLite**: extra settle — after the DB file settles, still
  `scan_database` from the stored cursor; if the DB is mid-checkpoint/locked,
  treat like the existing `Err(_) → files_skipped` path and retry next batch,
  not as fatal.
- **Backpressure**: dirty set capped (e.g. 10k paths). Overflow → set
  `full_resync_needed = true` and clear per-path detail. Queue depth and
  coalesced-event counters go to logs/metrics, not to unbounded memory.

### Ingest tiers

- **Tier 1 — dirty fast path** (hot path): for each dirty path, stat + build
  `FileTask` via the existing `prepare_file_task` with prior `FileState`.
  Skip unchanged (same size+mtime fast path already inside). Parse tails in the
  existing rayon pool, publish through the existing `writer_loop`. No directory
  walk. This is the whole performance win.
- **Tier 2 — scoped discovery** (on create/delete/rename/dir events, unknown
  paths, or any event whose parent dir isn't in state): re-run discovery for
  the affected source root only (e.g. one `discover_rollouts()` call), diff
  against `state.files` keys under that root to find tombstones, then proceed
  as Tier 1 for the delta. Never a full multi-source walk on the hot path.
- **Tier 3 — full resync** (section 8): literally call the existing
  `ingest_all` discovery path. Correctness backstop, not the hot path.

Tier 1/2 should be implemented as a `dirty_paths: HashSet<PathBuf>` parameter
threaded into `ingest_all` (or a sibling `ingest_dirty`) that constrains
discovery — not as a copy of the ingest body. One parse path, two discovery
scopes.

## 8. Reconciliation protocol (the correctness core)

Events are lossy on both platforms (coalescing, overflow, watcher gaps during
sleep/restart, missed subdir watches, rename half-reports). The design treats
every trigger below as funneling into Tier 3 unless the dirty set precisely
covers it:

1. **Startup**: always Tier 3 full scan before arming the watcher snapshot.
   Also runs `PendingIngest` recovery first (unchanged order). This bounds
   the offline window (daemon stopped, laptop asleep, `kill -9`).
2. **Periodic resync**: Tier 3 on a jittered timer, default 5–15 min
   (configurable; reuses `index_service_poll_interval` as the floor so existing
   configs keep meaning "at most this stale"). Jitter ±20% avoids lockstep
   with systemd/launchd neighbors. This is the backstop that makes every
   missed-event bug self-healing within minutes.
3. **Overflow/resync signals**: FSEvent `mustScanSubDirs` / history-drop, inotify
   `IN_Q_OVERFLOW`, `notify::EventKind::Other` resync markers, watcher `Error`
   of any kind → immediate Tier 3, then re-establish watches. Log at warn with
   the platform flag that caused it.
4. **Watch-gap detection**: watcher thread death, sleep/wake (detect via clock
   jump > resync interval or OS power notifications if cheap; clock jump alone
   suffices for v1), config-root change → Tier 3 + root re-resolution.
5. **Tombstone sweep**: deletes/renames may report only the old path (or only
   a dir event). Tier 2's per-root discovery diff against `state.files` finds
   vanished keys; vanished keys go through the existing `delete_paths` purge
   on both Tantivy and vector stores. Additionally, Tier 3's existing
   excluded/vanished handling stays as the final sweep. Never trust a delete
   event alone to name every affected session (directory renames move many).
6. **Opencode ownership drift**: DB compaction can reassign session ownership
   between DB files. The existing `owner_by_session` claim pass already
   reconciles this from scan output — event mode must run that pass on *any*
   opencode DB event (Tier 2 scoped to opencode roots), not just full scans.
7. **State-vs-index audit**: keep the `empty_index_rebuild` guard
   (`src/ingest.rs:687`) and the `PendingIngest` source-path invalidation on
   all tiers. If the index is empty but state is non-empty (or vice versa),
   Tier 3 rebuilds — events never paper over store divergence.

Invariant to assert in tests: **event-driven converge == poll converge**.
Given the same fixture mutation sequence, applying events-then-resync must
produce byte-identical `ingest.json` (modulo timestamps) and identical record
counts as a full `ingest_all`. Any divergence is a P0 bug in Tier 1/2 scoping,
not an acceptable approximation.

## 9. Config, CLI, and service integration

- New `UserConfig` keys (all optional, all with the defaults below):
  `index_service_watch_mode = "events" | "poll" | "hybrid"` (default `hybrid`
  at rollout, `events` after soak), `index_service_watch_debounce_ms` (1500),
  `index_service_watch_max_batch_ms` (8000), `index_service_resync_interval`
  (600; alias: existing `index_service_poll_interval` when the new key is
  unset, so current 30s configs become a 30s resync floor rather than breaking).
- CLI: `daemon run` gains `--watch-mode`, `--watch-debounce-ms`,
  `--resync-interval`; `--poll-interval` stays as an alias for the resync
  floor. Hidden legacy `index --watch/--watch-interval` maps to
  `watch_mode=events-or-hybrid` + resync floor for compat with installed
  launchd/systemd units (`build_index_command_args`, `src/cli.rs:6041` must
  emit the new flags for newly generated units; old units with `--watch` keep
  working).
- launchd (`KeepAlive`, `src/cli.rs:6148`) and systemd (`Type=simple`,
  `Restart=always`, `src/cli.rs:6285`) need **no** structural change — the
  daemon is already long-lived in continuous mode. Interval-mode units
  (`StartInterval` / `.timer`) are unaffected and stay poll-based.
- Shutdown: SIGTERM/SIGINT stops the watcher, flushes the dirty set with a
  short grace ingest (bounded, e.g. 5s), then exits. No event may be recorded
  as "handled" before its ingest commits and `ingest.json` saves.

## 10. Observability

Structured log lines (existing conventions) for: watcher start/stop, roots
added/dropped, events received/coalesced/filtered, debounce fires, dirty-set
size, tier chosen, ingest outcome (`records_added`, `files_scanned/skipped`
— same fields as today), resync cause (`startup`/`timer`/`overflow`/`error`/
`clock-jump`/`config-change`), lease-contention skips. Expose counters for a
future `daemon status`: `events_total`, `events_coalesced`, `ingests`,
`full_resyncs_by_cause`, `lease_skips`, `watch_errors`. A silent watcher is the
failure mode — if `events_total` stays 0 across two resync intervals while
resyncs find changes, log at warn (likely broken watch, e.g. wrong root).

## 11. Testing and acceptance

- Unit: filter table (sidecars/temps/excludes), debounce coalescing (100 rapid
  events → 1 ingest), backpressure overflow → resync flag, root re-resolution
  on config change, canonicalization matching `PathExcluder`.
- Integration (per source, macOS + Linux CI): append burst → eventually indexed
  with exact record counts; create → picked up without full walk (assert via
  walk counter or timing); rotate/truncate/same-size-rewrite → `delete_first`
  reparse, no duplicates; rename dir with N sessions → all N converge;
  delete → tombstone purge from index+vectors; torn write (append partial line,
  hold 200ms, complete) → no error, no offset skip; `kill -9` mid-ingest →
  restart converges via `PendingIngest` + startup scan.
- Opencode: dirty-session scan after DB append; locked-DB retry; ownership move
  between DBs converges.
- Soak: `fuzzer` appending/rotating across sources for X minutes; assert
  converge-equality with `ingest_all` (section 8 invariant) and zero record-id
  duplication.
- Resync proofs: synthesize overflow (fill inotify queue / set tiny
  buffer in test) → Tier 3 fires; stop watcher 60s, mutate, restart → startup
  scan converges.
- Perf: idle CPU/wakeups ~0 between resyncs; single-append ingest p50 < 3s
  including debounce; 10k-file corpus single-append does not walk.

## 12. Rollout

1. **Phase 1 — hybrid skeleton**: add `notify` dep, `watch.rs` with roots +
   filter + debounce + dirty set, Tier 3 triggers only (events cause an early
   `ingest_all` instead of waiting for the sleep). Correct by construction
   (still full scans), proves watcher coverage and filtering. Ship behind
   `hybrid`.
2. **Phase 2 — Tier 1/2 fast paths**: constrain discovery by dirty set /
   affected root. Prove converge-equality (section 8 invariant) in CI soak.
3. **Phase 3 — default flip**: `hybrid` → `events` default, poll kept as
   `--watch-mode poll` escape hatch. Raise resync default from 30s toward
   5–15 min once field data shows overflows are rare.
4. **Phase 4 — cleanup**: only after a release of soak data; remove the sleep
   loop if unneeded, keep `poll` mode for exotic filesystems (NFS/SMB where
   inotify/FSEvents don't fire — document this: network mounts stay on poll).

## 13. Open decisions (resolve before Phase 2)

- `notify` version + which debouncer (`mini` preferred; confirm rename-cookie
  behavior for atomic-save editors on both platforms).
- Exact debounce/settle numbers from a 24h field soak on a large corpus.
- Whether `scan_cache.json` TTL logic (`can_skip_fresh_scan`) stays as a
  second-level guard — recommendation: keep, it protects search-time paths.
- Memory-docs watching stays out of v1 (section 5) — confirm no large-memory
  workflow depends on sub-minute memory freshness.
