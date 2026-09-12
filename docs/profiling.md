# Index and search profiling

Profiling is opt-in at compile time. Default builds contain no capture backend, profiling environment checks, timers, counters, or evaluation of instrumentation arguments. The `profiling` feature adds the recorder; `MEMEX_PROFILE` enables one capture in that build.

```sh
cargo build --release --features profiling
MEMEX_PROFILE=/tmp/memex-refresh.json target/release/memex \
  --no-update-check --non-interactive search 'your query' --machine local
python3 scripts/profile_report.py /tmp/memex-refresh.json
```

The destination must not exist. On Unix it is created with mode `0600`. Search output stays on stdout. The profile is serialized after the command returns, including ordinary command errors. Panics, signals, `process::exit`, and forced termination do not guarantee a completed file. Long-running commands produce their report only when they return normally.

## Reading the capture

The JSON contains Chrome Trace Event `X` spans, in microseconds, on separate thread tracks. Open the local file in a compatible trace viewer. No upload or viewer launch is automatic.

`profile_report.py` reports inclusive and visible exclusive wall time by thread and phase, plus work counters. Wall time includes waits; it is not CPU time. Parent spans include their children. Concurrent thread durations must not be summed as request latency. `ingest.writer_wait` overlaps the writer thread's work.

To render a wall-time flamegraph with an installed Inferno CLI:

```sh
python3 scripts/profile_report.py /tmp/memex-refresh.json --folded > /tmp/memex-refresh.folded
inferno-flamegraph --countname microseconds /tmp/memex-refresh.folded > /tmp/memex-refresh.svg
```

Use `--thread ID` with `--folded` to render one thread's work separately. The JSON summary lists thread IDs. This avoids combining concurrent waits and work into a misleading request-latency total.

Use a sampling profiler separately for CPU stacks inside Tantivy, SQLite, embedding runtimes, and system calls. On macOS, `sample PID 5 1 -file /tmp/memex.sample` can attach to a running process. Sampling and tracing add overhead; compare uninstrumented and instrumented binaries on the same workload.

## Cost boundaries

- `cli.run`, `cli.search`, `search.local`: end-to-end command and local search.
- `ingest.lease_wait`, `ingest.freshness`, `ingest.discovery`, `ingest.file_check`: synchronization and discovery.
- `opencode.plan`, `opencode.hydrate`, `opencode.discover_legacy`: database and legacy-source work.
- `memory.refresh`, `git.metadata`, `analytics.resolve_metadata`: memory and repository metadata.
- `ingest.parse_file`, `ingest.writer`, `ingest.writer_wait`: producer/consumer work and waiting.
- `lexical.reader_open`, `lexical.source_ids`, `lexical.walk_records`, `lexical.search`: reader creation, cleanup lookups, full-record walks, and retrieval.
- `analytics.delete_path`, `analytics.delete_scope`, `analytics.flush`: SQLite deletion and accumulated writes.
- `lexical.stage`, `lexical.commit`, `lexical.merge_wait`, `lexical.publish`: lexical publication.
- `embeddings.model_init`, `embeddings.batch`, `vectors.coverage_check`, `vectors.backfill`, `vectors.load`, `vectors.stage`, `vectors.publish`: semantic indexing.
- `state.*`: ingestion, pending-publication, and scan-cache persistence.

Counters distinguish scheduled cleanup from actual work: legacy/database/scope deletion targets, source-ID queries and IDs found, SQLite deletion calls and rows deleted, reader opens, records walked/added/embedded, prefix reads/bytes, and files scanned/skipped/queued. Repeated scheduled deletions with zero matching IDs and zero deleted rows remain visible.

## Bounds and privacy

Capture is limited to 64 participating threads and 8,192 spans per thread. Counters continue accumulating when a thread's span buffer fills. The report includes dropped spans, refused threads, and unfinished spans. A partial capture is not a complete cost model; missing children inflate reported exclusive time. Buffers are per-thread, with no shared event-stream lock on the hot path. JSON conversion and file writes happen after measurement.

Only literal phase/counter names and numeric measurements are recorded. Query text, record bodies, source paths, session IDs, Git arguments, and thread names are excluded. The output path is not included in the report.

## Workload checks

Measure these separately:

1. Initial index creation or interrupted-publication recovery.
2. A repeated call within the scan-cache TTL.
3. A call after new messages and TTL expiry, with retrieval of the new content verified.

Record source-file and record counts, OpenCode legacy/SQLite coexistence, memory-document presence, segment count, and embedding model. Preserve the normal configuration and separate trace-export time from captured command time. Synthetic no-op fixtures alone do not establish live incremental latency.
