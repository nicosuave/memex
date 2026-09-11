# Refresh boundaries

`ingest::ingest_all` coordinates five domain operations:

1. Prepare/publish memory changes using the refresh-owned repository resolver.
2. Recover the stored ingestion checkpoint under the existing ingestion lease.
3. Observe sources and classify changes.
4. Parse with bounded backpressure and lazily prepare a writer when records or deletions exist.
5. Persist prepared analytics and publish the index/checkpoints.

## Ownership

- `repository.rs`: local-only gix discovery. A resolver belongs to one refresh; memory and analytics share it. No process-global repository cache or Git subprocesses.
- `ingest/discovery.rs`: filesystem/database observations and source-specific readiness. Transcript observations use one bounded operation-owned pool. OpenCode retains its failed/absent/ready distinctions and legacy fallback rules.
- `ingest/plan.rs`: pure file-change and refresh decisions. New, unchanged, appended, replaced, and parser-changed files are explicit cases. Refresh decisions distinguish unchanged, checkpoint-only, and index work.
- `ingest/execution.rs`: source parsing, record transforms, bounded delivery, and checkpoint assembly. Small workloads avoid unnecessary parser-pool dispatch.
- `ingest/publication.rs`: recovery operations and the writer. Staging is delayed until the first record or required deletion/vector work. Private staging cannot publish before the durable decision.
- `AnalyticsWriter::prepare`: resolves session facts and labels before SQL persistence. The returned prepared batch borrows the writer until commit, preventing interleaved accumulation. Deletions and inserts commit in one transaction.
- `MemoryStore::prepare_refresh`: produces a prepared snapshot while holding its write lock; publication is a separate operation.

The existing ingestion lease is retained across observation and execution. Checkpoints are loaded under that lease, so competing refreshes re-observe committed state. Read-only searches remain independent of the indexing writer. The [checkpoint storage contract](checkpoint-storage.md) defines sparse reads/deltas, migration, lifecycle locking, and recovery ordering.

## Cost contracts

- An unchanged refresh creates no lexical staging generation, writer, commit, or merge work.
- Full discovery for Claude, Codex, Pi, Omp, and Muse reuses directory stamps. Other providers retain their source-specific discovery. Each successful refresh records device, inode, and mtime for every directory the stamped walk enumerated, keyed by a fingerprint of the source roots, provider flags, and exclude patterns. The next full scan stats each stamped directory and reads only those whose stamp moved, taking the files of unchanged directories from the checkpoint. A directory's mtime changes on entry creation, removal, or rename and never on in-place appends, so known files keep their individual stat check. Stamps are written in the same checkpoint transaction as the file rows; a failed refresh persists none.
- Search-triggered refreshes on macOS replay the volume's persistent file-system event journal instead of walking. Every committed refresh stores the journal position it captured before reading anything, keyed by the discovery fingerprint plus the roots that existed. The next refresh replays events under the source roots from that position, unions them with the same sweep candidates the watch daemon stats — transcripts the checkpoint saw modified inside its hot window, plus every tracked database, whose writer can commit to the WAL for a whole session while the main file's mtime stays cold — and resolves the result exactly as the watch daemon resolves its dirty set: only those paths are stat-checked and parsed. FSEvents defers content-modification events for a file held open for writing, so that shared candidate set, not the event stream, is what bounds staleness for an active session. A directory rename or removal, a dropped or wrapped event range, a purged journal, a cursor more than 500,000 events behind (the event stream is host-wide, so activity on any volume counts toward that distance), a different volume, a root that started existing, a root that stopped being readable between the fingerprint and the replay, or a missing cursor falls back to the stamped walk; explicit `memex index` runs always walk.
- The replay runs on its own thread, registered with fseventsd before the process writes its lease or opens the checkpoint, and overlaps checkpoint recovery and memory refresh. A write on the volume in the milliseconds before registration makes fseventsd hold the answer for about 160 ms; a replay not answered within 50 ms of starting is abandoned and the refresh walks. The captured cursor is persisted either way.
- A parsed update with no indexable records advances checkpoints without opening a lexical writer.
- Metadata enrichment performs no subprocess calls. SQL persistence performs no source or repository discovery.
- Source-ID presence checks reuse one reader; analytics inventory returns only candidate paths.
- Record delivery remains bounded. Parsed whole-corpus records are never accumulated in a vector.
- Small known-size batches use a single writer. No refresh merges in the foreground: explicit `memex index`, daemon indexing, and search refresh all append with `NoMergePolicy`, and a refresh that added records schedules the detached compaction below. `memex index rebuild` indexes into a 1 GiB arena across the indexing threads with no merges and publishes the segments as written; the detached compaction folds them afterwards. Parse tasks start with the largest remaining input so one big transcript cannot become the tail of the parse phase, analytics receives each session's working directory from its parser instead of re-reading the transcript, and transcripts are mapped with `MADV_SEQUENTIAL` so a cold rebuild reads with a wide read-ahead window instead of the kernel's default fault-around. On a 200-file, 67k-record corpus the rebuild went from 14.7 s to 1.6 s and explicit indexing from 10.3 s to 1.6 s.
- When a refresh that added records leaves more than 8 small segments (under 5 % of the corpus, outside the three largest) it spawns one detached `memex index compact` process, which merges those small segments into one and exits. Segments at or above the 5 % share are left alone even when they fall outside the three largest. The spawn is skipped while any ingest holds the lease. A separate compaction lock stays held across the merge, so duplicate children exit without merging and at most one compaction runs at a time; no resident process is involved.
- Publication intent is written once after parsing has determined the final document-ID checkpoint and before shared record mutations. Existing pending recovery is retained on cancellation.

See [index-merge-cost-model.md](index-merge-cost-model.md) for the sustained aggregate comparison, including queries and terminal maintenance. See [profiling.md](profiling.md) for traces, counters, and per-thread wall-time flamegraphs. Initial creation, recovery, ordinary append updates, and no-op calls must be measured separately.
