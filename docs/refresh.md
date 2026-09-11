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

The existing ingestion lease is retained across observation and execution. Checkpoints are loaded under that lease, so competing refreshes re-observe committed state. Read-only searches remain independent of the indexing writer.

## Cost contracts

- An unchanged refresh creates no lexical staging generation, writer, commit, or merge work.
- A parsed update with no indexable records advances checkpoints without opening a lexical writer.
- Metadata enrichment performs no subprocess calls. SQL persistence performs no source or repository discovery.
- Source-ID presence checks reuse one reader; analytics inventory returns only candidate paths.
- Record delivery remains bounded. Parsed whole-corpus records are never accumulated in a vector.
- Small known-size batches use a single writer. Ordinary CLI indexing, daemon indexing, and automatic search refresh use tiered automatic merging with Tantivy's `LogMergePolicy` minimum layer size set to one document; ordinary initial indexing uses this policy too. `memex index rebuild` retains the default bulk merge policy. Incremental tiers group small peers without clipping them into the default 10,000-document floor; they are not a byte or latency bound.
- Publication intent is written once after parsing has determined the final document-ID checkpoint and before shared record mutations. Existing pending recovery is retained on cancellation.

See [index-merge-cost-model.md](../benches/reports/index-merge-cost-model.md) for the sustained aggregate comparison, including queries and terminal maintenance. See [profiling.md](profiling.md) for traces, counters, and per-thread wall-time flamegraphs. Initial creation, recovery, ordinary append updates, and no-op calls must be measured separately.
