# Shared-segment refresh measurements

2026-09-08, Apple M1 Pro. Baseline: `64f6472`; candidate: `perf/shared-segments`. Storage semantics and compatibility are defined in [index-storage.md](index-storage.md).

## Workload

An isolated live-index snapshot contained 58 segments and 971,668 documents by Tantivy `max_doc`, including any deleted documents. Both sides received identical twenty-record JSONL appends through search-triggered refresh. Every query retrieved all twenty new records. Source discovery used a controlled home; this measures refresh against a real existing corpus, not a full live-home scan.

Automatic indexing was enabled, scan-cache TTL was zero, and embeddings were disabled. Automatic refresh retained its bounded merge policy. Runs alternated before/after order. Twelve iterations yielded ten measured samples after excluding migration and two warmup iterations. p95 is nearest-rank, so with ten samples it is the maximum.

## Unprofiled latency

| Twenty-record update | Baseline | Shared segments |
|---|---:|---:|
| Median | 457.32 ms | 311.04 ms |
| p95 | 466.71 ms | 318.70 ms |

These final-binary samples contained 86–95 segments after earlier benchmark appends. Each append added 3,740–3,780 bytes. Median latency fell 32.0%; p95 fell 31.7%. The 250 ms small-update target remains unmet. These are matched-workload comparisons.

The preceding production cohort measured 428.33 → 302.15 ms median and 447.55 → 321.25 ms p95, before the final reader-lease ownership fix and subsequent corpus growth. Do not pool the cohorts.

### Low-segment explicit indexing

A separate final-binary cohort used explicit indexing, its normal automatic merge policy, and 6–12 segments. Twenty-record appends added 3,920–3,960 bytes. Both sides started from the same compacted corpus; migration and two warmups were excluded.

| Explicit index command | Baseline | Shared segments |
|---|---:|---:|
| Median | 201.45 ms | 203.78 ms |
| p95 | 1,456.88 ms | 1,597.08 ms |

This cohort measured a 1.2% median increase and a 9.6% p95 increase. The longest calls on both sides coincided with segment counts falling to six. Query verification ran outside the explicit-index timer. The subsequent [paired merge cost model](index-merge-cost-model.md) identifies foreground compaction and durability stalls; frozen-input replay does not reproduce a consistent storage-specific slowdown.

## Phase traces and CPU flamegraphs

A separate ten-sample run with phase capture enabled measured:

| Phase | Baseline median / p95 | Shared median / p95 |
|---|---:|---:|
| Entire command, external wall clock | 398.33 / 425.88 ms | 294.23 / 299.86 ms |
| Generation staging | 118.96 / 122.17 ms | 13.34 / 14.77 ms |

After migration, updates reused 378–432 inherited references and added six shared files each. No inherited payload files were linked or copied.

For each side, one additional twenty-record update captured phase traces and Samply stacks together on the same invocation:

| Measurement | Baseline | Shared segments |
|---|---:|---:|
| Trace duration | 425.88 ms | 305.48 ms |
| Staging wall time | 139.34 ms | 14.95 ms |
| Reader-open spans, summed | 57.80 ms | 74.74 ms |
| Commit wall time | 60.99 ms | 63.52 ms |
| Publication wall time | 61.02 ms | 70.26 ms |
| Sampled `linkat` CPU across threads | 418.81 CPU-ms | 1.60 CPU-ms |

The CPU flamegraphs confirm elimination of inherited linking. Reader opens, commit, and publication now dominate the measured lexical work. Reader/publication costs increased, but did not absorb the staging savings. Normal publication still sweeps shared-store reachability; its cost scales with retained references.

CPU weights use Samply `threadCPUDelta` in microseconds and exclude zero-CPU samples. Concurrent worker CPU and overlapping phase spans must not be added to, or subtracted from, request wall time. Profiled and production cohorts ran separately; their difference is not an instrumentation-overhead estimate.

## Correctness checks

`cargo fmt --check` and `cargo clippy -- -D warnings` passed. The release library run passed 664 tests, with two existing Markdown performance tests ignored and two known baseline OAuth database-open failures explicitly excluded. The selected profiling-enabled concurrency, retrieval, memory, multi-machine, and profiling integration suites passed all 27 tests.

Coverage includes lease retention after dropping the parent `SearchIndex`, legacy adoption, staging-branch isolation, missing/corrupt references, interrupted publication, recovery, and GC. The final production executable contains the shared-storage format marker and no `MEMEX_PROFILE` marker.

## Receipts and limits

Artifacts reside at `/var/folders/1g/_mkn835s65sdrx90n97sh9nc0000gp/T/memex-segments-jl6nvdwp`:

- `final-production-results.json`, `bench_final.py`: final-binary automatic-refresh and low-segment explicit-index samples.
- `production-results.json`: preceding unprofiled cohort.
- `auto-v3-results.json`, `bench_auto_v3.py`: verified phase-capture samples and workload.
- `sample-before/`, `sample-after/`: same-invocation `trace.json`, `trace-summary.json`, Samply profiles, CPU folded stacks, and rendered flamegraphs.

An earlier low-segment explicit-index trial regressed from 195 to 210 ms before duplicate durability barriers were removed; it does not establish final low-segment performance. `auto-results.json` is an invalid baseline-versus-baseline trial and is excluded. Candidate identity was verified by its storage-format marker and inherited-reference counters.

The phase/CPU captures precede the final directory-owned reader-lease lifetime fix; the final production cohort includes it. All format-changing measurements used isolated snapshots.
