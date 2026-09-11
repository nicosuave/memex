# Foreground merge cost model

## Unchanged cleanup: 167.26 to 160.88 ms per update

2026-09-09. Skipping trailing cleanup-directory synchronization when no removal was attempted lowers charged aggregate elapsed time by 3.81% against installed `231dee2` (0.18.1). Both variants use corrected blocking waits. All data-protection barriers remain governed by the [storage contract](../../docs/index-storage.md#readers-and-collection).

| Repeat | Variant | 260 appends + queries | Terminal maintenance + final query | Total charged wall time |
|---|---|---:|---:|---:|
| 0 | Installed baseline | 39.31749 s | 4.00384 s | 43.32133 s |
| 0 | Candidate | 37.66683 s | 3.83476 s | 41.50159 s |
| 1 | Installed baseline | 39.78992 s | 3.86171 s | 43.65163 s |
| 1 | Candidate | 38.18654 s | 3.97013 s | 42.15667 s |

Both repetitions improve: 4.20% and 3.42%. Across 520 twenty-record updates per variant, charged time is 86.97296 versus 83.65826 seconds, or 167.26 versus 160.88 ms per update including its query and amortized terminal work. Candidate terminal work costs more in the second repetition and remains included.

The gain is distributed across the workload: candidate index/query pairs are faster in 226/260 and 224/260 cases, with median paired gains of 5.990 and 5.900 ms. These summaries retain every observation and do not replace the aggregate acceptance measure. Two repetitions establish a result for this workload, not a statistical bound or universal speedup.

### Work, correctness, and provenance

Each repeat starts both variants from the same 740,555-live-document, 58-segment shared-format seed with all 7,857 ingest-state entries. Each receives 260 successive twenty-record Claude appends and a separate exact-record query after each append. Pair and terminal order reverse across repeats. Embeddings and automatic query indexing are disabled; no preparatory ingestion, state pruning, omitted calls, or outlier exclusions occur.

All 2,088 charged subprocesses reconcile with blocking-wait timestamps and pinned binary roles. Per-variant native terminal helpers retain three original partitions and merge the remainder. All four endpoints have four segments, 745,755 live documents, and a deletion-free 42,939-document remainder. Full stored-document fingerprint: `deebb938875993e83c026b2bccecbd842815e51180418ced518708685a257860`. Retained fingerprints, query records, and checkpoint progression match. Five validation calls follow all timing; the owned source is restored afterward.

Baseline is the exact installed production executable, SHA-256 `a1be4e54db342d730c331738c49294ad7149e6b2c69ef1ce1bafaeaa2cd04d2a`. Its four executable/helper roles are imported from the preceding publication experiment with original compiler receipts preserved. All 205 non-document inputs match `231dee2`; only the cost-model document differs from that build's frozen source. Candidate production SHA-256 is `e9f9ded48deaa70538b074217e4b2a9879fc44595ca84068f8057eaa665c3e50`. Fresh baseline measurements were collected; previous run timings were not reused.

### Paired trace and CPU explanation

Separate diagnostics cover 260 append/query cycles per variant and both terminal endpoints, with same-invocation CPU samples at steps 0, 127, 255, and 259 and terminal maintenance. Across append calls, comparable `lexical.publish` spans fall from 10.27042 to 8.66788 seconds, or 15.60%. Nested strict publication synchronization stays nearly unchanged, 1.22891 versus 1.21956 seconds; staging also stays near 2.36 seconds. These parent/child spans overlap and cannot be added.

Candidate cleanup counters across 261 append/terminal publications:

| Cleanup pass | Calls | Trailing directory-sync requests | No-attempt skips |
|---|---:|---:|---:|
| Superseded generations | 261 | 261 | 0 |
| Legacy files | 261 | 0 | 261 |
| Shared files/owners | 261 | 34 | 227 |

Generation pruning still synchronizes every publication in this workload. The 33 merge-bearing appends synchronize shared cleanup; the 227 nonmerging appends skip it. Terminal maintenance adds one shared cleanup sync and one legacy skip. These are synchronization requests, not counts of all physical full flushes. Baseline lacks the new cleanup spans/counters, so its missing values are unavailable, not zero. Both variants still record 261 strict final-generation `lexical.full_syncs` calls.

CPU samples retain the original work. Initial total sampled CPU is 1,029.939 versus 1,063.490 CPU-ms, with merger stacks accounting for approximately 88–89%. Terminal CPU is 3,245.043 versus 3,170.327 CPU-ms and remains approximately 94–95% merger work, dominated by FST and postings paths. Inclusive categories overlap. Step 127 is not an individual win: publication spans rise from 45.085 to 50.171 ms and CPU rises from 91.539 to 100.896 CPU-ms. That sample stays in the diagnostic report; acceptance uses the complete production repeats.

Production append indexing accounts for most savings, 68.7733 versus 65.6268 seconds. Terminal maintenance plus final query totals 7.86555 versus 7.80490 seconds, only 60.653 ms lower overall; the second-repeat increase remains charged. This supports less trailing cleanup synchronization, not faster Tantivy merging or a quantified physical-I/O saving.

Remaining candidate append cleanup spans total 1.48460 seconds for generation pruning, 0.56659 seconds for shared collection, and 0.01525 seconds for legacy scanning. Ingest, pending-intent, and scan-cache saves remain at 4.24368, 2.63133, and 2.69206 seconds across 260 appends. All are diagnostic elapsed spans, not a disjoint decomposition of production time.

`analysis/{production.json,diagnostic.json,traces.json,attribution.json}` retains the combined accounting. The five CPU pairs have matching `.cpu-us.folded`, `.cpu.json`, and `.cpu.svg` artifacts. Initial, step-127, and terminal PNGs for both variants were rendered at 1600 px and visually inspected. Graph widths use positive Samply thread-CPU deltas, not elapsed occupancy; CPU is not subtracted from wall time to infer I/O wait.

### Validation and limits

Thirteen cleanup-filter tests pass in both candidate modes, including nine new cleanup cases. Tests directly observe synchronization decisions, permission-denied attempts, empty-owner removal with zero file count, dry-run behavior, and error propagation. The permission fixture ran as unprivileged UID 502. All five integration suites pass in each mode: 35 default and 38 profiling-enabled tests. Formatting, Clippy, source review, and independent production-accounting review pass.

The complete default library suite records 735 passes and the unchanged upstream FSEvents assertion failure, which also fails its single exact retry. Profiling passes 737 library tests. Four intentional library ignores remain; no tests are excluded. Imported baseline receipts retain their original FSEvents failures. This does not establish a uniformly green test suite or physical power-loss validation.

Evidence is under `/Users/srnnkls/Library/Caches/memex-cleanup-aggregate-20260909`: `baseline-import.json`, `imported-publication-receipts/`, source manifests/proofs, `artifact-readiness.json`, native build/test receipts, and `harness-readiness.json` identify the experiment. `production/{summary.json,rows.jsonl,calls.jsonl,fingerprints.jsonl,source-restoration.json}` retains complete timing and correctness evidence. The calibrated observer, interruption/mutation guards, and strict CPU classifier are unchanged apart from experiment paths and explicit baseline-import handling.

No installation, live-index mutation, commit, or push occurred. Host load and cache carryover remain uncontrolled. These are separate index/query processes on one corpus, not automatic-search, all-provider discovery, or cold-cache measurements. Do not add percentages or compare per-update figures across separate experiments as a matched result.

## Publication preparation: 177.28 to 163.63 ms with corrected timing

2026-09-09. Against freshly built rebased `9464029` (0.18.1), publication-preparation batching reduces sustained aggregate elapsed time by 7.70%. Both variants use blocking process waits; the baseline already contains shared segments, incremental tiers, and private staging batching. This comparison isolates publication preparation and does not reuse the installed 0.17.5 executable or earlier polling-based timings.

| Repeat | Variant | 260 appends + queries | Terminal maintenance + final query | Total charged wall time |
|---|---|---:|---:|---:|
| 0 | Rebased baseline | 42.25950 s | 3.80967 s | 46.06917 s |
| 0 | Candidate | 38.55922 s | 3.71595 s | 42.27517 s |
| 1 | Rebased baseline | 42.38356 s | 3.73444 s | 46.11800 s |
| 1 | Candidate | 38.60536 s | 4.20754 s | 42.81291 s |

Repeat reductions are 8.24% and 7.17%. Combined charged time is 92.18717 versus 85.08808 seconds across 520 twenty-record updates per variant: 177.28 versus 163.63 ms per update, including its query and amortized terminal work. Candidate terminal maintenance plus final queries total 7.92350 seconds versus 7.54411 baseline; that additional cost stays in the result.

### Work, endpoints, and identity

Each repeat starts from the same complete shared-format seed: 740,555 live documents in 58 segments, with all 7,857 ingest-state entries retained. The persistent fixture receives 260 twenty-record Claude appends and a separate exact-record query after each append. Pair and terminal order reverse across repeats. Embeddings and automatic query indexing are disabled. No preparatory ingestion, state pruning, omitted calls, or outlier exclusions occur.

All 2,088 charged subprocess calls reconcile with launch/exit-observation timestamps and per-case native binaries/helpers. Each variant pays its own terminal compaction. All four endpoints have 745,755 live documents, the same three retained partitions, and one deletion-free 42,939-document remainder in four segments. Full stored-document fingerprint: `f735004bd1cee1b617d5acf5e81f6fd673006e4ae25cc613c3c1c6c9c61ccdf8`. Query identities, checkpoint progression, and paired state checks match. Five seed/endpoint fingerprints run after all timing; the owned source is restored afterward.

Baseline production SHA-256: `2b0d5171b3c5badf20248ae390e4a26625506c6c271b76f7ceec0f6363b15532`. Candidate production SHA-256: `a1be4e54db342d730c331738c49294ad7149e6b2c69ef1ce1bafaeaa2cd04d2a`. The [storage contract](../../docs/index-storage.md#macos-staging-durability) defines the retained recovery barriers. After measurement, this exact candidate was installed at `/Users/srnnkls/.cargo/bin/memex` as version 0.18.1; `installation.json` records the verified replacement and preserved previous executable. No live-index migration was run. Later sections describe historical installations.

### Paired trace and CPU explanation

The separate diagnostic pass covers 260 append/query cycles per variant and both terminal endpoints. Same-invocation CPU captures accompany steps 0, 127, 255, and 259 and terminal maintenance. All ten CPU-weighted folded totals reconcile with positive Samply `threadCPUDelta` microseconds; the merge classifier excludes unrelated index-opening/policy names.

| Diagnostic elapsed span, mean per append | Baseline | Candidate |
|---|---:|---:|
| `lexical.publish` | 50.931 ms | 37.884 ms |
| `lexical.publication_fullsync`, nested inside publication | 0.615 ms | 4.747 ms |
| `lexical.commit` | 2.864 ms | 2.881 ms |
| `lexical.merge_wait` | 5.799 ms | 5.941 ms |
| `state.ingest.load` | 7.166 ms | 7.172 ms |
| `state.ingest.save` | 16.295 ms | 16.054 ms |
| `state.pending.save` | 9.600 ms | 9.888 ms |
| `state.scan_cache.save` | 9.307 ms | 9.416 ms |

Publication spans total 13.24199 versus 9.84988 seconds, a 25.62% reduction in this diagnostic cohort. The strict final barrier gets more expensive because it now drains the preparation batch; its cost is already inside publication. It must not be added again. Staging stays near 9.2 ms per append. Publication and full-state persistence remain substantial costs.

At sampled step 127, publication falls from 64.043 to 44.953 ms while total sampled CPU is 99.910 versus 101.148 CPU-ms. Initial merge CPU is 991.200 versus 994.679 CPU-ms; terminal merge CPU is 3,419.280 versus 3,444.437 CPU-ms. The initial and terminal flamegraphs retain the same dominant `IndexMerger`, FST-building/traversal, and postings work. Step 259 has zero merge CPU in both variants. Merge schedules match across all four production sequences, so reduced merge work does not explain this result. Inclusive CPU categories overlap and cannot be summed.

Every observed append/terminal publication has seven additional wrapped `fsync` calls and unchanged `lexical.full_syncs=1`. Seven is conditional on the owner/metadata branches; the full-sync counter still covers only the strict final capability barrier, not all remaining full-device flushes.

Production append indexing averages 142.833 versus 128.747 ms, and following queries average 19.942 versus 19.646 ms. Native terminal maintenance averages 3,756.042 versus 3,946.845 ms: 5.08% more candidate time, fully charged. These production operation means are separate from instrumented span timings. No CPU subtraction is used to estimate I/O wait.

`analysis/{production.json,diagnostic.json,traces.json,publication-attribution.json}` retains the combined accounting and attribution. Matching `.cpu-us.folded`, `.cpu.json`, and `.cpu.svg` files describe the five sampled pairs. Six initial/step-127/terminal PNGs were rendered at 1600 px under `analysis/png/` and visually inspected; their widths represent sampled CPU, not elapsed time.

### Validation and limits

Both candidate builds pass all 10 focused durability tests, including preparation/barrier failure and retry, exact preparation ordering, manifest-temp cleanup, device rejection, and fallback. Baseline and candidate integration suites pass 35 default and 38 profiling-enabled tests. Formatting, test compilation, Clippy, source review, and independent production-accounting review pass.

The complete library suites are not uniformly green: the unchanged upstream `watch::tests::fsevents_defers_modify_until_close` assertion fails when modify events arrive with the descriptor still open. Baseline default reports 724 passed/1 failed and both bounded retries fail; baseline profiling passes 726. Candidate default reports 726 passed/1 failed, followed by a passing exact retry; candidate profiling reports 727 passed/1 failed and its exact retry fails. Four intentional library ignores remain in every run. No tests are excluded, and no security checks are relaxed; canonical `TMPDIR` avoids the earlier OAuth path-alias failures. These watcher failures are retained, not reclassified as passing checks.

The result covers this corpus, host, append size, query cadence, and two production repetitions. It is not a statistical bound, a cold-cache guarantee, or a measurement of automatic-search or all-provider discovery. No physical power-loss test was performed, and the advisor's 95 ms projection remains unverified. Old polling-based percentages cannot be added to this reduction or compared directly with these per-update values.

Receipts are under `/Users/srnnkls/Library/Caches/memex-publication-aggregate-20260909`: `artifact-readiness.json`, frozen-source manifests/proofs, per-binary identities, `native-build-test-handoff.json`, `harness-readiness.json`, and `calibration/receipt.json` identify the build, validation, and timer. `production/{summary.json,rows.jsonl,calls.jsonl,fingerprints.jsonl,source-restoration.json}` retains the complete workload accounting and data checks.

## Historical timing correction

The earlier production tables below include subprocess-observation delay. Their harness used Python 3.13 `subprocess.run(timeout=1800)` with file-backed output; timed waiting polls with sleeps up to 50 ms. Observing process exit can therefore lag behind completion, with a duration-dependent bias that can differ between variants. The reported 20.63%, 66.10%, and 8.32% reductions are not validated pure command-latency reductions. They must not be used as a baseline for corrected measurements or adjusted by subtracting an estimated delay.

The replacement harness uses blocking process waits and an independent deadline with owned process-group cleanup. A 40-call native calibration retained all observations: median delay from the child's completion marker to parent observation was 10.6182 ms with timed polling and 0.4471 ms with blocking waits. These calibration values are not memex measurements or corrections to historical rows. Timeout/interruption, signal inheritance, and source-restoration checks passed. Receipts are under `/Users/srnnkls/Library/Caches/memex-publication-aggregate-20260909`, referenced by `harness-readiness.json`.

Historical raw rows remain intact. Their document-equivalence checks and same-invocation trace/CPU evidence remain useful; the external stopwatch totals have the limitation above.

## Historical staging-sync result: 285.43 to 226.53 ms per update

2026-09-09. Batching private lexical staging synchronization reduces total charged production time by 20.63% against the installed shared-storage build. Amortized cost falls from 285.43 to 226.53 ms per twenty-record update, including its query and terminal compaction. This is an additional comparison against the installed baseline, not a new measurement against upstream; percentages from separate experiments must not be added.

| Repeat | Variant | 260 appends + queries | Terminal maintenance + final query | Total charged wall time |
|---|---|---:|---:|---:|
| 0 | Installed baseline | 69.23970 s | 4.58110 s | 73.82080 s |
| 0 | Candidate | 54.36273 s | 4.48811 s | 58.85083 s |
| 1 | Installed baseline | 70.26618 s | 4.33756 s | 74.60374 s |
| 1 | Candidate | 54.51980 s | 4.42661 s | 58.94641 s |

Both repetitions improve: 20.28% and 20.99%. Across 520 updates per variant, 148.42454 seconds becomes 117.79724 seconds. Candidate terminal work is slower in the second repeat and remains fully charged. All 2,088 measured subprocess calls are retained; no outliers are excluded.

### Matched work and durability boundary

Both variants start from the same shared-format seed: 740,555 live documents in 58 segments and the complete 7,857-entry ingest state. Each receives 260 successive twenty-record appends followed by separate queries; pair order and terminal order reverse across repeats. Embeddings and automatic indexing on query are disabled, and both index only the controlled Claude source. No preparatory ingestion or state pruning occurs.

Per-variant native terminal helpers preserve the same three large segments and compact the remainder. All four endpoints match exactly: 745,755 live documents in four segments, unchanged retained partitions, and a deletion-free 42,939-document remainder. Full stored-document fingerprint: `0167bea1da08c3a97b9bd8d38c942916f6496f3045d5a1f643402772ac2bb9ad`. Five seed/endpoint fingerprints run only after every timed operation. Exact query records, checkpoint progression, and paired state hashes also match.

The [storage durability contract](../../docs/index-storage.md#macos-staging-durability) defines eligible layouts and publication ordering. Failure-injection tests verify ordering and retry; physical power-loss testing was not performed. The advisor's approximately 85 ms projection is not an observed result.

### Paired traces and CPU flamegraphs

A separate diagnostic pass completed 260 append/query cycles per variant and both native terminal endpoints. Each operation has a phase trace. CPU samples accompany the same invocations at steps 0, 127, 255, and 259, plus terminal maintenance: five pairs. Diagnostic timings are excluded from production acceptance.

| Diagnostic phase, 260 append calls unless noted | Installed baseline | Candidate |
|---|---:|---:|
| `lexical.commit` | 12.76169 s | 0.92110 s |
| `lexical.merge_wait` | 4.26460 s | 1.86172 s |
| `lexical.publish` | 13.83172 s | 13.86677 s |
| `lexical.stage` | 2.55211 s | 2.55920 s |
| `state.pending.save` | 4.28727 s | 2.47332 s |
| `state.ingest.save` | 4.55386 s | 4.37346 s |
| `state.scan_cache.save` | 2.43551 s | 2.49705 s |
| Query `cli.search`, 261 calls | 2.82596 s | 2.62291 s |
| Terminal `benchmark.terminal_merge`, one call | 5.52554 s | 5.24063 s |

Repeated commit spans fall by 11.84059 seconds; merge-wait spans fall by 2.40288 seconds. Publication and staging stay essentially unchanged. Enclosing `cli.run` falls from 54.31121 to 37.65176 seconds, but overlaps these phases and must not be added to them. Pending-state and analytics spans also improve despite unchanged code; host/cache interactions prevent assigning those differences to a separate implementation win.

The same-call CPU evidence distinguishes synchronization savings from reduced indexing work. At nonmerging step 259, commit elapsed time falls from 53.884 to 3.291 ms while total sampled CPU stays nearly unchanged, 72.543 versus 72.245 CPU-ms. Initial merge CPU is 1,234.213 versus 1,249.427 CPU-ms; terminal merge CPU is 4,284.865 versus 4,185.897 CPU-ms. Both variants' initial and terminal flamegraphs remain dominated by `IndexMerger`, FST traversal/building, and postings. FST/postings categories overlap merge stacks and each other; their weights are not additive.

Publication remains the largest named candidate phase at 53.334 ms per append. Ingest-state save averages 16.821 ms, pending save 9.513 ms, and scan-cache save 9.604 ms. The retained capability probe is visible in candidate step 127: 18.723 sampled CPU-ms under `durability::full_sync`/`fcntl`. Native production terminal maintenance remains approximately unchanged, averaging 4.439 versus 4.409 seconds. None of this later work is removed from the aggregate.

The candidate records 4,246 wrapped staging fsyncs across append and terminal operations. `lexical.full_syncs=261` counts only the newly wrapped final publication barriers; it excludes capability probes and all other full-sync call sites. Absence of this counter in baseline does not mean baseline performs zero full flushes. The new final-barrier spans total 166.731 ms, nested inside publication.

`analysis-v2/{production.json,diagnostic.json,traces.json}` and the matching `.cpu-us.folded`, `.cpu.json`, and `.cpu.svg` files contain the corrected analysis. Initial, step-127, and terminal graphs for both variants were rendered at 1600 px and visually inspected. Widths use positive Samply `threadCPUDelta` microseconds, not elapsed stack occupancy. CPU cannot be subtracted from wall time to infer physical I/O wait.

The initial analyzer's broad merge classifier also matched `open_or_create_for_ingest_with_merge_policy`. The corrected classifier matches verified Tantivy merger/merge-worker symbols: step 259 now correctly has zero merge CPU in both variants. `classifier-correction.json` records the correction; all raw captures, total CPU weights, folded/SVG files, trace totals, and production acceptance are unchanged.

### Identity and validation

The accepted production executable is installed at `/Users/srnnkls/.cargo/bin/memex` (0.17.5), SHA-256 `c0d4e39d1950911036967f718382ca4980df269714d55cd35463c51ff6eb194a`. It replaces benchmark baseline `86d4eb09784b40b603b087d0165aa4c5de02512d8368b9d704d2a626988cd32e`, preserved with the experiment artifacts. `installation.json` records the verified replacement; no live-index migration was run. The upstream and policy-only sections below describe earlier installations and measurements.

Default/profile library suites pass 671/672 tests, respectively, with four ignored and the same two documented OAuth exclusions. Default/profile CLI suites pass 2/5 tests. Eight new durability tests, formatting, and Clippy pass. Independent source and production-accounting review found no remaining defects. Builds and tests finished before production and diagnostic captures.

Receipts are under `/Users/srnnkls/Library/Caches/memex-durability-aggregate-20260909`: `preservation.json`, source manifests, `candidate-build-manifest.json`, and `seed-preparation.json` identify the exact inputs; `production/{summary.json,rows.jsonl,calls.jsonl,fingerprints.jsonl,identities.json,source-restoration.json}` retain timing and correctness evidence.

The initial smoke failed an overstrict harness assertion: baseline Tantivy `.managed.json` retained 77 stale entries after compaction while all committed payloads were present. `benchmark-v1.py` and `harness-correction.json` preserve that evidence. The corrected check accepts stale bookkeeping but still validates every committed manifest reference and payload; `smoke-v2-validation.json` records equal endpoints. Smoke timings do not enter acceptance.

This result covers one corpus, host, append size, and query cadence with two production repetitions. It does not establish cold-cache behavior, automatic-search single-command latency, all-provider discovery cost, or a statistical bound. Host load and cache carryover remain uncontrolled.

## Installed baseline versus upstream: 856.71 to 290.39 ms per update, amortized

The historical comparisons below use the older broad merge-CPU classifier, which includes some index-opening/setup work through `open_or_create_for_ingest_with_merge_policy`. Those category values are not pure merger CPU. Their acceptance wall measurements and raw CPU flamegraph widths remain valid.

2026-09-09. Against fetched `upstream/main` at `224e8a164e9f99f1d3fead780e067b152c7cea40` (0.18.1), the installed optimized build (0.17.5) reduces amortized index-plus-query cost, including terminal maintenance, from 856.71 to 290.39 ms per twenty-record update. Total charged time is 66.10% lower; upstream takes 2.95 times as long on this workload. Amortized cost divides the complete charged total by 520 updates per variant, not by the number of subprocesses.

| Production measure, both repeats | Upstream | Current installed build |
|---|---:|---:|
| Amortized cost per update, including terminal maintenance + final query | 856.71 ms | 290.39 ms |
| Mean append index + following query, excluding terminal work | 845.09 ms | 276.02 ms |
| Total charged time across 520 updates and both terminal endpoints | 445.48697 s | 151.00280 s |

These are combined optimizations versus actual upstream, not the incremental merge-policy-only comparison below. The earlier 8.32% result compares two already-optimized shared-storage builds and must not be substituted for, or added to, this upstream comparison. A roughly 75-second current run is the sum of 260 updates, their queries, and terminal work—not the latency of one update.

### Complete production repeats

| Repeat | Variant | 260 appends + queries | Terminal maintenance + final query | Total charged wall time |
|---|---|---:|---:|---:|
| 0 | Upstream | 215.50827 s | 3.26005 s | 218.76832 s |
| 0 | Current | 71.78884 s | 4.18669 s | 75.97553 s |
| 1 | Upstream | 223.93897 s | 2.77968 s | 226.71865 s |
| 1 | Current | 71.74092 s | 3.28635 s | 75.02727 s |

Current total cost is 65.27% lower in repeat 0 and 66.91% lower in repeat 1. Every measured append and query is retained, including the first call and merge stalls. Terminal maintenance plus its final query is more expensive for current in both repeats, but the total remains lower after charging it.

### Matched workload and terminal state

Both variants start each repeat with 740,555 live documents in 58 segments. Native legacy and shared-storage seeds have identical logical index payloads, index metadata, and persisted application state, but require different physical storage layouts. One-time format adoption and fixture preparation are outside timing; this is a steady-state update comparison, not cold-start or migration latency.

Each variant receives the same 260 successive twenty-record appends and a separate CLI query verifying all twenty newly indexed records after each append. Both use `--only-source claude`, disabled embeddings, and queries without automatic refresh. The controlled source starts empty at checkpoint zero. State is not pruned, and the comparison does not selectively skip provider work on one side. Within-pair execution order alternates. Separate index and query processes are timed; no single-command automatic-search latency is inferred.

Native terminal helpers use the same compaction parameters for each storage format, preserving the three large inherited segments and merging the remainder. Both perform schema preflight outside the clock; the upstream-only upfront helper guard is absent. All four production endpoints match exactly: 745,755 live documents in four segments, the same three original large segment IDs, and one 42,939-live-document remainder. The full live-record fingerprint is `1f86a67abb8578177b2a8c10a0074a9f4e15b0275902238a3bdda92fbf3b8922`; retained-segment and remainder fingerprints also agree. Endpoint fingerprints run after timed operations. Deferred work is charged to equivalent terminal states, not discarded at the last append.

### Paired diagnostic explanation

The separate diagnostic pass completed 260 index-plus-query cycles per variant and both terminal endpoints, with the same final fingerprint as production. Every operation has a phase trace; index steps 0, 127, 133, 255, and 259 and terminal maintenance have CPU samples from that same invocation. Upstream diagnostics use an instrumentation-only overlay, separate from the pristine upstream production binary. Diagnostic timings do not enter the production acceptance totals.

| Diagnostic phase, summed across 260 append calls unless noted | Upstream | Current |
|---|---:|---:|
| `lexical.commit` | 126.17759 s | 13.07594 s |
| `lexical.merge_wait` | 40.76932 s | 3.99119 s |
| `lexical.stage` | 6.97255 s | 2.28829 s |
| `lexical.publish` | 10.41591 s | 14.02460 s |
| `state.pending.save` | 7.44187 s, 520 spans | 5.18088 s, 260 spans |
| `state.ingest.save` | 6.32521 s | 4.48170 s |
| `state.scan_cache.save` | 3.04170 s | 2.80492 s |
| Query `cli.search`, 261 calls including final query | 1.67665 s | 2.73405 s |
| Terminal `benchmark.terminal_merge`, one call | 3.24504 s | 4.12035 s |

The largest named reduction is repeated commit work, 113.10 seconds across 260 appends, followed by 36.78 seconds less merge wait. These are phase differences in the diagnostic cohort, not an additive decomposition of the production saving. Enclosing writer and ingest waits overlap these phases and must not be added again.

Same-call samples expose repeated small-update overhead beyond the merge policy. At step 127, upstream has eight active Tantivy indexing workers, while current has one; commit wall time is 487.226 versus 49.432 ms. Upstream stacks include postings/FST construction, serialization, writes, and synchronization across those workers. Upstream also spends 21.575 sampled CPU-ms in `clone_generation`/`linkat`, versus 2.358 CPU-ms in current shared-file adoption. Together with lower aggregate staging time, these stacks support reduced repeated generation setup and small-batch writer work, not a claim that all saved time is CPU or physical disk I/O. All five sampled append pairs show upstream commit durations of 463–533 ms versus 47–53 ms for current.

Merge computation is also reduced. At step 0, both variants merge: merge wait falls from 2,360.936 to 1,073.749 ms, and inclusive merge-stack CPU falls from 2,179.255 to 908.748 CPU-ms. FST-stack CPU is 1,204.421 versus 554.783 CPU-ms; postings-stack CPU is 957.870 versus 423.284 CPU-ms. These inclusive categories overlap and cannot be summed. Later paired steps have different merge schedules, so their CPU differences are not matched per-merge algorithm comparisons; an unchanged net segment count does not establish absence of merge work.

The gains do not eliminate costs elsewhere. Current query search spans and publication cost more. Terminal maintenance has 3,234.907 sampled CPU-ms across threads versus 2,556.968 upstream, including 3,098.236 versus 2,432.315 CPU-ms in inclusive merge stacks. This later work remains in the production total. Production query means are 27.12 ms upstream and 28.65 ms current; the aggregate benefit comes from indexing, whose mean falls from 817.97 to 247.36 ms, not from faster queries.

The remaining current small-update cost is predominantly the repeated commit/publication and checkpoint persistence path: commit and publication spans total 13.076 and 14.025 seconds, with pending, ingest, and scan-cache saves at 5.181, 4.482, and 2.805 seconds. Parser spans total only 0.242 seconds. Current sampled stacks retain synchronization, manifest/state serialization, shared-directory access, and publication work. This identifies substantial remaining phase costs without establishing that any durability barrier is redundant.

CPU graphs use positive Samply `threadCPUDelta` weights, not elapsed stack occupancy. CPU is not added to trace wall time or subtracted from command wall time to manufacture I/O wait; nested spans and overlapping CPU categories remain separate. Selected samples explain those calls, not aggregate CPU for every operation.

### Production identity, scope, and receipts

The pristine upstream production binary has SHA-256 `9d53acc4727aa203e89de8f1f2460c9b51e45c60f08a8122c96cd4afa13ad6e9`. Current production is the installed accepted binary, SHA-256 `86d4eb09784b40b603b087d0165aa4c5de02512d8368b9d704d2a626988cd32e`. This benchmark changes neither repository source nor the installed binary.

Receipts are under `/Users/srnnkls/Library/Caches/memex-upstream-aggregate-20260909`. `upstream-head.json`, `upstream-source-manifest.json`, and `production-final/identities.json` identify the fetched source and measured binaries; `seed-preparation.json` records seed equivalence. `production-final/summary.json`, `rows.jsonl`, `calls.jsonl`, and `fingerprints.jsonl` retain complete accounting and correctness evidence. Raw rows reproduce all four run totals, all 260 sequential live-document increments per run, and all four equal endpoint fingerprints. The interrupted first `production/` attempt is excluded in its entirety, as recorded in `production-excluded.json`; the unchanged `production-final/` workload completed both repeats.

The observed reduction applies to this corpus, append size, and query cadence with embeddings disabled. Two repeats do not establish a statistical bound, universal 100 ms updates, cold-cache performance, or performance across all providers. Cache carryover and host load remain uncontrolled; verification queries are part of the measured workload.

`analysis/production.json` contains independently recomputed acceptance totals and per-operation means. `analysis/diagnostic.json` and `analysis/traces.json` retain aggregate and per-call diagnostic attribution; `diagnostic/` contains the original paired traces and profiles. CPU flamegraphs are `analysis/r0-{upstream,current}-{000,127,133,255,259}-index.cpu.svg` and `analysis/r0-{upstream,current}-terminal-maintenance.cpu.svg`, with corresponding `.cpu.json` and `.cpu-us.folded` files. `upstream-overlay-source-manifest.json`, `upstream-overlay-build-metadata.json`, and `upstream-profile-metadata.json` record the separate diagnostic overlay and profile binary; its test-helper-only revision does not change the measured CLI code.

## Earlier accepted policy-only result: lower total cost with incremental tiers

2026-09-09. The accepted `minlayer1` candidate reduces total charged production wall time from 85.25710 to 77.21606 seconds in repeat 0 and from 84.37620 to 78.29550 seconds in repeat 1: 9.43% and 7.21% lower. These totals include every append, following query, terminal maintenance, and final query. Across both repeats, 169.63329 seconds becomes 155.51156 seconds, an 8.32% reduction on this workload.

The current source uses Tantivy's size-tiered automatic merging for incremental indexing, not the rejected manual 128-segment batch. The routing contract is in [refresh.md](../../docs/refresh.md#cost-contracts). The accepted production build is now installed at `/Users/srnnkls/.cargo/bin/memex` (version 0.17.5, SHA-256 `86d4eb09784b40b603b087d0165aa4c5de02512d8368b9d704d2a626988cd32e`), with verification recorded in `/Users/srnnkls/Library/Caches/memex-tiered-aggregate-20260909/installation.json`; installation did not migrate the live index.

### Production accounting and common endpoint

The baseline and candidate use the same current source and shared immutable storage, differing only in the `LogMergePolicy` minimum layer size: 10,000 documents versus one. Each of two repeats starts both variants from the same 740,555-live-document, 58-segment seed. One common fresh APFS snapshot was adopted into shared storage through GC without ingestion. There is no preparatory ingestion, warmup exclusion, or excluded outlier. Each variant receives 260 successive twenty-record appends to a persistent index and a separate query verifying all twenty records after each append. Execution order alternates within pairs; production binaries have profiling compiled out.

| Repeat | Variant | 260 appends + queries | Terminal maintenance + final query | Total charged wall time |
|---|---|---:|---:|---:|
| 0 | Baseline | 82.03684 s | 3.22026 s | 85.25710 s |
| 0 | Candidate | 73.87122 s | 3.34483 s | 77.21606 s |
| 1 | Baseline | 81.47042 s | 2.90578 s | 84.37620 s |
| 1 | Candidate | 74.69604 s | 3.59945 s | 78.29550 s |

Terminal maintenance merges the remainder while preserving the same three large original segment IDs. All four production endpoints have four segments and 745,755 live documents, including one 42,939-live-document remainder segment. Full live-record fingerprints agree at `5197a6bfab171b84ff87d11d38a6bc2c00014cdacedab09dc5e39a5dfea76474`; retained-segment and remainder fingerprints also agree. Fingerprints run only after all timed operations, so verification scans cannot warm an earlier measured call. This endpoint charges deferred compaction rather than treating retained segments as free work. Baseline and candidate have 38 and 33 append steps with merges per repeat, respectively; that is a count of merge-bearing calls, not individual merge operations.

The candidate's terminal maintenance plus query costs more in both production repeats. Its append-plus-query savings exceed that additional cost. These are complete run totals, not a percentile improvement or an index-only result. Separate index and query processes are timed; this is not the automatic-search single-command route.

### Diagnostic attribution: less merge work, higher reader cost

A separate paired diagnostic pass completed all 520 index calls and their queries, plus terminal maintenance and final queries for both variants, reaching the same endpoint fingerprint. Every operation has a phase trace; selected index calls and terminal maintenance have Samply CPU stacks from the same invocation. Diagnostic wall totals are not production acceptance measurements.

| Diagnostic phase, summed across calls | Baseline | Candidate |
|---|---:|---:|
| Append `lexical.merge_wait` | 16.92567 s | 4.04435 s |
| Append `lexical.commit` | 13.17547 s | 13.51799 s |
| Append `lexical.publish` | 13.38121 s | 13.90936 s |
| Append `lexical.reader_open` | 1.03953 s | 1.85921 s |
| Query `cli.search`, including final query | 1.53182 s | 2.58240 s |
| Query `lexical.reader_open`, including final query | 0.82628 s | 1.80575 s |
| Terminal `benchmark.terminal_merge` | 3.25560 s | 3.40226 s |

The 12.88-second reduction in accumulated append merge-wait spans is accompanied by higher reader-open, query, commit, and publication time. Query reader-open spans overlap enclosing search spans; these rows are not an additive wall-time decomposition. The candidate groups small peers instead of repeatedly placing them in the baseline's 10,000-document floor, but retaining more tiers can increase read costs.

The step-0 paired capture shows this mechanism on both wall and CPU axes: merge wait falls from 2,390.573 to 1,090.380 ms, and inclusive merge-stack CPU falls from 2,070.376 to 989.709 CPU-ms. FST-stack CPU falls from 1,205.954 to 620.987 CPU-ms; postings-stack CPU falls from 983.434 to 484.990 CPU-ms. These inclusive CPU categories overlap and must not be added.

The change does not make each append cheaper. At step 127 the candidate merges and the baseline does not: merge wait is 164.787 versus 0.095 ms. At step 133 the baseline merges and the candidate does not: 83.856 versus 0.049 ms. Terminal maintenance also has more candidate sampled CPU, 3,137.055 versus 2,593.005 CPU-ms across threads, with inclusive merge-stack CPU of 3,018.533 versus 2,474.220 CPU-ms. This later work is charged in the production totals rather than hidden behind faster selected calls.

Traces describe elapsed phases; positive Samply `threadCPUDelta` values describe sampled CPU. CPU is neither added to overlapping wall spans nor subtracted from command wall time to invent an I/O estimate. The sampled pairs explain specific calls, not aggregate CPU for every unsampled operation or per-merge algorithm comparisons when only one variant merges.

With merge waits reduced, repeated commit, publication, and checkpoint persistence remain substantial. Candidate pending-journal, ingest-state, and scan-cache save spans total 5.229, 4.323, and 2.860 seconds respectively, compared with 0.250 seconds in parser spans across 260 appends. These named phase costs identify the remaining durability/persistence path, not a proven redundant barrier or a measurement of physical I/O.

### Scope, receipts, and validation

The lower charged total holds for both completed production repeats of this corpus, append size, and query cadence. It is not a universal 100 ms target, a cold-cache guarantee, or a statistical bound from two runs. Alternating order does not eliminate host load or filesystem/cache carryover; verification queries are part of the workload and warm subsequent calls. Different corpus sizes, segment histories, and query-to-write ratios can change the trade-off.

Receipts are under `/Users/srnnkls/Library/Caches/memex-tiered-aggregate-20260909`:

- `production-v2/summary.json`, `rows.jsonl`, `calls.jsonl`, and `fingerprints.jsonl` contain the complete accepted production accounting and endpoint evidence. `analysis/production.json` independently checks totals, merge-bearing steps, and endpoint equality.
- `snapshot.json`, `source-equality.json`, `baseline-vs-candidate.diff`, and the binary `*.identity.json` files identify the common seed, source difference, and binaries.
- `diagnostic/` retains the separate paired captures. `analysis/diagnostic.json` and `analysis/traces.json` contain aggregate and per-call attribution.
- `analysis/r0-{baseline,candidate}-{000,127,133,255,259}-index.cpu.svg` and `analysis/r0-{baseline,candidate}-terminal-maintenance.cpu.svg` are CPU-weighted paired flamegraphs, with matching `.cpu.json` attribution and `.cpu-us.folded` stacks.

The step-0 and terminal-maintenance CPU flamegraphs for both variants were rendered and visually inspected; their widths represent CPU, not wall time.

The aborted `production/` attempt and the failed terminal harness invocation using an incompatible UUID format are excluded entirely. The corrected `production-v2/` workload is complete; no rows from failed attempts enter its totals.

Validation receipts in `build-logs/` record 663 default-build and 664 profiling-enabled library tests passing, with four ignored and two known OAuth failures explicitly excluded in each configuration. CLI integration tests passed 2/2 in the default build and 5/5 with profiling. `cargo fmt --check` and `cargo clippy -- -D warnings` passed. No builds or tests ran during the benchmark passes.

## Historical rejected 128-segment batching result

The historical tables below are retained, but their old local `/tmp` receipts were deleted during cleanup and are no longer available for independent reinspection. Their cohorts and exclusions are separate from the complete production-v2 comparison above.

2026-09-09. The sustained comparison rejected the fixed-fixture recommendation below. The installed `9ea0645` policy made average index-plus-query latency 9.2% worse on that workload. It improved p95/p99 but produced a larger worst-case compaction stall. The earlier 85.5% reduction applies only to the selected merge-triggering fixture.

Both variants use shared immutable storage. The comparison isolates the ordinary CLI merge-routing change; it does not invalidate the earlier shared-file reuse improvement.

### Sustained production workload

Each variant received 260 successive twenty-record appends against its own persistent index, followed by a separate query verifying all twenty new records. Unlike the frozen replays, generations and deferred work accumulated. Both started from the same pre-merge snapshot and completed with 746,595 live documents; every intermediate live-document count was checked. One preparatory ingestion consumed the existing pending append before measurement. All 260 measured calls are retained, including compactions.

Execution order alternated within each pair. Production binaries had profiling compiled out. A separate pass repeated the identical input sequence with phase traces on every call and sampled CPU captures at selected points, including both candidate compactions. No builds or tests ran during these passes. Query timing includes the separate CLI process; index-plus-query is the sum of those two timed calls and is not a measurement of the automatic-search single-command route.

| Sustained production measurement | Previous routing | Installed bounded routing |
|---|---:|---:|
| Index mean | 274.34 ms | 286.90 ms |
| Index median | 244.56 ms | 255.99 ms |
| Index p95 | 362.78 ms | 316.79 ms |
| Index p99 | 1,534.14 ms | 484.35 ms |
| Index maximum | 1,707.53 ms | 3,718.48 ms |
| Query mean | 17.30 ms | 31.70 ms |
| Index + query mean | 291.64 ms | 318.60 ms |
| Index + query median | 260.78 ms | 291.58 ms |
| Index + query p95 | 378.78 ms | 358.90 ms |
| Index + query p99 | 1,549.83 ms | 499.63 ms |
| Index + query maximum | 1,722.86 ms | 3,734.22 ms |
| Compaction calls | 37 | 2 |
| Observed segment range | 6–13 | 4–130 |
| Final segments | 7 | 19 |

Percentiles use nearest rank. Two candidate stalls exceed one second, versus five baseline stalls. Because two of 260 calls are less than 1%, candidate p99 excludes both expensive compactions; maximum and amortized mean are essential alongside p99.

### Combined trace/flamegraph findings

The candidate compacts on zero-based steps 117 and 244. In the paired pass those commands took 4,124.90 and 2,931.30 ms. CPU flamegraphs attribute 3,090.47 and 1,945.58 CPU-ms to the merge worker, including 2,078.62 and 1,247.04 CPU-ms in term-dictionary/postings paths. This is deferred merge computation returning to the foreground.

The existing `lexical.merge_wait` spans misleadingly remain only 0.084 and 0.061 ms. Manual bounded compaction blocks inside `maybe_compact_continuous_segments`, before that span. The trace intervals between commit completion and merge-wait entry are 3,788.11 and 2,562.68 ms. Combined with the merge-worker stacks and the publication call order, these identify the previously unlabelled compaction interval. A near-zero `lexical.merge_wait` alone does not establish absence of merge work.

Retained segments also increase read cost. Within the candidate's non-compacting calls:

| Segment count | Index median | Following query median |
|---|---:|---:|
| Up to 32 | 229.01 ms | 19.78 ms |
| 33–64 | 235.53 ms | 26.94 ms |
| 65–96 | 258.72 ms | 35.49 ms |
| 97–132 | 302.03 ms | 43.33 ms |

These strata are observational and correlated with progression through the workload, not an independently randomized per-segment cost estimate. They expose a cost that fresh-fixture replays excluded.

### Corrected conclusion and remaining uncertainty

The rejected 128-segment / 256 MiB policy traded frequent smaller work for accumulated reader overhead and rare larger stalls. Fixed-state replay proved removal of one immediate merge; it did not establish lower sustained or amortized cost. This motivated the incremental tiers measured above; the historical recommendation for a tighter manual byte budget was not the accepted implementation.

Alternating order limits, but does not remove, filesystem/cache carryover. First/second-in-pair index medians were 244.94/243.85 ms for the baseline and 253.79/258.20 ms for the candidate. Host load and child resource usage are retained in raw receipts. Cache contents and host I/O were not independently controlled, verification queries warmed each index between appends, and there is only one sustained production sequence per variant. The direction and mechanism are supported for this workload; there is no population-wide latency guarantee or precise attribution of every outlier.

Receipts: `/tmp/memex-sustained-final.7a55a1cf` contains `sustained.py`, input snapshots, binary hashes, `results.jsonl`, `summary.json`, and the summarizer. `paired-after-117/` and `paired-after-244/` contain the same-call traces, CPU profiles, resolved stacks, and rendered flamegraphs; `paired-before-6/` provides the earlier smaller-merge comparison. All 1,040 indexing calls across production and traced passes retained expected live-document counts and retrieved their twenty new records. The aborted pilot at `/tmp/memex-sustained-cost.o2mVWy` is excluded.

## Earlier fixed-state diagnosis

2026-09-08, Apple M1 Pro. Synchronous compaction explains the multi-second explicit-index tail. The earlier shared-storage p95 increase is an observed cohort result, not a reproducible storage-specific penalty: frozen-input replays did not retain a consistent slowdown.

Diagnostic baseline: `64f6472`. Diagnostic candidate: the reader-lease-safe shared-storage implementation. The investigation below preceded the CLI routing change; [implementation verification](#implementation-verification) measures that change separately.

## Workload and controls

The frozen pre-merge corpus had 741,375 live documents in twelve segments. Every replay added the same twenty records, 3,640 JSONL bytes, from the same source path and checkpoint. Explicit indexing merged eight segments into one and finished with six segments and 741,395 live documents.

The merge bucket contained an existing 9,607-document segment plus six twenty-document segments before the append. Existing input files totaled 52,643,682 bytes. The new twenty-document segment triggered the merge. File lengths describe logical input volume, not measured physical disk traffic.

Each replay used a fresh fixture cloned from the frozen pre-merge state. Immutable files were hard-linked during fixture setup, outside timing; metadata and leases were copied. Both sides had already migrated/warmed as appropriate. Baseline/candidate execution order alternated. No builds or tests ran during capture.

Three evidence sets are kept separate:

- Ten same-invocation phase-trace/Samply captures per side in an evolving corpus, with two merge-triggering pairs. The initial format-migration call is excluded.
- Six frozen-input production replays per side, with the first pair excluded; four additional frozen-input paired trace/Samply replays per side.
- Six production calls and two paired captures of the existing bounded search-refresh path from the candidate's frozen corpus. The first production call is excluded. Unselected OpenCode discovery checkpoints were cleared only in these isolated fixtures, preventing cleanup of absent providers. Assertions require zero such deletions and preservation of the full live-document count.

Every call retrieved all twenty new records. Frozen explicit and valid bounded fixtures retained 741,395 live documents. The bounded path retained thirteen segments instead of six: it deferred compaction, without skipping ingestion.

## Frozen-input results

| Workload | Baseline | Shared segments |
|---|---:|---:|
| Explicit index, production median, five samples | 1,913.67 ms | 1,783.27 ms |
| Explicit index, production range | 1,746.13–2,187.87 ms | 1,740.69–2,418.62 ms |
| Explicit index, paired-capture median, four samples | 1,717.91 ms | 1,684.57 ms |
| Bounded search refresh, production median, five samples | — | 257.57 ms |
| Bounded search refresh, production range | — | 243.48–267.37 ms |
| Bounded search refresh, paired-capture range | — | 221.73–222.18 ms |

These sample counts do not establish population p95 or statistical equivalence. Production and sampled cohorts ran separately; their difference is not a measurement of profiler overhead. The original [low-segment measurements](shared-segments-benchmark.md#low-segment-explicit-indexing) remain valid observations, but their 9.6% p95 increase cannot be treated as a stable regression coefficient.

## Traces locate the critical path; CPU stacks explain it

Four frozen paired captures per side give these medians:

| Measurement | Baseline | Shared segments |
|---|---:|---:|
| Staging wall time | 34.78 ms | 16.60 ms |
| Commit wall time | 57.02 ms | 64.31 ms |
| Merge-wait wall time | 1,500.55 ms | 1,454.49 ms |
| Publication wall time | 37.43 ms | 59.11 ms |
| Merge-worker CPU | 1,355.36 CPU-ms | 1,309.83 CPU-ms |
| Term-dictionary/postings CPU within that worker | 1,053.46 CPU-ms | 1,012.68 CPU-ms |
| Sample-held merge-worker `sync_all → fcntl` time | 72.62 ms | 77.89 ms |

CPU flamegraphs put roughly 77% of merge-worker CPU in FST/term-dictionary and postings paths. Remaining work includes copying, serialization, and writes. Shared-file lookup and GC are not the dominant CPU stacks. Publication costs about 22 ms more at the median, largely offset by staging's 18 ms saving in this small-segment cohort. These are phase medians, not an additive latency decomposition.

One representative candidate call, `paired-after-1`, has the following non-overlapping trace intervals. Their sum plus the uncovered remainder equals the measured command duration:

| Critical-path component | Wall time |
|---|---:|
| Staging | 17.17 ms |
| Memory refresh, state load, reader open, parsing | 15.16 ms |
| Pending journal and analytics persistence | 26.49 ms |
| Commit | 66.53 ms |
| Waiting for merge completion | 1,501.46 ms |
| Publication | 60.28 ms |
| Final ingest/scan checkpoints | 28.47 ms |
| Uncovered setup, handoffs, teardown | 15.59 ms |
| Total | 1,731.14 ms |

The writer calls `wait_merging_threads()` before publication in `src/ingest/publication.rs:227`. The main thread's `ingest.writer_wait` overlaps this work; it is not another 1.6 seconds to add. The merge worker's CPU is the work underlying the wait, not an extra latency term.

### Why twenty records trigger that much work

Tantivy 0.22.1's `LogMergePolicy` clips segment sizes to a 10,000-document floor and starts a merge when a level has eight segments. In this fixture, a 52.6 MB segment and tiny twenty-record segments enter the same bucket. The policy uses document counts, not bytes or a foreground latency budget.

The resulting local model is:

```text
explicit-index latency = fixed publication path
                       + foreground merge completion, when the bucket fills
```

For these inputs, fixed work is roughly 0.22–0.27 seconds; merge completion adds roughly 1.4–1.6 seconds in the frozen paired cohort. Vocabulary/postings work, byte volume, durability waits, and scheduling determine merge cost. A per-record or per-byte coefficient cannot be generalized from this fixture.

## What caused the noisier initial regression?

In the evolving diagnostic cohort, merge waits grew from 1,435 → 1,811 ms and 1,660 → 2,574 ms. Merge-worker CPU grew much less: 1,283 → 1,304 CPU-ms and 1,324 → 1,449 CPU-ms.

The first candidate outlier spent about 340 ms in sampled `sync_all → fcntl` stacks, versus 82 ms for its baseline partner. About 293 ms was beneath inverted-index serializer closure. The second had longer sampled intervals across postings/term reads and writes, without proportional CPU growth. These expose sync stalls and additional non-CPU elapsed time; they do not distinguish filesystem paging from scheduler contention or prove either was caused by the storage layout.

The frozen replays brought sync-stack occupancy back to similar ranges on both sides. Thus the supported conclusions are a deterministic foreground-compaction cost and a smaller publication/staging trade-off. A persistent storage-induced merge slowdown remains unproven.

## Remaining bounded-refresh cost

Valid bounded captures have no sampled merge computation and less than 0.1 ms in merge wait. They still spend about 62 ms in commit, 45–47 ms in publication, and 54–56 ms across the pending/final checkpoint writes. Reader opens total about 12 ms for three calls at thirteen segments. Sampled CPU across all threads is 67–68 CPU-ms, with file open/sync, state serialization, and shared-directory lookup visible in the stacks.

The 19 ms parser span is not 19 ms of JSON computation: it overlaps 18 ms of staging, and sampled stacks show `RecordSender::send → crossbeam → park`. The record channel capacity is eight (`src/ingest/mod.rs:39`), so the twenty-record producer encounters backpressure while the lazy writer opens. Do not add parser and staging durations or infer a parser bottleneck from that span.

The earlier roughly 75 ms reader-open result belongs to a much larger segment-count cohort. Reader reuse remains relevant there; it does not explain this low-segment tail.

## Initial policy rationale (superseded by sustained results)

Keep this 52.6 MB rewrite off the foreground small-update path. Reuse the existing bounded incremental policy rather than weakening durability or optimizing FST internals first. Keep bulk compaction explicit and retain a bound on deferred segment growth.

The rejected continuous policy used a 128-segment threshold and a 256 MiB input cap; that byte cap was not a 250 ms latency guarantee. The counterfactual proves that deferring this merge preserves ingestion and removes its CPU work. It does not erase maintenance debt or prove future bounded compactions will meet the target. Even the five-sample production bounded cohort still misses a 250 ms tail budget.

## Implementation verification

This historical verification concerns the rejected bounded policy, not the accepted incremental tiers. Ordinary CLI `index` then selected the existing bounded constructor; `index rebuild` / `reindex` kept the bulk constructor. The redundant CLI `continuous` flag was removed. Compaction thresholds and recovery ordering were unchanged in that comparison; the safety-limit error named `memex index rebuild`. The current canonical routing contract is in [refresh.md](../../docs/refresh.md#cost-contracts).

This comparison uses the shared-storage build on both sides, differing in CLI routing. Both run the same explicit-index command against copies of the candidate's frozen pre-merge fixture and consume the same 3,640-byte append. No provider checkpoints are cleared in this comparison. All thirty calls retrieved the twenty new records and retained 741,395 live documents. Every candidate call retained all twelve original segment IDs and added one; baseline calls merged down to six.

Twelve production iterations per side alternate execution order. The first two are warmups, leaving ten measured calls. Three additional pairs capture traces and Samply stacks together on the same invocation.

| Explicit-index measurement | Previous routing | Bounded routing |
|---|---:|---:|
| Production median | 1,839.75 ms | 266.02 ms |
| Production p95, nearest-rank | 11,295.96 ms | 369.14 ms |
| Paired-capture median | 1,669.47 ms | 228.11 ms |
| Merge-wait median in paired captures | 1,372.96 ms | 0.047 ms |
| Merge-worker CPU median | 1,170.71 CPU-ms | 0.048 CPU-ms |

Median production latency fell 85.5% on this merge-triggering fixture. The paired CPU flamegraphs lose the expensive FST/postings merge, and traces lose the matching foreground wait. The remaining merge-thread samples are idle-thread housekeeping, not compaction. Candidate paired calls spend 57–60 ms in commit and 56–64 ms in publication.

The baseline's unprofiled 11.30-second outlier has no same-call trace, so its cause is unassigned and no general p95 speedup is inferred. The first candidate warmup took 1.12 seconds and is excluded under the predeclared warmup rule; these measurements establish no cold-start guarantee. Candidate measured p95 still exceeds 250 ms. Compaction debt is deferred, not eliminated, and the earlier input-volume/segment-limit caveats still apply.

Validation passed: 664 library tests, 35 profiling-enabled integration tests, and two default-build integration tests. Two known baseline OAuth database-open failures were explicitly excluded; two existing Markdown performance tests were ignored. The CLI regression crosses the default automatic-merge threshold, checks old/new record contents and inherited segment IDs, preserves `CURRENT` on no-op, and verifies a bulk rebuild. `cargo fmt --check` and `cargo clippy -- -D warnings` passed.

Implementation receipts: `/tmp/memex-bounded-index.Jo53eQ` contains `bench.py`, `binary-identities.json`, `results.json`, and `summary.json`. `paired-before-1/` and `paired-after-1/` contain phase traces, resolved sampled CPU profiles, `cpu.svg` / `cpu.png`, and writer-only phase-time diagrams (`writer-wall.svg` / `.png`). CPU graphs use CPU weights; writer diagrams use trace wall durations and exclude other threads' overlapping waits. Both CPU graphs were rendered and visually inspected.

## Fixed-state durability breakdown (secondary target)

Updated 2026-09-09 from the implementation's existing same-invocation trace and CPU flamegraph, `paired-after-1`; this is a deeper breakdown of that capture, not a new benchmark.

| Non-overlapping component | Wall time |
|---|---:|
| Tantivy commit | 59.79 ms |
| Generation publication | 55.73 ms |
| Pending, ingest, and scan-cache checkpoint writes | 63.77 ms |
| Staging | 18.62 ms |
| Reader open | 4.42 ms |
| Parsing | 1.10 ms |
| Memory refresh and state load | 9.98 ms |
| Remaining setup, handoffs, teardown | 14.71 ms |
| Total | 228.11 ms |

The same call has 64.42 sampled CPU-ms across threads. Its CPU flamegraph shows file synchronization, manifest/state writes, and state serialization; parsing and merge computation are no longer dominant. Thread CPU cannot be subtracted from command wall time to label the remainder as disk I/O.

Per-file durability-barrier timing inside commit and publication, correlated with sampled sync stacks, would refine this historical fixed-state breakdown. Those two phases consume 115.51 ms here; checkpoint writes add another 63.77 ms. Identify redundant barriers or repeated metadata serialization before changing them, and retain crash-recovery ordering. This capture does not yet prove that a particular synchronization can be removed. Reader reuse remains a separate target for high-segment-count search workloads.

## Receipts and interpretation

Local artifacts: `/tmp/memex-regression-model.7VWYhc`.

- `binary-identities.json`, `capture.py`, `replay.py`, `bounded.py`: exact binaries and workloads.
- `results.json`, `replay-results.json`, `bounded-v2-results.json`: raw observations. The original `bounded` entries in `replay-results.json` are invalid comparisons: they also deleted absent-provider records. Only `bounded-v2-*` passed full-corpus preservation assertions.
- `paired-before-1/`, `paired-after-1/`: representative `trace.json`, `trace-summary.json`, Samply profile/symbols, `thread-costs.json`, and rendered `merge-cpu.svg` / `merge-sample-hold.svg` flamegraphs.
- `after-3/`: initial durability-stall example with the same artifacts.
- `bounded-v2-paired-1/`: valid bounded-path trace, CPU folded stacks, and idle merge-worker comparison.
- `thread_costs.py`: symbol resolution, CPU categories, and sample-held wall estimates.

CPU weights use the profile's declared `threadCPUDelta` microseconds; zero-CPU samples contribute no CPU weight. Sample-held wall graphs assign the interval until the next sample to the current stack, including coalesced idle intervals. They approximate stack occupancy, not exact syscall durations. Merge-worker lifetimes include pre-merge idle time and are not interchangeable with the trace's narrower merge-wait interval. Unresolved native frames remain unresolved; CPU is never subtracted from multi-thread request wall time to manufacture an I/O number.

All frozen paired captures passed complete-trace checks. SVG flamegraphs were rendered and visually inspected. The installed binary and live index were not changed.
