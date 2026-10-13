# Native reader measurements

Measured October 4, 2026 on ARM64. These results identify reader costs; they do
not establish whole-app performance parity with T3 Code.

## Workload and boundaries

The common text corpus has 20 projects, 1,000 conversations and 11,990 messages:
999 ten-message conversations and one 2,000-message conversation. Messages are
approximately 1 KiB of alternating user/assistant Markdown, including lists and
code. There are no tool calls, media, or provider traffic. Corpus SHA-256:
`6e9bebbd9b41aab6aeb5e191b06ed079dc5c4193e7ac4c068706ea021bb6abeb`.

`MatchedCorpusReaderPerformanceTests` verifies the corpus identity and measures
the real native reader in a 1,000 × 720 offscreen window. The ARM64 Debug build
times update, layout and synchronous drawing. Acquisition, indexing, search,
app launch, provider transport and GPU presentation are excluded. Controllers
and windows must be released between batches. The 60-message case models the
initial native history window; the 2,000-message case models fully loaded history.

Five fresh-controller opens and switches are measured per workload. Incremental
updates grow the final assistant message in cumulative 64-character chunks,
without pacing. Wheel samples separate dispatch and post-yield layout from the
16 ms minimum scheduling opportunities and asynchronous animation work. These
costs are not frame-rate measurements.

## Native results after lazy rich-content materialization

| Operation | Samples | Median | p95 |
| --- | ---: | ---: | ---: |
| Open 10 messages | 5 | 29.29 ms | 44.10 ms |
| Open initial 60 messages | 5 | 102.25 ms | 108.12 ms |
| Open all 2,000 messages | 5 | 522.43 ms | 536.88 ms |
| Incremental update, 60 messages | 75 | 4.65 ms | 9.19 ms |
| Incremental update, 2,000 messages | 75 | 84.24 ms | 96.91 ms |

The shared 12-core host had one-minute load averages of 16.1–24.1 during these
measurements. Treat the timings as observations under contention, not stable
latency guarantees or an isolated before/after comparison. The full-history
update cost remains material.

A native CPU profile identified height measurement constructing rich-content
view hierarchies for offscreen rows. Measurement now retains TextKit layout
plans, while visible cells create controls on demand. `LazyRichContentTests`
deterministically verifies that measuring 200 rich rows leaves fewer than ten
view trees alive, with offscreen rows unmaterialized until requested. It also
checks exact geometry at narrow/wide widths, control release/recreation, links,
attachments and retained horizontal code scrolling. Exact text shaping and
thumbnail preparation still scale with the number of loaded rows.

## T3 observation

T3 Code commit `ad5178a31aac09c93159ea152daf00720ca56358` was tested through its
production web UI using the same corpus in an isolated local server. Warm opens
of the long conversation took 518.9–1,090.1 ms, median 838.9 ms over five samples.
The boundary was pointer input to visible matching DOM content plus two animation
frame callbacks, at 1,280 × 720 and device scale 2. The first view displayed only
the final four message markers; it did not render all 2,000 messages at once.
Three full-text searches for the unique corpus marker took 353.4–415.8 ms.

These T3 UI timings include work excluded by the native renderer harness and
use a different viewport and build mode. They cannot support a faster/slower
whole-app conclusion. T3 streaming and Electron startup were not measured.
The disposable T3 server required removal of a duplicate `Option` import and ran
Node 26.8.2 rather than its declared 24.13.1; this is another applicability limit.

## Running the native harness

Set `MEMEX_MATCHED_PERF_CORPUS` to the corpus JSON path, with its `manifest.json`
beside it, and run the ARM64 Swift test target filtered to
`MatchedCorpusReaderPerformanceTests`, serially. The corpus and manifest schema
are defined by the test. The fixture used for this run is local scratch evidence
under `/tmp/t3-perf-evidence`; it is not bundled with the repository.

Each `matched_reader_perf` JSON line includes raw samples, timing boundaries,
viewport, host load and core counts. Keep compilation, indexing and other timed
workloads out of the measurement interval. A failed viewport, movement or
lifetime assertion invalidates that case; do not publish its timing as a result.
