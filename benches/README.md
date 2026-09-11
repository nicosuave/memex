# Index benchmarks

Run the Criterion suite with:

```sh
cargo bench --bench index
```

For a quick correctness smoke run (one iteration per case):

```sh
cargo bench --bench index -- --test
```

CI uses `cargo bench --bench index --profile dev -- --test` to exercise the same
fixture checks without compiling an optimized binary. Use the default optimized
bench profile for measurements.

The suite generates its own 1,024-record, eight-segment index under a temporary
directory. It needs no live corpus, model download, or `MEMEX_BENCH_*` variables.
Criterion is a dev dependency and the benchmark target has `harness = false`.

| Case | Timed work | Verification |
|---|---|---|
| `index/append_20_to_1024` | Open a single-thread writer, add 20 prepared records, commit, and join the writer | 1,044 live documents; appended text and tool payloads are retrievable |
| `index/merge_8_segments` | Open a single-thread writer, merge eight segments, and join the writer | One segment; live count and a multiset hash of every stored field are unchanged |
| `index/search_top_20` | Open a reader, execute a stemmed query, and materialize the top 20 records | The query returns 20 records before measurement |

Mutating cases use Criterion's `iter_custom`: each iteration gets a freshly built
fixture, and only the operation's elapsed time is returned to Criterion. Fixture
creation, record construction, verification, and directory cleanup are outside
that interval. Writer creation, commit, and merge completion are included. The
search case reuses a read-only fixture and includes result destruction.

These are warm filesystem microbenchmarks through the public index API. They use
a flat index, not the ingest coordinator: generation staging/publication,
checkpoint maintenance, source discovery, and process startup are not measured.
Use the recorded end-to-end experiments in [the merge cost model](reports/index-merge-cost-model.md) and
[the shared-segment report](reports/shared-segments-benchmark.md) when evaluating those costs.

The old `index::benchmark::{terminal_merge,fingerprint}` ignored tests were
one-off helpers for frozen-corpus experiments. They are retired; historical
receipts continue to describe their original binaries. Reusable fingerprint
verification now lives in `benches/support/fingerprint.rs`, alongside the fresh
fixture and timing support. No production API is exposed solely for benchmarking.
