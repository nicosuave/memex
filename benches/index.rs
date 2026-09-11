mod support;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::Duration;
use support::{Fixture, fingerprint, measure_isolated, query, records};

fn index_benchmarks(criterion: &mut Criterion) {
    let mut mutations = criterion.benchmark_group("index");
    mutations.sample_size(20);
    mutations.warm_up_time(Duration::from_secs(1));
    mutations.measurement_time(Duration::from_secs(3));

    mutations.throughput(Throughput::Elements(20));
    mutations.bench_function("append_20_to_1024", |bencher| {
        measure_isolated(
            bencher,
            || {
                (
                    Fixture::seeded(8, 128).expect("seed append fixture"),
                    records(1024, 20),
                )
            },
            |(fixture, additions)| {
                fixture
                    .append(black_box(additions))
                    .expect("append records")
            },
            |(fixture, additions)| {
                assert_eq!(fixture.index.doc_count().expect("document count"), 1044);
                for record in additions {
                    let stored = fixture
                        .index
                        .get_by_doc_id(record.doc_id)
                        .expect("read appended document")
                        .expect("appended document exists");
                    assert_eq!(stored.text, record.text);
                    assert_eq!(stored.tool_input, record.tool_input);
                    assert_eq!(stored.tool_output, record.tool_output);
                }
            },
        );
    });

    mutations.throughput(Throughput::Elements(1024));
    mutations.bench_function("merge_8_segments", |bencher| {
        measure_isolated(
            bencher,
            || {
                let fixture = Fixture::seeded(8, 128).expect("seed merge fixture");
                let before = fingerprint(&fixture.index).expect("fingerprint original documents");
                (fixture, before)
            },
            |(fixture, _)| fixture.merge_all().expect("merge all segments"),
            |(fixture, before)| {
                assert_eq!(
                    fixture
                        .index
                        .index
                        .searchable_segment_ids()
                        .expect("merged segments")
                        .len(),
                    1
                );
                assert_eq!(
                    fingerprint(&fixture.index).expect("fingerprint merged documents"),
                    *before
                );
            },
        );
    });
    mutations.finish();

    let fixture = Fixture::seeded(8, 128).expect("seed search fixture");
    let query = query();
    assert_eq!(
        fixture.index.search(&query).expect("validate search").len(),
        20
    );
    criterion.bench_function("index/search_top_20", |bencher| {
        bencher.iter(|| black_box(fixture.index.search(black_box(&query)).expect("search")));
    });
}

criterion_group!(benches, index_benchmarks);
criterion_main!(benches);
