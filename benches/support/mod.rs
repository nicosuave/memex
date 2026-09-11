mod fingerprint;

pub use fingerprint::fingerprint;

use anyhow::{Result, ensure};
use criterion::Bencher;
use memex::index::{QueryOptions, SearchIndex};
use memex::types::{Record, RecordLinks, SourceKind};
use std::time::{Duration, Instant};
use tantivy::IndexWriter;
use tantivy::merge_policy::NoMergePolicy;
use tempfile::TempDir;

pub struct Fixture {
    pub index: SearchIndex,
    // Drop the index and its file handles before removing the fixture directory.
    _root: TempDir,
}

impl Fixture {
    pub fn seeded(segments: usize, records_per_segment: usize) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let index = SearchIndex::open_or_create(root.path())?;
        let fixture = Self { index, _root: root };
        for segment in 0..segments {
            fixture.append(&records(segment * records_per_segment, records_per_segment))?;
        }
        ensure!(fixture.index.index.searchable_segment_ids()?.len() == segments);
        ensure!(fixture.index.doc_count()? == segments * records_per_segment);
        Ok(fixture)
    }

    pub fn append(&self, records: &[Record]) -> Result<()> {
        let mut writer: IndexWriter = self.index.index.writer_with_num_threads(1, 64_000_000)?;
        writer.set_merge_policy(Box::new(NoMergePolicy));
        for record in records {
            self.index.add_record(&mut writer, record)?;
        }
        writer.commit()?;
        writer.wait_merging_threads()?;
        Ok(())
    }

    pub fn merge_all(&self) -> Result<()> {
        let segments = self.index.index.searchable_segment_ids()?;
        ensure!(
            segments.len() > 1,
            "merge fixture must contain multiple segments"
        );
        let mut writer: IndexWriter = self.index.index.writer_with_num_threads(1, 64_000_000)?;
        writer.set_merge_policy(Box::new(NoMergePolicy));
        writer.merge(&segments).wait()?;
        writer.wait_merging_threads()?;
        Ok(())
    }
}

pub fn records(start: usize, count: usize) -> Vec<Record> {
    (start..start + count)
        .map(|id| Record {
            source: SourceKind::Codex,
            doc_id: id as u64,
            ts: 1_700_000_000 + id as u64,
            project: "benchmark".to_owned(),
            session_id: format!("session-{}", id / 128),
            turn_id: u32::try_from(id).expect("fixture turn ID fits u32"),
            role: "assistant".to_owned(),
            text: format!("benchmark database migrations document {id}"),
            tool_name: Some("shell".to_owned()),
            tool_input: Some(format!("inspect document {id}")),
            tool_output: Some(format!("document {id} is present")),
            links: RecordLinks::default(),
            source_path: format!("session-{}.jsonl", id / 128),
        })
        .collect()
}

pub fn query() -> QueryOptions {
    QueryOptions {
        query: "migration".to_owned(),
        project: None,
        role: None,
        tool: None,
        session_id: None,
        session_scope: None,
        source: None,
        since: None,
        until: None,
        limit: 20,
    }
}

/// Charge only the operation; build, verify, and destroy a fresh fixture per iteration.
pub fn measure_isolated<T>(
    bencher: &mut Bencher,
    mut setup: impl FnMut() -> T,
    mut operation: impl FnMut(&mut T),
    mut verify: impl FnMut(&T),
) {
    bencher.iter_custom(|iterations| {
        let mut elapsed = Duration::ZERO;
        for _ in 0..iterations {
            let mut fixture = setup();
            let started = Instant::now();
            operation(&mut fixture);
            elapsed += started.elapsed();
            verify(&fixture);
        }
        elapsed
    });
}
