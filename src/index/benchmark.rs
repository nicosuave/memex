use super::*;
use std::collections::BTreeMap;
use tantivy::merge_policy::NoMergePolicy;

fn root() -> Result<PathBuf> {
    Ok(PathBuf::from(std::env::var("MEMEX_BENCH_ROOT")?).join("index"))
}

fn retained_ids() -> Result<Vec<String>> {
    let mut ids: Vec<String> = serde_json::from_str(&std::env::var("MEMEX_BENCH_RETAINED_IDS")?)?;
    ids.sort();
    ids.dedup();
    anyhow::ensure!(ids.len() == 3, "expected three retained segment IDs");
    Ok(ids)
}

fn receipt(index: &SearchIndex, fingerprint: bool) -> Result<serde_json::Value> {
    let retained = retained_ids()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let schema = index.index.schema();
    let mut partitions = BTreeMap::new();
    let mut all_hashes = Vec::new();
    let mut remainder_hashes = Vec::new();
    let mut remainder_live = 0_u64;
    let mut remainder_max = 0_u64;
    let mut remainder_segments = 0;
    for segment in searcher.segment_readers() {
        let id = segment.segment_id().uuid_string();
        let keep = retained.contains(&id);
        anyhow::ensure!(
            keep == (segment.max_doc() >= 100_000),
            "segment partition changed"
        );
        let mut hashes = Vec::new();
        if fingerprint {
            let store = segment.get_store_reader(1)?;
            for doc_id in 0..segment.max_doc() {
                if segment.is_deleted(doc_id) {
                    continue;
                }
                let doc = store.get::<TantivyDocument>(doc_id)?;
                let mut fields = doc
                    .field_values()
                    .iter()
                    .map(|field| {
                        Ok((
                            schema.get_field_name(field.field()).to_owned(),
                            serde_json::to_string(&canonical_json(serde_json::to_value(
                                field.value(),
                            )?))?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                fields.sort();
                let hash: [u8; 32] = Sha256::digest(serde_json::to_vec(&fields)?).into();
                hashes.push(hash);
            }
            anyhow::ensure!(hashes.len() as u64 == u64::from(segment.num_docs()));
            all_hashes.extend_from_slice(&hashes);
        }
        if keep {
            partitions.insert(
                id,
                serde_json::json!({
                    "live_docs": segment.num_docs(), "max_doc": segment.max_doc(),
                    "sha256": fingerprint.then(|| multiset_hash(&mut hashes)),
                }),
            );
        } else {
            remainder_live += u64::from(segment.num_docs());
            remainder_max += u64::from(segment.max_doc());
            remainder_segments += 1;
            remainder_hashes.extend(hashes);
        }
    }
    anyhow::ensure!(partitions.keys().cloned().collect::<Vec<_>>() == retained);
    let expected: u64 = std::env::var("MEMEX_BENCH_EXPECTED_LIVE")?.parse()?;
    anyhow::ensure!(
        searcher.num_docs() == expected,
        "live document count mismatch"
    );
    Ok(serde_json::json!({
        "live_docs": searcher.num_docs(),
        "segments": searcher.segment_readers().len(),
        "retained": partitions,
        "remainder": {"live_docs": remainder_live, "max_doc": remainder_max,
            "segments": remainder_segments,
            "sha256": fingerprint.then(|| multiset_hash(&mut remainder_hashes))},
        "sha256": fingerprint.then(|| multiset_hash(&mut all_hashes)),
    }))
}

fn canonical_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(fields) => serde_json::Value::Object(
            fields
                .into_iter()
                .map(|(key, value)| (key, canonical_json(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json).collect())
        }
        value => value,
    }
}

fn multiset_hash(hashes: &mut [[u8; 32]]) -> String {
    hashes.sort_unstable();
    let mut digest = Sha256::new();
    digest.update(b"memex-stored-doc-multiset-v1\0");
    digest.update((hashes.len() as u64).to_le_bytes());
    for hash in hashes {
        digest.update(hash);
    }
    format!("{:x}", digest.finalize())
}

fn write_receipt(value: serde_json::Value) -> Result<()> {
    fs::write(
        std::env::var("MEMEX_BENCH_RECEIPT")?,
        serde_json::to_vec_pretty(&value)?,
    )?;
    Ok(())
}

#[test]
#[ignore = "offline benchmark maintenance; requires MEMEX_BENCH_* environment"]
fn terminal_merge() -> Result<()> {
    #[cfg(feature = "profiling")]
    let profile = crate::profiling::Session::from_env()?;
    let result = (|| -> Result<()> {
        crate::profiling::span!("benchmark.terminal_merge");
        let root = root()?;
        anyhow::ensure!(SearchIndex::exists(&root), "benchmark index is missing");
        let index = SearchIndex::open_or_create_for_ingest_with_merge_policy(&root, false)?;
        let before = receipt(&index, false)?;
        let retained = retained_ids()?;
        let segments = index.index.searchable_segment_metas()?;
        let remainder = segments
            .iter()
            .filter(|segment| !retained.contains(&segment.id().uuid_string()))
            .map(|segment| segment.id())
            .collect::<Vec<_>>();
        anyhow::ensure!(!remainder.is_empty(), "no remainder segments");
        let mut writer: IndexWriter = index.index.writer_with_num_threads(1, 64_000_000)?;
        writer.set_merge_policy(Box::new(NoMergePolicy));
        writer.merge(&remainder).wait()?;
        writer.wait_merging_threads()?;
        index.publish_generation()?;
        let published = SearchIndex::open_or_create(&root)?;
        let after = receipt(&published, false)?;
        anyhow::ensure!(after["segments"] == 4);
        anyhow::ensure!(before["retained"] == after["retained"]);
        anyhow::ensure!(before["remainder"]["live_docs"] == after["remainder"]["live_docs"]);
        anyhow::ensure!(after["remainder"]["live_docs"] == after["remainder"]["max_doc"]);
        write_receipt(after)
    })();
    #[cfg(feature = "profiling")]
    profile.finish()?;
    result
}

#[test]
#[ignore = "offline stored-document fingerprint; requires MEMEX_BENCH_* environment"]
fn fingerprint() -> Result<()> {
    let root = root()?;
    anyhow::ensure!(SearchIndex::exists(&root), "benchmark index is missing");
    write_receipt(receipt(&SearchIndex::open_or_create(&root)?, true)?)
}
