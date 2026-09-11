use anyhow::Result;
use memex::index::SearchIndex;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tantivy::TantivyDocument;

/// Compare all stored fields independent of segment layout and document ordering.
pub fn fingerprint(index: &SearchIndex) -> Result<(u64, [u8; 32])> {
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let schema = index.index.schema();
    let mut hashes = Vec::new();
    for segment in searcher.segment_readers() {
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
    }
    anyhow::ensure!(hashes.len() as u64 == searcher.num_docs());
    hashes.sort_unstable();
    let mut digest = Sha256::new();
    digest.update(b"memex-stored-doc-multiset-v1\0");
    digest.update((hashes.len() as u64).to_le_bytes());
    for hash in hashes {
        digest.update(hash);
    }
    Ok((searcher.num_docs(), digest.finalize().into()))
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(key, value)| (key, canonical_json(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        value => value,
    }
}
