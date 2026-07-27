use crate::types::{Record, SourceKind};
use anyhow::Result;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::Path;

const CHUNK_BYTES: usize = 6 * 1024;
const CHUNK_OVERLAP_BYTES: usize = 768;

#[derive(Debug, Clone)]
pub struct EmbeddingDocument {
    pub embedding_key: String,
    pub vector_id: u64,
    pub session_key: String,
    pub source: SourceKind,
    pub role: String,
    pub anchor_record_key: String,
    pub start_record_key: String,
    pub end_record_key: String,
    pub chunk_index: usize,
    pub start_offset: usize,
    pub end_offset: usize,
    pub content_hash: String,
    pub content: String,
}

#[derive(Debug, Default)]
pub struct EmbeddingSyncReport {
    pub documents: usize,
    pub embedded: usize,
    pub removed: usize,
    pub embedded_by_source: [u64; SourceKind::COUNT],
}

pub fn synchronize(
    index: &crate::index::SearchIndex,
    catalog_path: &Path,
    vector_index: &mut crate::vector::VectorIndex,
    embedder: &mut crate::embed::EmbedderHandle,
    progress: Option<&crate::progress::Progress>,
) -> Result<EmbeddingSyncReport> {
    let mut records = Vec::new();
    index.for_each_record(|record| {
        records.push(record);
        Ok(())
    })?;
    let documents = build_embedding_documents(records);
    let live_vector_ids: HashSet<u64> = documents
        .iter()
        .map(|document| document.vector_id)
        .collect();
    let stale_vector_ids: Vec<u64> = vector_index
        .vector_ids()
        .filter(|vector_id| !live_vector_ids.contains(vector_id))
        .collect();
    let removed = vector_index.remove_many(stale_vector_ids)?;
    if removed > 0 {
        // Persist vector eviction before removing mirror rows from the catalog.
        vector_index.save()?;
    }

    let mut catalog = crate::catalog::CatalogStore::open(catalog_path)?;
    catalog.replace_embedding_documents(&documents)?;

    let pending: Vec<&EmbeddingDocument> = documents
        .iter()
        .filter(|document| !vector_index.contains(document.vector_id))
        .collect();
    if let Some(progress) = progress {
        for document in &pending {
            progress.add_embed_total(document.source, 1);
            progress.add_embed_pending(document.source, 1);
        }
    }

    let mut report = EmbeddingSyncReport {
        documents: documents.len(),
        removed,
        ..EmbeddingSyncReport::default()
    };
    for batch in pending.chunks(64) {
        let texts: Vec<&str> = batch
            .iter()
            .map(|document| document.content.as_str())
            .collect();
        let embeddings = embedder.embed_texts(&texts)?;
        for (document, embedding) in batch.iter().zip(embeddings.iter()) {
            vector_index.add(document.vector_id, embedding)?;
            report.embedded += 1;
            report.embedded_by_source[document.source.idx()] += 1;
            if let Some(progress) = progress {
                progress.sub_embed_pending(document.source, 1);
                progress.add_embedded(document.source, 1);
            }
        }
    }
    vector_index.save()?;
    Ok(report)
}

pub fn synchronize_sessions(
    catalog_path: &Path,
    affected_session_keys: &[String],
    vector_index: &mut crate::vector::VectorIndex,
    embedder: &mut crate::embed::EmbedderHandle,
    progress: Option<&crate::progress::Progress>,
) -> Result<EmbeddingSyncReport> {
    if affected_session_keys.is_empty() {
        return Ok(EmbeddingSyncReport::default());
    }
    let mut catalog = crate::catalog::CatalogStore::open(catalog_path)?;
    let old_vector_ids: HashSet<u64> = catalog
        .embedding_vector_ids_for_session_keys(affected_session_keys)?
        .into_iter()
        .collect();
    let records = catalog.records_for_session_keys(affected_session_keys)?;
    let documents = build_embedding_documents(records);
    let new_vector_ids: HashSet<u64> = documents
        .iter()
        .map(|document| document.vector_id)
        .collect();
    let removed = vector_index.remove_many(old_vector_ids.difference(&new_vector_ids).copied())?;
    if removed > 0 {
        // Persist eviction before deleting its durable mirror identity.
        vector_index.save()?;
    }
    catalog.replace_embedding_documents_for_sessions(affected_session_keys, &documents)?;
    embed_pending_documents(documents, removed, vector_index, embedder, progress)
}

fn embed_pending_documents(
    documents: Vec<EmbeddingDocument>,
    removed: usize,
    vector_index: &mut crate::vector::VectorIndex,
    embedder: &mut crate::embed::EmbedderHandle,
    progress: Option<&crate::progress::Progress>,
) -> Result<EmbeddingSyncReport> {
    let pending: Vec<&EmbeddingDocument> = documents
        .iter()
        .filter(|document| !vector_index.contains(document.vector_id))
        .collect();
    if let Some(progress) = progress {
        for document in &pending {
            progress.add_embed_total(document.source, 1);
            progress.add_embed_pending(document.source, 1);
        }
    }
    let mut report = EmbeddingSyncReport {
        documents: documents.len(),
        removed,
        ..EmbeddingSyncReport::default()
    };
    for batch in pending.chunks(64) {
        let texts: Vec<&str> = batch
            .iter()
            .map(|document| document.content.as_str())
            .collect();
        let embeddings = embedder.embed_texts(&texts)?;
        for (document, embedding) in batch.iter().zip(embeddings.iter()) {
            vector_index.add(document.vector_id, embedding)?;
            report.embedded += 1;
            report.embedded_by_source[document.source.idx()] += 1;
            if let Some(progress) = progress {
                progress.sub_embed_pending(document.source, 1);
                progress.add_embedded(document.source, 1);
            }
        }
    }
    vector_index.save()?;
    Ok(report)
}

pub fn build_embedding_documents(mut records: Vec<Record>) -> Vec<EmbeddingDocument> {
    records.sort_by(|left, right| {
        (
            left.source.storage_label(),
            session_identity(left),
            left.ts,
            left.turn_id,
            left.doc_id,
        )
            .cmp(&(
                right.source.storage_label(),
                session_identity(right),
                right.ts,
                right.turn_id,
                right.doc_id,
            ))
    });

    let mut documents = Vec::new();
    let mut assistant_run = Vec::new();
    let mut active_session: Option<(SourceKind, String, String)> = None;
    for record in records {
        let session = (
            record.source,
            record.session_id.clone(),
            record.source_path.clone(),
        );
        if active_session
            .as_ref()
            .is_some_and(|active| active != &session)
        {
            emit_group(&mut documents, &mut assistant_run, "assistant");
        }
        active_session = Some(session);
        match record.role.as_str() {
            "user" => {
                emit_group(&mut documents, &mut assistant_run, "assistant");
                emit_group(&mut documents, &mut vec![record], "user");
            }
            "assistant" if !record.text.trim().is_empty() => assistant_run.push(record),
            _ => {}
        }
    }
    emit_group(&mut documents, &mut assistant_run, "assistant");
    assign_vector_ids(&mut documents);
    documents
}

fn emit_group(documents: &mut Vec<EmbeddingDocument>, members: &mut Vec<Record>, role: &str) {
    if members.is_empty() {
        return;
    }
    let members = std::mem::take(members);
    let source = members[0].source;
    let session_key =
        crate::catalog::session_key(source, &members[0].session_id, &members[0].source_path);
    let mut joined = String::new();
    let mut member_offsets = Vec::with_capacity(members.len());
    for member in &members {
        if !joined.is_empty() {
            joined.push_str("\n\n");
        }
        let start = joined.len();
        joined.push_str(&member.text);
        member_offsets.push((start, joined.len(), member.record_key.as_str()));
    }
    if joined.trim().is_empty() {
        return;
    }

    for (chunk_index, (start_offset, end_offset)) in chunk_ranges(&joined).into_iter().enumerate() {
        let content = joined[start_offset..end_offset].to_string();
        let content_hash = format!("{:x}", Sha256::digest(content.as_bytes()));
        let start_record_key = member_offsets
            .iter()
            .rfind(|(start, _, _)| *start <= start_offset)
            .map(|(_, _, key)| *key)
            .unwrap_or(member_offsets[0].2);
        let end_record_key = member_offsets
            .iter()
            .find(|(_, end, _)| *end >= end_offset)
            .map(|(_, _, key)| *key)
            .unwrap_or(member_offsets.last().expect("members are non-empty").2);
        let embedding_key = embedding_key(
            &session_key,
            role,
            start_record_key,
            end_record_key,
            chunk_index,
            &content_hash,
        );
        documents.push(EmbeddingDocument {
            embedding_key,
            vector_id: 0,
            session_key: session_key.clone(),
            source,
            role: role.to_string(),
            anchor_record_key: start_record_key.to_string(),
            start_record_key: start_record_key.to_string(),
            end_record_key: end_record_key.to_string(),
            chunk_index,
            start_offset,
            end_offset,
            content_hash,
            content,
        });
    }
}

fn chunk_ranges(text: &str) -> Vec<(usize, usize)> {
    if text.len() <= CHUNK_BYTES {
        return vec![(0, text.len())];
    }
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let mut end = (start + CHUNK_BYTES).min(text.len());
        while end > start && !text.is_char_boundary(end) {
            end -= 1;
        }
        if end == start {
            end = text[start..]
                .char_indices()
                .nth(1)
                .map(|(offset, _)| start + offset)
                .unwrap_or(text.len());
        }
        ranges.push((start, end));
        if end == text.len() {
            break;
        }
        let target = end.saturating_sub(CHUNK_OVERLAP_BYTES);
        start = target;
        while start < end && !text.is_char_boundary(start) {
            start += 1;
        }
        if start >= end {
            start = end;
        }
    }
    ranges
}

fn embedding_key(
    session_key: &str,
    role: &str,
    start_record_key: &str,
    end_record_key: &str,
    chunk_index: usize,
    content_hash: &str,
) -> String {
    let mut hasher = Sha256::new();
    for part in [
        "embdoc1",
        session_key,
        role,
        start_record_key,
        end_record_key,
        content_hash,
    ] {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    hasher.update((chunk_index as u64).to_le_bytes());
    format!("embdoc1_{:x}", hasher.finalize())
}

fn assign_vector_ids(documents: &mut [EmbeddingDocument]) {
    let mut claimed = HashMap::new();
    for document in documents {
        let mut salt = 0u64;
        loop {
            let mut hasher = Sha256::new();
            hasher.update(document.embedding_key.as_bytes());
            hasher.update(salt.to_le_bytes());
            let digest = hasher.finalize();
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&digest[..8]);
            let vector_id = (u64::from_le_bytes(bytes) & i64::MAX as u64).max(1);
            match claimed.get(&vector_id) {
                None => {
                    claimed.insert(vector_id, document.embedding_key.clone());
                    document.vector_id = vector_id;
                    break;
                }
                Some(key) if key == &document.embedding_key => {
                    document.vector_id = vector_id;
                    break;
                }
                Some(_) => salt += 1,
            }
        }
    }
}

fn session_identity(record: &Record) -> &str {
    if record.session_id.is_empty() {
        &record.source_path
    } else {
        &record.session_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RecordLinks;

    fn record(doc_id: u64, role: &str, text: &str) -> Record {
        Record {
            source: SourceKind::Codex,
            record_key: format!("rk-{doc_id}"),
            doc_id,
            ts: doc_id,
            project: "memex".to_string(),
            session_id: "session".to_string(),
            turn_id: doc_id as u32,
            role: role.to_string(),
            text: text.to_string(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks::default(),
            source_path: "/tmp/session.jsonl".to_string(),
        }
    }

    #[test]
    fn assistant_records_collapse_until_the_next_user_boundary() {
        let documents = build_embedding_documents(vec![
            record(1, "user", "question"),
            record(2, "assistant", "first"),
            record(3, "tool_use", "ignored tool payload"),
            record(4, "assistant", "second"),
            record(5, "user", "next question"),
        ]);

        assert_eq!(documents.len(), 3);
        assert_eq!(documents[0].role, "user");
        assert_eq!(documents[1].role, "assistant");
        assert_eq!(documents[1].content, "first\n\nsecond");
        assert_eq!(documents[1].start_record_key, "rk-2");
        assert_eq!(documents[1].end_record_key, "rk-4");
        assert_eq!(documents[2].role, "user");
    }

    #[test]
    fn long_documents_cover_the_tail_with_overlap() {
        let text = "abcdef".repeat(3_000);
        let documents = build_embedding_documents(vec![record(1, "assistant", &text)]);

        assert!(documents.len() > 1);
        assert_eq!(documents.first().expect("first").start_offset, 0);
        assert_eq!(documents.last().expect("last").end_offset, text.len());
        for pair in documents.windows(2) {
            assert!(pair[1].start_offset < pair[0].end_offset);
        }
    }

    #[test]
    fn vector_ids_are_stable() {
        let first = build_embedding_documents(vec![record(1, "user", "question")]);
        let second = build_embedding_documents(vec![record(1, "user", "question")]);

        assert_eq!(first[0].embedding_key, second[0].embedding_key);
        assert_eq!(first[0].vector_id, second[0].vector_id);
    }

    #[test]
    fn session_projection_replacement_preserves_unaffected_documents() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("catalog.sqlite");
        let mut writer = crate::analytics::AnalyticsWriter::open(&path).unwrap();
        let mut records = Vec::new();
        for (doc_id, session_id) in [(1, "session-a"), (2, "session-b")] {
            let mut record = record(doc_id, "assistant", session_id);
            record.session_id = session_id.to_string();
            record.source_path = format!("/tmp/{session_id}.jsonl");
            record.record_key = format!("rk-{session_id}");
            writer.record(&record).unwrap();
            records.push(record);
        }
        writer.flush().unwrap();
        drop(writer);
        let mut catalog = crate::catalog::CatalogStore::open(&path).unwrap();
        let documents = build_embedding_documents(records);
        catalog.replace_embedding_documents(&documents).unwrap();
        let session_a =
            crate::catalog::session_key(SourceKind::Codex, "session-a", "/tmp/session-a.jsonl");
        let session_b =
            crate::catalog::session_key(SourceKind::Codex, "session-b", "/tmp/session-b.jsonl");
        let session_b_ids = catalog
            .embedding_vector_ids_for_session_keys(std::slice::from_ref(&session_b))
            .unwrap();

        catalog
            .replace_embedding_documents_for_sessions(std::slice::from_ref(&session_a), &[])
            .unwrap();

        assert!(
            catalog
                .embedding_vector_ids_for_session_keys(&[session_a])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            catalog
                .embedding_vector_ids_for_session_keys(&[session_b])
                .unwrap(),
            session_b_ids
        );
    }
}
