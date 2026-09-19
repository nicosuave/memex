//! Offline, content-addressed transfer of existing vectors; never runs model inference.
use crate::{index::SearchIndex, types::Record, vector::VectorIndex};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{collections::HashSet, fs, path::Path};

const FORMAT: &str = "memex-vector-transfer-v1:utf8-prefix-8192:f32-le";

#[derive(Debug, Default, Serialize)]
pub struct Report {
    pub eligible: usize,
    pub matched: usize,
    pub missing: usize,
    pub existing: usize,
    pub distinct_exported: usize,
    pub unmatched_exported: usize,
}

fn key(record: &Record) -> Option<Vec<u8>> {
    if !matches!(record.role.as_str(), "user" | "assistant") || record.text.is_empty() {
        return None;
    }
    let mut end = record.text.len().min(8192);
    while !record.text.is_char_boundary(end) {
        end -= 1;
    }
    Some(Sha256::digest(&record.text.as_bytes()[..end]).to_vec())
}

fn encode(values: &[f32]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

fn decode(bytes: &[u8], dimensions: usize) -> Result<Vec<f32>> {
    ensure!(
        bytes.len() == dimensions * 4,
        "cached vector dimensions mismatch"
    );
    let values: Vec<f32> = bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|chunk| f32::from_le_bytes(*chunk))
        .collect();
    ensure!(
        values.iter().all(|value| value.is_finite()),
        "cached vector contains nonfinite values"
    );
    Ok(values)
}

fn source_identity(root: &Path, index: &SearchIndex) -> Result<String> {
    // Old and new generation names are durable snapshot identities. Writers must be stopped.
    let vectors = fs::read(root.join("vectors/current.json"))?;
    Ok(format!(
        "{}:{}:{:x}",
        root.canonicalize()?.display(),
        index.snapshot_version(),
        Sha256::digest(vectors)
    ))
}

fn open_index(root: &Path) -> Result<SearchIndex> {
    ensure!(
        SearchIndex::exists(&root.join("index")),
        "lexical index must already exist"
    );
    SearchIndex::open_or_create(&root.join("index"))
}

fn metadata(db: &Connection) -> Result<(String, usize, String, String, bool)> {
    Ok(db.query_row(
        "SELECT model, dimensions, source, format, complete FROM transfer_meta WHERE id=1",
        [],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?)
}

/// Export checkpoints every 1024 eligible records. An interrupted run can reuse its cache
/// only against the same stopped source snapshot; it rescans records without decoding cached vectors.
pub fn export(root: &Path, cache: &Path) -> Result<Report> {
    let index = open_index(root)?;
    let vectors = VectorIndex::open(&root.join("vectors"))?;
    let model = vectors
        .model()
        .context("source vectors have no model identity")?;
    let identity = source_identity(root, &index)?;
    let mut db = Connection::open(cache)?;
    db.execute_batch("PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS transfer_meta (
            id INTEGER PRIMARY KEY CHECK(id=1), model TEXT NOT NULL, dimensions INTEGER NOT NULL,
            source TEXT NOT NULL, format TEXT NOT NULL, complete INTEGER NOT NULL CHECK(complete IN (0,1)));
        CREATE TABLE IF NOT EXISTS vectors (hash BLOB PRIMARY KEY CHECK(length(hash)=32), embedding BLOB NOT NULL) WITHOUT ROWID;")?;
    db.execute(
        "INSERT OR IGNORE INTO transfer_meta VALUES (1, ?1, ?2, ?3, ?4, 0)",
        params![model, vectors.dimensions(), identity, FORMAT],
    )?;
    let (saved_model, dimensions, saved_source, format, _) = metadata(&db)?;
    ensure!(
        saved_model == model
            && dimensions == vectors.dimensions()
            && saved_source == identity
            && format == FORMAT,
        "cache belongs to a different source snapshot, model, dimensions, or format; use a separate cache"
    );
    db.execute("UPDATE transfer_meta SET complete=0 WHERE id=1", [])?;
    let mut report = Report::default();
    let transaction = db.transaction()?;
    index.for_each_record(|record| {
        let Some(hash) = key(&record) else {
            return Ok(());
        };
        report.eligible += 1;
        if !vectors.contains(record.doc_id) {
            report.missing += 1;
        } else {
            report.matched += 1;
            let cached = transaction
                .query_row("SELECT 1 FROM vectors WHERE hash=?1", [&hash], |_| Ok(()))
                .optional()?
                .is_some();
            if cached {
                report.existing += 1;
            } else {
                let embedding = vectors
                    .embedding(record.doc_id)?
                    .context("source vector disappeared")?;
                ensure!(
                    embedding.iter().all(|value| value.is_finite()),
                    "source vector {} contains nonfinite values",
                    record.doc_id
                );
                transaction.execute(
                    "INSERT INTO vectors VALUES (?1, ?2)",
                    params![hash, encode(&embedding)],
                )?;
            }
        }
        if report.eligible.is_multiple_of(1024) {
            transaction.execute_batch("COMMIT; BEGIN IMMEDIATE")?;
        }
        Ok(())
    })?;
    ensure!(
        source_identity(root, &open_index(root)?)? == identity,
        "source changed during export; cache remains incomplete"
    );
    transaction.execute("UPDATE transfer_meta SET complete=1 WHERE id=1", [])?;
    transaction.commit()?;
    report.distinct_exported =
        db.query_row("SELECT COUNT(*) FROM vectors", [], |row| row.get(0))?;
    Ok(report)
}

fn cache_for_import(cache: &Path, model: &str, dimensions: usize) -> Result<Connection> {
    let db = Connection::open_with_flags(cache, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let (saved_model, saved_dimensions, _, format, complete) = metadata(&db)?;
    ensure!(
        complete,
        "export is incomplete; resume export before importing"
    );
    ensure!(
        format == FORMAT && saved_model == model && saved_dimensions == dimensions,
        "cache format, model, or dimensions mismatch"
    );
    Ok(db)
}

fn transfer(
    index: &SearchIndex,
    db: &Connection,
    vectors: &mut VectorIndex,
    import: bool,
) -> Result<Report> {
    let mut report = Report::default();
    let mut hashes: HashSet<Vec<u8>> = db
        .prepare("SELECT hash FROM vectors")?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    report.distinct_exported = hashes.len();
    let mut lookup = db.prepare("SELECT embedding FROM vectors WHERE hash=?1")?;
    index.for_each_record(|record| {
        let Some(hash) = key(&record) else {
            return Ok(());
        };
        report.eligible += 1;
        let existing = vectors.contains(record.doc_id);
        report.existing += usize::from(existing);
        let bytes: Option<Vec<u8>> = lookup.query_row([&hash], |row| row.get(0)).optional()?;
        let Some(bytes) = bytes else {
            report.missing += usize::from(!existing);
            return Ok(());
        };
        let embedding = decode(&bytes, vectors.dimensions())?;
        hashes.remove(&hash);
        if import && !existing {
            vectors.add(record.doc_id, &embedding)?;
        }
        let actual = vectors
            .embedding(record.doc_id)?
            .with_context(|| format!("missing imported vector {}", record.doc_id))?;
        ensure!(
            encode(&actual) == bytes,
            "vector {} differs from cached f32 values",
            record.doc_id
        );
        report.matched += 1;
        Ok(())
    })?;
    report.unmatched_exported = hashes.len();
    Ok(report)
}

/// Import into a separately staged root. Existing compatible vectors are preserved and checked;
/// no generation is published until every matched vector passes exact bitwise verification.
pub fn import(root: &Path, cache: &Path, model: &str, dimensions: usize) -> Result<Report> {
    let db = cache_for_import(cache, model, dimensions)?;
    let (_, _, source, _, _) = metadata(&db)?;
    ensure!(
        !source.starts_with(&format!("{}:", root.canonicalize()?.display())),
        "import requires a separate staged root"
    );
    let index = open_index(root)?;
    let mut vectors = if VectorIndex::exists(&root.join("vectors")) {
        let vectors = VectorIndex::open(&root.join("vectors"))?;
        ensure!(
            vectors.model() == Some(model) && vectors.dimensions() == dimensions,
            "destination vector model or dimensions mismatch"
        );
        vectors
    } else {
        VectorIndex::open_or_create(&root.join("vectors"), dimensions, Some(model))?
    };
    let report = transfer(&index, &db, &mut vectors, true)?;
    vectors.save()?;
    let mut reopened = VectorIndex::open(&root.join("vectors"))?;
    transfer(&index, &db, &mut reopened, false)?;
    Ok(report)
}

/// Verify every content-matched vector, including exact f32 bits after disk reload.
pub fn verify(root: &Path, cache: &Path, model: &str, dimensions: usize) -> Result<Report> {
    let db = cache_for_import(cache, model, dimensions)?;
    let mut vectors = VectorIndex::open(&root.join("vectors"))?;
    ensure!(
        vectors.model() == Some(model) && vectors.dimensions() == dimensions,
        "destination vector model or dimensions mismatch"
    );
    transfer(&open_index(root)?, &db, &mut vectors, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RecordLinks, SourceKind};

    fn indexed(root: &Path, rows: &[(u64, &str)]) -> Result<()> {
        let index = SearchIndex::open_or_create(&root.join("index"))?;
        let mut writer = index.writer()?;
        for &(doc_id, text) in rows {
            index.add_record(
                &mut writer,
                &Record {
                    source: SourceKind::Claude,
                    doc_id,
                    ts: doc_id,
                    project: "test".into(),
                    session_id: "test".into(),
                    turn_id: 1,
                    role: "user".into(),
                    text: text.into(),
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: RecordLinks::default(),
                    source_path: "test.jsonl".into(),
                },
            )?;
        }
        writer.commit()?;
        writer.wait_merging_threads()?;
        Ok(())
    }

    #[test]
    fn transfer_remaps_exact_inputs_and_rejects_incomplete_or_incompatible_exports() -> Result<()> {
        let old = tempfile::tempdir()?;
        let new = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;
        let cache = cache_dir.path().join("vectors.sqlite3");
        let long = format!("{}🦀old tail", "a".repeat(8191));
        let new_tail = format!("{}🦀new tail", "a".repeat(8191));
        indexed(
            old.path(),
            &[
                (1, "same"),
                (2, &long),
                (3, "not embedded"),
                (4, "removed content"),
            ],
        )?;
        let mut original =
            VectorIndex::open_or_create(&old.path().join("vectors"), 3, Some("test"))?;
        original.add(1, &[0.25, -0.5, 0.75])?;
        original.add(2, &[0.125, 0.25, -0.5])?;
        original.add(4, &[0.5, 0.125, -0.25])?;
        original.save()?;
        let exported = export(old.path(), &cache)?;
        assert_eq!(
            (
                exported.matched,
                exported.missing,
                exported.distinct_exported
            ),
            (3, 1, 3)
        );
        indexed(
            new.path(),
            &[
                (101, "same"),
                (102, "same"),
                (103, &new_tail),
                (104, "changed"),
                (105, "not embedded"),
            ],
        )?;
        let db = Connection::open(&cache)?;
        db.execute("UPDATE transfer_meta SET complete=0", [])?;
        assert!(import(new.path(), &cache, "test", 3).is_err());
        assert!(!VectorIndex::exists(&new.path().join("vectors")));
        assert_eq!(export(old.path(), &cache)?.existing, 3);
        assert!(import(new.path(), &cache, "other", 3).is_err());
        assert!(import(new.path(), &cache, "test", 4).is_err());
        let imported = import(new.path(), &cache, "test", 3)?;
        assert_eq!(
            (imported.matched, imported.missing, imported.existing),
            (3, 2, 0)
        );
        assert_eq!(imported.unmatched_exported, 1);
        assert_eq!(verify(new.path(), &cache, "test", 3)?.matched, 3);
        assert_eq!(import(new.path(), &cache, "test", 3)?.existing, 3);
        let transferred = VectorIndex::open(&new.path().join("vectors"))?;
        assert_eq!(transferred.embedding(101)?, original.embedding(1)?);
        assert_eq!(transferred.embedding(103)?, original.embedding(2)?);
        // A corrupt/incompatible cache must fail before publishing any replacement.
        let pointer = fs::read(new.path().join("vectors/current.json"))?;
        db.execute(
            "UPDATE vectors SET embedding=?1",
            [encode(&[f32::NAN, 0.25, 0.5])],
        )?;
        assert!(import(new.path(), &cache, "test", 3).is_err());
        assert_eq!(fs::read(new.path().join("vectors/current.json"))?, pointer);
        db.execute("UPDATE vectors SET embedding=zeroblob(4)", [])?;
        assert!(import(new.path(), &cache, "test", 3).is_err());
        assert_eq!(fs::read(new.path().join("vectors/current.json"))?, pointer);
        Ok(())
    }
}
