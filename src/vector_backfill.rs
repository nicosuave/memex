use crate::config::Paths;
use crate::embed::{EmbedRuntimeConfig, EmbedderHandle, ModelChoice};
use crate::index::SearchIndex;
use crate::lease::{INGEST_LEASE_TIMEOUT, IngestLease};
use crate::vector::{VectorIndex, VectorInventory};
use anyhow::{Context, Result, anyhow};
use indicatif::{ProgressBar, ProgressStyle};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::HashSet;
use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const BATCH_SIZE: usize = 64;
const NON_TTY_REPORT_INTERVAL: Duration = Duration::from_secs(30);
const EMBED_MAX_CHARS: usize = 8192;
const BACKFILL_DB: &str = "embed-backfill.sqlite3";

#[derive(Debug, Clone)]
pub struct BackfillStatus {
    pub model: String,
    pub dimensions: usize,
    pub total: u64,
    pub completed: u64,
    pub checkpointed: u64,
    pub active_ms: u64,
    pub updated_at_ms: u64,
    pub phase: String,
    pub pid: u32,
    pub running: bool,
}

impl BackfillStatus {
    pub fn line(&self) -> String {
        let percent = if self.total == 0 {
            100.0
        } else {
            self.completed as f64 / self.total as f64 * 100.0
        };
        let remaining = self.total.saturating_sub(self.completed);
        let rate = if self.active_ms == 0 || self.checkpointed == 0 {
            0.0
        } else {
            self.checkpointed as f64 / (self.active_ms as f64 / 1000.0)
        };
        let eta = if rate > 0.0 {
            format_duration((remaining as f64 / rate).ceil() as u64)
        } else {
            "unknown".to_string()
        };
        let state = if self.running {
            format!("running pid {}", self.pid)
        } else {
            format!("checkpointed after pid {} stopped", self.pid)
        };
        let updated_ago = format_duration(now_ms().saturating_sub(self.updated_at_ms) / 1000);
        format!(
            "vector backfill: {}/{} ({percent:.1}%, model {}, phase {}, {}, checkpoint {}, active {}, updated {} ago, ETA {})",
            self.completed,
            self.total,
            self.model,
            self.phase,
            state,
            self.checkpointed,
            format_duration(self.active_ms / 1000),
            updated_ago,
            eta,
        )
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BackfillReport {
    pub embedded: usize,
    pub total: usize,
    pub resumed: usize,
}

#[derive(Debug)]
struct BackfillMeta {
    model: String,
    dimensions: usize,
    total: u64,
    base_completed: u64,
    started_at_ms: u64,
    active_ms: u64,
    updated_at_ms: u64,
    phase: String,
    pid: u32,
}

struct BackfillStore {
    connection: Connection,
    path: PathBuf,
    run_started: Instant,
    run_active_base_ms: u64,
}

impl BackfillStore {
    fn open(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(&path)?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             CREATE TABLE IF NOT EXISTS backfill_meta (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 model TEXT NOT NULL,
                 dimensions INTEGER NOT NULL,
                 total INTEGER NOT NULL,
                 base_completed INTEGER NOT NULL,
                 started_at_ms INTEGER NOT NULL,
                 active_ms INTEGER NOT NULL,
                 updated_at_ms INTEGER NOT NULL,
                 phase TEXT NOT NULL,
                 pid INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS backfill_vectors (
                 doc_id INTEGER PRIMARY KEY,
                 embedding BLOB NOT NULL
             );",
        )?;
        Ok(Self {
            connection,
            path,
            run_started: Instant::now(),
            run_active_base_ms: 0,
        })
    }

    fn prepare(
        &mut self,
        model: &str,
        dimensions: usize,
        total: u64,
        base_completed: u64,
    ) -> Result<()> {
        let previous = self.meta()?;
        if previous
            .as_ref()
            .is_some_and(|meta| meta.model != model || meta.dimensions != dimensions)
        {
            self.connection
                .execute("DELETE FROM backfill_vectors", [])?;
            self.connection.execute("DELETE FROM backfill_meta", [])?;
        }
        let previous = self.meta()?;
        let started_at_ms = previous
            .as_ref()
            .map(|meta| meta.started_at_ms)
            .unwrap_or_else(now_ms);
        self.run_active_base_ms = previous.as_ref().map(|meta| meta.active_ms).unwrap_or(0);
        self.run_started = Instant::now();
        self.connection.execute(
            "INSERT INTO backfill_meta (
                 id, model, dimensions, total, base_completed, started_at_ms,
                 active_ms, updated_at_ms, phase, pid
             ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, 'embedding', ?8)
             ON CONFLICT(id) DO UPDATE SET
                 model = excluded.model,
                 dimensions = excluded.dimensions,
                 total = excluded.total,
                 base_completed = excluded.base_completed,
                 updated_at_ms = excluded.updated_at_ms,
                 phase = excluded.phase,
                 pid = excluded.pid",
            params![
                model,
                dimensions as i64,
                total as i64,
                base_completed as i64,
                started_at_ms as i64,
                self.run_active_base_ms as i64,
                now_ms() as i64,
                std::process::id() as i64,
            ],
        )?;
        Ok(())
    }

    fn retain_pending(&mut self, live_ids: &HashSet<u64>, active_ids: &HashSet<u64>) -> Result<()> {
        let stale = {
            let mut statement = self
                .connection
                .prepare("SELECT doc_id FROM backfill_vectors")?;
            let mut rows = statement.query([])?;
            let mut stale = Vec::new();
            while let Some(row) = rows.next()? {
                let doc_id = row.get::<_, i64>(0)? as u64;
                if !live_ids.contains(&doc_id) || active_ids.contains(&doc_id) {
                    stale.push(doc_id);
                }
            }
            stale
        };
        if stale.is_empty() {
            return Ok(());
        }
        let transaction = self.connection.transaction()?;
        {
            let mut statement =
                transaction.prepare("DELETE FROM backfill_vectors WHERE doc_id = ?1")?;
            for doc_id in stale {
                statement.execute(params![doc_id as i64])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    fn ids(&self) -> Result<HashSet<u64>> {
        let count =
            self.connection
                .query_row("SELECT COUNT(*) FROM backfill_vectors", [], |row| {
                    row.get::<_, i64>(0)
                })?;
        let count = usize::try_from(count).context("invalid staged embedding count")?;
        let mut statement = self
            .connection
            .prepare("SELECT doc_id FROM backfill_vectors")?;
        let mut ids = HashSet::with_capacity(count);
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            ids.insert(row.get::<_, i64>(0)? as u64);
        }
        Ok(ids)
    }

    fn checkpoint(&mut self, rows: &[(u64, Vec<f32>)]) -> Result<BackfillStatus> {
        if rows.is_empty() {
            return self
                .status()?
                .ok_or_else(|| anyhow!("backfill metadata missing"));
        }
        let dimensions = self
            .meta()?
            .ok_or_else(|| anyhow!("backfill metadata missing"))?
            .dimensions;
        for (doc_id, embedding) in rows {
            validate_embedding(embedding, dimensions)
                .with_context(|| format!("stage embedding for doc_id {doc_id}"))?;
        }
        let active_ms = self
            .run_active_base_ms
            .saturating_add(self.run_started.elapsed().as_millis().min(u64::MAX as u128) as u64);
        let transaction = self.connection.transaction()?;
        {
            let mut statement = transaction.prepare(
                "INSERT INTO backfill_vectors(doc_id, embedding)
                 VALUES (?1, ?2)
                 ON CONFLICT(doc_id) DO UPDATE SET
                     embedding = excluded.embedding",
            )?;
            for (doc_id, embedding) in rows {
                statement.execute(params![*doc_id as i64, encode_embedding(embedding)])?;
            }
        }
        transaction.execute(
            "UPDATE backfill_meta
             SET active_ms = ?1, updated_at_ms = ?2, phase = 'embedding', pid = ?3
             WHERE id = 1",
            params![active_ms as i64, now_ms() as i64, std::process::id() as i64],
        )?;
        transaction.commit()?;
        self.status()?
            .ok_or_else(|| anyhow!("backfill metadata missing"))
    }

    fn set_phase(&mut self, phase: &str) -> Result<()> {
        let active_ms = self
            .run_active_base_ms
            .saturating_add(self.run_started.elapsed().as_millis().min(u64::MAX as u128) as u64);
        self.connection.execute(
            "UPDATE backfill_meta
             SET active_ms = ?1, updated_at_ms = ?2, phase = ?3, pid = ?4
             WHERE id = 1",
            params![
                active_ms as i64,
                now_ms() as i64,
                phase,
                std::process::id() as i64
            ],
        )?;
        Ok(())
    }

    fn for_each_vector(
        &self,
        dimensions: usize,
        mut visitor: impl FnMut(u64, Vec<f32>) -> Result<()>,
    ) -> Result<()> {
        let mut statement = self
            .connection
            .prepare("SELECT doc_id, embedding FROM backfill_vectors ORDER BY doc_id")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let doc_id = row.get::<_, i64>(0)? as u64;
            let bytes = row.get::<_, Vec<u8>>(1)?;
            let embedding = decode_embedding(&bytes, dimensions)
                .with_context(|| format!("decode staged embedding for doc_id {doc_id}"))?;
            visitor(doc_id, embedding)?;
        }
        Ok(())
    }

    fn meta(&self) -> Result<Option<BackfillMeta>> {
        self.connection
            .query_row(
                "SELECT model, dimensions, total, base_completed, started_at_ms,
                        active_ms, updated_at_ms, phase, pid
                 FROM backfill_meta WHERE id = 1",
                [],
                |row| {
                    Ok(BackfillMeta {
                        model: row.get(0)?,
                        dimensions: row.get::<_, i64>(1)? as usize,
                        total: row.get::<_, i64>(2)? as u64,
                        base_completed: row.get::<_, i64>(3)? as u64,
                        started_at_ms: row.get::<_, i64>(4)? as u64,
                        active_ms: row.get::<_, i64>(5)? as u64,
                        updated_at_ms: row.get::<_, i64>(6)? as u64,
                        phase: row.get(7)?,
                        pid: row.get::<_, i64>(8)? as u32,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    fn status(&self) -> Result<Option<BackfillStatus>> {
        let Some(meta) = self.meta()? else {
            return Ok(None);
        };
        let checkpointed =
            self.connection
                .query_row("SELECT COUNT(*) FROM backfill_vectors", [], |row| {
                    row.get::<_, i64>(0)
                })? as u64;
        Ok(Some(BackfillStatus {
            model: meta.model,
            dimensions: meta.dimensions,
            total: meta.total,
            completed: meta.base_completed.saturating_add(checkpointed),
            checkpointed,
            active_ms: meta.active_ms,
            updated_at_ms: meta.updated_at_ms,
            phase: meta.phase,
            pid: meta.pid,
            running: true,
        }))
    }

    fn clear(self) -> Result<()> {
        let path = self.path.clone();
        drop(self.connection);
        remove_sqlite_files(&path)
    }
}

pub fn status(paths: &Paths) -> Result<Option<BackfillStatus>> {
    let path = backfill_path(paths);
    if !path.exists() {
        return Ok(None);
    }
    let Some(mut status) = BackfillStore::open(path)?.status()? else {
        return Ok(None);
    };
    status.running = crate::lease::is_embedding_held_by(paths, status.pid);
    Ok(Some(status))
}

pub(crate) fn checkpoint_exists(paths: &Paths) -> bool {
    backfill_path(paths).exists()
}

/// Return whether the active vector generation is incomplete for the current lexical corpus.
///
/// This is intentionally separate from [`run`]: continuous indexing can make an exact decision
/// before paying the cost of starting an embedding process. Callers should avoid invoking it on
/// every idle poll because it inventories the embeddable lexical records.
pub(crate) fn needs_work(paths: &Paths, index: &SearchIndex, model: ModelChoice) -> Result<bool> {
    if checkpoint_exists(paths) {
        return Ok(true);
    }
    let inventory = match VectorIndex::inventory(&paths.vectors) {
        Ok(inventory) => inventory,
        // The explicit backfill run will build and atomically publish a replacement. Treat a
        // stable generation that fails validation as work instead of declaring it complete from
        // metadata and doc_ids.bin alone.
        Err(_) => return Ok(true),
    };
    if inventory.as_ref().is_some_and(|inventory| {
        inventory.model.as_deref() != Some(model.as_str())
            || model
                .known_dimensions()
                .is_some_and(|dimensions| inventory.dimensions != dimensions)
    }) {
        return Ok(true);
    }
    let live_ids = live_embeddable_ids(index)?;
    Ok(inventory
        .map(|inventory| inventory.doc_ids != live_ids)
        .unwrap_or(!live_ids.is_empty()))
}

pub fn run(
    paths: &Paths,
    index: &SearchIndex,
    model: ModelChoice,
    runtime: &EmbedRuntimeConfig,
) -> Result<BackfillReport> {
    let lease = IngestLease::acquire_embedding(paths, "embed", INGEST_LEASE_TIMEOUT)?;
    run_with_lease(paths, index, model, runtime, &lease)
}

pub(crate) fn run_with_lease(
    paths: &Paths,
    index: &SearchIndex,
    model: ModelChoice,
    runtime: &EmbedRuntimeConfig,
    _lease: &IngestLease,
) -> Result<BackfillReport> {
    let (inventory, rebuild_active) = match VectorIndex::inventory(&paths.vectors) {
        Ok(inventory) => (inventory, false),
        Err(error) => {
            eprintln!("active vector generation failed validation; rebuilding it: {error:#}");
            (None, true)
        }
    };
    let mut embedder = None;
    let dimensions = match model.known_dimensions() {
        Some(dimensions) => dimensions,
        None => {
            let handle = EmbedderHandle::with_model_and_runtime(model, runtime)?;
            let dimensions = handle.dims;
            embedder = Some(handle);
            dimensions
        }
    };
    let active_ids = compatible_active_ids(inventory, dimensions, model.as_str());
    let active_compatible = active_ids.is_some();
    let active_ids = active_ids.unwrap_or_default();

    let live_ids = live_embeddable_ids(index)?;
    let base_completed = live_ids.intersection(&active_ids).count() as u64;
    if active_compatible && active_ids == live_ids {
        if backfill_path(paths).exists() {
            remove_sqlite_files(&backfill_path(paths))?;
        }
        return Ok(BackfillReport {
            total: live_ids.len(),
            ..BackfillReport::default()
        });
    }

    let mut store = BackfillStore::open(backfill_path(paths))?;
    store.prepare(
        model.as_str(),
        dimensions,
        live_ids.len() as u64,
        base_completed,
    )?;
    store.retain_pending(&live_ids, &active_ids)?;
    let staged_ids = store.ids()?;
    let resumed = staged_ids.len();
    let completed_at_start = base_completed.saturating_add(resumed as u64);
    let stderr_is_terminal = std::io::stderr().is_terminal();
    let progress = stderr_is_terminal.then(|| {
        let bar = ProgressBar::new(live_ids.len() as u64);
        bar.set_style(
            ProgressStyle::with_template(
                "{spinner:.cyan} vector backfill {pos}/{len} [{elapsed_precise}, ETA {eta_precise}]",
            )
            .expect("static progress template"),
        );
        bar.set_position(completed_at_start);
        bar.enable_steady_tick(Duration::from_millis(80));
        bar
    });
    if !stderr_is_terminal && let Some(status) = store.status()? {
        eprintln!("{}", status.line());
    }

    if completed_at_start < live_ids.len() as u64 && embedder.is_none() {
        embedder = Some(EmbedderHandle::with_model_and_runtime(model, runtime)?);
    }
    let mut batch = Vec::<(u64, String)>::with_capacity(BATCH_SIZE);
    let mut embedded_this_run = 0usize;
    let mut last_report = Instant::now();
    index.for_each_record(|record| {
        if !record_needs_embedding(&record.role, &record.text)
            || active_ids.contains(&record.doc_id)
            || staged_ids.contains(&record.doc_id)
        {
            return Ok(());
        }
        let text = truncate_for_embedding(record.text);
        if text.is_empty() {
            return Ok(());
        }
        batch.push((record.doc_id, text));
        if batch.len() >= BATCH_SIZE {
            let pending = batch.len();
            let status = checkpoint_batch(
                &mut batch,
                embedder.as_mut().expect("embedder initialized"),
                &mut store,
            )?;
            embedded_this_run += pending;
            if let Some(progress) = &progress {
                progress.set_position(status.completed.min(status.total));
            } else if last_report.elapsed() >= NON_TTY_REPORT_INTERVAL {
                eprintln!("{}", status.line());
                last_report = Instant::now();
            }
        }
        Ok(())
    })?;
    if !batch.is_empty() {
        let pending = batch.len();
        let status = checkpoint_batch(
            &mut batch,
            embedder.as_mut().expect("embedder initialized"),
            &mut store,
        )?;
        embedded_this_run += pending;
        if let Some(progress) = &progress {
            progress.set_position(status.completed.min(status.total));
        } else {
            eprintln!("{}", status.line());
        }
    }
    // All new embeddings are durable now. Release the model before USearch opens the complete
    // active generation; repeated TUI/RPC backfills must not retain one model per refresh.
    drop(embedder);

    // These corpus-sized lookup tables are only needed while scanning for missing records.
    // Release them before opening VectorIndex, which loads its own complete doc ID set.
    drop(active_ids);
    drop(staged_ids);

    store.set_phase("finalizing")?;
    let mut vector = if rebuild_active {
        VectorIndex::empty_replacement(&paths.vectors, dimensions, Some(model.as_str()))?
    } else {
        VectorIndex::open_or_create(&paths.vectors, dimensions, Some(model.as_str()))?
    };
    vector.retain_doc_ids(&live_ids)?;
    store.for_each_vector(dimensions, |doc_id, embedding| {
        if live_ids.contains(&doc_id) {
            vector.add(doc_id, &embedding)?;
        }
        Ok(())
    })?;
    if let Some(missing) = live_ids.iter().find(|doc_id| !vector.contains(**doc_id)) {
        return Err(anyhow!(
            "vector backfill is incomplete after finalization; missing doc_id {missing}"
        ));
    }
    vector.save()?;
    if let Some(progress) = progress {
        progress.finish_with_message(format!("vector backfill {} complete", live_ids.len()));
    }
    store.clear()?;
    Ok(BackfillReport {
        embedded: embedded_this_run,
        total: live_ids.len(),
        resumed,
    })
}

fn compatible_active_ids(
    inventory: Option<VectorInventory>,
    dimensions: usize,
    model: &str,
) -> Option<HashSet<u64>> {
    inventory
        .filter(|inventory| {
            inventory.dimensions == dimensions && inventory.model.as_deref() == Some(model)
        })
        .map(|inventory| inventory.doc_ids)
}

fn checkpoint_batch(
    batch: &mut Vec<(u64, String)>,
    embedder: &mut EmbedderHandle,
    store: &mut BackfillStore,
) -> Result<BackfillStatus> {
    let texts = batch
        .iter()
        .map(|(_, text)| text.as_str())
        .collect::<Vec<_>>();
    let embeddings = embedder.embed_texts(&texts)?;
    if embeddings.len() != batch.len() {
        return Err(anyhow!(
            "embedding batch returned {} vectors for {} records",
            embeddings.len(),
            batch.len()
        ));
    }
    let rows = batch
        .drain(..)
        .zip(embeddings)
        .map(|((doc_id, _text), embedding)| (doc_id, embedding))
        .collect::<Vec<_>>();
    store.checkpoint(&rows)
}

fn record_needs_embedding(role: &str, text: &str) -> bool {
    (role == "user" || role == "assistant") && !text.is_empty()
}

fn live_embeddable_ids(index: &SearchIndex) -> Result<HashSet<u64>> {
    let mut live_ids = HashSet::new();
    index.for_each_record(|record| {
        if record_needs_embedding(&record.role, &record.text) {
            live_ids.insert(record.doc_id);
        }
        Ok(())
    })?;
    Ok(live_ids)
}

fn truncate_for_embedding(mut text: String) -> String {
    if text.len() <= EMBED_MAX_CHARS {
        return text;
    }
    let mut end = EMBED_MAX_CHARS.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text
}

fn encode_embedding(embedding: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(embedding.len() * 4);
    for value in embedding {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn decode_embedding(bytes: &[u8], dimensions: usize) -> Result<Vec<f32>> {
    let expected_bytes = dimensions
        .checked_mul(std::mem::size_of::<f32>())
        .ok_or_else(|| anyhow!("staged embedding dimensions overflow: {dimensions}"))?;
    if bytes.len() != expected_bytes {
        return Err(anyhow!(
            "invalid staged embedding size: expected {} bytes, got {}",
            expected_bytes,
            bytes.len()
        ));
    }
    let embedding = bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|chunk| f32::from_le_bytes(*chunk))
        .collect::<Vec<_>>();
    validate_embedding(&embedding, dimensions)?;
    Ok(embedding)
}

fn validate_embedding(embedding: &[f32], dimensions: usize) -> Result<()> {
    if embedding.len() != dimensions {
        return Err(anyhow!(
            "invalid staged embedding dimensions: expected {dimensions} values, got {}",
            embedding.len()
        ));
    }
    if let Some((dimension, value)) = embedding
        .iter()
        .enumerate()
        .find(|(_, value)| !value.is_finite())
    {
        return Err(anyhow!(
            "invalid staged embedding value at dimension {dimension}: {value}"
        ));
    }
    Ok(())
}

fn backfill_path(paths: &Paths) -> PathBuf {
    paths.state.join(BACKFILL_DB)
}

fn remove_sqlite_files(path: &Path) -> Result<()> {
    for candidate in [
        path.to_path_buf(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ] {
        match fs::remove_file(&candidate) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("remove backfill state {}", candidate.display()));
            }
        }
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn format_duration(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = seconds % 3600 / 60;
    let seconds = seconds % 60;
    if hours > 0 {
        format!("{hours}h{minutes:02}m")
    } else if minutes > 0 {
        format!("{minutes}m{seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Record, RecordLinks, SourceKind};

    fn test_vector(seed: f32) -> Vec<f32> {
        (0..4).map(|index| seed + index as f32).collect()
    }

    fn test_record(doc_id: u64, source_path: &str) -> Record {
        Record {
            source: SourceKind::Claude,
            doc_id,
            ts: doc_id,
            project: "project".to_string(),
            session_id: format!("session-{doc_id}"),
            turn_id: 1,
            role: "user".to_string(),
            text: "text".to_string(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks::default(),
            source_path: source_path.to_string(),
        }
    }

    fn active_vector_dir(root: &Path) -> PathBuf {
        let pointer: serde_json::Value =
            serde_json::from_slice(&fs::read(root.join("current.json")).unwrap()).unwrap();
        root.join("generations")
            .join(pointer["generation"].as_str().unwrap())
    }

    #[test]
    fn status_line_exposes_checkpoint_state_and_eta() {
        let status = BackfillStatus {
            model: "bge".to_string(),
            dimensions: 384,
            total: 20,
            completed: 10,
            checkpointed: 10,
            active_ms: 10_000,
            updated_at_ms: now_ms(),
            phase: "embedding".to_string(),
            pid: 42,
            running: false,
        };

        let line = status.line();
        assert!(line.contains("10/20 (50.0%"));
        assert!(line.contains("checkpointed after pid 42 stopped"));
        assert!(line.contains("checkpoint 10"));
        assert!(line.contains("ETA 10s"));
    }

    #[test]
    fn checkpoints_survive_reopen_and_track_progress() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join(BACKFILL_DB);
        {
            let mut store = BackfillStore::open(path.clone()).unwrap();
            store.prepare("test", 4, 3, 1).unwrap();
            let status = store.checkpoint(&[(2, test_vector(2.0))]).unwrap();
            assert_eq!(status.completed, 2);
            assert_eq!(status.checkpointed, 1);
        }

        let store = BackfillStore::open(path).unwrap();
        let status = store.status().unwrap().unwrap();
        assert_eq!(status.model, "test");
        assert_eq!(status.total, 3);
        assert_eq!(status.completed, 2);
        assert!(store.ids().unwrap().contains(&2));
    }

    #[test]
    fn incompatible_model_discards_only_staged_work() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join(BACKFILL_DB);
        let mut store = BackfillStore::open(path).unwrap();
        store.prepare("alpha", 4, 2, 0).unwrap();
        store.checkpoint(&[(1, test_vector(1.0))]).unwrap();
        store.prepare("beta", 4, 2, 0).unwrap();
        assert!(store.ids().unwrap().is_empty());
        assert_eq!(store.status().unwrap().unwrap().model, "beta");
    }

    #[test]
    fn staged_embeddings_round_trip() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join(BACKFILL_DB);
        let mut store = BackfillStore::open(path).unwrap();
        store.prepare("test", 4, 1, 0).unwrap();
        store.checkpoint(&[(7, test_vector(7.0))]).unwrap();
        let mut loaded = Vec::new();
        store
            .for_each_vector(4, |doc_id, embedding| {
                loaded.push((doc_id, embedding));
                Ok(())
            })
            .unwrap();
        assert_eq!(loaded, vec![(7, test_vector(7.0))]);
    }

    #[test]
    fn checkpoint_rejects_invalid_embeddings_before_persisting_them() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join(BACKFILL_DB);
        let mut store = BackfillStore::open(path).unwrap();
        store.prepare("test", 4, 2, 0).unwrap();

        let wrong_size = store.checkpoint(&[(7, vec![1.0; 3])]).unwrap_err();
        assert_eq!(
            format!("{wrong_size:#}"),
            "stage embedding for doc_id 7: invalid staged embedding dimensions: expected 4 values, got 3"
        );

        let non_finite = store
            .checkpoint(&[(8, vec![1.0, f32::NAN, 2.0, 3.0])])
            .unwrap_err();
        assert_eq!(
            format!("{non_finite:#}"),
            "stage embedding for doc_id 8: invalid staged embedding value at dimension 1: NaN"
        );
        assert!(store.ids().unwrap().is_empty());
    }

    #[test]
    fn corrupt_staged_embedding_reports_its_doc_id() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join(BACKFILL_DB);
        let mut store = BackfillStore::open(path).unwrap();
        store.prepare("test", 4, 1, 0).unwrap();
        store.checkpoint(&[(7, test_vector(7.0))]).unwrap();
        store
            .connection
            .execute(
                "UPDATE backfill_vectors SET embedding = ?1 WHERE doc_id = ?2",
                params![vec![0_u8; 15], 7_i64],
            )
            .unwrap();

        let error = store
            .for_each_vector(4, |_doc_id, _embedding| Ok(()))
            .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "decode staged embedding for doc_id 7: invalid staged embedding size: expected 16 bytes, got 15"
        );
    }

    #[test]
    fn same_size_non_finite_staged_embedding_is_rejected() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join(BACKFILL_DB);
        let mut store = BackfillStore::open(path).unwrap();
        store.prepare("test", 4, 1, 0).unwrap();
        store.checkpoint(&[(9, test_vector(9.0))]).unwrap();
        let mut corrupt = test_vector(9.0);
        corrupt[2] = f32::INFINITY;
        store
            .connection
            .execute(
                "UPDATE backfill_vectors SET embedding = ?1 WHERE doc_id = ?2",
                params![encode_embedding(&corrupt), 9_i64],
            )
            .unwrap();

        let error = store
            .for_each_vector(4, |_doc_id, _embedding| Ok(()))
            .unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "decode staged embedding for doc_id 9: invalid staged embedding value at dimension 2: inf"
        );
    }

    #[test]
    fn compatible_inventory_transfers_the_existing_id_set() {
        let doc_ids = (1..=1024).collect::<HashSet<_>>();
        let original_entry = doc_ids.get(&512).unwrap() as *const u64 as usize;
        let original_capacity = doc_ids.capacity();
        let inventory = VectorInventory {
            dimensions: 4,
            model: Some("test".to_string()),
            doc_ids,
            vector_count: 1024,
            index_bytes: 0,
            ids_bytes: 0,
        };

        let active_ids = compatible_active_ids(Some(inventory), 4, "test").unwrap();

        assert_eq!(active_ids.len(), 1024);
        assert_eq!(active_ids.capacity(), original_capacity);
        assert_eq!(
            active_ids.get(&512).unwrap() as *const u64 as usize,
            original_entry
        );
    }

    #[test]
    fn retain_pending_filters_large_lookup_sets_without_changing_semantics() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join(BACKFILL_DB);
        let mut store = BackfillStore::open(path).unwrap();
        store.prepare("test", 4, 1024, 0).unwrap();
        let rows = (1..=1024)
            .map(|doc_id| (doc_id, test_vector(doc_id as f32)))
            .collect::<Vec<_>>();
        store.checkpoint(&rows).unwrap();
        let live_ids = (1..=1024)
            .filter(|doc_id| doc_id % 2 == 0)
            .collect::<HashSet<_>>();
        let active_ids = (1..=1024)
            .filter(|doc_id| doc_id % 4 == 0)
            .collect::<HashSet<_>>();

        store.retain_pending(&live_ids, &active_ids).unwrap();

        let expected = (1..=1024)
            .filter(|doc_id| doc_id % 4 == 2)
            .collect::<HashSet<_>>();
        assert_eq!(store.ids().unwrap(), expected);
    }

    #[test]
    fn needs_work_matches_the_active_generation_to_the_lexical_corpus() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temporary.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let index = SearchIndex::open_or_create(&paths.index).unwrap();

        assert!(!needs_work(&paths, &index, ModelChoice::BGESmall).unwrap());

        let mut writer = index.writer().unwrap();
        index
            .add_record(&mut writer, &test_record(1, "one.jsonl"))
            .unwrap();
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        assert!(needs_work(&paths, &index, ModelChoice::BGESmall).unwrap());

        let mut active = VectorIndex::open_or_create(&paths.vectors, 384, Some("bge")).unwrap();
        active.add(1, &vec![0.1; 384]).unwrap();
        active.save().unwrap();
        assert!(!needs_work(&paths, &index, ModelChoice::BGESmall).unwrap());
        assert!(needs_work(&paths, &index, ModelChoice::MiniLM).unwrap());

        let mut writer = index.writer().unwrap();
        index
            .add_record(&mut writer, &test_record(2, "two.jsonl"))
            .unwrap();
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        assert!(needs_work(&paths, &index, ModelChoice::BGESmall).unwrap());

        active.add(2, &vec![0.2; 384]).unwrap();
        active.save().unwrap();
        assert!(!needs_work(&paths, &index, ModelChoice::BGESmall).unwrap());

        let mut writer = index.writer().unwrap();
        index.delete_by_source_path(&mut writer, "one.jsonl");
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        assert!(needs_work(&paths, &index, ModelChoice::BGESmall).unwrap());
    }

    #[test]
    fn corrupt_usearch_generation_is_detected_and_rebuilt_from_checkpoint() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temporary.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let index = SearchIndex::open_or_create(&paths.index).unwrap();
        let mut writer = index.writer().unwrap();
        index
            .add_record(&mut writer, &test_record(1, "one.jsonl"))
            .unwrap();
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();

        let mut active = VectorIndex::open_or_create(&paths.vectors, 384, Some("bge")).unwrap();
        active.add(1, &vec![0.1; 384]).unwrap();
        active.save().unwrap();
        drop(active);
        let active_dir = active_vector_dir(&paths.vectors);
        let metadata_before = fs::read(active_dir.join("meta.json")).unwrap();
        let ids_before = fs::read(active_dir.join("doc_ids.bin")).unwrap();

        // Replace only usearch.index with another valid, same-cardinality index whose document ID
        // disagrees with the untouched manifest and sidecar. The lightweight stats inventory
        // cannot see this.
        let donor_root = temporary.path().join("wrong-id-vector-donor");
        let mut donor = VectorIndex::open_or_create(&donor_root, 384, Some("bge")).unwrap();
        donor.add(2, &vec![0.3; 384]).unwrap();
        donor.save().unwrap();
        drop(donor);
        fs::copy(
            active_vector_dir(&donor_root).join("usearch.index"),
            active_dir.join("usearch.index"),
        )
        .unwrap();
        assert_eq!(
            fs::read(active_dir.join("meta.json")).unwrap(),
            metadata_before
        );
        assert_eq!(
            fs::read(active_dir.join("doc_ids.bin")).unwrap(),
            ids_before
        );
        assert!(VectorIndex::inventory(&paths.vectors).is_err());
        assert!(needs_work(&paths, &index, ModelChoice::BGESmall).unwrap());

        // A complete checkpoint lets this exercise repair without loading an embedding model.
        let mut store = BackfillStore::open(backfill_path(&paths)).unwrap();
        store.prepare("bge", 384, 1, 0).unwrap();
        store.checkpoint(&[(1, vec![0.2; 384])]).unwrap();
        drop(store);

        let report = run(
            &paths,
            &index,
            ModelChoice::BGESmall,
            &EmbedRuntimeConfig::default(),
        )
        .unwrap();

        assert_eq!(report.embedded, 0);
        assert_eq!(report.resumed, 1);
        assert_eq!(report.total, 1);
        let repaired = VectorIndex::open(&paths.vectors).unwrap();
        assert!(repaired.contains(1));
        drop(repaired);
        assert!(!needs_work(&paths, &index, ModelChoice::BGESmall).unwrap());
    }

    #[test]
    fn needs_work_includes_unfinished_checkpoint_cleanup() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temporary.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let index = SearchIndex::open_or_create(&paths.index).unwrap();
        let mut store = BackfillStore::open(backfill_path(&paths)).unwrap();
        store.prepare("bge", 384, 0, 0).unwrap();

        assert!(needs_work(&paths, &index, ModelChoice::BGESmall).unwrap());
    }

    #[test]
    fn completed_checkpoints_finalize_without_reembedding() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temporary.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let index = SearchIndex::open_or_create(&paths.index).unwrap();
        let mut writer = index.writer().unwrap();
        index
            .add_record(&mut writer, &test_record(1, "one.jsonl"))
            .unwrap();
        index
            .add_record(&mut writer, &test_record(2, "two.jsonl"))
            .unwrap();
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();

        let mut active = VectorIndex::open_or_create(&paths.vectors, 384, Some("bge")).unwrap();
        active.add(1, &vec![0.1; 384]).unwrap();
        active.save().unwrap();
        let mut store = BackfillStore::open(backfill_path(&paths)).unwrap();
        store.prepare("bge", 384, 2, 1).unwrap();
        store.checkpoint(&[(2, vec![0.2; 384])]).unwrap();
        drop(store);

        let report = run(
            &paths,
            &index,
            ModelChoice::BGESmall,
            &EmbedRuntimeConfig::default(),
        )
        .unwrap();

        assert_eq!(report.embedded, 0);
        assert_eq!(report.resumed, 1);
        assert_eq!(report.total, 2);
        assert!(!backfill_path(&paths).exists());
        let active = VectorIndex::open(&paths.vectors).unwrap();
        assert_eq!(active.model(), Some("bge"));
        assert!(active.contains(1));
        assert!(active.contains(2));
    }
}
