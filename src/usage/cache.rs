//! Per-file blob cache and canonical-facts storage.
//!
//! The SQLite cache maps (source, path) to parsed event blobs plus
//! cross-file dependency fingerprints, and hosts the canonical usage facts
//! tables that let reports and refreshes skip blob decoding.

use super::scan::{
    ParsedUsageFile, UsageFileDep, fingerprint_files, source_ordinal, source_spec, stable_triples,
};
use super::{TokenBuckets, UsageEvent, usage_timing};
use crate::types::SourceFilter;
use anyhow::Result;
use rayon::prelude::*;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize)]
pub(crate) struct CachedUsageEvent {
    source_record_id: Option<String>,
    session_id: Option<String>,
    request_id: Option<String>,
    message_id: Option<String>,
    timestamp_ms: u64,
    project: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    tokens: TokenBuckets,
    source_cost_usd: Option<f64>,
    cost_authoritative: bool,
    dedupe_confidence: String,
    conservative_undercount: bool,
    cache_chain_excluded: bool,
    sidechain: bool,
    permission_review: bool,
    source_order: u64,
}

/// Borrowed serialization view with the same Postcard layout as `CachedUsageEvent`.
/// `save_batch` serializes through this type to avoid cloning every string field.
#[derive(Serialize)]
struct CachedUsageEventRef<'a> {
    source_record_id: Option<&'a str>,
    session_id: Option<&'a str>,
    request_id: Option<&'a str>,
    message_id: Option<&'a str>,
    timestamp_ms: u64,
    project: Option<&'a str>,
    provider: Option<&'a str>,
    model: Option<&'a str>,
    tokens: &'a TokenBuckets,
    source_cost_usd: Option<f64>,
    cost_authoritative: bool,
    dedupe_confidence: &'a str,
    conservative_undercount: bool,
    cache_chain_excluded: bool,
    sidechain: bool,
    permission_review: bool,
    source_order: u64,
}

impl<'a> CachedUsageEventRef<'a> {
    fn from_event(event: &'a UsageEvent) -> Self {
        Self {
            source_record_id: event.source_record_id.as_deref(),
            session_id: event.session_id.as_deref(),
            request_id: event.request_id.as_deref(),
            message_id: event.message_id.as_deref(),
            timestamp_ms: event.timestamp_ms,
            project: event.project.as_deref(),
            provider: event.provider.as_deref(),
            model: event.model.as_deref(),
            tokens: &event.tokens,
            source_cost_usd: event.source_cost_usd,
            cost_authoritative: event.cost_authoritative,
            dedupe_confidence: event.dedupe_confidence,
            conservative_undercount: event.conservative_undercount,
            cache_chain_excluded: event.cache_chain_excluded,
            sidechain: event.sidechain,
            permission_review: event.permission_review,
            source_order: event.source_order,
        }
    }
}

impl CachedUsageEvent {
    #[allow(dead_code)]
    fn from_event(event: &UsageEvent) -> Self {
        Self {
            source_record_id: event.source_record_id.clone(),
            session_id: event.session_id.clone(),
            request_id: event.request_id.clone(),
            message_id: event.message_id.clone(),
            timestamp_ms: event.timestamp_ms,
            project: event.project.clone(),
            provider: event.provider.clone(),
            model: event.model.clone(),
            tokens: event.tokens.clone(),
            source_cost_usd: event.source_cost_usd,
            cost_authoritative: event.cost_authoritative,
            dedupe_confidence: event.dedupe_confidence.to_string(),
            conservative_undercount: event.conservative_undercount,
            cache_chain_excluded: event.cache_chain_excluded,
            sidechain: event.sidechain,
            permission_review: event.permission_review,
            source_order: event.source_order,
        }
    }

    pub(crate) fn into_event(self, source: &'static str, source_path: Arc<str>) -> UsageEvent {
        UsageEvent {
            source,
            source_path,
            source_record_id: self.source_record_id,
            session_id: self.session_id,
            request_id: self.request_id,
            message_id: self.message_id,
            timestamp_ms: self.timestamp_ms,
            project: self.project,
            provider: self.provider,
            model: self.model,
            tokens: self.tokens,
            source_cost_usd: self.source_cost_usd,
            cost_authoritative: self.cost_authoritative,
            dedupe_confidence: match self.dedupe_confidence.as_str() {
                "exact" => "exact",
                "strong" => "strong",
                _ => "heuristic",
            },
            conservative_undercount: self.conservative_undercount,
            cache_chain_excluded: self.cache_chain_excluded,
            sidechain: self.sidechain,
            permission_review: self.permission_review,
            source_order: self.source_order,
        }
    }
}

pub(crate) struct UsageCache {
    pub(crate) connection: Connection,
    // Serializes multi-transaction refreshes across processes. Blob chunks can
    // still commit independently so interrupted scans retain their progress.
    _refresh_lock: Option<File>,
}

pub(crate) struct CachedFileRow {
    pub(crate) size: u64,
    pub(crate) mtime_ns: i64,
    pub(crate) scanned_at_ms: i64,
    pub(crate) events_blob: Vec<u8>,
    pub(crate) deps: Vec<UsageFileDep>,
}

/// Row validity inputs without the event payload, for freshness checks.
pub(crate) struct CachedFileMeta {
    pub(crate) size: u64,
    pub(crate) mtime_ns: i64,
    pub(crate) scanned_at_ms: i64,
    pub(crate) deps: Vec<UsageFileDep>,
}

/// Dependency representation written before native path bytes were persisted.
#[derive(Serialize, Deserialize)]
struct LegacyUsageFileDep {
    path: String,
    size: u64,
    mtime_ns: i64,
    exists: bool,
}

impl From<LegacyUsageFileDep> for UsageFileDep {
    fn from(dependency: LegacyUsageFileDep) -> Self {
        Self {
            native_path: dependency.path.as_bytes().to_vec(),
            path: dependency.path,
            size: dependency.size,
            mtime_ns: dependency.mtime_ns,
            exists: dependency.exists,
        }
    }
}

impl UsageCache {
    pub(crate) fn open_for_refresh(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".refresh.lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        lock.lock()?;
        let mut cache = Self::open(path)?;
        cache._refresh_lock = Some(lock);
        Ok(cache)
    }

    pub(crate) fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(2))?;
        // Postcard encodes event fields positionally. Rebuild the disposable cache
        // when its event layout changes so old rows cannot decode with shifted fields.
        let event_format: i64 =
            connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if event_format != 1 {
            connection
                .execute_batch("DROP TABLE IF EXISTS usage_file_cache; PRAGMA user_version = 1;")?;
        }
        // Drop pre-postcard cache tables and any schema missing a required column: the
        // JSON-era claude table, the pre-rename blob column, and the deps_blob column that
        // records cross-file dependencies. A missing column means an older layout, so the
        // table is rebuilt rather than migrated.
        let current_columns: i64 = connection.query_row(
            "SELECT count(*) FROM pragma_table_info('usage_file_cache')
             WHERE name IN ('events_blob', 'deps_blob')",
            [],
            |row| row.get(0),
        )?;
        if current_columns < 2 {
            connection.execute_batch("DROP TABLE IF EXISTS usage_file_cache;")?;
        }
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             DROP TABLE IF EXISTS claude_usage_file_cache;
             CREATE TABLE IF NOT EXISTS usage_file_cache (
                 source TEXT NOT NULL,
                 path TEXT NOT NULL,
                 parser_version INTEGER NOT NULL,
                 size INTEGER NOT NULL,
                 mtime_ns INTEGER NOT NULL,
                 scanned_at_ms INTEGER NOT NULL,
                 events_blob BLOB NOT NULL,
                 deps_blob BLOB NOT NULL,
                 PRIMARY KEY (source, path)
             );",
        )?;
        // Pre-row_idx facts keyed (source, path, source_order): files with
        // several same-order events (every Cursor database) could never
        // insert, and surviving rows predate the refresh reconcile gates.
        // Facts are disposable derived state, so drop them — and their sync
        // rows, which would otherwise validate an empty table — and let the
        // next refresh rebuild through the legacy path.
        /// Canonical facts DDL. `row_idx` is the event's position within its file's
        /// contribution (not a timestamp): sources like Cursor emit many events with
        /// identical `(timestamp, source_order)`, so the old `(source, path,
        /// source_order)` key collided and every multi-event file failed its facts
        /// write. Reads break ties with `row_idx` after the report keys, which
        /// reproduces the assembly order exactly: a file's rows are always written in
        /// sorted order, so its `row_idx` subsequence preserves it.
        const FACTS_DDL: &str = "CREATE TABLE IF NOT EXISTS usage_facts (
                  source TEXT NOT NULL,
                  path TEXT NOT NULL,
                  row_idx INTEGER NOT NULL DEFAULT 0,
                  source_order INTEGER NOT NULL,
                  ordinal INTEGER NOT NULL,
                  timestamp_ms INTEGER NOT NULL,
                  session_id TEXT,
                  project TEXT,
                  provider TEXT,
                  model TEXT,
                  source_record_id TEXT,
                  request_id TEXT,
                  message_id TEXT,
                  raw_input INTEGER NOT NULL DEFAULT 0,
                  uncached_input INTEGER NOT NULL DEFAULT 0,
                  cache_read INTEGER NOT NULL DEFAULT 0,
                  cache_write INTEGER NOT NULL DEFAULT 0,
                  cache_write_1h INTEGER NOT NULL DEFAULT 0,
                  output INTEGER NOT NULL DEFAULT 0,
                  reasoning INTEGER NOT NULL DEFAULT 0,
                  source_cost_usd REAL,
                  cost_authoritative INTEGER NOT NULL DEFAULT 0,
                  dedupe_confidence TEXT NOT NULL DEFAULT '',
                  conservative_undercount INTEGER NOT NULL DEFAULT 0,
                  cache_chain_excluded INTEGER NOT NULL DEFAULT 0,
                  sidechain INTEGER NOT NULL DEFAULT 0,
                  permission_review INTEGER NOT NULL DEFAULT 0,
                  PRIMARY KEY (source, path, row_idx)
              );
              CREATE INDEX IF NOT EXISTS usage_facts_time
                  ON usage_facts(timestamp_ms);
              CREATE INDEX IF NOT EXISTS usage_facts_session
                  ON usage_facts(source, session_id);
              -- Per-source report order, served straight from the index with no
              -- sort step, including time-bounded ranges. `row_idx` is the
              -- final tiebreak so equal-key rows come out deterministic.
              -- Old-schema databases lose their facts table (and sync rows) to
              -- the row_idx migration above, so their indexes are rebuilt
              -- together with the table; steady-state opens hit the IF NOT
              -- EXISTS no-op below instead of rebuilding the index every time.
              CREATE INDEX IF NOT EXISTS usage_facts_source_time
                  ON usage_facts(ordinal, timestamp_ms, path, source_order, row_idx);
              -- Facts freshness per source: fingerprint of the file set the facts
              -- were built from (full identity for plain logs, paths only for
              -- volatile databases, whose bytes are judged by reuse windows),
              -- plus the parser version that built them and the warnings that
              -- build reported. Written atomically with the facts themselves, so
              -- a match means the facts (and their warnings) are current.
               CREATE TABLE IF NOT EXISTS usage_fact_sync (
                   source TEXT PRIMARY KEY,
                   fingerprint TEXT NOT NULL,
                   parser_version INTEGER NOT NULL,
                   generation TEXT NOT NULL,
                   warnings TEXT NOT NULL DEFAULT '[]'
               );";
        // Inspect before creating indexes that reference the new columns. DDL
        // and sync invalidation commit together: interruption cannot leave an
        // old checkpoint validating a newly empty facts table.
        let schema_current = |connection: &Connection| -> rusqlite::Result<bool> {
            connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('usage_facts') WHERE name = 'row_idx')
                    AND EXISTS(SELECT 1 FROM pragma_table_info('usage_fact_sync') WHERE name = 'generation')",
                [],
                |row| row.get(0),
            )
        };
        if !schema_current(&connection)? {
            let transaction = rusqlite::Transaction::new_unchecked(
                &connection,
                rusqlite::TransactionBehavior::Immediate,
            )?;
            // Another process may have migrated while this connection waited.
            if !schema_current(&transaction)? {
                transaction.execute_batch(
                    "DROP TABLE IF EXISTS usage_facts;
                     DROP TABLE IF EXISTS usage_fact_sync;
                     DROP TABLE IF EXISTS usage_fact_files;",
                )?;
                transaction.execute_batch(FACTS_DDL)?;
            }
            transaction.commit()?;
        }
        connection.execute_batch(FACTS_DDL)?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS usage_fact_files (
                 source TEXT NOT NULL,
                 path TEXT NOT NULL,
                 digest BLOB NOT NULL,
                 PRIMARY KEY (source, path)
             );
             CREATE TRIGGER IF NOT EXISTS usage_fact_insert_invalidates_digest
             AFTER INSERT ON usage_facts BEGIN
                 DELETE FROM usage_fact_files WHERE source = NEW.source AND path = NEW.path;
             END;
             CREATE TRIGGER IF NOT EXISTS usage_fact_update_invalidates_digest
             AFTER UPDATE ON usage_facts BEGIN
                 DELETE FROM usage_fact_files WHERE source = OLD.source AND path = OLD.path;
                 DELETE FROM usage_fact_files WHERE source = NEW.source AND path = NEW.path;
             END;
             CREATE TRIGGER IF NOT EXISTS usage_fact_delete_invalidates_digest
             AFTER DELETE ON usage_facts BEGIN
                 DELETE FROM usage_fact_files WHERE source = OLD.source AND path = OLD.path;
             END;",
        )?;
        if event_format != 1 || current_columns < 2 {
            connection.execute("DELETE FROM usage_fact_sync", [])?;
        }
        // A blob update is not a facts update. Invalidate in the same SQLite
        // transaction, including deletes/quarantines and writes from other
        // connections, so an interrupted refresh cannot bless old facts with
        // new blob metadata.
        connection.execute_batch(
            "CREATE TRIGGER IF NOT EXISTS usage_blob_insert_invalidates_facts
             AFTER INSERT ON usage_file_cache BEGIN
                 DELETE FROM usage_fact_sync WHERE source = NEW.source;
             END;
             CREATE TRIGGER IF NOT EXISTS usage_blob_update_invalidates_facts
             AFTER UPDATE ON usage_file_cache BEGIN
                 DELETE FROM usage_fact_sync WHERE source IN (OLD.source, NEW.source);
             END;
             CREATE TRIGGER IF NOT EXISTS usage_blob_delete_invalidates_facts
             AFTER DELETE ON usage_file_cache BEGIN
                 DELETE FROM usage_fact_sync WHERE source = OLD.source;
             END;",
        )?;
        // No VACUUM on the report path: it runs synchronously under the scan lock and
        // causes occasional large latency spikes. Run `vacuum_if_bloated` explicitly
        // from maintenance instead.
        Ok(Self {
            connection,
            _refresh_lock: None,
        })
    }

    /// Chunked saves rewrite blob rows continuously and freed pages are never returned to
    /// the filesystem, so the cache file can grow to a large multiple of its live data.
    /// Explicit maintenance only: never call on the report path.
    #[allow(dead_code)]
    pub(crate) fn vacuum_if_bloated(path: &Path) -> Result<()> {
        let connection = Connection::open(path)?;
        let stats = (|| -> rusqlite::Result<(i64, i64, i64)> {
            let single = |pragma: &str| connection.query_row(pragma, [], |row| row.get(0));
            Ok((
                single("PRAGMA page_count")?,
                single("PRAGMA freelist_count")?,
                single("PRAGMA page_size")?,
            ))
        })();
        if let Ok((page_count, freelist_count, page_size)) = stats
            && freelist_count.saturating_mul(page_size) >= 64 * 1024 * 1024
            && freelist_count >= page_count / 4
        {
            let _ = connection.execute_batch("VACUUM;");
        }
        Ok(())
    }

    pub(crate) fn load_source(
        &self,
        source: &str,
        parser_version: i64,
    ) -> Result<HashMap<String, CachedFileRow>> {
        self.connection.execute(
            "DELETE FROM usage_file_cache WHERE source = ?1 AND parser_version != ?2",
            params![source, parser_version],
        )?;
        let mut cached = HashMap::new();
        let mut invalid_paths = Vec::new();
        {
            let mut statement = self.connection.prepare(
                "SELECT path, size, mtime_ns, scanned_at_ms, events_blob, deps_blob FROM usage_file_cache
                 WHERE source = ?1 AND parser_version = ?2",
            )?;
            let mut rows = statement.query(params![source, parser_version])?;
            while let Some(row) = rows.next()? {
                let path: String = row.get(0)?;
                let size = row.get::<_, i64>(1)? as u64;
                let mtime_ns = row.get::<_, i64>(2)?;
                let scanned_at_ms = row.get::<_, i64>(3)?;
                let Ok(events_blob) = row.get::<_, Vec<u8>>(4) else {
                    invalid_paths.push(path);
                    continue;
                };
                let Ok(deps_blob) = row.get::<_, Vec<u8>>(5) else {
                    invalid_paths.push(path);
                    continue;
                };
                let deps = postcard::from_bytes::<Vec<UsageFileDep>>(&deps_blob).or_else(|_| {
                    postcard::from_bytes::<Vec<LegacyUsageFileDep>>(&deps_blob).map(
                        |dependencies| dependencies.into_iter().map(UsageFileDep::from).collect(),
                    )
                });
                let Ok(deps) = deps else {
                    invalid_paths.push(path);
                    continue;
                };
                cached.insert(
                    path,
                    CachedFileRow {
                        size,
                        mtime_ns,
                        scanned_at_ms,
                        events_blob,
                        deps,
                    },
                );
            }
        }
        for path in invalid_paths {
            self.connection.execute(
                "DELETE FROM usage_file_cache WHERE source = ?1 AND path = ?2",
                params![source, path],
            )?;
        }
        Ok(cached)
    }

    /// Metadata-only row load for freshness checks: same version purge and dependency
    /// quarantine as `load_source`, but without transferring event payload blobs.
    /// A row is usable here exactly when the scan's stat phase would treat it as a hit;
    /// corrupt event blobs are invisible at this level and demote to reparse on refresh.
    pub(crate) fn load_source_meta(
        &self,
        source: &str,
        parser_version: i64,
    ) -> Result<HashMap<String, CachedFileMeta>> {
        self.connection.execute(
            "DELETE FROM usage_file_cache WHERE source = ?1 AND parser_version != ?2",
            params![source, parser_version],
        )?;
        let mut cached = HashMap::new();
        let mut invalid_paths = Vec::new();
        {
            let mut statement = self.connection.prepare(
                "SELECT path, size, mtime_ns, scanned_at_ms, deps_blob FROM usage_file_cache
                 WHERE source = ?1 AND parser_version = ?2",
            )?;
            let mut rows = statement.query(params![source, parser_version])?;
            while let Some(row) = rows.next()? {
                let path: String = row.get(0)?;
                let size = row.get::<_, i64>(1)? as u64;
                let mtime_ns = row.get::<_, i64>(2)?;
                let scanned_at_ms = row.get::<_, i64>(3)?;
                let Ok(deps_blob) = row.get::<_, Vec<u8>>(4) else {
                    invalid_paths.push(path);
                    continue;
                };
                let deps = postcard::from_bytes::<Vec<UsageFileDep>>(&deps_blob).or_else(|_| {
                    postcard::from_bytes::<Vec<LegacyUsageFileDep>>(&deps_blob).map(
                        |dependencies| dependencies.into_iter().map(UsageFileDep::from).collect(),
                    )
                });
                let Ok(deps) = deps else {
                    invalid_paths.push(path);
                    continue;
                };
                cached.insert(
                    path,
                    CachedFileMeta {
                        size,
                        mtime_ns,
                        scanned_at_ms,
                        deps,
                    },
                );
            }
        }
        for path in invalid_paths {
            self.connection.execute(
                "DELETE FROM usage_file_cache WHERE source = ?1 AND path = ?2",
                params![source, path],
            )?;
        }
        Ok(cached)
    }

    /// Replace one source partition's canonical facts from its freshly scanned,
    /// reconciled, and sorted events — including uncached ones (e.g. unresolved
    /// forks) that never reach the blob cache, so fact rows always describe the
    /// current assembly exactly. One transaction with the freshness fingerprint:
    /// a failed write leaves the previous partition intact, mirroring blob-save
    /// failure semantics.
    pub(crate) fn replace_partition_facts(
        &mut self,
        filter: SourceFilter,
        events: &[UsageEvent],
        fingerprint: &[(String, u64, i64)],
        warnings: &[String],
    ) -> Result<()> {
        // Reconciliation still sees every occurrence. Only persistence is
        // incremental: compare each file's final canonical contribution, so a
        // winner change also rewrites affected files whose input did not change.
        let hash_start = Instant::now();
        let mut contributions =
            HashMap::<&str, Vec<&UsageEvent>, hashbrown::DefaultHashBuilder>::default();
        for run in events.chunk_by(|left, right| left.source_path == right.source_path) {
            contributions
                .entry(run[0].source_path.as_ref())
                .or_default()
                .extend(run);
        }
        usage_timing(hash_start, || {
            format!("{} canonical grouping", filter.as_str())
        });
        let serialization_start = Instant::now();
        let hashes: HashMap<&str, Vec<u8>> = contributions
            .par_iter()
            .map(|(path, events)| -> Result<_> {
                let mut hash = Sha256::new();
                let mut bytes = Vec::new();
                for event in events {
                    // Keep the persisted digest's length-prefixed byte stream,
                    // but feed SHA256 batches rather than two updates per event.
                    let prefix = bytes.len();
                    bytes.extend_from_slice(&[0; 8]);
                    postcard::to_io(&CachedUsageEventRef::from_event(event), &mut bytes)?;
                    let length = (bytes.len() - prefix - 8) as u64;
                    bytes[prefix..prefix + 8].copy_from_slice(&length.to_le_bytes());
                    if bytes.len() >= 64 * 1024 {
                        hash.update(&bytes);
                        bytes.clear();
                    }
                }
                hash.update(&bytes);
                Ok((*path, hash.finalize().to_vec()))
            })
            .collect::<Result<_>>()?;
        usage_timing(serialization_start, || {
            format!("{} canonical serialization and SHA256", filter.as_str())
        });
        usage_timing(hash_start, || {
            format!("{} canonical hashes", filter.as_str())
        });
        let paths_start = Instant::now();
        let transaction = self.connection.transaction()?;
        let previous: HashMap<String, Option<Vec<u8>>> = {
            let mut statement = transaction.prepare(
                // Seek once per file instead of scanning every fact's path.
                // Enumerate facts, not just digests: old/partial metadata may
                // omit a file whose obsolete facts must still be removed.
                "WITH RECURSIVE paths(path) AS (
                     SELECT min(path) FROM usage_facts WHERE source = ?1
                     UNION ALL
                     SELECT (
                         SELECT min(path) FROM usage_facts
                         WHERE source = ?1 AND path > paths.path
                     ) FROM paths WHERE path IS NOT NULL
                 )
                 SELECT paths.path, files.digest FROM paths
                 LEFT JOIN usage_fact_files AS files
                   ON files.source = ?1 AND files.path = paths.path
                 WHERE paths.path IS NOT NULL",
            )?;
            statement
                .query_map([filter.as_str()], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<rusqlite::Result<_>>()?
        };
        usage_timing(paths_start, || {
            format!("{} canonical paths", filter.as_str())
        });
        let persist_start = Instant::now();
        let mut changed: HashSet<&str> = hashes
            .iter()
            .filter(|(path, digest)| previous.get(**path).and_then(Option::as_ref) != Some(*digest))
            .map(|(path, _)| *path)
            .collect();
        changed.extend(
            previous
                .keys()
                .map(String::as_str)
                .filter(|path| !hashes.contains_key(path)),
        );
        let rows_start = Instant::now();
        {
            let mut delete =
                transaction.prepare("DELETE FROM usage_facts WHERE source = ?1 AND path = ?2")?;
            for path in &changed {
                delete.execute(params![filter.as_str(), path])?;
            }
        }
        if contributions.keys().all(|path| changed.contains(path)) {
            // A full population already visits every event. Keep physical rows
            // in report order so the first ordered read has good page locality.
            insert_fact_events(&transaction, filter, events)?;
        } else {
            insert_fact_events(
                &transaction,
                filter,
                changed
                    .iter()
                    .filter_map(|path| contributions.get(path))
                    .flatten()
                    .copied(),
            )?;
        }
        usage_timing(rows_start, || {
            format!("{} canonical row updates", filter.as_str())
        });
        let checkpoint_start = Instant::now();
        {
            let mut insert = transaction.prepare(
                "INSERT OR REPLACE INTO usage_fact_files(source, path, digest) VALUES (?1, ?2, ?3)",
            )?;
            for path in &changed {
                if let Some(digest) = hashes.get(path) {
                    insert.execute(params![filter.as_str(), path, digest])?;
                }
            }
        }
        write_fact_sync(&transaction, filter, fingerprint, warnings)?;
        transaction.commit()?;
        usage_timing(checkpoint_start, || {
            format!("{} canonical checkpoint and commit", filter.as_str())
        });
        usage_timing(persist_start, || {
            format!(
                "{} canonical writes ({} files)",
                filter.as_str(),
                changed.len()
            )
        });
        Ok(())
    }

    /// Atomically replace the facts of individual files (changed, vanished, or
    /// previously uncached): delete their rows, insert the fresh events, and
    /// refresh the partition fingerprint. Hits keep their rows untouched.
    pub(crate) fn upsert_file_facts(
        &mut self,
        filter: SourceFilter,
        removed_paths: &[String],
        events: &[UsageEvent],
        fingerprint: &[(String, u64, i64)],
        warnings: &[String],
    ) -> Result<()> {
        let transaction = self.connection.transaction()?;
        if !removed_paths.is_empty() {
            let mut delete =
                transaction.prepare("DELETE FROM usage_facts WHERE source = ?1 AND path = ?2")?;
            for path in removed_paths {
                delete.execute(params![filter.as_str(), path])?;
            }
        }
        insert_fact_events(&transaction, filter, events)?;
        write_fact_sync(&transaction, filter, fingerprint, warnings)?;
        transaction.commit()?;
        Ok(())
    }

    /// Freshness fingerprint recorded with a partition's facts, if any, plus the
    /// warnings that build reported. A missing row (or unreadable warnings JSON)
    /// means no usable facts: the caller falls back to a rebuild, which heals it.
    pub(crate) fn fact_sync(&self, source: &str) -> Result<Option<(String, i64, Vec<String>)>> {
        let row: Option<(String, i64, String)> = self
            .connection
            .query_row(
                "SELECT fingerprint, parser_version, warnings FROM usage_fact_sync WHERE source = ?1",
                params![source],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map(Some)
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                error => Err::<_, anyhow::Error>(error.into()),
            })?;
        row.map(
            |(fingerprint, version, warnings)| -> Result<(String, i64, Vec<String>)> {
                let warnings = serde_json::from_str(&warnings).unwrap_or_default();
                Ok((fingerprint, version, warnings))
            },
        )
        .transpose()
    }

    /// Identity of the committed facts that a snapshot actually loaded. This
    /// changes even when source filenames/mtimes do not (e.g. WAL updates).
    pub(crate) fn fact_generation(&self, source: &str) -> Result<Option<String>> {
        use rusqlite::OptionalExtension;
        Ok(self
            .connection
            .query_row(
                "SELECT generation FROM usage_fact_sync WHERE source = ?1",
                [source],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub(crate) fn invalidate_facts(&self, source: &str) -> Result<()> {
        self.connection
            .execute("DELETE FROM usage_fact_sync WHERE source = ?1", [source])?;
        Ok(())
    }
}

/// Insert canonical fact rows. The caller owns atomicity (same transaction as any
/// accompanying deletes and the sync row) and ordering within each file. The
/// order between files is irrelevant since reads order by key. `connection`
/// accepts transactions through deref.
fn insert_fact_events<'a>(
    connection: &Connection,
    filter: SourceFilter,
    events: impl IntoIterator<Item = &'a UsageEvent>,
) -> Result<()> {
    let ordinal = source_ordinal(filter) as i64;
    let mut statement = connection.prepare(
        "INSERT INTO usage_facts(
             source, path, row_idx, source_order, ordinal, timestamp_ms, session_id,
             project, provider, model, source_record_id, request_id, message_id,
             raw_input, uncached_input, cache_read, cache_write, cache_write_1h,
             output, reasoning, source_cost_usd, cost_authoritative,
             dedupe_confidence, conservative_undercount, cache_chain_excluded,
             sidechain, permission_review
         ) VALUES (
             ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
             ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27
         )",
    )?;
    // Per-file positions, in insertion order. Callers preserve the sorted
    // subsequence of each file, so `row_idx` reproduces report order as
    // the final read tiebreak.
    let mut per_file: HashMap<&str, i64> = HashMap::new();
    for event in events {
        debug_assert_eq!(event.source, filter.as_str());
        let path: &str = event.source_path.as_ref();
        let row_idx = per_file.entry(path).or_insert(0);
        let current = *row_idx;
        *row_idx += 1;
        statement.execute(params![
            filter.as_str(),
            path,
            current,
            event.source_order as i64,
            ordinal,
            event.timestamp_ms as i64,
            event.session_id.as_deref(),
            event.project.as_deref(),
            event.provider.as_deref(),
            event.model.as_deref(),
            event.source_record_id.as_deref(),
            event.request_id.as_deref(),
            event.message_id.as_deref(),
            event.tokens.raw_input as i64,
            event.tokens.uncached_input as i64,
            event.tokens.cache_read as i64,
            event.tokens.cache_write as i64,
            event.tokens.cache_write_1h as i64,
            event.tokens.output as i64,
            event.tokens.reasoning as i64,
            event.source_cost_usd,
            i64::from(event.cost_authoritative),
            event.dedupe_confidence,
            i64::from(event.conservative_undercount),
            i64::from(event.cache_chain_excluded),
            i64::from(event.sidechain),
            i64::from(event.permission_review),
        ])?;
    }
    Ok(())
}

/// Record a partition's freshness fingerprint atomically with its facts.
/// Warnings accumulate deduplicated across upserts: a warning stays until a full
/// (legacy) rebuild recomputes them, so a fixed file's warning can linger — the
/// same direction as blob-save staleness, and self-healing on the next rebuild.
pub(crate) fn write_fact_sync(
    connection: &Connection,
    filter: SourceFilter,
    fingerprint: &[(String, u64, i64)],
    warnings: &[String],
) -> Result<()> {
    connection.execute(
        "INSERT INTO usage_fact_sync(source, fingerprint, parser_version, warnings, generation)
         VALUES (?1, ?2, ?3, ?4, hex(randomblob(16)))
         ON CONFLICT(source) DO UPDATE SET
             fingerprint = excluded.fingerprint,
             parser_version = excluded.parser_version,
             warnings = excluded.warnings,
             generation = excluded.generation",
        params![
            filter.as_str(),
            fingerprint_files(&stable_triples(filter, fingerprint)),
            source_spec(filter).parser_version,
            serde_json::to_string(warnings).unwrap_or_else(|_| "[]".into()),
        ],
    )?;
    Ok(())
}

impl UsageCache {
    pub(crate) fn delete_stale(&mut self, source: &str, stale_paths: &[String]) -> Result<()> {
        let transaction = self.connection.transaction()?;
        for path in stale_paths {
            transaction.execute(
                "DELETE FROM usage_file_cache WHERE source = ?1 AND path = ?2",
                params![source, path],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn save_batch(
        &mut self,
        source: &str,
        parser_version: i64,
        scanned_at_ms: i64,
        parsed: &[ParsedUsageFile],
    ) -> Result<()> {
        let prepared = parsed
            .iter()
            .filter(|file| file.cacheable)
            .map(|file| {
                let cached = file
                    .events
                    .iter()
                    .map(CachedUsageEventRef::from_event)
                    .collect::<Vec<_>>();
                Ok((
                    file.path.to_string_lossy().to_string(),
                    file.size,
                    file.mtime_ns,
                    postcard::to_stdvec(&cached)?,
                    postcard::to_stdvec(&file.deps)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let transaction = self.connection.transaction()?;
        for (path, size, mtime_ns, events_blob, deps_blob) in prepared {
            transaction.execute(
                "INSERT INTO usage_file_cache(
                     source, path, parser_version, size, mtime_ns, scanned_at_ms, events_blob, deps_blob
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(source, path) DO UPDATE SET
                     parser_version = excluded.parser_version,
                     size = excluded.size,
                     mtime_ns = excluded.mtime_ns,
                     scanned_at_ms = excluded.scanned_at_ms,
                     events_blob = excluded.events_blob,
                     deps_blob = excluded.deps_blob",
                params![
                    source,
                    path,
                    parser_version,
                    size as i64,
                    mtime_ns,
                    scanned_at_ms,
                    events_blob,
                    deps_blob
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::scan::{
        FileParse, SourceScan, no_volatile_reuse, scan_files_cached, usage_file_metadata,
    };
    use super::super::{UsageQuery, cache_event, scan_usage};
    use super::*;
    use rusqlite::Connection;
    use std::fs;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn old_facts_schema_migrates_without_trusting_its_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("cache.sqlite3");
        let cache = UsageCache::open(&path).unwrap();
        cache
            .connection
            .execute_batch(
                "DROP TABLE usage_facts;
             DROP TABLE usage_fact_sync;
             CREATE TABLE usage_facts (
                 source TEXT, path TEXT, source_order INTEGER,
                 PRIMARY KEY (source, path, source_order));
             INSERT INTO usage_facts VALUES ('cursor', 'state.vscdb', 0);
             CREATE TABLE usage_fact_sync (
                 source TEXT PRIMARY KEY, fingerprint TEXT, parser_version INTEGER);
             INSERT INTO usage_fact_sync VALUES ('cursor', 'old-checkpoint', 1);",
            )
            .unwrap();
        drop(cache);
        // No old index is present: migration must precede new index creation.
        let mut migrated = UsageCache::open(&path).unwrap();
        assert!(migrated.fact_sync("cursor").unwrap().is_none());
        let mut event = cache_event("session", 0, "model", 10, 0, 0);
        event.source = "cursor";
        let other = event.clone();
        migrated
            .replace_partition_facts(SourceFilter::Cursor, &[event, other], &[], &[])
            .unwrap();
        let rows: u64 = migrated
            .connection
            .query_row("SELECT count(*) FROM usage_facts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 2);
        assert!(migrated.fact_generation("cursor").unwrap().is_some());
    }

    #[test]
    fn failed_facts_migration_rolls_back_the_old_rows_and_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("cache.sqlite3");
        let cache = UsageCache::open(&path).unwrap();
        cache
            .connection
            .execute_batch(
                "DROP TABLE usage_facts;
             DROP TABLE usage_fact_sync;
             CREATE TABLE usage_facts (source TEXT, source_order INTEGER);
             INSERT INTO usage_facts VALUES ('cursor', 0);
             CREATE TABLE usage_fact_sync (source TEXT, fingerprint TEXT);
             INSERT INTO usage_fact_sync VALUES ('cursor', 'old-checkpoint');
             CREATE TABLE usage_facts_time (block_index_creation INTEGER);",
            )
            .unwrap();
        drop(cache);
        assert!(
            UsageCache::open(&path).is_err(),
            "conflicting schema must abort migration"
        );
        let connection = Connection::open(&path).unwrap();
        let rows: u64 = connection
            .query_row("SELECT count(*) FROM usage_facts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 1, "old facts restored when DDL fails");
        let fingerprint: String = connection
            .query_row("SELECT fingerprint FROM usage_fact_sync", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(fingerprint, "old-checkpoint");
    }

    #[test]
    fn usage_event_layout_change_rebuilds_cached_rows() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("usage-cache.sqlite3");
        let cache = UsageCache::open(&path).expect("open cache");
        cache
            .connection
            .execute_batch(
                "INSERT INTO usage_file_cache(source, path, parser_version, size, mtime_ns,
                 scanned_at_ms, events_blob, deps_blob)
             VALUES ('codex', '/tmp/review.jsonl', 1, 10, 20, 30, X'00', X'00');
             PRAGMA user_version = 0;",
            )
            .expect("seed older event layout");
        drop(cache);
        let rebuilt = UsageCache::open(&path).expect("rebuild cache");
        let rows: u64 = rebuilt
            .connection
            .query_row("SELECT count(*) FROM usage_file_cache", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 0);
        assert_eq!(
            rebuilt
                .connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn usage_parser_version_change_invalidates_cached_rows() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("usage-cache.sqlite3");
        let cache = UsageCache::open(&path).expect("open cache");
        cache
            .connection
            .execute(
                "INSERT INTO usage_file_cache(
                    source, path, parser_version, size, mtime_ns, scanned_at_ms,
                    events_blob, deps_blob
                 ) VALUES ('claude', '/tmp/session.jsonl', 1, 10, 20, 30, ?1, ?2)",
                params![
                    postcard::to_stdvec(&Vec::<CachedUsageEvent>::new()).unwrap(),
                    postcard::to_stdvec(&Vec::<UsageFileDep>::new()).unwrap()
                ],
            )
            .expect("seed stale cache row");

        assert!(
            cache
                .load_source("claude", 2)
                .expect("load new parser version")
                .is_empty()
        );
        let rows: i64 = cache
            .connection
            .query_row(
                "SELECT count(*) FROM usage_file_cache WHERE source = 'claude'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn malformed_dependency_rows_are_quarantined() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("usage-cache.sqlite3");
        let cache = UsageCache::open(&path).expect("open cache");
        cache
            .connection
            .execute(
                "INSERT INTO usage_file_cache(
                    source, path, parser_version, size, mtime_ns, scanned_at_ms,
                    events_blob, deps_blob
                 ) VALUES ('omp', '/tmp/session.jsonl', 1, 10, 20, 30, ?1, ?2)",
                params![
                    postcard::to_stdvec(&Vec::<CachedUsageEvent>::new()).unwrap(),
                    vec![0xff_u8]
                ],
            )
            .expect("seed malformed cache row");

        assert!(cache.load_source("omp", 1).expect("load cache").is_empty());
        let rows: i64 = cache
            .connection
            .query_row(
                "SELECT count(*) FROM usage_file_cache WHERE source = 'omp'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn malformed_event_rows_are_quarantined() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("usage-cache.sqlite3");
        let cache = UsageCache::open(&path).expect("open cache");
        cache
            .connection
            .execute(
                "INSERT INTO usage_file_cache(
                    source, path, parser_version, size, mtime_ns, scanned_at_ms,
                    events_blob, deps_blob
                 ) VALUES ('omp', '/tmp/session.jsonl', 1, 10, 20, 30, ?1, ?2)",
                params![
                    "not a blob",
                    postcard::to_stdvec(&Vec::<UsageFileDep>::new()).unwrap()
                ],
            )
            .expect("seed malformed cache row");

        assert!(cache.load_source("omp", 1).expect("load cache").is_empty());
        let rows: i64 = cache
            .connection
            .query_row(
                "SELECT count(*) FROM usage_file_cache WHERE source = 'omp'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn canonical_delta_preserves_tied_row_order_in_each_changed_file() {
        let temp = tempfile::tempdir().unwrap();
        let mut cache = UsageCache::open(&temp.path().join("cache.sqlite3")).unwrap();
        let mut events = Vec::new();
        for index in 0..18 {
            let mut event = cache_event("session", index / 6, "model", index, 0, 0);
            event.source_path = Arc::from(["a", "keep", "z"][index as usize % 3]);
            event.source_order = 0;
            events.push(event);
        }
        super::super::snapshot::sort_usage_events(&mut events);
        cache
            .replace_partition_facts(SourceFilter::Claude, &events, &[], &[])
            .unwrap();
        cache
            .connection
            .execute_batch(
                "CREATE TRIGGER protect_unchanged BEFORE DELETE ON usage_facts
                 WHEN OLD.path = 'keep'
                 BEGIN SELECT RAISE(ABORT, 'unchanged file rewritten'); END;",
            )
            .unwrap();
        for event in &mut events {
            if event.source_path.as_ref() != "keep" {
                event.tokens.output += 100;
            }
        }
        cache
            .replace_partition_facts(SourceFilter::Claude, &events, &[], &[])
            .unwrap();
        let actual: Vec<_> = super::super::facts::read_ordinal_run(
            &cache.connection,
            source_ordinal(SourceFilter::Claude) as i64,
            None,
            None,
        )
        .unwrap()
        .into_iter()
        .map(super::super::facts::FactRow::into_event)
        .collect();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(events).unwrap()
        );
    }

    #[test]
    fn canonical_digest_keeps_legacy_framing_across_batches_and_interleaved_paths() {
        let temp = tempfile::tempdir().unwrap();
        let mut cache = UsageCache::open(&temp.path().join("cache.sqlite3")).unwrap();
        let mut events = Vec::new();
        for index in 0..600 {
            let mut event = cache_event("会話", index, "model", index, 0, 0);
            event.source_path = Arc::from(if index % 3 == 0 { "other" } else { "file" });
            event.message_id = Some(format!("{index}:{}", "é".repeat(200)));
            // Include one record larger than a hash batch as well as many small
            // records, so both batch flushing and final remainder are covered.
            if index == 10 {
                event.project = Some("large".repeat(20_000));
            }
            events.push(event);
        }
        cache
            .replace_partition_facts(SourceFilter::Claude, &events, &[], &[])
            .unwrap();
        for path in ["file", "other"] {
            let mut expected = Sha256::new();
            for event in events
                .iter()
                .filter(|event| event.source_path.as_ref() == path)
            {
                let bytes = postcard::to_stdvec(&CachedUsageEvent::from_event(event)).unwrap();
                expected.update((bytes.len() as u64).to_le_bytes());
                expected.update(bytes);
            }
            let stored: Vec<u8> = cache
                .connection
                .query_row(
                    "SELECT digest FROM usage_fact_files WHERE source = 'claude' AND path = ?1",
                    [path],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(stored, expected.finalize().as_slice());
        }
        // Compatible digests must reuse existing facts without rewriting them.
        cache
            .connection
            .execute_batch(
                "CREATE TRIGGER reject_rewrite BEFORE DELETE ON usage_facts
             BEGIN SELECT RAISE(ABORT, 'unchanged canonical contribution'); END;",
            )
            .unwrap();
        cache
            .replace_partition_facts(SourceFilter::Claude, &events, &[], &[])
            .unwrap();
    }

    #[test]
    fn same_order_facts_rows_do_not_collide() {
        // Cursor emits every event of a database with source_order 0; the
        // old (source, path, source_order) key rejected all but one row per
        // file and every such facts write failed.
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("usage-cache.sqlite3");
        let mut cache = UsageCache::open(&path).expect("open cache");
        let event = |record: &str| UsageEvent {
            source: "cursor",
            source_path: Arc::from("/tmp/state.vscdb"),
            source_record_id: Some(record.to_string()),
            session_id: Some("session".to_string()),
            request_id: None,
            message_id: None,
            timestamp_ms: 1000,
            project: None,
            provider: None,
            model: None,
            tokens: TokenBuckets::disjoint(10, 0, 0, 5),
            source_cost_usd: None,
            cost_authoritative: false,
            dedupe_confidence: "exact",
            conservative_undercount: false,
            cache_chain_excluded: false,
            sidechain: false,
            permission_review: false,
            source_order: 0,
        };
        let events = vec![event("a"), event("b")];
        cache
            .replace_partition_facts(crate::types::SourceFilter::Cursor, &events, &[], &[])
            .expect("facts write");
        let rows: i64 = cache
            .connection
            .query_row(
                "SELECT count(*) FROM usage_facts WHERE source = 'cursor'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 2);
        // Equal-key rows read back deterministic in write order (row_idx
        // tiebreak), matching the assembly order exactly.
        let runs = super::super::facts::read_fact_runs(
            &path,
            Some(crate::types::SourceFilter::Cursor),
            None,
            None,
        )
        .expect("read facts");
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0]
                .iter()
                .map(|row| row.source_record_id.clone())
                .collect::<Vec<_>>(),
            vec![Some("a".to_string()), Some("b".to_string())]
        );
    }

    #[test]
    fn hermes_disjoint_usage_parser_version_is_newer_than_repair_six() {
        assert_eq!(crate::sources::hermes::VERSIONS.usage, 7);
    }

    #[test]
    fn hermes_parser_version_change_reparses_a_repair_six_cache_row() {
        let temp = tempfile::tempdir().expect("tempdir");
        let db_path = temp.path().join("state.db");
        let conn = Connection::open(&db_path).expect("create db");
        conn.execute_batch(
            "CREATE TABLE sessions (id TEXT, model TEXT, started_at INTEGER, input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER, billing_provider TEXT, estimated_cost_usd REAL, cwd TEXT, git_repo_root TEXT, profile_name TEXT);",
        )
        .expect("create sessions");
        drop(conn);
        let cache_path = temp.path().join("usage-cache.sqlite3");
        let cache = UsageCache::open(&cache_path).expect("open cache");
        cache
            .connection
            .execute(
                "INSERT INTO usage_file_cache(
                    source, path, parser_version, size, mtime_ns, scanned_at_ms,
                    events_blob, deps_blob
                 ) VALUES ('hermes', ?1, 6, ?2, ?3, 30, ?4, ?5)",
                params![
                    db_path.to_string_lossy(),
                    fs::metadata(&db_path).unwrap().len() as i64,
                    usage_file_metadata(&db_path).unwrap().1,
                    postcard::to_stdvec(&Vec::<CachedUsageEvent>::new()).unwrap(),
                    postcard::to_stdvec(&Vec::<UsageFileDep>::new()).unwrap()
                ],
            )
            .expect("seed repair-six row");
        drop(cache);

        let mut cache = UsageCache::open(&cache_path).expect("reopen cache");
        let mut warnings = Vec::new();
        let mut events = Vec::new();
        let parses = AtomicUsize::new(0);
        scan_files_cached(
            SourceScan {
                source: "hermes",
                parser_version: crate::sources::hermes::VERSIONS.usage,
                volatile_reuse_ms: no_volatile_reuse,
            },
            std::slice::from_ref(&db_path),
            Some(&mut cache),
            &mut warnings,
            &mut events,
            |path| {
                parses.fetch_add(1, Ordering::SeqCst);
                crate::sources::hermes::parse_usage_file(path)
            },
        );
        assert_eq!(parses.load(Ordering::SeqCst), 1);
        assert!(warnings.is_empty());
        let version: i64 = cache
            .connection
            .query_row(
                "SELECT parser_version FROM usage_file_cache WHERE source = 'hermes'",
                [],
                |row| row.get(0),
            )
            .expect("reparsed row");
        assert_eq!(version, 7);
    }

    #[test]
    fn previous_dependency_postcard_format_loads_with_native_path_identity() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_path = temp.path().join("rollout.jsonl");
        let dependency_path = temp.path().join("parent.jsonl");
        fs::write(&source_path, "source").expect("write source");
        fs::write(&dependency_path, "dependency").expect("write dependency");
        let cache_path = temp.path().join("usage-cache.sqlite3");
        let cache = UsageCache::open(&cache_path).expect("open cache");
        let source_metadata = usage_file_metadata(&source_path).expect("source metadata");
        let dependency_metadata = usage_file_metadata(&dependency_path).expect("dep metadata");
        let source_key = source_path.to_string_lossy().to_string();
        let dependency_key = dependency_path.to_string_lossy().to_string();
        let legacy = vec![LegacyUsageFileDep {
            path: dependency_key.clone(),
            size: dependency_metadata.0,
            mtime_ns: dependency_metadata.1,
            exists: true,
        }];
        cache
            .connection
            .execute(
                "INSERT INTO usage_file_cache(
                    source, path, parser_version, size, mtime_ns, scanned_at_ms,
                    events_blob, deps_blob
                 ) VALUES ('codex', ?1, ?2, ?3, ?4, 30, ?5, ?6)",
                params![
                    source_key,
                    crate::sources::codex::VERSIONS.usage,
                    source_metadata.0 as i64,
                    source_metadata.1,
                    postcard::to_stdvec(&Vec::<CachedUsageEvent>::new()).unwrap(),
                    postcard::to_stdvec(&legacy).unwrap()
                ],
            )
            .expect("seed previous dependency format");

        let loaded = cache
            .load_source("codex", crate::sources::codex::VERSIONS.usage)
            .expect("load previous dependency format");
        let dependency = &loaded[&source_key].deps[0];
        assert_eq!(dependency.native_path, dependency_key.as_bytes());
        assert!(dependency.is_current());
    }

    #[test]
    fn legacy_nonempty_dependency_blob_cannot_become_a_dependency_free_hit() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_path = temp.path().join("rollout.jsonl");
        fs::write(&source_path, "source").expect("write source");
        let cache_path = temp.path().join("usage-cache.sqlite3");
        let cache = UsageCache::open(&cache_path).expect("open cache");
        let metadata = usage_file_metadata(&source_path).expect("metadata");
        let legacy = vec![LegacyUsageDependency {
            path: temp
                .path()
                .join("parent.jsonl")
                .to_string_lossy()
                .to_string(),
            size: 12,
            mtime_ns: 34,
        }];
        cache
            .connection
            .execute(
                "INSERT INTO usage_file_cache(
                    source, path, parser_version, size, mtime_ns, scanned_at_ms,
                    events_blob, deps_blob
                 ) VALUES ('codex', ?1, ?2, ?3, ?4, 30, ?5, ?6)",
                params![
                    source_path.to_string_lossy(),
                    crate::sources::codex::VERSIONS.usage,
                    metadata.0 as i64,
                    metadata.1,
                    postcard::to_stdvec(&Vec::<CachedUsageEvent>::new()).unwrap(),
                    postcard::to_stdvec(&legacy).unwrap()
                ],
            )
            .expect("seed legacy row");
        drop(cache);

        let mut cache = UsageCache::open(&cache_path).expect("reopen cache");
        let mut warnings = Vec::new();
        let mut events = Vec::new();
        let parses = AtomicUsize::new(0);
        scan_files_cached(
            SourceScan {
                source: "codex",
                parser_version: crate::sources::codex::VERSIONS.usage,
                volatile_reuse_ms: no_volatile_reuse,
            },
            std::slice::from_ref(&source_path),
            Some(&mut cache),
            &mut warnings,
            &mut events,
            |_| {
                parses.fetch_add(1, Ordering::SeqCst);
                Ok(FileParse::cacheable(Vec::new()))
            },
        );
        assert_eq!(parses.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn corrupted_dependency_blob_forces_a_reparse() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_path = temp.path().join("rollout.jsonl");
        fs::write(&source_path, "source").expect("write source");
        let cache_path = temp.path().join("usage-cache.sqlite3");
        let cache = UsageCache::open(&cache_path).expect("open cache");
        let metadata = usage_file_metadata(&source_path).expect("metadata");
        cache
            .connection
            .execute(
                "INSERT INTO usage_file_cache(
                    source, path, parser_version, size, mtime_ns, scanned_at_ms,
                    events_blob, deps_blob
                 ) VALUES ('codex', ?1, ?2, ?3, ?4, 30, ?5, ?6)",
                params![
                    source_path.to_string_lossy(),
                    crate::sources::codex::VERSIONS.usage,
                    metadata.0 as i64,
                    metadata.1,
                    postcard::to_stdvec(&Vec::<CachedUsageEvent>::new()).unwrap(),
                    vec![0xff_u8, 0x00_u8]
                ],
            )
            .expect("seed corrupt row");
        drop(cache);

        let mut cache = UsageCache::open(&cache_path).expect("reopen cache");
        let mut warnings = Vec::new();
        let mut events = Vec::new();
        let parses = AtomicUsize::new(0);
        scan_files_cached(
            SourceScan {
                source: "codex",
                parser_version: crate::sources::codex::VERSIONS.usage,
                volatile_reuse_ms: no_volatile_reuse,
            },
            std::slice::from_ref(&source_path),
            Some(&mut cache),
            &mut warnings,
            &mut events,
            |_| {
                parses.fetch_add(1, Ordering::SeqCst);
                Ok(FileParse::cacheable(Vec::new()))
            },
        );
        assert_eq!(parses.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn invalid_dependency_row_does_not_discard_valid_cache_rows() {
        let temp = tempfile::tempdir().expect("tempdir");
        let valid_path = temp.path().join("valid.jsonl");
        fs::write(&valid_path, "valid").expect("write valid source");
        let vanished_path = temp.path().join("vanished.jsonl");
        let malformed_path = temp.path().join("malformed.jsonl");
        let cache_path = temp.path().join("usage-cache.sqlite3");
        let cache = UsageCache::open(&cache_path).expect("open cache");
        let metadata = usage_file_metadata(&valid_path).expect("metadata");
        let empty_events = postcard::to_stdvec(&Vec::<CachedUsageEvent>::new()).unwrap();
        let empty_deps = postcard::to_stdvec(&Vec::<UsageFileDep>::new()).unwrap();
        cache
            .connection
            .execute(
                "INSERT INTO usage_file_cache(
                    source, path, parser_version, size, mtime_ns, scanned_at_ms,
                    events_blob, deps_blob
                 ) VALUES ('codex', ?1, ?2, ?3, ?4, 30, ?5, ?6)",
                params![
                    valid_path.to_string_lossy(),
                    crate::sources::codex::VERSIONS.usage,
                    metadata.0 as i64,
                    metadata.1,
                    empty_events,
                    empty_deps
                ],
            )
            .expect("seed valid row");
        cache
            .connection
            .execute(
                "INSERT INTO usage_file_cache(
                    source, path, parser_version, size, mtime_ns, scanned_at_ms,
                    events_blob, deps_blob
                 ) VALUES ('codex', ?1, ?2, 0, 0, 30, ?3, ?4)",
                params![
                    vanished_path.to_string_lossy(),
                    crate::sources::codex::VERSIONS.usage,
                    postcard::to_stdvec(&Vec::<CachedUsageEvent>::new()).unwrap(),
                    vec![0xff_u8, 0x00_u8]
                ],
            )
            .expect("seed invalid row");
        cache
            .connection
            .execute(
                "INSERT INTO usage_file_cache(
                    source, path, parser_version, size, mtime_ns, scanned_at_ms,
                    events_blob, deps_blob
                 ) VALUES ('codex', ?1, ?2, 0, 0, 30, ?3, 7)",
                params![
                    malformed_path.to_string_lossy(),
                    crate::sources::codex::VERSIONS.usage,
                    postcard::to_stdvec(&Vec::<CachedUsageEvent>::new()).unwrap(),
                ],
            )
            .expect("seed malformed dependency type row");

        let mut cache = cache;
        let mut warnings = Vec::new();
        let mut events = Vec::new();
        let parses = AtomicUsize::new(0);
        scan_files_cached(
            SourceScan {
                source: "codex",
                parser_version: crate::sources::codex::VERSIONS.usage,
                volatile_reuse_ms: no_volatile_reuse,
            },
            std::slice::from_ref(&valid_path),
            Some(&mut cache),
            &mut warnings,
            &mut events,
            |_| {
                parses.fetch_add(1, Ordering::SeqCst);
                Ok(FileParse::cacheable(Vec::new()))
            },
        );

        assert_eq!(parses.load(Ordering::SeqCst), 0);
        assert!(
            cache
                .load_source("codex", crate::sources::codex::VERSIONS.usage)
                .is_ok()
        );
        let invalid_rows: i64 = cache
            .connection
            .query_row(
                "SELECT count(*) FROM usage_file_cache WHERE source = 'codex' AND path = ?1",
                [vanished_path.to_string_lossy().as_ref()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(invalid_rows, 0);
        let malformed_rows: i64 = cache
            .connection
            .query_row(
                "SELECT count(*) FROM usage_file_cache WHERE source = 'codex' AND path = ?1",
                [malformed_path.to_string_lossy().as_ref()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(malformed_rows, 0);
    }

    #[test]
    fn claude_scanner_caches_normalized_usage_by_file_metadata() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        let transcript = projects.join("session.jsonl");
        std::fs::write(
            &transcript,
            concat!(
                r#"{"type":"assistant","sessionId":"session","requestId":"request","timestamp":"2026-07-03T01:02:05Z","cwd":"/repo/memex","costUSD":"invalid optional value","message":{"id":"message","model":"claude-sonnet-4-6","content":[{"type":"text","text":"ignored payload"}],"usage":{"inputTokens":10,"cacheReadInputTokens":2,"cacheCreationInputTokens":3,"outputTokens":4,"cache_creation":{"ephemeral_1h_input_tokens":1}}}}"#,
                "\n"
            ),
        )
        .expect("write transcript");
        let cache_path = tmp.path().join("usage-cache.sqlite3");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Claude),
            include_events: true,
            cache_path: Some(cache_path.clone()),
            ..UsageQuery::default()
        };

        let cold = scan_usage(&query).expect("cold scan");
        let warm = scan_usage(&query).expect("warm scan");
        let cache = Connection::open(cache_path).expect("open cache");
        let cached_files: u64 = cache
            .query_row(
                "SELECT count(*) FROM usage_file_cache WHERE source = 'claude'",
                [],
                |row| row.get(0),
            )
            .expect("count cached files");

        assert_eq!(cold.events, 1);
        assert_eq!(cold.details[0].tokens.total(), 19);
        assert_eq!(cold.details[0].tokens.cache_write_1h, 1);
        assert_eq!(cold.details[0].dedupe_confidence, "exact");
        assert_eq!(warm.total_tokens, cold.total_tokens);
        assert_eq!(cached_files, 1);
    }

    #[test]
    fn implementation_only_event_state_is_absent_from_public_json_and_cached_internally() {
        let mut event = cache_event("session", 0, "claude-sonnet-4-6", 100, 0, 0);
        event.cost_authoritative = true;
        event.cache_chain_excluded = true;
        event.sidechain = true;
        event.permission_review = true;
        event.source_order = 42;

        let json = serde_json::to_value(&event).unwrap();
        let object = json.as_object().unwrap();
        assert!(object.contains_key("source"));
        assert!(object.contains_key("model"));
        assert!(!object.contains_key("cost_authoritative"));
        assert!(!object.contains_key("cache_chain_excluded"));
        assert!(!object.contains_key("sidechain"));
        assert!(!object.contains_key("permission_review"));
        assert!(!object.contains_key("source_order"));

        let bytes = postcard::to_stdvec(&CachedUsageEvent::from_event(&event)).unwrap();
        let cached: CachedUsageEvent = postcard::from_bytes(&bytes).unwrap();
        assert!(cached.cost_authoritative);
        assert!(cached.cache_chain_excluded);
        let restored = cached.into_event("claude", Arc::from("cached"));
        assert!(restored.cost_authoritative);
        assert!(restored.cache_chain_excluded);
        assert!(restored.permission_review);
    }

    #[derive(Serialize)]
    struct LegacyUsageDependency {
        path: String,
        size: u64,
        mtime_ns: i64,
    }
}
