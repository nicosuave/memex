use super::{IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions, SourceFile};
use crate::state::{OpencodeDatabaseState, OpencodeSessionCursor};
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use simd_json::BorrowedValue;
use simd_json::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use walkdir::WalkDir;

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 2,
    index: 2,
    usage: 4,
};

/// Version for the SQLite event cursor and owned-session reconciliation rules.  This is
/// intentionally independent of the shared source identity/index versions: legacy JSON parsing
/// has not changed merely because database planning was added.
///
/// Bumped for the parent-linkage-only reclassification: unparented sessions
/// with non-primary agent values were stored as subagent and must reconcile
/// once so their records and analytics rows reflect the new kinds.
///
/// v3 also replaces the v1 event-rowid cursor with per-session
/// `(max_seq, max_time_updated)` cursors for v2 databases, so persisted v1
/// cursors must be re-derived once.
/// v4 removes legacy resurrection, splits tool records, and strengthens change detection.
pub const DATABASE_STATE_VERSION: u32 = 4;

pub fn matches_path(path: &str) -> bool {
    (path.contains("opencode/storage/message") || path.contains("opencode\\storage\\message"))
        || is_database_path(path)
}

pub(crate) fn is_database_path(path: &str) -> bool {
    let name = path.rsplit(['/', '\\']).next().unwrap_or(path);
    name.starts_with("opencode") && name.ends_with(".db")
}

pub fn data_roots() -> Vec<PathBuf> {
    std::env::var_os("OPENCODE_DATA_DIR")
        .map(|roots| {
            roots
                .to_string_lossy()
                .split(',')
                .map(|root| PathBuf::from(root.trim()))
                .collect()
        })
        .unwrap_or_else(|| vec![super::common::home().join(".local/share/opencode")])
}

pub fn storage_root() -> PathBuf {
    data_roots()
        .into_iter()
        .next()
        .unwrap_or_else(|| super::common::home().join(".local/share/opencode"))
        .join("storage")
}

pub fn message_root() -> PathBuf {
    storage_root().join("message")
}

pub fn parts_root() -> PathBuf {
    storage_root().join("part")
}

fn parts_root_for_session(session_dir: &Path) -> PathBuf {
    session_dir
        .parent()
        .and_then(Path::parent)
        .map(|storage| storage.join("part"))
        .unwrap_or_else(parts_root)
}

pub fn discover_sessions() -> anyhow::Result<Vec<SourceFile>> {
    crate::profiling::span!("opencode.discover_legacy");
    discover_sessions_from_roots(&data_roots())
}

pub fn discover_sessions_from_roots(roots: &[PathBuf]) -> anyhow::Result<Vec<SourceFile>> {
    let mut files = Vec::new();
    for root in roots {
        files.extend(discover_sessions_from_root(&root.join("storage/message"))?);
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    files.dedup_by(|left, right| left.path == right.path);
    Ok(files)
}

pub fn discover_sessions_from_root(root: &Path) -> anyhow::Result<Vec<SourceFile>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir()
            && entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("ses_"))
        {
            files.push(SourceFile {
                source: SourceKind::Opencode,
                path: entry.path(),
            });
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

/// Discover modern OpenCode databases without changing the legacy session-directory scan.
///
/// OpenCode's data directory may be configured as a comma-separated list, so discovery is
/// deliberately performed against every configured root and sorted globally for stable output.
pub fn discover_databases() -> anyhow::Result<Vec<SourceFile>> {
    crate::profiling::span!("opencode.discover_databases");
    discover_databases_from_roots(&data_roots())
}

pub(crate) fn discover_databases_from_roots(roots: &[PathBuf]) -> anyhow::Result<Vec<SourceFile>> {
    let mut paths = HashSet::new();
    for root in roots {
        let entries = match std::fs::read_dir(root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read OpenCode data root {}", root.display()));
            }
        };
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("opencode") && name.ends_with(".db"))
            {
                paths.insert(entry.path());
            }
        }
    }
    let mut files = paths
        .into_iter()
        .map(|path| SourceFile {
            source: SourceKind::Opencode,
            path,
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

/// Open an OpenCode database in a strictly read-only, WAL-compatible mode.
///
/// In particular, this helper does not set journal mode or run checkpoints: those operations
/// can write beside a database even when the main connection is read-only.
pub(crate) fn open_read_only_database(path: &Path) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("open OpenCode database read-only: {}", path.display()))?;
    connection
        .busy_timeout(Duration::from_millis(1_000))
        .with_context(|| format!("set OpenCode database busy timeout: {}", path.display()))?;
    connection
        .execute_batch("PRAGMA query_only = ON")
        .with_context(|| format!("enable SQLite query_only for {}", path.display()))?;
    Ok(connection)
}

const MODERN_SESSION_COLUMNS: &[&str] = &[
    "id",
    "parent_id",
    "directory",
    "time_created",
    "time_updated",
];
const MODERN_MESSAGE_COLUMNS: &[&str] = &["id", "session_id", "time_created", "data"];
const MODERN_PART_COLUMNS: &[&str] = &["id", "message_id", "data"];

fn require_modern_schema(connection: &Connection, path: &Path) -> Result<()> {
    for (table, columns) in [
        ("session", MODERN_SESSION_COLUMNS),
        ("message", MODERN_MESSAGE_COLUMNS),
        ("part", MODERN_PART_COLUMNS),
    ] {
        let exists = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
                [table],
                |row| row.get::<_, i64>(0),
            )
            .with_context(|| format!("inspect OpenCode schema in {}", path.display()))?;
        if exists == 0 {
            bail!(
                "unrecognized OpenCode SQLite schema in {}: missing table `{table}`",
                path.display()
            );
        }
        let pragma = format!("PRAGMA table_info({table})");
        let mut statement = connection
            .prepare(&pragma)
            .with_context(|| format!("inspect OpenCode `{table}` table in {}", path.display()))?;
        let found = statement
            .query_map([], |row| row.get::<_, String>(1))
            .with_context(|| format!("read OpenCode `{table}` columns in {}", path.display()))?
            .collect::<rusqlite::Result<HashSet<_>>>()
            .with_context(|| format!("read OpenCode `{table}` columns in {}", path.display()))?;
        for column in columns {
            if !found.contains(*column) {
                bail!(
                    "unrecognized OpenCode SQLite schema in {}: `{table}` lacks `{column}`",
                    path.display()
                );
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpencodeSession {
    pub id: String,
    pub parent_id: Option<String>,
    pub directory: String,
    pub time_created: u64,
    pub time_updated: u64,
}

/// Whether a table exists in an OpenCode database.
fn table_exists(connection: &Connection, path: &Path, table: &str) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [table],
            |row| row.get::<_, i64>(0),
        )
        .map(|exists| exists != 0)
        .with_context(|| format!("inspect OpenCode table `{table}` in {}", path.display()))
}

/// Message storage determines the generation independently of the session table's name.
fn has_v2_schema(connection: &Connection, path: &Path) -> Result<bool> {
    table_exists(connection, path, "session_message")
}

fn v2_session_table(connection: &Connection, path: &Path) -> Result<&'static str> {
    if table_exists(connection, path, "session_v2")? {
        Ok("session_v2")
    } else {
        Ok("session")
    }
}

/// Enumerate the modern session inventory from one OpenCode database.
///
/// Databases with a v2 projection use its metadata inventory exclusively. Without an explicit
/// unmigrated marker, a frozen legacy row cannot be distinguished from a deleted v2 session.
pub fn enumerate_sessions(path: &Path) -> Result<Vec<OpencodeSession>> {
    let connection = open_read_only_database(path)?;
    if has_v2_schema(&connection, path)? {
        Ok(v2_sessions_from_connection(&connection, path)?.0)
    } else {
        require_modern_schema(&connection, path)?;
        enumerate_sessions_from_connection(&connection, path)
    }
}

fn query_sessions(
    connection: &Connection,
    path: &Path,
    query: &str,
) -> Result<Vec<OpencodeSession>> {
    let mut statement = connection
        .prepare(query)
        .with_context(|| format!("prepare OpenCode session query for {}", path.display()))?;
    let rows = statement
        .query_map([], |row| {
            let id = row.get::<_, String>(0)?;
            let time_created = row.get::<_, i64>(3)?;
            let time_updated = row.get::<_, i64>(4)?;
            Ok((
                id,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                time_created,
                time_updated,
            ))
        })
        .with_context(|| format!("query OpenCode sessions in {}", path.display()))?;
    let mut sessions = Vec::new();
    for row in rows {
        let (id, parent_id, directory, time_created, time_updated) = row?;
        sessions.push(OpencodeSession {
            id: id.clone(),
            parent_id,
            directory,
            time_created: nonnegative_timestamp(time_created)
                .with_context(|| format!("session `{id}` has invalid time_created"))?,
            time_updated: nonnegative_timestamp(time_updated)
                .with_context(|| format!("session `{id}` has invalid time_updated"))?,
        });
    }
    Ok(sessions)
}

fn enumerate_sessions_from_connection(
    connection: &Connection,
    path: &Path,
) -> Result<Vec<OpencodeSession>> {
    query_sessions(
        connection,
        path,
        "SELECT id, parent_id, directory, time_created, time_updated
         FROM session ORDER BY id",
    )
}

/// Enumerate the v2 session inventory, which is authoritative wherever it overlaps the v1
/// `session` table.
fn enumerate_v2_sessions_from_connection(
    connection: &Connection,
    path: &Path,
) -> Result<Vec<OpencodeSession>> {
    let table = v2_session_table(connection, path)?;
    query_sessions(
        connection,
        path,
        &format!(
            "SELECT id, parent_id, directory, time_created, time_updated
         FROM {table} ORDER BY id"
        ),
    )
}

/// The projection is the database's ownership boundary. Do not infer incomplete migration
/// from missing rows: neither known layout provides durable per-session migration tombstones.
fn v2_sessions_from_connection(
    connection: &Connection,
    path: &Path,
) -> Result<(Vec<OpencodeSession>, HashSet<String>)> {
    let sessions = enumerate_v2_sessions_from_connection(connection, path)?;
    let v2_ids = sessions.iter().map(|session| session.id.clone()).collect();
    Ok((sessions, v2_ids))
}

fn nonnegative_timestamp(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow::anyhow!("negative timestamp"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseCursor {
    pub event_rowid: i64,
    pub event_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseScan {
    pub v2: bool,
    pub sessions: Vec<OpencodeSession>,
    pub dirty_session_ids: Vec<String>,
    pub removed_session_ids: Vec<String>,
    pub cursor: DatabaseCursor,
    pub session_cursors: HashMap<String, OpencodeSessionCursor>,
    pub v2_session_ids: HashSet<String>,
}

fn require_event_schema(connection: &Connection, path: &Path) -> Result<()> {
    let exists = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'event')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .with_context(|| format!("inspect OpenCode event schema in {}", path.display()))?;
    if exists == 0 {
        bail!(
            "unrecognized OpenCode SQLite schema in {}: missing table `event`",
            path.display()
        );
    }

    let mut statement = connection
        .prepare("PRAGMA table_info(event)")
        .with_context(|| format!("inspect OpenCode event table in {}", path.display()))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .with_context(|| format!("read OpenCode event columns in {}", path.display()))?
        .collect::<rusqlite::Result<HashSet<_>>>()
        .with_context(|| format!("read OpenCode event columns in {}", path.display()))?;
    for column in ["id", "aggregate_id"] {
        if !columns.contains(column) {
            bail!(
                "unrecognized OpenCode SQLite schema in {}: `event` lacks `{column}`",
                path.display()
            );
        }
    }
    connection
        .prepare("SELECT rowid, id, aggregate_id FROM event LIMIT 0")
        .with_context(|| {
            format!(
                "OpenCode `event` table in {} does not provide rowid/id/aggregate_id",
                path.display()
            )
        })?;
    Ok(())
}

fn current_event_cursor(connection: &Connection, path: &Path) -> Result<DatabaseCursor> {
    // v2-only databases have no `event` table and their planning ignores the event cursor.
    if !table_exists(connection, path, "event")? {
        return Ok(DatabaseCursor {
            event_rowid: 0,
            event_id: None,
        });
    }
    connection
        .query_row(
            "SELECT rowid, id FROM event ORDER BY rowid DESC LIMIT 1",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .with_context(|| format!("read OpenCode event high-water mark in {}", path.display()))
        .map(|row| match row {
            Some((event_rowid, event_id)) => DatabaseCursor {
                event_rowid,
                event_id: Some(event_id),
            },
            None => DatabaseCursor {
                event_rowid: 0,
                event_id: None,
            },
        })
}

/// Read the per-session high-water marks from the v2 `session_message` projection.
///
/// Prefer the durable event sequence where available. Projection maxima and row count
/// also detect changes in older schemas, but cannot detect every same-count replacement.
fn current_session_cursors(
    connection: &Connection,
    path: &Path,
) -> Result<HashMap<String, OpencodeSessionCursor>> {
    let mut statement = connection
        .prepare(
            "SELECT session_id, MAX(seq), MAX(time_updated), COUNT(*)
             FROM session_message GROUP BY session_id",
        )
        .with_context(|| {
            format!(
                "prepare OpenCode v2 session cursor query for {}",
                path.display()
            )
        })?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .with_context(|| format!("query OpenCode v2 session cursors in {}", path.display()))?;
    let mut cursors = HashMap::new();
    for row in rows {
        let (session_id, max_seq, max_time_updated, row_count) = row?;
        cursors.insert(
            session_id.clone(),
            OpencodeSessionCursor {
                max_seq: nonnegative_cursor(max_seq)
                    .with_context(|| format!("session `{session_id}` has invalid max seq"))?,
                max_time_updated: nonnegative_cursor(max_time_updated).with_context(|| {
                    format!("session `{session_id}` has invalid max time_updated")
                })?,
                row_count,
                event_sequence: None,
            },
        );
    }
    if table_exists(connection, path, "event_sequence")? {
        let mut statement = connection
            .prepare("SELECT aggregate_id, seq FROM event_sequence")
            .with_context(|| format!("prepare OpenCode event sequences in {}", path.display()))?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .with_context(|| format!("query OpenCode event sequences in {}", path.display()))?;
        for row in rows {
            let (session_id, sequence) = row?;
            // OpenCode uses -1 before a session has emitted its first event.
            // Keep the sentinel so the first event still changes this cursor.
            if sequence < -1 {
                bail!("session `{session_id}` has invalid event sequence: {sequence}");
            }
            cursors.entry(session_id).or_default().event_sequence = Some(sequence);
        }
    }
    Ok(cursors)
}

fn nonnegative_cursor(value: i64) -> Result<i64> {
    if value < 0 {
        bail!("negative cursor value");
    }
    Ok(value)
}

fn full_reconcile(
    sessions: &[OpencodeSession],
    previous: Option<&OpencodeDatabaseState>,
) -> (Vec<String>, Vec<String>) {
    let current_ids = sessions
        .iter()
        .map(|session| session.id.clone())
        .collect::<HashSet<_>>();
    let mut dirty = current_ids.iter().cloned().collect::<Vec<_>>();
    dirty.sort();
    let mut removed = previous
        .into_iter()
        .flat_map(|state| state.owned_session_ids.iter())
        .filter(|id| !current_ids.contains(*id))
        .cloned()
        .collect::<Vec<_>>();
    removed.sort();
    (dirty, removed)
}

/// Scan the short-lived inventory/event snapshot used to plan database hydration.
///
/// The connection is opened and all inventory/event reads are completed before this function
/// returns.  Hydration must happen afterwards using a separate connection: keeping this read
/// snapshot open while parsing messages would unnecessarily pin a WAL checkpoint and enlarge the
/// consistency window.
pub fn scan_database(
    path: &Path,
    previous: Option<&OpencodeDatabaseState>,
) -> Result<DatabaseScan> {
    crate::profiling::span!("opencode.plan");
    let connection = open_read_only_database(path)?;
    connection
        .execute_batch("BEGIN")
        .with_context(|| format!("begin OpenCode planning snapshot in {}", path.display()))?;
    let v2 = has_v2_schema(&connection, path)?;
    if !v2 {
        // Planning must reject databases that hydration cannot read, so ingest can apply its
        // per-database fallback consistently.  v2-only databases legitimately lack the v1 tables
        // and the event log, so these requirements are v1-only.
        require_modern_schema(&connection, path)?;
        require_event_schema(&connection, path)?;
    }
    let (sessions, session_cursors, v2_session_ids) = if v2 {
        let (sessions, v2_ids) = v2_sessions_from_connection(&connection, path)?;
        (
            sessions,
            current_session_cursors(&connection, path)?,
            v2_ids,
        )
    } else {
        (
            enumerate_sessions_from_connection(&connection, path)?,
            HashMap::new(),
            HashSet::new(),
        )
    };
    let cursor = if v2 {
        DatabaseCursor {
            event_rowid: 0,
            event_id: None,
        }
    } else {
        current_event_cursor(&connection, path)?
    };
    let (mut dirty, removed) = full_reconcile(&sessions, previous);

    if v2 {
        // v2 databases are event-sourced, so the `event` rowid cursor cannot detect in-place
        // `session_message` updates.  Per-session cursors drive change detection instead, and
        // the event delta path is bypassed entirely.
        if let Some(previous) =
            previous.filter(|state| state.parser_version == DATABASE_STATE_VERSION)
        {
            let mut dirty_ids = HashSet::new();
            for session in &sessions {
                let is_new = !previous.owned_session_ids.contains(&session.id);
                let is_changed =
                    previous.session_cursors.get(&session.id) != session_cursors.get(&session.id);
                if is_new || is_changed {
                    dirty_ids.insert(session.id.clone());
                }
            }
            dirty = dirty_ids.into_iter().collect();
            dirty.sort();
        }
    } else {
        let current_ids = sessions
            .iter()
            .map(|session| session.id.as_str())
            .collect::<HashSet<_>>();

        let valid_previous = match previous {
            Some(previous)
                if previous.parser_version == DATABASE_STATE_VERSION
                    && previous.event_rowid >= 0
                    && cursor.event_rowid >= previous.event_rowid =>
            {
                if previous.event_rowid == 0 && previous.event_id.is_none() {
                    true
                } else if previous.event_rowid > 0 {
                    connection
                        .query_row(
                            "SELECT id FROM event WHERE rowid = ?1",
                            [previous.event_rowid],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()
                        .with_context(|| {
                            format!("verify OpenCode event cursor in {}", path.display())
                        })?
                        .is_some_and(|event_id| Some(event_id) == previous.event_id)
                } else {
                    false
                }
            }
            _ => false,
        };

        if valid_previous {
            let mut statement = connection
                .prepare(
                    "SELECT aggregate_id FROM event
                     WHERE rowid > ?1 ORDER BY rowid",
                )
                .with_context(|| {
                    format!("prepare OpenCode event delta query for {}", path.display())
                })?;
            let rows = statement
                .query_map(
                    [previous.expect("valid previous exists").event_rowid],
                    |row| row.get::<_, Option<String>>(0),
                )
                .with_context(|| format!("query OpenCode event delta in {}", path.display()))?;
            let previous = previous.expect("valid previous exists");
            let mut dirty_ids = current_ids
                .iter()
                .filter(|id| !previous.owned_session_ids.contains(**id))
                .map(|id| (*id).to_string())
                .collect::<HashSet<_>>();
            for row in rows {
                if let Some(session_id) = row?.filter(|id| current_ids.contains(id.as_str())) {
                    dirty_ids.insert(session_id);
                }
            }
            dirty = dirty_ids.into_iter().collect();
            dirty.sort();
        }
    }

    let scan = DatabaseScan {
        v2,
        sessions,
        dirty_session_ids: dirty,
        removed_session_ids: removed,
        cursor,
        session_cursors,
        v2_session_ids,
    };
    connection
        .execute_batch("COMMIT")
        .with_context(|| format!("finish OpenCode planning snapshot in {}", path.display()))?;
    Ok(scan)
}

enum ModernJson<T> {
    Valid(T),
    Malformed,
}

fn parse_modern_value<T>(
    data: String,
    parse: impl FnOnce(&BorrowedValue<'_>) -> Result<T>,
) -> Result<ModernJson<T>> {
    let mut bytes = data.into_bytes();
    let value = match simd_json::to_borrowed_value(&mut bytes) {
        Ok(value) => value,
        Err(_) => return Ok(ModernJson::Malformed),
    };
    Ok(ModernJson::Valid(parse(&value)?))
}

#[derive(Clone, Default)]
pub(crate) struct SessionLinks {
    pub parent_session_id: Option<String>,
    pub thread_source: Option<String>,
    pub conversation_kind: Option<String>,
}

impl SessionLinks {
    fn record_links(&self) -> RecordLinks {
        RecordLinks {
            parent_session_id: self.parent_session_id.clone(),
            thread_source: self.thread_source.clone(),
            conversation_kind: self.conversation_kind.clone(),
            ..RecordLinks::default()
        }
    }
}

fn default_session_links() -> SessionLinks {
    SessionLinks {
        conversation_kind: Some("main".to_string()),
        ..SessionLinks::default()
    }
}

pub(crate) fn session_links_by_id() -> HashMap<String, SessionLinks> {
    session_links_by_id_from_roots(&data_roots())
}

#[allow(dead_code)]
pub(crate) fn session_links_by_id_from_root(root: &Path) -> HashMap<String, SessionLinks> {
    session_links_by_id_from_paths(&[root.to_path_buf()])
}

pub(crate) fn session_links_by_id_from_roots(roots: &[PathBuf]) -> HashMap<String, SessionLinks> {
    let storage_roots = roots
        .iter()
        .map(|root| root.join("storage/session"))
        .collect::<Vec<_>>();
    session_links_by_id_from_paths(&storage_roots)
}

fn session_links_by_id_from_paths(roots: &[PathBuf]) -> HashMap<String, SessionLinks> {
    let mut paths = Vec::new();
    for root in roots {
        paths.extend(
            WalkDir::new(root)
                .into_iter()
                .flatten()
                .filter(|entry| {
                    entry.file_type().is_file()
                        && entry.path().extension().and_then(|ext| ext.to_str()) == Some("json")
                })
                .map(|entry| entry.path().to_path_buf()),
        );
    }
    paths.sort();
    let mut links_by_id = HashMap::new();
    for path in paths {
        let Some(session_id) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .filter(|id| !id.is_empty())
            .map(str::to_string)
        else {
            continue;
        };
        let Ok(mut bytes) = std::fs::read(path) else {
            continue;
        };
        let Ok(value) = simd_json::to_borrowed_value(&mut bytes) else {
            continue;
        };
        links_by_id
            .entry(session_id)
            .or_insert_with(|| session_links_from_value(&value));
    }
    links_by_id
}

fn session_links_from_value(value: &BorrowedValue<'_>) -> SessionLinks {
    let parent_session_id = value
        .get("parentID")
        .and_then(|value| value.as_str())
        .filter(|id| !id.is_empty())
        .map(str::to_string);
    SessionLinks {
        conversation_kind: Some(if parent_session_id.is_some() {
            "fork".to_string()
        } else {
            "main".to_string()
        }),
        thread_source: parent_session_id.as_ref().map(|_| "fork".to_string()),
        parent_session_id,
    }
}

pub(crate) fn parse_index_records(
    session_dir: &Path,
    state: IndexParseState,
    session_links: &HashMap<String, SessionLinks>,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    let session_id = session_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown")
        .to_string();
    let links = session_links
        .get(&session_id)
        .cloned()
        .unwrap_or_else(default_session_links);
    let mut messages = Vec::new();
    for entry in std::fs::read_dir(session_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let Ok(mut bytes) = std::fs::read(path) else {
            continue;
        };
        let Ok(message) = simd_json::to_borrowed_value(&mut bytes) else {
            continue;
        };
        let Some(message_id) = message
            .get("id")
            .and_then(|value| value.as_str())
            .filter(|id| !id.is_empty())
        else {
            continue;
        };
        messages.push((
            message_id.to_string(),
            message
                .get("time")
                .and_then(|value| value.get("created"))
                .and_then(|value| value.as_u64())
                .unwrap_or(0),
            message
                .get("role")
                .and_then(|value| value.as_str())
                .unwrap_or("user")
                .to_string(),
        ));
    }
    messages.sort_by_key(|message| message.1);
    let source_path = session_dir.to_string_lossy().to_string();
    let project = SourceKind::Opencode.label().to_string();
    let mut turn_id = state.turn_id;
    for (message_id, timestamp, role) in messages {
        let part_dir = parts_root_for_session(session_dir).join(&message_id);
        if !part_dir.exists() {
            continue;
        }
        let Ok(part_entries) = std::fs::read_dir(part_dir) else {
            continue;
        };
        let mut part_files = part_entries
            .flatten()
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        part_files.sort();
        let mut text_parts = Vec::new();
        for path in part_files {
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let Ok(mut bytes) = std::fs::read(path) else {
                continue;
            };
            let Ok(part) = simd_json::to_borrowed_value(&mut bytes) else {
                continue;
            };
            if let Some(text) = part.get("text").and_then(|value| value.as_str()) {
                text_parts.push(text.to_string());
            }
        }
        if text_parts.is_empty() {
            continue;
        }
        let mut record_links = links.record_links();
        record_links.event_id = Some(message_id);
        emit(Record {
            source: SourceKind::Opencode,
            doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
            ts: timestamp,
            project: project.clone(),
            session_id: session_id.clone(),
            turn_id,
            role,
            text: text_parts.join("\n"),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: record_links,
            source_path: source_path.clone(),
        })?;
        turn_id += 1;
    }
    Ok(IndexParseOutput {
        legacy_turn_id: None,
        offset: 0,
        turn_id,
        pending_tool_calls: state.pending_tool_calls,
        session_id: Some(session_id),
        diagnostics: Default::default(),
        session_cwd: None,
    })
}

#[derive(Debug)]
struct ModernMessage {
    id: String,
    timestamp: u64,
    role: String,
    text_parts: Vec<String>,
}

/// Project one modern SQLite session into records.  This is kept separate from the legacy
/// directory parser so the latter's tolerant JSON behavior and discovery semantics remain intact.
pub(crate) fn parse_database_records(
    path: &Path,
    session_id: &str,
    state: IndexParseState,
    next_doc_id: &AtomicU64,
    emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    let connection = open_database_for_sessions(path)?;
    // Never fall back to frozen rows when a projected session has been deleted.
    if has_v2_schema(&connection, path)? {
        return parse_session_records_v2(&connection, path, session_id, state, next_doc_id, emit);
    }
    parse_session_records(&connection, path, session_id, state, next_doc_id, emit)
}

/// Open once for a batch of `parse_session_records` calls against the same database.
pub(crate) fn open_database_for_sessions(path: &Path) -> Result<Connection> {
    let connection = open_read_only_database(path)?;
    if !has_v2_schema(&connection, path)? {
        require_modern_schema(&connection, path)?;
    }
    Ok(connection)
}

pub(crate) fn parse_session_records(
    connection: &Connection,
    path: &Path,
    session_id: &str,
    state: IndexParseState,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    let Some(session) = enumerate_session_from_connection(connection, path, session_id)? else {
        return Ok(IndexParseOutput {
            legacy_turn_id: None,
            offset: 0,
            turn_id: state.turn_id,
            pending_tool_calls: state.pending_tool_calls,
            session_id: Some(session_id.to_string()),
            diagnostics: Default::default(),
            session_cwd: None,
        });
    };
    // Parent linkage is the only subagent signal: the `agent` column records
    // the selected agent (build/plan/custom), so a plan-mode session without
    // a parent is still interactive.
    let links = SessionLinks {
        parent_session_id: session.parent_id.clone(),
        thread_source: session.parent_id.as_ref().map(|_| "fork".to_string()),
        conversation_kind: Some(if session.parent_id.is_some() {
            "fork".to_string()
        } else {
            "main".to_string()
        }),
    };

    let mut diagnostics = ParseDiagnostics::default();
    let messages = collect_modern_messages(connection, path, session_id, &mut diagnostics)?;
    let mut turn_id = state.turn_id;
    for message in messages {
        emit_modern_message(
            message,
            session_id,
            path,
            &links,
            &mut turn_id,
            next_doc_id,
            &mut emit,
        )?;
    }
    Ok(IndexParseOutput {
        legacy_turn_id: None,
        offset: 0,
        turn_id,
        pending_tool_calls: state.pending_tool_calls,
        session_id: Some(session_id.to_string()),
        diagnostics,
        session_cwd: None,
    })
}

/// Read and group v1 `message`+`part` rows for one session into per-message projections in
/// `time_created, id, part id` order.
///
/// Malformed message/part JSON is counted in `diagnostics` and skipped, never an error.
fn collect_modern_messages(
    connection: &Connection,
    path: &Path,
    session_id: &str,
    diagnostics: &mut ParseDiagnostics,
) -> Result<Vec<ModernMessage>> {
    let mut statement = connection
        .prepare(
            "SELECT m.id, m.time_created, m.data, p.id, p.data
             FROM message AS m
             JOIN part AS p ON p.message_id = m.id
             WHERE m.session_id = ?1
             ORDER BY m.time_created, m.id, p.id",
        )
        .with_context(|| format!("prepare OpenCode message query for {}", path.display()))?;
    let rows = statement
        .query_map(params![session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .with_context(|| format!("query OpenCode messages in {}", path.display()))?;

    let mut messages = Vec::new();
    let mut current: Option<ModernMessage> = None;
    let mut malformed_message_ids = HashSet::new();
    for row in rows {
        let (message_id, timestamp, message_data, _part_id, part_data) = row?;
        if current
            .as_ref()
            .is_some_and(|message| message.id != message_id)
            && let Some(message) = current.take()
        {
            messages.push(message);
        }
        if current.is_none() {
            if malformed_message_ids.contains(&message_id) {
                continue;
            }
            let timestamp = nonnegative_timestamp(timestamp)
                .with_context(|| format!("message `{message_id}` has invalid time_created"))?;
            let role = match parse_modern_value(message_data, |value| {
                Ok(value
                    .get("role")
                    .and_then(|role| role.as_str())
                    .filter(|role| !role.is_empty())
                    .unwrap_or("user")
                    .to_string())
            })? {
                ModernJson::Valid(role) => role,
                ModernJson::Malformed => {
                    diagnostics.malformed_json_lines += 1;
                    malformed_message_ids.insert(message_id);
                    continue;
                }
            };
            current = Some(ModernMessage {
                id: message_id.clone(),
                timestamp,
                role,
                text_parts: Vec::new(),
            });
        }
        let text = match parse_modern_value(part_data, |value| {
            if value.get("type").and_then(|kind| kind.as_str()) != Some("text") {
                return Ok(None);
            }
            Ok(value
                .get("text")
                .and_then(|text| text.as_str())
                .filter(|text| !text.is_empty())
                .map(str::to_string))
        })? {
            ModernJson::Valid(text) => text,
            ModernJson::Malformed => {
                diagnostics.malformed_json_lines += 1;
                continue;
            }
        };
        let Some(text) = text else { continue };
        current
            .as_mut()
            .expect("current message was initialized")
            .text_parts
            .push(text);
    }
    if let Some(message) = current {
        messages.push(message);
    }
    Ok(messages)
}

fn enumerate_session_from_connection(
    connection: &Connection,
    path: &Path,
    session_id: &str,
) -> Result<Option<OpencodeSession>> {
    let row = connection
        .query_row(
            "SELECT id, parent_id, directory, time_created, time_updated
             FROM session WHERE id = ?1",
            [session_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()
        .with_context(|| {
            format!(
                "look up OpenCode session `{session_id}` in {}",
                path.display()
            )
        })?;
    let Some(row) = row else {
        return Ok(None);
    };
    let (id, parent_id, directory, time_created, time_updated) = row;
    Ok(Some(OpencodeSession {
        id,
        parent_id,
        directory,
        time_created: nonnegative_timestamp(time_created)
            .with_context(|| format!("session `{session_id}` has invalid time_created"))?,
        time_updated: nonnegative_timestamp(time_updated)
            .with_context(|| format!("session `{session_id}` has invalid time_updated"))?,
    }))
}

fn emit_modern_message(
    message: ModernMessage,
    session_id: &str,
    path: &Path,
    links: &SessionLinks,
    turn_id: &mut u32,
    next_doc_id: &AtomicU64,
    emit: &mut impl FnMut(Record) -> Result<()>,
) -> Result<()> {
    if message.text_parts.is_empty() {
        return Ok(());
    }
    let mut record_links = links.record_links();
    record_links.event_id = Some(message.id);
    emit(Record {
        source: SourceKind::Opencode,
        doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
        ts: message.timestamp,
        project: "opencode".to_string(),
        session_id: session_id.to_string(),
        turn_id: *turn_id,
        role: message.role,
        text: message.text_parts.join("\n"),
        tool_name: None,
        tool_input: None,
        tool_output: None,
        links: record_links,
        source_path: path.to_string_lossy().to_string(),
    })?;
    *turn_id = turn_id.saturating_add(1);
    Ok(())
}

/// One assistant message projected from the v2 `session_message` projection.
#[derive(Default)]
struct V2AssistantContent {
    text_parts: Vec<String>,
    tools: Vec<V2ToolContent>,
}

struct V2ToolContent {
    identity: String,
    tool_name: Option<String>,
    tool_input: Option<String>,
    tool_output: Option<String>,
}

fn v2_session_exists(connection: &Connection, path: &Path, session_id: &str) -> Result<bool> {
    let table = v2_session_table(connection, path)?;
    connection
        .query_row(
            &format!("SELECT 1 FROM {table} WHERE id = ?1"),
            [session_id],
            |_| Ok(()),
        )
        .optional()
        .map(|row| row.is_some())
        .with_context(|| {
            format!(
                "look up OpenCode v2 session `{session_id}` in {}",
                path.display()
            )
        })
}

fn v2_session_parent_id(
    connection: &Connection,
    path: &Path,
    session_id: &str,
) -> Result<Option<String>> {
    let table = v2_session_table(connection, path)?;
    let v2_parent = connection
        .query_row(
            &format!("SELECT parent_id FROM {table} WHERE id = ?1"),
            [session_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .with_context(|| {
            format!(
                "look up OpenCode v2 session parent for `{session_id}` in {}",
                path.display()
            )
        })?;
    Ok(v2_parent.flatten())
}

/// Parent-linkage links mirroring the v1 `parse_session_records` rule.
fn v2_session_links(parent_id: Option<String>) -> SessionLinks {
    SessionLinks {
        parent_session_id: parent_id.clone(),
        thread_source: parent_id.as_ref().map(|_| "fork".to_string()),
        conversation_kind: Some(if parent_id.is_some() {
            "fork".to_string()
        } else {
            "main".to_string()
        }),
    }
}

fn v2_string_field(value: &BorrowedValue<'_>, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(|value| value.as_str())
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

/// Join all non-empty `state.content[].text` items for a tool call (text items only); this is
/// the first level of the output fallback chain and mirrors the v1 `text_parts` join semantics.
fn v2_tool_content_text(state: &BorrowedValue<'_>) -> Option<String> {
    let items = state.get("content")?.as_array()?;
    let mut parts = Vec::new();
    for item in items {
        if item.get("type").and_then(|kind| kind.as_str()) != Some("text") {
            continue;
        }
        if let Some(text) = item
            .get("text")
            .and_then(|text| text.as_str())
            .filter(|text| !text.is_empty())
        {
            parts.push(text.to_string());
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

/// Tool fields for one assistant content item, per the documented fallback chain.
fn v2_tool_fields(item: &BorrowedValue<'_>) -> (Option<String>, Option<String>, Option<String>) {
    let tool_name = item
        .get("name")
        .and_then(|name| name.as_str())
        .filter(|name| !name.is_empty())
        .map(str::to_string);
    let Some(state) = item.get("state") else {
        return (tool_name, None, None);
    };
    let tool_input = state
        .get("input")
        .and_then(|input| serde_json::to_string(input).ok());
    let tool_output = v2_tool_content_text(state)
        .or_else(|| {
            state
                .get("metadata")
                .and_then(|metadata| v2_string_field(metadata, "output"))
        })
        .or_else(|| {
            state
                .get("metadata")
                .and_then(|metadata| v2_string_field(metadata, "outputPath"))
        })
        .or_else(|| {
            state
                .get("metadata")
                .and_then(|metadata| v2_string_field(metadata, "filepath"))
        });
    (tool_name, tool_input, tool_output)
}

fn v2_assistant_content(
    value: &BorrowedValue<'_>,
    diagnostics: &mut ParseDiagnostics,
) -> V2AssistantContent {
    let mut content = V2AssistantContent::default();
    let Some(items) = value.get("content").and_then(|content| content.as_array()) else {
        return content;
    };
    for (index, item) in items.iter().enumerate() {
        if item.as_object().is_none() {
            diagnostics.malformed_json_lines += 1;
            continue;
        }
        match item.get("type").and_then(|kind| kind.as_str()) {
            Some("text") => {
                if let Some(text) = item
                    .get("text")
                    .and_then(|text| text.as_str())
                    .filter(|text| !text.is_empty())
                {
                    content.text_parts.push(text.to_string());
                }
            }
            Some("tool") => {
                let (name, input, output) = v2_tool_fields(item);
                if name.is_some() || input.is_some() || output.is_some() {
                    // Upstream tool IDs survive content updates and reordering. Older or
                    // malformed data without an ID gets a deterministic positional fallback.
                    let identity = v2_string_field(item, "id")
                        .map(|id| format!("id:{id}"))
                        .unwrap_or_else(|| format!("index:{index}"));
                    content.tools.push(V2ToolContent {
                        identity,
                        tool_name: name,
                        tool_input: input,
                        tool_output: output,
                    });
                }
            }
            Some("reasoning") | Some("file") => {}
            Some(_) | None => {}
        }
    }
    content
}

#[allow(clippy::too_many_arguments)]
fn emit_v2_record(
    path: &Path,
    session_id: &str,
    links: &SessionLinks,
    turn_id: &mut u32,
    next_doc_id: &AtomicU64,
    emit: &mut impl FnMut(Record) -> Result<()>,
    event_id: String,
    ts: u64,
    role: String,
    text: String,
    tool_name: Option<String>,
    tool_input: Option<String>,
    tool_output: Option<String>,
) -> Result<()> {
    let mut record_links = links.record_links();
    record_links.event_id = Some(event_id);
    emit(Record {
        source: SourceKind::Opencode,
        doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
        ts,
        project: SourceKind::Opencode.label().to_string(),
        session_id: session_id.to_string(),
        turn_id: *turn_id,
        role,
        text,
        tool_name,
        tool_input,
        tool_output,
        links: record_links,
        source_path: path.to_string_lossy().to_string(),
    })?;
    *turn_id = turn_id.saturating_add(1);
    Ok(())
}

/// One projected message awaiting the merged, timestamp-ordered emission pass.
struct V2PendingRecord {
    event_id: String,
    ts: u64,
    role: String,
    text: String,
    tool_name: Option<String>,
    tool_input: Option<String>,
    tool_output: Option<String>,
}

/// Validate an emitted row's timestamp.  Invalid values are recorded as a diagnostic and the row
/// is skipped, never aborting the session (unlike the v1 path, which errors).
fn v2_emitted_timestamp(timestamp: i64, diagnostics: &mut ParseDiagnostics) -> Option<u64> {
    match u64::try_from(timestamp) {
        Ok(timestamp) => Some(timestamp),
        Err(_) => {
            // No dedicated bad-timestamp counter exists; reuse the malformed counter.
            diagnostics.malformed_json_lines += 1;
            None
        }
    }
}

/// Project one OpenCode v2 `session_message` session into records.
///
/// v2 is event-sourced: every message kind shares one table, assistant content lives in a
/// `content[]` array, and tool-only turns carry no text.  This deliberately does not reuse
/// `emit_modern_message`, whose empty-text guard would drop those turns. Legacy rows are not
/// merged into an authoritative v2 projection, including after a revert.
pub(crate) fn parse_session_records_v2(
    connection: &Connection,
    path: &Path,
    session_id: &str,
    state: IndexParseState,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    if !v2_session_exists(connection, path, session_id)? {
        return Ok(IndexParseOutput {
            legacy_turn_id: None,
            offset: 0,
            turn_id: state.turn_id,
            pending_tool_calls: state.pending_tool_calls,
            session_id: Some(session_id.to_string()),
            diagnostics: Default::default(),
            session_cwd: None,
        });
    }
    let parent_id = v2_session_parent_id(connection, path, session_id)?;
    let links = v2_session_links(parent_id);

    let mut statement = connection
        .prepare(
            "SELECT id, type, seq, time_created, data
             FROM session_message WHERE session_id = ?1 ORDER BY seq",
        )
        .with_context(|| format!("prepare OpenCode v2 message query for {}", path.display()))?;
    let rows = statement
        .query_map(params![session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .with_context(|| format!("query OpenCode v2 messages in {}", path.display()))?;

    let mut diagnostics = ParseDiagnostics::default();
    let mut pending: Vec<V2PendingRecord> = Vec::new();
    for row in rows {
        let (message_id, kind, _seq, timestamp, data) = row?;
        match kind.as_str() {
            "user" | "system" => {
                // Timestamps are validated only for emitted kinds, so a bad value on a skippable
                // row cannot abort the session.
                let Some(timestamp) = v2_emitted_timestamp(timestamp, &mut diagnostics) else {
                    continue;
                };
                let text = match parse_modern_value(data, |value| {
                    Ok(value
                        .get("text")
                        .and_then(|text| text.as_str())
                        .filter(|text| !text.is_empty())
                        .map(str::to_string))
                })? {
                    ModernJson::Valid(text) => text.unwrap_or_default(),
                    ModernJson::Malformed => {
                        diagnostics.malformed_json_lines += 1;
                        continue;
                    }
                };
                if text.is_empty() {
                    continue;
                }
                pending.push(V2PendingRecord {
                    event_id: message_id,
                    ts: timestamp,
                    role: kind,
                    text,
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                });
            }
            "assistant" => {
                let Some(timestamp) = v2_emitted_timestamp(timestamp, &mut diagnostics) else {
                    continue;
                };
                let content = match parse_modern_value(data, |value| {
                    Ok(v2_assistant_content(value, &mut diagnostics))
                })? {
                    ModernJson::Valid(content) => content,
                    ModernJson::Malformed => {
                        diagnostics.malformed_json_lines += 1;
                        continue;
                    }
                };
                let text = content.text_parts.join("\n");
                if !text.is_empty() {
                    pending.push(V2PendingRecord {
                        event_id: message_id.clone(),
                        ts: timestamp,
                        role: "assistant".to_string(),
                        text,
                        tool_name: None,
                        tool_input: None,
                        tool_output: None,
                    });
                }
                for tool in content.tools {
                    pending.push(V2PendingRecord {
                        event_id: format!("{message_id}:tool:{}", tool.identity),
                        ts: timestamp,
                        role: "assistant".to_string(),
                        text: String::new(),
                        tool_name: tool.tool_name,
                        tool_input: tool.tool_input,
                        tool_output: tool.tool_output,
                    });
                }
            }
            // Known non-content projections and any future type are skipped for forward
            // compatibility; `synthetic` is `{"text":…}` and must not reach the assistant path.
            "synthetic" | "idle" | "agent-switched" | "model-switched" | "compaction" | "shell" => {
            }
            other => diagnostics.increment_unknown_semantic(other),
        }
    }

    // A v2 projection owns the entire session. Missing rows may have been reverted or
    // deleted; never resurrect them from the frozen legacy message/part tables.
    // Stable sorting preserves projection order for equal timestamps.
    pending.sort_by_key(|record| record.ts);
    let mut turn_id = state.turn_id;
    for record in pending {
        emit_v2_record(
            path,
            session_id,
            &links,
            &mut turn_id,
            next_doc_id,
            &mut emit,
            record.event_id,
            record.ts,
            record.role,
            record.text,
            record.tool_name,
            record.tool_input,
            record.tool_output,
        )?;
    }
    Ok(IndexParseOutput {
        legacy_turn_id: None,
        offset: 0,
        turn_id,
        pending_tool_calls: state.pending_tool_calls,
        session_id: Some(session_id.to_string()),
        diagnostics,
        session_cwd: None,
    })
}

/// Convenience wrapper for callers that want owned records rather than a streaming callback.
pub fn parse_database_session(
    path: &Path,
    session_id: &str,
    starting_turn_id: u32,
    next_doc_id: &AtomicU64,
) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    parse_database_records(
        path,
        session_id,
        IndexParseState {
            turn_id: starting_turn_id,
            ..IndexParseState::default()
        },
        next_doc_id,
        |record| {
            records.push(record);
            Ok(())
        },
    )?;
    Ok(records)
}

/// Databases precede message files so duplicate reconciliation retains the database copy,
/// matching OpenCode's pre-cache scan order.
pub fn usage_files() -> Vec<PathBuf> {
    let roots = data_roots();
    let mut files = Vec::new();
    let mut projected_roots = HashSet::new();
    for root in &roots {
        let mut databases = std::fs::read_dir(root)
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.extension().and_then(|value| value.to_str()) == Some("db")
                    && path
                        .file_name()
                        .and_then(|value| value.to_str())
                        .is_some_and(|name| name.starts_with("opencode"))
            })
            .collect::<Vec<_>>();
        databases.sort();
        if databases.iter().any(|path| {
            // A readable projection owns this data root, including IDs deleted before this
            // scan. Unreadable databases retain the existing legacy-file fallback behavior.
            (|| -> Result<bool> {
                let connection = open_read_only_database(path)?;
                if !has_v2_schema(&connection, path)? {
                    return Ok(false);
                }
                let table = v2_session_table(&connection, path)?;
                connection.prepare(&format!(
                    "SELECT m.id, m.session_id, m.type, m.data FROM session_message AS m
                     JOIN {table} AS s ON s.id = m.session_id LIMIT 0"
                ))?;
                Ok(true)
            })()
            .unwrap_or(false)
        }) {
            projected_roots.insert(root);
        }
        files.extend(databases);
    }
    for root in &roots {
        if projected_roots.contains(root) {
            continue;
        }
        let message_root = root.join("storage/message");
        if message_root.exists() {
            files.extend(
                WalkDir::new(message_root)
                    .into_iter()
                    .flatten()
                    .filter(|entry| {
                        entry.file_type().is_file()
                            && entry.path().extension().and_then(|value| value.to_str())
                                == Some("json")
                    })
                    .map(|entry| entry.path().to_path_buf()),
            );
        }
    }
    files
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<Vec<UsageEvent>> {
    if path.extension().and_then(|value| value.to_str()) == Some("db") {
        parse_usage_database(path)
    } else {
        parse_usage_message(path)
    }
}

fn parse_usage_message(path: &Path) -> Result<Vec<UsageEvent>> {
    let mut bytes = std::fs::read(path)?;
    let Ok(value) = simd_json::to_borrowed_value(&mut bytes) else {
        return Ok(Vec::new());
    };
    let id = borrowed_string(&value, &["id"]).or_else(|| {
        path.file_stem()
            .and_then(|name| name.to_str())
            .map(str::to_string)
    });
    Ok(usage_event(&value, path, id, None).into_iter().collect())
}

fn parse_usage_database(path: &Path) -> Result<Vec<UsageEvent>> {
    let connection = open_read_only_database(path)?;
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let mut ids = HashSet::new();
    let mut events = Vec::new();
    if has_v2_schema(&connection, path)? {
        // v2 assistant rows carry the usage projection.  v2 `data` has no top-level
        // `sessionID`, so the selected `session_id` column is passed as the fallback.
        let table = v2_session_table(&connection, path)?;
        let mut statement = connection.prepare(&format!(
            "SELECT m.id, m.session_id, m.data FROM session_message AS m
             JOIN {table} AS s ON s.id = m.session_id WHERE m.type = 'assistant'"
        ))?;
        let rows = statement.query_map([], usage_row)?;
        collect_usage_events(path, &source_path, &mut ids, &mut events, rows)?;
    } else {
        let mut statement = connection.prepare("SELECT id, session_id, data FROM message")?;
        let rows = statement.query_map([], usage_row)?;
        collect_usage_events(path, &source_path, &mut ids, &mut events, rows)?;
    }
    Ok(events)
}

fn usage_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(String, Option<String>, String)> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
}

/// Project usage rows into events, deduping by id (v2 rows are read first and win).
fn collect_usage_events(
    path: &Path,
    source_path: &Arc<str>,
    ids: &mut HashSet<String>,
    events: &mut Vec<UsageEvent>,
    rows: impl Iterator<Item = rusqlite::Result<(String, Option<String>, String)>>,
) -> Result<()> {
    for row in rows {
        let (id, session, data) = row?;
        if ids.contains(&id) {
            continue;
        }
        let mut bytes = data.into_bytes();
        let Ok(value) = simd_json::to_borrowed_value(&mut bytes) else {
            continue;
        };
        if let Some(mut event) = usage_event(&value, path, Some(id.clone()), session.as_deref()) {
            event.source_path = source_path.clone();
            ids.insert(id);
            events.push(event);
        }
    }
    Ok(())
}

fn usage_event(
    value: &BorrowedValue<'_>,
    path: &Path,
    id: Option<String>,
    fallback_session: Option<&str>,
) -> Option<UsageEvent> {
    let usage = value.get("tokens")?;
    let number = |key: &str| usage.get(key).and_then(|value| value.as_u64()).unwrap_or(0);
    let reasoning = number("reasoning");
    let cache = usage.get("cache");
    let mut tokens = TokenBuckets::disjoint(
        number("input"),
        cache
            .and_then(|value| value.get("read"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0),
        cache
            .and_then(|value| value.get("write"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0),
        number("output").saturating_add(reasoning),
    );
    tokens.reasoning = reasoning;
    if tokens.additive_total() == 0 {
        return None;
    }
    let (provider, model) = usage_provider_model(value);
    Some(UsageEvent {
        source: "opencode",
        source_path: Arc::from(path.to_string_lossy()),
        source_record_id: id.clone(),
        session_id: borrowed_string(value, &["sessionID", "session_id"])
            .or_else(|| fallback_session.map(str::to_string)),
        request_id: None,
        message_id: id,
        timestamp_ms: value
            .get("time")
            .and_then(|value| value.get("created"))
            .map(timestamp_millis)
            .unwrap_or(0),
        project: Some(SourceKind::Opencode.label().to_string()),
        provider,
        model,
        tokens,
        credits: None,
        token_usage_available: true,
        source_cost_usd: value.get("cost").and_then(|value| value.as_f64()),
        cost_authoritative: false,
        dedupe_confidence: "exact",
        conservative_undercount: false,
        cache_chain_excluded: false,
        sidechain: false,
        permission_review: false,
        source_order: 0,
    })
}

/// Provider/model for an OpenCode usage row.
///
/// v1 stores top-level `providerID`/`modelID` strings; v2 stores `model` as an object
/// `{providerID, id, variant}` and has no top-level provider/model keys.  The object form is
/// checked first so v2 rows never silently yield `None`.
fn usage_provider_model(value: &BorrowedValue<'_>) -> (Option<String>, Option<String>) {
    if let Some(model_object) = value.get("model").and_then(|model| model.as_object()) {
        let provider = model_object
            .get("providerID")
            .and_then(|provider| provider.as_str())
            .filter(|provider| !provider.is_empty())
            .map(str::to_string)
            .or_else(|| borrowed_string(value, &["providerID", "provider"]));
        // Legacy object-form rows key the model as `modelID` rather than `id`.
        let model = model_object
            .get("id")
            .or_else(|| model_object.get("modelID"))
            .and_then(|model| model.as_str())
            .filter(|model| !model.is_empty())
            .map(str::to_string)
            .or_else(|| borrowed_string(value, &["modelID"]));
        return (provider, model);
    }
    (
        borrowed_string(value, &["providerID", "provider"]),
        borrowed_string(value, &["modelID", "model"]),
    )
}

fn borrowed_string(value: &BorrowedValue<'_>, aliases: &[&str]) -> Option<String> {
    aliases
        .iter()
        .find_map(|key| value.get(*key).and_then(|value| value.as_str()))
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn timestamp_millis(value: &BorrowedValue<'_>) -> u64 {
    value
        .as_u64()
        .map(|number| {
            if number < 10_000_000_000 {
                number.saturating_mul(1000)
            } else {
                number
            }
        })
        .or_else(|| {
            value
                .as_i64()
                .filter(|value| *value >= 0)
                .map(|value| value as u64)
        })
        .or_else(|| value.as_str().and_then(super::common::parse_iso_millis))
        .unwrap_or(0)
}

pub(crate) fn reconcile_usage(events: &mut Vec<UsageEvent>) {
    let mut seen = HashSet::new();
    events.retain(|event| {
        event.source != "opencode"
            || event
                .source_record_id
                .as_ref()
                .is_none_or(|record| seen.insert(record.clone()))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::fs;
    use std::sync::atomic::AtomicU64;

    fn modern_fixture(path: &Path) -> Connection {
        let connection = Connection::open(path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE session (
                    id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT NOT NULL,
                    time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL
                );
                 CREATE TABLE message (
                    id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                    time_created INTEGER NOT NULL, data TEXT NOT NULL
                 );
                 CREATE TABLE part (
                    id TEXT PRIMARY KEY, message_id TEXT NOT NULL, data TEXT NOT NULL
                 );
                 CREATE TABLE event (
                    id TEXT NOT NULL, aggregate_id TEXT NOT NULL
                 );
                 CREATE INDEX message_session_time ON message(session_id, time_created, id);
                 CREATE INDEX part_message_id ON part(message_id, id);",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO session VALUES (?1, NULL, ?2, ?3, ?4)",
                params!["s_root", "/repo", 10_i64, 20_i64],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO session VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["s_child", "s_root", "/repo/child", 30_i64, 40_i64],
            )
            .unwrap();
        connection
    }

    /// `modern_fixture` plus the OpenCode v2 `session_v2`/`session_message` projection.
    fn v2_fixture(path: &Path) -> Connection {
        let connection = modern_fixture(path);
        connection
            .execute_batch(
                "CREATE TABLE session_v2 (
                    id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT NOT NULL,
                    time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL
                 );
                 CREATE TABLE session_message (
                    id TEXT PRIMARY KEY, session_id TEXT NOT NULL, type TEXT NOT NULL,
                    seq INTEGER NOT NULL, time_created INTEGER NOT NULL,
                    time_updated INTEGER NOT NULL, data TEXT NOT NULL
                 );
                 CREATE UNIQUE INDEX session_message_session_seq_idx
                    ON session_message(session_id, seq);",
            )
            .unwrap();
        connection
    }

    /// A v2-only database: `session_v2`/`session_message` only, with no v1
    /// `session`/`message`/`part`/`event` tables.
    fn v2_only_fixture(path: &Path) -> Connection {
        let connection = Connection::open(path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE session_v2 (
                    id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT NOT NULL,
                    time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL
                 );
                 CREATE TABLE session_message (
                    id TEXT PRIMARY KEY, session_id TEXT NOT NULL, type TEXT NOT NULL,
                    seq INTEGER NOT NULL, time_created INTEGER NOT NULL,
                    time_updated INTEGER NOT NULL, data TEXT NOT NULL
                 );
                 CREATE UNIQUE INDEX session_message_session_seq_idx
                    ON session_message(session_id, seq);
                 INSERT INTO session_v2 VALUES ('s_v2only', NULL, '/repo/v2-only', 10, 200);
                 INSERT INTO session_message VALUES ('sm_user', 's_v2only', 'user', 1, 100, 100, '{\"text\":\"hello v2\"}');
                 INSERT INTO session_message VALUES ('sm_assistant', 's_v2only', 'assistant', 2, 200, 200, '{\"model\":{\"providerID\":\"anthropic\",\"id\":\"claude\"},\"content\":[{\"type\":\"text\",\"text\":\"assistant v2\"}],\"tokens\":{\"input\":1,\"output\":2},\"cost\":0.0,\"time\":{\"created\":200}}');",
            )
            .unwrap();
        connection
    }

    fn insert_v2_session(
        connection: &Connection,
        id: &str,
        parent_id: Option<&str>,
        directory: &str,
        time_created: i64,
        time_updated: i64,
    ) {
        connection
            .execute(
                "INSERT INTO session_v2 (id, parent_id, directory, time_created, time_updated)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![id, parent_id, directory, time_created, time_updated],
            )
            .unwrap();
    }

    fn insert_v2_message(
        connection: &Connection,
        id: &str,
        session_id: &str,
        kind: &str,
        seq: i64,
        time_created: i64,
        time_updated: i64,
        data: &str,
    ) {
        // Projection rows belong to an existing v2 session, just as upstream's FK requires.
        connection.execute(
            "INSERT OR IGNORE INTO session_v2
             SELECT id, parent_id, directory, time_created, time_updated FROM session WHERE id = ?1",
            [session_id],
        ).unwrap();
        connection
            .execute(
                "INSERT INTO session_message
                    (id, session_id, type, seq, time_created, time_updated, data)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![id, session_id, kind, seq, time_created, time_updated, data],
            )
            .unwrap();
    }

    fn state_from_scan(scan: &DatabaseScan) -> OpencodeDatabaseState {
        OpencodeDatabaseState {
            parser_version: DATABASE_STATE_VERSION,
            event_rowid: scan.cursor.event_rowid,
            event_id: scan.cursor.event_id.clone(),
            owned_session_ids: scan.sessions.iter().map(|s| s.id.clone()).collect(),
            session_cursors: scan.session_cursors.clone(),
        }
    }

    const V2_USER_DATA: &str = r#"{"text":"hello","time":{"created":100}}"#;
    const V2_ASSISTANT_DATA: &str = r#"{"agent":"build","model":{"providerID":"anthropic","id":"claude"},"content":[{"type":"text","text":"hi"}],"tokens":{"input":1,"output":2},"cost":0.0,"time":{"created":200}}"#;

    #[test]
    fn pinned_upstream_schema_plans_hydrates_and_counts_usage() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(include_str!(
                "../../tests/fixtures/opencode/upstream-5a833585.sql"
            ))
            .unwrap();
        connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             INSERT INTO project (id, worktree, time_created, time_updated, sandboxes)
                 VALUES ('project', '/repo', 1, 1, '[]');
             INSERT INTO session (id, project_id, parent_id, slug, directory, title, version, time_created, time_updated)
                 VALUES ('current', 'project', 'parent', 'slug', '/repo', 'test', '2', 1, 200);
             INSERT INTO event_sequence (aggregate_id, seq) VALUES ('current', 5);"
        ).unwrap();
        connection.execute(
            "INSERT INTO session_message VALUES ('answer', 'current', 'assistant', 2, 200, 200, ?1)",
            [V2_ASSISTANT_DATA],
        ).unwrap();
        // The pinned schema still has legacy tables. Their rows must not override v2 ownership.
        connection
            .execute(
                "INSERT INTO message VALUES ('answer', 'current', 200, 200, ?1)",
                [V2_ASSISTANT_DATA],
            )
            .unwrap();
        let scan = scan_database(&path, None).unwrap();
        assert_eq!(scan.dirty_session_ids, vec!["current"]);
        assert!(scan.v2_session_ids.contains("current"));
        assert_eq!(scan.session_cursors["current"].event_sequence, Some(5));
        assert_eq!(enumerate_sessions(&path).unwrap()[0].directory, "/repo");
        let records = parse_database_session(&path, "current", 0, &AtomicU64::new(1)).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].text, "hi");
        assert_eq!(
            records[0].links.parent_session_id.as_deref(),
            Some("parent")
        );
        let usage = parse_usage_database(&path).unwrap();
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].tokens.uncached_input, 1);
        assert_eq!(usage[0].model.as_deref(), Some("claude"));

        connection
            .execute("DELETE FROM session_message", [])
            .unwrap();
        let after = scan_database(&path, Some(&state_from_scan(&scan))).unwrap();
        assert_eq!(after.dirty_session_ids, vec!["current"]);
        assert!(
            parse_database_session(&path, "current", 0, &AtomicU64::new(1))
                .unwrap()
                .is_empty()
        );
        assert!(parse_usage_database(&path).unwrap().is_empty());
        connection.execute("DELETE FROM session", []).unwrap();
        assert_eq!(
            scan_database(&path, Some(&state_from_scan(&scan)))
                .unwrap()
                .removed_session_ids,
            vec!["current"]
        );
    }

    #[test]
    fn reasoning_is_included_in_output_and_total() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("message.json");
        std::fs::write(
            &path,
            r#"{
                "id": "message",
                "tokens": {
                    "input": 100,
                    "output": 20,
                    "reasoning": 30,
                    "cache": { "read": 40, "write": 10 }
                }
            }"#,
        )
        .unwrap();

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tokens.reasoning, 30);
        assert_eq!(events[0].tokens.output, 50);
        assert_eq!(events[0].tokens.total(), 200);
        assert_eq!(events[0].project.as_deref(), Some("opencode"));
    }

    #[test]
    fn plan_agent_without_parent_stays_main() {
        // The `agent` column records the selected agent, not subagent-ness:
        // only parent linkage classifies.
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT NOT NULL, time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL, agent TEXT);
                 INSERT INTO session VALUES ('ses_plan', NULL, '/repo/example', 1000, 1001, 'plan');
                 INSERT INTO session VALUES ('ses_child', 'ses_plan', '/repo/example', 1002, 1003, 'general');
                 CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, time_created INTEGER NOT NULL, data TEXT NOT NULL);
                 INSERT INTO message VALUES ('msg_1', 'ses_plan', 1001, '{\"role\":\"user\"}');
                 INSERT INTO message VALUES ('msg_2', 'ses_child', 1002, '{\"role\":\"user\"}');
                 CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT NOT NULL, data TEXT NOT NULL);
                 INSERT INTO part VALUES ('part_1', 'msg_1', '{\"type\":\"text\",\"text\":\"Plan the work\"}');
                 INSERT INTO part VALUES ('part_2', 'msg_2', '{\"type\":\"text\",\"text\":\"Do the work\"}');",
            )
            .unwrap();
        drop(connection);

        let plan = parse_database_session(&path, "ses_plan", 0, &AtomicU64::new(1)).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].links.conversation_kind.as_deref(), Some("main"));
        let child = parse_database_session(&path, "ses_child", 0, &AtomicU64::new(10)).unwrap();
        assert_eq!(child.len(), 1);
        assert_eq!(child[0].links.conversation_kind.as_deref(), Some("fork"));
    }

    #[test]
    fn modern_inventory_and_projection_are_deterministic() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = modern_fixture(&path);
        connection
            .execute(
                "INSERT INTO message VALUES (?1, ?2, ?3, ?4)",
                params!["m_b", "s_child", 100_i64, r#"{}"#],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO message VALUES (?1, ?2, ?3, ?4)",
                params!["m_a", "s_child", 100_i64, r#"{"role":"assistant"}"#],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO message VALUES (?1, ?2, ?3, ?4)",
                params!["m_empty", "s_child", 101_i64, r#"{"role":"assistant"}"#],
            )
            .unwrap();
        // Part ordering is by id, not insertion order. Non-text parts are ignored.
        for (id, message, data) in [
            ("p_z", "m_a", r#"{"type":"text","text":"second"}"#),
            ("p_a", "m_a", r#"{"type":"image","text":"not emitted"}"#),
            ("p_b", "m_a", r#"{"type":"text","text":"first"}"#),
            ("p_c", "m_b", r#"{"type":"text","text":"user fallback"}"#),
            ("p_d", "m_empty", r#"{"type":"tool","text":"ignored"}"#),
        ] {
            connection
                .execute(
                    "INSERT INTO part VALUES (?1, ?2, ?3)",
                    params![id, message, data],
                )
                .unwrap();
        }
        drop(connection);

        let sessions = enumerate_sessions(&path).unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].id, "s_child");
        assert_eq!(sessions[0].parent_id.as_deref(), Some("s_root"));
        assert_eq!(sessions[0].directory, "/repo/child");
        assert_eq!(
            (sessions[0].time_created, sessions[0].time_updated),
            (30, 40)
        );

        let records = parse_database_session(&path, "s_child", 7, &AtomicU64::new(11)).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].links.event_id.as_deref(), Some("m_a"));
        assert_eq!(records[0].text, "first\nsecond");
        assert_eq!(records[0].turn_id, 7);
        assert_eq!(records[1].links.event_id.as_deref(), Some("m_b"));
        assert_eq!(records[1].role, "user");
        assert_eq!(records[1].turn_id, 8);
        assert_eq!(
            records[0].links.parent_session_id.as_deref(),
            Some("s_root")
        );
        assert_eq!(records[0].source, SourceKind::Opencode);
        assert_eq!(records[0].source_path, path.to_string_lossy());
        assert_eq!(records[0].project, "opencode");
        assert_eq!(records[0].ts, 100);
    }

    #[test]
    fn database_plan_rejects_incomplete_hydration_schema() {
        for (mutation, expected) in [
            ("DROP TABLE message", "message"),
            ("DROP TABLE part", "part"),
            (
                "ALTER TABLE message RENAME TO message_old;
                 CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER)",
                "message",
            ),
            (
                "ALTER TABLE part RENAME TO part_old;
                 CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT)",
                "part",
            ),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("opencode.db");
            let connection = modern_fixture(&path);
            connection.execute_batch(mutation).unwrap();
            drop(connection);

            let error =
                scan_database(&path, None).expect_err("incomplete schema must fail planning");
            assert!(error.to_string().contains(expected));
        }
    }

    #[test]
    fn malformed_database_json_isolated_with_diagnostics() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = modern_fixture(&path);
        connection
            .execute_batch(
                "INSERT INTO message VALUES
                    ('m_bad', 's_child', 50, '{bad json'),
                    ('m_good', 's_child', 60, '{\"role\":\"assistant\"}'),
                    ('m_last', 's_child', 70, '{\"role\":\"user\"}');
                 INSERT INTO part VALUES
                    ('p_bad_message', 'm_bad', '{\"type\":\"text\",\"text\":\"ignored\"}'),
                    ('p_bad_part', 'm_good', '{bad json'),
                    ('p_good_part', 'm_good', '{\"type\":\"text\",\"text\":\"valid sibling\"}'),
                    ('p_last', 'm_last', '{\"type\":\"text\",\"text\":\"after\"}');",
            )
            .unwrap();
        drop(connection);

        let mut records = Vec::new();
        let output = parse_database_records(
            &path,
            "s_child",
            IndexParseState::default(),
            &AtomicU64::new(1),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(output.diagnostics.malformed_json_lines, 2);
        assert_eq!(
            records
                .iter()
                .map(|record| record.text.as_str())
                .collect::<Vec<_>>(),
            vec!["valid sibling", "after"]
        );
        assert_eq!(records[0].turn_id, 0);
        assert_eq!(records[1].turn_id, 1);
    }

    #[test]
    fn database_plan_handles_initial_noop_events_removals_and_cursor_reset() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = modern_fixture(&path);

        let initial = scan_database(&path, None).unwrap();
        assert_eq!(initial.dirty_session_ids, vec!["s_child", "s_root"]);
        assert!(initial.removed_session_ids.is_empty());
        assert_eq!(initial.cursor.event_rowid, 0);
        let initial_state = OpencodeDatabaseState {
            parser_version: DATABASE_STATE_VERSION,
            event_rowid: initial.cursor.event_rowid,
            event_id: initial.cursor.event_id.clone(),
            owned_session_ids: initial.sessions.iter().map(|s| s.id.clone()).collect(),
            ..Default::default()
        };
        let noop = scan_database(&path, Some(&initial_state)).unwrap();
        assert!(noop.dirty_session_ids.is_empty());
        assert!(noop.removed_session_ids.is_empty());

        connection
            .execute(
                "INSERT INTO event (id, aggregate_id) VALUES (?1, ?2)",
                params!["event-1", "s_child"],
            )
            .unwrap();
        let event_scan = scan_database(&path, Some(&initial_state)).unwrap();
        assert_eq!(event_scan.dirty_session_ids, vec!["s_child"]);
        assert_eq!(event_scan.cursor.event_id.as_deref(), Some("event-1"));
        let event_state = OpencodeDatabaseState {
            parser_version: DATABASE_STATE_VERSION,
            event_rowid: event_scan.cursor.event_rowid,
            event_id: event_scan.cursor.event_id.clone(),
            owned_session_ids: event_scan.sessions.iter().map(|s| s.id.clone()).collect(),
            ..Default::default()
        };

        connection
            .execute("DELETE FROM session WHERE id = 's_root'", [])
            .unwrap();
        let removed = scan_database(&path, Some(&event_state)).unwrap();
        assert!(removed.dirty_session_ids.is_empty());
        assert_eq!(removed.removed_session_ids, vec!["s_root"]);

        connection
            .execute("UPDATE event SET id = 'event-replaced'", [])
            .unwrap();
        let sentinel_reset = scan_database(&path, Some(&event_state)).unwrap();
        assert_eq!(sentinel_reset.dirty_session_ids, vec!["s_child"]);
        assert_eq!(
            sentinel_reset.cursor.event_id.as_deref(),
            Some("event-replaced")
        );

        connection.execute("DELETE FROM event", []).unwrap();
        let regression = scan_database(&path, Some(&event_state)).unwrap();
        assert_eq!(regression.dirty_session_ids, vec!["s_child"]);
    }

    #[test]
    fn newly_inventoried_session_is_dirty_without_a_new_event() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = modern_fixture(&path);
        drop(connection);

        let initial = scan_database(&path, None).unwrap();
        let previous = OpencodeDatabaseState {
            parser_version: DATABASE_STATE_VERSION,
            event_rowid: initial.cursor.event_rowid,
            event_id: initial.cursor.event_id,
            owned_session_ids: initial
                .sessions
                .iter()
                .map(|session| session.id.clone())
                .collect(),
            ..Default::default()
        };
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "INSERT INTO session VALUES (?1, NULL, ?2, ?3, ?4)",
                params!["s_new", "/repo/new", 50_i64, 60_i64],
            )
            .unwrap();
        drop(connection);

        let scan = scan_database(&path, Some(&previous)).unwrap();
        assert_eq!(scan.dirty_session_ids, vec!["s_new"]);
    }

    #[test]
    fn production_indexes_are_used_by_message_and_part_projection() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = modern_fixture(&path);
        let mut statement = connection
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT m.id, p.id FROM message AS m
                 JOIN part AS p ON p.message_id = m.id
                 WHERE m.session_id = 's_child'
                 ORDER BY m.time_created, m.id, p.id",
            )
            .unwrap();
        let details = statement
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .map(|row| row.unwrap())
            .collect::<Vec<_>>();
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("message_session_time"))
        );
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("part_message_id"))
        );
    }

    #[test]
    fn deleted_session_during_hydration_is_a_nonfatal_empty_projection() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = modern_fixture(&path);
        drop(connection);

        let output = parse_database_records(
            &path,
            "deleted-before-hydration",
            IndexParseState {
                turn_id: 23,
                ..IndexParseState::default()
            },
            &AtomicU64::new(1),
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(output.turn_id, 23);
        assert_eq!(
            output.session_id.as_deref(),
            Some("deleted-before-hydration")
        );
    }

    #[test]
    fn database_discovery_covers_all_roots_and_legacy_paths_stay_classified() {
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        fs::write(first.join("opencode.db"), []).unwrap();
        fs::write(second.join("opencode-work.db"), []).unwrap();
        fs::write(second.join("other.db"), []).unwrap();
        let databases = discover_databases_from_roots(&[second.clone(), first.clone()]).unwrap();
        assert_eq!(
            databases
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>(),
            vec![first.join("opencode.db"), second.join("opencode-work.db")]
        );

        let message_root = temp.path().join("opencode/storage/message");
        fs::create_dir_all(message_root.join("ses_legacy")).unwrap();
        let legacy = message_root.join("ses_legacy/msg.json");
        fs::write(&legacy, "{}").unwrap();
        assert_eq!(discover_sessions_from_root(&message_root).unwrap().len(), 1);
        assert_eq!(
            crate::sources::classify_path(&legacy.to_string_lossy()),
            SourceKind::Opencode
        );
        assert!(matches_path(&first.join("opencode.db").to_string_lossy()));
    }

    #[test]
    fn legacy_sessions_and_parts_are_discovered_per_data_root() {
        let temp = tempfile::tempdir().unwrap();
        let root_a = temp.path().join("a");
        let root_b = temp.path().join("b");
        for (root, session, message, text) in [
            (&root_a, "ses_a", "msg_a", "from a"),
            (&root_b, "ses_b", "msg_b", "from b"),
        ] {
            let session_dir = root.join("storage/message").join(session);
            let part_dir = root.join("storage/part").join(message);
            fs::create_dir_all(&session_dir).unwrap();
            fs::create_dir_all(&part_dir).unwrap();
            fs::write(
                session_dir.join(format!("{message}.json")),
                format!(r#"{{"id":"{message}","role":"user","time":{{"created":1}}}}"#),
            )
            .unwrap();
            fs::write(
                part_dir.join("part.json"),
                format!(r#"{{"text":"{text}"}}"#),
            )
            .unwrap();
            let session_meta = root.join("storage/session");
            fs::create_dir_all(&session_meta).unwrap();
            fs::write(
                session_meta.join(format!("{session}.json")),
                if session == "ses_b" {
                    r#"{"parentID":"ses_a"}"#
                } else {
                    r#"{}"#
                },
            )
            .unwrap();
        }

        let sessions = discover_sessions_from_roots(&[root_b.clone(), root_a.clone()]).unwrap();
        assert_eq!(sessions.len(), 2);
        assert!(sessions[0].path.starts_with(&root_a));
        assert!(sessions[1].path.starts_with(&root_b));
        let links = session_links_by_id_from_roots(&[root_b.clone(), root_a.clone()]);
        assert_eq!(links.len(), 2);
        assert_eq!(links["ses_b"].parent_session_id.as_deref(), Some("ses_a"));
        let mut records = Vec::new();
        for session in sessions {
            parse_index_records(
                &session.path,
                IndexParseState::default(),
                &HashMap::new(),
                &AtomicU64::new(1),
                |record| {
                    records.push(record);
                    Ok(())
                },
            )
            .unwrap();
        }
        records.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        assert_eq!(
            records
                .iter()
                .map(|record| record.text.as_str())
                .collect::<Vec<_>>(),
            vec!["from a", "from b"]
        );
    }

    #[test]
    fn read_only_reader_sees_committed_wal_data() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA wal_autocheckpoint = 0;
                 CREATE TABLE session (
                    id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT,
                    time_created INTEGER, time_updated INTEGER
                 );
                 CREATE TABLE message (
                    id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT
                 );
                 CREATE TABLE part (
                    id TEXT PRIMARY KEY, message_id TEXT, data TEXT
                 );
                 CREATE TABLE event (
                    id TEXT NOT NULL, aggregate_id TEXT NOT NULL
                 );
                 CREATE INDEX message_session_time ON message(session_id, time_created, id);
                 CREATE INDEX part_message_id ON part(message_id, id);
                 INSERT INTO session VALUES ('wal', NULL, '/wal', 1, 2);
                 INSERT INTO message VALUES ('wal-msg', 'wal', 3, '{\"role\":\"assistant\"}');
                 INSERT INTO part VALUES ('wal-part', 'wal-msg', '{\"type\":\"text\",\"text\":\"visible\"}');
                 INSERT INTO event VALUES ('wal-event-1', 'wal');",
            )
            .unwrap();
        let sessions = enumerate_sessions(&path).unwrap();
        assert_eq!(sessions[0].id, "wal");
        let initial = scan_database(&path, None).unwrap();
        let previous = OpencodeDatabaseState {
            parser_version: DATABASE_STATE_VERSION,
            event_rowid: initial.cursor.event_rowid,
            event_id: initial.cursor.event_id.clone(),
            owned_session_ids: initial.sessions.iter().map(|s| s.id.clone()).collect(),
            ..Default::default()
        };
        writer
            .execute_batch(
                "BEGIN;
                 UPDATE part SET data = '{\"type\":\"text\",\"text\":\"changed while open\"}'
                   WHERE id = 'wal-part';
                 INSERT INTO event VALUES ('wal-event-2', 'wal');
                 COMMIT;",
            )
            .unwrap();
        let delta = scan_database(&path, Some(&previous)).unwrap();
        assert_eq!(delta.dirty_session_ids, vec!["wal"]);
        let records = parse_database_session(&path, "wal", 0, &AtomicU64::new(0)).unwrap();
        assert_eq!(records[0].text, "changed while open");
        drop(writer);
    }

    #[test]
    fn v2_bumped_time_updated_marks_session_dirty() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_message(
            &connection,
            "sm_a1",
            "s_child",
            "user",
            1,
            100,
            100,
            V2_USER_DATA,
        );
        insert_v2_message(
            &connection,
            "sm_a2",
            "s_child",
            "assistant",
            2,
            200,
            200,
            V2_ASSISTANT_DATA,
        );
        drop(connection);

        let initial = scan_database(&path, None).unwrap();
        assert_eq!(
            initial.session_cursors["s_child"],
            OpencodeSessionCursor {
                max_seq: 2,
                max_time_updated: 200,
                row_count: 2,
                event_sequence: None,
            }
        );
        let previous = state_from_scan(&initial);

        // Same sequence, newer in-place update: only the time_updated cursor moves.
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "UPDATE session_message SET time_updated = 250 WHERE id = 'sm_a2'",
                [],
            )
            .unwrap();
        drop(connection);

        let scan = scan_database(&path, Some(&previous)).unwrap();
        assert_eq!(scan.dirty_session_ids, vec!["s_child"]);
    }

    #[test]
    fn v2_deleted_newest_row_marks_session_dirty() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_message(
            &connection,
            "sm_b1",
            "s_child",
            "user",
            1,
            100,
            100,
            V2_USER_DATA,
        );
        insert_v2_message(
            &connection,
            "sm_b2",
            "s_child",
            "assistant",
            2,
            200,
            200,
            V2_ASSISTANT_DATA,
        );
        drop(connection);

        let initial = scan_database(&path, None).unwrap();
        let previous = state_from_scan(&initial);

        let connection = Connection::open(&path).unwrap();
        connection
            .execute("DELETE FROM session_message WHERE id = 'sm_b2'", [])
            .unwrap();
        drop(connection);

        let scan = scan_database(&path, Some(&previous)).unwrap();
        assert_eq!(scan.dirty_session_ids, vec!["s_child"]);
        assert_eq!(
            scan.session_cursors["s_child"],
            OpencodeSessionCursor {
                max_seq: 1,
                max_time_updated: 100,
                row_count: 1,
                event_sequence: None,
            }
        );
    }

    #[test]
    fn frozen_legacy_sessions_are_not_in_the_v2_inventory() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        drop(connection);

        let initial = scan_database(&path, None).unwrap();
        assert!(initial.session_cursors.is_empty());
        assert!(initial.dirty_session_ids.is_empty());
        let previous = state_from_scan(&initial);

        // Legacy rows may be deleted migrated sessions, including before the first scan.
        let scan = scan_database(&path, Some(&previous)).unwrap();
        assert!(scan.dirty_session_ids.is_empty());
        assert!(scan.removed_session_ids.is_empty());
    }

    #[test]
    fn v2_deleted_middle_row_marks_session_dirty() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        for (id, seq, timestamp) in [("first", 1, 100), ("middle", 2, 200), ("last", 3, 300)] {
            insert_v2_message(
                &connection,
                id,
                "s_child",
                "user",
                seq,
                timestamp,
                timestamp,
                V2_USER_DATA,
            );
        }
        drop(connection);

        let initial = scan_database(&path, None).unwrap();
        let previous = state_from_scan(&initial);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute("DELETE FROM session_message WHERE id = 'middle'", [])
            .unwrap();
        drop(connection);

        let scan = scan_database(&path, Some(&previous)).unwrap();
        assert_eq!(scan.dirty_session_ids, vec!["s_child"]);
        assert_eq!(
            scan.session_cursors["s_child"],
            OpencodeSessionCursor {
                max_seq: 3,
                max_time_updated: 300,
                row_count: 2,
                event_sequence: None,
            }
        );
        assert_eq!(initial.session_cursors["s_child"].row_count, 3);
        let records = parse_database_session(&path, "s_child", 0, &AtomicU64::new(0)).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn v2_event_sequence_detects_same_count_middle_replacement() {
        for with_event_sequence in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("opencode.db");
            let connection = v2_fixture(&path);
            for (id, seq, timestamp) in [("first", 1, 100), ("middle", 2, 200), ("last", 3, 300)] {
                insert_v2_message(
                    &connection,
                    id,
                    "s_child",
                    "user",
                    seq,
                    timestamp,
                    timestamp,
                    V2_USER_DATA,
                );
            }
            if with_event_sequence {
                connection
                    .execute_batch(
                        "CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER NOT NULL);
                         INSERT INTO event_sequence VALUES ('s_child', 3);",
                    )
                    .unwrap();
            }
            let initial = scan_database(&path, None).unwrap();
            let previous = state_from_scan(&initial);
            connection
                .execute(
                    "UPDATE session_message SET id = 'replacement' WHERE id = 'middle'",
                    [],
                )
                .unwrap();
            if with_event_sequence {
                connection
                    .execute(
                        "UPDATE event_sequence SET seq = 4 WHERE aggregate_id = 's_child'",
                        [],
                    )
                    .unwrap();
            }
            drop(connection);

            let scan = scan_database(&path, Some(&previous)).unwrap();
            let cursor = &scan.session_cursors["s_child"];
            assert_eq!(cursor.max_seq, 3);
            assert_eq!(cursor.max_time_updated, 300);
            assert_eq!(cursor.row_count, 3);
            if with_event_sequence {
                assert_eq!(cursor.event_sequence, Some(4));
                assert_eq!(scan.dirty_session_ids, vec!["s_child"]);
            } else {
                // Older schemas retain the documented projection-only limitation.
                assert_eq!(cursor.event_sequence, None);
                assert!(scan.dirty_session_ids.is_empty());
            }
        }
    }

    #[test]
    fn v2_empty_projection_retains_event_sequence_and_detects_truncation() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(&connection, "s_child", None, "/repo", 1, 1);
        connection
            .execute_batch(
                "CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER NOT NULL);
                 INSERT INTO event_sequence VALUES ('s_child', 7);",
            )
            .unwrap();
        let initial = scan_database(&path, None).unwrap();
        assert_eq!(
            initial.session_cursors["s_child"],
            OpencodeSessionCursor {
                event_sequence: Some(7),
                ..Default::default()
            }
        );
        let previous = state_from_scan(&initial);
        assert!(
            scan_database(&path, Some(&previous))
                .unwrap()
                .dirty_session_ids
                .is_empty()
        );
        connection
            .execute(
                "UPDATE event_sequence SET seq = 0 WHERE aggregate_id = 's_child'",
                [],
            )
            .unwrap();
        let truncated = scan_database(&path, Some(&previous)).unwrap();
        assert_eq!(truncated.dirty_session_ids, vec!["s_child"]);
        assert_eq!(truncated.session_cursors["s_child"].event_sequence, Some(0));
        let previous = state_from_scan(&truncated);
        connection
            .execute("DELETE FROM event_sequence", [])
            .unwrap();
        drop(connection);
        let empty = scan_database(&path, Some(&previous)).unwrap();
        assert_eq!(empty.dirty_session_ids, vec!["s_child"]);
        assert!(!empty.session_cursors.contains_key("s_child"));
    }

    #[test]
    fn v2_empty_session_accepts_negative_one_event_sequence() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(&connection, "s_empty", None, "/repo", 1, 1);
        connection
            .execute_batch(
                "CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER NOT NULL);
                 INSERT INTO event_sequence VALUES ('s_empty', -1);",
            )
            .unwrap();

        let initial = scan_database(&path, None).unwrap();
        assert_eq!(initial.session_cursors["s_empty"].event_sequence, Some(-1));
        let previous = state_from_scan(&initial);
        assert!(
            scan_database(&path, Some(&previous))
                .unwrap()
                .dirty_session_ids
                .is_empty()
        );

        connection
            .execute(
                "UPDATE event_sequence SET seq = 0 WHERE aggregate_id = 's_empty'",
                [],
            )
            .unwrap();
        let updated = scan_database(&path, Some(&previous)).unwrap();
        assert_eq!(updated.dirty_session_ids, vec!["s_empty"]);
        assert_eq!(updated.session_cursors["s_empty"].event_sequence, Some(0));
    }

    #[test]
    fn v2_session_gaining_first_message_becomes_dirty() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(&connection, "s_child", None, "/repo", 1, 1);
        drop(connection);

        let initial = scan_database(&path, None).unwrap();
        assert!(initial.session_cursors.is_empty());
        let previous = state_from_scan(&initial);

        // A missing previous cursor counts as changed: sessions first seen before they had
        // any v2 messages must be picked up once messages appear.
        let connection = Connection::open(&path).unwrap();
        insert_v2_message(
            &connection,
            "sm_d1",
            "s_child",
            "user",
            1,
            150,
            150,
            V2_USER_DATA,
        );
        drop(connection);

        let scan = scan_database(&path, Some(&previous)).unwrap();
        assert_eq!(scan.dirty_session_ids, vec!["s_child"]);
    }

    #[test]
    fn v2_steady_state_rescan_stays_clean() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_message(
            &connection,
            "sm_h1",
            "s_child",
            "user",
            1,
            100,
            100,
            V2_USER_DATA,
        );
        drop(connection);

        let initial = scan_database(&path, None).unwrap();
        assert_eq!(
            initial.session_cursors["s_child"],
            OpencodeSessionCursor {
                max_seq: 1,
                max_time_updated: 100,
                row_count: 1,
                event_sequence: None,
            }
        );
        let previous = state_from_scan(&initial);

        // A v2 session with an unchanged cursor must not be re-hydrated on every run.
        let scan = scan_database(&path, Some(&previous)).unwrap();
        assert!(scan.dirty_session_ids.is_empty());
        assert!(scan.removed_session_ids.is_empty());
    }

    #[test]
    fn v2_all_messages_removed_marks_session_dirty() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_message(
            &connection,
            "sm_i1",
            "s_child",
            "user",
            1,
            100,
            100,
            V2_USER_DATA,
        );
        insert_v2_message(
            &connection,
            "sm_i2",
            "s_child",
            "assistant",
            2,
            200,
            200,
            V2_ASSISTANT_DATA,
        );
        drop(connection);

        let initial = scan_database(&path, None).unwrap();
        let previous = state_from_scan(&initial);

        // Deleting every `session_message` row leaves the `session_v2` row intact but drops the
        // cursor entirely, so the previously indexed records must be pruned by re-hydrating.
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "DELETE FROM session_message WHERE session_id = 's_child'",
                [],
            )
            .unwrap();
        drop(connection);

        let scan = scan_database(&path, Some(&previous)).unwrap();
        assert_eq!(scan.dirty_session_ids, vec!["s_child"]);
        assert!(!scan.session_cursors.contains_key("s_child"));
    }

    #[test]
    fn v2_inventory_reports_shared_session_removals() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(&connection, "s_v2only", None, "/repo/v2-only", 500, 501);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/v2-child",
            300,
            400,
        );
        drop(connection);

        let scan = scan_database(&path, None).unwrap();
        assert_eq!(
            scan.sessions
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            vec!["s_child", "s_v2only"]
        );
        assert_eq!(
            scan.v2_session_ids,
            HashSet::from(["s_child".to_string(), "s_v2only".to_string()])
        );
        let shared = scan
            .sessions
            .iter()
            .find(|session| session.id == "s_child")
            .unwrap();
        assert_eq!(shared.directory, "/repo/v2-child");
        assert_eq!((shared.time_created, shared.time_updated), (300, 400));

        let previous = state_from_scan(&scan);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute("DELETE FROM session_v2 WHERE id = 's_child'", [])
            .unwrap();
        drop(connection);

        let after = scan_database(&path, Some(&previous)).unwrap();
        assert!(after.dirty_session_ids.is_empty());
        assert_eq!(after.removed_session_ids, vec!["s_child"]);
        assert_eq!(
            after.v2_session_ids,
            HashSet::from(["s_v2only".to_string()])
        );
        assert_eq!(enumerate_sessions(&path).unwrap().len(), 1);
    }

    #[test]
    fn v2_parser_version_mismatch_forces_full_reconcile() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_message(
            &connection,
            "sm_f1",
            "s_child",
            "user",
            1,
            100,
            100,
            V2_USER_DATA,
        );
        drop(connection);

        let initial = scan_database(&path, None).unwrap();
        let mut previous = state_from_scan(&initial);
        // Cursors match, so only the parser-version bump can explain a full reconcile.
        previous.parser_version = DATABASE_STATE_VERSION - 1;
        previous.owned_session_ids.insert("s_gone".to_string());

        let scan = scan_database(&path, Some(&previous)).unwrap();
        assert_eq!(scan.dirty_session_ids, vec!["s_child"]);
        assert_eq!(scan.removed_session_ids, vec!["s_gone"]);
    }

    #[test]
    fn v2_dirty_session_ids_are_sorted() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_message(
            &connection,
            "sm_g1",
            "s_child",
            "user",
            1,
            100,
            100,
            V2_USER_DATA,
        );
        insert_v2_message(
            &connection,
            "sm_g2",
            "s_root",
            "assistant",
            1,
            100,
            100,
            V2_ASSISTANT_DATA,
        );
        drop(connection);

        let initial = scan_database(&path, None).unwrap();
        let previous = state_from_scan(&initial);

        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "UPDATE session_message SET time_updated = time_updated + 10",
                [],
            )
            .unwrap();
        drop(connection);

        let scan = scan_database(&path, Some(&previous)).unwrap();
        assert_eq!(scan.dirty_session_ids, vec!["s_child", "s_root"]);
    }

    fn parse_v2_records(
        path: &Path,
        session_id: &str,
        turn_id: u32,
        next_doc_id: u64,
    ) -> (Vec<Record>, IndexParseOutput) {
        let mut records = Vec::new();
        let output = parse_database_records(
            path,
            session_id,
            IndexParseState {
                turn_id,
                ..Default::default()
            },
            &AtomicU64::new(next_doc_id),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();
        (records, output)
    }

    #[test]
    fn v2_user_message_hydrates_text_and_links() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/child",
            30,
            40,
        );
        insert_v2_message(
            &connection,
            "sm_user",
            "s_child",
            "user",
            1,
            100,
            100,
            r#"{"text":"hello world","time":{"created":100}}"#,
        );
        drop(connection);

        let (records, output) = parse_v2_records(&path, "s_child", 0, 1);
        assert_eq!(records.len(), 1);
        assert!(output.diagnostics.is_empty());
        assert_eq!(records[0].role, "user");
        assert_eq!(records[0].text, "hello world");
        assert_eq!(records[0].ts, 100);
        assert_eq!(records[0].turn_id, 0);
        assert_eq!(records[0].links.event_id.as_deref(), Some("sm_user"));
        assert_eq!(records[0].links.conversation_kind.as_deref(), Some("fork"));
        assert_eq!(
            records[0].links.parent_session_id.as_deref(),
            Some("s_root")
        );
        assert_eq!(records[0].source_path, path.to_string_lossy());
        assert_eq!(records[0].project, "opencode");
    }

    #[test]
    fn v2_only_session_hydrates_without_a_v1_row() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(&connection, "s_v2only", None, "/repo/v2-only", 10, 20);
        insert_v2_message(
            &connection,
            "sm_only",
            "s_v2only",
            "user",
            1,
            100,
            100,
            r#"{"text":"v2 only"}"#,
        );
        drop(connection);

        let (records, _output) = parse_v2_records(&path, "s_v2only", 0, 1);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].text, "v2 only");
        assert_eq!(records[0].links.conversation_kind.as_deref(), Some("main"));
        assert_eq!(records[0].links.parent_session_id, None);
    }

    #[test]
    fn v2_assistant_prose_and_tool_hydrate_separate_records() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/child",
            30,
            40,
        );
        let data = serde_json::json!({
            "agent": "build",
            "content": [
                {"type": "reasoning", "text": "secret"},
                {"type": "text", "text": "running the command"},
                {
                    "type": "tool",
                    "id": "call_1",
                    "name": "bash",
                    "state": {
                        "status": "completed",
                        "input": {"command": "ls"},
                        "content": [{"type": "text", "text": "file.txt"}],
                        "metadata": {}
                    }
                }
            ],
            "time": {"created": 200}
        })
        .to_string();
        insert_v2_message(
            &connection,
            "sm_assistant",
            "s_child",
            "assistant",
            1,
            200,
            200,
            &data,
        );
        drop(connection);

        let (records, _output) = parse_v2_records(&path, "s_child", 0, 1);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].role, "assistant");
        assert_eq!(records[0].text, "running the command");
        assert_eq!(records[0].links.event_id.as_deref(), Some("sm_assistant"));
        assert!(records[0].tool_name.is_none());
        assert!(records[1].text.is_empty());
        assert_eq!(
            records[1].links.event_id.as_deref(),
            Some("sm_assistant:tool:id:call_1")
        );
        assert_eq!(records[1].tool_name.as_deref(), Some("bash"));
        assert_eq!(records[1].tool_output.as_deref(), Some("file.txt"));
        let tool_input: serde_json::Value =
            serde_json::from_str(records[1].tool_input.as_deref().unwrap()).unwrap();
        assert_eq!(tool_input, serde_json::json!({"command": "ls"}));
    }

    #[test]
    fn v2_tool_output_falls_back_to_metadata_output() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/child",
            30,
            40,
        );
        let data = serde_json::json!({
            "content": [
                {
                    "type": "tool",
                    "name": "read",
                    "state": {
                        "input": {"path": "a.txt"},
                        "content": [],
                        "metadata": {"output": "file contents"}
                    }
                }
            ]
        })
        .to_string();
        insert_v2_message(
            &connection,
            "sm_fallback",
            "s_child",
            "assistant",
            1,
            200,
            200,
            &data,
        );
        drop(connection);

        let (records, _output) = parse_v2_records(&path, "s_child", 0, 1);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tool_name.as_deref(), Some("read"));
        assert_eq!(records[0].tool_output.as_deref(), Some("file contents"));
    }

    #[test]
    fn v2_tool_only_assistant_message_preserves_every_tool_and_counts_usage_once() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/child",
            30,
            40,
        );
        let data = serde_json::json!({
            "tokens": {"input": 7, "output": 11},
            "content": [
                {
                    "type": "tool",
                    "id": "call_first",
                    "name": "first",
                    "state": {"input": {"a": 1}, "metadata": {"output": "one"}}
                },
                {
                    "type": "tool",
                    "name": "second",
                    "state": {"input": {"b": 2}, "metadata": {"output": "two"}}
                }
            ]
        })
        .to_string();
        insert_v2_message(
            &connection,
            "sm_tool_only",
            "s_child",
            "assistant",
            1,
            200,
            200,
            &data,
        );
        drop(connection);

        let (records, _output) = parse_v2_records(&path, "s_child", 0, 1);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].text, "");
        assert_eq!(records[0].tool_name.as_deref(), Some("first"));
        assert_eq!(records[0].tool_output.as_deref(), Some("one"));
        assert_eq!(records[0].tool_input.as_deref(), Some("{\"a\":1}"));
        assert_eq!(
            records[0].links.event_id.as_deref(),
            Some("sm_tool_only:tool:id:call_first")
        );
        assert_eq!(records[1].tool_name.as_deref(), Some("second"));
        assert_eq!(records[1].tool_output.as_deref(), Some("two"));
        assert_eq!(records[1].tool_input.as_deref(), Some("{\"b\":2}"));
        assert_eq!(
            records[1].links.event_id.as_deref(),
            Some("sm_tool_only:tool:index:1")
        );

        let (reparsed, _) = parse_v2_records(&path, "s_child", 10, 100);
        for (original, repeated) in records.iter().zip(&reparsed) {
            assert_eq!(original.links.event_id, repeated.links.event_id);
        }
        let usage = parse_usage_file(&path).unwrap();
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].message_id.as_deref(), Some("sm_tool_only"));
        assert_eq!(usage[0].tokens.uncached_input, 7);
        assert_eq!(usage[0].tokens.output, 11);

        // Inserting prose before a tool must not change an upstream-ID-based identity.
        let mut updated: serde_json::Value = serde_json::from_str(&data).unwrap();
        updated["content"].as_array_mut().unwrap().insert(
            0,
            serde_json::json!({"type": "text", "text": "new explanation"}),
        );
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "UPDATE session_message SET data = ?1 WHERE id = 'sm_tool_only'",
                [updated.to_string()],
            )
            .unwrap();
        drop(connection);
        let (updated_records, _) = parse_v2_records(&path, "s_child", 0, 1);
        assert_eq!(updated_records.len(), 3);
        assert_eq!(updated_records[0].text, "new explanation");
        assert_eq!(updated_records[1].links.event_id, records[0].links.event_id);
    }

    #[test]
    fn v2_reasoning_only_assistant_message_is_dropped() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/child",
            30,
            40,
        );
        let data = serde_json::json!({
            "content": [{"type": "reasoning", "text": "thinking"}]
        })
        .to_string();
        insert_v2_message(
            &connection,
            "sm_reasoning",
            "s_child",
            "assistant",
            1,
            200,
            200,
            &data,
        );
        drop(connection);

        let (records, _output) = parse_v2_records(&path, "s_child", 0, 1);
        assert!(records.is_empty());
    }

    #[test]
    fn v2_known_and_unknown_types_are_skipped() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/child",
            30,
            40,
        );
        for (seq, (id, kind, data)) in [
            ("sm_user", "user", r#"{"text":"kept"}"#),
            (
                "sm_synthetic",
                "synthetic",
                r#"{"text":"synthetic skipped"}"#,
            ),
            ("sm_shell", "shell", r#"{"text":"shell skipped"}"#),
            ("sm_unknown", "mystery", r#"{"foo":"bar"}"#),
        ]
        .into_iter()
        .enumerate()
        {
            insert_v2_message(
                &connection,
                id,
                "s_child",
                kind,
                seq as i64 + 1,
                100,
                100,
                data,
            );
        }
        drop(connection);

        let (records, output) = parse_v2_records(&path, "s_child", 0, 1);
        assert_eq!(
            records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            vec!["kept"]
        );
        assert_eq!(
            output.diagnostics.unknown_semantic_types.get("mystery"),
            Some(&1)
        );
    }

    #[test]
    fn v2_malformed_messages_and_items_count_diagnostics() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/child",
            30,
            40,
        );
        insert_v2_message(
            &connection,
            "sm_bad_message",
            "s_child",
            "assistant",
            1,
            100,
            100,
            "{bad json",
        );
        let data = serde_json::json!({
            "content": ["not-an-object", {"type": "text", "text": "kept"}]
        })
        .to_string();
        insert_v2_message(
            &connection,
            "sm_bad_item",
            "s_child",
            "assistant",
            2,
            200,
            200,
            &data,
        );
        drop(connection);

        let (records, output) = parse_v2_records(&path, "s_child", 0, 1);
        assert_eq!(output.diagnostics.malformed_json_lines, 2);
        assert_eq!(
            records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            vec!["kept"]
        );
    }

    #[test]
    fn v2_messages_emit_in_seq_order_and_increment_turn_ids() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/child",
            30,
            40,
        );
        // Inserted out of order to prove the query orders by `seq`.
        insert_v2_message(
            &connection,
            "sm_third",
            "s_child",
            "assistant",
            3,
            300,
            300,
            r#"{"content":[{"type":"text","text":"third"}]}"#,
        );
        insert_v2_message(
            &connection,
            "sm_first",
            "s_child",
            "user",
            1,
            100,
            100,
            r#"{"text":"first"}"#,
        );
        insert_v2_message(
            &connection,
            "sm_second",
            "s_child",
            "user",
            2,
            200,
            200,
            r#"{"text":"second"}"#,
        );
        drop(connection);

        let (records, _output) = parse_v2_records(&path, "s_child", 5, 10);
        assert_eq!(
            records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            vec!["first", "second", "third"]
        );
        assert_eq!(
            records.iter().map(|r| r.turn_id).collect::<Vec<_>>(),
            vec![5, 6, 7]
        );
        assert_eq!(
            records.iter().map(|r| r.doc_id).collect::<Vec<_>>(),
            vec![10, 11, 12]
        );
    }

    fn v2_assistant_data(content: serde_json::Value) -> String {
        serde_json::json!({"content": content}).to_string()
    }

    fn insert_v1_message(connection: &Connection, id: &str, session_id: &str, ts: i64, text: &str) {
        connection
            .execute(
                "INSERT INTO message (id, session_id, time_created, data) VALUES (?1, ?2, ?3, ?4)",
                params![id, session_id, ts, r#"{"role":"user"}"#],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO part (id, message_id, data) VALUES (?1, ?2, ?3)",
                params![
                    format!("part-{id}"),
                    id,
                    serde_json::json!({"type": "text", "text": text}).to_string()
                ],
            )
            .unwrap();
    }

    #[test]
    fn v2_dual_store_never_hydrates_missing_projection_rows() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/child",
            30,
            40,
        );
        insert_v2_message(
            &connection,
            "sm_shared",
            "s_child",
            "user",
            1,
            100,
            100,
            r#"{"text":"from v2"}"#,
        );
        // Missing projection rows are ambiguous: they may have been deleted before this scan.
        insert_v1_message(&connection, "sm_shared", "s_child", 100, "shadowed");
        insert_v1_message(&connection, "msg_v1only", "s_child", 50, "v1 only");
        insert_v1_message(&connection, "msg_empty", "s_child", 25, "");
        drop(connection);

        let (records, _output) = parse_v2_records(&path, "s_child", 0, 1);
        assert_eq!(
            records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            vec!["from v2"]
        );
        assert_eq!(
            records
                .iter()
                .map(|r| r.links.event_id.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("sm_shared")]
        );
        assert_eq!(
            records.iter().map(|r| r.turn_id).collect::<Vec<_>>(),
            vec![0]
        );
        assert_eq!(
            records.iter().map(|r| r.doc_id).collect::<Vec<_>>(),
            vec![1]
        );
        assert!(!records.iter().any(|r| r.text == "shadowed"));
        assert!(
            !records
                .iter()
                .any(|r| r.links.event_id.as_deref() == Some("msg_empty"))
        );
    }

    #[test]
    fn v2_tool_output_precedence_chain() {
        let cases = [
            (
                serde_json::json!({
                    "type": "tool",
                    "name": "t",
                    "state": {
                        "input": {},
                        "content": [{"type": "text", "text": "from content"}],
                        "metadata": {
                            "output": "from output",
                            "outputPath": "/tmp/out",
                            "filepath": "/tmp/file"
                        }
                    }
                }),
                "from content",
            ),
            (
                serde_json::json!({
                    "type": "tool",
                    "name": "t",
                    "state": {
                        "input": {},
                        "content": [],
                        "metadata": {
                            "output": "from output",
                            "outputPath": "/tmp/out",
                            "filepath": "/tmp/file"
                        }
                    }
                }),
                "from output",
            ),
            (
                serde_json::json!({
                    "type": "tool",
                    "name": "t",
                    "state": {
                        "input": {},
                        "content": [],
                        "metadata": {"outputPath": "/tmp/out", "filepath": "/tmp/file"}
                    }
                }),
                "/tmp/out",
            ),
            (
                serde_json::json!({
                    "type": "tool",
                    "name": "t",
                    "state": {"input": {}, "content": [], "metadata": {"filepath": "/tmp/file"}}
                }),
                "/tmp/file",
            ),
        ];
        for (tool, expected) in cases {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("opencode.db");
            let connection = v2_fixture(&path);
            insert_v2_session(
                &connection,
                "s_child",
                Some("s_root"),
                "/repo/child",
                30,
                40,
            );
            insert_v2_message(
                &connection,
                "sm_tool",
                "s_child",
                "assistant",
                1,
                100,
                100,
                &v2_assistant_data(serde_json::json!([tool])),
            );
            drop(connection);

            let (records, _output) = parse_v2_records(&path, "s_child", 0, 1);
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].tool_output.as_deref(), Some(expected));
        }
    }

    #[test]
    fn v2_tool_output_ignores_file_items() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/child",
            30,
            40,
        );
        let tool = serde_json::json!({
            "type": "tool",
            "name": "t",
            "state": {
                "input": {},
                "content": [{"type": "file", "text": "must not leak"}],
                "metadata": {"output": "fallback"}
            }
        });
        insert_v2_message(
            &connection,
            "sm_file_item",
            "s_child",
            "assistant",
            1,
            100,
            100,
            &v2_assistant_data(serde_json::json!([tool])),
        );
        drop(connection);

        let (records, _output) = parse_v2_records(&path, "s_child", 0, 1);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tool_output.as_deref(), Some("fallback"));
    }

    #[test]
    fn v2_system_message_role_passthrough() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/child",
            30,
            40,
        );
        insert_v2_message(
            &connection,
            "sm_system",
            "s_child",
            "system",
            1,
            100,
            100,
            r#"{"text":"system note"}"#,
        );
        drop(connection);

        let (records, _output) = parse_v2_records(&path, "s_child", 0, 1);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].role, "system");
        assert_eq!(records[0].text, "system note");
    }

    #[test]
    fn v2_negative_timestamp_on_skippable_row_is_tolerated() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/child",
            30,
            40,
        );
        // A bad timestamp on a skipped kind must not abort the session.
        insert_v2_message(
            &connection,
            "sm_skip",
            "s_child",
            "synthetic",
            1,
            -5,
            -5,
            r#"{"text":"skipped"}"#,
        );
        insert_v2_message(
            &connection,
            "sm_user",
            "s_child",
            "user",
            2,
            100,
            100,
            r#"{"text":"kept"}"#,
        );
        drop(connection);

        let (records, output) = parse_v2_records(&path, "s_child", 0, 1);
        assert_eq!(
            records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            vec!["kept"]
        );
        assert!(output.diagnostics.is_empty());
    }

    #[test]
    fn v2_negative_timestamp_on_emitted_row_is_diagnostic() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/child",
            30,
            40,
        );
        insert_v2_message(
            &connection,
            "sm_bad_ts",
            "s_child",
            "user",
            1,
            -5,
            -5,
            r#"{"text":"dropped"}"#,
        );
        insert_v2_message(
            &connection,
            "sm_user",
            "s_child",
            "user",
            2,
            100,
            100,
            r#"{"text":"kept"}"#,
        );
        drop(connection);

        let (records, output) = parse_v2_records(&path, "s_child", 0, 1);
        assert_eq!(
            records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            vec!["kept"]
        );
        assert_eq!(output.diagnostics.malformed_json_lines, 1);
    }

    #[test]
    fn enumerate_sessions_uses_authoritative_v2_inventory() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_session(&connection, "s_v2only", None, "/repo/v2-only", 500, 501);
        insert_v2_session(
            &connection,
            "s_child",
            Some("s_root"),
            "/repo/v2-child",
            300,
            400,
        );
        drop(connection);

        let sessions = enumerate_sessions(&path).unwrap();
        assert_eq!(
            sessions
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            vec!["s_child", "s_v2only"]
        );
        let child = sessions
            .iter()
            .find(|session| session.id == "s_child")
            .unwrap();
        assert_eq!(child.directory, "/repo/v2-child");
        assert_eq!((child.time_created, child.time_updated), (300, 400));
    }

    #[test]
    fn v2_usage_assistant_event_uses_model_object_and_column_session() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_message(
            &connection,
            "sm_usage",
            "s_child",
            "assistant",
            1,
            200,
            200,
            V2_ASSISTANT_DATA,
        );
        drop(connection);

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.provider.as_deref(), Some("anthropic"));
        assert_eq!(event.model.as_deref(), Some("claude"));
        assert_eq!(event.session_id.as_deref(), Some("s_child"));
        assert_eq!(event.source_record_id.as_deref(), Some("sm_usage"));
        assert_eq!(event.message_id.as_deref(), Some("sm_usage"));
        assert_eq!(event.tokens.uncached_input, 1);
        assert_eq!(event.tokens.output, 2);
        assert_eq!(event.timestamp_ms, 200_000);
        assert_eq!(event.source_cost_usd, Some(0.0));
    }

    #[test]
    fn v2_usage_token_buckets_include_cache_and_reasoning() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        let data = serde_json::json!({
            "model": {"providerID": "anthropic", "id": "claude"},
            "tokens": {
                "input": 10,
                "output": 2,
                "reasoning": 3,
                "cache": {"read": 4, "write": 5}
            },
            "time": {"created": 100}
        })
        .to_string();
        insert_v2_message(
            &connection,
            "sm_buckets",
            "s_child",
            "assistant",
            1,
            100,
            100,
            &data,
        );
        drop(connection);

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        let tokens = &events[0].tokens;
        assert_eq!(tokens.uncached_input, 10);
        assert_eq!(tokens.cache_read, 4);
        assert_eq!(tokens.cache_write, 5);
        assert_eq!(tokens.reasoning, 3);
        assert_eq!(tokens.output, 5);
    }

    #[test]
    fn v2_usage_never_resurrects_missing_projection_rows() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_fixture(&path);
        insert_v2_message(
            &connection,
            "sm_v2",
            "s_child",
            "assistant",
            1,
            100,
            100,
            V2_ASSISTANT_DATA,
        );
        // Same id in v1 with different tokens must not produce a second event.
        connection
            .execute(
                "INSERT INTO message VALUES ('sm_v2', 's_child', 100, ?1)",
                [r#"{"tokens":{"input":999,"output":999}}"#],
            )
            .unwrap();
        // A legacy-only row may have been deleted from v2 and must not be counted.
        connection
            .execute(
                "INSERT INTO message VALUES ('msg_v1only', 's_child', 300, ?1)",
                [r#"{"tokens":{"input":5,"output":1},"providerID":"openai","modelID":"gpt","time":{"created":300}}"#],
            )
            .unwrap();
        drop(connection);

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        let v2_event = events
            .iter()
            .find(|event| event.source_record_id.as_deref() == Some("sm_v2"))
            .expect("v2 event");
        assert_eq!(v2_event.tokens.uncached_input, 1);
        assert_eq!(v2_event.provider.as_deref(), Some("anthropic"));
    }

    #[test]
    fn v1_usage_database_keeps_top_level_provider_and_model() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = modern_fixture(&path);
        connection
            .execute(
                "INSERT INTO message VALUES ('msg_v1', 's_child', 400, ?1)",
                [r#"{"tokens":{"input":7,"output":3},"providerID":"anthropic","modelID":"claude-3","cost":0.5,"time":{"created":400}}"#],
            )
            .unwrap();
        drop(connection);

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].provider.as_deref(), Some("anthropic"));
        assert_eq!(events[0].model.as_deref(), Some("claude-3"));
        assert_eq!(events[0].tokens.uncached_input, 7);
        assert_eq!(events[0].timestamp_ms, 400_000);
    }

    #[test]
    fn v2_only_database_plans_enumerates_and_hydrates() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_only_fixture(&path);
        drop(connection);

        let scan = scan_database(&path, None).unwrap();
        assert_eq!(
            scan.sessions
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            vec!["s_v2only"]
        );
        assert_eq!(scan.v2_session_ids, HashSet::from(["s_v2only".to_string()]));
        assert_eq!(scan.dirty_session_ids, vec!["s_v2only"]);
        assert!(scan.removed_session_ids.is_empty());
        assert_eq!(
            scan.session_cursors["s_v2only"],
            OpencodeSessionCursor {
                max_seq: 2,
                max_time_updated: 200,
                row_count: 2,
                event_sequence: None,
            }
        );
        assert_eq!(scan.cursor.event_rowid, 0);
        assert_eq!(scan.cursor.event_id, None);

        let sessions = enumerate_sessions(&path).unwrap();
        assert_eq!(
            sessions
                .iter()
                .map(|session| session.id.as_str())
                .collect::<Vec<_>>(),
            vec!["s_v2only"]
        );
        assert_eq!(sessions[0].directory, "/repo/v2-only");

        // Hydration must also tolerate the absent v1 `message`/`part` tables.
        let records = parse_database_session(&path, "s_v2only", 0, &AtomicU64::new(1)).unwrap();
        assert_eq!(
            records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            vec!["hello v2", "assistant v2"]
        );
        assert_eq!(records[0].links.conversation_kind.as_deref(), Some("main"));
    }

    #[test]
    fn v2_only_database_usage_reads_v2_rows() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("opencode.db");
        let connection = v2_only_fixture(&path);
        drop(connection);

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].provider.as_deref(), Some("anthropic"));
        assert_eq!(events[0].model.as_deref(), Some("claude"));
        assert_eq!(events[0].session_id.as_deref(), Some("s_v2only"));
        assert_eq!(events[0].tokens.uncached_input, 1);
        assert_eq!(events[0].tokens.output, 2);
    }

    #[test]
    fn usage_provider_model_reads_legacy_object_model_id() {
        let mut data = br#"{"model":{"providerID":"p","modelID":"m"}}"#.to_vec();
        let value = simd_json::to_borrowed_value(&mut data).unwrap();
        assert_eq!(
            usage_provider_model(&value),
            (Some("p".to_string()), Some("m".to_string()))
        );
    }
}
