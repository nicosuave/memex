use super::{IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions, SourceFile};
use crate::state::OpencodeDatabaseState;
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
    usage: 3,
};

/// Version for the SQLite event cursor and owned-session reconciliation rules.  This is
/// intentionally independent of the shared source identity/index versions: legacy JSON parsing
/// has not changed merely because database planning was added.
///
/// Bumped for the parent-linkage-only reclassification: unparented sessions
/// with non-primary agent values were stored as subagent and must reconcile
/// once so their records and analytics rows reflect the new kinds.
pub const DATABASE_STATE_VERSION: u32 = 2;

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

/// Enumerate the modern session inventory from one OpenCode database.
pub fn enumerate_sessions(path: &Path) -> Result<Vec<OpencodeSession>> {
    let connection = open_read_only_database(path)?;
    require_modern_schema(&connection, path)?;
    enumerate_sessions_from_connection(&connection, path)
}

fn enumerate_sessions_from_connection(
    connection: &Connection,
    path: &Path,
) -> Result<Vec<OpencodeSession>> {
    let mut statement = connection
        .prepare(
            "SELECT id, parent_id, directory, time_created, time_updated
             FROM session ORDER BY id",
        )
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
    pub sessions: Vec<OpencodeSession>,
    pub dirty_session_ids: Vec<String>,
    pub removed_session_ids: Vec<String>,
    pub cursor: DatabaseCursor,
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
    let connection = open_read_only_database(path)?;
    connection
        .execute_batch("BEGIN")
        .with_context(|| format!("begin OpenCode planning snapshot in {}", path.display()))?;
    // Planning must reject databases that hydration cannot read, so ingest can apply its
    // per-database fallback consistently.
    require_modern_schema(&connection, path)?;
    require_event_schema(&connection, path)?;
    let sessions = enumerate_sessions_from_connection(&connection, path)?;
    let cursor = current_event_cursor(&connection, path)?;
    let (mut dirty, removed) = full_reconcile(&sessions, previous);
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
                    .with_context(|| format!("verify OpenCode event cursor in {}", path.display()))?
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

    let scan = DatabaseScan {
        sessions,
        dirty_session_ids: dirty,
        removed_session_ids: removed,
        cursor,
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
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    let connection = open_read_only_database(path)?;
    require_modern_schema(&connection, path)?;
    let Some(session) = enumerate_session_from_connection(&connection, path, session_id)? else {
        return Ok(IndexParseOutput {
            legacy_turn_id: None,
            offset: 0,
            turn_id: state.turn_id,
            pending_tool_calls: state.pending_tool_calls,
            session_id: Some(session_id.to_string()),
            diagnostics: Default::default(),
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

    let mut current: Option<ModernMessage> = None;
    let mut malformed_message_ids = HashSet::new();
    let mut turn_id = state.turn_id;
    let mut diagnostics = ParseDiagnostics::default();
    for row in rows {
        let (message_id, timestamp, message_data, _part_id, part_data) = row?;
        if current
            .as_ref()
            .is_some_and(|message| message.id != message_id)
            && let Some(message) = current.take()
        {
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
    })
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
        files.extend(databases);
    }
    for root in &roots {
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
    let mut statement = connection.prepare("SELECT id, session_id, data FROM message")?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let mut ids = HashSet::new();
    let mut events = Vec::new();
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
    Ok(events)
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
        provider: borrowed_string(value, &["providerID", "provider"]),
        model: borrowed_string(value, &["modelID", "model"]),
        tokens,
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
}
