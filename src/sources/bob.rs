//! IBM Bob task discovery and indexing.
//!
//! Bob (the IBM coding agent, both the IDE and the `bob` CLI) keeps every task in
//! one SQLite database, `~/.bob/db/bob.db` (WAL mode):
//!
//! * `tasks` — one row per task: `id`, `parent_id`, `task_type`
//!   (`normal`/`subtask`/`subagent`), `project_id` (a `file:` workspace URL),
//!   `title`, `costs` JSON, `created_at`/`updated_at` in Unix milliseconds.
//! * `messages` — one row per message: `task_id`, `role`
//!   (`system`/`user`/`assistant`/`tool`), `data` JSON, `created_at`.
//!   Assistant `data` carries `content`, optional `toolCalls[]` and
//!   `_meta.spend`; tool rows carry the output in `content` and the originating
//!   call in `toolUsage.signature`. A `spawn_subagent` result also embeds the
//!   child's whole transcript under `messages` with `_meta.subagentId`; the child
//!   task row has no messages of its own.
//!
//! Rows are written whole per turn and never edited in place, but Bob rewrites a
//! task's rows when it finishes or reopens: they are deleted and re-inserted with
//! new rowids and `created_at`, possibly in a different order. Message ids and
//! `_meta.timestamp` survive the rewrite and are the only stable identity and order.
//!
//! The database is shared by every task, but memex tracks state, deletes and
//! session metadata per source path. Each task is therefore exposed as a
//! virtual file `<db>/<task_id>`. That path routes through the database file,
//! so a `stat` fails with `ENOTDIR` rather than `NotFound` and the generic
//! missing-file sweep leaves it alone; discovery reconciles vanished tasks
//! itself. A task is replayed wholesale whenever its message count, newest row,
//! newest `created_at` or `updated_at` changes (a rewrite moves all of them),
//! paired with a delete-first re-parse like jcode.

use super::{IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions, SourceFile};
use crate::state::PendingToolCall;
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub const VERSIONS: ParserVersions = ParserVersions {
    // Bumped when session metadata rules change (labels, cwd, hierarchy).
    identity: 2,
    // Bumped when record extraction changes; every task is replayed on the next refresh.
    index: 2,
    usage: 1,
};

const DATABASE_NAME: &str = "bob.db";

/// Databases to index: `MEMEX_BOB_DB` (comma-separated, `~/` expanded) or `~/.bob/db/bob.db`.
/// Spellings are kept as configured: state keys embed the database path, so a changed
/// spelling reads as a new database.
pub fn database_paths() -> Vec<PathBuf> {
    std::env::var_os("MEMEX_BOB_DB")
        .map(|value| {
            value
                .to_string_lossy()
                .split(',')
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(|path| match path.strip_prefix("~/") {
                    Some(rest) => super::common::home().join(rest),
                    None => PathBuf::from(path),
                })
                .collect()
        })
        .unwrap_or_else(|| vec![super::common::home().join(".bob/db").join(DATABASE_NAME)])
}

/// Directories the watch daemon observes for database and WAL changes.
pub fn roots() -> Vec<PathBuf> {
    database_paths()
        .into_iter()
        .filter_map(|path| path.parent().map(Path::to_path_buf))
        .collect()
}

/// The default database name, or any path configured through `MEMEX_BOB_DB`.
pub fn is_db_path(path: &Path) -> bool {
    path.file_name().and_then(|name| name.to_str()) == Some(DATABASE_NAME)
        || database_paths().iter().any(|database| database == path)
}

/// Whether a persisted source path is a Bob task (`<db>/<task_id>`).
pub fn matches_path(path: &str) -> bool {
    split_virtual_path(Path::new(path)).is_some()
}

pub fn virtual_path(database: &Path, task_id: &str) -> PathBuf {
    database.join(task_id)
}

/// Split `<db>/<task_id>` into the database path and task id.
pub fn split_virtual_path(path: &Path) -> Option<(PathBuf, String)> {
    let task_id = path.file_name()?.to_str()?;
    let database = path.parent()?;
    (is_db_path(database) && !task_id.is_empty())
        .then(|| (database.to_path_buf(), task_id.to_string()))
}

/// Existing databases, for callers that enumerate files per source.
pub fn discover_databases() -> Vec<SourceFile> {
    database_paths()
        .into_iter()
        .filter(|path| path.is_file())
        .map(|path| SourceFile {
            source: SourceKind::Bob,
            path,
        })
        .collect()
}

pub fn usage_files() -> Vec<PathBuf> {
    discover_databases()
        .into_iter()
        .map(|file| file.path)
        .collect()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BobTask {
    pub id: String,
    pub parent_id: Option<String>,
    pub task_type: String,
    pub project_id: String,
    pub title: String,
    pub updated_at: u64,
    pub message_count: u64,
    pub max_rowid: i64,
    pub max_created_at: u64,
}

impl BobTask {
    /// Stable fingerprint of everything discovery can observe cheaply. Any change replays
    /// the task; identical values mean the indexed records are still current.
    pub fn fingerprint(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.updated_at.to_le_bytes());
        hasher.update(self.message_count.to_le_bytes());
        hasher.update(self.max_rowid.to_le_bytes());
        hasher.update(self.max_created_at.to_le_bytes());
        hasher.update(self.parent_id.as_deref().unwrap_or_default().as_bytes());
        hasher.update([0]);
        hasher.update(self.project_id.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    pub fn workspace(&self) -> Option<String> {
        workspace_from_project_id(&self.project_id)
    }

    fn conversation_kind(&self) -> &'static str {
        match (self.parent_id.as_deref(), self.task_type.as_str()) {
            (Some(_), "subagent") => "subagent",
            (Some(_), _) => "fork",
            (None, _) => "main",
        }
    }
}

/// `file:/Users/me/repo` or `file:///Users/me/repo` → `/Users/me/repo`.
pub fn workspace_from_project_id(project_id: &str) -> Option<String> {
    let rest = project_id.strip_prefix("file:")?;
    let path = rest.strip_prefix("//").unwrap_or(rest);
    let path = path.strip_prefix("localhost").unwrap_or(path);
    let decoded = percent_decode(path);
    (!decoded.is_empty() && decoded.starts_with('/')).then_some(decoded)
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && let Some(hex) = value.get(index + 1..index + 3)
            && let Ok(byte) = u8::from_str_radix(hex, 16)
        {
            out.push(byte);
            index += 3;
            continue;
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub(crate) fn open_read_only(path: &Path) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("open Bob database read-only: {}", path.display()))?;
    connection
        .busy_timeout(Duration::from_millis(2_000))
        .with_context(|| format!("set Bob database busy timeout: {}", path.display()))?;
    connection
        .execute_batch("PRAGMA query_only = ON")
        .with_context(|| format!("enable SQLite query_only for {}", path.display()))?;
    Ok(connection)
}

/// Every task that has at least one message, with the aggregates discovery fingerprints.
pub fn enumerate_tasks(database: &Path) -> Result<Vec<BobTask>> {
    let connection = open_read_only(database)?;
    let mut statement = connection
        .prepare(
            "SELECT t.id, t.parent_id, t.task_type, t.project_id, t.title, t.updated_at,
                    m.count, m.max_rowid, m.max_created_at
             FROM tasks AS t
             JOIN (SELECT task_id, COUNT(*) AS count, MAX(rowid) AS max_rowid,
                          MAX(created_at) AS max_created_at
                   FROM messages GROUP BY task_id) AS m ON m.task_id = t.id
             ORDER BY t.id",
        )
        .with_context(|| format!("prepare Bob task query for {}", database.display()))?;
    let rows = statement
        .query_map([], |row| {
            Ok(BobTask {
                id: row.get(0)?,
                parent_id: row.get(1)?,
                task_type: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                project_id: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                title: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                updated_at: row.get::<_, i64>(5)?.max(0) as u64,
                message_count: row.get::<_, i64>(6)?.max(0) as u64,
                max_rowid: row.get(7)?,
                max_created_at: row.get::<_, i64>(8)?.max(0) as u64,
            })
        })
        .with_context(|| format!("query Bob tasks in {}", database.display()))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .with_context(|| format!("read Bob tasks in {}", database.display()))
}

fn lookup_task(connection: &Connection, database: &Path, task_id: &str) -> Result<Option<BobTask>> {
    connection
        .query_row(
            "SELECT t.id, t.parent_id, t.task_type, t.project_id, t.title, t.updated_at,
                    (SELECT COUNT(*) FROM messages WHERE task_id = t.id),
                    (SELECT COALESCE(MAX(rowid), 0) FROM messages WHERE task_id = t.id),
                    (SELECT COALESCE(MAX(created_at), 0) FROM messages WHERE task_id = t.id)
             FROM tasks AS t WHERE t.id = ?1",
            [task_id],
            |row| {
                Ok(BobTask {
                    id: row.get(0)?,
                    parent_id: row.get(1)?,
                    task_type: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                    project_id: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                    title: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                    updated_at: row.get::<_, i64>(5)?.max(0) as u64,
                    message_count: row.get::<_, i64>(6)?.max(0) as u64,
                    max_rowid: row.get(7)?,
                    max_created_at: row.get::<_, i64>(8)?.max(0) as u64,
                })
            },
        )
        .optional()
        .with_context(|| format!("look up Bob task `{task_id}` in {}", database.display()))
}

thread_local! {
    /// Analytics asks for the cwd and title of every Bob session in turn; reopening the
    /// database per call would dominate a rebuild.
    static ANALYTICS_CONNECTION: std::cell::RefCell<Option<(PathBuf, Connection)>> =
        const { std::cell::RefCell::new(None) };
}

fn with_analytics_connection<T>(
    database: &Path,
    query: impl FnOnce(&Connection) -> Option<T>,
) -> Option<T> {
    ANALYTICS_CONNECTION.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_none_or(|(path, _)| path != database) {
            *slot = Some((database.to_path_buf(), open_read_only(database).ok()?));
        }
        query(&slot.as_ref()?.1)
    })
}

fn task_column(database: &Path, task_id: &str, column: &str) -> Option<String> {
    with_analytics_connection(database, |connection| {
        connection
            .query_row(
                &format!("SELECT {column} FROM tasks WHERE id = ?1"),
                [task_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .ok()
            .flatten()
            .flatten()
    })
}

/// Workspace directory of the task behind a virtual path.
pub fn session_cwd(path: &Path) -> Option<PathBuf> {
    let (database, task_id) = split_virtual_path(path)?;
    let project_id = task_column(&database, &task_id, "project_id")?;
    workspace_from_project_id(&project_id).map(PathBuf::from)
}

/// Title Bob assigned to the task behind a virtual path. Sub-agent sessions share the
/// parent's path but have no title of their own, so only the owning task answers.
pub fn session_title(path: &Path, session_id: &str) -> Option<String> {
    let (database, task_id) = split_virtual_path(path)?;
    if task_id != session_id {
        return None;
    }
    task_column(&database, &task_id, "title").filter(|title| !title.trim().is_empty())
}

fn message_timestamp(data: &Value, created_at: i64) -> u64 {
    data.pointer("/_meta/timestamp")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .unwrap_or(created_at.max(0) as u64)
}

/// Text of a message's `content`. Non-text blocks (images, structured payloads) are
/// counted rather than indexed.
fn content_text(data: &Value, diagnostics: &mut ParseDiagnostics) -> String {
    match data.get("content") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(blocks)) => blocks
            .iter()
            .filter_map(|block| match block {
                Value::String(text) => Some(text.clone()),
                Value::Object(_) => {
                    let text = block.get("text").and_then(Value::as_str);
                    if text.is_none() {
                        let kind = block.get("type").and_then(Value::as_str).unwrap_or("block");
                        diagnostics.increment_unknown_semantic(&format!("content_{kind}"));
                    }
                    text.map(str::to_string)
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Some(Value::Null) | None => String::new(),
        Some(_) => {
            diagnostics.increment_unknown_semantic("content_non_text");
            String::new()
        }
    }
}

fn json_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Discovery tolerates an unreadable database; the parser must too, or one locked or
/// mid-migration Bob database would abort indexing for every source. Ingest treats a
/// `NotFound` cause as a skipped file, and the task is retried on the next refresh.
fn skipped(error: anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("{error:#}"),
    ))
    .context("Bob task unavailable; skipped until the next refresh")
}

/// Sub-agent transcripts nest inside `spawn_subagent` results, and a sub-agent can spawn
/// its own; deeper than this is treated as data, not conversation.
const MAX_SUBAGENT_DEPTH: u8 = 4;

/// The session a batch of messages belongs to and the links every record inherits.
struct SessionScope {
    session_id: String,
    links: RecordLinks,
}

struct TaskParser<'a> {
    project: String,
    source_path: String,
    next_doc_id: &'a AtomicU64,
    turn_id: u32,
    pending_tool_calls: HashMap<String, PendingToolCall>,
    diagnostics: ParseDiagnostics,
}

impl TaskParser<'_> {
    #[allow(clippy::too_many_arguments)]
    fn record(
        &mut self,
        scope: &SessionScope,
        ts: u64,
        role: &str,
        text: String,
        tool_name: Option<String>,
        tool_input: Option<String>,
        tool_output: Option<String>,
        links: RecordLinks,
        emit: &mut impl FnMut(Record) -> Result<()>,
    ) -> Result<u64> {
        let doc_id = self.next_doc_id.fetch_add(1, Ordering::SeqCst);
        emit(Record {
            source: SourceKind::Bob,
            doc_id,
            ts,
            project: self.project.clone(),
            session_id: scope.session_id.clone(),
            turn_id: self.turn_id,
            role: role.to_string(),
            text,
            tool_name,
            tool_input,
            tool_output,
            links,
            source_path: self.source_path.clone(),
        })?;
        self.turn_id = self.turn_id.saturating_add(1);
        Ok(doc_id)
    }

    /// Emit an embedded transcript (a sub-agent's `messages`) in timestamp order.
    fn emit_transcript(
        &mut self,
        scope: &SessionScope,
        messages: &[Value],
        fallback_ts: u64,
        depth: u8,
        emit: &mut impl FnMut(Record) -> Result<()>,
    ) -> Result<()> {
        let mut ordered = messages
            .iter()
            .enumerate()
            .filter_map(|(index, message)| {
                if !message.is_object() {
                    self.diagnostics.non_object_json_lines += 1;
                    return None;
                }
                let ts = message
                    .pointer("/_meta/timestamp")
                    .and_then(Value::as_u64)
                    .filter(|value| *value > 0)
                    .unwrap_or(fallback_ts);
                Some((ts, index, message))
            })
            .collect::<Vec<_>>();
        ordered.sort_by_key(|(ts, index, _)| (*ts, *index));
        for (ts, index, message) in ordered {
            let message_id = message
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| format!("{}:{index}", scope.session_id));
            let role = message.get("role").and_then(Value::as_str).unwrap_or("");
            self.emit_message(scope, &message_id, role, message, ts, depth, emit)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_message(
        &mut self,
        scope: &SessionScope,
        message_id: &str,
        role: &str,
        data: &Value,
        ts: u64,
        depth: u8,
        emit: &mut impl FnMut(Record) -> Result<()>,
    ) -> Result<()> {
        match role {
            "user" => {
                // `_meta.mask` is what the person typed; `content` may be a whole skill
                // file expanded from a slash command.
                let text = data
                    .pointer("/_meta/mask")
                    .and_then(Value::as_str)
                    .filter(|mask| !mask.trim().is_empty())
                    .map(str::to_string)
                    .unwrap_or_else(|| content_text(data, &mut self.diagnostics));
                if text.trim().is_empty() {
                    return Ok(());
                }
                let links = RecordLinks {
                    event_id: Some(message_id.to_string()),
                    ..scope.links.clone()
                };
                self.record(scope, ts, "user", text, None, None, None, links, emit)?;
            }
            "assistant" => {
                let hidden = data
                    .pointer("/_meta/hide")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let text = content_text(data, &mut self.diagnostics);
                if !hidden && !text.trim().is_empty() {
                    let compacted = data.pointer("/_meta/compactedAt").is_some();
                    let mut links = RecordLinks {
                        event_id: Some(message_id.to_string()),
                        ..scope.links.clone()
                    };
                    if compacted {
                        links.thread_source = Some("compaction".to_string());
                    }
                    self.record(scope, ts, "assistant", text, None, None, None, links, emit)?;
                }
                let Some(calls) = data.get("toolCalls").and_then(Value::as_array) else {
                    return Ok(());
                };
                for (index, call) in calls.iter().enumerate() {
                    let call_id = call
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("{message_id}:{index}"));
                    let tool_name = call
                        .get("name")
                        .and_then(Value::as_str)
                        .filter(|name| !name.is_empty())
                        .map(str::to_string);
                    let tool_input = call
                        .get("arguments")
                        .map(json_text)
                        .filter(|text| !text.is_empty());
                    let links = RecordLinks {
                        event_id: Some(call_id.clone()),
                        parent_event_id: Some(message_id.to_string()),
                        ..scope.links.clone()
                    };
                    let doc_id = self.record(
                        scope,
                        ts,
                        "tool_use",
                        tool_input.clone().unwrap_or_default(),
                        tool_name.clone(),
                        tool_input.clone(),
                        None,
                        links.clone(),
                        emit,
                    )?;
                    let pending = super::common::pending_tool_call(
                        tool_name,
                        Some(call_id.clone()),
                        doc_id,
                        ts,
                        tool_input.as_deref(),
                        &links,
                        &scope.session_id,
                    );
                    if self.pending_tool_calls.insert(call_id, pending).is_some() {
                        self.diagnostics.duplicate_tool_calls += 1;
                    }
                }
            }
            "tool" => {
                let signature = data.pointer("/toolUsage/signature");
                let call_id = signature
                    .and_then(|signature| signature.get("id"))
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .map(str::to_string);
                let pending = call_id
                    .as_deref()
                    .and_then(|id| self.pending_tool_calls.remove(id));
                if call_id.is_some() && pending.is_none() {
                    self.diagnostics.orphan_tool_results += 1;
                }
                let tool_name = signature
                    .and_then(|signature| signature.get("name"))
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty())
                    .map(str::to_string)
                    .or_else(|| pending.and_then(|pending| pending.tool_name));
                let is_error = signature
                    .and_then(|signature| signature.get("isError"))
                    .and_then(Value::as_bool);
                // `_meta.hide` on a tool row marks an error Bob keeps out of the UI; it still
                // answers the call and is kept. A failed call with no output still gets a
                // result record so the error is visible against its `tool_use`.
                let mut text = content_text(data, &mut self.diagnostics);
                if text.trim().is_empty() && is_error == Some(true) {
                    text = "[tool error]".to_string();
                }
                if !text.trim().is_empty() {
                    let links = RecordLinks {
                        event_id: Some(message_id.to_string()),
                        parent_event_id: call_id.clone(),
                        parent_tool_use_id: call_id.clone(),
                        tool_result_is_error: is_error,
                        ..scope.links.clone()
                    };
                    self.record(
                        scope,
                        ts,
                        "tool_result",
                        text.clone(),
                        tool_name,
                        None,
                        Some(text),
                        links,
                        emit,
                    )?;
                }
                // A `spawn_subagent` result carries the child's whole transcript; the child
                // task row itself has no messages of its own.
                if depth < MAX_SUBAGENT_DEPTH
                    && let Some(messages) = data.get("messages").and_then(Value::as_array)
                    && let Some(child_id) = data
                        .pointer("/_meta/subagentId")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                {
                    let child = SessionScope {
                        session_id: child_id.to_string(),
                        links: RecordLinks {
                            parent_session_id: Some(scope.session_id.clone()),
                            thread_source: Some("subagent".to_string()),
                            conversation_kind: Some("subagent".to_string()),
                            parent_tool_use_id: call_id,
                            ..RecordLinks::default()
                        },
                    };
                    self.emit_transcript(&child, messages, ts, depth + 1, emit)?;
                }
            }
            // The per-task system prompt is configuration, not conversation.
            "system" => {}
            other => self.diagnostics.increment_unknown_semantic(other),
        }
        Ok(())
    }
}

pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    // Whole-document format: discovery pairs every change with a delete-first re-parse,
    // so `state.offset` is intentionally ignored.
    let (database, task_id) = split_virtual_path(path)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "unsupported Bob path {} (expected <database>/<task_id>)",
                path.display()
            )
        })
        .map_err(skipped)?;
    let connection = open_read_only(&database).map_err(skipped)?;
    // One read transaction: the task row and its messages come from the same snapshot,
    // so a rewrite landing mid-parse cannot pair old metadata with new rows.
    let connection = connection
        .unchecked_transaction()
        .with_context(|| format!("begin Bob read transaction for {}", database.display()))
        .map_err(skipped)?;
    let Some(task) = lookup_task(&connection, &database, &task_id).map_err(skipped)? else {
        return Ok(IndexParseOutput {
            offset: 0,
            turn_id: state.turn_id,
            legacy_turn_id: None,
            pending_tool_calls: state.pending_tool_calls,
            session_id: Some(task_id),
            diagnostics: ParseDiagnostics::default(),
            session_cwd: None,
        });
    };
    let workspace = task.workspace();
    let mut parser = TaskParser {
        project: workspace
            .as_deref()
            .map(super::common::project_from_path)
            .unwrap_or_else(|| SourceKind::Bob.label().to_string()),
        source_path: path.to_string_lossy().to_string(),
        next_doc_id,
        turn_id: state.turn_id,
        pending_tool_calls: state.pending_tool_calls,
        diagnostics: ParseDiagnostics::default(),
    };
    let scope = SessionScope {
        session_id: task.id.clone(),
        links: RecordLinks {
            parent_session_id: task.parent_id.clone(),
            thread_source: task
                .parent_id
                .as_ref()
                .map(|_| task.conversation_kind().to_string()),
            conversation_kind: Some(task.conversation_kind().to_string()),
            ..RecordLinks::default()
        },
    };

    let mut statement = connection
        .prepare(
            "SELECT rowid, id, role, data, created_at FROM messages WHERE task_id = ?1
             ORDER BY rowid",
        )
        .with_context(|| format!("prepare Bob message query for {}", database.display()))
        .map_err(skipped)?;
    let rows = statement
        .query_map([task_id.as_str()], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .with_context(|| format!("query Bob messages in {}", database.display()))
        .map_err(skipped)?;
    let mut count = 0u64;
    let mut messages = Vec::new();
    for row in rows {
        let (rowid, message_id, role, data, created_at) = row
            .with_context(|| format!("iterate Bob messages in {}", database.display()))
            .map_err(skipped)?;
        count += 1;
        match serde_json::from_str::<Value>(&data) {
            Ok(value) if value.is_object() => {
                let ts = message_timestamp(&value, created_at);
                messages.push((ts, rowid, message_id, role, value));
            }
            Ok(_) => parser.diagnostics.non_object_json_lines += 1,
            Err(_) => parser.diagnostics.malformed_json_lines += 1,
        }
    }
    // Bob rewrites a task's rows when it finishes or reopens: rowids and `created_at` are
    // reassigned and rows can change places, so only the message's own timestamp orders it.
    messages.sort_by_key(|(ts, rowid, ..)| (*ts, *rowid));
    for (ts, _, message_id, role, data) in &messages {
        parser.emit_message(&scope, message_id, role, data, *ts, 0, &mut emit)?;
    }

    Ok(IndexParseOutput {
        offset: count,
        turn_id: parser.turn_id,
        legacy_turn_id: None,
        // A task is always replayed from its first row, so nothing carries over.
        pending_tool_calls: HashMap::new(),
        session_id: Some(task.id),
        diagnostics: parser.diagnostics,
        session_cwd: workspace,
    })
}

/// Per-request token usage: every assistant message with `_meta.spend`, including the
/// assistant messages of sub-agent transcripts embedded in `spawn_subagent` results.
struct UsageCollector<'a> {
    source_path: Arc<str>,
    project: Option<String>,
    seen: &'a mut HashSet<String>,
    events: &'a mut Vec<UsageEvent>,
}

impl UsageCollector<'_> {
    fn spend_event(
        &mut self,
        message_id: &str,
        session_id: &str,
        data: &Value,
        ts: u64,
        sidechain: bool,
    ) {
        let Some(spend) = data
            .pointer("/_meta/spend")
            .filter(|spend| spend.is_object())
        else {
            return;
        };
        if !self.seen.insert(message_id.to_string()) {
            return;
        }
        // Observed convention: `input` is the full prompt including cached tokens
        // (input ≈ cacheRead + cacheWrite + fresh), like OpenAI-shaped usage.
        let usage = |key: &str| spend.get(key).and_then(Value::as_u64).unwrap_or(0);
        let input = usage("input");
        let cache_read = usage("cacheRead").min(input);
        let cache_write = usage("cacheWrite").min(input.saturating_sub(cache_read));
        let output = usage("output");
        let reasoning = usage("reasoningTokens").min(output);
        if input == 0 && output == 0 {
            return;
        }
        let cost = spend.get("cost").and_then(Value::as_f64);
        let order = self.events.len() as u64;
        self.events.push(UsageEvent {
            source: "bob",
            source_path: self.source_path.clone(),
            source_record_id: Some(message_id.to_string()),
            session_id: Some(session_id.to_string()),
            request_id: Some(message_id.to_string()),
            message_id: Some(message_id.to_string()),
            timestamp_ms: ts,
            project: self.project.clone(),
            provider: Some("ibm".to_string()),
            model: None,
            tokens: TokenBuckets {
                raw_input: input,
                uncached_input: input.saturating_sub(cache_read).saturating_sub(cache_write),
                cache_read,
                cache_write,
                cache_write_1h: 0,
                output,
                reasoning,
            },
            source_cost_usd: cost,
            // Bob prices its own requests; without a recorded cost nothing is authoritative.
            cost_authoritative: cost.is_some(),
            dedupe_confidence: "exact",
            conservative_undercount: false,
            cache_chain_excluded: true,
            sidechain,
            permission_review: false,
            source_order: order,
        });
    }

    /// Walk an embedded sub-agent transcript (and any it spawned in turn).
    fn embedded(&mut self, data: &Value, fallback_ts: u64, depth: u8) {
        if depth >= MAX_SUBAGENT_DEPTH {
            return;
        }
        let (Some(messages), Some(child_id)) = (
            data.get("messages").and_then(Value::as_array),
            data.pointer("/_meta/subagentId")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty()),
        ) else {
            return;
        };
        for (index, message) in messages.iter().enumerate() {
            let ts = message
                .pointer("/_meta/timestamp")
                .and_then(Value::as_u64)
                .filter(|value| *value > 0)
                .unwrap_or(fallback_ts);
            match message.get("role").and_then(Value::as_str) {
                Some("assistant") => {
                    let message_id = message
                        .get("id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("{child_id}:{index}"));
                    self.spend_event(&message_id, child_id, message, ts, true);
                }
                Some("tool") => self.embedded(message, ts, depth + 1),
                _ => {}
            }
        }
    }
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<Vec<UsageEvent>> {
    let connection = open_read_only(path)?;
    let source_path: Arc<str> = Arc::from(path.to_string_lossy().as_ref());
    let mut statement = connection
        .prepare(
            "SELECT m.id, m.task_id, m.role, m.data, m.created_at, t.project_id, t.task_type
             FROM messages AS m
             JOIN tasks AS t ON t.id = m.task_id
             WHERE (m.role = 'assistant' AND m.data LIKE '%\"spend\"%')
                OR (m.role = 'tool' AND m.data LIKE '%\"subagentId\"%')
             ORDER BY m.rowid",
        )
        .with_context(|| format!("prepare Bob usage query for {}", path.display()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
            ))
        })
        .with_context(|| format!("query Bob usage in {}", path.display()))?;
    let mut events = Vec::new();
    let mut seen = HashSet::new();
    for row in rows {
        let (message_id, task_id, role, data, created_at, project_id, task_type) = row?;
        let Ok(data) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        let mut collector = UsageCollector {
            source_path: source_path.clone(),
            project: project_id
                .as_deref()
                .and_then(workspace_from_project_id)
                .map(|workspace| super::common::project_from_path(&workspace)),
            seen: &mut seen,
            events: &mut events,
        };
        let ts = message_timestamp(&data, created_at);
        match role.as_str() {
            "assistant" => collector.spend_event(
                &message_id,
                &task_id,
                &data,
                ts,
                task_type.as_deref() == Some("subagent"),
            ),
            "tool" => collector.embedded(&data, ts, 0),
            _ => {}
        }
    }
    Ok(events)
}

#[cfg(test)]
pub(crate) mod fixtures {
    use rusqlite::Connection;
    use std::path::Path;

    pub const SCHEMA: &str = "
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            parent_id TEXT REFERENCES tasks(id),
            title TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'active',
            first_message TEXT,
            directory TEXT NOT NULL DEFAULT '',
            costs TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            task_type TEXT NOT NULL DEFAULT 'normal'
        );
        CREATE TABLE messages (
            id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
            role TEXT NOT NULL,
            data TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE INDEX idx_messages_task ON messages(task_id, created_at);
    ";

    pub fn create(path: &Path) -> Connection {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let connection = Connection::open(path).unwrap();
        connection
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
            .unwrap();
        connection.execute_batch(SCHEMA).unwrap();
        connection
    }

    pub fn insert_task(
        connection: &Connection,
        id: &str,
        parent_id: Option<&str>,
        task_type: &str,
        project_id: &str,
        title: &str,
        updated_at: i64,
    ) {
        connection
            .execute(
                "INSERT OR REPLACE INTO tasks (id, project_id, parent_id, title, created_at, updated_at, task_type)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6)",
                rusqlite::params![id, project_id, parent_id, title, updated_at, task_type],
            )
            .unwrap();
    }

    pub fn insert_message(
        connection: &Connection,
        id: &str,
        task_id: &str,
        role: &str,
        data: &str,
        created_at: i64,
    ) {
        connection
            .execute(
                "INSERT INTO messages (id, task_id, role, data, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![id, task_id, role, data, created_at],
            )
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn virtual_paths_round_trip() {
        let database = Path::new("/home/me/.bob/db/bob.db");
        let path = virtual_path(database, "task-1");
        assert_eq!(
            split_virtual_path(&path),
            Some((database.to_path_buf(), "task-1".to_string()))
        );
        assert!(matches_path(&path.to_string_lossy()));
        assert!(!matches_path("/home/me/.bob/db/bob.db"));
        assert!(!matches_path("/home/me/.claude/projects/x/session.jsonl"));
    }

    #[test]
    fn configured_database_paths_accept_any_file_name() {
        let _guard = crate::test_support::env_lock();
        let custom = Path::new("/srv/bob/mybob.sqlite");
        let _env =
            crate::test_support::EnvVarGuard::set_os(&[("MEMEX_BOB_DB", Some(custom.as_os_str()))]);
        assert!(is_db_path(custom));
        assert!(is_db_path(Path::new("/elsewhere/bob.db")));
        assert!(!is_db_path(Path::new("/srv/bob/other.sqlite")));
        let path = virtual_path(custom, "task-1");
        assert!(matches_path(&path.to_string_lossy()));
        assert_eq!(
            split_virtual_path(&path),
            Some((custom.to_path_buf(), "task-1".to_string()))
        );
    }

    #[test]
    fn unavailable_tasks_are_reported_as_skippable() {
        let temp = TempDir::new().unwrap();
        let is_skipped = |error: &anyhow::Error| {
            error.chain().any(|cause| {
                cause
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
            })
        };
        let mut emit = |_record| Ok(());
        // Not a virtual path at all.
        let error = parse_index_records(
            &temp.path().join("notes.jsonl"),
            IndexParseState::default(),
            &AtomicU64::new(1),
            &mut emit,
        )
        .unwrap_err();
        assert!(is_skipped(&error), "{error:#}");
        // A database that is not SQLite (locked, corrupt, or mid-migration behaves alike).
        let database = temp.path().join("bob.db");
        std::fs::write(&database, "not a database").unwrap();
        let error = parse_index_records(
            &virtual_path(&database, "task-1"),
            IndexParseState::default(),
            &AtomicU64::new(1),
            &mut emit,
        )
        .unwrap_err();
        assert!(is_skipped(&error), "{error:#}");
    }

    #[test]
    fn orders_by_timestamp_masks_prompts_and_extracts_subagent_transcripts() {
        let temp = TempDir::new().unwrap();
        let database = temp.path().join("db").join("bob.db");
        let writer = fixtures::create(&database);
        fixtures::insert_task(&writer, "task-1", None, "normal", "file:/work/repo", "T", 1);
        // A rewrite left the user row after the assistant row with a later created_at;
        // its own timestamp still says it came first.
        fixtures::insert_message(
            &writer,
            "m2",
            "task-1",
            "assistant",
            r#"{"role":"assistant","content":"on it","_meta":{"timestamp":1700000000200}}"#,
            1_700_000_900_000,
        );
        fixtures::insert_message(
            &writer,
            "m1",
            "task-1",
            "user",
            r#"{"role":"user","content":"Skill body\nlots of text","_meta":{"timestamp":1700000000100,"mask":"/pr-review 2331"}}"#,
            1_700_000_900_001,
        );
        fixtures::insert_message(
            &writer,
            "m3",
            "task-1",
            "assistant",
            r#"{"role":"assistant","content":"","_meta":{"timestamp":1700000000300},"toolCalls":[{"id":"call-1","name":"spawn_subagent","arguments":{"prompt":"count"}}]}"#,
            1_700_000_900_002,
        );
        fixtures::insert_message(
            &writer,
            "m4",
            "task-1",
            "tool",
            r#"{"role":"tool","content":"<task_result>2 files</task_result>","toolUsage":{"signature":{"id":"call-1","name":"spawn_subagent"}},"_meta":{"timestamp":1700000000900,"subagentId":"child-1","agentType":"general"},"messages":[{"role":"system","content":"You are Bob","id":"c0"},{"role":"assistant","content":"two files","id":"c2","_meta":{"timestamp":1700000000800,"spend":{"input":50,"output":5,"cacheRead":10,"cacheWrite":0,"cost":0.002}}},{"role":"user","content":"count files","id":"c1","_meta":{"timestamp":1700000000400}},{"role":"assistant","content":"","id":"c3","_meta":{"timestamp":1700000000500},"toolCalls":[{"id":"call-2","name":"execute_command","arguments":{"command":"ls"}}]},{"role":"tool","content":"a b","id":"c4","toolUsage":{"signature":{"id":"call-2","name":"execute_command"}},"_meta":{"timestamp":1700000000600,"hide":true}}]}"#,
            1_700_000_900_003,
        );

        let mut records = Vec::new();
        let parsed = parse_index_records(
            &virtual_path(&database, "task-1"),
            IndexParseState::default(),
            &AtomicU64::new(1),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert!(parsed.pending_tool_calls.is_empty());
        assert_eq!(parsed.diagnostics.orphan_tool_results, 0);
        let summary = records
            .iter()
            .map(|record| (record.session_id.as_str(), record.role.as_str(), record.ts))
            .collect::<Vec<_>>();
        assert_eq!(
            summary,
            [
                ("task-1", "user", 1_700_000_000_100),
                ("task-1", "assistant", 1_700_000_000_200),
                ("task-1", "tool_use", 1_700_000_000_300),
                ("task-1", "tool_result", 1_700_000_000_900),
                ("child-1", "user", 1_700_000_000_400),
                ("child-1", "tool_use", 1_700_000_000_500),
                ("child-1", "tool_result", 1_700_000_000_600),
                ("child-1", "assistant", 1_700_000_000_800),
            ]
        );
        assert_eq!(records[0].text, "/pr-review 2331");
        assert!(
            records
                .iter()
                .all(|record| record.source_path == records[0].source_path)
        );
        let child = &records[4];
        assert_eq!(child.links.conversation_kind.as_deref(), Some("subagent"));
        assert_eq!(child.links.thread_source.as_deref(), Some("subagent"));
        assert_eq!(child.links.parent_session_id.as_deref(), Some("task-1"));
        assert_eq!(child.links.parent_tool_use_id.as_deref(), Some("call-1"));
        assert_eq!(records[6].tool_output.as_deref(), Some("a b"));
        assert_eq!(
            records[6].links.parent_tool_use_id.as_deref(),
            Some("call-2")
        );

        // Usage reaches into the embedded transcript and attributes it to the child.
        let events = parse_usage_file(&database).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].session_id.as_deref(), Some("child-1"));
        assert_eq!(events[0].message_id.as_deref(), Some("c2"));
        assert_eq!(events[0].timestamp_ms, 1_700_000_000_800);
        assert_eq!(events[0].tokens.raw_input, 50);
        assert_eq!(events[0].tokens.uncached_input, 40);
        assert!(events[0].sidechain);
        assert_eq!(events[0].source_cost_usd, Some(0.002));
    }

    #[test]
    fn failed_tool_calls_without_output_still_get_a_result() {
        let temp = TempDir::new().unwrap();
        let database = temp.path().join("db").join("bob.db");
        let writer = fixtures::create(&database);
        fixtures::insert_task(&writer, "task-1", None, "normal", "file:/work/repo", "T", 1);
        fixtures::insert_message(
            &writer,
            "m1",
            "task-1",
            "assistant",
            r#"{"role":"assistant","content":"","_meta":{"timestamp":1},"toolCalls":[{"id":"call-1","name":"apply_diff","arguments":{}}]}"#,
            1,
        );
        fixtures::insert_message(
            &writer,
            "m2",
            "task-1",
            "tool",
            r#"{"role":"tool","content":[{"type":"image","data":"..."}],"toolUsage":{"signature":{"id":"call-1","name":"apply_diff","isError":true}},"_meta":{"timestamp":2,"hide":true}}"#,
            2,
        );
        let mut records = Vec::new();
        let parsed = parse_index_records(
            &virtual_path(&database, "task-1"),
            IndexParseState::default(),
            &AtomicU64::new(1),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].role, "tool_result");
        assert_eq!(records[1].text, "[tool error]");
        assert_eq!(records[1].links.tool_result_is_error, Some(true));
        assert_eq!(
            parsed
                .diagnostics
                .unknown_semantic_types
                .get("content_image"),
            Some(&1)
        );
        assert!(parsed.pending_tool_calls.is_empty());
    }

    #[test]
    fn workspace_from_file_urls() {
        assert_eq!(
            workspace_from_project_id("file:/Users/me/dev/repo").as_deref(),
            Some("/Users/me/dev/repo")
        );
        assert_eq!(
            workspace_from_project_id("file:///Users/me/my%20repo").as_deref(),
            Some("/Users/me/my repo")
        );
        assert_eq!(workspace_from_project_id("vscode-remote://x"), None);
        // A percent sign right before a multi-byte character must not slice mid-char.
        assert_eq!(
            workspace_from_project_id("file:/Users/me/100%€uro").as_deref(),
            Some("/Users/me/100%€uro")
        );
        assert_eq!(
            workspace_from_project_id("file:/Users/me/x%2").as_deref(),
            Some("/Users/me/x%2")
        );
    }

    #[test]
    fn configured_paths_expand_home() {
        let _guard = crate::test_support::env_lock();
        let _env = crate::test_support::EnvVarGuard::set(&[(
            "MEMEX_BOB_DB",
            Some("~/bob-a.db, /srv/bob-b.db"),
        )]);
        let paths = database_paths();
        assert_eq!(paths[0], super::super::common::home().join("bob-a.db"));
        assert_eq!(paths[1], Path::new("/srv/bob-b.db"));
    }

    #[test]
    fn parses_task_messages_tools_and_usage() {
        let temp = TempDir::new().unwrap();
        let database = temp.path().join("db").join("bob.db");
        let writer = fixtures::create(&database);
        fixtures::insert_task(
            &writer,
            "task-1",
            None,
            "normal",
            "file:/work/repo",
            "Fix the build",
            1_700_000_000_500,
        );
        fixtures::insert_task(
            &writer,
            "task-2",
            Some("task-1"),
            "subagent",
            "file:/work/repo",
            "worker",
            1_700_000_000_600,
        );
        fixtures::insert_message(
            &writer,
            "m0",
            "task-1",
            "system",
            r#"{"role":"system","content":"You are Bob"}"#,
            1_700_000_000_000,
        );
        fixtures::insert_message(
            &writer,
            "m1",
            "task-1",
            "user",
            r#"{"role":"user","content":"hello","_meta":{"timestamp":1700000000001}}"#,
            1_700_000_000_100,
        );
        fixtures::insert_message(
            &writer,
            "m2",
            "task-1",
            "assistant",
            r#"{"role":"assistant","content":"Calling tools","_meta":{"hide":true,"timestamp":1700000000002,"spend":{"input":100,"output":20,"cacheRead":40,"cacheWrite":5,"cost":0.0125,"reasoningTokens":7}},"toolCalls":[{"id":"call-1","name":"execute_command","arguments":{"command":"pwd"}}]}"#,
            1_700_000_000_200,
        );
        fixtures::insert_message(
            &writer,
            "m3",
            "task-1",
            "tool",
            r#"{"role":"tool","content":"/work/repo","toolUsage":{"signature":{"id":"call-1","name":"execute_command","isError":false}},"_meta":{"timestamp":1700000000003}}"#,
            1_700_000_000_300,
        );
        fixtures::insert_message(
            &writer,
            "m4",
            "task-1",
            "assistant",
            r#"{"role":"assistant","content":"done","_meta":{"timestamp":1700000000004,"spend":{"input":10,"output":2,"cacheRead":0,"cacheWrite":0,"cost":0.001}}}"#,
            1_700_000_000_400,
        );
        fixtures::insert_message(
            &writer,
            "m5",
            "task-2",
            "user",
            r#"{"role":"user","content":"count lines"}"#,
            1_700_000_000_500,
        );

        let tasks = enumerate_tasks(&database).unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].id, "task-1");
        assert_eq!(tasks[0].message_count, 5);
        assert_eq!(tasks[0].workspace().as_deref(), Some("/work/repo"));
        assert_ne!(tasks[0].fingerprint(), tasks[1].fingerprint());

        let path = virtual_path(&database, "task-1");
        let mut records = Vec::new();
        let parsed = parse_index_records(
            &path,
            IndexParseState::default(),
            &AtomicU64::new(1),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(parsed.session_id.as_deref(), Some("task-1"));
        assert_eq!(parsed.session_cwd.as_deref(), Some("/work/repo"));
        assert!(parsed.pending_tool_calls.is_empty());
        assert_eq!(parsed.diagnostics.orphan_tool_results, 0);
        let roles = records
            .iter()
            .map(|record| record.role.as_str())
            .collect::<Vec<_>>();
        assert_eq!(roles, ["user", "tool_use", "tool_result", "assistant"]);
        assert!(records.iter().all(|record| record.project == "repo"));
        assert!(
            records
                .iter()
                .all(|record| record.source_path == path.to_string_lossy())
        );
        assert_eq!(records[0].ts, 1_700_000_000_001);
        assert_eq!(records[0].links.conversation_kind.as_deref(), Some("main"));
        assert_eq!(records[1].tool_name.as_deref(), Some("execute_command"));
        assert_eq!(
            records[1].tool_input.as_deref(),
            Some(r#"{"command":"pwd"}"#)
        );
        assert_eq!(
            records[2].links.parent_tool_use_id.as_deref(),
            Some("call-1")
        );
        assert_eq!(records[2].tool_output.as_deref(), Some("/work/repo"));
        assert_eq!(records[2].links.tool_result_is_error, Some(false));
        assert_eq!(records[3].text, "done");
        assert_eq!(session_cwd(&path).as_deref(), Some(Path::new("/work/repo")));
        assert_eq!(
            session_title(&path, "task-1").as_deref(),
            Some("Fix the build")
        );
        assert_eq!(session_title(&path, "child-1"), None);

        let mut subagent = Vec::new();
        parse_index_records(
            &virtual_path(&database, "task-2"),
            IndexParseState::default(),
            &AtomicU64::new(1),
            |record| {
                subagent.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(subagent.len(), 1);
        assert_eq!(
            subagent[0].links.conversation_kind.as_deref(),
            Some("subagent")
        );
        assert_eq!(
            subagent[0].links.parent_session_id.as_deref(),
            Some("task-1")
        );

        let events = parse_usage_file(&database).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].source, "bob");
        assert_eq!(events[0].session_id.as_deref(), Some("task-1"));
        assert_eq!(events[0].project.as_deref(), Some("repo"));
        assert_eq!(events[0].tokens.raw_input, 100);
        assert_eq!(events[0].tokens.uncached_input, 55);
        assert_eq!(events[0].tokens.cache_read, 40);
        assert_eq!(events[0].tokens.cache_write, 5);
        assert_eq!(events[0].tokens.output, 20);
        assert_eq!(events[0].tokens.reasoning, 7);
        assert_eq!(events[0].source_cost_usd, Some(0.0125));
        assert_eq!(events[0].timestamp_ms, 1_700_000_000_002);
    }
}
