//! goose sessions, stored as rows in one SQLite database.
//!
//! Since 1.10 goose keeps every conversation in a single store at
//! `<data dir>/sessions/sessions.db` (WAL) instead of per-session JSONL files.
//! Sessions live in `sessions` (`working_dir`, `session_type`,
//! `parent_session_id`), turns in `messages` (`content_json` holds the typed
//! content blocks, `metadata_json` the visibility flags and per-request usage),
//! and one row per model request in `usage_ledger`.
//!
//! goose rewrites a session's message rows in place when a conversation is
//! edited, truncated, or compacted, and row ids carry no stable order: the
//! store reads back `(created_timestamp, id)`. Ingestion therefore replaces a
//! whole session at a time from a content fingerprint, like KiloCode.
//!
//! `input_tokens` in `usage_ledger` is the total prompt **including** the cache
//! read and cache write subsets (documented on `Usage` in
//! `goose-provider-types/src/conversation/token_usage.rs`). Memex keeps those
//! buckets disjoint, so the usage projection subtracts them; counting the
//! reported input as-is would bill every cached token twice.

use super::{
    ConversationKind, IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions,
    SourceFile, UsageDependency, UsageParseOutput,
};
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::{Context, Result, anyhow};
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 1,
    index: 1,
    usage: 1,
};

/// Session kinds goose never shows to the user: `goose configure`, recipes, and
/// other CLI plumbing record their own scratch conversations as `hidden`.
/// Everything else (`user`, `scheduled`, `sub_agent`, `terminal`, `gateway`,
/// `acp`) is a conversation someone can open again.
const INTERNAL_SESSION_TYPE: &str = "hidden";

pub fn matches_path(path: &str) -> bool {
    split_virtual_path(Path::new(path)).is_some() || is_database_path(Path::new(path))
}

/// `sessions.db` inside a `sessions` directory, which is where goose puts it.
fn is_database_path(path: &Path) -> bool {
    path.file_name().is_some_and(|name| name == "sessions.db")
        && path
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == "sessions")
}

/// Encode session ids into a single safe component, including slashes and dots.
pub fn virtual_path(database: &Path, session_id: &str) -> PathBuf {
    let encoded: String = session_id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let directory = database.parent().unwrap_or(database);
    directory.join(format!("goose-session-{encoded}"))
}

pub fn split_virtual_path(path: &Path) -> Option<(PathBuf, String)> {
    let encoded = path.file_name()?.to_str()?.strip_prefix("goose-session-")?;
    let directory = path.parent()?;
    if directory.file_name()? != "sessions" || !encoded.len().is_multiple_of(2) {
        return None;
    }
    let bytes = encoded
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok())
        .collect::<Option<Vec<_>>>()?;
    Some((
        directory.join("sessions.db"),
        String::from_utf8(bytes).ok()?,
    ))
}

/// The `sessions` directory of every goose data root.
///
/// goose 1.55 writes `$XDG_DATA_HOME/goose` on Linux and macOS, while the
/// current sources resolve the platform data directory through etcetera
/// (`~/Library/Application Support/Block/goose` on macOS,
/// `%APPDATA%\Block\goose\data` on Windows). Both layouts are real, so both are
/// offered and the ones without a store are skipped. `GOOSE_PATH_ROOT`, goose's
/// own override, wins everywhere.
pub fn roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(path_root) = std::env::var_os("GOOSE_PATH_ROOT")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
    {
        roots.push(path_root.join("data").join("sessions"));
    } else {
        roots.push(default_xdg_root().join("sessions"));
        roots.push(native_data_root().join("sessions"));
    }
    let mut roots: Vec<PathBuf> = roots
        .into_iter()
        .filter(|root| !root.as_os_str().is_empty())
        .collect();
    roots.sort();
    roots.dedup();
    roots
}

fn default_xdg_root() -> PathBuf {
    std::env::var_os("XDG_DATA_HOME")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| super::common::home().join(".local/share"))
        .join("goose")
}

fn native_data_root() -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        super::common::home().join("Library/Application Support/Block/goose")
    }
    #[cfg(target_os = "windows")]
    {
        std::env::var_os("APPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| super::common::home().join("AppData/Roaming"))
            .join("Block/goose/data")
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        // etcetera's XDG strategy is the same directory as `default_xdg_root`
        // on Linux, so the two candidates collapse into one.
        default_xdg_root()
    }
}

/// The session database under each root, when it exists.
pub fn db_paths() -> Vec<PathBuf> {
    db_paths_for(&roots())
}

fn db_paths_for(roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = roots
        .iter()
        .map(|root| root.join("sessions.db"))
        .filter(|path| path.is_file())
        .collect();
    paths.sort();
    paths.dedup();
    paths
}

pub fn discover() -> Vec<SourceFile> {
    discover_from_roots(&roots())
}

pub fn discover_from_roots(roots: &[PathBuf]) -> Vec<SourceFile> {
    db_paths_for(roots)
        .into_iter()
        .map(|path| SourceFile {
            source: SourceKind::Goose,
            path,
        })
        .collect()
}

pub fn usage_files() -> Vec<PathBuf> {
    db_paths()
}

/// Directories holding the session databases: the watcher's narrowest goose
/// roots, clear of the model catalog and MCP app churn beside them.
pub fn db_dirs() -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = roots()
        .into_iter()
        .filter(|root| root.join("sessions.db").is_file())
        .collect();
    dirs.sort();
    dirs.dedup();
    dirs
}

fn open_readonly(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open goose database {}", path.display()))?;
    // The live store is written while we read, and an idle writer keeps the
    // lock long enough to matter; never block it or race it long.
    let _ = conn.busy_timeout(std::time::Duration::from_millis(500));
    Ok(conn)
}

fn table_names(conn: &Connection) -> Result<std::collections::HashSet<String>> {
    let mut statement = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
    Ok(statement
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(Result::ok)
        .collect())
}

pub(crate) struct SessionFingerprint {
    pub id: String,
    pub fingerprint: String,
    /// Number of projected content blocks, so progress reports turns rather
    /// than SQLite pages.
    pub size: u64,
}

/// The session rows a fingerprint covers: labels, hierarchy, and working
/// directory feed ingestion, so a rename or move must replay the session.
fn session_metadata(conn: &Connection, id: &str) -> Result<Value> {
    conn.query_row(
        "SELECT id, session_type, parent_session_id, working_dir, name \
         FROM sessions WHERE id = ?1",
        [id],
        |row| {
            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "session_type": row.get::<_, Option<String>>(1)?,
                "parent_session_id": row.get::<_, Option<String>>(2)?,
                "working_dir": row.get::<_, Option<String>>(3)?,
                "name": row.get::<_, Option<String>>(4)?,
            }))
        },
    )
    .with_context(|| format!("read goose session {id}"))
}

/// Hash the conversation content of every session in one SQLite snapshot.
/// Ledger-only writes (a request finished, a cost landed) must not invalidate
/// transcript records, and edits need not move any timestamp, so the hash
/// covers content rather than file metadata.
pub(crate) fn enumerate_sessions(database: &Path) -> Result<Vec<SessionFingerprint>> {
    let conn = open_readonly(database)?;
    let transaction = conn.unchecked_transaction()?;
    let tables = table_names(&transaction)?;
    if !(tables.contains("sessions") && tables.contains("messages")) {
        return Err(anyhow!(
            "goose database {} has no sessions/messages tables",
            database.display()
        ));
    }
    let ids = {
        let mut statement = transaction.prepare(
            "SELECT id FROM sessions \
             WHERE COALESCE(session_type, 'user') <> ?1 \
               AND EXISTS (SELECT 1 FROM messages WHERE session_id = sessions.id) \
             ORDER BY id",
        )?;
        statement
            .query_map([INTERNAL_SESSION_TYPE], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let mut messages_statement = transaction.prepare(
        "SELECT role, content_json, created_timestamp, COALESCE(metadata_json, '{}') \
         FROM messages WHERE session_id = ?1 ORDER BY created_timestamp, id",
    )?;
    let mut result = Vec::with_capacity(ids.len());
    for id in ids {
        let mut hash = Sha256::new();
        hash.update(serde_json::to_vec(&session_metadata(&transaction, &id)?)?);
        let mut size = 0;
        let mut rows = messages_statement.query([&id])?;
        while let Some(row) = rows.next()? {
            let role = row.get::<_, Option<String>>(0)?.unwrap_or_default();
            let content = row.get::<_, Option<String>>(1)?.unwrap_or_default();
            let created = row.get::<_, Option<i64>>(2)?.unwrap_or_default();
            let metadata = row.get::<_, Option<String>>(3)?.unwrap_or_default();
            hash.update([0xff]);
            hash.update(role.as_bytes());
            hash.update(created.to_le_bytes());
            hash.update(content.as_bytes());
            // Metadata is read for the visibility predicate but never hashed:
            // goose rewrites its per-request usage on every reply, and that
            // must not look like a conversation edit.
            let metadata: Value = serde_json::from_str(&metadata).unwrap_or(Value::Null);
            if !is_user_visible(&metadata) {
                continue;
            }
            size += serde_json::from_str::<Value>(&content)
                .ok()
                .and_then(|value| {
                    value.as_array().map(|blocks| {
                        blocks.iter().filter(|block| projects_block(block)).count() as u64
                    })
                })
                .unwrap_or_default();
        }
        result.push(SessionFingerprint {
            id,
            fingerprint: format!("{:x}", hash.finalize()),
            size,
        });
    }
    drop(messages_statement);
    transaction.commit()?;
    Ok(result)
}

struct SessionRow {
    id: String,
    session_type: String,
    parent_session_id: Option<String>,
    working_dir: Option<String>,
}

/// One message row: the columns every projection needs, read once.
struct MessageRow {
    role: String,
    content: String,
    metadata: String,
    created: i64,
}

pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    // Each changed session is replaced, so state.offset is intentionally ignored.
    let source_path = path.to_string_lossy().to_string();
    let virtual_session = split_virtual_path(path);
    let database = virtual_session
        .as_ref()
        .map_or(path, |(database, _)| database.as_path());
    let selected_session = virtual_session.as_ref().map(|(_, id)| id.as_str());
    let mut diagnostics = ParseDiagnostics::default();
    let conn = open_readonly(database)?;
    // One snapshot across sessions and messages: goose commits rows while we read.
    let transaction = conn.unchecked_transaction()?;
    let tables = table_names(&transaction)?;
    if !(tables.contains("sessions") && tables.contains("messages")) {
        return Err(anyhow!(
            "goose database {} has no sessions/messages tables",
            path.display()
        ));
    }
    let mut sessions_statement = transaction
        .prepare(
            "SELECT id, COALESCE(session_type, 'user'), parent_session_id, working_dir \
             FROM sessions \
             WHERE COALESCE(session_type, 'user') <> ?1 \
               AND (?2 IS NULL OR id = ?2) ORDER BY created_at, id",
        )
        .with_context(|| format!("query goose sessions in {}", path.display()))?;
    let sessions = sessions_statement
        .query_map(
            rusqlite::params![INTERNAL_SESSION_TYPE, selected_session],
            |row| {
                Ok(SessionRow {
                    id: row.get(0)?,
                    session_type: row.get(1)?,
                    parent_session_id: row.get(2)?,
                    working_dir: row.get(3)?,
                })
            },
        )
        .with_context(|| format!("read goose sessions in {}", path.display()))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut messages_statement = transaction
        .prepare(
            "SELECT role, content_json, COALESCE(metadata_json, '{}'), created_timestamp \
             FROM messages WHERE session_id = ?1 ORDER BY created_timestamp, id",
        )
        .with_context(|| format!("prepare goose messages in {}", path.display()))?;

    let mut row_count = 0;
    let mut turn_id = state.turn_id;
    let mut session_cwd: Option<String> = None;
    for session in &sessions {
        let working_dir = session
            .working_dir
            .as_deref()
            .map(str::trim)
            .filter(|dir| !dir.is_empty());
        if session_cwd.is_none() {
            session_cwd = working_dir.map(str::to_string);
        }
        let project = working_dir.map(super::common::project_from_path);
        let parent = session
            .parent_session_id
            .as_deref()
            .map(str::trim)
            .filter(|parent| !parent.is_empty());
        let mut links = RecordLinks::default();
        if let Some(parent) = parent {
            links.parent_session_id = Some(parent.to_string());
            links.conversation_kind = Some(ConversationKind::Subagent.as_str().to_string());
        } else if session.session_type == "sub_agent" {
            // A sub-agent keeps its parent in the same column, but a store
            // written by an older version may have lost it; the kind still
            // tells a browser this was not a conversation someone started.
            links.conversation_kind = Some(ConversationKind::Subagent.as_str().to_string());
        } else {
            links.conversation_kind = Some(ConversationKind::Main.as_str().to_string());
        }
        let rows = messages_statement
            .query_map([session.id.as_str()], |row| {
                Ok(MessageRow {
                    role: row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                    content: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                    metadata: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                    created: row.get::<_, Option<i64>>(3)?.unwrap_or_default(),
                })
            })
            .with_context(|| format!("read goose messages in {}", path.display()))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for message in rows {
            let metadata: Value = match serde_json::from_str(&message.metadata) {
                Ok(value) => value,
                Err(_) => {
                    diagnostics.malformed_json_lines += 1;
                    Value::Null
                }
            };
            // goose records per-turn scaffolding (`<turn-context>` with the
            // clock and working directory) and extension chatter as messages
            // the user never saw. Indexing them would answer searches with
            // prompts nobody typed.
            if !is_user_visible(&metadata) {
                continue;
            }
            let Ok(blocks) = serde_json::from_str::<Value>(&message.content) else {
                diagnostics.malformed_json_lines += 1;
                continue;
            };
            let Some(blocks) = blocks.as_array() else {
                diagnostics.non_object_json_lines += 1;
                continue;
            };
            let ts = message.created.max(0) as u64 * 1000;
            for block in blocks {
                let block_type = block.get("type").and_then(Value::as_str).unwrap_or("");
                let base = || Record {
                    source: SourceKind::Goose,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts,
                    project: project
                        .clone()
                        .unwrap_or_else(|| SourceKind::Goose.label().to_string()),
                    session_id: session.id.clone(),
                    turn_id,
                    role: String::new(),
                    text: String::new(),
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: links.clone(),
                    source_path: source_path.clone(),
                };
                match block_type {
                    "text" => {
                        let Some(text) = block.get("text").and_then(Value::as_str) else {
                            diagnostics.increment_unknown_semantic("text_without_text");
                            continue;
                        };
                        if text.trim().is_empty() {
                            continue;
                        }
                        row_count += 1;
                        emit(Record {
                            role: message.role.clone(),
                            text: text.to_string(),
                            ..base()
                        })?;
                        turn_id += 1;
                    }
                    "thinking" => {
                        let thinking = block
                            .get("thinking")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        // An encrypted provider (Anthropic) keeps only a
                        // signature; there is nothing to index for it.
                        if thinking.trim().is_empty() {
                            if block
                                .get("signature")
                                .and_then(Value::as_str)
                                .is_some_and(|signature| !signature.is_empty())
                            {
                                diagnostics.encrypted_reasoning_dropped += 1;
                            }
                            continue;
                        }
                        row_count += 1;
                        if !include_reasoning {
                            continue;
                        }
                        emit(Record {
                            role: "reasoning".to_string(),
                            text: thinking.to_string(),
                            ..base()
                        })?;
                        turn_id += 1;
                    }
                    "toolRequest" => {
                        let call_id = block
                            .get("id")
                            .and_then(Value::as_str)
                            .filter(|id| !id.is_empty())
                            .map(str::to_string);
                        let call = block.get("toolCall");
                        let tool_name = call
                            .and_then(|call| call.pointer("/value/name"))
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        let arguments = call
                            .and_then(|call| call.pointer("/value/arguments"))
                            .and_then(value_to_string);
                        let failure = call
                            .filter(|call| {
                                call.get("status").and_then(Value::as_str) != Some("success")
                            })
                            .and_then(|call| call.get("error"))
                            .and_then(value_to_string);
                        let mut tool_links = links.clone();
                        tool_links.event_id = call_id.clone();
                        row_count += 1;
                        match (tool_name, failure) {
                            (Some(name), _) => emit(Record {
                                role: "tool_use".to_string(),
                                text: arguments.clone().unwrap_or_default(),
                                tool_name: Some(name),
                                tool_input: arguments,
                                links: tool_links,
                                ..base()
                            })?,
                            // A request the provider could not parse never ran a
                            // tool; keep the diagnostic visible as its own record.
                            (None, Some(error)) => emit(Record {
                                role: "tool_result".to_string(),
                                text: error.clone(),
                                tool_output: Some(error),
                                links: RecordLinks {
                                    tool_result_is_error: Some(true),
                                    ..tool_links
                                },
                                ..base()
                            })?,
                            (None, None) => {
                                diagnostics.increment_unknown_semantic("tool_request_without_call");
                                continue;
                            }
                        }
                        turn_id += 1;
                    }
                    "toolResponse" => {
                        let result_id = block
                            .get("id")
                            .and_then(Value::as_str)
                            .filter(|id| !id.is_empty())
                            .map(str::to_string);
                        let result = block.get("toolResult");
                        let is_error = result.is_some_and(|result| {
                            result.get("status").and_then(Value::as_str) != Some("success")
                        });
                        let Some(text) = result.and_then(tool_result_text) else {
                            diagnostics.increment_unknown_semantic("tool_response_without_body");
                            continue;
                        };
                        let mut result_links = links.clone();
                        if let Some(result_id) = &result_id {
                            result_links.event_id = Some(format!("{result_id}:result"));
                            result_links.parent_event_id = Some(result_id.clone());
                            result_links.parent_tool_use_id = Some(result_id.clone());
                        }
                        result_links.tool_result_is_error = Some(is_error);
                        row_count += 1;
                        emit(Record {
                            role: "tool_result".to_string(),
                            text: text.clone(),
                            tool_output: Some(text),
                            links: result_links,
                            ..base()
                        })?;
                        turn_id += 1;
                    }
                    "image" => {
                        let mime = block
                            .get("mimeType")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown");
                        row_count += 1;
                        emit(Record {
                            role: message.role.clone(),
                            text: format!("[image: {mime}]"),
                            ..base()
                        })?;
                        turn_id += 1;
                    }
                    "document" => {
                        let mime = block
                            .get("mimeType")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown");
                        let name = block
                            .get("name")
                            .and_then(Value::as_str)
                            .filter(|name| !name.is_empty());
                        let label = match name {
                            Some(name) => format!("[document: {name} ({mime})]"),
                            None => format!("[document: {mime}]"),
                        };
                        row_count += 1;
                        emit(Record {
                            role: message.role.clone(),
                            text: label,
                            ..base()
                        })?;
                        turn_id += 1;
                    }
                    // Approval prompts and extension notices are UI state, not
                    // conversation: they are re-created on every turn.
                    "toolConfirmationRequest" | "actionRequired" | "systemNotification" => {
                        diagnostics.increment_unknown_semantic(block_type);
                    }
                    "error" => {
                        let message = block
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("tool error");
                        row_count += 1;
                        emit(Record {
                            role: "tool_result".to_string(),
                            text: message.to_string(),
                            links: RecordLinks {
                                tool_result_is_error: Some(true),
                                ..links.clone()
                            },
                            ..base()
                        })?;
                        turn_id += 1;
                    }
                    other => diagnostics.increment_unknown_top_level(other),
                }
            }
        }
    }
    drop(messages_statement);
    drop(sessions_statement);
    transaction.commit()?;
    Ok(IndexParseOutput {
        offset: row_count,
        turn_id,
        legacy_turn_id: None,
        pending_tool_calls: state.pending_tool_calls,
        session_id: selected_session.map(str::to_owned),
        diagnostics,
        session_cwd,
    })
}

/// goose defaults both visibility flags to true; a store written before the
/// flags existed has no `metadata_json` at all, so absence means visible.
fn is_user_visible(metadata: &Value) -> bool {
    let visible = |key: &str| metadata.get(key).and_then(Value::as_bool);
    if metadata
        .get("turnContext")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return false;
    }
    visible("userVisible").unwrap_or(true)
}

/// Whether one content block becomes a Memex record.
///
/// Enumeration counts with this predicate so a session's recorded size always
/// equals the parser's offset. The two must not drift: the offset guards
/// against replaying a session whose recorded size shrank, so a block the
/// parser emits but enumeration does not count would reschedule it forever.
fn projects_block(block: &Value) -> bool {
    match block.get("type").and_then(Value::as_str).unwrap_or("") {
        "text" => block
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.trim().is_empty()),
        "thinking" => block
            .get("thinking")
            .and_then(Value::as_str)
            .is_some_and(|thinking| !thinking.trim().is_empty()),
        "toolRequest" => {
            let call = block.get("toolCall");
            let named = call
                .and_then(|call| call.pointer("/value/name"))
                .and_then(Value::as_str)
                .is_some();
            let failed = call
                .filter(|call| call.get("status").and_then(Value::as_str) != Some("success"))
                .and_then(|call| call.get("error"))
                .and_then(value_to_string)
                .is_some();
            named || failed
        }
        "toolResponse" => block.get("toolResult").and_then(tool_result_text).is_some(),
        "image" | "document" | "error" => true,
        _ => false,
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => serde_json::to_string(other).ok(),
    }
}

/// Flatten one `toolResult` envelope into the text a transcript reader expects:
/// the joined text blocks on success, the error body otherwise.
fn tool_result_text(result: &Value) -> Option<String> {
    let success = result.get("status").and_then(Value::as_str) == Some("success");
    if !success {
        return result
            .get("error")
            .and_then(value_to_string)
            .or_else(|| Some("[tool error]".to_string()));
    }
    let value = result.get("value")?;
    if let Some(content) = value.get("content").and_then(Value::as_array) {
        let text = content
            .iter()
            .filter_map(|block| {
                block
                    .get("text")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| {
                        block
                            .get("mimeType")
                            .and_then(Value::as_str)
                            .map(|mime| format!("[{mime}]"))
                    })
            })
            .collect::<Vec<_>>()
            .join("\n");
        if !text.is_empty() {
            return Some(text);
        }
    }
    // Structured MCP results (or an empty text body) still carry meaning.
    value
        .get("structuredContent")
        .and_then(value_to_string)
        .or_else(|| value_to_string(value))
}

/// Token buckets from one ledger row.
///
/// `input_tokens` includes the cache subsets, so the uncached remainder is what
/// Memex prices at the full input rate.
fn ledger_buckets(input: u64, cache_read: u64, cache_write: u64, output: u64) -> TokenBuckets {
    let cache_read = cache_read.min(input);
    let cache_write = cache_write.min(input - cache_read);
    TokenBuckets {
        raw_input: input,
        uncached_input: input - cache_read - cache_write,
        cache_read,
        cache_write,
        cache_write_1h: 0,
        output,
        reasoning: 0,
    }
}

struct LedgerRow {
    id: i64,
    session_id: String,
    created: i64,
    model: Option<String>,
    input: i64,
    output: i64,
    cache_read: i64,
    cache_write: i64,
    cost: Option<f64>,
    cost_source: Option<String>,
    provider: Option<String>,
    working_dir: Option<String>,
    parent_session_id: Option<String>,
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<UsageParseOutput> {
    let mut wal = path.as_os_str().to_os_string();
    wal.push("-wal");
    let wal = PathBuf::from(wal);
    let wal_before = UsageDependency::from_path_or_absent(&wal);
    let conn = open_readonly(path)?;
    let transaction = conn.unchecked_transaction()?;
    let tables = table_names(&transaction)?;
    let source_path: Arc<str> = Arc::from(path.to_string_lossy().to_string());
    let mut events = Vec::new();
    if tables.contains("usage_ledger") {
        let mut statement = transaction
            .prepare(
                "SELECT l.id, l.session_id, l.created_timestamp, l.model, \
                        l.input_tokens, l.output_tokens, l.cache_read_tokens, \
                        l.cache_write_tokens, l.cost, l.cost_source, \
                        s.provider_name, s.working_dir, s.parent_session_id \
                 FROM usage_ledger l JOIN sessions s ON s.id = l.session_id \
                 WHERE COALESCE(s.session_type, 'user') <> ?1 \
                 ORDER BY l.created_timestamp, l.id",
            )
            .with_context(|| format!("query goose usage in {}", path.display()))?;
        let rows = statement
            .query_map([INTERNAL_SESSION_TYPE], |row| {
                Ok(LedgerRow {
                    id: row.get(0)?,
                    session_id: row.get(1)?,
                    created: row.get::<_, Option<i64>>(2)?.unwrap_or_default(),
                    model: row.get(3)?,
                    input: row.get::<_, Option<i64>>(4)?.unwrap_or_default(),
                    output: row.get::<_, Option<i64>>(5)?.unwrap_or_default(),
                    cache_read: row.get::<_, Option<i64>>(6)?.unwrap_or_default(),
                    cache_write: row.get::<_, Option<i64>>(7)?.unwrap_or_default(),
                    cost: row.get(8)?,
                    cost_source: row.get(9)?,
                    provider: row.get(10)?,
                    working_dir: row.get(11)?,
                    parent_session_id: row.get(12)?,
                })
            })
            .context("read goose usage")?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for (order, row) in rows.into_iter().enumerate() {
            let tokens = ledger_buckets(
                row.input.max(0) as u64,
                row.cache_read.max(0) as u64,
                row.cache_write.max(0) as u64,
                row.output.max(0) as u64,
            );
            if tokens.additive_total() == 0 {
                continue;
            }
            let cost_source = row.cost_source.as_deref();
            events.push(UsageEvent {
                source: "goose",
                source_path: source_path.clone(),
                source_record_id: Some(row.id.to_string()),
                session_id: Some(row.session_id.clone()),
                request_id: None,
                message_id: Some(row.id.to_string()),
                timestamp_ms: row.created.max(0) as u64 * 1000,
                project: row
                    .working_dir
                    .as_deref()
                    .map(str::trim)
                    .filter(|dir| !dir.is_empty())
                    .map(super::common::project_from_path),
                provider: row.provider.filter(|provider| !provider.is_empty()),
                model: row.model.filter(|model| !model.is_empty()),
                tokens,
                credits: None,
                token_usage_available: true,
                source_cost_usd: row.cost,
                cost_authoritative: cost_source == Some("provider_reported"),
                dedupe_confidence: "exact",
                conservative_undercount: false,
                cache_chain_excluded: false,
                permission_review: false,
                sidechain: row
                    .parent_session_id
                    .as_deref()
                    .is_some_and(|parent| !parent.is_empty()),
                source_order: order as u64,
            });
        }
    }
    // Keep the read on one SQLite snapshot, and never cache a read that raced
    // a WAL commit.
    transaction.commit()?;
    let wal_after = UsageDependency::from_path_or_absent(&wal);
    Ok(UsageParseOutput {
        events,
        cacheable: wal_before == wal_after,
        deps: vec![wal_after],
    })
}

fn session_database(path: &Path) -> PathBuf {
    split_virtual_path(path).map_or_else(|| path.to_path_buf(), |(database, _)| database)
}

/// Working directory of one session, for analytics and session metadata.
pub fn session_cwd(path: &Path, session_id: &str) -> Option<PathBuf> {
    let conn = open_readonly(&session_database(path)).ok()?;
    let working_dir: Option<String> = conn
        .query_row(
            "SELECT working_dir FROM sessions WHERE id = ?1",
            [session_id],
            |row| row.get(0),
        )
        .ok()?;
    working_dir
        .map(|dir| dir.trim().to_string())
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
}

/// The name goose shows for a session: user-set, or generated from the first
/// exchange. Empty until goose names the conversation.
pub fn session_title(path: &Path, session_id: &str) -> Option<String> {
    let conn = open_readonly(&session_database(path)).ok()?;
    let name: Option<String> = conn
        .query_row(
            "SELECT name FROM sessions WHERE id = ?1",
            [session_id],
            |row| row.get(0),
        )
        .ok()?;
    name.map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    pub(crate) fn fixture_db(path: &Path) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(include_str!("../../fixtures/trajectory_parity/goose.sql"))
            .unwrap();
    }

    fn parse(path: &Path, include_reasoning: bool) -> Vec<Record> {
        let next_doc_id = AtomicU64::new(0);
        let mut records = Vec::new();
        parse_index_records(
            path,
            IndexParseState::default(),
            include_reasoning,
            &next_doc_id,
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();
        records
    }

    fn fixture() -> (tempfile::TempDir, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let database = temp.path().join("sessions").join("sessions.db");
        std::fs::create_dir_all(database.parent().unwrap()).unwrap();
        fixture_db(&database);
        (temp, database)
    }

    #[test]
    fn ledger_input_includes_cache_and_is_split_into_disjoint_buckets() {
        // goose reports 5881 prompt tokens of which 5632 were served from cache.
        let buckets = ledger_buckets(5881, 5632, 100, 162);
        assert_eq!(buckets.raw_input, 5881);
        assert_eq!(buckets.uncached_input, 149);
        assert_eq!(buckets.cache_read, 5632);
        assert_eq!(buckets.cache_write, 100);
        assert_eq!(buckets.output, 162);
        assert_eq!(buckets.additive_total(), 5881 + 162);
        // A provider that reports cache tokens larger than its input must not
        // underflow into an absurd bill.
        let clamped = ledger_buckets(10, 999, 999, 0);
        assert_eq!(clamped.cache_read, 10);
        assert_eq!(clamped.cache_write, 0);
        assert_eq!(clamped.uncached_input, 0);
    }

    #[test]
    fn session_scope_projects_tools_reasoning_and_skips_turn_context() {
        let (_temp, database) = fixture();
        let path = virtual_path(&database, "sess_main");
        let records = parse(&path, true);
        let roles = records
            .iter()
            .map(|record| (record.role.as_str(), record.text.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            roles,
            vec![
                ("user", "List the files"),
                ("tool_use", "{\"command\":\"ls\"}"),
                ("tool_result", "a.txt"),
                ("reasoning", "thinking about it"),
                ("assistant", "One file"),
            ]
        );
        let tool_use = &records[1];
        assert_eq!(tool_use.tool_name.as_deref(), Some("shell"));
        assert_eq!(tool_use.links.event_id.as_deref(), Some("call_1"));
        let tool_result = &records[2];
        assert_eq!(
            tool_result.links.parent_tool_use_id.as_deref(),
            Some("call_1")
        );
        assert_eq!(tool_result.links.tool_result_is_error, Some(false));
        // The turn-context message is scaffolding the user never saw.
        assert!(
            !records
                .iter()
                .any(|record| record.text.contains("<turn-context>"))
        );
        assert!(
            records
                .iter()
                .all(|record| record.links.conversation_kind.as_deref() != Some("subagent"))
        );
    }

    #[test]
    fn reasoning_is_opt_in_but_still_counts_towards_the_offset() {
        let (_temp, database) = fixture();
        let path = virtual_path(&database, "sess_main");
        let with = parse(&path, true);
        let without = parse(&path, false);
        assert_eq!(with.len(), without.len() + 1);
        assert!(!without.iter().any(|record| record.role == "reasoning"));
        // Same session, same records: the offset must not depend on the flag,
        // or an enable-reasoning refresh would splice two parsers' output.
        let next_doc_id = AtomicU64::new(0);
        let parsed = parse_index_records(
            &path,
            IndexParseState::default(),
            false,
            &next_doc_id,
            |_| Ok(()),
        )
        .unwrap();
        let with_id = AtomicU64::new(0);
        let parsed_with = parse_index_records(
            &path,
            IndexParseState::default(),
            true,
            &with_id,
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(parsed.offset, parsed_with.offset);
    }

    #[test]
    fn subagent_sessions_carry_their_parent() {
        let (_temp, database) = fixture();
        let path = virtual_path(&database, "sess_child");
        let records = parse(&path, false);
        assert!(!records.is_empty());
        for record in &records {
            assert_eq!(record.links.parent_session_id.as_deref(), Some("sess_main"));
            assert_eq!(record.links.conversation_kind.as_deref(), Some("subagent"));
        }
    }

    #[test]
    fn hidden_and_empty_sessions_stay_out_of_the_index() {
        let (_temp, database) = fixture();
        let sessions = enumerate_sessions(&database).unwrap();
        let ids = sessions.iter().map(|s| s.id.as_str()).collect::<Vec<_>>();
        assert_eq!(ids, vec!["sess_child", "sess_main"]);
    }

    #[test]
    fn content_fingerprints_ignore_ledger_only_writes() {
        let (_temp, database) = fixture();
        let before = enumerate_sessions(&database).unwrap();
        {
            let conn = Connection::open(&database).unwrap();
            conn.execute(
                "INSERT INTO usage_ledger (session_id, created_timestamp, model, input_tokens, output_tokens, cost, cost_source) \
                 VALUES ('sess_main', 100, 'gpt', 10, 5, 0.1, 'estimated')",
                [],
            )
            .unwrap();
        }
        let after = enumerate_sessions(&database).unwrap();
        assert_eq!(before.len(), after.len());
        assert_eq!(before[1].fingerprint, after[1].fingerprint);
        // Rewriting a conversation in place must invalidate its session.
        {
            let conn = Connection::open(&database).unwrap();
            conn.execute(
                "UPDATE messages SET content_json = '[{\"type\":\"text\",\"text\":\"edited\"}]' \
                 WHERE session_id = 'sess_main' AND role = 'assistant'",
                [],
            )
            .unwrap();
        }
        let edited = enumerate_sessions(&database).unwrap();
        assert_ne!(before[1].fingerprint, edited[1].fingerprint);
    }

    #[test]
    fn usage_events_join_session_metadata_and_split_cache() {
        let (_temp, database) = fixture();
        let parsed = parse_usage_file(&database).unwrap();
        assert!(parsed.cacheable);
        assert_eq!(parsed.events.len(), 2);
        let main = &parsed.events[0];
        assert_eq!(main.session_id.as_deref(), Some("sess_main"));
        assert_eq!(main.model.as_deref(), Some("gpt-4o"));
        assert_eq!(main.source, "goose");
        assert_eq!(main.tokens.raw_input, 1000);
        assert_eq!(main.tokens.uncached_input, 400);
        assert_eq!(main.tokens.cache_read, 600);
        assert_eq!(main.tokens.output, 50);
        assert_eq!(main.tokens.additive_total(), 1050);
        assert_eq!(main.source_cost_usd, Some(0.25));
        assert!(main.cost_authoritative);
        assert!(!main.sidechain);
        assert_eq!(main.timestamp_ms, 1_700_000_000_000);
        let child = &parsed.events[1];
        assert!(child.sidechain);
        assert_eq!(child.model.as_deref(), Some("gpt-4o-mini"));
        assert!(!child.cost_authoritative);
    }

    #[test]
    fn enumerated_size_equals_the_parser_offset() {
        let (_temp, database) = fixture();
        for (id, include_reasoning) in [
            ("sess_main", true),
            ("sess_main", false),
            ("sess_child", true),
            ("sess_child", false),
        ] {
            let size = enumerate_sessions(&database)
                .unwrap()
                .into_iter()
                .find(|session| session.id == id)
                .unwrap()
                .size;
            let next_doc_id = AtomicU64::new(0);
            let parsed = parse_index_records(
                &virtual_path(&database, id),
                IndexParseState::default(),
                include_reasoning,
                &next_doc_id,
                |_| Ok(()),
            )
            .unwrap();
            assert_eq!(size, parsed.offset, "{id} (reasoning={include_reasoning})");
        }
    }

    #[test]
    fn virtual_paths_round_trip_and_classify() {
        let database = Path::new("/home/user/.local/share/goose/sessions/sessions.db");
        let path = virtual_path(database, "20261020_1");
        assert_eq!(
            split_virtual_path(&path),
            Some((database.to_path_buf(), "20261020_1".to_string()))
        );
        assert!(matches_path(&path.to_string_lossy()));
        assert!(matches_path(&database.to_string_lossy()));
        assert!(!matches_path(
            "/home/user/.local/share/goose/sessions/legacy.jsonl"
        ));
    }

    #[test]
    fn metadata_without_visibility_flags_is_visible() {
        assert!(is_user_visible(&serde_json::json!({})));
        assert!(is_user_visible(&serde_json::json!({"userVisible": true})));
        assert!(!is_user_visible(&serde_json::json!({"userVisible": false})));
        assert!(!is_user_visible(&serde_json::json!({"turnContext": true})));
    }

    #[test]
    fn goose_path_root_replaces_the_default_roots() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        let database = root.join("data/sessions/sessions.db");
        std::fs::create_dir_all(database.parent().unwrap()).unwrap();
        fixture_db(&database);
        // SAFETY: the test binary is single-threaded for env access via the
        // shared guard used by the other source tests.
        let _guard = crate::test_support::env_lock();
        let _env = crate::test_support::EnvVarGuard::set(&[(
            "GOOSE_PATH_ROOT",
            Some(root.to_str().unwrap()),
        )]);
        assert_eq!(roots(), vec![root.join("data").join("sessions")]);
        assert_eq!(db_paths(), vec![database.clone()]);
        assert_eq!(db_dirs(), vec![root.join("data").join("sessions")]);
        assert_eq!(
            discover(),
            vec![SourceFile {
                source: SourceKind::Goose,
                path: database.clone()
            }]
        );
        assert_eq!(usage_files(), vec![database]);
    }
}
