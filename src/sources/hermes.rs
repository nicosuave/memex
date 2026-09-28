use super::{
    ConversationKind, IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions,
    SourceFile,
};
use crate::state::PendingToolCall;
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::{Context, Result, anyhow};
use chrono::DateTime;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params, types::Value as SqlValue};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 2,
    index: 2,
    usage: 7,
};

const CONTENT_JSON_PREFIX: &str = "\0json:";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Checkpoint {
    pub max_message_id: u64,
    pub generation: String,
    pub previous_generation: Option<String>,
}

#[derive(Clone, Debug)]
struct MessageRow {
    id: u64,
    session_id: String,
    role: String,
    content: Option<String>,
    tool_call_id: Option<String>,
    tool_calls: Option<String>,
    tool_name: Option<String>,
    timestamp_ms: u64,
    reasoning: Option<String>,
    reasoning_content: Option<String>,
    cwd: Option<String>,
    parent_session_id: Option<String>,
}

#[derive(Clone, Debug)]
struct ToolCall {
    id: String,
    name: Option<String>,
    arguments: String,
}

pub fn matches_path(path: &str) -> bool {
    path.ends_with("/.hermes/state.db")
        || path.contains("/.hermes/profiles/") && path.ends_with("/state.db")
        || path.ends_with("\\.hermes\\state.db")
        || path.contains("\\.hermes\\profiles\\") && path.ends_with("\\state.db")
}

pub fn discover() -> Vec<SourceFile> {
    discover_from_roots(&profile_roots())
}

pub fn profile_roots() -> Vec<PathBuf> {
    let home = super::common::home();
    let explicit_roots = std::env::var_os("HERMES_PROFILE_ROOTS");
    let hermes_home = std::env::var_os("HERMES_HOME").map(PathBuf::from);
    let state_dir = std::env::var_os("HERMES_STATE_DIR").map(PathBuf::from);
    let xdg_state_home = std::env::var_os("XDG_STATE_HOME").map(PathBuf::from);
    profile_roots_for(
        &home,
        explicit_roots.as_deref(),
        hermes_home.as_deref(),
        state_dir.as_deref(),
        xdg_state_home.as_deref(),
    )
}

fn profile_roots_for(
    home: &Path,
    explicit_roots: Option<&std::ffi::OsStr>,
    hermes_home: Option<&Path>,
    state_dir: Option<&Path>,
    xdg_state_home: Option<&Path>,
) -> Vec<PathBuf> {
    if let Some(roots) = explicit_roots {
        return split_roots(roots);
    }
    let mut roots = vec![home.join(".hermes"), home.join(".local/state/hermes")];
    if let Some(xdg) = xdg_state_home {
        roots.push(xdg.join("hermes"));
    }
    if let Some(hermes_home) = hermes_home {
        roots.push(hermes_home.to_path_buf());
    }
    if let Some(state_dir) = state_dir {
        roots.push(state_dir.to_path_buf());
    }
    roots
}

fn split_roots(roots: &std::ffi::OsStr) -> Vec<PathBuf> {
    roots
        .to_string_lossy()
        .split(',')
        .filter_map(|root| {
            let root = root.trim();
            (!root.is_empty()).then(|| PathBuf::from(root))
        })
        .collect()
}

pub fn discover_from_roots(roots: &[PathBuf]) -> Vec<SourceFile> {
    let mut candidates = Vec::new();
    for root in roots {
        if root.is_file() {
            if is_state_db(root) {
                candidates.push(root.clone());
            }
            continue;
        }
        if !root.is_dir() {
            continue;
        }
        if root.file_name().and_then(|v| v.to_str()) == Some("profiles") {
            add_profile_children(root, &mut candidates);
        } else {
            add_state(root, &mut candidates);
            add_profile_children(&root.join("profiles"), &mut candidates);
        }
    }
    let mut seen = HashSet::new();
    candidates.retain(|path| seen.insert(path.canonicalize().unwrap_or_else(|_| path.clone())));
    candidates.sort();
    candidates
        .into_iter()
        .map(|path| SourceFile {
            source: SourceKind::Hermes,
            path,
        })
        .collect()
}

fn add_state(root: &Path, out: &mut Vec<PathBuf>) {
    let path = root.join("state.db");
    if is_state_db(&path) {
        out.push(path);
    }
}

fn add_profile_children(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            add_state(&entry.path(), out);
        }
    }
}

fn is_state_db(path: &Path) -> bool {
    path.is_file() && path.file_name().and_then(|v| v.to_str()) == Some("state.db")
}

pub fn checkpoint(path: &Path, previous_offset: Option<u64>) -> Result<Checkpoint> {
    let connection = open_read_only(path)?;
    let transaction = connection.unchecked_transaction()?;
    let message_columns = table_columns(&transaction, "messages")?;
    let columns = table_columns(&transaction, "sessions")?;
    let active = if message_columns.contains("active") {
        "COALESCE(active, 1) != 0"
    } else {
        "1"
    };
    let active_identity = |through_id: Option<u64>| -> Result<(u64, u64, u64)> {
        let sql = format!(
            "SELECT COUNT(*), COALESCE(MAX(id), 0), COALESCE(SUM(id), 0)
             FROM messages WHERE {active}{}",
            if through_id.is_some() {
                " AND id <= ?1"
            } else {
                ""
            }
        );
        let values = match through_id {
            Some(id) => {
                transaction.query_row(&sql, params![id.min(i64::MAX as u64) as i64], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })?
            }
            None => transaction.query_row(&sql, [], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?,
        };
        Ok((
            values.0.max(0) as u64,
            values.1.max(0) as u64,
            values.2.max(0) as u64,
        ))
    };
    let (active_count, max_message_id, active_id_sum) = active_identity(None)?;
    let schema_version = transaction
        .query_row("SELECT MAX(version) FROM schema_version", [], |row| {
            row.get::<_, Option<i64>>(0)
        })
        .optional()
        .ok()
        .flatten()
        .flatten()
        .unwrap_or(0)
        .max(0) as u64;
    let rewind_generation = if columns.contains("rewind_count") {
        transaction
            .query_row(
                "SELECT COALESCE(SUM(rewind_count), 0) FROM sessions",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0)
            .max(0) as u64
    } else {
        0
    };
    let generation = |active_count: u64, active_max: u64, active_id_sum: u64| {
        format!("{schema_version}:{rewind_generation}:{active_count}:{active_max}:{active_id_sum}")
    };
    let previous_generation = previous_offset
        .map(|offset| active_identity(Some(offset)))
        .transpose()?
        .map(|(count, max, sum)| generation(count, max, sum));
    transaction.commit()?;
    Ok(Checkpoint {
        max_message_id,
        generation: generation(active_count, max_message_id, active_id_sum),
        previous_generation,
    })
}

pub fn session_cwd(path: &Path, session_id: &str) -> Option<String> {
    let connection = open_read_only(path).ok()?;
    let columns = table_columns(&connection, "sessions").ok()?;
    if !columns.contains("cwd") {
        return None;
    }
    connection
        .query_row(
            "SELECT cwd FROM sessions WHERE id = ?1",
            params![session_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .ok()
        .flatten()
        .flatten()
        .filter(|cwd| !cwd.is_empty())
}

pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    let connection = open_read_only(path)?;
    let rows = load_rows(&connection, state.offset)?;
    let source_path = path.to_string_lossy().to_string();
    let mut diagnostics = ParseDiagnostics::default();
    let mut pending_tool_calls = state.pending_tool_calls;
    let mut planned_calls = plan_tool_calls(&rows, &mut diagnostics);
    let mut max_message_id = state.offset;

    for row in &rows {
        max_message_id = max_message_id.max(row.id);
        let project = row
            .cwd
            .as_deref()
            .map(super::common::project_from_path)
            .unwrap_or_else(|| SourceKind::Hermes.label().to_string());
        let links = row_links(row);
        let base_turn = row.id.min((u32::MAX / 8) as u64).saturating_mul(8) as u32;
        let mut component = 0u32;

        match row.role.as_str() {
            "user" => {
                let text = content_text(row.content.as_deref());
                if !text.trim().is_empty() {
                    emit(Record {
                        source: SourceKind::Hermes,
                        doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                        ts: row.timestamp_ms,
                        project,
                        session_id: row.session_id.clone(),
                        turn_id: base_turn + component,
                        role: "user".to_string(),
                        text,
                        tool_name: None,
                        tool_input: None,
                        tool_output: None,
                        links,
                        source_path: source_path.clone(),
                    })?;
                }
            }
            "assistant" => {
                if include_reasoning && let Some(reasoning) = reasoning_text(row, &mut diagnostics)
                {
                    let mut reasoning_links = links.clone();
                    reasoning_links.event_id = Some(format!("{}:reasoning", row.id));
                    emit(Record {
                        source: SourceKind::Hermes,
                        doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                        ts: row.timestamp_ms,
                        project: project.clone(),
                        session_id: row.session_id.clone(),
                        turn_id: base_turn + component,
                        role: "reasoning".to_string(),
                        text: reasoning,
                        tool_name: None,
                        tool_input: None,
                        tool_output: None,
                        links: reasoning_links,
                        source_path: source_path.clone(),
                    })?;
                    component += 1;
                }

                let text = content_text(row.content.as_deref());
                if !text.trim().is_empty() {
                    emit(Record {
                        source: SourceKind::Hermes,
                        doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                        ts: row.timestamp_ms,
                        project: project.clone(),
                        session_id: row.session_id.clone(),
                        turn_id: base_turn + component,
                        role: "assistant".to_string(),
                        text,
                        tool_name: None,
                        tool_input: None,
                        tool_output: None,
                        links: links.clone(),
                        source_path: source_path.clone(),
                    })?;
                    component += 1;
                }

                for call in planned_calls.remove(&row.id).unwrap_or_default() {
                    let mut call_links = links.clone();
                    call_links.event_id = Some(call.id.clone());
                    call_links.parent_event_id = Some(row.id.to_string());
                    let doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
                    let replaced = pending_tool_calls.insert(
                        call.id.clone(),
                        super::common::pending_tool_call(
                            call.name.clone(),
                            Some(call.id.clone()),
                            doc_id,
                            row.timestamp_ms,
                            Some(&call.arguments),
                            &call_links,
                            &row.session_id,
                        ),
                    );
                    if replaced.is_some() {
                        diagnostics.duplicate_tool_calls += 1;
                    }
                    emit(Record {
                        source: SourceKind::Hermes,
                        doc_id,
                        ts: row.timestamp_ms,
                        project: project.clone(),
                        session_id: row.session_id.clone(),
                        turn_id: base_turn + component,
                        role: "tool_use".to_string(),
                        text: call.arguments.clone(),
                        tool_name: call.name,
                        tool_input: Some(call.arguments),
                        tool_output: None,
                        links: call_links,
                        source_path: source_path.clone(),
                    })?;
                    component += 1;
                }
            }
            "tool" => {
                let native_call_id = row.tool_call_id.as_deref().filter(|id| !id.is_empty());
                let matched_id = native_call_id
                    .filter(|id| pending_tool_calls.contains_key(*id))
                    .map(str::to_string)
                    .or_else(|| {
                        find_pending_call(
                            &pending_tool_calls,
                            &row.session_id,
                            row.tool_name.as_deref(),
                        )
                    });
                let pending = matched_id
                    .as_ref()
                    .and_then(|id| pending_tool_calls.remove(id));
                if native_call_id.is_some() && pending.is_none() {
                    diagnostics.orphan_tool_results += 1;
                }
                let tool_name = row
                    .tool_name
                    .clone()
                    .or_else(|| pending.as_ref().and_then(|call| call.tool_name.clone()));
                let output = content_text(row.content.as_deref());
                let mut result_links = links;
                if let Some(parent_id) = matched_id.or_else(|| native_call_id.map(str::to_string)) {
                    result_links.parent_event_id = Some(parent_id.clone());
                    result_links.parent_tool_use_id = Some(parent_id);
                }
                emit(Record {
                    source: SourceKind::Hermes,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: row.timestamp_ms,
                    project,
                    session_id: row.session_id.clone(),
                    turn_id: base_turn,
                    role: "tool_result".to_string(),
                    text: output.clone(),
                    tool_name,
                    tool_input: None,
                    tool_output: Some(output),
                    links: result_links,
                    source_path: source_path.clone(),
                })?;
            }
            "system" => {}
            role => diagnostics.increment_unknown_semantic(role),
        }
    }

    Ok(IndexParseOutput {
        offset: max_message_id,
        turn_id: 0,
        pending_tool_calls,
        session_id: None,
        diagnostics,
        legacy_turn_id: None,
        session_cwd: rows.iter().find_map(|row| row.cwd.clone()),
    })
}

fn open_read_only(path: &Path) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("open Hermes store {}", path.display()))?;
    connection.busy_timeout(Duration::from_secs(1))?;
    Ok(connection)
}

fn table_columns(connection: &Connection, table: &str) -> Result<HashSet<String>> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
    rows.collect::<std::result::Result<HashSet<_>, _>>()
        .map_err(Into::into)
}

fn optional_column(columns: &HashSet<String>, column: &str, fallback: &str) -> String {
    if columns.contains(column) {
        column.to_string()
    } else {
        fallback.to_string()
    }
}

fn load_rows(connection: &Connection, after_id: u64) -> Result<Vec<MessageRow>> {
    let message_columns = table_columns(connection, "messages")?;
    let session_columns = table_columns(connection, "sessions")?;
    let active = if message_columns.contains("active") {
        "COALESCE(m.active, 1) != 0"
    } else {
        "1"
    };
    let sql = format!(
        "SELECT m.id, m.session_id, m.role, m.content, m.tool_call_id, m.tool_calls,
                m.tool_name, m.timestamp,
                {}, {}, {}, {}
         FROM messages m
         LEFT JOIN sessions s ON s.id = m.session_id
         WHERE m.id > ?1 AND {active}
         ORDER BY m.id",
        optional_column(&message_columns, "reasoning", "NULL"),
        optional_column(&message_columns, "reasoning_content", "NULL"),
        optional_column(&session_columns, "cwd", "NULL"),
        optional_column(&session_columns, "parent_session_id", "NULL"),
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params![after_id.min(i64::MAX as u64) as i64], |row| {
        Ok(MessageRow {
            id: row.get::<_, i64>(0)?.max(0) as u64,
            session_id: row.get(1)?,
            role: row.get(2)?,
            content: row.get(3)?,
            tool_call_id: row.get(4)?,
            tool_calls: row.get(5)?,
            tool_name: row.get(6)?,
            timestamp_ms: timestamp_millis(row.get::<_, f64>(7)?),
            reasoning: row.get(8)?,
            reasoning_content: row.get(9)?,
            cwd: row.get(10)?,
            parent_session_id: row.get(11)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn plan_tool_calls(
    rows: &[MessageRow],
    diagnostics: &mut ParseDiagnostics,
) -> HashMap<u64, Vec<ToolCall>> {
    let mut planned = HashMap::new();
    for (index, row) in rows.iter().enumerate() {
        if row.role != "assistant" {
            continue;
        }
        let Some(raw) = row
            .tool_calls
            .as_deref()
            .filter(|raw| !raw.trim().is_empty())
        else {
            continue;
        };
        let value: Value = match serde_json::from_str(raw) {
            Ok(value) => value,
            Err(_) => {
                diagnostics.malformed_json_lines += 1;
                continue;
            }
        };
        let Some(entries) = value.as_array() else {
            diagnostics.increment_unknown_semantic("tool_calls_not_array");
            continue;
        };
        let mut calls = entries
            .iter()
            .enumerate()
            .filter_map(|(position, entry)| parse_tool_call(row.id, position, entry))
            .collect::<Vec<_>>();
        let idless_positions = calls
            .iter()
            .enumerate()
            .filter_map(|(position, call)| call.id.starts_with("hermes:").then_some(position))
            .collect::<Vec<_>>();
        if !idless_positions.is_empty() {
            let available = rows[index + 1..]
                .iter()
                .take_while(|next| next.role == "tool")
                .filter_map(|next| next.tool_call_id.clone())
                .collect::<Vec<_>>();
            if available.len() == idless_positions.len() {
                for (position, adopted) in idless_positions.into_iter().zip(available) {
                    calls[position].id = adopted;
                }
            }
        }
        if !calls.is_empty() {
            planned.insert(row.id, calls);
        }
    }
    planned
}

fn parse_tool_call(row_id: u64, position: usize, value: &Value) -> Option<ToolCall> {
    let object = value.as_object()?;
    let function = object.get("function").and_then(Value::as_object);
    let name = function
        .and_then(|function| function.get("name"))
        .or_else(|| object.get("name"))
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .map(str::to_string);
    let native_id = object
        .get("id")
        .or_else(|| object.get("call_id"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty());
    let arguments = function
        .and_then(|function| function.get("arguments"))
        .or_else(|| object.get("arguments"))
        .map(json_text)
        .unwrap_or_default();
    Some(ToolCall {
        id: native_id
            .map(str::to_string)
            .unwrap_or_else(|| format!("hermes:{row_id}:{position}")),
        name,
        arguments,
    })
}

fn find_pending_call(
    pending: &HashMap<String, PendingToolCall>,
    session_id: &str,
    tool_name: Option<&str>,
) -> Option<String> {
    let matches = pending
        .iter()
        .filter(|(_, call)| call.session_id.as_deref() == Some(session_id))
        .filter(|(_, call)| {
            tool_name.is_none_or(|name| call.tool_name.as_deref().is_none_or(|value| value == name))
        })
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    (matches.len() == 1).then(|| matches[0].clone())
}

fn row_links(row: &MessageRow) -> RecordLinks {
    RecordLinks {
        event_id: Some(row.id.to_string()),
        parent_session_id: row.parent_session_id.clone(),
        thread_source: row.parent_session_id.as_ref().map(|_| "fork".to_string()),
        conversation_kind: Some(
            if row.parent_session_id.is_some() {
                ConversationKind::Fork
            } else {
                ConversationKind::Main
            }
            .as_str()
            .to_string(),
        ),
        ..RecordLinks::default()
    }
}

fn reasoning_text(row: &MessageRow, diagnostics: &mut ParseDiagnostics) -> Option<String> {
    let text = row
        .reasoning_content
        .as_deref()
        .filter(|text| !text.trim().is_empty())
        .or_else(|| {
            row.reasoning
                .as_deref()
                .filter(|text| !text.trim().is_empty())
        })
        .map(str::trim)?;
    if is_encrypted_reasoning(text) {
        diagnostics.encrypted_reasoning_dropped += 1;
        None
    } else {
        Some(text.to_string())
    }
}

fn is_encrypted_reasoning(text: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return false;
    };
    let Some(object) = value.as_object() else {
        return false;
    };
    matches!(
        object.get("type").and_then(Value::as_str),
        Some("redacted_thinking" | "encrypted_reasoning")
    ) || object.contains_key("encrypted_content")
}

fn content_text(content: Option<&str>) -> String {
    let Some(content) = content else {
        return String::new();
    };
    let value = if let Some(encoded) = content.strip_prefix(CONTENT_JSON_PREFIX) {
        match serde_json::from_str::<Value>(encoded) {
            Ok(value) => value,
            Err(_) => return encoded.to_string(),
        }
    } else {
        return content.to_string();
    };
    match value {
        Value::Array(blocks) => blocks
            .iter()
            .filter_map(|block| {
                let object = block.as_object()?;
                let block_type = object.get("type").and_then(Value::as_str).unwrap_or("");
                matches!(block_type, "text" | "input_text" | "output_text")
                    .then(|| object.get("text").and_then(Value::as_str))
                    .flatten()
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Value::String(text) => text,
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn json_text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

fn timestamp_millis(value: f64) -> u64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    let millis = if value > 100_000_000_000.0 {
        value
    } else {
        value * 1_000.0
    };
    millis.round().clamp(0.0, u64::MAX as f64) as u64
}

type Row = HashMap<String, SqlValue>;

pub(crate) fn parse_usage_file(path: &Path) -> Result<crate::sources::UsageParseOutput> {
    let mut wal = path.as_os_str().to_os_string();
    wal.push("-wal");
    let wal = PathBuf::from(wal);
    let wal_before = crate::sources::UsageDependency::from_path_or_absent(&wal);
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open Hermes SQLite database {}", path.display()))?;
    let transaction = conn.unchecked_transaction()?;
    let tables = table_names(&transaction)?;
    if !tables.contains("sessions") {
        return Err(anyhow!("Hermes database has no sessions table"));
    }
    let sessions = rows(
        &transaction,
        "sessions",
        &[
            "id",
            "model",
            "started_at",
            "ended_at",
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "reasoning_tokens",
            "billing_provider",
            "billing_mode",
            "estimated_cost_usd",
            "actual_cost_usd",
            "cost_status",
            "cost_source",
            "cwd",
            "git_repo_root",
            "profile_name",
        ],
        &["id", "started_at", "ended_at", "model", "profile_name"],
    )?;
    let model_rows = if tables.contains("session_model_usage") {
        rows(
            &transaction,
            "session_model_usage",
            &[
                "session_id",
                "model",
                "billing_provider",
                "billing_base_url",
                "billing_mode",
                "task",
                "api_call_count",
                "input_tokens",
                "output_tokens",
                "cache_read_tokens",
                "cache_write_tokens",
                "reasoning_tokens",
                "estimated_cost_usd",
                "actual_cost_usd",
                "cost_status",
                "cost_source",
                "first_seen",
                "last_seen",
            ],
            &[
                "session_id",
                "model",
                "task",
                "billing_provider",
                "first_seen",
                "last_seen",
            ],
        )?
    } else {
        Vec::new()
    };
    let mut by_session: HashMap<String, Vec<&Row>> = HashMap::new();
    for row in &model_rows {
        if let Some(id) = text(row, "session_id") {
            by_session.entry(id).or_default().push(row);
        }
    }
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let mut events = Vec::new();
    for session in &sessions {
        let Some(id) = text(session, "id") else {
            continue;
        };
        let aggregate = buckets(session);
        let Some(model_rows) = by_session.get(&id) else {
            if !is_zero(&aggregate) || cost(session).is_some() {
                events.push(event(
                    &source_path,
                    &id,
                    Some(&id),
                    session,
                    aggregate,
                    cost(session),
                    cost(session).is_some(),
                    true,
                    timestamp(session),
                    0,
                ));
            }
            continue;
        };
        let mut summed = TokenBuckets::default();
        let mut invalid_model = false;
        let mut attributed_model_cost = 0.0;
        let authoritative_cost = cost(session);
        let model_event_start = events.len();
        for row in model_rows {
            let raw_current = buckets(row);
            let current = cap_to_remaining(raw_current.clone(), aggregate.clone(), summed.clone());
            let inconsistent = current != raw_current;
            invalid_model |= inconsistent;
            summed = add(summed, &current);
            if !is_zero(&current) {
                let model = text(row, "model").or_else(|| text(session, "model"));
                let task = text(row, "task").unwrap_or_else(|| "default".to_string());
                let identity = serde_json::to_string(&(
                    id.as_str(),
                    model.as_deref().unwrap_or("unknown"),
                    task.as_str(),
                    text(session, "profile_name").unwrap_or_default(),
                ))
                .expect("tuple serialization cannot fail");
                let id = format!("model:{identity}");
                let source_cost = (!inconsistent).then(|| cost(row)).flatten();
                if let Some(cost) = source_cost {
                    attributed_model_cost += cost;
                }
                events.push(model_event(
                    &source_path,
                    &id,
                    &text(row, "session_id"),
                    row,
                    session,
                    model,
                    current,
                    source_cost,
                    authoritative_cost.is_some() && source_cost.is_none(),
                    inconsistent,
                    timestamp(row).max(timestamp(session)),
                    timestamp(row).max(timestamp(session)),
                ));
            }
        }
        let residual = subtract(aggregate, summed);
        let model_cost_exceeds_authority =
            authoritative_cost.is_some_and(|authoritative| attributed_model_cost > authoritative);
        let reconcile_with_authority =
            authoritative_cost.is_some() && (invalid_model || model_cost_exceeds_authority);
        if reconcile_with_authority {
            for event in &mut events[model_event_start..] {
                event.source_cost_usd = None;
                event.cost_authoritative = true;
            }
        }
        let source_cost = if reconcile_with_authority {
            authoritative_cost
        } else {
            authoritative_cost.and_then(|authoritative| {
                (authoritative > attributed_model_cost)
                    .then_some(authoritative - attributed_model_cost)
            })
        };
        if !is_zero(&residual) || source_cost.is_some() {
            events.push(event(
                &source_path,
                &format!("session:{id}:residual"),
                Some(&id),
                session,
                residual,
                source_cost,
                authoritative_cost.is_some(),
                true,
                timestamp(session),
                timestamp(session),
            ));
        }
    }
    // Keep sessions and model usage on the same SQLite snapshot. This matters in WAL mode,
    // where Hermes can commit the aggregate and detail rows independently while we read.
    transaction.commit()?;
    let wal_after = crate::sources::UsageDependency::from_path_or_absent(&wal);
    Ok(crate::sources::UsageParseOutput {
        events,
        cacheable: cacheable_after_wal_read(&wal_before, &wal_after),
        // SQLite WAL pages can contain committed data not yet checkpointed into the main
        // database. The SHM file is coordination metadata and must not make scans stale.
        deps: vec![wal_after],
    })
}

fn table_names(conn: &Connection) -> Result<HashSet<String>> {
    let mut statement = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
    Ok(statement
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(Result::ok)
        .collect())
}

fn rows(conn: &Connection, table: &str, wanted: &[&str], order_by: &[&str]) -> Result<Vec<Row>> {
    let columns = table_columns(conn, table)?;
    let selected: Vec<&str> = wanted
        .iter()
        .copied()
        .filter(|column| columns.contains(*column))
        .collect();
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    let order = order_by
        .iter()
        .filter(|column| columns.contains(**column))
        .map(|column| format!("\"{column}\""));
    let order = order.collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT {} FROM {}{}",
        selected
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect::<Vec<_>>()
            .join(","),
        table,
        if order.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {order}")
        }
    );
    let mut statement = conn.prepare(&sql)?;
    let mut result = Vec::new();
    let mut query = statement.query([])?;
    while let Some(row) = query.next()? {
        let mut values = Row::new();
        for (index, column) in selected.iter().enumerate() {
            values.insert((*column).to_string(), row.get(index)?);
        }
        result.push(values);
    }
    Ok(result)
}

fn number(row: &Row, key: &str) -> u64 {
    match row.get(key) {
        Some(SqlValue::Integer(value)) => (*value).max(0) as u64,
        Some(SqlValue::Real(value)) if *value >= 0.0 => *value as u64,
        _ => 0,
    }
}
fn text(row: &Row, key: &str) -> Option<String> {
    match row.get(key) {
        Some(SqlValue::Text(value)) => Some(value.clone()),
        Some(SqlValue::Integer(value)) => Some(value.to_string()),
        _ => None,
    }
}
fn cost(row: &Row) -> Option<f64> {
    let valid = |value: Option<&SqlValue>| match value {
        Some(SqlValue::Real(value)) if value.is_finite() && *value >= 0.0 => Some(*value),
        Some(SqlValue::Integer(value)) if *value >= 0 => Some(*value as f64),
        _ => None,
    };
    valid(row.get("actual_cost_usd")).or_else(|| valid(row.get("estimated_cost_usd")))
}
fn timestamp(row: &Row) -> u64 {
    let numeric_timestamp = |key: &str| match row.get(key) {
        Some(SqlValue::Integer(value)) => timestamp_number(*value as f64),
        Some(SqlValue::Real(value)) => timestamp_number(*value),
        _ => 0,
    };
    let text_timestamp = |key: &str| {
        text(row, key).and_then(|value| {
            DateTime::parse_from_rfc3339(&value)
                .ok()
                .map(|date| date.timestamp_millis().max(0) as u64)
        })
    };
    numeric_timestamp("last_seen")
        .max(numeric_timestamp("first_seen"))
        .max(numeric_timestamp("ended_at"))
        .max(numeric_timestamp("started_at"))
        .max(text_timestamp("last_seen").unwrap_or(0))
        .max(text_timestamp("first_seen").unwrap_or(0))
        .max(text_timestamp("ended_at").unwrap_or(0))
        .max(text_timestamp("started_at").unwrap_or(0))
}
fn timestamp_number(value: f64) -> u64 {
    if !value.is_finite() || value < 0.0 {
        return 0;
    }
    let millis = if value < 10_000_000_000.0 {
        value * 1000.0
    } else {
        value
    };
    millis.round().min(u64::MAX as f64) as u64
}
fn buckets(row: &Row) -> TokenBuckets {
    let input = number(row, "input_tokens");
    let cache_read = number(row, "cache_read_tokens");
    let cache_write = number(row, "cache_write_tokens");
    let output = number(row, "output_tokens");
    let mut buckets = TokenBuckets::disjoint(input, cache_read, cache_write, output);
    buckets.reasoning = number(row, "reasoning_tokens").min(output);
    buckets
}
fn add(a: TokenBuckets, b: &TokenBuckets) -> TokenBuckets {
    TokenBuckets {
        raw_input: a.raw_input.saturating_add(b.raw_input),
        uncached_input: a.uncached_input.saturating_add(b.uncached_input),
        cache_read: a.cache_read.saturating_add(b.cache_read),
        cache_write: a.cache_write.saturating_add(b.cache_write),
        cache_write_1h: 0,
        output: a.output.saturating_add(b.output),
        reasoning: a.reasoning.saturating_add(b.reasoning),
    }
}
fn is_zero(value: &TokenBuckets) -> bool {
    value.raw_input == 0
        && value.uncached_input == 0
        && value.output == 0
        && value.cache_write == 0
        && value.cache_read == 0
        && value.reasoning == 0
}
fn subtract(a: TokenBuckets, b: TokenBuckets) -> TokenBuckets {
    TokenBuckets {
        raw_input: a.raw_input.saturating_sub(b.raw_input),
        uncached_input: a.uncached_input.saturating_sub(b.uncached_input),
        cache_read: a.cache_read.saturating_sub(b.cache_read),
        cache_write: a.cache_write.saturating_sub(b.cache_write),
        cache_write_1h: 0,
        output: a.output.saturating_sub(b.output),
        reasoning: a.reasoning.saturating_sub(b.reasoning),
    }
}
fn cap_to_remaining(
    value: TokenBuckets,
    aggregate: TokenBuckets,
    used: TokenBuckets,
) -> TokenBuckets {
    let cap = |total: u64, already: u64, current: u64| current.min(total.saturating_sub(already));
    TokenBuckets {
        raw_input: cap(aggregate.raw_input, used.raw_input, value.raw_input),
        uncached_input: cap(
            aggregate.uncached_input,
            used.uncached_input,
            value.uncached_input,
        ),
        cache_read: cap(aggregate.cache_read, used.cache_read, value.cache_read),
        cache_write: cap(aggregate.cache_write, used.cache_write, value.cache_write),
        cache_write_1h: 0,
        output: cap(aggregate.output, used.output, value.output),
        reasoning: cap(aggregate.reasoning, used.reasoning, value.reasoning),
    }
}
fn cacheable_after_wal_read(
    wal_before: &crate::sources::UsageDependency,
    wal_after: &crate::sources::UsageDependency,
) -> bool {
    wal_before == wal_after
}
#[allow(clippy::too_many_arguments)]
fn event(
    path: &Arc<str>,
    record: &str,
    session_id: Option<&str>,
    row: &Row,
    tokens: TokenBuckets,
    source_cost_usd: Option<f64>,
    cost_authoritative: bool,
    conservative: bool,
    timestamp_ms: u64,
    order: u64,
) -> UsageEvent {
    UsageEvent {
        source: "hermes",
        source_path: path.clone(),
        source_record_id: Some(record.to_string()),
        session_id: session_id.map(str::to_string),
        request_id: None,
        message_id: None,
        timestamp_ms,
        project: text(row, "cwd").or_else(|| text(row, "git_repo_root")),
        provider: text(row, "billing_provider"),
        model: text(row, "model"),
        tokens,
        credits: None,
        token_usage_available: true,
        source_cost_usd,
        cost_authoritative,
        dedupe_confidence: "strong",
        conservative_undercount: conservative,
        cache_chain_excluded: true,
        sidechain: false,
        permission_review: false,
        source_order: order,
    }
}
#[allow(clippy::too_many_arguments)]
fn model_event(
    path: &Arc<str>,
    record: &str,
    session_id: &Option<String>,
    row: &Row,
    session: &Row,
    model: Option<String>,
    tokens: TokenBuckets,
    source_cost_usd: Option<f64>,
    cost_authoritative: bool,
    conservative: bool,
    timestamp_ms: u64,
    order: u64,
) -> UsageEvent {
    let mut event = event(
        path,
        record,
        session_id.as_deref(),
        row,
        tokens,
        source_cost_usd,
        cost_authoritative,
        conservative,
        timestamp_ms,
        order,
    );
    event.model = model;
    event.provider = text(row, "billing_provider").or_else(|| text(session, "billing_provider"));
    event.project = text(session, "cwd").or_else(|| text(session, "git_repo_root"));
    event
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    fn db(path: &Path, model_usage: bool) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch("CREATE TABLE sessions (id TEXT, model TEXT, started_at INTEGER, input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER, billing_provider TEXT, estimated_cost_usd REAL, cwd TEXT, git_repo_root TEXT, profile_name TEXT); CREATE TABLE messages (content TEXT, system_prompt TEXT);").unwrap();
        if model_usage {
            conn.execute_batch("CREATE TABLE session_model_usage (session_id TEXT, model TEXT, billing_provider TEXT, task TEXT, input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER, estimated_cost_usd REAL, first_seen INTEGER);").unwrap();
        }
    }

    #[test]
    fn fixture_projects_active_rows_tools_and_opt_in_reasoning() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(include_str!("../../fixtures/trajectory_parity/hermes.sql"))
            .unwrap();
        drop(connection);

        let mut records = Vec::new();
        let parsed = parse_index_records(
            &path,
            IndexParseState::default(),
            false,
            &AtomicU64::new(1),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(parsed.offset, 107);
        assert!(!records.iter().any(|record| record.role == "reasoning"));
        assert!(!records.iter().any(|record| record.text.contains("rewound")));
        assert!(records.iter().any(|record| {
            record.role == "tool_use"
                && record.tool_name.as_deref() == Some("terminal")
                && record.links.event_id.as_deref() == Some("call-hermes-1")
        }));
        assert!(records.iter().any(|record| {
            record.role == "tool_result"
                && record.links.parent_tool_use_id.as_deref() == Some("call-hermes-1")
        }));

        let mut reasoning = Vec::new();
        let parsed = parse_index_records(
            &path,
            IndexParseState::default(),
            true,
            &AtomicU64::new(1),
            |record| {
                reasoning.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert!(reasoning.iter().any(|record| {
            record.role == "reasoning" && record.text == "Check the working directory."
        }));
        assert_eq!(parsed.diagnostics.encrypted_reasoning_dropped, 1);
    }

    #[test]
    fn discovery_only_returns_canonical_root_and_profile_databases() {
        let temp = tempfile::tempdir().unwrap();
        for path in [
            "state.db",
            "profiles/a/state.db",
            "profiles/b/state.db",
            "snapshots/state.db",
            "nested/state.db",
            "sessions/old.jsonl",
        ] {
            let path = temp.path().join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"x").unwrap();
        }
        let files =
            discover_from_roots(&[temp.path().to_path_buf(), temp.path().join("profiles/a")]);
        assert_eq!(
            files
                .iter()
                .map(|f| f
                    .path
                    .strip_prefix(temp.path())
                    .unwrap()
                    .to_string_lossy()
                    .to_string())
                .collect::<Vec<_>>(),
            vec!["profiles/a/state.db", "profiles/b/state.db", "state.db"]
        );
    }

    #[test]
    fn discovery_allows_backup_ancestors_and_repo_profile_names() {
        let temp = tempfile::tempdir().unwrap();
        let backup_root = temp.path().join("backup/.hermes");
        let repo_profile = backup_root.join("profiles/repo");
        for path in [backup_root.join("state.db"), repo_profile.join("state.db")] {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"x").unwrap();
        }

        let files = discover_from_roots(&[backup_root]);
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn discovery_allows_any_profile_name_but_not_artifact_slots() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join(".hermes");
        let blocked_names = [
            "backup",
            "cron",
            "kanban",
            "repo",
            "repos",
            "sandbox",
            "sandboxes",
            "snapshot",
            "snapshots",
            "upgrade",
            "upgrades",
            "verification",
        ];
        for name in blocked_names {
            for path in [
                root.join("profiles").join(name).join("state.db"),
                root.join(name).join("state.db"),
            ] {
                fs::create_dir_all(path.parent().unwrap()).unwrap();
                fs::write(path, b"x").unwrap();
            }
        }

        let files = discover_from_roots(std::slice::from_ref(&root));
        assert_eq!(files.len(), blocked_names.len());
        assert!(files.iter().all(|file| {
            file.path.file_name().and_then(|name| name.to_str()) == Some("state.db")
                && file
                    .path
                    .parent()
                    .and_then(Path::parent)
                    .and_then(Path::file_name)
                    .and_then(|name| name.to_str())
                    == Some("profiles")
        }));
    }

    #[test]
    fn explicit_profile_root_discovers_only_that_profile_database() {
        let temp = tempfile::tempdir().unwrap();
        let profile = temp.path().join("profiles/verification");
        let sibling = temp.path().join("profiles/backup");
        for path in [profile.join("state.db"), sibling.join("state.db")] {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"x").unwrap();
        }

        let files = discover_from_roots(std::slice::from_ref(&profile));
        assert_eq!(
            files,
            vec![SourceFile {
                source: SourceKind::Hermes,
                path: profile.join("state.db")
            }]
        );
    }

    #[test]
    fn legacy_sessions_are_aggregates_and_ignore_transcript_tables() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        db(&path, false);
        let conn = Connection::open(&path).unwrap();
        conn.execute("INSERT INTO sessions VALUES ('s','m',1000,100,20,30,4,5,'p',1.25,'/work','/repo','prof')", []).unwrap();
        drop(conn);
        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.events.len(), 1);
        assert_eq!(events.events[0].tokens.uncached_input, 100);
        assert_eq!(events.events[0].tokens.cache_read, 30);
        assert_eq!(events.events[0].tokens.reasoning, 5);
        assert_eq!(events.events[0].session_id.as_deref(), Some("s"));
        assert_eq!(events.events[0].source_cost_usd, Some(1.25));
    }

    #[test]
    fn database_paths_with_uri_metacharacters_open_read_only() {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("profile#100%");
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("state?db");
        db(&path, false);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
            .unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES ('s','m',1000,1,0,0,0,0,'p',NULL,'/work','/repo','prof')",
            [],
        )
        .unwrap();

        let parsed = parse_usage_file(&path).unwrap();
        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].tokens.raw_input, 1);
        drop(conn);
    }

    #[cfg(unix)]
    #[test]
    fn wal_dependency_preserves_invalid_utf8_path_bytes_and_invalidates_on_change() {
        let temp = tempfile::tempdir().unwrap();
        let invalid_directory = std::ffi::OsString::from_vec(b"profile-\xFF".to_vec());
        let directory = temp.path().join(invalid_directory);
        if let Err(error) = fs::create_dir(&directory) {
            if error.kind() == std::io::ErrorKind::InvalidInput
                || error.raw_os_error() == Some(libc::EILSEQ)
            {
                return;
            }
            panic!("create invalid-UTF-8 directory: {error}");
        }
        let path = directory.join("state.db");
        db(&path, false);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
            .unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES ('s','m',1000,1,0,0,0,0,'p',NULL,'/work','/repo','prof')",
            [],
        )
        .unwrap();

        let parsed = parse_usage_file(&path).unwrap();
        let dependency = parsed.deps.first().unwrap();
        let mut wal_name = path.file_name().unwrap().to_os_string();
        wal_name.push("-wal");
        let wal = path.with_file_name(wal_name);
        assert!(wal.as_os_str().as_bytes().contains(&0xFF));
        assert_eq!(dependency.native_path, wal.as_os_str().as_bytes());
        assert!(dependency.is_current());

        fs::write(&wal, b"changed").unwrap();
        assert!(!dependency.is_current());
        drop(conn);
    }

    #[test]
    fn session_only_aggregate_without_cost_can_use_catalog_pricing() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session-only.db");
        db(&path, false);
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES ('s','claude-sonnet-4-6',1000,100,0,0,0,0,'anthropic',NULL,'/work','/repo','prof')",
            [],
        )
        .unwrap();
        drop(conn);

        let event = &parse_usage_file(&path).unwrap().events[0];
        assert!(!event.cost_authoritative);
        assert_eq!(
            crate::usage::event_cost_nanos(event, crate::usage::CostMode::Source),
            None
        );
        assert!(crate::usage::event_cost_nanos(event, crate::usage::CostMode::Auto).is_some());
    }

    #[test]
    fn positive_residual_without_session_cost_can_use_catalog_pricing() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("residual.db");
        db(&path, true);
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES ('s','claude-sonnet-4-6',1000,100,0,0,0,0,'anthropic',NULL,'/work','/repo','prof')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','claude-sonnet-4-6','anthropic','task',40,0,0,0,0,NULL,1000)",
            [],
        )
        .unwrap();
        drop(conn);

        let residual = parse_usage_file(&path)
            .unwrap()
            .events
            .into_iter()
            .find(|event| event.source_record_id.as_deref() == Some("session:s:residual"))
            .unwrap();
        assert!(!residual.cost_authoritative);
        assert_eq!(
            crate::usage::event_cost_nanos(&residual, crate::usage::CostMode::Source),
            None
        );
        assert!(crate::usage::event_cost_nanos(&residual, crate::usage::CostMode::Auto).is_some());
    }

    #[test]
    fn capped_mixed_rows_without_session_cost_can_use_catalog_pricing() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("capped-mixed.db");
        db(&path, true);
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES ('s','claude-sonnet-4-6',1000,100,0,0,0,0,'anthropic',NULL,'/work','/repo','prof')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','claude-sonnet-4-6','anthropic','priced',80,0,0,0,0,1.0,1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','claude-sonnet-4-6','anthropic','capped',80,0,0,0,0,NULL,1000)",
            [],
        )
        .unwrap();
        drop(conn);

        let events = parse_usage_file(&path).unwrap().events;
        let capped = events
            .iter()
            .find(|event| event.conservative_undercount)
            .unwrap();
        assert!(!capped.cost_authoritative);
        assert_eq!(
            crate::usage::event_cost_nanos(capped, crate::usage::CostMode::Source),
            None
        );
        assert!(crate::usage::event_cost_nanos(capped, crate::usage::CostMode::Auto).is_some());
    }

    #[test]
    fn read_transaction_keeps_aggregate_tables_on_one_wal_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        db(&path, true);
        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
            .unwrap();
        let reader = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let transaction = reader.unchecked_transaction().unwrap();
        assert_eq!(
            transaction
                .query_row("SELECT COUNT(*) FROM sessions", [], |row| row
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
        writer
            .execute(
                "INSERT INTO sessions VALUES ('s','m',1000,1,0,0,0,0,'p',NULL,'/work','/repo','prof')",
                [],
            )
            .unwrap();
        writer
            .execute(
                "INSERT INTO session_model_usage VALUES ('s','m','p','task',1,0,0,0,0,NULL,1000)",
                [],
            )
            .unwrap();
        assert_eq!(
            transaction
                .query_row("SELECT COUNT(*) FROM session_model_usage", [], |row| row
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
        transaction.commit().unwrap();
    }

    #[test]
    fn model_rows_count_once_and_residuals_are_non_negative() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        db(&path, true);
        let conn = Connection::open(&path).unwrap();
        conn.execute("INSERT INTO sessions VALUES ('s','fallback',1000,100,20,30,4,5,'p',10.0,'/work','/repo','prof')", []).unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','m','p','task',60,10,20,2,3,6.0,1000)",
            [],
        )
        .unwrap();
        drop(conn);
        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.events.len(), 2);
        assert_eq!(
            events.events[0].tokens.raw_input + events.events[1].tokens.raw_input,
            100
        );
        assert!(
            events
                .events
                .iter()
                .any(|event| event.conservative_undercount)
        );
    }

    #[test]
    fn inconsistent_model_costs_use_one_authoritative_session_fallback() {
        for (index, (aggregate_input, first_input, second_input, first_cost, residual)) in [
            (10, 4, 10, 12.0, 0), // zero residual, retained model cost over aggregate
            (10, 4, 10, 6.0, 0),  // zero residual, retained model cost under aggregate
            (10, 4, 1, 12.0, 5),  // positive residual, retained model cost over aggregate
            (10, 4, 1, 6.0, 5),   // positive residual, retained model cost under aggregate
        ]
        .into_iter()
        .enumerate()
        {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(format!("cost-{index}.db"));
            db(&path, true);
            let conn = Connection::open(&path).unwrap();
            conn.execute(
                "INSERT INTO sessions VALUES ('s','fallback',1000,?1,0,0,0,0,'p',10.0,'/work','/repo','prof')",
                [aggregate_input],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO session_model_usage VALUES ('s','m1','p','task-1',?1,0,0,0,0,?2,1000)",
                rusqlite::params![first_input, first_cost],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO session_model_usage VALUES ('s','m2','p','task-2',?1,?2,0,0,0,3.0,1000)",
                rusqlite::params![second_input, 1],
            )
            .unwrap();
            drop(conn);

            let events = parse_usage_file(&path).unwrap().events;
            assert!(events.iter().all(|event| {
                !event
                    .source_record_id
                    .as_deref()
                    .unwrap()
                    .starts_with("model:")
                    || event.source_cost_usd.is_none()
            }));
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.source_record_id.as_deref() == Some("session:s:residual"))
                    .count(),
                1
            );
            assert_eq!(
                events
                    .iter()
                    .find(|event| event.source_record_id.as_deref() == Some("session:s:residual"))
                    .unwrap()
                    .tokens
                    .raw_input,
                residual
            );
            assert_eq!(
                events
                    .iter()
                    .filter_map(|event| event.source_cost_usd)
                    .sum::<f64>(),
                10.0
            );
            for mode in [crate::usage::CostMode::Auto, crate::usage::CostMode::Source] {
                let total = events
                    .iter()
                    .filter_map(|event| crate::usage::event_cost_nanos(event, mode))
                    .sum::<u64>();
                assert_eq!(total, 10_000_000_000, "{mode:?}");
            }
            assert!(
                events
                    .iter()
                    .filter(|event| event
                        .source_record_id
                        .as_deref()
                        .unwrap()
                        .starts_with("model:"))
                    .all(|event| event.cost_authoritative)
            );
        }
    }

    #[test]
    fn exact_model_tokens_emit_cost_only_residual_for_uncovered_source_cost() {
        for (index, model_cost) in [None, Some(6.0)].into_iter().enumerate() {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(format!("exact-cost-{index}.db"));
            db(&path, true);
            let conn = Connection::open(&path).unwrap();
            conn.execute(
                "INSERT INTO sessions VALUES ('s','claude-sonnet-4-6',1000,10,0,0,0,0,'anthropic',10.0,'/work','/repo','prof')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO session_model_usage VALUES ('s','claude-sonnet-4-6','anthropic','task',10,0,0,0,0,?1,1000)",
                [model_cost],
            )
            .unwrap();
            drop(conn);

            let events = parse_usage_file(&path).unwrap().events;
            let residual = events
                .iter()
                .find(|event| event.source_record_id.as_deref() == Some("session:s:residual"))
                .expect("cost-only residual");
            assert!(is_zero(&residual.tokens));
            assert_eq!(
                residual.source_cost_usd,
                Some(model_cost.map_or(10.0, |_| 4.0))
            );
            assert_eq!(
                events
                    .iter()
                    .filter_map(|event| event.source_cost_usd)
                    .sum::<f64>(),
                10.0
            );
        }
    }

    #[test]
    fn authoritative_session_cost_reconciles_model_cost_matrix() {
        for (index, (session_cost, model_cost, expected_model_cost, expected_residual)) in [
            (None, Some(6.0), Some(6.0), None),
            (Some(0.0), Some(6.0), None, Some(0.0)),
            (Some(5.0), Some(6.0), None, Some(5.0)),
            (Some(6.0), Some(6.0), Some(6.0), None),
            (Some(10.0), Some(6.0), Some(6.0), Some(4.0)),
            (Some(10.0), None, None, Some(10.0)),
        ]
        .into_iter()
        .enumerate()
        {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(format!("cost-matrix-{index}.db"));
            db(&path, true);
            let conn = Connection::open(&path).unwrap();
            conn.execute(
                "INSERT INTO sessions VALUES ('s','fallback',1000,10,0,0,0,0,'p',?1,'/work','/repo','prof')",
                [session_cost],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO session_model_usage VALUES ('s','m','p','task',10,0,0,0,0,?1,1000)",
                [model_cost],
            )
            .unwrap();
            drop(conn);

            let events = parse_usage_file(&path).unwrap().events;
            let model = events
                .iter()
                .find(|event| {
                    event
                        .source_record_id
                        .as_deref()
                        .unwrap()
                        .starts_with("model:")
                })
                .unwrap();
            let residual = events
                .iter()
                .find(|event| event.source_record_id.as_deref() == Some("session:s:residual"));
            assert_eq!(model.tokens.raw_input, 10);
            assert_eq!(model.source_cost_usd, expected_model_cost);
            assert_eq!(
                model.cost_authoritative,
                session_cost.is_some() && expected_model_cost.is_none()
            );
            assert_eq!(
                residual.and_then(|event| event.source_cost_usd),
                expected_residual
            );
            assert_eq!(
                events
                    .iter()
                    .filter_map(|event| event.source_cost_usd)
                    .sum::<f64>(),
                session_cost.or(model_cost).unwrap_or(0.0)
            );
            if let Some(session_cost) = session_cost {
                for mode in [crate::usage::CostMode::Auto, crate::usage::CostMode::Source] {
                    let total = events
                        .iter()
                        .filter_map(|event| crate::usage::event_cost_nanos(event, mode))
                        .sum::<u64>();
                    assert_eq!(total, (session_cost * 1_000_000_000.0) as u64, "{mode:?}");
                }
            }
        }
    }

    #[test]
    fn valid_priced_model_followed_by_capped_row_reconciles_authoritative_cost() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("valid-priced-capped.db");
        db(&path, true);
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES ('s','fallback',1000,10,0,0,0,0,'p',5.0,'/work','/repo','prof')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','m1','p','priced',8,0,0,0,0,2.0,1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','m2','p','capped',8,0,0,0,0,NULL,1000)",
            [],
        )
        .unwrap();
        drop(conn);

        let events = parse_usage_file(&path).unwrap().events;
        assert!(
            events
                .iter()
                .filter(|event| event
                    .source_record_id
                    .as_deref()
                    .unwrap()
                    .starts_with("model:"))
                .all(|event| event.cost_authoritative)
        );
        for mode in [crate::usage::CostMode::Auto, crate::usage::CostMode::Source] {
            let total = events
                .iter()
                .filter_map(|event| crate::usage::event_cost_nanos(event, mode))
                .sum::<u64>();
            assert_eq!(total, 5_000_000_000, "{mode:?}");
        }
    }

    #[test]
    fn invalid_model_without_authoritative_cost_preserves_valid_model_costs() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("uncosted-invalid-model.db");
        db(&path, true);
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES ('s','fallback',1000,10,0,0,0,0,'p',NULL,'/work','/repo','prof')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','a-valid','p','valid',4,0,0,0,0,2.0,1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','z-capped','p','capped',10,0,0,0,0,3.0,1000)",
            [],
        )
        .unwrap();
        drop(conn);

        let events = parse_usage_file(&path).unwrap().events;
        let model_events: Vec<_> = events
            .iter()
            .filter(|event| {
                event
                    .source_record_id
                    .as_deref()
                    .is_some_and(|id| id.starts_with("model:"))
            })
            .collect();
        assert_eq!(model_events.len(), 2);
        let model_costs: Vec<_> = model_events
            .iter()
            .filter_map(|event| event.source_cost_usd)
            .collect();
        assert_eq!(model_costs, vec![2.0]);
        assert!(
            model_events
                .iter()
                .any(|event| event.tokens.raw_input == 6 && event.source_cost_usd.is_none())
        );
        assert_eq!(
            events
                .iter()
                .map(|event| event.tokens.raw_input)
                .sum::<u64>(),
            10
        );
    }

    #[test]
    fn zero_token_session_with_authoritative_cost_emits_cost_only_event() {
        for (index, session_cost) in [Some(0.0), Some(3.0)].into_iter().enumerate() {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(format!("zero-token-cost-{index}.db"));
            db(&path, false);
            let conn = Connection::open(&path).unwrap();
            conn.execute(
                "INSERT INTO sessions VALUES ('s','fallback',1000,0,0,0,0,0,'p',?1,'/work','/repo','prof')",
                [session_cost],
            )
            .unwrap();
            drop(conn);

            let events = parse_usage_file(&path).unwrap().events;
            assert_eq!(events.len(), 1);
            assert!(is_zero(&events[0].tokens));
            assert_eq!(events[0].source_cost_usd, session_cost);
        }
    }

    #[test]
    fn cost_over_authority_preserves_positive_token_residual() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("cost-over-authority-residual.db");
        db(&path, true);
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES ('s','fallback',1000,10,0,0,0,0,'p',5.0,'/work','/repo','prof')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','m','p','task',4,0,0,0,0,6.0,1000)",
            [],
        )
        .unwrap();
        drop(conn);

        let events = parse_usage_file(&path).unwrap().events;
        assert_eq!(
            events
                .iter()
                .map(|event| event.tokens.raw_input)
                .sum::<u64>(),
            10
        );
        assert_eq!(
            events
                .iter()
                .filter_map(|event| event.source_cost_usd)
                .sum::<f64>(),
            5.0
        );
    }

    #[test]
    fn zero_token_model_cost_is_not_attributed() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("zero-token-model-cost.db");
        db(&path, true);
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES ('s','fallback',1000,10,0,0,0,0,'p',10.0,'/work','/repo','prof')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','m1','p','emitted',10,0,0,0,0,6.0,1000)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','m2','p','zero',0,0,0,0,0,3.0,1000)",
            [],
        )
        .unwrap();
        drop(conn);

        let events = parse_usage_file(&path).unwrap().events;
        assert_eq!(
            events
                .iter()
                .filter_map(|event| event.source_cost_usd)
                .sum::<f64>(),
            10.0
        );
        assert_eq!(
            events
                .iter()
                .find(|event| event.source_record_id.as_deref() == Some("session:s:residual"))
                .unwrap()
                .source_cost_usd,
            Some(4.0)
        );
    }

    #[test]
    fn model_event_timestamp_falls_back_to_session_timestamp() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("model-timestamp.db");
        db(&path, true);
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES ('s','fallback',1720000000,10,0,0,0,0,'p',NULL,'/work','/repo','prof')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','m','p','task',10,0,0,0,0,NULL,NULL)",
            [],
        )
        .unwrap();
        drop(conn);

        let event = &parse_usage_file(&path).unwrap().events[0];
        assert_eq!(event.timestamp_ms, 1_720_000_000_000);
        assert_eq!(event.source_order, 1_720_000_000_000);
    }

    #[test]
    fn model_event_timestamp_falls_back_when_first_and_last_seen_are_omitted() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("model-timestamp-omitted.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE sessions (id TEXT, model TEXT, started_at INTEGER, input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER, billing_provider TEXT, estimated_cost_usd REAL, cwd TEXT, git_repo_root TEXT, profile_name TEXT); CREATE TABLE session_model_usage (session_id TEXT, model TEXT, billing_provider TEXT, task TEXT, input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER, estimated_cost_usd REAL);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES ('s','fallback',1720000000,10,0,0,0,0,'p',NULL,'/work','/repo','prof')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','m','p','task',10,0,0,0,0,NULL)",
            [],
        )
        .unwrap();
        drop(conn);

        let event = &parse_usage_file(&path).unwrap().events[0];
        assert_eq!(event.timestamp_ms, 1_720_000_000_000);
        assert_eq!(event.source_order, 1_720_000_000_000);
    }

    #[test]
    fn model_event_timestamp_falls_back_when_first_and_last_seen_are_null() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("model-timestamp-null.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE sessions (id TEXT, model TEXT, started_at INTEGER, input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER, billing_provider TEXT, estimated_cost_usd REAL, cwd TEXT, git_repo_root TEXT, profile_name TEXT); CREATE TABLE session_model_usage (session_id TEXT, model TEXT, billing_provider TEXT, task TEXT, input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER, estimated_cost_usd REAL, first_seen INTEGER, last_seen INTEGER);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES ('s','fallback',1720000000,10,0,0,0,0,'p',NULL,'/work','/repo','prof')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','m','p','task',10,0,0,0,0,NULL,NULL,NULL)",
            [],
        )
        .unwrap();
        drop(conn);

        let event = &parse_usage_file(&path).unwrap().events[0];
        assert_eq!(event.timestamp_ms, 1_720_000_000_000);
        assert_eq!(event.source_order, 1_720_000_000_000);
    }

    #[test]
    fn malformed_database_is_an_error_and_jsonl_is_not_discovered() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        fs::write(&path, b"not sqlite").unwrap();
        assert!(parse_usage_file(&path).is_err());
        fs::write(temp.path().join("usage.jsonl"), b"{}\n").unwrap();
        assert_eq!(discover_from_roots(&[temp.path().to_path_buf()]).len(), 1);
    }

    #[test]
    fn numeric_timestamps_are_epoch_milliseconds_and_sidecar_absence_is_tracked() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        db(&path, false);
        let conn = Connection::open(&path).unwrap();
        conn.execute("INSERT INTO sessions VALUES ('s','m',1720000000.125,1,1,0,0,0,'p',NULL,'/private','/private','profile')", []).unwrap();
        drop(conn);
        let parsed = parse_usage_file(&path).unwrap();
        assert_eq!(parsed.events[0].timestamp_ms, 1_720_000_000_125);
        assert_eq!(parsed.deps.len(), 1);
        assert!(parsed.deps.iter().all(|dependency| !dependency.exists));
        let wal = PathBuf::from(format!("{}-wal", path.to_string_lossy()));
        fs::write(&wal, b"wal").unwrap();
        let current = crate::sources::UsageDependency::from_path_or_absent(&wal);
        assert!(current.exists);
        assert!(
            !parsed
                .deps
                .iter()
                .any(|dependency| dependency.path == current.path && dependency.is_current())
        );
    }

    #[test]
    fn wal_change_during_read_makes_parse_output_non_cacheable() {
        let before = crate::sources::UsageDependency {
            path: "state.db-wal".to_string(),
            native_path: b"state.db-wal".to_vec(),
            size: 10,
            mtime_ns: 1,
            exists: true,
        };
        let after = crate::sources::UsageDependency {
            size: 20,
            ..before.clone()
        };
        assert!(!cacheable_after_wal_read(&before, &after));
        assert!(cacheable_after_wal_read(&before, &before));
    }

    #[test]
    fn model_identity_is_encoded_and_ordered_by_logical_dimensions() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        db(&path, true);
        let conn = Connection::open(&path).unwrap();
        conn.execute("INSERT INTO sessions VALUES ('s','fallback',1720000000,10,2,0,0,0,'p',3.0,'/private','/private','profile')", []).unwrap();
        conn.execute("INSERT INTO session_model_usage VALUES ('s','model:z','p','task:b',5,1,0,0,0,1.0,1720000000)", []).unwrap();
        conn.execute("INSERT INTO session_model_usage VALUES ('s','model:a','p','task:a',5,1,0,0,0,1.0,1720000000)", []).unwrap();
        drop(conn);
        let events = parse_usage_file(&path).unwrap().events;
        assert_eq!(events.len(), 3);
        assert!(
            events[0]
                .source_record_id
                .as_deref()
                .unwrap()
                .contains("model:a")
        );
        assert!(
            events[1]
                .source_record_id
                .as_deref()
                .unwrap()
                .contains("task:b")
        );
        assert_eq!(
            events[2].source_record_id.as_deref(),
            Some("session:s:residual")
        );
        assert!(is_zero(&events[2].tokens));
        assert_eq!(events[2].source_cost_usd, Some(1.0));
        assert_ne!(events[0].source_record_id, events[1].source_record_id);
    }

    #[test]
    fn disjoint_buckets_preserve_cache_beyond_input_and_reconcile_every_bucket() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        db(&path, true);
        let conn = Connection::open(&path).unwrap();
        conn.execute("INSERT INTO sessions VALUES ('s','fallback',1000,10,3,100,7,2,'p',NULL,'/work','/repo','prof')", []).unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','m','p','task',10,3,100,7,2,NULL,1000)",
            [],
        )
        .unwrap();
        drop(conn);
        let events = parse_usage_file(&path).unwrap().events;
        assert_eq!(events.len(), 1);
        let tokens = &events[0].tokens;
        assert_eq!(tokens.raw_input, 10);
        assert_eq!(tokens.uncached_input, 10);
        assert_eq!(tokens.cache_read, 100);
        assert_eq!(tokens.cache_write, 7);
        assert_eq!(tokens.output, 3);
        assert_eq!(tokens.reasoning, 2);
        assert_eq!(tokens.total(), 120);
    }

    #[test]
    fn default_discovery_keeps_all_profiles_when_home_points_at_one_profile() {
        let temp = tempfile::tempdir().unwrap();
        for relative in [
            ".hermes/state.db",
            ".hermes/profiles/main/state.db",
            ".hermes/profiles/builder/state.db",
            ".hermes/profiles/researcher/state.db",
        ] {
            let path = temp.path().join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"x").unwrap();
        }
        let roots = profile_roots_for(
            temp.path(),
            None,
            Some(&temp.path().join(".hermes/profiles/main")),
            None,
            None,
        );
        let files = discover_from_roots(&roots);
        assert_eq!(files.len(), 4);
        assert_eq!(
            files
                .iter()
                .map(|file| file.path.strip_prefix(temp.path()).unwrap().to_path_buf())
                .collect::<Vec<_>>(),
            [
                PathBuf::from(".hermes/profiles/builder/state.db"),
                PathBuf::from(".hermes/profiles/main/state.db"),
                PathBuf::from(".hermes/profiles/researcher/state.db"),
                PathBuf::from(".hermes/state.db"),
            ]
        );
    }

    #[test]
    fn aggregate_fixture_matrix_matches_each_database_sessions_exactly_once() {
        let temp = tempfile::tempdir().unwrap();
        let expected = [
            (10, 100, 7, 3, 2),
            (20, 30, 4, 5, 1),
            (6, 8, 2, 9, 4),
            (11, 13, 1, 2, 0),
        ];
        let mut total = TokenBuckets::default();
        for (index, (input, cache_read, cache_write, output, reasoning)) in
            expected.iter().enumerate()
        {
            let path = temp.path().join(format!("db-{index}.db"));
            db(&path, true);
            let conn = Connection::open(&path).unwrap();
            conn.execute("INSERT INTO sessions VALUES ('s','m',1000,?1,?2,?3,?4,?5,'p',NULL,'/work','/repo','prof')", rusqlite::params![input, output, cache_read, cache_write, reasoning]).unwrap();
            conn.execute("INSERT INTO session_model_usage VALUES ('s','m','p','task',?1,?2,?3,?4,?5,NULL,1000)", rusqlite::params![input, output, cache_read, cache_write, reasoning]).unwrap();
            drop(conn);
            let events = parse_usage_file(&path).unwrap().events;
            let mut actual = TokenBuckets::default();
            for event in events {
                actual = add(actual, &event.tokens);
            }
            let session = TokenBuckets::disjoint(*input, *cache_read, *cache_write, *output);
            let mut session = session;
            session.reasoning = *reasoning;
            assert_eq!(actual, session);
            total = add(total, &actual);
        }
        assert_eq!(total.raw_input, 47);
        assert_eq!(total.uncached_input, 47);
        assert_eq!(total.cache_read, 151);
        assert_eq!(total.cache_write, 14);
        assert_eq!(total.output, 19);
        assert_eq!(total.reasoning, 7);
        assert_eq!(total.total(), 231);
    }

    #[test]
    fn reconciliation_has_independent_positive_residual_and_overaggregate_caps() {
        let cases = [
            ((10, 100, 7, 3, 2), (4, 60, 2, 1, 1), (6, 40, 5, 2, 1)),
            ((10, 100, 7, 3, 2), (20, 200, 10, 5, 4), (0, 0, 0, 0, 0)),
        ];
        for (index, (session, model, residual)) in cases.into_iter().enumerate() {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(format!("case-{index}.db"));
            db(&path, true);
            let conn = Connection::open(&path).unwrap();
            conn.execute("INSERT INTO sessions VALUES ('s','m',1000,?1,?2,?3,?4,?5,'p',NULL,'/work','/repo','prof')", rusqlite::params![session.0, session.3, session.1, session.2, session.4]).unwrap();
            conn.execute("INSERT INTO session_model_usage VALUES ('s','m','p','task',?1,?2,?3,?4,?5,NULL,1000)", rusqlite::params![model.0, model.3, model.1, model.2, model.4]).unwrap();
            drop(conn);
            let events = parse_usage_file(&path).unwrap().events;
            assert_eq!(events.len(), if residual.0 == 0 { 1 } else { 2 });
            let mut actual = TokenBuckets::default();
            for event in events {
                actual = add(actual, &event.tokens);
            }
            let expected = TokenBuckets {
                raw_input: session.0,
                uncached_input: session.0,
                cache_read: session.1,
                cache_write: session.2,
                cache_write_1h: 0,
                output: session.3,
                reasoning: session.4,
            };
            assert_eq!(actual, expected);
            assert_eq!(
                actual.total(),
                session.0 + session.1 + session.2 + session.3
            );
        }
    }

    #[test]
    fn explicit_profile_roots_are_the_only_discovery_scope() {
        let temp = tempfile::tempdir().unwrap();
        let explicit = temp.path().join("explicit");
        let other = temp.path().join("other");
        for path in [explicit.join("state.db"), other.join("state.db")] {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"x").unwrap();
        }
        let roots = profile_roots_for(
            temp.path(),
            Some(explicit.as_os_str()),
            Some(&other),
            Some(&other),
            Some(&other),
        );
        assert_eq!(roots, vec![explicit]);
    }
}
