//! Antigravity conversation discovery and indexing.
//!
//! Antigravity keeps two human-readable projections alongside the encrypted
//! `.pb` trajectories, neither of which needs decryption:
//!
//! * `conversations/<uuid>.db` — per-conversation SQLite stores (default
//!   `~/.gemini/antigravity-ide`). The `steps` table orders the plaintext
//!   twin of the encrypted trajectory by `idx`; each `step_payload` is a
//!   protobuf message of the same "step" shape as the decrypted `.pb` steps:
//!   * `step_type 14`  - user message (text at payload `19.f2`).
//!   * `step_type 23`  - model response (text at payload `30.f4`).
//!   * `step_type 15`  - model reasoning (text at payload `20.f3`).
//!   * `step_type 5/7/8/9/17/21/101/132` - tool executions (name at
//!     `5.f4.2`, JSON args at `5.f4.3`, `toolAction`/`toolSummary` inside).
//!   * timestamps at payload `5.1` as `{1: seconds, 2: nanos}`.
//! * `brain/*/.system_generated/logs/overview.txt` — JSONL activity logs with
//!   `USER_INPUT`, `PLANNER_RESPONSE`, `RUN_COMMAND`, `VIEW_FILE` and
//!   `CODE_ACTION` records; this also covers the legacy `~/.gemini/antigravity`
//!   profile, which only writes encrypted `.pb` files plus overview logs.
//!
//! A `.db` store is rewritten wholesale as the conversation grows, so the
//! parser replays the whole file every time (like jcode) and `prepare_file_task`
//! pairs it with atomic delete-first re-parses.

use super::{IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions, SourceFile};
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::UsageEvent;
use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use walkdir::WalkDir;

pub const VERSIONS: ParserVersions = ParserVersions {
    // Rebuild analytics metadata with decoded project directory URLs.
    identity: 3,
    // Bumped whenever record extraction logic changes; forces a full re-parse.
    index: 2,
    usage: 1,
};

/// The tool-bearing `step_type` values observed in real stores.
const TOOL_STEP_TYPES: &[u64] = &[5, 7, 8, 9, 17, 21, 101, 132];

/// Profiles searched for `conversations/` stores and `brain/` overview logs.
const PROFILES: &[&str] = &["antigravity-ide", "antigravity"];

fn is_wal_or_shm(name: &str) -> bool {
    name.ends_with("-wal.db") || name.ends_with("-shm.db") || name.ends_with(".tmp")
}

pub fn matches_path(path: &str) -> bool {
    let normalized = path.replace('\\', "/");
    let in_gemini = normalized.contains(".gemini/") || normalized.contains("antigravity");
    if !in_gemini {
        return false;
    }
    if normalized.ends_with(".db") || normalized.ends_with(".pb") {
        return normalized.contains("conversations/") && !is_wal_or_shm(&normalized);
    }
    normalized.ends_with("overview.txt") && normalized.contains(".system_generated/logs/")
}

/// Root of all Antigravity profiles: `~/.gemini` by default.
pub fn sessions_root() -> PathBuf {
    std::env::var_os("ANTIGRAVITY_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| super::common::home().join(".gemini"))
}

pub(crate) fn is_db_path(path: &Path) -> bool {
    path.extension().and_then(|ext| ext.to_str()) == Some("db")
        && !is_wal_or_shm(path.file_name().and_then(|n| n.to_str()).unwrap_or(""))
}

fn is_overview_path(path: &Path) -> bool {
    path.file_name().and_then(|n| n.to_str()) == Some("overview.txt")
}

pub fn discover() -> Vec<SourceFile> {
    let base = sessions_root();
    let mut files = Vec::new();
    for profile in PROFILES {
        let conversations = base.join(profile).join("conversations");
        if conversations.is_dir()
            && let Ok(entries) = std::fs::read_dir(&conversations)
        {
            for entry in entries.flatten() {
                let path = entry.path();
                if is_db_path(&path) {
                    files.push(SourceFile {
                        source: SourceKind::Antigravity,
                        path,
                    });
                }
            }
        }
        let brains = base.join(profile).join("brain");
        if brains.is_dir() {
            for entry in WalkDir::new(&brains).into_iter().flatten() {
                let path = entry.path();
                if entry.file_type().is_file()
                    && is_overview_path(path)
                    && path.to_string_lossy().contains(".system_generated/logs/")
                {
                    files.push(SourceFile {
                        source: SourceKind::Antigravity,
                        path: path.to_path_buf(),
                    });
                }
            }
        }
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    files.dedup_by(|a, b| a.path == b.path);
    files
}

pub fn usage_files() -> Vec<PathBuf> {
    discover().into_iter().map(|file| file.path).collect()
}

pub(crate) fn parse_usage_file(_path: &Path) -> Result<Vec<UsageEvent>> {
    // Token counts are embedded in the protobuf step payloads but their exact
    // buckets are not yet validated; reporting none beats reporting wrong
    // numbers. Refine once real usage can be cross-checked.
    Ok(Vec::new())
}

pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    // Whole-document formats: `prepare_file_task` pairs these with delete-first
    // re-parses, so `state.offset` is intentionally ignored below.
    let source_path = path.to_string_lossy().to_string();
    let mut diagnostics = ParseDiagnostics::default();
    let (session_id, turn_id, offset) = if is_db_path(path) {
        index_db_file(
            path,
            include_reasoning,
            next_doc_id,
            &mut emit,
            &source_path,
            &mut diagnostics,
            state.turn_id,
        )?
    } else if is_overview_path(path) {
        index_overview_file(
            path,
            next_doc_id,
            &mut emit,
            &source_path,
            &mut diagnostics,
            state.turn_id,
        )?
    } else {
        anyhow::bail!(
            "unsupported antigravity file {} (expected a conversation .db or overview.txt)",
            path.display()
        );
    };
    Ok(IndexParseOutput {
        offset,
        turn_id,
        legacy_turn_id: None,
        pending_tool_calls: state.pending_tool_calls,
        session_id: Some(session_id),
        diagnostics,
    })
}

fn index_db_file(
    path: &Path,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    emit: &mut impl FnMut(Record) -> Result<()>,
    source_path: &str,
    diagnostics: &mut ParseDiagnostics,
    start_turn_id: u32,
) -> Result<(String, u32, u64)> {
    let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open antigravity store {}", path.display()))?;
    let store_id = |query: &str| {
        conn.prepare(query).ok().and_then(|mut stmt| {
            stmt.query_row([], |row| row.get::<_, Option<String>>(0))
                .ok()
                .flatten()
        })
    };
    let session_id = store_id("SELECT trajectory_id FROM trajectory_meta LIMIT 1")
        .or_else(|| store_id("SELECT cascade_id FROM trajectory_meta LIMIT 1"))
        .or_else(|| {
            path.file_stem()
                .and_then(|name| name.to_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "unknown".to_string());

    // Project heuristic: the user payload carries the active project root as a
    // `file://` URL (payload `19.4.2.*.13`); take its leaf directory name.
    let mut project: Option<String> = None;

    let mut stmt = conn
        .prepare("SELECT step_type, status, step_payload FROM steps ORDER BY idx")
        .with_context(|| format!("query steps in {}", path.display()))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<Vec<u8>>>(2)?.unwrap_or_default(),
            ))
        })
        .with_context(|| format!("read steps in {}", path.display()))?;

    let file_len = std::fs::metadata(path)?.len();
    let mut turn_id = start_turn_id;
    for row in rows {
        let (step_type, _status, payload) =
            row.with_context(|| format!("iterate steps in {}", path.display()))?;
        if payload.is_empty() {
            continue;
        }
        let step_type = step_type.max(0) as u64;
        let ts = step_timestamp(&payload);
        let message_id = step_message_id(&payload);
        if project.is_none() && step_type == 14 {
            project = project_from_user_payload(&payload);
        }
        match step_type {
            14 => {
                let Some(text) = user_text(&payload) else {
                    continue;
                };
                let mut links = RecordLinks::default();
                if let Some(ref id) = message_id {
                    links.event_id = Some(id.clone());
                }
                emit(Record {
                    source: SourceKind::Antigravity,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts,
                    project: project
                        .clone()
                        .unwrap_or_else(|| SourceKind::Antigravity.label().to_string()),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "user".to_string(),
                    text,
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links,
                    source_path: source_path.to_string(),
                })?;
                turn_id += 1;
            }
            23 => {
                let Some(text) = model_text(&payload) else {
                    continue;
                };
                let mut links = RecordLinks::default();
                if let Some(ref id) = message_id {
                    links.event_id = Some(id.clone());
                }
                emit(Record {
                    source: SourceKind::Antigravity,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts,
                    project: project
                        .clone()
                        .unwrap_or_else(|| SourceKind::Antigravity.label().to_string()),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "assistant".to_string(),
                    text,
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links,
                    source_path: source_path.to_string(),
                })?;
                turn_id += 1;
            }
            15 => {
                if !include_reasoning {
                    continue;
                }
                let Some(text) = reasoning_text(&payload) else {
                    continue;
                };
                let mut links = RecordLinks::default();
                if let Some(ref id) = message_id {
                    links.event_id = Some(format!("{id}:reasoning"));
                }
                emit(Record {
                    source: SourceKind::Antigravity,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts,
                    project: project
                        .clone()
                        .unwrap_or_else(|| SourceKind::Antigravity.label().to_string()),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "reasoning".to_string(),
                    text,
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links,
                    source_path: source_path.to_string(),
                })?;
                turn_id += 1;
            }
            _ if TOOL_STEP_TYPES.contains(&step_type) => {
                let Some((tool_name, args)) = tool_call(&payload) else {
                    continue;
                };
                let (action, summary) = tool_action_summary(&args);
                // Arguments are stored but not indexed, so a summary-only `text` would put
                // them out of reach of search. Lead with the summary for display and carry
                // the arguments after it.
                let text = match summary.clone().or(action) {
                    Some(label) if label == args => label,
                    Some(label) => format!("{label}\n{args}"),
                    None => args.clone(),
                };
                let mut links = RecordLinks::default();
                if let Some(ref id) = message_id {
                    links.event_id = Some(id.clone());
                }
                emit(Record {
                    source: SourceKind::Antigravity,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts,
                    project: project
                        .clone()
                        .unwrap_or_else(|| SourceKind::Antigravity.label().to_string()),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "tool_use".to_string(),
                    text,
                    tool_name: Some(tool_name),
                    tool_input: Some(args),
                    tool_output: summary,
                    links,
                    source_path: source_path.to_string(),
                })?;
                turn_id += 1;
            }
            // System/status notes (e.g. step_type 90) are ephemeral and skipped.
            90 => {}
            other => diagnostics.increment_unknown_top_level(&format!("step_type_{other}")),
        }
    }

    Ok((session_id, turn_id, file_len))
}

fn index_overview_file(
    path: &Path,
    next_doc_id: &AtomicU64,
    emit: &mut impl FnMut(Record) -> Result<()>,
    source_path: &str,
    diagnostics: &mut ParseDiagnostics,
    start_turn_id: u32,
) -> Result<(String, u32, u64)> {
    let text = std::fs::read_to_string(path)?;
    let file_len = text.len() as u64;
    let session_id = path.to_string_lossy().to_string();
    let mut turn_id = start_turn_id;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            diagnostics.malformed_json_lines += 1;
            continue;
        };
        let Some(kind) = value.get("type").and_then(|v| v.as_str()) else {
            diagnostics.non_object_json_lines += 1;
            continue;
        };
        let status = value.get("status").and_then(|v| v.as_str()).unwrap_or("");
        let ts = value
            .get("created_at")
            .and_then(|v| v.as_str())
            .and_then(super::common::parse_iso_millis)
            .unwrap_or(0);
        let content = value
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if content.is_empty() && kind != "RUN_COMMAND" {
            continue;
        }
        match kind {
            "USER_INPUT" if status == "DONE" => {
                emit(Record {
                    source: SourceKind::Antigravity,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts,
                    project: SourceKind::Antigravity.label().to_string(),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "user".to_string(),
                    text: content,
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: RecordLinks::default(),
                    source_path: source_path.to_string(),
                })?;
                turn_id += 1;
            }
            "PLANNER_RESPONSE" => {
                emit(Record {
                    source: SourceKind::Antigravity,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts,
                    project: SourceKind::Antigravity.label().to_string(),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "assistant".to_string(),
                    text: content,
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: RecordLinks::default(),
                    source_path: source_path.to_string(),
                })?;
                turn_id += 1;
            }
            "RUN_COMMAND" | "VIEW_FILE" | "CODE_ACTION" => {
                let tool_input = if content.is_empty() {
                    value.get("tool_calls").map(|v| v.to_string())
                } else {
                    Some(content)
                };
                let Some(tool_input) = tool_input else {
                    continue;
                };
                emit(Record {
                    source: SourceKind::Antigravity,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts,
                    project: SourceKind::Antigravity.label().to_string(),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "tool_use".to_string(),
                    text: tool_input.clone(),
                    tool_name: Some(kind.to_lowercase()),
                    tool_input: Some(tool_input),
                    tool_output: None,
                    links: RecordLinks::default(),
                    source_path: source_path.to_string(),
                })?;
                turn_id += 1;
            }
            other => diagnostics.increment_unknown_top_level(other),
        }
    }
    Ok((session_id, turn_id, file_len))
}

/// Project name from a user step payload: the first `file://` project root at
/// payload `19.4.2.*.13`. Returns the referenced path's leaf directory (the
/// repo dir).
fn project_from_user_payload(payload: &[u8]) -> Option<String> {
    let url = project_root_from_payload(payload)?;
    parse_file_url_leaf(&url)
}

/// The first `file://` project root at payload `19.4.2.*.13`, if any. Public for
/// cross-module use (e.g. transfer's cwd resolution).
pub(crate) fn project_root_from_payload(payload: &[u8]) -> Option<String> {
    let fields = parse(payload)?;
    fields
        .into_iter()
        .find(|field| field.field == 19)
        .as_ref()
        .and_then(|field| field.bytes.as_deref())
        .and_then(parse)
        .and_then(|user| {
            let mut roots = Vec::new();
            for field in user {
                if field.field == 4
                    && let Some(blocks) = field.bytes.as_deref()
                    && let Some(block_fields) = parse(blocks)
                {
                    for block in block_fields {
                        if block.field == 2
                            && let Some(file_fields) = block.bytes.as_deref()
                            && let Some(file_fields) = parse(file_fields)
                        {
                            for file in file_fields {
                                if file.field == 13
                                    && let Some(url) = file.bytes_string()
                                {
                                    roots.push(url);
                                }
                            }
                        }
                    }
                }
            }
            roots.into_iter().next()
        })
}

/// Best-effort working directory for an Antigravity conversation store.
///
/// Antigravity records the active project root as a `file://` URL on user
/// steps. This is used by analytics to resolve the enclosing Git repository.
pub(crate) fn session_cwd(path: &Path) -> Option<PathBuf> {
    if !is_db_path(path) {
        return None;
    }
    let conn =
        Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).ok()?;
    let mut stmt = conn
        .prepare("SELECT step_payload FROM steps WHERE step_type = 14 ORDER BY idx")
        .ok()?;
    let rows = stmt
        .query_map([], |row| row.get::<_, Option<Vec<u8>>>(0))
        .ok()?;
    for payload in rows.flatten().flatten() {
        let Some(url) = project_root_from_payload(&payload) else {
            continue;
        };
        if let Some(root) = file_url_path(&url) {
            return Some(root);
        }
    }
    None
}

fn file_url_path(url: &str) -> Option<PathBuf> {
    url::Url::parse(url).ok()?.to_file_path().ok()
}

/// Decode a `file://` URL and return the referenced path's leaf directory.
fn parse_file_url_leaf(url: &str) -> Option<String> {
    file_url_path(url)?
        .file_name()
        .and_then(|n| n.to_str())
        .map(str::to_string)
}

fn user_text(payload: &[u8]) -> Option<String> {
    // payload `19.f2`
    parse(payload)?
        .into_iter()
        .find(|field| field.field == 19)
        .and_then(|field| field.bytes.as_deref().and_then(parse))
        .and_then(|sub| {
            sub.into_iter()
                .find(|field| field.field == 2)
                .and_then(|field| field.bytes_string())
        })
}

fn reasoning_text(payload: &[u8]) -> Option<String> {
    // payload `20.f3`
    parse(payload)?
        .into_iter()
        .find(|field| field.field == 20)
        .and_then(|field| field.bytes.as_deref().and_then(parse))
        .and_then(|sub| {
            sub.into_iter()
                .find(|field| field.field == 3)
                .and_then(|field| field.bytes_string())
        })
}

fn model_text(payload: &[u8]) -> Option<String> {
    // payload `30.f4`
    parse(payload)?
        .into_iter()
        .find(|field| field.field == 30)
        .and_then(|field| field.bytes.as_deref().and_then(parse))
        .and_then(|sub| {
            sub.into_iter()
                .find(|field| field.field == 4)
                .and_then(|field| field.bytes_string())
        })
}

fn tool_call(payload: &[u8]) -> Option<(String, String)> {
    // payload `5.f4.{2: name, 3: args-json}`
    let fields = parse(payload)?;
    let tool_field = fields.into_iter().find(|field| field.field == 5)?;
    let tool_bytes = tool_field.bytes?;
    let tool_fields = parse(&tool_bytes)?;
    let detail_field = tool_fields.into_iter().find(|field| field.field == 4)?;
    let detail_bytes = detail_field.bytes?;
    let detail = parse(&detail_bytes)?;
    let mut name = None;
    let mut args = None;
    for field in detail {
        match field.field {
            2 => name = field.bytes_string(),
            3 => args = field.bytes_string(),
            _ => {}
        }
    }
    Some((name?, args?))
}

fn tool_action_summary(args: &str) -> (Option<String>, Option<String>) {
    let Ok(value) = serde_json::from_str::<Value>(args) else {
        return (None, None);
    };
    let action = value
        .get("toolAction")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let summary = value
        .get("toolSummary")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    (action, summary)
}

/// Message timestamp from the step payload: `5.1.{1: seconds, 2: nanos}` as
/// epoch milliseconds.
fn step_timestamp(payload: &[u8]) -> u64 {
    let Some(fields) = parse(payload) else {
        return 0;
    };
    let Some(meta_field) = fields.into_iter().find(|field| field.field == 5) else {
        return 0;
    };
    let Some(meta_bytes) = meta_field.bytes else {
        return 0;
    };
    let Some(meta) = parse(&meta_bytes) else {
        return 0;
    };
    let Some(time_field) = meta.into_iter().find(|field| field.field == 1) else {
        return 0;
    };
    let Some(time_bytes) = time_field.bytes else {
        return 0;
    };
    let Some(time) = parse(&time_bytes) else {
        return 0;
    };
    let mut seconds = 0u64;
    let mut nanos = 0u64;
    for field in time {
        match field.field {
            1 => seconds = field.varint.unwrap_or(0),
            2 => nanos = field.varint.unwrap_or(0),
            _ => {}
        }
    }
    seconds
        .saturating_mul(1000)
        .saturating_add(nanos / 1_000_000)
}

/// Message UUID embedded in the step metadata (`5.12`), used as the record
/// event id so searches can link back to the originating message.
fn step_message_id(payload: &[u8]) -> Option<String> {
    let fields = parse(payload)?;
    let meta_field = fields.into_iter().find(|field| field.field == 5)?;
    let meta_bytes = meta_field.bytes?;
    let meta = parse(&meta_bytes)?;
    meta.into_iter()
        .find(|field| field.field == 12)
        .and_then(|field| field.bytes_string())
}

// --- bare-minimum protobuf wire reader (varint + length-delimited) ---

struct Field {
    field: u64,
    varint: Option<u64>,
    bytes: Option<Vec<u8>>,
}

impl Field {
    fn bytes_string(self) -> Option<String> {
        let bytes = self.bytes.as_ref()?;
        String::from_utf8(bytes.clone()).ok()
    }
}

fn parse(data: &[u8]) -> Option<Vec<Field>> {
    let mut fields = Vec::new();
    let mut pos = 0usize;
    while pos < data.len() {
        let (tag, next) = read_varint(data, pos)?;
        pos = next;
        let field = tag >> 3;
        match tag & 7 {
            0 => {
                let (value, next) = read_varint(data, pos)?;
                pos = next;
                fields.push(Field {
                    field,
                    varint: Some(value),
                    bytes: None,
                });
            }
            2 => {
                let (len, next) = read_varint(data, pos)?;
                pos = next;
                let len = len as usize;
                let end = pos.checked_add(len)?;
                if end > data.len() {
                    return None;
                }
                fields.push(Field {
                    field,
                    varint: None,
                    bytes: Some(data[pos..end].to_vec()),
                });
                pos = end;
            }
            1 => pos = pos.checked_add(8)?,
            5 => pos = pos.checked_add(4)?,
            _ => return None,
        }
    }
    Some(fields)
}

fn read_varint(data: &[u8], mut pos: usize) -> Option<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0;
    loop {
        let byte = *data.get(pos)?;
        pos += 1;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((value, pos));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{EnvVarGuard, env_lock};
    use std::fs;

    fn field_varint(field: u64, value: u64) -> Vec<u8> {
        let mut out = encode_varint(field << 3);
        out.extend(encode_varint(value));
        out
    }

    fn field_bytes(field: u64, bytes: &[u8]) -> Vec<u8> {
        let mut out = encode_varint((field << 3) | 2);
        out.extend(encode_varint(bytes.len() as u64));
        out.extend(bytes);
        out
    }

    fn encode_varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }

    fn timestamp_msg(seconds: u64, nanos: u64) -> Vec<u8> {
        let mut out = field_varint(1, seconds);
        out.extend(field_varint(2, nanos));
        out
    }

    fn user_step(prompt: &str) -> Vec<u8> {
        let mut msg = field_varint(1, 14);
        let mut meta = field_bytes(1, &timestamp_msg(1_780_894_247, 226_543_000));
        meta.extend(field_bytes(12, b"user-msg-uuid"));
        msg.extend(field_bytes(5, &meta));
        msg.extend(field_bytes(19, &field_bytes(2, prompt.as_bytes())));
        msg
    }

    fn model_step(response: &str) -> Vec<u8> {
        let mut msg = field_varint(1, 23);
        let meta = field_bytes(1, &timestamp_msg(1_780_894_248, 918_951_000));
        msg.extend(field_bytes(5, &meta));
        msg.extend(field_bytes(30, &field_bytes(4, response.as_bytes())));
        msg
    }

    fn reasoning_step(thinking: &str) -> Vec<u8> {
        let mut msg = field_varint(1, 15);
        let meta = field_bytes(1, &timestamp_msg(1_780_894_249, 100_000_000));
        msg.extend(field_bytes(5, &meta));
        msg.extend(field_bytes(20, &field_bytes(3, thinking.as_bytes())));
        msg
    }

    fn tool_step() -> Vec<u8> {
        let mut msg = field_varint(1, 8);
        let mut meta = field_bytes(1, &timestamp_msg(1_780_894_250, 0));
        let mut detail = field_bytes(2, b"bash");
        detail.extend(field_bytes(3, br#"{"command":"ls"}"#));
        meta.extend(field_bytes(4, &detail));
        msg.extend(field_bytes(5, &meta));
        msg
    }

    fn write_store(dir: &Path) -> PathBuf {
        fs::create_dir_all(dir.join("antigravity-ide/conversations")).unwrap();
        let db = dir.join("antigravity-ide/conversations/conv-1.db");
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE trajectory_meta (trajectory_id TEXT, cascade_id TEXT);
             CREATE TABLE steps (
                idx INTEGER PRIMARY KEY,
                step_type INTEGER NOT NULL DEFAULT 0,
                status INTEGER NOT NULL DEFAULT 0,
                has_subtrajectory NUMERIC NOT NULL DEFAULT false,
                step_payload BLOB
             );
             INSERT INTO trajectory_meta (trajectory_id, cascade_id) VALUES ('traj-1', 'cascade-1');",
        )
        .unwrap();
        let insert = |idx: i64, step_type: i64, payload: &[u8]| {
            conn.execute(
                "INSERT INTO steps (idx, step_type, status, has_subtrajectory, step_payload)
                 VALUES (?1, ?2, 3, false, ?3)",
                rusqlite::params![idx, step_type, payload],
            )
            .unwrap();
        };
        insert(
            0,
            14,
            &user_step("@[file:///Users/x/src/repo-api/README.md] do it"),
        );
        insert(1, 15, &reasoning_step("thinking hard"));
        insert(2, 23, &model_step("response text"));
        insert(3, 8, &tool_step());
        db
    }

    fn emit_collect(path: &Path, include_reasoning: bool) -> (Vec<Record>, IndexParseOutput) {
        let next_doc_id = AtomicU64::new(0);
        let mut records = Vec::new();
        let output = parse_index_records(
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
        (records, output)
    }

    #[test]
    fn parses_stores_users_models_and_tools() {
        let temp = tempfile::tempdir().unwrap();
        let db = write_store(temp.path());
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("ANTIGRAVITY_HOME", Some(temp.path().to_str().unwrap()))]);
        let (records, output) = emit_collect(&db, false);

        assert_eq!(output.session_id.as_deref(), Some("traj-1"));
        assert_eq!(records.len(), 3); // user, model, tool; reasoning skipped
        let user = &records[0];
        assert_eq!(user.role, "user");
        assert_eq!(user.text, "@[file:///Users/x/src/repo-api/README.md] do it");
        assert_eq!(user.ts, 1_780_894_247_226);
        // Project from the `file://` project root at 19.4.<i>.2.<i>.13;
        // this fixture has no _13 root field, so it falls back to "antigravity".
        assert_eq!(user.project, "antigravity");
        assert_eq!(records[1].role, "assistant");
        assert_eq!(records[1].text, "response text");
        assert_eq!(records[2].role, "tool_use");
        assert_eq!(records[2].tool_name.as_deref(), Some("bash"));
        assert!(
            records[2]
                .tool_input
                .as_deref()
                .unwrap()
                .contains("\"command\"")
        );

        // Reasoning appears only with include_reasoning.
        let (with_reasoning, _) = emit_collect(&db, true);
        let reasoning = with_reasoning
            .iter()
            .find(|record| record.role == "reasoning")
            .expect("reasoning record");
        assert_eq!(reasoning.text, "thinking hard");
        assert_eq!(reasoning.ts, 1_780_894_249_100);
    }

    #[test]
    fn project_looks_for_file_project_root_field() {
        // Index under the 19.4.2.13 shaped envelope.
        // Build: 19 { 4 { 2 { 13: "file:///Users/x/src/repo-api" } } } merged with text.
        let project_block = field_bytes(13, b"file:///Users/x/src/repo-api");
        let inner2 = field_bytes(2, &project_block);
        let inner4 = field_bytes(4, &inner2);
        let mut msg = field_varint(1, 14);
        msg.extend(field_bytes(5, &field_bytes(1, &timestamp_msg(1, 0))));
        msg.extend(field_bytes(19, &inner4));
        assert_eq!(project_from_user_payload(&msg).as_deref(), Some("repo-api"));
    }

    #[test]
    fn all_tool_step_types_emit_tool_use_records() {
        let temp = tempfile::tempdir().unwrap();
        let db = write_store(temp.path());
        let conn = Connection::open(&db).unwrap();
        conn.execute("DELETE FROM steps", []).unwrap();
        for (idx, step_type) in TOOL_STEP_TYPES.iter().enumerate() {
            conn.execute(
                "INSERT INTO steps (idx, step_type, step_payload) VALUES (?1, ?2, ?3)",
                rusqlite::params![idx as i64, *step_type as i64, tool_step()],
            )
            .unwrap();
        }
        let (records, _) = emit_collect(&db, false);
        assert_eq!(records.len(), TOOL_STEP_TYPES.len());
        for record in records {
            assert_eq!(record.role, "tool_use");
            assert_eq!(record.tool_name.as_deref(), Some("bash"));
            assert_eq!(record.tool_input.as_deref(), Some(r#"{"command":"ls"}"#));
        }
    }

    #[test]
    fn project_and_cwd_decode_file_urls() {
        let temp = tempfile::tempdir().unwrap();
        let db = write_store(temp.path());
        let conn = Connection::open(&db).unwrap();
        let cwd = temp.path().join("My project #1 café%done");
        fs::create_dir(&cwd).unwrap();
        let url = url::Url::from_directory_path(&cwd).unwrap();
        let mut user = field_bytes(2, b"hello");
        user.extend(field_bytes(
            4,
            &field_bytes(2, &field_bytes(13, url.as_str().as_bytes())),
        ));
        let payload = field_bytes(19, &user);
        conn.execute(
            "UPDATE steps SET step_payload = ?1 WHERE idx = 0",
            rusqlite::params![payload],
        )
        .unwrap();

        assert_eq!(session_cwd(&db).as_deref(), Some(cwd.as_path()));
        assert!(session_cwd(&db).unwrap().is_dir());
        let (records, _) = emit_collect(&db, false);
        assert!(
            records
                .iter()
                .all(|r| r.project == "My project #1 café%done")
        );
    }

    #[test]
    fn session_cwd_skips_invalid_project_urls() {
        let temp = tempfile::tempdir().unwrap();
        let db = write_store(temp.path());
        let conn = Connection::open(&db).unwrap();
        for (idx, url) in ["not a URL", "https://example.com/repo", "file:///tmp/repo"]
            .iter()
            .enumerate()
        {
            let payload = field_bytes(
                19,
                &field_bytes(4, &field_bytes(2, &field_bytes(13, url.as_bytes()))),
            );
            conn.execute(
                "INSERT INTO steps (idx, step_type, step_payload) VALUES (?1, 14, ?2)",
                rusqlite::params![idx as i64 + 4, payload],
            )
            .unwrap();
        }
        assert_eq!(session_cwd(&db).as_deref(), Some(Path::new("/tmp/repo")));
    }

    #[test]
    fn session_cwd_reads_project_root_from_store() {
        let temp = tempfile::tempdir().unwrap();
        let db = write_store(temp.path());
        let project_block = field_bytes(13, b"file:///Users/x/src/repo-api");
        let inner2 = field_bytes(2, &project_block);
        let inner4 = field_bytes(4, &inner2);
        let payload = field_bytes(19, &inner4);
        let conn = Connection::open(&db).unwrap();
        conn.execute(
            "INSERT INTO steps (idx, step_type, status, has_subtrajectory, step_payload) \
             VALUES (4, 14, 3, false, ?1)",
            rusqlite::params![payload],
        )
        .unwrap();

        assert_eq!(
            session_cwd(&db).as_deref(),
            Some(Path::new("/Users/x/src/repo-api"))
        );
    }

    #[test]
    fn discovers_stores_and_overview_logs() {
        let temp = tempfile::tempdir().unwrap();
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("ANTIGRAVITY_HOME", Some(temp.path().to_str().unwrap()))]);
        write_store(temp.path());
        let logs = temp
            .path()
            .join("antigravity-ide/brain/comp-a/.system_generated/logs");
        fs::create_dir_all(&logs).unwrap();
        fs::write(
            logs.join("overview.txt"),
            r#"{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-05-19T12:35:48Z","content":"hello"}"#,
        )
        .unwrap();
        fs::write(
            temp.path()
                .join("antigravity-ide/conversations/conv-1.db-wal"),
            b"wal",
        )
        .unwrap();

        let files = discover();
        let names = files
            .iter()
            .map(|file| file.path.file_name().unwrap().to_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert!(names.contains(&"conv-1.db".to_string()));
        assert!(names.contains(&"overview.txt".to_string()));
        assert!(!names.iter().any(|name| name.ends_with("-wal")));
    }

    #[test]
    fn parses_overview_log() {
        let temp = tempfile::tempdir().unwrap();
        let logs = temp.path().join("brain/comp-a/.system_generated/logs");
        fs::create_dir_all(&logs).unwrap();
        let overview = logs.join("overview.txt");
        fs::write(
            &overview,
            r#"{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-05-19T12:35:48Z","content":"<USER_REQUEST>\nhello\n</USER_REQUEST>"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-05-19T12:36:00Z","content":"hi there"}"#,
        )
        .unwrap();
        let (records, output) = emit_collect(&overview, false);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].role, "user");
        assert!(records[0].text.contains("hello"));
        assert_eq!(records[1].role, "assistant");
        assert_eq!(records[1].text, "hi there");
        assert_eq!(records[0].ts, 1_779_194_148_000);
        assert!(output.session_id.is_some());
    }

    #[test]
    fn overview_tool_kinds_emit_tool_use_records() {
        let temp = tempfile::tempdir().unwrap();
        let overview = temp.path().join("overview.txt");
        fs::write(
            &overview,
            r#"{"type":"RUN_COMMAND","content":"ls"}
{"type":"VIEW_FILE","content":"README.md"}
{"type":"CODE_ACTION","content":"edit README.md"}
{"type":"RUN_COMMAND","tool_calls":[{"command":"pwd"}]}"#,
        )
        .unwrap();
        let (records, _) = emit_collect(&overview, false);
        assert_eq!(records.len(), 4);
        for (record, (name, input)) in records.iter().zip([
            ("run_command", "ls"),
            ("view_file", "README.md"),
            ("code_action", "edit README.md"),
            ("run_command", r#"[{"command":"pwd"}]"#),
        ]) {
            assert_eq!(record.role, "tool_use");
            assert_eq!(record.tool_name.as_deref(), Some(name));
            assert_eq!(record.tool_input.as_deref(), Some(input));
            assert_eq!(record.text, input);
        }
    }

    #[test]
    fn matches_path_classifies_antigravity_paths() {
        assert!(matches_path(
            "/Users/x/.gemini/antigravity-ide/conversations/abc.db"
        ));
        assert!(matches_path(
            "/Users/x/.gemini/antigravity/brain/comp/.system_generated/logs/overview.txt"
        ));
        assert!(!matches_path(
            "/Users/x/.gemini/antigravity-ide/conversations/abc.db-wal"
        ));
        assert!(!matches_path("/Users/x/.claude/projects/abc.jsonl"));
    }
}
