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
    // Rebuild analytics metadata with normalized overview IDs and reliable CWDs.
    identity: 5,
    // Bumped whenever record extraction logic changes; forces a full re-parse.
    index: 5,
    usage: 1,
};

/// The tool-bearing `step_type` values observed in real stores.
const TOOL_STEP_TYPES: &[u64] = &[5, 7, 8, 9, 17, 21, 101, 132];

/// Profiles searched for `conversations/` stores and `brain/` overview/transcript logs.
const PROFILES: &[&str] = &["antigravity-cli", "antigravity-ide", "antigravity"];

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
    (normalized.ends_with("overview.txt")
        || normalized.ends_with("transcript.jsonl")
        || normalized.ends_with("transcript_full.jsonl"))
        && normalized.contains(".system_generated/logs/")
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

pub(crate) fn is_transcript_path(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|n| n.to_str()),
        Some("transcript.jsonl" | "transcript_full.jsonl")
    )
}

/// Shared conversation identity for sibling projections, independent of format.
pub(crate) fn projection_session_id(path: &Path) -> Option<String> {
    if is_db_path(path) {
        path.file_stem()?.to_str().map(str::to_string)
    } else if is_transcript_path(path) || is_overview_path(path) {
        Some(session_id_from_brain_path(path))
    } else {
        None
    }
}

/// All supported locations in canonical preference order, including absent files.
pub(crate) fn projection_paths(session_id: &str) -> Vec<PathBuf> {
    let base = sessions_root();
    let mut paths = Vec::new();
    for name in [
        "transcript_full.jsonl",
        "transcript.jsonl",
        "database",
        "overview.txt",
    ] {
        for profile in PROFILES {
            let root = base.join(profile);
            paths.push(if name == "database" {
                root.join("conversations").join(format!("{session_id}.db"))
            } else {
                root.join("brain")
                    .join(session_id)
                    .join(".system_generated/logs")
                    .join(name)
            });
        }
    }
    paths
}

pub fn discover() -> Vec<SourceFile> {
    let base = sessions_root();
    let mut sessions = std::collections::HashSet::new();
    for profile in PROFILES {
        let root = base.join(profile);
        if let Ok(entries) = std::fs::read_dir(root.join("conversations")) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file()
                    && is_db_path(&path)
                    && let Some(key) = projection_session_id(&path)
                {
                    sessions.insert(key);
                }
            }
        }
        for entry in WalkDir::new(root.join("brain")).into_iter().flatten() {
            let path = entry.path();
            if entry.file_type().is_file()
                && (is_transcript_path(path) || is_overview_path(path))
                && path
                    .parent()
                    .is_some_and(|p| p.ends_with(".system_generated/logs"))
                && let Some(key) = projection_session_id(path)
            {
                sessions.insert(key);
            }
        }
    }
    let by_session = sessions.into_iter().filter_map(|key| {
        projection_paths(&key)
            .into_iter()
            .find(|path| path.is_file())
            .map(|path| SourceFile {
                source: SourceKind::Antigravity,
                path,
            })
    });
    let mut files: Vec<SourceFile> = by_session.collect();
    files.sort_by(|a, b| a.path.cmp(&b.path));
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
    let (session_id, turn_id, offset, session_cwd) = if is_db_path(path) {
        index_db_file(
            path,
            include_reasoning,
            next_doc_id,
            &mut emit,
            &source_path,
            &mut diagnostics,
            state.turn_id,
        )?
    } else if is_transcript_path(path) {
        index_transcript_file(
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
            "unsupported antigravity file {} (expected a conversation .db, transcript[_full].jsonl, or overview.txt)",
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
        session_cwd,
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
) -> Result<(String, u32, u64, Option<String>)> {
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
    // `file://` URL (payload `19.4.2.*.13`); take its leaf directory name and
    // keep the decoded path itself as the session working directory.
    let mut project: Option<String> = None;
    let mut session_cwd: Option<PathBuf> = None;

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
        if project.is_none()
            && step_type == 14
            && let Some(url) = project_root_from_payload(&payload)
            && let Some(root) = file_url_path(&url)
        {
            if session_cwd.is_none() {
                session_cwd = Some(root.clone());
            }
            project = root
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_string);
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

    Ok((
        session_id,
        turn_id,
        file_len,
        session_cwd.map(|p| p.to_string_lossy().into_owned()),
    ))
}

fn index_overview_file(
    path: &Path,
    next_doc_id: &AtomicU64,
    emit: &mut impl FnMut(Record) -> Result<()>,
    source_path: &str,
    diagnostics: &mut ParseDiagnostics,
    start_turn_id: u32,
) -> Result<(String, u32, u64, Option<String>)> {
    let text = std::fs::read_to_string(path)?;
    let file_len = text.len() as u64;
    let session_id = session_id_from_brain_path(path);
    let mut turn_id = start_turn_id;
    let session_cwd = cwd_from_lines(text.lines());
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
    Ok((
        session_id,
        turn_id,
        file_len,
        session_cwd.map(|p| p.to_string_lossy().into_owned()),
    ))
}

fn session_id_from_brain_path(path: &Path) -> String {
    let mut current = path.parent();
    while let Some(p) = current {
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if !name.is_empty() && name != "logs" && name != ".system_generated" && name != "brain" {
            return name.to_string();
        }
        current = p.parent();
    }
    path.file_stem()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string()
}

fn extract_user_request(text: &str) -> &str {
    if let Some(start) = text.find("<USER_REQUEST>") {
        let after = &text[start + "<USER_REQUEST>".len()..];
        if let Some(end) = after.find("</USER_REQUEST>") {
            return after[..end].trim();
        }
    }
    text.trim()
}

fn cwd_path(text: &str) -> Option<PathBuf> {
    let text = text.trim().trim_matches('"');
    if text.starts_with("file:") {
        return file_url_path(text);
    }
    let path = PathBuf::from(text);
    path.is_absolute().then_some(path)
}

// Only explicit Cwd arguments and workspace mappings establish a project root.
// SearchPath may name a file; DirectoryPath/SearchDirectory may name any subtree.
fn workspace_mapping_cwd(value: &Value) -> Option<PathBuf> {
    let content = value.get("content")?.as_str()?;
    let (_, rest) = content.split_once("[URI] -> [CorpusName]:")?;
    rest.lines()
        .filter_map(|line| line.split_once("->"))
        .find_map(|(uri, _)| cwd_path(uri))
}

fn cwd_from_lines<'a>(lines: impl Iterator<Item = &'a str>) -> Option<PathBuf> {
    let mut workspace = None;
    for line in lines {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        // A later explicit Cwd must override an earlier workspace fallback.
        if let Some(calls) = value.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                if let Some(cwd) = call
                    .get("args")
                    .and_then(|args| args.get("Cwd"))
                    .and_then(Value::as_str)
                    .and_then(cwd_path)
                {
                    return Some(cwd);
                }
            }
        }
        if workspace.is_none() {
            workspace = workspace_mapping_cwd(&value);
        }
    }
    workspace
}

fn index_transcript_file(
    path: &Path,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    emit: &mut impl FnMut(Record) -> Result<()>,
    source_path: &str,
    diagnostics: &mut ParseDiagnostics,
    start_turn_id: u32,
) -> Result<(String, u32, u64, Option<String>)> {
    let text = std::fs::read_to_string(path)?;
    let file_len = text.len() as u64;
    let session_id = session_id_from_brain_path(path);
    let mut turn_id = start_turn_id;

    // Resolve the working directory before emitting anything so every record
    // carries the same project: tool invocations hold it in their args, and a
    // cwd found mid-file would otherwise split the session across two projects.
    let session_cwd = cwd_from_lines(text.lines());
    let project = session_cwd
        .as_ref()
        .and_then(|cwd| cwd.file_name())
        .and_then(|name| name.to_str())
        .unwrap_or_else(|| SourceKind::Antigravity.label())
        .to_string();

    // Text-bearing steps share one record shape; only the role and event id
    // vary. Tool-bearing steps fill their own tool fields and stay inline.
    let text_record = |ts: u64, turn_id: u32, role: &str, text: String, event_id: String| Record {
        source: SourceKind::Antigravity,
        doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
        ts,
        project: project.to_string(),
        session_id: session_id.to_string(),
        turn_id,
        role: role.to_string(),
        text,
        tool_name: None,
        tool_input: None,
        tool_output: None,
        links: RecordLinks {
            event_id: Some(event_id),
            ..Default::default()
        },
        source_path: source_path.to_string(),
    };

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
        let step_index = value
            .get("step_index")
            .and_then(|v| v.as_u64())
            .unwrap_or(turn_id as u64);

        let mut emit = |mut record: Record| -> Result<()> {
            if let Some(fields) = value.get("truncated_fields").and_then(Value::as_array)
                && !fields.is_empty()
            {
                record.links.source_content =
                    Some(serde_json::json!({"truncated_fields": fields}).to_string());
                record.text.push_str(&format!(
                    "\n[Antigravity truncated fields: {}]",
                    Value::Array(fields.clone())
                ));
            }
            emit(record)
        };

        match kind {
            "USER_INPUT" if status == "DONE" => {
                let content = value.get("content").and_then(|v| v.as_str()).unwrap_or("");
                let text = extract_user_request(content).to_string();
                if text.is_empty() {
                    continue;
                }
                emit(text_record(
                    ts,
                    turn_id,
                    "user",
                    text,
                    format!("{session_id}:{step_index}"),
                ))?;
                turn_id += 1;
            }
            "PLANNER_RESPONSE" => {
                if include_reasoning
                    && let Some(thinking) = value.get("thinking").and_then(|v| v.as_str())
                {
                    let trimmed = thinking.trim();
                    if !trimmed.is_empty() {
                        emit(text_record(
                            ts,
                            turn_id,
                            "reasoning",
                            trimmed.to_string(),
                            format!("{session_id}:{step_index}:reasoning"),
                        ))?;
                        turn_id += 1;
                    }
                }

                if let Some(content) = value.get("content").and_then(|v| v.as_str()) {
                    let trimmed = content.trim();
                    if !trimmed.is_empty() {
                        emit(text_record(
                            ts,
                            turn_id,
                            "assistant",
                            trimmed.to_string(),
                            format!("{session_id}:{step_index}:response"),
                        ))?;
                        turn_id += 1;
                    }
                }

                if let Some(tool_calls) = value.get("tool_calls").and_then(|v| v.as_array()) {
                    for (call_idx, call) in tool_calls.iter().enumerate() {
                        let name = call.get("name").and_then(|v| v.as_str()).unwrap_or("tool");
                        let args_str = call.get("args").map(|v| v.to_string()).unwrap_or_default();
                        let summary = call
                            .get("args")
                            .and_then(|a| a.get("toolSummary").or_else(|| a.get("toolAction")))
                            .and_then(|s| s.as_str())
                            .map(|s| s.trim_matches('"').to_string());
                        let links = RecordLinks {
                            event_id: Some(format!("{session_id}:{step_index}:call:{call_idx}")),
                            ..Default::default()
                        };
                        emit(Record {
                            source: SourceKind::Antigravity,
                            doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                            ts,
                            project: project.clone(),
                            session_id: session_id.clone(),
                            turn_id,
                            role: "tool_use".to_string(),
                            text: format!("{name} {args_str}"),
                            tool_name: Some(name.to_string()),
                            tool_input: Some(args_str),
                            tool_output: summary,
                            links,
                            source_path: source_path.to_string(),
                        })?;
                        turn_id += 1;
                    }
                }
            }
            "GENERIC" | "RUN_COMMAND" | "VIEW_FILE" | "LIST_DIRECTORY" | "GREP_SEARCH"
            | "SEARCH_WEB" | "CODE_ACTION" => {
                let content = value.get("content").and_then(|v| v.as_str()).unwrap_or("");
                let trimmed = content.trim();
                if !trimmed.is_empty() {
                    let links = RecordLinks {
                        event_id: Some(format!("{session_id}:{step_index}:output")),
                        ..Default::default()
                    };
                    emit(Record {
                        source: SourceKind::Antigravity,
                        doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                        ts,
                        project: project.clone(),
                        session_id: session_id.clone(),
                        turn_id,
                        role: "tool".to_string(),
                        text: trimmed.to_string(),
                        tool_name: (kind != "GENERIC").then(|| kind.to_lowercase()),
                        tool_input: None,
                        tool_output: Some(trimmed.to_string()),
                        links,
                        source_path: source_path.to_string(),
                    })?;
                    turn_id += 1;
                }
            }
            "SYSTEM_MESSAGE" => {}
            "ERROR_MESSAGE" => {
                let content = value.get("content").and_then(|v| v.as_str()).unwrap_or("");
                let trimmed = content.trim();
                if !trimmed.is_empty() {
                    let text = if trimmed.to_ascii_lowercase().starts_with("error") {
                        trimmed.to_string()
                    } else {
                        format!("Error: {trimmed}")
                    };
                    let links = RecordLinks {
                        event_id: Some(format!("{session_id}:{step_index}:error")),
                        ..Default::default()
                    };
                    emit(Record {
                        source: SourceKind::Antigravity,
                        doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                        ts,
                        project: project.clone(),
                        session_id: session_id.clone(),
                        turn_id,
                        role: "tool".to_string(),
                        text: text.clone(),
                        tool_name: None,
                        tool_input: None,
                        tool_output: Some(text),
                        links,
                        source_path: source_path.to_string(),
                    })?;
                    turn_id += 1;
                }
            }
            other => diagnostics.increment_unknown_top_level(other),
        }
    }

    let session_cwd_str = session_cwd.map(|p| p.to_string_lossy().into_owned());
    Ok((session_id, turn_id, file_len, session_cwd_str))
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
    if is_db_path(path) {
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
        return None;
    }
    if (is_transcript_path(path) || is_overview_path(path))
        && let Ok(text) = std::fs::read_to_string(path)
    {
        return cwd_from_lines(text.lines());
    }
    None
}

fn file_url_path(url: &str) -> Option<PathBuf> {
    url::Url::parse(url).ok()?.to_file_path().ok()
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
        // Index under the 19.4.2.13 shaped envelope:
        // 19 { 2: text, 4 { 2 { 13: "file:///Users/x/src/repo-api" } } }.
        let temp = tempfile::tempdir().unwrap();
        let db = write_store(temp.path());
        let conn = Connection::open(&db).unwrap();
        let mut user = field_bytes(2, b"do it");
        user.extend(field_bytes(
            4,
            &field_bytes(2, &field_bytes(13, b"file:///Users/x/src/repo-api")),
        ));
        conn.execute(
            "UPDATE steps SET step_payload = ?1 WHERE idx = 0",
            rusqlite::params![field_bytes(19, &user)],
        )
        .unwrap();
        let (records, output) = emit_collect(&db, false);
        assert!(records.iter().all(|record| record.project == "repo-api"));
        assert_eq!(output.session_cwd.as_deref(), Some("/Users/x/src/repo-api"));
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
        let (records, output) = emit_collect(&db, false);
        // Directory URLs decode with a trailing slash; compare as paths.
        assert_eq!(
            output.session_cwd.as_deref().map(Path::new),
            Some(cwd.as_path())
        );
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
        assert!(matches_path(
            "/Users/x/.gemini/antigravity-cli/brain/uuid-123/.system_generated/logs/transcript.jsonl"
        ));
        assert!(!matches_path(
            "/Users/x/.gemini/antigravity-ide/conversations/abc.db-wal"
        ));
        assert!(!matches_path("/Users/x/.claude/projects/abc.jsonl"));
    }

    #[test]
    fn parses_transcript_jsonl_log() {
        let temp = tempfile::tempdir().unwrap();
        let logs = temp
            .path()
            .join("brain/sess-uuid-456/.system_generated/logs");
        fs::create_dir_all(&logs).unwrap();
        let transcript = logs.join("transcript.jsonl");
        fs::write(
            &transcript,
            r#"{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-15T23:44:54Z","content":"<USER_REQUEST>\nfix the bug\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\ntime\n</ADDITIONAL_METADATA>"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-09-15T23:44:55Z","thinking":"Planning fix","tool_calls":[{"name":"run_command","args":{"CommandLine":"cargo test","Cwd":"/apps/myproj"}}]}
{"step_index":2,"source":"MODEL","type":"GENERIC","status":"DONE","created_at":"2026-09-15T23:44:56Z","content":"test passed"}
{"step_index":3,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-09-15T23:44:57Z","content":"Done fixing!"}"#,
        )
        .unwrap();

        let (records, output) = emit_collect(&transcript, true);
        assert_eq!(output.session_id.as_deref(), Some("sess-uuid-456"));
        assert_eq!(output.session_cwd.as_deref(), Some("/apps/myproj"));

        // Expect: user, reasoning, tool_use, tool, assistant
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].role, "user");
        assert_eq!(records[0].text, "fix the bug");
        assert_eq!(records[0].project, "myproj");

        assert_eq!(records[1].role, "reasoning");
        assert_eq!(records[1].text, "Planning fix");

        assert_eq!(records[2].role, "tool_use");
        assert_eq!(records[2].tool_name.as_deref(), Some("run_command"));

        assert_eq!(records[3].role, "tool");
        assert_eq!(records[3].text, "test passed");

        assert_eq!(records[4].role, "assistant");
        assert_eq!(records[4].text, "Done fixing!");
    }

    #[test]
    fn discover_keeps_richest_projection_per_conversation() {
        let temp = tempfile::tempdir().unwrap();
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("ANTIGRAVITY_HOME", Some(temp.path().to_str().unwrap()))]);

        // A cli conversation with all three projections: transcript.jsonl wins.
        let cli_conv = temp.path().join("antigravity-cli/conversations/aaa-111.db");
        fs::create_dir_all(cli_conv.parent().unwrap()).unwrap();
        fs::write(&cli_conv, b"").unwrap();
        let cli_logs = temp
            .path()
            .join("antigravity-cli/brain/aaa-111/.system_generated/logs");
        fs::create_dir_all(&cli_logs).unwrap();
        fs::write(cli_logs.join("overview.txt"), b"{}").unwrap();
        fs::write(cli_logs.join("transcript.jsonl"), b"{}").unwrap();

        // An ide conversation with a store and overview.txt (no transcript):
        // the store wins.
        let ide_conv = temp.path().join("antigravity-ide/conversations/bbb-222.db");
        fs::create_dir_all(ide_conv.parent().unwrap()).unwrap();
        fs::write(&ide_conv, b"").unwrap();
        let ide_logs = temp
            .path()
            .join("antigravity-ide/brain/bbb-222/.system_generated/logs");
        fs::create_dir_all(&ide_logs).unwrap();
        fs::write(ide_logs.join("overview.txt"), b"{}").unwrap();

        // A legacy conversation with only overview.txt: it survives.
        let legacy_logs = temp
            .path()
            .join("antigravity/brain/ccc-333/.system_generated/logs");
        fs::create_dir_all(&legacy_logs).unwrap();
        fs::write(legacy_logs.join("overview.txt"), b"{}").unwrap();

        let files = discover();
        let paths: Vec<String> = files
            .iter()
            .map(|file| file.path.to_string_lossy().into_owned())
            .collect();
        assert_eq!(files.len(), 3, "paths: {paths:?}");
        assert!(
            paths
                .iter()
                .any(|p| p.ends_with("aaa-111/.system_generated/logs/transcript.jsonl"))
        );
        assert!(paths.iter().any(|p| p.ends_with("bbb-222.db")));
        assert!(
            paths
                .iter()
                .any(|p| p.ends_with("ccc-333/.system_generated/logs/overview.txt"))
        );
    }

    #[test]
    fn session_id_from_brain_path_finds_uuid() {
        let path = Path::new(
            "/root/.gemini/antigravity-cli/brain/e2a4562b-5476-4222-84bf-5110195946bf/.system_generated/logs/transcript.jsonl",
        );
        assert_eq!(
            session_id_from_brain_path(path),
            "e2a4562b-5476-4222-84bf-5110195946bf"
        );
    }

    #[test]
    fn captured_format_typed_results_preserve_output_and_truncation() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp
            .path()
            .join("brain/session/.system_generated/logs/transcript.jsonl");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            include_str!("../../tests/fixtures/antigravity/transcript.jsonl"),
        )
        .unwrap();
        let (records, output) = emit_collect(&path, false);
        assert_eq!(records.len(), 14);
        assert_eq!(output.session_cwd.as_deref(), Some("/repo"));
        for (index, name) in [
            "run_command",
            "view_file",
            "list_directory",
            "grep_search",
            "search_web",
            "code_action",
        ]
        .iter()
        .enumerate()
        {
            let call = &records[1 + index * 2];
            let result = &records[2 + index * 2];
            assert_eq!(call.role, "tool_use");
            assert_eq!(result.role, "tool");
            assert_eq!(result.tool_name.as_deref(), Some(*name));
            assert!(
                result
                    .tool_output
                    .as_deref()
                    .is_some_and(|text| !text.is_empty())
            );
        }
        assert_eq!(
            records[2].tool_output.as_deref(),
            Some("/repo\nExit code: 0")
        );
        assert!(records[8].text.contains("Antigravity truncated fields"));
        let metadata: Value =
            serde_json::from_str(records[8].links.source_content.as_deref().unwrap()).unwrap();
        assert_eq!(metadata["truncated_fields"], serde_json::json!(["content"]));
        // The malformed line is skipped and the stream error is indexed.
        let error = &records[13];
        assert_eq!(error.role, "tool");
        assert_eq!(
            error.tool_output.as_deref(),
            Some(
                "Error: The stream was interrupted. Please continue the task you were working on."
            )
        );
        assert!(
            error
                .links
                .event_id
                .as_deref()
                .is_some_and(|id| id.ends_with(":error"))
        );
    }

    #[test]
    fn full_transcript_wins_and_retains_full_only_content() {
        let temp = tempfile::tempdir().unwrap();
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("ANTIGRAVITY_HOME", Some(temp.path().to_str().unwrap()))]);
        let logs = temp
            .path()
            .join("antigravity-cli/brain/session/.system_generated/logs");
        fs::create_dir_all(&logs).unwrap();
        fs::write(
            logs.join("transcript.jsonl"),
            r#"{"type":"GREP_SEARCH","content":"clipped","truncated_fields":["content"]}"#,
        )
        .unwrap();
        let full = logs.join("transcript_full.jsonl");
        fs::write(
            &full,
            r#"{"type":"GREP_SEARCH","content":"full-only-searchable-sentinel"}"#,
        )
        .unwrap();
        let files = discover();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, full);
        assert!(matches_path(full.to_str().unwrap()));
        let (records, _) = emit_collect(&files[0].path, false);
        assert_eq!(records[0].text, "full-only-searchable-sentinel");
        assert!(records[0].links.source_content.is_none());
    }

    #[test]
    fn cwd_ignores_search_targets_and_prefers_later_explicit_directory() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("transcript.jsonl");
        fs::write(&path, r#"{"type":"PLANNER_RESPONSE","tool_calls":[{"name":"grep_search","args":{"SearchPath":"/repo/src/main.rs"}}]}
{"type":"SYSTEM_MESSAGE","content":"[URI] -> [CorpusName]:\nfile:///fallback%20repo -> fallback"}
{"type":"PLANNER_RESPONSE","tool_calls":[{"name":"run_command","args":{"Cwd":"file:///my%20repo"}}]}"#).unwrap();
        let (records, output) = emit_collect(&path, false);
        assert_eq!(output.session_cwd.as_deref(), Some("/my repo"));
        assert!(records.iter().all(|record| record.project == "my repo"));
        assert_eq!(session_cwd(&path), Some(PathBuf::from("/my repo")));
        assert_eq!(
            cwd_from_lines(
                [r#"{"content":"[URI] -> [CorpusName]:\nfile:///fallback%20repo -> fallback"}"#]
                    .into_iter()
            ),
            Some(PathBuf::from("/fallback repo"))
        );
        assert_eq!(cwd_from_lines([r#"{"tool_calls":[{"args":{"SearchPath":"/repo/src/main.rs","DirectoryPath":"/repo/src"}}]}"#].into_iter()), None);
    }

    #[test]
    fn overview_resume_uses_conversation_id() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp
            .path()
            .join("brain/e2a4562b-5476-4222-84bf-5110195946bf/.system_generated/logs/overview.txt");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            r#"{"type":"USER_INPUT","status":"DONE","content":"hello"}"#,
        )
        .unwrap();
        let (records, output) = emit_collect(&path, false);
        let record = &records[0];
        assert_eq!(
            output.session_id.as_deref(),
            Some("e2a4562b-5476-4222-84bf-5110195946bf")
        );
        let session = crate::resume::ResumeSession {
            source: SourceKind::Antigravity,
            session_id: &record.session_id,
            project: &record.project,
            source_path: &record.source_path,
            source_dir: path.parent().unwrap().to_str().unwrap(),
        };
        let template = crate::resume::default_resume_template("antigravity", true).unwrap();
        assert_eq!(
            crate::resume::expand_resume_template(&template, &session, "/repo"),
            "cd '/repo' && agy --conversation e2a4562b-5476-4222-84bf-5110195946bf"
        );
    }
}
