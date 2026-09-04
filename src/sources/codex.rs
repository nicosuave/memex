use super::{
    ConversationKind, IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions,
    SessionIdentity, SourceFile, SourceMetadata, UsageDependency, UsageParseOutput,
};
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::Result;
use memchr::{memchr, memmem};
use memmap2::Mmap;
use once_cell::sync::Lazy;
use regex::Regex;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use simd_json::BorrowedValue;
use simd_json::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 2,
    index: 4,
    usage: 4,
};

pub fn classify_path(path: &str) -> Option<SourceKind> {
    if path.contains(".codex/sessions")
        || path.contains(".codex\\sessions")
        || path.contains(".codex/archived_sessions")
        || path.contains(".codex\\archived_sessions")
        || path.contains(".codex/history.jsonl")
        || path.contains(".codex\\history.jsonl")
    {
        Some(SourceKind::Codex)
    } else {
        None
    }
}

pub fn is_history_path(path: &Path) -> bool {
    path.file_name().and_then(|name| name.to_str()) == Some("history.jsonl")
}

pub fn homes() -> Vec<PathBuf> {
    std::env::var_os("CODEX_HOME")
        .map(|roots| {
            roots
                .to_string_lossy()
                .split(',')
                .map(|root| PathBuf::from(root.trim()))
                .collect()
        })
        .unwrap_or_else(|| vec![super::common::home().join(".codex")])
}

pub fn rollout_roots() -> Vec<PathBuf> {
    homes()
        .into_iter()
        .flat_map(|home| {
            let active = home.join("sessions");
            let archived = home.join("archived_sessions");
            if active.exists() || archived.exists() {
                vec![active, archived]
            } else {
                vec![home]
            }
        })
        .collect()
}

pub fn discover_rollouts() -> Vec<SourceFile> {
    super::common::jsonl_files(rollout_roots())
        .into_iter()
        .map(|path| SourceFile {
            source: SourceKind::Codex,
            path,
        })
        .collect()
}

pub fn history_paths() -> Vec<PathBuf> {
    homes()
        .into_iter()
        .map(|home| home.join("history.jsonl"))
        .filter(|path| path.exists())
        .collect()
}

/// Load the human-facing titles maintained by Codex's local thread database.
/// The rollout JSONL files do not carry this value themselves.
pub fn session_titles(session_ids: &[String]) -> HashMap<String, String> {
    let mut titles = HashMap::new();
    for home in homes() {
        for path in state_database_paths(&home) {
            let Ok(connection) = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            ) else {
                continue;
            };
            for session_id in session_ids {
                if titles.contains_key(session_id) {
                    continue;
                }
                if let Some(title) = codex_thread_title(&connection, session_id) {
                    titles.insert(session_id.clone(), title);
                }
            }
        }
    }
    titles
}

fn state_database_paths(home: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(home) else {
        return Vec::new();
    };
    let mut versioned_paths = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str()?;
            let version = name
                .strip_prefix("state_")?
                .strip_suffix(".sqlite")?
                .parse::<u64>()
                .ok()?;
            Some((version, entry.path()))
        })
        .collect::<Vec<_>>();
    versioned_paths.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    versioned_paths.into_iter().map(|(_, path)| path).collect()
}

fn codex_thread_title(connection: &Connection, session_id: &str) -> Option<String> {
    // Current Codex builds have all three columns. The second query keeps the
    // reader useful with older databases that only stored `title`.
    for sql in [
        "SELECT COALESCE(NULLIF(name, ''), NULLIF(title, ''), NULLIF(first_user_message, '')) FROM threads WHERE id = ?1",
        "SELECT NULLIF(title, '') FROM threads WHERE id = ?1",
    ] {
        let title = connection
            .query_row(sql, params![session_id], |row| {
                row.get::<_, Option<String>>(0)
            })
            .optional()
            .ok()
            .flatten()
            .flatten()
            .map(|title| title.trim().to_string())
            .filter(|title| !title.is_empty());
        if title.is_some() {
            return title;
        }
    }
    None
}

pub fn session_id_from_path(path: &Path) -> Option<String> {
    static UUID: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
        Regex::new(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})")
            .expect("uuid regex")
    });
    let stem = path.file_stem()?.to_string_lossy();
    UUID.captures(&stem)
        .and_then(|captures| captures.get(1))
        .map(|value| value.as_str().to_string())
}

#[derive(Clone, Default)]
struct SessionLinks {
    parent_session_id: Option<String>,
    thread_source: Option<String>,
    conversation_kind: Option<String>,
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

#[derive(Clone)]
struct SessionMeta {
    session_id: String,
    project: String,
    cwd: Option<PathBuf>,
    links: SessionLinks,
}

fn fallback_meta(path: &Path) -> SessionMeta {
    SessionMeta {
        session_id: session_id_from_path(path).unwrap_or_else(|| "unknown".to_string()),
        project: SourceKind::Codex.label().to_string(),
        cwd: None,
        links: SessionLinks {
            conversation_kind: Some(ConversationKind::Main.as_str().to_string()),
            ..SessionLinks::default()
        },
    }
}

fn apply_meta(payload: &simd_json::borrowed::Object<'_>, metadata: &mut SessionMeta) {
    if let Some(id) = payload.get("id").and_then(|value| value.as_str()) {
        metadata.session_id = id.to_string();
    }
    if let Some(cwd) = payload.get("cwd").and_then(|value| value.as_str()) {
        metadata.project = super::common::project_from_path(cwd);
        metadata.cwd = Some(PathBuf::from(cwd));
    }
    let forked_from_id = payload
        .get("forked_from_id")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let parent_thread_id = payload
        .get("source")
        .and_then(|value| value.as_object())
        .and_then(|source| source.get("subagent"))
        .and_then(|value| value.as_object())
        .and_then(|subagent| subagent.get("thread_spawn"))
        .and_then(|value| value.as_object())
        .and_then(|spawn| spawn.get("parent_thread_id"))
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let thread_source = payload
        .get("thread_source")
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .or_else(|| parent_thread_id.as_ref().map(|_| "subagent".to_string()))
        .or_else(|| forked_from_id.as_ref().map(|_| "fork".to_string()));
    metadata.links.parent_session_id = forked_from_id.clone().or(parent_thread_id);
    metadata.links.thread_source = thread_source.clone();
    metadata.links.conversation_kind = Some(
        if thread_source.as_deref() == Some("subagent") {
            ConversationKind::Subagent
        } else if forked_from_id.is_some() {
            ConversationKind::Fork
        } else {
            ConversationKind::Main
        }
        .as_str()
        .to_string(),
    );
}

fn read_meta_until(path: &Path, limit: u64) -> Result<SessionMeta> {
    let mut metadata = fallback_meta(path);
    if limit == 0 {
        return Ok(metadata);
    }
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let limit = (limit as usize).min(mmap.len());
    let mut start = 0usize;
    let mut buffer = Vec::new();
    while start < limit {
        let slice = &mmap[start..limit];
        let relative = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..relative];
        start += relative + 1;
        if line.is_empty() {
            continue;
        }
        buffer.clear();
        buffer.extend_from_slice(line);
        if let Ok(value) = simd_json::to_borrowed_value(&mut buffer)
            && value.get("type").and_then(|value| value.as_str()) == Some("session_meta")
            && let Some(payload) = value.get("payload").and_then(|value| value.as_object())
        {
            apply_meta(payload, &mut metadata);
        }
    }
    Ok(metadata)
}

pub fn probe(path: &Path) -> Result<SourceMetadata> {
    let limit = path.metadata()?.len();
    let metadata = read_meta_until(path, limit)?;
    let kind = match metadata.links.conversation_kind.as_deref() {
        Some("subagent") => ConversationKind::Subagent,
        Some("fork") => ConversationKind::Fork,
        _ => ConversationKind::Main,
    };
    Ok(SourceMetadata {
        session: SessionIdentity {
            source: SourceKind::Codex,
            session_id: metadata.session_id,
            parent_session_id: metadata.links.parent_session_id,
            conversation_kind: kind,
            source_path: path.to_path_buf(),
        },
        cwd: metadata.cwd,
        project: Some(metadata.project),
        git_branch: None,
    })
}

pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut start = state.offset as usize;
    let mut turn_id = state.turn_id;
    let mut pending_tool_calls = state.pending_tool_calls;
    let source_path = path.to_string_lossy().to_string();
    let mut metadata = read_meta_until(path, state.offset)?;
    let mut buffer = Vec::new();
    let mut diagnostics = ParseDiagnostics::default();

    while start < mmap.len() {
        let slice = &mmap[start..];
        let relative = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..relative];
        start += relative + 1;
        if line.is_empty() {
            continue;
        }
        buffer.clear();
        buffer.extend_from_slice(line);
        let value = match simd_json::to_borrowed_value(&mut buffer) {
            Ok(value) => value,
            Err(_) => {
                diagnostics.malformed_json_lines += 1;
                continue;
            }
        };
        let Some(object) = value.as_object() else {
            diagnostics.non_object_json_lines += 1;
            continue;
        };
        let entry_type = object
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let timestamp = object
            .get("timestamp")
            .and_then(|value| value.as_str())
            .and_then(super::common::parse_iso_millis)
            .unwrap_or(0);
        if entry_type == "session_meta" {
            if let Some(payload) = object.get("payload").and_then(|value| value.as_object()) {
                apply_meta(payload, &mut metadata);
            }
            continue;
        }
        if entry_type == "turn_context" {
            continue;
        }
        if entry_type == "event_msg" {
            let Some(payload) = object.get("payload").and_then(|value| value.as_object()) else {
                continue;
            };
            if payload.get("type").and_then(|value| value.as_str()) == Some("agent_reasoning")
                && include_reasoning
                && let Some(text) = payload
                    .get("text")
                    .and_then(|value| value.as_str())
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
            {
                let mut links = metadata.links.record_links();
                links.event_id = super::common::borrowed_string(payload, "id");
                emit(Record {
                    source: SourceKind::Codex,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: metadata.project.clone(),
                    session_id: metadata.session_id.clone(),
                    turn_id,
                    role: "reasoning".to_string(),
                    text: text.to_string(),
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links,
                    source_path: source_path.clone(),
                })?;
                turn_id += 1;
            }
            continue;
        }
        if entry_type != "response_item" {
            if !matches!(entry_type, "compacted" | "world_state" | "ghost_snapshot") {
                diagnostics.increment_unknown_top_level(entry_type);
            }
            continue;
        }
        let Some(payload) = object.get("payload").and_then(|value| value.as_object()) else {
            continue;
        };
        let payload_type = payload
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let mut links = metadata.links.record_links();
        links.event_id = super::common::borrowed_string(payload, "id");
        match payload_type {
            "message" => {
                let role = payload
                    .get("role")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                let mut text_parts = Vec::new();
                if let Some(content) = payload.get("content") {
                    if let Some(text) = content.as_str() {
                        text_parts.push(text);
                    } else if let Some(array) = content.as_array() {
                        for block in array {
                            if let Some(text) = block
                                .as_object()
                                .and_then(|object| object.get("text"))
                                .and_then(|value| value.as_str())
                            {
                                text_parts.push(text);
                            }
                        }
                    }
                }
                let text = text_parts.join("\n").trim().to_string();
                if text.is_empty() || is_system_instruction(&text) {
                    continue;
                }
                emit(Record {
                    source: SourceKind::Codex,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: metadata.project.clone(),
                    session_id: metadata.session_id.clone(),
                    turn_id,
                    role: role.to_string(),
                    text,
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links,
                    source_path: source_path.clone(),
                })?;
                turn_id += 1;
            }
            "function_call" => {
                let tool_name = payload
                    .get("name")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let tool_input = payload
                    .get("arguments")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let call_id = payload
                    .get("call_id")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                if let Some(call_id) = &call_id {
                    links.event_id = Some(call_id.clone());
                }
                let doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
                if let Some(call_id) = call_id {
                    let replaced = pending_tool_calls.insert(
                        call_id.clone(),
                        super::common::pending_tool_call(
                            tool_name.clone(),
                            Some(call_id),
                            doc_id,
                            timestamp,
                            tool_input.as_deref(),
                            &links,
                            &metadata.session_id,
                        ),
                    );
                    if replaced.is_some() {
                        diagnostics.duplicate_tool_calls += 1;
                    }
                }
                emit(Record {
                    source: SourceKind::Codex,
                    doc_id,
                    ts: timestamp,
                    project: metadata.project.clone(),
                    session_id: metadata.session_id.clone(),
                    turn_id,
                    role: "tool_use".to_string(),
                    text: tool_input.clone().unwrap_or_default(),
                    tool_name,
                    tool_input,
                    tool_output: None,
                    links,
                    source_path: source_path.clone(),
                })?;
                turn_id += 1;
            }
            "function_call_output" => {
                let call_id = payload
                    .get("call_id")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                let pending = (!call_id.is_empty())
                    .then(|| pending_tool_calls.remove(call_id))
                    .flatten();
                if !call_id.is_empty() && pending.is_none() {
                    diagnostics.orphan_tool_results += 1;
                }
                let tool_name = pending.and_then(|call| call.tool_name);
                let tool_output = payload.get("output").and_then(value_text);
                let text = tool_output.clone().unwrap_or_default();
                if text.is_empty() {
                    continue;
                }
                if !call_id.is_empty() {
                    links.parent_event_id = Some(call_id.to_string());
                    links.parent_tool_use_id = Some(call_id.to_string());
                }
                emit(Record {
                    source: SourceKind::Codex,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: metadata.project.clone(),
                    session_id: metadata.session_id.clone(),
                    turn_id,
                    role: "tool_result".to_string(),
                    text,
                    tool_name,
                    tool_input: None,
                    tool_output,
                    links,
                    source_path: source_path.clone(),
                })?;
                turn_id += 1;
            }
            "custom_tool_call" => {
                let tool_name = payload
                    .get("name")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let tool_input = payload.get("input").and_then(value_text);
                let call_id = payload
                    .get("call_id")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                if let Some(call_id) = &call_id {
                    links.event_id = Some(call_id.clone());
                }
                let doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
                if let Some(call_id) = call_id {
                    let replaced = pending_tool_calls.insert(
                        call_id.clone(),
                        super::common::pending_tool_call(
                            tool_name.clone(),
                            Some(call_id),
                            doc_id,
                            timestamp,
                            tool_input.as_deref(),
                            &links,
                            &metadata.session_id,
                        ),
                    );
                    if replaced.is_some() {
                        diagnostics.duplicate_tool_calls += 1;
                    }
                }
                emit(Record {
                    source: SourceKind::Codex,
                    doc_id,
                    ts: timestamp,
                    project: metadata.project.clone(),
                    session_id: metadata.session_id.clone(),
                    turn_id,
                    role: "tool_use".to_string(),
                    text: tool_input.clone().unwrap_or_default(),
                    tool_name,
                    tool_input,
                    tool_output: None,
                    links,
                    source_path: source_path.clone(),
                })?;
                turn_id += 1;
            }
            "web_search_call" | "tool_search_call" => {
                let tool_name = if payload_type == "web_search_call" {
                    "web_search"
                } else {
                    "tool_search"
                };
                let tool_input = if payload_type == "web_search_call" {
                    payload
                        .get("action")
                        .or_else(|| payload.get("query"))
                        .and_then(value_text)
                } else {
                    payload.get("arguments").and_then(value_text)
                };
                let call_id = payload
                    .get("call_id")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                if let Some(call_id) = &call_id {
                    links.event_id = Some(call_id.clone());
                }
                let doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
                if let Some(call_id) = call_id {
                    let replaced = pending_tool_calls.insert(
                        call_id.clone(),
                        super::common::pending_tool_call(
                            Some(tool_name.to_string()),
                            Some(call_id),
                            doc_id,
                            timestamp,
                            tool_input.as_deref(),
                            &links,
                            &metadata.session_id,
                        ),
                    );
                    if replaced.is_some() {
                        diagnostics.duplicate_tool_calls += 1;
                    }
                }
                emit(Record {
                    source: SourceKind::Codex,
                    doc_id,
                    ts: timestamp,
                    project: metadata.project.clone(),
                    session_id: metadata.session_id.clone(),
                    turn_id,
                    role: "tool_use".to_string(),
                    text: tool_input.clone().unwrap_or_default(),
                    tool_name: Some(tool_name.to_string()),
                    tool_input,
                    tool_output: None,
                    links,
                    source_path: source_path.clone(),
                })?;
                turn_id += 1;
            }
            "custom_tool_call_output" | "tool_search_output" => {
                let call_id = payload
                    .get("call_id")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                let pending = (!call_id.is_empty())
                    .then(|| pending_tool_calls.remove(call_id))
                    .flatten();
                if !call_id.is_empty() && pending.is_none() {
                    diagnostics.orphan_tool_results += 1;
                }
                let tool_name = pending.and_then(|call| call.tool_name).or_else(|| {
                    (payload_type == "tool_search_output").then(|| "tool_search".to_string())
                });
                let tool_output = if payload_type == "tool_search_output" {
                    payload.get("tools").and_then(value_text)
                } else {
                    payload.get("output").and_then(value_text)
                };
                let text = tool_output.clone().unwrap_or_default();
                if text.is_empty() {
                    continue;
                }
                if !call_id.is_empty() {
                    links.parent_event_id = Some(call_id.to_string());
                    links.parent_tool_use_id = Some(call_id.to_string());
                }
                emit(Record {
                    source: SourceKind::Codex,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: metadata.project.clone(),
                    session_id: metadata.session_id.clone(),
                    turn_id,
                    role: "tool_result".to_string(),
                    text,
                    tool_name,
                    tool_input: None,
                    tool_output,
                    links,
                    source_path: source_path.clone(),
                })?;
                turn_id += 1;
            }
            "reasoning" => {
                if payload.contains_key("encrypted_content") {
                    diagnostics.encrypted_reasoning_dropped += 1;
                }
            }
            _ => diagnostics.increment_unknown_semantic(payload_type),
        }
    }

    Ok(IndexParseOutput {
        offset: mmap.len() as u64,
        turn_id,
        pending_tool_calls,
        session_id: Some(metadata.session_id),
        diagnostics,
    })
}

fn value_text(value: &BorrowedValue<'_>) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_string());
    }
    if let Some(array) = value.as_array() {
        let text = array
            .iter()
            .filter_map(|item| {
                item.as_object()
                    .and_then(|object| object.get("text").or_else(|| object.get("content")))
                    .and_then(|value| value.as_str())
            })
            .collect::<Vec<_>>()
            .join("\n");
        if !text.is_empty() {
            return Some(text);
        }
    }
    Some(value.to_string())
}

pub(crate) fn parse_history_records(
    path: &Path,
    state: IndexParseState,
    session_ids: &HashSet<String>,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut start = state.offset as usize;
    let mut turn_id = state.turn_id;
    let source_path = path.to_string_lossy().to_string();
    let mut buffer = Vec::new();
    while start < mmap.len() {
        let slice = &mmap[start..];
        let relative = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..relative];
        start += relative + 1;
        if line.is_empty() {
            continue;
        }
        buffer.clear();
        buffer.extend_from_slice(line);
        let Ok(value): Result<BorrowedValue<'_>, _> = simd_json::to_borrowed_value(&mut buffer)
        else {
            continue;
        };
        let Some(object) = value.as_object() else {
            continue;
        };
        let session_id = object
            .get("session_id")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if session_id.is_empty() || session_ids.contains(session_id) {
            continue;
        }
        let text = object
            .get("text")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if text.is_empty() {
            continue;
        }
        let timestamp = object
            .get("ts")
            .and_then(|value| value.as_i64())
            .unwrap_or(0)
            .max(0) as u64
            * 1000;
        emit(Record {
            source: SourceKind::Codex,
            doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
            ts: timestamp,
            project: SourceKind::Codex.label().to_string(),
            session_id: session_id.to_string(),
            turn_id,
            role: "user".to_string(),
            text: text.to_string(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks {
                conversation_kind: Some(ConversationKind::Main.as_str().to_string()),
                ..RecordLinks::default()
            },
            source_path: source_path.clone(),
        })?;
        turn_id += 1;
    }
    Ok(IndexParseOutput {
        offset: mmap.len() as u64,
        turn_id,
        pending_tool_calls: state.pending_tool_calls,
        session_id: None,
        diagnostics: Default::default(),
    })
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
struct UsageTokens {
    input: u64,
    cached: u64,
    output: u64,
    reasoning: u64,
}

impl UsageTokens {
    fn from(value: &BorrowedValue<'_>) -> Self {
        let number = |aliases: &[&str]| {
            aliases
                .iter()
                .find_map(|key| value.get(*key).and_then(|value| value.as_u64()))
                .unwrap_or(0)
        };
        Self {
            input: number(&["input_tokens", "inputTokens", "prompt_tokens"]),
            cached: number(&[
                "cached_input_tokens",
                "cachedInputTokens",
                "cache_read_input_tokens",
            ]),
            output: number(&["output_tokens", "outputTokens", "completion_tokens"]),
            reasoning: number(&[
                "reasoning_output_tokens",
                "reasoningTokens",
                "reasoning_tokens",
            ]),
        }
    }

    fn zero(self) -> bool {
        self.input == 0 && self.cached == 0 && self.output == 0 && self.reasoning == 0
    }

    fn add(self, rhs: Self) -> Self {
        Self {
            input: self.input.saturating_add(rhs.input),
            cached: self.cached.saturating_add(rhs.cached),
            output: self.output.saturating_add(rhs.output),
            reasoning: self.reasoning.saturating_add(rhs.reasoning),
        }
    }

    fn sub(self, rhs: Self) -> Self {
        Self {
            input: self.input.saturating_sub(rhs.input),
            cached: self.cached.saturating_sub(rhs.cached),
            output: self.output.saturating_sub(rhs.output),
            reasoning: self.reasoning.saturating_sub(rhs.reasoning),
        }
    }

    fn min(self, rhs: Self) -> Self {
        Self {
            input: self.input.min(rhs.input),
            cached: self.cached.min(rhs.cached),
            output: self.output.min(rhs.output),
            reasoning: self.reasoning.min(rhs.reasoning),
        }
    }

    fn max(self, rhs: Self) -> Self {
        Self {
            input: self.input.max(rhs.input),
            cached: self.cached.max(rhs.cached),
            output: self.output.max(rhs.output),
            reasoning: self.reasoning.max(rhs.reasoning),
        }
    }

    fn at_least(self, rhs: Self) -> bool {
        self.input >= rhs.input
            && self.cached >= rhs.cached
            && self.output >= rhs.output
            && self.reasoning >= rhs.reasoning
    }

    fn at_most(self, rhs: Self) -> bool {
        self.input <= rhs.input
            && self.cached <= rhs.cached
            && self.output <= rhs.output
            && self.reasoning <= rhs.reasoning
    }
}

#[derive(Default)]
struct UsageCounter {
    counted: UsageTokens,
    raw_baseline: UsageTokens,
    watermark: UsageTokens,
    seen: Vec<UsageTokens>,
    inherited_seen: HashSet<UsageTokens>,
    divergent: bool,
    interleaved: bool,
}

impl UsageCounter {
    fn establish_unresolved_fork_baseline(&mut self, total: UsageTokens) {
        self.raw_baseline = total;
        self.watermark = self.watermark.max(total);
        if self.seen.last() != Some(&total) {
            self.seen.push(total);
            if self.seen.len() > 64 {
                self.seen.remove(0);
            }
        }
    }

    fn seed_inherited(&mut self, snapshots: &[UsageTokens]) {
        let Some(baseline) = snapshots.iter().copied().reduce(UsageTokens::max) else {
            return;
        };
        self.inherited_seen.extend(snapshots.iter().copied());
        self.raw_baseline = baseline;
        self.watermark = self.watermark.max(baseline);
    }

    fn account(&mut self, last: Option<UsageTokens>, total: Option<UsageTokens>) -> UsageTokens {
        if let Some(total) = total {
            if self.seen.contains(&total) || self.inherited_seen.contains(&total) {
                return UsageTokens::default();
            }
            if !total.at_least(self.watermark) {
                self.interleaved = true;
            }
        }
        let baseline = self.watermark.max(self.raw_baseline);
        let delta = match (last, total) {
            (Some(last), Some(total)) if self.interleaved => {
                last.min(contained_usage(total, baseline, self.counted))
            }
            (None, Some(total)) if self.interleaved => {
                contained_usage(total, baseline, self.counted)
            }
            (Some(last), Some(total)) => {
                let total_delta = total.sub(baseline);
                if !self.divergent && total.at_least(baseline) && total_delta.at_most(last) {
                    total_delta
                } else {
                    last
                }
            }
            (None, Some(total)) if self.divergent => contained_usage(total, baseline, self.counted),
            (None, Some(total)) => total.sub(baseline),
            (Some(last), None) => last,
            (None, None) => return UsageTokens::default(),
        };
        self.counted = self.counted.add(delta);
        if let Some(total) = total {
            self.raw_baseline = total;
            self.divergent |= total != self.counted;
            self.watermark = self.watermark.max(total);
            if self.seen.last() != Some(&total) {
                self.seen.push(total);
                if self.seen.len() > 64 {
                    self.seen.remove(0);
                }
            }
        } else {
            self.raw_baseline = self.counted;
            self.watermark = self.watermark.max(self.counted);
        }
        delta
    }
}

fn contained_usage(
    current: UsageTokens,
    watermark: UsageTokens,
    counted: UsageTokens,
) -> UsageTokens {
    fn one(current: u64, watermark: u64, counted: u64) -> u64 {
        if current >= watermark {
            current.saturating_sub(watermark.max(counted))
        } else {
            current.saturating_sub(counted)
        }
    }
    UsageTokens {
        input: one(current.input, watermark.input, counted.input),
        cached: one(current.cached, watermark.cached, counted.cached),
        output: one(current.output, watermark.output, counted.output),
        reasoning: one(current.reasoning, watermark.reasoning, counted.reasoning),
    }
}

struct ParentData {
    deps: Vec<UsageDependency>,
    snapshots: Vec<(u64, UsageTokens)>,
}

type ParentSlot = Option<Arc<ParentData>>;

/// Source-owned fork resolver. The usage cache asks this object whether its recorded
/// dependency set is still complete, but does not interpret Codex hierarchy itself.
pub(crate) struct UsageParentIndex {
    by_session: HashMap<String, Vec<PathBuf>>,
    parents: Mutex<HashMap<String, ParentSlot>>,
}

impl UsageParentIndex {
    pub fn new(files: &[PathBuf]) -> Self {
        let mut by_session: HashMap<String, Vec<PathBuf>> = HashMap::new();
        for path in files {
            if let Some(session) = session_id_from_path(path) {
                by_session.entry(session).or_default().push(path.clone());
            }
        }
        Self {
            by_session,
            parents: Mutex::new(HashMap::new()),
        }
    }

    fn load(&self, parent: &str) -> ParentSlot {
        if let Some(slot) = self.parents.lock().unwrap().get(parent) {
            return slot.clone();
        }
        let loaded = self.by_session.get(parent).and_then(|paths| {
            let mut deps = Vec::new();
            let mut snapshots = Vec::new();
            for path in paths {
                let Ok(dependency) = UsageDependency::from_path(path) else {
                    continue;
                };
                let Ok(file_snapshots) = total_usage_snapshots(path) else {
                    continue;
                };
                deps.push(dependency);
                snapshots.extend(file_snapshots);
            }
            (!deps.is_empty()).then(|| Arc::new(ParentData { deps, snapshots }))
        });
        self.parents
            .lock()
            .unwrap()
            .entry(parent.to_string())
            .or_insert(loaded)
            .clone()
    }

    pub fn deps_match_current_candidates(&self, deps: &[UsageDependency]) -> bool {
        let Some(first) = deps.first() else {
            return true;
        };
        let Some(session) = session_id_from_path(Path::new(&first.path)) else {
            return true;
        };
        let current: HashSet<&str> = self
            .by_session
            .get(&session)
            .map(|paths| paths.iter().filter_map(|path| path.to_str()).collect())
            .unwrap_or_default();
        let recorded: HashSet<&str> = deps.iter().map(|dep| dep.path.as_str()).collect();
        current == recorded
    }

    fn resolve(
        &self,
        parent: &str,
        cutoff_ms: u64,
    ) -> Option<(Vec<UsageDependency>, Vec<UsageTokens>)> {
        let data = self.load(parent)?;
        let totals = data
            .snapshots
            .iter()
            .filter(|(timestamp, _)| *timestamp <= cutoff_ms)
            .map(|(_, totals)| *totals)
            .collect::<Vec<_>>();
        (!totals.is_empty()).then(|| (data.deps.clone(), totals))
    }
}

static USAGE_LINE_NEEDLES: Lazy<Vec<memmem::Finder<'static>>> = Lazy::new(|| {
    [
        &b"token_count"[..],
        b"turn_context",
        b"session_meta",
        b"task_started",
        b"\"usage\"",
    ]
    .into_iter()
    .map(memmem::Finder::new)
    .collect()
});

fn usage_timestamp(value: &BorrowedValue<'_>) -> u64 {
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
                .filter(|number| *number >= 0)
                .map(|number| number as u64)
        })
        .or_else(|| value.as_str().and_then(super::common::parse_iso_millis))
        .unwrap_or(0)
}

fn borrowed_string(value: &BorrowedValue<'_>, aliases: &[&str]) -> Option<String> {
    aliases
        .iter()
        .find_map(|key| value.get(*key).and_then(|value| value.as_str()))
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn usage_parent_session_id(payload: &BorrowedValue<'_>) -> Option<String> {
    borrowed_string(
        payload,
        &["forked_from_id", "parent_session_id", "parentSessionId"],
    )
    .or_else(|| {
        payload
            .get("source")
            .and_then(|value| value.get("subagent"))
            .and_then(|value| value.get("thread_spawn"))
            .and_then(|value| value.get("parent_thread_id"))
            .and_then(|value| value.as_str())
            .map(str::to_string)
    })
}

fn total_usage_snapshots(path: &Path) -> Result<Vec<(u64, UsageTokens)>> {
    static TOKEN_COUNT_NEEDLE: Lazy<memmem::Finder<'static>> =
        Lazy::new(|| memmem::Finder::new(b"token_count"));
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut start = 0usize;
    let mut buffer = Vec::new();
    let mut snapshots = Vec::new();
    while start < mmap.len() {
        let slice = &mmap[start..];
        let relative = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..relative];
        start += relative + usize::from(relative < slice.len());
        if TOKEN_COUNT_NEEDLE.find(line).is_none() {
            continue;
        }
        buffer.clear();
        buffer.extend_from_slice(line);
        let Ok(value) = simd_json::to_borrowed_value(&mut buffer) else {
            continue;
        };
        if value.get("type").and_then(|value| value.as_str()) != Some("event_msg") {
            continue;
        }
        let Some(payload) = value.get("payload") else {
            continue;
        };
        if payload.get("type").and_then(|value| value.as_str()) != Some("token_count") {
            continue;
        }
        let info = payload.get("info").unwrap_or(payload);
        let Some(total) = info
            .get("total_token_usage")
            .map(UsageTokens::from)
            .filter(|total| !total.zero())
        else {
            continue;
        };
        snapshots.push((
            value.get("timestamp").map(usage_timestamp).unwrap_or(0),
            total,
        ));
    }
    Ok(snapshots)
}

pub(crate) fn parse_usage_file(
    path: &Path,
    parents: &UsageParentIndex,
) -> Result<UsageParseOutput> {
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let mut session = session_id_from_path(path);
    let mut parent = None;
    let mut fork_timestamp_ms = None;
    let mut fork_resolved = false;
    let mut parent_deps = Vec::new();
    let mut project = None;
    let mut model = None;
    let mut turn = None;
    let mut counter = UsageCounter::default();
    let mut event_index = 0u64;
    let mut unresolved_fork_baseline_seen = false;
    let mut events = Vec::new();
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut start = 0usize;
    let mut line_index = 0u64;
    let mut buffer = Vec::new();
    while start < mmap.len() {
        let slice = &mmap[start..];
        let relative = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..relative];
        start += relative + usize::from(relative < slice.len());
        let source_order = line_index;
        line_index += 1;
        if !USAGE_LINE_NEEDLES
            .iter()
            .any(|needle| needle.find(line).is_some())
        {
            continue;
        }
        buffer.clear();
        buffer.extend_from_slice(line);
        let Ok(value) = simd_json::to_borrowed_value(&mut buffer) else {
            continue;
        };
        let kind = value
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let payload = value.get("payload");
        match (kind, payload) {
            ("session_meta", Some(payload)) => {
                session = borrowed_string(payload, &["id", "session_id"]).or(session);
                parent = usage_parent_session_id(payload);
                if parent.is_some() {
                    fork_timestamp_ms = value.get("timestamp").map(usage_timestamp);
                }
                if let (Some(parent_id), Some(fork_ms)) = (&parent, fork_timestamp_ms)
                    && let Some((deps, inherited)) = parents.resolve(parent_id, fork_ms)
                {
                    counter.seed_inherited(&inherited);
                    unresolved_fork_baseline_seen = true;
                    fork_resolved = true;
                    parent_deps = deps;
                }
                // Usage reports historically expose Codex's full cwd. Keep that projection
                // stable even though the search index uses the leaf project label.
                project = borrowed_string(payload, &["cwd"]);
            }
            ("turn_context", Some(payload)) => {
                model = borrowed_string(payload, &["model", "model_name"]);
            }
            ("event_msg", Some(payload))
                if payload.get("type").and_then(|value| value.as_str()) == Some("task_started") =>
            {
                turn = borrowed_string(payload, &["turn_id", "turnId"]);
            }
            ("event_msg", Some(payload))
                if payload.get("type").and_then(|value| value.as_str()) == Some("token_count") =>
            {
                let info = payload.get("info").unwrap_or(payload);
                let last = info
                    .get("last_token_usage")
                    .map(UsageTokens::from)
                    .filter(|tokens| !tokens.zero());
                let total = info
                    .get("total_token_usage")
                    .map(UsageTokens::from)
                    .filter(|tokens| !tokens.zero());
                let event_timestamp_ms = value.get("timestamp").map(usage_timestamp).unwrap_or(0);
                if parent.is_some()
                    && fork_timestamp_ms.is_some_and(|fork| event_timestamp_ms <= fork)
                {
                    if let Some(total) = total {
                        counter.establish_unresolved_fork_baseline(total);
                        unresolved_fork_baseline_seen = true;
                    }
                    continue;
                }
                if parent.is_some()
                    && !unresolved_fork_baseline_seen
                    && let Some(total) = total
                {
                    counter.establish_unresolved_fork_baseline(total);
                    unresolved_fork_baseline_seen = true;
                    continue;
                }
                let delta = counter.account(last, total);
                if delta.zero() {
                    continue;
                }
                events.push(UsageEvent {
                    source: "codex",
                    source_path: source_path.clone(),
                    source_record_id: Some(format!("event:{event_index}")),
                    session_id: session.clone(),
                    request_id: turn.clone(),
                    message_id: None,
                    timestamp_ms: event_timestamp_ms,
                    project: project.clone(),
                    provider: Some("openai".into()),
                    model: model
                        .clone()
                        .or_else(|| borrowed_string(info, &["model", "model_name"])),
                    tokens: TokenBuckets::codex(
                        delta.input,
                        delta.cached,
                        delta.output,
                        delta.reasoning,
                    ),
                    source_cost_usd: None,
                    cost_authoritative: false,
                    dedupe_confidence: "strong",
                    conservative_undercount: counter.interleaved
                        || (parent.is_some() && !fork_resolved),
                    cache_chain_excluded: false,
                    sidechain: false,
                    source_order,
                });
                event_index += 1;
            }
            _ => {
                let usage = value
                    .get("usage")
                    .or_else(|| value.get("data").and_then(|value| value.get("usage")))
                    .or_else(|| value.get("result").and_then(|value| value.get("usage")))
                    .or_else(|| value.get("response").and_then(|value| value.get("usage")));
                let Some(usage) = usage else {
                    continue;
                };
                let tokens = UsageTokens::from(usage);
                if tokens.zero() {
                    continue;
                }
                events.push(UsageEvent {
                    source: "codex",
                    source_path: source_path.clone(),
                    source_record_id: Some(format!("line:{source_order}")),
                    session_id: session.clone(),
                    request_id: None,
                    message_id: None,
                    timestamp_ms: value
                        .get("timestamp")
                        .or_else(|| value.get("created_at"))
                        .map(usage_timestamp)
                        .unwrap_or(0),
                    project: project.clone(),
                    provider: Some("openai".into()),
                    model: model
                        .clone()
                        .or_else(|| borrowed_string(&value, &["model", "model_name"])),
                    tokens: TokenBuckets::codex(
                        tokens.input,
                        tokens.cached,
                        tokens.output,
                        tokens.reasoning,
                    ),
                    source_cost_usd: None,
                    cost_authoritative: false,
                    dedupe_confidence: "strong",
                    conservative_undercount: false,
                    cache_chain_excluded: false,
                    sidechain: false,
                    source_order,
                });
            }
        }
    }
    Ok(UsageParseOutput {
        events,
        cacheable: parent.is_none() || fork_resolved,
        deps: parent_deps,
    })
}

pub(crate) fn reconcile_usage(events: &mut Vec<UsageEvent>) {
    let mut seen = HashSet::new();
    events.retain(|event| {
        if event.source != "codex" {
            return true;
        }
        let Some(session) = &event.session_id else {
            return true;
        };
        let Some(record) = &event.source_record_id else {
            return true;
        };
        seen.insert((session.clone(), record.clone(), event.tokens.clone()))
    });
}

fn is_system_instruction(text: &str) -> bool {
    let text = text.trim_start();
    text.starts_with("<system_instruction>") || text.starts_with("<system-instruction>")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn usage(input: u64, cached: u64, output: u64) -> UsageTokens {
        UsageTokens {
            input,
            cached,
            output,
            reasoning: 0,
        }
    }

    #[test]
    fn repeated_total_does_not_repeat_last() {
        let mut counter = UsageCounter::default();
        assert_eq!(
            counter.account(Some(usage(100, 20, 10)), Some(usage(100, 20, 10))),
            usage(100, 20, 10)
        );
        assert_eq!(
            counter.account(Some(usage(100, 20, 10)), Some(usage(100, 20, 10))),
            usage(0, 0, 0)
        );
    }

    #[test]
    fn interleaved_stream_never_recounts_high_water_gap() {
        let mut counter = UsageCounter::default();
        assert_eq!(
            counter.account(None, Some(usage(1000, 0, 0))),
            usage(1000, 0, 0)
        );
        assert_eq!(
            counter.account(Some(usage(200, 0, 0)), Some(usage(200, 0, 0))),
            usage(0, 0, 0)
        );
        assert_eq!(
            counter.account(Some(usage(900, 0, 0)), Some(usage(1100, 0, 0))),
            usage(100, 0, 0)
        );
        assert_eq!(counter.counted.input, 1100);
    }

    #[test]
    fn discovers_active_and_archived_rollouts() {
        let temp = tempfile::tempdir().unwrap();
        let active = temp.path().join("sessions");
        let archived = temp.path().join("archived_sessions");
        fs::create_dir_all(&active).unwrap();
        fs::create_dir_all(&archived).unwrap();
        fs::write(active.join("active.jsonl"), "{}\n").unwrap();
        fs::write(archived.join("archived.jsonl"), "{}\n").unwrap();
        let files = super::super::common::jsonl_files([active, archived]);
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn codex_thread_title_prefers_an_explicit_name() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state_5.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads (id TEXT PRIMARY KEY, name TEXT, title TEXT, first_user_message TEXT);
                 INSERT INTO threads VALUES ('session-1', 'Pinned name', 'Generated title', 'First prompt');",
            )
            .unwrap();

        assert_eq!(
            codex_thread_title(&connection, "session-1").as_deref(),
            Some("Pinned name")
        );
        assert_eq!(codex_thread_title(&connection, "missing"), None);
    }

    #[test]
    fn state_database_paths_prefer_the_newest_schema_version() {
        let temp = tempfile::tempdir().unwrap();
        for name in ["state_2.sqlite", "state_12.sqlite", "state.sqlite"] {
            fs::write(temp.path().join(name), "").unwrap();
        }

        let names = state_database_paths(temp.path())
            .into_iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(names, ["state_12.sqlite", "state_2.sqlite"]);
    }

    #[test]
    fn history_records_use_the_canonical_codex_source() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("history.jsonl");
        fs::write(
            &path,
            "{\"session_id\":\"missing-session\",\"ts\":42,\"text\":\"fallback prompt\"}\n",
        )
        .unwrap();
        let mut records = Vec::new();

        parse_history_records(
            &path,
            IndexParseState::default(),
            &HashSet::new(),
            &AtomicU64::new(1),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source, SourceKind::Codex);
        assert_eq!(records[0].project, "codex");
    }

    #[test]
    fn probe_resolves_nested_subagent_parent() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp
            .path()
            .join("rollout-2026-07-20T00-00-00-11111111-1111-4111-8111-111111111111.jsonl");
        fs::write(
            &path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"11111111-1111-4111-8111-111111111111\",\"cwd\":\"/repo/memex\",\"source\":{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"22222222-2222-4222-8222-222222222222\"}}}}}\n",
        )
        .unwrap();
        let metadata = probe(&path).unwrap();
        assert_eq!(
            metadata.session.parent_session_id.as_deref(),
            Some("22222222-2222-4222-8222-222222222222")
        );
        assert_eq!(
            metadata.session.conversation_kind,
            ConversationKind::Subagent
        );
        assert_eq!(metadata.project.as_deref(), Some("memex"));
    }

    #[test]
    fn parity_fixture_indexes_semantic_tools_and_filters_encrypted_reasoning() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("codex.jsonl");
        fs::write(
            &path,
            include_str!("../../fixtures/trajectory_parity/codex.jsonl"),
        )
        .unwrap();

        let mut without_reasoning = Vec::new();
        let parsed = parse_index_records(
            &path,
            IndexParseState::default(),
            false,
            &AtomicU64::new(1),
            |record| {
                without_reasoning.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert!(
            !without_reasoning
                .iter()
                .any(|record| record.role == "reasoning")
        );
        assert_eq!(parsed.diagnostics.encrypted_reasoning_dropped, 1);
        assert_eq!(parsed.diagnostics.malformed_json_lines, 1);
        assert_eq!(
            parsed
                .diagnostics
                .unknown_semantic_types
                .get("future_semantic_event"),
            Some(&1)
        );

        let mut with_reasoning = Vec::new();
        parse_index_records(
            &path,
            IndexParseState::default(),
            true,
            &AtomicU64::new(1),
            |record| {
                with_reasoning.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert!(with_reasoning
            .iter()
            .any(|record| record.role == "reasoning"
                && record.text == "Plaintext summary only"));
        assert!(
            with_reasoning
                .iter()
                .any(|record| record.tool_name.as_deref() == Some("apply_patch"))
        );
        assert!(
            with_reasoning
                .iter()
                .any(|record| record.tool_name.as_deref() == Some("web_search"))
        );
        assert!(
            with_reasoning
                .iter()
                .any(|record| record.tool_name.as_deref() == Some("tool_search"))
        );
        assert!(
            !with_reasoning
                .iter()
                .any(|record| { record.text.contains("ciphertext-must-never-be-indexed") })
        );
    }
}
