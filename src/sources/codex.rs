use super::common::tool_value_text as value_text;
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
    // Recompute ownership after excluding copied parent session metadata.
    identity: 4,
    index: 9,
    usage: 6,
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

pub(crate) fn discover_rollouts_with_inventory(
    inventory: &mut crate::directory_inventory::DiscoveryInventory,
) -> Result<Vec<SourceFile>> {
    Ok(
        super::common::jsonl_files_with_inventory(rollout_roots(), inventory)?
            .into_iter()
            .map(|path| SourceFile {
                source: SourceKind::Codex,
                path,
            })
            .collect(),
    )
}

pub fn history_paths() -> Vec<PathBuf> {
    homes()
        .into_iter()
        .map(|home| home.join("history.jsonl"))
        .filter(|path| path.exists())
        .collect()
}

/// Provider metadata stays separate from transcript-derived title fallbacks.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SessionTitleMetadata {
    pub title: Option<String>,
    pub first_user_message: Option<String>,
    pub agent_path: Option<String>,
}

/// Load titles without treating an agent identifier as a conversation title.
pub fn session_titles(session_ids: &[String]) -> HashMap<String, String> {
    session_title_metadata(session_ids)
        .into_iter()
        .filter_map(|(id, metadata)| {
            metadata
                .title
                .or(metadata.first_user_message)
                .map(|title| (id, title))
        })
        .collect()
}

/// Read each database's schema and prepare its lookup once for the whole batch.
pub fn session_title_metadata(session_ids: &[String]) -> HashMap<String, SessionTitleMetadata> {
    let mut titles = HashMap::new();
    if session_ids.is_empty() {
        return titles;
    }
    for home in homes() {
        for path in state_database_paths(&home) {
            let Ok(connection) = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            ) else {
                continue;
            };
            let Some(query) = title_metadata_query(&connection) else {
                continue;
            };
            let Ok(mut statement) = connection.prepare(&query) else {
                continue;
            };
            for session_id in session_ids {
                if titles.contains_key(session_id) {
                    continue;
                }
                if let Some(metadata) = codex_thread_title_metadata(&mut statement, session_id) {
                    titles.insert(session_id.clone(), metadata);
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

fn title_metadata_query(connection: &Connection) -> Option<String> {
    let mut statement = connection.prepare("PRAGMA table_info(threads)").ok()?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .ok()?
        .collect::<rusqlite::Result<HashSet<_>>>()
        .ok()?;
    if !columns.contains("id") {
        return None;
    }
    // Only these fixed identifiers enter SQL; unavailable older columns are NULL.
    let fields = ["name", "title", "first_user_message", "agent_path"].map(|field| {
        if columns.contains(field) {
            field
        } else {
            "NULL"
        }
    });
    Some(format!(
        "SELECT {} FROM threads WHERE id = ?1",
        fields.join(", ")
    ))
}

fn codex_thread_title_metadata(
    statement: &mut rusqlite::Statement<'_>,
    session_id: &str,
) -> Option<SessionTitleMetadata> {
    let (name, title, first_user_message, agent_path) = statement
        .query_row(params![session_id], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .optional()
        .ok()
        .flatten()?;
    let normalize = |value: Option<String>| {
        value
            .map(|text| crate::analytics::sanitize_label(&text))
            .filter(|text| !text.is_empty())
    };
    let metadata = SessionTitleMetadata {
        title: normalize(name).or_else(|| normalize(title)),
        first_user_message: normalize(first_user_message),
        agent_path: agent_path.filter(|path| {
            path.strip_prefix("/root/")
                .is_some_and(|name| !name.trim().is_empty())
        }),
    };
    (metadata.title.is_some()
        || metadata.first_user_message.is_some()
        || metadata.agent_path.is_some())
    .then_some(metadata)
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
    owner_id: Option<String>,
    project: String,
    cwd: Option<PathBuf>,
    links: SessionLinks,
    source_turn_id: Option<String>,
}

fn fallback_meta(path: &Path) -> SessionMeta {
    SessionMeta {
        session_id: session_id_from_path(path).unwrap_or_else(|| "unknown".to_string()),
        owner_id: session_id_from_path(path),
        project: SourceKind::Codex.label().to_string(),
        cwd: None,
        source_turn_id: None,
        links: SessionLinks {
            conversation_kind: Some(ConversationKind::Main.as_str().to_string()),
            ..SessionLinks::default()
        },
    }
}

fn is_guardian_review(payload: &simd_json::borrowed::Object<'_>) -> bool {
    payload
        .get("thread_source")
        .and_then(|value| value.as_str())
        == Some("guardian_review")
        || payload
            .get("source")
            .and_then(|value| value.get("subagent"))
            .and_then(|value| value.get("other"))
            .and_then(|value| value.as_str())
            == Some("guardian")
}

// A rollout filename identifies its owner even when copied history precedes its
// metadata. For nonstandard filenames, the first declared ID establishes ownership.
fn accepts_meta(owner: &mut Option<String>, id: Option<&str>) -> bool {
    let Some(id) = id.filter(|id| !id.is_empty()) else {
        return true;
    };
    match owner {
        Some(owner) => owner == id,
        None => {
            *owner = Some(id.to_string());
            true
        }
    }
}

fn apply_meta(payload: &simd_json::borrowed::Object<'_>, metadata: &mut SessionMeta) {
    let id = payload
        .get("id")
        .or_else(|| payload.get("session_id"))
        .and_then(|value| value.as_str());
    if !accepts_meta(&mut metadata.owner_id, id) {
        return;
    }
    if let Some(id) = &metadata.owner_id {
        metadata.session_id = id.clone();
    }
    if let Some(cwd) = payload.get("cwd").and_then(|value| value.as_str()) {
        metadata.project = super::common::project_from_path(cwd);
        metadata.cwd = Some(PathBuf::from(cwd));
    }
    let forked_from_id = payload
        .get("forked_from_id")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    // Older CLIs record spawned agents as `"source": {"subagent": "<role>"}`
    // with no `thread_spawn` wrapper or explicit `thread_source`; any
    // object- or role-string-shaped marker means this thread is a subagent.
    let subagent_marker = payload
        .get("source")
        .and_then(|value| value.as_object())
        .and_then(|source| source.get("subagent"));
    let subagent_present = subagent_marker.is_some_and(|marker| {
        marker.as_object().is_some() || marker.as_str().is_some_and(|role| !role.is_empty())
    });
    let guardian_review = is_guardian_review(payload);
    let parent_thread_id = payload
        .get("parent_thread_id")
        .and_then(|value| value.as_str())
        .or_else(|| {
            subagent_marker
                .and_then(|value| value.as_object())
                .and_then(|subagent| subagent.get("thread_spawn"))
                .and_then(|value| value.as_object())
                .and_then(|spawn| spawn.get("parent_thread_id"))
                .and_then(|value| value.as_str())
        })
        .map(str::to_string);
    let thread_source = payload
        .get("thread_source")
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .or_else(|| guardian_review.then(|| "guardian_review".to_string()))
        .or_else(|| {
            (parent_thread_id.is_some() && forked_from_id.is_none()).then(|| "subagent".to_string())
        })
        .or_else(|| subagent_present.then(|| "subagent".to_string()))
        .or_else(|| forked_from_id.as_ref().map(|_| "fork".to_string()));
    metadata.links.parent_session_id = forked_from_id.clone().or(parent_thread_id);
    metadata.links.thread_source = thread_source.clone();
    metadata.links.conversation_kind = Some(
        if guardian_review {
            ConversationKind::GuardianReview
        } else if thread_source.as_deref() == Some("subagent") {
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
    if limit == 0 {
        return Ok(fallback_meta(path));
    }
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let limit = (limit as usize).min(mmap.len());
    Ok(read_meta_prefix(path, &mmap[..limit], None).0)
}

fn read_meta_prefix(
    path: &Path,
    prefix: &[u8],
    cached_offsets: Option<&[u64]>,
) -> (SessionMeta, Vec<u64>) {
    crate::profiling::span!("codex.metadata_recovery");
    let mut metadata = fallback_meta(path);
    let mut offsets = Vec::new();
    let mut buffer = Vec::new();
    if let Some(cached) = cached_offsets {
        for &offset in cached {
            let Ok(start) = usize::try_from(offset) else {
                return read_meta_prefix(path, prefix, None);
            };
            if start >= prefix.len()
                || (start > 0 && prefix[start - 1] != b'\n')
                || offsets.last().is_some_and(|last| *last >= offset)
            {
                return read_meta_prefix(path, prefix, None);
            }
            let slice = &prefix[start..];
            let length = memchr(b'\n', slice).unwrap_or(slice.len());
            buffer.clear();
            buffer.extend_from_slice(&slice[..length]);
            if !apply_meta_line(&mut buffer, &mut metadata) {
                return read_meta_prefix(path, prefix, None);
            }
            offsets.push(offset);
        }
        return (metadata, offsets);
    }
    crate::profiling::count!("codex.metadata_prefix_scanned_bytes", prefix.len());
    offsets = scan_meta_lines(prefix, &mut metadata);
    (metadata, offsets)
}

const META_LINE_MARKERS: [&[u8]; 6] = [
    b"session_meta",
    b"turn_context",
    b"task_started",
    b"task_complete",
    b"turn_aborted",
    b"\\u",
];

fn scan_meta_lines(bytes: &[u8], metadata: &mut SessionMeta) -> Vec<u64> {
    let mut buffer = Vec::new();
    let mut offsets = Vec::new();
    let mut start = 0;
    while start < bytes.len() {
        let line_start = start;
        let slice = &bytes[start..];
        let length = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..length];
        start += length + 1;
        if META_LINE_MARKERS
            .iter()
            .all(|marker| memmem::find(line, marker).is_none())
        {
            continue;
        }
        buffer.clear();
        buffer.extend_from_slice(line);
        if apply_meta_line(&mut buffer, metadata) {
            offsets.push(line_start as u64);
        }
    }
    offsets
}

pub(crate) fn cwd_with_metadata_checkpoint(
    path: &Path,
    offset: u64,
    metadata_offsets: &[u64],
) -> Result<Option<PathBuf>> {
    let file = File::open(path)?;
    let length = file.metadata()?.len();
    if length < offset || length == 0 {
        return Ok(read_meta_until(path, length)?.cwd);
    }
    let mmap = unsafe { Mmap::map(&file)? };
    let boundary = (offset as usize).min(mmap.len());
    let (mut metadata, _) = read_meta_prefix(path, &mmap[..boundary], Some(metadata_offsets));
    crate::profiling::count!("codex.metadata_tail_scanned_bytes", mmap.len() - boundary);
    scan_meta_lines(&mmap[boundary..], &mut metadata);
    Ok(metadata.cwd)
}

/// Applies a line that changes session metadata and reports whether it did, so its offset
/// can be replayed on the next incremental parse instead of rescanning the prefix.
fn apply_meta_line(line: &mut [u8], metadata: &mut SessionMeta) -> bool {
    crate::profiling::count!("codex.metadata_candidates_decoded", 1);
    let Ok(value) = simd_json::to_borrowed_value(line) else {
        return false;
    };
    let Some(payload) = value.get("payload").and_then(|value| value.as_object()) else {
        return false;
    };
    match value.get("type").and_then(|value| value.as_str()) {
        Some("session_meta") => apply_meta(payload, metadata),
        Some("turn_context") => {
            metadata.source_turn_id = super::common::borrowed_string(payload, "turn_id");
        }
        Some("event_msg") => match payload.get("type").and_then(|value| value.as_str()) {
            Some("task_started") => {
                metadata.source_turn_id = super::common::borrowed_string(payload, "turn_id");
            }
            Some("task_complete" | "turn_aborted") => metadata.source_turn_id = None,
            _ => return false,
        },
        _ => return false,
    }
    true
}

pub fn probe(path: &Path) -> Result<SourceMetadata> {
    let limit = path.metadata()?.len();
    let metadata = read_meta_until(path, limit)?;
    let kind = match metadata.links.conversation_kind.as_deref() {
        Some("subagent") => ConversationKind::Subagent,
        Some("guardian_review") => ConversationKind::GuardianReview,
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

#[cfg(test)]
pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    parse_index_records_with_metadata_offsets(
        path,
        state,
        include_reasoning,
        next_doc_id,
        None,
        emit,
    )
    .map(|(output, _)| output)
}

pub(crate) fn parse_index_records_with_metadata_offsets(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    cached_offsets: Option<&[u64]>,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<(IndexParseOutput, Vec<u64>)> {
    let mut legacy_turn_id = state.legacy_ordinal()?;
    let mut emit = |mut record: Record| {
        if record.links.source_record_offset.is_none() {
            record.links.legacy_turn_id = Some(legacy_turn_id);
            legacy_turn_id += 1;
        }
        emit(record)
    };
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut start = super::jsonl::resume_offset(&mmap, state.offset, |line| {
        simd_json::to_borrowed_value(&mut line.to_vec()).is_ok()
    });
    let mut turn_id = state.turn_id;
    let mut pending_tool_calls = state.pending_tool_calls;
    let source_path = path.to_string_lossy().to_string();
    let prefix_len = start;
    let (mut metadata, mut metadata_offsets) = read_meta_prefix(
        path,
        &mmap[..prefix_len],
        cached_offsets.filter(|_| state.offset > 0),
    );
    let mut buffer = Vec::new();
    let mut diagnostics = ParseDiagnostics::default();

    while start < mmap.len() {
        let source_record_offset = start as u64;
        let line_start = start;
        let slice = &mmap[start..];
        let relative = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..relative];
        start += relative + usize::from(relative < slice.len());
        if line.is_empty() {
            continue;
        }
        buffer.clear();
        buffer.extend_from_slice(line);
        let value = match simd_json::to_borrowed_value(&mut buffer) {
            Ok(value) => value,
            Err(_) => {
                if relative == slice.len() {
                    start = line_start;
                    break;
                }

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
                metadata_offsets.push(line_start as u64);
            }
            continue;
        }
        if entry_type == "turn_context" {
            metadata.source_turn_id = object
                .get("payload")
                .and_then(|v| v.as_object())
                .and_then(|payload| super::common::borrowed_string(payload, "turn_id"));
            metadata_offsets.push(line_start as u64);
            continue;
        }
        if entry_type == "event_msg" {
            let Some(payload) = object.get("payload").and_then(|value| value.as_object()) else {
                continue;
            };
            let event_type = payload
                .get("type")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            if matches!(
                event_type,
                "task_started" | "task_complete" | "turn_aborted"
            ) {
                if event_type == "task_started" {
                    metadata.source_turn_id = super::common::borrowed_string(payload, "turn_id");
                }
                metadata_offsets.push(line_start as u64);
                let mut links = metadata.links.record_links();
                links.source_turn_id = super::common::borrowed_string(payload, "turn_id")
                    .or_else(|| metadata.source_turn_id.clone());
                links.event_id = super::common::borrowed_string(payload, "id");
                links.lifecycle_event = Some(event_type.to_string());
                links.source_record_type = Some(format!("event_msg/{event_type}"));
                links.source_record_offset = Some(source_record_offset);
                emit(Record {
                    source: SourceKind::Codex,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: metadata.project.clone(),
                    session_id: metadata.session_id.clone(),
                    turn_id,
                    role: "lifecycle".to_string(),
                    text: match event_type {
                        "task_started" => "Turn started",
                        "task_complete" => "Turn completed",
                        _ => "Turn interrupted",
                    }
                    .to_string(),
                    tool_name: None,
                    tool_input: None,
                    // Keep the complete original event available without repeating its answer.
                    tool_output: Some(serde_json::to_string(payload)?),
                    links,
                    source_path: source_path.clone(),
                })?;
                turn_id += 1;
                if event_type != "task_started" {
                    metadata.source_turn_id = None;
                }
                continue;
            }
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
                links.source_turn_id = metadata.source_turn_id.clone();
                links.source_record_type = Some("event_msg/agent_reasoning".to_string());
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
        links.source_turn_id = super::common::borrowed_string(payload, "turn_id")
            .or_else(|| metadata.source_turn_id.clone());
        links.assistant_phase = super::common::borrowed_string(payload, "phase");
        links.source_record_type = Some(format!("response_item/{payload_type}"));
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
                        if array.iter().any(|block| {
                            matches!(
                                block.get("type").and_then(|v| v.as_str()),
                                Some(
                                    "image"
                                        | "input_image"
                                        | "local_image"
                                        | "localImage"
                                        | "file"
                                        | "input_file"
                                        | "document"
                                        | "attachment"
                                )
                            )
                        }) {
                            let display_blocks = array
                                .iter()
                                .filter(|block| {
                                    matches!(
                                        block.get("type").and_then(|v| v.as_str()),
                                        Some(
                                            "text"
                                                | "input_text"
                                                | "output_text"
                                                | "image"
                                                | "input_image"
                                                | "local_image"
                                                | "localImage"
                                                | "file"
                                                | "input_file"
                                                | "document"
                                                | "attachment"
                                        )
                                    )
                                })
                                .collect::<Vec<_>>();
                            links.source_content = Some(serde_json::to_string(&display_blocks)?);
                        }
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
                if text.is_empty() && links.source_content.is_none() {
                    continue;
                }
                // The old parser skipped every message with either prefix,
                // including bundled requests and non-user roles. These records
                // must not consume another source record's legacy identity.
                if text.is_empty()
                    || text.starts_with("<system_instruction>")
                    || text.starts_with("<system-instruction>")
                {
                    links.source_record_offset = Some(source_record_offset);
                }
                // Keep injected instructions inspectable in raw transcripts. Only a
                // standalone wrapper changes role; a bundled user request stays a user record.
                let role = if role == "user" && is_standalone_system_instruction(&text) {
                    "system"
                } else {
                    role
                };
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

    Ok((
        IndexParseOutput {
            offset: start as u64,
            turn_id,
            legacy_turn_id: Some(legacy_turn_id),
            pending_tool_calls,
            session_id: Some(metadata.session_id),
            diagnostics,
        },
        metadata_offsets,
    ))
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
    let mut start = super::jsonl::resume_offset(&mmap, state.offset, |line| {
        simd_json::to_borrowed_value(&mut line.to_vec()).is_ok()
    });
    let mut turn_id = state.turn_id;
    let source_path = path.to_string_lossy().to_string();
    let mut buffer = Vec::new();
    while start < mmap.len() {
        let line_start = start;
        let slice = &mmap[start..];
        let relative = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..relative];
        start += relative + usize::from(relative < slice.len());
        if line.is_empty() {
            continue;
        }
        buffer.clear();
        buffer.extend_from_slice(line);
        let Ok(value): Result<BorrowedValue<'_>, _> = simd_json::to_borrowed_value(&mut buffer)
        else {
            if relative == slice.len() {
                start = line_start;
                break;
            }
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
        legacy_turn_id: Some(turn_id),
        offset: start as u64,
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
        &[
            "forked_from_id",
            "parent_session_id",
            "parentSessionId",
            "parent_thread_id",
        ],
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
    let mut permission_review = false;
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
                let id = borrowed_string(payload, &["id", "session_id"]);
                if !accepts_meta(&mut session, id.as_deref()) {
                    continue;
                }
                permission_review = payload.as_object().is_some_and(is_guardian_review);
                // Review sessions do not inherit the parent task's token counters.
                parent = if permission_review {
                    borrowed_string(payload, &["forked_from_id"])
                } else {
                    usage_parent_session_id(payload)
                };
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
                    permission_review,
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
                    permission_review,
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

fn is_standalone_system_instruction(text: &str) -> bool {
    let text = text.trim();
    ["system_instruction", "system-instruction"]
        .iter()
        .any(|tag| {
            let open = format!("<{tag}>");
            let close = format!("</{tag}>");
            text.strip_prefix(&open).is_some_and(|rest| {
                rest.find(&close)
                    .is_some_and(|end| rest[end + close.len()..].trim().is_empty())
            })
        })
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

    fn parse_transcript(path: &Path, state: IndexParseState) -> (Vec<Record>, IndexParseOutput) {
        let mut records = Vec::new();
        let output = parse_index_records(path, state, false, &AtomicU64::new(1), |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();
        (records, output)
    }

    #[test]
    fn copied_parent_metadata_cannot_replace_rollout_ownership() {
        use serde_json::json;
        let owner = "01a08752-673e-7032-bfac-faf9eb95ae40";
        let parent = "01a08749-9316-72b0-a489-1ae139f6789f";
        for filename in [format!("rollout-{owner}.jsonl"), "session.jsonl".into()] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(&filename);
            let own_meta = |cwd| {
                json!({"type":"session_meta", "timestamp": 1, "payload": {
                    "id": owner, "cwd": cwd, "forked_from_id": parent,
                    "source": {"subagent": {"thread_spawn": {
                        "parent_thread_id": parent, "agent_path": "/root/review_placeholder_fix"
                    }}}
                }})
            };
            let foreign = json!({"type":"session_meta", "timestamp": 2, "payload": {
                "id": parent, "cwd": "/repo/parent", "source": "vscode"
            }});
            let message = json!({"type":"response_item", "payload": {
                "type":"message", "role":"assistant", "content":"Review result"
            }});
            let mut lines = Vec::new();
            if filename.starts_with("rollout-") {
                // The filename also protects against foreign metadata before our header.
                lines.push(foreign.clone());
            }
            lines.extend([own_meta("/repo/initial"), foreign.clone(), message.clone()]);
            let prefix = lines.iter().map(|v| format!("{v}\n")).collect::<String>();
            let suffix = [own_meta("/repo/updated"), foreign, message,
                json!({"type":"response", "timestamp":3, "usage":{"input_tokens":20,"output_tokens":3}})]
                .iter().map(|v| format!("{v}\n")).collect::<String>();
            fs::write(&path, &prefix).unwrap();
            let (_, checkpoint) = parse_transcript(&path, IndexParseState::default());
            use std::io::Write;
            fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .write_all(suffix.as_bytes())
                .unwrap();
            let metadata = probe(&path).unwrap();
            assert_eq!(metadata.session.session_id, owner);
            assert_eq!(
                metadata.session.conversation_kind,
                ConversationKind::Subagent
            );
            assert_eq!(metadata.session.parent_session_id.as_deref(), Some(parent));
            assert_eq!(metadata.cwd, Some(PathBuf::from("/repo/updated")));
            let (records, output) = parse_transcript(&path, IndexParseState::default());
            assert_eq!(output.session_id.as_deref(), Some(owner));
            assert_eq!(records.len(), 2);
            for record in &records {
                assert_eq!(record.session_id, owner);
                assert_eq!(record.links.conversation_kind.as_deref(), Some("subagent"));
                assert_eq!(record.links.parent_session_id.as_deref(), Some(parent));
            }
            assert_eq!(records[0].project, "initial");
            assert_eq!(records[1].project, "updated");
            let (incremental, _) = parse_transcript(
                &path,
                IndexParseState {
                    offset: checkpoint.offset,
                    turn_id: checkpoint.turn_id,
                    legacy_turn_id: checkpoint.legacy_turn_id,
                    pending_tool_calls: checkpoint.pending_tool_calls,
                },
            );
            assert_eq!(incremental.len(), 1);
            assert_eq!(incremental[0].session_id, owner);
            assert_eq!(
                incremental[0].links.conversation_kind.as_deref(),
                Some("subagent")
            );
            let usage = parse_usage_file(&path, &UsageParentIndex::new(&[])).unwrap();
            assert_eq!(usage.events.len(), 1);
            assert_eq!(usage.events[0].session_id.as_deref(), Some(owner));
            assert_eq!(usage.events[0].project.as_deref(), Some("/repo/updated"));
        }
    }

    #[test]
    fn metadata_without_an_id_preserves_filename_fallback() {
        let owner = "01a08752-673e-7032-bfac-faf9eb95ae40";
        let mut metadata = fallback_meta(Path::new(&format!("rollout-{owner}.jsonl")));
        let mut bytes = br#"{"cwd":"/repo/fallback","source":"vscode"}"#.to_vec();
        let value = simd_json::to_borrowed_value(&mut bytes).unwrap();
        apply_meta(value.as_object().unwrap(), &mut metadata);
        assert_eq!(metadata.session_id, owner);
        assert_eq!(metadata.project, "fallback");
    }

    #[test]
    fn reader_records_preserve_legacy_ids_across_full_and_incremental_parsing() {
        use crate::retrieval::canonical_record_id;
        use serde_json::json;
        use std::io::Write;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let response = |payload| json!({"type":"response_item", "payload":payload});
        // Every pre-reader emitting branch, including native IDs that still
        // advance the ordinal used by subsequent id-less records.
        let legacy = vec![
            response(json!({"type":"message", "role":"user", "content":"Question"})),
            response(json!({"type":"message", "role":"assistant", "content":"Commentary A"})),
            response(
                json!({"type":"message", "role":"assistant", "content":"Commentary B", "id":"native-answer"}),
            ),
            response(
                json!({"type":"function_call", "name":"read", "call_id":"call", "arguments":"{}"}),
            ),
            response(
                json!({"type":"function_call_output", "call_id":"call", "output":"read result"}),
            ),
            response(json!({"type":"custom_tool_call", "name":"custom", "input":"custom input"})),
            response(json!({"type":"custom_tool_call_output", "output":"custom result"})),
            response(json!({"type":"web_search_call", "query":"web input"})),
            response(json!({"type":"tool_search_call", "arguments":"tool input"})),
            response(json!({"type":"tool_search_output", "tools":[{"name":"found"}]})),
            json!({"type":"event_msg", "payload":{"type":"agent_reasoning", "text":"Reasoning"}}),
            response(json!({"type":"message", "role":"assistant", "content":""})),
            response(json!({"type":"function_call_output", "output":""})),
        ];
        let added = vec![
            json!({"type":"event_msg", "payload":{"type":"task_started", "turn_id":"turn"}}),
            json!({"type":"event_msg", "payload":{"type":"task_complete", "turn_id":"turn"}}),
            json!({"type":"event_msg", "payload":{"type":"turn_aborted", "turn_id":"turn"}}),
            response(
                json!({"type":"message", "role":"user", "content":[{"type":"input_image", "image_url":"/tmp/image.png"}]}),
            ),
            response(
                json!({"type":"message", "role":"user", "content":[{"type":"input_file", "file_id":"file"}]}),
            ),
            response(
                json!({"type":"message", "role":"user", "content":"  <system_instruction>context</system_instruction>"}),
            ),
            response(
                json!({"type":"message", "role":"user", "content":"<system-instruction>context</system-instruction> Actual request"}),
            ),
            response(
                json!({"type":"message", "role":"assistant", "content":"<system_instruction>unclosed prefix"}),
            ),
            response(
                json!({"type":"message", "role":"developer", "content":"<system-instruction>context</system-instruction>"}),
            ),
        ];
        let mut mixed = Vec::new();
        for index in 0..legacy.len().max(added.len()) {
            if let Some(value) = added.get(index) {
                mixed.push(value.clone());
            }
            if let Some(value) = legacy.get(index) {
                mixed.push(value.clone());
            }
        }
        let encoded = |values: &[serde_json::Value]| {
            values.iter().map(|v| format!("{v}\n")).collect::<String>()
        };
        for include_reasoning in [false, true] {
            let parse = |state| {
                let mut records = Vec::new();
                let output = parse_index_records(
                    &path,
                    state,
                    include_reasoning,
                    &AtomicU64::new(1),
                    |record| {
                        records.push(record);
                        Ok(())
                    },
                )
                .unwrap();
                (records, output)
            };
            fs::write(&path, encoded(&legacy)).unwrap();
            let (baseline, _) = parse(IndexParseState::default());
            assert_eq!(baseline.len(), 10 + usize::from(include_reasoning));
            // Strip the added identity fields to calculate precisely the old
            // canonical hash from each pre-reader ordinal and source identity.
            let baseline_ids = baseline
                .iter()
                .enumerate()
                .map(|(ordinal, record)| {
                    let mut original = record.clone();
                    original.turn_id = ordinal as u32;
                    original.links.legacy_turn_id = None;
                    original.links.source_record_offset = None;
                    canonical_record_id(&original)
                })
                .collect::<Vec<_>>();
            fs::write(&path, encoded(&mixed)).unwrap();
            let (full, output) = parse(IndexParseState::default());
            let old_records = full
                .iter()
                .filter(|record| record.links.source_record_offset.is_none())
                .collect::<Vec<_>>();
            assert_eq!(
                old_records
                    .iter()
                    .map(|record| canonical_record_id(record))
                    .collect::<Vec<_>>(),
                baseline_ids
            );
            assert_eq!(
                old_records
                    .iter()
                    .map(|record| &record.text)
                    .collect::<Vec<_>>(),
                baseline
                    .iter()
                    .map(|record| &record.text)
                    .collect::<Vec<_>>()
            );
            assert_eq!(output.legacy_turn_id, Some(baseline.len() as u32));
            assert_eq!(full.len(), baseline.len() + added.len());
            let full_ids = full.iter().map(canonical_record_id).collect::<Vec<_>>();
            assert_eq!(full_ids.iter().collect::<HashSet<_>>().len(), full.len());

            fs::write(&path, "").unwrap();
            let mut state = IndexParseState::default();
            let mut incremental = Vec::new();
            for value in &mixed {
                writeln!(
                    fs::OpenOptions::new().append(true).open(&path).unwrap(),
                    "{value}"
                )
                .unwrap();
                let (records, output) = parse(state);
                incremental.extend(records);
                state = IndexParseState {
                    offset: output.offset,
                    turn_id: output.turn_id,
                    legacy_turn_id: output.legacy_turn_id,
                    pending_tool_calls: output.pending_tool_calls,
                };
            }
            assert_eq!(
                incremental
                    .iter()
                    .map(canonical_record_id)
                    .collect::<Vec<_>>(),
                full_ids
            );
            assert_eq!(
                incremental.iter().map(|r| r.turn_id).collect::<Vec<_>>(),
                full.iter().map(|r| r.turn_id).collect::<Vec<_>>()
            );
            assert_eq!(state.legacy_turn_id, Some(baseline.len() as u32));
            assert!(
                parse_index_records(
                    &path,
                    IndexParseState {
                        offset: state.offset,
                        turn_id: state.turn_id,
                        ..IndexParseState::default()
                    },
                    include_reasoning,
                    &AtomicU64::new(1),
                    |_| Ok(())
                )
                .is_err()
            );
        }
    }

    #[test]
    fn structured_tool_values_preserve_json_and_verbatim_strings() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        for output in [
            serde_json::json!({"ok":true,"rows":[1,2]}),
            serde_json::json!([{"type":"text","text":"hello", "extra":42}]),
            serde_json::json!("plain\noutput"),
        ] {
            fs::write(&path, format!("{}\n", serde_json::json!({"type":"response_item", "payload":{"type":"function_call_output", "call_id":"call", "output":output}}))).unwrap();
            let (records, _) = parse_transcript(&path, IndexParseState::default());
            let actual = records[0].tool_output.as_ref().unwrap();
            if let Some(text) = output.as_str() {
                assert_eq!(actual, text);
            } else {
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(actual).unwrap(),
                    output
                );
            }
        }
    }

    #[test]
    fn source_content_excludes_non_display_siblings() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let image = serde_json::json!({"type":"input_image", "image_url":"/tmp/photo.png"});
        fs::write(&path, format!("{}\n", serde_json::json!({"type":"response_item", "payload":{"type":"message", "role":"user", "content":[image, {"type":"reasoning", "encrypted_content":"private-ciphertext"}, {"type":"tool_use","input":"private-tool"}]}}))).unwrap();
        let (records, _) = parse_transcript(&path, IndexParseState::default());
        assert_eq!(records.len(), 1);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                records[0].links.source_content.as_ref().unwrap()
            )
            .unwrap(),
            serde_json::json!([image])
        );
        let serialized = serde_json::to_string(&records).unwrap();
        assert!(!serialized.contains("private-ciphertext"));
        assert!(!serialized.contains("private-tool"));
    }

    #[test]
    fn source_content_preserves_typed_attachments_and_image_only_messages() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let contents = [
            serde_json::json!([{"type":"input_image", "image_url":"data:image/png;base64,AAAA", "detail":"original"}]),
            serde_json::json!([{"type":"input_text", "text":"<<ImageDisplayed>> What is this?"}, {"type":"localImage", "path":"/tmp/photo.png"}]),
            serde_json::json!([{"type":"input_file", "file_id":"file-123", "filename":"notes.pdf"}]),
            serde_json::json!([{"type":"input_text", "text":"ordinary message"}]),
        ];
        fs::write(&path, contents.iter().map(|content| format!("{}\n", serde_json::json!({"type":"response_item", "payload":{"type":"message", "role":"user", "content":content}}))).collect::<String>()).unwrap();
        let (records, _) = parse_transcript(&path, IndexParseState::default());
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].text, "");
        assert_eq!(records[1].text, "<<ImageDisplayed>> What is this?");
        assert_eq!(records[2].text, "");
        for (record, content) in records[..3].iter().zip(&contents[..3]) {
            assert_eq!(
                &serde_json::from_str::<serde_json::Value>(
                    record.links.source_content.as_ref().unwrap()
                )
                .unwrap(),
                content
            );
            assert!(serde_json::to_value(record).unwrap()["source_content"].is_string());
        }
        assert!(records[3].links.source_content.is_none());
    }

    #[test]
    fn transcript_preserves_turn_phase_and_explicit_lifecycle_without_echoes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let lines = [
            serde_json::json!({"type":"event_msg", "payload":{"type":"task_started", "turn_id":"turn-a"}}),
            serde_json::json!({"type":"event_msg", "payload":{"type":"user_message", "message":"Question"}}),
            serde_json::json!({"type":"response_item", "payload":{"type":"message", "role":"user", "content":"Question", "id":"user-a"}}),
            serde_json::json!({"type":"turn_context", "payload":{"turn_id":"turn-a"}}),
            serde_json::json!({"type":"response_item", "payload":{"type":"message", "role":"assistant", "phase":"commentary", "content":"Working"}}),
            serde_json::json!({"type":"event_msg", "payload":{"type":"agent_message", "message":"Answer"}}),
            serde_json::json!({"type":"response_item", "payload":{"type":"message", "role":"assistant", "phase":"final_answer", "content":"Answer"}}),
            serde_json::json!({"type":"event_msg", "payload":{"type":"task_complete", "turn_id":"turn-a", "last_agent_message":"Answer"}}),
            serde_json::json!({"type":"response_item", "payload":{"type":"message", "role":"user", "content":"Question", "id":"user-b"}}),
        ];
        fs::write(
            &path,
            lines.iter().map(|v| format!("{v}\n")).collect::<String>(),
        )
        .unwrap();
        let (records, _) = parse_transcript(&path, IndexParseState::default());
        assert_eq!(records.len(), 6);
        assert!(
            records[..5]
                .iter()
                .all(|r| r.links.source_turn_id.as_deref() == Some("turn-a"))
        );
        assert_eq!(
            records[2].links.assistant_phase.as_deref(),
            Some("commentary")
        );
        assert_eq!(
            records[3].links.assistant_phase.as_deref(),
            Some("final_answer")
        );
        assert_eq!(
            records[4].links.lifecycle_event.as_deref(),
            Some("task_complete")
        );
        assert_eq!(records[4].role, "lifecycle");
        assert!(
            records[4]
                .tool_output
                .as_ref()
                .unwrap()
                .contains("last_agent_message")
        );
        let lifecycle: serde_json::Value =
            serde_json::from_str(records[4].tool_output.as_ref().unwrap()).unwrap();
        assert_eq!(lifecycle["type"], "task_complete");
        assert_eq!(lifecycle["last_agent_message"], "Answer");
        assert_eq!(records[5].links.source_turn_id, None);
        // Equal user text with distinct source records is not discarded.
        assert_eq!(records.iter().filter(|r| r.text == "Question").count(), 2);
        assert_eq!(
            records.iter().map(|r| r.turn_id).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4, 5]
        );
        let json = serde_json::to_value(&records[3]).unwrap();
        assert_eq!(json["assistant_phase"], "final_answer");
        assert_eq!(json["source_turn_id"], "turn-a");
        assert!(json.get("lifecycle_event").is_none());
    }

    #[test]
    fn standalone_injected_instructions_are_preserved_without_hiding_bundled_requests() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let texts = [
            "<system_instruction>Injected context</system_instruction>",
            "<system-instruction>Other context</system-instruction>",
            "<system_instruction>Context</system_instruction>\nActual request",
            "Please explain <system_instruction> literally",
        ];
        fs::write(&path, texts.iter().map(|text| format!("{}\n", serde_json::json!({"type":"response_item", "payload":{"type":"message", "role":"user", "content":text}}))).collect::<String>()).unwrap();
        let (records, _) = parse_transcript(&path, IndexParseState::default());
        assert_eq!(
            records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            texts
        );
        assert_eq!(
            records.iter().map(|r| r.role.as_str()).collect::<Vec<_>>(),
            ["system", "system", "user", "user"]
        );
        assert!(
            records
                .iter()
                .all(|r| r.links.source_record_type.as_deref() == Some("response_item/message"))
        );
        fs::write(&path, format!("{}\n", serde_json::json!({"type":"response_item", "payload":{"type":"message", "role":"assistant", "content":texts[0]}}))).unwrap();
        let (records, _) = parse_transcript(&path, IndexParseState::default());
        assert_eq!(records[0].role, "assistant");
        assert_eq!(records[0].text, texts[0]);
    }

    #[test]
    fn incremental_transcript_recovers_turn_identity_and_does_not_infer_completion() {
        use std::io::Write;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        fs::write(
            &path,
            "{\"type\":\"turn_context\",\"payload\":{\"turn_id\":\"turn-a\"}}\n",
        )
        .unwrap();
        let (_, first) = parse_transcript(&path, IndexParseState::default());
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(file, "{}", serde_json::json!({"type":"response_item", "payload":{"type":"message", "role":"assistant", "phase":"final_answer", "content":"Partial answer"}})).unwrap();
        let (records, second) = parse_transcript(
            &path,
            IndexParseState {
                offset: first.offset,
                turn_id: first.turn_id,
                legacy_turn_id: first.legacy_turn_id,
                ..IndexParseState::default()
            },
        );
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].links.source_turn_id.as_deref(), Some("turn-a"));
        assert_eq!(records[0].links.lifecycle_event, None);
        writeln!(file, "{}", serde_json::json!({"type":"event_msg", "payload":{"type":"turn_aborted", "turn_id":"turn-a", "reason":"interrupted"}})).unwrap();
        let (records, _) = parse_transcript(
            &path,
            IndexParseState {
                offset: second.offset,
                turn_id: second.turn_id,
                legacy_turn_id: second.legacy_turn_id,
                ..IndexParseState::default()
            },
        );
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].links.source_turn_id.as_deref(), Some("turn-a"));
        assert_eq!(
            records[0].links.lifecycle_event.as_deref(),
            Some("turn_aborted")
        );
    }

    #[test]
    fn metadata_offsets_recover_partial_headers_and_reject_invalid_checkpoints() {
        let path = Path::new("rollout.jsonl");
        let first = br#"{"type":"session_meta","payload":{"id":"first","cwd":"/first"}}
"#;
        let other = br#"{"type":"response_item","payload":{"text":"ignored"}}
"#;
        let last = br#"{"type":"session_meta","payload":{"id":"last","forked_from_id":"parent"}}
"#;
        let bytes = [first.as_slice(), other.as_slice(), last.as_slice()].concat();
        let (full, offsets) = read_meta_prefix(path, &bytes, None);
        assert_eq!(offsets, vec![0, (first.len() + other.len()) as u64]);
        let (cached, reused) = read_meta_prefix(path, &bytes, Some(&offsets));
        assert_eq!(reused, offsets);
        assert_eq!(cached.session_id, full.session_id);
        assert_eq!(cached.cwd, full.cwd);
        assert_eq!(cached.cwd.as_deref(), Some(Path::new("/first")));
        assert_eq!(cached.links.parent_session_id, full.links.parent_session_id);
        for invalid in [
            vec![u64::MAX],
            vec![1],
            vec![first.len() as u64],
            vec![0, 0],
        ] {
            let (recovered, positions) = read_meta_prefix(path, &bytes, Some(&invalid));
            assert_eq!(recovered.session_id, full.session_id);
            assert_eq!(positions, offsets);
        }
    }

    #[test]
    fn incremental_metadata_offsets_include_new_headers() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("rollout.jsonl");
        let first = r#"{"type":"session_meta","payload":{"id":"first","cwd":"/first"}}
"#;
        std::fs::write(&path, first).unwrap();
        let ids = AtomicU64::new(1);
        let (parsed, offsets) = parse_index_records_with_metadata_offsets(
            &path,
            IndexParseState::default(),
            false,
            &ids,
            None,
            |_| Ok(()),
        )
        .unwrap();
        let appended = r#"{"type":"session_meta","payload":{"id":"first","cwd":"/last"}}
{"type":"response_item","payload":{"type":"message","role":"user","content":"new text"}}
"#;
        std::fs::write(&path, format!("{first}{appended}")).unwrap();
        let mut records = Vec::new();
        let (_, updated) = parse_index_records_with_metadata_offsets(
            &path,
            IndexParseState {
                offset: parsed.offset,
                turn_id: parsed.turn_id,
                legacy_turn_id: parsed.legacy_turn_id,
                pending_tool_calls: parsed.pending_tool_calls,
            },
            false,
            &ids,
            Some(&offsets),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(updated, vec![0, first.len() as u64]);
        assert_eq!(records.len(), 1);
        // One rollout file owns one session identity, so a later header updates the
        // working directory without reassigning the session.
        assert_eq!(records[0].session_id, "first");
        assert_eq!(records[0].project, "last");
        assert_eq!(
            cwd_with_metadata_checkpoint(&path, first.len() as u64, &offsets)
                .unwrap()
                .as_deref(),
            Some(Path::new("/last"))
        );
    }

    #[test]
    fn metadata_prefilter_preserves_escaped_types_and_later_updates() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("rollout.jsonl");
        let initial = r#"{"type":"session_meta","payload":{"id":"first","cwd":"/first"}}
{"type":"response_item","payload":{"text":"session_meta is ordinary content"}}
{"type":"response_item","payload":{"text":"escaped \u0073ession_meta content"}}
not json
"#;
        let later = r#"{"type":"ses\u0073ion_meta","payload":{"id":"first","cwd":"/last","forked_from_id":"parent"}}
"#;
        std::fs::write(&path, format!("{initial}{later}")).unwrap();
        let early = read_meta_until(&path, initial.len() as u64).unwrap();
        assert_eq!(early.session_id, "first");
        assert_eq!(early.cwd.as_deref(), Some(Path::new("/first")));
        let latest = read_meta_until(&path, u64::MAX).unwrap();
        assert_eq!(latest.session_id, "first");
        assert_eq!(latest.cwd.as_deref(), Some(Path::new("/last")));
        assert_eq!(latest.links.parent_session_id.as_deref(), Some("parent"));
    }

    #[test]
    fn metadata_prefilter_handles_fully_escaped_type_and_key() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("rollout.jsonl");
        std::fs::write(&path, r#"{"\u0074ype":"\u0073\u0065\u0073\u0073\u0069\u006f\u006e\u005f\u006d\u0065\u0074\u0061","payload":{"id":"escaped","cwd":"/escaped"}}
"#).unwrap();
        let metadata = read_meta_until(&path, u64::MAX).unwrap();
        assert_eq!(metadata.session_id, "escaped");
        assert_eq!(metadata.cwd.as_deref(), Some(Path::new("/escaped")));
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
            codex_thread_title_metadata(
                &mut connection
                    .prepare(&title_metadata_query(&connection).unwrap())
                    .unwrap(),
                "session-1"
            )
            .unwrap()
            .title
            .as_deref(),
            Some("Pinned name")
        );
        assert_eq!(
            codex_thread_title_metadata(
                &mut connection
                    .prepare(&title_metadata_query(&connection).unwrap())
                    .unwrap(),
                "missing"
            ),
            None
        );
    }

    #[test]
    fn codex_title_metadata_filters_each_candidate_before_falling_back() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(
            "CREATE TABLE threads (id TEXT, name TEXT, title TEXT, first_user_message TEXT, agent_path TEXT);
             INSERT INTO threads VALUES ('title', '<environment_context>injected</environment_context>', 'Useful title', 'Actual request', '/root/cache_invalidation');
             INSERT INTO threads VALUES ('first', '  ', '<recommended_plugins>injected</recommended_plugins>', 'Actual request', '/root');
             INSERT INTO threads VALUES ('empty', NULL, '', '<INSTRUCTIONS>injected</INSTRUCTIONS>', '/root/');"
        ).unwrap();
        let mut statement = connection
            .prepare(&title_metadata_query(&connection).unwrap())
            .unwrap();
        assert_eq!(
            codex_thread_title_metadata(&mut statement, "title"),
            Some(SessionTitleMetadata {
                title: Some("Useful title".into()),
                first_user_message: Some("Actual request".into()),
                agent_path: Some("/root/cache_invalidation".into()),
            })
        );
        assert_eq!(
            codex_thread_title_metadata(&mut statement, "first"),
            Some(SessionTitleMetadata {
                title: None,
                first_user_message: Some("Actual request".into()),
                agent_path: None,
            })
        );
        assert_eq!(codex_thread_title_metadata(&mut statement, "empty"), None);
    }

    #[test]
    fn codex_title_metadata_reads_older_optional_column_sets() {
        for schema in [
            "CREATE TABLE threads (id TEXT, title TEXT); INSERT INTO threads VALUES ('s', 'Older title');",
            "CREATE TABLE threads (id TEXT, title TEXT, first_user_message TEXT); INSERT INTO threads VALUES ('s', 'Older title', 'First prompt');",
        ] {
            let connection = Connection::open_in_memory().unwrap();
            connection.execute_batch(schema).unwrap();
            let mut statement = connection
                .prepare(&title_metadata_query(&connection).unwrap())
                .unwrap();
            let metadata = codex_thread_title_metadata(&mut statement, "s").unwrap();
            assert_eq!(metadata.title.as_deref(), Some("Older title"));
            assert_eq!(metadata.agent_path, None);
            if schema.contains("first_user_message") {
                assert_eq!(metadata.first_user_message.as_deref(), Some("First prompt"));
            } else {
                assert_eq!(metadata.first_user_message, None);
            }
        }
    }

    #[test]
    fn codex_title_metadata_preserves_only_rooted_agent_paths() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads (id TEXT, agent_path TEXT);
            INSERT INTO threads VALUES ('valid', '/root/research/cache_invalidation'),
                ('root', '/root'), ('blank', '/root/  '), ('relative', 'cache_invalidation');",
            )
            .unwrap();
        let mut statement = connection
            .prepare(&title_metadata_query(&connection).unwrap())
            .unwrap();
        assert_eq!(
            codex_thread_title_metadata(&mut statement, "valid"),
            Some(SessionTitleMetadata {
                agent_path: Some("/root/research/cache_invalidation".into()),
                ..Default::default()
            })
        );
        for id in ["root", "blank", "relative"] {
            assert_eq!(codex_thread_title_metadata(&mut statement, id), None);
        }
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
    fn probe_marks_role_string_subagent_source() {
        // Older CLIs write `"source": {"subagent": "review"}` with neither
        // `thread_spawn` nor an explicit `thread_source`.
        let temp = tempfile::tempdir().unwrap();
        let path = temp
            .path()
            .join("rollout-2026-01-26T13-38-17-019bfb99-7735-77a0-8792-176cdc56fda7.jsonl");
        fs::write(
            &path,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"019bfb99-7735-77a0-8792-176cdc56fda7\",\"cwd\":\"/repo/memex\",\"source\":{\"subagent\":\"review\"}}}\n",
        )
        .unwrap();
        let metadata = probe(&path).unwrap();
        assert_eq!(
            metadata.session.conversation_kind,
            ConversationKind::Subagent
        );
        assert_eq!(metadata.session.parent_session_id, None);
    }

    #[test]
    fn guardian_reviews_preserve_identity_and_parent_in_both_projections() {
        for marker in [
            serde_json::json!({"thread_source": "guardian_review"}),
            serde_json::json!({"source": {"subagent": {"other": "guardian"}}}),
            serde_json::json!({"thread_source": "guardian_review", "source": {"subagent": {"other": "guardian"}}}),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("guardian.jsonl");
            let mut payload = marker;
            payload["id"] = serde_json::json!("guardian-session");
            payload["parent_thread_id"] = serde_json::json!("parent-session");
            fs::write(
                &path,
                format!(
                    "{}\n{}\n",
                    serde_json::json!({"type": "session_meta", "payload": payload}),
                    serde_json::json!({"type": "response_item", "payload": {
                        "type": "message", "role": "assistant", "content": [
                            {"type": "output_text", "text": "Permission allowed"}
                        ]
                    }})
                ),
            )
            .unwrap();
            let metadata = probe(&path).unwrap();
            assert_eq!(
                metadata.session.conversation_kind,
                ConversationKind::GuardianReview
            );
            assert_eq!(
                metadata.session.parent_session_id.as_deref(),
                Some("parent-session")
            );
            let mut records = Vec::new();
            parse_index_records(
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
            assert_eq!(records.len(), 1);
            assert_eq!(
                records[0].links.conversation_kind.as_deref(),
                Some("guardian_review")
            );
            assert_eq!(
                records[0].links.parent_session_id.as_deref(),
                Some("parent-session")
            );
            let mut bytes = serde_json::to_vec(&payload).unwrap();
            let borrowed = simd_json::to_borrowed_value(&mut bytes).unwrap();
            assert_eq!(
                usage_parent_session_id(&borrowed).as_deref(),
                Some("parent-session")
            );
        }
    }

    #[test]
    fn guardian_usage_keeps_initial_tokens_without_inheriting_parent_counters() {
        for marker in [
            serde_json::json!({"thread_source": "guardian_review"}),
            serde_json::json!({"source": {"subagent": {"other": "guardian"}}}),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("guardian.jsonl");
            let mut payload = marker;
            payload["id"] = serde_json::json!("review");
            payload["parent_thread_id"] = serde_json::json!("parent");
            fs::write(&path, format!("{}\n{}\n{}\n",
                serde_json::json!({"type": "session_meta", "timestamp": 1, "payload": payload}),
                serde_json::json!({"type": "event_msg", "timestamp": 2, "payload": {
                    "type": "token_count", "info": {"total_token_usage": {"input_tokens": 10, "output_tokens": 2}}
                }}),
                serde_json::json!({"type": "response", "timestamp": 3, "usage": {"input_tokens": 20, "output_tokens": 3}})
            )).unwrap();
            let parsed = parse_usage_file(&path, &UsageParentIndex::new(&[])).unwrap();
            assert!(parsed.cacheable);
            assert!(parsed.deps.is_empty());
            assert_eq!(parsed.events.len(), 2);
            assert!(parsed.events.iter().all(|event| event.permission_review));
            assert_eq!(parsed.events[0].tokens.additive_total(), 12);
            assert_eq!(parsed.events[1].tokens.additive_total(), 23);
        }
    }

    #[test]
    fn top_level_parents_preserve_ordinary_subagents_and_fork_precedence() {
        for (payload, expected_kind, expected_parent) in [
            (
                serde_json::json!({"source": {"subagent": {"other": "review"}}, "parent_thread_id": "parent"}),
                ConversationKind::Subagent,
                "parent",
            ),
            (
                serde_json::json!({"forked_from_id": "fork", "parent_thread_id": "parent"}),
                ConversationKind::Fork,
                "fork",
            ),
            (
                serde_json::json!({"thread_source": "guardian_review", "forked_from_id": "fork", "parent_thread_id": "parent"}),
                ConversationKind::GuardianReview,
                "fork",
            ),
        ] {
            let mut bytes = serde_json::to_vec(&payload).unwrap();
            let borrowed = simd_json::to_borrowed_value(&mut bytes).unwrap();
            let mut metadata = fallback_meta(Path::new("session.jsonl"));
            apply_meta(borrowed.as_object().unwrap(), &mut metadata);
            assert_eq!(
                metadata.links.conversation_kind.as_deref(),
                Some(expected_kind.as_str())
            );
            assert_eq!(
                metadata.links.parent_session_id.as_deref(),
                Some(expected_parent)
            );
            assert_eq!(
                usage_parent_session_id(&borrowed).as_deref(),
                Some(expected_parent)
            );
        }
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
