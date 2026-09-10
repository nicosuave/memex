use super::{
    ConversationKind, IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions,
    SessionIdentity, SourceFile, SourceMetadata,
};
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::{Context, Result};
use memchr::memmem;
use once_cell::sync::Lazy;
use serde_json::Value;
use simd_json::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use walkdir::WalkDir;

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 2,
    // Preserve pre-reader fallback IDs independently of attachment-only messages.
    index: 6,
    usage: 4,
};

/// Return the human-facing title Claude stores alongside a conversation.
///
/// Title records are metadata rather than transcript messages, so the search
/// index intentionally does not contain them. The shared session catalog reads
/// this metadata so every client sees saved names and subsequent renames.
pub fn session_title(path: &Path, session_id: &str) -> Option<String> {
    use memchr::memchr;
    use memmap2::Mmap;
    let file = File::open(path).ok()?;
    let bytes = unsafe { Mmap::map(&file).ok()? };
    let mut custom_title = None;
    let mut ai_title = None;
    let mut agent_name = None;

    let mut offset = 0;
    while offset < bytes.len() {
        let remaining = &bytes[offset..];
        let length = memchr(b'\n', remaining).unwrap_or(remaining.len());
        let line = &remaining[..length];
        offset += length + usize::from(length < remaining.len());
        // Most lines contain large transcript/tool bodies. Only metadata
        // candidates need JSON decoding; still inspect every line for renames.
        // Escaped JSON strings may encode the type using Unicode escapes.
        if ![
            b"custom-title".as_slice(),
            b"ai-title",
            b"agent-name",
            b"\\u",
        ]
        .iter()
        .any(|needle| memmem::find(line, needle).is_some())
        {
            continue;
        }
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        if value
            .get("sessionId")
            .and_then(Value::as_str)
            .is_some_and(|id| id != session_id)
        {
            continue;
        }
        let Some(entry_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };
        let title = match entry_type {
            "custom-title" => value
                .get("customTitle")
                .or_else(|| value.get("title"))
                .and_then(Value::as_str),
            "ai-title" => value.get("aiTitle").and_then(Value::as_str),
            "agent-name" => value
                .get("agentName")
                .or_else(|| value.get("name"))
                .and_then(Value::as_str),
            _ => None,
        }
        .map(crate::analytics::sanitize_label)
        .filter(|title| !title.is_empty());

        match entry_type {
            "custom-title" if title.is_some() => custom_title = title,
            "ai-title" if title.is_some() => ai_title = title,
            "agent-name" if title.is_some() => agent_name = title,
            _ => {}
        }
    }

    custom_title.or(ai_title).or(agent_name)
}

pub fn discover(root: &Path, _include_agents: bool) -> Result<Vec<SourceFile>> {
    // `_include_agents` is a retired opt-in kept only for CLI compatibility:
    // agent transcripts are always indexed now, matching every other source.
    // Consumers hide them from default views via `conversation_kind`.
    let mut files = Vec::new();
    for entry in WalkDir::new(root).into_iter().filter_map(Result::ok) {
        if !entry.file_type().is_file()
            || entry.path().extension().and_then(|ext| ext.to_str()) != Some("jsonl")
        {
            continue;
        }
        let name = entry.file_name().to_string_lossy();
        let is_agent = name.starts_with("agent-");
        let under_subagents = entry.path().ancestors().any(|ancestor| {
            ancestor.file_name().and_then(|name| name.to_str()) == Some("subagents")
        });
        if under_subagents && !is_agent {
            // Workflow journals and other non-transcript files.
            continue;
        }
        // Standard sessions live at most two levels below root
        // (`<project>/<session>.jsonl`); only agent transcripts nest
        // deeper, under a `subagents/` directory (see `is_subagent_path`).
        // Anything deeper outside `subagents/` is not a session file.
        let relative_depth = entry
            .path()
            .strip_prefix(root)
            .map(|path| path.components().count())
            .unwrap_or(0);
        if relative_depth > 2 && !under_subagents {
            continue;
        }
        files.push(SourceFile {
            source: SourceKind::Claude,
            path: entry.path().to_path_buf(),
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

pub fn usage_files() -> Vec<PathBuf> {
    super::common::jsonl_files(crate::config::default_claude_sources())
}

pub fn session_id_from_path(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

pub fn project_from_path(path: &Path) -> String {
    path.parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .map(decode_project_name)
        .unwrap_or_else(|| SourceKind::Claude.label().to_string())
}

fn decode_project_name(folder_name: &str) -> String {
    let prefixes_to_strip = ["-home-", "-mnt-c-Users-", "-mnt-c-users-", "-Users-"];
    let mut name = folder_name;
    if name.len() > 10 {
        let bytes = name.as_bytes();
        if bytes[0] == b'-'
            && bytes[2] == b'-'
            && bytes[3] == b'-'
            && bytes[1].is_ascii_alphabetic()
            && name[4..].to_lowercase().starts_with("users-")
        {
            name = &name[10..];
        }
    }
    for prefix in prefixes_to_strip {
        if name.to_lowercase().starts_with(&prefix.to_lowercase()) {
            name = &name[prefix.len()..];
            break;
        }
    }
    let parts: Vec<&str> = name.split('-').filter(|part| !part.is_empty()).collect();
    let skip_dirs = [
        "projects",
        "code",
        "repos",
        "src",
        "dev",
        "work",
        "documents",
    ];
    let mut meaningful = Vec::new();
    let mut found_project = false;
    for (index, part) in parts.iter().enumerate() {
        if index == 0 && !found_project {
            let remaining: Vec<String> = parts[index + 1..]
                .iter()
                .map(|part| part.to_lowercase())
                .collect();
            if remaining
                .iter()
                .any(|directory| skip_dirs.contains(&directory.as_str()))
            {
                continue;
            }
        }
        if skip_dirs.contains(&part.to_lowercase().as_str()) {
            found_project = true;
            continue;
        }
        meaningful.push(*part);
        found_project = true;
    }
    if meaningful.is_empty() {
        folder_name.to_string()
    } else {
        meaningful.join("-")
    }
}

pub fn is_subagent_path(path: &Path) -> bool {
    session_id_from_path(path).starts_with("agent-")
        || path
            .components()
            .any(|component| component.as_os_str().to_str() == Some("subagents"))
}

pub fn classify(path: &Path, is_sidechain: bool) -> ConversationKind {
    if is_subagent_path(path) {
        ConversationKind::Subagent
    } else if is_sidechain {
        ConversationKind::Sidechain
    } else {
        ConversationKind::Main
    }
}

pub fn probe(path: &Path) -> Result<SourceMetadata> {
    let fallback_id = session_id_from_path(path);
    let mut session_id = fallback_id;
    let mut parent_session_id = None;
    let mut cwd = None;
    let mut project = Some(project_from_path(path));
    let mut conversation_kind = classify(path, false);

    let reader = BufReader::new(File::open(path)?);
    for line in reader.lines().take(64) {
        let Ok(line) = line else { continue };
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if let Some(id) = value.get("sessionId").and_then(Value::as_str) {
            session_id = id.to_string();
        }
        if let Some(value_cwd) = value.get("cwd").and_then(Value::as_str) {
            cwd = Some(PathBuf::from(value_cwd));
            project = Some(super::common::project_from_path(value_cwd));
        }
        let sidechain = value
            .get("isSidechain")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        conversation_kind = classify(path, sidechain);
        if conversation_kind == ConversationKind::Subagent {
            parent_session_id = value
                .get("sessionId")
                .and_then(Value::as_str)
                .map(str::to_string);
        }
        if cwd.is_some() && value.get("sessionId").is_some() {
            break;
        }
    }

    Ok(SourceMetadata {
        session: SessionIdentity {
            source: SourceKind::Claude,
            session_id,
            parent_session_id,
            conversation_kind,
            source_path: path.to_path_buf(),
        },
        cwd,
        project,
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
    let mut legacy_turn_id = state.legacy_ordinal()?;
    let mut emit = |mut record: Record| {
        if record.links.source_record_offset.is_none() {
            record.links.legacy_turn_id = Some(legacy_turn_id);
            legacy_turn_id += 1;
        }
        emit(record)
    };
    use memchr::memchr;
    use memmap2::Mmap;
    use simd_json::prelude::*;

    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut start = state.offset as usize;
    let mut turn_id = state.turn_id;
    let mut pending_tool_calls = state.pending_tool_calls;
    let project = project_from_path(path);
    let session_id = session_id_from_path(path);
    let is_agent_file = is_subagent_path(path);
    let source_path = path.to_string_lossy().to_string();
    let mut buffer = Vec::new();
    let mut diagnostics = ParseDiagnostics::default();

    while start < mmap.len() {
        let source_record_offset = start as u64;
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
        if entry_type != "user" && entry_type != "assistant" {
            if !matches!(
                entry_type,
                "progress"
                    | "summary"
                    | "system"
                    | "file-history-snapshot"
                    | "queue-operation"
                    | "pr-link"
                    | "last-prompt"
                    | "custom-title"
                    | "ai-title"
                    | "agent-name"
                    | "permission-mode"
                    | "attachment"
                    | "mode"
            ) {
                diagnostics.increment_unknown_top_level(entry_type);
            }
            continue;
        }
        let entry_uuid = super::common::borrowed_string(object, "uuid");
        let entry_parent_uuid = super::common::borrowed_string(object, "parentUuid");
        let sidechain = object
            .get("isSidechain")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let kind = classify(path, sidechain);
        let thread_source = match kind {
            ConversationKind::Subagent => Some("subagent".to_string()),
            ConversationKind::Sidechain => Some("sidechain".to_string()),
            _ => None,
        };
        let entry_links = RecordLinks {
            event_id: entry_uuid.clone(),
            parent_event_id: entry_parent_uuid,
            logical_parent_event_id: super::common::borrowed_string(object, "logicalParentUuid"),
            parent_session_id: is_agent_file
                .then(|| super::common::borrowed_string(object, "sessionId"))
                .flatten(),
            thread_source,
            conversation_kind: Some(kind.as_str().to_string()),
            parent_tool_use_id: super::common::borrowed_string(object, "parentToolUseID"),
            source_tool_use_id: super::common::borrowed_string(object, "sourceToolUseID"),
            source_tool_assistant_uuid: super::common::borrowed_string(
                object,
                "sourceToolAssistantUUID",
            ),
            ..RecordLinks::default()
        };
        let timestamp = object
            .get("timestamp")
            .and_then(|value| value.as_str())
            .and_then(super::common::parse_iso_millis)
            .unwrap_or(0);
        let Some(message) = object.get("message").and_then(|value| value.as_object()) else {
            continue;
        };
        let content = message.get("content");
        let mut text_parts = Vec::new();
        let mut content_index = 0usize;
        if let Some(content) = content {
            if let Some(text) = content.as_str() {
                text_parts.push(text);
            } else if let Some(array) = content.as_array() {
                for block in array {
                    let Some(block_object) = block.as_object() else {
                        continue;
                    };
                    match block_object
                        .get("type")
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                    {
                        "text" => {
                            if let Some(text) =
                                block_object.get("text").and_then(|value| value.as_str())
                            {
                                text_parts.push(text);
                            }
                        }
                        "tool_use" => {
                            let tool_name = block_object
                                .get("name")
                                .and_then(|value| value.as_str())
                                .map(str::to_string);
                            let tool_input = block_object
                                .get("input")
                                .and_then(super::common::tool_value_text);
                            let text = tool_input.clone().unwrap_or_default();
                            let tool_id = block_object
                                .get("id")
                                .and_then(|value| value.as_str())
                                .map(str::to_string);
                            let mut links = entry_links.clone();
                            if let Some(tool_id) = &tool_id {
                                links.event_id = Some(tool_id.clone());
                                links.parent_event_id = entry_uuid.clone();
                            }
                            let doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
                            if let Some(tool_id) = tool_id {
                                let replaced = pending_tool_calls.insert(
                                    tool_id.clone(),
                                    super::common::pending_tool_call(
                                        tool_name.clone(),
                                        Some(tool_id),
                                        doc_id,
                                        timestamp,
                                        tool_input.as_deref(),
                                        &links,
                                        &session_id,
                                    ),
                                );
                                if replaced.is_some() {
                                    diagnostics.duplicate_tool_calls += 1;
                                }
                            }
                            emit(Record {
                                source: SourceKind::Claude,
                                doc_id,
                                ts: timestamp,
                                project: project.clone(),
                                session_id: session_id.clone(),
                                turn_id,
                                role: "tool_use".to_string(),
                                text,
                                tool_name,
                                tool_input,
                                tool_output: None,
                                links,
                                source_path: source_path.clone(),
                            })?;
                            turn_id += 1;
                        }
                        "thinking" => {
                            let thinking = block_object
                                .get("thinking")
                                .and_then(|value| value.as_str())
                                .map(str::trim)
                                .filter(|text| !text.is_empty());
                            if let Some(thinking) = thinking {
                                if include_reasoning {
                                    let mut links = entry_links.clone();
                                    links.event_id = entry_uuid
                                        .as_ref()
                                        .map(|uuid| format!("{uuid}:reasoning:{content_index}"));
                                    emit(Record {
                                        source: SourceKind::Claude,
                                        doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                                        ts: timestamp,
                                        project: project.clone(),
                                        session_id: session_id.clone(),
                                        turn_id,
                                        role: "reasoning".to_string(),
                                        text: thinking.to_string(),
                                        tool_name: None,
                                        tool_input: None,
                                        tool_output: None,
                                        links,
                                        source_path: source_path.clone(),
                                    })?;
                                    turn_id += 1;
                                }
                            } else if block_object.contains_key("signature")
                                || block_object.contains_key("data")
                            {
                                diagnostics.encrypted_reasoning_dropped += 1;
                            }
                        }
                        "redacted_thinking" => {
                            diagnostics.encrypted_reasoning_dropped += 1;
                        }
                        "tool_result" | "image" | "document" => {}
                        unknown => diagnostics.increment_unknown_semantic(unknown),
                    }
                    content_index += 1;
                }
            }
        }

        if entry_type == "user"
            && let Some(array) = content.and_then(|value| value.as_array())
        {
            for block in array {
                let Some(block_object) = block.as_object() else {
                    continue;
                };
                if block_object.get("type").and_then(|value| value.as_str()) != Some("tool_result")
                {
                    continue;
                }
                let tool_output = block_object
                    .get("content")
                    .and_then(super::common::tool_value_text);
                let mut text = super::common::tool_result_text(block).unwrap_or_default();
                if text.is_empty()
                    && let Some(content) = block_object.get("content")
                {
                    text = super::common::tool_value_text(content).unwrap_or_default();
                }
                let tool_use_id = block_object
                    .get("tool_use_id")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let pending = tool_use_id
                    .as_ref()
                    .and_then(|id| pending_tool_calls.remove(id));
                if tool_use_id.is_some() && pending.is_none() {
                    diagnostics.orphan_tool_results += 1;
                }
                let tool_name = pending.and_then(|call| call.tool_name);
                let mut links = entry_links.clone();
                links.tool_result_is_error = block_object.get("is_error").and_then(|v| v.as_bool());
                if let Some(tool_use_id) = &tool_use_id {
                    links.event_id = entry_uuid
                        .as_ref()
                        .map(|uuid| format!("{uuid}:tool_result:{tool_use_id}"));
                    links.parent_event_id = Some(tool_use_id.clone());
                    links.parent_tool_use_id = Some(tool_use_id.clone());
                }
                emit(Record {
                    source: SourceKind::Claude,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: project.clone(),
                    session_id: session_id.clone(),
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
        }

        let text = text_parts.join(" ").trim().to_string();
        let mut entry_links = entry_links;
        if let Some(array) = content.and_then(|v| v.as_array())
            && array.iter().any(|block| {
                matches!(
                    block.get("type").and_then(|v| v.as_str()),
                    Some("image" | "document")
                )
            })
        {
            // Tool and reasoning blocks have their own governed projections. Do not
            // duplicate them or expose hidden thinking through attachment metadata.
            let display_blocks = array
                .iter()
                .filter(|block| {
                    matches!(
                        block.get("type").and_then(|v| v.as_str()),
                        Some("text" | "image" | "document")
                    )
                })
                .collect::<Vec<_>>();
            entry_links.source_content = Some(serde_json::to_string(&display_blocks)?);
        }
        if !text.is_empty() || entry_links.source_content.is_some() {
            if text.is_empty() {
                entry_links.source_record_offset = Some(source_record_offset);
            }
            emit(Record {
                source: SourceKind::Claude,
                doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                ts: timestamp,
                project: project.clone(),
                session_id: session_id.clone(),
                turn_id,
                role: entry_type.to_string(),
                text,
                tool_name: None,
                tool_input: None,
                tool_output: None,
                links: entry_links,
                source_path: source_path.clone(),
            })?;
            turn_id += 1;
        }
    }

    Ok(IndexParseOutput {
        offset: mmap.len() as u64,
        turn_id,
        legacy_turn_id: Some(legacy_turn_id),
        pending_tool_calls,
        session_id: Some(session_id),
        diagnostics,
    })
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<Vec<UsageEvent>> {
    static USAGE_NEEDLE: Lazy<memmem::Finder<'static>> =
        Lazy::new(|| memmem::Finder::new(b"\"usage\""));
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let fallback_session = Some(session_id_from_path(path));
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut line = Vec::with_capacity(16 * 1024);
    let mut index = 0u64;
    let mut events = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if USAGE_NEEDLE.find(&line).is_some()
            && let Ok(value) = simd_json::to_borrowed_value(&mut line)
            && value.get("type").and_then(|value| value.as_str()) == Some("assistant")
            && let Some(message) = value.get("message")
            && let Some(usage) = message.get("usage")
        {
            let number = |value: &simd_json::BorrowedValue, names: &[&str]| {
                names
                    .iter()
                    .find_map(|name| value.get(*name).and_then(|value| value.as_u64()))
                    .unwrap_or(0)
            };
            let cache_write = number(
                usage,
                &["cache_creation_input_tokens", "cacheCreationInputTokens"],
            );
            let mut tokens = TokenBuckets::disjoint(
                number(usage, &["input_tokens", "inputTokens"]),
                number(usage, &["cache_read_input_tokens", "cacheReadInputTokens"]),
                cache_write,
                number(usage, &["output_tokens", "outputTokens"]),
            );
            tokens.cache_write_1h = usage
                .get("cache_creation")
                .and_then(|cache| cache.get("ephemeral_1h_input_tokens"))
                .and_then(|value| value.as_u64())
                .unwrap_or_default()
                .min(tokens.cache_write);
            if tokens.additive_total() > 0 {
                let string = |names: &[&str]| {
                    names
                        .iter()
                        .find_map(|name| value.get(*name).and_then(|value| value.as_str()))
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                };
                let session_id = string(&["sessionId", "session_id"]);
                let request_id = string(&["requestId", "request_id"]);
                let message_id = message
                    .get("id")
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.is_empty())
                    .map(str::to_string);
                let exact_dedupe = message_id.is_some() && request_id.is_some();
                let project = value
                    .get("cwd")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let timestamp_ms = value
                    .get("timestamp")
                    .map(usage_timestamp_millis)
                    .unwrap_or(0);
                events.push(UsageEvent {
                    source: "claude",
                    source_path: source_path.clone(),
                    source_record_id: Some(format!("line:{index}")),
                    session_id: session_id.or_else(|| fallback_session.clone()),
                    request_id,
                    message_id,
                    timestamp_ms,
                    project,
                    provider: Some("anthropic".into()),
                    model: message
                        .get("model")
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty())
                        .map(str::to_string),
                    tokens,
                    source_cost_usd: value.get("costUSD").and_then(|value| value.as_f64()),
                    cost_authoritative: false,
                    dedupe_confidence: if exact_dedupe { "exact" } else { "heuristic" },
                    conservative_undercount: false,
                    cache_chain_excluded: false,
                    permission_review: false,
                    sidechain: value
                        .get("isSidechain")
                        .and_then(|value| value.as_bool())
                        .unwrap_or(false),
                    source_order: index,
                });
            }
        }
        index += 1;
    }
    Ok(events)
}

fn usage_timestamp_millis(timestamp: &simd_json::BorrowedValue<'_>) -> u64 {
    timestamp
        .as_u64()
        .map(|value| {
            if value < 10_000_000_000 {
                value.saturating_mul(1_000)
            } else {
                value
            }
        })
        .or_else(|| {
            timestamp
                .as_i64()
                .filter(|value| *value >= 0)
                .map(|value| value as u64)
        })
        .or_else(|| timestamp.as_str().and_then(super::common::parse_iso_millis))
        .unwrap_or(0)
}

pub(crate) fn reconcile_usage(events: &mut Vec<UsageEvent>) {
    use std::collections::HashMap;

    let mut best_exact: HashMap<(&str, &str), usize> = HashMap::new();
    let mut keep = vec![true; events.len()];
    for (index, event) in events.iter().enumerate() {
        if event.source != "claude" {
            continue;
        }
        let (Some(message), Some(request)) =
            (event.message_id.as_deref(), event.request_id.as_deref())
        else {
            continue;
        };
        let key = (message, request);
        if let Some(previous) = best_exact.get(&key).copied() {
            if choose_usage(&events[previous], event) {
                keep[index] = false;
            } else {
                keep[previous] = false;
                best_exact.insert(key, index);
            }
        } else {
            best_exact.insert(key, index);
        }
    }
    let mut by_message: HashMap<&str, Vec<usize>> = HashMap::new();
    for (index, event) in events.iter().enumerate() {
        if keep[index]
            && event.source == "claude"
            && let Some(message) = event.message_id.as_deref()
        {
            by_message.entry(message).or_default().push(index);
        }
    }
    for indices in by_message.into_values() {
        if indices.len() < 2 || !indices.iter().any(|index| !events[*index].sidechain) {
            continue;
        }
        for index in indices {
            if events[index].sidechain {
                keep[index] = false;
            }
        }
    }
    let mut index = 0usize;
    events.retain(|_| {
        let retain = keep[index];
        index += 1;
        retain
    });
}

fn choose_usage(left: &UsageEvent, right: &UsageEvent) -> bool {
    if left.sidechain != right.sidechain {
        return !left.sidechain;
    }
    let left_parent = !left.source_path.contains("/subagents/");
    let right_parent = !right.source_path.contains("/subagents/");
    if left_parent != right_parent {
        return left_parent;
    }
    let left_total = left.tokens.additive_total();
    let right_total = right.tokens.additive_total();
    if left_total != right_total {
        return left_total > right_total;
    }
    left.source_order >= right.source_order
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn discovery_always_includes_agent_transcripts() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("main.jsonl"), "{}\n").unwrap();
        fs::write(temp.path().join("agent-child.jsonl"), "{}\n").unwrap();
        // The retired opt-in flag no longer gates anything.
        assert_eq!(discover(temp.path(), false).unwrap().len(), 2);
        assert_eq!(discover(temp.path(), true).unwrap().len(), 2);
    }

    #[test]
    fn discovery_excludes_nested_workflow_journals() {
        let temp = tempfile::tempdir().unwrap();
        let subagents = temp.path().join("project/session/subagents/workflows/run");
        fs::create_dir_all(&subagents).unwrap();
        fs::write(subagents.join("agent-child.jsonl"), "{}\n").unwrap();
        fs::write(subagents.join("journal.jsonl"), "{}\n").unwrap();

        let files = discover(temp.path(), false).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].path.file_name().and_then(|name| name.to_str()),
            Some("agent-child.jsonl")
        );
    }

    #[test]
    fn probe_shares_sidechain_session_and_project_identity() {
        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("-Users-nico-Code-memex");
        fs::create_dir_all(&project).unwrap();
        let path = project.join("fallback.jsonl");
        fs::write(
            &path,
            "{\"type\":\"assistant\",\"sessionId\":\"session-1\",\"cwd\":\"/Users/nico/Code/memex\",\"isSidechain\":true}\n",
        )
        .unwrap();
        let metadata = probe(&path).unwrap();
        assert_eq!(metadata.session.session_id, "session-1");
        assert_eq!(
            metadata.session.conversation_kind,
            ConversationKind::Sidechain
        );
        assert_eq!(metadata.project.as_deref(), Some("memex"));
    }

    #[test]
    fn session_title_prefers_custom_title_over_ai_title() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"type\":\"ai-title\",\"sessionId\":\"session-1\",\"aiTitle\":\"Generated title\"}\n",
                "{\"type\":\"custom-title\",\"sessionId\":\"session-1\",\"customTitle\":\"My title\"}\n"
            ),
        )
        .unwrap();

        assert_eq!(
            session_title(&path, "session-1").as_deref(),
            Some("My title")
        );
        assert_eq!(session_title(&path, "another-session"), None);
    }

    #[test]
    fn session_title_scans_past_large_messages_and_keeps_last_matching_rename() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let lines = [
            serde_json::json!({"type":"custom-title", "sessionId":"s", "customTitle":"Original"}),
            serde_json::json!({"type":"assistant", "message":{"content":"x".repeat(262144)}}),
            serde_json::json!({"type":"assistant", "customTitle":"False match", "message":{"content":"custom-title"}}),
            serde_json::json!({"type":"custom-title", "sessionId":"other", "customTitle":"Other session"}),
            serde_json::json!({"type":"agent-name", "sessionId":"s", "agentName":"Agent name"}),
            serde_json::json!({"type":"custom-title", "sessionId":"s", "customTitle":"Renamed"}),
            serde_json::json!({"type":"ai-title", "sessionId":"s", "aiTitle":"Later generated title"}),
        ];
        // The final event intentionally has no trailing newline.
        fs::write(
            &path,
            lines
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        assert_eq!(session_title(&path, "s").as_deref(), Some("Renamed"));
        assert_eq!(
            session_title(&path, "other").as_deref(),
            Some("Other session")
        );
    }

    #[test]
    fn session_title_keeps_agent_fallback_and_empty_file_behavior() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        fs::write(&path, "").unwrap();
        assert_eq!(session_title(&path, "s"), None);
        fs::write(&path, "malformed custom-title\n{\"type\":\"agent-name\",\"sessionId\":\"s\",\"name\":\"Task name\"}").unwrap();
        assert_eq!(session_title(&path, "s").as_deref(), Some("Task name"));
        fs::write(
            &path,
            r#"{"type":"ai\u002dtitle","sessionId":"s","aiTitle":"Escaped type"}"#,
        )
        .unwrap();
        assert_eq!(session_title(&path, "s").as_deref(), Some("Escaped type"));
    }

    #[test]
    fn usage_preserves_the_full_cwd_and_does_not_invent_a_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"type\":\"assistant\",\"cwd\":\"/Users/nico/Code/memex\",",
                "\"message\":{\"id\":\"one\",\"usage\":{\"input_tokens\":1}}}\n",
                "{\"type\":\"assistant\",",
                "\"message\":{\"id\":\"two\",\"usage\":{\"input_tokens\":1}}}\n"
            ),
        )
        .unwrap();

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events[0].project.as_deref(), Some("/Users/nico/Code/memex"));
        assert_eq!(events[1].project, None);
    }

    #[test]
    fn usage_converts_numeric_second_timestamps_to_milliseconds() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"type\":\"assistant\",\"timestamp\":1776386452,",
                "\"message\":{\"id\":\"seconds\",\"usage\":{\"input_tokens\":1}}}\n",
                "{\"type\":\"assistant\",\"timestamp\":1776386452437,",
                "\"message\":{\"id\":\"milliseconds\",\"usage\":{\"input_tokens\":1}}}\n"
            ),
        )
        .unwrap();

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events[0].timestamp_ms, 1_776_386_452_000);
        assert_eq!(events[1].timestamp_ms, 1_776_386_452_437);
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
            fs::write(&path, format!("{}\n", serde_json::json!({"type":"user", "sessionId":"session", "message":{"content":[{"type":"tool_result", "tool_use_id":"call", "content":output}]}}))).unwrap();
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
    fn attachment_records_preserve_legacy_ids_across_full_and_incremental_parsing() {
        use crate::retrieval::canonical_record_id;
        use serde_json::json;
        use std::io::Write;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let entry = |role, content| json!({"type":role, "sessionId":"session", "message":{"content":content}});
        let legacy = vec![
            entry("user", json!("Question")),
            entry(
                "assistant",
                json!([
                    {"type":"tool_use", "id":"call", "name":"Read", "input":{}},
                    {"type":"thinking", "thinking":"Reasoning"},
                    {"type":"text", "text":"Working"}
                ]),
            ),
            entry(
                "user",
                json!([{"type":"tool_result", "tool_use_id":"call", "content":"read result"}]),
            ),
            entry("assistant", json!("Answer A")),
            entry(
                "assistant",
                json!([{"type":"tool_use", "name":"Other", "input":"other input"}]),
            ),
            json!({"type":"assistant", "uuid":"native-answer", "message":{"content":"Answer B"}}),
        ];
        let image =
            json!({"type":"image", "source":{"type":"url", "url":"https://example.com/image.png"}});
        let document = json!({"type":"document", "source":{"type":"base64", "media_type":"application/pdf", "data":"AAAA"}});
        let mut mixed = legacy.clone();
        // A tool-only source line gains an attachment-only aggregate message;
        // the tool's existing identity and the next legacy ordinal both survive.
        mixed[4]["message"]["content"]
            .as_array_mut()
            .unwrap()
            .push(image.clone());
        mixed.insert(2, entry("user", json!([document])));
        mixed.insert(0, entry("user", json!([image])));
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
            assert_eq!(baseline.len(), 7 + usize::from(include_reasoning));
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
            assert_eq!(full.len(), baseline.len() + 3);
            assert_eq!(output.legacy_turn_id, Some(baseline.len() as u32));
            let full_ids = full.iter().map(canonical_record_id).collect::<Vec<_>>();
            assert_eq!(
                full_ids
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
                full.len()
            );

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
    fn tool_result_envelope_failure_survives_plain_text_projection() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        for flag in [
            serde_json::json!(true),
            serde_json::json!(false),
            serde_json::json!(1),
            serde_json::Value::Null,
        ] {
            fs::write(&path, format!("{}\n", serde_json::json!({"type":"user", "sessionId":"session", "message":{"content":[{"type":"tool_result", "tool_use_id":"call", "is_error":flag, "content":"permission denied"}]}}))).unwrap();
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
            let result = &records[0];
            assert_eq!(result.tool_output.as_deref(), Some("permission denied"));
            assert_eq!(result.links.tool_result_is_error, flag.as_bool());
            let json = serde_json::to_value(result).unwrap();
            assert_eq!(
                json.get("tool_result_is_error").and_then(|v| v.as_bool()),
                flag.as_bool()
            );
        }
    }

    #[test]
    fn source_content_preserves_images_documents_without_exposing_reasoning() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let image = serde_json::json!({"type":"image", "source":{"type":"base64", "media_type":"image/png", "data":"AAAA"}});
        let document = serde_json::json!({"type":"document", "source":{"type":"url", "url":"https://example.com/notes.pdf"}, "title":"Notes"});
        let contents = [
            serde_json::json!([image]),
            serde_json::json!([{"type":"text", "text":"<<ImageDisplayed>>"}, document, {"type":"thinking", "thinking":"private reasoning"}]),
            serde_json::json!([{"type":"text", "text":"plain text"}]),
        ];
        fs::write(&path, contents.iter().map(|content| format!("{}\n", serde_json::json!({"type":"user", "sessionId":"session", "message":{"role":"user", "content":content}}))).collect::<String>()).unwrap();
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
        assert!(
            !parsed
                .diagnostics
                .unknown_semantic_types
                .contains_key("document")
        );
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].text, "");
        assert_eq!(records[1].text, "<<ImageDisplayed>>");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                records[0].links.source_content.as_ref().unwrap()
            )
            .unwrap(),
            contents[0]
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                records[1].links.source_content.as_ref().unwrap()
            )
            .unwrap(),
            serde_json::json!([contents[1][0], document])
        );
        assert!(
            !serde_json::to_string(&records)
                .unwrap()
                .contains("private reasoning")
        );
        assert!(records[2].links.source_content.is_none());
    }

    #[test]
    fn parity_fixture_makes_plain_reasoning_opt_in_and_drops_redacted_payloads() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("claude.jsonl");
        fs::write(
            &path,
            include_str!("../../fixtures/trajectory_parity/claude.jsonl"),
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
        assert!(with_reasoning.iter().any(|record| {
            record.role == "reasoning" && record.text == "Plain Claude reasoning"
        }));
        assert!(!with_reasoning.iter().any(|record| {
            record.text.contains("ciphertext-must-never-be-indexed")
                || record.text.contains("signature-must-never-be-indexed")
        }));
    }
}
