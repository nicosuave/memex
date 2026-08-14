use super::{IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions, SourceFile};
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::Result;
use memchr::memchr;
use memmap2::Mmap;
use simd_json::BorrowedValue;
use simd_json::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 2,
    index: 3,
    usage: 4,
};

pub fn matches_path(path: &str) -> bool {
    path.contains(".pi/agent/sessions")
        || path.contains(".pi\\agent\\sessions")
        || path.contains("pi/agent/sessions")
        || path.contains("pi\\agent\\sessions")
}

pub fn agent_root() -> PathBuf {
    std::env::var_os("PI_CODING_AGENT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| super::common::home().join(".pi/agent"))
}

pub fn sessions_root() -> PathBuf {
    if let Some(root) = std::env::var_os("PI_CODING_AGENT_SESSION_DIR") {
        return PathBuf::from(root);
    }
    let agent = agent_root();
    configured_session_root(&agent).unwrap_or_else(|| agent.join("sessions"))
}

fn configured_session_root(agent: &Path) -> Option<PathBuf> {
    let value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(agent.join("settings.json")).ok()?).ok()?;
    let raw = value.get("sessionDir")?.as_str()?.trim();
    if raw.is_empty() {
        return None;
    }
    if raw == "~" {
        return Some(super::common::home());
    }
    if let Some(relative) = raw.strip_prefix("~/") {
        return Some(super::common::home().join(relative));
    }
    let path = PathBuf::from(raw);
    Some(if path.is_absolute() {
        path
    } else {
        agent.join(path)
    })
}

pub fn discover() -> Vec<SourceFile> {
    super::common::jsonl_files([sessions_root()])
        .into_iter()
        .map(|path| SourceFile {
            source: SourceKind::Pi,
            path,
        })
        .collect()
}

pub fn session_id_from_path(path: &Path) -> String {
    crate::sources::codex::session_id_from_path(path)
        .unwrap_or_else(|| path.to_string_lossy().into_owned())
}

pub fn project_from_path(path: &Path) -> String {
    path.parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .map(|name| {
            let key = if name.starts_with("--") && name.ends_with("--") && name.len() > 4 {
                &name[1..name.len() - 1]
            } else {
                name
            };
            key.trim_matches('-')
                .rsplit('-')
                .find(|part| !part.is_empty())
                .unwrap_or(key)
                .to_string()
        })
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| SourceKind::Pi.label().to_string())
}

pub(crate) fn apply_session_identity(
    id: Option<&str>,
    cwd: Option<&str>,
    session_id: &mut String,
    project: &mut String,
) {
    if let Some(id) = id.filter(|id| !id.is_empty()) {
        *session_id = id.to_string();
    }
    if let Some(cwd) = cwd.filter(|cwd| !cwd.is_empty()) {
        *project = super::common::project_from_path(cwd);
    }
}

fn apply_session_header(
    object: &simd_json::borrowed::Object<'_>,
    session_id: &mut String,
    project: &mut String,
) {
    apply_session_identity(
        object.get("id").and_then(|value| value.as_str()),
        object.get("cwd").and_then(|value| value.as_str()),
        session_id,
        project,
    );
}

fn optional_string(object: &simd_json::borrowed::Object<'_>, key: &str) -> Option<String> {
    object
        .get(key)
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

fn base_links(object: &simd_json::borrowed::Object<'_>, conversation_kind: &str) -> RecordLinks {
    RecordLinks {
        event_id: optional_string(object, "id"),
        parent_event_id: optional_string(object, "parentId"),
        logical_parent_event_id: optional_string(object, "fromId"),
        thread_source: (conversation_kind != "main").then(|| conversation_kind.to_string()),
        conversation_kind: Some(conversation_kind.to_string()),
        ..RecordLinks::default()
    }
}

fn content_text(content: Option<&BorrowedValue<'_>>) -> String {
    let Some(content) = content else {
        return String::new();
    };
    if let Some(text) = content.as_str() {
        return text.to_string();
    }
    if let Some(array) = content.as_array() {
        let mut parts = Vec::new();
        for item in array {
            if let Some(text) = item.as_str() {
                parts.push(text.to_string());
                continue;
            }
            let Some(object) = item.as_object() else {
                continue;
            };
            if object.contains_key("encrypted_content") {
                continue;
            }
            match object
                .get("type")
                .and_then(|value| value.as_str())
                .unwrap_or("")
            {
                "text" => {
                    if let Some(text) = object.get("text").and_then(|value| value.as_str()) {
                        parts.push(text.to_string());
                    }
                }
                "thinking" | "redacted_thinking" | "encrypted_reasoning" => {
                    // Plaintext reasoning is projected separately as role=reasoning so it
                    // remains BM25-only and is never embedded as assistant prose.
                }
                "toolCall" => {}
                _ => {
                    if let Some(text) = object.get("text").and_then(|value| value.as_str()) {
                        parts.push(text.to_string());
                    } else if let Some(text) =
                        object.get("content").and_then(|value| value.as_str())
                    {
                        parts.push(text.to_string());
                    }
                }
            }
        }
        return parts.join("\n");
    }
    content.to_string()
}

fn summary_message_text(message: &simd_json::borrowed::Object<'_>, role: &str) -> String {
    let summary = if role == "branchSummary" || role == "compactionSummary" {
        message.get("summary").and_then(|value| value.as_str())
    } else {
        None
    };
    summary
        .map(str::to_string)
        .unwrap_or_else(|| content_text(message.get("content")))
        .trim()
        .to_string()
}

fn bash_text(command: &str, output: &str, exit_code: Option<i64>) -> String {
    let mut parts = Vec::new();
    if !command.is_empty() {
        parts.push(format!("$ {command}"));
    }
    if !output.is_empty() {
        parts.push(output.to_string());
    }
    if let Some(code) = exit_code {
        parts.push(format!("exit code: {code}"));
    }
    parts.join("\n")
}

pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    parse_index_records_for(
        path,
        state,
        SourceKind::Pi,
        include_reasoning,
        next_doc_id,
        emit,
    )
}

pub(crate) fn parse_index_records_for(
    path: &Path,
    state: IndexParseState,
    source: SourceKind,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut start = state.offset as usize;
    let mut turn_id = state.turn_id;

    let source_path = path.to_string_lossy().to_string();
    let mut session_id = session_id_from_path(path);
    let mut project = project_from_path(path);
    let mut pending_tool_calls = state.pending_tool_calls;
    let mut diagnostics = ParseDiagnostics::default();

    let mut buf = Vec::new();
    if start > 0 && !mmap.is_empty() {
        let rel = memchr(b'\n', &mmap).unwrap_or(mmap.len());
        let line = &mmap[..rel];
        if !line.is_empty() {
            buf.extend_from_slice(line);
            if let Ok(value) = simd_json::to_borrowed_value(&mut buf)
                && let Some(obj) = value.as_object()
                && obj.get("type").and_then(|v| v.as_str()) == Some("session")
            {
                apply_session_header(obj, &mut session_id, &mut project);
            }
            buf.clear();
        }
    }
    while start < mmap.len() {
        let slice = &mmap[start..];
        let rel = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..rel];
        start += rel + usize::from(rel < slice.len());
        if line.is_empty() {
            continue;
        }
        buf.clear();
        buf.extend_from_slice(line);
        let value: BorrowedValue = match simd_json::to_borrowed_value(&mut buf) {
            Ok(v) => v,
            Err(_) => {
                diagnostics.malformed_json_lines += 1;
                continue;
            }
        };
        let obj = match value.as_object() {
            Some(o) => o,
            None => {
                diagnostics.non_object_json_lines += 1;
                continue;
            }
        };
        let entry_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let timestamp = obj.get("timestamp").map(timestamp_millis).unwrap_or(0);
        let conversation_kind = match entry_type {
            "branch_summary" => "branch",
            "compaction" => "compaction",
            _ => "main",
        };
        let mut base_links = base_links(obj, conversation_kind);

        if entry_type == "session" {
            apply_session_header(obj, &mut session_id, &mut project);
            continue;
        }

        if entry_type == "compaction" || entry_type == "branch_summary" {
            let summary = obj
                .get("summary")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim();
            if summary.is_empty() {
                continue;
            }
            let record = Record {
                source,
                doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                ts: timestamp,
                project: project.clone(),
                session_id: session_id.clone(),
                turn_id,
                role: "assistant".to_string(),
                text: format!("{entry_type}: {summary}"),
                tool_name: None,
                tool_input: None,
                tool_output: None,
                links: base_links,
                source_path: source_path.clone(),
            };
            emit(record)?;
            turn_id += 1;
            continue;
        }

        if entry_type == "custom_message" {
            let text = content_text(obj.get("content")).trim().to_string();
            if text.is_empty() {
                continue;
            }
            let custom_type = obj.get("customType").and_then(|v| v.as_str()).unwrap_or("");
            let prefix = if custom_type.is_empty() {
                "custom_message".to_string()
            } else {
                format!("custom_message({custom_type})")
            };
            let record = Record {
                source,
                doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                ts: timestamp,
                project: project.clone(),
                session_id: session_id.clone(),
                turn_id,
                role: "assistant".to_string(),
                text: format!("{prefix}: {text}"),
                tool_name: None,
                tool_input: None,
                tool_output: None,
                links: base_links,
                source_path: source_path.clone(),
            };
            emit(record)?;
            turn_id += 1;
            continue;
        }

        if entry_type != "message" {
            if !matches!(
                entry_type,
                "model_change" | "thinking_level_change" | "session_info" | "label" | "custom"
            ) {
                diagnostics.increment_unknown_top_level(entry_type);
            }
            continue;
        }
        let message = match obj.get("message").and_then(|v| v.as_object()) {
            Some(m) => m,
            None => continue,
        };
        let timestamp = if timestamp == 0 {
            message.get("timestamp").map(timestamp_millis).unwrap_or(0)
        } else {
            timestamp
        };
        let role = message.get("role").and_then(|v| v.as_str()).unwrap_or("");
        if conversation_kind == "main" {
            match role {
                "branchSummary" => {
                    base_links.thread_source = Some("branch".to_string());
                    base_links.conversation_kind = Some("branch".to_string());
                }
                "compactionSummary" => {
                    base_links.thread_source = Some("compaction".to_string());
                    base_links.conversation_kind = Some("compaction".to_string());
                }
                _ => {}
            }
        }

        match role {
            "user" | "assistant" => {
                let content = message.get("content");
                if role == "assistant"
                    && let Some(arr) = content.and_then(|v| v.as_array())
                {
                    for block in arr {
                        let Some(block_obj) = block.as_object() else {
                            continue;
                        };
                        let block_type = block_obj
                            .get("type")
                            .and_then(|value| value.as_str())
                            .unwrap_or("");
                        if block_type == "thinking" {
                            let thinking = block_obj
                                .get("thinking")
                                .and_then(|value| value.as_str())
                                .map(str::trim)
                                .filter(|text| !text.is_empty());
                            if let Some(thinking) = thinking {
                                if include_reasoning {
                                    let mut links = base_links.clone();
                                    links.event_id = base_links
                                        .event_id
                                        .as_ref()
                                        .map(|id| format!("{id}:reasoning"));
                                    emit(Record {
                                        source,
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
                            } else if block_obj.contains_key("encrypted_content")
                                || block_obj.contains_key("data")
                                || block_obj.contains_key("signature")
                            {
                                diagnostics.encrypted_reasoning_dropped += 1;
                            }
                            continue;
                        }
                        if matches!(block_type, "redacted_thinking" | "encrypted_reasoning") {
                            diagnostics.encrypted_reasoning_dropped += 1;
                            continue;
                        }
                        if block_type != "toolCall" {
                            if !matches!(block_type, "text" | "image") {
                                diagnostics.increment_unknown_semantic(block_type);
                            }
                            continue;
                        }
                        let tool_name = block_obj
                            .get("name")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let tool_input = block_obj.get("arguments").map(|v| v.to_string());
                        let mut links = base_links.clone();
                        let tool_call_id = block_obj
                            .get("id")
                            .and_then(|v| v.as_str())
                            .map(str::to_string);
                        if let Some(tool_call_id) = &tool_call_id {
                            links.event_id = Some(tool_call_id.clone());
                            links.parent_event_id = base_links.event_id.clone();
                        }
                        let doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
                        if let Some(tool_call_id) = tool_call_id {
                            let replaced = pending_tool_calls.insert(
                                tool_call_id.clone(),
                                super::common::pending_tool_call(
                                    tool_name.clone(),
                                    Some(tool_call_id),
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
                        let record = Record {
                            source,
                            doc_id,
                            ts: timestamp,
                            project: project.clone(),
                            session_id: session_id.clone(),
                            turn_id,
                            role: "tool_use".to_string(),
                            text: tool_input.clone().unwrap_or_default(),
                            tool_name,
                            tool_input,
                            tool_output: None,
                            links,
                            source_path: source_path.clone(),
                        };
                        emit(record)?;
                        turn_id += 1;
                    }
                }

                let text = content_text(content).trim().to_string();
                if text.is_empty() {
                    continue;
                }
                let record = Record {
                    source,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: project.clone(),
                    session_id: session_id.clone(),
                    turn_id,
                    role: role.to_string(),
                    text,
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: base_links,
                    source_path: source_path.clone(),
                };
                emit(record)?;
                turn_id += 1;
            }
            "toolResult" | "tool" => {
                let tool_call_id = message
                    .get("toolCallId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let tool_name = message
                    .get("toolName")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        (!tool_call_id.is_empty())
                            .then(|| pending_tool_calls.get(tool_call_id))
                            .flatten()
                            .and_then(|call| call.tool_name.clone())
                    });
                if !tool_call_id.is_empty() && pending_tool_calls.remove(tool_call_id).is_none() {
                    diagnostics.orphan_tool_results += 1;
                }
                let mut output = content_text(message.get("content"));
                if message
                    .get("isError")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false)
                    && !output.to_ascii_lowercase().starts_with("error")
                {
                    output = format!("Error: {output}");
                }
                let tool_output = Some(output);
                let text = tool_output.clone().unwrap_or_default();
                if text.trim().is_empty() {
                    continue;
                }
                let mut links = base_links;
                if !tool_call_id.is_empty() {
                    links.parent_tool_use_id = Some(tool_call_id.to_string());
                }
                let record = Record {
                    source,
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
                };
                emit(record)?;
                turn_id += 1;
            }
            "bashExecution" => {
                if message
                    .get("excludeFromContext")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    continue;
                }
                let command = message
                    .get("command")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let output = message
                    .get("output")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let exit_code = message.get("exitCode").and_then(|v| v.as_i64());
                let text = bash_text(&command, &output, exit_code);
                if text.trim().is_empty() {
                    continue;
                }
                let record = Record {
                    source,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: project.clone(),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "tool_result".to_string(),
                    text,
                    tool_name: Some("Bash".to_string()),
                    tool_input: if command.is_empty() {
                        None
                    } else {
                        Some(command)
                    },
                    tool_output: if output.is_empty() {
                        None
                    } else {
                        Some(output)
                    },
                    links: base_links,
                    source_path: source_path.clone(),
                };
                emit(record)?;
                turn_id += 1;
            }
            "custom" | "branchSummary" | "compactionSummary" => {
                let text = summary_message_text(message, role);
                if text.is_empty() {
                    continue;
                }
                let record = Record {
                    source,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: project.clone(),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "assistant".to_string(),
                    text: format!("{role}: {text}"),
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: base_links,
                    source_path: source_path.clone(),
                };
                emit(record)?;
                turn_id += 1;
            }
            _ => diagnostics.increment_unknown_semantic(role),
        }
    }

    Ok(IndexParseOutput {
        offset: mmap.len() as u64,
        turn_id,
        pending_tool_calls,
        session_id: Some(session_id),
        diagnostics,
    })
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<Vec<UsageEvent>> {
    parse_usage_file_for(path, "pi", &[])
}

pub(crate) fn parse_usage_file_for(
    path: &Path,
    source: &'static str,
    excluded_models: &[&str],
) -> Result<Vec<UsageEvent>> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let mut session = session_id_from_path(path);
    let mut project = project_from_path(path);
    let mut current_model = None;
    let mut current_provider = None;
    let mut start = 0usize;
    let mut index = 0u64;
    let mut buffer = Vec::new();
    let mut events = Vec::new();
    while start < mmap.len() {
        let slice = &mmap[start..];
        let relative = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..relative];
        start += relative + usize::from(relative < slice.len());
        buffer.clear();
        buffer.extend_from_slice(line);
        let Ok(value) = simd_json::to_borrowed_value(&mut buffer) else {
            index += 1;
            continue;
        };
        let kind = value
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if kind == "session" {
            apply_session_identity(
                value.get("id").and_then(|value| value.as_str()),
                value.get("cwd").and_then(|value| value.as_str()),
                &mut session,
                &mut project,
            );
            index += 1;
            continue;
        }
        if kind == "model_change" {
            current_model = borrowed_string(&value, &["modelId", "model", "model_id"]);
            current_provider = borrowed_string(&value, &["provider", "providerId", "provider_id"]);
            index += 1;
            continue;
        }
        let Some(message) = (kind == "message").then(|| value.get("message")).flatten() else {
            index += 1;
            continue;
        };
        if message.get("role").and_then(|value| value.as_str()) != Some("assistant") {
            index += 1;
            continue;
        }
        let Some(usage) = message.get("usage") else {
            index += 1;
            continue;
        };
        let number = |aliases: &[&str]| {
            aliases
                .iter()
                .find_map(|key| usage.get(*key).and_then(|value| value.as_u64()))
                .unwrap_or(0)
        };
        let tokens = TokenBuckets::disjoint(
            number(&[
                "input",
                "inputTokens",
                "input_tokens",
                "promptTokens",
                "prompt_tokens",
            ]),
            number(&[
                "cacheRead",
                "cacheReadTokens",
                "cache_read",
                "cache_read_tokens",
                "cacheReadInputTokens",
                "cache_read_input_tokens",
            ]),
            number(&[
                "cacheWrite",
                "cacheWriteTokens",
                "cache_write",
                "cache_write_tokens",
                "cacheCreationTokens",
                "cache_creation_tokens",
                "cacheCreationInputTokens",
                "cache_creation_input_tokens",
            ]),
            number(&[
                "output",
                "outputTokens",
                "output_tokens",
                "completionTokens",
                "completion_tokens",
            ]),
        );
        if tokens.additive_total() > 0 {
            events.push(UsageEvent {
                source,
                source_path: source_path.clone(),
                source_record_id: Some(format!("line:{index}")),
                session_id: Some(session.clone()),
                request_id: None,
                message_id: borrowed_string(&value, &["id"]),
                timestamp_ms: value.get("timestamp").map(timestamp_millis).unwrap_or(0),
                project: Some(project.clone()),
                provider: borrowed_string(message, &["provider"])
                    .or_else(|| borrowed_string(&value, &["provider"]))
                    .or_else(|| current_provider.clone()),
                model: borrowed_string(message, &["model", "modelId"])
                    .or_else(|| borrowed_string(&value, &["model", "modelId"]))
                    .or_else(|| current_model.clone())
                    .filter(|model| !excluded_models.contains(&model.as_str())),
                tokens,
                source_cost_usd: usage
                    .get("cost")
                    .and_then(|value| value.get("total"))
                    .and_then(|value| value.as_f64()),
                cost_authoritative: false,
                dedupe_confidence: "exact",
                conservative_undercount: false,
                cache_chain_excluded: false,
                sidechain: false,
                source_order: index,
            });
        }
        index += 1;
    }
    Ok(events)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn parity_fixture_supports_aliases_errors_numeric_timestamps_and_opt_in_reasoning() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pi.jsonl");
        fs::write(
            &path,
            include_str!("../../fixtures/trajectory_parity/pi.jsonl"),
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
        let result = without_reasoning
            .iter()
            .find(|record| record.role == "tool_result")
            .unwrap();
        assert_eq!(result.ts, 1_782_864_002_000);
        assert_eq!(result.text, "Error: permission denied");
        assert_eq!(parsed.diagnostics.malformed_json_lines, 1);
        assert_eq!(parsed.diagnostics.encrypted_reasoning_dropped, 1);
        assert_eq!(
            parsed.diagnostics.unknown_semantic_types.get("futureRole"),
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
        assert!(
            with_reasoning.iter().any(|record| {
                record.role == "reasoning" && record.text == "Plain Pi reasoning"
            })
        );
        assert!(
            !with_reasoning
                .iter()
                .any(|record| record.text.contains("ciphertext-must-never-be-indexed"))
        );
    }
}
