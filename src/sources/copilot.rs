use super::{IndexParseOutput, IndexParseState, ParserVersions, SourceFile};
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::Result;
use memchr::memchr;
use memmap2::Mmap;
use simd_json::BorrowedValue;
use simd_json::prelude::*;
use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use walkdir::WalkDir;

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 2,
    index: 2,
    usage: 3,
};

pub fn matches_path(path: &str) -> bool {
    path.contains(".copilot/session-state")
        || path.contains(".copilot\\session-state")
        || path.contains("/session-state/")
        || path.contains("\\session-state\\")
}

pub fn root() -> PathBuf {
    std::env::var_os("COPILOT_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| super::common::home().join(".copilot"))
}

pub fn session_root() -> PathBuf {
    root().join("session-state")
}

pub fn discover_sessions() -> Vec<SourceFile> {
    discover_sessions_from_root(&session_root())
}

pub fn discover_sessions_from_root(root: &Path) -> Vec<SourceFile> {
    let mut files = WalkDir::new(root)
        .into_iter()
        .flatten()
        .filter(|entry| {
            entry.file_type().is_file()
                && entry.path().file_name().and_then(|name| name.to_str()) == Some("events.jsonl")
        })
        .map(|entry| SourceFile {
            source: SourceKind::Copilot,
            path: entry.path().to_path_buf(),
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.path.cmp(&right.path));
    files
}

pub fn session_id_from_path(path: &Path) -> Option<String> {
    path.parent()?
        .file_name()?
        .to_str()
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub fn usage_files() -> Vec<PathBuf> {
    let mut roots = vec![root().join("otel")];
    if let Some(path) = std::env::var_os("COPILOT_OTEL_FILE_EXPORTER_PATH") {
        roots.push(PathBuf::from(path));
    }
    roots
        .into_iter()
        .flat_map(|root| {
            if root.is_file() {
                vec![root]
            } else {
                super::common::jsonl_files([root])
            }
        })
        .collect()
}

#[derive(Debug, Default)]
struct CopilotWorkspace {
    cwd: Option<String>,
    git_root: Option<String>,
    repository: Option<String>,
    branch: Option<String>,
}

pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut start = state.offset as usize;
    let mut turn_id = state.turn_id;

    let source_path = path.to_string_lossy().to_string();
    let mut session_id = crate::sources::copilot::session_id_from_path(path)
        .unwrap_or_else(|| "unknown".to_string());
    let mut workspace = read_copilot_workspace(path);
    let mut project = copilot_project(&workspace);
    let mut pending_tool_calls = state.pending_tool_calls;

    while start < mmap.len() {
        let slice = &mmap[start..];
        let rel = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..rel];
        start += rel + usize::from(rel < slice.len());
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value = match serde_json::from_slice(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(obj) = value.as_object() else {
            continue;
        };
        let entry_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let data = obj
            .get("data")
            .or_else(|| obj.get("payload"))
            .unwrap_or(&serde_json::Value::Null);
        let timestamp = obj
            .get("timestamp")
            .and_then(json_timestamp_millis)
            .or_else(|| data.get("timestamp").and_then(json_timestamp_millis))
            .unwrap_or(0);

        match entry_type {
            "session.start" | "session.resume" => {
                if let Some(id) = data
                    .get("sessionId")
                    .or_else(|| data.get("session_id"))
                    .and_then(|v| v.as_str())
                {
                    session_id = id.to_string();
                }
                merge_copilot_workspace(&mut workspace, data.get("context").unwrap_or(data));
                project = copilot_project(&workspace);
            }
            "session.context_changed" => {
                merge_copilot_workspace(&mut workspace, data);
                project = copilot_project(&workspace);
            }
            "user.message" => {
                let text = data
                    .get("content")
                    .or_else(|| data.get("message"))
                    .or_else(|| data.get("prompt"))
                    .and_then(text_from_json)
                    .unwrap_or_default();
                if text.trim().is_empty() {
                    continue;
                }
                let links = copilot_record_links(obj, data, &session_id, turn_id);
                let record = Record {
                    source: SourceKind::Copilot,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: project.clone(),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "user".to_string(),
                    text,
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links,
                    source_path: source_path.clone(),
                };
                emit(record)?;
                turn_id += 1;
            }
            "assistant.message" => {
                let text = data
                    .get("content")
                    .and_then(text_from_json)
                    .unwrap_or_default();
                if text.trim().is_empty() {
                    continue;
                }
                let links = copilot_record_links(obj, data, &session_id, turn_id);
                let record = Record {
                    source: SourceKind::Copilot,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: project.clone(),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "assistant".to_string(),
                    text,
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links,
                    source_path: source_path.clone(),
                };
                emit(record)?;
                turn_id += 1;
            }
            "tool.execution_start" | "tool.user_requested" => {
                let tool_name = data
                    .get("toolName")
                    .or_else(|| data.get("name"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let tool_input = data
                    .get("arguments")
                    .map(json_to_text)
                    .filter(|s| !s.is_empty());
                let text = tool_input.clone().unwrap_or_default();
                let mut links = copilot_record_links(obj, data, &session_id, turn_id);
                let call_id = data
                    .get("toolCallId")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                if let Some(call_id) = &call_id {
                    links.event_id = Some(call_id.clone());
                }
                let doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
                if let Some(call_id) = call_id {
                    pending_tool_calls.insert(
                        call_id.clone(),
                        super::common::pending_tool_call(
                            tool_name.clone(),
                            Some(call_id),
                            doc_id,
                            timestamp,
                            tool_input.as_deref(),
                            &links,
                            &session_id,
                        ),
                    );
                }
                let record = Record {
                    source: SourceKind::Copilot,
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
                };
                emit(record)?;
                turn_id += 1;
            }
            "tool.execution_complete" => {
                let call_id = data
                    .get("toolCallId")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let tool_name = data
                    .get("toolName")
                    .or_else(|| data.get("name"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        (!call_id.is_empty())
                            .then(|| pending_tool_calls.get(call_id))
                            .flatten()
                            .and_then(|call| call.tool_name.clone())
                    });
                if !call_id.is_empty() {
                    pending_tool_calls.remove(call_id);
                }
                let tool_output = copilot_tool_output(data);
                let text = tool_output.clone().unwrap_or_default();
                if text.trim().is_empty() {
                    continue;
                }
                let mut links = copilot_record_links(obj, data, &session_id, turn_id);
                if !call_id.is_empty() {
                    links.parent_event_id = Some(call_id.to_string());
                    links.parent_tool_use_id = Some(call_id.to_string());
                }
                let record = Record {
                    source: SourceKind::Copilot,
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
            "session.task_complete" => {
                let text = data
                    .get("summary")
                    .and_then(text_from_json)
                    .unwrap_or_default();
                if text.trim().is_empty() {
                    continue;
                }
                let links = copilot_record_links(obj, data, &session_id, turn_id);
                let record = Record {
                    source: SourceKind::Copilot,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: project.clone(),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "assistant".to_string(),
                    text,
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links,
                    source_path: source_path.clone(),
                };
                emit(record)?;
                turn_id += 1;
            }
            _ => {}
        }
    }

    Ok(IndexParseOutput {
        offset: mmap.len() as u64,
        turn_id,
        pending_tool_calls,
        session_id: Some(session_id),
        diagnostics: Default::default(),
    })
}

fn read_copilot_workspace(events_path: &Path) -> CopilotWorkspace {
    let Some(dir) = events_path.parent() else {
        return CopilotWorkspace::default();
    };
    let path = dir.join("workspace.yaml");
    let Ok(contents) = std::fs::read_to_string(path) else {
        return CopilotWorkspace::default();
    };
    parse_copilot_workspace_yaml(&contents)
}

fn parse_copilot_workspace_yaml(contents: &str) -> CopilotWorkspace {
    let mut workspace = CopilotWorkspace::default();
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty()
            || trimmed.starts_with('#')
            || line.chars().next().is_some_and(|c| c.is_whitespace())
        {
            continue;
        }
        let Some((key, value)) = trimmed.split_once(':') else {
            continue;
        };
        let value = value
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();
        if value.is_empty() {
            continue;
        }
        match key.trim() {
            "cwd" => workspace.cwd = Some(value),
            "gitRoot" | "git_root" => workspace.git_root = Some(value),
            "repository" => workspace.repository = Some(value),
            "branch" => workspace.branch = Some(value),
            _ => {}
        }
    }
    workspace
}

fn merge_copilot_workspace(workspace: &mut CopilotWorkspace, value: &serde_json::Value) {
    if let Some(cwd) = value.get("cwd").and_then(|v| v.as_str()) {
        workspace.cwd = Some(cwd.to_string());
    }
    if let Some(git_root) = value
        .get("gitRoot")
        .or_else(|| value.get("git_root"))
        .and_then(|v| v.as_str())
    {
        workspace.git_root = Some(git_root.to_string());
    }
    if let Some(repository) = value.get("repository").and_then(|v| v.as_str()) {
        workspace.repository = Some(repository.to_string());
    }
    if let Some(branch) = value.get("branch").and_then(|v| v.as_str()) {
        workspace.branch = Some(branch.to_string());
    }
}

fn copilot_project(workspace: &CopilotWorkspace) -> String {
    if let Some(repo) = workspace.repository.as_deref() {
        if let Some((_, name)) = repo.rsplit_once('/')
            && !name.is_empty()
        {
            return name.to_string();
        }
        if !repo.is_empty() {
            return repo.to_string();
        }
    }
    if let Some(git_root) = workspace.git_root.as_deref() {
        return super::common::project_from_path(git_root);
    }
    if let Some(cwd) = workspace.cwd.as_deref() {
        return super::common::project_from_path(cwd);
    }
    "copilot".to_string()
}

fn copilot_record_links(
    obj: &serde_json::Map<String, serde_json::Value>,
    data: &serde_json::Value,
    session_id: &str,
    turn_id: u32,
) -> RecordLinks {
    let parent_session_id = copilot_string_field(data, obj, COPILOT_PARENT_SESSION_KEYS);
    let explicit_thread_source =
        copilot_string_field(data, obj, &["threadSource", "thread_source", "source"]);
    let thread_source = explicit_thread_source.or_else(|| {
        parent_session_id
            .as_ref()
            .filter(|parent| parent.as_str() != session_id)
            .map(|_| "fork".to_string())
    });
    let conversation_kind = match thread_source.as_deref() {
        Some("subagent") => "subagent",
        Some("sidechain") => "sidechain",
        Some("branch") => "branch",
        Some("fork") => "fork",
        _ => "main",
    };

    RecordLinks {
        event_id: copilot_string_field(data, obj, COPILOT_EVENT_KEYS)
            .or_else(|| Some(format!("{session_id}:{turn_id}"))),
        parent_event_id: copilot_string_field(data, obj, COPILOT_PARENT_EVENT_KEYS),
        logical_parent_event_id: copilot_string_field(data, obj, COPILOT_LOGICAL_PARENT_KEYS),
        parent_session_id,
        thread_source,
        conversation_kind: Some(conversation_kind.to_string()),
        ..RecordLinks::default()
    }
}

const COPILOT_EVENT_KEYS: &[&str] = &[
    "id",
    "eventId",
    "event_id",
    "messageId",
    "message_id",
    "requestId",
    "request_id",
    "responseId",
    "response_id",
];
const COPILOT_PARENT_EVENT_KEYS: &[&str] = &[
    "parentId",
    "parent_id",
    "parentMessageId",
    "parent_message_id",
    "parentEventId",
    "parent_event_id",
];
const COPILOT_LOGICAL_PARENT_KEYS: &[&str] = &["fromId", "from_id", "rootId", "root_id"];
const COPILOT_PARENT_SESSION_KEYS: &[&str] = &[
    "parentSessionId",
    "parent_session_id",
    "forkedFromSessionId",
    "forked_from_session_id",
];

fn copilot_string_field(
    data: &serde_json::Value,
    obj: &serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<String> {
    for key in keys {
        if let Some(value) = data
            .get(*key)
            .or_else(|| obj.get(*key))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            return Some(value.to_string());
        }
    }
    None
}

fn json_timestamp_millis(value: &serde_json::Value) -> Option<u64> {
    if let Some(text) = value.as_str() {
        if let Some(ts) = super::common::parse_iso_millis(text) {
            return Some(ts);
        }
        return text.parse::<u64>().ok();
    }
    if let Some(n) = value.as_u64() {
        return Some(if n < 10_000_000_000 { n * 1000 } else { n });
    }
    None
}

fn text_from_json(value: &serde_json::Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.trim().to_string());
    }
    if let Some(arr) = value.as_array() {
        let mut parts = Vec::new();
        for item in arr {
            if let Some(text) = item.as_str() {
                parts.push(text);
                continue;
            }
            if let Some(obj) = item.as_object()
                && let Some(text) = obj
                    .get("text")
                    .or_else(|| obj.get("content"))
                    .and_then(|v| v.as_str())
            {
                parts.push(text);
            }
        }
        if !parts.is_empty() {
            return Some(parts.join("\n").trim().to_string());
        }
    }
    None
}

fn json_to_text(value: &serde_json::Value) -> String {
    if let Some(text) = text_from_json(value) {
        return text;
    }
    serde_json::to_string(value).unwrap_or_default()
}

fn copilot_tool_output(data: &serde_json::Value) -> Option<String> {
    if let Some(result) = data.get("result") {
        for key in ["detailedContent", "content"] {
            if let Some(text) = result.get(key).and_then(text_from_json)
                && !text.trim().is_empty()
            {
                return Some(text);
            }
        }
        if let Some(contents) = result.get("contents").and_then(|v| v.as_array()) {
            let mut parts = Vec::new();
            for item in contents {
                if let Some(text) = text_from_json(item) {
                    parts.push(text);
                }
            }
            if !parts.is_empty() {
                return Some(parts.join("\n"));
            }
        }
    }
    if let Some(error) = data.get("error") {
        if let Some(message) = error.get("message").and_then(|v| v.as_str()) {
            return Some(message.to_string());
        }
        return Some(json_to_text(error));
    }
    None
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<Vec<UsageEvent>> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let mut start = 0usize;
    let mut index = 0u64;
    let mut buffer = Vec::new();
    let mut events = Vec::new();
    let mut seen = HashSet::new();
    while start < mmap.len() {
        let slice = &mmap[start..];
        let relative = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..relative];
        start += relative + usize::from(relative < slice.len());
        buffer.clear();
        buffer.extend_from_slice(line);
        if let Ok(value) = simd_json::to_borrowed_value(&mut buffer) {
            extract_usage(&value, &source_path, path, index, &mut events, &mut seen);
        }
        index += 1;
    }
    Ok(events)
}

fn extract_usage(
    value: &BorrowedValue<'_>,
    source_path: &Arc<str>,
    path: &Path,
    index: u64,
    events: &mut Vec<UsageEvent>,
    seen: &mut HashSet<String>,
) {
    if let Some(array) = value.as_array() {
        for child in array {
            extract_usage(child, source_path, path, index, events, seen);
        }
        return;
    }
    let Some(object) = value.as_object() else {
        return;
    };
    let attributes = object.get("attributes").unwrap_or(value);
    let operation = attribute_str(attributes, "gen_ai.operation.name")
        .or_else(|| object.get("name").and_then(|value| value.as_str()));
    let input = attribute_u64(attributes, "gen_ai.usage.input_tokens");
    let output = attribute_u64(attributes, "gen_ai.usage.output_tokens");
    if operation == Some("chat") && (input > 0 || output > 0) {
        let trace = object
            .get("traceId")
            .or_else(|| object.get("trace_id"))
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let span = object
            .get("spanId")
            .or_else(|| object.get("span_id"))
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let response = attribute_str(attributes, "gen_ai.response.id").unwrap_or("");
        let id = if !trace.is_empty() || !span.is_empty() {
            format!("{trace}:{span}")
        } else if !response.is_empty() {
            format!("response:{response}")
        } else {
            format!("{}:{index}:{input}:{output}", path.display())
        };
        if seen.insert(id.clone()) {
            let cache =
                attribute_u64(attributes, "gen_ai.usage.cache_read.input_tokens").min(input);
            events.push(UsageEvent {
                source: "copilot",
                source_path: source_path.clone(),
                source_record_id: Some(id),
                session_id: attribute_str(attributes, "gen_ai.conversation.id").map(str::to_string),
                request_id: attribute_str(attributes, "gen_ai.response.id").map(str::to_string),
                message_id: None,
                timestamp_ms: object
                    .get("startTimeUnixNano")
                    .and_then(value_u64)
                    .map(|value| value / 1_000_000)
                    .or_else(|| object.get("timestamp").map(timestamp_millis))
                    .unwrap_or(0),
                project: attribute_str(attributes, "copilot_chat.repo.remote_url")
                    .or_else(|| attribute_str(attributes, "github.copilot.git.repository"))
                    .map(str::to_string),
                provider: Some("github-copilot".into()),
                model: attribute_str(attributes, "gen_ai.response.model")
                    .or_else(|| attribute_str(attributes, "gen_ai.request.model"))
                    .map(str::to_string),
                tokens: TokenBuckets::codex(
                    input,
                    cache,
                    output,
                    attribute_u64(attributes, "gen_ai.usage.reasoning.output_tokens")
                        .max(attribute_u64(attributes, "gen_ai.usage.reasoning_tokens")),
                ),
                source_cost_usd: None,
                cost_authoritative: false,
                dedupe_confidence: "exact",
                conservative_undercount: false,
                cache_chain_excluded: false,
                sidechain: false,
                source_order: index,
            });
        }
    }
    for child in object.values() {
        if child.is_array() || child.is_object() {
            extract_usage(child, source_path, path, index, events, seen);
        }
    }
}

fn attribute<'a>(attributes: &'a BorrowedValue<'a>, key: &str) -> Option<&'a BorrowedValue<'a>> {
    if let Some(object) = attributes.as_object() {
        return object.get(key).map(unwrap_attribute);
    }
    attributes.as_array()?.iter().find_map(|item| {
        (item.get("key").and_then(|value| value.as_str()) == Some(key))
            .then(|| item.get("value").map(unwrap_attribute))
            .flatten()
    })
}

fn unwrap_attribute<'a>(value: &'a BorrowedValue<'a>) -> &'a BorrowedValue<'a> {
    ["stringValue", "intValue", "doubleValue", "boolValue"]
        .iter()
        .find_map(|key| value.get(*key))
        .unwrap_or(value)
}

fn value_u64(value: &BorrowedValue<'_>) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn attribute_u64(attributes: &BorrowedValue<'_>, key: &str) -> u64 {
    attribute(attributes, key).and_then(value_u64).unwrap_or(0)
}

fn attribute_str<'a>(attributes: &'a BorrowedValue<'a>, key: &str) -> Option<&'a str> {
    attribute(attributes, key)?.as_str()
}

fn timestamp_millis(value: &BorrowedValue<'_>) -> u64 {
    value_u64(value)
        .or_else(|| value.as_str().and_then(super::common::parse_iso_millis))
        .unwrap_or(0)
}

pub(crate) fn reconcile_usage(events: &mut Vec<UsageEvent>) {
    let mut seen = HashSet::new();
    events.retain(|event| {
        event.source != "copilot"
            || event
                .source_record_id
                .as_ref()
                .is_none_or(|record| seen.insert(record.clone()))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_decoder_reads_wrapped_otel_attributes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("usage.jsonl");
        std::fs::write(
            &path,
            r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"name":"chat","traceId":"trace","spanId":"span","startTimeUnixNano":"1750000000000000000","attributes":[{"key":"gen_ai.operation.name","value":{"stringValue":"chat"}},{"key":"gen_ai.usage.input_tokens","value":{"intValue":"100"}},{"key":"gen_ai.usage.cache_read.input_tokens","value":{"intValue":"40"}},{"key":"gen_ai.usage.output_tokens","value":{"intValue":"20"}},{"key":"gen_ai.conversation.id","value":{"stringValue":"session"}},{"key":"gen_ai.response.id","value":{"stringValue":"response"}},{"key":"gen_ai.response.model","value":{"stringValue":"gpt-5"}}]}]}]}]}"#,
        )
        .unwrap();

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source_record_id.as_deref(), Some("trace:span"));
        assert_eq!(events[0].session_id.as_deref(), Some("session"));
        assert_eq!(events[0].request_id.as_deref(), Some("response"));
        assert_eq!(events[0].model.as_deref(), Some("gpt-5"));
        assert_eq!(events[0].tokens.uncached_input, 60);
        assert_eq!(events[0].tokens.cache_read, 40);
        assert_eq!(events[0].tokens.output, 20);
        assert_eq!(events[0].timestamp_ms, 1_750_000_000_000);
    }
}
