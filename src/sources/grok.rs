use super::{IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions, SourceFile};
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::Result;
use memchr::memchr;
use memmap2::Mmap;
use serde_json::Value;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use walkdir::WalkDir;

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 1,
    index: 1,
    usage: 1,
};

pub fn matches_path(path: &str) -> bool {
    (path.contains("/.grok/sessions/") || path.contains("\\.grok\\sessions\\"))
        && (path.ends_with("/updates.jsonl") || path.ends_with("\\updates.jsonl"))
}

pub fn root() -> PathBuf {
    std::env::var_os("GROK_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| super::common::home().join(".grok"))
}

pub fn session_root() -> PathBuf {
    root().join("sessions")
}

pub fn discover_sessions() -> Vec<SourceFile> {
    discover_sessions_from_root(&session_root())
}

pub fn discover_sessions_from_root(root: &Path) -> Vec<SourceFile> {
    let mut files = WalkDir::new(root)
        .min_depth(2)
        .max_depth(3)
        .into_iter()
        .flatten()
        .filter(|entry| {
            entry.file_type().is_file()
                && entry.path().file_name().and_then(|name| name.to_str()) == Some("updates.jsonl")
        })
        .map(|entry| SourceFile {
            source: SourceKind::Grok,
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

/// Working directory recorded in the session's `summary.json` sidecar.
pub fn session_cwd(updates_path: &Path) -> Option<String> {
    read_summary(updates_path).cwd
}

#[derive(Debug, Default)]
struct GrokSummary {
    session_id: Option<String>,
    cwd: Option<String>,
    project: Option<String>,
}

fn read_summary(updates_path: &Path) -> GrokSummary {
    let Some(parent) = updates_path.parent() else {
        return GrokSummary::default();
    };
    let Ok(contents) = std::fs::read(parent.join("summary.json")) else {
        return GrokSummary::default();
    };
    let Ok(value) = serde_json::from_slice::<Value>(&contents) else {
        return GrokSummary::default();
    };
    let cwd = value
        .pointer("/info/cwd")
        .and_then(Value::as_str)
        .map(str::to_string);
    let project = value
        .get("git_root_dir")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .or(cwd.as_deref())
        .map(super::common::project_from_path);
    GrokSummary {
        session_id: value
            .pointer("/info/id")
            .and_then(Value::as_str)
            .map(str::to_string),
        cwd,
        project,
    }
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
    let mut diagnostics = ParseDiagnostics::default();

    let source_path = path.to_string_lossy().to_string();
    let summary = read_summary(path);
    let mut session_id = summary
        .session_id
        .or_else(|| session_id_from_path(path))
        .unwrap_or_else(|| "unknown".to_string());
    let project = summary.project.unwrap_or_else(|| {
        summary
            .cwd
            .as_deref()
            .map(super::common::project_from_path)
            .unwrap_or_else(|| SourceKind::Grok.label().to_string())
    });

    while start < mmap.len() {
        let slice = &mmap[start..];
        let rel = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..rel];
        start += rel + usize::from(rel < slice.len());
        if line.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_slice(line) {
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
        if let Some(id) = value
            .pointer("/params/sessionId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        {
            session_id = id.to_string();
        }
        let Some(update) = value.pointer("/params/update") else {
            diagnostics.increment_unknown_top_level(
                object
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap_or("missing_update"),
            );
            continue;
        };
        let kind = update
            .get("sessionUpdate")
            .and_then(Value::as_str)
            .unwrap_or("");
        let timestamp = grok_timestamp_millis(&value);
        let event_id = value
            .pointer("/params/_meta/eventId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| Some(format!("{session_id}:{turn_id}")));
        let base_links = RecordLinks {
            event_id,
            conversation_kind: Some("main".to_string()),
            ..RecordLinks::default()
        };

        match kind {
            "user_message_chunk" | "agent_message_chunk" => {
                let text = content_text(update.get("content"));
                if text.trim().is_empty() {
                    continue;
                }
                emit(Record {
                    source: SourceKind::Grok,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: project.clone(),
                    session_id: session_id.clone(),
                    turn_id,
                    role: if kind == "user_message_chunk" {
                        "user".to_string()
                    } else {
                        "assistant".to_string()
                    },
                    text,
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: base_links,
                    source_path: source_path.clone(),
                })?;
                turn_id += 1;
            }
            "agent_thought_chunk" => {
                if !include_reasoning {
                    continue;
                }
                let text = content_text(update.get("content"));
                if text.trim().is_empty() {
                    continue;
                }
                emit(Record {
                    source: SourceKind::Grok,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: project.clone(),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "reasoning".to_string(),
                    text,
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: base_links,
                    source_path: source_path.clone(),
                })?;
                turn_id += 1;
            }
            "tool_call" => {
                let call_id = update
                    .get("toolCallId")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let tool_name = grok_tool_name(update);
                let tool_input = update
                    .get("rawInput")
                    .map(json_text)
                    .filter(|s| !s.is_empty());
                let doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
                let mut links = base_links;
                if let Some(call_id) = &call_id {
                    links.event_id = Some(call_id.clone());
                    pending_tool_calls.insert(
                        call_id.clone(),
                        super::common::pending_tool_call(
                            tool_name.clone(),
                            Some(call_id.clone()),
                            doc_id,
                            timestamp,
                            tool_input.as_deref(),
                            &links,
                            &session_id,
                        ),
                    );
                }
                emit(Record {
                    source: SourceKind::Grok,
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
                })?;
                turn_id += 1;
            }
            "tool_call_update" => {
                let status = update.get("status").and_then(Value::as_str).unwrap_or("");
                if !matches!(status, "completed" | "failed") {
                    continue;
                }
                let call_id = update
                    .get("toolCallId")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let pending = (!call_id.is_empty())
                    .then(|| pending_tool_calls.remove(call_id))
                    .flatten();
                let tool_name = grok_tool_name(update).or_else(|| pending?.tool_name);
                let tool_output = grok_tool_output(update);
                let text = tool_output.clone().unwrap_or_default();
                if text.trim().is_empty() {
                    continue;
                }
                let mut links = base_links;
                if !call_id.is_empty() {
                    links.parent_event_id = Some(call_id.to_string());
                    links.parent_tool_use_id = Some(call_id.to_string());
                }
                emit(Record {
                    source: SourceKind::Grok,
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
            "session_recap" => {
                let text = update
                    .get("summary")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if text.trim().is_empty() {
                    continue;
                }
                let mut links = base_links;
                links.thread_source = Some("compaction".to_string());
                links.conversation_kind = Some("compaction".to_string());
                emit(Record {
                    source: SourceKind::Grok,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: project.clone(),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "assistant".to_string(),
                    text: format!("session_recap: {text}"),
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links,
                    source_path: source_path.clone(),
                })?;
                turn_id += 1;
            }
            "hook_execution" | "plan" | "retry_state" | "task_backgrounded" | "task_completed"
            | "turn_completed" => {}
            "" => diagnostics.increment_unknown_semantic("missing_session_update"),
            unknown => diagnostics.increment_unknown_semantic(unknown),
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

fn grok_timestamp_millis(value: &Value) -> u64 {
    if let Some(timestamp) = value
        .pointer("/params/_meta/agentTimestampMs")
        .and_then(Value::as_u64)
    {
        return timestamp;
    }
    let timestamp = value.get("timestamp").and_then(Value::as_u64).unwrap_or(0);
    if timestamp < 10_000_000_000 {
        timestamp.saturating_mul(1000)
    } else {
        timestamp
    }
}

fn content_text(value: Option<&Value>) -> String {
    let Some(value) = value else {
        return String::new();
    };
    if let Some(text) = value.as_str() {
        return text.to_string();
    }
    if let Some(text) = value.get("text").and_then(Value::as_str) {
        return text.to_string();
    }
    let Some(items) = value.as_array() else {
        return String::new();
    };
    items
        .iter()
        .filter_map(|item| {
            item.get("content")
                .and_then(|content| content.get("text"))
                .or_else(|| item.get("text"))
                .and_then(Value::as_str)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn grok_tool_name(update: &Value) -> Option<String> {
    update
        .pointer("/_meta/x.ai~1tool/name")
        .or_else(|| update.get("tool_name"))
        .or_else(|| update.get("title"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn grok_tool_output(update: &Value) -> Option<String> {
    let content = content_text(update.get("content"));
    if !content.trim().is_empty() {
        return Some(content);
    }
    let output = update.get("rawOutput")?;
    for key in [
        "output_for_prompt",
        "tool_output_for_prompt",
        "content_concise",
        "summary_for_prompt",
        "raw_output",
    ] {
        if let Some(value) = find_string_field(output, key) {
            return Some(value.to_string());
        }
    }
    Some(json_text(output))
}

fn find_string_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    match value {
        Value::Object(object) => {
            if let Some(value) = object.get(key).and_then(Value::as_str) {
                return Some(value);
            }
            object
                .values()
                .find_map(|value| find_string_field(value, key))
        }
        Value::Array(items) => items.iter().find_map(|value| find_string_field(value, key)),
        _ => None,
    }
}

fn json_text(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<Vec<UsageEvent>> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let summary = read_summary(path);
    let project = summary
        .project
        .or_else(|| summary.cwd.as_deref().map(super::common::project_from_path));
    let fallback_session_id = summary.session_id.or_else(|| session_id_from_path(path));
    let mut start = 0usize;
    let mut order = 0u64;
    let mut events = Vec::new();

    while start < mmap.len() {
        let slice = &mmap[start..];
        let relative = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..relative];
        start += relative + usize::from(relative < slice.len());
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        let Some(update) = value.pointer("/params/update") else {
            continue;
        };
        if update.get("sessionUpdate").and_then(Value::as_str) != Some("turn_completed") {
            continue;
        }
        let Some(usage) = update.get("usage") else {
            continue;
        };
        let session_id = value
            .pointer("/params/sessionId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| fallback_session_id.clone());
        let request_id = update
            .get("prompt_id")
            .and_then(Value::as_str)
            .map(str::to_string);
        let timestamp_ms = grok_timestamp_millis(&value);
        let source_record_id = value
            .pointer("/params/_meta/eventId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| request_id.clone());

        if let Some(models) = usage.get("modelUsage").and_then(Value::as_object) {
            for (model, buckets) in models {
                events.push(grok_usage_event(
                    source_path.clone(),
                    source_record_id.as_ref().map(|id| format!("{id}:{model}")),
                    session_id.clone(),
                    request_id.clone(),
                    timestamp_ms,
                    project.clone(),
                    Some(model.clone()),
                    buckets,
                    order,
                ));
                order += 1;
            }
        } else {
            events.push(grok_usage_event(
                source_path.clone(),
                source_record_id,
                session_id,
                request_id,
                timestamp_ms,
                project.clone(),
                None,
                usage,
                order,
            ));
            order += 1;
        }
    }
    Ok(events)
}

#[allow(clippy::too_many_arguments)]
fn grok_usage_event(
    source_path: Arc<str>,
    source_record_id: Option<String>,
    session_id: Option<String>,
    request_id: Option<String>,
    timestamp_ms: u64,
    project: Option<String>,
    model: Option<String>,
    usage: &Value,
    source_order: u64,
) -> UsageEvent {
    let input = usage_u64(usage, "inputTokens");
    let cache_read = usage_u64(usage, "cachedReadTokens")
        .max(usage_u64(usage, "cacheReadInputTokens"))
        .min(input);
    let cache_write = usage_u64(usage, "cacheCreationTokens");
    let output = usage_u64(usage, "outputTokens");
    let reasoning = usage_u64(usage, "reasoningTokens").min(output);
    UsageEvent {
        source: "grok",
        source_path,
        source_record_id,
        session_id,
        request_id,
        message_id: None,
        timestamp_ms,
        project,
        provider: Some("xai".to_string()),
        model,
        tokens: TokenBuckets {
            raw_input: input,
            uncached_input: input.saturating_sub(cache_read).saturating_sub(cache_write),
            cache_read,
            cache_write,
            cache_write_1h: 0,
            output,
            reasoning,
        },
        source_cost_usd: usage
            .get("costUsdTicks")
            .and_then(Value::as_u64)
            .map(|ticks| ticks as f64 / 10_000_000_000.0),
        cost_authoritative: false,
        dedupe_confidence: "exact",
        conservative_undercount: false,
        cache_chain_excluded: true,
        sidechain: false,
        source_order,
    }
}

fn usage_u64(usage: &Value, key: &str) -> u64 {
    usage.get(key).and_then(Value::as_u64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn discovers_only_grok_session_updates() {
        let temp = TempDir::new().unwrap();
        let session = temp.path().join("encoded-cwd").join("session-id");
        fs::create_dir_all(&session).unwrap();
        fs::write(session.join("updates.jsonl"), "").unwrap();
        fs::write(session.join("chat_history.jsonl"), "").unwrap();

        let files = discover_sessions_from_root(temp.path());
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].source, SourceKind::Grok);
        assert_eq!(files[0].path, session.join("updates.jsonl"));
    }

    #[test]
    fn parses_messages_tools_metadata_and_opt_in_reasoning() {
        let temp = TempDir::new().unwrap();
        let session = temp.path().join("encoded-cwd").join("session-id");
        fs::create_dir_all(&session).unwrap();
        fs::write(
            session.join("summary.json"),
            r#"{"info":{"id":"session-id","cwd":"/work/repo"},"git_root_dir":"/work/repo"}"#,
        )
        .unwrap();
        let lines = [
            r#"{"timestamp":1700000000,"params":{"sessionId":"session-id","update":{"sessionUpdate":"user_message_chunk","content":{"type":"text","text":"hello"}},"_meta":{"eventId":"user-1","agentTimestampMs":1700000000001}}}"#,
            r#"{"timestamp":1700000001,"params":{"sessionId":"session-id","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"reasoning"}},"_meta":{"eventId":"thought-1"}}}"#,
            r#"{"timestamp":1700000002,"params":{"sessionId":"session-id","update":{"sessionUpdate":"tool_call","toolCallId":"call-1","rawInput":{"command":"pwd"},"_meta":{"x.ai/tool":{"name":"run_terminal_command"}}},"_meta":{"eventId":"tool-1"}}}"#,
            r#"{"timestamp":1700000003,"params":{"sessionId":"session-id","update":{"sessionUpdate":"tool_call_update","toolCallId":"call-1","status":"completed","content":[{"type":"content","content":{"type":"text","text":"/work/repo"}}]},"_meta":{"eventId":"result-1"}}}"#,
            r#"{"timestamp":1700000004,"params":{"sessionId":"session-id","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"done"}},"_meta":{"eventId":"assistant-1"}}}"#,
        ];
        fs::write(session.join("updates.jsonl"), lines.join("\n") + "\n").unwrap();

        let mut records = Vec::new();
        let parsed = parse_index_records(
            &session.join("updates.jsonl"),
            IndexParseState::default(),
            true,
            &AtomicU64::new(1),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(
            session_cwd(&session.join("updates.jsonl")).as_deref(),
            Some("/work/repo")
        );
        assert_eq!(parsed.session_id.as_deref(), Some("session-id"));
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].role, "user");
        assert_eq!(records[0].project, "repo");
        assert_eq!(records[1].role, "reasoning");
        assert_eq!(
            records[2].tool_name.as_deref(),
            Some("run_terminal_command")
        );
        assert_eq!(records[3].role, "tool_result");
        assert_eq!(
            records[3].links.parent_tool_use_id.as_deref(),
            Some("call-1")
        );
        assert_eq!(records[4].text, "done");
    }

    #[test]
    fn parses_grok_turn_usage_by_model() {
        let temp = TempDir::new().unwrap();
        let session = temp.path().join("encoded-cwd").join("session-id");
        fs::create_dir_all(&session).unwrap();
        fs::write(
            session.join("summary.json"),
            r#"{"info":{"id":"session-id","cwd":"/work/repo"}}"#,
        )
        .unwrap();
        fs::write(
            session.join("updates.jsonl"),
            r#"{"timestamp":1700000000,"params":{"sessionId":"session-id","update":{"sessionUpdate":"turn_completed","prompt_id":"prompt-1","usage":{"modelUsage":{"grok-test":{"inputTokens":100,"cachedReadTokens":40,"cacheCreationTokens":5,"outputTokens":20,"reasoningTokens":7,"costUsdTicks":125000000}}}},"_meta":{"eventId":"event-1","agentTimestampMs":1700000000001}}}"#,
        )
        .unwrap();

        let events = parse_usage_file(&session.join("updates.jsonl")).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].source, "grok");
        assert_eq!(events[0].model.as_deref(), Some("grok-test"));
        assert_eq!(events[0].tokens.uncached_input, 55);
        assert_eq!(events[0].tokens.cache_read, 40);
        assert_eq!(events[0].tokens.cache_write, 5);
        assert_eq!(events[0].tokens.output, 20);
        assert_eq!(events[0].tokens.reasoning, 7);
        assert_eq!(events[0].source_cost_usd, Some(0.0125));
    }
}
