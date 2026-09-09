use super::{IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions, SourceFile};
use crate::analytics::sanitize_label;
use crate::types::{
    Record, RecordLinks, SourceKind, jcode_text_is_subagent_directive,
    jcode_tmp_cwd_is_worker_sandbox,
};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::Result;
use simd_json::BorrowedValue;
use simd_json::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use walkdir::WalkDir;

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 1,
    // Bumped for the worker-sandbox cwd rule: forces a full re-parse so
    // bare-/tmp-cwd sessions reclassify on next index.
    index: 5,
    usage: 1,
};

pub fn matches_path(path: &str) -> bool {
    let normalized = path.replace('\\', "/");
    normalized.contains(".jcode/sessions")
        || normalized.contains("jcode/sessions")
        || (normalized.contains(".jcode") && normalized.ends_with(".json"))
}

pub fn sessions_root() -> PathBuf {
    if let Some(root) = std::env::var_os("JCODE_SESSIONS_DIR") {
        return PathBuf::from(root);
    }
    let base = std::env::var_os("JCODE_HOME")
        .or_else(|| std::env::var_os("JCODE_DIR"))
        .map(PathBuf::from)
        .unwrap_or_else(|| super::common::home().join(".jcode"));
    base.join("sessions")
}

pub fn discover() -> Vec<SourceFile> {
    let root = sessions_root();
    if !root.exists() {
        return Vec::new();
    }
    let mut files = WalkDir::new(root)
        .into_iter()
        .flatten()
        .filter(|entry| {
            entry.file_type().is_file()
                && entry
                    .path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|name| name.starts_with("session_") && name.ends_with(".json"))
        })
        .map(|entry| SourceFile {
            source: SourceKind::Jcode,
            path: entry.path().to_path_buf(),
        })
        .collect::<Vec<_>>();
    files.sort_by(|a, b| a.path.cmp(&b.path));
    files
}

pub fn session_id_from_path(path: &Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string()
}

pub fn cwd_from_jcode_session(path: &Path) -> Option<PathBuf> {
    let Ok(mut bytes) = std::fs::read(path) else {
        return None;
    };
    let Ok(value) = simd_json::to_borrowed_value(&mut bytes) else {
        return None;
    };
    value
        .get("working_dir")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
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
            if let Some(text) = object.get("text").and_then(|v| v.as_str()) {
                parts.push(text.to_string());
            } else if let Some(text) = object.get("content").and_then(|v| v.as_str()) {
                parts.push(text.to_string());
            }
        }
        return parts.join("\n");
    }
    String::new()
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

/// Resume origin for jcode parses. A session file is rewritten in place as
/// a single JSON object, so no byte offset can resume mid-file: the parser
/// always replays from this origin and ingest always pairs jcode with
/// atomic delete-first reparse (see `prepare_file_task`). The reported
/// output offset is the file length — a stored change marker, never a seek
/// position — so `state.offset` is deliberately unread.
pub(crate) const JCODE_PARSE_ORIGIN: u64 = 0;

pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    // Whole-document format: `prepare_file_task` forces whole-file replacement
    // (delete + reparse from scratch) on any change, so `state.offset` is
    // intentionally ignored here and every record is re-emitted.
    let mut bytes = std::fs::read(path)?;
    let bytes_len = bytes.len() as u64;
    let mut diagnostics = ParseDiagnostics::default();
    let Ok(value) = simd_json::to_borrowed_value(&mut bytes) else {
        diagnostics.malformed_json_lines += 1;
        return Ok(IndexParseOutput {
            legacy_turn_id: None,
            offset: JCODE_PARSE_ORIGIN,
            turn_id: state.turn_id,
            pending_tool_calls: state.pending_tool_calls,
            session_id: Some(session_id_from_path(path)),
            diagnostics,
        });
    };

    let session_id = value
        .get("id")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| session_id_from_path(path));

    let parent_session_id = value
        .get("parent_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let working_dir = value
        .get("working_dir")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // A bare /tmp working directory is not evidence on its own — users
    // legitimately work in /tmp — so the tmp cue needs a worker-sandbox
    // leaf name (see `crate::types::jcode_tmp_cwd_is_worker_sandbox`).
    let mut conversation_kind =
        if parent_session_id.is_some() || jcode_tmp_cwd_is_worker_sandbox(working_dir) {
            "subagent"
        } else {
            "main"
        };
    // Check directives in the first real user message for subagent hints.
    // Jcode sessions open with <system-reminder> preamble messages, so the
    // spawning directive usually sits in the second or third user message;
    // preamble-only messages sanitize to empty and are skipped. Patterns
    // live in `crate::types::jcode_text_is_subagent_directive`, shared with
    // the analytics `infer_session_kind` backstop.
    if conversation_kind == "main"
        && let Some(messages) = value.get("messages").and_then(|v| v.as_array())
    {
        for message in messages {
            let Some(obj) = message.as_object() else {
                continue;
            };
            if obj.get("role").and_then(|v| v.as_str()) != Some("user") {
                continue;
            }
            let mut first_text = String::new();
            if let Some(content) = obj.get("content") {
                if let Some(s) = content.as_str() {
                    first_text = s.to_string();
                } else if let Some(arr) = content.as_array() {
                    for block in arr {
                        if let Some(t) = block
                            .as_object()
                            .and_then(|o| o.get("text"))
                            .and_then(|v| v.as_str())
                        {
                            first_text.push_str(t);
                            first_text.push('\n');
                        }
                    }
                }
            }
            if sanitize_label(&first_text).is_empty() {
                continue;
            }
            if jcode_text_is_subagent_directive(&first_text.to_lowercase()) {
                conversation_kind = "subagent";
            }
            break;
        }
    }

    let project = if !working_dir.is_empty() {
        super::common::project_from_path(working_dir)
    } else {
        SourceKind::Jcode.label().to_string()
    };

    let default_timestamp = value.get("created_at").map(timestamp_millis).unwrap_or(0);

    let source_path = path.to_string_lossy().to_string();
    let mut turn_id = state.turn_id;
    let mut pending_tool_calls = state.pending_tool_calls;

    let base_links = RecordLinks {
        parent_session_id,
        conversation_kind: Some(conversation_kind.to_string()),
        thread_source: (conversation_kind != "main").then(|| conversation_kind.to_string()),
        ..RecordLinks::default()
    };

    let messages = value.get("messages").and_then(|v| v.as_array());
    if let Some(messages) = messages {
        for message in messages {
            let Some(msg_obj) = message.as_object() else {
                continue;
            };
            let role = msg_obj.get("role").and_then(|v| v.as_str()).unwrap_or("");
            let msg_id = msg_obj
                .get("id")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let timestamp = msg_obj
                .get("timestamp")
                .map(timestamp_millis)
                .filter(|t| *t > 0)
                .unwrap_or(default_timestamp);

            let mut msg_links = base_links.clone();
            if let Some(ref id) = msg_id {
                msg_links.event_id = Some(id.clone());
            }

            match role {
                "user" => {
                    let content = msg_obj.get("content");
                    let mut text_parts = Vec::new();
                    if let Some(arr) = content.and_then(|v| v.as_array()) {
                        for block in arr {
                            let Some(block_obj) = block.as_object() else {
                                if let Some(s) = block.as_str() {
                                    text_parts.push(s.to_string());
                                }
                                continue;
                            };
                            let block_type =
                                block_obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
                            if block_type == "tool_result" {
                                let tool_call_id = block_obj
                                    .get("tool_use_id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                let tool_name = if !tool_call_id.is_empty() {
                                    pending_tool_calls
                                        .remove(tool_call_id)
                                        .and_then(|call| call.tool_name)
                                } else {
                                    None
                                };
                                let mut output = content_text(block_obj.get("content"));
                                if output.is_empty()
                                    && let Some(text) =
                                        block_obj.get("text").and_then(|v| v.as_str())
                                {
                                    output = text.to_string();
                                }
                                if block_obj
                                    .get("is_error")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false)
                                    && !output.to_ascii_lowercase().starts_with("error")
                                {
                                    output = format!("Error: {output}");
                                }
                                if !output.trim().is_empty() {
                                    let mut links = msg_links.clone();
                                    if !tool_call_id.is_empty() {
                                        links.parent_tool_use_id = Some(tool_call_id.to_string());
                                    }
                                    emit(Record {
                                        source: SourceKind::Jcode,
                                        doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                                        ts: timestamp,
                                        project: project.clone(),
                                        session_id: session_id.clone(),
                                        turn_id,
                                        role: "tool_result".to_string(),
                                        text: output.clone(),
                                        tool_name,
                                        tool_input: None,
                                        tool_output: Some(output),
                                        links,
                                        source_path: source_path.clone(),
                                    })?;
                                    turn_id += 1;
                                }
                            } else if let Some(text) =
                                block_obj.get("text").and_then(|v| v.as_str())
                            {
                                text_parts.push(text.to_string());
                            }
                        }
                    } else if let Some(text) = content.and_then(|v| v.as_str()) {
                        text_parts.push(text.to_string());
                    }

                    let full_text = text_parts.join("\n").trim().to_string();
                    // Preamble-only messages (system context wrappers with no
                    // real content) are dropped: they add no searchable value,
                    // and a session containing nothing else yields zero
                    // records, excluding it from the index altogether.
                    if !sanitize_label(&full_text).is_empty() {
                        emit(Record {
                            source: SourceKind::Jcode,
                            doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                            ts: timestamp,
                            project: project.clone(),
                            session_id: session_id.clone(),
                            turn_id,
                            role: "user".to_string(),
                            text: full_text,
                            tool_name: None,
                            tool_input: None,
                            tool_output: None,
                            links: msg_links,
                            source_path: source_path.clone(),
                        })?;
                        turn_id += 1;
                    }
                }
                "assistant" => {
                    let content = msg_obj.get("content");
                    let mut text_parts = Vec::new();
                    if let Some(arr) = content.and_then(|v| v.as_array()) {
                        for block in arr {
                            let Some(block_obj) = block.as_object() else {
                                if let Some(s) = block.as_str() {
                                    text_parts.push(s.to_string());
                                }
                                continue;
                            };
                            let block_type =
                                block_obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
                            match block_type {
                                "text" => {
                                    if let Some(text) =
                                        block_obj.get("text").and_then(|v| v.as_str())
                                    {
                                        text_parts.push(text.to_string());
                                    }
                                }
                                "tool_use" => {
                                    let tool_name = block_obj
                                        .get("name")
                                        .and_then(|v| v.as_str())
                                        .map(str::to_string);
                                    let tool_call_id = block_obj
                                        .get("id")
                                        .and_then(|v| v.as_str())
                                        .map(str::to_string);
                                    let tool_input = block_obj.get("input").map(|v| v.to_string());

                                    let mut links = msg_links.clone();
                                    if let Some(ref id) = tool_call_id {
                                        links.event_id = Some(id.clone());
                                        links.parent_event_id = msg_id.clone();
                                    }
                                    let doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
                                    if let Some(ref call_id) = tool_call_id {
                                        let replaced = pending_tool_calls.insert(
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
                                        if replaced.is_some() {
                                            diagnostics.duplicate_tool_calls += 1;
                                        }
                                    }
                                    emit(Record {
                                        source: SourceKind::Jcode,
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
                                "thinking" => {
                                    if include_reasoning
                                        && let Some(thinking) = block_obj
                                            .get("thinking")
                                            .and_then(|v| v.as_str())
                                            .filter(|t| !t.trim().is_empty())
                                    {
                                        let mut links = msg_links.clone();
                                        if let Some(ref id) = msg_id {
                                            links.event_id = Some(format!("{id}:reasoning"));
                                        }
                                        emit(Record {
                                            source: SourceKind::Jcode,
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
                                }
                                _ => {}
                            }
                        }
                    } else if let Some(text) = content.and_then(|v| v.as_str()) {
                        text_parts.push(text.to_string());
                    }

                    let full_text = text_parts.join("\n").trim().to_string();
                    if !full_text.is_empty() {
                        emit(Record {
                            source: SourceKind::Jcode,
                            doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                            ts: timestamp,
                            project: project.clone(),
                            session_id: session_id.clone(),
                            turn_id,
                            role: "assistant".to_string(),
                            text: full_text,
                            tool_name: None,
                            tool_input: None,
                            tool_output: None,
                            links: msg_links,
                            source_path: source_path.clone(),
                        })?;
                        turn_id += 1;
                    }
                }
                _ => {}
            }
        }
    }

    Ok(IndexParseOutput {
        legacy_turn_id: None,
        // Change marker only: resume always restarts at JCODE_PARSE_ORIGIN.
        offset: bytes_len,
        turn_id,
        pending_tool_calls,
        session_id: Some(session_id),
        diagnostics,
    })
}

pub fn usage_files() -> Vec<PathBuf> {
    discover().into_iter().map(|f| f.path).collect()
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<Vec<UsageEvent>> {
    let mut bytes = std::fs::read(path)?;
    let Ok(value) = simd_json::to_borrowed_value(&mut bytes) else {
        return Ok(Vec::new());
    };
    let session_id = value
        .get("id")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| Some(session_id_from_path(path)));

    let working_dir = value
        .get("working_dir")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let project = if !working_dir.is_empty() {
        Some(super::common::project_from_path(working_dir))
    } else {
        Some(SourceKind::Jcode.label().to_string())
    };

    let model = value
        .get("model")
        .and_then(|v| v.as_str())
        .filter(|m| *m != "unknown")
        .map(str::to_string);

    let provider = value
        .get("provider_key")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let mut events = Vec::new();
    let messages = value.get("messages").and_then(|v| v.as_array());

    if let Some(messages) = messages {
        for message in messages {
            let Some(msg_obj) = message.as_object() else {
                continue;
            };
            if msg_obj.get("role").and_then(|v| v.as_str()) != Some("assistant") {
                continue;
            }
            let usage = msg_obj.get("token_usage").or_else(|| msg_obj.get("usage"));
            let Some(usage) = usage else {
                continue;
            };
            let number = |key: &str| usage.get(key).and_then(|v| v.as_u64()).unwrap_or(0);
            let input = number("input_tokens").max(number("input"));
            let output = number("output_tokens").max(number("output"));
            let cache_read = number("cache_read_input_tokens").max(number("cache_read"));
            let cache_write = number("cache_creation_input_tokens")
                .max(number("cache_creation_tokens"))
                .max(number("cache_write"));
            let reasoning = number("reasoning_tokens").max(number("reasoning"));

            let mut tokens = TokenBuckets::disjoint(
                input,
                cache_read,
                cache_write,
                output.saturating_add(reasoning),
            );
            tokens.reasoning = reasoning;
            if tokens.additive_total() == 0 {
                continue;
            }

            let msg_id = msg_obj
                .get("id")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let timestamp_ms = msg_obj.get("timestamp").map(timestamp_millis).unwrap_or(0);

            events.push(UsageEvent {
                source: "jcode",
                source_path: source_path.clone(),
                source_record_id: msg_id.clone(),
                session_id: session_id.clone(),
                request_id: None,
                message_id: msg_id,
                timestamp_ms,
                project: project.clone(),
                provider: provider.clone(),
                model: model.clone(),
                tokens,
                source_cost_usd: None,
                cost_authoritative: false,
                dedupe_confidence: "exact",
                conservative_undercount: false,
                cache_chain_excluded: false,
                sidechain: false,
                permission_review: false,
                source_order: 0,
            });
        }
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_jcode_session_record_and_usage() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session_test_123.json");
        std::fs::write(
            &path,
            r#"{
                "id": "session_test_123",
                "parent_id": null,
                "title": "Test Jcode Session",
                "working_dir": "/repo/memex",
                "provider_key": "meta-muse",
                "model": "muse-spark-1.2-contributor",
                "messages": [
                    {
                        "id": "msg_001",
                        "role": "user",
                        "timestamp": 1788338812928,
                        "content": [
                            {"type": "text", "text": "Hello Jcode"}
                        ]
                    },
                    {
                        "id": "msg_002",
                        "role": "assistant",
                        "timestamp": 1788338819456,
                        "content": [
                            {"type": "text", "text": "I can help with that."},
                            {"type": "tool_use", "id": "call_001", "name": "bash", "input": {"command": "ls -la"}},
                            {"type": "thinking", "thinking": "Let's check directory"}
                        ],
                        "token_usage": {
                            "input_tokens": 1500,
                            "output_tokens": 80,
                            "cache_read_input_tokens": 500
                        }
                    },
                    {
                        "id": "msg_003",
                        "role": "user",
                        "timestamp": 1788338821616,
                        "content": [
                            {"type": "tool_result", "tool_use_id": "call_001", "content": "total 0\n"}
                        ]
                    }
                ]
            }"#,
        )
        .unwrap();

        let mut records = Vec::new();
        let next_doc_id = AtomicU64::new(1);
        let out = parse_index_records(
            &path,
            IndexParseState::default(),
            true,
            &next_doc_id,
            |rec| {
                records.push(rec);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(out.session_id.as_deref(), Some("session_test_123"));
        assert_eq!(records.len(), 5); // user text, tool_use, reasoning, assistant text, tool_result
        assert_eq!(records[0].role, "user");
        assert_eq!(records[0].text, "Hello Jcode");
        assert_eq!(records[0].project, "memex");
        assert_eq!(records[1].role, "tool_use");
        assert_eq!(records[1].tool_name.as_deref(), Some("bash"));
        assert_eq!(records[2].role, "reasoning");
        assert_eq!(records[2].text, "Let's check directory");
        assert_eq!(records[3].role, "assistant");
        assert_eq!(records[3].text, "I can help with that.");
        assert_eq!(records[4].role, "tool_result");
        assert_eq!(records[4].tool_name.as_deref(), Some("bash"));

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tokens.uncached_input, 1500);
        assert_eq!(events[0].tokens.output, 80);
        assert_eq!(events[0].tokens.cache_read, 500);
        assert_eq!(
            events[0].model.as_deref(),
            Some("muse-spark-1.2-contributor")
        );
        assert_eq!(events[0].provider.as_deref(), Some("meta-muse"));
    }

    fn kind_for_user_texts(texts: &[&str]) -> Option<String> {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session_probe.json");
        let messages: Vec<serde_json::Value> = texts
            .iter()
            .enumerate()
            .map(|(i, text)| {
                serde_json::json!({
                    "id": format!("m{i:03}"),
                    "role": "user",
                    "timestamp": i as u64,
                    "content": text,
                })
            })
            .collect();
        let doc = serde_json::json!({
            "id": "session_probe",
            "parent_id": null,
            "working_dir": "/repo/example",
            "messages": messages,
        });
        std::fs::write(&path, serde_json::to_string(&doc).unwrap()).unwrap();
        let mut kinds = Vec::new();
        let next_doc_id = AtomicU64::new(1);
        parse_index_records(
            &path,
            IndexParseState::default(),
            true,
            &next_doc_id,
            |rec| {
                kinds.push(rec.links.conversation_kind.clone());
                Ok(())
            },
        )
        .unwrap();
        kinds.into_iter().next().flatten()
    }

    #[test]
    fn directive_behind_preamble_is_classified_as_subagent() {
        let kind = kind_for_user_texts(&[
            "<system-reminder>\n# Session Context\nDate: 2026-09-01\n</system-reminder>",
            "You are downstream A8-A11 complete workflow mapper for BenchBox (fresh 01:48Z run). Read-only, no repo writes outside /tmp.",
        ]);
        assert_eq!(kind.as_deref(), Some("subagent"));
    }

    #[test]
    fn worker_role_directives_are_classified_as_subagent() {
        let kind = kind_for_user_texts(&[
            "You are the focused implementation worker for Bossmode task task_f852ab12.",
        ]);
        assert_eq!(kind.as_deref(), Some("subagent"));
        let kind = kind_for_user_texts(&[
            "You are an investigation subagent. In /repo/example, triage the failure.",
        ]);
        assert_eq!(kind.as_deref(), Some("subagent"));
    }

    #[test]
    fn fresh_cycle_and_validation_directives_are_subagent() {
        let kind = kind_for_user_texts(&[
            "<system-reminder>\n# Session Context\nDate: 2026-09-02\n</system-reminder>",
            "FRESH 12:38Z definitive \u{2014} supersede wolf 183 authoritative: At live poll 12:38:43Z origin/develop STILL d7d518e. 0 repo writes.",
        ]);
        assert_eq!(kind.as_deref(), Some("subagent"));
        let kind = kind_for_user_texts(&[
            "Deep validation: Orchestration ORIGIN d7d518e STEADY ~255m at 07:50:05Z live triad. Read-only, 0 repo writes outside /tmp.",
        ]);
        assert_eq!(kind.as_deref(), Some("subagent"));
    }

    #[test]
    fn preamble_only_session_yields_no_records() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session_empty.json");
        let doc = serde_json::json!({
            "id": "session_empty",
            "parent_id": null,
            "working_dir": "/repo/example",
            "messages": [
                {"id": "m001", "role": "user", "timestamp": 1,
                 "content": "<system-reminder>\n# Session Context\nDate: 2026-09-02\n</system-reminder>"},
            ],
        });
        std::fs::write(&path, serde_json::to_string(&doc).unwrap()).unwrap();
        let mut records = Vec::new();
        let next_doc_id = AtomicU64::new(1);
        parse_index_records(
            &path,
            IndexParseState::default(),
            true,
            &next_doc_id,
            |rec| {
                records.push(rec);
                Ok(())
            },
        )
        .unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn ordinary_prompts_with_worker_words_stay_main() {
        // Adversarial negatives: each contains a bare worker-vocabulary
        // word, but none in the role-assignment or constraint forms the
        // classifier requires.
        for text in [
            "Add a `role: manager` column to the users table and backfill it.",
            "Please review the implementation worker pool sizing in src/pool.rs",
            "Our CI policy says 0 repo writes from forks - document that in CONTRIBUTING.md",
        ] {
            assert_eq!(
                kind_for_user_texts(&[text]),
                Some("main".to_string()),
                "should stay main: {text}"
            );
        }
    }

    #[test]
    fn interactive_prompts_stay_main() {
        // Handoff continuations and read-only reviews are routinely issued
        // interactively; they must not match the worker-role patterns.
        let kind = kind_for_user_texts(&[
            "take over and fully complete all work from the interrupted session `muse resume abc123`",
        ]);
        assert_eq!(kind.as_deref(), Some("main"));
        let kind = kind_for_user_texts(&[
            "Perform a final independent read-only pre-publication review of release 0.4.0.",
        ]);
        assert_eq!(kind.as_deref(), Some("main"));
    }
}
