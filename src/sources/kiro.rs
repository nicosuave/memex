//! Kiro CLI's session.json metadata and append-only messages.jsonl transcripts.
use super::{IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions, SourceFile};
use crate::types::{Record, RecordLinks, SourceKind};
use anyhow::Result;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use walkdir::WalkDir;

pub const VERSIONS: ParserVersions = ParserVersions {
    // Refresh analytics for archived workspace project grouping.
    identity: 2,
    index: 2,
    usage: 1,
};

pub fn sessions_root() -> PathBuf {
    std::env::var_os("KIRO_SESSIONS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| super::common::home().join(".kiro/sessions"))
}

pub fn matches_path(path: &str) -> bool {
    let path = path.replace('\\', "/");
    path.contains("/.kiro/sessions/") && path.ends_with("/messages.jsonl")
}

pub fn discover() -> Vec<SourceFile> {
    discover_from_root(&sessions_root())
}

fn discover_from_root(root: &Path) -> Vec<SourceFile> {
    let mut files = WalkDir::new(root)
        .min_depth(3)
        .max_depth(3)
        .into_iter()
        .flatten()
        .filter(|entry| entry.file_type().is_file() && entry.file_name() == "messages.jsonl")
        .map(|entry| SourceFile {
            source: SourceKind::Kiro,
            path: entry.into_path(),
        })
        .collect::<Vec<_>>();
    files.sort_by(|a, b| a.path.cmp(&b.path));
    files
}

#[derive(Default, serde::Serialize)]
struct Metadata {
    id: Option<String>,
    cwd: Option<String>,
    parent: Option<String>,
    reason: Option<String>,
}

fn string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn metadata(path: &Path) -> Metadata {
    let value = std::fs::read(path.with_file_name("session.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .unwrap_or(Value::Null);
    let cwd = ["workspacePaths", "rootPaths"].into_iter().find_map(|key| {
        value
            .get(key)?
            .as_array()?
            .iter()
            .filter_map(Value::as_str)
            .find(|s| !s.is_empty())
            .map(str::to_owned)
    });
    Metadata {
        id: string(&value, "id"),
        cwd,
        parent: string(&value, "parentSessionId"),
        reason: string(&value, "createdReason"),
    }
}

pub fn session_cwd(path: &Path) -> Option<String> {
    metadata(path).cwd
}

pub(crate) fn metadata_fingerprint(path: &Path) -> String {
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&metadata(path)).expect("metadata strings serialize"))
    )
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<super::UsageParseOutput> {
    let meta = metadata(path);
    let session_id = meta.id.unwrap_or_else(|| {
        path.parent()
            .and_then(Path::file_name)
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned()
    });
    let mut output = super::UsageParseOutput::cacheable(Vec::new());
    output
        .deps
        .push(super::UsageDependency::from_path_or_absent(
            &path.with_file_name("session.json"),
        ));
    let mut reader = BufReader::new(std::fs::File::open(path)?);
    let mut line = String::new();
    let mut order = 0;
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        if !line.ends_with('\n') {
            output.cacheable = false;
            break;
        }
        order += 1;
        if line.trim().is_empty() {
            continue;
        }
        // Like the index parser, a malformed line (or a summary this parser
        // cannot price in credits) is skipped instead of dropping the file.
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let payload = &value["payload"];
        if payload["type"] != "usage_summary" {
            continue;
        }
        let Some(summaries) = payload["promptTurnSummaries"]
            .as_array()
            .filter(|v| !v.is_empty())
        else {
            continue;
        };
        let Some(credits) = summaries.iter().try_fold(0.0, |total: f64, summary| {
            matches!(summary["unit"].as_str(), Some("credit" | "credits"))
                .then(|| summary["usage"].as_f64())
                .flatten()
                .filter(|amount| amount.is_finite() && *amount >= 0.0)
                .map(|amount| total + amount)
                .filter(|total| total.is_finite())
        }) else {
            continue;
        };
        let Some(timestamp_ms) = value["timestamp"]
            .as_str()
            .and_then(super::common::parse_iso_millis)
        else {
            continue;
        };
        let Some(execution_id) = string(payload, "executionId").or_else(|| string(&value, "id"))
        else {
            continue;
        };
        output.events.push(crate::usage::UsageEvent {
            source: "kiro",
            source_path: path.to_string_lossy().as_ref().into(),
            source_record_id: Some(execution_id),
            session_id: Some(session_id.clone()),
            request_id: None,
            message_id: string(&value, "id"),
            timestamp_ms,
            project: meta.cwd.clone(),
            provider: None,
            model: None,
            tokens: crate::usage::TokenBuckets::default(),
            credits: Some(credits),
            token_usage_available: false,
            source_cost_usd: None,
            cost_authoritative: false,
            dedupe_confidence: "exact",
            conservative_undercount: false,
            cache_chain_excluded: true,
            sidechain: meta.parent.is_some(),
            permission_review: false,
            source_order: order,
        });
    }
    Ok(output)
}

pub(crate) fn reconcile_usage(events: &mut Vec<crate::usage::UsageEvent>) {
    let mut latest = std::collections::HashMap::new();
    for (index, event) in events
        .iter()
        .enumerate()
        .filter(|(_, e)| e.source == "kiro")
    {
        let Some(id) = &event.source_record_id else {
            continue;
        };
        let key = (event.session_id.clone(), id.clone());
        let rank = (event.timestamp_ms, event.source_order, index);
        let previous = latest.entry(key).or_insert(rank);
        if rank > *previous {
            *previous = rank;
        }
    }
    let keep = latest
        .into_values()
        .map(|(_, _, index)| index)
        .collect::<std::collections::HashSet<_>>();
    let mut index = 0;
    events.retain(|event| {
        let retain =
            event.source != "kiro" || event.source_record_id.is_none() || keep.contains(&index);
        index += 1;
        retain
    });
}

fn source_content(payload: &Value) -> Result<Option<String>> {
    let mut blocks = Vec::new();
    for (field, kind, reference) in [
        ("images", "image", "image_url"),
        ("documents", "document", "file_url"),
    ] {
        for attachment in payload[field].as_array().into_iter().flatten() {
            let mut block = match attachment {
                Value::Object(object) => Value::Object(object.clone()),
                Value::String(value) if !value.is_empty() => {
                    serde_json::json!({reference: value})
                }
                _ => continue,
            };
            block["type"] = kind.into();
            if let Some(title) = string(attachment, "displayName") {
                block["title"] = title.into();
            }
            if field == "documents" {
                if let Some(content) = attachment.get("content").and_then(Value::as_str) {
                    block["source"] = serde_json::json!({"type": "text", "data": content});
                } else if let Some(id) = string(attachment, "id") {
                    let key = if id.starts_with("file:") {
                        "file_url"
                    } else {
                        "file_id"
                    };
                    block[key] = id.into();
                }
            }
            blocks.push(block);
        }
    }
    if blocks.is_empty() {
        Ok(None)
    } else {
        Ok(Some(serde_json::to_string(&blocks)?))
    }
}

pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    let mut reader = BufReader::new(std::fs::File::open(path)?);
    reader.seek(SeekFrom::Start(state.offset))?;
    let mut offset = state.offset;
    let mut turn_id = state.turn_id;
    let mut pending_tool_calls = state.pending_tool_calls;
    let mut diagnostics = ParseDiagnostics::default();
    let meta = metadata(path);
    let session_id = meta.id.unwrap_or_else(|| {
        path.parent()
            .and_then(Path::file_name)
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned()
    });
    let project = meta
        .cwd
        .as_deref()
        .map(super::common::project_from_path)
        .unwrap_or_else(|| "kiro".into());
    let kind = if meta.parent.is_some() && meta.reason.as_deref() == Some("tangent") {
        "fork"
    } else {
        "main"
    };
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 || line.last() != Some(&b'\n') {
            break;
        }
        offset += read as u64;
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let value: Value = match serde_json::from_slice(&line) {
            Ok(value) => value,
            Err(_) => {
                diagnostics.malformed_json_lines += 1;
                continue;
            }
        };
        if !value.is_object() {
            diagnostics.non_object_json_lines += 1;
            continue;
        }
        let payload = &value["payload"];
        let event_type = payload["type"].as_str().unwrap_or("missing_payload_type");
        let mut record = Record {
            source: SourceKind::Kiro,
            doc_id: 0,
            ts: value["timestamp"]
                .as_str()
                .and_then(super::common::parse_iso_millis)
                .unwrap_or(0),
            project: project.clone(),
            session_id: session_id.clone(),
            turn_id,
            role: String::new(),
            text: String::new(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks {
                event_id: string(&value, "id"),
                parent_session_id: meta.parent.clone(),
                conversation_kind: Some(kind.into()),
                ..RecordLinks::default()
            },
            source_path: path.to_string_lossy().into_owned(),
        };
        match event_type {
            "user" | "assistant" => {
                record.role = event_type.into();
                if event_type == "assistant" {
                    match payload["operationType"]
                        .as_str()
                        .unwrap_or("missing_operation_type")
                    {
                        "Say" => {}
                        "Reasoning" if include_reasoning => record.role = "reasoning".into(),
                        "Reasoning" => continue,
                        "Summary" => {
                            record.links.conversation_kind = Some("compaction".into());
                            record.links.thread_source = Some("compaction".into());
                        }
                        unknown => {
                            diagnostics.increment_unknown_semantic(unknown);
                            continue;
                        }
                    }
                }
                record.text = string(payload, "content").unwrap_or_default();
                if record.role == "user" || payload["operationType"] == "Say" {
                    record.links.source_content = source_content(payload)?;
                }
                if record.text.trim().is_empty() && record.links.source_content.is_none() {
                    continue;
                }
                if event_type == "user" {
                    turn_id += 1;
                    record.turn_id = turn_id;
                }
            }
            "tool_call" | "sub_agent_start" => {
                let subagent = event_type == "sub_agent_start";
                let call_id = string(
                    payload,
                    if subagent {
                        "subSessionId"
                    } else {
                        "toolCallId"
                    },
                );
                record.role = "tool_use".into();
                record.tool_name = if subagent {
                    Some(format!(
                        "subagent:{}",
                        payload["subAgentName"].as_str().unwrap_or("unknown")
                    ))
                } else {
                    string(payload, "toolName")
                };
                record.tool_input = if subagent {
                    string(payload, "prompt")
                } else {
                    payload.get("args").map(|v| v.to_string())
                };
                record.text = record.tool_input.clone().unwrap_or_default();
                record.links.source_tool_use_id = call_id.clone();
                record.doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
                if let Some(id) = call_id {
                    let pending = super::common::pending_tool_call(
                        record.tool_name.clone(),
                        record.links.event_id.clone(),
                        record.doc_id,
                        record.ts,
                        record.tool_input.as_deref(),
                        &record.links,
                        &session_id,
                    );
                    if pending_tool_calls.insert(id, pending).is_some() {
                        diagnostics.duplicate_tool_calls += 1;
                    }
                }
                emit(record)?;
                continue;
            }
            "tool_result" | "sub_agent_complete" => {
                let subagent = event_type == "sub_agent_complete";
                let call_id = string(
                    payload,
                    if subagent {
                        "subSessionId"
                    } else {
                        "toolCallId"
                    },
                );
                let pending = call_id
                    .as_ref()
                    .and_then(|id| pending_tool_calls.remove(id));
                if pending.is_none() {
                    diagnostics.orphan_tool_results += 1;
                }
                record.role = "tool_result".into();
                record.tool_name = pending.as_ref().and_then(|p| p.tool_name.clone());
                record.links.parent_event_id = pending.and_then(|p| p.tool_use_event_id);
                record.links.parent_tool_use_id = call_id;
                record.tool_output = payload
                    .get(if subagent { "response" } else { "content" })
                    .map(|v| {
                        v.as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| v.to_string())
                    });
                record.text = record.tool_output.clone().unwrap_or_default();
            }
            "steering_inclusion" => {
                record.links.source_content = source_content(payload)?;
                if record.links.source_content.is_none() {
                    continue;
                }
                record.role = "system".into();
            }
            "turn_start"
            | "turn_end"
            | "session_start"
            | "session_metadata"
            | "session_event"
            | "ContextualHookInvoked"
            | "pending_interaction"
            | "interaction_resolved"
            | "usage_summary"
            | "tombstone" => continue,
            unknown => {
                diagnostics.increment_unknown_top_level(unknown);
                continue;
            }
        }
        record.doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
        emit(record)?;
    }
    Ok(IndexParseOutput {
        legacy_turn_id: None,
        offset,
        turn_id,
        pending_tool_calls,
        session_id: Some(session_id),
        session_cwd: session_cwd(path),
        diagnostics,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::io::Write;

    fn event(id: &str, payload: Value) -> String {
        json!({"id": id, "timestamp": "2026-09-04T11:11:05.286Z", "payload": payload}).to_string()
            + "\n"
    }

    #[test]
    fn attachments_survive_full_and_incremental_parsing_without_reasoning() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("messages.jsonl");
        let image = json!({"data":"AAAA", "mimeType":"image/png"});
        let document = json!({"id":"file:///old/rules.md", "displayName":"Rules",
            "content":"historical rules", "scope":"workspace"});
        let prefix = event(
            "image",
            json!({"type":"user", "content":"",
            "images":[image, "file:///tmp/photo.png"], "documents":[]}),
        );
        fs::write(&path, &prefix).unwrap();
        let ids = AtomicU64::new(1);
        let mut records = Vec::new();
        let parsed = parse_index_records(&path, IndexParseState::default(), false, &ids, |r| {
            records.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].turn_id, 1);
        let suffix = event(
            "docs",
            json!({"type":"steering_inclusion",
            "documents":["file:///tmp/report.pdf", document]}),
        ) + &event(
            "mixed",
            json!({"type":"user", "content":"Read this",
                "documents":[{"id":"provider-id", "displayName":"Report"}]}),
        ) + &event(
            "reason",
            json!({"type":"assistant", "operationType":"Reasoning",
                "content":"private thought", "documents":[document], "reasoningSignature":"secret"}),
        ) + &event(
            "empty",
            json!({"type":"user", "images":[null, 12], "documents":[]}),
        );
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(suffix.as_bytes())
            .unwrap();
        parse_index_records(
            &path,
            IndexParseState {
                offset: parsed.offset,
                turn_id: parsed.turn_id,
                legacy_turn_id: parsed.legacy_turn_id,
                pending_tool_calls: parsed.pending_tool_calls,
            },
            false,
            &ids,
            |r| {
                records.push(r);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(records.len(), 3);
        let blocks = |r: &Record| {
            serde_json::from_str::<Value>(r.links.source_content.as_ref().unwrap()).unwrap()
        };
        assert_eq!(
            blocks(&records[0]),
            json!([
                {"type":"image", "data":"AAAA", "mimeType":"image/png"},
                {"type":"image", "image_url":"file:///tmp/photo.png"}
            ])
        );
        assert_eq!(records[1].role, "system");
        assert!(records[1].text.is_empty());
        assert_eq!(
            blocks(&records[1])[0],
            json!({"type":"document", "file_url":"file:///tmp/report.pdf"})
        );
        assert_eq!(
            blocks(&records[1])[1]["source"],
            json!({"type":"text", "data":"historical rules"})
        );
        assert_eq!(blocks(&records[1])[1]["content"], "historical rules");
        assert_eq!(blocks(&records[2])[0]["file_id"], "provider-id");
        assert_eq!(records[2].text, "Read this");
        let mut full = Vec::new();
        parse_index_records(&path, IndexParseState::default(), true, &ids, |r| {
            full.push(r);
            Ok(())
        })
        .unwrap();
        for (incremental, complete) in records.iter().zip(&full) {
            assert_eq!(
                incremental.links.source_content,
                complete.links.source_content
            );
            assert_eq!(incremental.turn_id, complete.turn_id);
            assert_eq!(
                crate::retrieval::canonical_record_id(incremental),
                crate::retrieval::canonical_record_id(complete)
            );
        }
        assert_eq!(full.len(), 4);
        assert_eq!(full[3].role, "reasoning");
        assert!(full[3].links.source_content.is_none());
    }

    #[test]
    fn projection_preserves_history_and_pairs_tools_across_appends() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("workspace/session");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("messages.jsonl");
        fs::write(dir.join("session.json"), r#"{"id":"s","workspacePaths":["/work/project"],"parentSessionId":"parent","createdReason":"tangent"}"#).unwrap();
        let prefix = event(
            "u",
            json!({"type":"user","content":"original","source":"steer"}),
        ) + &event(
            "r",
            json!({"type":"assistant","operationType":"Reasoning","content":"thought","reasoningSignature":"secret"}),
        ) + &event(
            "c",
            json!({"type":"tool_call","toolCallId":"call","toolName":"read","args":{"path":"a"}}),
        );
        fs::write(&path, &prefix).unwrap();
        let ids = AtomicU64::new(1);
        let mut records = Vec::new();
        let parsed = parse_index_records(&path, IndexParseState::default(), false, &ids, |r| {
            records.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].project, "project");
        assert_eq!(records[0].session_id, "s");
        assert_eq!(records[0].links.conversation_kind.as_deref(), Some("fork"));
        assert_eq!(
            records[0].links.parent_session_id.as_deref(),
            Some("parent")
        );
        assert_eq!(records[0].ts, 1788520265286);
        let suffix = event(
            "out",
            json!({"type":"tool_result","toolCallId":"call","content":"file contents"}),
        ) + &event(
            "t",
            json!({"type":"tombstone","kind":"summarization","effectiveFromMessageId":"u"}),
        ) + &event(
            "s",
            json!({"type":"assistant","operationType":"Summary","content":"summary"}),
        ) + &event(
            "sub",
            json!({"type":"sub_agent_start","subSessionId":"child","subAgentName":"reviewer","prompt":"review"}),
        ) + &event(
            "subout",
            json!({"type":"sub_agent_complete","subSessionId":"child","response":"reviewed"}),
        ) + &event(
            "say",
            json!({"type":"assistant","operationType":"Say","content":"done"}),
        );
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(suffix.as_bytes())
            .unwrap();
        let state = IndexParseState {
            offset: parsed.offset,
            turn_id: parsed.turn_id,
            legacy_turn_id: parsed.legacy_turn_id,
            pending_tool_calls: parsed.pending_tool_calls,
        };
        let parsed = parse_index_records(&path, state, false, &ids, |r| {
            records.push(r);
            Ok(())
        })
        .unwrap();
        assert!(parsed.diagnostics.is_empty());
        assert!(parsed.pending_tool_calls.is_empty());
        assert_eq!(records.len(), 7);
        assert_eq!(records[2].tool_name.as_deref(), Some("read"));
        assert_eq!(records[2].links.parent_event_id.as_deref(), Some("c"));
        assert_eq!(records[2].links.parent_tool_use_id.as_deref(), Some("call"));
        assert_eq!(
            records[3].links.conversation_kind.as_deref(),
            Some("compaction")
        );
        assert_eq!(records[5].tool_name.as_deref(), Some("subagent:reviewer"));
        let mut full = Vec::new();
        parse_index_records(&path, IndexParseState::default(), false, &ids, |r| {
            full.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!(
            records
                .iter()
                .map(|r| (&r.text, r.turn_id))
                .collect::<Vec<_>>(),
            full.iter()
                .map(|r| (&r.text, r.turn_id))
                .collect::<Vec<_>>()
        );
        let mut reasoning = Vec::new();
        parse_index_records(&path, IndexParseState::default(), true, &ids, |r| {
            reasoning.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!(reasoning.len(), full.len() + 1);
        assert_eq!(reasoning[1].role, "reasoning");
        assert_eq!(reasoning[1].text, "thought");
    }

    #[test]
    fn usage_skips_malformed_lines_and_unsupported_summaries() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("messages.jsonl");
        let summary = |execution: &str, summaries: Value| {
            event(
                execution,
                json!({"type":"usage_summary", "executionId":execution,
                    "promptTurnSummaries":summaries}),
            )
        };
        let contents = "invalid\n".to_owned()
            + &summary(
                "good",
                json!([{"unit":"credit","usage":1.5},{"unit":"credits","usage":0.5}]),
            )
            + &summary(
                "tokens",
                json!([{"unit":"credit","usage":1.0},{"unit":"token","usage":9.0}]),
            )
            + &summary("negative", json!([{"unit":"credit","usage":-1.0}]))
            + &json!({"id":"untimed","payload":{"type":"usage_summary",
                "promptTurnSummaries":[{"unit":"credit","usage":4.0}]}})
            .to_string()
            + "\n"
            + &summary("later", json!([{"unit":"credit","usage":3.0}]));
        fs::write(&path, contents).unwrap();
        let parsed = parse_usage_file(&path).unwrap();
        assert!(parsed.cacheable);
        assert_eq!(
            parsed
                .events
                .iter()
                .map(|e| (e.source_record_id.as_deref().unwrap(), e.credits.unwrap()))
                .collect::<Vec<_>>(),
            vec![("good", 2.0), ("later", 3.0)]
        );
    }

    #[test]
    fn discovery_metadata_and_incomplete_lines() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("workspace/session");
        fs::create_dir_all(dir.join("snapshots/a/b")).unwrap();
        let path = dir.join("messages.jsonl");
        fs::write(&path, "").unwrap();
        fs::write(dir.join("snapshots/a/b/messages.jsonl"), "").unwrap();
        fs::create_dir_all(temp.path().join("workspace/empty")).unwrap();
        fs::write(temp.path().join("workspace/empty/session.json"), "{}").unwrap();
        assert_eq!(discover_from_root(temp.path()).len(), 1);
        let ids = AtomicU64::new(1);
        assert_eq!(
            parse_index_records(&path, IndexParseState::default(), false, &ids, |_| Ok(()))
                .unwrap()
                .offset,
            0
        );
        let missing = metadata_fingerprint(&path);
        fs::write(
            dir.join("session.json"),
            r#"{"workspacePaths":[],"rootPaths":["/fallback"],"status":"idle"}"#,
        )
        .unwrap();
        let fingerprint = metadata_fingerprint(&path);
        assert_ne!(missing, fingerprint);
        assert_eq!(session_cwd(&path).as_deref(), Some("/fallback"));
        fs::write(dir.join("session.json"), r#"{"workspacePaths":[],"rootPaths":["/fallback"],"status":"busy","lastModifiedAt":"later"}"#).unwrap();
        assert_eq!(fingerprint, metadata_fingerprint(&path));
        let complete =
            "invalid\n[]\n".to_owned() + &event("u", json!({"type":"user","content":"hello"}));
        let tail = event(
            "a",
            json!({"type":"assistant","operationType":"Say","content":"tail"}),
        );
        fs::write(&path, complete.clone() + tail.trim_end()).unwrap();
        let mut records = Vec::new();
        let parsed = parse_index_records(&path, IndexParseState::default(), false, &ids, |r| {
            records.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!(parsed.offset, complete.len() as u64);
        assert_eq!(parsed.diagnostics.malformed_json_lines, 1);
        assert_eq!(parsed.diagnostics.non_object_json_lines, 1);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].session_id, "session");
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"\n")
            .unwrap();
        let state = IndexParseState {
            offset: parsed.offset,
            turn_id: parsed.turn_id,
            legacy_turn_id: parsed.legacy_turn_id,
            pending_tool_calls: parsed.pending_tool_calls,
        };
        parse_index_records(&path, state, false, &ids, |r| {
            records.push(r);
            Ok(())
        })
        .unwrap();
        assert_eq!(records[1].text, "tail");
    }
}
