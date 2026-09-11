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
    identity: 1,
    // Bumped for subagent* directory matching: forces a full re-parse so
    // plural-dir worker transcripts reclassify on next index.
    index: 2,
    usage: 1,
};

pub fn matches_path(path: &str) -> bool {
    let normalized = path.replace('\\', "/");
    normalized.contains("muse/sessions")
        || normalized.contains(".local/share/muse")
        || (normalized.contains("muse") && normalized.ends_with("session.jsonl"))
}

pub fn sessions_root() -> PathBuf {
    if let Some(root) = std::env::var_os("MUSE_SESSIONS_DIR") {
        return PathBuf::from(root);
    }
    std::env::var_os("MUSE_DATA_DIR")
        .or_else(|| std::env::var_os("MUSE_HOME"))
        .map(|p| PathBuf::from(p).join("sessions"))
        .unwrap_or_else(|| super::common::home().join(".local/share/muse/sessions"))
}

pub fn discover(walk: Option<&mut crate::ingest::directories::StampedWalk>) -> Vec<SourceFile> {
    let root = sessions_root();
    if !root.exists() {
        return Vec::new();
    }
    let mut files = super::common::files_under(&root, walk)
        .into_iter()
        .filter(|path| path.file_name().and_then(|n| n.to_str()) == Some("session.jsonl"))
        .map(|path| SourceFile {
            source: SourceKind::Muse,
            path,
        })
        .collect::<Vec<_>>();
    files.sort_by(|a, b| a.path.cmp(&b.path));
    files
}

pub fn session_id_from_path(path: &Path) -> String {
    path.parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string()
}

pub fn cwd_from_muse_session(path: &Path) -> Option<PathBuf> {
    let Ok(file) = File::open(path) else {
        return None;
    };
    let Ok(mmap) = (unsafe { Mmap::map(&file) }) else {
        return None;
    };
    let mut start = 0;
    let mut buf = Vec::new();
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
        let Ok(value) = simd_json::to_borrowed_value(&mut buf) else {
            continue;
        };
        if let Some(payload) = value.get("payload")
            && let Some(record) = payload.get("record")
        {
            if let Some(cwd) = record.get("cwd").and_then(|v| v.as_str())
                && !cwd.is_empty()
            {
                return Some(PathBuf::from(cwd));
            }
            if let Some(root) = record.get("workspace_root").and_then(|v| v.as_str())
                && !root.is_empty()
            {
                return Some(PathBuf::from(root));
            }
        }
        if start > 64 * 1024 {
            break;
        }
    }
    None
}

fn timestamp_millis(value: &BorrowedValue<'_>) -> u64 {
    value
        .as_u64()
        .map(|number| {
            if number > 1_000_000_000_000_000 {
                number / 1000
            } else if number < 10_000_000_000 {
                number.saturating_mul(1000)
            } else {
                number
            }
        })
        .or_else(|| {
            value
                .as_i64()
                .filter(|value| *value >= 0)
                .map(|value| (value as u64) / 1000)
        })
        .or_else(|| value.as_str().and_then(super::common::parse_iso_millis))
        .unwrap_or(0)
}

/// Recover session id, project, and base timestamp from an already-indexed
/// prefix row. Used before resuming at a saved offset so appended-only parses
/// keep the metadata established by the leading rows.
fn recover_muse_header(
    value: &BorrowedValue<'_>,
    session_id: &mut String,
    project: &mut String,
    default_timestamp: &mut u64,
) {
    if let Some(stream) = value.get("stream")
        && let Some(sid) = stream.get("id").and_then(|v| v.as_str())
        && !sid.is_empty()
    {
        *session_id = sid.to_string();
    }
    let recorded_at = value.get("recorded_at").map(timestamp_millis).unwrap_or(0);
    if recorded_at > 0 && *default_timestamp == 0 {
        *default_timestamp = recorded_at;
    }
    let Some(payload) = value.get("payload") else {
        return;
    };
    let kind = payload.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    if kind != "metadata" && kind != "route_facts" {
        return;
    }
    if let Some(record) = payload.get("record") {
        if let Some(root) = record.get("workspace_root").and_then(|v| v.as_str())
            && !root.is_empty()
        {
            *project = super::common::project_from_path(root);
        } else if let Some(cwd) = record.get("cwd").and_then(|v| v.as_str())
            && !cwd.is_empty()
        {
            *project = super::common::project_from_path(cwd);
        }
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
    let mut start = super::jsonl::resume_offset(&mmap, state.offset, |line| {
        simd_json::to_borrowed_value(&mut line.to_vec()).is_ok()
    });
    let mut turn_id = state.turn_id;
    let mut pending_tool_calls = state.pending_tool_calls;
    let mut diagnostics = ParseDiagnostics::default();

    let source_path = path.to_string_lossy().to_string();
    let normalized_path = source_path.replace('\\', "/");
    // Muse writes worker transcripts under `<session>/subagent*/…` (plural is
    // also seen); match on path components, not substrings, so a session id
    // that merely contains the word never misfires.
    let is_subagent = Path::new(&normalized_path)
        .components()
        .any(|c| c.as_os_str().to_string_lossy().starts_with("subagent"));

    let mut session_id = session_id_from_path(path);
    let mut parent_session_id = None;
    if is_subagent
        && let Some(parent_dir) = path
            .parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.parent())
        && let Some(pid) = parent_dir.file_name().and_then(|n| n.to_str())
    {
        parent_session_id = Some(pid.to_string());
    }

    let conversation_kind = if is_subagent { "subagent" } else { "main" };
    let mut project = SourceKind::Muse.label().to_string();
    let mut default_timestamp = 0;

    let mut buf = Vec::new();
    // On incremental appends parsing resumes at `state.offset`, skipping the
    // leading metadata/route-facts rows that carry workspace_root/cwd. Recover
    // them (plus session id and base timestamp) from the already-indexed prefix
    // first, mirroring the Pi session-header recovery.
    if start > 0 && !mmap.is_empty() {
        let prefix_end = start.min(mmap.len());
        let mut scan = 0;
        while scan < prefix_end {
            let slice = &mmap[scan..prefix_end];
            let rel = memchr(b'\n', slice).unwrap_or(slice.len());
            let line = &slice[..rel];
            scan += rel + usize::from(rel < slice.len());
            if line.is_empty() {
                continue;
            }
            buf.clear();
            buf.extend_from_slice(line);
            let Ok(value) = simd_json::to_borrowed_value(&mut buf) else {
                continue;
            };
            recover_muse_header(
                &value,
                &mut session_id,
                &mut project,
                &mut default_timestamp,
            );
        }
        buf.clear();
    }
    while start < mmap.len() {
        let line_start = start;
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
                if rel == slice.len() {
                    start = line_start;
                    break;
                }

                diagnostics.malformed_json_lines += 1;
                continue;
            }
        };

        if let Some(stream) = value.get("stream")
            && let Some(sid) = stream.get("id").and_then(|v| v.as_str())
            && !sid.is_empty()
        {
            session_id = sid.to_string();
        }

        let recorded_at = value.get("recorded_at").map(timestamp_millis).unwrap_or(0);
        if recorded_at > 0 && default_timestamp == 0 {
            default_timestamp = recorded_at;
        }

        let payload = match value.get("payload") {
            Some(p) => p,
            None => continue,
        };
        let p_kind = payload.get("kind").and_then(|v| v.as_str()).unwrap_or("");

        if p_kind == "metadata" || p_kind == "route_facts" {
            if let Some(record) = payload.get("record") {
                if let Some(root) = record.get("workspace_root").and_then(|v| v.as_str()) {
                    if !root.is_empty() {
                        project = super::common::project_from_path(root);
                    }
                } else if let Some(cwd) = record.get("cwd").and_then(|v| v.as_str())
                    && !cwd.is_empty()
                {
                    project = super::common::project_from_path(cwd);
                }
            }
            continue;
        }

        let event = match payload.get("event") {
            Some(e) => e,
            None => continue,
        };
        let ekind = event.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let timestamp = if recorded_at > 0 {
            recorded_at
        } else {
            default_timestamp
        };

        let base_links = RecordLinks {
            parent_session_id: parent_session_id.clone(),
            conversation_kind: Some(conversation_kind.to_string()),
            thread_source: (conversation_kind != "main").then(|| conversation_kind.to_string()),
            ..RecordLinks::default()
        };

        match ekind {
            "started" => {
                if let Some(prompt) = event.get("prompt").and_then(|v| v.as_str()) {
                    let text = prompt.trim().to_string();
                    if !text.is_empty() {
                        emit(Record {
                            source: SourceKind::Muse,
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
                            links: base_links,
                            source_path: source_path.clone(),
                        })?;
                        turn_id += 1;
                    }
                }
            }
            "assistant_message_committed" => {
                if let Some(text) = event.get("text").and_then(|v| v.as_str()) {
                    let text = text.trim().to_string();
                    if !text.is_empty() {
                        let mut links = base_links;
                        if let Some(msg_id) = event.get("message_id").and_then(|v| v.as_str()) {
                            links.event_id = Some(msg_id.to_string());
                        }
                        emit(Record {
                            source: SourceKind::Muse,
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
                        })?;
                        turn_id += 1;
                    }
                }
            }
            "assistant_tool_calls_committed" => {
                let msg_id = event
                    .get("message_id")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                if let Some(tool_calls) = event.get("tool_calls").and_then(|v| v.as_array()) {
                    for call in tool_calls {
                        let tool_name = call
                            .get("name")
                            .and_then(|v| v.as_str())
                            .map(str::to_string);
                        let tool_call_id = call
                            .get("call_id")
                            .or_else(|| call.get("id"))
                            .and_then(|v| v.as_str())
                            .map(str::to_string);
                        let tool_input = call.get("args").map(|v| {
                            if let Some(s) = v.as_str() {
                                s.to_string()
                            } else {
                                v.to_string()
                            }
                        });

                        let mut links = base_links.clone();
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
                            source: SourceKind::Muse,
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
                }
            }
            "tool_result_batch_committed" => {
                if let Some(results) = event.get("results").and_then(|v| v.as_array()) {
                    for res in results {
                        let tool_call_id = res
                            .get("tool_call_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let tool_name = if !tool_call_id.is_empty() {
                            pending_tool_calls
                                .remove(tool_call_id)
                                .and_then(|call| call.tool_name)
                        } else {
                            None
                        };
                        let output = res
                            .get("text")
                            .or_else(|| res.get("output"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        if !output.trim().is_empty() {
                            let mut links = base_links.clone();
                            if !tool_call_id.is_empty() {
                                links.parent_tool_use_id = Some(tool_call_id.to_string());
                            }
                            emit(Record {
                                source: SourceKind::Muse,
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
                    }
                }
            }
            "reasoning_committed" => {
                if include_reasoning
                    && let Some(text) = event
                        .get("text")
                        .or_else(|| event.get("reasoning"))
                        .and_then(|v| v.as_str())
                {
                    let text = text.trim().to_string();
                    if !text.is_empty() {
                        let mut links = base_links;
                        if let Some(msg_id) = event.get("message_id").and_then(|v| v.as_str()) {
                            links.event_id = Some(format!("{msg_id}:reasoning"));
                        }
                        emit(Record {
                            source: SourceKind::Muse,
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
                            links,
                            source_path: source_path.clone(),
                        })?;
                        turn_id += 1;
                    }
                }
            }
            _ => {}
        }
    }

    Ok(IndexParseOutput {
        legacy_turn_id: None,
        offset: start as u64,
        turn_id,
        pending_tool_calls,
        session_id: Some(session_id),
        diagnostics,
    })
}

pub fn usage_files() -> Vec<PathBuf> {
    discover(None).into_iter().map(|f| f.path).collect()
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<Vec<UsageEvent>> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let mut session_id = session_id_from_path(path);
    let mut project = SourceKind::Muse.label().to_string();
    let mut current_model = None;
    let mut current_provider = None;

    let mut start = 0;
    let mut buf = Vec::new();
    let mut events = Vec::new();

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
        let Ok(value) = simd_json::to_borrowed_value(&mut buf) else {
            continue;
        };

        if let Some(stream) = value.get("stream")
            && let Some(sid) = stream.get("id").and_then(|v| v.as_str())
            && !sid.is_empty()
        {
            session_id = sid.to_string();
        }

        let recorded_at = value.get("recorded_at").map(timestamp_millis).unwrap_or(0);
        let payload = match value.get("payload") {
            Some(p) => p,
            None => continue,
        };

        if let Some(record) = payload.get("record") {
            if let Some(root) = record.get("workspace_root").and_then(|v| v.as_str())
                && !root.is_empty()
            {
                project = super::common::project_from_path(root);
            }
            if let Some(model) = record.get("model_id").and_then(|v| v.as_str()) {
                current_model = Some(model.to_string());
            }
            if let Some(provider) = record.get("provider_id").and_then(|v| v.as_str()) {
                current_provider = Some(provider.to_string());
            }
        }

        let event = match payload.get("event") {
            Some(e) => e,
            None => continue,
        };
        let ekind = event.get("kind").and_then(|v| v.as_str()).unwrap_or("");

        if ekind == "model_completed" {
            let usage = match event.get("usage") {
                Some(u) => u,
                None => continue,
            };
            let number = |key: &str| usage.get(key).and_then(|v| v.as_u64()).unwrap_or(0);
            let input = number("input_tokens").max(number("input"));
            let output = number("output_tokens").max(number("output"));
            let cache_read = number("cache_read_tokens").max(number("cached_tokens"));
            let cache_write = number("cache_write_tokens");
            let reasoning = number("reasoning_tokens");

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

            let model = event
                .get("model")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .or_else(|| current_model.clone());

            events.push(UsageEvent {
                source: "muse",
                source_path: source_path.clone(),
                source_record_id: None,
                session_id: Some(session_id.clone()),
                request_id: None,
                message_id: None,
                timestamp_ms: recorded_at,
                project: Some(project.clone()),
                provider: current_provider.clone(),
                model,
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
    fn parses_muse_session_record_and_usage() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        std::fs::write(
            &path,
            concat!(
                r#"{"stream":{"kind":"session","id":"01a05f7c-3b49-77a1"},"recorded_at":1788308372342264,"payload":{"kind":"metadata","record":{"workspace_root":"/repo/memex","provider_id":"meta","model_id":"muse-spark-1.2"}}}"#, "\n",
                r#"{"stream":{"kind":"session","id":"01a05f7c-3b49-77a1"},"recorded_at":1788308372900000,"payload":{"kind":"run","event":{"kind":"started","prompt":"Hello Muse"}}}"#, "\n",
                r#"{"stream":{"kind":"session","id":"01a05f7c-3b49-77a1"},"recorded_at":1788308373000000,"payload":{"kind":"run","event":{"kind":"reasoning_committed","message_id":"m1","text":"Let me think"}}}"#, "\n",
                r#"{"stream":{"kind":"session","id":"01a05f7c-3b49-77a1"},"recorded_at":1788308373100000,"payload":{"kind":"run","event":{"kind":"assistant_tool_calls_committed","message_id":"m1","tool_calls":[{"call_id":"call1","name":"bash","args":"{\"command\":\"pwd\"}"}]}}}"#, "\n",
                r#"{"stream":{"kind":"session","id":"01a05f7c-3b49-77a1"},"recorded_at":1788308373200000,"payload":{"kind":"run","event":{"kind":"tool_result_batch_committed","results":[{"tool_call_id":"call1","text":"/repo/memex"}]}}}"#, "\n",
                r#"{"stream":{"kind":"session","id":"01a05f7c-3b49-77a1"},"recorded_at":1788308373300000,"payload":{"kind":"run","event":{"kind":"model_completed","model":"muse-spark-1.2","usage":{"input_tokens":100,"output_tokens":20,"cached_tokens":10,"reasoning_tokens":5}}}}"#, "\n",
                r#"{"stream":{"kind":"session","id":"01a05f7c-3b49-77a1"},"recorded_at":1788308373400000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","message_id":"m2","text":"I checked the current directory."}}}"#, "\n"
            ),
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

        assert_eq!(out.session_id.as_deref(), Some("01a05f7c-3b49-77a1"));
        assert_eq!(records.len(), 5); // user started, reasoning, tool_use, tool_result, assistant text
        assert_eq!(records[0].role, "user");
        assert_eq!(records[0].text, "Hello Muse");
        assert_eq!(records[0].project, "memex");
        assert_eq!(records[1].role, "reasoning");
        assert_eq!(records[1].text, "Let me think");
        assert_eq!(records[2].role, "tool_use");
        assert_eq!(records[2].tool_name.as_deref(), Some("bash"));
        assert_eq!(records[3].role, "tool_result");
        assert_eq!(records[3].tool_name.as_deref(), Some("bash"));
        assert_eq!(records[4].role, "assistant");
        assert_eq!(records[4].text, "I checked the current directory.");

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tokens.uncached_input, 100);
        assert_eq!(events[0].tokens.output, 25);
        assert_eq!(events[0].tokens.cache_read, 10);
        assert_eq!(events[0].tokens.reasoning, 5);
        assert_eq!(events[0].model.as_deref(), Some("muse-spark-1.2"));
        assert_eq!(events[0].provider.as_deref(), Some("meta"));
    }

    #[test]
    fn appended_rows_keep_metadata_project() {
        use std::io::Write;

        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        std::fs::write(
            &path,
            concat!(
                r#"{"stream":{"kind":"session","id":"sess-1"},"recorded_at":1788308372342264,"payload":{"kind":"metadata","record":{"workspace_root":"/Users/joe/Developer/memex"}}}"#, "\n",
                r#"{"stream":{"kind":"session","id":"sess-1"},"recorded_at":1788308372900000,"payload":{"kind":"run","event":{"kind":"started","prompt":"Hello"}}}"#, "\n",
            ),
        )
        .unwrap();

        let next_doc_id = AtomicU64::new(1);
        let mut first_records = Vec::new();
        let out = parse_index_records(
            &path,
            IndexParseState::default(),
            false,
            &next_doc_id,
            |rec| {
                first_records.push(rec);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(first_records.len(), 1);
        assert_eq!(first_records[0].project, "memex");

        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(
                br#"{"stream":{"kind":"session","id":"sess-1"},"recorded_at":1788308373400000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","message_id":"m9","text":"Follow-up"}}}"#,
            )
            .unwrap();
        // Newline-terminate the appended row so the resumed parse sees a full line.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"\n")
            .unwrap();

        let mut resumed = Vec::new();
        let resumed_out = parse_index_records(
            &path,
            IndexParseState {
                offset: out.offset,
                turn_id: out.turn_id,
                ..IndexParseState::default()
            },
            false,
            &next_doc_id,
            |rec| {
                resumed.push(rec);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].project, "memex");
        assert_eq!(resumed[0].session_id, "sess-1");
        assert_eq!(resumed_out.turn_id, out.turn_id + 1);
    }
}
