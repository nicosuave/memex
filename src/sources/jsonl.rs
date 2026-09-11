pub(super) fn resume_offset(
    bytes: &[u8],
    offset: u64,
    valid_json: impl FnOnce(&[u8]) -> bool,
) -> usize {
    let offset = usize::try_from(offset)
        .unwrap_or(bytes.len())
        .min(bytes.len());
    if offset == 0 || bytes[offset - 1] == b'\n' {
        return offset;
    }
    let line_start = memchr::memrchr(b'\n', &bytes[..offset]).map_or(0, |index| index + 1);
    if valid_json(&bytes[line_start..offset]) {
        offset
    } else {
        line_start
    }
}

#[cfg(test)]
mod tests {
    use crate::sources::{self, IndexParseOutput, IndexParseState};
    use crate::types::Record;
    use std::collections::HashSet;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::Path;
    use std::sync::atomic::AtomicU64;

    // Claude is absent on purpose: its parser stops at the byte boundary discovery
    // captured and retries a tail only when the JSON error is truncation, so that a late
    // `sessionKind: "bg"` marker can still reclassify the whole transcript. Its own tests
    // cover that contract.
    const SOURCES: [&str; 9] = [
        "codex",
        "codex_history",
        "pi",
        "omp",
        "openclaw",
        "muse",
        "grok",
        "copilot",
        "cursor",
    ];

    fn message(source: &str, text: &str) -> String {
        let text = serde_json::to_string(text).unwrap();
        match source {
            "claude" => {
                format!(r#"{{"type":"user","message":{{"role":"user","content":{text}}}}}"#)
            }
            "codex" => format!(
                r#"{{"type":"response_item","payload":{{"type":"message","role":"user","content":{text}}}}}"#
            ),
            "codex_history" => format!(r#"{{"session_id":"session","ts":1,"text":{text}}}"#),
            "pi" | "omp" | "openclaw" => {
                format!(r#"{{"type":"message","message":{{"role":"user","content":{text}}}}}"#)
            }
            "muse" => format!(
                r#"{{"payload":{{"kind":"run","event":{{"kind":"started","prompt":{text}}}}}}}"#
            ),
            "grok" => format!(
                r#"{{"params":{{"sessionId":"session","update":{{"sessionUpdate":"user_message_chunk","content":{{"type":"text","text":{text}}}}}}}}}"#
            ),
            "copilot" => format!(r#"{{"type":"user.message","data":{{"content":{text}}}}}"#),
            "cursor" => format!(r#"{{"role":"user","message":{{"content":{text}}}}}"#),
            _ => unreachable!(),
        }
    }

    fn parse(
        source: &str,
        path: &Path,
        state: IndexParseState,
        records: &mut Vec<Record>,
    ) -> IndexParseOutput {
        let ids = AtomicU64::new(records.len() as u64 + 1);
        let emit = |record| {
            records.push(record);
            Ok(())
        };
        match source {
            "claude" => sources::claude::parse_index_records(path, state, false, &ids, emit),
            "codex" => sources::codex::parse_index_records(path, state, false, &ids, emit),
            "codex_history" => {
                sources::codex::parse_history_records(path, state, &HashSet::new(), &ids, emit)
            }
            "pi" => sources::pi::parse_index_records(path, state, false, &ids, emit),
            "omp" => sources::omp::parse_index_records(path, state, false, &ids, emit),
            "openclaw" => sources::openclaw::parse_index_records(path, state, false, &ids, emit),
            "muse" => sources::muse::parse_index_records(path, state, false, &ids, emit),
            "grok" => sources::grok::parse_index_records(path, state, false, &ids, emit),
            "copilot" => sources::copilot::parse_index_records(path, state, &ids, emit),
            "cursor" => sources::cursor::parse_index_records(path, 1, state, &ids, emit),
            _ => unreachable!(),
        }
        .unwrap()
    }

    fn state(output: &IndexParseOutput) -> IndexParseState {
        IndexParseState {
            offset: output.offset,
            turn_id: output.turn_id,
            legacy_turn_id: output.legacy_turn_id,
            pending_tool_calls: output.pending_tool_calls.clone(),
        }
    }

    fn tool_pair(source: &str) -> (&'static str, &'static str) {
        match source {
            "claude" => (
                r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"call","name":"shell","input":{"cmd":"pwd"}}]}}"#,
                r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"call","content":"done"}]}}"#,
            ),
            "codex" => (
                r#"{"type":"response_item","payload":{"type":"function_call","call_id":"call","name":"shell","arguments":"pwd"}}"#,
                r#"{"type":"response_item","payload":{"type":"function_call_output","call_id":"call","output":"done"}}"#,
            ),
            "pi" | "omp" | "openclaw" => (
                r#"{"type":"message","message":{"role":"assistant","content":[{"type":"toolCall","id":"call","name":"shell","arguments":{"cmd":"pwd"}}]}}"#,
                r#"{"type":"message","message":{"role":"toolResult","toolCallId":"call","content":[{"type":"text","text":"done"}]}}"#,
            ),
            "muse" => (
                r#"{"payload":{"kind":"run","event":{"kind":"assistant_tool_calls_committed","tool_calls":[{"call_id":"call","name":"shell","args":"pwd"}]}}}"#,
                r#"{"payload":{"kind":"run","event":{"kind":"tool_result_batch_committed","results":[{"tool_call_id":"call","text":"done"}]}}}"#,
            ),
            "grok" => (
                r#"{"params":{"update":{"sessionUpdate":"tool_call","toolCallId":"call","rawInput":{"cmd":"pwd"},"_meta":{"x.ai/tool":{"name":"shell"}}}}}"#,
                r#"{"params":{"update":{"sessionUpdate":"tool_call_update","toolCallId":"call","status":"completed","content":[{"type":"content","content":{"type":"text","text":"done"}}]}}}"#,
            ),
            "copilot" => (
                r#"{"type":"tool.execution_start","data":{"toolCallId":"call","toolName":"shell","arguments":{"cmd":"pwd"}}}"#,
                r#"{"type":"tool.execution_complete","data":{"toolCallId":"call","success":true,"result":{"content":"done"}}}"#,
            ),
            "cursor" => (
                r#"{"role":"assistant","message":{"content":[{"type":"tool_use","id":"call","name":"shell","input":{"cmd":"pwd"}}]}}"#,
                r#"{"role":"user","message":{"content":[{"type":"tool_result","tool_use_id":"call","content":"done"}]}}"#,
            ),
            _ => unreachable!(),
        }
    }

    #[test]
    fn torn_tool_records_preserve_pending_state_and_valid_eof_calls() {
        for source in SOURCES
            .into_iter()
            .filter(|source| *source != "codex_history")
        {
            for legacy_checkpoint in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let path = temp.path().join("session.jsonl");
                let (call, result) = tool_pair(source);
                let mut writer = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .unwrap();
                writer
                    .write_all(&call.as_bytes()[..call.len() - 1])
                    .unwrap();
                let mut records = Vec::new();
                let torn_call = parse(source, &path, IndexParseState::default(), &mut records);
                assert!(records.is_empty(), "{source}");
                assert!(torn_call.pending_tool_calls.is_empty(), "{source}");
                assert_eq!(torn_call.offset, 0, "{source}");
                writer.write_all(b"}").unwrap();
                let accepted_call = parse(source, &path, state(&torn_call), &mut records);
                assert_eq!(records.len(), 1, "{source}");
                assert_eq!(accepted_call.offset, call.len() as u64, "{source}");
                assert_eq!(accepted_call.pending_tool_calls.len(), 1, "{source}");
                assert_eq!(
                    accepted_call.pending_tool_calls["call"]
                        .tool_name
                        .as_deref(),
                    Some("shell"),
                    "{source}"
                );
                writer.write_all(b"\n").unwrap();
                writer
                    .write_all(&result.as_bytes()[..result.len() - 2])
                    .unwrap();
                let torn_result = parse(source, &path, state(&accepted_call), &mut records);
                assert_eq!(records.len(), 1, "{source}: valid EOF call must not replay");
                assert_eq!(torn_result.offset, (call.len() + 1) as u64, "{source}");
                assert_eq!(torn_result.turn_id, 1, "{source}");
                assert_eq!(
                    serde_json::to_value(&torn_result.pending_tool_calls).unwrap(),
                    serde_json::to_value(&accepted_call.pending_tool_calls).unwrap(),
                    "{source}"
                );
                let mut checkpoint = state(&torn_result);
                if legacy_checkpoint {
                    checkpoint.offset = path.metadata().unwrap().len();
                }
                writer
                    .write_all(&result.as_bytes()[result.len() - 2..])
                    .unwrap();
                let completed = parse(source, &path, checkpoint, &mut records);
                assert_eq!(records.len(), 2, "{source}, legacy={legacy_checkpoint}");
                assert_eq!(records[1].role, "tool_result", "{source}");
                assert_eq!(records[1].tool_name.as_deref(), Some("shell"), "{source}");
                assert_eq!(
                    records[1].links.parent_tool_use_id.as_deref(),
                    Some("call"),
                    "{source}"
                );
                assert_eq!(completed.turn_id, 2, "{source}");
                assert!(completed.pending_tool_calls.is_empty(), "{source}");
                parse(source, &path, state(&completed), &mut records);
                assert_eq!(records.len(), 2, "{source}");
            }
        }
    }

    #[test]
    fn codex_torn_metadata_preserves_offsets_and_pending_tools() {
        for cached in [false, true] {
            for legacy_checkpoint in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let path = temp.path().join("session.jsonl");
                let header = "{\"type\":\"session_meta\",\"payload\":{\"id\":\"session\",\"cwd\":\"/work/old\"}}\n";
                let next_header =
                    r#"{"type":"session_meta","payload":{"id":"session","cwd":"/work/new"}}"#;
                let (call, result) = tool_pair("codex");
                let prefix = format!("{header}{call}\n");
                let split = next_header.len() - 2;
                std::fs::write(&path, format!("{prefix}{}", &next_header[..split])).unwrap();
                let ids = AtomicU64::new(1);
                let mut records = Vec::new();
                let (first, offsets) = sources::codex::parse_index_records_with_metadata_offsets(
                    &path,
                    IndexParseState::default(),
                    false,
                    &ids,
                    None,
                    |record| {
                        records.push(record);
                        Ok(())
                    },
                )
                .unwrap();
                assert_eq!(first.offset, prefix.len() as u64);
                assert_eq!(offsets, [0]);
                assert_eq!(first.pending_tool_calls.len(), 1);
                let mut checkpoint = state(&first);
                if legacy_checkpoint {
                    checkpoint.offset = path.metadata().unwrap().len();
                }
                let mut writer = OpenOptions::new().append(true).open(&path).unwrap();
                writer.write_all(&next_header.as_bytes()[split..]).unwrap();
                let (second, updated_offsets) =
                    sources::codex::parse_index_records_with_metadata_offsets(
                        &path,
                        checkpoint,
                        false,
                        &ids,
                        cached.then_some(offsets.as_slice()),
                        |record| {
                            records.push(record);
                            Ok(())
                        },
                    )
                    .unwrap();
                assert_eq!(updated_offsets, [0, prefix.len() as u64]);
                assert_eq!(second.offset, (prefix.len() + next_header.len()) as u64);
                assert_eq!(second.pending_tool_calls.len(), 1);
                assert_eq!(records.len(), 1);
                writer
                    .write_all(format!("\n{result}\n").as_bytes())
                    .unwrap();
                let (third, final_offsets) =
                    sources::codex::parse_index_records_with_metadata_offsets(
                        &path,
                        state(&second),
                        false,
                        &ids,
                        cached.then_some(updated_offsets.as_slice()),
                        |record| {
                            records.push(record);
                            Ok(())
                        },
                    )
                    .unwrap();
                assert_eq!(final_offsets, updated_offsets);
                assert_eq!(records.len(), 2);
                assert_eq!(records[1].project, "new");
                assert_eq!(records[1].session_id, "session");
                assert_eq!(records[1].tool_name.as_deref(), Some("shell"));
                assert_eq!(third.turn_id, 2);
                assert!(third.pending_tool_calls.is_empty());
            }
        }
    }

    #[test]
    fn legacy_midrecord_checkpoint_recovers_without_replaying_prefix() {
        for source in SOURCES {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("session.jsonl");
            let prefix = format!("{}\n", message(source, "prefix"));
            let tail = message(source, "completed café");
            let split = tail.find('é').unwrap() + 1;
            std::fs::write(&path, format!("{prefix}{tail}\n")).unwrap();
            let mut records = Vec::new();
            let checkpoint = IndexParseState {
                offset: (prefix.len() + split) as u64,
                turn_id: 1,
                legacy_turn_id: Some(1),
                ..Default::default()
            };
            let output = parse(source, &path, checkpoint, &mut records);
            assert_eq!(
                records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
                ["completed café"],
                "{source}"
            );
            assert_eq!(records[0].turn_id, 1, "{source}");
            assert_eq!(output.turn_id, 2, "{source}");
            parse(source, &path, state(&output), &mut records);
            assert_eq!(records.len(), 1, "{source}");
        }
    }

    #[test]
    fn invalid_unterminated_json_waits_for_newline_before_skipping() {
        for source in SOURCES {
            for malformed in ["{broken}", "   ", "{\"text\":\"bad\\x\"}"] {
                let temp = tempfile::tempdir().unwrap();
                let path = temp.path().join("session.jsonl");
                let prefix = format!("{}\n", message(source, "prefix"));
                std::fs::write(&path, format!("{prefix}{malformed}")).unwrap();
                let mut records = Vec::new();
                let first = parse(source, &path, IndexParseState::default(), &mut records);
                assert_eq!(first.offset, prefix.len() as u64, "{source}: {malformed}");
                let mut writer = OpenOptions::new().append(true).open(&path).unwrap();
                writer
                    .write_all(format!("\n{}\n", message(source, "next")).as_bytes())
                    .unwrap();
                let output = parse(source, &path, state(&first), &mut records);
                assert_eq!(output.offset, path.metadata().unwrap().len(), "{source}");
                assert_eq!(
                    records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
                    ["prefix", "next"],
                    "{source}"
                );
            }
        }
    }

    #[test]
    fn torn_eof_completion_preserves_records_source_matrix() {
        for source in SOURCES {
            for partial_utf8 in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let path = temp.path().join("session.jsonl");
                let prefix = format!("{}\n", message(source, "prefix"));
                let tail = message(source, "completed café");
                let split = if partial_utf8 {
                    tail.find('é').unwrap() + 1
                } else {
                    tail.len() - 2
                };
                let mut writer = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .unwrap();
                writer.write_all(prefix.as_bytes()).unwrap();
                writer.write_all(&tail.as_bytes()[..split]).unwrap();
                writer.flush().unwrap();
                let mut records = Vec::new();
                let first = parse(source, &path, IndexParseState::default(), &mut records);
                assert_eq!(
                    records.len(),
                    1,
                    "{source}: completed prefix must be emitted while writer remains open"
                );
                let retry = parse(source, &path, state(&first), &mut records);
                assert_eq!(
                    records.len(),
                    1,
                    "{source}: unchanged tail duplicated prefix"
                );
                writer.write_all(&tail.as_bytes()[split..]).unwrap();
                writer.flush().unwrap();
                let completed = parse(source, &path, state(&retry), &mut records);
                assert_eq!(
                    records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
                    ["prefix", "completed café"],
                    "{source}, partial_utf8={partial_utf8}, first_offset={}, prefix_bytes={}",
                    first.offset,
                    prefix.len()
                );
                assert_eq!(first.offset, prefix.len() as u64, "{source}");
                assert_eq!(retry.offset, first.offset, "{source}");
                assert_eq!(
                    completed.offset,
                    (prefix.len() + tail.len()) as u64,
                    "{source}: valid final JSON needs no newline"
                );
                writer
                    .write_all(format!("\n{}\n", message(source, "next")).as_bytes())
                    .unwrap();
                writer.flush().unwrap();
                let appended = parse(source, &path, state(&completed), &mut records);
                let unchanged = parse(source, &path, state(&appended), &mut records);
                assert_eq!(
                    records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
                    ["prefix", "completed café", "next"],
                    "{source}"
                );
                assert_eq!(unchanged.offset, appended.offset, "{source}");
                assert_eq!(unchanged.turn_id, 3, "{source}");
            }
        }
    }
}
