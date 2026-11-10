//! Synthetic CLI contract fixture for end-to-end Qt tests. Never reads user data.
use serde_json::{Value, json};

fn option(args: &[String], name: &str) -> Option<String> {
    args.iter().enumerate().find_map(|(index, arg)| {
        arg.strip_prefix(&format!("--{name}="))
            .map(str::to_owned)
            .or_else(|| {
                (arg == &format!("--{name}"))
                    .then(|| args.get(index + 1).cloned())
                    .flatten()
            })
    })
}

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let op = args
        .iter()
        .find(|s| {
            matches!(
                s.as_str(),
                "machines" | "projects" | "sessions" | "search" | "session" | "activity"
            )
        })
        .map(String::as_str)
        .unwrap_or("");
    let machine = option(&args, "machine").unwrap_or_else(|| "local".into());
    let root = std::env::var("MEMEX_ROOT").expect("isolated fixture root");
    let source_path = format!("{root}/conversation.jsonl");
    let session = |id: &str, label: &str, kind: &str| {
        json!({
            "source": "codex", "session_id": id, "source_path": source_path,
            "project": "/synthetic/project", "label": label, "machine": machine,
            "last_at": "2026-09-13T10:00:00Z", "message_count": 130,
            "conversation_kind": kind, "resume_cmd": "codex resume fixture", "cwd": root
        })
    };
    let result: Value = match op {
        "machines" => {
            json!([{"id":"local","label":"This computer"},{"id":"fixture-peer","label":"Fixture peer"}])
        }
        "projects" => {
            // Project summaries arrive independently of session lists. Keep the
            // peer slow so UI tests exercise partially aggregated results.
            if machine == "fixture-peer" {
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
            json!([{"project":"/synthetic/project","session_count":4,"last_at":"2026-09-13T10:00:00Z"}])
        }
        "sessions" if args.iter().any(|s| s == "--count") => {
            json!({"total":if option(&args,"origin").as_deref()==Some("subagent") {1} else {2}})
        }
        "sessions" => {
            if let Some(id) = option(&args, "session-id") {
                json!([session(
                    &id,
                    if id == "first" {
                        "Qt parity fixture"
                    } else {
                        "Second conversation"
                    },
                    "main"
                )])
            } else if option(&args, "origin").as_deref() == Some("subagent") {
                json!([session("child", "Subagent fixture", "subagent")])
            } else {
                json!([
                    session("first", "Qt parity fixture", "main"),
                    session("second", "Second conversation", "main")
                ])
            }
        }
        "search" => {
            let query = args.last().map(String::as_str).unwrap_or("");
            if query.contains("missing") {
                json!([])
            } else {
                json!([{"source":"codex","session_id":"first","source_path":source_path,"project":"/synthetic/project","machine":machine,"record_id":"r12","snippet":"needle repeated needle","ts":"2026-09-13T10:00:00Z","conversation_kind":"main"}])
            }
        }
        "session" => {
            let offset: usize = option(&args, "offset")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let limit: usize = option(&args, "limit")
                .and_then(|s| s.parse().ok())
                .unwrap_or(60);
            let mut rows: Vec<_> = (offset..130.min(offset + limit)).map(record).collect();
            rows.push(json!({"type":"page","total":130,"next_offset":if offset+limit<130 {Some(offset+limit)} else {None}}));
            json!(rows)
        }
        "activity" => {
            let points: Vec<_> = (0..28u64)
                .flat_map(|day| {
                    ["codex", "claude"].into_iter().enumerate().map(move |(index, source)| {
                    json!({"timestamp_ms": 1786924800000u64 + day * 86400000, "source": source,
                           "value": (day * 7 + index as u64 * 3) % 6 + 1})
                })
                })
                .collect();
            json!({"token_usage_enabled":true,"partial":false,"points":points})
        }
        _ => {
            eprintln!("Unknown fixture operation: {op}");
            std::process::exit(2)
        }
    };
    println!("{result}");
}

fn record(index: usize) -> Value {
    let message = match index {
        12 => json!({"role": "user", "text": "Earlier needle repeated needle"}),
        121 => {
            json!({"role": "tool_use", "tool_name": "functions.exec_command", "event_id": "call-1", "tool_input": "{\"cmd\":\"cargo test\"}", "text": ""})
        }
        122 => {
            json!({"role": "tool_result", "parent_tool_use_id": "call-1", "tool_output": "{\"exit_code\":0,\"output\":\"All tests passed\"}", "text": ""})
        }
        125 => {
            json!({"role": "user", "text": "<environment_context>\nSynthetic environment\n</environment_context>\nExplain the Qt implementation."})
        }
        126 => {
            json!({"role": "assistant", "text": "## Native Qt Quick\nRust owns the application state.\n\n```rust\nfn main() {\n    memex_qt::run();\n}\n```\n\n[Open the source](example.rs:2)"})
        }
        129 => json!({"role": "assistant", "text": "Latest answer with needle"}),
        _ => {
            json!({"role": if index.is_multiple_of(2) { "user" } else { "assistant" }, "text": format!("Message {index}")})
        }
    };
    json!({"record_id": format!("r{index}"), "record": message, "unknown_fixture_field": "preserved"})
}
