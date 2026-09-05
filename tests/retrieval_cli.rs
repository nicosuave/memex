use memex::config::Paths;
use memex::index::SearchIndex;
use memex::retrieval::canonical_record_id;
use memex::types::{Record, RecordLinks, SourceKind};
use serde_json::Value;
use std::path::Path;
use std::process::{Command, Output};

fn fixture() -> (tempfile::TempDir, Vec<Record>) {
    let temp = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
    std::fs::write(
        temp.path().join("config.toml"),
        "auto_index_on_search = false\n",
    )
    .unwrap();
    let records = vec![
        record(1, "αβγδε".into()),
        record(
            2,
            format!("{} late_needle evidence", "padding ".repeat(5000)),
        ),
        record(3, "final outcome".into()),
    ];
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    let mut writer = index.writer().unwrap();
    for record in &records {
        index.add_record(&mut writer, record).unwrap();
    }
    writer.commit().unwrap();
    (temp, records)
}

fn record(id: u64, text: String) -> Record {
    Record {
        source: SourceKind::Codex,
        doc_id: id,
        ts: id * 1000,
        project: "test".into(),
        session_id: "session".into(),
        turn_id: id as u32,
        role: "assistant".into(),
        text,
        tool_name: None,
        tool_input: None,
        tool_output: None,
        links: RecordLinks {
            event_id: Some(format!("event-{id}")),
            ..Default::default()
        },
        source_path: "/tmp/fixture.jsonl".into(),
    }
}

fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_memex"))
        .args(args)
        .arg("--root")
        .arg(root)
        .output()
        .unwrap()
}

fn values(root: &Path, args: &[&str]) -> Vec<Value> {
    let out = run(root, args);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[test]
fn search_defaults_to_compact_match_centered_references_and_full_is_explicit() {
    let (root, records) = fixture();
    let hits = values(
        root.path(),
        &["search", "late_needle", "--machine", "local"],
    );
    assert_eq!(hits.len(), 1);
    let hit = &hits[0];
    assert!(hit.get("text").is_none());
    assert!(hit["snippet"].as_str().unwrap().contains("late_needle"));
    assert!(hit["snippet"].as_str().unwrap().chars().count() <= 400);
    assert_eq!(hit["machine"], "local");
    assert_eq!(hit["record_id"], canonical_record_id(&records[1]));
    assert_eq!(hit["session_id"], "session");
    assert_eq!(hit["source_path"], "/tmp/fixture.jsonl");
    assert!(serde_json::to_string(hit).unwrap().len() < 2000);
    let full = values(
        root.path(),
        &["search", "late_needle", "--machine", "local", "--full"],
    );
    assert_eq!(full[0]["text"], records[1].text);
    let projected = values(
        root.path(),
        &[
            "search",
            "late_needle",
            "--machine",
            "local",
            "--fields",
            "text,doc_id",
        ],
    );
    assert_eq!(projected[0].as_object().unwrap().len(), 2);
    assert_eq!(projected[0]["text"], records[1].text);
}

#[test]
fn stable_record_reads_continue_without_losing_unicode() {
    let (root, records) = fixture();
    let id = canonical_record_id(&records[0]);
    let first = values(
        root.path(),
        &["show", "--record-id", &id, "--max-chars", "2"],
    );
    assert_eq!(first[0]["record"]["text"], "αβ");
    assert_eq!(first[0]["content"]["returned_chars"], 2);
    assert_eq!(first[0]["content"]["continuations"][0]["offset_chars"], 2);
    let second = values(
        root.path(),
        &[
            "show",
            "--record-id",
            &id,
            "--field",
            "text",
            "--offset-chars",
            "2",
            "--max-chars",
            "3",
        ],
    );
    assert_eq!(second[0]["record"]["text"], "γδε");
    assert_eq!(second[0]["content"]["truncated"], false);
    let full = values(root.path(), &["show", "2", "--full"]);
    assert_eq!(full[0]["record"]["text"], records[1].text);
    let default = values(root.path(), &["show", "2"]);
    assert_eq!(
        default[0]["record"]["text"]
            .as_str()
            .unwrap()
            .chars()
            .count(),
        16_000
    );
    assert_eq!(default[0]["content"]["truncated"], true);
    assert!(
        !run(root.path(), &["show", "1", "--max-chars", "0"])
            .status
            .success()
    );
    assert!(
        !run(
            root.path(),
            &["show", "1", "--field", "text", "--offset-chars", "6"]
        )
        .status
        .success()
    );
}

#[test]
fn session_and_context_page_metadata_preserve_unread_records() {
    let (root, _) = fixture();
    let session = values(root.path(), &["session", "session", "--max-chars", "5"]);
    assert_eq!(session.len(), 2);
    assert_eq!(session[0]["record"]["doc_id"], 1);
    assert_eq!(session[1]["type"], "page");
    assert_eq!(session[1]["next_offset"], 1);
    let next = values(
        root.path(),
        &["session", "session", "--offset", "1", "--max-chars", "4"],
    );
    assert_eq!(next[0]["record"]["doc_id"], 2);
    assert_eq!(next[0]["content"]["continuations"][0]["offset_chars"], 4);
    assert_eq!(next[1]["next_offset"], 2);
    let context = values(
        root.path(),
        &[
            "context",
            "--doc-id",
            "2",
            "--machine",
            "local",
            "--max-chars",
            "5",
        ],
    );
    assert_eq!(context[0]["records"].as_array().unwrap().len(), 1);
    assert_eq!(context[0]["records"][0]["record"]["doc_id"], 2);
    assert_eq!(context[0]["order"], "anchor_first");
    assert_eq!(context[0]["next_offset"], 1);
    assert_eq!(context[0]["total"], 3);
    let next = values(root.path(), &["context", "--doc-id", "2", "--offset", "2"]);
    assert_eq!(next[0]["records"][0]["record"]["doc_id"], 3);
    assert!(next[0]["next_offset"].is_null());
    let full = values(root.path(), &["session", "session", "--full"]);
    assert_eq!(full.len(), 3);
    assert!(full.iter().all(|item| item.get("type").is_none()));
}

#[test]
fn hydrate_applies_one_budget_in_input_order_and_preserves_continuation() {
    let (root, _) = fixture();
    let request = root.path().join("requests.jsonl");
    std::fs::write(&request, "{\"session_id\":\"session\",\"offset\":0,\"limit\":3}\n{\"session_id\":\"session\",\"offset\":2,\"limit\":1}\n").unwrap();
    let out = values(
        root.path(),
        &["hydrate", request.to_str().unwrap(), "--max-chars", "7"],
    );
    assert_eq!(out.len(), 2);
    assert_eq!(out[0]["records"].as_array().unwrap().len(), 2);
    assert_eq!(out[0]["records"][1]["text"], "pa");
    assert_eq!(out[0]["next_offset"], 2);
    assert_eq!(out[1]["records"], serde_json::json!([]));
    assert_eq!(out[1]["next_offset"], 2);
}

#[test]
fn cli_rejects_conflicting_or_ambiguous_read_options() {
    let (root, _) = fixture();
    for args in [
        vec!["show", "1", "--record-id", "rid1_other"],
        vec!["show", "1", "--full", "--max-chars", "2"],
        vec!["search", "needle", "--full", "--fields", "text"],
    ] {
        assert!(
            !run(root.path(), &args).status.success(),
            "accepted {args:?}"
        );
    }
}

#[cfg(unix)]
#[test]
fn remote_context_and_stable_reads_use_the_originating_machine() {
    use std::os::unix::fs::PermissionsExt;
    let (remote, records) = fixture();
    let local = tempfile::tempdir().unwrap();
    let fake_bin = tempfile::tempdir().unwrap();
    let binary = env!("CARGO_BIN_EXE_memex");
    let command = binary.replace('\\', "\\\\").replace('"', "\\\"");
    std::fs::write(
        local.path().join("config.toml"),
        format!(
            r#"auto_index_on_search = false
[multi_machine]
timeout_seconds = 10
[[machines]]
id = "remote"
command = "{command}"
[machines.control]
type = "ssh"
host = "fixture-host"
[machines.index]
type = "remote"
"#
        ),
    )
    .unwrap();
    let ssh = fake_bin.path().join("ssh");
    std::fs::write(
        &ssh,
        r#"#!/bin/sh
for argument in "$@"; do
  remote_command="$argument"
done
exec sh -c "$remote_command --root \"$MEMEX_TEST_REMOTE_ROOT\""
"#,
    )
    .unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755)).unwrap();
    let inherited = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![fake_bin.path().to_path_buf()];
    paths.extend(std::env::split_paths(&inherited));
    let path = std::env::join_paths(paths).unwrap();
    let id = canonical_record_id(&records[1]);
    for args in [
        vec![
            "context",
            "--record-id",
            &id,
            "--before",
            "0",
            "--after",
            "0",
        ],
        vec!["show", "--record-id", &id],
        vec!["show", "2"],
    ] {
        let output = Command::new(binary)
            .args(&args)
            .args(["--machine", "remote", "--max-chars", "4", "--root"])
            .arg(local.path())
            .env("PATH", &path)
            .env("MEMEX_TEST_REMOTE_ROOT", remote.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["machine"], "remote");
        let item = if args[0] == "context" {
            &value["records"][0]
        } else {
            &value
        };
        assert_eq!(item["record_id"], id);
        assert_eq!(item["record"]["text"], "padd");
        assert_eq!(item["content"]["truncated"], true);
    }
}

#[test]
fn tool_payload_continuations_can_be_used_directly_as_field_selectors() {
    let (root, _) = fixture();
    let paths = Paths::new(Some(root.path().to_path_buf())).unwrap();
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    let mut tool = record(4, "abc".into());
    tool.tool_input = Some("λμν".into());
    tool.tool_output = Some("result".into());
    let mut writer = index.writer().unwrap();
    index.add_record(&mut writer, &tool).unwrap();
    writer.commit().unwrap();
    drop(writer);
    let page = values(root.path(), &["show", "4", "--max-chars", "4"]);
    assert_eq!(page[0]["record"]["text"], "abc");
    assert_eq!(page[0]["record"]["tool_input"], "λ");
    let next = &page[0]["content"]["continuations"][0];
    let offset = next["offset_chars"].to_string();
    let second = values(
        root.path(),
        &[
            "show",
            "4",
            "--field",
            next["field"].as_str().unwrap(),
            "--offset-chars",
            &offset,
        ],
    );
    assert_eq!(second[0]["record"]["tool_input"], "μν");
    assert_eq!(second[0]["content"]["truncated"], false);
}

#[test]
fn retrieval_evaluation_reports_supplied_outcomes_separately_from_search_metrics() {
    let (root, _) = fixture();
    let dataset = root.path().join("evaluation.jsonl");
    let outcomes = root.path().join("outcomes.jsonl");
    let qrel = serde_json::json!({ "machine":"local", "source":"codex", "session_id":"session", "source_path":"/tmp/fixture.jsonl", "doc_id":2 });
    std::fs::write(
        &dataset,
        format!(
            "{}\n{}\n",
            serde_json::json!({"id":"case-a","query":"late_needle","relevant":[qrel]}),
            serde_json::json!({"id":"case-b","query":"late_needle","relevant":[qrel]})
        ),
    )
    .unwrap();
    std::fs::write(
        &outcomes,
        "{\"case_id\":\"case-a\",\"correct_conclusion\":true,\"context_tokens\":2000}\n",
    )
    .unwrap();
    let report = values(
        root.path(),
        &[
            "eval-retrieval",
            dataset.to_str().unwrap(),
            "--outcomes",
            outcomes.to_str().unwrap(),
        ],
    );
    assert_eq!(report[0]["cases"], 2);
    assert_eq!(report[0]["outcomes"]["evaluated_cases"], 1);
    assert_eq!(report[0]["outcomes"]["accuracy"], 1.0);
    assert_eq!(
        report[0]["outcomes"]["correct_conclusions_per_1000_context_tokens"],
        0.5
    );
}

#[test]
fn rpc_serializes_bounded_bodies_before_transport() {
    use std::io::Write;
    use std::process::Stdio;
    let (root, records) = fixture();
    let selector = serde_json::to_value(memex::retrieval::ContextSelector::doc_id(2)).unwrap();
    for request in [
        serde_json::json!({"op":"read_record","selector":selector,"field":"text","offset_chars":0,"max_chars":4}),
        serde_json::json!({"op":"read_context","selector":selector,"options":{"before":1,"after":1,"expand_interactions":false},"offset":0,"max_chars":4}),
        serde_json::json!({"op":"read_session_pages","requests":[{"session_id":"session","source_path":"","offset":1,"limit":2}],"max_chars":4}),
    ] {
        let mut child = Command::new(env!("CARGO_BIN_EXE_memex"))
            .args(["rpc", "--root"])
            .arg(root.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(
                serde_json::to_string(&serde_json::json!({"protocol":1,"request":request}))
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            out.stdout.len() < 3000,
            "bounded response included a large body"
        );
        let value: Value = serde_json::from_slice(&out.stdout).unwrap();
        let payload = &value["response"];
        let item = match payload["kind"].as_str().unwrap() {
            "bounded_record" => &payload["record"],
            "bounded_context" => &payload["context"]["records"][0],
            "bounded_session_pages" => &payload["pages"][0]["records"][0],
            other => panic!("unexpected {other}: {payload}"),
        };
        assert_eq!(item["record_id"], canonical_record_id(&records[1]));
        assert_eq!(item["record"]["text"], "padd");
        assert_eq!(item["content"]["continuations"][0]["offset_chars"], 4);
    }
}
