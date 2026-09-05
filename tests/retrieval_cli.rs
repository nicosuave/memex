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
