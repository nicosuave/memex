use memex::analytics::{AnalyticsStore, analytics_path};
use memex::config::Paths;
use rusqlite::{Connection, params};
use serde_json::Value;
use std::path::Path;
use std::process::{Command, Output};

fn run(root: &Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_memex"))
        .args(["--no-update-check", "--non-interactive"])
        .args(arguments)
        .arg("--root")
        .arg(root)
        .output()
        .unwrap()
}

fn session_ids(output: Output) -> Vec<String> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rows: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    let mut ids: Vec<String> = rows
        .iter()
        .map(|row| row["session_id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    ids
}

#[test]
fn sessions_returns_rows_for_every_repeated_session_id() {
    let root = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(root.path().to_path_buf())).unwrap();
    let db = analytics_path(&paths.state);
    drop(AnalyticsStore::open(&db).unwrap());
    let connection = Connection::open(&db).unwrap();
    for index in 0..30 {
        connection
            .execute(
                "insert into sessions (source, session_id, source_path, project, started_at, last_at)
                 values ('codex', ?1, ?2, 'memex', 0, ?3)",
                params![format!("s-{index:02}"), format!("/p/{index}"), 1000 + index],
            )
            .unwrap();
    }
    drop(connection);

    let single = run(
        root.path(),
        &["sessions", "--session-id", "s-07", "--format", "json"],
    );
    assert_eq!(session_ids(single), ["s-07"]);

    let wanted: Vec<String> = (0..25).map(|index| format!("s-{index:02}")).collect();
    let mut arguments = vec!["sessions", "--format", "json", "--session-id", "missing"];
    for id in &wanted {
        arguments.extend(["--session-id", id]);
    }
    assert_eq!(session_ids(run(root.path(), &arguments)), wanted);

    arguments.extend(["--limit", "3"]);
    assert_eq!(session_ids(run(root.path(), &arguments)).len(), 3);

    let count = run(
        root.path(),
        &[
            "sessions",
            "--count",
            "--session-id",
            "s-01",
            "--session-id",
            "s-02",
        ],
    );
    assert!(!count.status.success());
}
