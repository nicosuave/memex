use memex::analytics::{AnalyticsStore, analytics_path};
use memex::config::Paths;
use rusqlite::{Connection, params};
use serde_json::{Value, json};
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

#[test]
fn projects_json_aggregates_all_sessions_and_matches_project_filter() {
    let root = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(root.path().to_path_buf())).unwrap();
    let db = analytics_path(&paths.state);
    drop(AnalyticsStore::open(&db).unwrap());
    let mut connection = Connection::open(&db).unwrap();
    let transaction = connection.transaction().unwrap();
    for index in 0..225 {
        transaction.execute(
            "insert into sessions (source, session_id, source_path, project, repo_project, started_at, last_at)
             values ('codex', ?1, ?2, 'worktree', 'repo', 0, 1000)",
            params![format!("s-{index}"), format!("/p/{index}")],
        ).unwrap();
    }
    transaction.execute_batch(
        "insert into sessions (source, session_id, source_path, project, repo_project, started_at, last_at, conversation_kind) values
         ('codex-session', 'alias-1', '/alias1', 'worktree', 'repo', 0, 2000, 'main'),
         ('codex-history', 'alias-2', '/alias2', 'worktree', 'repo', 0, 3000, 'subagent'),
         ('claude', 'claude-1', '/claude', 'worktree', 'repo', 0, 4000, null),
         ('codex', 'no-date', '/no-date', 'undated', null, 0, 0, null),
         ('codex', 'empty-repo', '/empty-repo', 'other', '', 0, 0, 'main'),
         ('codex', 'review', '/review', 'worktree', 'repo', 0, 9000, 'guardian_review'),
         ('codex', 'review-only', '/review-only', 'worktree', 'review-only', 0, 9001, 'guardian_review'),
         ('codex', 'unfiled-review', '/unfiled-review', 'other', null, 0, 9002, 'guardian_review');",
    ).unwrap();
    transaction.commit().unwrap();
    let output = run(root.path(), &["projects", "--format", "json"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let values: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        values,
        json!([
            {"project":"repo", "session_count":228, "last_at":"1970-01-01T00:00:04Z"},
            {"project":"Unfiled", "session_count":2, "last_at":null},
        ])
    );
    let filtered = run(
        root.path(),
        &["projects", "--source", "codex", "--format", "json"],
    );
    assert!(filtered.status.success());
    let filtered: Value = serde_json::from_slice(&filtered.stdout).unwrap();
    assert_eq!(filtered[0]["session_count"], 227);
    assert_eq!(filtered[0]["last_at"], "1970-01-01T00:00:03Z");
    for project in values.as_array().unwrap() {
        let sessions = run(
            root.path(),
            &[
                "sessions",
                "--project",
                project["project"].as_str().unwrap(),
                "--limit",
                "1000",
                "--format",
                "json",
            ],
        );
        assert!(sessions.status.success());
        let sessions: Vec<Value> = serde_json::from_slice(&sessions.stdout).unwrap();
        assert_eq!(
            sessions.len(),
            project["session_count"].as_u64().unwrap() as usize
        );
    }
    let jsonl = run(root.path(), &["projects"]);
    assert!(jsonl.status.success());
    let lines: Vec<Value> = String::from_utf8(jsonl.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(json!(lines), values);
}

#[test]
fn projects_reports_missing_cache_without_indexing() {
    let root = tempfile::tempdir().unwrap();
    let output = run(root.path(), &["projects", "--format", "json"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("run `memex index` first"));
    let paths = Paths::new(Some(root.path().to_path_buf())).unwrap();
    assert!(!analytics_path(&paths.state).exists());
}
