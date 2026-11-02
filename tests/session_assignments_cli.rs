use memex::analytics::{AnalyticsStore, analytics_path};
use memex::config::Paths;
use rusqlite::Connection;
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

fn json(root: &Path, arguments: &[&str]) -> Value {
    let output = run(root, arguments);
    assert!(
        output.status.success(),
        "{arguments:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn session_ids(root: &Path, arguments: &[&str]) -> Vec<String> {
    let mut arguments = arguments.to_vec();
    arguments.extend(["--machine", "local", "--format", "json"]);
    json(
        root,
        &["sessions"]
            .iter()
            .chain(&arguments)
            .copied()
            .collect::<Vec<_>>(),
    )
    .as_array()
    .unwrap()
    .iter()
    .map(|row| row["session_id"].as_str().unwrap().to_string())
    .collect()
}

/// Recreate the rebuildable catalog the way `memex index rebuild` does: from scratch.
fn seed_catalog(paths: &Paths) {
    let db = analytics_path(&paths.state);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    drop(AnalyticsStore::open(&db).unwrap());
    Connection::open(&db)
        .unwrap()
        .execute_batch(
            "insert into sessions (source, session_id, source_path, project, repo_project, started_at, last_at, parent_session_id) values
             ('codex', 'session-1', '/scratch/one.jsonl', 'scratch', null, 0, 1000, null),
             ('codex', 'session-2', '/repo/two.jsonl', 'repo-a', 'ProjectA', 0, 2000, null),
             ('codex', 'child', '/scratch/child.jsonl', 'scratch', null, 0, 3000, 'session-1'),
             ('claude', 'unrelated', '/scratch/u.jsonl', 'scratch', null, 0, 4000, null);",
        )
        .unwrap();
}

#[test]
fn project_assignments_and_tags_follow_the_issue_acceptance_examples() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let paths = Paths::new(Some(root.to_path_buf())).unwrap();
    seed_catalog(&paths);
    assert_eq!(
        session_ids(root, &["--project", "Unfiled"]),
        ["unrelated", "child", "session-1"]
    );

    // Assign Session 1 to ProjectA; its derived repository facts are unchanged.
    let report = json(root, &["session", "set-project", "session-1", "ProjectA"]);
    assert_eq!(report["project"], "ProjectA");
    assert_eq!(report["project_origin"], "session");
    assert_eq!(report["derived_project"], "Unfiled");
    assert_eq!(report["changed"][0], "ProjectA");
    assert_eq!(
        session_ids(root, &["--project", "ProjectA"]),
        ["child", "session-2", "session-1"],
        "the sub-agent inherits its parent's assignment"
    );
    let rows = json(
        root,
        &[
            "sessions",
            "--session-id",
            "session-1",
            "--machine",
            "local",
            "--format",
            "json",
        ],
    );
    assert_eq!(rows[0]["assigned_project"], "ProjectA");
    assert!(rows[0].get("repo_project").is_none());
    let projects = json(root, &["projects", "--format", "json"]);
    assert_eq!(projects[0]["project"], "Unfiled");
    assert_eq!(projects[0]["session_count"], 1);
    assert_eq!(projects[1]["project"], "ProjectA");
    assert_eq!(projects[1]["session_count"], 3);

    // Add `tooling` to Session 2: it stays in ProjectA and is also found by tag.
    let report = json(root, &["session", "tag", "add", "session-2", "Tooling"]);
    assert_eq!(report["tags"][0], "tooling");
    assert_eq!(report["project"], "ProjectA");
    assert_eq!(session_ids(root, &["--tag", "tooling"]), ["session-2"]);
    let tagged_projects = json(root, &["projects", "--tag", "tooling", "--format", "json"]);
    assert_eq!(tagged_projects.as_array().unwrap().len(), 1);
    assert_eq!(tagged_projects[0]["session_count"], 1);
    let tags = run(root, &["session", "tag", "ls"]);
    assert_eq!(
        String::from_utf8_lossy(&tags.stdout).trim(),
        r#"{"tag":"tooling","session_count":1}"#
    );

    // Reindex and rebuild replace the catalog; assignments remain.
    seed_catalog(&paths);
    assert_eq!(
        session_ids(root, &["--project", "ProjectA"]),
        ["child", "session-2", "session-1"]
    );
    assert_eq!(session_ids(root, &["--tag", "tooling"]), ["session-2"]);

    // A child can override what it inherits.
    let child = json(root, &["session", "inspect", "child"]);
    assert_eq!(child["project_origin"], "parent");
    assert_eq!(child["parent"]["session_id"], "session-1");
    json(root, &["session", "set-project", "child", "Elsewhere"]);
    assert_eq!(session_ids(root, &["--project", "Elsewhere"]), ["child"]);
    json(root, &["session", "clear-project", "child"]);

    // Removing Session 1's override returns it (and its child) to Unfiled only.
    let report = json(root, &["session", "clear-project", "session-1"]);
    assert_eq!(report["project"], "Unfiled");
    assert_eq!(report["project_origin"], "derived");
    assert_eq!(
        session_ids(root, &["--project", "Unfiled"]),
        ["unrelated", "child", "session-1"]
    );
    assert_eq!(session_ids(root, &["--project", "ProjectA"]), ["session-2"]);

    let report = json(root, &["session", "tag", "rm", "session-2", "tooling"]);
    assert_eq!(report["changed"][0], "tooling");
    assert!(session_ids(root, &["--tag", "tooling"]).is_empty());
}

#[test]
fn assignments_require_an_indexed_unambiguous_session() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let paths = Paths::new(Some(root.to_path_buf())).unwrap();
    seed_catalog(&paths);
    Connection::open(analytics_path(&paths.state))
        .unwrap()
        .execute(
            "insert into sessions (source, session_id, source_path, project, started_at, last_at)
             values ('claude', 'session-1', '/claude/one.jsonl', 'scratch', 0, 1)",
            [],
        )
        .unwrap();

    let missing = run(root, &["session", "set-project", "nope", "ProjectA"]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("no indexed session 'nope'"));

    let ambiguous = run(root, &["session", "tag", "add", "session-1", "x"]);
    assert!(!ambiguous.status.success());
    assert!(String::from_utf8_lossy(&ambiguous.stderr).contains("pass --source"));

    // Providers are distinct identities: tagging the Claude session leaves Codex's alone.
    json(
        root,
        &[
            "session",
            "tag",
            "add",
            "session-1",
            "x",
            "--source",
            "claude",
        ],
    );
    let tagged = json(
        root,
        &[
            "sessions",
            "--tag",
            "x",
            "--machine",
            "local",
            "--format",
            "json",
        ],
    );
    assert_eq!(tagged.as_array().unwrap().len(), 1);
    assert_eq!(tagged[0]["source"], "claude");
}
