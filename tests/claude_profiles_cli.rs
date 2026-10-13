use serde_json::Value;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn run(home: &Path, root: &Path, arguments: &[&str]) -> Output {
    let output = Command::new(env!("CARGO_BIN_EXE_memex"))
        .env_clear()
        .env("HOME", home)
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        // Configured `claude_paths` take precedence over the environment's config directory.
        .env("CLAUDE_CONFIG_DIR", home.join("environment-only"))
        .args(["--no-update-check", "--non-interactive"])
        .args(arguments)
        .arg("--root")
        .arg(root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn transcript(home: &Path, relative: &str, session: &str) {
    let path = home.join(relative);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        path,
        format!(
            "{{\"type\":\"user\",\"uuid\":\"{session}-1\",\"sessionId\":\"{session}\",\"cwd\":\"/tmp/project\",\"timestamp\":\"2026-10-11T10:00:00Z\",\"message\":{{\"role\":\"user\",\"content\":\"{session} question\"}}}}\n"
        ),
    )
    .unwrap();
}

fn index(home: &Path, root: &Path) {
    run(
        home,
        root,
        &["index", "--only-source", "claude", "--no-embeddings"],
    );
}

fn sessions(home: &Path, root: &Path, profile: Option<&str>) -> Vec<(String, String)> {
    let mut arguments = vec!["sessions", "--machine", "local", "--format", "json"];
    if let Some(profile) = profile {
        arguments.extend(["--profile", profile]);
    }
    let output = run(home, root, &arguments);
    let values: Value = serde_json::from_slice(&output.stdout).unwrap();
    let mut sessions = values
        .as_array()
        .unwrap()
        .iter()
        .map(|session| {
            (
                session["session_id"].as_str().unwrap().to_string(),
                session["profile"].as_str().unwrap().to_string(),
            )
        })
        .collect::<Vec<_>>();
    sessions.sort();
    sessions
}

fn count(home: &Path, root: &Path, profile: &str) -> u64 {
    let output = run(
        home,
        root,
        &[
            "sessions",
            "--count",
            "--machine",
            "local",
            "--profile",
            profile,
        ],
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    value["total"].as_u64().unwrap()
}

#[test]
fn configured_claude_paths_index_every_profile_and_filter_by_profile() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    let root = home.join("memex");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("config.toml"),
        "embeddings = false\nclaude_paths = [\"~/.claude\", \"~/.claude-profiles/*\"]\n",
    )
    .unwrap();
    transcript(home, ".claude/projects/-tmp-project/legacy.jsonl", "legacy");
    transcript(
        home,
        ".claude-profiles/work/projects/-tmp-project/work.jsonl",
        "work",
    );
    transcript(
        home,
        "environment-only/projects/-tmp-project/ignored.jsonl",
        "ignored",
    );
    fs::create_dir_all(home.join(".claude-profiles/shared/agents")).unwrap();

    index(home, &root);
    assert_eq!(
        sessions(home, &root, None),
        [
            ("legacy".to_string(), ".claude".to_string()),
            ("work".to_string(), "work".to_string()),
        ]
    );
    assert_eq!(
        sessions(home, &root, Some("work")),
        [("work".to_string(), "work".to_string())]
    );
    assert_eq!(count(home, &root, ".claude"), 1);

    // A profile created later is found without changing the configuration.
    transcript(
        home,
        ".claude-profiles/personal/projects/-tmp-project/personal.jsonl",
        "personal",
    );
    index(home, &root);
    assert_eq!(
        sessions(home, &root, Some("personal")),
        [("personal".to_string(), "personal".to_string())]
    );
    assert_eq!(count(home, &root, "work"), 1);
}
