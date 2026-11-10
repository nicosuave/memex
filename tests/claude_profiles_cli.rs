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

#[cfg(unix)]
#[test]
fn default_profile_resume_clears_an_inherited_different_profile() {
    use memex::resume::{
        ResumeSession, claude_config_dir, default_resume_template, expand_resume_template,
    };
    use memex::types::SourceKind;
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let stub = temp.path().join("claude");
    fs::write(
        &stub,
        "#!/bin/sh\nprintf '%s' \"${CLAUDE_CONFIG_DIR-default}\"\n",
    )
    .unwrap();
    fs::set_permissions(&stub, fs::Permissions::from_mode(0o755)).unwrap();
    let home = directories::BaseDirs::new()
        .unwrap()
        .home_dir()
        .to_path_buf();
    let custom = temp.path().join("work's profile");
    for config_dir in [
        home.join(".claude"),
        home.join(".config/claude"),
        custom.clone(),
    ] {
        let projects = config_dir.join("projects");
        let path = projects
            .join("-tmp/session.jsonl")
            .to_string_lossy()
            .into_owned();
        let selected = claude_config_dir(SourceKind::Claude, &path, &[projects]);
        let mut session = ResumeSession {
            source: SourceKind::Claude,
            session_id: "session",
            project: "tmp",
            source_path: &path,
            source_dir: "unused",
            claude_config_dir: selected,
        };
        let template = default_resume_template("claude", true).unwrap();
        let invoke = |session: &ResumeSession| {
            let command = expand_resume_template(&template, session, "/tmp");
            let output = Command::new("/bin/sh")
                .args(["-c", &command])
                .env("PATH", format!("{}:/usr/bin:/bin", temp.path().display()))
                .env("CLAUDE_CONFIG_DIR", "/tmp/other-profile")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap()
        };
        let expected = if config_dir == custom {
            custom.to_string_lossy().into_owned()
        } else {
            "default".to_string()
        };
        assert_eq!(invoke(&session), expected);
        // Remote sessions deliberately preserve the environment on their owning machine.
        session.claude_config_dir = None;
        assert_eq!(invoke(&session), "/tmp/other-profile");
    }
}

fn agent_response(id: &str, minute: u32, tokens: u64) -> String {
    let value = serde_json::json!({
        "type": "assistant", "uuid": id, "sessionId": "parent",
        "isSidechain": true, "cwd": "/tmp/project",
        "timestamp": format!("2026-10-11T10:{minute:02}:00.000Z"),
        "message": {"id": id, "model": "claude-sonnet-4-6",
            "content": [{"type": "text", "text": id}],
            "usage": {"input_tokens": tokens, "output_tokens": 0}}
    });
    format!("{value}\n")
}

fn indexed_texts(root: &Path) -> Vec<String> {
    let index = memex::index::SearchIndex::open_or_create(&root.join("index")).unwrap();
    let mut texts = Vec::new();
    index
        .for_each_record(|record| {
            texts.push(record.text);
            Ok(())
        })
        .unwrap();
    texts.sort();
    texts
}

#[test]
fn a_resumed_tail_copy_preserves_workflow_history_and_usage() {
    use memex::types::SourceFilter;
    use memex::usage::{UsageQuery, scan_usage};

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("memex");
    fs::create_dir_all(&root).unwrap();
    fs::write(
        root.join("config.toml"),
        "embeddings = false\nclaude_paths = [\"~/.claude\"]\n",
    )
    .unwrap();
    let projects = temp.path().join(".claude/projects");
    let subagents = projects.join("-tmp/parent/subagents");
    let direct = subagents.join("agent-a.jsonl");
    let workflow = subagents.join("workflows/wf_1/agent-a.jsonl");
    fs::create_dir_all(workflow.parent().unwrap()).unwrap();
    let first = agent_response("first", 1, 10);
    let second = agent_response("second", 2, 20);
    let third = agent_response("third", 3, 30);
    let workflow_bytes = format!("{first}{second}");
    fs::write(&direct, &second).unwrap();
    fs::write(&workflow, &workflow_bytes).unwrap();
    index(temp.path(), &root);
    assert_eq!(indexed_texts(&root), ["first", "second"]);
    let query = UsageQuery {
        source: Some(SourceFilter::Claude),
        claude_sources: Some(vec![projects]),
        cache_path: Some(temp.path().join("usage.sqlite3")),
        ..UsageQuery::default()
    };
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 30);

    // A direct copy containing only the tail receives a resumed response.
    let resumed_bytes = format!("{second}{third}");
    fs::write(&direct, &resumed_bytes).unwrap();
    index(temp.path(), &root);
    assert_eq!(indexed_texts(&root), ["first", "second", "third"]);
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 60);
    assert_eq!(count(temp.path(), &root, ".claude"), 1);
    let listed: Value = serde_json::from_slice(
        &run(
            temp.path(),
            &root,
            &["sessions", "--machine", "local", "--format", "json"],
        )
        .stdout,
    )
    .unwrap();
    assert_eq!(
        listed[0]["source_path"].as_str().unwrap(),
        direct.to_str().unwrap()
    );
    assert_eq!(fs::read_to_string(&direct).unwrap(), resumed_bytes);
    assert_eq!(fs::read_to_string(&workflow).unwrap(), workflow_bytes);

    // An unchanged refresh keeps the union; growth of the non-owner is observed too.
    index(temp.path(), &root);
    assert_eq!(indexed_texts(&root), ["first", "second", "third"]);
    fs::write(
        &workflow,
        format!("{workflow_bytes}{}", agent_response("fourth", 4, 40)),
    )
    .unwrap();
    index(temp.path(), &root);
    assert_eq!(indexed_texts(&root), ["first", "fourth", "second", "third"]);
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 100);

    // Excluded copies must not contribute messages to the merged projection.
    run(
        temp.path(),
        &root,
        &[
            "index",
            "--only-source",
            "claude",
            "--no-embeddings",
            "--exclude",
            "*/workflows/**",
        ],
    );
    assert_eq!(indexed_texts(&root), ["second", "third"]);
}
