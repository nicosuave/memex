use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn run(home: &Path, root: &Path, arguments: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_memex"))
        .env_clear()
        .env("HOME", home)
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("CLAUDE_CONFIG_DIR", home.join("claude-config"))
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
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn claude_transfer_lands_in_the_project_directory_claude_code_uses() {
    let temp = tempfile::tempdir().unwrap();
    let home = fs::canonicalize(temp.path()).unwrap();
    let root = home.join("memex");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("config.toml"), "embeddings = false\n").unwrap();
    // Transfer keeps a recorded working directory only while it exists.
    let cwd = home.join("work").join("my.app_x");
    fs::create_dir_all(&cwd).unwrap();
    let cwd = cwd.to_str().unwrap();
    let projects = home.join("claude-config").join("projects");
    let source = projects.join("-elsewhere").join("source-session.jsonl");
    fs::create_dir_all(source.parent().unwrap()).unwrap();
    let records = [
        json!({"type": "user", "uuid": "u1", "sessionId": "source-session", "cwd": cwd,
            "timestamp": "2026-11-21T10:00:00Z",
            "message": {"role": "user", "content": "where does this land"}}),
        json!({"type": "assistant", "uuid": "a1", "parentUuid": "u1",
            "sessionId": "source-session", "cwd": cwd, "timestamp": "2026-11-21T10:00:01Z",
            "message": {"role": "assistant",
                "content": [{"type": "text", "text": "in the project directory"}]}}),
    ];
    fs::write(
        &source,
        records.map(|record| format!("{record}\n")).concat(),
    )
    .unwrap();
    run(
        &home,
        &root,
        &["index", "--only-source", "claude", "--no-embeddings"],
    );

    let output = run(
        &home,
        &root,
        &["transfer", "source-session", "--to", "claude"],
    );

    let generated = output
        .lines()
        .find_map(|line| line.strip_prefix("generated: "))
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("no generated path in {output}"));
    // Claude Code 2.1.330 replaces every character but ASCII letters and digits with `-`, so
    // the dot and underscore become dashes along with the separators. The temp path is ASCII
    // and shorter than the 200 characters past which Claude Code appends a hash.
    let expected: String = cwd
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect();
    assert!(expected.ends_with("-work-my-app-x") && expected.len() <= 200);
    assert_eq!(generated.parent(), Some(projects.join(&expected).as_path()));
    assert!(generated.is_file());
    // A separator-only swap would have produced `...-work-my.app_x`.
    let entries: Vec<_> = fs::read_dir(&projects)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        !entries.iter().any(|name| name.ends_with("my.app_x")),
        "{entries:?}"
    );
}
