use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn run(home: &Path, root: &Path, trace: Option<&Path>, query: &str) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_memex"));
    command
        .env_clear()
        .env("HOME", home)
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("CLAUDE_CONFIG_DIR", home.join(".claude"))
        .env("CODEX_HOME", home.join(".codex"))
        .env("OPENCODE_DATA_DIR", home.join("opencode"))
        .args([
            "--no-update-check",
            "--non-interactive",
            "search",
            query,
            "--machine",
            "local",
            "--root",
        ])
        .arg(root);
    if let Some(trace) = trace {
        command.env("MEMEX_PROFILE", trace);
    }
    command.output().unwrap()
}

fn fixture() -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    let project = temp.path().join(".claude/projects/private-project");
    fs::create_dir_all(&project).unwrap();
    fs::write(project.join("private-session.jsonl"),
        "{\"type\":\"user\",\"uuid\":\"private-event\",\"timestamp\":\"2026-09-01T00:00:00Z\",\"message\":{\"role\":\"user\",\"content\":\"privateneedle\"}}\n").unwrap();
    fs::create_dir(temp.path().join("index")).unwrap();
    fs::write(
        temp.path().join("index/config.toml"),
        "embeddings = false\nscan_cache_ttl = 0\n",
    )
    .unwrap();
    temp
}

#[cfg(not(feature = "profiling"))]
#[test]
fn default_build_ignores_trace_environment_entirely() {
    let temp = fixture();
    let invalid = temp.path().join("missing/trace.json");
    let output = run(
        temp.path(),
        &temp.path().join("index"),
        Some(&invalid),
        "privateneedle",
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("privateneedle"));
    assert!(!invalid.exists());
}

#[cfg(feature = "profiling")]
#[test]
fn trace_covers_worker_threads_without_recording_private_data() {
    let temp = fixture();
    let trace = temp.path().join("trace.json");
    let output = run(
        temp.path(),
        &temp.path().join("index"),
        Some(&trace),
        "privateneedle",
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("privateneedle"));
    let text = fs::read_to_string(&trace).unwrap();
    assert!(!text.contains("private"));
    assert!(!text.contains(temp.path().to_str().unwrap()));
    let document: serde_json::Value = serde_json::from_str(&text).unwrap();
    let events = document["traceEvents"].as_array().unwrap();
    for name in [
        "cli.run",
        "ingest.all",
        "ingest.parse_file",
        "ingest.writer",
        "lexical.commit",
        "lexical.merge_wait",
        "lexical.search",
    ] {
        assert!(events.iter().any(|e| e["name"] == name), "missing {name}");
    }
    let thread = |name: &str| {
        events.iter().find(|e| e["name"] == name).unwrap()["tid"]
            .as_u64()
            .unwrap()
    };
    assert_ne!(thread("cli.run"), thread("ingest.writer"));
    assert_ne!(thread("ingest.writer"), thread("ingest.parse_file"));
    assert_eq!(document["incomplete_spans"], 0);
    let counters = document["threads"].as_array().unwrap();
    assert_eq!(
        counters
            .iter()
            .filter_map(|t| t["counters"]["ingest.records_added"].as_u64())
            .sum::<u64>(),
        1
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&trace).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

#[cfg(feature = "profiling")]
#[test]
fn capture_is_opt_in_and_existing_files_are_never_overwritten() {
    let temp = fixture();
    let root = temp.path().join("index");
    assert!(
        run(temp.path(), &root, None, "privateneedle")
            .status
            .success()
    );
    let trace = temp.path().join("trace.json");
    fs::write(&trace, "sentinel").unwrap();
    assert!(
        !run(temp.path(), &root, Some(&trace), "privateneedle")
            .status
            .success()
    );
    assert_eq!(fs::read_to_string(&trace).unwrap(), "sentinel");
}

#[cfg(feature = "profiling")]
#[test]
fn failed_commands_still_finish_the_trace() {
    let temp = fixture();
    let trace = temp.path().join("trace.json");
    let output = run(temp.path(), &temp.path().join("index"), Some(&trace), "(");
    assert!(!output.status.success());
    let document: serde_json::Value = serde_json::from_slice(&fs::read(trace).unwrap()).unwrap();
    assert_eq!(document["incomplete_spans"], 0);
    assert!(
        document["traceEvents"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["name"] == "cli.run")
    );
}
