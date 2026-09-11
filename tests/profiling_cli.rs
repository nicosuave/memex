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

fn run_index(home: &Path, root: &Path, rebuild: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_memex"));
    command
        .env_clear()
        .env("HOME", home)
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .args(["--no-update-check", "--non-interactive", "index"]);
    if rebuild {
        command.arg("rebuild");
    }
    command
        .args([
            "--only-source",
            "claude",
            "--no-embeddings",
            "--claude-path",
        ])
        .arg(home.join(".claude/projects"))
        .arg("--root")
        .arg(root)
        .output()
        .unwrap()
}

#[test]
fn explicit_incremental_indexing_preserves_segments_until_bulk_rebuild() {
    use memex::index::SearchIndex;
    use std::collections::BTreeSet;
    use std::io::Write;

    let temp = fixture();
    let root = temp.path().join("index");
    let source = temp
        .path()
        .join(".claude/projects/private-project/private-session.jsonl");
    let mut expected = BTreeSet::from(["privateneedle".to_owned()]);
    {
        let mut file = fs::OpenOptions::new().append(true).open(&source).unwrap();
        for seed in 1..128 {
            let text = format!("privateneedle seed-{seed}");
            let record = serde_json::json!({
                "type": "user",
                "uuid": format!("private-seed-{seed}"),
                "timestamp": "2026-09-01T00:00:00Z",
                "message": {"role": "user", "content": text},
            });
            writeln!(file, "{record}").unwrap();
            expected.insert(text);
        }
    }
    let mut seed_segments = Vec::new();
    let mut previous_segments = Vec::new();
    let mut compacted_peers = false;
    for append in 0..=10 {
        if append > 0 {
            let text = format!("privateneedle update-{append}");
            let record = serde_json::json!({
                "type": "user",
                "uuid": format!("private-event-{append}"),
                "timestamp": "2026-09-01T00:00:00Z",
                "message": {"role": "user", "content": text},
            });
            writeln!(
                fs::OpenOptions::new().append(true).open(&source).unwrap(),
                "{record}"
            )
            .unwrap();
            expected.insert(text);
        }
        let output = run_index(temp.path(), &root, false);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let index = SearchIndex::open_or_create(&root.join("index")).unwrap();
        let segments = index.index.searchable_segment_ids().unwrap();
        if append == 0 {
            assert_eq!(segments.len(), 1);
            seed_segments = segments.clone();
        } else {
            assert!(seed_segments.iter().all(|id| segments.contains(id)));
            compacted_peers |= segments.len() < previous_segments.len() + 1
                && previous_segments.iter().any(|id| !segments.contains(id));
        }
        assert_eq!(index.doc_count().unwrap(), expected.len());
        assert_eq!(
            index
                .recent_records(expected.len())
                .unwrap()
                .into_iter()
                .map(|record| record.text)
                .collect::<BTreeSet<_>>(),
            expected
        );
        previous_segments = segments;
    }
    assert!(compacted_peers, "tiny peer segments were never compacted");
    let current = fs::read(root.join("index/CURRENT")).unwrap();
    assert!(run_index(temp.path(), &root, false).status.success());
    assert_eq!(fs::read(root.join("index/CURRENT")).unwrap(), current);

    let output = run_index(temp.path(), &root, true);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rebuilt = SearchIndex::open_or_create(&root.join("index")).unwrap();
    let segments = rebuilt.index.searchable_segment_ids().unwrap();
    assert_eq!(segments.len(), 1);
    assert!(segments.iter().all(|id| !previous_segments.contains(id)));
    assert_eq!(rebuilt.doc_count().unwrap(), expected.len());
    assert_eq!(
        rebuilt
            .recent_records(expected.len())
            .unwrap()
            .into_iter()
            .map(|record| record.text)
            .collect::<BTreeSet<_>>(),
        expected
    );
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

#[cfg(feature = "profiling")]
#[test]
fn no_op_and_checkpoint_only_refreshes_do_not_open_a_lexical_writer() {
    use std::io::Write;
    let temp = fixture();
    let root = temp.path().join("index");
    assert!(
        run(temp.path(), &root, None, "privateneedle")
            .status
            .success()
    );
    let generation = fs::read(root.join("index/CURRENT")).unwrap();
    for (name, append) in [("noop.json", false), ("checkpoint.json", true)] {
        if append {
            let source = temp
                .path()
                .join(".claude/projects/private-project/private-session.jsonl");
            fs::OpenOptions::new()
                .append(true)
                .open(source)
                .unwrap()
                .write_all(b"{\"type\":\"progress\",\"data\":{\"message\":\"unindexed update\"}}\n")
                .unwrap();
        }
        let trace = temp.path().join(name);
        let output = run(temp.path(), &root, Some(&trace), "privateneedle");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let profile: serde_json::Value = serde_json::from_slice(&fs::read(trace).unwrap()).unwrap();
        for event in profile["traceEvents"].as_array().unwrap() {
            assert!(
                ![
                    "lexical.stage",
                    "lexical.writer_open",
                    "lexical.commit",
                    "lexical.publish"
                ]
                .iter()
                .any(|name| event["name"] == *name)
            );
        }
        assert_eq!(fs::read(root.join("index/CURRENT")).unwrap(), generation);
    }
}
