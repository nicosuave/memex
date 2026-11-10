use serde_json::{Value, json};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct Fixture {
    _temp: tempfile::TempDir,
    home: PathBuf,
    root: PathBuf,
    project: PathBuf,
}

fn fixture(config: &str) -> Fixture {
    let temp = tempfile::tempdir().expect("tempdir");
    let home = temp.path().join("home");
    let root = temp.path().join("memex");
    let project = home
        .join(".claude")
        .join("projects")
        .join("-tmp-session-read");
    fs::create_dir_all(&project).expect("create Claude project");
    fs::create_dir_all(&root).expect("create memex root");
    fs::write(root.join("config.toml"), config).expect("write config");
    let mut seed = File::create(project.join("known.jsonl")).expect("create transcript");
    write_claude_record(&mut seed, "known", 0, "seed");
    let indexed = memex(
        &home,
        &root,
        &["index", "--only-source", "claude", "--no-embeddings"],
    )
    .output()
    .expect("run initial index");
    assert_success(&indexed, "initial index");
    Fixture {
        _temp: temp,
        home,
        root,
        project,
    }
}

fn memex(home: &Path, root: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_memex"));
    command
        .env("HOME", home)
        .env("CODEX_HOME", home.join(".codex"))
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("XDG_DATA_HOME", home.join(".local").join("share"))
        .env("PI_CODING_AGENT_DIR", home.join(".pi").join("agent"))
        .args(args)
        .args(["--non-interactive", "--no-update-check", "--root"])
        .arg(root);
    command
}

fn write_claude_record(file: &mut File, session: &str, record_id: usize, text: &str) {
    let record = json!({
        "type": "user",
        "uuid": format!("{session}-user-{record_id}"),
        "parentUuid": Value::Null,
        "sessionId": session,
        "isSidechain": false,
        "timestamp": format!("2026-07-26T17:00:{record_id:02}Z"),
        "message": { "content": text }
    });
    writeln!(file, "{record}").expect("write Claude record");
    file.flush().expect("flush transcript");
}

fn write_after_index(fixture: &Fixture) {
    let mut known = OpenOptions::new()
        .append(true)
        .open(fixture.project.join("known.jsonl"))
        .expect("open transcript for append");
    write_claude_record(&mut known, "known", 1, "continued");
    let mut fresh = File::create(fixture.project.join("fresh.jsonl")).expect("create transcript");
    write_claude_record(&mut fresh, "fresh", 0, "started");
}

fn session_texts(fixture: &Fixture, session: &str) -> Vec<String> {
    session_output_texts(&session_output(fixture, session))
}

fn session_output(fixture: &Fixture, session: &str) -> Output {
    let output = memex(
        &fixture.home,
        &fixture.root,
        &["session", session, "--full"],
    )
    .output()
    .expect("run session");
    assert_success(&output, "session");
    output
}

fn session_output_texts(output: &Output) -> Vec<String> {
    String::from_utf8(output.stdout.clone())
        .expect("UTF-8 session output")
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|line| line["record"]["text"].as_str().map(str::to_owned))
        .collect()
}

fn batch_texts(fixture: &Fixture, sessions: &[&str]) -> Vec<String> {
    batch_lines(fixture, sessions)
        .iter()
        .flat_map(|page| page["records"].as_array().cloned().unwrap_or_default())
        .filter_map(|record| record["text"].as_str().map(str::to_owned))
        .collect()
}

fn batch_lines(fixture: &Fixture, sessions: &[&str]) -> Vec<Value> {
    let mut child = memex(&fixture.home, &fixture.root, &["session", "batch"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("run session batch");
    {
        let stdin = child.stdin.as_mut().expect("batch stdin");
        for session in sessions {
            writeln!(stdin, "{}", json!({ "session_id": session, "limit": 50 }))
                .expect("write batch request");
        }
    }
    let output = child.wait_with_output().expect("wait for session batch");
    assert_success(&output, "session batch");
    String::from_utf8(output.stdout)
        .expect("UTF-8 batch output")
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect()
}

/// The ingest lease file of the fixture's memex root.
fn lease_path(fixture: &Fixture) -> PathBuf {
    fixture
        .root
        .parent()
        .expect("memex root parent")
        .join(".memex.ingest.lock")
}

#[test]
fn session_reads_what_was_written_since_the_last_index() {
    let fixture = fixture("embeddings = false\nscan_cache_ttl = 0\n");
    write_after_index(&fixture);
    assert_eq!(session_texts(&fixture, "known"), ["seed", "continued"]);
    assert_eq!(session_texts(&fixture, "fresh"), ["started"]);
}

#[test]
fn session_batch_reads_what_was_written_since_the_last_index() {
    let fixture = fixture("embeddings = false\nscan_cache_ttl = 0\n");
    write_after_index(&fixture);
    assert_eq!(
        batch_texts(&fixture, &["known", "fresh"]),
        ["seed", "continued", "started"]
    );
}

fn rpc_session_texts(fixture: &Fixture, session: &str) -> Vec<String> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_memex"))
        .env("HOME", &fixture.home)
        .args(["rpc", "--root"])
        .arg(&fixture.root)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("run rpc");
    {
        let stdin = child.stdin.as_mut().expect("rpc stdin");
        let request = json!({
            "protocol": 1,
            "request": {"op": "session", "session_id": session, "source_path": ""}
        });
        write!(stdin, "{request}").expect("write rpc request");
    }
    let output = child.wait_with_output().expect("wait for rpc");
    assert_success(&output, "rpc session");
    let response: Value = serde_json::from_slice(&output.stdout).expect("rpc response JSON");
    response["response"]["context"]["records"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|record| record["text"].as_str().map(str::to_owned))
        .collect()
}

#[test]
fn rpc_session_reads_what_was_written_since_the_last_index() {
    let fixture = fixture("embeddings = false\nscan_cache_ttl = 0\n");
    write_after_index(&fixture);
    assert_eq!(rpc_session_texts(&fixture, "known"), ["seed", "continued"]);
    assert_eq!(rpc_session_texts(&fixture, "fresh"), ["started"]);
}

#[test]
fn session_reads_leave_the_index_alone_when_auto_indexing_is_off() {
    let fixture = fixture("auto_index_on_search = false\nembeddings = false\nscan_cache_ttl = 0\n");
    write_after_index(&fixture);
    assert_eq!(session_texts(&fixture, "known"), ["seed"]);
    assert!(session_texts(&fixture, "fresh").is_empty());
}

#[test]
fn sessions_named_without_a_source_path_read_their_appends_within_the_scan_ttl() {
    let fixture = fixture("embeddings = false\n");
    write_after_index(&fixture);
    assert_eq!(session_texts(&fixture, "known"), ["seed", "continued"]);
    let mut known = OpenOptions::new()
        .append(true)
        .open(fixture.project.join("known.jsonl"))
        .expect("open transcript for append");
    write_claude_record(&mut known, "known", 2, "batched");
    assert_eq!(
        batch_texts(&fixture, &["known"]),
        ["seed", "continued", "batched"]
    );
}

#[test]
fn bounded_session_reads_serve_the_index_while_an_ingest_holds_the_lease() {
    let fixture = fixture("embeddings = false\n");
    write_after_index(&fixture);
    let lease = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(lease_path(&fixture))
        .expect("open lease");
    lease.lock().expect("hold the ingest lease");
    let output = session_output(&fixture, "known");
    assert_eq!(session_output_texts(&output), ["seed"]);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("reading the index as it stands"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    lease.unlock().expect("release the ingest lease");
    assert_eq!(session_texts(&fixture, "known"), ["seed", "continued"]);
}

#[test]
fn a_failed_strict_refresh_fails_batch_pages_one_by_one_and_a_single_read() {
    let fixture = fixture("embeddings = false\nindex_freshness = \"strict\"\n");
    write_after_index(&fixture);
    // A directory where the lease file belongs makes every refresh fail at once.
    fs::remove_file(lease_path(&fixture)).expect("remove the lease file");
    fs::create_dir(lease_path(&fixture)).expect("block the lease");
    let lines = batch_lines(&fixture, &["known"]);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0]["error"]
            .as_str()
            .is_some_and(|error| error.contains("refresh the index before a session read")),
        "{lines:?}"
    );
    let output = memex(
        &fixture.home,
        &fixture.root,
        &["session", "known", "--full"],
    )
    .output()
    .expect("run session");
    assert!(
        !output.status.success(),
        "a strict read fails with its refresh"
    );
}

fn assert_success(output: &Output, operation: &str) {
    assert!(
        output.status.success(),
        "{operation} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
