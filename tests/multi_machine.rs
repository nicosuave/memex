#![cfg(unix)]

use serde_json::Value;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn memex_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_memex"))
}

fn write_config(root: &Path, contents: &str) {
    fs::create_dir_all(root).expect("create memex root");
    fs::write(root.join("config.toml"), contents).expect("write memex config");
}

#[test]
fn rpc_ping_round_trips_through_the_real_binary() {
    let root = tempfile::tempdir().expect("temporary root");
    write_config(root.path(), "auto_index_on_search = false\n");

    let mut child = Command::new(memex_binary())
        .args(["rpc", "--root"])
        .arg(root.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start memex rpc");
    child
        .stdin
        .take()
        .expect("RPC stdin")
        .write_all(br#"{"protocol":1,"request":{"op":"ping"}}"#)
        .expect("write RPC request");
    let output = child.wait_with_output().expect("wait for memex rpc");

    assert!(
        output.status.success(),
        "RPC failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response: Value = serde_json::from_slice(&output.stdout).expect("valid RPC response");
    assert_eq!(response["protocol"], 1);
    assert_eq!(response["response"]["kind"], "pong");
    assert_eq!(response["response"]["version"], env!("CARGO_PKG_VERSION"));
}

#[test]
fn federated_search_uses_the_configured_ssh_rpc_backend() {
    // The platform temporary directory can be too long for an SSH control socket path
    // (macOS `/var/folders/...`), so this workspace lives directly under `/tmp`.
    let temp = tempfile::Builder::new()
        .tempdir_in("/tmp")
        .expect("temporary workspace");
    let coordinator_root = temp.path().join("coordinator");
    let remote_root = temp.path().join("remote");
    let empty_source = temp.path().join("empty-source");
    let fake_bin = temp.path().join("bin");
    fs::create_dir_all(&fake_bin).expect("create fake bin directory");
    fs::create_dir_all(&empty_source).expect("create empty source directory");
    write_config(&remote_root, "auto_index_on_search = false\n");

    let binary = memex_binary();
    let index_output = Command::new(&binary)
        .args(["index", "--source"])
        .arg(&empty_source)
        .args([
            "--no-codex",
            "--no-opencode",
            "--no-cursor",
            "--no-pi",
            "--no-omp",
            "--no-openclaw",
            "--no-copilot",
            "--no-grok",
            "--no-jcode",
            "--no-muse",
            "--no-kiro",
            "--no-kilocode",
            "--no-kimi",
            "--no-goose",
            "--no-embeddings",
            "--root",
        ])
        .arg(&remote_root)
        .output()
        .expect("initialize remote index");
    assert!(
        index_output.status.success(),
        "remote index initialization failed: {}",
        String::from_utf8_lossy(&index_output.stderr)
    );

    let command = binary
        .to_str()
        .expect("memex binary path must be UTF-8")
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    write_config(
        &coordinator_root,
        &format!(
            r#"auto_index_on_search = false

[multi_machine]
default = ["remote"]
timeout_seconds = 5

[[machines]]
id = "remote"
command = "{command}"

[machines.control]
type = "ssh"
host = "integration-host"

[machines.index]
type = "remote"
"#
        ),
    );

    let fake_ssh = fake_bin.join("ssh");
    fs::write(
        &fake_ssh,
        r#"#!/bin/sh
printf '%s\n' "$@" > "$MEMEX_TEST_SSH_ARGS"
for argument in "$@"; do
  remote_command="$argument"
done
exec sh -c "$remote_command --root \"$MEMEX_TEST_REMOTE_ROOT\""
"#,
    )
    .expect("write fake ssh");
    let mut permissions = fs::metadata(&fake_ssh)
        .expect("fake ssh metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fake_ssh, permissions).expect("make fake ssh executable");

    let inherited_path = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![fake_bin];
    paths.extend(std::env::split_paths(&inherited_path));
    let test_path = std::env::join_paths(paths).expect("construct test PATH");
    let ssh_args = temp.path().join("ssh-args");
    let output = Command::new(binary)
        .args(["search", "needle", "--root"])
        .arg(&coordinator_root)
        .args(["--machine", "remote", "--json-array"])
        .env("PATH", test_path)
        .env("MEMEX_TEST_REMOTE_ROOT", &remote_root)
        .env("MEMEX_TEST_SSH_ARGS", &ssh_args)
        .output()
        .expect("run federated search");

    assert!(
        output.status.success(),
        "federated search failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let results: Value = serde_json::from_slice(&output.stdout).expect("JSON search output");
    assert_eq!(results, Value::Array(Vec::new()));

    // One shared connection per host through a private control socket, probed so it closes
    // when the peer stops answering, and a connect phase bounded by the smaller of
    // `timeout_seconds` and 5 seconds.
    let control_dir = coordinator_root.join("state").join("ssh");
    let control_path = format!(
        "ControlPath={}/%C",
        control_dir.to_str().expect("UTF-8 control directory")
    );
    let remote_command = format!("{} rpc", memex_binary().display());
    let ssh_args = fs::read_to_string(&ssh_args).expect("recorded ssh arguments");
    assert_eq!(
        ssh_args.lines().collect::<Vec<_>>(),
        [
            "-T",
            "-o",
            "BatchMode=yes",
            "-o",
            "ControlMaster=auto",
            "-o",
            control_path.as_str(),
            "-o",
            "ControlPersist=300",
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=2",
            "-o",
            "ConnectTimeout=5",
            "--",
            "integration-host",
            remote_command.as_str(),
        ]
    );
    let control_dir = fs::symlink_metadata(&control_dir).expect("control directory");
    assert!(control_dir.is_dir());
    assert_eq!(control_dir.permissions().mode() & 0o777, 0o700);
}

/// Peers answered by a fake `ssh` that logs each call as `<host> <requested limit>`.
/// `fail-host` is unreachable, `capped-host` holds 150 candidates, and every other host
/// holds 40. Each peer's candidates belong to one conversation.
struct FakePeers {
    temp: tempfile::TempDir,
}

impl FakePeers {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("temporary workspace");
        write_config(
            &temp.path().join("root"),
            r#"auto_index_on_search = false
[multi_machine]
timeout_seconds = 5
[[machines]]
id = "capped"
ssh = "capped-host"
[[machines]]
id = "unreachable"
ssh = "fail-host"
[[machines]]
id = "partial-a"
ssh = "partial-a-host"
[[machines]]
id = "partial-b"
ssh = "partial-b-host"
[[machines]]
id = "partial-c"
ssh = "partial-c-host"
"#,
        );
        let bin = temp.path().join("bin");
        fs::create_dir(&bin).expect("create fake bin directory");
        let ssh = bin.join("ssh");
        fs::write(
            &ssh,
            r#"#!/bin/sh
while [ "$#" -gt 0 ] && [ "$1" != -- ]; do shift; done
target="$2"
request=$(cat)
limit=$(printf '%s' "$request" | sed -n 's/.*"limit":\([0-9]*\).*/\1/p')
printf '%s %s\n' "$target" "$limit" >> "$MEMEX_TEST_SSH_LOG"
case "$target" in
  fail-host) echo "ssh: connect to host fail-host port 22: Operation timed out" >&2; exit 255 ;;
  capped-host) count=150 ;;
  *) count=40 ;;
esac
[ "$limit" -lt "$count" ] && count=$limit
printf '{"protocol":1,"response":{"kind":"records","records":['
i=1
while [ "$i" -le "$count" ]; do
  [ "$i" -gt 1 ] && printf ','
  printf '[1.0,{"source":"codex","doc_id":%s,"ts":%s,"project":"fixture","session_id":"%s","turn_id":%s,"role":"user","text":"needle","source_path":"/%s.jsonl"}]' "$i" "$i" "$target" "$i" "$target"
  i=$((i+1))
done
printf ']}}'
"#,
        )
        .expect("write fake ssh");
        fs::set_permissions(&ssh, fs::Permissions::from_mode(0o755))
            .expect("make fake ssh executable");
        Self { temp }
    }

    /// Runs one conversation search (one result per conversation, so no peer can fill the
    /// result limit by itself) and returns its output with the calls each host received.
    fn search(&self, args: &[&str]) -> (std::process::Output, Vec<String>) {
        let log = self.temp.path().join("ssh-calls");
        let _ = fs::remove_file(&log);
        let mut paths = vec![self.temp.path().join("bin")];
        paths.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        let output = Command::new(memex_binary())
            .args(["--no-update-check", "--non-interactive", "search", "needle"])
            .args([
                "--unique-session",
                "--origin",
                "all",
                "--json-array",
                "--root",
            ])
            .arg(self.temp.path().join("root"))
            .args(args)
            .env(
                "PATH",
                std::env::join_paths(paths).expect("construct test PATH"),
            )
            .env("MEMEX_TEST_SSH_LOG", &log)
            .output()
            .expect("run federated search");
        let calls = fs::read_to_string(&log)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect();
        (output, calls)
    }
}

fn result_machines(output: &std::process::Output) -> Vec<String> {
    assert!(
        output.status.success(),
        "federated search failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let results: Value = serde_json::from_slice(&output.stdout).expect("JSON search output");
    let mut machines: Vec<String> = results
        .as_array()
        .expect("result array")
        .iter()
        .map(|result| result["machine"].as_str().expect("machine").to_string())
        .collect();
    machines.sort();
    machines
}

fn calls_to<'a>(calls: &'a [String], host: &str) -> Vec<&'a str> {
    calls
        .iter()
        .filter_map(|call| call.strip_prefix(host)?.strip_prefix(' '))
        .collect()
}

#[test]
fn an_unreachable_peer_is_asked_once_per_search_across_query_views_and_rounds() {
    let peers = FakePeers::new();
    let (output, calls) = peers.search(&[
        "--query",
        "second view",
        "--machine",
        "capped",
        "--machine",
        "unreachable",
    ]);

    assert_eq!(result_machines(&output), ["capped"]);
    // Two query views in each of two rounds: the capped peer filled its first request of
    // 100 candidates, so the search deepened to 200, where it ran out.
    assert_eq!(
        calls_to(&calls, "capped-host"),
        ["100", "100", "200", "200"]
    );
    assert_eq!(calls_to(&calls, "fail-host"), ["100"], "{calls:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        stderr
            .matches("Warning: machine 'unreachable' unavailable")
            .count(),
        1,
        "{stderr}"
    );
    assert!(stderr.contains("Operation timed out"), "{stderr}");
}

#[test]
fn deepening_follows_peers_that_filled_their_request_not_the_sum_of_candidates() {
    let peers = FakePeers::new();
    // Three peers return 40 of the 100 candidates asked for: 120 in total, but none has more.
    let (output, calls) = peers.search(&[
        "--machine",
        "partial-a",
        "--machine",
        "partial-b",
        "--machine",
        "partial-c",
    ]);
    assert_eq!(
        result_machines(&output),
        ["partial-a", "partial-b", "partial-c"]
    );
    for host in ["partial-a-host", "partial-b-host", "partial-c-host"] {
        assert_eq!(calls_to(&calls, host), ["100"], "{calls:?}");
    }

    // A peer that returned all 100 candidates it was asked for is asked again for more.
    let (output, calls) = peers.search(&["--machine", "capped", "--machine", "partial-a"]);
    assert_eq!(result_machines(&output), ["capped", "partial-a"]);
    assert_eq!(calls_to(&calls, "capped-host"), ["100", "200"]);
    assert_eq!(calls_to(&calls, "partial-a-host"), ["100", "200"]);
}
