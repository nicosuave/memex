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
    let temp = tempfile::tempdir().expect("temporary workspace");
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
    let output = Command::new(binary)
        .args(["search", "needle", "--root"])
        .arg(&coordinator_root)
        .args(["--machine", "remote", "--json-array"])
        .env("PATH", test_path)
        .env("MEMEX_TEST_REMOTE_ROOT", &remote_root)
        .output()
        .expect("run federated search");

    assert!(
        output.status.success(),
        "federated search failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let results: Value = serde_json::from_slice(&output.stdout).expect("JSON search output");
    assert_eq!(results, Value::Array(Vec::new()));
}
