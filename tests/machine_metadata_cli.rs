use memex::analytics::{AnalyticsStore, analytics_path};
use memex::config::Paths;
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

fn seed(root: &Path, project: &str, count: usize) {
    let paths = Paths::new(Some(root.to_path_buf())).unwrap();
    let db = analytics_path(&paths.state);
    drop(AnalyticsStore::open(&db).unwrap());
    let mut connection = Connection::open(db).unwrap();
    let transaction = connection.transaction().unwrap();
    for index in 0..count {
        transaction.execute(
            "insert into sessions (source, session_id, source_path, project, repo_project, cwd, started_at, last_at, label)
             values ('codex', ?1, ?2, 'worktree', ?3, '/peer/work', 0, 2000, 'Fixture title')",
            params![format!("s-{index}"), format!("/peer/{index}.jsonl"), project],
        ).unwrap();
    }
    transaction.commit().unwrap();
}

struct Fixture {
    directory: tempfile::TempDir,
    local: std::path::PathBuf,
    peer: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let local = directory.path().join("local");
        let peer = directory.path().join("peer");
        seed(&local, "local-project", 1);
        seed(&peer, "peer-project", 225);
        // Resume metadata must not depend on provider binaries installed on the host.
        std::fs::write(
            peer.join("config.toml"),
            "codex_resume_cmd = \"fixture-codex resume {session_id}\"\n",
        )
        .unwrap();
        std::fs::write(
            local.join("config.toml"),
            r#"
[multi_machine]
default = ["peer"]
timeout_seconds = 2
[[machines]]
id = "peer"
label = "Peer label"
ssh = "fixture-host"
command = "/verified/memex"
[[machines]]
id = "second"
ssh = "second-host"
[[machines]]
id = "disabled"
enabled = false
"#,
        )
        .unwrap();
        let executable = directory.path().join("ssh");
        std::fs::write(&executable, r#"#!/bin/sh
[ "$1" = '-T' ] && [ "$2" = '-o' ] && [ "$3" = 'BatchMode=yes' ] && [ "$4" = '--' ] && [ "$5" = 'fixture-host' ] && [ "$6" = '/verified/memex rpc' ] || exit 19
if [ "$MEMEX_TEST_LEGACY" = 1 ]; then
    cat > /dev/null
    echo "Error: invalid memex RPC request: unknown variant 'projects'" >&2
    exit 1
fi
exec "$MEMEX_TEST_BIN" --no-update-check --non-interactive rpc --root "$MEMEX_TEST_PEER_ROOT"
"#).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        Self {
            directory,
            local,
            peer,
        }
    }

    fn run(&self, args: &[&str], legacy: bool) -> Output {
        let path = format!(
            "{}:{}",
            self.directory.path().display(),
            std::env::var("PATH").unwrap_or_default()
        );
        Command::new(env!("CARGO_BIN_EXE_memex"))
            .args(["--no-update-check", "--non-interactive"])
            .args(args)
            .arg("--root")
            .arg(&self.local)
            .env("PATH", path)
            .env("MEMEX_TEST_BIN", env!("CARGO_BIN_EXE_memex"))
            .env("MEMEX_TEST_PEER_ROOT", &self.peer)
            .env("MEMEX_TEST_LEGACY", if legacy { "1" } else { "0" })
            .output()
            .unwrap()
    }

    fn json(&self, args: &[&str]) -> Value {
        let output = self.run(args, false);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }
}

#[test]
fn machine_discovery_lists_all_enabled_peers_without_transport_details() {
    let fixture = Fixture::new();
    assert_eq!(
        fixture.json(&["machines", "--format", "json"]),
        json!([
            {"id":"local", "label":"This Mac"},
            {"id":"peer", "label":"Peer label"},
            {"id":"second", "label":"second"},
        ])
    );
}

#[test]
fn metadata_keeps_local_defaults_and_round_trips_complete_peer_results() {
    let fixture = Fixture::new();
    for command in ["projects", "sessions"] {
        let local = fixture.json(&[command, "--format", "json"]);
        assert_eq!(local.as_array().unwrap().len(), 1);
        assert!(local[0].get("machine").is_none());
        let explicit = fixture.json(&[command, "--machine", "local", "--format", "json"]);
        let mut expected = local;
        expected[0]["machine"] = json!("local");
        assert_eq!(explicit, expected);
    }
    let projects = fixture.json(&["projects", "--machine", "peer", "--format", "json"]);
    assert_eq!(
        projects,
        json!([{"project":"peer-project", "session_count":225,
        "last_at":"1970-01-01T00:00:02Z", "machine":"peer"}])
    );
    let sessions = fixture.json(&[
        "sessions",
        "--machine",
        "peer",
        "--format",
        "json",
        "--project",
        "peer-project",
        "--source",
        "codex",
        "--cwd",
        "/peer/work",
        "--origin",
        "interactive",
        "--since",
        "1970-01-01T00:00:01Z",
        "--limit",
        "3",
    ]);
    assert_eq!(sessions.as_array().unwrap().len(), 3);
    for session in sessions.as_array().unwrap() {
        assert_eq!(session["machine"], "peer");
        assert_eq!(session["repo_project"], "peer-project");
        assert_eq!(session["cwd"], "/peer/work");
        assert_eq!(session["label"], "Fixture title");
        assert_eq!(
            session["resume_cmd"],
            format!(
                "fixture-codex resume {}",
                session["session_id"].as_str().unwrap()
            )
        );
    }
    assert_eq!(
        fixture.json(&[
            "projects",
            "--machine",
            "peer",
            "--source",
            "claude",
            "--format",
            "json"
        ]),
        json!([])
    );
    assert_eq!(
        fixture.json(&[
            "sessions",
            "--machine",
            "peer",
            "--project",
            "local-project",
            "--format",
            "json"
        ]),
        json!([])
    );
    assert_eq!(
        fixture.json(&[
            "sessions",
            "--machine",
            "peer",
            "--origin",
            "subagent",
            "--format",
            "json"
        ]),
        json!([])
    );
    assert!(
        !fixture.peer.join("index").exists(),
        "metadata must not auto-index the peer"
    );
}

#[test]
fn old_peers_fail_explicitly_instead_of_returning_partial_metadata() {
    let fixture = Fixture::new();
    for command in ["projects", "sessions"] {
        let output = fixture.run(&[command, "--machine", "peer", "--format", "json"], true);
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error.contains(&format!("does not support {command} metadata")),
            "{error}"
        );
        assert!(error.contains("update Memex on that peer"));
        assert!(output.stdout.is_empty());
    }
    let unknown = fixture.run(
        &["projects", "--machine", "missing", "--format", "json"],
        false,
    );
    assert!(!unknown.status.success());
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("unknown machine 'missing'"));
}
