#![cfg(unix)]

use memex::analytics::{AnalyticsWriter, analytics_path};
use memex::config::Paths;
use memex::types::{Record, RecordLinks, SourceKind};
use serde_json::Value;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

fn seed(root: &Path, timestamp: u64) {
    let paths = Paths::new(Some(root.to_path_buf())).unwrap();
    paths.ensure_dirs().unwrap();
    let workspace = root.join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    std::os::unix::fs::symlink(&workspace, root.join("alias")).unwrap();
    let cwd = workspace.canonicalize().unwrap();
    let mut writer = AnalyticsWriter::open(analytics_path(&paths.state)).unwrap();
    for (doc_id, session_id, source_path, source) in [
        (1, "shared", "/shared.jsonl", SourceKind::Codex),
        (2, "shared", "/other.jsonl", SourceKind::Codex),
        (3, "other", "/shared.jsonl", SourceKind::Codex),
        (4, "shared", "/shared.jsonl", SourceKind::Claude),
    ] {
        writer
            .record(&Record {
                doc_id,
                source,
                ts: timestamp + doc_id,
                project: "fixture".into(),
                session_id: session_id.into(),
                source_path: source_path.into(),
                turn_id: 1,
                role: "user".into(),
                text: "fixture".into(),
                tool_name: None,
                tool_input: None,
                tool_output: None,
                links: RecordLinks::default(),
            })
            .unwrap();
        writer.set_session_cwd(source, source_path, session_id, cwd.to_str().unwrap());
    }
    writer.flush().unwrap();
}

fn run(root: &Path, bin: &Path, peer: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_memex"))
        .current_dir(root)
        .args([
            "--no-update-check",
            "sessions",
            "--format",
            "json",
            "--root",
        ])
        .arg(root)
        .args(args)
        .env("TEST_MEMEX", env!("CARGO_BIN_EXE_memex"))
        .env("PEER_ROOT", peer)
        .env(
            "PATH",
            format!("{}:{}", bin.display(), std::env::var("PATH").unwrap()),
        )
        .output()
        .unwrap()
}

fn rows(output: &Output) -> Vec<Value> {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn sessions_federate_exact_identities_and_keep_results_when_a_peer_fails() {
    let temp = tempfile::tempdir().unwrap();
    let local = temp.path().join("local");
    let peer = temp.path().join("peer");
    let bin = temp.path().join("bin");
    seed(&local, 10);
    seed(&peer, 20);
    std::fs::write(local.join("config.toml"), "auto_index_on_search = false\ncodex_resume_cmd = 'local-resume {session_id}'\n[multi_machine]\ndefault = ['local', 'peer']\n[[machines]]\nid = 'peer'\nssh = 'peer'\n[[machines]]\nid = 'offline'\nssh = 'offline'\n").unwrap();
    std::fs::write(
        peer.join("config.toml"),
        "auto_index_on_search = false\ncodex_resume_cmd = 'peer-resume {session_id}'\n",
    )
    .unwrap();
    std::fs::create_dir(&bin).unwrap();
    let ssh = bin.join("ssh");
    std::fs::write(&ssh, "#!/bin/sh\ncase \"$5\" in peer) cd \"$PEER_ROOT\" || exit 1; exec \"$TEST_MEMEX\" --no-update-check rpc --root \"$PEER_ROOT\";; *) echo unavailable >&2; exit 1;; esac\n").unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755)).unwrap();

    let exact = [
        "--session-id",
        "shared",
        "--source-path",
        "/shared.jsonl",
        "--source",
        "codex",
        "--origin",
        "all",
    ];
    let selected = rows(&run(&local, &bin, &peer, &exact));
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0]["machine"], "peer");
    assert_eq!(selected[0]["resume_cmd"], "peer-resume shared");
    assert_eq!(selected[1]["machine"], "local");
    assert_eq!(selected[1]["resume_cmd"], "local-resume shared");
    for row in &selected {
        assert_eq!(row["session_id"], "shared");
        assert_eq!(row["source_path"], "/shared.jsonl");
        assert_eq!(row["source"], "codex");
    }
    let mut cwd_filter = exact.to_vec();
    cwd_filter.extend(["--cwd", "alias"]);
    assert_eq!(rows(&run(&local, &bin, &peer, &cwd_filter)), selected);

    let mut limited = exact.to_vec();
    limited.extend(["--limit", "1"]);
    assert_eq!(rows(&run(&local, &bin, &peer, &limited)), selected[..1]);

    let mut partial = exact.to_vec();
    partial.extend(["--machine", "local", "--machine", "offline"]);
    let result = run(&local, &bin, &peer, &partial);
    assert_eq!(rows(&result), selected[1..]);
    assert!(String::from_utf8_lossy(&result.stderr).contains("offline:"));
    assert!(
        !run(&local, &bin, &peer, &["--machine", "offline"])
            .status
            .success()
    );
    assert!(
        !run(&local, &bin, &peer, &["--machine", "unknown"])
            .status
            .success()
    );
    assert!(
        !run(
            &local,
            &bin,
            &peer,
            &["--count", "--machine", "local", "--machine", "peer"]
        )
        .status
        .success()
    );

    let count = run(&local, &bin, &peer, &["--count"]);
    assert!(count.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&count.stdout).unwrap()["total"],
        4
    );
}
