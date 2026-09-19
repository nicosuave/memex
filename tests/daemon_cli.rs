use reqwest::blocking::Client;
use serde_json::json;
use std::io::Read;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MCP_PROTOCOL: &str = "2025-11-25";

struct TestDirs {
    root: tempfile::TempDir,
    home: tempfile::TempDir,
    claude: tempfile::TempDir,
}

impl TestDirs {
    fn new() -> Self {
        Self {
            root: tempfile::tempdir().expect("temporary Memex root"),
            home: tempfile::tempdir().expect("isolated home"),
            claude: tempfile::tempdir().expect("empty Claude source"),
        }
    }

    fn write_config(&self, contents: &str) {
        std::fs::write(self.root.path().join("config.toml"), contents)
            .expect("write daemon config");
    }
}

struct ChildGuard {
    child: Child,
    logs: Arc<Mutex<Vec<u8>>>,
}

impl ChildGuard {
    fn spawn(dirs: &TestDirs, args: &[&str]) -> Self {
        Self::spawn_with_env(dirs, args, &[])
    }

    fn spawn_with_env(dirs: &TestDirs, args: &[&str], environment: &[(&str, &Path)]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_memex"));
        command
            .arg("--no-update-check")
            .args(args)
            .env("HOME", dirs.home.path())
            .env("CLAUDE_CONFIG_DIR", dirs.claude.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (name, value) in environment {
            command.env(name, value);
        }
        let mut child = command.spawn().expect("start Memex child");
        let logs = Arc::new(Mutex::new(Vec::new()));
        drain(child.stdout.take().unwrap(), Arc::clone(&logs));
        drain(child.stderr.take().unwrap(), Arc::clone(&logs));
        Self { child, logs }
    }

    fn daemon(dirs: &TestDirs, extra: &[&str]) -> Self {
        let root = dirs.root.path().to_str().unwrap();
        let claude = dirs.claude.path().to_str().unwrap();
        let mut args = vec![
            "daemon",
            "run",
            "--root",
            root,
            "--only-source",
            "claude",
            "--claude-path",
            claude,
            "--no-embeddings",
            "--poll-interval",
            "1",
        ];
        args.extend_from_slice(extra);
        Self::spawn(dirs, &args)
    }

    fn assert_running(&mut self) {
        if let Some(status) = self.child.try_wait().expect("inspect Memex child") {
            panic!(
                "Memex child exited early with {status}: {}",
                self.diagnostics()
            );
        }
    }

    fn wait_for_exit(&mut self) -> std::process::ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = self.child.try_wait().expect("inspect Memex child") {
                return status;
            }
            if Instant::now() >= deadline {
                panic!("Memex child did not exit: {}", self.diagnostics());
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    fn stop(&mut self) {
        if self
            .child
            .try_wait()
            .expect("inspect Memex child")
            .is_none()
        {
            self.child.kill().expect("stop Memex child");
            self.child.wait().expect("reap Memex child");
        }
    }

    fn diagnostics(&self) -> String {
        let bytes = self.logs.lock().expect("lock child logs");
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn drain(mut reader: impl Read + Send + 'static, logs: Arc<Mutex<Vec<u8>>>) {
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        loop {
            let Ok(read) = reader.read(&mut buffer) else {
                break;
            };
            if read == 0 {
                break;
            }
            let mut logs = logs.lock().expect("lock child logs");
            let remaining = 64 * 1024_usize - logs.len().min(64 * 1024);
            logs.extend_from_slice(&buffer[..read.min(remaining)]);
        }
    });
}

fn free_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve test port");
    listener.local_addr().expect("test socket address")
}

fn client() -> Client {
    Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .expect("HTTP client")
}

fn wait_for_health(child: &mut ChildGuard, address: SocketAddr, expected: &str) {
    let client = client();
    let url = format!("http://{address}/healthz");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(response) = client.get(&url).send()
            && response.status().is_success()
            && response.text().ok().as_deref() == Some(expected)
        {
            return;
        }
        child.assert_running();
        if Instant::now() >= deadline {
            panic!(
                "listener {address} did not become healthy as {expected:?}: {}",
                child.diagnostics()
            );
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn assert_listener_closes(address: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("listener {address} remained alive after its parent exited");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn initialize_mcp(root: &Path, address: SocketAddr, host: Option<&str>, origin: Option<&str>) {
    let (status, body) = mcp_initialize_response(root, address, host, origin);
    assert_eq!(status, 200, "MCP initialize failed: {body}");
    assert!(
        body.contains("serverInfo"),
        "unexpected MCP response: {body}"
    );
    assert!(body.contains("memex"), "unexpected MCP response: {body}");
}

fn mcp_initialize_response(
    root: &Path,
    address: SocketAddr,
    host: Option<&str>,
    origin: Option<&str>,
) -> (reqwest::StatusCode, String) {
    let owner_key =
        std::fs::read_to_string(root.join("web-auth-token")).expect("generated MCP owner key");
    assert!(!owner_key.trim().is_empty());
    let client = client();
    let mut request = client
        .post(format!("http://{address}/mcp"))
        .bearer_auth(owner_key.trim())
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", MCP_PROTOCOL)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": MCP_PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": "daemon-integration-test", "version": "1"}
            }
        }));
    if let Some(host) = host {
        request = request.header("Host", host);
    }
    if let Some(origin) = origin {
        request = request.header("Origin", origin);
    }
    let response = request.send().expect("initialize MCP");
    let status = response.status();
    let body = response.text().expect("read MCP initialize response");
    (status, body)
}

#[test]
fn configured_daemon_runs_index_web_and_mcp_in_one_process() {
    let dirs = TestDirs::new();
    let web = free_address();
    let mcp = free_address();
    dirs.write_config(&format!(
        "auto_index_on_search = false\nindex_service_web_ui = true\nindex_service_mcp = true\nindex_service_web_listen = '{web}'\n\n[mcp]\nlisten = '{mcp}'\nallowed_hosts = []\nallowed_origins = []\n"
    ));

    let mut daemon = ChildGuard::daemon(&dirs, &[]);
    wait_for_health(&mut daemon, mcp, "memex-mcp");
    wait_for_health(&mut daemon, web, "ok");
    initialize_mcp(dirs.root.path(), mcp, None, None);

    daemon.stop();
    assert_listener_closes(mcp);
    assert_listener_closes(web);
}

#[cfg(unix)]
#[test]
fn daemon_hands_off_stable_executable_preserving_arguments_and_environment() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    for mode in ["events", "poll"] {
        let dirs = TestDirs::new();
        dirs.write_config("auto_index_on_search = false\nindex_service_mcp = false\n");
        let executable = dirs.home.path().join("memex");
        symlink(env!("CARGO_BIN_EXE_memex"), &executable).unwrap();
        let result = dirs.home.path().join("handoff");
        let args = [
            "--no-update-check",
            "daemon",
            "run",
            "--root",
            dirs.root.path().to_str().unwrap(),
            "--only-source",
            "claude",
            "--claude-path",
            dirs.claude.path().to_str().unwrap(),
            "--no-embeddings",
            "--watch-mode",
            mode,
            "--poll-interval",
            "3600",
        ];
        let child = Command::new(&executable)
            .args(args)
            .env("HOME", dirs.home.path())
            .env("MEMEX_HANDOFF_RESULT", &result)
            .env("MEMEX_HANDOFF_VALUE", "preserved")
            .env_remove("MEMEX_SERVICE_MANAGER")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut daemon = ChildGuard {
            child,
            logs: Arc::new(Mutex::new(Vec::new())),
        };
        let paths = memex::config::Paths::new(Some(dirs.root.path().to_path_buf())).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !memex::daemon_runtime::read(&paths)
            .unwrap()
            .is_some_and(|info| info.ready)
        {
            daemon.assert_running();
            assert!(Instant::now() < deadline, "daemon readiness timed out");
            std::thread::sleep(Duration::from_millis(50));
        }
        let replacement = dirs.home.path().join("replacement");
        std::fs::write(&replacement, "#!/bin/sh\nif [ \"$1\" = '--version' ]; then printf 'memex 99.0.0\\n'; exit 0; fi\nprintf '%s\\n' \"$MEMEX_HANDOFF_VALUE\" \"$@\" > \"$MEMEX_HANDOFF_RESULT\"\n").unwrap();
        std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o755)).unwrap();
        let staged = dirs.home.path().join("next-link");
        symlink(&replacement, &staged).unwrap();
        std::fs::rename(&staged, &executable).unwrap();
        assert!(daemon.wait_for_exit().success());
        let expected = format!("preserved\n{}\n", args.join("\n"));
        assert_eq!(std::fs::read_to_string(&result).unwrap(), expected);
        assert!(memex::daemon_runtime::read(&paths).unwrap().is_none());
    }
}

#[test]
fn daemon_no_mcp_flag_overrides_configured_enablement() {
    let dirs = TestDirs::new();
    let web = free_address();
    let reserved_mcp = TcpListener::bind("127.0.0.1:0").expect("occupy configured MCP socket");
    let configured_mcp = reserved_mcp.local_addr().unwrap();
    dirs.write_config(&format!(
        "auto_index_on_search = false\nindex_service_web_ui = true\nindex_service_mcp = true\nindex_service_web_listen = '{web}'\n\n[mcp]\nlisten = '{configured_mcp}'\n"
    ));

    let mut web_only = ChildGuard::daemon(&dirs, &["--no-mcp"]);
    wait_for_health(&mut web_only, web, "ok");
    web_only.assert_running();
    web_only.stop();
    drop(reserved_mcp);
    assert_listener_closes(web);
}

#[test]
fn daemon_mcp_flag_overrides_configured_disablement() {
    let dirs = TestDirs::new();
    let configured_for_flag = free_address();
    dirs.write_config(&format!(
        "auto_index_on_search = false\nindex_service_mcp = false\n\n[mcp]\nlisten = '{configured_for_flag}'\n"
    ));
    let mut enabled = ChildGuard::daemon(&dirs, &["--mcp"]);
    wait_for_health(&mut enabled, configured_for_flag, "memex-mcp");
    enabled.stop();
    assert_listener_closes(configured_for_flag);
}

#[test]
fn daemon_mcp_listen_flag_overrides_configured_disablement() {
    let dirs = TestDirs::new();
    let explicit_mcp = free_address();
    dirs.write_config("auto_index_on_search = false\nindex_service_mcp = false\n");
    let mut mcp_only = ChildGuard::daemon(&dirs, &["--mcp-listen", &explicit_mcp.to_string()]);
    wait_for_health(&mut mcp_only, explicit_mcp, "memex-mcp");
    initialize_mcp(dirs.root.path(), explicit_mcp, None, None);
    mcp_only.stop();
    assert_listener_closes(explicit_mcp);
}

#[test]
fn standalone_mcp_uses_config_and_explicit_options_replace_it() {
    let dirs = TestDirs::new();
    let configured = free_address();
    dirs.write_config(&format!(
        "auto_index_on_search = false\n\n[mcp]\nlisten = '{configured}'\nallowed_hosts = ['config.example']\nallowed_origins = ['https://config.example']\npublic_url = 'http://{configured}'\n"
    ));
    let root = dirs.root.path().to_str().unwrap();

    let mut from_config = ChildGuard::spawn(&dirs, &["mcp", "--root", root]);
    wait_for_health(&mut from_config, configured, "memex-mcp");
    initialize_mcp(
        dirs.root.path(),
        configured,
        Some("config.example"),
        Some("https://config.example"),
    );
    from_config.stop();
    assert_listener_closes(configured);

    let reserved_config = TcpListener::bind("127.0.0.1:0").expect("occupy configured MCP socket");
    let configured_override = reserved_config.local_addr().unwrap();
    let explicit = free_address();
    let explicit_url = format!("http://{explicit}");
    let explicit_address = explicit.to_string();
    dirs.write_config(&format!(
        "auto_index_on_search = false\n\n[mcp]\nlisten = '{configured_override}'\nallowed_hosts = ['stale.example']\nallowed_origins = ['https://stale.example']\npublic_url = 'http://{configured_override}'\n"
    ));
    let mut overridden = ChildGuard::spawn(
        &dirs,
        &[
            "mcp",
            "--root",
            root,
            "--listen",
            &explicit_address,
            "--public-url",
            &explicit_url,
            "--allowed-host",
            "override.example",
            "--allowed-origin",
            "https://override.example",
        ],
    );
    wait_for_health(&mut overridden, explicit, "memex-mcp");
    let metadata: serde_json::Value = client()
        .get(format!(
            "http://{explicit}/.well-known/oauth-authorization-server"
        ))
        .send()
        .expect("explicit OAuth metadata")
        .json()
        .expect("OAuth metadata JSON");
    assert_eq!(metadata["issuer"], explicit_url);
    initialize_mcp(
        dirs.root.path(),
        explicit,
        Some("override.example"),
        Some("https://override.example"),
    );
    assert_eq!(
        mcp_initialize_response(
            dirs.root.path(),
            explicit,
            Some("stale.example"),
            Some("https://override.example"),
        )
        .0,
        reqwest::StatusCode::FORBIDDEN
    );
    assert_eq!(
        mcp_initialize_response(
            dirs.root.path(),
            explicit,
            Some("override.example"),
            Some("https://stale.example"),
        )
        .0,
        reqwest::StatusCode::FORBIDDEN
    );
    overridden.stop();
    drop(reserved_config);
    assert_listener_closes(explicit);
}

#[test]
fn daemon_releases_mcp_when_web_bind_fails_after_mcp_startup() {
    let dirs = TestDirs::new();
    let occupied_web = TcpListener::bind("127.0.0.1:0").expect("occupy web socket");
    let web = occupied_web.local_addr().unwrap();
    let mcp = free_address();
    dirs.write_config(&format!(
        "auto_index_on_search = false\nindex_service_web_ui = true\nindex_service_mcp = true\nindex_service_web_listen = '{web}'\n\n[mcp]\nlisten = '{mcp}'\n"
    ));

    let mut daemon = ChildGuard::daemon(&dirs, &[]);
    let status = daemon.wait_for_exit();
    assert!(
        !status.success(),
        "daemon unexpectedly survived a web bind failure"
    );
    assert_listener_closes(mcp);
}

#[test]
fn malformed_mcp_public_url_fails_startup_without_a_live_listener() {
    let dirs = TestDirs::new();
    let mcp = free_address();
    dirs.write_config(&format!(
        "auto_index_on_search = false\nindex_service_mcp = true\n\n[mcp]\nlisten = '{mcp}'\npublic_url = 'not a URL'\n"
    ));

    let mut daemon = ChildGuard::daemon(&dirs, &[]);
    let status = daemon.wait_for_exit();
    assert!(
        !status.success(),
        "daemon accepted a malformed MCP public URL"
    );
    assert_listener_closes(mcp);
}

fn wait_for_log(child: &mut ChildGuard, needle: &str, timeout_secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if child.diagnostics().contains(needle) {
            return;
        }
        child.assert_running();
        if Instant::now() >= deadline {
            panic!("timed out waiting for {needle:?}: {}", child.diagnostics());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn spawn_index_daemon(dirs: &TestDirs, extra: &[&str]) -> ChildGuard {
    let root = dirs.root.path().to_str().unwrap();
    let claude = dirs.claude.path().to_str().unwrap();
    let mut args = vec![
        "daemon",
        "run",
        "--root",
        root,
        "--only-source",
        "claude",
        "--claude-path",
        claude,
        "--no-embeddings",
    ];
    args.extend_from_slice(extra);
    ChildGuard::spawn(&dirs, &args)
}

const MINIMAL_CLAUDE_LINE: &str = "{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"hello\"}]},\"uuid\":\"u1\",\"timestamp\":\"2024-01-01T00:00:00Z\"}\n";

#[test]
fn daemon_event_mode_indexes_new_transcripts_without_resync() {
    let dirs = TestDirs::new();
    // Resync an hour out: if the transcript gets indexed, events did it.
    let mut daemon = spawn_index_daemon(
        &dirs,
        &["--watch-mode", "events", "--poll-interval", "3600"],
    );
    wait_for_log(&mut daemon, "indexed 0 records", 60);

    std::fs::write(
        dirs.claude.path().join("session.jsonl"),
        MINIMAL_CLAUDE_LINE,
    )
    .expect("write transcript");
    wait_for_log(&mut daemon, "indexed 1 records", 90);
    daemon.stop();
}

#[test]
fn daemon_poll_mode_still_indexes_on_interval() {
    let dirs = TestDirs::new();
    let mut daemon = spawn_index_daemon(&dirs, &["--watch-mode", "poll", "--poll-interval", "1"]);
    wait_for_log(&mut daemon, "indexed 0 records", 60);

    std::fs::write(
        dirs.claude.path().join("session.jsonl"),
        MINIMAL_CLAUDE_LINE,
    )
    .expect("write transcript");
    wait_for_log(&mut daemon, "indexed 1 records", 60);
    daemon.stop();
}

#[test]
fn daemon_event_mode_scans_only_the_changed_transcript() {
    let dirs = TestDirs::new();
    let first = dirs.claude.path().join("first.jsonl");
    std::fs::write(&first, MINIMAL_CLAUDE_LINE).unwrap();
    std::fs::write(dirs.claude.path().join("second.jsonl"), MINIMAL_CLAUDE_LINE).unwrap();
    let mut daemon = spawn_index_daemon(
        &dirs,
        &["--watch-mode", "events", "--poll-interval", "3600"],
    );
    wait_for_log(&mut daemon, "indexed 2 records across 2 files", 60);
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&first)
            .unwrap();
        writeln!(
            file,
            "{}",
            json!({
                "type": "user", "uuid": "u2",
                "message": {"role": "user", "content": "appended"},
            })
        )
        .unwrap();
    }
    wait_for_log(&mut daemon, "indexed 1 records across 1 files", 90);
    daemon.stop();
}

fn wait_for_session_text(daemon: &mut ChildGuard, root: &Path, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if root.join("index/CURRENT").exists()
            && let Ok(index) = memex::index::SearchIndex::open_or_create(&root.join("index"))
            && let Ok(records) = index.records_by_session_id("wal-session")
            && records.iter().any(|record| record.text == expected)
        {
            return;
        }
        daemon.assert_running();
        assert!(
            Instant::now() < deadline,
            "WAL update {expected:?} was not indexed: {}",
            daemon.diagnostics()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn daemon_event_mode_indexes_held_open_wal_without_resync() {
    let dirs = TestDirs::new();
    let source = tempfile::tempdir().unwrap();
    let database = source.path().join("opencode-work.db");
    let writer = rusqlite::Connection::open(&database).unwrap();
    writer.execute_batch(r#"
        PRAGMA journal_mode=WAL;
        PRAGMA wal_autocheckpoint=0;
        CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT, time_created INTEGER, time_updated INTEGER);
        CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
        CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, data TEXT);
        CREATE TABLE event (id TEXT NOT NULL, aggregate_id TEXT NOT NULL);
        INSERT INTO session VALUES ('wal-session', NULL, '/wal-test', 1, 2);
        INSERT INTO message VALUES ('message', 'wal-session', 3, '{"role":"assistant"}');
        INSERT INTO part VALUES ('part', 'message', '{"type":"text","text":"before WAL commit"}');
        INSERT INTO event VALUES ('event-1', 'wal-session');
    "#).unwrap();
    let mut daemon = ChildGuard::spawn_with_env(
        &dirs,
        &[
            "daemon",
            "run",
            "--root",
            dirs.root.path().to_str().unwrap(),
            "--only-source",
            "opencode",
            "--no-embeddings",
            "--watch-mode",
            "events",
            "--poll-interval",
            "3600",
        ],
        &[("OPENCODE_DATA_DIR", source.path())],
    );
    wait_for_session_text(&mut daemon, dirs.root.path(), "before WAL commit");
    let before = std::fs::metadata(&database).unwrap();
    // Keep the writer and WAL open through multiple commits. On macOS the
    // sweep must observe the updates even when FSEvents defers delivery.
    for (event, text) in [
        ("event-2", "first WAL commit"),
        ("event-3", "second WAL commit"),
    ] {
        writer
            .execute(
                "UPDATE part SET data = ?1 WHERE id = 'part'",
                [json!({"type": "text", "text": text}).to_string()],
            )
            .unwrap();
        writer
            .execute("INSERT INTO event VALUES (?1, 'wal-session')", [event])
            .unwrap();
        let after = std::fs::metadata(&database).unwrap();
        assert_eq!(after.len(), before.len());
        assert_eq!(after.modified().unwrap(), before.modified().unwrap());
        wait_for_session_text(&mut daemon, dirs.root.path(), text);
    }
    daemon.stop();
}

#[cfg(unix)]
#[test]
fn daemon_embedding_worker_tracks_new_records_in_events_and_poll_modes() {
    use memex::config::Paths;
    use memex::vector::VectorIndex;
    use std::io::Write;

    for mode in ["events", "poll"] {
        let dirs = TestDirs::new();
        dirs.write_config("auto_index_on_search = false\n");
        let transcript = dirs.claude.path().join("session.jsonl");
        std::fs::write(&transcript, "{\"type\":\"user\",\"uuid\":\"u1\",\"sessionId\":\"worker-test\",\"message\":{\"content\":\"first searchable request\"}}\n").unwrap();
        let mut daemon = ChildGuard::spawn(
            &dirs,
            &[
                "daemon",
                "run",
                "--root",
                dirs.root.path().to_str().unwrap(),
                "--only-source",
                "claude",
                "--claude-path",
                dirs.claude.path().to_str().unwrap(),
                "--embeddings",
                "--model",
                "bge",
                "--poll-interval",
                "1",
                "--watch-mode",
                mode,
            ],
        );
        let paths = Paths::new(Some(dirs.root.path().to_path_buf())).unwrap();
        for expected in [1, 2] {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                daemon.assert_running();
                if VectorIndex::inventory(&paths.vectors)
                    .ok()
                    .flatten()
                    .is_some_and(|inventory| inventory.vector_count == expected)
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "embedding worker did not reach {expected} vectors in {mode}: {}",
                    daemon.diagnostics()
                );
                std::thread::sleep(Duration::from_millis(100));
            }
            if expected == 1 {
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&transcript)
                    .unwrap();
                writeln!(file, "{{\"type\":\"assistant\",\"uuid\":\"a1\",\"sessionId\":\"worker-test\",\"message\":{{\"content\":\"second searchable answer\"}}}}").unwrap();
            }
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while memex::lease::is_embedding_held(&paths) {
            daemon.assert_running();
            assert!(
                Instant::now() < deadline,
                "embedding worker did not finish: {}",
                daemon.diagnostics()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(!paths.state.join("embed-backfill.sqlite3").exists());
        assert!(
            Command::new("kill")
                .args(["-TERM", &daemon.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        assert!(daemon.wait_for_exit().success());
        assert!(!memex::lease::is_embedding_held(&paths));
    }
}

#[cfg(unix)]
#[test]
fn poll_daemon_reaps_worker_for_deletion_and_resumes_after_shutdown() {
    use memex::config::Paths;
    use memex::vector::VectorIndex;
    use memex::vector_backfill;

    let dirs = TestDirs::new();
    dirs.write_config("auto_index_on_search = false\n");
    let original = dirs.claude.path().join("original.jsonl");
    std::fs::write(&original, "{\"type\":\"user\",\"uuid\":\"old\",\"sessionId\":\"old-session\",\"message\":{\"content\":\"existing vector\"}}\n").unwrap();
    let mut seed = ChildGuard::spawn(
        &dirs,
        &[
            "index",
            "--root",
            dirs.root.path().to_str().unwrap(),
            "--only-source",
            "claude",
            "--claude-path",
            dirs.claude.path().to_str().unwrap(),
            "--no-embeddings",
        ],
    );
    assert!(seed.wait_for_exit().success(), "{}", seed.diagnostics());
    let paths = Paths::new(Some(dirs.root.path().to_path_buf())).unwrap();
    let index = memex::index::SearchIndex::open_or_create(&paths.index).unwrap();
    let old_id = index
        .doc_ids_by_source_path(original.to_str().unwrap())
        .unwrap()[0];
    drop(index);
    let mut vectors = VectorIndex::open_or_create(&paths.vectors, 384, Some("bge")).unwrap();
    vectors.add(old_id, &vec![0.25; 384]).unwrap();
    vectors.save().unwrap();
    drop(vectors);
    let transcript = (0..4096)
        .map(|id| format!("{{\"type\":\"user\",\"uuid\":\"u{id}\",\"sessionId\":\"resume-test\",\"message\":{{\"content\":\"request {id}: explain why durable embedding checkpoints avoid repeating inference after a process stops\"}}}}\n"))
        .collect::<String>();
    std::fs::write(dirs.claude.path().join("session.jsonl"), transcript).unwrap();
    let args = [
        "daemon",
        "run",
        "--root",
        dirs.root.path().to_str().unwrap(),
        "--only-source",
        "claude",
        "--claude-path",
        dirs.claude.path().to_str().unwrap(),
        "--embeddings",
        "--model",
        "bge",
        "--poll-interval",
        "1",
        "--watch-mode",
        "poll",
    ];
    let mut daemon = ChildGuard::spawn(&dirs, &args);
    let deadline = Instant::now() + Duration::from_secs(90);
    let worker_pid = loop {
        daemon.assert_running();
        if let Some(status) = vector_backfill::status(&paths).unwrap()
            && status.running
            && status.checkpointed > 0
            && status.completed < status.total
        {
            break status.pid;
        }
        assert!(
            Instant::now() < deadline,
            "worker did not produce a partial checkpoint: {}",
            daemon.diagnostics()
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    std::fs::remove_file(&original).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let replacement_worker_pid = loop {
        daemon.assert_running();
        let index = memex::index::SearchIndex::open_or_create(&paths.index).unwrap();
        let deleted = index
            .doc_ids_by_source_path(original.to_str().unwrap())
            .unwrap()
            .is_empty();
        if deleted
            && let Some(status) = vector_backfill::status(&paths).unwrap()
            && status.running
            && status.pid != worker_pid
            && status.checkpointed > 0
        {
            break status.pid;
        }
        assert!(
            Instant::now() < deadline,
            "poll daemon did not reap and restart its worker for deletion: {}",
            daemon.diagnostics()
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        !Command::new("kill")
            .args(["-0", &worker_pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let worker_pid = replacement_worker_pid;
    assert!(
        Command::new("kill")
            .args(["-TERM", &daemon.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(daemon.wait_for_exit().success());
    assert!(
        !Command::new("kill")
            .args(["-0", &worker_pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let checkpoint = vector_backfill::status(&paths)
        .unwrap()
        .expect("durable checkpoint survives shutdown");
    assert!(!checkpoint.running);
    assert!(checkpoint.checkpointed > 0);
    assert_eq!(
        VectorIndex::inventory(&paths.vectors)
            .unwrap()
            .unwrap()
            .vector_count,
        0
    );

    let mut restarted = ChildGuard::spawn(&dirs, &args);
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        restarted.assert_running();
        if VectorIndex::inventory(&paths.vectors)
            .ok()
            .flatten()
            .is_some_and(|inventory| inventory.vector_count == 4096)
            && !paths.state.join("embed-backfill.sqlite3").exists()
            && restarted.diagnostics().contains(&format!(
                "{} resumed from checkpoints",
                checkpoint.checkpointed
            ))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "resumed worker did not finish: {}",
            restarted.diagnostics()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        Command::new("kill")
            .args(["-TERM", &restarted.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(restarted.wait_for_exit().success());
    assert!(!memex::lease::is_embedding_held(&paths));
}

#[cfg(unix)]
#[test]
fn daemon_embeds_memory_only_startup_and_edits_without_conversation_changes() {
    use memex::config::Paths;
    use memex::vector::VectorIndex;

    for mode in ["events", "poll"] {
        let dirs = TestDirs::new();
        dirs.write_config("auto_index_on_search = false\n");
        let note = dirs.claude.path().join("project/memory/note.md");
        std::fs::create_dir_all(note.parent().unwrap()).unwrap();
        std::fs::write(&note, "# Knowledge\nFirst memory fact.\n").unwrap();
        let paths = Paths::new(Some(dirs.root.path().to_path_buf())).unwrap();
        let mut daemon = ChildGuard::spawn(
            &dirs,
            &[
                "daemon",
                "run",
                "--root",
                dirs.root.path().to_str().unwrap(),
                "--only-source",
                "claude",
                "--claude-path",
                dirs.claude.path().to_str().unwrap(),
                "--embeddings",
                "--model",
                "bge",
                "--poll-interval",
                "1",
                "--watch-mode",
                mode,
            ],
        );
        let mut previous = None;
        for pass in 0..2 {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                daemon.assert_running();
                let generation = std::fs::read_dir(paths.root.join("memory/vectors"))
                    .ok()
                    .and_then(|entries| {
                        entries
                            .filter_map(Result::ok)
                            .map(|entry| entry.path())
                            .find(|path| {
                                Some(path) != previous.as_ref()
                                    && VectorIndex::inventory(path)
                                        .ok()
                                        .flatten()
                                        .is_some_and(|inventory| inventory.vector_count > 0)
                            })
                    });
                if generation.is_some() && !memex::lease::is_embedding_held(&paths) {
                    previous = generation;
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "memory-only pass {pass} was not embedded in {mode}: {}",
                    daemon.diagnostics()
                );
                std::thread::sleep(Duration::from_millis(100));
            }
            if pass == 0 {
                // No conversation changed and the previous embedding worker has exited.
                std::fs::write(&note, "# Knowledge\nA changed memory fact.\n").unwrap();
            }
        }
        assert!(
            !VectorIndex::exists(&paths.vectors)
                || VectorIndex::open(&paths.vectors).unwrap().is_empty()
        );
        assert!(
            Command::new("kill")
                .args(["-TERM", &daemon.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        assert!(daemon.wait_for_exit().success());
    }
}

#[test]
fn poll_daemon_defers_deletion_while_an_external_embedding_writer_holds_the_lease() {
    use memex::config::Paths;
    use memex::index::SearchIndex;
    use memex::lease::IngestLease;
    use memex::vector::VectorIndex;

    let dirs = TestDirs::new();
    dirs.write_config("auto_index_on_search = false\n");
    let transcript = dirs.claude.path().join("session.jsonl");
    std::fs::write(&transcript, "{\"type\":\"user\",\"uuid\":\"u1\",\"sessionId\":\"external-test\",\"message\":{\"content\":\"external embedding writer\"}}\n").unwrap();
    let paths = Paths::new(Some(dirs.root.path().to_path_buf())).unwrap();
    let mut daemon = ChildGuard::daemon(&dirs, &["--watch-mode", "poll"]);
    let deadline = Instant::now() + Duration::from_secs(15);
    while !memex::daemon_runtime::read(&paths)
        .unwrap()
        .is_some_and(|state| state.ready)
    {
        daemon.assert_running();
        assert!(
            Instant::now() < deadline,
            "daemon did not become ready: {}",
            daemon.diagnostics()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    let doc_id = index
        .doc_ids_by_source_path(transcript.to_str().unwrap())
        .unwrap()[0];
    let lease =
        IngestLease::acquire_embedding(&paths, "external embed", Duration::from_secs(1)).unwrap();
    let mut vectors = VectorIndex::open_or_create(&paths.vectors, 384, Some("bge")).unwrap();
    vectors.add(doc_id, &vec![0.25; 384]).unwrap();
    vectors.save().unwrap();
    drop(vectors);
    std::fs::remove_file(&transcript).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !daemon
        .diagnostics()
        .contains("index deferred while another embedding writer")
    {
        daemon.assert_running();
        assert!(
            Instant::now() < deadline,
            "poll daemon did not defer: {}",
            daemon.diagnostics()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        SearchIndex::open_or_create(&paths.index)
            .unwrap()
            .doc_count()
            .unwrap(),
        1
    );
    drop(lease);
    let deadline = Instant::now() + Duration::from_secs(10);
    while SearchIndex::open_or_create(&paths.index)
        .unwrap()
        .doc_count()
        .unwrap()
        != 0
    {
        daemon.assert_running();
        assert!(
            Instant::now() < deadline,
            "poll daemon did not retry deletion: {}",
            daemon.diagnostics()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(VectorIndex::open(&paths.vectors).unwrap().is_empty());
    daemon.stop();
}
