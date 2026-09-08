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

#[test]
fn daemon_mcp_flags_override_the_configured_enablement() {
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

    let configured_for_flag = free_address();
    dirs.write_config(&format!(
        "auto_index_on_search = false\nindex_service_mcp = false\n\n[mcp]\nlisten = '{configured_for_flag}'\n"
    ));
    let mut enabled = ChildGuard::daemon(&dirs, &["--mcp"]);
    wait_for_health(&mut enabled, configured_for_flag, "memex-mcp");
    enabled.stop();
    assert_listener_closes(configured_for_flag);

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
