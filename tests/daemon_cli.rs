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
        let mut command = Command::new(env!("CARGO_BIN_EXE_memex"));
        command
            .arg("--no-update-check")
            .args(args)
            .env("HOME", dirs.home.path())
            .env("CLAUDE_CONFIG_DIR", dirs.claude.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
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
