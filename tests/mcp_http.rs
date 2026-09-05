use reqwest::blocking::{Client, Response};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

const ALLOWED_ORIGIN: &str = "https://chat.example";
const LATEST_PROTOCOL: &str = "2026-07-28";
const LEGACY_PROTOCOL: &str = "2025-11-25";
const TOOL_NAMES: [&str; 6] = [
    "context", "hydrate", "search", "session", "sessions", "show",
];

struct McpHttpServer {
    child: Child,
    root: tempfile::TempDir,
    url: String,
    token: String,
    client: Client,
}

impl McpHttpServer {
    fn start(allowed_origins: &[&str]) -> Self {
        Self::start_with(allowed_origins, &[])
    }

    fn start_with(allowed_origins: &[&str], allowed_hosts: &[&str]) -> Self {
        let root = tempfile::tempdir().expect("temporary MCP root");
        Self::start_with_root(root, allowed_origins, allowed_hosts)
    }

    fn start_with_root(
        root: tempfile::TempDir,
        allowed_origins: &[&str],
        allowed_hosts: &[&str],
    ) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_memex"));
        command
            .args(["mcp", "--root"])
            .arg(root.path())
            .args(["--listen", "127.0.0.1:0"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        for origin in allowed_origins {
            command.args(["--allowed-origin", origin]);
        }
        for host in allowed_hosts {
            command.args(["--allowed-host", host]);
        }

        let mut child = command.spawn().expect("start HTTP MCP server");
        let stderr = child.stderr.take().expect("capture MCP stderr");
        let (lines_tx, lines_rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let Ok(line) = line else { break };
                let _ = lines_tx.send(line);
            }
        });

        let prefix = "MCP listening on ";
        let mut startup_lines = Vec::new();
        let url = loop {
            let line = match lines_rx.recv_timeout(Duration::from_secs(15)) {
                Ok(line) => line,
                Err(error) => {
                    let _ = child.kill();
                    let status = child.wait().ok();
                    panic!(
                        "HTTP MCP did not announce its listener ({error}); status: {status:?}; stderr: {}",
                        startup_lines.join("\n")
                    );
                }
            };
            if let Some(url) = line.strip_prefix(prefix) {
                break url.to_owned();
            }
            startup_lines.push(line);
        };
        let token = std::fs::read_to_string(root.path().join("web-auth-token"))
            .expect("HTTP MCP bearer token")
            .trim()
            .to_owned();
        assert!(!token.is_empty(), "HTTP MCP bearer token must not be empty");
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .expect("HTTP client");

        Self {
            child,
            root,
            url,
            token,
            client,
        }
    }

    fn latest_request(
        &self,
        id: u64,
        method: &str,
        params: Value,
    ) -> reqwest::blocking::RequestBuilder {
        let mut request = self
            .client
            .post(&self.url)
            .bearer_auth(&self.token)
            .header("Origin", ALLOWED_ORIGIN)
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", LATEST_PROTOCOL)
            .header("Mcp-Method", method);
        if method == "tools/call" {
            request = request.header(
                "Mcp-Name",
                params["name"].as_str().expect("tools/call name"),
            );
        }
        request.json(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": with_latest_meta(params),
        }))
    }

    fn token_path(&self) -> PathBuf {
        self.root.path().join("web-auth-token")
    }
}

impl Drop for McpHttpServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn with_latest_meta(mut params: Value) -> Value {
    params
        .as_object_mut()
        .expect("MCP request params must be an object")
        .insert(
            "_meta".to_owned(),
            json!({
                "io.modelcontextprotocol/protocolVersion": LATEST_PROTOCOL,
                "io.modelcontextprotocol/clientInfo": {
                    "name": "memex-http-integration-test",
                    "version": "1",
                },
                "io.modelcontextprotocol/clientCapabilities": {},
            }),
        );
    params
}

fn response_payload(response: Response) -> Value {
    assert_eq!(response.status(), 200, "unexpected response: {response:?}");
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let body = response.text().expect("read MCP response");
    if content_type.starts_with("application/json") {
        return serde_json::from_str(&body).expect("JSON MCP response");
    }
    assert!(
        content_type.starts_with("text/event-stream"),
        "unexpected content type {content_type}: {body}"
    );
    body.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|data| !data.is_empty())
        .map(|data| serde_json::from_str(data).expect("JSON SSE event"))
        .last()
        .unwrap_or_else(|| panic!("SSE response contained no data event: {body}"))
}

fn tool_names(payload: &Value) -> Vec<&str> {
    let mut names: Vec<_> = payload["result"]["tools"]
        .as_array()
        .expect("tools/list result")
        .iter()
        .map(|tool| {
            assert_eq!(tool["inputSchema"]["type"], "object");
            tool["name"].as_str().expect("tool name")
        })
        .collect();
    names.sort_unstable();
    names
}

fn read_token(path: &Path) -> String {
    std::fs::read_to_string(path)
        .expect("read web auth token")
        .trim()
        .to_owned()
}

#[test]
fn bearer_origin_and_cors_are_enforced() {
    let server = McpHttpServer::start(&[ALLOWED_ORIGIN]);
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": with_latest_meta(json!({})),
    });

    for authorization in [None, Some("Bearer definitely-not-the-token")] {
        let mut request = server
            .client
            .post(&server.url)
            .header("Origin", ALLOWED_ORIGIN)
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", LATEST_PROTOCOL)
            .header("Mcp-Method", "tools/list")
            .json(&body);
        if let Some(authorization) = authorization {
            request = request.header("Authorization", authorization);
        }
        let response = request.send().expect("unauthorized MCP request");
        assert_eq!(response.status(), 401);
        assert_eq!(
            response
                .headers()
                .get("www-authenticate")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer")
        );
    }

    let rejected_origin = server
        .client
        .post(&server.url)
        .bearer_auth(&server.token)
        .header("Origin", "https://evil.example")
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", LATEST_PROTOCOL)
        .header("Mcp-Method", "tools/list")
        .json(&body)
        .send()
        .expect("disallowed-origin MCP request");
    assert_eq!(rejected_origin.status(), 403);

    let rejected_preflight = server
        .client
        .request(reqwest::Method::OPTIONS, &server.url)
        .header("Origin", "https://evil.example")
        .header("Access-Control-Request-Method", "POST")
        .send()
        .expect("disallowed-origin CORS preflight");
    assert_eq!(rejected_preflight.status(), 403);

    let rejected_host = server
        .client
        .post(&server.url)
        .bearer_auth(&server.token)
        .header("Host", "attacker.example")
        .header("Origin", ALLOWED_ORIGIN)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", LATEST_PROTOCOL)
        .header("Mcp-Method", "tools/list")
        .json(&body)
        .send()
        .expect("disallowed-host MCP request");
    assert_eq!(rejected_host.status(), 403);

    let preflight = server
        .client
        .request(reqwest::Method::OPTIONS, &server.url)
        .header("Origin", ALLOWED_ORIGIN)
        .header("Access-Control-Request-Method", "POST")
        .header(
            "Access-Control-Request-Headers",
            "authorization, content-type, accept, mcp-protocol-version, mcp-method, mcp-name",
        )
        .send()
        .expect("CORS preflight");
    assert!(preflight.status().is_success(), "{preflight:?}");
    assert_eq!(
        preflight
            .headers()
            .get("access-control-allow-origin")
            .and_then(|value| value.to_str().ok()),
        Some(ALLOWED_ORIGIN)
    );
    let methods = preflight
        .headers()
        .get("access-control-allow-methods")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    assert!(methods.split(',').any(|method| method.trim() == "post"));
    let headers = preflight
        .headers()
        .get("access-control-allow-headers")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    for expected in [
        "authorization",
        "content-type",
        "accept",
        "mcp-protocol-version",
        "mcp-method",
        "mcp-name",
    ] {
        assert!(
            headers.split(',').any(|header| header.trim() == expected),
            "preflight must allow {expected}; got {headers}"
        );
    }

    let accepted = server
        .latest_request(2, "tools/list", json!({}))
        .send()
        .expect("authorized MCP request");
    assert_eq!(tool_names(&response_payload(accepted)), TOOL_NAMES);
    assert_eq!(read_token(&server.token_path()), server.token);

    let deny_by_default = McpHttpServer::start(&[]);
    let response = deny_by_default
        .client
        .post(&deny_by_default.url)
        .bearer_auth(&deny_by_default.token)
        .header("Origin", ALLOWED_ORIGIN)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", LATEST_PROTOCOL)
        .header("Mcp-Method", "tools/list")
        .json(&body)
        .send()
        .expect("default-origin MCP request");
    assert_eq!(response.status(), 403);

    let allowed_host = McpHttpServer::start_with(&[], &["proxy.example"]);
    let response = allowed_host
        .client
        .post(&allowed_host.url)
        .bearer_auth(&allowed_host.token)
        .header("Host", "proxy.example")
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", LATEST_PROTOCOL)
        .header("Mcp-Method", "tools/list")
        .json(&body)
        .send()
        .expect("explicitly allowed-host MCP request");
    assert_eq!(tool_names(&response_payload(response)), TOOL_NAMES);
}

#[test]
fn latest_protocol_is_stateless_and_serves_tools_over_sse() {
    let server = McpHttpServer::start(&[ALLOWED_ORIGIN]);

    let list_response = server
        .latest_request(10, "tools/list", json!({}))
        .send()
        .expect("latest tools/list");
    assert_eq!(list_response.status(), 200);
    assert!(
        list_response.headers().get("mcp-session-id").is_none(),
        "stateless server must not issue an MCP session"
    );
    assert!(
        list_response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream")),
        "HTTP MCP responses should use SSE"
    );
    let latest_tools = response_payload(list_response);
    assert_eq!(latest_tools["id"], 10);
    assert_eq!(latest_tools["result"]["resultType"], "complete");
    assert_eq!(tool_names(&latest_tools), TOOL_NAMES);

    let call_response = server
        .latest_request(
            11,
            "tools/call",
            json!({"name":"show","arguments":{"record_id":"missing"}}),
        )
        .send()
        .expect("latest tools/call");
    assert_eq!(call_response.status(), 200);
    assert!(call_response.headers().get("mcp-session-id").is_none());
    let call = response_payload(call_response);
    assert_eq!(call["id"], 11);
    assert_eq!(call["result"]["resultType"], "complete");
    assert_eq!(call["result"]["isError"], true, "{call}");

    let get = server
        .client
        .get(&server.url)
        .bearer_auth(&server.token)
        .header("Origin", ALLOWED_ORIGIN)
        .header("Accept", "text/event-stream")
        .send()
        .expect("GET MCP endpoint");
    assert_eq!(get.status(), 405);
    assert!(get.headers().get("mcp-session-id").is_none());
}

#[test]
fn legacy_initialize_and_tools_list_remain_compatible_without_sessions() {
    let server = McpHttpServer::start(&[ALLOWED_ORIGIN]);
    let initialize = server
        .client
        .post(&server.url)
        .bearer_auth(&server.token)
        .header("Origin", ALLOWED_ORIGIN)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", LEGACY_PROTOCOL)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 20,
            "method": "initialize",
            "params": {
                "protocolVersion": LEGACY_PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name":"legacy-integration-test","version":"1"},
            },
        }))
        .send()
        .expect("legacy initialize");
    assert_eq!(initialize.status(), 200);
    assert!(initialize.headers().get("mcp-session-id").is_none());
    let initialize = response_payload(initialize);
    assert_eq!(initialize["id"], 20);
    assert_eq!(initialize["result"]["protocolVersion"], LEGACY_PROTOCOL);
    assert_eq!(initialize["result"]["serverInfo"]["name"], "memex");

    let list = server
        .client
        .post(&server.url)
        .bearer_auth(&server.token)
        .header("Origin", ALLOWED_ORIGIN)
        .header("Accept", "application/json, text/event-stream")
        .header("MCP-Protocol-Version", LEGACY_PROTOCOL)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 21,
            "method": "tools/list",
            "params": {},
        }))
        .send()
        .expect("legacy tools/list");
    assert_eq!(list.status(), 200);
    assert!(list.headers().get("mcp-session-id").is_none());
    let legacy_tools = response_payload(list);
    assert_eq!(legacy_tools["id"], 21);
    assert!(
        legacy_tools["result"].get("resultType").is_none(),
        "legacy results must retain their pre-2026 shape: {legacy_tools}"
    );
    assert_eq!(tool_names(&legacy_tools), TOOL_NAMES);
}
