use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::{
    StatusCode,
    blocking::{Client, RequestBuilder, Response},
    redirect::Policy,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpListener},
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

const REDIRECT_URI: &str = "https://client.example/callback";
const SCOPE: &str = "memex:read offline_access";
const VERIFIER: &str = "mcp-oauth-integration-test-verifier-00000000000000000000";

struct OAuthMcpServer {
    child: Child,
    public_url: String,
    mcp_url: String,
    client: Client,
}

impl OAuthMcpServer {
    fn start(root: &Path) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve OAuth MCP port");
        let address = listener.local_addr().expect("reserved OAuth MCP address");
        drop(listener);
        Self::start_at(root, address)
    }

    fn restart(root: &Path, public_url: &str) -> Self {
        let address = public_url
            .strip_prefix("http://")
            .expect("test OAuth URL uses loopback HTTP")
            .parse::<SocketAddr>()
            .expect("OAuth MCP socket address");
        Self::start_at(root, address)
    }

    fn start_at(root: &Path, address: SocketAddr) -> Self {
        let public_url = format!("http://{address}");
        let mcp_url = format!("{public_url}/mcp");

        let mut child = Command::new(env!("CARGO_BIN_EXE_memex"))
            .args(["mcp", "--root"])
            .arg(root)
            .args([
                "--listen",
                &address.to_string(),
                "--public-url",
                &public_url,
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start OAuth MCP server");
        let stderr = child.stderr.take().expect("capture OAuth MCP stderr");
        let (lines_tx, lines_rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let Ok(line) = line else { break };
                let _ = lines_tx.send(line);
            }
        });
        let mut startup_lines = Vec::new();
        loop {
            let line = match lines_rx.recv_timeout(Duration::from_secs(15)) {
                Ok(line) => line,
                Err(error) => {
                    let _ = child.kill();
                    let status = child.wait().ok();
                    panic!(
                        "OAuth MCP did not announce its listener ({error}); status: {status:?}; stderr: {}",
                        startup_lines.join("\n")
                    );
                }
            };
            if line == format!("MCP listening on {mcp_url}") {
                break;
            }
            startup_lines.push(line);
        }

        let client = Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(15))
            .build()
            .expect("OAuth HTTP client");
        Self {
            child,
            public_url,
            mcp_url,
            client,
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{path}", self.public_url)
    }

    fn owner_key(root: &Path) -> String {
        std::fs::read_to_string(root.join("web-auth-token"))
            .expect("OAuth owner key")
            .trim()
            .to_owned()
    }

    fn register(&self, client_name: &str) -> String {
        let response = self
            .client
            .post(self.endpoint("/oauth/register"))
            .json(&json!({
                "client_name": client_name,
                "redirect_uris": [REDIRECT_URI],
                "token_endpoint_auth_method": "none",
            }))
            .send()
            .expect("dynamic client registration");
        let registration = json_response(response, StatusCode::CREATED);
        assert_eq!(registration["redirect_uris"], json!([REDIRECT_URI]));
        assert_eq!(registration["token_endpoint_auth_method"], "none");
        registration["client_id"]
            .as_str()
            .expect("registered client id")
            .to_owned()
    }

    fn authorize_get(
        &self,
        client_id: &str,
        redirect_uri: &str,
        resource: &str,
        verifier: &str,
        state: &str,
    ) -> Response {
        self.authorize_request(client_id, redirect_uri, resource, verifier, state)
            .send()
            .expect("authorization request")
    }

    fn authorize_request(
        &self,
        client_id: &str,
        redirect_uri: &str,
        resource: &str,
        verifier: &str,
        state: &str,
    ) -> RequestBuilder {
        self.authorize_request_with_scope(
            client_id,
            redirect_uri,
            resource,
            verifier,
            state,
            Some(SCOPE),
        )
    }

    fn authorize_request_with_scope(
        &self,
        client_id: &str,
        redirect_uri: &str,
        resource: &str,
        verifier: &str,
        state: &str,
        scope: Option<&str>,
    ) -> RequestBuilder {
        let request = self.client.get(self.endpoint("/oauth/authorize")).query(&[
            ("response_type", "code"),
            ("client_id", client_id),
            ("redirect_uri", redirect_uri),
            ("code_challenge", &pkce_challenge(verifier)),
            ("code_challenge_method", "S256"),
            ("resource", resource),
            ("state", state),
        ]);
        match scope {
            Some(scope) => request.query(&[("scope", scope)]),
            None => request,
        }
    }

    fn approval(&self, request_id: &str, owner_key: &str, decision: &str) -> Response {
        self.client
            .post(self.endpoint("/oauth/authorize"))
            .form(&[
                ("request_id", request_id),
                ("owner_key", owner_key),
                ("decision", decision),
            ])
            .send()
            .expect("authorization decision")
    }

    fn approve_code(
        &self,
        client_id: &str,
        owner_key: &str,
        verifier: &str,
        state: &str,
    ) -> String {
        self.approve_code_with_scope(client_id, owner_key, verifier, state, Some(SCOPE))
    }

    fn approve_code_with_scope(
        &self,
        client_id: &str,
        owner_key: &str,
        verifier: &str,
        state: &str,
        scope: Option<&str>,
    ) -> String {
        let response = self
            .authorize_request_with_scope(
                client_id,
                REDIRECT_URI,
                &self.mcp_url,
                verifier,
                state,
                scope,
            )
            .send()
            .expect("authorization request");
        let html = text_response(response, StatusCode::OK);
        let request_id = input_value(&html, "request_id");
        let response = self.approval(&request_id, owner_key, "approve");
        let params = redirect_params(response);
        assert_eq!(params.get("state").map(String::as_str), Some(state));
        assert_eq!(
            params.get("iss").map(String::as_str),
            Some(self.public_url.as_str())
        );
        params.get("code").expect("authorization code").to_owned()
    }

    fn exchange_code(
        &self,
        client_id: &str,
        code: &str,
        redirect_uri: &str,
        verifier: &str,
        resource: &str,
    ) -> Response {
        self.client
            .post(self.endpoint("/oauth/token"))
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("client_id", client_id),
                ("redirect_uri", redirect_uri),
                ("code_verifier", verifier),
                ("resource", resource),
            ])
            .send()
            .expect("authorization code exchange")
    }

    fn refresh(&self, client_id: &str, refresh_token: &str) -> Response {
        self.client
            .post(self.endpoint("/oauth/token"))
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh_token),
                ("client_id", client_id),
                ("resource", self.mcp_url.as_str()),
            ])
            .send()
            .expect("refresh token exchange")
    }

    fn refresh_without_resource(&self, client_id: &str, refresh_token: &str) -> Response {
        self.client
            .post(self.endpoint("/oauth/token"))
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh_token),
                ("client_id", client_id),
            ])
            .send()
            .expect("refresh token exchange without resource")
    }

    fn revoke(&self, client_id: &str, token: &str) -> Response {
        self.client
            .post(self.endpoint("/oauth/revoke"))
            .form(&[("token", token), ("client_id", client_id)])
            .send()
            .expect("OAuth token revocation")
    }

    fn mcp_tools(&self, bearer: Option<&str>) -> Response {
        let mut request = self
            .client
            .post(&self.mcp_url)
            .header("Accept", "application/json, text/event-stream")
            .header("MCP-Protocol-Version", "2025-11-25")
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
                "params": {},
            }));
        if let Some(bearer) = bearer {
            request = request.bearer_auth(bearer);
        }
        request.send().expect("MCP tools/list")
    }
}

impl Drop for OAuthMcpServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Debug)]
struct Tokens {
    access: String,
    refresh: String,
}

fn tokens(response: Response) -> Tokens {
    tokens_for_scope(response, SCOPE)
}

fn tokens_for_scope(response: Response, expected_scope: &str) -> Tokens {
    assert_no_store(&response);
    let body = json_response(response, StatusCode::OK);
    assert_eq!(body["token_type"], "Bearer");
    assert_eq!(body["scope"], expected_scope);
    assert!(body["expires_in"].as_u64().is_some_and(|value| value > 0));
    let access = body["access_token"]
        .as_str()
        .expect("access token")
        .to_owned();
    let refresh = body["refresh_token"]
        .as_str()
        .expect("refresh token")
        .to_owned();
    assert!(!access.is_empty());
    assert!(!refresh.is_empty());
    assert_ne!(access, refresh);
    Tokens { access, refresh }
}

fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn json_response(response: Response, status: StatusCode) -> Value {
    let actual = response.status();
    let body = response.text().expect("read JSON response");
    assert_eq!(actual, status, "unexpected HTTP response: {body}");
    serde_json::from_str(&body).unwrap_or_else(|error| panic!("invalid JSON ({error}): {body}"))
}

fn text_response(response: Response, status: StatusCode) -> String {
    let actual = response.status();
    let body = response.text().expect("read text response");
    assert_eq!(actual, status, "unexpected HTTP response: {body}");
    body
}

fn assert_no_store(response: &Response) {
    assert!(
        response
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.to_ascii_lowercase().contains("no-store")),
        "OAuth token response must not be cached: {response:?}"
    );
    assert!(
        response
            .headers()
            .get("pragma")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.to_ascii_lowercase().contains("no-cache")),
        "OAuth token response must carry Pragma: no-cache: {response:?}"
    );
}

fn oauth_error(response: Response, expected: &str) {
    assert_no_store(&response);
    let body = json_response(response, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], expected, "{body}");
}

fn input_value(html: &str, name: &str) -> String {
    let name = format!("name=\"{name}\"");
    let tag = html
        .split('<')
        .filter_map(|part| part.split_once('>').map(|(tag, _)| tag))
        .find(|tag| tag.contains(&name))
        .unwrap_or_else(|| panic!("approval page has no {name} input: {html}"));
    let marker = "value=\"";
    let value = tag
        .split_once(marker)
        .and_then(|(_, suffix)| suffix.split_once('"').map(|(value, _)| value))
        .unwrap_or_else(|| panic!("approval input has no value: <{tag}>"));
    value.to_owned()
}

fn redirect_params(response: Response) -> HashMap<String, String> {
    assert!(
        matches!(response.status(), StatusCode::FOUND | StatusCode::SEE_OTHER),
        "authorization decision should redirect: {response:?}"
    );
    let location = response
        .headers()
        .get("location")
        .and_then(|value| value.to_str().ok())
        .expect("authorization redirect location");
    let url = reqwest::Url::parse(location).expect("authorization redirect URL");
    let mut callback = url.clone();
    callback.set_query(None);
    callback.set_fragment(None);
    assert_eq!(callback.as_str(), REDIRECT_URI);
    url.query_pairs().into_owned().collect()
}

fn mcp_payload(response: Response) -> Value {
    let actual = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let body = response.text().expect("read MCP response");
    assert_eq!(actual, StatusCode::OK, "unexpected MCP response: {body}");
    if content_type.starts_with("application/json") {
        return serde_json::from_str(&body).expect("JSON MCP response");
    }
    assert!(content_type.starts_with("text/event-stream"), "{body}");
    body.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|data| !data.is_empty())
        .map(|data| serde_json::from_str(data).expect("JSON SSE event"))
        .last()
        .unwrap_or_else(|| panic!("SSE response contained no data event: {body}"))
}

#[test]
fn metadata_registration_authentication_and_public_url_validation() {
    let root = tempfile::tempdir().expect("OAuth MCP root");
    let server = OAuthMcpServer::start(root.path());

    let protected = json_response(
        server
            .client
            .get(server.endpoint("/.well-known/oauth-protected-resource/mcp"))
            .send()
            .expect("protected-resource metadata"),
        StatusCode::OK,
    );
    assert_eq!(protected["resource"], server.mcp_url);
    assert_eq!(
        protected["authorization_servers"],
        json!([server.public_url])
    );
    assert!(
        protected["scopes_supported"]
            .as_array()
            .is_some_and(|scopes| scopes.iter().any(|scope| scope == "memex:read"))
    );

    let authorization = json_response(
        server
            .client
            .get(server.endpoint("/.well-known/oauth-authorization-server"))
            .send()
            .expect("authorization-server metadata"),
        StatusCode::OK,
    );
    assert_eq!(authorization["issuer"], server.public_url);
    assert_eq!(
        authorization["authorization_endpoint"],
        server.endpoint("/oauth/authorize")
    );
    assert_eq!(
        authorization["token_endpoint"],
        server.endpoint("/oauth/token")
    );
    assert_eq!(
        authorization["registration_endpoint"],
        server.endpoint("/oauth/register")
    );
    assert_eq!(
        authorization["revocation_endpoint"],
        server.endpoint("/oauth/revoke")
    );
    assert!(
        authorization["code_challenge_methods_supported"]
            .as_array()
            .is_some_and(|methods| methods.iter().any(|method| method == "S256"))
    );

    let challenge = server.mcp_tools(None);
    assert_eq!(challenge.status(), StatusCode::UNAUTHORIZED);
    let challenge = challenge
        .headers()
        .get("www-authenticate")
        .and_then(|value| value.to_str().ok())
        .expect("OAuth Bearer challenge");
    assert!(challenge.starts_with("Bearer "), "{challenge}");
    assert!(challenge.contains("scope=\"memex:read\""), "{challenge}");
    assert!(
        challenge.contains(&format!(
            "resource_metadata=\"{}/.well-known/oauth-protected-resource/mcp\"",
            server.public_url
        )),
        "{challenge}"
    );

    let owner_key = OAuthMcpServer::owner_key(root.path());
    let owner_access = mcp_payload(server.mcp_tools(Some(&owner_key)));
    assert_eq!(owner_access["result"]["tools"].as_array().unwrap().len(), 6);

    let client_id = server.register("metadata test client");
    let wrong_redirect = server.authorize_get(
        &client_id,
        "https://other.example/callback",
        &server.mcp_url,
        VERIFIER,
        "wrong-redirect",
    );
    assert_eq!(wrong_redirect.status(), StatusCode::BAD_REQUEST);
    let wrong_resource = server.authorize_get(
        &client_id,
        REDIRECT_URI,
        &server.public_url,
        VERIFIER,
        "wrong-resource",
    );
    assert_eq!(wrong_resource.status(), StatusCode::BAD_REQUEST);
    let missing_pkce = server
        .client
        .get(server.endpoint("/oauth/authorize"))
        .query(&[
            ("response_type", "code"),
            ("client_id", client_id.as_str()),
            ("redirect_uri", REDIRECT_URI),
            ("resource", server.mcp_url.as_str()),
            ("scope", SCOPE),
        ])
        .send()
        .expect("authorization without PKCE");
    assert_eq!(missing_pkce.status(), StatusCode::BAD_REQUEST);
    let foreign_origin = server
        .authorize_request(
            &client_id,
            REDIRECT_URI,
            &server.mcp_url,
            VERIFIER,
            "foreign-origin",
        )
        .header("Origin", "https://evil.example")
        .send()
        .expect("foreign-origin authorization request");
    assert_eq!(foreign_origin.status(), StatusCode::FORBIDDEN);
    let oversized_state = "s".repeat(2049);
    let oversized_state = server.authorize_get(
        &client_id,
        REDIRECT_URI,
        &server.mcp_url,
        VERIFIER,
        &oversized_state,
    );
    assert_eq!(oversized_state.status(), StatusCode::BAD_REQUEST);
    drop(server);

    let invalid = Command::new(env!("CARGO_BIN_EXE_memex"))
        .args([
            "mcp",
            "--root",
            root.path().to_str().expect("UTF-8 test root"),
            "--listen",
            "127.0.0.1:0",
            "--public-url",
            "http://example.com",
        ])
        .output()
        .expect("validate non-loopback HTTP public URL");
    assert!(!invalid.status.success());
}

#[test]
fn authorization_pkce_refresh_rotation_and_replay_protection() {
    let root = tempfile::tempdir().expect("OAuth MCP root");
    let server = OAuthMcpServer::start(root.path());
    let owner_key = OAuthMcpServer::owner_key(root.path());
    let client_id = server.register("<script>alert('approval')</script>");

    let approval = server.authorize_get(
        &client_id,
        REDIRECT_URI,
        &server.mcp_url,
        VERIFIER,
        "approval-state",
    );
    let html = text_response(approval, StatusCode::OK);
    assert!(!html.contains("<script>alert('approval')</script>"));
    assert!(html.contains("&lt;script&gt;"), "{html}");
    assert!(
        !html.contains(&owner_key),
        "owner key must never be reflected"
    );
    let request_id = input_value(&html, "request_id");

    let malformed = server.approval("not-a-request", &owner_key, "approve");
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    let foreign_approval = server
        .client
        .post(server.endpoint("/oauth/authorize"))
        .header("Origin", "https://evil.example")
        .form(&[
            ("request_id", request_id.as_str()),
            ("owner_key", owner_key.as_str()),
            ("decision", "approve"),
        ])
        .send()
        .expect("foreign-origin approval");
    assert_eq!(foreign_approval.status(), StatusCode::FORBIDDEN);
    let wrong_owner = text_response(
        server.approval(&request_id, "wrong-owner-key", "approve"),
        StatusCode::UNAUTHORIZED,
    );
    assert_eq!(input_value(&wrong_owner, "request_id"), request_id);
    assert!(!wrong_owner.contains(&owner_key));
    let recovered = redirect_params(server.approval(&request_id, &owner_key, "approve"));
    assert!(recovered.contains_key("code"));
    assert_eq!(
        recovered.get("state").map(String::as_str),
        Some("approval-state")
    );
    assert_eq!(
        recovered.get("iss").map(String::as_str),
        Some(server.public_url.as_str())
    );
    assert_eq!(
        server.approval(&request_id, &owner_key, "approve").status(),
        StatusCode::BAD_REQUEST,
        "a successful approval must consume its request"
    );

    let denied_html = text_response(
        server.authorize_get(
            &client_id,
            REDIRECT_URI,
            &server.mcp_url,
            VERIFIER,
            "denied-state",
        ),
        StatusCode::OK,
    );
    let denied = redirect_params(server.approval(
        &input_value(&denied_html, "request_id"),
        &owner_key,
        "deny",
    ));
    assert_eq!(
        denied.get("error").map(String::as_str),
        Some("access_denied")
    );
    assert_eq!(
        denied.get("state").map(String::as_str),
        Some("denied-state")
    );

    let bad_pkce_code = server.approve_code(&client_id, &owner_key, VERIFIER, "bad-pkce");
    oauth_error(
        server.exchange_code(
            &client_id,
            &bad_pkce_code,
            REDIRECT_URI,
            "incorrect-verifier-00000000000000000000000000000000",
            &server.mcp_url,
        ),
        "invalid_grant",
    );
    let bad_redirect_code =
        server.approve_code(&client_id, &owner_key, VERIFIER, "bad-token-redirect");
    oauth_error(
        server.exchange_code(
            &client_id,
            &bad_redirect_code,
            "https://other.example/callback",
            VERIFIER,
            &server.mcp_url,
        ),
        "invalid_grant",
    );
    let bad_resource_code =
        server.approve_code(&client_id, &owner_key, VERIFIER, "bad-token-resource");
    oauth_error(
        server.exchange_code(
            &client_id,
            &bad_resource_code,
            REDIRECT_URI,
            VERIFIER,
            &server.public_url,
        ),
        "invalid_grant",
    );

    let code = server.approve_code(&client_id, &owner_key, VERIFIER, "approved-state");
    let first =
        tokens(server.exchange_code(&client_id, &code, REDIRECT_URI, VERIFIER, &server.mcp_url));
    oauth_error(
        server.exchange_code(&client_id, &code, REDIRECT_URI, VERIFIER, &server.mcp_url),
        "invalid_grant",
    );
    let mcp = mcp_payload(server.mcp_tools(Some(&first.access)));
    assert_eq!(mcp["result"]["tools"].as_array().unwrap().len(), 6);

    let second = tokens(server.refresh(&client_id, &first.refresh));
    assert_ne!(second.access, first.access);
    assert_ne!(second.refresh, first.refresh);
    oauth_error(server.refresh(&client_id, &first.refresh), "invalid_grant");
    oauth_error(server.refresh(&client_id, &second.refresh), "invalid_grant");
    assert_eq!(
        server.mcp_tools(Some(&second.access)).status(),
        StatusCode::UNAUTHORIZED,
        "refresh replay must revoke the token family"
    );
}

#[test]
fn read_scope_grants_refresh_beyond_the_previous_history_cap() {
    let root = tempfile::tempdir().expect("OAuth MCP root");
    let server = OAuthMcpServer::start(root.path());
    let owner_key = OAuthMcpServer::owner_key(root.path());
    let client_id = server.register("read scope refresh test client");

    let default_code =
        server.approve_code_with_scope(&client_id, &owner_key, VERIFIER, "default-read", None);
    let default_read = tokens_for_scope(
        server.exchange_code(
            &client_id,
            &default_code,
            REDIRECT_URI,
            VERIFIER,
            &server.mcp_url,
        ),
        "memex:read",
    );
    oauth_error(
        server.refresh_without_resource(&client_id, &default_read.refresh),
        "invalid_grant",
    );
    let default_rotated = tokens_for_scope(
        server.refresh(&client_id, &default_read.refresh),
        "memex:read",
    );
    assert_ne!(default_rotated.refresh, default_read.refresh);

    let explicit_code = server.approve_code_with_scope(
        &client_id,
        &owner_key,
        VERIFIER,
        "explicit-read",
        Some("memex:read"),
    );
    let mut latest = tokens_for_scope(
        server.exchange_code(
            &client_id,
            &explicit_code,
            REDIRECT_URI,
            VERIFIER,
            &server.mcp_url,
        ),
        "memex:read",
    );
    let first_refresh = latest.refresh.clone();
    for _ in 0..721 {
        latest = tokens_for_scope(server.refresh(&client_id, &latest.refresh), "memex:read");
    }

    oauth_error(server.refresh(&client_id, &first_refresh), "invalid_grant");
    oauth_error(server.refresh(&client_id, &latest.refresh), "invalid_grant");
    assert_eq!(
        server.mcp_tools(Some(&latest.access)).status(),
        StatusCode::UNAUTHORIZED,
        "replaying the first retained refresh token must revoke the latest access token"
    );
}

#[test]
fn grants_persist_and_revoke_individually_or_all_at_once() {
    let root = tempfile::tempdir().expect("OAuth MCP root");
    let server = OAuthMcpServer::start(root.path());
    let owner_key = OAuthMcpServer::owner_key(root.path());
    let client_id = server.register("persistence test client");
    let code = server.approve_code(&client_id, &owner_key, VERIFIER, "persist");
    let issued =
        tokens(server.exchange_code(&client_id, &code, REDIRECT_URI, VERIFIER, &server.mcp_url));
    let public_url = server.public_url.clone();
    drop(server);

    let server = OAuthMcpServer::restart(root.path(), &public_url);
    let persisted = mcp_payload(server.mcp_tools(Some(&issued.access)));
    assert_eq!(persisted["result"]["tools"].as_array().unwrap().len(), 6);
    let rotated = tokens(server.refresh(&client_id, &issued.refresh));
    assert_eq!(
        server.revoke(&client_id, &rotated.refresh).status(),
        StatusCode::OK
    );
    assert_eq!(
        server.mcp_tools(Some(&rotated.access)).status(),
        StatusCode::UNAUTHORIZED
    );

    let code = server.approve_code(&client_id, &owner_key, VERIFIER, "revoke-all");
    let revoke_all =
        tokens(server.exchange_code(&client_id, &code, REDIRECT_URI, VERIFIER, &server.mcp_url));
    drop(server);

    let output = Command::new(env!("CARGO_BIN_EXE_memex"))
        .args(["mcp", "--root"])
        .arg(root.path())
        .arg("--revoke-all")
        .output()
        .expect("revoke all OAuth grants");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let server = OAuthMcpServer::restart(root.path(), &public_url);
    assert_eq!(
        server.mcp_tools(Some(&revoke_all.access)).status(),
        StatusCode::UNAUTHORIZED
    );
    oauth_error(
        server.refresh(&client_id, &revoke_all.refresh),
        "invalid_grant",
    );
}
