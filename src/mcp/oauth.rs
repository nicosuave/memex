use std::{
    collections::{HashMap, VecDeque},
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, ensure};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Form, Query, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tower_http::cors::{Any, CorsLayer};
use url::Url;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

use crate::{config::Paths, web_auth::WebAuth};

const DATABASE_FILE: &str = "mcp-oauth.sqlite3";
const RESOURCE_PATH: &str = "/mcp";
const SCOPE_READ: &str = "memex:read";
const SCOPE_OFFLINE: &str = "offline_access";
const CODE_TTL_SECONDS: i64 = 5 * 60;
const ACCESS_TTL_SECONDS: i64 = 60 * 60;
const REFRESH_TTL_SECONDS: i64 = 30 * 24 * 60 * 60;
const UNAPPROVED_CLIENT_TTL_SECONDS: i64 = 24 * 60 * 60;
const PENDING_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_CLIENTS: i64 = 256;
const MAX_PENDING: usize = 256;
const MAX_GRANTS: i64 = 256;
const MAX_REFRESH_TOKENS_PER_FAMILY: i64 = 256;
const MAX_FAILED_APPROVALS: usize = 8;
const FAILED_APPROVAL_WINDOW: Duration = Duration::from_secs(60);

pub(super) struct OAuthServer {
    origin: String,
    resource: String,
    database: PathBuf,
    owner: Arc<WebAuth>,
    pending: Mutex<PendingState>,
    work_limit: Arc<tokio::sync::Semaphore>,
}

#[derive(Default)]
struct PendingState {
    requests: HashMap<String, PendingRequest>,
    failed_approvals: VecDeque<Instant>,
}

struct PendingRequest {
    created_at: Instant,
    client_id: String,
    client_name: String,
    redirect_uri: String,
    code_challenge: String,
    resource: String,
    scope: String,
    state: Option<String>,
}

#[derive(Deserialize)]
struct RegistrationRequest {
    redirect_uris: Vec<String>,
    #[serde(default)]
    client_name: Option<String>,
    #[serde(default)]
    token_endpoint_auth_method: Option<String>,
}

#[derive(Serialize)]
struct RegistrationResponse {
    client_id: String,
    client_name: String,
    redirect_uris: Vec<String>,
    token_endpoint_auth_method: &'static str,
}

#[derive(Deserialize)]
struct AuthorizeQuery {
    response_type: String,
    client_id: String,
    redirect_uri: String,
    code_challenge: String,
    code_challenge_method: String,
    resource: String,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    state: Option<String>,
}

#[derive(Deserialize)]
struct ApprovalForm {
    request_id: String,
    owner_key: String,
    decision: String,
}

#[derive(Deserialize)]
struct TokenForm {
    grant_type: String,
    client_id: String,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    redirect_uri: Option<String>,
    #[serde(default)]
    code_verifier: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    resource: Option<String>,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Deserialize)]
struct RevokeForm {
    token: String,
    client_id: String,
    #[serde(default)]
    token_type_hint: Option<String>,
}

#[derive(Serialize)]
struct TokenResponse {
    access_token: String,
    token_type: &'static str,
    expires_in: i64,
    scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
}

struct StoredCode {
    client_id: String,
    redirect_uri: String,
    resource: String,
    code_challenge: String,
    scope: String,
    expires_at: i64,
}

struct StoredRefresh {
    grant_id: String,
    family_id: String,
    client_id: String,
    resource: String,
    scope: String,
    expires_at: i64,
    used_at: Option<i64>,
    revoked_at: Option<i64>,
}

#[derive(Serialize)]
struct OAuthError<'a> {
    error: &'a str,
    error_description: &'a str,
}

impl OAuthServer {
    pub(super) fn new(paths: &Paths, public_url: &str, owner: Arc<WebAuth>) -> Result<Self> {
        let origin = validate_public_origin(public_url)?;
        let resource = format!("{origin}{RESOURCE_PATH}");
        let database = paths.root.join(DATABASE_FILE);
        let server = Self {
            origin,
            resource,
            database,
            owner,
            pending: Mutex::new(PendingState::default()),
            work_limit: Arc::new(tokio::sync::Semaphore::new(16)),
        };
        let connection = server.open_database(true)?;
        initialize_schema(&connection)?;
        prune_database(&connection, now_seconds()?)?;
        Ok(server)
    }

    pub(super) fn resource(&self) -> &str {
        &self.resource
    }

    pub(super) fn origin(&self) -> &str {
        &self.origin
    }

    pub(super) fn challenge(&self) -> String {
        format!(
            "Bearer resource_metadata=\"{}/.well-known/oauth-protected-resource/mcp\", scope=\"memex:read\"",
            self.origin
        )
    }

    pub(super) fn authorize_access(&self, token: &str) -> Result<bool> {
        let hash = token_hash(token);
        let now = now_seconds()?;
        let connection = self.open_database(false)?;
        Ok(connection
            .query_row(
                "select 1
                   from oauth_access_tokens a
                   join oauth_grants g on g.id = a.grant_id
                  where a.token_hash = ?1
                    and a.expires_at > ?2
                    and g.revoked_at is null
                    and g.resource = ?3
                    and (' ' || g.scope || ' ') like '% memex:read %'",
                params![hash.as_slice(), now, self.resource],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    pub(super) fn router(self: Arc<Self>) -> Router {
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods([Method::GET, Method::POST])
            .allow_headers(Any);
        let interoperable = Router::new()
            .route(
                "/.well-known/oauth-protected-resource",
                get(protected_resource_metadata),
            )
            .route(
                "/.well-known/oauth-protected-resource/mcp",
                get(protected_resource_metadata),
            )
            .route(
                "/.well-known/oauth-authorization-server",
                get(authorization_server_metadata),
            )
            .route("/oauth/register", post(register))
            .route("/oauth/token", post(token))
            .route("/oauth/revoke", post(revoke))
            .layer(cors);
        let approval =
            Router::new().route("/oauth/authorize", get(authorize_get).post(authorize_post));

        let security_state = Arc::clone(&self);
        interoperable
            .merge(approval)
            .layer(DefaultBodyLimit::max(32 * 1024))
            .layer(middleware::from_fn_with_state(
                security_state,
                validate_host_and_add_security_headers,
            ))
            .with_state(self)
    }

    fn open_database(&self, create: bool) -> Result<Connection> {
        open_database_path(&self.database, create)
    }

    fn database_permit(self: &Arc<Self>) -> Option<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.work_limit).try_acquire_owned().ok()
    }

    fn insert_pending(&self, request: PendingRequest) -> Result<(String, String)> {
        let now = Instant::now();
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pending
            .requests
            .retain(|_, request| now.duration_since(request.created_at) <= PENDING_TTL);
        if pending.requests.len() >= MAX_PENDING
            && let Some(oldest) = pending
                .requests
                .iter()
                .min_by_key(|(_, request)| request.created_at)
                .map(|(request_id, _)| request_id.clone())
        {
            pending.requests.remove(&oldest);
        }
        let request_id = random_token()?;
        let html = approval_html(&request, &request_id);
        pending.requests.insert(request_id.clone(), request);
        Ok((request_id, html))
    }

    fn take_pending(&self, request_id: &str) -> Option<PendingRequest> {
        let now = Instant::now();
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pending
            .requests
            .retain(|_, request| now.duration_since(request.created_at) <= PENDING_TTL);
        pending.requests.remove(request_id)
    }

    fn approval_allowed(&self) -> bool {
        let now = Instant::now();
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while pending
            .failed_approvals
            .front()
            .is_some_and(|attempt| now.duration_since(*attempt) > FAILED_APPROVAL_WINDOW)
        {
            pending.failed_approvals.pop_front();
        }
        pending.failed_approvals.len() < MAX_FAILED_APPROVALS
    }

    fn record_failed_approval(&self) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pending.failed_approvals.push_back(Instant::now());
        while pending.failed_approvals.len() > MAX_FAILED_APPROVALS {
            pending.failed_approvals.pop_front();
        }
    }
}

pub(super) fn revoke_all(paths: &Paths) -> Result<usize> {
    let database = paths.root.join(DATABASE_FILE);
    if !database.exists() {
        return Ok(0);
    }
    let mut connection = open_database_path(&database, false)?;
    initialize_schema(&connection)?;
    let now = now_seconds()?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let revoked = transaction.execute(
        "update oauth_grants set revoked_at = ?1 where revoked_at is null",
        params![now],
    )?;
    transaction.execute("delete from oauth_access_tokens", [])?;
    transaction.execute("delete from oauth_refresh_tokens", [])?;
    transaction.execute("delete from oauth_codes", [])?;
    transaction.commit()?;
    Ok(revoked)
}

async fn validate_host_and_add_security_headers(
    State(server): State<Arc<OAuthServer>>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let hosts: Vec<_> = request.headers().get_all(header::HOST).iter().collect();
    let host_allowed = hosts.len() == 1
        && hosts[0].to_str().ok().is_some_and(|host| {
            host == public_authority(&server.origin) || is_loopback_authority(host)
        });
    if !host_allowed {
        return add_security_headers(
            (StatusCode::BAD_REQUEST, "host is not allowed").into_response(),
        );
    }
    add_security_headers(next.run(request).await)
}

fn add_security_headers(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

async fn protected_resource_metadata(
    State(server): State<Arc<OAuthServer>>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "resource": server.resource,
        "authorization_servers": [server.origin],
        "scopes_supported": [SCOPE_READ, SCOPE_OFFLINE],
        "bearer_methods_supported": ["header"]
    }))
}

async fn authorization_server_metadata(
    State(server): State<Arc<OAuthServer>>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "issuer": server.origin,
        "authorization_endpoint": format!("{}/oauth/authorize", server.origin),
        "token_endpoint": format!("{}/oauth/token", server.origin),
        "registration_endpoint": format!("{}/oauth/register", server.origin),
        "revocation_endpoint": format!("{}/oauth/revoke", server.origin),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "token_endpoint_auth_methods_supported": ["none"],
        "code_challenge_methods_supported": ["S256"],
        "scopes_supported": [SCOPE_READ, SCOPE_OFFLINE],
        "authorization_response_iss_parameter_supported": true
    }))
}

async fn register(
    State(server): State<Arc<OAuthServer>>,
    Json(request): Json<RegistrationRequest>,
) -> Response {
    if request.redirect_uris.is_empty()
        || request.redirect_uris.len() > 16
        || request
            .token_endpoint_auth_method
            .as_deref()
            .unwrap_or("none")
            != "none"
    {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_client_metadata",
            "public clients with redirect_uris and token_endpoint_auth_method none are required",
        );
    }
    let mut redirects = Vec::with_capacity(request.redirect_uris.len());
    for redirect in request.redirect_uris {
        if redirect.len() > 2048 {
            return oauth_error(
                StatusCode::BAD_REQUEST,
                "invalid_redirect_uri",
                "redirect_uri is too long",
            );
        }
        match validate_redirect_uri(&redirect) {
            Ok(redirect) if !redirects.contains(&redirect) => redirects.push(redirect),
            Ok(_) => {}
            Err(_) => {
                return oauth_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_redirect_uri",
                    "redirect_uris must contain absolute URLs without fragments or userinfo",
                );
            }
        }
    }
    if redirects.is_empty() {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_redirect_uri",
            "at least one redirect_uri is required",
        );
    }
    let client_name = request
        .client_name
        .unwrap_or_else(|| "OAuth client".to_owned());
    if client_name.trim().is_empty() || client_name.chars().count() > 200 {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "invalid_client_metadata",
            "client_name must be between 1 and 200 characters",
        );
    }
    let client_id = match random_token() {
        Ok(value) => format!("memex_{value}"),
        Err(error) => return internal_error(error),
    };
    let stored_redirects = match serde_json::to_string(&redirects) {
        Ok(value) => value,
        Err(error) => return internal_error(error.into()),
    };
    let response = RegistrationResponse {
        client_id: client_id.clone(),
        client_name: client_name.clone(),
        redirect_uris: redirects,
        token_endpoint_auth_method: "none",
    };
    let Some(database_permit) = server.database_permit() else {
        return database_busy();
    };
    let server_for_db = Arc::clone(&server);
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let _database_permit = database_permit;
        let mut connection = server_for_db.open_database(false)?;
        let now = now_seconds()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        prune_database_tx(&transaction, now)?;
        prune_unapproved_clients_tx(&transaction, now)?;
        let mut count: i64 =
            transaction.query_row("select count(*) from oauth_clients", [], |row| row.get(0))?;
        if count >= MAX_CLIENTS {
            transaction.execute(
                "delete from oauth_clients
                  where client_id = (
                    select c.client_id from oauth_clients c
                     where c.approved_at is null
                       and not exists (select 1 from oauth_codes x where x.client_id = c.client_id)
                     order by c.created_at asc limit 1
                  )",
                [],
            )?;
            count = transaction
                .query_row("select count(*) from oauth_clients", [], |row| row.get(0))?;
        }
        ensure!(count < MAX_CLIENTS, "client registration limit reached");
        transaction.execute(
            "insert into oauth_clients (client_id, client_name, redirect_uris, created_at)
             values (?1, ?2, ?3, ?4)",
            params![client_id, client_name, stored_redirects, now],
        )?;
        transaction.commit()?;
        Ok(())
    })
    .await;
    match result {
        Ok(Ok(())) => (StatusCode::CREATED, Json(response)).into_response(),
        Ok(Err(error)) if error.to_string().contains("limit reached") => oauth_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "temporarily_unavailable",
            "client registration limit reached",
        ),
        Ok(Err(error)) => internal_error(error),
        Err(error) => internal_error(error.into()),
    }
}

async fn authorize_get(
    State(server): State<Arc<OAuthServer>>,
    headers: HeaderMap,
    Query(query): Query<AuthorizeQuery>,
) -> Response {
    if !valid_request_origin(&headers, server.origin()) {
        return (StatusCode::FORBIDDEN, "origin is not allowed").into_response();
    }
    if query.client_id.len() > 128
        || query.redirect_uri.len() > 2048
        || query.resource.len() > 2048
        || query
            .scope
            .as_deref()
            .is_some_and(|scope| scope.len() > 256)
        || query
            .state
            .as_deref()
            .is_some_and(|state| state.len() > 2048)
    {
        return authorization_error("authorization parameter is too long");
    }
    if query.response_type != "code"
        || query.code_challenge_method != "S256"
        || !valid_code_challenge(&query.code_challenge)
        || query.resource != server.resource
    {
        return authorization_error("invalid authorization request");
    }
    let scope = match normalize_scope(query.scope.as_deref()) {
        Ok(scope) => scope,
        Err(_) => return authorization_error("unsupported scope"),
    };
    let client_id = query.client_id.clone();
    let Some(database_permit) = server.database_permit() else {
        return database_busy();
    };
    let server_for_db = Arc::clone(&server);
    let client = tokio::task::spawn_blocking(move || -> Result<Option<(String, Vec<String>)>> {
        let _database_permit = database_permit;
        let connection = server_for_db.open_database(false)?;
        connection
            .query_row(
                "select client_name, redirect_uris from oauth_clients where client_id = ?1",
                params![client_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
            .map(|(name, redirects)| Ok((name, serde_json::from_str(&redirects)?)))
            .transpose()
    })
    .await;
    let (client_name, redirects) = match client {
        Ok(Ok(Some(client))) => client,
        Ok(Ok(None)) => return authorization_error("unknown client_id"),
        Ok(Err(error)) => return internal_error(error),
        Err(error) => return internal_error(error.into()),
    };
    if !redirects
        .iter()
        .any(|redirect| redirect == &query.redirect_uri)
    {
        return authorization_error("redirect_uri is not registered");
    }
    let pending = PendingRequest {
        created_at: Instant::now(),
        client_id: query.client_id,
        client_name,
        redirect_uri: query.redirect_uri,
        code_challenge: query.code_challenge,
        resource: query.resource,
        scope,
        state: query.state,
    };
    let (_request_id, html) = match server.insert_pending(pending) {
        Ok(value) => value,
        Err(error) => return internal_error(error),
    };
    Html(html).into_response()
}

async fn authorize_post(
    State(server): State<Arc<OAuthServer>>,
    headers: HeaderMap,
    Form(form): Form<ApprovalForm>,
) -> Response {
    if !valid_request_origin(&headers, server.origin()) {
        return (StatusCode::FORBIDDEN, "origin is not allowed").into_response();
    }
    if form.request_id.len() != 43 || form.decision.len() > 16 || form.owner_key.len() > 256 {
        return authorization_error("approval parameter is invalid");
    }
    let Some(request) = server.take_pending(&form.request_id) else {
        return authorization_error("authorization request is invalid or expired");
    };
    if form.decision == "deny" {
        return authorization_redirect(
            &request.redirect_uri,
            &server.origin,
            request.state.as_deref(),
            [("error", "access_denied")],
        );
    }
    if form.decision != "approve" || !server.owner.authorize_bearer(form.owner_key.trim()) {
        if !server.approval_allowed() {
            return oauth_error(
                StatusCode::TOO_MANY_REQUESTS,
                "temporarily_unavailable",
                "too many failed approval attempts; try again shortly",
            );
        }
        server.record_failed_approval();
        return (StatusCode::UNAUTHORIZED, "owner key is invalid").into_response();
    }
    let code = match random_token() {
        Ok(value) => value,
        Err(error) => return internal_error(error),
    };
    let code_hash = token_hash(&code);
    let redirect_uri = request.redirect_uri.clone();
    let state = request.state.clone();
    let Some(database_permit) = server.database_permit() else {
        return database_busy();
    };
    let server_for_db = Arc::clone(&server);
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let _database_permit = database_permit;
        let mut connection = server_for_db.open_database(false)?;
        let now = now_seconds()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        prune_database_tx(&transaction, now)?;
        let count: i64 =
            transaction.query_row("select count(*) from oauth_codes", [], |row| row.get(0))?;
        ensure!(
            count < MAX_PENDING as i64,
            "authorization code limit reached"
        );
        transaction.execute(
            "insert into oauth_codes
             (code_hash, client_id, redirect_uri, resource, code_challenge, scope, expires_at)
             values (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                code_hash.as_slice(),
                request.client_id,
                request.redirect_uri,
                request.resource,
                request.code_challenge,
                request.scope,
                now + CODE_TTL_SECONDS
            ],
        )?;
        transaction.commit()?;
        Ok(())
    })
    .await;
    match result {
        Ok(Ok(())) => authorization_redirect(
            &redirect_uri,
            &server.origin,
            state.as_deref(),
            [("code", code.as_str())],
        ),
        Ok(Err(error)) => internal_error(error),
        Err(error) => internal_error(error.into()),
    }
}

async fn token(State(server): State<Arc<OAuthServer>>, Form(form): Form<TokenForm>) -> Response {
    let Some(database_permit) = server.database_permit() else {
        return database_busy();
    };
    let server_for_db = Arc::clone(&server);
    let result = tokio::task::spawn_blocking(move || {
        let _database_permit = database_permit;
        match form.grant_type.as_str() {
            "authorization_code" => exchange_code(&server_for_db, form),
            "refresh_token" => rotate_refresh(&server_for_db, form),
            _ => Err(TokenFailure::oauth(
                "unsupported_grant_type",
                "grant_type must be authorization_code or refresh_token",
            )),
        }
    })
    .await;
    match result {
        Ok(Ok(response)) => Json(response).into_response(),
        Ok(Err(error)) => error.into_response(),
        Err(error) => internal_error(error.into()),
    }
}

async fn revoke(State(server): State<Arc<OAuthServer>>, Form(form): Form<RevokeForm>) -> Response {
    let _hint = form.token_type_hint;
    let Some(database_permit) = server.database_permit() else {
        return database_busy();
    };
    let server_for_db = Arc::clone(&server);
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let _database_permit = database_permit;
        let hash = token_hash(&form.token);
        let mut connection = server_for_db.open_database(false)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let grant: Option<(String, String)> = transaction
            .query_row(
                "select g.id, g.client_id
                   from oauth_access_tokens a join oauth_grants g on g.id = a.grant_id
                  where a.token_hash = ?1
                 union all
                 select g.id, g.client_id
                   from oauth_refresh_tokens r join oauth_grants g on g.id = r.grant_id
                  where r.token_hash = ?1 limit 1",
                params![hash.as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((grant_id, _)) = grant.filter(|(_, client)| client == &form.client_id) {
            transaction.execute(
                "update oauth_grants set revoked_at = ?1 where id = ?2",
                params![now_seconds()?, grant_id],
            )?;
            transaction.execute(
                "delete from oauth_access_tokens where grant_id = ?1",
                params![grant_id],
            )?;
            transaction.execute(
                "delete from oauth_refresh_tokens where grant_id = ?1",
                params![grant_id],
            )?;
        }
        transaction.commit()?;
        Ok(())
    })
    .await;
    match result {
        Ok(Ok(())) => StatusCode::OK.into_response(),
        Ok(Err(error)) => internal_error(error),
        Err(error) => internal_error(error.into()),
    }
}

struct TokenFailure {
    error: &'static str,
    description: &'static str,
    internal: Option<anyhow::Error>,
}

impl TokenFailure {
    fn oauth(error: &'static str, description: &'static str) -> Self {
        Self {
            error,
            description,
            internal: None,
        }
    }

    fn internal(error: impl Into<anyhow::Error>) -> Self {
        Self {
            error: "server_error",
            description: "OAuth storage operation failed",
            internal: Some(error.into()),
        }
    }

    fn into_response(self) -> Response {
        if let Some(error) = self.internal {
            internal_error(error)
        } else {
            oauth_error(StatusCode::BAD_REQUEST, self.error, self.description)
        }
    }
}

impl From<anyhow::Error> for TokenFailure {
    fn from(value: anyhow::Error) -> Self {
        Self::internal(value)
    }
}

impl From<rusqlite::Error> for TokenFailure {
    fn from(value: rusqlite::Error) -> Self {
        Self::internal(value)
    }
}

fn exchange_code(
    server: &OAuthServer,
    form: TokenForm,
) -> std::result::Result<TokenResponse, TokenFailure> {
    let code = form
        .code
        .ok_or_else(|| TokenFailure::oauth("invalid_request", "code is required"))?;
    let redirect_uri = form
        .redirect_uri
        .ok_or_else(|| TokenFailure::oauth("invalid_request", "redirect_uri is required"))?;
    let verifier = form
        .code_verifier
        .ok_or_else(|| TokenFailure::oauth("invalid_request", "code_verifier is required"))?;
    let resource = form
        .resource
        .ok_or_else(|| TokenFailure::oauth("invalid_target", "resource is required"))?;
    if resource != server.resource || !valid_code_verifier(&verifier) {
        return Err(TokenFailure::oauth(
            "invalid_grant",
            "authorization code is invalid",
        ));
    }
    let code_hash = token_hash(&code);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let access_token = random_token()?;
    let access_hash = token_hash(&access_token);
    let refresh_token = random_token()?;
    let refresh_hash = token_hash(&refresh_token);
    let grant_id = random_token()?;
    let family_id = random_token()?;
    let now = now_seconds()?;
    let mut connection = server.open_database(false)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    prune_database_tx(&transaction, now)?;
    let stored: Option<StoredCode> = transaction
        .query_row(
            "select client_id, redirect_uri, resource, code_challenge, scope, expires_at
               from oauth_codes where code_hash = ?1",
            params![code_hash.as_slice()],
            |row| {
                Ok(StoredCode {
                    client_id: row.get(0)?,
                    redirect_uri: row.get(1)?,
                    resource: row.get(2)?,
                    code_challenge: row.get(3)?,
                    scope: row.get(4)?,
                    expires_at: row.get(5)?,
                })
            },
        )
        .optional()?;
    let Some(stored) = stored else {
        return Err(TokenFailure::oauth(
            "invalid_grant",
            "authorization code is invalid",
        ));
    };
    if stored.client_id != form.client_id
        || stored.redirect_uri != redirect_uri
        || stored.resource != resource
        || stored.code_challenge != challenge
        || stored.expires_at <= now
    {
        return Err(TokenFailure::oauth(
            "invalid_grant",
            "authorization code is invalid",
        ));
    }
    let count: i64 = transaction.query_row(
        "select count(*) from oauth_grants where revoked_at is null",
        [],
        |row| row.get(0),
    )?;
    if count >= MAX_GRANTS {
        return Err(TokenFailure::oauth(
            "temporarily_unavailable",
            "active grant limit reached",
        ));
    }
    let mut total: i64 =
        transaction.query_row("select count(*) from oauth_grants", [], |row| row.get(0))?;
    if total >= MAX_GRANTS {
        transaction.execute(
            "delete from oauth_grants where id = (
                select id from oauth_grants where revoked_at is not null
                order by revoked_at asc limit 1
            )",
            [],
        )?;
        total = transaction.query_row("select count(*) from oauth_grants", [], |row| row.get(0))?;
    }
    if total >= MAX_GRANTS {
        return Err(TokenFailure::oauth(
            "temporarily_unavailable",
            "grant storage limit reached",
        ));
    }
    transaction.execute(
        "delete from oauth_codes where code_hash = ?1",
        params![code_hash.as_slice()],
    )?;
    transaction.execute(
        "update oauth_clients set approved_at = coalesce(approved_at, ?1) where client_id = ?2",
        params![now, stored.client_id],
    )?;
    transaction.execute(
        "insert into oauth_grants (id, family_id, client_id, resource, scope, created_at, revoked_at)
         values (?1, ?2, ?3, ?4, ?5, ?6, null)",
        params![grant_id, family_id, stored.client_id, resource, stored.scope, now],
    )?;
    transaction.execute(
        "insert into oauth_access_tokens (token_hash, grant_id, expires_at) values (?1, ?2, ?3)",
        params![access_hash.as_slice(), grant_id, now + ACCESS_TTL_SECONDS],
    )?;
    let issue_refresh = has_scope(&stored.scope, SCOPE_OFFLINE);
    if issue_refresh {
        transaction.execute(
            "insert into oauth_refresh_tokens
             (token_hash, grant_id, family_id, client_id, resource, scope, expires_at, used_at)
             values (?1, ?2, ?3, ?4, ?5, ?6, ?7, null)",
            params![
                refresh_hash.as_slice(),
                grant_id,
                family_id,
                stored.client_id,
                resource,
                stored.scope,
                now + REFRESH_TTL_SECONDS
            ],
        )?;
    }
    transaction.commit()?;
    Ok(TokenResponse {
        access_token,
        token_type: "Bearer",
        expires_in: ACCESS_TTL_SECONDS,
        scope: stored.scope,
        refresh_token: issue_refresh.then_some(refresh_token),
    })
}

fn rotate_refresh(
    server: &OAuthServer,
    form: TokenForm,
) -> std::result::Result<TokenResponse, TokenFailure> {
    let presented = form
        .refresh_token
        .ok_or_else(|| TokenFailure::oauth("invalid_request", "refresh_token is required"))?;
    let hash = token_hash(&presented);
    let access_token = random_token()?;
    let access_hash = token_hash(&access_token);
    let refresh_token = random_token()?;
    let refresh_hash = token_hash(&refresh_token);
    let now = now_seconds()?;
    let mut connection = server.open_database(false)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    prune_database_tx(&transaction, now)?;
    let stored: Option<StoredRefresh> = transaction
        .query_row(
            "select r.grant_id, r.family_id, r.client_id, r.resource, r.scope,
                    r.expires_at, r.used_at, g.revoked_at
               from oauth_refresh_tokens r join oauth_grants g on g.id = r.grant_id
              where r.token_hash = ?1",
            params![hash.as_slice()],
            |row| {
                Ok(StoredRefresh {
                    grant_id: row.get(0)?,
                    family_id: row.get(1)?,
                    client_id: row.get(2)?,
                    resource: row.get(3)?,
                    scope: row.get(4)?,
                    expires_at: row.get(5)?,
                    used_at: row.get(6)?,
                    revoked_at: row.get(7)?,
                })
            },
        )
        .optional()?;
    let Some(stored) = stored else {
        return Err(TokenFailure::oauth(
            "invalid_grant",
            "refresh token is invalid",
        ));
    };
    // Authenticate the public client binding before treating a used token as a
    // replay. An unrelated caller cannot revoke a family merely by presenting
    // a leaked token hash with a mismatched client or resource.
    if stored.client_id != form.client_id
        || stored.resource != server.resource
        || form.resource.as_deref() != Some(stored.resource.as_str())
    {
        return Err(TokenFailure::oauth(
            "invalid_grant",
            "refresh token is invalid",
        ));
    }
    if stored.used_at.is_some() {
        transaction.execute(
            "update oauth_grants set revoked_at = ?1 where family_id = ?2",
            params![now, stored.family_id],
        )?;
        transaction.execute(
            "delete from oauth_access_tokens where grant_id in (select id from oauth_grants where family_id = ?1)",
            params![stored.family_id],
        )?;
        transaction.execute(
            "delete from oauth_refresh_tokens where family_id = ?1",
            params![stored.family_id],
        )?;
        transaction.commit()?;
        return Err(TokenFailure::oauth(
            "invalid_grant",
            "refresh token reuse detected",
        ));
    }
    if stored.revoked_at.is_some() || stored.expires_at <= now {
        return Err(TokenFailure::oauth(
            "invalid_grant",
            "refresh token is invalid or expired",
        ));
    }
    let family_size: i64 = transaction.query_row(
        "select count(*) from oauth_refresh_tokens where family_id = ?1",
        params![stored.family_id],
        |row| row.get(0),
    )?;
    if family_size >= MAX_REFRESH_TOKENS_PER_FAMILY {
        return Err(TokenFailure::oauth(
            "temporarily_unavailable",
            "refresh rotation limit reached; reconnect this client",
        ));
    }
    let requested_scope = match form.scope.as_deref() {
        Some(scope) => normalize_scope(Some(scope))
            .map_err(|_| TokenFailure::oauth("invalid_scope", "scope is invalid"))?,
        None => stored.scope.clone(),
    };
    if !scope_is_subset(&requested_scope, &stored.scope) {
        return Err(TokenFailure::oauth(
            "invalid_scope",
            "scope cannot be expanded",
        ));
    }
    transaction.execute(
        "update oauth_refresh_tokens set used_at = ?1 where token_hash = ?2 and used_at is null",
        params![now, hash.as_slice()],
    )?;
    transaction.execute(
        "delete from oauth_access_tokens where grant_id = ?1",
        params![stored.grant_id],
    )?;
    transaction.execute(
        "insert into oauth_access_tokens (token_hash, grant_id, expires_at) values (?1, ?2, ?3)",
        params![
            access_hash.as_slice(),
            stored.grant_id,
            now + ACCESS_TTL_SECONDS
        ],
    )?;
    transaction.execute(
        "insert into oauth_refresh_tokens
         (token_hash, grant_id, family_id, client_id, resource, scope, expires_at, used_at)
         values (?1, ?2, ?3, ?4, ?5, ?6, ?7, null)",
        params![
            refresh_hash.as_slice(),
            stored.grant_id,
            stored.family_id,
            stored.client_id,
            stored.resource,
            requested_scope,
            now + REFRESH_TTL_SECONDS
        ],
    )?;
    transaction.commit()?;
    Ok(TokenResponse {
        access_token,
        token_type: "Bearer",
        expires_in: ACCESS_TTL_SECONDS,
        scope: requested_scope,
        refresh_token: Some(refresh_token),
    })
}

fn initialize_schema(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "pragma foreign_keys = on;
         create table if not exists oauth_clients (
             client_id text primary key,
             client_name text not null,
             redirect_uris text not null,
             created_at integer not null,
             approved_at integer
         );
         create table if not exists oauth_codes (
             code_hash blob primary key,
             client_id text not null references oauth_clients(client_id) on delete cascade,
             redirect_uri text not null,
             resource text not null,
             code_challenge text not null,
             scope text not null,
             expires_at integer not null
         );
         create table if not exists oauth_grants (
             id text primary key,
             family_id text not null unique,
             client_id text not null references oauth_clients(client_id),
             resource text not null,
             scope text not null,
             created_at integer not null,
             revoked_at integer
         );
         create table if not exists oauth_access_tokens (
             token_hash blob primary key,
             grant_id text not null references oauth_grants(id) on delete cascade,
             expires_at integer not null
         );
         create table if not exists oauth_refresh_tokens (
             token_hash blob primary key,
             grant_id text not null references oauth_grants(id) on delete cascade,
             family_id text not null,
             client_id text not null,
             resource text not null,
             scope text not null,
             expires_at integer not null,
             used_at integer
         );
         create index if not exists oauth_access_expiry on oauth_access_tokens(expires_at);
         create index if not exists oauth_refresh_expiry on oauth_refresh_tokens(expires_at);
         create index if not exists oauth_refresh_family on oauth_refresh_tokens(family_id);",
    )?;
    let has_approved_at: bool = connection.query_row(
        "select exists(
            select 1 from pragma_table_info('oauth_clients') where name = 'approved_at'
        )",
        [],
        |row| row.get(0),
    )?;
    if !has_approved_at {
        connection.execute(
            "alter table oauth_clients add column approved_at integer",
            [],
        )?;
        connection.execute(
            "update oauth_clients set approved_at = created_at
              where exists (select 1 from oauth_grants g where g.client_id = oauth_clients.client_id)",
            [],
        )?;
    }
    Ok(())
}

fn prune_database(connection: &Connection, now: i64) -> Result<()> {
    connection.execute(
        "delete from oauth_codes where expires_at <= ?1",
        params![now],
    )?;
    connection.execute(
        "delete from oauth_access_tokens where expires_at <= ?1",
        params![now],
    )?;
    connection.execute(
        "delete from oauth_refresh_tokens where expires_at <= ?1",
        params![now],
    )?;
    connection.execute(
        "delete from oauth_grants
          where (revoked_at is not null and revoked_at <= ?1 - ?2)
             or (not exists (select 1 from oauth_access_tokens a where a.grant_id = oauth_grants.id)
                 and not exists (select 1 from oauth_refresh_tokens r where r.grant_id = oauth_grants.id))",
        params![now, REFRESH_TTL_SECONDS],
    )?;
    connection.execute(
        "delete from oauth_clients
          where created_at <= ?1 - ?2
            and approved_at is null
            and not exists (select 1 from oauth_codes c where c.client_id = oauth_clients.client_id)",
        params![now, UNAPPROVED_CLIENT_TTL_SECONDS],
    )?;
    Ok(())
}

fn prune_database_tx(transaction: &rusqlite::Transaction<'_>, now: i64) -> Result<()> {
    transaction.execute(
        "delete from oauth_codes where expires_at <= ?1",
        params![now],
    )?;
    transaction.execute(
        "delete from oauth_access_tokens where expires_at <= ?1",
        params![now],
    )?;
    transaction.execute(
        "delete from oauth_refresh_tokens where expires_at <= ?1",
        params![now],
    )?;
    transaction.execute(
        "delete from oauth_grants
          where (revoked_at is not null and revoked_at <= ?1 - ?2)
             or (not exists (select 1 from oauth_access_tokens a where a.grant_id = oauth_grants.id)
                 and not exists (select 1 from oauth_refresh_tokens r where r.grant_id = oauth_grants.id))",
        params![now, REFRESH_TTL_SECONDS],
    )?;
    Ok(())
}

fn prune_unapproved_clients_tx(transaction: &rusqlite::Transaction<'_>, now: i64) -> Result<()> {
    transaction.execute(
        "delete from oauth_clients
          where created_at <= ?1 - ?2
            and approved_at is null
            and not exists (select 1 from oauth_codes c where c.client_id = oauth_clients.client_id)",
        params![now, UNAPPROVED_CLIENT_TTL_SECONDS],
    )?;
    Ok(())
}

fn open_database_path(path: &Path, create: bool) -> Result<Connection> {
    if create {
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("OAuth database path has no parent"))?;
        std::fs::create_dir_all(parent)?;
        if !path.exists() {
            create_private_file(path)?;
        }
    }
    validate_private_file(path)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW;
    let connection = Connection::open_with_flags(path, flags)
        .with_context(|| format!("failed to open OAuth database {}", path.display()))?;
    connection.busy_timeout(Duration::from_millis(250))?;
    connection.pragma_update(None, "foreign_keys", "on")?;
    Ok(connection)
}

#[cfg(unix)]
fn create_private_file(path: &Path) -> Result<()> {
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => {
            file.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(unix))]
fn create_private_file(path: &Path) -> Result<()> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => {
            file.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn validate_private_file(path: &Path) -> Result<()> {
    let file = open_private_read(path)?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file(),
        "OAuth database is not a regular file: {}",
        path.display()
    );
    #[cfg(unix)]
    {
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() },
            "OAuth database is not owned by the current user: {}",
            path.display()
        );
        ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "OAuth database permissions are too broad: {} (run chmod 600 {})",
            path.display(),
            path.display()
        );
    }
    Ok(())
}

#[cfg(unix)]
fn open_private_read(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?)
}

#[cfg(not(unix))]
fn open_private_read(path: &Path) -> Result<File> {
    Ok(OpenOptions::new().read(true).open(path)?)
}

fn validate_public_origin(value: &str) -> Result<String> {
    let url = Url::parse(value).context("public URL must be an absolute URL")?;
    ensure!(
        url.scheme() == "https" || (url.scheme() == "http" && is_loopback_host(&url)),
        "public URL must use HTTPS (HTTP is allowed only for loopback development)"
    );
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "public URL must not contain userinfo"
    );
    ensure!(url.host_str().is_some(), "public URL must contain a host");
    ensure!(
        url.path() == "/" && url.query().is_none() && url.fragment().is_none(),
        "public URL must be a base origin without a path, query, or fragment"
    );
    Ok(url.origin().ascii_serialization())
}

fn is_loopback_host(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        None => false,
    }
}

fn public_authority(origin: &str) -> &str {
    origin
        .split_once("://")
        .map(|(_, authority)| authority)
        .unwrap_or(origin)
}

fn is_loopback_authority(authority: &str) -> bool {
    Url::parse(&format!("http://{authority}")).is_ok_and(|url| {
        is_loopback_host(&url)
            && url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none()
    })
}

fn valid_request_origin(headers: &HeaderMap, expected: &str) -> bool {
    let origins: Vec<_> = headers.get_all(header::ORIGIN).iter().collect();
    origins.len() <= 1
        && origins
            .first()
            .is_none_or(|origin| origin.to_str().ok() == Some(expected))
}

fn validate_redirect_uri(value: &str) -> Result<String> {
    let url = Url::parse(value).context("redirect_uri must be an absolute URL")?;
    ensure!(
        url.scheme() == "https" || (url.scheme() == "http" && is_loopback_host(&url)),
        "redirect_uri must use HTTPS (HTTP is allowed only on loopback)"
    );
    ensure!(url.host_str().is_some(), "redirect_uri must contain a host");
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "redirect_uri must not contain userinfo"
    );
    ensure!(
        url.fragment().is_none(),
        "redirect_uri must not contain a fragment"
    );
    Ok(url.to_string())
}

fn normalize_scope(value: Option<&str>) -> Result<String> {
    let value = value.unwrap_or(SCOPE_READ);
    let mut scopes = Vec::new();
    for scope in value.split_ascii_whitespace() {
        ensure!(
            scope == SCOPE_READ || scope == SCOPE_OFFLINE,
            "unsupported scope"
        );
        if !scopes.contains(&scope) {
            scopes.push(scope);
        }
    }
    ensure!(
        !scopes.is_empty() && scopes.contains(&SCOPE_READ),
        "memex:read is required"
    );
    Ok(scopes.join(" "))
}

fn scope_is_subset(requested: &str, available: &str) -> bool {
    requested
        .split_ascii_whitespace()
        .all(|scope| has_scope(available, scope))
}

fn has_scope(scopes: &str, expected: &str) -> bool {
    scopes
        .split_ascii_whitespace()
        .any(|scope| scope == expected)
}

fn valid_code_challenge(value: &str) -> bool {
    value.len() == 43
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn valid_code_verifier(value: &str) -> bool {
    (43..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~'))
}

fn random_token() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|error| anyhow!("secure random generation failed: {error}"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn token_hash(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

fn now_seconds() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs()
        .try_into()
        .context("timestamp overflow")
}

fn approval_html(request: &PendingRequest, request_id: &str) -> String {
    format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Approve Memex access</title><style>body{{font:16px system-ui,sans-serif;max-width:42rem;margin:4rem auto;padding:0 1.25rem;color:#171717}}main{{border:1px solid #ddd;border-radius:12px;padding:1.5rem}}code{{overflow-wrap:anywhere}}label{{display:block;margin:1.25rem 0 .5rem}}input{{box-sizing:border-box;width:100%;padding:.7rem}}.actions{{display:flex;gap:.75rem;margin-top:1rem}}button{{padding:.7rem 1rem}}</style></head><body><main><h1>Approve Memex access</h1><p>App label supplied by the client: <strong>{}</strong></p><p>This grants read access to your full Memex history, including configured remote history.</p><p>After approval, your browser will return to this exact destination:</p><p><code>{}</code></p><form method="post" action="/oauth/authorize"><input type="hidden" name="request_id" value="{}"><label for="owner_key">Owner key</label><input id="owner_key" name="owner_key" type="password" required autocomplete="off"><div class="actions"><button type="submit" name="decision" value="approve">Approve</button><button type="submit" name="decision" value="deny" formnovalidate>Deny</button></div></form></main></body></html>"#,
        html_escape(&request.client_name),
        html_escape(&request.redirect_uri),
        html_escape(request_id)
    )
}

fn html_escape(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '&' => "&amp;".to_owned(),
            '<' => "&lt;".to_owned(),
            '>' => "&gt;".to_owned(),
            '"' => "&quot;".to_owned(),
            '\'' => "&#39;".to_owned(),
            _ => character.to_string(),
        })
        .collect()
}

fn authorization_redirect<const N: usize>(
    redirect_uri: &str,
    issuer: &str,
    state: Option<&str>,
    values: [(&str, &str); N],
) -> Response {
    let Ok(mut redirect) = Url::parse(redirect_uri) else {
        return authorization_error("stored redirect URI is invalid");
    };
    {
        let mut query = redirect.query_pairs_mut();
        for (key, value) in values {
            query.append_pair(key, value);
        }
        if let Some(state) = state {
            query.append_pair("state", state);
        }
        query.append_pair("iss", issuer);
    }
    Redirect::to(redirect.as_str()).into_response()
}

fn authorization_error(description: &'static str) -> Response {
    oauth_error(StatusCode::BAD_REQUEST, "invalid_request", description)
}

fn database_busy() -> Response {
    oauth_error(
        StatusCode::TOO_MANY_REQUESTS,
        "temporarily_unavailable",
        "too many concurrent OAuth storage operations",
    )
}

fn oauth_error(status: StatusCode, error: &'static str, description: &'static str) -> Response {
    (
        status,
        Json(OAuthError {
            error,
            error_description: description,
        }),
    )
        .into_response()
}

fn internal_error(error: anyhow::Error) -> Response {
    eprintln!("OAuth request failed: {error:#}");
    oauth_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "server_error",
        "OAuth request could not be completed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn public_origin_requires_https_or_loopback_http() {
        assert_eq!(
            validate_public_origin("https://example.com/").unwrap(),
            "https://example.com"
        );
        assert_eq!(
            validate_public_origin("http://127.0.0.1:4567").unwrap(),
            "http://127.0.0.1:4567"
        );
        assert!(validate_public_origin("http://example.com").is_err());
        assert!(validate_public_origin("https://example.com/base").is_err());
        assert!(validate_public_origin("https://user@example.com").is_err());
        assert!(validate_redirect_uri("javascript:alert(1)").is_err());
        assert!(validate_redirect_uri("http://example.com/callback").is_err());
        assert!(validate_redirect_uri("http://127.0.0.1:4567/callback").is_ok());
    }

    #[test]
    fn scope_normalization_requires_read_and_never_expands() {
        assert_eq!(normalize_scope(None).unwrap(), SCOPE_READ);
        assert_eq!(
            normalize_scope(Some("offline_access memex:read")).unwrap(),
            "offline_access memex:read"
        );
        assert!(normalize_scope(Some("offline_access")).is_err());
        assert!(normalize_scope(Some("memex:write")).is_err());
        assert!(scope_is_subset(SCOPE_READ, "memex:read offline_access"));
        assert!(!scope_is_subset("memex:read offline_access", SCOPE_READ));
    }

    #[test]
    fn approval_html_escapes_client_controlled_values() {
        let request = PendingRequest {
            created_at: Instant::now(),
            client_id: "client".into(),
            client_name: "<script>".into(),
            redirect_uri: "https://example.com/?x=\"bad\"".into(),
            code_challenge: "x".into(),
            resource: "https://memex.example/mcp".into(),
            scope: SCOPE_READ.into(),
            state: None,
        };
        let html = approval_html(&request, "request&one");
        assert!(!html.contains("<script>"));
        assert!(html.contains("&lt;script&gt;"));
        assert!(html.contains("request&amp;one"));
    }

    #[test]
    fn pkce_syntax_matches_rfc_7636() {
        assert!(valid_code_challenge(&"a".repeat(43)));
        assert!(!valid_code_challenge(&"a".repeat(42)));
        assert!(valid_code_verifier(&format!("{}~", "a".repeat(42))));
        assert!(!valid_code_verifier("contains a space"));
    }

    #[test]
    fn access_tokens_are_bound_to_the_configured_resource() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        let owner = Arc::new(WebAuth::load_or_create(&paths).unwrap());
        let first = OAuthServer::new(&paths, "https://first.example", Arc::clone(&owner)).unwrap();
        let token = "an-opaque-access-token";
        let hash = token_hash(token);
        let now = now_seconds().unwrap();
        let connection = first.open_database(false).unwrap();
        connection
            .execute(
                "insert into oauth_clients (client_id, client_name, redirect_uris, created_at)
                 values ('client', 'Client', '[\"https://client.example/callback\"]', ?1)",
                params![now],
            )
            .unwrap();
        connection
            .execute(
                "insert into oauth_grants
                 (id, family_id, client_id, resource, scope, created_at, revoked_at)
                 values ('grant', 'family', 'client', ?1, ?2, ?3, null)",
                params![first.resource(), SCOPE_READ, now],
            )
            .unwrap();
        connection
            .execute(
                "insert into oauth_access_tokens (token_hash, grant_id, expires_at)
                 values (?1, 'grant', ?2)",
                params![hash.as_slice(), now + ACCESS_TTL_SECONDS],
            )
            .unwrap();

        assert!(first.authorize_access(token).unwrap());
        let second = OAuthServer::new(&paths, "https://second.example", owner).unwrap();
        assert!(!second.authorize_access(token).unwrap());
    }

    #[test]
    fn approved_clients_survive_revocation_and_expiry_cleanup() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        let owner = Arc::new(WebAuth::load_or_create(&paths).unwrap());
        let server = OAuthServer::new(&paths, "https://memex.example", owner.clone()).unwrap();
        let connection = server.open_database(false).unwrap();
        let old = now_seconds().unwrap() - UNAPPROVED_CLIENT_TTL_SECONDS - 1;
        connection.execute(
            "insert into oauth_clients (client_id, client_name, redirect_uris, created_at, approved_at)
             values ('approved', 'Approved', '[]', ?1, ?1), ('unapproved', 'Unapproved', '[]', ?1, null)",
            params![old],
        ).unwrap();
        revoke_all(&paths).unwrap();
        drop(server);
        let _restarted = OAuthServer::new(&paths, "https://memex.example", owner).unwrap();
        let remaining: String = connection
            .query_row("select client_id from oauth_clients", [], |row| row.get(0))
            .unwrap();
        assert_eq!(remaining, "approved");
        let count: i64 = connection
            .query_row("select count(*) from oauth_clients", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}
