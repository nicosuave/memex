use crate::analytics::{AnalyticsStore, ProjectGrouping, analytics_path};
use crate::config::{Paths, UserConfig};
use crate::index::{QueryOptions, SearchIndex};
use crate::types::SourceFilter;
use crate::usage::{CostMode, UsageQuery, scan_usage_activity};
use crate::web_auth::WebAuth;
use anyhow::{Context, Result, anyhow};
use chrono::{Duration, Utc};
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

pub const DEFAULT_LISTEN: &str = "127.0.0.1:6363";
const MAX_SESSION_OFFSET: usize = 100_000;
const UI_HTML: &str = include_str!("../web/dist/index.html");
const UI_CSS: &[u8] = include_bytes!("../web/dist/assets/app.css");
const UI_JS: &[u8] = include_bytes!("../web/dist/assets/app.js");

pub fn serve(root: Option<PathBuf>, listen: &str) -> Result<()> {
    validate_listener(listen)?;
    let paths = Paths::new(root)?;
    let auth = Arc::new(WebAuth::load_or_create(&paths)?);
    let server = bind(listen)?;
    println!("memex web UI: http://{listen}");
    serve_requests(server, paths, restrict_hosts(listen), auth);
    Ok(())
}

pub fn spawn(root: Option<PathBuf>, listen: &str) -> Result<JoinHandle<()>> {
    validate_listener(listen)?;
    let paths = Paths::new(root)?;
    let auth = Arc::new(WebAuth::load_or_create(&paths)?);
    let server = bind(listen)?;
    let listen = listen.to_string();
    let restrict_hosts = restrict_hosts(&listen);
    let handle = std::thread::Builder::new()
        .name("memex-web".to_string())
        .spawn(move || {
            println!("memex web UI: http://{listen}");
            serve_requests(server, paths, restrict_hosts, auth);
        })
        .context("failed to start web UI thread")?;
    Ok(handle)
}

fn bind(listen: &str) -> Result<Server> {
    Server::http(listen).map_err(|err| anyhow!("failed to bind web UI to {listen}: {err}"))
}

pub fn bootstrap_url(root: Option<PathBuf>, listen: &str) -> Result<String> {
    validate_listener(listen)?;
    let paths = Paths::new(root)?;
    let auth = WebAuth::load_or_create(&paths)?;
    let bootstrap = auth.create_bootstrap_token()?;
    let (host, port) = listen_host_port(listen)?;
    let browser_host = match host {
        "127.0.0.1" | "localhost" => "localhost".to_string(),
        "::1" => "[::1]".to_string(),
        _ => unreachable!("listener validation rejected unsupported loopback host"),
    };
    Ok(format!(
        "http://{browser_host}:{port}/#bootstrap={bootstrap}"
    ))
}

fn serve_requests(server: Server, paths: Paths, restrict_hosts: bool, auth: Arc<WebAuth>) {
    for request in server.incoming_requests() {
        let request_paths = paths.clone();
        let request_auth = Arc::clone(&auth);
        std::thread::spawn(move || {
            if let Err(err) = handle_request(request, &request_paths, restrict_hosts, &request_auth)
            {
                eprintln!("web UI request failed: {err:#}");
            }
        });
    }
}

fn handle_request(
    request: Request,
    paths: &Paths,
    restrict_hosts: bool,
    auth: &WebAuth,
) -> Result<()> {
    if restrict_hosts && !has_local_host(&request) {
        return respond_text(request, StatusCode(403), "invalid host", "text/plain");
    }

    let parsed = match parse_url(request.url()) {
        Ok(url) => url,
        Err(err) => {
            return respond_json_error(request, StatusCode(400), &err.to_string());
        }
    };

    if parsed.path() == "/auth/exchange" {
        if request.method() != &Method::Post {
            return respond_text(request, StatusCode(405), "method not allowed", "text/plain");
        }
        let Some(token) = bearer_token(&request) else {
            return respond_unauthorized(request);
        };
        let session = match auth.exchange_bootstrap_token(token) {
            Ok(session) => session,
            Err(_) => return respond_unauthorized(request),
        };
        return respond_json(
            request,
            StatusCode(200),
            &SessionTokenPayload { token: &session },
        );
    }

    if request.method() != &Method::Get && request.method() != &Method::Head {
        return respond_text(request, StatusCode(405), "method not allowed", "text/plain");
    }
    if (parsed.path() == "/api" || parsed.path().starts_with("/api/"))
        && !request_is_authorized(&request, auth)
    {
        return respond_unauthorized(request);
    }

    match parsed.path() {
        "/" => respond(
            request,
            StatusCode(200),
            UI_HTML.as_bytes().to_vec(),
            "text/html; charset=utf-8",
            true,
        ),
        "/assets/app.css" => respond(
            request,
            StatusCode(200),
            UI_CSS.to_vec(),
            "text/css; charset=utf-8",
            false,
        ),
        "/assets/app.js" => respond(
            request,
            StatusCode(200),
            UI_JS.to_vec(),
            "application/javascript; charset=utf-8",
            false,
        ),
        "/healthz" => respond_text(request, StatusCode(200), "ok", "text/plain"),
        "/api/stats" => match stats_payload(paths) {
            Ok(payload) => respond_json(request, StatusCode(200), &payload),
            Err(err) => respond_json_error(request, StatusCode(503), &err.to_string()),
        },
        "/api/search" => match SearchRequest::from_url(&parsed) {
            Ok(params) => match search_payload(paths, &params) {
                Ok(payload) => respond_json(request, StatusCode(200), &payload),
                Err(err) => respond_json_error(request, StatusCode(503), &err.to_string()),
            },
            Err(err) => respond_json_error(request, StatusCode(400), &err.to_string()),
        },
        "/api/activity" => match ActivityRequest::from_url(&parsed) {
            Ok(params) => match activity_payload(paths, &params) {
                Ok(payload) => respond_json(request, StatusCode(200), &payload),
                Err(err) => respond_json_error(request, StatusCode(503), &err.to_string()),
            },
            Err(err) => respond_json_error(request, StatusCode(400), &err.to_string()),
        },
        "/api/session" => match SessionRequest::from_url(&parsed) {
            Ok(params) => match session_payload(paths, &params) {
                Ok(Some(payload)) => respond_json(request, StatusCode(200), &payload),
                Ok(None) => respond_json_error(request, StatusCode(404), "session not found"),
                Err(err) => respond_json_error(request, StatusCode(503), &err.to_string()),
            },
            Err(err) => respond_json_error(request, StatusCode(400), &err.to_string()),
        },
        _ => respond_json_error(request, StatusCode(404), "not found"),
    }
}

fn request_is_authorized(request: &Request, auth: &WebAuth) -> bool {
    bearer_token(request)
        .is_some_and(|token| auth.authorize_bearer(token) || auth.authorize_session(token))
}

fn bearer_token(request: &Request) -> Option<&str> {
    request
        .headers()
        .iter()
        .find(|header| header.field.equiv("Authorization"))
        .and_then(|header| header.value.as_str().strip_prefix("Bearer "))
        .filter(|token| !token.is_empty())
}

fn respond_unauthorized(request: Request) -> Result<()> {
    let body = serde_json::to_vec(&ErrorPayload {
        error: "authentication required; run `memex index-service open`",
    })?;
    respond_with_headers(
        request,
        StatusCode(401),
        body,
        "application/json; charset=utf-8",
        false,
        vec![header("WWW-Authenticate", "Bearer realm=\"memex\"")?],
    )
}

#[derive(Serialize)]
struct SessionTokenPayload<'a> {
    token: &'a str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActivityMetric {
    Sessions,
    Tokens,
}

#[derive(Debug)]
struct ActivityRequest {
    metric: ActivityMetric,
    source: Option<SourceFilter>,
    project: Option<String>,
    days: i64,
}

impl ActivityRequest {
    fn from_url(url: &RequestUrl) -> Result<Self> {
        let mut metric = ActivityMetric::Sessions;
        let mut source = None;
        let mut project = None;
        let mut days = 30;
        for (key, value) in url.query_pairs() {
            match key {
                "metric" if value == "tokens" => metric = ActivityMetric::Tokens,
                "metric" if value == "sessions" || value.is_empty() => {}
                "metric" => return Err(anyhow!("unknown activity metric: {value}")),
                "source" if !value.is_empty() && value != "all" => {
                    source = Some(parse_source(value)?);
                }
                "project" if !value.trim().is_empty() => {
                    project = Some(value.trim().to_string());
                }
                "days" => {
                    days = value
                        .parse::<i64>()
                        .context("days must be a positive integer")?
                        .clamp(1, 365);
                }
                _ => {}
            }
        }
        Ok(Self {
            metric,
            source,
            project,
            days,
        })
    }
}

#[derive(Serialize)]
struct ActivityPayload {
    metric: &'static str,
    days: i64,
    token_usage_enabled: bool,
    partial: bool,
    points: Vec<ActivityPoint>,
}

#[derive(Serialize)]
struct ActivityPoint {
    date: String,
    source: String,
    value: u64,
}

fn activity_payload(paths: &Paths, params: &ActivityRequest) -> Result<ActivityPayload> {
    let config = UserConfig::load(paths)?;
    let token_usage_enabled = config.token_usage_enabled();
    let since = Utc::now() - Duration::days(params.days);
    let since_ms = since.timestamp_millis().max(0) as u64;
    let mut buckets: BTreeMap<(String, String), u64> = BTreeMap::new();

    let partial = match params.metric {
        ActivityMetric::Sessions => {
            let store = AnalyticsStore::open_read_only(analytics_path(&paths.state))?;
            for session in store.query_sessions(
                params.source,
                Some(since_ms),
                params.project.as_deref(),
                ProjectGrouping::Flat,
                None,
            )? {
                add_activity_value(&mut buckets, session.last_at, session.source.label(), 1);
            }
            false
        }
        ActivityMetric::Tokens if !token_usage_enabled => false,
        ActivityMetric::Tokens => {
            let query = UsageQuery {
                source: params.source,
                project: params.project.clone(),
                project_grouping: ProjectGrouping::Flat,
                session_keys: None,
                since_ms: Some(since_ms),
                until_ms: None,
                cost_mode: CostMode::Source,
                include_events: false,
                cache_path: Some(paths.state.join("usage-cache.sqlite3")),
                memo_ttl_ms: 60_000,
            };
            let (points, partial) = scan_usage_activity(&query)?;
            for point in points {
                add_activity_value(
                    &mut buckets,
                    point.timestamp_ms,
                    point.source,
                    point.total_tokens,
                );
            }
            partial
        }
    };

    let points = buckets
        .into_iter()
        .map(|((date, source), value)| ActivityPoint {
            date,
            source,
            value,
        })
        .collect();
    Ok(ActivityPayload {
        metric: match params.metric {
            ActivityMetric::Sessions => "sessions",
            ActivityMetric::Tokens => "tokens",
        },
        days: params.days,
        token_usage_enabled,
        partial,
        points,
    })
}

fn add_activity_value(
    buckets: &mut BTreeMap<(String, String), u64>,
    timestamp_ms: u64,
    source: &str,
    value: u64,
) {
    let Some(timestamp) = chrono::DateTime::<Utc>::from_timestamp_millis(timestamp_ms as i64)
    else {
        return;
    };
    let key = (timestamp.format("%Y-%m-%d").to_string(), source.to_string());
    let bucket = buckets.entry(key).or_default();
    *bucket = bucket.saturating_add(value);
}

#[derive(Debug)]
struct SessionRequest {
    session_id: String,
    offset: usize,
    limit: usize,
}

impl SessionRequest {
    fn from_url(url: &RequestUrl) -> Result<Self> {
        let mut session_id = None;
        let mut offset = 0;
        let mut limit = 100;
        for (key, value) in url.query_pairs() {
            match key {
                "id" if !value.is_empty() => session_id = Some(value.to_string()),
                "offset" => {
                    offset = value
                        .parse::<usize>()
                        .context("offset must be a non-negative integer")?;
                }
                "limit" => {
                    limit = value
                        .parse::<usize>()
                        .context("limit must be a positive integer")?
                        .clamp(1, 200);
                }
                _ => {}
            }
        }
        if offset > MAX_SESSION_OFFSET {
            return Err(anyhow!("offset must not exceed {MAX_SESSION_OFFSET}"));
        }
        Ok(Self {
            session_id: session_id.ok_or_else(|| anyhow!("missing session id"))?,
            offset,
            limit,
        })
    }
}

fn restrict_hosts(listen: &str) -> bool {
    let host = listen
        .rsplit_once(':')
        .map_or(listen, |(host, _)| host)
        .trim_matches(['[', ']']);
    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

pub fn validate_listener(listen: &str) -> Result<()> {
    let (host, _) = listen_host_port(listen)?;
    if !matches!(host, "localhost" | "127.0.0.1" | "::1") {
        return Err(anyhow!(
            "refusing non-loopback web listener {listen}; bind to localhost and use an authenticated TLS reverse proxy for remote access"
        ));
    }
    Ok(())
}

fn listen_host_port(listen: &str) -> Result<(&str, u16)> {
    let (host, port) = listen
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("web listener must include a port: {listen}"))?;
    let host = host.trim_matches(['[', ']']);
    if host.is_empty() {
        return Err(anyhow!("web listener must include a host: {listen}"));
    }
    let port = port
        .parse::<u16>()
        .with_context(|| format!("invalid web listener port: {listen}"))?;
    Ok((host, port))
}

fn has_local_host(request: &Request) -> bool {
    let Some(host) = request
        .headers()
        .iter()
        .find(|header| header.field.equiv("Host"))
        .map(|header| header.value.as_str())
    else {
        return false;
    };
    host == "localhost"
        || host.starts_with("localhost:")
        || host == "127.0.0.1"
        || host.starts_with("127.0.0.1:")
        || host == "[::1]"
        || host.starts_with("[::1]:")
}

struct RequestUrl {
    path: String,
    query: Vec<(String, String)>,
}

impl RequestUrl {
    fn path(&self) -> &str {
        &self.path
    }

    fn query_pairs(&self) -> impl Iterator<Item = (&str, &str)> {
        self.query
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

fn parse_url(value: &str) -> Result<RequestUrl> {
    let (path, query) = value.split_once('?').unwrap_or((value, ""));
    if !path.starts_with('/') {
        return Err(anyhow!("invalid request URL: {value}"));
    }
    let query = query
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            Ok((decode_query_component(key)?, decode_query_component(value)?))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RequestUrl {
        path: path.to_string(),
        query,
    })
}

fn decode_query_component(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let high = hex_value(bytes[index + 1])
                    .ok_or_else(|| anyhow!("invalid percent encoding"))?;
                let low = hex_value(bytes[index + 2])
                    .ok_or_else(|| anyhow!("invalid percent encoding"))?;
                decoded.push((high << 4) | low);
                index += 3;
            }
            b'%' => return Err(anyhow!("invalid percent encoding")),
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).context("query parameter is not valid UTF-8")
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Debug)]
struct SearchRequest {
    query: String,
    source: Option<SourceFilter>,
    project: Option<String>,
    offset: usize,
    limit: usize,
}

impl SearchRequest {
    fn from_url(url: &RequestUrl) -> Result<Self> {
        let mut query = String::new();
        let mut source = None;
        let mut project = None;
        let mut offset = 0;
        let mut limit = 50;

        for (key, value) in url.query_pairs() {
            match key {
                "q" => query = value.to_string(),
                "source" if !value.is_empty() && value != "all" => {
                    source = Some(parse_source(value)?);
                }
                "project" if !value.trim().is_empty() => {
                    project = Some(value.trim().to_string());
                }
                "offset" => {
                    offset = value
                        .parse::<usize>()
                        .context("offset must be a non-negative integer")?;
                }
                "limit" => {
                    limit = value
                        .parse::<usize>()
                        .context("limit must be a positive integer")?
                        .clamp(1, 100);
                }
                _ => {}
            }
        }

        Ok(Self {
            query: query.trim().to_string(),
            source,
            project,
            offset,
            limit,
        })
    }
}

fn parse_source(value: &str) -> Result<SourceFilter> {
    match value {
        "claude" => Ok(SourceFilter::Claude),
        "codex" => Ok(SourceFilter::Codex),
        "opencode" => Ok(SourceFilter::Opencode),
        "cursor" => Ok(SourceFilter::Cursor),
        "pi" => Ok(SourceFilter::Pi),
        "omp" => Ok(SourceFilter::Omp),
        "openclaw" | "open-claw" => Ok(SourceFilter::OpenClaw),
        "copilot" => Ok(SourceFilter::Copilot),
        "hermes" => Ok(SourceFilter::Hermes),
        _ => Err(anyhow!("unknown source: {value}")),
    }
}

#[derive(Serialize)]
struct StatsPayload {
    documents: usize,
}

fn stats_payload(paths: &Paths) -> Result<StatsPayload> {
    let index = open_index(paths)?;
    Ok(StatsPayload {
        documents: index.doc_count()?,
    })
}

#[derive(Serialize)]
struct SearchPayload {
    query: String,
    offset: usize,
    has_more: bool,
    results: Vec<SessionSummary>,
}

#[derive(Serialize)]
struct SessionSummary {
    session_id: String,
    project: String,
    source: String,
    role: String,
    ts: u64,
    score: Option<f32>,
    snippet: String,
}

fn search_payload(paths: &Paths, params: &SearchRequest) -> Result<SearchPayload> {
    let index = open_index(paths)?;
    let target = params.offset.saturating_add(params.limit).saturating_add(1);
    let document_count = index.doc_count()?.max(1);
    let mut candidate_limit = target.saturating_mul(4).max(100).min(document_count);

    let summaries = loop {
        let records: Vec<(Option<f32>, crate::types::Record)> = if params.query.is_empty() {
            index
                .recent_records_filtered(candidate_limit, params.source, params.project.as_deref())?
                .into_iter()
                .map(|record| (None, record))
                .collect()
        } else {
            index
                .search(&QueryOptions {
                    query: params.query.clone(),
                    project: params.project.clone(),
                    role: None,
                    tool: None,
                    session_id: None,
                    source: params.source,
                    since: None,
                    until: None,
                    limit: candidate_limit,
                })?
                .into_iter()
                .map(|(score, record)| (Some(score), record))
                .collect()
        };

        let raw_count = records.len();
        let mut seen = HashSet::new();
        let mut summaries = Vec::new();
        for (score, record) in records {
            if !seen.insert(record.session_id.clone()) {
                continue;
            }
            summaries.push(SessionSummary {
                session_id: record.session_id,
                project: record.project,
                source: record.source.label().to_string(),
                role: record.role,
                ts: record.ts,
                score,
                snippet: summarize(&record.text, 360),
            });
            if summaries.len() == target {
                break;
            }
        }

        let exhausted = raw_count < candidate_limit || candidate_limit == document_count;
        if summaries.len() >= target || exhausted {
            break summaries;
        }
        candidate_limit = candidate_limit.saturating_mul(2).min(document_count);
    };

    let has_more = summaries.len() > params.offset.saturating_add(params.limit);
    let results = summaries
        .into_iter()
        .skip(params.offset)
        .take(params.limit)
        .collect();

    Ok(SearchPayload {
        query: params.query.clone(),
        offset: params.offset,
        has_more,
        results,
    })
}

#[derive(Serialize)]
struct SessionPayload {
    session_id: String,
    project: String,
    source: String,
    started_at: u64,
    ended_at: u64,
    offset: usize,
    total: usize,
    messages: Vec<MessagePayload>,
}

#[derive(Serialize)]
struct MessagePayload {
    role: String,
    content: String,
    ts: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_name: Option<String>,
}

fn session_payload(paths: &Paths, params: &SessionRequest) -> Result<Option<SessionPayload>> {
    let index = open_index(paths)?;
    let (records, total) =
        index.records_by_session_id_page(&params.session_id, params.offset, params.limit)?;
    if total == 0 {
        return Ok(None);
    }
    if records.is_empty() {
        return Err(anyhow!("session page offset is past the end"));
    }

    let first = records.first().expect("records is not empty");
    let project = first.project.clone();
    let source = first.source.label().to_string();
    let started_at = records.iter().map(|record| record.ts).min().unwrap_or(0);
    let ended_at = records.iter().map(|record| record.ts).max().unwrap_or(0);
    let messages = records
        .into_iter()
        .map(|record| MessagePayload {
            role: record.role,
            content: record.text,
            ts: record.ts,
            tool_name: record.tool_name,
        })
        .collect();

    Ok(Some(SessionPayload {
        session_id: params.session_id.clone(),
        project,
        source,
        started_at,
        ended_at,
        offset: params.offset,
        total,
        messages,
    }))
}

fn open_index(paths: &Paths) -> Result<SearchIndex> {
    if !paths.index.join("meta.json").exists() {
        return Err(anyhow!(
            "index is not ready yet; run `memex index` or wait for the daemon"
        ));
    }
    SearchIndex::open_or_create(&paths.index)
}

fn summarize(text: &str, max_chars: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= max_chars {
        return compact;
    }
    let mut summary: String = compact.chars().take(max_chars.saturating_sub(1)).collect();
    summary.push('…');
    summary
}

fn respond_json<T: Serialize>(request: Request, status: StatusCode, value: &T) -> Result<()> {
    let body = serde_json::to_vec(value)?;
    respond(
        request,
        status,
        body,
        "application/json; charset=utf-8",
        false,
    )
}

#[derive(Serialize)]
struct ErrorPayload<'a> {
    error: &'a str,
}

fn respond_json_error(request: Request, status: StatusCode, message: &str) -> Result<()> {
    respond_json(request, status, &ErrorPayload { error: message })
}

fn respond_text(
    request: Request,
    status: StatusCode,
    body: &str,
    content_type: &str,
) -> Result<()> {
    respond(
        request,
        status,
        body.as_bytes().to_vec(),
        content_type,
        false,
    )
}

fn respond(
    request: Request,
    status: StatusCode,
    body: Vec<u8>,
    content_type: &str,
    html: bool,
) -> Result<()> {
    respond_with_headers(request, status, body, content_type, html, Vec::new())
}

fn respond_with_headers(
    request: Request,
    status: StatusCode,
    body: Vec<u8>,
    content_type: &str,
    html: bool,
    extra_headers: Vec<Header>,
) -> Result<()> {
    let is_head = request.method() == &Method::Head;
    let response_body = if is_head { Vec::new() } else { body };
    let mut response = Response::from_data(response_body)
        .with_status_code(status)
        .with_header(header("Content-Type", content_type)?)
        .with_header(header("Cache-Control", "no-store")?)
        .with_header(header("X-Content-Type-Options", "nosniff")?)
        .with_header(header("Referrer-Policy", "no-referrer")?);
    if html {
        response.add_header(header(
            "Content-Security-Policy",
            "default-src 'self'; style-src 'self'; script-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'",
        )?);
    }
    for extra_header in extra_headers {
        response.add_header(extra_header);
    }
    request.respond(response)?;
    Ok(())
}

fn header(name: &str, value: &str) -> Result<Header> {
    Header::from_bytes(name.as_bytes(), value.as_bytes())
        .map_err(|_| anyhow!("invalid HTTP header: {name}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Record, RecordLinks, SourceKind};
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpStream};
    use tempfile::TempDir;

    fn http_round_trip(paths: &Paths, auth: &WebAuth, request: String) -> String {
        let server = Server::http("127.0.0.1:0").unwrap();
        let address = server.server_addr().to_ip().unwrap();
        let client = std::thread::spawn(move || {
            let mut stream = TcpStream::connect(address).unwrap();
            stream.write_all(request.as_bytes()).unwrap();
            stream.shutdown(Shutdown::Write).unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            response
        });
        let request = server.recv().unwrap();
        handle_request(request, paths, true, auth).unwrap();
        client.join().unwrap()
    }

    #[test]
    fn search_request_decodes_and_caps_values() {
        let url = parse_url(
            "/api/search?q=error%20handling&source=openclaw&project=memex&offset=200&limit=1000",
        )
        .unwrap();
        let request = SearchRequest::from_url(&url).unwrap();

        assert_eq!(request.query, "error handling");
        assert_eq!(request.source, Some(SourceFilter::OpenClaw));
        assert_eq!(request.project.as_deref(), Some("memex"));
        assert_eq!(request.offset, 200);
        assert_eq!(request.limit, 100);
    }
    #[test]
    fn search_request_accepts_omp_source() {
        let url = parse_url("/api/search?source=omp").unwrap();
        let request = SearchRequest::from_url(&url).unwrap();

        assert_eq!(request.source, Some(SourceFilter::Omp));
    }

    #[test]
    fn session_request_decodes_and_caps_page_size() {
        let url = parse_url("/api/session?id=session-a&offset=40&limit=1000").unwrap();
        let request = SessionRequest::from_url(&url).unwrap();

        assert_eq!(request.session_id, "session-a");
        assert_eq!(request.offset, 40);
        assert_eq!(request.limit, 200);
    }

    #[test]
    fn session_request_rejects_excessive_offsets() {
        let url = parse_url(&format!(
            "/api/session?id=session-a&offset={}&limit=200",
            MAX_SESSION_OFFSET + 1
        ))
        .unwrap();

        let error = SessionRequest::from_url(&url).unwrap_err();

        assert_eq!(
            error.to_string(),
            format!("offset must not exceed {MAX_SESSION_OFFSET}")
        );
    }

    #[test]
    fn activity_request_parses_metric_filters_and_range() {
        let url =
            parse_url("/api/activity?metric=tokens&source=codex&project=memex&days=1000").unwrap();
        let request = ActivityRequest::from_url(&url).unwrap();

        assert_eq!(request.metric, ActivityMetric::Tokens);
        assert_eq!(request.source, Some(SourceFilter::Codex));
        assert_eq!(request.project.as_deref(), Some("memex"));
        assert_eq!(request.days, 365);
    }

    #[test]
    fn search_payload_groups_results_by_session() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let index = SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
        let mut writer = index.writer().unwrap();
        for (doc_id, session_id, text) in [
            (1, "session-a", "alpha one"),
            (2, "session-a", "alpha two"),
            (3, "session-b", "alpha three"),
        ] {
            index
                .add_record(
                    &mut writer,
                    &Record {
                        source: SourceKind::Claude,
                        doc_id,
                        ts: doc_id * 1_000,
                        project: "memex".to_string(),
                        session_id: session_id.to_string(),
                        turn_id: doc_id as u32,
                        role: "assistant".to_string(),
                        text: text.to_string(),
                        tool_name: None,
                        tool_input: None,
                        tool_output: None,
                        links: RecordLinks::default(),
                        source_path: "/tmp/session.jsonl".to_string(),
                    },
                )
                .unwrap();
        }
        writer.commit().unwrap();

        let payload = search_payload(
            &paths,
            &SearchRequest {
                query: "alpha".to_string(),
                source: None,
                project: Some("memex".to_string()),
                offset: 0,
                limit: 30,
            },
        )
        .unwrap();

        assert_eq!(payload.results.len(), 2);
        assert!(!payload.has_more);

        let first_search_page = search_payload(
            &paths,
            &SearchRequest {
                query: "alpha".to_string(),
                source: None,
                project: Some("memex".to_string()),
                offset: 0,
                limit: 1,
            },
        )
        .unwrap();
        assert_eq!(first_search_page.results.len(), 1);
        assert!(first_search_page.has_more);

        let second_search_page = search_payload(
            &paths,
            &SearchRequest {
                query: "alpha".to_string(),
                source: None,
                project: Some("memex".to_string()),
                offset: 1,
                limit: 1,
            },
        )
        .unwrap();
        assert_eq!(second_search_page.offset, 1);
        assert_eq!(second_search_page.results.len(), 1);
        assert!(!second_search_page.has_more);

        let page = session_payload(
            &paths,
            &SessionRequest {
                session_id: "session-a".to_string(),
                offset: 1,
                limit: 1,
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(page.total, 2);
        assert_eq!(page.offset, 1);
        assert_eq!(page.messages.len(), 1);

        let (records, total) = index
            .records_by_session_id_page("session-a", usize::MAX, 200)
            .unwrap();
        assert!(records.is_empty());
        assert_eq!(total, 2);

        let filtered = search_payload(
            &paths,
            &SearchRequest {
                query: String::new(),
                source: Some(SourceFilter::Codex),
                project: None,
                offset: 0,
                limit: 30,
            },
        )
        .unwrap();
        assert!(filtered.results.is_empty());
    }

    #[test]
    fn ui_uses_embedded_local_only_assets() {
        assert!(UI_HTML.contains("/assets/app.css"));
        assert!(UI_HTML.contains("/assets/app.js"));
        assert!(!UI_CSS.is_empty());
        assert!(!UI_JS.is_empty());
        assert!(!UI_HTML.contains("https://cdn."));
    }

    #[test]
    fn loopback_listeners_restrict_host_headers() {
        assert!(restrict_hosts(DEFAULT_LISTEN));
        assert!(restrict_hosts("[::1]:6363"));
        assert!(restrict_hosts("localhost:6363"));
        assert!(!restrict_hosts("0.0.0.0:6363"));
        assert!(!restrict_hosts("192.168.1.20:6363"));
    }

    #[test]
    fn rejects_non_loopback_listeners() {
        assert!(validate_listener(DEFAULT_LISTEN).is_ok());
        assert!(validate_listener("localhost:8080").is_ok());
        assert!(validate_listener("[::1]:6363").is_ok());
        assert!(validate_listener("127.0.0.2:6363").is_err());
        assert!(validate_listener("0.0.0.0:6363").is_err());
        assert!(validate_listener("192.168.1.20:6363").is_err());
    }

    #[test]
    fn private_api_requires_authentication_and_accepts_single_use_bootstrap_session() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
        let auth = WebAuth::load_or_create(&paths).unwrap();

        let bootstrap_page = http_round_trip(
            &paths,
            &auth,
            "GET / HTTP/1.1\r\nHost: localhost:6363\r\nConnection: close\r\n\r\n".to_string(),
        );
        assert!(bootstrap_page.starts_with("HTTP/1.1 200"));

        let health = http_round_trip(
            &paths,
            &auth,
            "GET /healthz HTTP/1.1\r\nHost: localhost:6363\r\nConnection: close\r\n\r\n"
                .to_string(),
        );
        assert!(health.starts_with("HTTP/1.1 200"));
        assert!(health.ends_with("ok"));

        let unauthorized = http_round_trip(
            &paths,
            &auth,
            "GET /api/stats HTTP/1.1\r\nHost: localhost:6363\r\nConnection: close\r\n\r\n"
                .to_string(),
        );
        assert!(unauthorized.starts_with("HTTP/1.1 401"));
        assert!(unauthorized.contains("WWW-Authenticate: Bearer realm=\"memex\""));

        let bootstrap = auth.create_bootstrap_token().unwrap();
        let exchange = http_round_trip(
            &paths,
            &auth,
            format!(
                "POST /auth/exchange HTTP/1.1\r\nHost: localhost:6363\r\nAuthorization: Bearer {bootstrap}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(exchange.starts_with("HTTP/1.1 200"));
        let body = exchange.split("\r\n\r\n").nth(1).unwrap();
        let session = serde_json::from_str::<serde_json::Value>(body).unwrap()["token"]
            .as_str()
            .unwrap()
            .to_string();

        let authorized = http_round_trip(
            &paths,
            &auth,
            format!(
                "GET /api/stats HTTP/1.1\r\nHost: localhost:6363\r\nAuthorization: Bearer {session}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(authorized.starts_with("HTTP/1.1 200"));

        let replay = http_round_trip(
            &paths,
            &auth,
            format!(
                "POST /auth/exchange HTTP/1.1\r\nHost: localhost:6363\r\nAuthorization: Bearer {bootstrap}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(replay.starts_with("HTTP/1.1 401"));
    }

    #[test]
    fn private_api_accepts_installation_bearer_token() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
        let auth = WebAuth::load_or_create(&paths).unwrap();
        let token = std::fs::read_to_string(crate::web_auth::token_path(&paths)).unwrap();

        let response = http_round_trip(
            &paths,
            &auth,
            format!(
                "GET /api/stats HTTP/1.1\r\nHost: localhost:6363\r\nAuthorization: Bearer {}\r\nConnection: close\r\n\r\n",
                token.trim()
            ),
        );
        assert!(response.starts_with("HTTP/1.1 200"));
    }
}
