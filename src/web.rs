use crate::analytics::{AnalyticsStore, ProjectGrouping, SessionKindFilter, analytics_path};
use crate::config::{Paths, UserConfig};
use crate::index::{QueryOptions, SearchIndex, SessionScopeKey, TimestampOrder};
use crate::types::SourceFilter;
use crate::usage::{CostMode, UsageQuery, scan_usage, scan_usage_activity};
use crate::web_auth::WebAuth;
use anyhow::{Context, Result, anyhow};
use base64::Engine as _;
use chrono::Utc;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

pub const DEFAULT_LISTEN: &str = "127.0.0.1:6363";
const MAX_MESSAGE_CONTENT_BYTES: usize = 64 * 1024;
const MAX_SESSION_CONTENT_BYTES: usize = 1024 * 1024;
const MAX_CONTENT_PAGE_BYTES: usize = 64 * 1024;
const WEB_WORKER_COUNT: usize = 8;
const WEB_REQUEST_QUEUE_CAPACITY: usize = 64;
const UI_HTML: &str = include_str!("../web/dist/index.html");
const UI_CSS: &[u8] = include_bytes!("../web/dist/assets/app.css");
const UI_JS: &[u8] = include_bytes!("../web/dist/assets/app.js");

pub fn serve(root: Option<PathBuf>, listen: &str) -> Result<()> {
    validate_listener(listen)?;
    let paths = Paths::new(root)?;
    let auth = Arc::new(WebAuth::load_or_create(&paths)?);
    let server = bind(listen)?;
    let login_url = bootstrap_url_for_auth(&auth, listen)?;
    println!("memex web UI: {login_url}");
    let cookie_name = session_cookie_name(&paths, listen)?;
    serve_requests(server, paths, restrict_hosts(listen), cookie_name, auth);
    Ok(())
}

pub fn spawn(root: Option<PathBuf>, listen: &str) -> Result<JoinHandle<()>> {
    validate_listener(listen)?;
    let paths = Paths::new(root)?;
    let auth = Arc::new(WebAuth::load_or_create(&paths)?);
    let server = bind(listen)?;
    let cookie_name = session_cookie_name(&paths, listen)?;
    let listen = listen.to_string();
    let restrict_hosts = restrict_hosts(&listen);
    let handle = std::thread::Builder::new()
        .name("memex-web".to_string())
        .spawn(move || {
            println!("memex web UI: http://{listen}");
            println!("Open it with: memex web open --listen {listen}");
            serve_requests(server, paths, restrict_hosts, cookie_name, auth);
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
    bootstrap_url_for_auth(&auth, listen)
}

fn bootstrap_url_for_auth(auth: &WebAuth, listen: &str) -> Result<String> {
    let bootstrap = auth.create_bootstrap_token()?;
    let (host, port) = listen_host_port(listen)?;
    let browser_host = match host {
        "127.0.0.1" | "localhost" => host.to_string(),
        "::1" => "[::1]".to_string(),
        _ => unreachable!("listener validation rejected unsupported loopback host"),
    };
    Ok(format!(
        "http://{browser_host}:{port}/#bootstrap={bootstrap}"
    ))
}

fn serve_requests(
    server: Server,
    paths: Paths,
    restrict_hosts: bool,
    cookie_name: String,
    auth: Arc<WebAuth>,
) {
    let (sender, receiver) = mpsc::sync_channel(WEB_REQUEST_QUEUE_CAPACITY);
    let receiver = Arc::new(Mutex::new(receiver));
    for worker in 0..WEB_WORKER_COUNT {
        let receiver = Arc::clone(&receiver);
        let request_paths = paths.clone();
        let request_auth = Arc::clone(&auth);
        let request_cookie_name = cookie_name.clone();
        let _ = std::thread::Builder::new()
            .name(format!("memex-web-{worker}"))
            .spawn(move || {
                loop {
                    let request = {
                        let Ok(receiver) = receiver.lock() else {
                            return;
                        };
                        match receiver.recv() {
                            Ok(request) => request,
                            Err(_) => return,
                        }
                    };
                    if let Err(err) = handle_request(
                        request,
                        &request_paths,
                        restrict_hosts,
                        &request_cookie_name,
                        &request_auth,
                    ) {
                        eprintln!("web UI request failed: {err:#}");
                    }
                }
            });
    }
    for request in server.incoming_requests() {
        if sender.send(request).is_err() {
            break;
        }
    }
}

fn handle_request(
    request: Request,
    paths: &Paths,
    restrict_hosts: bool,
    cookie_name: &str,
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
        if !browser_request_is_same_origin(&request) {
            return respond_text(
                request,
                StatusCode(403),
                "cross-origin request denied",
                "text/plain",
            );
        }
        let Some(token) = bearer_token(&request) else {
            return respond_unauthorized(request);
        };
        let session = match auth.exchange_bootstrap_token(token) {
            Ok(session) => session,
            Err(_) => return respond_unauthorized(request),
        };
        return respond_json_with_headers(
            request,
            StatusCode(200),
            &SessionTokenPayload { token: &session },
            vec![session_cookie_header(cookie_name, &session)?],
        );
    }

    if request.method() != &Method::Get && request.method() != &Method::Head {
        return respond_text(request, StatusCode(405), "method not allowed", "text/plain");
    }
    if (parsed.path() == "/api" || parsed.path().starts_with("/api/"))
        && !request_is_authorized(&request, cookie_name, auth)
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
                Err(err) if err.downcast_ref::<StaleSnapshot>().is_some() => {
                    respond_json_error(request, StatusCode(409), &err.to_string())
                }
                Err(err) if err.downcast_ref::<AmbiguousSessionScope>().is_some() => {
                    respond_json_error(request, StatusCode(400), &err.to_string())
                }
                Err(err) if err.downcast_ref::<InvalidSessionOffset>().is_some() => {
                    respond_json_error(request, StatusCode(400), &err.to_string())
                }
                Err(err) => respond_json_error(request, StatusCode(503), &err.to_string()),
            },
            Err(err) => respond_json_error(request, StatusCode(400), &err.to_string()),
        },
        "/api/session/content" => match SessionContentRequest::from_url(&parsed) {
            Ok(params) => match session_content_payload(paths, &params) {
                Ok(Some(payload)) => respond_json(request, StatusCode(200), &payload),
                Ok(None) => respond_json_error(request, StatusCode(404), "record not found"),
                Err(err) if err.downcast_ref::<StaleSnapshot>().is_some() => {
                    respond_json_error(request, StatusCode(409), &err.to_string())
                }
                Err(err) if err.downcast_ref::<InvalidContentOffset>().is_some() => {
                    respond_json_error(request, StatusCode(400), &err.to_string())
                }
                Err(err) if err.downcast_ref::<AmbiguousSessionScope>().is_some() => {
                    respond_json_error(request, StatusCode(400), &err.to_string())
                }
                Err(err) => respond_json_error(request, StatusCode(503), &err.to_string()),
            },
            Err(err) => respond_json_error(request, StatusCode(400), &err.to_string()),
        },
        _ => respond_json_error(request, StatusCode(404), "not found"),
    }
}

fn request_is_authorized(request: &Request, cookie_name: &str, auth: &WebAuth) -> bool {
    if let Some(token) = bearer_token(request) {
        return auth.authorize_bearer(token) || auth.authorize_session(token);
    }
    cookie_request_is_same_origin(request)
        && cookie_value(request, cookie_name).is_some_and(|token| auth.authorize_session(token))
}

fn request_header<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request
        .headers()
        .iter()
        .find(|header| header.field.as_str().as_str().eq_ignore_ascii_case(name))
        .map(|header| header.value.as_str())
}

fn browser_request_is_same_origin(request: &Request) -> bool {
    let origin_ok = request_header(request, "Origin").is_none_or(|origin| {
        request_header(request, "Host").is_some_and(|host| origin == format!("http://{host}"))
    });
    let fetch_site_ok = request_header(request, "Sec-Fetch-Site")
        .is_none_or(|site| matches!(site, "same-origin" | "none"));
    origin_ok && fetch_site_ok
}

fn cookie_request_is_same_origin(request: &Request) -> bool {
    (request_header(request, "Origin").is_some()
        || request_header(request, "Sec-Fetch-Site").is_some())
        && browser_request_is_same_origin(request)
}

fn cookie_value<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    let mut found = None;
    for header in request
        .headers()
        .iter()
        .filter(|header| header.field.equiv("Cookie"))
    {
        for pair in header.value.as_str().split(';') {
            let Some((key, value)) = pair.trim().split_once('=') else {
                continue;
            };
            if key == name {
                if found.is_some() || value.is_empty() {
                    return None;
                }
                found = Some(value);
            }
        }
    }
    found
}

fn session_cookie_name(paths: &Paths, listen: &str) -> Result<String> {
    let root = std::fs::canonicalize(&paths.root)
        .with_context(|| format!("failed to resolve {}", paths.root.display()))?;
    let (_, port) = listen_host_port(listen)?;
    let digest = Sha256::digest(format!("{}\0{port}", root.display()).as_bytes());
    Ok(format!(
        "memex_session_{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&digest[..9])
    ))
}

fn session_cookie_header(name: &str, value: &str) -> Result<Header> {
    header(
        "Set-Cookie",
        &format!(
            "{name}={value}; HttpOnly; SameSite=Strict; Path=/; Max-Age={}",
            crate::web_auth::SESSION_TTL.as_secs()
        ),
    )
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
        error: "authentication required; run `memex web open`",
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimeRange {
    Day,
    Week,
    Month,
    All,
}

impl TimeRange {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "24h" => Ok(Self::Day),
            "7d" => Ok(Self::Week),
            "30d" => Ok(Self::Month),
            "all" => Ok(Self::All),
            _ => Err(anyhow!(
                "unknown time range: {value} (expected 24h, 7d, 30d, or all)"
            )),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Day => "24h",
            Self::Week => "7d",
            Self::Month => "30d",
            Self::All => "all",
        }
    }

    fn since_ms(self, now: u64) -> Option<u64> {
        let days = match self {
            Self::Day => 1,
            Self::Week => 7,
            Self::Month => 30,
            Self::All => return None,
        };
        Some(now.saturating_sub(days * 86_400_000))
    }
}

#[derive(Debug)]
struct ActivityRequest {
    metric: ActivityMetric,
    query: String,
    source: Option<SourceFilter>,
    project: Option<String>,
    days: i64,
    range: Option<TimeRange>,
    origin: SessionKindFilter,
}

impl ActivityRequest {
    fn from_url(url: &RequestUrl) -> Result<Self> {
        let mut metric = ActivityMetric::Sessions;
        let mut query = String::new();
        let mut source = None;
        let mut project = None;
        let mut days = 30;
        let mut range = None;
        let mut origin = SessionKindFilter::Primary;
        for (key, value) in url.query_pairs() {
            match key {
                "metric" if value == "tokens" => metric = ActivityMetric::Tokens,
                "metric" if value == "sessions" || value.is_empty() => {}
                "metric" => return Err(anyhow!("unknown activity metric: {value}")),
                "q" => query = value.trim().to_string(),
                "source" if !value.is_empty() && value != "all" => {
                    source = Some(parse_source(value)?);
                }
                "project" if !value.trim().is_empty() => {
                    project = Some(value.trim().to_string());
                }
                "origin" => origin = parse_origin(value)?,
                "range" => range = Some(TimeRange::parse(value)?),
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
            query,
            source,
            project,
            days,
            range,
            origin,
        })
    }
}

fn activity_matching_session_scopes(
    paths: &Paths,
    params: &ActivityRequest,
    since_ms: Option<u64>,
) -> Result<Option<HashSet<(String, String, String)>>> {
    if params.query.is_empty() {
        return Ok(None);
    }

    let index = open_index(paths)?;
    let scopes = index
        .session_scopes_matching_query(&QueryOptions {
            query: params.query.clone(),
            project: params.project.clone(),
            role: None,
            tool: None,
            session_id: None,
            session_scope: None,
            source: params.source,
            since: since_ms,
            until: None,
            limit: 1,
        })?
        .into_iter()
        .map(|(source, session_id, source_path)| {
            (source.storage_label().to_string(), session_id, source_path)
        })
        .collect();
    Ok(Some(scopes))
}

#[derive(Serialize)]
struct ActivityPayload {
    metric: &'static str,
    days: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    range: Option<&'static str>,
    bucket_keys: Vec<String>,
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

#[derive(Default)]
struct AllowedUsageScopes {
    exact: HashSet<(String, String, String)>,
    copilot_session_ids: HashSet<String>,
}

impl AllowedUsageScopes {
    fn insert(&mut self, scope: (String, String, String)) {
        if scope.0 == "copilot" {
            self.copilot_session_ids.insert(scope.1.clone());
        }
        self.exact.insert(scope);
    }

    fn is_empty(&self) -> bool {
        self.exact.is_empty()
    }

    fn contains(&self, source: &str, session_id: &str, source_path: &str) -> bool {
        if source == "copilot" {
            self.copilot_session_ids.contains(session_id)
        } else {
            self.exact.contains(&(
                source.to_string(),
                session_id.to_string(),
                source_path.to_string(),
            ))
        }
    }
}

fn activity_payload(paths: &Paths, params: &ActivityRequest) -> Result<ActivityPayload> {
    let config = UserConfig::load(paths)?;
    let token_usage_enabled = config.token_usage_enabled();
    let now = Utc::now().timestamp_millis().max(0) as u64;
    let since_ms = params
        .range
        .map(|range| range.since_ms(now))
        .unwrap_or_else(|| Some(now.saturating_sub(params.days as u64 * 86_400_000)));
    let bucket_ms = if params.range == Some(TimeRange::Day) {
        3_600_000
    } else {
        86_400_000
    };
    let mut buckets: BTreeMap<(u64, String), u64> = BTreeMap::new();
    let matching_scopes = activity_matching_session_scopes(paths, params, since_ms)?;

    let partial = match (params.metric, matching_scopes.as_ref()) {
        (ActivityMetric::Sessions, matching_scopes) => {
            let store = AnalyticsStore::open_read_only(analytics_path(&paths.state))?;
            for session in store.query_sessions_filtered(
                params.source,
                since_ms,
                params.project.as_deref(),
                ProjectGrouping::Flat,
                Some(params.origin),
                None,
            )? {
                if matching_scopes.is_some_and(|scopes| {
                    !scopes.contains(&(
                        session.source.storage_label().to_string(),
                        session.session_id.clone(),
                        session.source_path.clone(),
                    ))
                }) {
                    continue;
                }
                add_activity_value(
                    &mut buckets,
                    session.last_at,
                    session.source.label(),
                    1,
                    bucket_ms,
                );
            }
            false
        }
        (ActivityMetric::Tokens, _) if !token_usage_enabled => false,
        (ActivityMetric::Tokens, Some(matching_scopes)) => {
            let store = AnalyticsStore::open_read_only(analytics_path(&paths.state))?;
            let allowed_scopes = store
                .query_sessions_filtered(
                    params.source,
                    since_ms,
                    params.project.as_deref(),
                    ProjectGrouping::Flat,
                    Some(params.origin),
                    None,
                )?
                .into_iter()
                .map(|session| {
                    (
                        session.source.storage_label().to_string(),
                        session.session_id,
                        session.source_path,
                    )
                })
                .filter(|scope| matching_scopes.contains(scope))
                .fold(AllowedUsageScopes::default(), |mut scopes, scope| {
                    scopes.insert(scope);
                    scopes
                });
            if allowed_scopes.is_empty() {
                false
            } else {
                let query = UsageQuery {
                    source: params.source,
                    project: params.project.clone(),
                    project_grouping: ProjectGrouping::Flat,
                    session_keys: None,
                    since_ms,
                    until_ms: None,
                    cost_mode: CostMode::Source,
                    include_events: true,
                    include_reviews: params.origin == SessionKindFilter::All,
                    cache_path: Some(paths.state.join("usage-cache.sqlite3")),
                    memo_ttl_ms: 60_000,
                };
                let report = scan_usage(&query)?;
                let partial = !report.warnings.is_empty();
                for event in report.details {
                    let Some(session_id) = event.session_id else {
                        continue;
                    };
                    if !allowed_scopes.contains(
                        event.source,
                        &session_id,
                        event.source_path.as_ref(),
                    ) {
                        continue;
                    }
                    add_activity_value(
                        &mut buckets,
                        event.timestamp_ms,
                        event.source,
                        event.tokens.total(),
                        bucket_ms,
                    );
                }
                partial
            }
        }
        (ActivityMetric::Tokens, None) => {
            // Interactive/subagent subsets need the indexed session roster.
            // Regular/all can include unindexed usage; the event review marker
            // provides permission-review filtering independently.
            let session_keys = if matches!(
                params.origin,
                SessionKindFilter::All | SessionKindFilter::Regular
            ) {
                None
            } else {
                let store = AnalyticsStore::open_read_only(analytics_path(&paths.state))?;
                let keys = store
                    .query_sessions_filtered(
                        params.source,
                        since_ms,
                        params.project.as_deref(),
                        ProjectGrouping::Flat,
                        Some(params.origin),
                        None,
                    )?
                    .into_iter()
                    .map(|session| {
                        (
                            session.source.storage_label().to_string(),
                            session.session_id,
                        )
                    })
                    .collect::<HashSet<_>>();
                Some(keys)
            };
            let query = UsageQuery {
                source: params.source,
                project: params.project.clone(),
                project_grouping: ProjectGrouping::Flat,
                session_keys,
                since_ms,
                until_ms: None,
                cost_mode: CostMode::Source,
                include_events: false,
                include_reviews: params.origin == SessionKindFilter::All,
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
                    bucket_ms,
                );
            }
            partial
        }
    };

    let (bucket_keys, points, days) = activity_buckets(buckets, since_ms, now, bucket_ms);
    Ok(ActivityPayload {
        metric: match params.metric {
            ActivityMetric::Sessions => "sessions",
            ActivityMetric::Tokens => "tokens",
        },
        days: match params.range {
            Some(TimeRange::Day) => 1,
            Some(TimeRange::Week) => 7,
            Some(TimeRange::Month) => 30,
            Some(TimeRange::All) => days,
            None => params.days,
        },
        range: params.range.map(TimeRange::label),
        bucket_keys,
        token_usage_enabled,
        partial,
        points,
    })
}

fn add_activity_value(
    buckets: &mut BTreeMap<(u64, String), u64>,
    timestamp_ms: u64,
    source: &str,
    value: u64,
    bucket_ms: u64,
) {
    let Some(timestamp) = chrono::DateTime::<Utc>::from_timestamp_millis(timestamp_ms as i64)
    else {
        return;
    };
    let key = (
        timestamp.timestamp_millis() as u64 / bucket_ms * bucket_ms,
        source.to_string(),
    );
    let bucket = buckets.entry(key).or_default();
    *bucket = bucket.saturating_add(value);
}

fn activity_buckets(
    buckets: BTreeMap<(u64, String), u64>,
    since: Option<u64>,
    now: u64,
    unit: u64,
) -> (Vec<String>, Vec<ActivityPoint>, i64) {
    let end = now / unit * unit;
    let start = since
        .map(|since| since / unit * unit)
        .or_else(|| {
            buckets
                .first_key_value()
                .map(|((timestamp, _), _)| *timestamp)
        })
        .unwrap_or(end)
        .min(end);
    // Keep all history in view without mounting one chart column per historical day.
    let units = (end - start) / unit + 1;
    let step = units.div_ceil(60) * unit;
    let label = |timestamp: u64| {
        let date = chrono::DateTime::<Utc>::from_timestamp_millis(timestamp as i64)
            .expect("activity timestamps are validated before bucketing");
        if unit < 86_400_000 {
            date.format("%Y-%m-%dT%H:00Z").to_string()
        } else {
            date.format("%Y-%m-%d").to_string()
        }
    };
    let bucket_keys = (0..units.div_ceil(step / unit))
        .map(|index| label(start + index * step))
        .collect();
    let mut grouped: BTreeMap<(String, String), u64> = BTreeMap::new();
    for ((timestamp, source), value) in buckets {
        if timestamp < start || timestamp > end {
            continue;
        }
        let timestamp = start + (timestamp - start) / step * step;
        let total = grouped.entry((label(timestamp), source)).or_default();
        *total = total.saturating_add(value);
    }
    let points = grouped
        .into_iter()
        .map(|((date, source), value)| ActivityPoint {
            date,
            source,
            value,
        })
        .collect();
    (bucket_keys, points, ((end - start) / 86_400_000 + 1) as i64)
}

#[derive(Debug)]
struct SessionRequest {
    session_id: String,
    source_path: Option<String>,
    source: Option<crate::types::SourceKind>,
    selection: SessionSelection,
    limit: usize,
    version: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum SessionSelection {
    Offset(usize),
    Before(usize),
    Tail,
    Around(String),
}

impl SessionRequest {
    fn from_url(url: &RequestUrl) -> Result<Self> {
        let mut session_id = None;
        let mut source_path = None;
        let mut source = None;
        let mut offset = 0;
        let mut offset_supplied = false;
        let mut before = None;
        let mut tail = false;
        let mut around = None;
        let mut limit = 100;
        let mut version = None;
        for (key, value) in url.query_pairs() {
            match key {
                "id" if !value.is_empty() => session_id = Some(value.to_string()),
                "source_path" if !value.is_empty() => source_path = Some(value.to_string()),
                "session_source" if !value.is_empty() => {
                    source = Some(parse_session_source(value)?);
                }
                "offset" => {
                    offset_supplied = true;
                    offset = value
                        .parse::<usize>()
                        .context("offset must be a non-negative integer")?;
                }
                "before" => {
                    before = Some(
                        value
                            .parse::<usize>()
                            .context("before must be a non-negative integer")?,
                    );
                }
                "tail" => match value {
                    "true" => tail = true,
                    "" | "false" => {}
                    _ => return Err(anyhow!("tail must be true or false")),
                },
                "around" if !value.is_empty() => around = Some(value.to_string()),
                "limit" => {
                    limit = value
                        .parse::<usize>()
                        .context("limit must be a positive integer")?
                        .clamp(1, 200);
                }
                "version" if !value.is_empty() => version = Some(value.to_string()),
                _ => {}
            }
        }
        if before == Some(0) {
            return Err(anyhow!("before must be positive"));
        }
        let selection_count = usize::from(offset_supplied)
            + before.is_some() as usize
            + usize::from(tail)
            + around.is_some() as usize;
        if selection_count > 1 {
            return Err(anyhow!(
                "choose exactly one of offset, before, tail=true, or around"
            ));
        }
        let selection = if tail {
            SessionSelection::Tail
        } else if let Some(before) = before {
            SessionSelection::Before(before)
        } else if let Some(record_id) = around {
            SessionSelection::Around(record_id)
        } else {
            SessionSelection::Offset(offset)
        };
        Ok(Self {
            session_id: session_id.ok_or_else(|| anyhow!("missing session id"))?,
            source_path,
            source,
            selection,
            limit,
            version,
        })
    }
}

#[derive(Debug)]
struct SessionContentRequest {
    session_id: String,
    source_path: Option<String>,
    source: Option<crate::types::SourceKind>,
    record_id: String,
    offset: usize,
    limit: usize,
    version: Option<String>,
}

impl SessionContentRequest {
    fn from_url(url: &RequestUrl) -> Result<Self> {
        let mut session_id = None;
        let mut source_path = None;
        let mut source = None;
        let mut record_id = None;
        let mut offset = 0;
        let mut limit = MAX_CONTENT_PAGE_BYTES;
        let mut version = None;
        for (key, value) in url.query_pairs() {
            match key {
                "id" if !value.is_empty() => session_id = Some(value.to_string()),
                "source_path" if !value.is_empty() => source_path = Some(value.to_string()),
                "session_source" if !value.is_empty() => {
                    source = Some(parse_session_source(value)?);
                }
                "record_id" if !value.is_empty() => record_id = Some(value.to_string()),
                "offset" => {
                    offset = value
                        .parse::<usize>()
                        .context("offset must be a non-negative integer")?;
                }
                "limit" => {
                    limit = value
                        .parse::<usize>()
                        .context("limit must be a positive integer")?
                        .clamp(1, MAX_CONTENT_PAGE_BYTES);
                }
                "version" if !value.is_empty() => version = Some(value.to_string()),
                _ => {}
            }
        }
        Ok(Self {
            session_id: session_id.ok_or_else(|| anyhow!("missing session id"))?,
            source_path,
            source,
            record_id: record_id.ok_or_else(|| anyhow!("missing record_id"))?,
            offset,
            limit,
            version,
        })
    }
}

fn parse_session_source(value: &str) -> Result<crate::types::SourceKind> {
    crate::types::SourceKind::ALL
        .into_iter()
        .find(|source| source.label() == value)
        .ok_or_else(|| anyhow!("unknown session_source: {value}"))
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
    origin: SessionKindFilter,
    range: TimeRange,
    sort: SearchSort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchSort {
    Relevance,
    Newest,
    Oldest,
}

impl SearchRequest {
    fn from_url(url: &RequestUrl) -> Result<Self> {
        let mut query = String::new();
        let mut source = None;
        let mut project = None;
        let mut offset = 0;
        let mut limit = 50;
        let mut origin = SessionKindFilter::Primary;
        let mut range = TimeRange::All;
        let mut requested_sort = None;

        for (key, value) in url.query_pairs() {
            match key {
                "q" => query = value.to_string(),
                "source" if !value.is_empty() && value != "all" => {
                    source = Some(parse_source(value)?);
                }
                "project" if !value.trim().is_empty() => {
                    project = Some(value.trim().to_string());
                }
                "origin" => origin = parse_origin(value)?,
                "range" => range = TimeRange::parse(value)?,
                "sort" if !value.is_empty() => {
                    requested_sort = Some(match value {
                        "relevance" => SearchSort::Relevance,
                        "newest" => SearchSort::Newest,
                        "oldest" => SearchSort::Oldest,
                        _ => {
                            return Err(anyhow!(
                                "unknown sort: {value} (expected relevance, newest, or oldest)"
                            ));
                        }
                    });
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

        let query = query.trim().to_string();
        let sort = if query.is_empty() {
            match requested_sort {
                Some(SearchSort::Oldest) => SearchSort::Oldest,
                _ => SearchSort::Newest,
            }
        } else {
            requested_sort.unwrap_or(SearchSort::Relevance)
        };

        Ok(Self {
            query,
            source,
            project,
            offset,
            limit,
            origin,
            range,
            sort,
        })
    }
}

/// Shared `origin` vocabulary with the CLI (`--origin`) and the TUI kind
/// filter: `interactive` (default), `subagent`, or `all`.
fn parse_origin(value: &str) -> Result<SessionKindFilter> {
    match value {
        "" | "interactive" => Ok(SessionKindFilter::Primary),
        "subagent" => Ok(SessionKindFilter::Subagent),
        "regular" => Ok(SessionKindFilter::Regular),
        "all" => Ok(SessionKindFilter::All),
        _ => Err(anyhow!(
            "unknown origin: {value} (expected interactive, subagent, regular, or all)"
        )),
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
        "grok" => Ok(SourceFilter::Grok),
        "hermes" => Ok(SourceFilter::Hermes),
        "jcode" => Ok(SourceFilter::Jcode),
        "muse" => Ok(SourceFilter::Muse),
        "antigravity" => Ok(SourceFilter::Antigravity),
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
    record_id: String,
    source_path: String,
    project: String,
    source: String,
    role: String,
    ts: u64,
    score: Option<f32>,
    snippet: String,
    snippet_matches: Vec<SnippetMatch>,
}

#[derive(Serialize)]
struct SnippetMatch {
    start: usize,
    end: usize,
}

fn search_payload(paths: &Paths, params: &SearchRequest) -> Result<SearchPayload> {
    let index = open_index(paths)?;
    let matchers = crate::cli::build_matchers(&params.query)?;
    let since = params
        .range
        .since_ms(Utc::now().timestamp_millis().max(0) as u64);
    let target = params.offset.saturating_add(params.limit).saturating_add(1);
    let document_count = index.doc_count()?.max(1);
    let mut candidate_limit = target.saturating_mul(4).max(100).min(document_count);
    let query_options = |limit| QueryOptions {
        query: params.query.clone(),
        project: params.project.clone(),
        role: None,
        tool: None,
        session_id: None,
        session_scope: None,
        source: params.source,
        since,
        until: None,
        limit,
    };
    let mut main_scope_cache = HashMap::new();
    let summaries = loop {
        let records: Vec<(Option<f32>, crate::types::Record)> = match params.sort {
            SearchSort::Relevance => index
                .search(&query_options(candidate_limit))?
                .into_iter()
                .map(|(score, record)| (Some(score), record))
                .collect(),
            SearchSort::Newest if params.query.is_empty() => index
                .recent_records_filtered_since(
                    candidate_limit,
                    params.source,
                    params.project.as_deref(),
                    since,
                )?
                .into_iter()
                .map(|record| (None, record))
                .collect(),
            SearchSort::Newest | SearchSort::Oldest => index
                .search_by_timestamp(
                    &query_options(candidate_limit),
                    if params.sort == SearchSort::Newest {
                        TimestampOrder::Newest
                    } else {
                        TimestampOrder::Oldest
                    },
                )?
                .into_iter()
                .map(|record| (None, record))
                .collect(),
        };

        let raw_count = records.len();
        // Session-grouped origin filter with prefer-main: a sidechain hit
        // inside a primary session must not hide the session (mirrors the
        // CLI, the TUI, and the analytics accumulator).
        let mut group_kind: HashMap<(String, String, String), Option<String>> = HashMap::new();
        for (_, record) in &records {
            let dominated = record.links.conversation_kind.as_deref() == Some("main");
            group_kind
                .entry((
                    record.source.storage_label().to_string(),
                    record.session_id.clone(),
                    record.source_path.clone(),
                ))
                .and_modify(|kind| {
                    if dominated {
                        *kind = Some("main".to_string());
                    }
                })
                .or_insert_with(|| record.links.conversation_kind.clone());
        }
        if params.origin != SessionKindFilter::All
            && matches!(params.sort, SearchSort::Newest | SearchSort::Oldest)
            && (params.sort == SearchSort::Oldest || !params.query.is_empty())
        {
            let candidate_scopes = records
                .iter()
                .filter_map(|(_, record)| {
                    let key = (
                        record.source.storage_label().to_string(),
                        record.session_id.clone(),
                        record.source_path.clone(),
                    );
                    (group_kind.get(&key).and_then(|kind| kind.as_deref()) != Some("main")).then(
                        || SessionScopeKey {
                            source: record.source,
                            session_id: record.session_id.clone(),
                            source_path: record.source_path.clone(),
                        },
                    )
                })
                .collect::<HashSet<_>>();
            for scope in candidate_scopes {
                let has_main = if let Some(has_main) = main_scope_cache.get(&scope) {
                    *has_main
                } else {
                    let has_main = index.session_scope_has_matching_conversation_kind(
                        &query_options(1),
                        &scope,
                        "main",
                    )?;
                    main_scope_cache.insert(scope.clone(), has_main);
                    has_main
                };
                if has_main
                    && let Some(kind) = group_kind.get_mut(&(
                        scope.source.storage_label().to_string(),
                        scope.session_id,
                        scope.source_path,
                    ))
                {
                    *kind = Some("main".to_string());
                }
            }
        }
        let mut seen = HashSet::new();
        let mut summaries = Vec::new();
        for (score, record) in records {
            let session_key = (
                record.source.storage_label().to_string(),
                record.session_id.clone(),
                record.source_path.clone(),
            );
            if !seen.insert(session_key.clone()) {
                continue;
            }
            if !params.origin.matches_kind(
                group_kind
                    .get(&session_key)
                    .and_then(|kind| kind.as_deref()),
            ) {
                continue;
            }
            let snippet = if params.query.is_empty() {
                summarize(&record.text, 360)
            } else {
                crate::cli::match_preview(&record.text, &matchers, 160)
            };
            let snippet_matches = snippet_match_spans(&snippet, &matchers);
            summaries.push(SessionSummary {
                record_id: crate::retrieval::canonical_record_id(&record),
                session_id: record.session_id,
                source_path: record.source_path,
                project: record.project,
                source: record.source.label().to_string(),
                role: record.role,
                ts: record.ts,
                score,
                snippet,
                snippet_matches,
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

#[derive(Debug, Serialize)]
struct SessionPayload {
    version: String,
    session_id: String,
    source_path: String,
    project: String,
    source: String,
    started_at: u64,
    ended_at: u64,
    offset: usize,
    total: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_offset: Option<usize>,
    messages: Vec<MessagePayload>,
}

#[derive(Debug, Serialize)]
struct MessagePayload {
    record_id: String,
    role: String,
    content: String,
    content_bytes: usize,
    truncated: bool,
    ts: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_name: Option<String>,
}

#[derive(Debug, Serialize)]
struct SessionContentPayload {
    version: String,
    record_id: String,
    offset: usize,
    content: String,
    content_bytes: usize,
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_offset: Option<usize>,
}

#[derive(Debug)]
struct StaleSnapshot {
    requested: String,
    current: String,
}

impl std::fmt::Display for StaleSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "index snapshot changed (requested {}, current {})",
            self.requested, self.current
        )
    }
}

impl std::error::Error for StaleSnapshot {}

#[derive(Debug)]
struct InvalidContentOffset(usize);

impl std::fmt::Display for InvalidContentOffset {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "content offset {} is not a UTF-8 boundary",
            self.0
        )
    }
}

impl std::error::Error for InvalidContentOffset {}

#[derive(Debug)]
struct AmbiguousSessionScope;

impl std::fmt::Display for AmbiguousSessionScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            "session id is ambiguous; provide source_path and session_source from search",
        )
    }
}

impl std::error::Error for AmbiguousSessionScope {}

#[derive(Debug)]
struct InvalidSessionOffset;

impl std::fmt::Display for InvalidSessionOffset {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "session page boundary is past the end")
    }
}

impl std::error::Error for InvalidSessionOffset {}

fn session_payload(paths: &Paths, params: &SessionRequest) -> Result<Option<SessionPayload>> {
    let index = open_index(paths)?;
    ensure_snapshot(&index, params.version.as_deref())?;
    let version = index.snapshot_version().to_string();
    let mut resolved_source_path = params.source_path.clone();
    let mut resolved_source = params.source;
    if let SessionSelection::Around(record_id) = &params.selection
        && (resolved_source_path.is_none() || resolved_source.is_none())
    {
        let mut anchors = index
            .records_by_canonical_id_in_session_scope(
                record_id,
                &params.session_id,
                resolved_source_path.as_deref(),
                resolved_source,
            )?
            .into_iter();
        let Some(anchor) = anchors.next() else {
            ensure_snapshot_current(paths, &version)?;
            return Ok(None);
        };
        if anchors.next().is_some() {
            return Err(AmbiguousSessionScope.into());
        }
        resolved_source_path.get_or_insert(anchor.source_path);
        resolved_source.get_or_insert(anchor.source);
    }
    if resolved_source_path.is_none() || resolved_source.is_none() {
        let scopes = index.matching_session_scopes(
            &params.session_id,
            resolved_source_path.as_deref(),
            resolved_source,
        )?;
        match scopes.as_slice() {
            [] => {
                ensure_snapshot_current(paths, &version)?;
                return Ok(None);
            }
            [(source, path)] => {
                resolved_source.get_or_insert(*source);
                resolved_source_path.get_or_insert_with(|| path.clone());
            }
            _ => return Err(AmbiguousSessionScope.into()),
        }
    }
    let source_path = resolved_source_path.as_deref();
    let source = resolved_source;
    let (records, total, offset) = match &params.selection {
        SessionSelection::Offset(offset) => {
            let (records, total) = index.records_by_session_scope_page(
                &params.session_id,
                source_path,
                source,
                *offset,
                params.limit,
            )?;
            (records, total, *offset)
        }
        SessionSelection::Before(before) => {
            let (_, total) = index.records_by_session_scope_page(
                &params.session_id,
                source_path,
                source,
                usize::MAX,
                1,
            )?;
            if total > 0 && *before > total {
                ensure_snapshot_current(paths, &version)?;
                return Err(InvalidSessionOffset.into());
            }
            let before = (*before).min(total);
            let offset = before.saturating_sub(params.limit);
            let limit = before - offset;
            let (records, page_total) = index.records_by_session_scope_page(
                &params.session_id,
                source_path,
                source,
                offset,
                limit,
            )?;
            if page_total != total {
                ensure_snapshot_current(paths, &version)?;
                return Err(anyhow!("session changed while reading"));
            }
            (records, total, offset)
        }
        SessionSelection::Tail => {
            let (_, total) = index.records_by_session_scope_page(
                &params.session_id,
                source_path,
                source,
                usize::MAX,
                1,
            )?;
            let offset = total.saturating_sub(params.limit);
            let (records, page_total) = index.records_by_session_scope_page(
                &params.session_id,
                source_path,
                source,
                offset,
                params.limit,
            )?;
            if page_total != total {
                ensure_snapshot_current(paths, &version)?;
                return Err(anyhow!("session changed while reading"));
            }
            (records, total, offset)
        }
        SessionSelection::Around(record_id) => {
            let Some((records, total, offset)) = index.records_by_session_scope_around(
                &params.session_id,
                source_path,
                source,
                record_id,
                params.limit,
            )?
            else {
                ensure_snapshot_current(paths, &version)?;
                return Ok(None);
            };
            (records, total, offset)
        }
    };
    if total == 0 {
        ensure_snapshot_current(paths, &version)?;
        return Ok(None);
    }
    if records.is_empty() {
        ensure_snapshot_current(paths, &version)?;
        return Err(InvalidSessionOffset.into());
    }

    let first = records.first().expect("records is not empty");
    let project = first.project.clone();
    let source_label = first.source.label().to_string();
    let resolved_source_path = source_path.unwrap_or(&first.source_path).to_string();
    let Some((started_at, ended_at)) =
        index.session_scope_time_bounds(&params.session_id, source_path, source)?
    else {
        ensure_snapshot_current(paths, &version)?;
        return Err(anyhow!("session changed while reading"));
    };
    let mut content_limits = vec![0; records.len()];
    let priority = match &params.selection {
        SessionSelection::Tail => (0..records.len()).rev().collect::<Vec<_>>(),
        SessionSelection::Around(record_id) => {
            let anchor = records
                .iter()
                .position(|record| crate::retrieval::canonical_record_id(record) == *record_id)
                .unwrap_or(records.len() / 2);
            let mut priority = (0..records.len()).collect::<Vec<_>>();
            priority.sort_by_key(|index| index.abs_diff(anchor));
            priority
        }
        SessionSelection::Offset(_) | SessionSelection::Before(_) => {
            (0..records.len()).collect::<Vec<_>>()
        }
    };
    let mut content_budget = MAX_SESSION_CONTENT_BYTES;
    for index in priority {
        let limit = MAX_MESSAGE_CONTENT_BYTES.min(content_budget);
        let actual = utf8_prefix(&records[index].text, limit).len();
        content_limits[index] = actual;
        content_budget -= actual;
    }
    let messages: Vec<MessagePayload> = records
        .into_iter()
        .enumerate()
        .map(|(index, record)| {
            let content_bytes = record.text.len();
            let content = utf8_prefix(&record.text, content_limits[index]).to_string();
            MessagePayload {
                record_id: crate::retrieval::canonical_record_id(&record),
                role: record.role,
                truncated: content.len() < content_bytes,
                content,
                content_bytes,
                ts: record.ts,
                tool_name: record.tool_name,
            }
        })
        .collect();
    let next_offset = (offset + messages.len() < total).then_some(offset + messages.len());

    ensure_snapshot_current(paths, &version)?;
    Ok(Some(SessionPayload {
        version,
        session_id: params.session_id.clone(),
        source_path: resolved_source_path,
        project,
        source: source_label,
        started_at,
        ended_at,
        offset,
        total,
        next_offset,
        messages,
    }))
}

fn session_content_payload(
    paths: &Paths,
    params: &SessionContentRequest,
) -> Result<Option<SessionContentPayload>> {
    let index = open_index(paths)?;
    ensure_snapshot(&index, params.version.as_deref())?;
    let version = index.snapshot_version().to_string();
    let mut matches = index
        .records_by_canonical_id_in_session_scope(
            &params.record_id,
            &params.session_id,
            params.source_path.as_deref(),
            params.source,
        )?
        .into_iter();
    let Some(record) = matches.next() else {
        ensure_snapshot_current(paths, &version)?;
        return Ok(None);
    };
    if matches.next().is_some() {
        return Err(AmbiguousSessionScope.into());
    }
    if params.offset > record.text.len() {
        ensure_snapshot_current(paths, &version)?;
        return Ok(None);
    }
    if !record.text.is_char_boundary(params.offset) {
        return Err(InvalidContentOffset(params.offset).into());
    }
    let remaining = &record.text[params.offset..];
    let content = utf8_prefix(remaining, params.limit).to_string();
    let next = params.offset + content.len();
    ensure_snapshot_current(paths, &version)?;
    Ok(Some(SessionContentPayload {
        version,
        record_id: params.record_id.clone(),
        offset: params.offset,
        content,
        content_bytes: record.text.len(),
        truncated: next < record.text.len(),
        next_offset: (next < record.text.len()).then_some(next),
    }))
}

fn ensure_snapshot(index: &SearchIndex, requested: Option<&str>) -> Result<()> {
    if let Some(requested) = requested
        && requested != index.snapshot_version()
    {
        return Err(StaleSnapshot {
            requested: requested.to_string(),
            current: index.snapshot_version().to_string(),
        }
        .into());
    }
    Ok(())
}

fn ensure_snapshot_current(paths: &Paths, expected: &str) -> Result<()> {
    if !expected.starts_with("legacy-") {
        return Ok(());
    }
    let current = open_index(paths)?;
    if current.snapshot_version() != expected {
        return Err(StaleSnapshot {
            requested: expected.to_string(),
            current: current.snapshot_version().to_string(),
        }
        .into());
    }
    Ok(())
}

fn utf8_prefix(value: &str, max_bytes: usize) -> &str {
    let mut end = value.len().min(max_bytes);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn open_index(paths: &Paths) -> Result<SearchIndex> {
    if !SearchIndex::exists(&paths.index) {
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

fn snippet_match_spans(text: &str, matchers: &[regex::Regex]) -> Vec<SnippetMatch> {
    let mut byte_spans = matchers
        .iter()
        .flat_map(|matcher| matcher.find_iter(text).map(|hit| (hit.start(), hit.end())))
        .collect::<Vec<_>>();
    byte_spans.sort_unstable();

    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in byte_spans {
        if let Some((_, previous_end)) = merged.last_mut()
            && start < *previous_end
        {
            *previous_end = (*previous_end).max(end);
            continue;
        }
        merged.push((start, end));
    }

    merged
        .into_iter()
        .map(|(start, end)| SnippetMatch {
            start: text[..start].chars().count(),
            end: text[..end].chars().count(),
        })
        .collect()
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

fn respond_json_with_headers<T: Serialize>(
    request: Request,
    status: StatusCode,
    value: &T,
    headers: Vec<Header>,
) -> Result<()> {
    let body = serde_json::to_vec(value)?;
    respond_with_headers(
        request,
        status,
        body,
        "application/json; charset=utf-8",
        false,
        headers,
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
        let cookie_name = session_cookie_name(paths, DEFAULT_LISTEN).unwrap();
        handle_request(request, paths, true, &cookie_name, auth).unwrap();
        client.join().unwrap()
    }

    fn response_header<'a>(response: &'a str, name: &str) -> Option<&'a str> {
        response
            .split("\r\n\r\n")
            .next()?
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim())
    }

    fn record(doc_id: u64, session_id: &str, source_path: &str, text: String) -> Record {
        Record {
            source: SourceKind::Claude,
            doc_id,
            ts: doc_id * 1_000,
            project: "memex".to_string(),
            session_id: session_id.to_string(),
            turn_id: doc_id as u32,
            role: "assistant".to_string(),
            text,
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks::default(),
            source_path: source_path.to_string(),
        }
    }

    fn publish_records(paths: &Paths, records: impl IntoIterator<Item = Record>) {
        let index = SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
        let mut writer = index.writer().unwrap();
        for record in records {
            index.add_record(&mut writer, &record).unwrap();
        }
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        index.publish_generation().unwrap();
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
        assert_eq!(request.sort, SearchSort::Relevance);

        assert_eq!(
            SearchRequest::from_url(&parse_url("/api/search").unwrap())
                .unwrap()
                .sort,
            SearchSort::Newest
        );
        assert_eq!(
            SearchRequest::from_url(&parse_url("/api/search?sort=relevance").unwrap())
                .unwrap()
                .sort,
            SearchSort::Newest
        );
        assert_eq!(
            SearchRequest::from_url(&parse_url("/api/search?q=needle&sort=oldest").unwrap())
                .unwrap()
                .sort,
            SearchSort::Oldest
        );
        assert!(SearchRequest::from_url(&parse_url("/api/search?sort=sideways").unwrap()).is_err());
    }

    #[test]
    fn time_ranges_filter_search_and_activity_together() {
        use crate::analytics::AnalyticsWriter;
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let now = Utc::now().timestamp_millis() as u64;
        let hour = 3_600_000;
        let records = [1, 3, 48, 240, 9600]
            .into_iter()
            .enumerate()
            .map(|(index, hours)| {
                let mut value = record(
                    index as u64 + 1,
                    &format!("session-{index}"),
                    &format!("/tmp/session-{index}.jsonl"),
                    "needle".to_string(),
                );
                value.ts = now - hours * hour;
                value
            })
            .collect::<Vec<_>>();
        let mut analytics = AnalyticsWriter::open(analytics_path(&paths.state)).unwrap();
        for record in &records {
            analytics.record(record).unwrap();
        }
        analytics.flush().unwrap();
        drop(analytics);
        publish_records(&paths, records);
        for (range, expected) in [("24h", 2), ("7d", 3), ("30d", 4), ("all", 5)] {
            for query in ["", "needle", "needle&sort=oldest"] {
                let request = SearchRequest::from_url(
                    &parse_url(&format!("/api/search?range={range}&q={query}")).unwrap(),
                )
                .unwrap();
                let search = search_payload(&paths, &request).unwrap();
                assert_eq!(search.results.len(), expected, "{range}: {query}");
            }
            let request = ActivityRequest::from_url(
                &parse_url(&format!("/api/activity?range={range}")).unwrap(),
            )
            .unwrap();
            let activity = activity_payload(&paths, &request).unwrap();
            assert_eq!(activity.range, Some(range));
            assert_eq!(
                activity.points.iter().map(|point| point.value).sum::<u64>(),
                expected as u64
            );
            assert!(activity.bucket_keys.len() <= 60);
            assert!(
                activity
                    .points
                    .iter()
                    .all(|point| activity.bucket_keys.contains(&point.date))
            );
            if range == "24h" {
                assert_eq!(activity.bucket_keys.len(), 25);
                assert!(activity.points.iter().all(|point| point.date.contains('T')));
                assert_eq!(activity.points.len(), 2);
            }
        }
        for range in ["yesterday", "365d", ""] {
            assert!(
                SearchRequest::from_url(&parse_url(&format!("/api/search?range={range}")).unwrap())
                    .is_err()
            );
            assert!(
                ActivityRequest::from_url(
                    &parse_url(&format!("/api/activity?range={range}")).unwrap()
                )
                .is_err()
            );
        }
    }

    #[test]
    fn legacy_search_hits_open_and_expand_without_rebuilding() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let schema = crate::index::build_schema_with_canonical_record_id(false).unwrap();
        drop(tantivy::Index::create_in_dir(&paths.index, schema).unwrap());
        let index = SearchIndex::open_or_create(&paths.index).unwrap();
        let text = format!("needle {}", "é".repeat(40_000));
        let records = [
            record(1, "legacy", "/tmp/legacy.jsonl", "first".to_string()),
            record(2, "legacy", "/tmp/legacy.jsonl", text.clone()),
            record(3, "legacy", "/tmp/legacy.jsonl", "last".to_string()),
            record(
                4,
                "legacy",
                "/tmp/other.jsonl",
                "needle other path".to_string(),
            ),
        ];
        let mut writer = index.writer().unwrap();
        for record in records {
            index.add_record(&mut writer, &record).unwrap();
        }
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        let search = search_payload(
            &paths,
            &SearchRequest::from_url(&parse_url("/api/search?q=needle").unwrap()).unwrap(),
        )
        .unwrap();
        let hit = search
            .results
            .iter()
            .find(|hit| hit.source_path == "/tmp/legacy.jsonl")
            .unwrap();
        for (source_path, source) in [
            (Some(hit.source_path.clone()), Some(SourceKind::Claude)),
            (None, None),
        ] {
            let page = session_payload(
                &paths,
                &SessionRequest {
                    session_id: hit.session_id.clone(),
                    source_path: source_path.clone(),
                    source,
                    selection: SessionSelection::Around(hit.record_id.clone()),
                    limit: 3,
                    version: None,
                },
            )
            .unwrap()
            .unwrap();
            assert_eq!(page.total, 3);
            assert_eq!(page.messages[1].record_id, hit.record_id);
            assert!(page.messages[1].truncated);
            let content = session_content_payload(
                &paths,
                &SessionContentRequest {
                    session_id: hit.session_id.clone(),
                    source_path,
                    source,
                    record_id: hit.record_id.clone(),
                    offset: page.messages[1].content.len(),
                    limit: MAX_CONTENT_PAGE_BYTES,
                    version: Some(page.version),
                },
            )
            .unwrap()
            .unwrap();
            assert_eq!(page.messages[1].content.clone() + &content.content, text);
            assert!(!content.truncated);
        }
    }
    #[test]
    fn search_request_accepts_omp_source() {
        let url = parse_url("/api/search?source=omp").unwrap();
        let request = SearchRequest::from_url(&url).unwrap();

        assert_eq!(request.source, Some(SourceFilter::Omp));
    }

    #[test]
    fn search_request_accepts_jcode_and_muse_sources() {
        let url_jcode = parse_url("/api/search?source=jcode").unwrap();
        assert_eq!(
            SearchRequest::from_url(&url_jcode).unwrap().source,
            Some(SourceFilter::Jcode)
        );
        let url_muse = parse_url("/api/search?source=muse").unwrap();
        assert_eq!(
            SearchRequest::from_url(&url_muse).unwrap().source,
            Some(SourceFilter::Muse)
        );
    }

    #[test]
    fn session_request_decodes_and_caps_page_size() {
        let url = parse_url("/api/session?id=session-a&offset=40&limit=1000").unwrap();
        let request = SessionRequest::from_url(&url).unwrap();

        assert_eq!(request.session_id, "session-a");
        assert_eq!(request.selection, SessionSelection::Offset(40));
        assert_eq!(request.limit, 200);
    }

    #[test]
    fn session_request_parses_bounded_window_selectors() {
        let tail = SessionRequest::from_url(
            &parse_url(
                "/api/session?id=s&source_path=%2Ftmp%2Fs.jsonl&session_source=codex&tail=true&version=g1",
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(tail.selection, SessionSelection::Tail);
        assert_eq!(tail.source_path.as_deref(), Some("/tmp/s.jsonl"));
        assert_eq!(tail.source, Some(SourceKind::Codex));
        assert_eq!(tail.version.as_deref(), Some("g1"));

        let around = SessionRequest::from_url(
            &parse_url("/api/session?id=s&around=rid1_hit&limit=21").unwrap(),
        )
        .unwrap();
        assert_eq!(
            around.selection,
            SessionSelection::Around("rid1_hit".to_string())
        );

        let before =
            SessionRequest::from_url(&parse_url("/api/session?id=s&before=40").unwrap()).unwrap();
        assert_eq!(before.selection, SessionSelection::Before(40));
        assert!(
            SessionRequest::from_url(&parse_url("/api/session?id=s&offset=1&tail=true").unwrap())
                .is_err()
        );
        assert!(
            SessionRequest::from_url(
                &parse_url("/api/session?id=s&around=rid1_hit&before=1").unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn session_request_accepts_large_boundaries_and_rejects_invalid_numbers() {
        for (selector, selection) in [
            ("offset", SessionSelection::Offset(100_041)),
            ("before", SessionSelection::Before(100_041)),
        ] {
            let request = SessionRequest::from_url(
                &parse_url(&format!("/api/session?id=s&{selector}=100041&limit=40")).unwrap(),
            )
            .unwrap();
            assert_eq!(request.selection, selection);
            for invalid in ["-1", "not-a-number", "999999999999999999999999999999"] {
                assert!(
                    SessionRequest::from_url(
                        &parse_url(&format!("/api/session?id=s&{selector}={invalid}")).unwrap(),
                    )
                    .is_err()
                );
            }
        }
        assert!(
            SessionRequest::from_url(&parse_url("/api/session?id=s&before=0").unwrap()).is_err()
        );
    }

    #[test]
    fn session_pages_cross_one_hundred_thousand_messages() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let total = 100_081;
        publish_records(
            &paths,
            (1..=total).map(|id| record(id, "large", "/tmp/large.jsonl", format!("message {id}"))),
        );
        let request = |selector: &str| {
            SessionRequest::from_url(
            &parse_url(&format!("/api/session?id=large&session_source=claude&source_path=%2Ftmp%2Flarge.jsonl&limit=40&{selector}")).unwrap(),
        ).unwrap()
        };
        let tail = session_payload(&paths, &request("tail=true"))
            .unwrap()
            .unwrap();
        assert_eq!(tail.offset, 100_041);
        assert_eq!(tail.messages.len(), 40);
        let before = session_payload(&paths, &request(&format!("before={}", tail.offset)))
            .unwrap()
            .unwrap();
        assert_eq!(before.offset, 100_001);
        assert_eq!(before.next_offset, Some(tail.offset));
        assert_eq!(before.messages.len(), 40);
        assert_eq!(before.messages.last().unwrap().content, "message 100041");
        assert_eq!(tail.messages.first().unwrap().content, "message 100042");
        let forward = session_payload(
            &paths,
            &request(&format!("offset={}", before.next_offset.unwrap())),
        )
        .unwrap()
        .unwrap();
        assert_eq!(forward.offset, tail.offset);
        assert_eq!(
            forward.messages.first().unwrap().record_id,
            tail.messages.first().unwrap().record_id
        );
        for selector in [format!("before={}", total + 1), format!("offset={}", total)] {
            let error = session_payload(&paths, &request(&selector)).unwrap_err();
            assert!(error.downcast_ref::<InvalidSessionOffset>().is_some());
        }
    }

    #[test]
    fn activity_request_parses_metric_filters_and_range() {
        let url = parse_url(
            "/api/activity?metric=tokens&q=%20searchtrino%20&source=codex&project=memex&days=1000",
        )
        .unwrap();
        let request = ActivityRequest::from_url(&url).unwrap();

        assert_eq!(request.metric, ActivityMetric::Tokens);
        assert_eq!(request.query, "searchtrino");
        assert_eq!(request.source, Some(SourceFilter::Codex));
        assert_eq!(request.project.as_deref(), Some("memex"));
        assert_eq!(request.days, 365);
    }

    #[test]
    fn activity_query_uses_every_exact_matching_scope_and_clear_restores_filters() {
        use crate::analytics::AnalyticsWriter;

        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let now = Utc::now().timestamp_millis().max(0) as u64;
        let mut records = Vec::new();

        for index in 0..61 {
            let mut value = record(
                index + 1,
                &format!("matching-{index}"),
                &format!("/tmp/matching-{index}.jsonl"),
                "searchtrino".to_string(),
            );
            value.ts = now - index;
            records.push(value);
        }
        for index in 0..4 {
            let mut value = record(
                index + 100,
                &format!("unmatched-{index}"),
                &format!("/tmp/unmatched-{index}.jsonl"),
                "unrelated".to_string(),
            );
            value.ts = now - index;
            records.push(value);
        }

        let mut other_project = record(
            200,
            "other-project",
            "/tmp/other-project.jsonl",
            "searchtrino".to_string(),
        );
        other_project.project = "elsewhere".to_string();
        other_project.ts = now;
        records.push(other_project);

        let mut other_source = record(
            201,
            "other-source",
            "/tmp/other-source.jsonl",
            "searchtrino".to_string(),
        );
        other_source.source = SourceKind::Codex;
        other_source.ts = now;
        records.push(other_source);

        for (doc_id, source_path, text) in [
            (202, "/tmp/shared-match.jsonl", "searchtrino"),
            (203, "/tmp/shared-miss.jsonl", "unrelated"),
        ] {
            let mut value = record(doc_id, "shared-id", source_path, text.to_string());
            value.ts = now;
            records.push(value);
        }

        let mut analytics = AnalyticsWriter::open(analytics_path(&paths.state)).unwrap();
        for value in &records {
            analytics.record(value).unwrap();
        }
        analytics.flush().unwrap();
        drop(analytics);
        publish_records(&paths, records);

        let total = |query: &str| {
            let request = ActivityRequest::from_url(
                &parse_url(&format!(
                    "/api/activity?range=all&source=claude&project=memex&q={query}"
                ))
                .unwrap(),
            )
            .unwrap();
            activity_payload(&paths, &request)
                .unwrap()
                .points
                .iter()
                .map(|point| point.value)
                .sum::<u64>()
        };

        assert_eq!(total("searchtrino"), 62);
        assert_eq!(total(""), 67);
        assert_eq!(total("%20%20"), 67);
    }

    #[test]
    fn token_activity_query_filters_same_session_id_by_exact_source_path() {
        use crate::analytics::AnalyticsWriter;
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        std::fs::write(paths.root.join("config.toml"), "token_usage = true\n").unwrap();

        let claude_root = temp.path().join("claude");
        let project_dir = claude_root.join("projects/memex");
        std::fs::create_dir_all(&project_dir).unwrap();
        let matching_path = project_dir.join("matching.jsonl");
        let other_path = project_dir.join("other.jsonl");
        let usage_line = |input_tokens| {
            format!(
                r#"{{"type":"assistant","sessionId":"shared-id","requestId":"request-{input_tokens}","timestamp":"2026-09-05T12:00:00Z","cwd":"/repo/memex","message":{{"id":"message-{input_tokens}","model":"claude-sonnet-4-6","usage":{{"inputTokens":{input_tokens}}}}}}}"#
            ) + "\n"
        };
        std::fs::write(&matching_path, usage_line(10)).unwrap();
        std::fs::write(&other_path, usage_line(90)).unwrap();
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(claude_root.as_os_str()))]);

        let now = Utc::now().timestamp_millis().max(0) as u64;
        let mut matching = record(
            1,
            "shared-id",
            matching_path.to_string_lossy().as_ref(),
            "searchtrino".to_string(),
        );
        matching.ts = now;
        let mut other = record(
            2,
            "shared-id",
            other_path.to_string_lossy().as_ref(),
            "unrelated".to_string(),
        );
        other.ts = now;
        let records = vec![matching, other];
        let mut analytics = AnalyticsWriter::open(analytics_path(&paths.state)).unwrap();
        for value in &records {
            analytics.record(value).unwrap();
        }
        analytics.flush().unwrap();
        drop(analytics);
        publish_records(&paths, records);

        let total = |query: &str| {
            let request = ActivityRequest::from_url(
                &parse_url(&format!(
                    "/api/activity?metric=tokens&range=all&origin=all&source=claude&project=memex&q={query}"
                ))
                .unwrap(),
            )
            .unwrap();
            activity_payload(&paths, &request)
                .unwrap()
                .points
                .iter()
                .map(|point| point.value)
                .sum::<u64>()
        };

        assert_eq!(total("searchtrino"), 10);
        assert_eq!(total("missing-query"), 0);
        assert_eq!(total(""), 100);
    }

    #[test]
    fn token_activity_query_correlates_copilot_session_state_with_otel_usage() {
        use crate::analytics::AnalyticsWriter;
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        std::fs::write(paths.root.join("config.toml"), "token_usage = true\n").unwrap();

        let copilot_root = temp.path().join("copilot");
        let session_path = |session_id: &str| {
            copilot_root
                .join("session-state")
                .join(session_id)
                .join("events.jsonl")
        };
        let matching_path = session_path("matching-session");
        let other_path = session_path("other-session");
        std::fs::create_dir_all(matching_path.parent().unwrap()).unwrap();
        std::fs::create_dir_all(other_path.parent().unwrap()).unwrap();
        std::fs::write(&matching_path, "").unwrap();
        std::fs::write(&other_path, "").unwrap();
        let otel_dir = copilot_root.join("otel");
        std::fs::create_dir_all(&otel_dir).unwrap();
        let span = |trace: &str, session: &str, input: u64| {
            format!(
                r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":[{{"name":"chat","traceId":"{trace}","spanId":"span","startTimeUnixNano":"1750000000000000000","attributes":[{{"key":"gen_ai.usage.input_tokens","value":{{"intValue":"{input}"}}}},{{"key":"gen_ai.conversation.id","value":{{"stringValue":"{session}"}}}}]}}]}}]}}]}}"#
            )
        };
        std::fs::write(
            otel_dir.join("usage.jsonl"),
            format!(
                "{}\n{}\n",
                span("matching-trace", "matching-session", 10),
                span("other-trace", "other-session", 90)
            ),
        )
        .unwrap();
        let _env = EnvVarGuard::set_os(&[("COPILOT_HOME", Some(copilot_root.as_os_str()))]);

        let now = Utc::now().timestamp_millis().max(0) as u64;
        let mut matching = record(
            1,
            "matching-session",
            matching_path.to_string_lossy().as_ref(),
            "searchtrino".to_string(),
        );
        matching.source = SourceKind::Copilot;
        matching.ts = now;
        let mut other = record(
            2,
            "other-session",
            other_path.to_string_lossy().as_ref(),
            "unrelated".to_string(),
        );
        other.source = SourceKind::Copilot;
        other.ts = now;
        let records = vec![matching, other];
        let mut analytics = AnalyticsWriter::open(analytics_path(&paths.state)).unwrap();
        for value in &records {
            analytics.record(value).unwrap();
        }
        analytics.flush().unwrap();
        drop(analytics);
        publish_records(&paths, records);

        let request = ActivityRequest::from_url(
            &parse_url(
                "/api/activity?metric=tokens&range=all&origin=all&source=copilot&q=searchtrino",
            )
            .unwrap(),
        )
        .unwrap();
        let total = activity_payload(&paths, &request)
            .unwrap()
            .points
            .iter()
            .map(|point| point.value)
            .sum::<u64>();

        assert_eq!(total, 10);
    }

    #[test]
    fn requests_default_to_interactive_origin() {
        let search = SearchRequest::from_url(&parse_url("/api/search?q=hi").unwrap()).unwrap();
        assert_eq!(search.origin, SessionKindFilter::Primary);
        let activity = ActivityRequest::from_url(&parse_url("/api/activity").unwrap()).unwrap();
        assert_eq!(activity.origin, SessionKindFilter::Primary);
    }

    #[test]
    fn requests_parse_origin_values() {
        for (query, expected) in [
            ("subagent", SessionKindFilter::Subagent),
            ("all", SessionKindFilter::All),
            ("regular", SessionKindFilter::Regular),
            ("interactive", SessionKindFilter::Primary),
            ("", SessionKindFilter::Primary),
        ] {
            let search = SearchRequest::from_url(
                &parse_url(&format!("/api/search?q=hi&origin={query}")).unwrap(),
            )
            .unwrap();
            assert_eq!(search.origin, expected);
            let activity = ActivityRequest::from_url(
                &parse_url(&format!("/api/activity?origin={query}")).unwrap(),
            )
            .unwrap();
            assert_eq!(activity.origin, expected);
        }
        assert!(SearchRequest::from_url(&parse_url("/api/search?origin=bots").unwrap()).is_err());
        assert!(
            ActivityRequest::from_url(&parse_url("/api/activity?origin=bots").unwrap()).is_err()
        );
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
        writer.wait_merging_threads().unwrap();
        index.publish_generation().unwrap();
        let index = SearchIndex::open_or_create(&paths.index).unwrap();

        let payload = search_payload(
            &paths,
            &SearchRequest {
                query: "alpha".to_string(),
                source: None,
                project: Some("memex".to_string()),
                offset: 0,
                limit: 30,
                origin: SessionKindFilter::Primary,
                range: TimeRange::All,
                sort: SearchSort::Relevance,
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
                origin: SessionKindFilter::Primary,
                range: TimeRange::All,
                sort: SearchSort::Relevance,
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
                origin: SessionKindFilter::Primary,
                range: TimeRange::All,
                sort: SearchSort::Relevance,
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
                source_path: None,
                source: None,
                selection: SessionSelection::Offset(1),
                limit: 1,
                version: None,
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
                origin: SessionKindFilter::Primary,
                range: TimeRange::All,
                sort: SearchSort::Newest,
            },
        )
        .unwrap();
        assert!(filtered.results.is_empty());
    }

    #[test]
    fn search_sort_orders_complete_filtered_session_set_before_pagination() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let mut records = (1..=110)
            .map(|doc_id| {
                let mut value = record(doc_id, "dense", "/tmp/dense.jsonl", "needle".to_string());
                value.ts = 10_000 + doc_id;
                value
            })
            .collect::<Vec<_>>();
        for (doc_id, session_id, ts, text) in [
            (200, "second", 9_000, "needle".to_string()),
            (201, "third", 8_000, "needle".to_string()),
            (300, "relevant", 100, "needle ".repeat(12)),
        ] {
            let mut value = record(
                doc_id,
                session_id,
                &format!("/tmp/{session_id}.jsonl"),
                text,
            );
            value.ts = ts;
            records.push(value);
        }
        let mut outsider = record(400, "outsider", "/tmp/outsider.jsonl", "needle".to_string());
        outsider.source = SourceKind::Codex;
        outsider.project = "other".to_string();
        outsider.ts = 50_000;
        records.push(outsider);
        publish_records(&paths, records);

        let payload = |query: &str| {
            search_payload(
                &paths,
                &SearchRequest::from_url(&parse_url(query).unwrap()).unwrap(),
            )
            .unwrap()
        };
        let relevance = payload(
            "/api/search?q=needle&sort=relevance&source=claude&project=memex&origin=all&limit=1",
        );
        assert_eq!(relevance.results[0].session_id, "relevant");
        assert!(relevance.results[0].score.is_some());

        let newest = payload(
            "/api/search?q=needle&sort=newest&source=claude&project=memex&origin=all&limit=1",
        );
        assert_eq!(newest.results[0].session_id, "dense");
        assert_eq!(newest.results[0].ts, 10_110);
        assert!(newest.has_more);

        // The first 100 timestamp-ordered matching records all belong to `dense`. Page two must
        // still be selected from the globally grouped session order, not from a loaded UI page.
        let second_page = payload(
            "/api/search?q=needle&sort=newest&source=claude&project=memex&origin=all&offset=1&limit=1",
        );
        assert_eq!(second_page.results[0].session_id, "second");
        assert_eq!(second_page.results[0].ts, 9_000);
        assert!(second_page.has_more);

        let oldest =
            payload("/api/search?sort=oldest&source=claude&project=memex&origin=all&limit=1");
        assert_eq!(oldest.results[0].session_id, "relevant");
        assert_eq!(oldest.results[0].ts, 100);
    }

    #[test]
    fn chronological_representative_survives_prefer_main_origin_classification() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let mut records = (1..=110)
            .map(|doc_id| {
                let mut value = record(doc_id, "mixed", "/tmp/mixed.jsonl", "needle".to_string());
                value.ts = doc_id;
                value.links.conversation_kind = Some("sidechain".to_string());
                value
            })
            .collect::<Vec<_>>();
        let mut other = record(200, "other", "/tmp/other.jsonl", "needle".to_string());
        other.ts = 200;
        other.links.conversation_kind = Some("main".to_string());
        records.push(other);
        let mut main = record(300, "mixed", "/tmp/mixed.jsonl", "needle".to_string());
        main.ts = 10_000;
        main.links.conversation_kind = Some("main".to_string());
        records.push(main);
        let expected_record_id = crate::retrieval::canonical_record_id(&records[0]);
        publish_records(&paths, records);

        let payload = search_payload(
            &paths,
            &SearchRequest::from_url(
                &parse_url("/api/search?q=needle&sort=oldest&origin=interactive&limit=1").unwrap(),
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(payload.results[0].session_id, "mixed");
        assert_eq!(payload.results[0].ts, 1);
        assert_eq!(payload.results[0].record_id, expected_record_id);
        assert!(payload.has_more);
    }

    #[test]
    fn search_payload_centers_and_marks_late_unicode_matches() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let text = format!(
            "Readable message prefix {}needle evidence",
            "界 ".repeat(240)
        );
        publish_records(
            &paths,
            [record(1, "late-match", "/tmp/late-match.jsonl", text)],
        );

        let searched = search_payload(
            &paths,
            &SearchRequest {
                query: "text:needle".to_string(),
                source: None,
                project: None,
                offset: 0,
                limit: 30,
                origin: SessionKindFilter::Primary,
                range: TimeRange::All,
                sort: SearchSort::Relevance,
            },
        )
        .unwrap();
        let result = &searched.results[0];
        assert!(result.snippet.starts_with('…'));
        assert!(result.snippet.contains("needle evidence"));
        assert!(result.snippet.chars().count() <= 160);
        assert_eq!(result.snippet_matches.len(), 1);
        let hit = &result.snippet_matches[0];
        assert_eq!(
            result
                .snippet
                .chars()
                .skip(hit.start)
                .take(hit.end - hit.start)
                .collect::<String>(),
            "needle"
        );

        let recent = search_payload(
            &paths,
            &SearchRequest {
                query: String::new(),
                source: None,
                project: None,
                offset: 0,
                limit: 30,
                origin: SessionKindFilter::Primary,
                range: TimeRange::All,
                sort: SearchSort::Newest,
            },
        )
        .unwrap();
        assert!(
            recent.results[0]
                .snippet
                .starts_with("Readable message prefix")
        );
        assert!(recent.results[0].snippet_matches.is_empty());
    }

    #[test]
    fn session_tail_is_bounded_and_prioritizes_latest_messages() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        publish_records(
            &paths,
            (1..=40).map(|doc_id| {
                record(
                    doc_id,
                    "long-session",
                    "/tmp/long.jsonl",
                    "é".repeat(40_000),
                )
            }),
        );

        let page = session_payload(
            &paths,
            &SessionRequest {
                session_id: "long-session".to_string(),
                source_path: Some("/tmp/long.jsonl".to_string()),
                source: Some(SourceKind::Claude),
                selection: SessionSelection::Tail,
                limit: 20,
                version: None,
            },
        )
        .unwrap()
        .unwrap();

        assert_eq!(page.offset, 20);
        assert_eq!(page.total, 40);
        assert_eq!(page.started_at, 1_000);
        assert_eq!(page.ended_at, 40_000);
        assert!(page.messages[0].content.is_empty());
        assert!(!page.messages.last().unwrap().content.is_empty());
        assert!(
            page.messages
                .iter()
                .all(|message| message.content.is_char_boundary(message.content.len()))
        );
        assert!(
            page.messages
                .iter()
                .map(|message| message.content.len())
                .sum::<usize>()
                <= MAX_SESSION_CONTENT_BYTES
        );
    }

    #[test]
    fn session_around_finds_a_late_hit_without_prefix_paging() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let records = (1..=400)
            .map(|doc_id| {
                record(
                    doc_id,
                    "late",
                    "/tmp/late.jsonl",
                    format!("message {doc_id}"),
                )
            })
            .collect::<Vec<_>>();
        let hit_id = crate::retrieval::canonical_record_id(&records[389]);
        publish_records(&paths, records);

        let page = session_payload(
            &paths,
            &SessionRequest {
                session_id: "late".to_string(),
                source_path: None,
                source: None,
                selection: SessionSelection::Around(hit_id.clone()),
                limit: 21,
                version: None,
            },
        )
        .unwrap()
        .unwrap();

        assert_eq!(page.offset, 379);
        assert_eq!(page.messages.len(), 21);
        assert_eq!(page.messages[10].record_id, hit_id);
        assert_eq!(page.next_offset, None);
    }

    #[test]
    fn source_path_disambiguates_reused_session_ids() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        publish_records(
            &paths,
            [
                record(
                    1,
                    "reused",
                    "/tmp/first.jsonl",
                    "collision first".to_string(),
                ),
                record(
                    2,
                    "reused",
                    "/tmp/second.jsonl",
                    "collision second".to_string(),
                ),
            ],
        );

        let search = search_payload(
            &paths,
            &SearchRequest {
                query: "collision".to_string(),
                source: None,
                project: None,
                offset: 0,
                limit: 10,
                origin: SessionKindFilter::Primary,
                range: TimeRange::All,
                sort: SearchSort::Relevance,
            },
        )
        .unwrap();
        assert_eq!(search.results.len(), 2);
        assert_ne!(search.results[0].record_id, search.results[1].record_id);

        for result in search.results {
            let page = session_payload(
                &paths,
                &SessionRequest {
                    session_id: result.session_id,
                    source_path: Some(result.source_path.clone()),
                    source: Some(SourceKind::Claude),
                    selection: SessionSelection::Offset(0),
                    limit: 10,
                    version: None,
                },
            )
            .unwrap()
            .unwrap();
            assert_eq!(page.total, 1);
            assert_eq!(page.source_path, result.source_path);
        }
    }

    #[test]
    fn session_source_disambiguates_same_id_and_path_across_sources() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let claude = record(
            1,
            "shared",
            "/tmp/shared.jsonl",
            "shared claude".to_string(),
        );
        let mut codex = record(2, "shared", "/tmp/shared.jsonl", "shared codex".to_string());
        codex.source = SourceKind::Codex;
        publish_records(&paths, [claude, codex]);

        let ambiguous = session_payload(
            &paths,
            &SessionRequest {
                session_id: "shared".to_string(),
                source_path: Some("/tmp/shared.jsonl".to_string()),
                source: None,
                selection: SessionSelection::Offset(0),
                limit: 10,
                version: None,
            },
        )
        .unwrap_err();
        assert!(ambiguous.downcast_ref::<AmbiguousSessionScope>().is_some());

        let scoped = session_payload(
            &paths,
            &SessionRequest {
                session_id: "shared".to_string(),
                source_path: Some("/tmp/shared.jsonl".to_string()),
                source: Some(SourceKind::Codex),
                selection: SessionSelection::Offset(0),
                limit: 10,
                version: None,
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(scoped.total, 1);
        assert_eq!(scoped.source, "codex");
        assert_eq!(scoped.messages[0].content, "shared codex");
    }

    #[test]
    fn stale_snapshot_versions_are_rejected() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        publish_records(
            &paths,
            [record(
                1,
                "versioned",
                "/tmp/versioned.jsonl",
                "one".to_string(),
            )],
        );
        let first = session_payload(
            &paths,
            &SessionRequest {
                session_id: "versioned".to_string(),
                source_path: Some("/tmp/versioned.jsonl".to_string()),
                source: Some(SourceKind::Claude),
                selection: SessionSelection::Offset(0),
                limit: 10,
                version: None,
            },
        )
        .unwrap()
        .unwrap();
        publish_records(
            &paths,
            [record(
                2,
                "versioned",
                "/tmp/versioned.jsonl",
                "two".to_string(),
            )],
        );

        let error = session_payload(
            &paths,
            &SessionRequest {
                session_id: "versioned".to_string(),
                source_path: Some("/tmp/versioned.jsonl".to_string()),
                source: Some(SourceKind::Claude),
                selection: SessionSelection::Offset(0),
                limit: 10,
                version: Some(first.version),
            },
        )
        .unwrap_err();
        assert!(error.downcast_ref::<StaleSnapshot>().is_some());
    }

    #[test]
    fn content_expansion_uses_utf8_safe_byte_offsets() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let record = record(
            1,
            "expand",
            "/tmp/expand.jsonl",
            "é".repeat(MAX_CONTENT_PAGE_BYTES),
        );
        let record_id = crate::retrieval::canonical_record_id(&record);
        publish_records(&paths, [record]);

        let first = session_content_payload(
            &paths,
            &SessionContentRequest {
                session_id: "expand".to_string(),
                source_path: Some("/tmp/expand.jsonl".to_string()),
                source: Some(SourceKind::Claude),
                record_id: record_id.clone(),
                offset: 0,
                limit: MAX_CONTENT_PAGE_BYTES - 1,
                version: None,
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(first.content.len(), MAX_CONTENT_PAGE_BYTES - 2);
        assert!(first.truncated);

        let error = session_content_payload(
            &paths,
            &SessionContentRequest {
                session_id: "expand".to_string(),
                source_path: Some("/tmp/expand.jsonl".to_string()),
                source: Some(SourceKind::Claude),
                record_id,
                offset: 1,
                limit: 10,
                version: None,
            },
        )
        .unwrap_err();
        assert!(error.downcast_ref::<InvalidContentOffset>().is_some());
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
    fn search_payload_filters_sessions_by_origin() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let index = SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
        let mut writer = index.writer().unwrap();
        for (doc_id, session_id, kind) in [
            (1, "session-main", "main"),
            (2, "session-sub", "subagent"),
            (3, "session-review", "guardian_review"),
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
                        text: "fix the flaky widget".to_string(),
                        tool_name: None,
                        tool_input: None,
                        tool_output: None,
                        links: RecordLinks {
                            conversation_kind: Some(kind.to_string()),
                            ..RecordLinks::default()
                        },
                        source_path: "/tmp/session.jsonl".to_string(),
                    },
                )
                .unwrap();
        }
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        index.publish_generation().unwrap();

        let payload_for = |origin| {
            search_payload(
                &paths,
                &SearchRequest {
                    query: "flaky".to_string(),
                    source: None,
                    project: None,
                    offset: 0,
                    limit: 30,
                    origin,
                    range: TimeRange::All,
                    sort: SearchSort::Relevance,
                },
            )
            .unwrap()
        };
        let ids = |payload: SearchPayload| {
            payload
                .results
                .iter()
                .map(|result| result.session_id.clone())
                .collect::<Vec<_>>()
        };

        // Default hides subagent sessions.
        assert_eq!(
            ids(payload_for(SessionKindFilter::Primary)),
            vec!["session-main"]
        );
        assert_eq!(
            ids(payload_for(SessionKindFilter::Subagent)),
            vec!["session-sub"]
        );
        let mut all = ids(payload_for(SessionKindFilter::All));
        all.sort();
        assert_eq!(all, vec!["session-main", "session-review", "session-sub"]);
        let mut regular = ids(payload_for(SessionKindFilter::Regular));
        regular.sort();
        assert_eq!(regular, vec!["session-main", "session-sub"]);
    }

    #[test]
    fn activity_payload_filters_sessions_by_origin() {
        use crate::analytics::AnalyticsWriter;
        use crate::types::Record;

        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let mut writer =
            AnalyticsWriter::open(analytics_path(&paths.state)).expect("open analytics");
        for (session_id, kind, ts) in [
            ("s-main", "main", now_ms),
            ("s-sub", "subagent", now_ms),
            ("s-review", "guardian_review", now_ms),
        ] {
            writer
                .record(&Record {
                    source: SourceKind::Claude,
                    doc_id: 1,
                    ts,
                    project: "memex".to_string(),
                    session_id: session_id.to_string(),
                    turn_id: 0,
                    role: "user".to_string(),
                    text: "fix the flaky widget".to_string(),
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: RecordLinks {
                        conversation_kind: Some(kind.to_string()),
                        ..RecordLinks::default()
                    },
                    source_path: "/tmp/session.jsonl".to_string(),
                })
                .expect("record");
        }
        writer.flush().expect("flush");

        let total = |origin| {
            activity_payload(
                &paths,
                &ActivityRequest {
                    metric: ActivityMetric::Sessions,
                    query: String::new(),
                    source: None,
                    project: None,
                    days: 30,
                    origin,
                    range: None,
                },
            )
            .expect("payload")
            .points
            .iter()
            .map(|point| point.value)
            .sum::<u64>()
        };
        assert_eq!(total(SessionKindFilter::Primary), 1);
        assert_eq!(total(SessionKindFilter::Subagent), 1);
        assert_eq!(total(SessionKindFilter::Regular), 2);
        assert_eq!(total(SessionKindFilter::All), 3);
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
    fn bootstrap_urls_preserve_the_listener_host() {
        let temp = TempDir::new().unwrap();
        let root = Some(temp.path().to_path_buf());

        assert!(
            bootstrap_url(root.clone(), "127.0.0.1:6363")
                .unwrap()
                .starts_with("http://127.0.0.1:6363/#bootstrap=")
        );
        assert!(
            bootstrap_url(root.clone(), "localhost:6363")
                .unwrap()
                .starts_with("http://localhost:6363/#bootstrap=")
        );
        assert!(
            bootstrap_url(root, "[::1]:6363")
                .unwrap()
                .starts_with("http://[::1]:6363/#bootstrap=")
        );
    }

    #[test]
    fn private_api_requires_authentication_and_accepts_single_use_bootstrap_session() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        SearchIndex::open_or_create_for_ingest(&paths.index)
            .unwrap()
            .publish_generation()
            .unwrap();
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
                "POST /auth/exchange HTTP/1.1\r\nHost: localhost:6363\r\nOrigin: http://localhost:6363\r\nSec-Fetch-Site: same-origin\r\nAuthorization: Bearer {bootstrap}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(exchange.starts_with("HTTP/1.1 200"));
        let set_cookie = response_header(&exchange, "Set-Cookie").unwrap();
        let cookie = set_cookie.split(';').next().unwrap();
        assert!(set_cookie.contains("HttpOnly"));
        assert!(set_cookie.contains("SameSite=Strict"));
        assert!(set_cookie.contains("Path=/"));
        assert!(set_cookie.contains("Max-Age=43200"));
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

        let cookie_authorized = http_round_trip(
            &paths,
            &auth,
            format!(
                "GET /api/stats HTTP/1.1\r\nHost: localhost:6363\r\nOrigin: http://localhost:6363\r\nSec-Fetch-Site: same-origin\r\nCookie: {cookie}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(cookie_authorized.starts_with("HTTP/1.1 200"));

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
    fn browser_auth_rejects_cross_origin_invalid_expired_and_ambiguous_credentials() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        SearchIndex::open_or_create_for_ingest(&paths.index)
            .unwrap()
            .publish_generation()
            .unwrap();
        let auth = WebAuth::load_or_create(&paths).unwrap();
        let cookie_name = session_cookie_name(&paths, DEFAULT_LISTEN).unwrap();

        for token in ["invalid".to_string(), auth.create_expired_bootstrap_token()] {
            let response = http_round_trip(
                &paths,
                &auth,
                format!(
                    "POST /auth/exchange HTTP/1.1\r\nHost: 127.0.0.1:6363\r\nOrigin: http://127.0.0.1:6363\r\nSec-Fetch-Site: same-origin\r\nAuthorization: Bearer {token}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                ),
            );
            assert!(response.starts_with("HTTP/1.1 401"));
        }

        let bootstrap = auth.create_bootstrap_token().unwrap();
        let cross_origin = http_round_trip(
            &paths,
            &auth,
            format!(
                "POST /auth/exchange HTTP/1.1\r\nHost: localhost:6363\r\nOrigin: http://localhost:7777\r\nSec-Fetch-Site: same-site\r\nAuthorization: Bearer {bootstrap}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(cross_origin.starts_with("HTTP/1.1 403"));

        let exchange = http_round_trip(
            &paths,
            &auth,
            format!(
                "POST /auth/exchange HTTP/1.1\r\nHost: localhost:6363\r\nOrigin: http://localhost:6363\r\nSec-Fetch-Site: same-origin\r\nAuthorization: Bearer {bootstrap}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(exchange.starts_with("HTTP/1.1 200"));
        let cookie = response_header(&exchange, "Set-Cookie")
            .unwrap()
            .split(';')
            .next()
            .unwrap();

        let cross_port = http_round_trip(
            &paths,
            &auth,
            format!(
                "GET /api/stats HTTP/1.1\r\nHost: localhost:6363\r\nOrigin: http://localhost:7777\r\nSec-Fetch-Site: same-site\r\nCookie: {cookie}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(cross_port.starts_with("HTTP/1.1 401"));

        let duplicate = http_round_trip(
            &paths,
            &auth,
            format!(
                "GET /api/stats HTTP/1.1\r\nHost: localhost:6363\r\nOrigin: http://localhost:6363\r\nSec-Fetch-Site: same-origin\r\nCookie: {cookie}; {cookie_name}=other\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(duplicate.starts_with("HTTP/1.1 401"));

        let invalid_bearer = http_round_trip(
            &paths,
            &auth,
            format!(
                "GET /api/stats HTTP/1.1\r\nHost: localhost:6363\r\nOrigin: http://localhost:6363\r\nSec-Fetch-Site: same-origin\r\nAuthorization: Bearer invalid\r\nCookie: {cookie}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(invalid_bearer.starts_with("HTTP/1.1 401"));
    }

    #[test]
    fn cookie_namespace_varies_by_root_and_port_and_sessions_end_on_restart() {
        let first = TempDir::new().unwrap();
        let second = TempDir::new().unwrap();
        let first_paths = Paths::new(Some(first.path().to_path_buf())).unwrap();
        let second_paths = Paths::new(Some(second.path().to_path_buf())).unwrap();
        let first_auth = WebAuth::load_or_create(&first_paths).unwrap();
        let second_auth = WebAuth::load_or_create(&first_paths).unwrap();
        let _other_root_auth = WebAuth::load_or_create(&second_paths).unwrap();
        let session = first_auth
            .exchange_bootstrap_token(&first_auth.create_bootstrap_token().unwrap())
            .unwrap();

        assert_ne!(
            session_cookie_name(&first_paths, "127.0.0.1:6363").unwrap(),
            session_cookie_name(&first_paths, "127.0.0.1:6364").unwrap()
        );
        assert_ne!(
            session_cookie_name(&first_paths, DEFAULT_LISTEN).unwrap(),
            session_cookie_name(&second_paths, DEFAULT_LISTEN).unwrap()
        );
        assert!(!second_auth.authorize_session(&session));
    }

    #[test]
    fn private_api_accepts_installation_bearer_token() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        SearchIndex::open_or_create_for_ingest(&paths.index)
            .unwrap()
            .publish_generation()
            .unwrap();
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
