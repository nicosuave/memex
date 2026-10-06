//! Blocking JSON-over-HTTP transport shared by the remote model clients.

use std::fmt;
use std::io::Read;
use std::sync::Once;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use ureq::Agent;
use ureq::tls::{RootCerts, TlsConfig, TlsProvider};

const MAX_ERROR_BODY_BYTES: u64 = 64 * 1024;
const MAX_ERROR_MESSAGE_BYTES: usize = 512;
const DEFAULT_BACKOFF_BASE: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Least time a retry must have left in the budget for its attempt.
const MIN_ATTEMPT_TIME: Duration = Duration::from_secs(1);

static EMBEDDING_PLAIN_HTTP_WARNING: Once = Once::new();
static RERANK_PLAIN_HTTP_WARNING: Once = Once::new();

/// What a client sends, which selects the wording of its errors and warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Purpose {
    /// Transcript text sent to an embeddings server.
    Embedding,
    /// Search results sent to a rerank server.
    Rerank,
}

impl Purpose {
    /// Noun used in request and response errors.
    fn noun(self) -> &'static str {
        match self {
            Self::Embedding => "embedding",
            Self::Rerank => "rerank",
        }
    }

    /// Name of the URL setting in validation errors.
    fn url_label(self) -> &'static str {
        match self {
            Self::Embedding => "embedding base URL",
            Self::Rerank => "rerank URL",
        }
    }

    fn warn_plain_http(self) {
        match self {
            Self::Embedding => EMBEDDING_PLAIN_HTTP_WARNING.call_once(|| {
                eprintln!(
                    "warning: embedding_base_url uses plain http to a non-local host; transcript text is sent unencrypted"
                );
            }),
            Self::Rerank => RERANK_PLAIN_HTTP_WARNING.call_once(|| {
                eprintln!(
                    "warning: rerank URL uses plain http to a non-local host; search results are sent unencrypted"
                );
            }),
        }
    }
}

/// Connection settings for a remote model server.
#[derive(Clone, PartialEq, Eq)]
pub struct RemoteEndpoint {
    /// Server URL; each client documents the request URL it derives from it.
    pub base_url: String,
    /// Bearer token sent only when present and non-empty.
    pub api_key: Option<String>,
    /// Timeout for each request attempt, including reading the response body.
    pub timeout: Duration,
    /// Retries after the first attempt for transient failures.
    pub max_retries: u32,
}

impl fmt::Debug for RemoteEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteEndpoint")
            .field("base_url", &self.base_url)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("timeout", &self.timeout)
            .field("max_retries", &self.max_retries)
            .finish()
    }
}

impl RemoteEndpoint {
    /// [`Self::validate_for`] with [`Purpose::Embedding`] wording.
    pub fn validate(&self) -> Result<()> {
        self.validate_for(Purpose::Embedding)
    }

    /// Rejects base URLs that are not `http`/`https` with a non-empty host,
    /// that carry credentials, a query, or a fragment, or that use plain `http`
    /// to a non-loopback host while an API key is set.
    ///
    /// Plain `http` to a non-loopback host without an API key is accepted with
    /// a warning on stderr, printed at most once per process and purpose. Errors
    /// name the URL by [`url_origin`] only.
    pub fn validate_for(&self, purpose: Purpose) -> Result<()> {
        let label = purpose.url_label();
        let url = url::Url::parse(&self.base_url).with_context(|| format!("invalid {label}"))?;
        let origin = url_origin(&self.base_url);
        if !matches!(url.scheme(), "http" | "https") {
            bail!(
                "{label} {origin} must use http or https, not {:?}",
                url.scheme()
            );
        }
        if url.host_str().is_none_or(str::is_empty) {
            bail!("{label} has no host");
        }
        if !url.username().is_empty() || url.password().is_some() {
            bail!("{label} must not contain credentials; set the API key instead");
        }
        if url.query().is_some() || url.fragment().is_some() {
            bail!("{label} {origin} must not contain a query or fragment");
        }
        if url.scheme() == "http" && !is_loopback(&url) {
            if self.bearer_key().is_some() {
                bail!(
                    "{label} {origin} uses plain http to a non-local host; use https or a loopback host, because the API key would be sent in clear text"
                );
            }
            purpose.warn_plain_http();
        }
        Ok(())
    }

    fn bearer_key(&self) -> Option<&str> {
        self.api_key.as_deref().filter(|key| !key.is_empty())
    }
}

/// Upper bounds a client applies to its endpoint and responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientLimits {
    /// The endpoint's `max_retries` is clamped to this.
    pub max_retries: u32,
    /// The endpoint's `timeout` is clamped to this.
    pub max_timeout: Duration,
    /// Largest accepted success response body, in MiB.
    pub max_success_body_mib: u64,
    /// Cap on the time budget of one request including its retries; the budget is
    /// the clamped `timeout` times the number of attempts, at most this. Each
    /// attempt times out by the end of the budget, and a retry that would have less
    /// than a second of it left after its wait is not made. `None` sets no budget.
    pub max_total: Option<Duration>,
}

/// Blocking client that posts JSON to a validated [`RemoteEndpoint`].
///
/// Redirects are not followed. Requests fail over to retries with exponential
/// backoff and jitter, or the server's `Retry-After` seconds capped at 30 s,
/// only for HTTP 429 and 5xx, timeouts, and connection failures, within the
/// time budget of [`ClientLimits::max_total`]. Error text derived from the server
/// or the transport has control and invisible formatting characters removed and
/// the API key redacted, and names request URLs by [`url_origin`] only.
pub struct HttpClient {
    endpoint: RemoteEndpoint,
    purpose: Purpose,
    max_success_body_mib: u64,
    /// Time from the first attempt after which no retry starts.
    budget: Option<Duration>,
    agent: Agent,
    backoff_base: Duration,
}

impl fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpClient")
            .field("endpoint", &self.endpoint)
            .field("purpose", &self.purpose)
            .field("max_success_body_mib", &self.max_success_body_mib)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct ErrorEnvelope {
    error: ErrorDetail,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ErrorDetail {
    Object { message: String },
    Text(String),
}

/// Outcome of one failed request attempt.
enum AttemptError {
    /// 429, 5xx, timeout, or connection failure; `retry_after` is the server's
    /// hint, and `kind` is a short label free of server-supplied text.
    Transient {
        error: anyhow::Error,
        kind: String,
        retry_after: Option<Duration>,
    },
    /// Failure that a retry cannot fix.
    Fatal(anyhow::Error),
}

impl HttpClient {
    /// Validates `endpoint` for `purpose`, rejects a zero timeout, and clamps
    /// the endpoint's `max_retries` and `timeout` to `limits`.
    pub fn new(
        mut endpoint: RemoteEndpoint,
        purpose: Purpose,
        limits: ClientLimits,
    ) -> Result<Self> {
        endpoint.validate_for(purpose)?;
        if endpoint.timeout.is_zero() {
            bail!(
                "{} request timeout must be greater than zero",
                purpose.noun()
            );
        }
        endpoint.max_retries = endpoint.max_retries.min(limits.max_retries);
        endpoint.timeout = endpoint.timeout.min(limits.max_timeout);
        let budget = limits.max_total.map(|cap| {
            endpoint
                .timeout
                .saturating_mul(endpoint.max_retries.saturating_add(1))
                .min(cap)
        });
        let tls = TlsConfig::builder()
            .provider(TlsProvider::NativeTls)
            .root_certs(RootCerts::PlatformVerifier)
            .build();
        let agent: Agent = Agent::config_builder()
            .tls_config(tls)
            .http_status_as_error(false)
            .max_redirects(0)
            .timeout_global(Some(endpoint.timeout))
            .build()
            .into();
        Ok(Self {
            endpoint,
            purpose,
            max_success_body_mib: limits.max_success_body_mib,
            budget,
            agent,
            backoff_base: DEFAULT_BACKOFF_BASE,
        })
    }

    /// The validated endpoint with clamped retries and timeout.
    pub fn endpoint(&self) -> &RemoteEndpoint {
        &self.endpoint
    }

    /// Posts `body` to `url` and returns `parse` applied to the success body.
    ///
    /// A non-2xx status fails with `"{noun} server returned HTTP {status}:
    /// {message}"`. An error from `parse` is returned without retrying.
    pub fn post_json<B, R>(
        &self,
        url: &str,
        body: &B,
        parse: impl Fn(&[u8]) -> Result<R>,
    ) -> Result<R>
    where
        B: Serialize + ?Sized,
    {
        self.post_json_with(url, body, |_, _| None, parse)
    }

    /// [`Self::post_json`], where `describe_status` may replace the error for
    /// a non-2xx status; it receives the status and the sanitized, truncated
    /// server message.
    ///
    /// Whether the status is retried does not depend on `describe_status`.
    pub fn post_json_with<B, R>(
        &self,
        url: &str,
        body: &B,
        describe_status: impl Fn(u16, &str) -> Option<anyhow::Error>,
        parse: impl Fn(&[u8]) -> Result<R>,
    ) -> Result<R>
    where
        B: Serialize + ?Sized,
    {
        let noun = self.purpose.noun();
        let origin = url_origin(url);
        let attempts = self.endpoint.max_retries.saturating_add(1);
        let started = Instant::now();
        let mut attempt: u32 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            let timeout = self.budget.map(|budget| {
                self.endpoint
                    .timeout
                    .min(budget.saturating_sub(started.elapsed()))
            });
            match self.attempt(url, body, timeout, &describe_status, &parse) {
                Ok(parsed) => return Ok(parsed),
                Err(AttemptError::Fatal(error)) => return Err(error),
                Err(AttemptError::Transient {
                    error,
                    kind,
                    retry_after,
                }) => {
                    if attempt >= attempts {
                        return Err(error.context(format!(
                            "{noun} request to {origin} failed after {attempt} attempt(s)"
                        )));
                    }
                    let delay = retry_after
                        .map(|hint| hint.min(MAX_BACKOFF))
                        .unwrap_or_else(|| backoff_delay(self.backoff_base, attempt));
                    if let Some(budget) = self.budget
                        && started
                            .elapsed()
                            .saturating_add(delay)
                            .saturating_add(MIN_ATTEMPT_TIME)
                            > budget
                    {
                        return Err(error.context(format!(
                            "{noun} request to {origin} failed after {attempt} attempt(s); \
                             a retry in {}ms would exceed the {}ms time budget",
                            delay.as_millis(),
                            budget.as_millis()
                        )));
                    }
                    eprintln!(
                        "warning: {noun} request attempt {attempt}/{attempts} failed ({kind}), retrying in {}ms",
                        delay.as_millis()
                    );
                    thread::sleep(delay);
                }
            }
        }
    }

    /// Replaces the API key and its URL-encoded and base64 forms with
    /// `<redacted>`, removes control characters and [`is_invisible_format`]
    /// characters, and replaces the forms again.
    ///
    /// The forms are built from the key as configured and from the key without
    /// the removed characters, so neither a key that contains such characters
    /// nor an echo split by inserted ones escapes the redaction.
    pub fn sanitize(&self, text: &str) -> String {
        let Some(key) = self.endpoint.bearer_key() else {
            return strip_invisible(text);
        };
        let stripped_key = strip_invisible(key);
        let mut forms: Vec<String> = [key, stripped_key.as_str()]
            .into_iter()
            .filter(|key| !key.is_empty())
            .flat_map(|key| {
                [
                    key.to_owned(),
                    url::form_urlencoded::byte_serialize(key.as_bytes()).collect(),
                    STANDARD.encode(key),
                    STANDARD_NO_PAD.encode(key),
                    URL_SAFE.encode(key),
                    URL_SAFE_NO_PAD.encode(key),
                ]
            })
            .collect();
        // Longest first, so a padded form is replaced before its unpadded prefix.
        forms.sort_by_key(|form| std::cmp::Reverse(form.len()));
        forms.dedup();
        let redact = |mut text: String| {
            for form in &forms {
                text = text.replace(form.as_str(), "<redacted>");
            }
            text
        };
        redact(strip_invisible(&redact(text.to_owned())))
    }

    #[cfg(test)]
    pub(crate) fn set_backoff_base(&mut self, base: Duration) {
        self.backoff_base = base;
    }

    fn attempt<B, R>(
        &self,
        url: &str,
        body: &B,
        timeout: Option<Duration>,
        describe_status: &impl Fn(u16, &str) -> Option<anyhow::Error>,
        parse: &impl Fn(&[u8]) -> Result<R>,
    ) -> Result<R, AttemptError>
    where
        B: Serialize + ?Sized,
    {
        let mut request = self.agent.post(url);
        if let Some(timeout) = timeout {
            request = request.config().timeout_global(Some(timeout)).build();
        }
        if let Some(key) = self.endpoint.bearer_key() {
            request = request.header("Authorization", format!("Bearer {key}"));
        }
        let response = request
            .send_json(body)
            .map_err(|e| self.transport_error(url, e))?;
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        let mut response_body = response.into_body();

        if !(200..300).contains(&status) {
            let mut raw = Vec::new();
            // A partial error body still yields a useful message.
            let _ = response_body
                .as_reader()
                .take(MAX_ERROR_BODY_BYTES)
                .read_to_end(&mut raw);
            let message = self.error_message(&raw);
            let error = describe_status(status, &message).unwrap_or_else(|| {
                anyhow!(
                    "{} server returned HTTP {status}: {message}",
                    self.purpose.noun()
                )
            });
            return Err(if status == 429 || (500..600).contains(&status) {
                AttemptError::Transient {
                    error,
                    kind: format!("HTTP {status}"),
                    retry_after,
                }
            } else {
                AttemptError::Fatal(error)
            });
        }

        let limit = self.max_success_body_mib.saturating_mul(1024 * 1024);
        let raw = response_body
            .with_config()
            .limit(limit)
            .read_to_vec()
            .map_err(|e| self.transport_error(url, e))?;
        parse(&raw).map_err(AttemptError::Fatal)
    }

    fn transport_error(&self, url: &str, error: ureq::Error) -> AttemptError {
        let noun = self.purpose.noun();
        let kind = match &error {
            ureq::Error::Timeout(_) => Some("timeout"),
            ureq::Error::Io(_) => Some("I/O error"),
            ureq::Error::ConnectionFailed => Some("connection failed"),
            ureq::Error::HostNotFound => Some("host not found"),
            _ => None,
        };
        let error = match error {
            ureq::Error::BodyExceedsLimit(_) => anyhow!(
                "{noun} response exceeds the {} MiB limit",
                self.max_success_body_mib
            ),
            ureq::Error::Http(_) => {
                anyhow!(
                    "{noun} request could not be built; check that the API key is a valid header value"
                )
            }
            // These describe the connection or TLS session and never the URL.
            known @ (ureq::Error::Timeout(_)
            | ureq::Error::Io(_)
            | ureq::Error::ConnectionFailed
            | ureq::Error::HostNotFound
            | ureq::Error::Tls(_)
            | ureq::Error::NativeTls(_)
            | ureq::Error::TlsRequired
            | ureq::Error::BodyStalled) => anyhow!(
                "{noun} request to {} failed: {}",
                url_origin(url),
                scrub_url(&self.sanitize(&known.to_string()), url)
            ),
            // Other errors may quote the URL in a normalized form.
            _ => anyhow!(
                "{noun} request to {} failed: the HTTP client rejected the request or response",
                url_origin(url)
            ),
        };
        match kind {
            Some(kind) => AttemptError::Transient {
                error,
                kind: kind.to_owned(),
                retry_after: None,
            },
            None => AttemptError::Fatal(error),
        }
    }

    /// Extracts the API error message from `raw`, falling back to the raw text,
    /// sanitized by [`Self::sanitize`] and truncated.
    fn error_message(&self, raw: &[u8]) -> String {
        let message = match serde_json::from_slice::<ErrorEnvelope>(raw) {
            Ok(ErrorEnvelope {
                error: ErrorDetail::Object { message } | ErrorDetail::Text(message),
            }) => message,
            Err(_) => String::from_utf8_lossy(raw).into_owned(),
        };
        let message = self.sanitize(&message);
        let message = truncate_on_char_boundary(message.trim(), MAX_ERROR_MESSAGE_BYTES);
        if message.is_empty() {
            "<empty response body>".to_owned()
        } else {
            message.to_owned()
        }
    }
}

/// `scheme://host[:port]` of `url`, or `<invalid URL>`, so that a path, query, or
/// userinfo that may carry a token never reaches an error or warning.
pub fn url_origin(url: &str) -> String {
    let Ok(parsed) = url::Url::parse(url) else {
        return "<invalid URL>".to_owned();
    };
    match (parsed.host_str(), parsed.port()) {
        (Some(host), Some(port)) => format!("{}://{host}:{port}", parsed.scheme()),
        (Some(host), None) => format!("{}://{host}", parsed.scheme()),
        (None, _) => format!("{}:", parsed.scheme()),
    }
}

/// `text` with `url` and its normalized forms replaced by [`url_origin`], and its
/// path and query by `<path>`.
fn scrub_url(text: &str, url: &str) -> String {
    let origin = url_origin(url);
    let mut forms = vec![url.to_owned()];
    if let Ok(parsed) = url::Url::parse(url) {
        let normalized = parsed.as_str();
        forms.push(normalized.to_owned());
        forms.push(normalized.trim_end_matches('/').to_owned());
        let path_and_query = match parsed.query() {
            Some(query) => format!("{}?{query}", parsed.path()),
            None => parsed.path().to_owned(),
        };
        if path_and_query != "/" {
            forms.push(path_and_query);
        }
    }
    // Longest first, so a whole URL is replaced before its path.
    forms.sort_by_key(|form| std::cmp::Reverse(form.len()));
    forms.dedup();
    let mut text = text.to_owned();
    for form in forms.iter().filter(|form| !form.is_empty()) {
        let replacement = if form.starts_with('/') {
            "<path>"
        } else {
            origin.as_str()
        };
        text = text.replace(form.as_str(), replacement);
    }
    text
}

/// `text` without control characters and [`is_invisible_format`] characters.
fn strip_invisible(text: &str) -> String {
    text.chars()
        .filter(|&c| !c.is_control() && !is_invisible_format(c))
        .collect()
}

/// Unicode format (Cf), line separator (Zl), and paragraph separator (Zp)
/// characters, which can hide or reorder terminal text; as of Unicode 16.0.
fn is_invisible_format(c: char) -> bool {
    matches!(
        c,
        '\u{00AD}'
            | '\u{0600}'..='\u{0605}'
            | '\u{061C}'
            | '\u{06DD}'
            | '\u{070F}'
            | '\u{0890}'..='\u{0891}'
            | '\u{08E2}'
            | '\u{180E}'
            | '\u{200B}'..='\u{200F}'
            | '\u{2028}'..='\u{202E}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{206F}'
            | '\u{FEFF}'
            | '\u{FFF9}'..='\u{FFFB}'
            | '\u{110BD}'
            | '\u{110CD}'
            | '\u{13430}'..='\u{1343F}'
            | '\u{1BCA0}'..='\u{1BCA3}'
            | '\u{1D173}'..='\u{1D17A}'
            | '\u{E0001}'
            | '\u{E0020}'..='\u{E007F}'
    )
}

/// True for `localhost`, `127.0.0.0/8`, and `::1`.
fn is_loopback(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(addr)) => addr.is_loopback(),
        Some(url::Host::Ipv6(addr)) => addr.is_loopback(),
        None => false,
    }
}

/// Exponential backoff for retry number `attempt` (1-based), capped at 30s,
/// with up to 50% added jitter.
fn backoff_delay(base: Duration, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(31);
    let delay = 2u32
        .checked_pow(exponent)
        .and_then(|factor| base.checked_mul(factor))
        .unwrap_or(MAX_BACKOFF)
        .min(MAX_BACKOFF);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos())
        .unwrap_or(0);
    let half = delay / 2;
    let jitter_nanos = half
        .as_nanos()
        .checked_mul(u128::from(nanos % 1000))
        .map(|scaled| scaled / 1000)
        .unwrap_or(0);
    let jitter = Duration::from_nanos(u64::try_from(jitter_nanos).unwrap_or(u64::MAX));
    half.saturating_add(jitter).min(MAX_BACKOFF)
}

fn truncate_on_char_boundary(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    text.get(..end).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::remote_server::{
        KEY, Reply, endpoint, expect_err, join, reply, serve,
    };
    use serde_json::{Value, json};

    const LIMITS: ClientLimits = ClientLimits {
        max_retries: 10,
        max_timeout: Duration::from_secs(600),
        max_success_body_mib: 128,
        max_total: None,
    };

    fn client(base: &str, key: Option<&str>, max_retries: u32) -> Result<HttpClient> {
        let mut client =
            HttpClient::new(endpoint(base, key, max_retries), Purpose::Embedding, LIMITS)?;
        client.set_backoff_base(Duration::from_millis(1));
        Ok(client)
    }

    fn post(client: &HttpClient, base: &str) -> Result<Value> {
        client.post_json(
            &format!("{base}/embeddings"),
            &json!({"input": ["a"]}),
            |raw| Ok(serde_json::from_slice(raw)?),
        )
    }

    fn ok(_: usize, _: &Value) -> Reply {
        reply(200, json!({"data": []}))
    }

    #[test]
    fn retries_429_and_5xx_then_succeeds() -> Result<()> {
        let (base, server) = serve(|n, body| match n {
            0 => Reply {
                status: 429,
                body: json!({"error": {"message": "slow down"}}).to_string(),
                retry_after: Some("0"),
            },
            1 => reply(503, json!({"error": {"message": "overloaded"}})),
            _ => ok(n, body),
        })?;
        let client = client(&base, None, 3)?;
        assert_eq!(post(&client, &base)?, json!({"data": []}));
        assert_eq!(join(server)?.len(), 3);
        Ok(())
    }

    #[test]
    fn does_not_retry_400() -> Result<()> {
        let (base, server) = serve(|_, _| reply(400, json!({"error": {"message": "bad input"}})))?;
        let client = client(&base, None, 3)?;
        let err = expect_err(post(&client, &base))?;
        assert_eq!(join(server)?.len(), 1);
        assert!(
            err.contains("HTTP 400") && err.contains("bad input"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn gives_up_after_max_retries() -> Result<()> {
        let (base, server) = serve(|_, _| reply(503, json!({"error": "unavailable"})))?;
        let client = client(&base, None, 2)?;
        let err = expect_err(post(&client, &base))?;
        assert_eq!(join(server)?.len(), 3);
        assert!(err.contains("after 3 attempt(s)"), "{err}");
        assert!(
            err.contains("HTTP 503") && err.contains("unavailable"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn sends_authorization_only_with_key() -> Result<()> {
        for (key, expected) in [
            (Some(KEY), Some(format!("Bearer {KEY}"))),
            (Some(""), None),
            (None, None),
        ] {
            let (base, server) = serve(ok)?;
            post(&client(&base, key, 0)?, &base)?;
            let captured = join(server)?;
            assert_eq!(captured.len(), 1);
            assert_eq!(captured.first().and_then(|c| c.auth.clone()), expected);
        }
        Ok(())
    }

    #[test]
    fn redirects_are_not_followed() -> Result<()> {
        let (target, target_server) = serve(ok)?;
        let server = tiny_http::Server::http("127.0.0.1:0").map_err(|e| anyhow!("bind: {e}"))?;
        let addr = server
            .server_addr()
            .to_ip()
            .ok_or_else(|| anyhow!("no ip address"))?;
        let location = tiny_http::Header::from_bytes("Location", format!("{target}/embeddings"))
            .map_err(|()| anyhow!("location header"))?;
        let redirector = thread::spawn(move || {
            let mut auths = Vec::new();
            while let Ok(Some(request)) = server.recv_timeout(Duration::from_millis(500)) {
                auths.push(
                    request
                        .headers()
                        .iter()
                        .find(|h| h.field.equiv("Authorization"))
                        .map(|h| h.value.as_str().to_owned()),
                );
                let response = tiny_http::Response::from_string("moved")
                    .with_status_code(302)
                    .with_header(location.clone());
                let _ = request.respond(response);
            }
            auths
        });
        let base = format!("http://{addr}/v1");
        let err = expect_err(post(&client(&base, Some(KEY), 3)?, &base))?;
        let auths = redirector
            .join()
            .map_err(|_| anyhow!("redirect server panicked"))?;
        assert!(err.contains("HTTP 302"), "{err}");
        assert!(!err.contains(KEY), "{err}");
        assert_eq!(auths, vec![Some(format!("Bearer {KEY}"))]);
        assert!(join(target_server)?.is_empty());
        Ok(())
    }

    #[test]
    fn plain_http_to_remote_host_requires_no_key() -> Result<()> {
        let err = expect_err(endpoint("http://example.com/v1", Some(KEY), 0).validate())?;
        assert!(err.contains("use https or a loopback host"), "{err}");
        assert!(!err.contains(KEY), "{err}");
        endpoint("http://example.com/v1", None, 0).validate()?;
        endpoint("http://example.com/v1", Some(""), 0).validate()?;
        endpoint("https://example.com/v1", Some(KEY), 0).validate()?;
        for base in [
            "http://localhost:11434/v1",
            "http://LOCALHOST/v1",
            "http://127.0.0.1:8080/v1",
            "http://127.4.5.6/v1",
            "http://[::1]:8080/v1",
        ] {
            endpoint(base, Some(KEY), 0).validate()?;
        }
        for base in [
            "http://128.0.0.1/v1",
            "http://[::2]/v1",
            "http://localhost.example.com/v1",
            "http://localhost./v1",
            "http://[::ffff:127.0.0.1]/v1",
        ] {
            assert!(endpoint(base, Some(KEY), 0).validate().is_err(), "{base}");
        }
        Ok(())
    }

    #[test]
    fn strips_control_characters_and_encoded_keys() -> Result<()> {
        const ODD_KEY: &str = "sk/odd+key=value";
        let url_encoded: String =
            url::form_urlencoded::byte_serialize(ODD_KEY.as_bytes()).collect();
        let message = format!(
            "\u{1b}[2J\u{1b}]0;title\u{7}\u{202E}bad key sk/odd\u{1b}+key\u{2028}=value url {url_encoded} b64 {} b64url {}\u{FEFF}",
            STANDARD.encode(ODD_KEY),
            URL_SAFE_NO_PAD.encode(ODD_KEY)
        );
        let (base, server) = serve(move |_, _| reply(401, json!({"error": {"message": message}})))?;
        let client = client(&base, Some(ODD_KEY), 0)?;
        let err = expect_err(post(&client, &base))?;
        join(server)?;
        assert!(!err.chars().any(char::is_control), "{err:?}");
        for invisible in ['\u{202E}', '\u{2028}', '\u{FEFF}'] {
            assert!(!err.contains(invisible), "{err:?}");
        }
        for form in [
            ODD_KEY.to_owned(),
            url_encoded,
            STANDARD.encode(ODD_KEY),
            URL_SAFE_NO_PAD.encode(ODD_KEY),
        ] {
            assert!(!err.contains(&form), "{form} in {err}");
        }
        assert_eq!(err.matches("<redacted>").count(), 4, "{err}");
        Ok(())
    }

    #[test]
    fn errors_name_urls_by_origin_only() -> Result<()> {
        assert_eq!(
            url_origin("https://user:pw@h.example:8443/tok-abc/rerank?k=v#f"),
            "https://h.example:8443"
        );
        assert_eq!(url_origin("http://[::1]/tok-abc"), "http://[::1]");
        assert_eq!(url_origin("not a url tok-abc"), "<invalid URL>");
        for base in [
            "not a url tok-abc",
            "ftp://h.example/tok-abc/rerank",
            "https://h.example/tok-abc/rerank?tok-abc",
            "http://h.example/tok-abc/rerank",
        ] {
            let err = expect_err(endpoint(base, Some(KEY), 0).validate_for(Purpose::Rerank))?;
            assert!(!err.contains("tok-abc"), "{err}");
        }

        let (base, server) = serve(|_, _| reply(503, json!({"error": "down"})))?;
        let client = client(&base, None, 0)?;
        let err = expect_err(client.post_json(
            &format!("{base}/tok-abc/rerank"),
            &json!({}),
            |_| Ok(()),
        ))?;
        join(server)?;
        assert!(err.contains("HTTP 503"), "{err}");
        assert!(!err.contains("tok-abc"), "{err}");
        let unreachable =
            client.post_json("http://127.0.0.1:1/tok-abc/rerank", &json!({}), |_| Ok(()));
        let err = expect_err(unreachable)?;
        assert!(err.contains("http://127.0.0.1:1"), "{err}");
        assert!(!err.contains("tok-abc"), "{err}");
        Ok(())
    }

    #[test]
    fn retry_that_would_exceed_the_time_budget_is_skipped() -> Result<()> {
        let (base, server) = serve(|_, _| Reply {
            status: 429,
            body: json!({"error": {"message": "slow down"}}).to_string(),
            retry_after: Some("30"),
        })?;
        let limits = ClientLimits {
            max_total: Some(Duration::from_secs(30)),
            ..LIMITS
        };
        // The budget is the 5 s timeout times 4 attempts: 20 s, below the cap.
        let client = HttpClient::new(endpoint(&base, None, 3), Purpose::Rerank, limits)?;
        assert_eq!(client.budget, Some(Duration::from_secs(20)));
        let started = Instant::now();
        let err = expect_err(post(&client, &base))?;
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(join(server)?.len(), 1);
        assert!(
            err.contains("would exceed the 20000ms time budget"),
            "{err}"
        );
        assert!(
            err.contains("HTTP 429") && err.contains("slow down"),
            "{err}"
        );
        // Without a budget the endpoint keeps its retries.
        assert_eq!(
            HttpClient::new(endpoint(&base, None, 3), Purpose::Rerank, LIMITS)?.budget,
            None
        );
        Ok(())
    }

    #[test]
    fn keys_with_stripped_characters_are_redacted_in_every_form() -> Result<()> {
        // The transport refuses U+200B in a header value, so only the tab key reaches the
        // server; the sanitizer is checked directly for both.
        for (key, sent) in [("sk-a\u{200B}b\tc", false), ("sk-a\tb\tc", true)] {
            let mut forms = Vec::new();
            for form_key in [key, "sk-abc"] {
                forms.extend([
                    form_key.to_owned(),
                    url::form_urlencoded::byte_serialize(form_key.as_bytes()).collect(),
                    STANDARD.encode(form_key),
                    URL_SAFE_NO_PAD.encode(form_key),
                ]);
            }
            let echoed = forms.join(" | ");
            let message = echoed.clone();
            let (base, server) =
                serve(move |_, _| reply(401, json!({"error": {"message": message}})))?;
            let client = client(&base, Some(key), 0)?;
            let outcome = post(&client, &base);
            let captured = join(server)?;
            let errors = [
                expect_err(outcome)?,
                client.sanitize(&echoed),
                client.sanitize("sk-a\u{FEFF}bc"),
            ];
            for err in &errors {
                for form in &forms {
                    assert!(!err.contains(form.as_str()), "{form:?} in {err:?}");
                }
                assert!(!err.contains("sk-a"), "{err:?}");
            }
            assert_eq!(captured.len(), usize::from(sent), "{key:?}");
            if sent {
                assert!(errors[0].contains("HTTP 401"), "{}", errors[0]);
            }
        }
        Ok(())
    }

    #[test]
    fn attempts_end_with_the_time_budget() -> Result<()> {
        let server = tiny_http::Server::http("127.0.0.1:0").map_err(|e| anyhow!("bind: {e}"))?;
        let addr = server
            .server_addr()
            .to_ip()
            .ok_or_else(|| anyhow!("no ip address"))?;
        // Holds every request unanswered until it has seen no request for 6 s.
        let staller = thread::spawn(move || {
            let mut held = Vec::new();
            while let Ok(Some(request)) = server.recv_timeout(Duration::from_secs(6)) {
                held.push(request);
            }
            held.len()
        });
        let limits = ClientLimits {
            max_total: Some(Duration::from_millis(3500)),
            ..LIMITS
        };
        let mut settings = endpoint(&format!("http://{addr}/v1"), None, 3);
        settings.timeout = Duration::from_secs(2);
        let mut client = HttpClient::new(settings, Purpose::Rerank, limits)?;
        client.set_backoff_base(Duration::from_millis(1));
        let started = Instant::now();
        let err =
            expect_err(
                client.post_json(&format!("http://{addr}/v1/rerank"), &json!({}), |_| Ok(())),
            )?;
        let elapsed = started.elapsed();
        // A 2 s attempt, then a retry cut to the 1.5 s left of the 3.5 s budget.
        assert!(
            elapsed >= Duration::from_millis(3300) && elapsed < Duration::from_millis(4500),
            "{elapsed:?}: {err}"
        );
        assert!(err.contains("timeout"), "{err}");
        let held = staller
            .join()
            .map_err(|_| anyhow!("stalling server panicked"))?;
        assert_eq!(held, 2);
        Ok(())
    }

    #[test]
    fn transport_text_loses_normalized_url_forms() {
        let url = "HTTPS://H.Example:443/tok-abc/rerank";
        let text = "failed for https://h.example/tok-abc/rerank, https://h.example/tok-abc/rerank/ \
                    and /tok-abc/rerank";
        let scrubbed = scrub_url(text, url);
        assert!(!scrubbed.contains("tok-abc"), "{scrubbed}");
        assert!(scrubbed.contains("https://h.example"), "{scrubbed}");
        assert_eq!(
            scrub_url("bad HTTPS://H.Example:443/tok-abc/rerank", url),
            "bad https://h.example"
        );
        let with_query = "https://h.example/v1?token=tok-abc";
        assert!(!scrub_url("see /v1?token=tok-abc", with_query).contains("tok-abc"));
    }

    #[test]
    fn invisible_format_ranges_are_exact() {
        for c in [
            '\u{00AD}',
            '\u{0600}',
            '\u{0605}',
            '\u{061C}',
            '\u{06DD}',
            '\u{070F}',
            '\u{0890}',
            '\u{0891}',
            '\u{08E2}',
            '\u{180E}',
            '\u{200B}',
            '\u{200F}',
            '\u{2028}',
            '\u{2029}',
            '\u{202A}',
            '\u{202E}',
            '\u{2060}',
            '\u{2064}',
            '\u{2066}',
            '\u{206F}',
            '\u{FEFF}',
            '\u{FFF9}',
            '\u{FFFB}',
            '\u{110BD}',
            '\u{110CD}',
            '\u{13430}',
            '\u{1343F}',
            '\u{1BCA0}',
            '\u{1BCA3}',
            '\u{1D173}',
            '\u{1D17A}',
            '\u{E0001}',
            '\u{E0020}',
            '\u{E007F}',
        ] {
            assert!(is_invisible_format(c), "{:04X}", u32::from(c));
        }
        for c in [
            '\u{00AC}',
            '\u{00AE}',
            '\u{05FF}',
            '\u{0606}',
            '\u{200A}',
            '\u{2010}',
            '\u{2027}',
            '\u{202F}',
            '\u{205F}',
            '\u{2065}',
            '\u{2070}',
            '\u{FEFE}',
            '\u{FFF8}',
            '\u{FFFC}',
            '\u{1342F}',
            '\u{13440}',
            '\u{1BCA4}',
            '\u{1D172}',
            '\u{1D17B}',
            '\u{E0000}',
            '\u{E0002}',
            '\u{E0080}',
            'a',
            ' ',
        ] {
            assert!(!is_invisible_format(c), "{:04X}", u32::from(c));
        }
    }

    #[test]
    fn retry_attempt_is_cut_to_the_budget_left() -> Result<()> {
        let server = tiny_http::Server::http("127.0.0.1:0").map_err(|e| anyhow!("bind: {e}"))?;
        let addr = server
            .server_addr()
            .to_ip()
            .ok_or_else(|| anyhow!("no ip address"))?;
        // Answers the first request with 503 and holds every later one unanswered.
        let staller = thread::spawn(move || {
            let mut held = Vec::new();
            let mut seen = 0;
            while let Ok(Some(request)) = server.recv_timeout(Duration::from_secs(4)) {
                seen += 1;
                if seen == 1 {
                    let _ = request
                        .respond(tiny_http::Response::from_string("{}").with_status_code(503));
                } else {
                    held.push(request);
                }
            }
            seen
        });
        // The 5 s timeout exceeds the 2 s budget.
        let limits = ClientLimits {
            max_total: Some(Duration::from_secs(2)),
            ..LIMITS
        };
        let mut settings = endpoint(&format!("http://{addr}/v1"), None, 3);
        settings.timeout = Duration::from_secs(5);
        let mut client = HttpClient::new(settings, Purpose::Rerank, limits)?;
        client.set_backoff_base(Duration::from_millis(1));
        let started = Instant::now();
        let err =
            expect_err(
                client.post_json(&format!("http://{addr}/v1/rerank"), &json!({}), |_| Ok(())),
            )?;
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(1800) && elapsed < Duration::from_millis(3000),
            "{elapsed:?}: {err}"
        );
        assert!(err.contains("timeout"), "{err}");
        let seen = staller
            .join()
            .map_err(|_| anyhow!("stalling server panicked"))?;
        assert_eq!(seen, 2);
        Ok(())
    }

    #[test]
    fn transient_kind_excludes_server_text() -> Result<()> {
        let (base, server) =
            serve(|_, _| reply(503, json!({"error": {"message": "secret detail"}})))?;
        let client = client(&base, None, 0)?;
        let outcome = client.attempt(
            &format!("{base}/embeddings"),
            &json!({"input": ["a"]}),
            None,
            &|_, _| None,
            &|_| Ok(()),
        );
        join(server)?;
        match outcome {
            Err(AttemptError::Transient { kind, error, .. }) => {
                assert_eq!(kind, "HTTP 503");
                assert!(format!("{error:#}").contains("secret detail"));
            }
            _ => bail!("expected a transient error"),
        }
        Ok(())
    }

    #[test]
    fn backoff_is_bounded() {
        let base = Duration::from_millis(500);
        let first = backoff_delay(base, 1);
        assert!(
            first >= Duration::from_millis(250) && first <= base,
            "{first:?}"
        );
        assert!(backoff_delay(base, 40) <= MAX_BACKOFF);
    }

    #[test]
    fn truncates_on_char_boundary() {
        let text = "ab\u{e9}cd";
        assert_eq!(truncate_on_char_boundary(text, 3), "ab");
        assert_eq!(truncate_on_char_boundary(text, 4), "ab\u{e9}");
        assert_eq!(truncate_on_char_boundary(text, 100), text);
    }
}
