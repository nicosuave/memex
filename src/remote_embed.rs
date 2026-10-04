//! Embedding client for OpenAI-compatible `/embeddings` endpoints.

use std::fmt;
use std::io::Read;
use std::sync::Once;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use ureq::Agent;
use ureq::tls::{RootCerts, TlsConfig, TlsProvider};

/// Largest number of inputs sent in one request; larger batch sizes are clamped.
pub const MAX_BATCH_SIZE: usize = 2048;
/// Largest accepted vector length, for configured and returned dimensions.
pub const MAX_DIMENSIONS: usize = 65536;
const MAX_SUCCESS_BODY_MIB: u64 = 128;
const MAX_SUCCESS_BODY_BYTES: u64 = MAX_SUCCESS_BODY_MIB * 1024 * 1024;
const MAX_ERROR_BODY_BYTES: u64 = 64 * 1024;
const MAX_RETRIES: u32 = 10;
const MAX_TIMEOUT: Duration = Duration::from_secs(600);
const MAX_ERROR_MESSAGE_BYTES: usize = 512;
const DEFAULT_BACKOFF_BASE: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const PROBE_TEXT: &str = "dimension probe";

static PLAIN_HTTP_WARNING: Once = Once::new();

/// Connection settings for an OpenAI-compatible embeddings server.
#[derive(Clone, PartialEq, Eq)]
pub struct RemoteEndpoint {
    /// Base URL; requests go to `{base_url}/embeddings` with trailing `/` trimmed.
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
    /// Rejects base URLs that are not `http`/`https` with a non-empty host,
    /// that carry credentials, a query, or a fragment, or that use plain `http`
    /// to a non-loopback host while an API key is set.
    ///
    /// Plain `http` to a non-loopback host without an API key is accepted with
    /// a warning on stderr, printed at most once per process.
    pub fn validate(&self) -> Result<()> {
        let url = url::Url::parse(&self.base_url)
            .with_context(|| format!("invalid embedding base URL {:?}", self.base_url))?;
        if !matches!(url.scheme(), "http" | "https") {
            bail!(
                "embedding base URL {:?} must use http or https, not {:?}",
                self.base_url,
                url.scheme()
            );
        }
        if url.host_str().is_none_or(str::is_empty) {
            bail!("embedding base URL {:?} has no host", self.base_url);
        }
        if !url.username().is_empty() || url.password().is_some() {
            bail!("embedding base URL must not contain credentials; set the API key instead");
        }
        if url.query().is_some() || url.fragment().is_some() {
            bail!(
                "embedding base URL {:?} must not contain a query or fragment",
                self.base_url
            );
        }
        if url.scheme() == "http" && !is_loopback(&url) {
            if self.bearer_key().is_some() {
                bail!(
                    "embedding base URL {:?} uses plain http to a non-local host; use https or a loopback host, because the API key would be sent in clear text",
                    self.base_url
                );
            }
            PLAIN_HTTP_WARNING.call_once(|| {
                eprintln!(
                    "warning: embedding_base_url uses plain http to a non-local host; transcript text is sent unencrypted"
                );
            });
        }
        Ok(())
    }

    fn embeddings_url(&self) -> String {
        format!("{}/embeddings", self.base_url.trim_end_matches('/'))
    }

    fn bearer_key(&self) -> Option<&str> {
        self.api_key.as_deref().filter(|key| !key.is_empty())
    }
}

/// Blocking client that embeds texts through a remote endpoint.
pub struct RemoteEmbedder {
    endpoint: RemoteEndpoint,
    url: String,
    agent: Agent,
    model: String,
    dimensions: Option<usize>,
    batch_size: usize,
    dims: Option<usize>,
    /// Whether `dims` came from a probe rather than [`Self::expect_dimensions`].
    dims_probed: bool,
    backoff_base: Duration,
}

impl fmt::Debug for RemoteEmbedder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteEmbedder")
            .field("endpoint", &self.endpoint)
            .field("model", &self.model)
            .field("dimensions", &self.dimensions)
            .field("batch_size", &self.batch_size)
            .field("dims", &self.dims)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
struct EmbeddingRequest<'a> {
    model: &'a str,
    input: &'a [&'a str],
    encoding_format: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    dimensions: Option<usize>,
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingItem>,
}

#[derive(Deserialize)]
struct EmbeddingItem {
    embedding: Vec<f32>,
    index: Option<usize>,
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

impl RemoteEmbedder {
    /// Creates a client; `dimensions` is sent as the OpenAI `dimensions`
    /// parameter and must be in `1..=MAX_DIMENSIONS`, and `batch_size` must be
    /// non-zero and is clamped to [`MAX_BATCH_SIZE`].
    ///
    /// The endpoint's `max_retries` is clamped to 10 and its `timeout` to 600 s.
    pub fn new(
        mut endpoint: RemoteEndpoint,
        model: String,
        dimensions: Option<usize>,
        batch_size: usize,
    ) -> Result<Self> {
        endpoint.validate()?;
        if endpoint.timeout.is_zero() {
            bail!("embedding request timeout must be greater than zero");
        }
        if model.trim().is_empty() {
            bail!("embedding model name must not be empty");
        }
        if dimensions == Some(0) {
            bail!("embedding_dimensions must be greater than zero");
        }
        if let Some(dims) = dimensions.filter(|&dims| dims > MAX_DIMENSIONS) {
            bail!("embedding_dimensions={dims} exceeds the maximum of {MAX_DIMENSIONS}");
        }
        if batch_size == 0 {
            bail!("embedding batch size must be greater than zero");
        }
        endpoint.max_retries = endpoint.max_retries.min(MAX_RETRIES);
        endpoint.timeout = endpoint.timeout.min(MAX_TIMEOUT);
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
            url: endpoint.embeddings_url(),
            endpoint,
            agent,
            model,
            dimensions,
            batch_size: batch_size.min(MAX_BATCH_SIZE),
            dims: None,
            dims_probed: false,
            backoff_base: DEFAULT_BACKOFF_BASE,
        })
    }

    /// Sends one short probe request and returns and caches the vector length.
    pub fn probe_dimensions(&mut self) -> Result<usize> {
        let vectors = self.request(&[PROBE_TEXT])?;
        let len = vectors.first().map_or(0, Vec::len);
        if let Some(expected) = self.dimensions {
            check_configured_dims(len, expected)?;
        }
        self.dims = Some(len);
        self.dims_probed = true;
        Ok(len)
    }

    /// Embeds `texts` in input order, probing the vector length first if unknown.
    ///
    /// Every returned vector has the length reported by [`Self::dims`].
    pub fn embed(&mut self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let expected = match self.dims {
            Some(dims) => dims,
            None => self.probe_dimensions()?,
        };
        let mut out = Vec::with_capacity(texts.len());
        for chunk in texts.chunks(self.batch_size) {
            let vectors = self.request(chunk)?;
            let len = vectors.first().map_or(0, Vec::len);
            if len != expected {
                if let Some(configured) = self.dimensions {
                    check_configured_dims(len, configured)?;
                }
                if self.dims_probed {
                    bail!("server returned {len} dimensions but the probe established {expected}");
                }
                bail!(
                    "server returned {len} dimensions but the stored index expects {expected}; run `memex embed` to rebuild the vectors"
                );
            }
            out.extend(vectors);
        }
        Ok(out)
    }

    /// Vector length established by [`Self::probe_dimensions`] or
    /// [`Self::expect_dimensions`]; `None` before either succeeds.
    pub fn dims(&self) -> Option<usize> {
        self.dims
    }

    /// Sets the expected vector length without sending a request; later
    /// responses must have length `n`. A configured `dimensions` parameter is
    /// still sent with each [`Self::embed`] request.
    pub fn expect_dimensions(&mut self, n: usize) {
        self.dims = Some(n);
        self.dims_probed = false;
    }

    #[cfg(test)]
    pub(crate) fn set_backoff_base(&mut self, base: Duration) {
        self.backoff_base = base;
    }

    /// Sends one validated request for `inputs`, retrying transient failures.
    fn request(&self, inputs: &[&str]) -> Result<Vec<Vec<f32>>> {
        let body = EmbeddingRequest {
            model: &self.model,
            input: inputs,
            encoding_format: "float",
            dimensions: self.dimensions,
        };
        let attempts = self.endpoint.max_retries.saturating_add(1);
        let mut attempt: u32 = 0;
        loop {
            attempt = attempt.saturating_add(1);
            match self.attempt(&body, inputs.len()) {
                Ok(vectors) => return Ok(vectors),
                Err(AttemptError::Fatal(error)) => return Err(error),
                Err(AttemptError::Transient {
                    error,
                    kind,
                    retry_after,
                }) => {
                    if attempt >= attempts {
                        return Err(error.context(format!(
                            "embedding request to {} failed after {attempt} attempt(s)",
                            self.url
                        )));
                    }
                    let delay = retry_after
                        .map(|hint| hint.min(MAX_BACKOFF))
                        .unwrap_or_else(|| backoff_delay(self.backoff_base, attempt));
                    eprintln!(
                        "warning: embedding request attempt {attempt}/{attempts} failed ({kind}), retrying in {}ms",
                        delay.as_millis()
                    );
                    thread::sleep(delay);
                }
            }
        }
    }

    fn attempt(
        &self,
        body: &EmbeddingRequest<'_>,
        expected: usize,
    ) -> Result<Vec<Vec<f32>>, AttemptError> {
        let mut request = self.agent.post(&self.url);
        if let Some(key) = self.endpoint.bearer_key() {
            request = request.header("Authorization", format!("Bearer {key}"));
        }
        let response = request
            .send_json(body)
            .map_err(|e| self.transport_error(e))?;
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
            let error = match (status, self.dimensions) {
                (400 | 422, Some(dims)) => anyhow!(
                    "embedding_dimensions={dims} rejected by {}: {message} (HTTP {status})",
                    self.model
                ),
                _ => anyhow!("embedding server returned HTTP {status}: {message}"),
            };
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

        let raw = response_body
            .with_config()
            .limit(MAX_SUCCESS_BODY_BYTES)
            .read_to_vec()
            .map_err(|e| self.transport_error(e))?;
        let parsed: EmbeddingResponse = serde_json::from_slice(&raw).map_err(|e| {
            AttemptError::Fatal(anyhow!(
                "embedding server returned an invalid response body: {e}"
            ))
        })?;
        validate_response(parsed, expected).map_err(AttemptError::Fatal)
    }

    fn transport_error(&self, error: ureq::Error) -> AttemptError {
        let kind = match &error {
            ureq::Error::Timeout(_) => Some("timeout"),
            ureq::Error::Io(_) => Some("I/O error"),
            ureq::Error::ConnectionFailed => Some("connection failed"),
            ureq::Error::HostNotFound => Some("host not found"),
            _ => None,
        };
        let error = match error {
            ureq::Error::BodyExceedsLimit(_) => {
                anyhow!("embedding response exceeds the {MAX_SUCCESS_BODY_MIB} MiB limit")
            }
            ureq::Error::Http(_) => {
                anyhow!(
                    "embedding request could not be built; check that the API key is a valid header value"
                )
            }
            other => anyhow!(
                "embedding request to {} failed: {}",
                self.url,
                self.sanitize(&other.to_string())
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

    /// Removes control characters, then replaces the API key and its
    /// URL-encoded and base64 forms with `<redacted>`.
    ///
    /// Control characters go first so that ones inserted inside an echoed key
    /// cannot split it past the redaction.
    fn sanitize(&self, text: &str) -> String {
        let mut clean: String = text.chars().filter(|c| !c.is_control()).collect();
        let Some(key) = self.endpoint.bearer_key() else {
            return clean;
        };
        let url_encoded: String = url::form_urlencoded::byte_serialize(key.as_bytes()).collect();
        let mut forms = [
            key.to_owned(),
            url_encoded,
            STANDARD.encode(key),
            STANDARD_NO_PAD.encode(key),
            URL_SAFE.encode(key),
            URL_SAFE_NO_PAD.encode(key),
        ];
        // Longest first, so a padded form is replaced before its unpadded prefix.
        forms.sort_by_key(|form| std::cmp::Reverse(form.len()));
        for form in forms.iter().filter(|form| !form.is_empty()) {
            clean = clean.replace(form.as_str(), "<redacted>");
        }
        clean
    }
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

fn check_configured_dims(len: usize, configured: usize) -> Result<()> {
    if len != configured {
        bail!(
            "server returned {len} dimensions but embedding_dimensions={configured}; this server/model may not support the `dimensions` parameter; remove it or pick a supported size"
        );
    }
    Ok(())
}

/// Orders `response` by index and checks count, coverage, uniform length in
/// `1..=MAX_DIMENSIONS`, and finiteness.
///
/// Items without `index` take their position, which is allowed only when no
/// item carries one.
fn validate_response(response: EmbeddingResponse, expected: usize) -> Result<Vec<Vec<f32>>> {
    if response.data.len() != expected {
        bail!(
            "embedding server returned {} embeddings for {expected} inputs",
            response.data.len()
        );
    }
    let indexed = response
        .data
        .iter()
        .filter(|item| item.index.is_some())
        .count();
    if indexed != 0 && indexed != expected {
        bail!("embedding server returned an index on only some embeddings");
    }
    let mut slots: Vec<Option<Vec<f32>>> = vec![None; expected];
    let mut width: Option<usize> = None;
    for (position, item) in response.data.into_iter().enumerate() {
        let index = item.index.unwrap_or(position);
        let len = item.embedding.len();
        if len == 0 {
            bail!("embedding server returned an empty vector at index {index}");
        }
        if len > MAX_DIMENSIONS {
            bail!(
                "embedding server returned {len} dimensions, more than the maximum of {MAX_DIMENSIONS}"
            );
        }
        match width {
            Some(width) if width != len => {
                bail!("embedding server returned vectors of different lengths ({width} and {len})")
            }
            _ => width = Some(len),
        }
        if item.embedding.iter().any(|value| !value.is_finite()) {
            bail!("embedding server returned a non-finite value at index {index}");
        }
        let Some(slot) = slots.get_mut(index) else {
            bail!("embedding server returned index {index} for {expected} inputs");
        };
        if slot.is_some() {
            bail!("embedding server returned index {index} twice");
        }
        *slot = Some(item.embedding);
    }
    // Equal counts with no duplicates and no out-of-range index fill every slot.
    slots
        .into_iter()
        .enumerate()
        .map(|(index, slot)| slot.ok_or_else(|| anyhow!("embedding server omitted index {index}")))
        .collect()
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
    use serde_json::{Value, json};
    use std::thread::JoinHandle;
    use tiny_http::{Header, Response, Server};

    const KEY: &str = "sk-memex-SECRET-7f3a9c41";

    struct Captured {
        path: String,
        auth: Option<String>,
        body: Value,
    }

    struct Reply {
        status: u16,
        body: String,
        retry_after: Option<&'static str>,
    }

    fn reply(status: u16, body: Value) -> Reply {
        Reply {
            status,
            body: body.to_string(),
            retry_after: None,
        }
    }

    fn inputs(body: &Value) -> Vec<String> {
        body["input"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Vectors of `width` whose first component is the position in the request.
    fn embeddings(count: usize, width: usize) -> Value {
        let data: Vec<Value> = (0..count)
            .map(|i| {
                let mut v = vec![0.5_f32; width];
                if let Some(first) = v.first_mut() {
                    *first = i as f32;
                }
                json!({"object": "embedding", "index": i, "embedding": v})
            })
            .collect();
        json!({"object": "list", "data": data})
    }

    /// Serves requests until 500ms pass without one, returning what was received.
    fn serve<F>(mut handler: F) -> Result<(String, JoinHandle<Vec<Captured>>)>
    where
        F: FnMut(usize, &Value) -> Reply + Send + 'static,
    {
        let server = Server::http("127.0.0.1:0").map_err(|e| anyhow!("bind: {e}"))?;
        let addr = server
            .server_addr()
            .to_ip()
            .ok_or_else(|| anyhow!("no ip address"))?;
        let handle = thread::spawn(move || {
            let mut captured = Vec::new();
            while let Ok(Some(mut request)) = server.recv_timeout(Duration::from_millis(500)) {
                let mut raw = String::new();
                let _ = request.as_reader().read_to_string(&mut raw);
                let body: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
                let auth = request
                    .headers()
                    .iter()
                    .find(|h| h.field.equiv("Authorization"))
                    .map(|h| h.value.as_str().to_owned());
                let out = handler(captured.len(), &body);
                let mut response = Response::from_string(out.body).with_status_code(out.status);
                if let Ok(h) = Header::from_bytes("Content-Type", "application/json") {
                    response = response.with_header(h);
                }
                if let Some(value) = out.retry_after
                    && let Ok(h) = Header::from_bytes("Retry-After", value)
                {
                    response = response.with_header(h);
                }
                captured.push(Captured {
                    path: request.url().to_owned(),
                    auth,
                    body,
                });
                let _ = request.respond(response);
            }
            captured
        });
        Ok((format!("http://{addr}/v1"), handle))
    }

    fn join(handle: JoinHandle<Vec<Captured>>) -> Result<Vec<Captured>> {
        handle.join().map_err(|_| anyhow!("server thread panicked"))
    }

    fn endpoint(base: &str, key: Option<&str>, max_retries: u32) -> RemoteEndpoint {
        RemoteEndpoint {
            base_url: base.to_owned(),
            api_key: key.map(str::to_owned),
            timeout: Duration::from_secs(5),
            max_retries,
        }
    }

    fn client(
        base: &str,
        key: Option<&str>,
        dimensions: Option<usize>,
        batch_size: usize,
        max_retries: u32,
    ) -> Result<RemoteEmbedder> {
        let mut embedder = RemoteEmbedder::new(
            endpoint(base, key, max_retries),
            "test-model".to_owned(),
            dimensions,
            batch_size,
        )?;
        embedder.set_backoff_base(Duration::from_millis(1));
        Ok(embedder)
    }

    fn echo(_: usize, body: &Value) -> Reply {
        reply(200, embeddings(inputs(body).len(), 3))
    }

    fn expect_err<T>(result: Result<T>) -> Result<String> {
        match result {
            Ok(_) => bail!("expected an error"),
            Err(e) => Ok(format!("{e:#}")),
        }
    }

    #[test]
    fn reorders_by_index() -> Result<()> {
        let (base, server) = serve(|_, body| {
            let mut value = embeddings(inputs(body).len(), 3);
            if let Some(data) = value["data"].as_array_mut() {
                data.reverse();
            }
            reply(200, value)
        })?;
        let mut embedder = client(&base, None, None, 16, 0)?;
        let out = embedder.embed(&["a", "b", "c"])?;
        join(server)?;
        let firsts: Vec<f32> = out.iter().filter_map(|v| v.first().copied()).collect();
        assert_eq!(firsts, vec![0.0, 1.0, 2.0]);
        Ok(())
    }

    #[test]
    fn rejects_count_mismatch() -> Result<()> {
        let (base, server) = serve(|_, _| reply(200, embeddings(2, 3)))?;
        let mut embedder = client(&base, None, None, 16, 0)?;
        let err = expect_err(embedder.probe_dimensions())?;
        join(server)?;
        assert!(err.contains("returned 2 embeddings for 1 inputs"), "{err}");
        Ok(())
    }

    #[test]
    fn rejects_duplicate_index() -> Result<()> {
        let (base, server) = serve(|_, _| {
            reply(
                200,
                json!({"data": [
                    {"index": 0, "embedding": [1.0]},
                    {"index": 0, "embedding": [2.0]}
                ]}),
            )
        })?;
        let mut embedder = client(&base, None, Some(1), 16, 0)?;
        embedder.dims = Some(1);
        let err = expect_err(embedder.embed(&["a", "b"]))?;
        join(server)?;
        assert!(err.contains("index 0 twice"), "{err}");
        Ok(())
    }

    #[test]
    fn rejects_non_uniform_lengths() -> Result<()> {
        let (base, server) = serve(|_, body| {
            if inputs(body).len() == 1 {
                return reply(200, embeddings(1, 2));
            }
            reply(
                200,
                json!({"data": [
                    {"index": 0, "embedding": [1.0, 2.0]},
                    {"index": 1, "embedding": [1.0, 2.0, 3.0]}
                ]}),
            )
        })?;
        let mut embedder = client(&base, None, None, 16, 0)?;
        let err = expect_err(embedder.embed(&["a", "b"]))?;
        join(server)?;
        assert!(err.contains("different lengths"), "{err}");
        Ok(())
    }

    #[test]
    fn rejects_out_of_range_json_values() -> Result<()> {
        let (base, server) = serve(|_, _| Reply {
            status: 200,
            body: r#"{"data":[{"index":0,"embedding":[1.0,1e39]}]}"#.to_owned(),
            retry_after: None,
        })?;
        let mut embedder = client(&base, None, None, 16, 0)?;
        let err = expect_err(embedder.probe_dimensions())?;
        join(server)?;
        // Precise f32 parsing rejects overflow before vector validation runs.
        assert!(err.contains("invalid response body"), "{err}");
        assert!(err.contains("number out of range"), "{err}");
        Ok(())
    }

    #[test]
    fn rejects_non_finite_values() -> Result<()> {
        for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let response = EmbeddingResponse {
                data: vec![EmbeddingItem {
                    embedding: vec![1.0, value],
                    index: Some(0),
                }],
            };
            let err = expect_err(validate_response(response, 1))?;
            assert!(err.contains("non-finite value at index 0"), "{err}");
        }
        Ok(())
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
            _ => echo(n, body),
        })?;
        let mut embedder = client(&base, None, None, 16, 3)?;
        assert_eq!(embedder.probe_dimensions()?, 3);
        assert_eq!(join(server)?.len(), 3);
        Ok(())
    }

    #[test]
    fn does_not_retry_400() -> Result<()> {
        let (base, server) = serve(|_, _| reply(400, json!({"error": {"message": "bad input"}})))?;
        let mut embedder = client(&base, None, None, 16, 3)?;
        let err = expect_err(embedder.probe_dimensions())?;
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
        let mut embedder = client(&base, None, None, 16, 2)?;
        let err = expect_err(embedder.probe_dimensions())?;
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
            let (base, server) = serve(echo)?;
            client(&base, key, None, 16, 0)?.probe_dimensions()?;
            let captured = join(server)?;
            assert_eq!(captured.len(), 1);
            assert_eq!(captured.first().and_then(|c| c.auth.clone()), expected);
        }
        Ok(())
    }

    #[test]
    fn sends_dimensions_only_when_configured() -> Result<()> {
        for (dimensions, expected) in [(Some(3), Some(json!(3))), (None, None)] {
            let (base, server) = serve(echo)?;
            client(&base, None, dimensions, 16, 0)?.probe_dimensions()?;
            let captured = join(server)?;
            let body = captured.first().map(|c| c.body.clone()).unwrap_or_default();
            assert_eq!(body.get("dimensions").cloned(), expected);
            assert_eq!(body["model"], json!("test-model"));
            assert_eq!(body["encoding_format"], json!("float"));
            assert_eq!(body["input"], json!([PROBE_TEXT]));
        }
        Ok(())
    }

    #[test]
    fn reports_rejected_dimensions() -> Result<()> {
        let (base, server) = serve(|_, _| {
            reply(
                400,
                json!({"error": {"message": "This model does not support specifying dimensions."}}),
            )
        })?;
        let mut embedder = client(&base, None, Some(64), 16, 3)?;
        let err = expect_err(embedder.probe_dimensions())?;
        assert_eq!(join(server)?.len(), 1);
        assert!(
            err.starts_with(
                "embedding_dimensions=64 rejected by test-model: This model does not support specifying dimensions."
            ),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn reports_ignored_dimensions() -> Result<()> {
        let (base, server) = serve(echo)?;
        let mut embedder = client(&base, None, Some(64), 16, 0)?;
        let err = expect_err(embedder.embed(&["a"]))?;
        join(server)?;
        assert!(
            err.contains("server returned 3 dimensions but embedding_dimensions=64"),
            "{err}"
        );
        assert!(
            err.contains("may not support the `dimensions` parameter"),
            "{err}"
        );
        assert_eq!(embedder.dims(), None);
        Ok(())
    }

    #[test]
    fn probe_sets_length_and_later_mismatch_fails() -> Result<()> {
        let (base, server) = serve(|_, body| {
            let count = inputs(body).len();
            reply(200, embeddings(count, if count == 1 { 3 } else { 4 }))
        })?;
        let mut embedder = client(&base, None, None, 16, 0)?;
        assert_eq!(embedder.probe_dimensions()?, 3);
        assert_eq!(embedder.dims(), Some(3));
        let err = expect_err(embedder.embed(&["a", "b"]))?;
        join(server)?;
        assert!(
            err.contains("server returned 4 dimensions but the probe established 3"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn api_key_never_appears_in_errors() -> Result<()> {
        let (base, server) = serve(|_, _| {
            reply(
                401,
                json!({"error": {"message": format!("Incorrect API key provided: {KEY}")}}),
            )
        })?;
        let mut embedder = client(&base, Some(KEY), None, 16, 0)?;
        let err = expect_err(embedder.probe_dimensions())?;
        join(server)?;
        assert!(err.contains("HTTP 401"), "{err}");
        assert!(!err.contains(KEY), "{err}");

        let unreachable = client("http://127.0.0.1:1/v1", Some(KEY), None, 16, 0)?;
        let err = expect_err(unreachable.request(&["a"]))?;
        assert!(!err.contains(KEY), "{err}");
        assert!(!format!("{unreachable:?}").contains(KEY));
        Ok(())
    }

    #[test]
    fn splits_batches_and_preserves_order() -> Result<()> {
        let (base, server) = serve(|_, body| {
            let data: Vec<Value> = inputs(body)
                .iter()
                .enumerate()
                .map(|(i, text)| {
                    let k: f32 = text.trim_start_matches('t').parse().unwrap_or(-1.0);
                    json!({"index": i, "embedding": [k, 0.0]})
                })
                .collect();
            reply(200, json!({"data": data}))
        })?;
        let mut embedder = client(&base, None, None, 2, 0)?;
        embedder.probe_dimensions()?;
        let texts = ["t0", "t1", "t2", "t3", "t4"];
        let out = embedder.embed(&texts)?;
        let captured = join(server)?;
        let sizes: Vec<usize> = captured.iter().map(|c| inputs(&c.body).len()).collect();
        assert_eq!(sizes, vec![1, 2, 2, 1]);
        let firsts: Vec<f32> = out.iter().filter_map(|v| v.first().copied()).collect();
        assert_eq!(firsts, vec![0.0, 1.0, 2.0, 3.0, 4.0]);
        Ok(())
    }

    #[test]
    fn trims_trailing_slash() -> Result<()> {
        let (base, server) = serve(echo)?;
        client(&format!("{base}/"), None, None, 16, 0)?.probe_dimensions()?;
        let captured = join(server)?;
        assert_eq!(
            captured.first().map(|c| c.path.as_str()),
            Some("/v1/embeddings")
        );
        Ok(())
    }

    #[test]
    fn rejects_invalid_endpoints_and_settings() -> Result<()> {
        for base in [
            "ftp://example.com/v1",
            "file:///tmp/x",
            "http://",
            "not a url",
            "http://user:pw@example.com/v1",
            "http://example.com/v1?x=1",
        ] {
            assert!(endpoint(base, None, 0).validate().is_err(), "{base}");
            assert!(client(base, None, None, 16, 0).is_err(), "{base}");
        }
        endpoint("https://api.openai.com/v1", None, 0).validate()?;
        assert!(client("http://localhost:11434/v1", None, None, 0, 0).is_err());
        assert!(client("http://localhost:11434/v1", None, Some(0), 16, 0).is_err());
        let clamped = client("http://localhost:11434/v1", None, None, 100_000, 0)?;
        assert_eq!(clamped.batch_size, MAX_BATCH_SIZE);
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
    fn uses_position_when_every_index_is_missing() -> Result<()> {
        let (base, server) = serve(|_, _| {
            reply(
                200,
                json!({"data": [{"embedding": [0.0, 1.0]}, {"embedding": [1.0, 1.0]}]}),
            )
        })?;
        let mut embedder = client(&base, None, None, 16, 0)?;
        embedder.expect_dimensions(2);
        let out = embedder.embed(&["a", "b"])?;
        join(server)?;
        let firsts: Vec<f32> = out.iter().filter_map(|v| v.first().copied()).collect();
        assert_eq!(firsts, vec![0.0, 1.0]);
        Ok(())
    }

    #[test]
    fn rejects_partially_missing_index() -> Result<()> {
        let (base, server) = serve(|_, _| {
            reply(
                200,
                json!({"data": [{"index": 1, "embedding": [0.0]}, {"embedding": [1.0]}]}),
            )
        })?;
        let mut embedder = client(&base, None, None, 16, 0)?;
        embedder.expect_dimensions(1);
        let err = expect_err(embedder.embed(&["a", "b"]))?;
        join(server)?;
        assert!(err.contains("index on only some embeddings"), "{err}");
        Ok(())
    }

    #[test]
    fn expect_dimensions_skips_probe_and_checks_length() -> Result<()> {
        let (base, server) = serve(echo)?;
        let mut embedder = client(&base, None, None, 16, 0)?;
        embedder.expect_dimensions(3);
        assert_eq!(embedder.dims(), Some(3));
        embedder.embed(&["a", "b"])?;
        embedder.expect_dimensions(5);
        let err = expect_err(embedder.embed(&["a"]))?;
        let captured = join(server)?;
        assert_eq!(captured.len(), 2);
        assert!(captured.iter().all(|c| inputs(&c.body) != [PROBE_TEXT]));
        assert!(
            err.contains("server returned 3 dimensions but the stored index expects 5"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn expected_length_overrides_matching_configured_dimensions() -> Result<()> {
        let (base, server) = serve(echo)?;
        let mut embedder = client(&base, None, Some(3), 16, 0)?;
        embedder.expect_dimensions(5);
        let err = expect_err(embedder.embed(&["a"]))?;
        join(server)?;
        assert!(
            err.contains("server returned 3 dimensions but the stored index expects 5"),
            "{err}"
        );
        assert!(err.contains("run `memex embed`"), "{err}");
        Ok(())
    }

    #[test]
    fn rejects_oversized_dimensions() -> Result<()> {
        let base = "http://localhost:11434/v1";
        let err = expect_err(client(base, None, Some(MAX_DIMENSIONS + 1), 16, 0))?;
        assert!(err.contains("exceeds the maximum"), "{err}");
        client(base, None, Some(MAX_DIMENSIONS), 16, 0)?;

        let (base, server) =
            serve(|_, body| reply(200, embeddings(inputs(body).len(), MAX_DIMENSIONS + 1)))?;
        let mut embedder = client(&base, None, None, 16, 0)?;
        let probe_err = expect_err(embedder.probe_dimensions())?;
        embedder.expect_dimensions(MAX_DIMENSIONS + 1);
        let embed_err = expect_err(embedder.embed(&["a", "b"]))?;
        let captured = join(server)?;
        assert_eq!(captured.len(), 2);
        assert!(probe_err.contains("more than the maximum"), "{probe_err}");
        assert!(embed_err.contains("more than the maximum"), "{embed_err}");
        Ok(())
    }

    #[test]
    fn clamps_retries_and_timeout() -> Result<()> {
        let mut settings = endpoint("http://localhost:11434/v1", None, 1000);
        settings.timeout = Duration::from_secs(3600);
        let embedder = RemoteEmbedder::new(settings, "m".to_owned(), None, 16)?;
        assert_eq!(embedder.endpoint.max_retries, MAX_RETRIES);
        assert_eq!(embedder.endpoint.timeout, MAX_TIMEOUT);
        Ok(())
    }

    #[test]
    fn strips_control_characters_and_encoded_keys() -> Result<()> {
        const ODD_KEY: &str = "sk/odd+key=value";
        let url_encoded: String =
            url::form_urlencoded::byte_serialize(ODD_KEY.as_bytes()).collect();
        let message = format!(
            "\u{1b}[2J\u{1b}]0;title\u{7}bad key sk/odd\u{1b}+key=value url {url_encoded} b64 {} b64url {}",
            STANDARD.encode(ODD_KEY),
            URL_SAFE_NO_PAD.encode(ODD_KEY)
        );
        let (base, server) = serve(move |_, _| reply(401, json!({"error": {"message": message}})))?;
        let mut embedder = client(&base, Some(ODD_KEY), None, 16, 0)?;
        let err = expect_err(embedder.probe_dimensions())?;
        join(server)?;
        assert!(!err.chars().any(char::is_control), "{err:?}");
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
    fn transient_kind_excludes_server_text() -> Result<()> {
        let (base, server) =
            serve(|_, _| reply(503, json!({"error": {"message": "secret detail"}})))?;
        let embedder = client(&base, None, None, 16, 0)?;
        let body = EmbeddingRequest {
            model: "test-model",
            input: &["a"],
            encoding_format: "float",
            dimensions: None,
        };
        let outcome = embedder.attempt(&body, 1);
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
