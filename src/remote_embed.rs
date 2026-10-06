//! Embedding client for OpenAI-compatible `/embeddings` endpoints.

use std::fmt;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};

pub use crate::remote_http::RemoteEndpoint;
use crate::remote_http::{ClientLimits, HttpClient, Purpose};

/// Largest number of inputs sent in one request; larger batch sizes are clamped.
pub const MAX_BATCH_SIZE: usize = 2048;
/// Largest accepted vector length, for configured and returned dimensions.
pub const MAX_DIMENSIONS: usize = 65536;
const LIMITS: ClientLimits = ClientLimits {
    max_retries: 10,
    max_timeout: Duration::from_secs(600),
    max_success_body_mib: 128,
    max_total: None,
};
const PROBE_TEXT: &str = "dimension probe";

/// Blocking client that embeds texts through a remote endpoint.
///
/// Requests go to `{base_url}/embeddings` with trailing `/` trimmed.
pub struct RemoteEmbedder {
    http: HttpClient,
    url: String,
    model: String,
    dimensions: Option<usize>,
    batch_size: usize,
    dims: Option<usize>,
    /// Whether `dims` came from a probe rather than [`Self::expect_dimensions`].
    dims_probed: bool,
}

impl fmt::Debug for RemoteEmbedder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteEmbedder")
            .field("endpoint", self.http.endpoint())
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

impl RemoteEmbedder {
    /// Creates a client; `dimensions` is sent as the OpenAI `dimensions`
    /// parameter and must be in `1..=MAX_DIMENSIONS`, and `batch_size` must be
    /// non-zero and is clamped to [`MAX_BATCH_SIZE`].
    ///
    /// The endpoint's `max_retries` is clamped to 10 and its `timeout` to 600 s.
    pub fn new(
        endpoint: RemoteEndpoint,
        model: String,
        dimensions: Option<usize>,
        batch_size: usize,
    ) -> Result<Self> {
        let http = HttpClient::new(endpoint, Purpose::Embedding, LIMITS)?;
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
        Ok(Self {
            url: format!(
                "{}/embeddings",
                http.endpoint().base_url.trim_end_matches('/')
            ),
            http,
            model,
            dimensions,
            batch_size: batch_size.min(MAX_BATCH_SIZE),
            dims: None,
            dims_probed: false,
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
        self.http.set_backoff_base(base);
    }

    /// Sends one validated request for `inputs`, retrying transient failures.
    fn request(&self, inputs: &[&str]) -> Result<Vec<Vec<f32>>> {
        let body = EmbeddingRequest {
            model: &self.model,
            input: inputs,
            encoding_format: "float",
            dimensions: self.dimensions,
        };
        self.http.post_json_with(
            &self.url,
            &body,
            |status, message| match (status, self.dimensions) {
                (400 | 422, Some(dims)) => Some(anyhow!(
                    "embedding_dimensions={dims} rejected by {}: {message} (HTTP {status})",
                    self.model
                )),
                _ => None,
            },
            |raw| {
                let parsed: EmbeddingResponse = serde_json::from_slice(raw).map_err(|e| {
                    anyhow!("embedding server returned an invalid response body: {e}")
                })?;
                validate_response(parsed, inputs.len())
            },
        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::remote_server::{
        KEY, Reply, endpoint, expect_err, join, reply, serve,
    };
    use serde_json::{Value, json};

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
    fn rejects_non_finite_values() -> Result<()> {
        let (base, server) = serve(|_, _| Reply {
            status: 200,
            body: r#"{"data":[{"index":0,"embedding":[1.0,1e39]}]}"#.to_owned(),
            retry_after: None,
        })?;
        let mut embedder = client(&base, None, None, 16, 0)?;
        let err = expect_err(embedder.probe_dimensions())?;
        join(server)?;
        assert!(err.contains("non-finite"), "{err}");
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
        assert_eq!(embedder.http.endpoint().max_retries, LIMITS.max_retries);
        assert_eq!(embedder.http.endpoint().timeout, LIMITS.max_timeout);
        Ok(())
    }
}
