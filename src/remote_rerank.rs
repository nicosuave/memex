//! Reranking client for Cohere-style `/rerank` endpoints.

use std::fmt;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::remote_http::{ClientLimits, HttpClient, Purpose, RemoteEndpoint};
use crate::rerank::RerankBackend;

/// Longest accepted provider model name, in characters.
pub const MAX_MODEL_NAME_CHARS: usize = 200;
const LIMITS: ClientLimits = ClientLimits {
    max_retries: 3,
    max_timeout: Duration::from_secs(30),
    max_success_body_mib: 1,
    max_total: Some(Duration::from_secs(30)),
};

/// Request body layout of a rerank server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RerankDialect {
    /// `{"model", "query", "documents"}`: Cohere, Jina, Voyage, vLLM, llama.cpp, and
    /// OpenRouter.
    #[default]
    Documents,
    /// `{"query", "texts", "truncate": true}`: Hugging Face text-embeddings-inference,
    /// which serves one model and truncates inputs to its window.
    Texts,
}

impl RerankDialect {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Documents => "documents",
            Self::Texts => "texts",
        }
    }
}

/// Resolved settings of a remote reranker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteRerankConfig {
    /// `base_url` is the full rerank endpoint URL, used verbatim.
    pub endpoint: RemoteEndpoint,
    /// The provider's model name; required by [`RerankDialect::Documents`] and not sent
    /// in [`RerankDialect::Texts`].
    pub model: Option<String>,
    pub dialect: RerankDialect,
    /// Fields added to every request body, such as provider routing options.
    pub extra_body: Option<RerankExtraBody>,
}

/// Longest accepted `rerank_extra_body` source, in bytes.
pub const MAX_EXTRA_BODY_BYTES: usize = 1024;
/// Deepest accepted nesting of objects and arrays in `rerank_extra_body`; the top-level
/// object is depth 1.
pub const MAX_EXTRA_BODY_DEPTH: usize = 4;
/// Top-level request fields memex sets in some dialect, which an extra body may not set.
pub const RESERVED_BODY_KEYS: &[&str] =
    &["model", "query", "documents", "texts", "top_n", "truncate"];

/// Validated top-level fields merged into every rerank request body.
///
/// `Debug` output never contains the fields, and errors from [`RerankExtraBody::parse`]
/// never quote the source.
#[derive(Clone, PartialEq, Eq)]
pub struct RerankExtraBody(Map<String, Value>);

impl fmt::Debug for RerankExtraBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl RerankExtraBody {
    /// Parses a JSON object of extra request fields; `None` when `source` is blank or `{}`.
    ///
    /// Rejects a source longer than [`MAX_EXTRA_BODY_BYTES`], invalid JSON, a value that
    /// is not an object, a top-level key given twice or listed in [`RESERVED_BODY_KEYS`],
    /// and nesting deeper than [`MAX_EXTRA_BODY_DEPTH`].
    pub fn parse(source: &str) -> Result<Option<Self>> {
        if source.len() > MAX_EXTRA_BODY_BYTES {
            bail!("must be at most {MAX_EXTRA_BODY_BYTES} bytes");
        }
        if source.trim().is_empty() {
            return Ok(None);
        }
        // serde_json messages can quote the input, so only the position is kept.
        let value: Value = serde_json::from_str(source).map_err(|error| {
            anyhow!(
                "is not valid JSON (line {}, column {})",
                error.line(),
                error.column()
            )
        })?;
        let Value::Object(fields) = value else {
            bail!("must be a JSON object");
        };
        // A parsed Value keeps the last of repeated keys, so count them in the source.
        let TopLevelKeys(keys) =
            serde_json::from_str(source).map_err(|_| anyhow!("must be a JSON object"))?;
        if keys.len() != fields.len() {
            bail!("must not repeat a top-level key");
        }
        if let Some(reserved) = RESERVED_BODY_KEYS
            .iter()
            .find(|reserved| fields.contains_key(**reserved))
        {
            bail!("must not set `{reserved}`, which memex sets");
        }
        if fields
            .values()
            .any(|value| exceeds_depth(value, MAX_EXTRA_BODY_DEPTH - 1))
        {
            bail!("must not nest objects and arrays more than {MAX_EXTRA_BODY_DEPTH} deep");
        }
        Ok((!fields.is_empty()).then_some(Self(fields)))
    }
}

/// Whether `value` nests objects and arrays more than `remaining` levels deep.
fn exceeds_depth(value: &Value, remaining: usize) -> bool {
    let mut children: Box<dyn Iterator<Item = &Value>> = match value {
        Value::Array(items) => Box::new(items.iter()),
        Value::Object(fields) => Box::new(fields.values()),
        _ => return false,
    };
    match remaining.checked_sub(1) {
        None => true,
        Some(remaining) => children.any(|child| exceeds_depth(child, remaining)),
    }
}

/// Every top-level key of a JSON object in source order, repeats included.
struct TopLevelKeys(Vec<String>);

impl<'de> Deserialize<'de> for TopLevelKeys {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct KeysVisitor;

        impl<'de> serde::de::Visitor<'de> for KeysVisitor {
            type Value = TopLevelKeys;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<TopLevelKeys, A::Error> {
                let mut keys = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    map.next_value::<serde::de::IgnoredAny>()?;
                    keys.push(key);
                }
                Ok(TopLevelKeys(keys))
            }
        }

        deserializer.deserialize_map(KeysVisitor)
    }
}

/// Check a provider model name: non-empty, at most [`MAX_MODEL_NAME_CHARS`] characters,
/// and free of whitespace and control characters.
pub fn validate_model_name(model: &str) -> Result<()> {
    if model.is_empty() {
        bail!("remote rerank model name must not be empty");
    }
    if model.chars().count() > MAX_MODEL_NAME_CHARS {
        bail!("remote rerank model name is longer than {MAX_MODEL_NAME_CHARS} characters");
    }
    if model.chars().any(|c| c.is_control() || c.is_whitespace()) {
        bail!(
            "remote rerank model name {model:?} must not contain whitespace or control characters"
        );
    }
    Ok(())
}

/// Blocking client that scores documents through a remote rerank endpoint.
pub struct RemoteReranker {
    http: HttpClient,
    model: Option<String>,
    dialect: RerankDialect,
    extra_body: Option<RerankExtraBody>,
}

impl fmt::Debug for RemoteReranker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteReranker")
            .field("endpoint", self.http.endpoint())
            .field("model", &self.model)
            .field("dialect", &self.dialect)
            .field("extra_body", &self.extra_body)
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum RerankRequest<'a> {
    Documents {
        model: &'a str,
        query: &'a str,
        documents: &'a [String],
    },
    Texts {
        query: &'a str,
        texts: &'a [String],
        truncate: bool,
    },
}

impl RemoteReranker {
    /// Creates a client for `config`.
    ///
    /// The endpoint's `max_retries` is clamped to 3 and its `timeout` to 30 s, and a
    /// request with its retries gets at most 30 s; responses larger than 1 MiB are
    /// rejected. Fails without a model in the [`RerankDialect::Documents`] dialect.
    pub fn new(config: &RemoteRerankConfig) -> Result<Self> {
        match (&config.model, config.dialect) {
            (Some(model), _) => validate_model_name(model)?,
            (None, RerankDialect::Documents) => {
                bail!("the documents rerank dialect requires a model name")
            }
            (None, RerankDialect::Texts) => {}
        }
        Ok(Self {
            http: HttpClient::new(config.endpoint.clone(), Purpose::Rerank, LIMITS)?,
            model: config.model.clone(),
            dialect: config.dialect,
            extra_body: config.extra_body.clone(),
        })
    }

    /// The JSON request body: the dialect's fields, then each extra field whose key the
    /// dialect does not set.
    fn request_body(&self, query: &str, documents: &[String]) -> Result<Value> {
        let request = match (self.dialect, self.model.as_deref()) {
            (RerankDialect::Documents, Some(model)) => RerankRequest::Documents {
                model,
                query,
                documents,
            },
            (RerankDialect::Documents, None) => {
                bail!("the documents rerank dialect requires a model name")
            }
            (RerankDialect::Texts, _) => RerankRequest::Texts {
                query,
                texts: documents,
                truncate: true,
            },
        };
        let mut body = serde_json::to_value(request)?;
        if let (Value::Object(fields), Some(RerankExtraBody(extra))) = (&mut body, &self.extra_body)
        {
            for (key, value) in extra {
                fields.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
        Ok(body)
    }

    #[cfg(test)]
    pub(crate) fn set_backoff_base(&mut self, base: Duration) {
        self.http.set_backoff_base(base);
    }
}

impl RerankBackend for RemoteReranker {
    /// Relevance probabilities in `[0, 1]`, one per document in input order.
    ///
    /// Fails when the request fails or the response does not score every document
    /// exactly once with a finite number.
    fn rerank(&mut self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        if documents.is_empty() {
            return Ok(Vec::new());
        }
        let body = self.request_body(query, documents)?;
        let url = &self.http.endpoint().base_url;
        let has_extra_body = self.extra_body.is_some();
        self.http.post_json_with(
            url,
            &body,
            |status, message| match status {
                401 | 403 => Some(anyhow!(
                    "rerank server rejected the request (HTTP {status}): {message}; check \
                     rerank_api_key or rerank_api_key_env"
                )),
                // Providers such as OpenRouter also answer 404 when no endpoint satisfies
                // the routing policy of the extra body.
                404 if has_extra_body => Some(anyhow!(
                    "rerank server returned HTTP 404: {message}; the URL may be wrong, or no \
                     provider matches the request's provider policy in rerank_extra_body"
                )),
                404 => Some(anyhow!(
                    "rerank server returned HTTP 404: {message}; rerank_url must be the full \
                     endpoint URL, such as https://api.cohere.com/v2/rerank"
                )),
                _ => None,
            },
            |raw| parse_scores(raw, documents.len()),
        )
    }
}

/// Scores in input order from a rerank response for `expected` documents.
///
/// The items are the top-level array, or the array under `results` or `data`. Each item
/// needs an integer `index` and a numeric `relevance_score` or `score`, and every index
/// below `expected` must appear exactly once.
fn parse_scores(raw: &[u8], expected: usize) -> Result<Vec<f32>> {
    let response: Value = serde_json::from_slice(raw)
        .map_err(|e| anyhow!("rerank server returned an invalid response body: {e}"))?;
    let items = match &response {
        Value::Array(items) => items,
        Value::Object(object) => match (object.get("results"), object.get("data")) {
            (Some(Value::Array(items)), _) | (None, Some(Value::Array(items))) => items,
            _ => bail!("rerank response has no `results` or `data` array"),
        },
        _ => bail!("rerank response is neither an object nor an array"),
    };
    if items.len() != expected {
        bail!(
            "rerank server returned {} results for {expected} documents",
            items.len()
        );
    }
    let mut slots: Vec<Option<f32>> = vec![None; expected];
    for item in items {
        let index = item
            .get("index")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("rerank result has no integer `index`"))?;
        let score = item
            .get("relevance_score")
            .or_else(|| item.get("score"))
            .ok_or_else(|| anyhow!("rerank result {index} has no `relevance_score` or `score`"))?
            .as_f64()
            .ok_or_else(|| anyhow!("rerank result {index} has a non-numeric score"))?;
        // A finite f64 beyond the f32 range becomes infinite here.
        let score = score as f32;
        if !score.is_finite() {
            bail!("rerank result {index} has a non-finite score");
        }
        let Some(slot) = usize::try_from(index)
            .ok()
            .and_then(|index| slots.get_mut(index))
        else {
            bail!("rerank server returned index {index} for {expected} documents");
        };
        if slot.replace(score).is_some() {
            bail!("rerank server returned index {index} twice");
        }
    }
    // Equal counts with no duplicates and no out-of-range index fill every slot.
    let scores = slots
        .into_iter()
        .enumerate()
        .map(|(index, slot)| slot.ok_or_else(|| anyhow!("rerank server omitted index {index}")))
        .collect::<Result<Vec<f32>>>()?;
    // Search ranking expects probabilities, as the local reranker returns. Hosted APIs
    // return relevance probabilities in [0, 1], while some self-hosted servers return raw
    // logits; a score outside [0, 1] marks the response as logits, and the sigmoid maps
    // every score of it. The sigmoid is monotonic, so the order is the same either way.
    if scores.iter().all(|score| (0.0..=1.0).contains(score)) {
        Ok(scores)
    } else {
        scores.into_iter().map(crate::rerank::sigmoid).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::remote_server::{
        KEY, Reply, endpoint, expect_err, join, reply, serve,
    };
    use serde_json::json;

    fn config(url: &str, key: Option<&str>, dialect: RerankDialect) -> RemoteRerankConfig {
        RemoteRerankConfig {
            endpoint: endpoint(url, key, 0),
            model: Some("rerank-test-1".to_owned()),
            dialect,
            extra_body: None,
        }
    }

    fn client(url: &str, key: Option<&str>, retries: u32) -> Result<RemoteReranker> {
        let mut config = config(url, key, RerankDialect::Documents);
        config.endpoint.max_retries = retries;
        let mut reranker = RemoteReranker::new(&config)?;
        reranker.set_backoff_base(Duration::from_millis(1));
        Ok(reranker)
    }

    fn documents(count: usize) -> Vec<String> {
        (0..count).map(|i| format!("doc {i}")).collect()
    }

    /// Scores each document `0.1 * (index + 1)`, listed in reverse order.
    fn scored(_: usize, body: &Value) -> Reply {
        let count = body["documents"]
            .as_array()
            .or_else(|| body["texts"].as_array())
            .map_or(0, Vec::len);
        let results: Vec<Value> = (0..count)
            .rev()
            .map(|i| json!({"index": i, "relevance_score": 0.1 * (i as f64 + 1.0)}))
            .collect();
        reply(200, json!({"results": results}))
    }

    fn assert_close(actual: &[f32], expected: &[f32]) {
        assert_eq!(actual.len(), expected.len(), "{actual:?}");
        for (a, e) in actual.iter().zip(expected) {
            assert!((a - e).abs() < 1e-6, "{actual:?} != {expected:?}");
        }
    }

    #[test]
    fn documents_dialect_sends_model_and_bearer_key() -> Result<()> {
        let (base, server) = serve(scored)?;
        let url = format!("{base}/rerank");
        let scores = client(&url, Some(KEY), 0)?.rerank("needle", &documents(3))?;
        let captured = join(server)?;
        assert_close(&scores, &[0.1, 0.2, 0.3]);
        assert_eq!(captured.len(), 1);
        let request = captured.first().ok_or_else(|| anyhow!("no request"))?;
        assert_eq!(request.path, "/v1/rerank");
        assert_eq!(request.auth, Some(format!("Bearer {KEY}")));
        assert_eq!(
            request.body,
            json!({
                "model": "rerank-test-1",
                "query": "needle",
                "documents": ["doc 0", "doc 1", "doc 2"]
            })
        );
        Ok(())
    }

    #[test]
    fn texts_dialect_omits_model_and_keyless_authorization() -> Result<()> {
        let (base, server) = serve(|_, body| {
            let count = body["texts"].as_array().map_or(0, Vec::len);
            let items: Vec<Value> = (0..count)
                .map(|i| json!({"index": i, "score": 0.5}))
                .collect();
            reply(200, Value::Array(items))
        })?;
        let url = format!("{base}/rerank");
        let mut unnamed = config(&url, None, RerankDialect::Texts);
        unnamed.model = None;
        for config in [config(&url, None, RerankDialect::Texts), unnamed] {
            let scores = RemoteReranker::new(&config)?.rerank("needle", &documents(2))?;
            assert_close(&scores, &[0.5, 0.5]);
        }
        let captured = join(server)?;
        assert_eq!(captured.len(), 2);
        for request in &captured {
            assert_eq!(request.auth, None);
            assert_eq!(
                request.body,
                json!({"query": "needle", "texts": ["doc 0", "doc 1"], "truncate": true})
            );
        }
        Ok(())
    }

    #[test]
    fn extra_body_reaches_both_dialects_without_replacing_memex_fields() -> Result<()> {
        let provider = json!({"data_collection": "deny", "zdr": true, "allow_fallbacks": false});
        let extra = RerankExtraBody::parse(&json!({"provider": provider}).to_string())?;
        let (base, server) = serve(scored)?;
        let url = format!("{base}/rerank");
        for dialect in [RerankDialect::Documents, RerankDialect::Texts] {
            let config = RemoteRerankConfig {
                extra_body: extra.clone(),
                ..config(&url, None, dialect)
            };
            let scores = RemoteReranker::new(&config)?.rerank("needle", &documents(2))?;
            assert_close(&scores, &[0.1, 0.2]);
        }
        let captured = join(server)?;
        let bodies: Vec<&Value> = captured.iter().map(|request| &request.body).collect();
        assert_eq!(
            bodies,
            [
                &json!({"model": "rerank-test-1", "query": "needle",
                        "documents": ["doc 0", "doc 1"], "provider": provider}),
                &json!({"query": "needle", "texts": ["doc 0", "doc 1"], "truncate": true,
                        "provider": provider}),
            ]
        );

        // Built directly, since parse rejects these keys: every field memex sets wins.
        let colliding: Map<String, Value> = RESERVED_BODY_KEYS
            .iter()
            .map(|key| ((*key).to_owned(), json!("from-extra-body")))
            .collect();
        for dialect in [RerankDialect::Documents, RerankDialect::Texts] {
            let plain = RemoteReranker::new(&config(&url, None, dialect))?;
            let expected = plain.request_body("needle", &documents(2))?;
            let merged = RemoteReranker::new(&RemoteRerankConfig {
                extra_body: Some(RerankExtraBody(colliding.clone())),
                ..config(&url, None, dialect)
            })?
            .request_body("needle", &documents(2))?;
            let expected = expected
                .as_object()
                .ok_or_else(|| anyhow!("not an object"))?;
            for (key, value) in expected {
                assert_eq!(merged.get(key), Some(value), "{dialect:?} {key}");
            }
        }
        Ok(())
    }

    #[test]
    fn extra_body_rejects_invalid_input_without_quoting_it() {
        let marker = "sk-extra-SECRET";
        let deep = format!(r#"{{"a":{{"b":[{{"c":["{marker}"]}}]}}}}"#);
        let long = format!(r#"{{"provider":"{}"}}"#, "x".repeat(MAX_EXTRA_BODY_BYTES));
        for (source, expected) in [
            (
                format!(r#"{{"provider": {marker}}}"#),
                "is not valid JSON (line 1",
            ),
            (format!(r#"{{"provider": "{marker}""#), "is not valid JSON"),
            (format!(r#"["{marker}"]"#), "must be a JSON object"),
            (format!(r#""{marker}""#), "must be a JSON object"),
            ("null".to_owned(), "must be a JSON object"),
            ("42".to_owned(), "must be a JSON object"),
            (
                format!(r#"{{"model": "{marker}"}}"#),
                "must not set `model`",
            ),
            (
                format!(r#"{{"provider": "{marker}", "provider": {{}}}}"#),
                "must not repeat a top-level key",
            ),
            (
                format!(r#"{{"model": "{marker}"}}"#),
                "must not set `model`",
            ),
            (
                format!(r#"{{"top_n": 3, "x": "{marker}"}}"#),
                "must not set `top_n`",
            ),
            (
                format!(r#"{{"truncate": false, "x": "{marker}"}}"#),
                "must not set `truncate`",
            ),
            (deep, "more than 4 deep"),
            (long, "at most 1024 bytes"),
        ] {
            let error = match RerankExtraBody::parse(&source) {
                Ok(parsed) => panic!("{source} parsed as {parsed:?}"),
                Err(error) => format!("{error:#}"),
            };
            assert!(error.contains(expected), "{source}: {error}");
            assert!(!error.contains(marker), "{error}");
        }
        for key in RESERVED_BODY_KEYS {
            assert!(RerankExtraBody::parse(&format!(r#"{{"{key}": 1}}"#)).is_err());
        }
    }

    #[test]
    fn extra_body_accepts_four_levels_and_hides_its_fields() -> Result<()> {
        assert_eq!(RerankExtraBody::parse("")?, None);
        assert_eq!(RerankExtraBody::parse(" \n")?, None);
        assert_eq!(RerankExtraBody::parse("{}")?, None);
        let padding = MAX_EXTRA_BODY_BYTES - r#"{"provider":""}"#.len();
        let largest = format!(r#"{{"provider":"{}"}}"#, "x".repeat(padding));
        assert_eq!(largest.len(), MAX_EXTRA_BODY_BYTES);
        assert!(RerankExtraBody::parse(&largest)?.is_some());
        let parsed = RerankExtraBody::parse(r#"{"a":{"b":[{"c":"sk-extra-SECRET"}]}}"#)?;
        let config = RemoteRerankConfig {
            extra_body: parsed,
            ..config("http://127.0.0.1:1/rerank", None, Default::default())
        };
        assert!(config.extra_body.is_some());
        let reranker = RemoteReranker::new(&config)?;
        for debug in [format!("{config:?}"), format!("{reranker:?}")] {
            assert!(!debug.contains("sk-extra-SECRET"), "{debug}");
            assert!(debug.contains("<redacted>"), "{debug}");
        }
        Ok(())
    }

    #[test]
    fn empty_documents_send_no_request() -> Result<()> {
        let mut reranker = client("http://127.0.0.1:1/rerank", None, 0)?;
        assert!(reranker.rerank("needle", &[])?.is_empty());
        Ok(())
    }

    #[test]
    fn parses_results_data_and_bare_arrays_in_any_order() -> Result<()> {
        for body in [
            json!({"results": [
                {"index": 2, "relevance_score": 0.9, "document": {"text": "x"}},
                {"index": 0, "relevance_score": 0.2},
                {"index": 1, "relevance_score": 0.4}
            ]}),
            json!({"object": "list", "data": [
                {"index": 1, "relevance_score": 0.4},
                {"index": 2, "relevance_score": 0.9},
                {"index": 0, "relevance_score": 0.2}
            ]}),
            json!([
                {"index": 1, "score": 0.4},
                {"index": 0, "score": 0.2},
                {"index": 2, "score": 0.9}
            ]),
        ] {
            let scores = parse_scores(body.to_string().as_bytes(), 3)?;
            assert_close(&scores, &[0.2, 0.4, 0.9]);
        }
        Ok(())
    }

    #[test]
    fn rejects_incomplete_or_invalid_results() {
        for (body, expected) in [
            (
                r#"{"results":[{"index":0,"relevance_score":0.1}]}"#,
                "1 results for 2",
            ),
            (
                r#"{"results":[{"index":0,"relevance_score":0.1},{"index":0,"relevance_score":0.2}]}"#,
                "index 0 twice",
            ),
            (
                r#"{"results":[{"index":0,"relevance_score":0.1},{"index":2,"relevance_score":0.2}]}"#,
                "index 2 for 2 documents",
            ),
            (
                r#"{"results":[{"index":0,"relevance_score":0.1},{"relevance_score":0.2}]}"#,
                "no integer `index`",
            ),
            (
                r#"{"results":[{"index":0,"relevance_score":0.1},{"index":1.0,"relevance_score":0.2}]}"#,
                "no integer `index`",
            ),
            (
                r#"{"results":[{"index":0,"relevance_score":0.1},{"index":-1,"relevance_score":0.2}]}"#,
                "no integer `index`",
            ),
            (
                r#"{"results":[{"index":0,"relevance_score":0.1},{"index":1,"relevance_score":"0.2"}]}"#,
                "non-numeric score",
            ),
            (
                r#"{"results":[{"index":0,"relevance_score":0.1},{"index":1}]}"#,
                "no `relevance_score` or `score`",
            ),
            (
                r#"{"results":[{"index":0,"relevance_score":0.1},{"index":1,"relevance_score":1e39}]}"#,
                "non-finite score",
            ),
            (
                r#"{"results":[{"index":0,"relevance_score":0.1},{"index":1,"relevance_score":NaN}]}"#,
                "invalid response body",
            ),
            (r#"{"scores":[0.1,0.2]}"#, "no `results` or `data` array"),
            (r#""ok""#, "neither an object nor an array"),
        ] {
            let error = match parse_scores(body.as_bytes(), 2) {
                Ok(scores) => panic!("{body} parsed as {scores:?}"),
                Err(error) => format!("{error:#}"),
            };
            assert!(error.contains(expected), "{body}: {error}");
        }
    }

    #[test]
    fn logits_outside_the_unit_interval_go_through_the_sigmoid_once() -> Result<()> {
        let logits = parse_scores(
            br#"[{"index":0,"score":-2.0},{"index":1,"score":0.0},{"index":2,"score":3.5}]"#,
            3,
        )?;
        let expected = [-2.0, 0.0, 3.5]
            .into_iter()
            .map(crate::rerank::sigmoid)
            .collect::<Result<Vec<f32>>>()?;
        assert_close(&logits, &expected);
        let probabilities = parse_scores(
            br#"[{"index":0,"score":0.0},{"index":1,"score":1.0},{"index":2,"score":0.25}]"#,
            3,
        )?;
        assert_eq!(probabilities, vec![0.0, 1.0, 0.25]);
        Ok(())
    }

    #[test]
    fn retries_429_and_5xx_but_not_client_errors() -> Result<()> {
        let (base, server) = serve(|n, body| match n {
            0 => reply(429, json!({"error": {"message": "slow down"}})),
            1 => reply(502, json!({"error": "bad gateway"})),
            _ => scored(n, body),
        })?;
        let url = format!("{base}/rerank");
        let scores = client(&url, None, 3)?.rerank("q", &documents(2))?;
        assert_eq!(join(server)?.len(), 3);
        assert_close(&scores, &[0.1, 0.2]);

        for (status, expected) in [
            (400, "rerank server returned HTTP 400: bad input"),
            (
                401,
                "rerank server rejected the request (HTTP 401): bad input",
            ),
        ] {
            let (base, server) =
                serve(move |_, _| reply(status, json!({"error": {"message": "bad input"}})))?;
            let url = format!("{base}/rerank");
            let error = expect_err(client(&url, None, 3)?.rerank("q", &documents(2)))?;
            assert_eq!(join(server)?.len(), 1, "{status}");
            assert!(error.starts_with(expected), "{error}");
        }
        Ok(())
    }

    #[test]
    fn api_key_never_appears_in_errors_or_debug() -> Result<()> {
        let (base, server) = serve(|_, _| {
            reply(
                401,
                json!({"error": {"message": format!("invalid key {KEY} for query secret-query")}}),
            )
        })?;
        let url = format!("{base}/rerank");
        let mut reranker = client(&url, Some(KEY), 0)?;
        let error = expect_err(reranker.rerank("q", &documents(1)))?;
        join(server)?;
        assert!(error.contains("HTTP 401"), "{error}");
        assert!(!error.contains(KEY), "{error}");
        assert!(!format!("{reranker:?}").contains(KEY));
        assert!(!format!("{:?}", config(&url, Some(KEY), RerankDialect::Documents)).contains(KEY));

        let mut unreachable = client("http://127.0.0.1:1/rerank", Some(KEY), 0)?;
        let error = expect_err(unreachable.rerank("q", &documents(1)))?;
        assert!(!error.contains(KEY), "{error}");
        assert!(!error.contains("doc 0"), "{error}");
        Ok(())
    }

    #[test]
    fn rejects_invalid_model_names_and_endpoints() {
        for model in ["", "a b", "a\tb", "a\u{7}b"] {
            for dialect in [RerankDialect::Documents, RerankDialect::Texts] {
                let mut config = config("https://api.example.test/v1/rerank", None, dialect);
                config.model = Some(model.to_owned());
                assert!(RemoteReranker::new(&config).is_err(), "{model:?}");
            }
        }
        let mut unnamed = config(
            "https://api.example.test/v1/rerank",
            None,
            Default::default(),
        );
        unnamed.model = None;
        let error = expect_err(RemoteReranker::new(&unnamed)).unwrap_or_default();
        assert!(error.contains("requires a model name"), "{error}");
        let long = "m".repeat(MAX_MODEL_NAME_CHARS + 1);
        assert!(validate_model_name(&long).is_err());
        assert!(validate_model_name(&long[1..]).is_ok());
        assert!(validate_model_name("cohere/rerank-v3.5").is_ok());
        assert!(
            RemoteReranker::new(&config(
                "http://example.com/rerank",
                Some(KEY),
                Default::default()
            ))
            .is_err()
        );
        assert!(
            RemoteReranker::new(&config(
                "ftp://example.com/rerank",
                None,
                Default::default()
            ))
            .is_err()
        );
    }

    #[test]
    fn not_found_names_the_full_endpoint_url_requirement() -> Result<()> {
        let (base, server) = serve(|_, _| reply(404, json!({"error": "no route"})))?;
        let url = format!("{base}/tok-abc");
        let error = expect_err(client(&url, None, 3)?.rerank("q", &documents(1)))?;
        assert_eq!(join(server)?.len(), 1);
        assert_eq!(
            error,
            "rerank server returned HTTP 404: no route; rerank_url must be the full endpoint \
             URL, such as https://api.cohere.com/v2/rerank"
        );
        Ok(())
    }

    #[test]
    fn not_found_with_an_extra_body_names_its_provider_policy() -> Result<()> {
        let (base, server) = serve(|_, _| reply(404, json!({"error": "no endpoints"})))?;
        let url = format!("{base}/rerank");
        let config = RemoteRerankConfig {
            extra_body: RerankExtraBody::parse(r#"{"provider": {"zdr": true}}"#)?,
            ..config(&url, None, RerankDialect::Documents)
        };
        let error = expect_err(RemoteReranker::new(&config)?.rerank("q", &documents(1)))?;
        assert_eq!(join(server)?.len(), 1);
        assert_eq!(
            error,
            "rerank server returned HTTP 404: no endpoints; the URL may be wrong, or no \
             provider matches the request's provider policy in rerank_extra_body"
        );
        assert!(!error.contains("zdr"), "{error}");
        Ok(())
    }

    #[test]
    fn server_echoes_keep_the_body_but_redact_the_key() -> Result<()> {
        let (base, server) = serve(|_, body| {
            reply(
                400,
                json!({"error": {"message": format!("bad request {body} for key {KEY}")}}),
            )
        })?;
        let url = format!("{base}/rerank");
        let config = RemoteRerankConfig {
            extra_body: RerankExtraBody::parse(r#"{"provider": {"order": ["echo-marker"]}}"#)?,
            ..config(&url, Some(KEY), RerankDialect::Documents)
        };
        let error = expect_err(RemoteReranker::new(&config)?.rerank("q", &documents(1)))?;
        join(server)?;
        assert!(error.contains("HTTP 400"), "{error}");
        assert!(!error.contains(KEY), "{error}");
        // Server text is sanitized but not stripped of request content: the extra body is
        // sent to the server as is, so a server that echoes it shows it in the error.
        assert!(error.contains("echo-marker"), "{error}");
        Ok(())
    }

    #[test]
    fn every_request_field_is_reserved() -> Result<()> {
        let documents = documents(1);
        for request in [
            RerankRequest::Documents {
                model: "m",
                query: "q",
                documents: &documents,
            },
            RerankRequest::Texts {
                query: "q",
                texts: &documents,
                truncate: true,
            },
        ] {
            // A new variant fails to compile here until it is added to the list above.
            match &request {
                RerankRequest::Documents { .. } | RerankRequest::Texts { .. } => {}
            }
            let body = serde_json::to_value(request)?;
            let fields = body.as_object().ok_or_else(|| anyhow!("not an object"))?;
            for key in fields.keys() {
                assert!(RESERVED_BODY_KEYS.contains(&key.as_str()), "{key}");
            }
        }
        Ok(())
    }

    #[test]
    fn retry_after_beyond_the_time_budget_fails_without_waiting() -> Result<()> {
        let (base, server) = serve(|_, _| Reply {
            status: 429,
            body: json!({"error": {"message": "slow down"}}).to_string(),
            retry_after: Some("30"),
        })?;
        let url = format!("{base}/rerank");
        let started = std::time::Instant::now();
        let error = expect_err(client(&url, None, 3)?.rerank("q", &documents(1)))?;
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(join(server)?.len(), 1);
        assert!(error.contains("time budget"), "{error}");
        assert!(
            error.contains("rerank server returned HTTP 429: slow down"),
            "{error}"
        );
        Ok(())
    }

    #[test]
    fn mixed_and_saturated_logits_all_go_through_the_sigmoid() -> Result<()> {
        let mixed = parse_scores(
            br#"[{"index":0,"score":-1.0},{"index":1,"score":0.5},{"index":2,"score":3.0}]"#,
            3,
        )?;
        let expected = [-1.0, 0.5, 3.0]
            .into_iter()
            .map(crate::rerank::sigmoid)
            .collect::<Result<Vec<f32>>>()?;
        assert_close(&mixed, &expected);
        // Large logits saturate to equal probabilities; search keeps the retrieval
        // order of ties.
        let saturated = parse_scores(
            br#"[{"index":0,"score":40.0},{"index":1,"score":60.0},{"index":2,"score":-200.0}]"#,
            3,
        )?;
        assert_eq!(saturated, vec![1.0, 1.0, 0.0]);
        Ok(())
    }

    #[test]
    fn clamps_retries_and_timeout() -> Result<()> {
        let mut settings = config("http://localhost:8080/rerank", None, Default::default());
        settings.endpoint.max_retries = 100;
        settings.endpoint.timeout = Duration::from_secs(3600);
        let reranker = RemoteReranker::new(&settings)?;
        assert_eq!(reranker.http.endpoint().max_retries, LIMITS.max_retries);
        assert_eq!(reranker.http.endpoint().timeout, LIMITS.max_timeout);
        Ok(())
    }
}
