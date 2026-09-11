use crate::analytics::{AnalyticsStore, ProjectGrouping, SessionKindFilter, analytics_path};
use crate::config::{MachineConfig, Paths, UserConfig, default_claude_sources};
use crate::embed::{EmbedderHandle, ModelChoice};
use crate::index::{QueryOptions, SearchIndex, SessionScopeKey};
use crate::ingest::{IngestOptions, IngestReport, ingest_all, ingest_if_stale};
use crate::lease::{INGEST_LEASE_TIMEOUT, IngestLease};
use crate::memory_search::{
    MemoryReadRequest, MemoryReadValue, MemorySearchHit, MemorySearchOptions,
};
use crate::read_budget::{ContentPage, ReadBudget, ReadField};
use crate::retrieval::{
    ContextOptions, ContextRelation, ContextResult, ContextSelector, canonical_record_id,
    context_records, resolve_record,
};
use crate::types::{Record, SourceFilter};
use crate::usage::{
    CacheWaste, CostMode, UsageQuery, UsageSummary, scan_usage, scan_usage_activity,
};
use crate::vector::VectorIndex;
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub const LOCAL_MACHINE_ID: &str = "local";
// Additive operations stay on protocol 1 so search/usage remain compatible with
// older peers. New hydration operations require a peer that understands them;
// callers surface an explicit RPC-response error when an older peer rejects one.
const RPC_PROTOCOL: u32 = 1;
const RRF_K: f32 = 60.0;
pub const MAX_SESSION_PAGE_SIZE: usize = 500;
pub const MAX_SESSION_BATCH_SIZE: usize = 32;
pub const MAX_RPC_REQUEST_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_HYDRATE_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_HYDRATE_LINE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    Lexical,
    Semantic,
    Hybrid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchSpec {
    pub query: String,
    pub project: Option<String>,
    pub role: Option<String>,
    pub tool: Option<String>,
    pub session_id: Option<String>,
    #[serde(default)]
    pub session_scope: Option<Vec<SessionScopeKey>>,
    /// Working-directory/repository scope resolved independently on each machine.
    #[serde(default)]
    pub cwd: Option<String>,
    pub source: Option<SourceFilter>,
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub limit: usize,
    pub mode: SearchMode,
    pub recency_weight: f32,
    pub recency_half_life_days: f32,
    pub min_score: Option<f32>,
    pub project_grouping: Option<ProjectGrouping>,
}

impl SearchSpec {
    fn query_options(&self) -> QueryOptions {
        QueryOptions {
            query: self.query.clone(),
            project: self.project.clone(),
            role: self.role.clone(),
            tool: self.tool.clone(),
            session_id: self.session_id.clone(),
            session_scope: self.session_scope.clone(),
            source: self.source,
            since: self.since,
            until: self.until,
            limit: self.limit,
        }
    }
}

/// Deepen past stale and filtered records until `limit` matches are found.
fn search_filtered_records(
    vector: &VectorIndex,
    index: &SearchIndex,
    embedding: &[f32],
    limit: usize,
    options: &QueryOptions,
) -> Result<Vec<(f32, Record)>> {
    let has_filters = options.project.is_some()
        || options.role.is_some()
        || options.tool.is_some()
        || options.session_id.is_some()
        || options.session_scope.is_some()
        || options.source.is_some()
        || options.since.is_some()
        || options.until.is_some();
    let allowed_doc_ids = has_filters
        .then(|| index.doc_ids_matching_filters(options))
        .transpose()?;
    let mut accepted_records = HashMap::new();
    let candidates = vector.search_filtered(embedding, limit, |doc_id| {
        if let Some(allowed_doc_ids) = &allowed_doc_ids {
            return Ok(allowed_doc_ids.contains(&doc_id));
        }
        let Some(record) = index.get_by_doc_id(doc_id)? else {
            return Ok(false);
        };
        accepted_records.insert(doc_id, record);
        Ok(true)
    })?;

    candidates
        .into_iter()
        .map(|(doc_id, distance)| {
            let record = match accepted_records.remove(&doc_id) {
                Some(record) => record,
                None => index
                    .get_by_doc_id(doc_id)?
                    .ok_or_else(|| anyhow!("accepted vector record {doc_id} was not found"))?,
            };
            Ok((distance, record))
        })
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocatedRecord {
    pub machine: String,
    pub score: f32,
    pub record: Record,
}

/// Memory results keep document identity and provenance without acquiring a
/// synthetic conversation/session identity for federation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocatedMemoryHit {
    pub machine: String,
    #[serde(flatten)]
    pub hit: MemorySearchHit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionContext {
    pub records: Vec<Record>,
    pub cwd: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionPageRequest {
    pub session_id: String,
    pub source_path: String,
    pub offset: usize,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionPageContext {
    pub session_id: String,
    pub source_path: String,
    pub records: Vec<Record>,
    pub cwd: Option<String>,
    pub offset: usize,
    pub total: usize,
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundedRecord {
    pub record_id: String,
    pub record: Record,
    pub content: ContentPage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundedContextRecord {
    pub record_id: String,
    pub relation: ContextRelation,
    pub distance: i64,
    pub record: Record,
    pub content: ContentPage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundedContext {
    pub anchor_record_id: String,
    pub order: String,
    pub offset: usize,
    pub total: usize,
    pub next_offset: Option<usize>,
    pub records: Vec<BoundedContextRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundedSessionPage {
    pub session_id: String,
    pub source_path: String,
    pub cwd: Option<String>,
    pub offset: usize,
    pub total: usize,
    pub next_offset: Option<usize>,
    pub records: Vec<BoundedRecord>,
}

#[derive(Debug)]
pub(crate) struct PeerSessionPageError {
    message: String,
}

impl std::fmt::Display for PeerSessionPageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "session-page read failed: {}", self.message)
    }
}

impl std::error::Error for PeerSessionPageError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageSpec {
    pub source: Option<SourceFilter>,
    pub project: Option<String>,
    pub project_grouping: crate::analytics::ProjectGrouping,
    pub session_keys: Option<Vec<(String, String)>>,
    #[serde(default)]
    pub machine_session_keys: Option<Vec<(String, String, String)>>,
    pub since_ms: Option<u64>,
    pub until_ms: Option<u64>,
    pub cost_mode: CostMode,
    pub include_events: bool,
    pub memo_ttl_ms: u64,
    /// Origin filter. None/Regular exclude permission reviews without requiring
    /// indexed sessions; All explicitly includes reviews. Optional for RPC
    /// requests from peers that predate the field.
    #[serde(default)]
    pub kind: Option<SessionKindFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageReportWire {
    pub authority: String,
    pub events: u64,
    pub total_tokens: u64,
    pub unknown_model_events: u64,
    pub conservative_events: u64,
    pub cost_mode: CostMode,
    pub price_catalog: String,
    pub known_cost_usd: f64,
    pub priced_events: u64,
    pub unpriced_events: u64,
    pub cache_waste: CacheWaste,
    pub by_source: Vec<UsageSummary>,
    pub details: Vec<serde_json::Value>,
    pub warnings: Vec<String>,
    pub failures: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageActivityPointWire {
    pub machine: String,
    pub source: String,
    pub timestamp_ms: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionActivitySpec {
    pub source: Option<SourceFilter>,
    pub project: Option<String>,
    pub project_grouping: ProjectGrouping,
    pub since_ms: Option<u64>,
    pub until_ms: Option<u64>,
    #[serde(default)]
    pub kind: Option<SessionKindFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionActivityPointWire {
    pub machine: String,
    pub source: String,
    pub timestamp_ms: u64,
}

#[derive(Debug)]
pub struct Federated<T> {
    pub items: Vec<T>,
    pub failures: Vec<(String, String)>,
    /// Number of candidates collected before the final result limit was applied.
    pub candidate_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum RpcOperation {
    Ping,
    Projects {
        source: Option<SourceFilter>,
    },
    Sessions {
        request: crate::cli::SessionsRequest,
    },
    SessionCount {
        request: crate::cli::SessionsRequest,
        query: Option<String>,
    },
    MemorySearch {
        options: MemorySearchOptions,
    },
    MemoryRead {
        request: MemoryReadRequest,
    },
    Search {
        spec: SearchSpec,
    },
    Recent {
        limit: usize,
        project_grouping: Option<ProjectGrouping>,
    },
    Session {
        session_id: String,
        source_path: String,
    },
    Show {
        doc_id: u64,
    },
    ResolveRecord {
        selector: ContextSelector,
    },
    Context {
        selector: ContextSelector,
        options: ContextOptions,
    },
    ReadRecord {
        selector: ContextSelector,
        field: Option<ReadField>,
        offset_chars: usize,
        max_chars: usize,
    },
    ReadContext {
        selector: ContextSelector,
        options: ContextOptions,
        offset: usize,
        max_chars: usize,
    },
    ReadSessionPages {
        requests: Vec<SessionPageRequest>,
        max_chars: usize,
    },
    SessionPage {
        request: SessionPageRequest,
    },
    SessionBatch {
        requests: Vec<SessionPageRequest>,
    },
    Index,
    Usage {
        spec: UsageSpec,
    },
    UsageActivity {
        spec: UsageSpec,
    },
    SessionActivity {
        spec: SessionActivitySpec,
    },
    HomeActivity {
        request: crate::web::ActivityRequest,
        now: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcRequest {
    protocol: u32,
    request: RpcOperation,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    usage_progress: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RpcPayload {
    Projects {
        projects: Vec<serde_json::Value>,
    },
    Sessions {
        sessions: Vec<serde_json::Value>,
    },
    SessionCount {
        count: crate::cli::SessionCount,
    },
    Pong {
        version: String,
    },
    MemoryHits {
        hits: Vec<MemorySearchHit>,
    },
    MemoryDocument {
        document: Box<MemoryReadValue>,
    },
    Records {
        records: Vec<(f32, Record)>,
    },
    Session {
        context: SessionContext,
    },
    Record {
        record: Box<Record>,
    },
    Context {
        context: ContextResult,
    },
    BoundedRecord {
        record: Box<BoundedRecord>,
    },
    BoundedContext {
        context: BoundedContext,
    },
    BoundedSessionPages {
        pages: Vec<BoundedSessionPage>,
    },
    SessionPage {
        context: SessionPageContext,
    },
    SessionBatch {
        contexts: Vec<SessionPageContext>,
    },
    Index {
        records_added: usize,
        records_embedded: usize,
        files_scanned: usize,
        files_skipped: usize,
    },
    Usage {
        report: Box<UsageReportWire>,
    },
    UsageActivity {
        points: Vec<UsageActivityPointWire>,
        partial: bool,
    },
    SessionActivity {
        points: Vec<SessionActivityPointWire>,
    },
    HomeActivity {
        activity: crate::web::RawActivityPayload,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcResponse {
    protocol: u32,
    response: RpcPayload,
}

pub fn selected_machine_ids(config: &UserConfig, requested: &[String]) -> Result<Vec<String>> {
    let mut ids = if !requested.is_empty() {
        requested.to_vec()
    } else if !config.multi_machine.default.is_empty() {
        config.multi_machine.default.clone()
    } else {
        let mut defaults = vec![LOCAL_MACHINE_ID.to_string()];
        defaults.extend(
            config
                .machines
                .iter()
                .filter(|machine| machine.enabled())
                .map(|machine| machine.id.clone()),
        );
        defaults
    };
    let mut seen = std::collections::HashSet::new();
    ids.retain(|id| seen.insert(id.clone()));
    if ids.is_empty() {
        ids.push(LOCAL_MACHINE_ID.to_string());
    }
    for id in &ids {
        if id == LOCAL_MACHINE_ID {
            continue;
        }
        let machine = config
            .machines
            .iter()
            .find(|machine| machine.id == *id)
            .ok_or_else(|| anyhow!("unknown machine '{id}'"))?;
        validate_machine(machine)?;
    }
    Ok(ids)
}

pub fn federated_search(
    paths: &Paths,
    config: &UserConfig,
    requested: &[String],
    spec: &SearchSpec,
    auto_index_local: bool,
) -> Result<Federated<LocatedRecord>> {
    let ids = selected_machine_ids(config, requested)?;
    let timeout = Duration::from_secs(config.multi_machine.timeout_seconds());
    let (tx, rx) = std::sync::mpsc::channel();

    std::thread::scope(|scope| {
        for id in &ids {
            let tx = tx.clone();
            let spec = spec.clone();
            if id == LOCAL_MACHINE_ID {
                let paths = paths.clone();
                let config = config.clone();
                scope.spawn(move || {
                    let result = search_local(&paths, &config, &spec, auto_index_local);
                    let _ = tx.send((LOCAL_MACHINE_ID.to_string(), result));
                });
            } else {
                let machine = config
                    .machines
                    .iter()
                    .find(|machine| machine.id == *id)
                    .expect("selected machines were validated")
                    .clone();
                scope.spawn(move || {
                    let result =
                        rpc_records(&machine, RpcOperation::Search { spec }, timeout, "search");
                    let _ = tx.send((machine.id.clone(), result));
                });
            }
        }
        drop(tx);
    });

    let mut successes = Vec::new();
    let mut failures = Vec::new();
    for (machine, result) in rx {
        match result {
            Ok(records) => successes.push((machine, records)),
            Err(err) => failures.push((machine, err.to_string())),
        }
    }
    if successes.is_empty() {
        let message = failures
            .iter()
            .map(|(machine, error)| format!("{machine}: {error}"))
            .collect::<Vec<_>>()
            .join("; ");
        bail!("all machine searches failed: {message}");
    }

    let use_rrf = successes.len() > 1;
    let mut items = Vec::new();
    for (machine, mut records) in successes {
        records.sort_by(|left, right| {
            right
                .0
                .partial_cmp(&left.0)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        items.extend(
            records
                .into_iter()
                .enumerate()
                .map(|(rank, (score, record))| LocatedRecord {
                    machine: machine.clone(),
                    score: if use_rrf {
                        1.0 / (RRF_K + rank as f32 + 1.0)
                    } else {
                        score
                    },
                    record,
                }),
        );
    }
    items.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.record.ts.cmp(&left.record.ts))
    });
    let candidate_count = items.len();
    if items.len() > spec.limit {
        items.truncate(spec.limit);
    }
    Ok(Federated {
        items,
        failures,
        candidate_count,
    })
}

/// Query each selected machine's memory snapshot using the same routing and
/// timeout policy as conversation search. Older peers report an explicit
/// unsupported-operation failure rather than silently omitting memories.
pub fn federated_memory_search(
    paths: &Paths,
    config: &UserConfig,
    requested: &[String],
    options: &MemorySearchOptions,
    auto_index_local: bool,
) -> Result<Federated<LocatedMemoryHit>> {
    options.validate()?;
    let ids = selected_machine_ids(config, requested)?;
    let timeout = Duration::from_secs(config.multi_machine.timeout_seconds());
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        for id in &ids {
            let tx = tx.clone();
            let options = options.clone();
            if id == LOCAL_MACHINE_ID {
                scope.spawn(move || {
                    let result = (|| {
                        if auto_index_local {
                            ensure_local_index(paths, config)?;
                        }
                        crate::memory_search::search_memory(paths, &options)
                    })();
                    let _ = tx.send((LOCAL_MACHINE_ID.to_string(), result));
                });
            } else {
                let machine = machine_by_id(config, id).expect("selected machines validated");
                scope.spawn(move || {
                    let result = (|| match rpc(
                        machine,
                        RpcOperation::MemorySearch { options },
                        timeout,
                    )? {
                        RpcPayload::MemoryHits { hits } => Ok(hits),
                        RpcPayload::Error { message } => bail!("memory search failed: {message}"),
                        _ => bail!(
                            "unexpected memory search response; upgrade peer for memory retrieval"
                        ),
                    })()
                    .with_context(|| {
                        format!(
                            "memory retrieval on '{}'; upgrade peers that do not support memories",
                            machine.id
                        )
                    });
                    let _ = tx.send((machine.id.clone(), result));
                });
            }
        }
        drop(tx);
    });
    let mut successes = Vec::new();
    let mut failures = Vec::new();
    for (machine, result) in rx {
        match result {
            Ok(hits) if hits.len() <= options.limit => successes.push((machine, hits)),
            Ok(_) => failures.push((
                machine,
                "memory search exceeded requested result limit".into(),
            )),
            Err(error) => failures.push((machine, format!("{error:#}"))),
        }
    }
    let use_rrf = successes.len() > 1;
    let mut items = Vec::new();
    for (machine, hits) in successes {
        for (rank, mut hit) in hits.into_iter().enumerate() {
            if use_rrf {
                hit.score = 1.0 / (RRF_K + rank as f32 + 1.0);
            }
            items.push(LocatedMemoryHit {
                machine: machine.clone(),
                hit,
            });
        }
    }
    items.sort_by(|left, right| {
        let order = if options.sort_by_timestamp {
            right.hit.mtime_ms.cmp(&left.hit.mtime_ms)
        } else {
            right.hit.score.total_cmp(&left.hit.score)
        };
        order
            .then_with(|| right.hit.mtime_ms.cmp(&left.hit.mtime_ms))
            .then_with(|| left.machine.cmp(&right.machine))
            .then_with(|| left.hit.section_ref.cmp(&right.hit.section_ref))
    });
    let candidate_count = items.len();
    items.truncate(options.limit);
    failures.sort();
    Ok(Federated {
        items,
        failures,
        candidate_count,
    })
}

pub fn read_memory(
    paths: &Paths,
    config: &UserConfig,
    machine_id: &str,
    request: &MemoryReadRequest,
) -> Result<MemoryReadValue> {
    request.validate()?;
    let value = if machine_id == LOCAL_MACHINE_ID {
        crate::memory_search::read_memory(paths, request)?
    } else {
        let machine = machine_by_id(config, machine_id)
            .ok_or_else(|| anyhow!("unknown machine '{machine_id}'"))?;
        match rpc(
            machine,
            RpcOperation::MemoryRead {
                request: request.clone(),
            },
            Duration::from_secs(config.multi_machine.timeout_seconds()),
        )
        .with_context(|| {
            format!("memory read on '{machine_id}'; upgrade peer for memory retrieval")
        })? {
            RpcPayload::MemoryDocument { document } => *document,
            RpcPayload::Error { message } => bail!("memory read failed: {message}"),
            _ => bail!("unexpected memory read response; upgrade peer for memory retrieval"),
        }
    };
    if value.memory_id != request.memory_id {
        bail!("memory read returned a different document");
    }
    if value.text.chars().count() > request.max_chars {
        bail!("memory read exceeded requested content budget");
    }
    if value.content.returned_chars != value.text.chars().count()
        || value.content.returned_chars > value.content.total_chars
    {
        bail!("memory read returned inconsistent content metadata");
    }
    Ok(value)
}

pub fn federated_recent(
    paths: &Paths,
    config: &UserConfig,
    requested: &[String],
    limit: usize,
    project_grouping: Option<ProjectGrouping>,
    auto_index_local: bool,
) -> Result<Federated<LocatedRecord>> {
    let ids = selected_machine_ids(config, requested)?;
    let timeout = Duration::from_secs(config.multi_machine.timeout_seconds());
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        for id in &ids {
            let tx = tx.clone();
            if id == LOCAL_MACHINE_ID {
                let paths = paths.clone();
                let config = config.clone();
                scope.spawn(move || {
                    let _ = tx.send((
                        LOCAL_MACHINE_ID.to_string(),
                        recent_local(&paths, &config, limit, project_grouping, auto_index_local),
                    ));
                });
            } else {
                let machine = config
                    .machines
                    .iter()
                    .find(|machine| machine.id == *id)
                    .expect("selected machines were validated")
                    .clone();
                scope.spawn(move || {
                    let result = rpc_records(
                        &machine,
                        RpcOperation::Recent {
                            limit,
                            project_grouping,
                        },
                        timeout,
                        "recent sessions",
                    );
                    let _ = tx.send((machine.id.clone(), result));
                });
            }
        }
        drop(tx);
    });
    let mut items = Vec::new();
    let mut failures = Vec::new();
    let mut successes = 0usize;
    for (id, records) in rx {
        match records {
            Ok(records) => {
                successes += 1;
                items.extend(records.into_iter().map(|(score, record)| LocatedRecord {
                    machine: id.clone(),
                    score,
                    record,
                }));
            }
            Err(err) => failures.push((id, err.to_string())),
        }
    }
    if successes == 0 {
        bail!(
            "all machines failed: {}",
            failures
                .iter()
                .map(|(machine, error)| format!("{machine}: {error}"))
                .collect::<Vec<_>>()
                .join("; ")
        );
    }
    items.sort_by_key(|item| std::cmp::Reverse(item.record.ts));
    let candidate_count = items.len();
    Ok(Federated {
        items,
        failures,
        candidate_count,
    })
}

pub fn session_records(
    paths: &Paths,
    config: &UserConfig,
    machine_id: &str,
    session_id: &str,
    source_path: &str,
) -> Result<Vec<Record>> {
    Ok(session_context(paths, config, machine_id, session_id, source_path)?.records)
}

pub fn session_context(
    paths: &Paths,
    config: &UserConfig,
    machine_id: &str,
    session_id: &str,
    source_path: &str,
) -> Result<SessionContext> {
    if machine_id == LOCAL_MACHINE_ID {
        let index = SearchIndex::open_or_create(&paths.index)?;
        return Ok(SessionContext {
            records: records_for_session(&index, session_id, source_path)?,
            cwd: discover_cwd(std::path::Path::new(source_path), session_id),
        });
    }
    let machine = config
        .machines
        .iter()
        .find(|machine| machine.id == machine_id)
        .ok_or_else(|| anyhow!("unknown machine '{machine_id}'"))?;
    match rpc(
        machine,
        RpcOperation::Session {
            session_id: session_id.to_string(),
            source_path: source_path.to_string(),
        },
        Duration::from_secs(config.multi_machine.timeout_seconds()),
    )? {
        RpcPayload::Session { context } => Ok(context),
        RpcPayload::Error { message } => Err(anyhow!("session failed: {message}")),
        other => Err(anyhow!("session returned unexpected response: {other:?}")),
    }
}

pub fn record_by_doc_id(
    paths: &Paths,
    config: &UserConfig,
    machine_id: &str,
    doc_id: u64,
) -> Result<Record> {
    if machine_id == LOCAL_MACHINE_ID {
        let index = SearchIndex::open_or_create(&paths.index)?;
        return index
            .get_by_doc_id(doc_id)?
            .ok_or_else(|| anyhow!("doc_id not found"));
    }
    let machine = config
        .machines
        .iter()
        .find(|machine| machine.id == machine_id)
        .ok_or_else(|| anyhow!("unknown machine '{machine_id}'"))?;
    match rpc(
        machine,
        RpcOperation::Show { doc_id },
        Duration::from_secs(config.multi_machine.timeout_seconds()),
    )? {
        RpcPayload::Record { record } => Ok(*record),
        RpcPayload::Error { message } => Err(anyhow!("show failed: {message}")),
        other => Err(anyhow!("show returned unexpected response: {other:?}")),
    }
}

/// Resolve a record on exactly the machine that supplied its retrieval reference.
/// Legacy document-ID reads retain the existing RPC operation for older peers.
pub fn record_by_selector(
    paths: &Paths,
    config: &UserConfig,
    machine_id: &str,
    selector: &ContextSelector,
) -> Result<Record> {
    if let ContextSelector::DocId {
        id,
        session_id: None,
        source: None,
    } = selector
    {
        return record_by_doc_id(paths, config, machine_id, *id);
    }
    if machine_id == LOCAL_MACHINE_ID {
        return resolve_record(&SearchIndex::open_or_create(&paths.index)?, selector);
    }
    let machine = machine_by_id(config, machine_id)
        .ok_or_else(|| anyhow!("unknown machine '{machine_id}'"))?;
    match rpc(
        machine,
        RpcOperation::ResolveRecord {
            selector: selector.clone(),
        },
        Duration::from_secs(config.multi_machine.timeout_seconds()),
    )? {
        RpcPayload::Record { record } => Ok(*record),
        RpcPayload::Error { message } => Err(anyhow!("record resolution failed: {message}")),
        other => Err(anyhow!(
            "record resolution returned unexpected response: {other:?}"
        )),
    }
}

pub fn context_on_machine(
    paths: &Paths,
    config: &UserConfig,
    machine_id: &str,
    selector: &ContextSelector,
    options: ContextOptions,
) -> Result<ContextResult> {
    options.validate()?;
    if machine_id == LOCAL_MACHINE_ID {
        return context_records(
            &SearchIndex::open_or_create(&paths.index)?,
            selector,
            options,
        );
    }
    let machine = machine_by_id(config, machine_id)
        .ok_or_else(|| anyhow!("unknown machine '{machine_id}'"))?;
    match rpc(
        machine,
        RpcOperation::Context {
            selector: selector.clone(),
            options,
        },
        Duration::from_secs(config.multi_machine.timeout_seconds()),
    )? {
        RpcPayload::Context { context } => Ok(context),
        RpcPayload::Error { message } => Err(anyhow!("context failed: {message}")),
        other => Err(anyhow!("context returned unexpected response: {other:?}")),
    }
}

pub fn read_record(
    paths: &Paths,
    config: &UserConfig,
    machine_id: &str,
    selector: &ContextSelector,
    field: Option<ReadField>,
    offset_chars: usize,
    max_chars: Option<usize>,
) -> Result<BoundedRecord> {
    validate_context_selector(selector)?;
    if max_chars == Some(0) {
        bail!("max_chars must be greater than zero for a record read");
    }
    if max_chars.is_none() {
        let record = record_by_selector(paths, config, machine_id, selector)?;
        let mut budget = ReadBudget::from_remaining(None);
        let bounded = apply_record_budget(record, &mut budget, field, offset_chars)?;
        validate_bounded_record(&bounded, selector, None)?;
        return Ok(bounded);
    }
    if machine_id == LOCAL_MACHINE_ID {
        let record = resolve_record(&SearchIndex::open_or_create(&paths.index)?, selector)?;
        let mut budget = ReadBudget::from_remaining(max_chars);
        return apply_record_budget(record, &mut budget, field, offset_chars);
    }
    let machine = machine_by_id(config, machine_id)
        .ok_or_else(|| anyhow!("unknown machine '{machine_id}'"))?;
    let response = rpc(
        machine,
        RpcOperation::ReadRecord {
            selector: selector.clone(),
            field,
            offset_chars,
            max_chars: max_chars.expect("bounded branch"),
        },
        Duration::from_secs(config.multi_machine.timeout_seconds()),
    )
    .with_context(|| bounded_rpc_help(machine_id, "record read"))?;
    match response {
        RpcPayload::BoundedRecord { record } => {
            validate_bounded_record(&record, selector, max_chars)?;
            Ok(*record)
        }
        RpcPayload::Error { message } => Err(anyhow!("record read failed: {message}")),
        other => Err(anyhow!(
            "record read returned an unexpected response; upgrade peer for bounded reads: {other:?}"
        )),
    }
}

pub fn read_context(
    paths: &Paths,
    config: &UserConfig,
    machine_id: &str,
    selector: &ContextSelector,
    options: ContextOptions,
    offset: usize,
    max_chars: Option<usize>,
) -> Result<BoundedContext> {
    validate_context_selector(selector)?;
    options.validate()?;
    if max_chars.is_none() {
        let context = context_on_machine(paths, config, machine_id, selector, options)?;
        let bounded = apply_context_budget(context, offset, None)?;
        validate_bounded_context(&bounded, selector, offset, None)?;
        return Ok(bounded);
    }
    if machine_id == LOCAL_MACHINE_ID {
        let context = context_records(
            &SearchIndex::open_or_create(&paths.index)?,
            selector,
            options,
        )?;
        return apply_context_budget(context, offset, max_chars);
    }
    let machine = machine_by_id(config, machine_id)
        .ok_or_else(|| anyhow!("unknown machine '{machine_id}'"))?;
    let response = rpc(
        machine,
        RpcOperation::ReadContext {
            selector: selector.clone(),
            options,
            offset,
            max_chars: max_chars.expect("bounded branch"),
        },
        Duration::from_secs(config.multi_machine.timeout_seconds()),
    )
    .with_context(|| bounded_rpc_help(machine_id, "context read"))?;
    match response {
        RpcPayload::BoundedContext { context } => {
            validate_bounded_context(&context, selector, offset, max_chars)?;
            Ok(context)
        }
        RpcPayload::Error { message } => Err(anyhow!("context read failed: {message}")),
        other => Err(anyhow!(
            "context read returned an unexpected response; upgrade peer for bounded reads: {other:?}"
        )),
    }
}

pub fn read_session_pages(
    paths: &Paths,
    config: &UserConfig,
    machine_id: &str,
    requests: &[SessionPageRequest],
    max_chars: Option<usize>,
) -> Result<Vec<BoundedSessionPage>> {
    validate_session_batch(requests)?;
    if max_chars.is_none() {
        let contexts = batch_session_contexts(paths, config, machine_id, requests)?;
        let pages = apply_session_pages_budget(contexts, None)?;
        validate_bounded_session_pages(&pages, requests, None)?;
        return Ok(pages);
    }
    if machine_id == LOCAL_MACHINE_ID {
        let contexts = requests
            .iter()
            .map(|request| session_page_context_local(paths, request))
            .collect::<Result<Vec<_>>>()?;
        return apply_session_pages_budget(contexts, max_chars);
    }
    let machine = machine_by_id(config, machine_id)
        .ok_or_else(|| anyhow!("unknown machine '{machine_id}'"))?;
    let response = rpc(
        machine,
        RpcOperation::ReadSessionPages {
            requests: requests.to_vec(),
            max_chars: max_chars.expect("bounded branch"),
        },
        Duration::from_secs(config.multi_machine.timeout_seconds()),
    )
    .with_context(|| bounded_rpc_help(machine_id, "session-page read"))?;
    match response {
        RpcPayload::BoundedSessionPages { pages } => {
            validate_bounded_session_pages(&pages, requests, max_chars)?;
            Ok(pages)
        }
        RpcPayload::Error { message } => Err(PeerSessionPageError { message }.into()),
        other => Err(anyhow!(
            "session-page read returned an unexpected response; upgrade peer for bounded reads: {other:?}"
        )),
    }
}

pub fn session_page_context(
    paths: &Paths,
    config: &UserConfig,
    machine_id: &str,
    request: &SessionPageRequest,
) -> Result<SessionPageContext> {
    validate_session_page_request(request)?;
    if machine_id == LOCAL_MACHINE_ID {
        return session_page_context_local(paths, request);
    }
    let machine = config
        .machines
        .iter()
        .find(|machine| machine.id == machine_id)
        .ok_or_else(|| anyhow!("unknown machine '{machine_id}'"))?;
    match rpc(
        machine,
        RpcOperation::SessionPage {
            request: request.clone(),
        },
        Duration::from_secs(config.multi_machine.timeout_seconds()),
    )? {
        RpcPayload::SessionPage { context } => {
            validate_session_page_context(&context, request)?;
            Ok(context)
        }
        RpcPayload::Error { message } => Err(anyhow!("session page failed: {message}")),
        other => Err(anyhow!(
            "session page returned unexpected response: {other:?}"
        )),
    }
}

pub fn batch_session_contexts(
    paths: &Paths,
    config: &UserConfig,
    machine_id: &str,
    requests: &[SessionPageRequest],
) -> Result<Vec<SessionPageContext>> {
    validate_session_batch(requests)?;
    if machine_id == LOCAL_MACHINE_ID {
        return requests
            .iter()
            .map(|request| session_page_context_local(paths, request))
            .collect();
    }
    let machine = config
        .machines
        .iter()
        .find(|machine| machine.id == machine_id)
        .ok_or_else(|| anyhow!("unknown machine '{machine_id}'"))?;
    match rpc(
        machine,
        RpcOperation::SessionBatch {
            requests: requests.to_vec(),
        },
        Duration::from_secs(config.multi_machine.timeout_seconds()),
    )? {
        RpcPayload::SessionBatch { contexts } => {
            if contexts.len() != requests.len() {
                bail!(
                    "session batch returned {} contexts for {} requests",
                    contexts.len(),
                    requests.len()
                );
            }
            for (context, request) in contexts.iter().zip(requests) {
                validate_session_page_context(context, request)?;
            }
            Ok(contexts)
        }
        RpcPayload::Error { message } => Err(anyhow!("session batch failed: {message}")),
        other => Err(anyhow!(
            "session batch returned unexpected response: {other:?}"
        )),
    }
}

fn session_page_context_local(
    paths: &Paths,
    request: &SessionPageRequest,
) -> Result<SessionPageContext> {
    let index = SearchIndex::open_or_create(&paths.index)?;
    let (records, total) = records_for_session_page(&index, request)?;
    let next_offset = (request.offset.saturating_add(records.len()) < total)
        .then_some(request.offset.saturating_add(records.len()));
    Ok(SessionPageContext {
        session_id: request.session_id.clone(),
        source_path: request.source_path.clone(),
        records,
        cwd: discover_cwd(
            std::path::Path::new(&request.source_path),
            &request.session_id,
        ),
        offset: request.offset,
        total,
        next_offset,
    })
}

fn validate_session_page_request(request: &SessionPageRequest) -> Result<()> {
    if request.session_id.is_empty() {
        bail!("session page session_id must not be empty");
    }
    if request.limit == 0 {
        bail!("session page limit must be greater than zero");
    }
    if request.limit > MAX_SESSION_PAGE_SIZE {
        bail!(
            "session page limit {} exceeds maximum {}",
            request.limit,
            MAX_SESSION_PAGE_SIZE
        );
    }
    Ok(())
}

fn validate_session_batch(requests: &[SessionPageRequest]) -> Result<()> {
    if requests.len() > MAX_SESSION_BATCH_SIZE {
        bail!(
            "session batch size {} exceeds maximum {}",
            requests.len(),
            MAX_SESSION_BATCH_SIZE
        );
    }
    for request in requests {
        validate_session_page_request(request)?;
    }
    Ok(())
}

fn validate_session_page_context(
    context: &SessionPageContext,
    request: &SessionPageRequest,
) -> Result<()> {
    if context.session_id != request.session_id || context.source_path != request.source_path {
        bail!("session page response does not match its request");
    }
    if context.offset != request.offset {
        bail!("session page response has an unexpected offset");
    }
    if context.records.len() > request.limit {
        bail!("session page response exceeds its requested limit");
    }
    if context.offset > context.total {
        if !context.records.is_empty() || context.next_offset.is_some() {
            bail!("session page response has invalid out-of-range offset");
        }
        return Ok(());
    }
    let expected_next = context
        .offset
        .checked_add(context.records.len())
        .ok_or_else(|| anyhow!("session page response has overflowing pagination metadata"))?;
    if context.total < expected_next {
        bail!("session page response has invalid pagination total");
    }
    if expected_next < context.total {
        if context.records.is_empty() || context.next_offset != Some(expected_next) {
            bail!("session page response has inconsistent continuation metadata");
        }
    } else if context.next_offset.is_some() {
        bail!("session page response has invalid pagination metadata");
    }
    Ok(())
}

fn apply_record_budget(
    mut record: Record,
    budget: &mut ReadBudget,
    field: Option<ReadField>,
    offset_chars: usize,
) -> Result<BoundedRecord> {
    let record_id = canonical_record_id(&record);
    let content = budget.apply(&mut record, field, offset_chars)?;
    Ok(BoundedRecord {
        record_id,
        record,
        content,
    })
}

fn apply_context_budget(
    mut context: ContextResult,
    offset: usize,
    max_chars: Option<usize>,
) -> Result<BoundedContext> {
    let order = if max_chars.is_some() {
        context
            .records
            .sort_by_key(|item| item.relation != ContextRelation::Anchor);
        "anchor_first"
    } else {
        "chronological"
    };
    let total = context.records.len();
    let mut budget = ReadBudget::from_remaining(max_chars);
    let mut records = Vec::new();
    if max_chars != Some(0) {
        for item in context.records.into_iter().skip(offset) {
            if budget.remaining() == Some(0) {
                break;
            }
            let bounded = apply_record_budget(item.record, &mut budget, None, 0)?;
            records.push(BoundedContextRecord {
                record_id: bounded.record_id,
                relation: item.relation,
                distance: item.distance,
                record: bounded.record,
                content: bounded.content,
            });
        }
    }
    let next_offset = bounded_next_offset(offset, records.len(), total);
    Ok(BoundedContext {
        anchor_record_id: context.anchor_record_id,
        order: order.to_string(),
        offset,
        total,
        next_offset,
        records,
    })
}

fn apply_session_pages_budget(
    contexts: Vec<SessionPageContext>,
    max_chars: Option<usize>,
) -> Result<Vec<BoundedSessionPage>> {
    let mut budget = ReadBudget::from_remaining(max_chars);
    contexts
        .into_iter()
        .map(|context| {
            let mut records = Vec::new();
            if budget.remaining() != Some(0) {
                for record in context.records {
                    if budget.remaining() == Some(0) {
                        break;
                    }
                    records.push(apply_record_budget(record, &mut budget, None, 0)?);
                }
            }
            Ok(BoundedSessionPage {
                session_id: context.session_id,
                source_path: context.source_path,
                cwd: context.cwd,
                offset: context.offset,
                total: context.total,
                next_offset: bounded_next_offset(context.offset, records.len(), context.total),
                records,
            })
        })
        .collect()
}

fn bounded_next_offset(offset: usize, returned: usize, total: usize) -> Option<usize> {
    let end = offset.saturating_add(returned);
    (end < total).then_some(end)
}

fn validate_context_selector(selector: &ContextSelector) -> Result<()> {
    let (id, session_id) = match selector {
        ContextSelector::RecordId { id, session_id, .. }
        | ContextSelector::EventId { id, session_id, .. } => (Some(id.as_str()), session_id),
        ContextSelector::DocId { session_id, .. } => (None, session_id),
    };
    if id.is_some_and(str::is_empty) {
        bail!("record selector id must not be empty");
    }
    if session_id.as_deref().is_some_and(str::is_empty) {
        bail!("record selector session_id must not be empty");
    }
    Ok(())
}

fn record_matches_selector(record: &Record, selector: &ContextSelector) -> bool {
    let (session_id, source, identity_matches) = match selector {
        ContextSelector::RecordId {
            id,
            session_id,
            source,
        } => (session_id, source, canonical_record_id(record) == *id),
        ContextSelector::DocId {
            id,
            session_id,
            source,
        } => (session_id, source, record.doc_id == *id),
        ContextSelector::EventId {
            id,
            session_id,
            source,
        } => (
            session_id,
            source,
            record.links.event_id.as_deref() == Some(id.as_str()),
        ),
    };
    identity_matches
        && session_id
            .as_deref()
            .is_none_or(|session_id| record.session_id == session_id)
        && source.is_none_or(|source| record.source == source)
}

fn validate_content_page(record: &Record, content: &ContentPage) -> Result<usize> {
    let actual = record.text.chars().count()
        + record
            .tool_input
            .as_deref()
            .map_or(0, |value| value.chars().count())
        + record
            .tool_output
            .as_deref()
            .map_or(0, |value| value.chars().count());
    if actual != content.returned_chars {
        bail!("bounded response content length does not match returned_chars");
    }
    if content.returned_chars > content.total_chars {
        bail!("bounded response returned_chars exceeds total_chars");
    }
    if content.truncated != !content.continuations.is_empty() {
        bail!("bounded response has inconsistent truncation metadata");
    }
    let mut fields = Vec::new();
    for continuation in &content.continuations {
        if continuation.offset_chars > continuation.total_chars {
            bail!("bounded response has invalid continuation metadata");
        }
        if fields.contains(&continuation.field) {
            bail!("bounded response repeats a continuation field");
        }
        fields.push(continuation.field);
    }
    Ok(actual)
}

fn validate_bounded_record(
    bounded: &BoundedRecord,
    selector: &ContextSelector,
    max_chars: Option<usize>,
) -> Result<()> {
    if bounded.record_id != canonical_record_id(&bounded.record) {
        bail!("record read returned a non-canonical record identity");
    }
    if !record_matches_selector(&bounded.record, selector) {
        bail!("record read response does not match its selector");
    }
    let returned = validate_content_page(&bounded.record, &bounded.content)?;
    if max_chars.is_some_and(|max_chars| returned > max_chars) {
        bail!("record read response exceeds its requested character budget");
    }
    Ok(())
}

fn validate_bounded_context(
    context: &BoundedContext,
    selector: &ContextSelector,
    offset: usize,
    max_chars: Option<usize>,
) -> Result<()> {
    if context.offset != offset {
        bail!("context read response has an unexpected offset");
    }
    let expected_order = if max_chars.is_some() {
        "anchor_first"
    } else {
        "chronological"
    };
    if context.order != expected_order {
        bail!("context read response has an unexpected order");
    }
    validate_bounded_pagination(
        context.offset,
        context.total,
        context.next_offset,
        context.records.len(),
        "context read",
    )?;
    let mut returned = 0usize;
    let mut anchor_count = 0usize;
    for item in &context.records {
        if item.record_id != canonical_record_id(&item.record) {
            bail!("context read returned a non-canonical record identity");
        }
        if item.relation == ContextRelation::Anchor {
            anchor_count += 1;
            if item.record_id != context.anchor_record_id
                || !record_matches_selector(&item.record, selector)
            {
                bail!("context read returned the wrong anchor record");
            }
        }
        returned = returned
            .checked_add(validate_content_page(&item.record, &item.content)?)
            .ok_or_else(|| anyhow!("context read character count overflowed"))?;
    }
    if max_chars.is_some_and(|max_chars| returned > max_chars) {
        bail!("context read response exceeds its requested character budget");
    }
    if max_chars.is_some_and(|max_chars| max_chars > 0) && offset == 0 {
        if context.records.first().is_none_or(|item| {
            item.relation != ContextRelation::Anchor
                || item.record_id != context.anchor_record_id
                || !record_matches_selector(&item.record, selector)
        }) || anchor_count != 1
        {
            bail!("bounded context response does not begin with exactly the selected anchor");
        }
    } else if anchor_count > 1 {
        bail!("context read response contains duplicate anchors");
    }
    Ok(())
}

fn validate_bounded_session_pages(
    pages: &[BoundedSessionPage],
    requests: &[SessionPageRequest],
    max_chars: Option<usize>,
) -> Result<()> {
    if pages.len() != requests.len() {
        bail!(
            "session-page read returned {} pages for {} requests",
            pages.len(),
            requests.len()
        );
    }
    let mut returned = 0usize;
    for (page, request) in pages.iter().zip(requests) {
        if page.session_id != request.session_id
            || page.source_path != request.source_path
            || page.offset != request.offset
        {
            bail!("session-page read response does not match its request");
        }
        if page.records.len() > request.limit {
            bail!("session-page read response exceeds its requested record limit");
        }
        validate_bounded_pagination(
            page.offset,
            page.total,
            page.next_offset,
            page.records.len(),
            "session-page read",
        )?;
        for bounded in &page.records {
            if bounded.record_id != canonical_record_id(&bounded.record) {
                bail!("session-page read returned a non-canonical record identity");
            }
            if bounded.record.session_id != request.session_id
                || (!request.source_path.is_empty()
                    && bounded.record.source_path != request.source_path)
            {
                bail!("session-page read returned a record outside its requested session");
            }
            returned = returned
                .checked_add(validate_content_page(&bounded.record, &bounded.content)?)
                .ok_or_else(|| anyhow!("session-page read character count overflowed"))?;
        }
    }
    if max_chars.is_some_and(|max_chars| returned > max_chars) {
        bail!("session-page read response exceeds its requested character budget");
    }
    Ok(())
}

fn validate_bounded_pagination(
    offset: usize,
    total: usize,
    next_offset: Option<usize>,
    returned: usize,
    context: &str,
) -> Result<()> {
    if offset > total {
        if returned != 0 || next_offset.is_some() {
            bail!("{context} response has invalid out-of-range pagination");
        }
        return Ok(());
    }
    let expected_end = offset
        .checked_add(returned)
        .ok_or_else(|| anyhow!("{context} response has overflowing pagination metadata"))?;
    if expected_end > total {
        bail!("{context} response has an invalid pagination total");
    }
    let expected_next = (expected_end < total).then_some(expected_end);
    if next_offset != expected_next {
        bail!("{context} response has inconsistent continuation metadata");
    }
    Ok(())
}

fn bounded_rpc_help(machine_id: &str, operation: &str) -> String {
    format!(
        "{operation} requires bounded-read RPC support on machine '{machine_id}'; upgrade peer for bounded reads"
    )
}

pub fn machine_by_id<'a>(config: &'a UserConfig, id: &str) -> Option<&'a MachineConfig> {
    config.machines.iter().find(|machine| machine.id == id)
}

fn usage_spec_for_machine(spec: &UsageSpec, machine: &str) -> UsageSpec {
    let mut machine_spec = spec.clone();
    if let Some(keys) = &spec.machine_session_keys {
        machine_spec.session_keys = Some(
            keys.iter()
                .filter(|(key_machine, _, _)| key_machine == machine)
                .map(|(_, source, session_id)| (source.clone(), session_id.clone()))
                .collect(),
        );
        machine_spec.machine_session_keys = None;
    }
    machine_spec
}

pub fn federated_usage(
    paths: &Paths,
    config: &UserConfig,
    requested: &[String],
    spec: &UsageSpec,
) -> Result<UsageReportWire> {
    let ids = selected_machine_ids(config, requested)?;
    let timeout = Duration::from_secs(config.multi_machine.timeout_seconds());
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        for id in &ids {
            let tx = tx.clone();
            let spec = usage_spec_for_machine(spec, id);
            if id == LOCAL_MACHINE_ID {
                let paths = paths.clone();
                let config = config.clone();
                scope.spawn(move || {
                    let result = usage_local(&paths, &config, &spec);
                    let _ = tx.send((LOCAL_MACHINE_ID.to_string(), result));
                });
            } else {
                let machine = config
                    .machines
                    .iter()
                    .find(|machine| machine.id == *id)
                    .expect("selected machines were validated")
                    .clone();
                scope.spawn(move || {
                    let result = match rpc(&machine, RpcOperation::Usage { spec }, timeout) {
                        Ok(RpcPayload::Usage { report }) => Ok(*report),
                        Ok(RpcPayload::Error { message }) => Err(anyhow!(message)),
                        Ok(other) => Err(anyhow!("unexpected usage response: {other:?}")),
                        Err(err) => Err(err),
                    };
                    let _ = tx.send((machine.id.clone(), result));
                });
            }
        }
        drop(tx);
    });

    let mut reports = Vec::new();
    let mut failures = Vec::new();
    for (machine, result) in rx {
        match result {
            Ok(report) => reports.push((machine, report)),
            Err(err) => failures.push((machine, err.to_string())),
        }
    }
    if reports.is_empty() {
        bail!(
            "all machine usage scans failed: {}",
            failures
                .iter()
                .map(|(machine, error)| format!("{machine}: {error}"))
                .collect::<Vec<_>>()
                .join("; ")
        );
    }
    Ok(merge_usage_reports(reports, failures, spec.cost_mode))
}

pub fn federated_usage_activity(
    paths: &Paths,
    config: &UserConfig,
    requested: &[String],
    spec: &UsageSpec,
) -> Result<(Vec<UsageActivityPointWire>, bool)> {
    let ids = selected_machine_ids(config, requested)?;
    let timeout = Duration::from_secs(config.multi_machine.timeout_seconds());
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        for id in &ids {
            let tx = tx.clone();
            let spec = usage_spec_for_machine(spec, id);
            if id == LOCAL_MACHINE_ID {
                let paths = paths.clone();
                let config = config.clone();
                scope.spawn(move || {
                    let result = usage_activity_local(&paths, &config, &spec);
                    let _ = tx.send((LOCAL_MACHINE_ID.to_string(), result));
                });
            } else {
                let machine = config
                    .machines
                    .iter()
                    .find(|machine| machine.id == *id)
                    .expect("selected machines were validated")
                    .clone();
                scope.spawn(move || {
                    let result = match rpc(&machine, RpcOperation::UsageActivity { spec }, timeout)
                    {
                        Ok(RpcPayload::UsageActivity { points, partial }) => Ok((points, partial)),
                        Ok(RpcPayload::Error { message }) => Err(anyhow!(message)),
                        Ok(other) => Err(anyhow!("unexpected usage activity response: {other:?}")),
                        Err(err) => Err(err),
                    };
                    let _ = tx.send((machine.id.clone(), result));
                });
            }
        }
        drop(tx);
    });
    let mut points = Vec::new();
    let mut partial = false;
    let mut successes = 0usize;
    let mut errors = Vec::new();
    for (machine, result) in rx {
        match result {
            Ok((mut machine_points, machine_partial)) => {
                successes += 1;
                partial |= machine_partial;
                for point in &mut machine_points {
                    point.machine.clone_from(&machine);
                }
                points.extend(machine_points);
            }
            Err(err) => {
                partial = true;
                errors.push(format!("{machine}: {err}"));
            }
        }
    }
    if successes == 0 {
        bail!(
            "all machine usage activity scans failed: {}",
            errors.join("; ")
        );
    }
    points.sort_by_key(|point| point.timestamp_ms);
    Ok((points, partial))
}

pub fn federated_session_activity(
    paths: &Paths,
    config: &UserConfig,
    requested: &[String],
    spec: &SessionActivitySpec,
) -> Result<(Vec<SessionActivityPointWire>, bool)> {
    let ids = selected_machine_ids(config, requested)?;
    let timeout = Duration::from_secs(config.multi_machine.timeout_seconds());
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        for id in &ids {
            let tx = tx.clone();
            let spec = spec.clone();
            if id == LOCAL_MACHINE_ID {
                let paths = paths.clone();
                scope.spawn(move || {
                    let result = session_activity_local(&paths, &spec);
                    let _ = tx.send((LOCAL_MACHINE_ID.to_string(), result));
                });
            } else {
                let machine = config
                    .machines
                    .iter()
                    .find(|machine| machine.id == *id)
                    .expect("selected machines were validated")
                    .clone();
                scope.spawn(move || {
                    let result =
                        match rpc(&machine, RpcOperation::SessionActivity { spec }, timeout) {
                            Ok(RpcPayload::SessionActivity { points }) => Ok(points),
                            Ok(RpcPayload::Error { message }) => Err(anyhow!(message)),
                            Ok(other) => {
                                Err(anyhow!("unexpected session activity response: {other:?}"))
                            }
                            Err(err) => Err(err),
                        };
                    let _ = tx.send((machine.id.clone(), result));
                });
            }
        }
        drop(tx);
    });
    let mut points = Vec::new();
    let mut successes = 0usize;
    let mut errors = Vec::new();
    for (machine, result) in rx {
        match result {
            Ok(mut machine_points) => {
                successes += 1;
                for point in &mut machine_points {
                    point.machine.clone_from(&machine);
                }
                points.extend(machine_points);
            }
            Err(err) => errors.push(format!("{machine}: {err}")),
        }
    }
    if successes == 0 {
        bail!(
            "all machine session activity queries failed: {}",
            errors.join("; ")
        );
    }
    points.sort_by_key(|point| point.timestamp_ms);
    Ok((points, !errors.is_empty()))
}

/// Discovery deliberately ignores the configured default search subset and never
/// exposes transport commands, hosts, credentials, or index configuration.
pub fn configured_machine_summaries(config: &UserConfig) -> Result<Vec<serde_json::Value>> {
    let mut items = vec![serde_json::json!({"id": LOCAL_MACHINE_ID, "label": "This Mac"})];
    let mut seen = HashSet::from([LOCAL_MACHINE_ID.to_string()]);
    for machine in config.machines.iter().filter(|machine| machine.enabled()) {
        validate_machine(machine)?;
        if !seen.insert(machine.id.clone()) {
            bail!("duplicate machine id '{}'", machine.id);
        }
        items.push(serde_json::json!({
            "id": machine.id,
            "label": machine.label.as_deref().filter(|label| !label.trim().is_empty()).unwrap_or(&machine.id),
        }));
    }
    Ok(items)
}

pub(crate) fn remote_project_summaries(
    config: &UserConfig,
    id: &str,
    source: Option<SourceFilter>,
) -> Result<Vec<serde_json::Value>> {
    match metadata_rpc(config, id, RpcOperation::Projects { source }, "projects")? {
        RpcPayload::Projects { projects } => Ok(projects),
        _ => bail!("machine '{id}' returned an unexpected projects metadata response"),
    }
}

pub(crate) fn remote_sessions(
    config: &UserConfig,
    id: &str,
    request: crate::cli::SessionsRequest,
) -> Result<Vec<serde_json::Value>> {
    match metadata_rpc(config, id, RpcOperation::Sessions { request }, "sessions")? {
        RpcPayload::Sessions { sessions } => Ok(sessions),
        _ => bail!("machine '{id}' returned an unexpected sessions metadata response"),
    }
}

pub(crate) fn remote_session_count(
    config: &UserConfig,
    id: &str,
    request: crate::cli::SessionsRequest,
    query: Option<String>,
) -> Result<crate::cli::SessionCount> {
    match metadata_rpc(
        config,
        id,
        RpcOperation::SessionCount { request, query },
        "session count",
    )? {
        RpcPayload::SessionCount { count } => Ok(count),
        _ => bail!("machine '{id}' returned an unexpected session count metadata response"),
    }
}

pub(crate) fn remote_activity(
    config: &UserConfig,
    id: &str,
    request: crate::web::ActivityRequest,
    now: u64,
) -> Result<crate::web::RawActivityPayload> {
    remote_activity_with(&request, now, |operation| {
        metadata_rpc(config, id, operation, "activity")
    })
}

fn remote_activity_with(
    request: &crate::web::ActivityRequest,
    now: u64,
    mut call: impl FnMut(RpcOperation) -> Result<RpcPayload>,
) -> Result<crate::web::RawActivityPayload> {
    match call(RpcOperation::HomeActivity {
        request: request.clone(),
        now,
    }) {
        Ok(RpcPayload::HomeActivity { activity }) => return Ok(activity),
        Err(error)
            if error
                .to_string()
                .contains("unknown variant `home_activity`") => {}
        Err(error) => return Err(error),
        _ => bail!("unexpected activity response"),
    }
    if !request.query.is_empty() {
        bail!("This machine needs a newer Memex build for activity filtered by search.");
    }
    let since_ms = request
        .range
        .map(|range| range.since_ms(now))
        .unwrap_or_else(|| Some(now.saturating_sub(request.days as u64 * 86_400_000)));
    let operation = match request.metric {
        crate::web::ActivityMetric::Sessions => RpcOperation::SessionActivity {
            spec: SessionActivitySpec {
                source: request.source,
                project: request.project.clone(),
                project_grouping: ProjectGrouping::Flat,
                since_ms,
                until_ms: None,
                kind: Some(request.origin),
            },
        },
        crate::web::ActivityMetric::Tokens => RpcOperation::UsageActivity {
            spec: UsageSpec {
                source: request.source,
                project: request.project.clone(),
                project_grouping: ProjectGrouping::Flat,
                session_keys: None,
                machine_session_keys: None,
                since_ms,
                until_ms: None,
                cost_mode: CostMode::Source,
                include_events: false,
                memo_ttl_ms: 60_000,
                kind: Some(request.origin),
            },
        },
    };
    match call(operation)? {
        RpcPayload::SessionActivity { points }
            if matches!(request.metric, crate::web::ActivityMetric::Sessions) =>
        {
            Ok(crate::web::RawActivityPayload::from_remote_points(
                points
                    .into_iter()
                    .map(|point| (point.timestamp_ms, point.source, 1)),
                request,
                false,
            ))
        }
        RpcPayload::UsageActivity { points, partial }
            if matches!(request.metric, crate::web::ActivityMetric::Tokens) =>
        {
            Ok(crate::web::RawActivityPayload::from_remote_points(
                points
                    .into_iter()
                    .map(|point| (point.timestamp_ms, point.source, point.total_tokens)),
                request,
                partial,
            ))
        }
        _ => bail!("unexpected legacy activity response"),
    }
}

fn metadata_rpc(
    config: &UserConfig,
    id: &str,
    operation: RpcOperation,
    name: &str,
) -> Result<RpcPayload> {
    let machine = config
        .machines
        .iter()
        .find(|machine| machine.id == id)
        .ok_or_else(|| anyhow!("unknown machine '{id}'"))?;
    let response = rpc(
        machine,
        operation,
        Duration::from_secs(config.multi_machine.timeout_seconds()),
    );
    let response = match response {
        Ok(RpcPayload::Error { message }) => Err(anyhow!(message)),
        value => value,
    };
    response.map_err(|error| {
        let message = error.to_string();
        if message.contains("unknown variant") || message.contains("unsupported RPC protocol") {
            anyhow!("machine '{id}' does not support {name} metadata; update Memex on that peer to use this view. {message}")
        } else { error }
    })
}

pub fn remote_shell_command(machine: &MachineConfig, command: &str) -> Result<String> {
    validate_machine(machine)?;
    let target = machine
        .ssh_target()
        .ok_or_else(|| anyhow!("machine '{}' has no SSH control transport", machine.id))?;
    Ok(format!(
        "ssh -t -- {} {}",
        shell_quote(target),
        shell_quote(command)
    ))
}

pub fn run_rpc_stdio(root: Option<std::path::PathBuf>) -> Result<()> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let mut input = Vec::new();
    let mut stdin = std::io::stdin().take(MAX_RPC_REQUEST_BYTES as u64 + 1);
    stdin.read_to_end(&mut input)?;
    if input.len() > MAX_RPC_REQUEST_BYTES {
        bail!("RPC request exceeds maximum size of {MAX_RPC_REQUEST_BYTES} bytes");
    }
    let request: RpcRequest =
        serde_json::from_slice(&input).context("invalid memex RPC request")?;
    let response = if request.protocol != RPC_PROTOCOL {
        RpcPayload::Error {
            message: format!(
                "unsupported RPC protocol {}; expected {RPC_PROTOCOL}",
                request.protocol
            ),
        }
    } else {
        let report_progress = request.usage_progress
            && matches!(&request.request,
            RpcOperation::HomeActivity { request, .. } if request.metric == crate::web::ActivityMetric::Tokens);
        let action = || handle_rpc(&paths, &config, request.request);
        let result = if report_progress {
            crate::usage::with_usage_progress(action, |progress| {
                writeln!(
                    std::io::stderr(),
                    "MEMEX_PROGRESS {}",
                    serde_json::to_string(&progress)?
                )?;
                Ok(())
            })
            .and_then(|result| result)
        } else {
            action()
        };
        match result {
            Ok(response) => response,
            Err(err) => RpcPayload::Error {
                message: err.to_string(),
            },
        }
    };
    serde_json::to_writer(
        std::io::stdout(),
        &RpcResponse {
            protocol: RPC_PROTOCOL,
            response,
        },
    )?;
    Ok(())
}

fn handle_rpc(paths: &Paths, config: &UserConfig, request: RpcOperation) -> Result<RpcPayload> {
    match request {
        RpcOperation::Projects { source } => Ok(RpcPayload::Projects {
            projects: crate::cli::collect_projects(paths, source)?,
        }),
        RpcOperation::Sessions { request } => Ok(RpcPayload::Sessions {
            sessions: crate::cli::collect_sessions(
                request.session_id,
                request.source_path,
                request.cwd.map(std::path::PathBuf::from),
                request.project,
                crate::cli::parse_source_filter(request.source)?,
                request.since,
                request.limit,
                request.origin,
                Some(paths.root.clone()),
            )?,
        }),
        RpcOperation::SessionCount { request, query } => Ok(RpcPayload::SessionCount {
            count: crate::cli::collect_session_count(paths, request, query)?,
        }),
        RpcOperation::Ping => Ok(RpcPayload::Pong {
            version: env!("CARGO_PKG_VERSION").to_string(),
        }),
        RpcOperation::MemorySearch { options } => {
            options.validate()?;
            ensure_local_index(paths, config)?;
            Ok(RpcPayload::MemoryHits {
                hits: crate::memory_search::search_memory(paths, &options)?,
            })
        }
        RpcOperation::MemoryRead { request } => Ok(RpcPayload::MemoryDocument {
            document: Box::new(read_memory(paths, config, LOCAL_MACHINE_ID, &request)?),
        }),
        RpcOperation::Search { spec } => Ok(RpcPayload::Records {
            records: search_local(paths, config, &spec, true)?,
        }),
        RpcOperation::Recent {
            limit,
            project_grouping,
        } => Ok(RpcPayload::Records {
            records: recent_local(paths, config, limit, project_grouping, true)?,
        }),
        RpcOperation::Session {
            session_id,
            source_path,
        } => {
            let index = SearchIndex::open_or_create(&paths.index)?;
            let records = records_for_session(&index, &session_id, &source_path)?;
            Ok(RpcPayload::Session {
                context: SessionContext {
                    records,
                    cwd: discover_cwd(std::path::Path::new(&source_path), &session_id),
                },
            })
        }
        RpcOperation::Show { doc_id } => {
            let index = SearchIndex::open_or_create(&paths.index)?;
            let record = index
                .get_by_doc_id(doc_id)?
                .ok_or_else(|| anyhow!("doc_id not found"))?;
            Ok(RpcPayload::Record {
                record: Box::new(record),
            })
        }
        RpcOperation::ResolveRecord { selector } => {
            let index = SearchIndex::open_or_create(&paths.index)?;
            Ok(RpcPayload::Record {
                record: Box::new(resolve_record(&index, &selector)?),
            })
        }
        RpcOperation::Context { selector, options } => {
            let index = SearchIndex::open_or_create(&paths.index)?;
            Ok(RpcPayload::Context {
                context: context_records(&index, &selector, options)?,
            })
        }
        RpcOperation::ReadRecord {
            selector,
            field,
            offset_chars,
            max_chars,
        } => {
            validate_context_selector(&selector)?;
            if max_chars == 0 {
                bail!("max_chars must be greater than zero for a record read");
            }
            let index = SearchIndex::open_or_create(&paths.index)?;
            let record = resolve_record(&index, &selector)?;
            let mut budget = ReadBudget::from_remaining(Some(max_chars));
            Ok(RpcPayload::BoundedRecord {
                record: Box::new(apply_record_budget(
                    record,
                    &mut budget,
                    field,
                    offset_chars,
                )?),
            })
        }
        RpcOperation::ReadContext {
            selector,
            options,
            offset,
            max_chars,
        } => {
            validate_context_selector(&selector)?;
            let index = SearchIndex::open_or_create(&paths.index)?;
            let context = context_records(&index, &selector, options)?;
            Ok(RpcPayload::BoundedContext {
                context: apply_context_budget(context, offset, Some(max_chars))?,
            })
        }
        RpcOperation::ReadSessionPages {
            requests,
            max_chars,
        } => {
            validate_session_batch(&requests)?;
            let contexts = requests
                .iter()
                .map(|request| session_page_context_local(paths, request))
                .collect::<Result<Vec<_>>>()?;
            Ok(RpcPayload::BoundedSessionPages {
                pages: apply_session_pages_budget(contexts, Some(max_chars))?,
            })
        }
        RpcOperation::SessionPage { request } => {
            validate_session_page_request(&request)?;
            Ok(RpcPayload::SessionPage {
                context: session_page_context_local(paths, &request)?,
            })
        }
        RpcOperation::SessionBatch { requests } => {
            validate_session_batch(&requests)?;
            let contexts = requests
                .iter()
                .map(|request| session_page_context_local(paths, request))
                .collect::<Result<Vec<_>>>()?;
            Ok(RpcPayload::SessionBatch { contexts })
        }
        RpcOperation::Index => {
            let report = index_local(paths, config, false)?;
            Ok(RpcPayload::Index {
                records_added: report.records_added,
                records_embedded: report.records_embedded,
                files_scanned: report.files_scanned,
                files_skipped: report.files_skipped,
            })
        }
        RpcOperation::Usage { spec } => Ok(RpcPayload::Usage {
            report: Box::new(usage_local(paths, config, &spec)?),
        }),
        RpcOperation::UsageActivity { spec } => {
            let (points, partial) = usage_activity_local(paths, config, &spec)?;
            Ok(RpcPayload::UsageActivity { points, partial })
        }
        RpcOperation::SessionActivity { spec } => Ok(RpcPayload::SessionActivity {
            points: session_activity_local(paths, &spec)?,
        }),
        RpcOperation::HomeActivity { request, now } => Ok(RpcPayload::HomeActivity {
            activity: crate::web::raw_activity_payload(paths, &request, now)?,
        }),
    }
}

fn usage_local(paths: &Paths, config: &UserConfig, spec: &UsageSpec) -> Result<UsageReportWire> {
    if !config.token_usage_enabled() {
        bail!("token usage tracking is disabled on this machine");
    }
    let report = scan_usage(&usage_query(paths, spec)?)?;
    let details = report
        .details
        .iter()
        .map(serde_json::to_value)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(UsageReportWire {
        authority: report.authority.to_string(),
        events: report.events,
        total_tokens: report.total_tokens,
        unknown_model_events: report.unknown_model_events,
        conservative_events: report.conservative_events,
        cost_mode: report.cost_mode,
        price_catalog: report.price_catalog.to_string(),
        known_cost_usd: report.known_cost_usd,
        priced_events: report.priced_events,
        unpriced_events: report.unpriced_events,
        cache_waste: report.cache_waste.clone(),
        by_source: report.by_source.clone(),
        details,
        warnings: report.warnings.clone(),
        failures: Vec::new(),
    })
}

fn usage_activity_local(
    paths: &Paths,
    config: &UserConfig,
    spec: &UsageSpec,
) -> Result<(Vec<UsageActivityPointWire>, bool)> {
    if !config.token_usage_enabled() {
        bail!("token usage tracking is disabled on this machine");
    }
    let (points, partial) = scan_usage_activity(&usage_query(paths, spec)?)?;
    Ok((
        points
            .into_iter()
            .map(|point| UsageActivityPointWire {
                machine: LOCAL_MACHINE_ID.to_string(),
                source: point.source.to_string(),
                timestamp_ms: point.timestamp_ms,
                total_tokens: point.total_tokens,
            })
            .collect(),
        partial,
    ))
}

fn session_activity_local(
    paths: &Paths,
    spec: &SessionActivitySpec,
) -> Result<Vec<SessionActivityPointWire>> {
    let store = AnalyticsStore::open_read_only(analytics_path(&paths.state))?;
    let rows = store.query_source_timestamps_filtered(
        spec.source,
        spec.since_ms,
        spec.until_ms,
        spec.project.as_deref(),
        spec.project_grouping,
        Some(spec.kind.unwrap_or_default()),
    )?;
    Ok(rows
        .into_iter()
        .filter(|(_, timestamp_ms)| *timestamp_ms > 0)
        .map(|(source, timestamp_ms)| SessionActivityPointWire {
            machine: LOCAL_MACHINE_ID.to_string(),
            source: source.storage_label().to_string(),
            timestamp_ms,
        })
        .collect())
}

/// Session keys allowed by an origin filter, resolved against the local
/// analytics store. Keys collapse to usage-event coordinates
/// `(source label, session id)`; codex storage variants collapse via
/// `label()`, matching `scan_usage_activity`'s
/// `(event.source, session_id)` check. Returns an error when the analytics
/// store cannot be opened so callers surface it instead of silently
/// showing unfiltered totals.
pub(crate) fn usage_session_keys_for_kind(
    paths: &Paths,
    kind: SessionKindFilter,
) -> Result<HashSet<(String, String)>> {
    let store = AnalyticsStore::open_read_only(analytics_path(&paths.state))?;
    let rows =
        store.query_sessions_filtered(None, None, None, ProjectGrouping::Flat, Some(kind), None)?;
    Ok(rows
        .into_iter()
        .map(|row| (row.source.label().to_string(), row.session_id))
        .collect())
}

/// Intersects an explicit session-key filter (e.g. from a text search) with
/// the origin filter. Either side being unrestricted leaves the other side.
fn apply_kind_to_session_keys(
    session_keys: Option<HashSet<(String, String)>>,
    kind: Option<SessionKindFilter>,
    allowed: Option<HashSet<(String, String)>>,
) -> Option<HashSet<(String, String)>> {
    let kind = kind.unwrap_or(SessionKindFilter::Regular);
    if matches!(kind, SessionKindFilter::All | SessionKindFilter::Regular) {
        return session_keys;
    }
    let allowed = allowed?;
    Some(match session_keys {
        Some(keys) => keys.intersection(&allowed).cloned().collect(),
        None => allowed,
    })
}

fn usage_query(paths: &Paths, spec: &UsageSpec) -> Result<UsageQuery> {
    let session_keys: Option<HashSet<(String, String)>> = spec
        .session_keys
        .as_ref()
        .map(|keys| keys.iter().cloned().collect());
    let allowed = match spec.kind.unwrap_or(SessionKindFilter::Regular) {
        SessionKindFilter::All | SessionKindFilter::Regular => None,
        kind => Some(usage_session_keys_for_kind(paths, kind)?),
    };
    Ok(UsageQuery {
        source: spec.source,
        project: spec.project.clone(),
        project_grouping: spec.project_grouping,
        session_keys: apply_kind_to_session_keys(session_keys, spec.kind, allowed),
        since_ms: spec.since_ms,
        until_ms: spec.until_ms,
        cost_mode: spec.cost_mode,
        include_events: spec.include_events,
        include_reviews: spec.kind == Some(SessionKindFilter::All),
        cache_path: Some(paths.state.join("usage-cache.sqlite3")),
        memo_ttl_ms: spec.memo_ttl_ms,
    })
}

fn merge_usage_reports(
    reports: Vec<(String, UsageReportWire)>,
    failures: Vec<(String, String)>,
    cost_mode: CostMode,
) -> UsageReportWire {
    let multi = reports.len() + failures.len() > 1;
    let mut merged = UsageReportWire {
        authority: "multi-machine reconstructed usage (not subscription quota)".to_string(),
        events: 0,
        total_tokens: 0,
        unknown_model_events: 0,
        conservative_events: 0,
        cost_mode,
        price_catalog: reports
            .first()
            .map(|(_, report)| report.price_catalog.clone())
            .unwrap_or_default(),
        known_cost_usd: 0.0,
        priced_events: 0,
        unpriced_events: 0,
        cache_waste: CacheWaste::default(),
        by_source: Vec::new(),
        details: Vec::new(),
        warnings: Vec::new(),
        failures,
    };
    for (machine, mut report) in reports {
        merged.events = merged.events.saturating_add(report.events);
        merged.total_tokens = merged.total_tokens.saturating_add(report.total_tokens);
        merged.unknown_model_events = merged
            .unknown_model_events
            .saturating_add(report.unknown_model_events);
        merged.conservative_events = merged
            .conservative_events
            .saturating_add(report.conservative_events);
        merged.known_cost_usd += report.known_cost_usd;
        merged.priced_events = merged.priced_events.saturating_add(report.priced_events);
        merged.unpriced_events = merged
            .unpriced_events
            .saturating_add(report.unpriced_events);
        absorb_cache_waste(&mut merged.cache_waste, &report.cache_waste);
        for row in &mut report.by_source {
            if multi || machine != LOCAL_MACHINE_ID {
                row.source = format!("{machine}/{}", row.source);
            }
        }
        merged.by_source.extend(report.by_source);
        for mut detail in report.details {
            if let Some(object) = detail.as_object_mut() {
                object.insert(
                    "machine".to_string(),
                    serde_json::Value::String(machine.clone()),
                );
            }
            merged.details.push(detail);
        }
        merged.warnings.extend(
            report
                .warnings
                .into_iter()
                .map(|warning| format!("{machine}: {warning}")),
        );
    }
    for (machine, error) in &merged.failures {
        merged
            .warnings
            .push(format!("{machine}: usage unavailable: {error}"));
    }
    merged
}

fn absorb_cache_waste(total: &mut CacheWaste, row: &CacheWaste) {
    total.missed_tokens = total.missed_tokens.saturating_add(row.missed_tokens);
    total.missed_cost_usd += row.missed_cost_usd;
    total.miss_count = total.miss_count.saturating_add(row.miss_count);
    total.idle_misses = total.idle_misses.saturating_add(row.idle_misses);
    total.model_switch_misses = total
        .model_switch_misses
        .saturating_add(row.model_switch_misses);
}

pub(crate) fn resolve_vector_query_model(
    vector: &VectorIndex,
    configured_model: impl FnOnce() -> Result<ModelChoice>,
) -> Result<Option<ModelChoice>> {
    if vector.is_empty() {
        return Ok(None);
    }
    match vector.model() {
        Some(model) => Ok(Some(ModelChoice::parse(model)?)),
        None => Ok(Some(configured_model()?)),
    }
}

fn search_local(
    paths: &Paths,
    config: &UserConfig,
    spec: &SearchSpec,
    auto_index: bool,
) -> Result<Vec<(f32, Record)>> {
    crate::profiling::span!("search.local");
    if auto_index {
        ensure_local_index(paths, config)?;
    }
    let index = SearchIndex::open_or_create(&paths.index)?;
    let mut options = spec.query_options();
    if let Some(cwd) = spec.cwd.as_deref() {
        options.session_scope = Some(session_scope_for_cwd(paths, cwd)?);
    }
    let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let mut results = match spec.mode {
        SearchMode::Lexical => index.search(&options)?,
        SearchMode::Semantic => {
            let vector = match VectorIndex::open(&paths.vectors) {
                Ok(vector) => vector,
                Err(err) if err.to_string() == "vector index not found" => {
                    return lexical_results(&index, &options, spec, now_ms);
                }
                Err(err) => return Err(err),
            };
            let Some(model) = resolve_vector_query_model(&vector, || config.resolve_model(None))?
            else {
                return lexical_results(&index, &options, spec, now_ms);
            };
            let runtime = config.resolve_embed_runtime()?;
            let mut embedder = EmbedderHandle::with_model_and_runtime(model, &runtime)?;
            let embedding = embedder
                .embed_texts(&[spec.query.as_str()])?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("embedding missing"))?;
            search_filtered_records(&vector, &index, &embedding, spec.limit, &options)?
                .into_iter()
                .map(|(distance, record)| (1.0 / (1.0 + distance), record))
                .collect()
        }
        SearchMode::Hybrid => {
            let vector = match VectorIndex::open(&paths.vectors) {
                Ok(vector) => vector,
                Err(err) if err.to_string() == "vector index not found" => {
                    return lexical_results(&index, &options, spec, now_ms);
                }
                Err(err) => return Err(err),
            };
            let Some(model) = resolve_vector_query_model(&vector, || config.resolve_model(None))?
            else {
                return lexical_results(&index, &options, spec, now_ms);
            };
            let candidate_limit = (spec.limit * 5).clamp(50, 500);
            let lexical = index.search(&QueryOptions {
                limit: candidate_limit,
                ..options.clone()
            })?;
            let runtime = config.resolve_embed_runtime()?;
            let mut embedder = EmbedderHandle::with_model_and_runtime(model, &runtime)?;
            let embedding = embedder
                .embed_texts(&[spec.query.as_str()])?
                .into_iter()
                .next()
                .ok_or_else(|| anyhow!("embedding missing"))?;
            let semantic =
                search_filtered_records(&vector, &index, &embedding, candidate_limit, &options)?;
            let mut records = HashMap::new();
            let mut scores = HashMap::<u64, f32>::new();
            for (rank, (_, record)) in lexical.into_iter().enumerate() {
                if matches_filters(&record, &options) {
                    *scores.entry(record.doc_id).or_default() += 1.0 / (RRF_K + rank as f32 + 1.0);
                    records.insert(record.doc_id, record);
                }
            }
            for (rank, (_, record)) in semantic.into_iter().enumerate() {
                let doc_id = record.doc_id;
                *scores.entry(doc_id).or_default() += 1.0 / (RRF_K + rank as f32 + 1.0);
                records.entry(doc_id).or_insert(record);
            }
            scores
                .into_iter()
                .filter_map(|(doc_id, score)| records.remove(&doc_id).map(|record| (score, record)))
                .collect()
        }
    };
    for (score, record) in &mut results {
        *score = apply_recency(
            *score,
            record.ts,
            now_ms,
            spec.recency_weight,
            spec.recency_half_life_days,
        );
    }
    results.retain(|(_, record)| matches_filters(record, &options));
    if let Some(min_score) = spec.min_score {
        results.retain(|(score, _)| *score >= min_score);
    }
    results.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.1.ts.cmp(&left.1.ts))
    });
    results.truncate(spec.limit);
    apply_project_grouping(paths, &mut results, spec.project_grouping);
    Ok(results)
}

fn session_scope_for_cwd(paths: &Paths, cwd: &str) -> Result<Vec<SessionScopeKey>> {
    let db = analytics_path(&paths.state);
    if !db.exists() {
        return Ok(Vec::new());
    }
    let store = AnalyticsStore::open_read_only(db)?;
    Ok(store
        .query_sessions_detailed(None, None, Some(cwd), None, None)?
        .into_iter()
        .map(|row| SessionScopeKey {
            source: row.source,
            session_id: row.session_id,
            source_path: row.source_path,
        })
        .collect())
}

fn lexical_results(
    index: &SearchIndex,
    options: &QueryOptions,
    spec: &SearchSpec,
    now_ms: u64,
) -> Result<Vec<(f32, Record)>> {
    let mut results = index.search(options)?;
    for (score, record) in &mut results {
        *score = apply_recency(
            *score,
            record.ts,
            now_ms,
            spec.recency_weight,
            spec.recency_half_life_days,
        );
    }
    results.retain(|(_, record)| matches_filters(record, options));
    if let Some(min_score) = spec.min_score {
        results.retain(|(score, _)| *score >= min_score);
    }
    Ok(results)
}

fn recent_local(
    paths: &Paths,
    config: &UserConfig,
    limit: usize,
    project_grouping: Option<ProjectGrouping>,
    auto_index: bool,
) -> Result<Vec<(f32, Record)>> {
    if auto_index {
        ensure_local_index(paths, config)?;
    }
    let index = SearchIndex::open_or_create(&paths.index)?;
    let mut records: Vec<_> = index
        .recent_records(limit)?
        .into_iter()
        .map(|record| (0.0, record))
        .collect();
    apply_project_grouping(paths, &mut records, project_grouping);
    Ok(records)
}

fn apply_project_grouping(
    paths: &Paths,
    records: &mut [(f32, Record)],
    grouping: Option<ProjectGrouping>,
) {
    let Some(grouping) = grouping.filter(|grouping| *grouping != ProjectGrouping::Flat) else {
        return;
    };
    let Ok(store) = AnalyticsStore::open_read_only(analytics_path(&paths.state)) else {
        return;
    };
    let keys: Vec<_> = records
        .iter()
        .map(|(_, record)| {
            (
                record.source,
                record.session_id.clone(),
                record.source_path.clone(),
            )
        })
        .collect();
    let Ok(projects) = store.query_session_projects(&keys, grouping) else {
        return;
    };
    for (_, record) in records {
        let key = (
            record.source,
            record.session_id.clone(),
            record.source_path.clone(),
        );
        if let Some(project) = projects.get(&key) {
            record.project.clone_from(project);
        }
    }
}

fn ensure_local_index(paths: &Paths, config: &UserConfig) -> Result<()> {
    if config.auto_index_on_search_default() {
        let _ = index_local(paths, config, true)?;
    }
    Ok(())
}

fn index_local(paths: &Paths, config: &UserConfig, stale_only: bool) -> Result<IngestReport> {
    crate::profiling::span!("index.local");
    paths.ensure_dirs()?;
    let lease = IngestLease::acquire(paths, "RPC index", INGEST_LEASE_TIMEOUT)?;
    let index = SearchIndex::open_or_create_for_ingest(&paths.index)?;
    let options = IngestOptions {
        claude_sources: default_claude_sources(),
        include_agents: false,
        include_reasoning: config.include_reasoning_default(),
        include_codex: true,
        include_opencode: true,
        include_cursor: true,
        include_pi: true,
        include_omp: true,
        include_openclaw: true,
        include_copilot: true,
        include_grok: true,
        include_jcode: true,
        include_muse: true,
        include_antigravity: true,
        exclude_patterns: config.exclude_path_patterns(),
        embeddings: config.embeddings_default(),
        backfill_embeddings: false,
        model: config.resolve_model(None)?,
        embed_runtime: config.resolve_embed_runtime()?,
        tool_content_limits: config.indexed_tool_content_limits()?,
    };
    if stale_only {
        Ok(
            ingest_if_stale(paths, &index, &options, config.scan_cache_ttl(), &lease)?.unwrap_or(
                IngestReport {
                    records_added: 0,
                    records_embedded: 0,
                    files_scanned: 0,
                    files_skipped: 0,
                    diagnostics: Default::default(),
                },
            ),
        )
    } else {
        ingest_all(paths, &index, &options, &lease)
    }
}

fn records_for_session(
    index: &SearchIndex,
    session_id: &str,
    source_path: &str,
) -> Result<Vec<Record>> {
    let mut records = index.records_by_session_id(session_id)?;
    if !source_path.is_empty() {
        records.retain(|record| record.source_path == source_path);
    }
    records.sort_by(|left, right| {
        left.turn_id
            .cmp(&right.turn_id)
            .then_with(|| left.ts.cmp(&right.ts))
            .then_with(|| left.doc_id.cmp(&right.doc_id))
    });
    Ok(records)
}

fn records_for_session_page(
    index: &SearchIndex,
    request: &SessionPageRequest,
) -> Result<(Vec<Record>, usize)> {
    index.records_by_session_path_page(
        &request.session_id,
        (!request.source_path.is_empty()).then_some(request.source_path.as_str()),
        request.offset,
        request.limit,
    )
}

fn discover_cwd(path: &std::path::Path, session_id: &str) -> Option<String> {
    let file = std::fs::File::open(path).ok()?;
    let reader = std::io::BufReader::new(file);
    let mut fallback = None;
    for line in std::io::BufRead::lines(reader).map_while(Result::ok) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        let cwd = value
            .get("cwd")
            .and_then(|value| value.as_str())
            .or_else(|| {
                value
                    .get("payload")
                    .and_then(|payload| payload.get("cwd"))
                    .and_then(|value| value.as_str())
            })
            .map(str::to_string);
        if fallback.is_none() {
            fallback.clone_from(&cwd);
        }
        let matches_session = value
            .get("sessionId")
            .and_then(|value| value.as_str())
            .or_else(|| value.get("session_id").and_then(|value| value.as_str()))
            .is_some_and(|id| id == session_id);
        if matches_session && cwd.is_some() {
            return cwd;
        }
        if value.get("type").and_then(|value| value.as_str()) == Some("session_meta")
            && cwd.is_some()
        {
            return cwd;
        }
    }
    fallback
}

fn rpc_records(
    machine: &MachineConfig,
    operation: RpcOperation,
    timeout: Duration,
    context: &str,
) -> Result<Vec<(f32, Record)>> {
    match rpc(machine, operation, timeout)? {
        RpcPayload::Records { records } => Ok(records),
        RpcPayload::Error { message } => Err(anyhow!("{context} failed: {message}")),
        other => Err(anyhow!("{context} returned unexpected response: {other:?}")),
    }
}

fn rpc(machine: &MachineConfig, operation: RpcOperation, timeout: Duration) -> Result<RpcPayload> {
    validate_machine(machine)?;
    if !machine.uses_remote_index() {
        bail!(
            "machine '{}' uses unsupported index backend '{}'",
            machine.id,
            machine
                .index
                .as_ref()
                .map(|index| index.kind.as_str())
                .unwrap_or("unknown")
        );
    }
    let target = machine
        .ssh_target()
        .ok_or_else(|| anyhow!("machine '{}' has no SSH control transport", machine.id))?;
    let command = format!("{} rpc", machine.command());
    let mut child = Command::new("ssh")
        .args(["-T", "-o", "BatchMode=yes", "--", target, &command])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to start SSH for '{}'", machine.id))?;
    let usage_progress = matches!(&operation,
        RpcOperation::HomeActivity { request, .. } if request.metric == crate::web::ActivityMetric::Tokens);
    let request = serde_json::to_vec(&RpcRequest {
        protocol: RPC_PROTOCOL,
        request: operation,
        usage_progress,
    })?;
    child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("missing SSH stdin"))?
        .write_all(&request)?;

    let (status, stdout, stderr) = wait_rpc_child(
        &mut child,
        &machine.id,
        timeout,
        usage_progress,
        crate::usage::publish_remote_scan_progress,
    )?;
    if !status.success() {
        let message = String::from_utf8_lossy(&stderr).trim().to_string();
        bail!(
            "SSH request to '{}' exited with {status}: {}",
            machine.id,
            if message.is_empty() {
                "no error output"
            } else {
                &message
            }
        );
    }
    let response: RpcResponse = serde_json::from_slice(&stdout)
        .with_context(|| format!("invalid RPC response from '{}'", machine.id))?;
    if response.protocol != RPC_PROTOCOL {
        bail!(
            "machine '{}' uses RPC protocol {}; expected {RPC_PROTOCOL}",
            machine.id,
            response.protocol
        );
    }
    Ok(response.response)
}

#[derive(Debug, Deserialize)]
struct RpcUsageProgress {
    source: String,
    done: usize,
    total: usize,
}

fn wait_rpc_child(
    child: &mut std::process::Child,
    machine_id: &str,
    timeout: Duration,
    usage_progress: bool,
    mut publish: impl FnMut(crate::usage::UsageScanProgress),
) -> Result<(std::process::ExitStatus, Vec<u8>, Vec<u8>)> {
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("missing SSH stdout"))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("missing SSH stderr"))?;
    let stdout_thread = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).map(|_| bytes)
    });
    let (progress_sender, progress_receiver) = std::sync::mpsc::sync_channel(64);
    let stderr_thread = std::thread::spawn(move || -> std::io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        let mut line = Vec::new();
        let mut discard_line = false;
        let mut buffer = [0; 8192];
        loop {
            let count = stderr.read(&mut buffer)?;
            if count == 0 {
                return Ok(bytes);
            }
            if !usage_progress {
                bytes.extend_from_slice(&buffer[..count]);
                continue;
            }
            // Keep the final diagnostic even after a long stream of progress.
            let excess = (bytes.len() + count).saturating_sub(65_536);
            bytes.drain(..excess);
            bytes.extend_from_slice(&buffer[..count]);
            for &byte in &buffer[..count] {
                if byte == b'\n' {
                    if !discard_line
                        && let Some(json) = line.strip_prefix(b"MEMEX_PROGRESS ")
                        && let Ok(progress) = serde_json::from_slice::<RpcUsageProgress>(json)
                    {
                        let _ = progress_sender.try_send((Instant::now(), progress));
                    }
                    line.clear();
                    discard_line = false;
                } else if !discard_line {
                    if line.len() == 65_536 {
                        line.clear();
                        discard_line = true;
                    } else {
                        line.push(byte);
                    }
                }
            }
        }
    });

    let mut deadline = Instant::now() + timeout;
    let mut completed = HashMap::new();
    let status = loop {
        for (received_at, progress) in progress_receiver.try_iter() {
            let Some(source) = crate::types::SourceKind::from_label(&progress.source) else {
                continue;
            };
            if progress.total == 0 || progress.done == 0 || progress.done > progress.total {
                continue;
            }
            let previous = completed.entry(source.label()).or_insert(0);
            if progress.done <= *previous {
                continue;
            }
            *previous = progress.done;
            deadline = received_at + timeout;
            publish(crate::usage::UsageScanProgress {
                source: source.label(),
                done: progress.done,
                total: progress.total,
            });
        }
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!(
                "SSH request to '{}' timed out after {}s",
                machine_id,
                timeout.as_secs()
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let stdout = stdout_thread
        .join()
        .map_err(|_| anyhow!("SSH stdout reader panicked"))??;
    let stderr = stderr_thread
        .join()
        .map_err(|_| anyhow!("SSH stderr reader panicked"))??;
    Ok((status, stdout, stderr))
}

fn validate_machine(machine: &MachineConfig) -> Result<()> {
    if machine.id.is_empty()
        || machine.id == LOCAL_MACHINE_ID
        || !machine
            .id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("invalid machine id '{}'", machine.id);
    }
    let target = machine
        .ssh_target()
        .ok_or_else(|| anyhow!("machine '{}' has no SSH control transport", machine.id))?;
    if target.starts_with('-') || target.is_empty() || target.chars().any(char::is_whitespace) {
        bail!("machine '{}' has an unsafe SSH target", machine.id);
    }
    let command = machine.command();
    if command.starts_with('-')
        || command.is_empty()
        || !command.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-' | b'~')
        })
    {
        bail!("machine '{}' has an unsafe command", machine.id);
    }
    Ok(())
}

fn matches_filters(record: &Record, options: &QueryOptions) -> bool {
    options
        .project
        .as_ref()
        .is_none_or(|project| record.project == *project)
        && options
            .role
            .as_ref()
            .is_none_or(|role| record.role == *role)
        && options
            .tool
            .as_ref()
            .is_none_or(|tool| record.tool_name.as_deref() == Some(tool.as_str()))
        && options
            .session_id
            .as_ref()
            .is_none_or(|session| record.session_id == *session)
        && options.session_scope.as_ref().is_none_or(|scope| {
            scope.iter().any(|key| {
                key.source == record.source
                    && key.session_id == record.session_id
                    && key.source_path == record.source_path
            })
        })
        && options
            .source
            .is_none_or(|source| source.matches(record.source))
        && options.since.is_none_or(|since| record.ts >= since)
        && options.until.is_none_or(|until| record.ts <= until)
}

fn apply_recency(score: f32, ts: u64, now_ms: u64, weight: f32, half_life_days: f32) -> f32 {
    if score <= 0.0 || weight <= 0.0 || half_life_days <= 0.0 || ts == 0 {
        return score;
    }
    let age_ms = now_ms.saturating_sub(ts);
    let age_days = age_ms as f32 / (1000.0 * 60.0 * 60.0 * 24.0);
    let decay = (-std::f32::consts::LN_2 * age_days / half_life_days).exp();
    score * (1.0 + weight * decay)
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::AnalyticsWriter;
    use crate::config::{ControlConfig, IndexBackendConfig, MultiMachineConfig};
    use crate::types::{RecordLinks, SourceKind};
    use tempfile::TempDir;

    fn activity_progress_child(script: &str) -> std::process::Child {
        Command::new("sh")
            .args(["-c", script])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    }

    #[test]
    fn activity_rpc_progress_extends_idle_deadline_and_preserves_output() {
        let mut child = activity_progress_child(
            r#"
            for done in 1 2 3 4 5; do
                printf 'MEMEX_PROGRESS {"source":"codex","done":%s,"total":5}\n' "$done" >&2
                sleep 0.1
            done
            printf 'complete'
            printf 'diagnostic\n' >&2
        "#,
        );
        let started = Instant::now();
        let mut updates = Vec::new();
        let (status, output, stderr) = wait_rpc_child(
            &mut child,
            "fixture",
            Duration::from_millis(250),
            true,
            |progress| updates.push(progress.done),
        )
        .unwrap();
        assert!(status.success());
        assert!(started.elapsed() > Duration::from_millis(250));
        assert_eq!(output, b"complete");
        assert!(String::from_utf8(stderr).unwrap().contains("diagnostic"));
        assert_eq!(updates, [1, 2, 3, 4, 5]);
    }

    #[test]
    fn activity_rpc_nonadvancing_or_unrequested_progress_times_out_and_reaps_child() {
        for (progress, enabled) in [
            (r#"{"source":"codex","done":0,"total":5}"#, true),
            (r#"{"source":"codex","done":1,"total":5}"#, true),
            (r#"{"source":"codex","done":4,"total":3}"#, true),
            (r#"{"source":"codex","done":-1,"total":3}"#, true),
            (r#"{"source":"codex","done":1,"total":0}"#, true),
            (r#"{"source":"unknown","done":1,"total":3}"#, true),
            ("invalid JSON", true),
            (r#"{"source":"codex","done":2,"total":5}"#, false),
        ] {
            let script = format!(
                "while :; do printf '%s\\n' 'MEMEX_PROGRESS {progress}' >&2; sleep 0.05; done"
            );
            let mut child = activity_progress_child(&script);
            let started = Instant::now();
            let result = wait_rpc_child(
                &mut child,
                "fixture",
                Duration::from_millis(200),
                enabled,
                |_| {},
            );
            assert!(result.unwrap_err().to_string().contains("timed out"));
            assert!(started.elapsed() < Duration::from_secs(2));
            assert!(child.try_wait().unwrap().is_some());
        }
        let mut child = activity_progress_child(
            r#"
            printf 'MEMEX_PROGRESS {"source":"codex","done":3,"total":5}\n' >&2
            sleep 0.1
            while :; do printf 'MEMEX_PROGRESS {"source":"codex","done":2,"total":5}\n' >&2; sleep 0.05; done
        "#,
        );
        assert!(
            wait_rpc_child(
                &mut child,
                "fixture",
                Duration::from_millis(200),
                true,
                |_| {}
            )
            .is_err()
        );
        assert!(child.try_wait().unwrap().is_some());
    }

    #[test]
    fn activity_rpc_progress_discards_overlong_lines_and_bounds_diagnostics() {
        let mut child = activity_progress_child(
            r#"
            awk 'BEGIN { for (i=0; i<70000; i++) printf "x"; print "" }' >&2
            printf 'MEMEX_PROGRESS {"source":"codex","done":1,"total":1}\n' >&2
            sleep 0.1
            printf 'complete'
            printf 'final SSH failure detail\n' >&2
        "#,
        );
        let mut updates = Vec::new();
        let (_, output, stderr) = wait_rpc_child(
            &mut child,
            "fixture",
            Duration::from_secs(2),
            true,
            |progress| updates.push(progress.done),
        )
        .unwrap();
        assert_eq!(output, b"complete");
        assert_eq!(stderr.len(), 65_536);
        assert!(
            String::from_utf8(stderr)
                .unwrap()
                .ends_with("final SSH failure detail\n")
        );
        assert_eq!(updates, [1]);
    }

    #[test]
    fn activity_rpc_progress_is_optional_for_existing_requests() {
        let request: RpcRequest =
            serde_json::from_value(serde_json::json!({"protocol": 1, "request": {"op": "ping"}}))
                .unwrap();
        assert!(!request.usage_progress);
        assert!(
            serde_json::to_value(request)
                .unwrap()
                .get("usage_progress")
                .is_none()
        );
    }

    #[test]
    fn legacy_home_activity_preserves_every_nonquery_filter() {
        use crate::web::{ActivityMetric, ActivityRequest, TimeRange};
        let now = 200 * 86_400_000;
        for metric in [ActivityMetric::Sessions, ActivityMetric::Tokens] {
            for origin in [
                SessionKindFilter::All,
                SessionKindFilter::Regular,
                SessionKindFilter::Primary,
                SessionKindFilter::Subagent,
            ] {
                for range in [
                    None,
                    Some(TimeRange::Day),
                    Some(TimeRange::Week),
                    Some(TimeRange::Month),
                    Some(TimeRange::All),
                ] {
                    for filtered in [false, true] {
                        let request = ActivityRequest {
                            metric,
                            query: String::new(),
                            source: filtered.then_some(SourceFilter::Codex),
                            project: filtered.then(|| "memex".to_string()),
                            days: 10,
                            range,
                            origin,
                        };
                        let mut calls = 0;
                        let payload = remote_activity_with(&request, now, |operation| {
                            calls += 1;
                            match operation {
                                RpcOperation::HomeActivity { .. } => {
                                    Err(anyhow!("unknown variant `home_activity`"))
                                }
                                RpcOperation::SessionActivity { spec } => {
                                    assert!(matches!(metric, ActivityMetric::Sessions));
                                    assert_eq!(spec.source, request.source);
                                    assert_eq!(spec.project, request.project);
                                    assert_eq!(spec.kind, Some(origin));
                                    assert_eq!(spec.project_grouping, ProjectGrouping::Flat);
                                    assert_eq!(spec.until_ms, None);
                                    assert_eq!(
                                        spec.since_ms,
                                        range
                                            .map(|value| value.since_ms(now))
                                            .unwrap_or(Some(now - 10 * 86_400_000))
                                    );
                                    Ok(RpcPayload::SessionActivity {
                                        points: vec![SessionActivityPointWire {
                                            machine: "peer".into(),
                                            source: "codex".into(),
                                            timestamp_ms: now,
                                        }],
                                    })
                                }
                                RpcOperation::UsageActivity { spec } => {
                                    assert!(matches!(metric, ActivityMetric::Tokens));
                                    assert_eq!(spec.source, request.source);
                                    assert_eq!(spec.project, request.project);
                                    assert_eq!(spec.kind, Some(origin));
                                    assert_eq!(spec.project_grouping, ProjectGrouping::Flat);
                                    assert_eq!(spec.until_ms, None);
                                    assert_eq!(
                                        spec.since_ms,
                                        range
                                            .map(|value| value.since_ms(now))
                                            .unwrap_or(Some(now - 10 * 86_400_000))
                                    );
                                    assert!(spec.session_keys.is_none());
                                    assert!(!spec.include_events);
                                    Ok(RpcPayload::UsageActivity {
                                        points: vec![UsageActivityPointWire {
                                            machine: "peer".into(),
                                            source: "codex".into(),
                                            timestamp_ms: now,
                                            total_tokens: 42,
                                        }],
                                        partial: true,
                                    })
                                }
                                _ => panic!("unexpected legacy operation"),
                            }
                        })
                        .unwrap();
                        assert_eq!(calls, 2);
                        let json = serde_json::to_value(payload).unwrap();
                        assert_eq!(
                            json["points"][0]["value"],
                            if matches!(metric, ActivityMetric::Tokens) {
                                42
                            } else {
                                1
                            }
                        );
                        assert_eq!(json["partial"], matches!(metric, ActivityMetric::Tokens));
                    }
                }
            }
        }
    }

    #[test]
    fn legacy_home_activity_never_ignores_query_or_other_errors() {
        use crate::web::{ActivityMetric, ActivityRequest, TimeRange};
        for metric in [ActivityMetric::Sessions, ActivityMetric::Tokens] {
            let mut request = ActivityRequest {
                metric,
                query: "exact search".into(),
                source: None,
                project: None,
                days: 30,
                range: Some(TimeRange::All),
                origin: SessionKindFilter::All,
            };
            let mut calls = 0;
            let error = remote_activity_with(&request, 100, |_| {
                calls += 1;
                Err(anyhow!("unknown variant `home_activity`"))
            })
            .unwrap_err();
            assert_eq!(calls, 1);
            assert!(error.to_string().contains("activity filtered by search"));
            request.query.clear();
            for message in [
                "connection timed out",
                "invalid activity data",
                "unknown variant `another_operation`",
                "unsupported RPC protocol",
            ] {
                let mut calls = 0;
                let error = remote_activity_with(&request, 100, |_| {
                    calls += 1;
                    Err(anyhow!(message.to_string()))
                })
                .unwrap_err();
                assert_eq!(calls, 1);
                assert_eq!(error.to_string(), message);
            }
        }
    }

    fn machine(id: &str) -> MachineConfig {
        MachineConfig {
            id: id.to_string(),
            label: None,
            ssh: None,
            command: None,
            enabled: None,
            control: Some(ControlConfig {
                kind: "ssh".to_string(),
                host: "mini".to_string(),
            }),
            index: Some(IndexBackendConfig {
                kind: "remote".to_string(),
                bucket: None,
                prefix: None,
                cache: None,
            }),
        }
    }

    fn search_spec(mode: SearchMode) -> SearchSpec {
        SearchSpec {
            query: "query readiness".to_string(),
            project: None,
            role: None,
            tool: None,
            session_id: None,
            session_scope: None,
            cwd: None,
            source: None,
            since: None,
            until: None,
            limit: 10,
            mode,
            recency_weight: 0.0,
            recency_half_life_days: 30.0,
            min_score: None,
            project_grouping: None,
        }
    }

    fn test_record(doc_id: u64, session_id: &str, source_path: &str, turn_id: u32) -> Record {
        Record {
            source: SourceKind::Codex,
            doc_id,
            ts: doc_id,
            project: "memex".to_string(),
            session_id: session_id.to_string(),
            turn_id,
            role: "assistant".to_string(),
            text: format!("record {doc_id}"),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks::default(),
            source_path: source_path.to_string(),
        }
    }

    fn write_test_index(paths: &Paths, records: &[Record]) {
        paths.ensure_dirs().unwrap();
        let index = SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
        let mut writer = index.writer().unwrap();
        for record in records {
            index.add_record(&mut writer, record).unwrap();
        }
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        index.publish_generation().unwrap();
    }

    fn rpc_handler_round_trip(
        paths: &Paths,
        config: &UserConfig,
        operation: RpcOperation,
    ) -> RpcPayload {
        let request = serde_json::to_vec(&RpcRequest {
            protocol: RPC_PROTOCOL,
            request: operation,
            usage_progress: false,
        })
        .unwrap();
        let request: RpcRequest = serde_json::from_slice(&request).unwrap();
        let response = handle_rpc(paths, config, request.request).unwrap();
        let response = serde_json::to_vec(&RpcResponse {
            protocol: RPC_PROTOCOL,
            response,
        })
        .unwrap();
        serde_json::from_slice::<RpcResponse>(&response)
            .unwrap()
            .response
    }

    #[test]
    fn vector_query_model_prefers_metadata_and_uses_configured_fallback() {
        let tmp = TempDir::new().unwrap();

        let mut with_metadata =
            VectorIndex::open_or_create(&tmp.path().join("with-metadata"), 64, Some("bge"))
                .unwrap();
        with_metadata.add(1, &[0.0; 64]).unwrap();
        let selected = resolve_vector_query_model(&with_metadata, || {
            Err(anyhow!("configured fallback should not be resolved"))
        })
        .unwrap();
        assert_eq!(selected, Some(ModelChoice::BGESmall));

        let mut without_metadata =
            VectorIndex::open_or_create(&tmp.path().join("without-metadata"), 64, None).unwrap();
        without_metadata.add(1, &[0.0; 64]).unwrap();
        let selected =
            resolve_vector_query_model(&without_metadata, || Ok(ModelChoice::MiniLM)).unwrap();
        assert_eq!(selected, Some(ModelChoice::MiniLM));
    }

    #[test]
    fn empty_vector_index_falls_back_for_federated_semantic_and_hybrid_search() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();

        let index = SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
        let mut writer = index.writer().unwrap();
        index
            .add_record(
                &mut writer,
                &Record {
                    source: SourceKind::Codex,
                    doc_id: 1,
                    ts: 1,
                    project: "memex".to_string(),
                    session_id: "session".to_string(),
                    turn_id: 1,
                    role: "assistant".to_string(),
                    text: "query readiness".to_string(),
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: RecordLinks::default(),
                    source_path: "session.jsonl".to_string(),
                },
            )
            .unwrap();
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        index.publish_generation().unwrap();

        let vector = VectorIndex::open_or_create(&paths.vectors, 64, Some("bge")).unwrap();
        assert_eq!(
            resolve_vector_query_model(&vector, || {
                Err(anyhow!("configured fallback should not be resolved"))
            })
            .unwrap(),
            None
        );
        vector.save().unwrap();

        for mode in [SearchMode::Semantic, SearchMode::Hybrid] {
            let result = federated_search(
                &paths,
                &UserConfig::default(),
                &[LOCAL_MACHINE_ID.to_string()],
                &search_spec(mode),
                false,
            )
            .unwrap();

            assert!(result.failures.is_empty());
            assert_eq!(result.items.len(), 1);
            assert_eq!(result.items[0].record.doc_id, 1);
        }
    }

    #[test]
    fn local_show_page_and_batch_hydration_preserve_machine_selectors() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        let records = vec![
            test_record(1, "session", "a.jsonl", 1),
            test_record(2, "session", "a.jsonl", 2),
            test_record(3, "session", "b.jsonl", 3),
        ];
        write_test_index(&paths, &records);

        let config = UserConfig::default();
        assert_eq!(
            record_by_doc_id(&paths, &config, LOCAL_MACHINE_ID, 2)
                .unwrap()
                .source_path,
            "a.jsonl"
        );

        let page = session_page_context(
            &paths,
            &config,
            LOCAL_MACHINE_ID,
            &SessionPageRequest {
                session_id: "session".to_string(),
                source_path: "a.jsonl".to_string(),
                offset: 1,
                limit: 1,
            },
        )
        .unwrap();
        assert_eq!(page.total, 2);
        assert_eq!(
            page.records
                .iter()
                .map(|record| record.doc_id)
                .collect::<Vec<_>>(),
            [2]
        );
        assert_eq!(page.next_offset, None);

        let contexts = batch_session_contexts(
            &paths,
            &config,
            LOCAL_MACHINE_ID,
            &[
                SessionPageRequest {
                    session_id: "session".to_string(),
                    source_path: "a.jsonl".to_string(),
                    offset: 0,
                    limit: 1,
                },
                SessionPageRequest {
                    session_id: "session".to_string(),
                    source_path: "b.jsonl".to_string(),
                    offset: 0,
                    limit: 1,
                },
            ],
        )
        .unwrap();
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[0].records[0].doc_id, 1);
        assert_eq!(contexts[1].records[0].doc_id, 3);
    }

    #[test]
    fn bounded_rpc_handlers_apply_budgets_before_round_trip() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        let records = vec![
            test_record(1, "session", "a.jsonl", 1),
            test_record(2, "session", "a.jsonl", 2),
            test_record(3, "session", "b.jsonl", 3),
        ];
        write_test_index(&paths, &records);
        let config = UserConfig::default();

        let selector = ContextSelector::doc_id(2);
        let RpcPayload::BoundedRecord { record } = rpc_handler_round_trip(
            &paths,
            &config,
            RpcOperation::ReadRecord {
                selector: selector.clone(),
                field: Some(ReadField::Text),
                offset_chars: 1,
                max_chars: 2,
            },
        ) else {
            panic!("unexpected bounded-record response");
        };
        assert_eq!(record.record.text, "ec");
        assert_eq!(record.content.returned_chars, 2);
        assert_eq!(record.content.total_chars, 8);
        assert_eq!(record.content.continuations[0].offset_chars, 3);
        validate_bounded_record(&record, &selector, Some(2)).unwrap();

        let RpcPayload::BoundedContext { context } = rpc_handler_round_trip(
            &paths,
            &config,
            RpcOperation::ReadContext {
                selector: selector.clone(),
                options: ContextOptions {
                    before: 1,
                    after: 0,
                    expand_interactions: false,
                },
                offset: 0,
                max_chars: 3,
            },
        ) else {
            panic!("unexpected bounded-context response");
        };
        assert_eq!(context.order, "anchor_first");
        assert_eq!(context.records.len(), 1);
        assert_eq!(context.records[0].relation, ContextRelation::Anchor);
        assert_eq!(context.records[0].record.text, "rec");
        validate_bounded_context(&context, &selector, 0, Some(3)).unwrap();

        let requests = vec![
            SessionPageRequest {
                session_id: "session".to_string(),
                source_path: "a.jsonl".to_string(),
                offset: 0,
                limit: 1,
            },
            SessionPageRequest {
                session_id: "session".to_string(),
                source_path: "b.jsonl".to_string(),
                offset: 0,
                limit: 1,
            },
        ];
        let RpcPayload::BoundedSessionPages { pages } = rpc_handler_round_trip(
            &paths,
            &config,
            RpcOperation::ReadSessionPages {
                requests: requests.clone(),
                max_chars: 10,
            },
        ) else {
            panic!("unexpected bounded-session response");
        };
        assert_eq!(pages[0].records[0].content.returned_chars, 8);
        assert_eq!(pages[1].records[0].content.returned_chars, 2);
        validate_bounded_session_pages(&pages, &requests, Some(10)).unwrap();

        let RpcPayload::BoundedSessionPages { pages } = rpc_handler_round_trip(
            &paths,
            &config,
            RpcOperation::ReadSessionPages {
                requests: requests.clone(),
                max_chars: 0,
            },
        ) else {
            panic!("unexpected exhausted-session response");
        };
        assert!(pages.iter().all(|page| page.records.is_empty()));
        assert!(pages.iter().all(|page| page.next_offset == Some(0)));
        validate_bounded_session_pages(&pages, &requests, Some(0)).unwrap();
    }

    #[test]
    fn bounded_response_validation_rejects_identity_and_scope_mismatches() {
        let selector = ContextSelector::doc_id(1);
        let record = test_record(1, "session", "source.jsonl", 1);
        let mut budget = ReadBudget::from_remaining(Some(4));
        let mut bounded = apply_record_budget(record, &mut budget, None, 0).unwrap();

        bounded.record_id = "rid1_wrong".to_string();
        assert!(validate_bounded_record(&bounded, &selector, Some(4)).is_err());

        bounded.record_id = canonical_record_id(&bounded.record);
        bounded.record.doc_id = 2;
        assert!(validate_bounded_record(&bounded, &selector, Some(4)).is_err());

        let request = SessionPageRequest {
            session_id: "session".to_string(),
            source_path: "source.jsonl".to_string(),
            offset: 0,
            limit: 1,
        };
        bounded.record.doc_id = 1;
        bounded.record_id = canonical_record_id(&bounded.record);
        bounded.record.session_id = "other".to_string();
        let page = BoundedSessionPage {
            session_id: request.session_id.clone(),
            source_path: request.source_path.clone(),
            cwd: None,
            offset: 0,
            total: 1,
            next_offset: None,
            records: vec![bounded],
        };
        assert!(validate_bounded_session_pages(&[page], &[request], Some(4)).is_err());
    }

    #[test]
    fn session_hydration_limits_are_enforced_before_rpc() {
        let request = SessionPageRequest {
            session_id: "session".to_string(),
            source_path: String::new(),
            offset: 0,
            limit: MAX_SESSION_PAGE_SIZE + 1,
        };
        assert!(validate_session_page_request(&request).is_err());

        let requests = vec![
            SessionPageRequest {
                limit: 1,
                ..request.clone()
            };
            MAX_SESSION_BATCH_SIZE + 1
        ];
        assert!(validate_session_batch(&requests).is_err());
    }

    #[test]
    fn session_hydration_validation_rejects_empty_ids_and_inconsistent_pages() {
        assert!(
            validate_session_page_request(&SessionPageRequest {
                session_id: String::new(),
                source_path: String::new(),
                offset: 0,
                limit: 1,
            })
            .is_err()
        );

        let request = SessionPageRequest {
            session_id: "session".to_string(),
            source_path: String::new(),
            offset: 1,
            limit: 2,
        };
        let records = vec![test_record(1, "session", "source", 1)];

        assert!(
            validate_session_page_context(
                &SessionPageContext {
                    session_id: request.session_id.clone(),
                    source_path: request.source_path.clone(),
                    records: records.clone(),
                    cwd: None,
                    offset: request.offset,
                    total: 4,
                    next_offset: Some(4),
                },
                &request,
            )
            .is_err()
        );

        assert!(
            validate_session_page_context(
                &SessionPageContext {
                    session_id: request.session_id.clone(),
                    source_path: request.source_path.clone(),
                    records,
                    cwd: None,
                    offset: 5,
                    total: 4,
                    next_offset: Some(6),
                },
                &request,
            )
            .is_err()
        );

        let valid = SessionPageContext {
            session_id: request.session_id.clone(),
            source_path: request.source_path.clone(),
            records: vec![test_record(1, "session", "source", 1)],
            cwd: None,
            offset: 1,
            total: 4,
            next_offset: Some(2),
        };
        assert!(validate_session_page_context(&valid, &request).is_ok());
    }

    #[test]
    fn record_filters_enforce_exact_session_scope() {
        let record = test_record(1, "session", "source.jsonl", 1);
        let mut options = search_spec(SearchMode::Semantic).query_options();
        options.session_scope = Some(vec![SessionScopeKey {
            source: record.source,
            session_id: record.session_id.clone(),
            source_path: record.source_path.clone(),
        }]);
        assert!(matches_filters(&record, &options));

        options.session_scope = Some(Vec::new());
        assert!(!matches_filters(&record, &options));

        options.session_scope = Some(vec![SessionScopeKey {
            source: record.source,
            session_id: record.session_id.clone(),
            source_path: "other.jsonl".to_string(),
        }]);
        assert!(!matches_filters(&record, &options));
    }

    #[test]
    fn filtered_vector_search_deepens_past_stale_and_filtered_records() {
        let temp = TempDir::new().unwrap();
        let index_path = temp.path().join("index");
        std::fs::create_dir(&index_path).unwrap();
        let index = SearchIndex::open_or_create_for_ingest(&index_path).unwrap();
        let mut writer = index.writer().unwrap();
        let mut filtered = test_record(2, "session", "source.jsonl", 2);
        filtered.project = "filtered".to_string();
        filtered.tool_name = Some("Read".to_string());
        let mut wanted = test_record(3, "session", "source.jsonl", 3);
        wanted.tool_name = Some("Read".to_string());
        for record in [filtered, wanted] {
            index.add_record(&mut writer, &record).unwrap();
        }
        writer.commit().unwrap();
        drop(writer);

        let query = [1.0, 0.0, 0.0, 0.0];
        let mut vector =
            VectorIndex::open_or_create(&temp.path().join("vectors"), 4, Some("test")).unwrap();
        vector.add(1, &query).unwrap();
        vector.add(2, &[1.0, 0.1, 0.0, 0.0]).unwrap();
        vector.add(3, &[0.0, 1.0, 0.0, 0.0]).unwrap();

        let mut spec = search_spec(SearchMode::Semantic);
        spec.project = Some("memex".to_string());
        spec.role = Some("assistant".to_string());
        spec.tool = Some("Read".to_string());
        spec.session_id = Some("session".to_string());
        spec.session_scope = Some(vec![SessionScopeKey {
            source: SourceKind::Codex,
            session_id: "session".to_string(),
            source_path: "source.jsonl".to_string(),
        }]);
        spec.source = Some(SourceFilter::Codex);
        spec.since = Some(2);
        spec.until = Some(4);
        spec.limit = 1;
        let results =
            search_filtered_records(&vector, &index, &query, 1, &spec.query_options()).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.doc_id, 3);
    }

    #[test]
    fn selection_defaults_to_local_and_enabled_machines() {
        let config = UserConfig {
            machines: vec![machine("mini")],
            ..UserConfig::default()
        };
        assert_eq!(
            selected_machine_ids(&config, &[]).unwrap(),
            vec!["local", "mini"]
        );
    }

    #[test]
    fn usage_session_keys_are_partitioned_by_machine() {
        let spec = UsageSpec {
            source: None,
            project: None,
            project_grouping: ProjectGrouping::Flat,
            session_keys: None,
            machine_session_keys: Some(vec![
                ("local".into(), "codex".into(), "shared".into()),
                ("mini".into(), "claude".into(), "shared".into()),
            ]),
            since_ms: None,
            until_ms: None,
            cost_mode: CostMode::Source,
            include_events: false,
            memo_ttl_ms: 0,
            kind: None,
        };

        let local = usage_spec_for_machine(&spec, "local");
        let mini = usage_spec_for_machine(&spec, "mini");
        let other = usage_spec_for_machine(&spec, "other");

        assert_eq!(
            local.session_keys,
            Some(vec![("codex".into(), "shared".into())])
        );
        assert_eq!(
            mini.session_keys,
            Some(vec![("claude".into(), "shared".into())])
        );
        assert_eq!(other.session_keys, Some(Vec::new()));
        assert!(local.machine_session_keys.is_none());
        assert!(mini.machine_session_keys.is_none());
    }

    #[test]
    fn default_usage_review_filter_does_not_require_indexed_sessions() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        let mut spec = UsageSpec {
            source: None,
            project: None,
            project_grouping: ProjectGrouping::Flat,
            session_keys: None,
            machine_session_keys: None,
            since_ms: None,
            until_ms: None,
            cost_mode: CostMode::Source,
            include_events: false,
            memo_ttl_ms: 0,
            kind: None,
        };
        for kind in [
            None,
            Some(SessionKindFilter::Regular),
            Some(SessionKindFilter::All),
        ] {
            spec.kind = kind;
            let query = usage_query(&paths, &spec).expect("query without analytics database");
            assert!(query.session_keys.is_none());
            assert_eq!(query.include_reviews, kind == Some(SessionKindFilter::All));
        }
        spec.session_keys = Some(vec![("codex".into(), "unindexed".into())]);
        spec.kind = Some(SessionKindFilter::Regular);
        assert_eq!(
            usage_query(&paths, &spec).unwrap().session_keys,
            Some(HashSet::from([("codex".into(), "unindexed".into())]))
        );
    }

    #[test]
    fn usage_query_resolves_origin_filter_against_analytics() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let mut analytics =
            AnalyticsWriter::open(analytics_path(&paths.state)).expect("analytics writer");
        for (session_id, kind) in [("main-ses", None), ("sub-ses", Some("subagent"))] {
            analytics
                .record(&Record {
                    source: SourceKind::Codex,
                    doc_id: 1,
                    ts: 10,
                    project: "memex".to_string(),
                    session_id: session_id.to_string(),
                    turn_id: 1,
                    role: "user".to_string(),
                    text: "hello".to_string(),
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: RecordLinks {
                        conversation_kind: kind.map(str::to_string),
                        ..RecordLinks::default()
                    },
                    source_path: format!("{session_id}.jsonl"),
                })
                .expect("record session");
        }
        analytics.flush().expect("flush analytics");

        let spec = |kind: Option<SessionKindFilter>,
                    session_keys: Option<Vec<(String, String)>>| {
            UsageSpec {
                source: None,
                project: None,
                project_grouping: ProjectGrouping::Flat,
                session_keys,
                machine_session_keys: None,
                since_ms: None,
                until_ms: None,
                cost_mode: CostMode::Source,
                include_events: false,
                memo_ttl_ms: 0,
                kind,
            }
        };

        let sub = usage_query(&paths, &spec(Some(SessionKindFilter::Subagent), None))
            .expect("subagent query");
        assert_eq!(
            sub.session_keys,
            Some(HashSet::from([(
                "codex".to_string(),
                "sub-ses".to_string()
            )]))
        );

        let primary = usage_query(&paths, &spec(Some(SessionKindFilter::Primary), None))
            .expect("primary query");
        assert_eq!(
            primary.session_keys,
            Some(HashSet::from([(
                "codex".to_string(),
                "main-ses".to_string()
            )]))
        );

        let all =
            usage_query(&paths, &spec(Some(SessionKindFilter::All), None)).expect("all query");
        assert!(all.session_keys.is_none());

        let none = usage_query(&paths, &spec(None, None)).expect("unfiltered query");
        assert!(none.session_keys.is_none());

        // An explicit text-search filter intersects with the origin filter.
        let both = vec![
            ("codex".to_string(), "main-ses".to_string()),
            ("codex".to_string(), "sub-ses".to_string()),
        ];
        let intersected = usage_query(&paths, &spec(Some(SessionKindFilter::Subagent), Some(both)))
            .expect("intersected query");
        assert_eq!(
            intersected.session_keys,
            Some(HashSet::from([(
                "codex".to_string(),
                "sub-ses".to_string()
            )]))
        );
    }

    #[test]
    fn sessions_rpc_preserves_exact_identity_and_canonical_resume_command() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        std::fs::write(
            paths.root.join("config.toml"),
            "codex_resume_cmd = 'configured-resume {session_id} {source_path_shell}'\n",
        )
        .unwrap();
        let config = UserConfig::load(&paths).unwrap();
        let mut analytics = AnalyticsWriter::open(analytics_path(&paths.state)).unwrap();
        for (id, path, ts, source) in [
            ("shared", "/old session.jsonl", 1, SourceKind::Codex),
            ("shared", "/new.jsonl", 2, SourceKind::Codex),
            ("other", "/old session.jsonl", 3, SourceKind::Codex),
            ("shared", "/old session.jsonl", 4, SourceKind::Claude),
        ] {
            let mut record = test_record(ts, id, path, 1);
            record.source = source;
            record.ts = ts;
            analytics.record(&record).unwrap();
        }
        analytics.flush().unwrap();
        for (path, count) in [("/old session.jsonl", 1), ("/absent.jsonl", 0)] {
            let request: crate::cli::SessionsRequest = serde_json::from_value(serde_json::json!({
                "source": "codex", "session_id": "shared", "source_path": path,
                "origin": "all", "limit": 1,
            }))
            .unwrap();
            let encoded = serde_json::to_vec(&RpcOperation::Sessions {
                request: request.clone(),
            })
            .unwrap();
            let decoded: RpcOperation = serde_json::from_slice(&encoded).unwrap();
            let RpcPayload::Sessions { sessions } = handle_rpc(&paths, &config, decoded).unwrap()
            else {
                panic!("expected sessions metadata");
            };
            assert_eq!(sessions.len(), count);
            let mcp = crate::cli::mcp_sessions(Some(paths.root.clone()), request).unwrap();
            assert_eq!(mcp["results"].as_array().unwrap().len(), count);
            if count == 1 {
                assert_eq!(sessions[0]["source"], "codex");
                assert_eq!(sessions[0]["session_id"], "shared");
                assert_eq!(sessions[0]["source_path"], path);
                assert_eq!(
                    sessions[0]["resume_cmd"],
                    "configured-resume shared '/old session.jsonl'"
                );
                assert_eq!(mcp["results"][0]["resume_cmd"], sessions[0]["resume_cmd"]);
            }
        }
    }

    #[test]
    fn session_count_rpc_counts_exact_identities_with_filters_and_missing_metadata() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let config = UserConfig::load(&paths).unwrap();
        let mut records = Vec::new();
        for (doc, source, path, kind, project) in [
            (1, SourceKind::Codex, "a.jsonl", None, "memex"),
            (2, SourceKind::Codex, "b.jsonl", Some("subagent"), "memex"),
            (3, SourceKind::Claude, "a.jsonl", None, "memex"),
            (
                4,
                SourceKind::Codex,
                "review.jsonl",
                Some("guardian_review"),
                "memex",
            ),
            (5, SourceKind::Codex, "other.jsonl", None, "other"),
        ] {
            let mut record = test_record(doc, "shared", path, 1);
            record.source = source;
            record.project = project.into();
            record.text = "needle".into();
            record.ts = 1_700_000_000_000 + doc * 1000;
            record.links.conversation_kind = kind.map(str::to_string);
            records.push(record);
        }
        let mut duplicate = records[0].clone();
        duplicate.doc_id = 6;
        records.push(duplicate);
        let mut analytics = AnalyticsWriter::open(analytics_path(&paths.state)).unwrap();
        for record in &records {
            analytics.record(record).unwrap();
        }
        analytics.flush().unwrap();
        // Listing groups by repository metadata; lexical search uses indexed project.
        rusqlite::Connection::open(analytics_path(&paths.state))
            .unwrap()
            .execute("UPDATE sessions SET repo_project = project", [])
            .unwrap();
        write_test_index(&paths, &records);
        let count = |filters: serde_json::Value, query: Option<&str>| {
            let request = serde_json::from_value(filters).unwrap();
            let encoded = serde_json::to_vec(&RpcOperation::SessionCount {
                request,
                query: query.map(str::to_string),
            })
            .unwrap();
            let decoded = serde_json::from_slice(&encoded).unwrap();
            let RpcPayload::SessionCount { count } = handle_rpc(&paths, &config, decoded).unwrap()
            else {
                panic!("expected count");
            };
            count.total
        };
        for query in [None, Some("needle")] {
            for (origin, expected) in [
                ("all", 5),
                ("regular", 4),
                ("interactive", 3),
                ("subagent", 1),
            ] {
                assert_eq!(
                    count(serde_json::json!({"origin": origin, "limit": 1}), query),
                    Some(expected)
                );
            }
            assert_eq!(
                count(
                    serde_json::json!({"source": "codex", "project": "memex"}),
                    query
                ),
                Some(2)
            );
            assert_eq!(
                count(
                    serde_json::json!({"source": "claude", "session_id": "shared", "source_path": "a.jsonl"}),
                    query
                ),
                Some(1)
            );
            assert_eq!(
                count(
                    serde_json::json!({"since": "1700000003000", "origin": "all"}),
                    query
                ),
                Some(3)
            );
            assert_eq!(
                count(serde_json::json!({"source_path": "missing"}), query),
                Some(0)
            );
        }
        assert_eq!(count(serde_json::json!({}), Some("absent")), Some(0));
        let mut missing = test_record(7, "uncached", "uncached.jsonl", 1);
        missing.text = "needle".into();
        records.push(missing);
        write_test_index(&paths, &records);
        assert_eq!(count(serde_json::json!({}), Some("needle")), None);
        assert_eq!(
            count(serde_json::json!({"origin": "all"}), Some("needle")),
            Some(6)
        );
    }

    #[test]
    fn session_activity_rpc_reads_complete_analytics_history() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let mut analytics =
            AnalyticsWriter::open(analytics_path(&paths.state)).expect("analytics writer");
        for (session_id, ts) in [("old", 10), ("new", 20), ("review", 30)] {
            analytics
                .record(&Record {
                    source: SourceKind::Codex,
                    doc_id: ts,
                    ts,
                    project: "memex".to_string(),
                    session_id: session_id.to_string(),
                    turn_id: 1,
                    role: "user".to_string(),
                    text: "hello".to_string(),
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: RecordLinks {
                        conversation_kind: (session_id == "review")
                            .then(|| "guardian_review".into()),
                        ..RecordLinks::default()
                    },
                    source_path: format!("{session_id}.jsonl"),
                })
                .expect("record session");
        }
        analytics.flush().expect("flush analytics");

        let points = session_activity_local(
            &paths,
            &SessionActivitySpec {
                source: Some(SourceFilter::Codex),
                project: Some("memex".to_string()),
                project_grouping: ProjectGrouping::Flat,
                since_ms: None,
                until_ms: None,
                kind: None,
            },
        )
        .expect("session activity");

        assert_eq!(points.len(), 2);
        assert_eq!(points[0].timestamp_ms, 10);
        assert_eq!(points[1].timestamp_ms, 20);
        let all = session_activity_local(
            &paths,
            &SessionActivitySpec {
                source: Some(SourceFilter::Codex),
                project: None,
                project_grouping: ProjectGrouping::Flat,
                since_ms: None,
                until_ms: None,
                kind: Some(SessionKindFilter::All),
            },
        )
        .unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn home_activity_rpc_round_trips_filtered_complete_history() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let mut writer = AnalyticsWriter::open(analytics_path(&paths.state)).unwrap();
        for id in 1..=240 {
            let mut row = test_record(id, &format!("session-{id}"), &format!("/{id}.jsonl"), 1);
            row.source = SourceKind::Codex;
            row.project = if id % 2 == 0 { "memex" } else { "other" }.into();
            writer.record(&row).unwrap();
        }
        writer.flush().unwrap();
        drop(writer);
        let response = rpc_handler_round_trip(
            &paths,
            &UserConfig::default(),
            RpcOperation::HomeActivity {
                request: crate::web::ActivityRequest {
                    metric: crate::web::ActivityMetric::Sessions,
                    query: String::new(),
                    source: Some(SourceFilter::Codex),
                    project: Some("memex".into()),
                    days: 30,
                    range: Some(crate::web::TimeRange::All),
                    origin: SessionKindFilter::Regular,
                },
                now: 2_000_000_000_000,
            },
        );
        let RpcPayload::HomeActivity { activity } = response else {
            panic!("expected home activity");
        };
        let json = serde_json::to_value(activity).unwrap();
        let total = json["points"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["value"].as_u64().unwrap())
            .sum::<u64>();
        assert_eq!(total, 120);
        assert_eq!(json["partial"], false);
    }

    #[test]
    fn explicit_defaults_are_respected() {
        let config = UserConfig {
            multi_machine: MultiMachineConfig {
                default: vec!["mini".to_string()],
                timeout_seconds: None,
            },
            machines: vec![machine("mini")],
            ..UserConfig::default()
        };
        assert_eq!(selected_machine_ids(&config, &[]).unwrap(), vec!["mini"]);
    }

    #[test]
    fn unsafe_ssh_targets_are_rejected() {
        let mut machine = machine("mini");
        machine.control.as_mut().unwrap().host = "-oProxyCommand=oops".to_string();
        assert!(validate_machine(&machine).is_err());
    }

    #[test]
    fn usage_reports_merge_and_keep_machine_provenance() {
        let report = |tokens, source: &str| UsageReportWire {
            authority: "local".to_string(),
            events: 1,
            total_tokens: tokens,
            unknown_model_events: 0,
            conservative_events: 0,
            cost_mode: CostMode::Auto,
            price_catalog: "test".to_string(),
            known_cost_usd: 0.5,
            priced_events: 1,
            unpriced_events: 0,
            cache_waste: CacheWaste::default(),
            by_source: vec![UsageSummary {
                source: source.to_string(),
                events: 1,
                total_tokens: tokens,
                ..UsageSummary::default()
            }],
            details: Vec::new(),
            warnings: Vec::new(),
            failures: Vec::new(),
        };

        let merged = merge_usage_reports(
            vec![
                ("local".to_string(), report(10, "codex")),
                ("mini".to_string(), report(20, "claude")),
            ],
            Vec::new(),
            CostMode::Auto,
        );

        assert_eq!(merged.events, 2);
        assert_eq!(merged.total_tokens, 30);
        assert_eq!(merged.by_source[0].source, "local/codex");
        assert_eq!(merged.by_source[1].source, "mini/claude");
    }
}
