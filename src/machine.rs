use crate::analytics::{AnalyticsStore, ProjectGrouping, analytics_path};
use crate::config::{MachineConfig, Paths, UserConfig, default_claude_source};
use crate::embed::{EmbedderHandle, ModelChoice};
use crate::index::{QueryOptions, SearchIndex};
use crate::ingest::{IngestOptions, IngestReport, ingest_all, ingest_if_stale};
use crate::lease::{INGEST_LEASE_TIMEOUT, IngestLease};
use crate::types::{Record, SourceFilter};
use crate::usage::{
    CacheWaste, CostMode, UsageQuery, UsageSummary, scan_usage, scan_usage_activity,
};
use crate::vector::VectorIndex;
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub const LOCAL_MACHINE_ID: &str = "local";
const RPC_PROTOCOL: u32 = 1;
const RRF_K: f32 = 60.0;

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
            source: self.source,
            since: self.since,
            until: self.until,
            limit: self.limit,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocatedRecord {
    pub machine: String,
    pub score: f32,
    pub record: Record,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionContext {
    pub records: Vec<Record>,
    pub cwd: Option<String>,
}

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum RpcOperation {
    Ping,
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
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcRequest {
    protocol: u32,
    request: RpcOperation,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RpcPayload {
    Pong {
        version: String,
    },
    Records {
        records: Vec<(f32, Record)>,
    },
    Session {
        context: SessionContext,
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
    if items.len() > spec.limit {
        items.truncate(spec.limit);
    }
    Ok(Federated { items, failures })
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
    Ok(Federated { items, failures })
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
    std::io::stdin().read_to_end(&mut input)?;
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
        match handle_rpc(&paths, &config, request.request) {
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
        RpcOperation::Ping => Ok(RpcPayload::Pong {
            version: env!("CARGO_PKG_VERSION").to_string(),
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
    }
}

fn usage_local(paths: &Paths, config: &UserConfig, spec: &UsageSpec) -> Result<UsageReportWire> {
    if !config.token_usage_enabled() {
        bail!("token usage tracking is disabled on this machine");
    }
    let report = scan_usage(&usage_query(paths, spec))?;
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
    let (points, partial) = scan_usage_activity(&usage_query(paths, spec))?;
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

fn usage_query(paths: &Paths, spec: &UsageSpec) -> UsageQuery {
    UsageQuery {
        source: spec.source,
        project: spec.project.clone(),
        project_grouping: spec.project_grouping,
        session_keys: spec
            .session_keys
            .as_ref()
            .map(|keys| keys.iter().cloned().collect()),
        since_ms: spec.since_ms,
        until_ms: spec.until_ms,
        cost_mode: spec.cost_mode,
        include_events: spec.include_events,
        cache_path: Some(paths.state.join("usage-cache.sqlite3")),
        memo_ttl_ms: spec.memo_ttl_ms,
    }
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
    if auto_index {
        ensure_local_index(paths, config)?;
    }
    let index = SearchIndex::open_or_create(&paths.index)?;
    let options = spec.query_options();
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
            let mut records = Vec::new();
            for (doc_id, distance) in vector.search(&embedding, spec.limit)? {
                if let Some(record) = index.get_by_doc_id(doc_id)?
                    && matches_filters(&record, &options)
                {
                    records.push((1.0 / (1.0 + distance), record));
                }
            }
            records
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
            let semantic = vector.search(&embedding, candidate_limit)?;
            let mut records = HashMap::new();
            let mut scores = HashMap::<u64, f32>::new();
            for (rank, (_, record)) in lexical.into_iter().enumerate() {
                if matches_filters(&record, &options) {
                    *scores.entry(record.doc_id).or_default() += 1.0 / (RRF_K + rank as f32 + 1.0);
                    records.insert(record.doc_id, record);
                }
            }
            for (rank, (doc_id, _)) in semantic.into_iter().enumerate() {
                if let Some(record) = index.get_by_doc_id(doc_id)?
                    && matches_filters(&record, &options)
                {
                    *scores.entry(doc_id).or_default() += 1.0 / (RRF_K + rank as f32 + 1.0);
                    records.entry(doc_id).or_insert(record);
                }
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
    paths.ensure_dirs()?;
    let lease = IngestLease::acquire(paths, "RPC index", INGEST_LEASE_TIMEOUT)?;
    let index = SearchIndex::open_or_create_for_ingest(&paths.index)?;
    let options = IngestOptions {
        claude_source: default_claude_source(),
        include_agents: false,
        include_reasoning: config.include_reasoning_default(),
        include_codex: true,
        include_opencode: true,
        include_cursor: true,
        include_pi: true,
        include_omp: true,
        include_openclaw: true,
        include_copilot: true,
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
    let request = serde_json::to_vec(&RpcRequest {
        protocol: RPC_PROTOCOL,
        request: operation,
    })?;
    child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("missing SSH stdin"))?
        .write_all(&request)?;

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
    let stderr_thread = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).map(|_| bytes)
    });

    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            bail!(
                "SSH request to '{}' timed out after {}s",
                machine.id,
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
    fn session_activity_rpc_reads_complete_analytics_history() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let mut analytics =
            AnalyticsWriter::open(analytics_path(&paths.state)).expect("analytics writer");
        for (session_id, ts) in [("old", 10), ("new", 20)] {
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
                    links: RecordLinks::default(),
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
            },
        )
        .expect("session activity");

        assert_eq!(points.len(), 2);
        assert_eq!(points[0].timestamp_ms, 10);
        assert_eq!(points[1].timestamp_ms, 20);
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
