//! Reconstructed local token usage.
//!
//! This module intentionally does not model provider quota percentages. Local logs are useful for
//! request-level accounting, but they are not authoritative subscription-limit telemetry.

use crate::analytics::ProjectGrouping;
use crate::types::{SourceFilter, SourceKind};
use anyhow::{Context, Result};
use chrono::DateTime;
use clap::ValueEnum;
use once_cell::sync::Lazy;
use rayon::prelude::*;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use walkdir::WalkDir;

#[derive(Clone, Debug, Default)]
pub struct UsageQuery {
    pub source: Option<SourceFilter>,
    pub project: Option<String>,
    pub project_grouping: ProjectGrouping,
    pub session_keys: Option<HashSet<(String, String)>>,
    pub since_ms: Option<u64>,
    pub until_ms: Option<u64>,
    pub cost_mode: CostMode,
    pub include_events: bool,
    pub cache_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
#[value(rename_all = "kebab-case")]
pub enum CostMode {
    Source,
    #[default]
    Auto,
    Reprice,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TokenBuckets {
    /// Provider-reported input. For OpenAI-shaped records this includes the cached subset.
    pub raw_input: u64,
    pub uncached_input: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    /// One-hour cache writes, a subset of `cache_write`.
    pub cache_write_1h: u64,
    /// Billable output, including reasoning when a provider reports it separately.
    pub output: u64,
    /// Reasoning output, retained as a subset of `output` for reporting.
    pub reasoning: u64,
}

impl TokenBuckets {
    fn additive_total(&self) -> u64 {
        self.uncached_input
            .saturating_add(self.cache_read)
            .saturating_add(self.cache_write)
            .saturating_add(self.output)
    }

    pub fn total(&self) -> u64 {
        self.additive_total()
    }

    fn codex(input: u64, cached: u64, output: u64, reasoning: u64) -> Self {
        let cache_read = cached.min(input);
        Self {
            raw_input: input,
            uncached_input: input.saturating_sub(cache_read),
            cache_read,
            cache_write: 0,
            cache_write_1h: 0,
            output,
            reasoning,
        }
    }

    fn disjoint(input: u64, cache_read: u64, cache_write: u64, output: u64) -> Self {
        Self {
            raw_input: input,
            uncached_input: input,
            cache_read,
            cache_write,
            cache_write_1h: 0,
            output,
            reasoning: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct UsageEvent {
    pub source: String,
    pub source_path: String,
    pub source_record_id: Option<String>,
    pub session_id: Option<String>,
    pub request_id: Option<String>,
    pub message_id: Option<String>,
    pub timestamp_ms: u64,
    pub project: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub tokens: TokenBuckets,
    pub source_cost_usd: Option<f64>,
    pub dedupe_confidence: &'static str,
    pub conservative_undercount: bool,
    #[serde(skip)]
    sidechain: bool,
    #[serde(skip)]
    source_order: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct UsageSummary {
    pub source: String,
    pub events: u64,
    pub uncached_input: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub output: u64,
    pub reasoning: u64,
    pub total_tokens: u64,
    pub known_cost_usd: f64,
    pub priced_events: u64,
    pub unpriced_events: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct UsageReport {
    pub authority: &'static str,
    pub events: u64,
    pub total_tokens: u64,
    pub unknown_model_events: u64,
    pub conservative_events: u64,
    pub cost_mode: CostMode,
    pub price_catalog: &'static str,
    pub known_cost_usd: f64,
    pub priced_events: u64,
    pub unpriced_events: u64,
    pub by_source: Vec<UsageSummary>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<UsageEvent>,
    pub warnings: Vec<String>,
}

pub fn scan_usage(query: &UsageQuery) -> Result<UsageReport> {
    let mut events = Vec::new();
    let mut warnings = Vec::new();
    if query
        .source
        .is_none_or(|source| source == SourceFilter::Claude)
        && let Err(error) = scan_claude(
            &mut events,
            &mut warnings,
            query.since_ms,
            query.cache_path.as_deref(),
        )
    {
        warnings.push(format!(
            "{} scanner: {error:#}",
            SourceFilter::Claude.as_str()
        ));
    }
    let mut scan =
        |filter: SourceFilter, f: fn(&mut Vec<UsageEvent>, &mut Vec<String>) -> Result<()>| {
            if query.source.is_none_or(|source| source == filter)
                && let Err(error) = f(&mut events, &mut warnings)
            {
                warnings.push(format!("{} scanner: {error:#}", filter.as_str()));
            }
        };
    scan(SourceFilter::Codex, scan_codex);
    scan(SourceFilter::Opencode, scan_opencode);
    scan(SourceFilter::Pi, scan_pi);
    scan(SourceFilter::Cursor, scan_cursor);
    scan(SourceFilter::Copilot, scan_copilot);

    reconcile_claude(&mut events);
    reconcile_codex_copies(&mut events);
    let mut project_cache = HashMap::new();
    events.retain(|event| {
        query
            .since_ms
            .is_none_or(|since| event.timestamp_ms >= since)
            && query
                .until_ms
                .is_none_or(|until| event.timestamp_ms < until)
            && query.project.as_deref().is_none_or(|project| {
                event.project.as_deref().is_some_and(|candidate| {
                    usage_project_matches(
                        candidate,
                        project,
                        query.project_grouping,
                        &mut project_cache,
                    )
                })
            })
            && query.session_keys.as_ref().is_none_or(|session_keys| {
                event.session_id.as_ref().is_some_and(|session_id| {
                    session_keys.contains(&(event.source.clone(), session_id.clone()))
                })
            })
    });
    events.sort_by(|a, b| {
        (a.timestamp_ms, &a.source_path, a.source_order).cmp(&(
            b.timestamp_ms,
            &b.source_path,
            b.source_order,
        ))
    });

    let mut by_source: HashMap<String, UsageSummary> = HashMap::new();
    let mut report = UsageReport {
        authority: "local_log",
        cost_mode: query.cost_mode,
        price_catalog: PRICE_CATALOG_ID,
        warnings,
        ..UsageReport::default()
    };
    for event in &events {
        let total = event.tokens.additive_total();
        report.events += 1;
        report.total_tokens = report.total_tokens.saturating_add(total);
        report.unknown_model_events += u64::from(event.model.is_none());
        report.conservative_events += u64::from(event.conservative_undercount);
        let cost = event_cost_nanos(event, query.cost_mode);
        if let Some(cost) = cost {
            report.priced_events += 1;
            report.known_cost_usd += cost as f64 / 1_000_000_000.0;
        } else {
            report.unpriced_events += 1;
        }
        let row = by_source
            .entry(event.source.clone())
            .or_insert_with(|| UsageSummary {
                source: event.source.clone(),
                ..UsageSummary::default()
            });
        row.events += 1;
        row.uncached_input = row
            .uncached_input
            .saturating_add(event.tokens.uncached_input);
        row.cache_read = row.cache_read.saturating_add(event.tokens.cache_read);
        row.cache_write = row.cache_write.saturating_add(event.tokens.cache_write);
        row.output = row.output.saturating_add(event.tokens.output);
        row.reasoning = row.reasoning.saturating_add(event.tokens.reasoning);
        row.total_tokens = row.total_tokens.saturating_add(total);
        if let Some(cost) = cost {
            row.priced_events += 1;
            row.known_cost_usd += cost as f64 / 1_000_000_000.0;
        } else {
            row.unpriced_events += 1;
        }
    }
    report.by_source = by_source.into_values().collect();
    report.by_source.sort_by(|a, b| a.source.cmp(&b.source));
    if query.include_events {
        report.details = events;
    }
    Ok(report)
}

fn home() -> PathBuf {
    directories::BaseDirs::new()
        .map(|dirs| dirs.home_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("/"))
}

fn jsonl_files(roots: impl IntoIterator<Item = PathBuf>) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for root in roots {
        if !root.exists() {
            continue;
        }
        for entry in WalkDir::new(root).follow_links(false).into_iter().flatten() {
            let path = entry.path();
            if entry.file_type().is_file()
                && path.extension().and_then(|v| v.to_str()) == Some("jsonl")
            {
                files.push(path.to_path_buf());
            }
        }
    }
    files.sort();
    files.dedup();
    files
}

fn lines(path: &Path) -> Result<impl Iterator<Item = (u64, Value)>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    Ok(BufReader::new(file)
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let line = line.ok()?;
            serde_json::from_str(&line)
                .ok()
                .map(|value| (index as u64, value))
        }))
}

fn timestamp_ms(value: &Value) -> u64 {
    value
        .as_u64()
        .map(|n| if n < 10_000_000_000 { n * 1000 } else { n })
        .or_else(|| value.as_i64().filter(|n| *n >= 0).map(|n| n as u64))
        .or_else(|| {
            value
                .as_str()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|d| d.timestamp_millis().max(0) as u64)
        })
        .unwrap_or(0)
}

fn u64_at(value: &Value, aliases: &[&str]) -> u64 {
    aliases
        .iter()
        .find_map(|key| value.get(*key).and_then(Value::as_u64))
        .unwrap_or(0)
}

fn str_at(value: &Value, aliases: &[&str]) -> Option<String> {
    aliases
        .iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn usage_project_matches(
    candidate: &str,
    project: &str,
    grouping: ProjectGrouping,
    cache: &mut HashMap<String, String>,
) -> bool {
    let candidate_key = match grouping {
        ProjectGrouping::Flat => usage_project_key(candidate),
        ProjectGrouping::Repository => cache
            .entry(candidate.to_string())
            .or_insert_with(|| {
                if Path::new(candidate).is_dir() {
                    crate::analytics::repository_project_for_cwd(candidate)
                        .unwrap_or_else(|| usage_project_key(candidate))
                } else {
                    usage_project_key(candidate)
                }
            })
            .clone(),
    };
    candidate.eq_ignore_ascii_case(project)
        || candidate_key.eq_ignore_ascii_case(&usage_project_key(project))
}

fn usage_project_key(value: &str) -> String {
    let trimmed = value.trim().trim_end_matches(['/', '\\']);
    let tail = trimmed.rsplit(['/', '\\', ':']).next().unwrap_or(trimmed);
    let tail = tail.strip_suffix(".git").unwrap_or(tail);
    let encoded = tail.trim_matches('-');
    if tail.starts_with('-')
        && (encoded.to_ascii_lowercase().starts_with("users-")
            || encoded.to_ascii_lowercase().starts_with("home-"))
    {
        return encoded.rsplit('-').next().unwrap_or(encoded).to_string();
    }
    tail.to_string()
}

#[derive(Deserialize)]
struct ClaudeUsageLine {
    #[serde(
        rename = "type",
        default,
        deserialize_with = "deserialize_optional_string"
    )]
    kind: Option<String>,
    message: Option<ClaudeUsageMessage>,
    #[serde(
        rename = "sessionId",
        alias = "session_id",
        default,
        deserialize_with = "deserialize_optional_string"
    )]
    session_id: Option<String>,
    #[serde(
        rename = "requestId",
        alias = "request_id",
        default,
        deserialize_with = "deserialize_optional_string"
    )]
    request_id: Option<String>,
    timestamp: Option<Value>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    cwd: Option<String>,
    #[serde(
        rename = "costUSD",
        default,
        deserialize_with = "deserialize_optional_f64"
    )]
    cost_usd: Option<f64>,
    #[serde(
        rename = "isSidechain",
        default,
        deserialize_with = "deserialize_bool_or_false"
    )]
    is_sidechain: bool,
}

#[derive(Deserialize)]
struct ClaudeUsageMessage {
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    id: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_string")]
    model: Option<String>,
    usage: Option<ClaudeTokenUsage>,
}

#[derive(Deserialize)]
struct ClaudeTokenUsage {
    #[serde(
        default,
        alias = "inputTokens",
        deserialize_with = "deserialize_u64_or_zero"
    )]
    input_tokens: u64,
    #[serde(
        default,
        alias = "cacheReadInputTokens",
        deserialize_with = "deserialize_u64_or_zero"
    )]
    cache_read_input_tokens: u64,
    #[serde(
        default,
        alias = "cacheCreationInputTokens",
        deserialize_with = "deserialize_u64_or_zero"
    )]
    cache_creation_input_tokens: u64,
    #[serde(
        default,
        alias = "outputTokens",
        deserialize_with = "deserialize_u64_or_zero"
    )]
    output_tokens: u64,
    cache_creation: Option<Value>,
}

fn deserialize_optional_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Value::deserialize(deserializer)?
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_string))
}

fn deserialize_optional_f64<'de, D>(deserializer: D) -> std::result::Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Value::deserialize(deserializer)?.as_f64())
}

fn deserialize_bool_or_false<'de, D>(deserializer: D) -> std::result::Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Value::deserialize(deserializer)?.as_bool().unwrap_or(false))
}

fn deserialize_u64_or_zero<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Value::deserialize(deserializer)?.as_u64().unwrap_or(0))
}

const CLAUDE_USAGE_CACHE_VERSION: i64 = 1;
static CLAUDE_SCAN_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[derive(Serialize, Deserialize)]
struct ClaudeCachedEvent {
    source_record_id: Option<String>,
    session_id: Option<String>,
    request_id: Option<String>,
    message_id: Option<String>,
    timestamp_ms: u64,
    project: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    tokens: TokenBuckets,
    source_cost_usd: Option<f64>,
    exact_dedupe: bool,
    sidechain: bool,
    source_order: u64,
}

impl ClaudeCachedEvent {
    fn from_event(event: &UsageEvent) -> Self {
        Self {
            source_record_id: event.source_record_id.clone(),
            session_id: event.session_id.clone(),
            request_id: event.request_id.clone(),
            message_id: event.message_id.clone(),
            timestamp_ms: event.timestamp_ms,
            project: event.project.clone(),
            provider: event.provider.clone(),
            model: event.model.clone(),
            tokens: event.tokens.clone(),
            source_cost_usd: event.source_cost_usd,
            exact_dedupe: event.dedupe_confidence == "exact",
            sidechain: event.sidechain,
            source_order: event.source_order,
        }
    }

    fn into_event(self, source_path: String) -> UsageEvent {
        UsageEvent {
            source: "claude".into(),
            source_path,
            source_record_id: self.source_record_id,
            session_id: self.session_id,
            request_id: self.request_id,
            message_id: self.message_id,
            timestamp_ms: self.timestamp_ms,
            project: self.project,
            provider: self.provider,
            model: self.model,
            tokens: self.tokens,
            source_cost_usd: self.source_cost_usd,
            dedupe_confidence: if self.exact_dedupe {
                "exact"
            } else {
                "heuristic"
            },
            conservative_undercount: false,
            sidechain: self.sidechain,
            source_order: self.source_order,
        }
    }
}

struct ClaudeUsageCache {
    connection: Connection,
}

struct ParsedClaudeFile {
    index: usize,
    path: PathBuf,
    size: u64,
    mtime_ns: i64,
    events: Vec<UsageEvent>,
}

struct PreparedClaudeCacheFile {
    path: PathBuf,
    size: u64,
    mtime_ns: i64,
    max_timestamp_ms: u64,
    events_json: Vec<u8>,
}

impl ClaudeUsageCache {
    fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        connection.busy_timeout(std::time::Duration::from_secs(2))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS claude_usage_file_cache (
                 path TEXT PRIMARY KEY,
                 parser_version INTEGER NOT NULL,
                 size INTEGER NOT NULL,
                 mtime_ns INTEGER NOT NULL,
                 max_timestamp_ms INTEGER NOT NULL,
                 events_json BLOB NOT NULL
             );",
        )?;
        Ok(Self { connection })
    }

    fn load(
        &self,
        path: &Path,
        size: u64,
        mtime_ns: i64,
        since_ms: Option<u64>,
    ) -> Result<Option<Vec<UsageEvent>>> {
        let source_path = path.to_string_lossy();
        let cached = self
            .connection
            .query_row(
                "SELECT events_json, max_timestamp_ms FROM claude_usage_file_cache
                 WHERE path = ?1 AND parser_version = ?2 AND size = ?3 AND mtime_ns = ?4",
                params![
                    source_path.as_ref(),
                    CLAUDE_USAGE_CACHE_VERSION,
                    size as i64,
                    mtime_ns
                ],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, u64>(1)?)),
            )
            .optional()?;
        let Some((blob, max_timestamp_ms)) = cached else {
            return Ok(None);
        };
        if since_ms.is_some_and(|since_ms| max_timestamp_ms < since_ms) {
            return Ok(Some(Vec::new()));
        }
        let cached: Vec<ClaudeCachedEvent> = serde_json::from_slice(&blob)?;
        Ok(Some(
            cached
                .into_iter()
                .map(|event| event.into_event(source_path.to_string()))
                .collect(),
        ))
    }

    fn save_batch(&mut self, parsed: &[ParsedClaudeFile]) -> Result<()> {
        let prepared = parsed
            .iter()
            .map(|file| {
                let cached = file
                    .events
                    .iter()
                    .map(ClaudeCachedEvent::from_event)
                    .collect::<Vec<_>>();
                Ok(PreparedClaudeCacheFile {
                    path: file.path.clone(),
                    size: file.size,
                    mtime_ns: file.mtime_ns,
                    max_timestamp_ms: file
                        .events
                        .iter()
                        .map(|event| event.timestamp_ms)
                        .max()
                        .unwrap_or(0),
                    events_json: serde_json::to_vec(&cached)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let transaction = self.connection.transaction()?;
        for file in prepared {
            transaction.execute(
                "INSERT INTO claude_usage_file_cache(
                     path, parser_version, size, mtime_ns, max_timestamp_ms, events_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(path) DO UPDATE SET
                     parser_version = excluded.parser_version,
                     size = excluded.size,
                     mtime_ns = excluded.mtime_ns,
                     max_timestamp_ms = excluded.max_timestamp_ms,
                     events_json = excluded.events_json",
                params![
                    file.path.to_string_lossy().as_ref(),
                    CLAUDE_USAGE_CACHE_VERSION,
                    file.size as i64,
                    file.mtime_ns,
                    file.max_timestamp_ms,
                    file.events_json
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }
}

fn usage_file_metadata(path: &Path) -> Result<(u64, i64)> {
    let metadata = path.metadata()?;
    let mtime_ns = metadata
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64;
    Ok((metadata.len(), mtime_ns))
}

fn scan_claude(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    since_ms: Option<u64>,
    cache_path: Option<&Path>,
) -> Result<()> {
    let _scan_guard = CLAUDE_SCAN_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let roots = if let Some(config) = std::env::var_os("CLAUDE_CONFIG_DIR") {
        config
            .to_string_lossy()
            .split(',')
            .map(|part| {
                let path = PathBuf::from(part.trim());
                if path.file_name().and_then(|n| n.to_str()) == Some("projects") {
                    path
                } else {
                    path.join("projects")
                }
            })
            .collect()
    } else {
        vec![
            home().join(".claude/projects"),
            home().join(".config/claude/projects"),
        ]
    };
    let files = jsonl_files(roots);
    let mut cache = match cache_path.map(ClaudeUsageCache::open).transpose() {
        Ok(cache) => cache,
        Err(error) => {
            warnings.push(format!("Claude usage cache disabled: {error:#}"));
            None
        }
    };
    let mut slots = (0..files.len()).map(|_| None).collect::<Vec<_>>();
    let mut missing = Vec::new();
    for (index, path) in files.iter().enumerate() {
        let metadata = usage_file_metadata(path)?;
        let cached = cache.as_ref().map_or(Ok(None), |cache| {
            cache.load(path, metadata.0, metadata.1, since_ms)
        });
        match cached {
            Ok(Some(events)) => slots[index] = Some(events),
            Ok(None) => missing.push((index, path.clone(), metadata)),
            Err(error) => {
                warnings.push(format!("Claude usage cache read failed: {error:#}"));
                missing.push((index, path.clone(), metadata));
            }
        }
    }
    let parsed = missing
        .par_iter()
        .map(|(index, path, metadata)| {
            scan_claude_file(path).map(|events| ParsedClaudeFile {
                index: *index,
                path: path.clone(),
                size: metadata.0,
                mtime_ns: metadata.1,
                events,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if let Some(cache) = &mut cache
        && let Err(error) = cache.save_batch(&parsed)
    {
        warnings.push(format!("Claude usage cache write failed: {error:#}"));
    }
    for file in parsed {
        slots[file.index] = Some(file.events);
    }
    for events in slots.into_iter().flatten() {
        out.extend(events);
    }
    Ok(())
}

fn scan_claude_file(path: &Path) -> Result<Vec<UsageEvent>> {
    let source_path = path.to_string_lossy().to_string();
    let fallback_session = path
        .file_stem()
        .and_then(|n| n.to_str())
        .map(str::to_string);
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut events = Vec::new();
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let Ok(line) = line else {
            continue;
        };
        if !line.contains("\"usage\"") {
            continue;
        }
        let Ok(value) = serde_json::from_str::<ClaudeUsageLine>(&line) else {
            continue;
        };
        if value.kind.as_deref() != Some("assistant") {
            continue;
        }
        let Some(message) = value.message else {
            continue;
        };
        let Some(usage) = message.usage else {
            continue;
        };
        let cache_write = usage.cache_creation_input_tokens;
        let mut tokens = TokenBuckets::disjoint(
            usage.input_tokens,
            usage.cache_read_input_tokens,
            cache_write,
            usage.output_tokens,
        );
        tokens.cache_write_1h = usage
            .cache_creation
            .as_ref()
            .and_then(|cache| cache.get("ephemeral_1h_input_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default()
            .min(tokens.cache_write);
        if tokens.additive_total() == 0 {
            continue;
        }
        let exact_dedupe = message.id.is_some() && value.request_id.is_some();
        events.push(UsageEvent {
            source: "claude".into(),
            source_path: source_path.clone(),
            source_record_id: Some(format!("line:{index}")),
            session_id: value.session_id.or_else(|| fallback_session.clone()),
            request_id: value.request_id,
            message_id: message.id,
            timestamp_ms: value.timestamp.as_ref().map(timestamp_ms).unwrap_or(0),
            project: value.cwd,
            provider: Some("anthropic".into()),
            model: message.model,
            tokens,
            source_cost_usd: value.cost_usd,
            dedupe_confidence: if exact_dedupe { "exact" } else { "heuristic" },
            conservative_undercount: false,
            sidechain: value.is_sidechain,
            source_order: index as u64,
        });
    }
    Ok(events)
}

fn reconcile_claude(events: &mut Vec<UsageEvent>) {
    let mut best_exact: HashMap<(String, String), usize> = HashMap::new();
    let mut keep = vec![true; events.len()];
    for index in 0..events.len() {
        if events[index].source != "claude" {
            continue;
        }
        let (Some(message), Some(request)) = (
            events[index].message_id.clone(),
            events[index].request_id.clone(),
        ) else {
            continue;
        };
        let key = (message, request);
        if let Some(previous) = best_exact.get(&key).copied() {
            let winner = choose_claude(&events[previous], &events[index]);
            if winner {
                keep[index] = false;
            } else {
                keep[previous] = false;
                best_exact.insert(key, index);
            }
        } else {
            best_exact.insert(key, index);
        }
    }
    let mut by_message: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, event) in events.iter().enumerate() {
        if keep[index]
            && event.source == "claude"
            && let Some(message) = &event.message_id
        {
            by_message.entry(message.clone()).or_default().push(index);
        }
    }
    for indices in by_message.into_values() {
        if indices.len() < 2 || !indices.iter().any(|index| !events[*index].sidechain) {
            continue;
        }
        // Message-only matching is a sidechain replay fallback. Keep every distinct parent
        // request; suppress only sidechain copies of a message present in a parent transcript.
        for index in indices {
            if events[index].sidechain {
                keep[index] = false;
            }
        }
    }
    let mut index = 0usize;
    events.retain(|_| {
        let retain = keep[index];
        index += 1;
        retain
    });
}

/// True means `a` wins. Prefer parent/non-sidechain, then completeness, then source order.
fn choose_claude(a: &UsageEvent, b: &UsageEvent) -> bool {
    if a.sidechain != b.sidechain {
        return !a.sidechain;
    }
    let a_parent = !a.source_path.contains("/subagents/");
    let b_parent = !b.source_path.contains("/subagents/");
    if a_parent != b_parent {
        return a_parent;
    }
    let a_total = a.tokens.additive_total();
    let b_total = b.tokens.additive_total();
    if a_total != b_total {
        return a_total > b_total;
    }
    a.source_order >= b.source_order
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
struct CodexTokens {
    input: u64,
    cached: u64,
    output: u64,
    reasoning: u64,
}

impl CodexTokens {
    fn from(value: &Value) -> Self {
        Self {
            input: u64_at(value, &["input_tokens", "inputTokens", "prompt_tokens"]),
            cached: u64_at(
                value,
                &[
                    "cached_input_tokens",
                    "cachedInputTokens",
                    "cache_read_input_tokens",
                ],
            ),
            output: u64_at(
                value,
                &["output_tokens", "outputTokens", "completion_tokens"],
            ),
            reasoning: u64_at(
                value,
                &[
                    "reasoning_output_tokens",
                    "reasoningTokens",
                    "reasoning_tokens",
                ],
            ),
        }
    }
    fn zero(self) -> bool {
        self.input == 0 && self.cached == 0 && self.output == 0 && self.reasoning == 0
    }
    fn add(self, rhs: Self) -> Self {
        Self {
            input: self.input.saturating_add(rhs.input),
            cached: self.cached.saturating_add(rhs.cached),
            output: self.output.saturating_add(rhs.output),
            reasoning: self.reasoning.saturating_add(rhs.reasoning),
        }
    }
    fn sub(self, rhs: Self) -> Self {
        Self {
            input: self.input.saturating_sub(rhs.input),
            cached: self.cached.saturating_sub(rhs.cached),
            output: self.output.saturating_sub(rhs.output),
            reasoning: self.reasoning.saturating_sub(rhs.reasoning),
        }
    }
    fn min(self, rhs: Self) -> Self {
        Self {
            input: self.input.min(rhs.input),
            cached: self.cached.min(rhs.cached),
            output: self.output.min(rhs.output),
            reasoning: self.reasoning.min(rhs.reasoning),
        }
    }
    fn max(self, rhs: Self) -> Self {
        Self {
            input: self.input.max(rhs.input),
            cached: self.cached.max(rhs.cached),
            output: self.output.max(rhs.output),
            reasoning: self.reasoning.max(rhs.reasoning),
        }
    }
    fn at_least(self, rhs: Self) -> bool {
        self.input >= rhs.input
            && self.cached >= rhs.cached
            && self.output >= rhs.output
            && self.reasoning >= rhs.reasoning
    }
    fn at_most(self, rhs: Self) -> bool {
        self.input <= rhs.input
            && self.cached <= rhs.cached
            && self.output <= rhs.output
            && self.reasoning <= rhs.reasoning
    }
}

#[derive(Default)]
struct CodexCounter {
    counted: CodexTokens,
    raw_baseline: CodexTokens,
    watermark: CodexTokens,
    seen: Vec<CodexTokens>,
    divergent: bool,
    interleaved: bool,
}

impl CodexCounter {
    fn establish_unresolved_fork_baseline(&mut self, total: CodexTokens) {
        self.raw_baseline = total;
        self.watermark = self.watermark.max(total);
        if self.seen.last() != Some(&total) {
            self.seen.push(total);
            if self.seen.len() > 64 {
                self.seen.remove(0);
            }
        }
    }

    fn account(&mut self, last: Option<CodexTokens>, total: Option<CodexTokens>) -> CodexTokens {
        if let Some(total) = total {
            if self.seen.contains(&total) {
                return CodexTokens::default();
            }
            if !total.at_least(self.watermark) {
                self.interleaved = true;
            }
        }
        let baseline = self.watermark.max(self.raw_baseline);
        let delta = match (last, total) {
            (Some(last), Some(total)) if self.interleaved => {
                last.min(contained(total, baseline, self.counted))
            }
            (None, Some(total)) if self.interleaved => contained(total, baseline, self.counted),
            (Some(last), Some(total)) => {
                let total_delta = total.sub(baseline);
                if !self.divergent && total.at_least(baseline) && total_delta.at_most(last) {
                    total_delta
                } else {
                    last
                }
            }
            (None, Some(total)) if self.divergent => contained(total, baseline, self.counted),
            (None, Some(total)) => total.sub(baseline),
            (Some(last), None) => last,
            (None, None) => return CodexTokens::default(),
        };
        self.counted = self.counted.add(delta);
        if let Some(total) = total {
            self.raw_baseline = total;
            self.divergent |= total != self.counted;
            self.watermark = self.watermark.max(total);
            if self.seen.last() != Some(&total) {
                self.seen.push(total);
                if self.seen.len() > 64 {
                    self.seen.remove(0);
                }
            }
        } else {
            self.raw_baseline = self.counted;
            self.watermark = self.watermark.max(self.counted);
        }
        delta
    }
}

fn contained(current: CodexTokens, watermark: CodexTokens, counted: CodexTokens) -> CodexTokens {
    fn one(current: u64, watermark: u64, counted: u64) -> u64 {
        if current >= watermark {
            current.saturating_sub(watermark.max(counted))
        } else {
            current.saturating_sub(counted)
        }
    }
    CodexTokens {
        input: one(current.input, watermark.input, counted.input),
        cached: one(current.cached, watermark.cached, counted.cached),
        output: one(current.output, watermark.output, counted.output),
        reasoning: one(current.reasoning, watermark.reasoning, counted.reasoning),
    }
}

fn scan_codex(out: &mut Vec<UsageEvent>, _warnings: &mut Vec<String>) -> Result<()> {
    let homes: Vec<PathBuf> = std::env::var_os("CODEX_HOME")
        .map(|v| {
            v.to_string_lossy()
                .split(',')
                .map(|s| PathBuf::from(s.trim()))
                .collect()
        })
        .unwrap_or_else(|| vec![home().join(".codex")]);
    let mut files = Vec::new();
    for root in homes {
        let active = root.join("sessions");
        let archived = root.join("archived_sessions");
        if active.exists() || archived.exists() {
            files.extend(jsonl_files([active, archived]));
        } else {
            files.extend(jsonl_files([root]));
        }
    }
    for path in files {
        scan_codex_file(&path, out)?;
    }
    Ok(())
}

fn scan_codex_file(path: &Path, out: &mut Vec<UsageEvent>) -> Result<()> {
    let source_path = path.to_string_lossy().to_string();
    let mut session = path
        .file_stem()
        .and_then(|n| n.to_str())
        .map(str::to_string);
    let mut parent = None;
    let mut fork_timestamp_ms = None;
    let mut project = None;
    let mut model = None;
    let mut turn = None;
    let mut counter = CodexCounter::default();
    let mut event_index = 0u64;
    let mut unresolved_fork_baseline_seen = false;
    for (line_index, value) in lines(path)? {
        let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
        let payload = value.get("payload").unwrap_or(&Value::Null);
        match kind {
            "session_meta" => {
                session = str_at(payload, &["id", "session_id"]).or(session);
                parent = str_at(
                    payload,
                    &["forked_from_id", "parent_session_id", "parentSessionId"],
                );
                if parent.is_some() {
                    fork_timestamp_ms = value.get("timestamp").map(timestamp_ms);
                }
                project = str_at(payload, &["cwd"]);
            }
            "turn_context" => model = str_at(payload, &["model", "model_name"]),
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("task_started") => {
                turn = str_at(payload, &["turn_id", "turnId"])
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("token_count") => {
                let info = payload.get("info").unwrap_or(payload);
                let last = info
                    .get("last_token_usage")
                    .map(CodexTokens::from)
                    .filter(|v| !v.zero());
                let total = info
                    .get("total_token_usage")
                    .map(CodexTokens::from)
                    .filter(|v| !v.zero());
                let event_timestamp_ms = value.get("timestamp").map(timestamp_ms).unwrap_or(0);
                if parent.is_some()
                    && fork_timestamp_ms.is_some_and(|fork| event_timestamp_ms <= fork)
                {
                    if let Some(total) = total {
                        counter.establish_unresolved_fork_baseline(total);
                        unresolved_fork_baseline_seen = true;
                    }
                    continue;
                }
                if parent.is_some()
                    && !unresolved_fork_baseline_seen
                    && let Some(total) = total
                {
                    counter.establish_unresolved_fork_baseline(total);
                    unresolved_fork_baseline_seen = true;
                    continue;
                }
                let delta = counter.account(last, total);
                if delta.zero() {
                    continue;
                }
                out.push(UsageEvent {
                    source: "codex".into(),
                    source_path: source_path.clone(),
                    source_record_id: Some(format!("event:{event_index}")),
                    session_id: session.clone(),
                    request_id: turn.clone(),
                    message_id: None,
                    timestamp_ms: event_timestamp_ms,
                    project: project.clone(),
                    provider: Some("openai".into()),
                    model: model
                        .clone()
                        .or_else(|| str_at(info, &["model", "model_name"])),
                    tokens: TokenBuckets::codex(
                        delta.input,
                        delta.cached,
                        delta.output,
                        delta.reasoning,
                    ),
                    source_cost_usd: None,
                    dedupe_confidence: "strong",
                    conservative_undercount: counter.interleaved || parent.is_some(),
                    sidechain: false,
                    source_order: line_index,
                });
                event_index += 1;
            }
            _ => {
                if let Some(usage) = value
                    .get("usage")
                    .or_else(|| value.pointer("/data/usage"))
                    .or_else(|| value.pointer("/result/usage"))
                    .or_else(|| value.pointer("/response/usage"))
                {
                    let tokens = CodexTokens::from(usage);
                    if tokens.zero() {
                        continue;
                    }
                    out.push(UsageEvent {
                        source: "codex".into(),
                        source_path: source_path.clone(),
                        source_record_id: Some(format!("line:{line_index}")),
                        session_id: session.clone(),
                        request_id: None,
                        message_id: None,
                        timestamp_ms: value
                            .get("timestamp")
                            .or_else(|| value.get("created_at"))
                            .map(timestamp_ms)
                            .unwrap_or(0),
                        project: project.clone(),
                        provider: Some("openai".into()),
                        model: model
                            .clone()
                            .or_else(|| str_at(&value, &["model", "model_name"])),
                        tokens: TokenBuckets::codex(
                            tokens.input,
                            tokens.cached,
                            tokens.output,
                            tokens.reasoning,
                        ),
                        source_cost_usd: None,
                        dedupe_confidence: "strong",
                        conservative_undercount: false,
                        sidechain: false,
                        source_order: line_index,
                    });
                }
            }
        }
    }
    Ok(())
}

fn reconcile_codex_copies(events: &mut Vec<UsageEvent>) {
    let mut seen = HashSet::new();
    events.retain(|event| {
        if event.source != "codex" {
            return true;
        }
        let Some(session) = &event.session_id else {
            return true;
        };
        let Some(record) = &event.source_record_id else {
            return true;
        };
        seen.insert((session.clone(), record.clone(), event.tokens.clone()))
    });
}

fn scan_pi(out: &mut Vec<UsageEvent>, _warnings: &mut Vec<String>) -> Result<()> {
    let roots = [crate::ingest::pi_sessions_root()];
    for path in jsonl_files(roots) {
        let source_path = path.to_string_lossy().to_string();
        let mut session = crate::ingest::pi_session_id_from_path(&path);
        let mut project = crate::ingest::project_from_pi_session_path(&path);
        let mut current_model = None;
        let mut current_provider = None;
        for (index, value) in lines(&path)? {
            if value.get("type").and_then(Value::as_str) == Some("session") {
                crate::ingest::apply_pi_session_identity(
                    value.get("id").and_then(Value::as_str),
                    value.get("cwd").and_then(Value::as_str),
                    &mut session,
                    &mut project,
                );
                continue;
            }
            if value.get("type").and_then(Value::as_str) == Some("model_change") {
                current_model = str_at(&value, &["modelId", "model", "model_id"]);
                current_provider = str_at(&value, &["provider", "providerId", "provider_id"]);
                continue;
            }
            if value.get("type").and_then(Value::as_str) != Some("message") {
                continue;
            }
            let Some(message) = value.get("message") else {
                continue;
            };
            if message.get("role").and_then(Value::as_str) != Some("assistant") {
                continue;
            }
            let Some(usage) = message.get("usage") else {
                continue;
            };
            let tokens = TokenBuckets::disjoint(
                u64_at(
                    usage,
                    &[
                        "input",
                        "inputTokens",
                        "input_tokens",
                        "promptTokens",
                        "prompt_tokens",
                    ],
                ),
                u64_at(
                    usage,
                    &[
                        "cacheRead",
                        "cacheReadTokens",
                        "cache_read",
                        "cache_read_tokens",
                        "cacheReadInputTokens",
                        "cache_read_input_tokens",
                    ],
                ),
                u64_at(
                    usage,
                    &[
                        "cacheWrite",
                        "cacheWriteTokens",
                        "cache_write",
                        "cache_write_tokens",
                        "cacheCreationTokens",
                        "cache_creation_tokens",
                        "cacheCreationInputTokens",
                        "cache_creation_input_tokens",
                    ],
                ),
                u64_at(
                    usage,
                    &[
                        "output",
                        "outputTokens",
                        "output_tokens",
                        "completionTokens",
                        "completion_tokens",
                    ],
                ),
            );
            if tokens.additive_total() == 0 {
                continue;
            }
            out.push(UsageEvent {
                source: "pi".into(),
                source_path: source_path.clone(),
                source_record_id: Some(format!("line:{index}")),
                session_id: Some(session.clone()),
                request_id: None,
                message_id: str_at(&value, &["id"]),
                timestamp_ms: value.get("timestamp").map(timestamp_ms).unwrap_or(0),
                project: Some(project.clone()),
                provider: str_at(message, &["provider"])
                    .or_else(|| str_at(&value, &["provider"]))
                    .or_else(|| current_provider.clone()),
                model: str_at(message, &["model", "modelId"])
                    .or_else(|| str_at(&value, &["model", "modelId"]))
                    .or_else(|| current_model.clone()),
                tokens,
                source_cost_usd: usage.pointer("/cost/total").and_then(Value::as_f64),
                dedupe_confidence: "exact",
                conservative_undercount: false,
                sidechain: false,
                source_order: index,
            });
        }
    }
    Ok(())
}

fn scan_opencode(out: &mut Vec<UsageEvent>, warnings: &mut Vec<String>) -> Result<()> {
    let roots: Vec<PathBuf> = std::env::var_os("OPENCODE_DATA_DIR")
        .map(|v| {
            v.to_string_lossy()
                .split(',')
                .map(|s| PathBuf::from(s.trim()))
                .collect()
        })
        .unwrap_or_else(|| vec![home().join(".local/share/opencode")]);
    for root in roots {
        let mut database_ids = HashSet::new();
        let mut databases = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&root) {
            databases.extend(entries.flatten().map(|entry| entry.path()).filter(|path| {
                path.extension().and_then(|v| v.to_str()) == Some("db")
                    && path
                        .file_name()
                        .and_then(|v| v.to_str())
                        .is_some_and(|n| n.starts_with("opencode"))
            }));
        }
        for database in databases {
            if let Err(error) = scan_opencode_db(&database, out, &mut database_ids) {
                warnings.push(format!("{}: {error:#}", database.display()));
            }
        }
        let message_root = root.join("storage/message");
        if message_root.exists() {
            for entry in WalkDir::new(message_root)
                .into_iter()
                .flatten()
                .filter(|e| {
                    e.file_type().is_file()
                        && e.path().extension().and_then(|v| v.to_str()) == Some("json")
                })
            {
                let value: Value = match File::open(entry.path())
                    .ok()
                    .and_then(|file| serde_json::from_reader(file).ok())
                {
                    Some(value) => value,
                    None => continue,
                };
                let id = str_at(&value, &["id"]).or_else(|| {
                    entry
                        .path()
                        .file_stem()
                        .and_then(|n| n.to_str())
                        .map(str::to_string)
                });
                if id.as_ref().is_some_and(|id| database_ids.contains(id)) {
                    continue;
                }
                push_opencode_event(&value, entry.path(), id, out);
            }
        }
    }
    Ok(())
}

fn scan_opencode_db(
    path: &Path,
    out: &mut Vec<UsageEvent>,
    ids: &mut HashSet<String>,
) -> Result<()> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(std::time::Duration::from_secs(1))?;
    let mut stmt = conn.prepare("SELECT id, session_id, data FROM message")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    for row in rows {
        let (id, session, data) = row?;
        let Ok(mut value) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        if value.get("sessionID").is_none()
            && let Some(object) = value.as_object_mut()
        {
            object.insert(
                "sessionID".into(),
                session.map(Value::String).unwrap_or(Value::Null),
            );
        }
        let before = out.len();
        if !ids.contains(&id) {
            push_opencode_event(&value, path, Some(id.clone()), out);
        }
        if out.len() > before {
            ids.insert(id);
        }
    }
    Ok(())
}

fn push_opencode_event(value: &Value, path: &Path, id: Option<String>, out: &mut Vec<UsageEvent>) {
    let Some(usage) = value.get("tokens") else {
        return;
    };
    let reasoning = u64_at(usage, &["reasoning"]);
    let mut tokens = TokenBuckets::disjoint(
        u64_at(usage, &["input"]),
        usage
            .pointer("/cache/read")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        usage
            .pointer("/cache/write")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        u64_at(usage, &["output"]).saturating_add(reasoning),
    );
    // OpenCode persists reasoning separately from output. Fold it into the
    // additive output bucket while retaining the detail field for reporting.
    tokens.reasoning = reasoning;
    if tokens.additive_total() == 0 {
        return;
    }
    out.push(UsageEvent {
        source: "opencode".into(),
        source_path: path.to_string_lossy().to_string(),
        source_record_id: id.clone(),
        session_id: str_at(value, &["sessionID", "session_id"]),
        request_id: None,
        message_id: id,
        timestamp_ms: value
            .pointer("/time/created")
            .map(timestamp_ms)
            .unwrap_or(0),
        // Ingestion currently groups all OpenCode sessions under this
        // synthetic project, so usage must use the same attribution.
        project: Some(SourceKind::Opencode.label().to_string()),
        provider: str_at(value, &["providerID", "provider"]),
        model: str_at(value, &["modelID", "model"]),
        tokens,
        source_cost_usd: value.get("cost").and_then(Value::as_f64),
        dedupe_confidence: "exact",
        conservative_undercount: false,
        sidechain: false,
        source_order: 0,
    });
}

fn scan_cursor(out: &mut Vec<UsageEvent>, warnings: &mut Vec<String>) -> Result<()> {
    let user = if cfg!(target_os = "macos") {
        home().join("Library/Application Support/Cursor/User")
    } else {
        home().join(".config/Cursor/User")
    };
    let mut databases = vec![user.join("globalStorage/state.vscdb")];
    databases.extend(
        WalkDir::new(user.join("workspaceStorage"))
            .max_depth(3)
            .into_iter()
            .flatten()
            .filter(|e| e.file_type().is_file() && e.file_name() == "state.vscdb")
            .map(|e| e.path().to_path_buf()),
    );
    let mut seen = HashSet::new();
    let project_by_session = cursor_project_by_session();
    for path in databases.into_iter().filter(|path| path.exists()) {
        if let Err(error) = scan_cursor_db(&path, out, &mut seen, &project_by_session) {
            warnings.push(format!("{}: {error:#}", path.display()));
        }
    }
    Ok(())
}

fn cursor_project_by_session() -> HashMap<String, String> {
    let root = crate::ingest::cursor_projects_root();
    let mut projects = HashMap::new();
    for entry in WalkDir::new(root).into_iter().flatten().filter(|entry| {
        entry.file_type().is_file()
            && entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl")
    }) {
        let path = entry.path();
        let project = crate::ingest::project_from_cursor_path(path);
        projects.insert(
            crate::ingest::cursor_session_id_from_path(path),
            project.clone(),
        );
        projects.insert(crate::ingest::cursor_transcript_id(path), project);
    }
    projects
}

fn scan_cursor_db(
    path: &Path,
    out: &mut Vec<UsageEvent>,
    seen: &mut HashSet<String>,
    project_by_session: &HashMap<String, String>,
) -> Result<()> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(std::time::Duration::from_secs(1))?;
    for (table, query) in [
        (
            "cursorDiskKV",
            "SELECT key, value FROM cursorDiskKV WHERE key LIKE 'composerData:%' OR key LIKE 'bubbleId:%'",
        ),
        (
            "ItemTable",
            "SELECT key, value FROM ItemTable WHERE key IN ('aiService.generations', 'workbench.panel.aichat.view.aichat.chatdata')",
        ),
    ] {
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
            [table],
            |row| row.get(0),
        )?;
        if !exists {
            continue;
        }
        let mut stmt = conn.prepare(query)?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (key, raw) = row?;
            let Ok(value) = serde_json::from_str::<Value>(&raw) else {
                continue;
            };
            extract_cursor_objects(&value, &key, table, path, out, seen, project_by_session);
        }
    }
    Ok(())
}

fn extract_cursor_objects(
    value: &Value,
    fallback_id: &str,
    table: &str,
    path: &Path,
    out: &mut Vec<UsageEvent>,
    seen: &mut HashSet<String>,
    project_by_session: &HashMap<String, String>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                extract_cursor_objects(
                    value,
                    fallback_id,
                    table,
                    path,
                    out,
                    seen,
                    project_by_session,
                );
            }
        }
        Value::Object(object) => {
            let input = nested_u64(object, &["inputTokens", "input_tokens"]);
            let output = nested_u64(object, &["outputTokens", "output_tokens"]);
            let session_id = object_string(object, &["sessionId", "composerId"]).or_else(|| {
                fallback_id
                    .strip_prefix("composerData:")
                    .filter(|id| !id.is_empty())
                    .map(str::to_string)
            });
            let project = session_id
                .as_ref()
                .and_then(|session_id| project_by_session.get(session_id))
                .cloned();
            let counted = input > 0 || output > 0;
            if counted {
                let id = object_string(
                    object,
                    &["generationUUID", "generationId", "bubbleId", "id"],
                )
                .unwrap_or_else(|| {
                    format!(
                        "{fallback_id}:{}:{}",
                        object.get("createdAt").unwrap_or(&Value::Null),
                        input + output
                    )
                });
                let dedupe = format!("{id}:{input}:{output}");
                if seen.insert(dedupe) {
                    out.push(UsageEvent {
                        source: "cursor".into(),
                        source_path: path.to_string_lossy().to_string(),
                        source_record_id: Some(id),
                        session_id,
                        request_id: None,
                        message_id: None,
                        timestamp_ms: object
                            .get("createdAt")
                            .or_else(|| object.get("timestamp"))
                            .map(timestamp_ms)
                            .unwrap_or(0),
                        project,
                        provider: None,
                        model: object_string(object, &["model", "modelName"])
                            .or_else(|| {
                                object
                                    .get("modelInfo")
                                    .and_then(|v| str_at(v, &["modelName"]))
                            })
                            .or_else(|| {
                                object
                                    .get("modelConfig")
                                    .and_then(|v| str_at(v, &["modelName"]))
                            }),
                        tokens: TokenBuckets::disjoint(input, 0, 0, output),
                        source_cost_usd: None,
                        dedupe_confidence: if table == "cursorDiskKV" {
                            "exact"
                        } else {
                            "strong"
                        },
                        conservative_undercount: false,
                        sidechain: false,
                        source_order: 0,
                    });
                }
            }
            for (key, child) in object {
                if counted && matches!(key.as_str(), "usage" | "tokenCount") {
                    continue;
                }
                if child.is_array() || child.is_object() {
                    extract_cursor_objects(
                        child,
                        fallback_id,
                        table,
                        path,
                        out,
                        seen,
                        project_by_session,
                    );
                }
            }
        }
        _ => {}
    }
}

fn nested_u64(object: &Map<String, Value>, aliases: &[&str]) -> u64 {
    aliases
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_u64))
        .or_else(|| {
            object.get("tokenCount").and_then(|v| {
                aliases
                    .iter()
                    .find_map(|key| v.get(*key).and_then(Value::as_u64))
            })
        })
        .or_else(|| {
            object.get("usage").and_then(|v| {
                aliases
                    .iter()
                    .find_map(|key| v.get(*key).and_then(Value::as_u64))
            })
        })
        .unwrap_or(0)
}

fn object_string(object: &Map<String, Value>, aliases: &[&str]) -> Option<String> {
    aliases
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
        .map(str::to_string)
}

fn scan_copilot(out: &mut Vec<UsageEvent>, _warnings: &mut Vec<String>) -> Result<()> {
    let mut roots = vec![home().join(".copilot/otel")];
    if let Some(path) = std::env::var_os("COPILOT_OTEL_FILE_EXPORTER_PATH") {
        roots.push(PathBuf::from(path));
    }
    let files = roots
        .into_iter()
        .flat_map(|root| {
            if root.is_file() {
                vec![root]
            } else {
                jsonl_files([root])
            }
        })
        .collect::<Vec<_>>();
    let mut seen = HashSet::new();
    for path in files {
        for (index, value) in lines(&path)? {
            extract_otel(&value, &path, index, out, &mut seen);
        }
    }
    Ok(())
}

fn extract_otel(
    value: &Value,
    path: &Path,
    index: u64,
    out: &mut Vec<UsageEvent>,
    seen: &mut HashSet<String>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                extract_otel(value, path, index, out, seen);
            }
        }
        Value::Object(object) => {
            let attrs = otel_attributes(object.get("attributes").unwrap_or(value));
            let operation = attrs
                .get("gen_ai.operation.name")
                .and_then(Value::as_str)
                .or_else(|| object.get("name").and_then(Value::as_str));
            let input = attrs
                .get("gen_ai.usage.input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let output = attrs
                .get("gen_ai.usage.output_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if operation == Some("chat") && (input > 0 || output > 0) {
                let trace = object
                    .get("traceId")
                    .or_else(|| object.get("trace_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let span = object
                    .get("spanId")
                    .or_else(|| object.get("span_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let response = attrs
                    .get("gen_ai.response.id")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let id = if !trace.is_empty() || !span.is_empty() {
                    format!("{trace}:{span}")
                } else if !response.is_empty() {
                    format!("response:{response}")
                } else {
                    format!("{}:{index}:{input}:{output}", path.display())
                };
                if seen.insert(id.clone()) {
                    let cache = attrs
                        .get("gen_ai.usage.cache_read.input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        .min(input);
                    out.push(UsageEvent {
                        source: "copilot".into(),
                        source_path: path.to_string_lossy().to_string(),
                        source_record_id: Some(id),
                        session_id: attrs
                            .get("gen_ai.conversation.id")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        request_id: attrs
                            .get("gen_ai.response.id")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        message_id: None,
                        timestamp_ms: object
                            .get("startTimeUnixNano")
                            .and_then(Value::as_str)
                            .and_then(|v| v.parse::<u64>().ok())
                            .map(|v| v / 1_000_000)
                            .or_else(|| object.get("timestamp").map(timestamp_ms))
                            .unwrap_or(0),
                        project: attrs
                            .get("copilot_chat.repo.remote_url")
                            .or_else(|| attrs.get("github.copilot.git.repository"))
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        provider: Some("github-copilot".into()),
                        model: attrs
                            .get("gen_ai.response.model")
                            .or_else(|| attrs.get("gen_ai.request.model"))
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        tokens: TokenBuckets::codex(
                            input,
                            cache,
                            output,
                            attrs
                                .get("gen_ai.usage.reasoning.output_tokens")
                                .or_else(|| attrs.get("gen_ai.usage.reasoning_tokens"))
                                .and_then(Value::as_u64)
                                .unwrap_or(0),
                        ),
                        source_cost_usd: None,
                        dedupe_confidence: "exact",
                        conservative_undercount: false,
                        sidechain: false,
                        source_order: index,
                    });
                }
            }
            for child in object.values() {
                if child.is_array() || child.is_object() {
                    extract_otel(child, path, index, out, seen);
                }
            }
        }
        _ => {}
    }
}

fn otel_attributes(value: &Value) -> HashMap<String, Value> {
    if let Some(object) = value.as_object() {
        return object
            .iter()
            .map(|(k, v)| (k.clone(), unwrap_otel_value(v)))
            .collect();
    }
    let mut out = HashMap::new();
    if let Some(array) = value.as_array() {
        for item in array {
            if let (Some(key), Some(value)) =
                (item.get("key").and_then(Value::as_str), item.get("value"))
            {
                out.insert(key.to_string(), unwrap_otel_value(value));
            }
        }
    }
    out
}

fn unwrap_otel_value(value: &Value) -> Value {
    for key in ["stringValue", "intValue", "doubleValue", "boolValue"] {
        if let Some(inner) = value.get(key) {
            if key == "intValue"
                && let Some(text) = inner.as_str()
            {
                return text
                    .parse::<u64>()
                    .map(Value::from)
                    .unwrap_or_else(|_| inner.clone());
            }
            return inner.clone();
        }
    }
    value.clone()
}

// Rates are nano-USD per million tokens. The catalog is deliberately small and versioned:
// unknown models remain unpriced instead of silently inheriting a guessed family rate.
const PRICE_CATALOG_ID: &str = "official-api-prices-2026-07-15";

#[derive(Clone, Copy)]
struct Rates {
    input: u64,
    cache_read: u64,
    cache_write_5m: u64,
    cache_write_1h: u64,
    output: u64,
}

const fn usd_per_million(value_milli_usd: u64) -> u64 {
    value_milli_usd * 1_000_000
}

fn event_cost_nanos(event: &UsageEvent, mode: CostMode) -> Option<u64> {
    let source = event
        .source_cost_usd
        .filter(|value| value.is_finite() && *value >= 0.0)
        .and_then(|value| {
            let nanos = value * 1_000_000_000.0;
            (nanos <= u64::MAX as f64).then_some(nanos.round() as u64)
        });
    match mode {
        CostMode::Source => source,
        CostMode::Auto => source.or_else(|| calculated_cost_nanos(event)),
        CostMode::Reprice => calculated_cost_nanos(event),
    }
}

fn calculated_cost_nanos(event: &UsageEvent) -> Option<u64> {
    let rates = rates_for(event.provider.as_deref(), event.model.as_deref()?)?;
    let cache_write_1h = event.tokens.cache_write_1h.min(event.tokens.cache_write);
    let cache_write_5m = event.tokens.cache_write.saturating_sub(cache_write_1h);
    let total = (event.tokens.uncached_input as u128) * (rates.input as u128)
        + (event.tokens.cache_read as u128) * (rates.cache_read as u128)
        + (cache_write_5m as u128) * (rates.cache_write_5m as u128)
        + (cache_write_1h as u128) * (rates.cache_write_1h as u128)
        + (event.tokens.output as u128) * (rates.output as u128);
    // Rates are per million tokens. Reasoning is retained as an output subset and is not charged
    // a second time.
    u64::try_from(total / 1_000_000).ok()
}

fn rates_for(provider: Option<&str>, model: &str) -> Option<Rates> {
    let model = model.trim().to_ascii_lowercase();
    let provider = provider.unwrap_or("").trim().to_ascii_lowercase();
    let exact_or_snapshot = |base: &str| {
        model == base
            || model.strip_prefix(base).is_some_and(|suffix| {
                suffix.starts_with("-20")
                    && suffix[1..].chars().all(|c| c.is_ascii_digit() || c == '-')
            })
    };

    let openai = provider.is_empty()
        || provider.contains("openai")
        || provider.contains("codex")
        || provider.contains("github-copilot");
    if openai {
        if exact_or_snapshot("gpt-5.5") {
            return Some(openai_rates(5_000, 500, 30_000));
        }
        if exact_or_snapshot("gpt-5.4") {
            return Some(openai_rates(2_500, 250, 15_000));
        }
        if exact_or_snapshot("gpt-5.4-mini") {
            return Some(openai_rates(750, 75, 4_500));
        }
        if exact_or_snapshot("gpt-5.3-codex") || exact_or_snapshot("gpt-5.2-codex") {
            return Some(openai_rates(1_750, 175, 14_000));
        }
        if exact_or_snapshot("gpt-5-codex") || exact_or_snapshot("gpt-5") {
            return Some(openai_rates(1_250, 125, 10_000));
        }
        if exact_or_snapshot("gpt-4o") {
            return Some(openai_rates(2_500, 1_250, 10_000));
        }
        if exact_or_snapshot("gpt-4o-mini") {
            return Some(openai_rates(150, 75, 600));
        }
    }

    let anthropic = provider.is_empty() || provider.contains("anthropic");
    if anthropic {
        if [
            "claude-opus-4-8",
            "claude-opus-4-7",
            "claude-opus-4-6",
            "claude-opus-4-5",
        ]
        .iter()
        .any(|base| exact_or_snapshot(base))
        {
            return Some(claude_rates(5_000, 6_250, 10_000, 500, 25_000));
        }
        if exact_or_snapshot("claude-opus-4-1") || exact_or_snapshot("claude-opus-4") {
            return Some(claude_rates(15_000, 18_750, 30_000, 1_500, 75_000));
        }
        if exact_or_snapshot("claude-sonnet-5") {
            // Promotional rate valid on the catalog's 2026-07-15 effective date.
            return Some(claude_rates(2_000, 2_500, 4_000, 200, 10_000));
        }
        if ["claude-sonnet-4-6", "claude-sonnet-4-5", "claude-sonnet-4"]
            .iter()
            .any(|base| exact_or_snapshot(base))
        {
            return Some(claude_rates(3_000, 3_750, 6_000, 300, 15_000));
        }
        if exact_or_snapshot("claude-haiku-4-5") {
            return Some(claude_rates(1_000, 1_250, 2_000, 100, 5_000));
        }
    }
    None
}

fn openai_rates(input: u64, cached: u64, output: u64) -> Rates {
    Rates {
        input: usd_per_million(input),
        cache_read: usd_per_million(cached),
        cache_write_5m: usd_per_million(input),
        cache_write_1h: usd_per_million(input),
        output: usd_per_million(output),
    }
}

fn claude_rates(input: u64, write_5m: u64, write_1h: u64, read: u64, output: u64) -> Rates {
    Rates {
        input: usd_per_million(input),
        cache_read: usd_per_million(read),
        cache_write_5m: usd_per_million(write_5m),
        cache_write_1h: usd_per_million(write_1h),
        output: usd_per_million(output),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(input: u64, cached: u64, output: u64) -> CodexTokens {
        CodexTokens {
            input,
            cached,
            output,
            reasoning: 0,
        }
    }

    #[test]
    fn codex_repeated_total_does_not_repeat_last() {
        let mut counter = CodexCounter::default();
        assert_eq!(
            counter.account(Some(c(100, 20, 10)), Some(c(100, 20, 10))),
            c(100, 20, 10)
        );
        assert_eq!(
            counter.account(Some(c(100, 20, 10)), Some(c(100, 20, 10))),
            c(0, 0, 0)
        );
    }

    #[test]
    fn codex_interleaved_stream_never_recounts_high_water_gap() {
        let mut counter = CodexCounter::default();
        assert_eq!(counter.account(None, Some(c(1000, 0, 0))), c(1000, 0, 0));
        assert_eq!(
            counter.account(Some(c(200, 0, 0)), Some(c(200, 0, 0))),
            c(0, 0, 0)
        );
        assert_eq!(
            counter.account(Some(c(900, 0, 0)), Some(c(1100, 0, 0))),
            c(100, 0, 0)
        );
        assert_eq!(counter.counted.input, 1100);
    }

    #[test]
    fn openai_cached_input_is_a_subset() {
        let tokens = TokenBuckets::codex(100, 80, 10, 4);
        assert_eq!(tokens.uncached_input, 20);
        assert_eq!(tokens.cache_read, 80);
        assert_eq!(tokens.additive_total(), 110);
    }

    #[test]
    fn claude_scanner_caches_normalized_usage_by_file_metadata() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        let transcript = projects.join("session.jsonl");
        std::fs::write(
            &transcript,
            concat!(
                r#"{"type":"assistant","sessionId":"session","requestId":"request","timestamp":"2026-07-03T01:02:05Z","cwd":"/repo/memex","costUSD":"invalid optional value","message":{"id":"message","model":"claude-sonnet-4-6","content":[{"type":"text","text":"ignored payload"}],"usage":{"inputTokens":10,"cacheReadInputTokens":2,"cacheCreationInputTokens":3,"outputTokens":4,"cache_creation":{"ephemeral_1h_input_tokens":1}}}}"#,
                "\n"
            ),
        )
        .expect("write transcript");
        let cache_path = tmp.path().join("usage-cache.sqlite3");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Claude),
            include_events: true,
            cache_path: Some(cache_path.clone()),
            ..UsageQuery::default()
        };

        let cold = scan_usage(&query).expect("cold scan");
        let warm = scan_usage(&query).expect("warm scan");
        let cache = Connection::open(cache_path).expect("open cache");
        let cached_files: u64 = cache
            .query_row("SELECT count(*) FROM claude_usage_file_cache", [], |row| {
                row.get(0)
            })
            .expect("count cached files");

        assert_eq!(cold.events, 1);
        assert_eq!(cold.details[0].tokens.total(), 19);
        assert_eq!(cold.details[0].tokens.cache_write_1h, 1);
        assert_eq!(cold.details[0].dedupe_confidence, "exact");
        assert_eq!(warm.total_tokens, cold.total_tokens);
        assert_eq!(cached_files, 1);
    }

    #[test]
    fn opencode_reasoning_is_included_in_output_and_total() {
        let value = serde_json::json!({
            "path": { "cwd": "/repo/memex" },
            "tokens": {
                "input": 100,
                "output": 20,
                "reasoning": 30,
                "cache": { "read": 40, "write": 10 }
            }
        });
        let mut events = Vec::new();

        push_opencode_event(
            &value,
            Path::new("message.json"),
            Some("message".into()),
            &mut events,
        );

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tokens.reasoning, 30);
        assert_eq!(events[0].tokens.output, 50);
        assert_eq!(events[0].tokens.total(), 200);
        assert_eq!(events[0].project.as_deref(), Some("opencode"));
    }

    #[test]
    fn opencode_project_filter_matches_indexed_project() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let message_dir = tmp.path().join("storage/message/ses_test");
        std::fs::create_dir_all(&message_dir).expect("create message directory");
        std::fs::write(
            message_dir.join("msg_test.json"),
            serde_json::to_vec(&serde_json::json!({
                "id": "msg_test",
                "sessionID": "ses_test",
                "path": { "cwd": "/repo/memex" },
                "time": { "created": 1_750_000_000_000u64 },
                "tokens": {
                    "input": 10,
                    "output": 5,
                    "reasoning": 0,
                    "cache": { "read": 0, "write": 0 }
                }
            }))
            .expect("serialize message"),
        )
        .expect("write message");
        let _env = EnvVarGuard::set_os(&[("OPENCODE_DATA_DIR", Some(tmp.path().as_os_str()))]);
        let mut query = UsageQuery {
            source: Some(SourceFilter::Opencode),
            project: Some("opencode".into()),
            project_grouping: ProjectGrouping::Flat,
            include_events: true,
            ..UsageQuery::default()
        };

        let matching = scan_usage(&query).expect("scan matching project");
        query.project = Some("memex".into());
        let mismatched = scan_usage(&query).expect("scan mismatched project");
        query.project = Some("opencode".into());
        query.session_keys = Some(HashSet::from([("opencode".into(), "ses_test".into())]));
        let matching_session = scan_usage(&query).expect("scan matching session");
        query.session_keys = Some(HashSet::from([("opencode".into(), "ses_other".into())]));
        let mismatched_session = scan_usage(&query).expect("scan mismatched session");

        assert_eq!(matching.events, 1);
        assert_eq!(matching.details[0].project.as_deref(), Some("opencode"));
        assert_eq!(mismatched.events, 0);
        assert_eq!(matching_session.events, 1);
        assert_eq!(mismatched_session.events, 0);
    }

    #[test]
    fn cursor_nested_token_containers_are_not_recounted() {
        let value = serde_json::json!([
            {
                "generationUUID": "usage-parent",
                "composerId": "composer-main",
                "usage": { "inputTokens": 10, "outputTokens": 5 },
                "children": [
                    {
                        "generationId": "nested-request",
                        "inputTokens": 7,
                        "outputTokens": 3
                    }
                ]
            },
            {
                "generationUUID": "count-parent",
                "tokenCount": { "input_tokens": 20, "output_tokens": 8 }
            }
        ]);
        let mut events = Vec::new();
        let mut seen = HashSet::new();
        let project_by_session =
            HashMap::from([("composer-main".to_string(), "memex".to_string())]);

        extract_cursor_objects(
            &value,
            "fixture",
            "cursorDiskKV",
            Path::new("state.vscdb"),
            &mut events,
            &mut seen,
            &project_by_session,
        );

        assert_eq!(events.len(), 3);
        let ids: HashSet<_> = events
            .iter()
            .filter_map(|event| event.source_record_id.as_deref())
            .collect();
        assert_eq!(
            ids,
            HashSet::from(["usage-parent", "nested-request", "count-parent"])
        );
        assert_eq!(
            events.iter().map(|event| event.tokens.total()).sum::<u64>(),
            53
        );
        assert_eq!(events[0].project.as_deref(), Some("memex"));
    }

    #[test]
    fn usage_project_matching_normalizes_paths_slugs_and_remotes() {
        let mut cache = HashMap::new();

        for candidate in [
            "/Users/nico/Code/memex",
            "--Users-nico-Code-memex--",
            "git@github.com:nicosuave/memex.git",
        ] {
            assert!(usage_project_matches(
                candidate,
                "memex",
                ProjectGrouping::Flat,
                &mut cache,
            ));
        }
        assert!(!usage_project_matches(
            "/Users/nico/Code/other",
            "memex",
            ProjectGrouping::Flat,
            &mut cache,
        ));
    }

    #[test]
    fn pi_scanner_uses_configured_session_directory() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let agent_root = tmp.path().join("pi-agent");
        let session_root = agent_root.join("custom/sessions/--C--Users-alice-Code-memex--");
        std::fs::create_dir_all(&session_root).expect("create session root");
        std::fs::write(
            agent_root.join("settings.json"),
            r#"{ "sessionDir": "custom/sessions" }"#,
        )
        .expect("write settings");
        std::fs::write(
            session_root.join("session.jsonl"),
            concat!(
                r#"{"type":"message","id":"a1","timestamp":"2026-07-03T01:02:05Z","message":{"role":"assistant","provider":"anthropic","model":"claude-sonnet-4-6","usage":{"input":10,"cacheRead":2,"cacheWrite":3,"output":4}}}"#,
                "\n"
            ),
        )
        .expect("write session");
        let _env = EnvVarGuard::set_os(&[
            ("PI_CODING_AGENT_SESSION_DIR", None),
            ("PI_CODING_AGENT_DIR", Some(agent_root.as_os_str())),
        ]);
        let report = scan_usage(&UsageQuery {
            source: Some(SourceFilter::Pi),
            project: Some("memex".into()),
            project_grouping: ProjectGrouping::Flat,
            include_events: true,
            ..UsageQuery::default()
        })
        .expect("scan pi");

        assert!(report.warnings.is_empty());
        assert_eq!(report.events, 1);
        assert_eq!(report.details[0].tokens.total(), 19);
        assert_eq!(report.details[0].project.as_deref(), Some("memex"));
        assert!(
            report.details[0]
                .source_path
                .ends_with("custom/sessions/--C--Users-alice-Code-memex--/session.jsonl")
        );
    }

    #[test]
    fn pi_scanner_matches_indexed_header_and_filename_session_ids() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let session_root = tmp.path().join("--Users-nico-Code-other--");
        std::fs::create_dir_all(&session_root).expect("create session root");

        let filename_id = "11111111-1111-1111-1111-111111111111";
        let header_id = "22222222-2222-2222-2222-222222222222";
        std::fs::write(
            session_root.join(format!("20260703T010203Z_{filename_id}.jsonl")),
            format!(
                concat!(
                    r#"{{"type":"session","id":"{header_id}","cwd":"/Users/nico/Code/memex"}}"#,
                    "\n",
                    r#"{{"type":"message","id":"a1","timestamp":"2026-07-03T01:02:05Z","message":{{"role":"assistant","usage":{{"input":10,"output":4}}}}}}"#,
                    "\n"
                ),
                header_id = header_id,
            ),
        )
        .expect("write header session");

        let fallback_id = "33333333-3333-3333-3333-333333333333";
        let fallback_stem = format!("20260703T010204Z_{fallback_id}");
        std::fs::write(
            session_root.join(format!("{fallback_stem}.jsonl")),
            concat!(
                r#"{"type":"message","id":"a2","timestamp":"2026-07-03T01:02:06Z","message":{"role":"assistant","usage":{"input":20,"output":5}}}"#,
                "\n"
            ),
        )
        .expect("write filename session");

        let _env =
            EnvVarGuard::set_os(&[("PI_CODING_AGENT_SESSION_DIR", Some(tmp.path().as_os_str()))]);
        let mut query = UsageQuery {
            source: Some(SourceFilter::Pi),
            include_events: true,
            ..UsageQuery::default()
        };

        query.session_keys = Some(HashSet::from([("pi".into(), header_id.into())]));
        let header = scan_usage(&query).expect("scan header session");
        query.session_keys = Some(HashSet::from([("pi".into(), filename_id.into())]));
        let overridden_filename = scan_usage(&query).expect("scan overridden filename session");
        query.session_keys = Some(HashSet::from([("pi".into(), fallback_id.into())]));
        let fallback = scan_usage(&query).expect("scan filename session");
        query.session_keys = Some(HashSet::from([("pi".into(), fallback_stem)]));
        let full_stem = scan_usage(&query).expect("scan full filename stem");

        assert_eq!(header.events, 1);
        assert_eq!(header.details[0].session_id.as_deref(), Some(header_id));
        assert_eq!(header.details[0].project.as_deref(), Some("memex"));
        assert_eq!(overridden_filename.events, 0);
        assert_eq!(fallback.events, 1);
        assert_eq!(fallback.details[0].session_id.as_deref(), Some(fallback_id));
        assert_eq!(full_stem.events, 0);
    }

    #[test]
    fn claude_cache_write_durations_get_distinct_rates() {
        let mut tokens = TokenBuckets::disjoint(100, 40, 30, 20);
        tokens.cache_write_1h = 10;
        let event = UsageEvent {
            source: "claude".into(),
            source_path: "x".into(),
            source_record_id: None,
            session_id: None,
            request_id: None,
            message_id: None,
            timestamp_ms: 0,
            project: None,
            provider: Some("anthropic".into()),
            model: Some("claude-sonnet-4-6".into()),
            tokens,
            source_cost_usd: None,
            dedupe_confidence: "exact",
            conservative_undercount: false,
            sidechain: false,
            source_order: 0,
        };
        // 100*3 + 40*.3 + 20*3.75 + 10*6 + 20*15 = $0.000747
        assert_eq!(calculated_cost_nanos(&event), Some(747_000));
    }

    #[test]
    fn auto_cost_honors_explicit_zero_source_cost() {
        let event = UsageEvent {
            source: "claude".into(),
            source_path: "x".into(),
            source_record_id: None,
            session_id: None,
            request_id: None,
            message_id: None,
            timestamp_ms: 0,
            project: None,
            provider: Some("anthropic".into()),
            model: Some("claude-sonnet-4-6".into()),
            tokens: TokenBuckets::disjoint(100, 0, 0, 0),
            source_cost_usd: Some(0.0),
            dedupe_confidence: "exact",
            conservative_undercount: false,
            sidechain: false,
            source_order: 0,
        };
        assert_eq!(event_cost_nanos(&event, CostMode::Auto), Some(0));
        assert_eq!(event_cost_nanos(&event, CostMode::Reprice), Some(300_000));
    }
}
