//! Reconstructed local token usage.
//!
//! This module intentionally does not model provider quota percentages. Local logs are useful for
//! request-level accounting, but they are not authoritative subscription-limit telemetry.

use crate::analytics::ProjectGrouping;
use crate::types::{SourceFilter, SourceKind};
use anyhow::{Context, Result};
use chrono::DateTime;
use clap::ValueEnum;
use memchr::memmem;
use once_cell::sync::Lazy;
use rayon::prelude::*;
use rusqlite::{Connection, OpenFlags, params};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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
    /// Reuse the previous in-process scan result when it is at most this old. Filters
    /// (`since_ms`, `project`, `session_keys`, ...) apply after assembly, so repeated
    /// queries over the same corpus can share one scan. Zero disables the memo.
    pub memo_ttl_ms: u64,
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
    let _scan_guard = USAGE_SCAN_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (assembled, warnings) = memoized_usage_events(query);
    let mut project_cache = HashMap::new();
    // Assembled events are already sorted; filtering preserves that order.
    let events: Vec<&UsageEvent> = assembled
        .iter()
        .filter(|event| {
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
        })
        .collect();

    let mut by_source: HashMap<String, UsageSummary> = HashMap::new();
    let mut report = UsageReport {
        authority: "local_log",
        cost_mode: query.cost_mode,
        price_catalog: PRICE_CATALOG_ID,
        warnings: warnings.as_ref().clone(),
        ..UsageReport::default()
    };
    for event in events.iter().copied() {
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
        report.details = events.into_iter().cloned().collect();
    }
    Ok(report)
}

struct UsageMemo {
    key: (Option<SourceFilter>, Option<PathBuf>),
    built: Instant,
    events: Arc<Vec<UsageEvent>>,
    warnings: Arc<Vec<String>>,
}

static USAGE_MEMO: Lazy<Mutex<Option<UsageMemo>>> = Lazy::new(|| Mutex::new(None));

/// Returns the assembled (pre-filter) events, reusing the previous in-process assembly
/// when the query opts into a memo TTL. Callers must hold `USAGE_SCAN_LOCK`.
fn memoized_usage_events(query: &UsageQuery) -> (Arc<Vec<UsageEvent>>, Arc<Vec<String>>) {
    let key = (query.source, query.cache_path.clone());
    let ttl = Duration::from_millis(query.memo_ttl_ms);
    if !ttl.is_zero()
        && let Some(memo) = USAGE_MEMO
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
        && memo.key == key
        && memo.built.elapsed() < ttl
    {
        return (memo.events.clone(), memo.warnings.clone());
    }
    let (events, warnings) = assemble_usage_events(query.source, query.cache_path.as_deref());
    // Stamp the memo after assembly: an assembly slower than the TTL would otherwise be
    // expired the moment it finishes, and queued follow-up queries would reassemble.
    let built = Instant::now();
    let events = Arc::new(events);
    let warnings = Arc::new(warnings);
    if !ttl.is_zero() {
        *USAGE_MEMO
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(UsageMemo {
            key,
            built,
            events: events.clone(),
            warnings: warnings.clone(),
        });
    }
    (events, warnings)
}

fn assemble_usage_events(
    source: Option<SourceFilter>,
    cache_path: Option<&Path>,
) -> (Vec<UsageEvent>, Vec<String>) {
    let mut events = Vec::new();
    let mut warnings = Vec::new();
    let mut cache = match cache_path.map(UsageCache::open).transpose() {
        Ok(cache) => cache,
        Err(error) => {
            warnings.push(format!("usage cache disabled: {error:#}"));
            None
        }
    };
    type SourceScanner =
        fn(&mut Vec<UsageEvent>, &mut Vec<String>, Option<&mut UsageCache>) -> Result<()>;
    const SCANNERS: [(SourceFilter, SourceScanner); 6] = [
        (SourceFilter::Claude, scan_claude),
        (SourceFilter::Codex, scan_codex),
        (SourceFilter::Opencode, scan_opencode),
        (SourceFilter::Pi, scan_pi),
        (SourceFilter::Cursor, scan_cursor),
        (SourceFilter::Copilot, scan_copilot),
    ];
    for (filter, scanner) in SCANNERS {
        if source.is_none_or(|selected| selected == filter)
            && let Err(error) = scanner(&mut events, &mut warnings, cache.as_mut())
        {
            warnings.push(format!("{} scanner: {error:#}", filter.as_str()));
        }
    }
    publish_scan_progress(None);

    reconcile_claude(&mut events);
    reconcile_codex_copies(&mut events);
    reconcile_cursor_copies(&mut events);
    reconcile_copilot_copies(&mut events);
    reconcile_opencode_copies(&mut events);
    events.par_sort_by(|a, b| {
        (a.timestamp_ms, &a.source_path, a.source_order).cmp(&(
            b.timestamp_ms,
            &b.source_path,
            b.source_order,
        ))
    });
    (events, warnings)
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
    // Claude Code 2.1.210+ writes BOTH spellings on one line; a serde `alias` would
    // reject that as a duplicate field and silently drop the event, so each spelling
    // gets its own field and they are merged at use sites.
    #[serde(
        rename = "sessionId",
        default,
        deserialize_with = "deserialize_optional_string"
    )]
    session_id_camel: Option<String>,
    #[serde(
        rename = "session_id",
        default,
        deserialize_with = "deserialize_optional_string"
    )]
    session_id_snake: Option<String>,
    #[serde(
        rename = "requestId",
        default,
        deserialize_with = "deserialize_optional_string"
    )]
    request_id_camel: Option<String>,
    #[serde(
        rename = "request_id",
        default,
        deserialize_with = "deserialize_optional_string"
    )]
    request_id_snake: Option<String>,
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

// Parser versions also cover the cached blob encoding (postcard); bump them when either
// the per-source parsing or `CachedUsageEvent` changes shape.
const CLAUDE_PARSER_VERSION: i64 = 4;
const CODEX_PARSER_VERSION: i64 = 4;
const PI_PARSER_VERSION: i64 = 2;
const CURSOR_PARSER_VERSION: i64 = 2;
const COPILOT_PARSER_VERSION: i64 = 2;
const OPENCODE_PARSER_VERSION: i64 = 2;
/// Reuse cached Cursor state databases this long even when their metadata changed: a
/// running Cursor rewrites its (potentially multi-GB) databases continuously, and
/// re-reading them on every scan makes live scans unusable.
const VOLATILE_DB_REUSE_MS: i64 = 60_000;
/// Cache rows are persisted after every chunk of parsed files, not once per source, so an
/// interrupted cold scan resumes from the last completed chunk instead of starting over.
const PARSE_SAVE_CHUNK: usize = 128;
static USAGE_SCAN_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Parse-phase progress of the usage scan currently holding `USAGE_SCAN_LOCK`. Cache hits
/// are not counted: progress is only published while files are being (re)parsed, which is
/// the phase that can take minutes on a cold cache.
#[derive(Clone, Copy, Debug)]
pub struct UsageScanProgress {
    pub source: &'static str,
    pub done: usize,
    pub total: usize,
}

static USAGE_SCAN_PROGRESS: Lazy<Mutex<Option<UsageScanProgress>>> = Lazy::new(|| Mutex::new(None));

pub fn usage_scan_progress() -> Option<UsageScanProgress> {
    *USAGE_SCAN_PROGRESS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn publish_scan_progress(progress: Option<UsageScanProgress>) {
    *USAGE_SCAN_PROGRESS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = progress;
}

fn bump_scan_progress() {
    if let Some(progress) = USAGE_SCAN_PROGRESS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_mut()
    {
        progress.done += 1;
    }
}

#[derive(Serialize, Deserialize)]
struct CachedUsageEvent {
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
    dedupe_confidence: String,
    conservative_undercount: bool,
    sidechain: bool,
    source_order: u64,
}

impl CachedUsageEvent {
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
            dedupe_confidence: event.dedupe_confidence.to_string(),
            conservative_undercount: event.conservative_undercount,
            sidechain: event.sidechain,
            source_order: event.source_order,
        }
    }

    fn into_event(self, source: &'static str, source_path: String) -> UsageEvent {
        UsageEvent {
            source: source.into(),
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
            dedupe_confidence: match self.dedupe_confidence.as_str() {
                "exact" => "exact",
                "strong" => "strong",
                _ => "heuristic",
            },
            conservative_undercount: self.conservative_undercount,
            sidechain: self.sidechain,
            source_order: self.source_order,
        }
    }
}

struct UsageCache {
    connection: Connection,
}

struct CachedFileRow {
    size: u64,
    mtime_ns: i64,
    scanned_at_ms: i64,
    events_blob: Vec<u8>,
    deps: Vec<UsageFileDep>,
}

/// A parse closure's output: the file's events plus whether the result may be persisted.
/// `cacheable` is false when parsing depended on state outside the file that can change
/// while the file itself does not — e.g. a codex fork whose parent rollout was not yet on
/// disk, so its baseline was guessed and must be recomputed once the parent appears.
struct FileParse {
    events: Vec<UsageEvent>,
    cacheable: bool,
    /// Other files this parse's result depends on; a change to any invalidates the cache.
    deps: Vec<UsageFileDep>,
}

impl FileParse {
    fn cacheable(events: Vec<UsageEvent>) -> Self {
        Self {
            events,
            cacheable: true,
            deps: Vec::new(),
        }
    }
}

struct ParsedUsageFile {
    index: usize,
    path: PathBuf,
    size: u64,
    mtime_ns: i64,
    events: Vec<UsageEvent>,
    cacheable: bool,
    deps: Vec<UsageFileDep>,
}

impl UsageCache {
    fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(2))?;
        // Drop pre-postcard cache tables and any schema missing a required column: the
        // JSON-era claude table, the pre-rename blob column, and the deps_blob column that
        // records cross-file dependencies. A missing column means an older layout, so the
        // table is rebuilt rather than migrated.
        let current_columns: i64 = connection.query_row(
            "SELECT count(*) FROM pragma_table_info('usage_file_cache')
             WHERE name IN ('events_blob', 'deps_blob')",
            [],
            |row| row.get(0),
        )?;
        if current_columns < 2 {
            connection.execute_batch("DROP TABLE IF EXISTS usage_file_cache;")?;
        }
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             DROP TABLE IF EXISTS claude_usage_file_cache;
             CREATE TABLE IF NOT EXISTS usage_file_cache (
                 source TEXT NOT NULL,
                 path TEXT NOT NULL,
                 parser_version INTEGER NOT NULL,
                 size INTEGER NOT NULL,
                 mtime_ns INTEGER NOT NULL,
                 scanned_at_ms INTEGER NOT NULL,
                 events_blob BLOB NOT NULL,
                 deps_blob BLOB NOT NULL,
                 PRIMARY KEY (source, path)
             );",
        )?;
        Ok(Self { connection })
    }

    fn load_source(
        &self,
        source: &str,
        parser_version: i64,
    ) -> Result<HashMap<String, CachedFileRow>> {
        self.connection.execute(
            "DELETE FROM usage_file_cache WHERE source = ?1 AND parser_version != ?2",
            params![source, parser_version],
        )?;
        let mut statement = self.connection.prepare(
            "SELECT path, size, mtime_ns, scanned_at_ms, events_blob, deps_blob FROM usage_file_cache
             WHERE source = ?1 AND parser_version = ?2",
        )?;
        let rows = statement.query_map(params![source, parser_version], |row| {
            let deps_blob: Vec<u8> = row.get(5)?;
            Ok((
                row.get::<_, String>(0)?,
                CachedFileRow {
                    size: row.get::<_, i64>(1)? as u64,
                    mtime_ns: row.get(2)?,
                    scanned_at_ms: row.get(3)?,
                    events_blob: row.get(4)?,
                    deps: postcard::from_bytes(&deps_blob).unwrap_or_default(),
                },
            ))
        })?;
        rows.collect::<std::result::Result<HashMap<_, _>, _>>()
            .map_err(Into::into)
    }

    fn delete_stale(&mut self, source: &str, stale_paths: &[String]) -> Result<()> {
        let transaction = self.connection.transaction()?;
        for path in stale_paths {
            transaction.execute(
                "DELETE FROM usage_file_cache WHERE source = ?1 AND path = ?2",
                params![source, path],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn save_batch(
        &mut self,
        source: &str,
        parser_version: i64,
        scanned_at_ms: i64,
        parsed: &[ParsedUsageFile],
    ) -> Result<()> {
        let prepared = parsed
            .iter()
            .filter(|file| file.cacheable)
            .map(|file| {
                let cached = file
                    .events
                    .iter()
                    .map(CachedUsageEvent::from_event)
                    .collect::<Vec<_>>();
                Ok((
                    file.path.to_string_lossy().to_string(),
                    file.size,
                    file.mtime_ns,
                    postcard::to_stdvec(&cached)?,
                    postcard::to_stdvec(&file.deps)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let transaction = self.connection.transaction()?;
        for (path, size, mtime_ns, events_blob, deps_blob) in prepared {
            transaction.execute(
                "INSERT INTO usage_file_cache(
                     source, path, parser_version, size, mtime_ns, scanned_at_ms, events_blob, deps_blob
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(source, path) DO UPDATE SET
                     parser_version = excluded.parser_version,
                     size = excluded.size,
                     mtime_ns = excluded.mtime_ns,
                     scanned_at_ms = excluded.scanned_at_ms,
                     events_blob = excluded.events_blob,
                     deps_blob = excluded.deps_blob",
                params![
                    source,
                    path,
                    parser_version,
                    size as i64,
                    mtime_ns,
                    scanned_at_ms,
                    events_blob,
                    deps_blob
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }
}

fn epoch_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[derive(Clone, Copy)]
struct SourceScan {
    source: &'static str,
    parser_version: i64,
    /// Returns how long cached rows for this path may be reused even if the file metadata
    /// changed, or `None` to always re-parse on change. Used for databases that are
    /// continuously rewritten while their application runs; plain log files must return
    /// `None` so appends are picked up immediately.
    volatile_reuse_ms: fn(&Path) -> Option<i64>,
}

/// Scan `files` through the per-file cache: unchanged files are served from cached blobs
/// (decoded in parallel), changed or new files are re-parsed in parallel, and cache rows
/// for vanished files are dropped. Events are appended to `out` in `files` order.
fn scan_files_cached(
    scan: SourceScan,
    files: &[PathBuf],
    cache: Option<&mut UsageCache>,
    warnings: &mut Vec<String>,
    out: &mut Vec<UsageEvent>,
    parse: impl Fn(&Path) -> Result<FileParse> + Sync,
) {
    scan_files_cached_with(scan, files, cache, warnings, out, parse, |_| true);
}

/// Like `scan_files_cached`, but with a source-specific validity predicate over a cached
/// row's recorded dependencies. `deps_current` runs in addition to each dependency's own
/// metadata check; a source uses it to invalidate cache hits on state that per-file metadata
/// cannot see — e.g. codex forks, whose baseline depends on the *set* of parent rollout
/// copies, so a newly appearing parent copy must invalidate the child even though every
/// already-recorded dependency is still unchanged.
#[allow(clippy::too_many_arguments)]
fn scan_files_cached_with(
    scan: SourceScan,
    files: &[PathBuf],
    cache: Option<&mut UsageCache>,
    warnings: &mut Vec<String>,
    out: &mut Vec<UsageEvent>,
    parse: impl Fn(&Path) -> Result<FileParse> + Sync,
    deps_current: impl Fn(&[UsageFileDep]) -> bool,
) {
    let SourceScan {
        source,
        parser_version,
        volatile_reuse_ms,
    } = scan;
    let now_ms = epoch_ms_now();
    let mut rows = match cache.as_deref() {
        Some(cache) => match cache.load_source(source, parser_version) {
            Ok(rows) => rows,
            Err(error) => {
                warnings.push(format!("{source} usage cache read failed: {error:#}"));
                HashMap::new()
            }
        },
        None => HashMap::new(),
    };
    let mut slots: Vec<Option<Vec<UsageEvent>>> = (0..files.len()).map(|_| None).collect();
    let mut hits: Vec<(usize, String, Vec<u8>)> = Vec::new();
    let mut missing: Vec<(usize, PathBuf, (u64, i64))> = Vec::new();
    for (index, path) in files.iter().enumerate() {
        let metadata = match usage_file_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) => {
                warnings.push(format!(
                    "{source} usage file skipped ({}): {error:#}",
                    path.display()
                ));
                continue;
            }
        };
        let key = path.to_string_lossy().to_string();
        match rows.remove(&key) {
            // A dependency change (e.g. a fork's parent rollout was extended, or a new parent
            // copy appeared) invalidates the cached result even when the file itself is
            // unchanged, so it must re-parse.
            Some(row)
                if ((row.size, row.mtime_ns) == metadata
                    || volatile_reuse_ms(path).is_some_and(|window| {
                        now_ms.saturating_sub(row.scanned_at_ms) < window
                    }))
                    && row.deps.iter().all(UsageFileDep::is_current)
                    && deps_current(&row.deps) =>
            {
                hits.push((index, key, row.events_blob));
            }
            _ => missing.push((index, path.clone(), metadata)),
        }
    }
    let decoded = hits
        .into_par_iter()
        .map(|(index, key, blob)| {
            let events = postcard::from_bytes::<Vec<CachedUsageEvent>>(&blob).map(|events| {
                events
                    .into_iter()
                    .map(|event| event.into_event(source, key.clone()))
                    .collect::<Vec<_>>()
            });
            (index, key, events)
        })
        .collect::<Vec<_>>();
    for (index, key, events) in decoded {
        match events {
            Ok(events) => slots[index] = Some(events),
            // A corrupt cached blob demotes the file to a fresh parse.
            Err(_) => {
                let path = PathBuf::from(&key);
                match usage_file_metadata(&path) {
                    Ok(metadata) => missing.push((index, path, metadata)),
                    Err(error) => warnings.push(format!(
                        "{source} usage file skipped ({}): {error:#}",
                        path.display()
                    )),
                }
            }
        }
    }
    let mut cache = cache;
    let stale_paths: Vec<String> = rows.into_keys().collect();
    if let Some(cache) = cache.as_deref_mut()
        && !stale_paths.is_empty()
        && let Err(error) = cache.delete_stale(source, &stale_paths)
    {
        warnings.push(format!("{source} usage cache write failed: {error:#}"));
    }
    if !missing.is_empty() {
        publish_scan_progress(Some(UsageScanProgress {
            source,
            done: 0,
            total: missing.len(),
        }));
    }
    // Parse and persist in chunks so an interrupted cold scan keeps the chunks it finished;
    // the next scan resumes from there instead of re-parsing the whole source.
    let mut save_warned = false;
    for chunk in missing.chunks(PARSE_SAVE_CHUNK) {
        let parsed = parse_missing_usage_files(source, chunk, warnings, &parse);
        // Unresolved-fork parses (cacheable == false) are excluded from persistence so a
        // later scan re-runs them once their fork parent is available; they still populate
        // `out`.
        if let Some(cache) = cache.as_deref_mut()
            && parsed.iter().any(|file| file.cacheable)
            && let Err(error) = cache.save_batch(source, parser_version, now_ms, &parsed)
            && !save_warned
        {
            save_warned = true;
            warnings.push(format!("{source} usage cache write failed: {error:#}"));
        }
        for file in parsed {
            slots[file.index] = Some(file.events);
        }
    }
    for events in slots.into_iter().flatten() {
        out.extend(events);
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
    cache: Option<&mut UsageCache>,
) -> Result<()> {
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
    scan_files_cached(
        SourceScan {
            source: "claude",
            parser_version: CLAUDE_PARSER_VERSION,
            volatile_reuse_ms: |_| None,
        },
        &files,
        cache,
        warnings,
        out,
        |path| scan_claude_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

fn parse_missing_usage_files(
    source: &str,
    missing: &[(usize, PathBuf, (u64, i64))],
    warnings: &mut Vec<String>,
    parse: &(impl Fn(&Path) -> Result<FileParse> + Sync),
) -> Vec<ParsedUsageFile> {
    let outcomes = missing
        .par_iter()
        .map(|(index, path, metadata)| {
            let outcome = parse(path).map(|parsed| ParsedUsageFile {
                index: *index,
                path: path.clone(),
                size: metadata.0,
                mtime_ns: metadata.1,
                events: parsed.events,
                cacheable: parsed.cacheable,
                deps: parsed.deps,
            });
            bump_scan_progress();
            outcome
        })
        .collect::<Vec<_>>();
    let mut parsed = Vec::with_capacity(outcomes.len());
    for ((_, path, _), outcome) in missing.iter().zip(outcomes) {
        match outcome {
            Ok(file) => parsed.push(file),
            Err(error) => warnings.push(format!(
                "{source} usage file skipped ({}): {error:#}",
                path.display()
            )),
        }
    }
    parsed
}

/// Stream a file line by line as raw bytes, reusing one buffer. Line indices count every
/// line in the file so record ids stay stable across parser changes.
fn for_each_line(path: &Path, mut visit: impl FnMut(u64, &[u8])) -> Result<()> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut line = Vec::with_capacity(16 * 1024);
    let mut index = 0u64;
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            return Ok(());
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        visit(index, &line);
        index += 1;
    }
}

fn scan_claude_file(path: &Path) -> Result<Vec<UsageEvent>> {
    static USAGE_NEEDLE: Lazy<memmem::Finder<'static>> =
        Lazy::new(|| memmem::Finder::new(b"\"usage\""));
    let source_path = path.to_string_lossy().to_string();
    let fallback_session = path
        .file_stem()
        .and_then(|n| n.to_str())
        .map(str::to_string);
    let mut events = Vec::new();
    for_each_line(path, |index, line| {
        if USAGE_NEEDLE.find(line).is_none() {
            return;
        }
        let Ok(value) = serde_json::from_slice::<ClaudeUsageLine>(line) else {
            return;
        };
        if value.kind.as_deref() != Some("assistant") {
            return;
        }
        let Some(message) = value.message else {
            return;
        };
        let Some(usage) = message.usage else {
            return;
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
            return;
        }
        let session_id = value.session_id_camel.or(value.session_id_snake);
        let request_id = value.request_id_camel.or(value.request_id_snake);
        let exact_dedupe = message.id.is_some() && request_id.is_some();
        events.push(UsageEvent {
            source: "claude".into(),
            source_path: source_path.clone(),
            source_record_id: Some(format!("line:{index}")),
            session_id: session_id.or_else(|| fallback_session.clone()),
            request_id,
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
            source_order: index,
        });
    })?;
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
    /// Cumulative totals the fork parent reached before the fork point. Fork files replay
    /// the parent's history with re-stamped timestamps; any total in this set is a replayed
    /// snapshot, not new usage, regardless of event order or replay truncation.
    inherited_seen: HashSet<CodexTokens>,
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

    /// Seeds the counter from the fork parent's snapshots at-or-before the fork timestamp
    /// (the CodexBar inherited-baseline policy): new usage counts as growth beyond the
    /// parent's final pre-fork totals, and every replayed parent snapshot is suppressed.
    /// `snapshots` may be merged from several parent copies and so need not be ordered; the
    /// baseline is the component-wise maximum rather than the last element.
    fn seed_inherited(&mut self, snapshots: &[CodexTokens]) {
        let Some(baseline) = snapshots.iter().copied().reduce(CodexTokens::max) else {
            return;
        };
        self.inherited_seen.extend(snapshots.iter().copied());
        self.raw_baseline = baseline;
        self.watermark = self.watermark.max(baseline);
    }

    fn account(&mut self, last: Option<CodexTokens>, total: Option<CodexTokens>) -> CodexTokens {
        if let Some(total) = total {
            if self.seen.contains(&total) || self.inherited_seen.contains(&total) {
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

/// A file whose content a cached parse depended on, recorded so the cached result can be
/// invalidated when that file changes. Used for codex fork children, whose baseline comes
/// from a separate parent rollout that can change (be synced, extended) without the child
/// file changing.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
struct UsageFileDep {
    path: String,
    size: u64,
    mtime_ns: i64,
}

impl UsageFileDep {
    /// True if the file still matches the recorded size and mtime. A vanished or changed
    /// dependency invalidates the cached parse that depended on it.
    fn is_current(&self) -> bool {
        usage_file_metadata(Path::new(&self.path))
            .map(|(size, mtime_ns)| size == self.size && mtime_ns == self.mtime_ns)
            .unwrap_or(false)
    }
}

struct CodexParentData {
    /// Every rollout file carrying this session id; a child depends on all of them so a
    /// change to any copy re-parses it.
    deps: Vec<UsageFileDep>,
    /// Snapshots merged across all copies. Order is not meaningful — callers filter by
    /// timestamp and treat the set as a suppression set plus a component-wise-max baseline.
    snapshots: Vec<(u64, CodexTokens)>,
}

/// Resolves a forked session's parent rollout files and the parent's cumulative token
/// totals, so fork children can inherit a baseline instead of recounting replayed history.
/// The same session id can appear in more than one file (active plus archived, or across
/// comma-separated CODEX_HOME roots); snapshots are merged across every copy so a child
/// never inherits a truncated baseline from an arbitrary one, and every copy's metadata
/// travels with the resolution so a child re-parses when any of them changes.
type CodexParentSlot = Option<Arc<CodexParentData>>;

struct CodexParentIndex {
    by_session: HashMap<String, Vec<PathBuf>>,
    parents: Mutex<HashMap<String, CodexParentSlot>>,
}

impl CodexParentIndex {
    fn new(files: &[PathBuf]) -> Self {
        let mut by_session: HashMap<String, Vec<PathBuf>> = HashMap::new();
        for path in files {
            if let Some(session) = codex_session_uuid_from_stem(path) {
                by_session.entry(session).or_default().push(path.clone());
            }
        }
        Self {
            by_session,
            parents: Mutex::new(HashMap::new()),
        }
    }

    fn load(&self, parent: &str) -> CodexParentSlot {
        if let Some(slot) = self.parents.lock().unwrap().get(parent) {
            return slot.clone();
        }
        let loaded = self.by_session.get(parent).and_then(|paths| {
            let mut deps = Vec::new();
            let mut snapshots = Vec::new();
            for path in paths {
                let Ok((size, mtime_ns)) = usage_file_metadata(path) else {
                    continue;
                };
                let Ok(file_snapshots) = codex_total_snapshots(path) else {
                    continue;
                };
                deps.push(UsageFileDep {
                    path: path.to_string_lossy().to_string(),
                    size,
                    mtime_ns,
                });
                snapshots.extend(file_snapshots);
            }
            (!deps.is_empty()).then(|| Arc::new(CodexParentData { deps, snapshots }))
        });
        self.parents
            .lock()
            .unwrap()
            .entry(parent.to_string())
            .or_insert(loaded)
            .clone()
    }

    /// True if the current set of parent copies still matches the copies recorded when a fork
    /// child was cached. A child's `deps` are the parent rollout copies present at resolution
    /// time; if a fuller copy later appears at a new path (an archive lands, another
    /// CODEX_HOME root syncs), the merged baseline changes even though every recorded copy is
    /// unchanged, so the child must re-parse. Non-fork rows have no deps and are always valid.
    fn deps_match_current_candidates(&self, deps: &[UsageFileDep]) -> bool {
        let Some(first) = deps.first() else {
            return true;
        };
        let Some(session) = codex_session_uuid_from_stem(Path::new(&first.path)) else {
            return true;
        };
        let current: HashSet<&str> = self
            .by_session
            .get(&session)
            .map(|paths| paths.iter().filter_map(|path| path.to_str()).collect())
            .unwrap_or_default();
        let recorded: HashSet<&str> = deps.iter().map(|dep| dep.path.as_str()).collect();
        current == recorded
    }

    /// Parent snapshots recorded at-or-before `cutoff_ms` paired with the dependency
    /// fingerprints of every parent copy. `None` means the parent is unknown or had no usage
    /// before the fork, so the caller falls back to the unresolved-fork baseline and does not
    /// cache.
    fn resolve(
        &self,
        parent: &str,
        cutoff_ms: u64,
    ) -> Option<(Vec<UsageFileDep>, Vec<CodexTokens>)> {
        let data = self.load(parent)?;
        let totals: Vec<CodexTokens> = data
            .snapshots
            .iter()
            .filter(|(ts, _)| *ts <= cutoff_ms)
            .map(|(_, totals)| *totals)
            .collect();
        if totals.is_empty() {
            return None;
        }
        Some((data.deps.clone(), totals))
    }
}

/// Codex records a fork/subagent parent either as a flat `forked_from_id` or nested under
/// `source.subagent.thread_spawn.parent_thread_id`. Matches `apply_codex_session_meta` in
/// `ingest.rs` so both shapes are treated as forks.
fn codex_parent_session_id(payload: &Value) -> Option<String> {
    str_at(
        payload,
        &["forked_from_id", "parent_session_id", "parentSessionId"],
    )
    .or_else(|| {
        payload
            .pointer("/source/subagent/thread_spawn/parent_thread_id")
            .and_then(Value::as_str)
            .map(str::to_string)
    })
}

fn codex_session_uuid_from_stem(path: &Path) -> Option<String> {
    let stem = path.file_stem()?.to_str()?;
    if stem.len() < 36 {
        return None;
    }
    let candidate = &stem[stem.len() - 36..];
    let uuid_shaped = candidate.bytes().enumerate().all(|(i, b)| {
        if matches!(i, 8 | 13 | 18 | 23) {
            b == b'-'
        } else {
            b.is_ascii_hexdigit()
        }
    });
    uuid_shaped.then(|| candidate.to_string())
}

/// Extracts every (timestamp, total_token_usage) snapshot from a rollout file.
fn codex_total_snapshots(path: &Path) -> Result<Vec<(u64, CodexTokens)>> {
    static TOKEN_COUNT_NEEDLE: Lazy<memmem::Finder<'static>> =
        Lazy::new(|| memmem::Finder::new(b"token_count"));
    let mut snapshots = Vec::new();
    for_each_line(path, |_, line| {
        if TOKEN_COUNT_NEEDLE.find(line).is_none() {
            return;
        }
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            return;
        };
        if value.get("type").and_then(Value::as_str) != Some("event_msg") {
            return;
        }
        let payload = value.get("payload").unwrap_or(&Value::Null);
        if payload.get("type").and_then(Value::as_str) != Some("token_count") {
            return;
        }
        let info = payload.get("info").unwrap_or(payload);
        let Some(total) = info
            .get("total_token_usage")
            .map(CodexTokens::from)
            .filter(|total| !total.zero())
        else {
            return;
        };
        let ts = value.get("timestamp").map(timestamp_ms).unwrap_or(0);
        snapshots.push((ts, total));
    })?;
    Ok(snapshots)
}

fn scan_codex(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
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
    let parents = CodexParentIndex::new(&files);
    scan_files_cached_with(
        SourceScan {
            source: "codex",
            parser_version: CODEX_PARSER_VERSION,
            volatile_reuse_ms: |_| None,
        },
        &files,
        cache,
        warnings,
        out,
        |path| scan_codex_file(path, &parents),
        |deps| parents.deps_match_current_candidates(deps),
    );
    Ok(())
}

/// Matches every line kind `scan_codex_file` can consume; other lines (the vast majority:
/// prompts, tool output, reasoning) are skipped without a JSON parse.
static CODEX_LINE_NEEDLES: Lazy<Vec<memmem::Finder<'static>>> = Lazy::new(|| {
    [
        &b"token_count"[..],
        b"turn_context",
        b"session_meta",
        b"task_started",
        b"\"usage\"",
    ]
    .into_iter()
    .map(memmem::Finder::new)
    .collect()
});

fn scan_codex_file(path: &Path, parents: &CodexParentIndex) -> Result<FileParse> {
    let source_path = path.to_string_lossy().to_string();
    let mut session = path
        .file_stem()
        .and_then(|n| n.to_str())
        .map(str::to_string);
    let mut parent = None;
    let mut fork_timestamp_ms = None;
    let mut fork_resolved = false;
    let mut parent_deps: Vec<UsageFileDep> = Vec::new();
    let mut project = None;
    let mut model = None;
    let mut turn = None;
    let mut counter = CodexCounter::default();
    let mut event_index = 0u64;
    let mut unresolved_fork_baseline_seen = false;
    let mut out = Vec::new();
    for_each_line(path, |line_index, line| {
        if !CODEX_LINE_NEEDLES
            .iter()
            .any(|needle| needle.find(line).is_some())
        {
            return;
        }
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            return;
        };
        let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
        let payload = value.get("payload").unwrap_or(&Value::Null);
        match kind {
            "session_meta" => {
                session = str_at(payload, &["id", "session_id"]).or_else(|| session.take());
                parent = codex_parent_session_id(payload);
                if parent.is_some() {
                    fork_timestamp_ms = value.get("timestamp").map(timestamp_ms);
                }
                if let (Some(parent_id), Some(fork_ms)) = (&parent, fork_timestamp_ms)
                    && let Some((deps, inherited)) = parents.resolve(parent_id, fork_ms)
                {
                    counter.seed_inherited(&inherited);
                    // The parent baseline is resolved; the first post-fork event is a
                    // real turn and must not be swallowed as a baseline guess.
                    unresolved_fork_baseline_seen = true;
                    fork_resolved = true;
                    parent_deps = deps;
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
                    return;
                }
                if parent.is_some()
                    && !unresolved_fork_baseline_seen
                    && let Some(total) = total
                {
                    counter.establish_unresolved_fork_baseline(total);
                    unresolved_fork_baseline_seen = true;
                    return;
                }
                let delta = counter.account(last, total);
                if delta.zero() {
                    return;
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
                    conservative_undercount: counter.interleaved
                        || (parent.is_some() && !fork_resolved),
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
                        return;
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
    })?;
    // A fork whose parent rollout was not on disk this scan was counted with a guessed
    // baseline; persisting it would freeze that guess even after the parent appears, so the
    // result is provisional and must be recomputed on the next scan.
    let cacheable = parent.is_none() || fork_resolved;
    // A resolved fork depends on the parent rollout's content: if any parent copy is later
    // synced further or extended, the cached child must be re-parsed so its baseline
    // reflects the fuller parent instead of a partial prefix.
    Ok(FileParse {
        events: out,
        cacheable,
        deps: parent_deps,
    })
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

fn scan_pi(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = jsonl_files([crate::ingest::pi_sessions_root()]);
    scan_files_cached(
        SourceScan {
            source: "pi",
            parser_version: PI_PARSER_VERSION,
            volatile_reuse_ms: |_| None,
        },
        &files,
        cache,
        warnings,
        out,
        |path| scan_pi_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

fn scan_pi_file(path: &Path) -> Result<Vec<UsageEvent>> {
    let mut out = Vec::new();
    {
        let source_path = path.to_string_lossy().to_string();
        let mut session = crate::ingest::pi_session_id_from_path(path);
        let mut project = crate::ingest::project_from_pi_session_path(path);
        let mut current_model = None;
        let mut current_provider = None;
        for (index, value) in lines(path)? {
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
    Ok(out)
}

fn scan_opencode(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let roots: Vec<PathBuf> = std::env::var_os("OPENCODE_DATA_DIR")
        .map(|v| {
            v.to_string_lossy()
                .split(',')
                .map(|s| PathBuf::from(s.trim()))
                .collect()
        })
        .unwrap_or_else(|| vec![home().join(".local/share/opencode")]);
    // Databases come before message files so `reconcile_opencode_copies` keeps the
    // database copy of a message, matching the pre-cache suppression order.
    let mut files = Vec::new();
    for root in &roots {
        let mut databases = Vec::new();
        if let Ok(entries) = std::fs::read_dir(root) {
            databases.extend(entries.flatten().map(|entry| entry.path()).filter(|path| {
                path.extension().and_then(|v| v.to_str()) == Some("db")
                    && path
                        .file_name()
                        .and_then(|v| v.to_str())
                        .is_some_and(|n| n.starts_with("opencode"))
            }));
        }
        databases.sort();
        files.extend(databases);
    }
    for root in &roots {
        let message_root = root.join("storage/message");
        if message_root.exists() {
            files.extend(
                WalkDir::new(message_root)
                    .into_iter()
                    .flatten()
                    .filter(|e| {
                        e.file_type().is_file()
                            && e.path().extension().and_then(|v| v.to_str()) == Some("json")
                    })
                    .map(|e| e.path().to_path_buf()),
            );
        }
    }
    scan_files_cached(
        SourceScan {
            source: "opencode",
            parser_version: OPENCODE_PARSER_VERSION,
            // Only the databases are volatile; message JSON files are updated in place
            // while a response streams and must re-parse as soon as they change.
            volatile_reuse_ms: |path| {
                (path.extension().and_then(|v| v.to_str()) == Some("db"))
                    .then_some(VOLATILE_DB_REUSE_MS)
            },
        },
        &files,
        cache,
        warnings,
        out,
        |path| {
            if path.extension().and_then(|v| v.to_str()) == Some("db") {
                scan_opencode_db(path)
            } else {
                scan_opencode_message_file(path)
            }
            .map(FileParse::cacheable)
        },
    );
    Ok(())
}

/// A message can exist both in an OpenCode database and as a JSON file under
/// `storage/message` (and in databases from several roots). Keep the first copy in scan
/// order, which lists databases first.
fn reconcile_opencode_copies(events: &mut Vec<UsageEvent>) {
    let mut seen = HashSet::new();
    events.retain(|event| {
        if event.source != "opencode" {
            return true;
        }
        let Some(record) = &event.source_record_id else {
            return true;
        };
        seen.insert(record.clone())
    });
}

fn scan_opencode_message_file(path: &Path) -> Result<Vec<UsageEvent>> {
    let mut out = Vec::new();
    let value: Value = match File::open(path)
        .ok()
        .and_then(|file| serde_json::from_reader(file).ok())
    {
        Some(value) => value,
        None => return Ok(out),
    };
    let id = str_at(&value, &["id"]).or_else(|| {
        path.file_stem()
            .and_then(|n| n.to_str())
            .map(str::to_string)
    });
    push_opencode_event(&value, path, id, &mut out);
    Ok(out)
}

fn scan_opencode_db(path: &Path) -> Result<Vec<UsageEvent>> {
    let mut out = Vec::new();
    let mut ids = HashSet::new();
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(Duration::from_secs(1))?;
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
            push_opencode_event(&value, path, Some(id.clone()), &mut out);
        }
        if out.len() > before {
            ids.insert(id);
        }
    }
    Ok(out)
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

fn scan_cursor(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
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
    let databases: Vec<PathBuf> = databases.into_iter().filter(|path| path.exists()).collect();
    let start = out.len();
    scan_files_cached(
        SourceScan {
            source: "cursor",
            parser_version: CURSOR_PARSER_VERSION,
            volatile_reuse_ms: |_| Some(VOLATILE_DB_REUSE_MS),
        },
        &databases,
        cache,
        warnings,
        out,
        |path| scan_cursor_db(path).map(FileParse::cacheable),
    );
    apply_cursor_projects(&mut out[start..], &cursor_project_by_session());
    Ok(())
}

/// Project attribution comes from `.cursor/projects` transcripts, which change independently
/// of the database files the cache is keyed on. Derive it fresh on every scan (cached rows
/// included) so a transcript that is indexed or moved after a database was cached still
/// updates attribution.
fn apply_cursor_projects(events: &mut [UsageEvent], project_by_session: &HashMap<String, String>) {
    for event in events {
        event.project = event
            .session_id
            .as_deref()
            .and_then(|session_id| project_by_session.get(session_id))
            .cloned();
    }
}

/// Cursor generations can appear in both the global and a workspace database. Keep the
/// first copy in database order, mirroring the shared `seen` set the scanners used before
/// results were cached per database.
fn reconcile_cursor_copies(events: &mut Vec<UsageEvent>) {
    let mut seen = HashSet::new();
    events.retain(|event| {
        if event.source != "cursor" {
            return true;
        }
        let Some(record) = &event.source_record_id else {
            return true;
        };
        seen.insert((record.clone(), event.tokens.raw_input, event.tokens.output))
    });
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

fn scan_cursor_db(path: &Path) -> Result<Vec<UsageEvent>> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(Duration::from_secs(1))?;
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
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })?;
        for row in rows {
            let (key, raw) = row?;
            let Some(raw) = raw else {
                continue;
            };
            let Ok(value) = serde_json::from_str::<Value>(&raw) else {
                continue;
            };
            extract_cursor_objects(&value, &key, table, path, &mut out, &mut seen);
        }
    }
    Ok(out)
}

fn extract_cursor_objects(
    value: &Value,
    fallback_id: &str,
    table: &str,
    path: &Path,
    out: &mut Vec<UsageEvent>,
    seen: &mut HashSet<String>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                extract_cursor_objects(value, fallback_id, table, path, out, seen);
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
                        // Derived per scan by `apply_cursor_projects`; never cached.
                        project: None,
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
                    extract_cursor_objects(child, fallback_id, table, path, out, seen);
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

fn scan_copilot(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
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
    scan_files_cached(
        SourceScan {
            source: "copilot",
            parser_version: COPILOT_PARSER_VERSION,
            volatile_reuse_ms: |_| None,
        },
        &files,
        cache,
        warnings,
        out,
        |path| scan_copilot_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

fn scan_copilot_file(path: &Path) -> Result<Vec<UsageEvent>> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for (index, value) in lines(path)? {
        extract_otel(&value, path, index, &mut out, &mut seen);
    }
    Ok(out)
}

/// OTel spans can repeat across exporter files (same trace/span or response id). Keep the
/// first copy in file order, mirroring the shared `seen` set the scanner used before
/// results were cached per file.
fn reconcile_copilot_copies(events: &mut Vec<UsageEvent>) {
    let mut seen = HashSet::new();
    events.retain(|event| {
        if event.source != "copilot" {
            return true;
        }
        let Some(record) = &event.source_record_id else {
            return true;
        };
        seen.insert(record.clone())
    });
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
            .query_row(
                "SELECT count(*) FROM usage_file_cache WHERE source = 'claude'",
                [],
                |row| row.get(0),
            )
            .expect("count cached files");

        assert_eq!(cold.events, 1);
        assert_eq!(cold.details[0].tokens.total(), 19);
        assert_eq!(cold.details[0].tokens.cache_write_1h, 1);
        assert_eq!(cold.details[0].dedupe_confidence, "exact");
        assert_eq!(warm.total_tokens, cold.total_tokens);
        assert_eq!(cached_files, 1);
    }

    #[test]
    fn claude_warm_cache_reconciles_old_parents_before_since_filter() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        let subagents = projects.join("subagents");
        std::fs::create_dir_all(&subagents).expect("create projects");
        std::fs::write(
            projects.join("parent.jsonl"),
            concat!(
                r#"{"type":"assistant","sessionId":"parent","requestId":"parent-request","timestamp":1000,"cwd":"/repo/memex","message":{"id":"shared-message","model":"claude-sonnet-4-6","usage":{"inputTokens":10}}}"#,
                "\n"
            ),
        )
        .expect("write parent transcript");
        std::fs::write(
            subagents.join("agent.jsonl"),
            concat!(
                r#"{"type":"assistant","sessionId":"agent","requestId":"sidechain-request","timestamp":3000,"cwd":"/repo/memex","isSidechain":true,"message":{"id":"shared-message","model":"claude-sonnet-4-6","usage":{"inputTokens":10}}}"#,
                "\n"
            ),
        )
        .expect("write sidechain transcript");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Claude),
            since_ms: Some(2_000_000),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            ..UsageQuery::default()
        };

        let cold = scan_usage(&query).expect("cold scan");
        let cache = Connection::open(query.cache_path.as_ref().expect("cache path"))
            .expect("open usage cache");
        let cached_files: u64 = cache
            .query_row(
                "SELECT count(*) FROM usage_file_cache WHERE source = 'claude'",
                [],
                |row| row.get(0),
            )
            .expect("count cached files");
        let warm = scan_usage(&query).expect("warm scan");

        assert_eq!(cold.events, 0);
        assert_eq!(cached_files, 2);
        assert_eq!(warm.events, cold.events);
        assert_eq!(warm.total_tokens, 0);
    }

    #[test]
    fn claude_lines_with_both_session_field_spellings_are_counted() {
        // Claude Code 2.1.210+ writes `session_id` AND `sessionId` (and can do the same
        // for request ids) on one line; a duplicate-field parse error must not drop it.
        let tmp = tempfile::tempdir().expect("tempdir");
        let transcript = tmp.path().join("session.jsonl");
        std::fs::write(
            &transcript,
            concat!(
                r#"{"type":"assistant","session_id":"ses-1","sessionId":"ses-1","requestId":"req-1","request_id":"req-1","timestamp":1000,"cwd":"/repo/memex","message":{"id":"msg-1","model":"claude-opus-4-8","usage":{"input_tokens":2,"cache_read_input_tokens":52196,"cache_creation_input_tokens":558,"output_tokens":108}}}"#,
                "\n"
            ),
        )
        .expect("write transcript");

        let events = scan_claude_file(&transcript).expect("scan transcript");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].session_id.as_deref(), Some("ses-1"));
        assert_eq!(events[0].request_id.as_deref(), Some("req-1"));
        assert_eq!(events[0].dedupe_confidence, "exact");
        assert_eq!(events[0].tokens.total(), 52_864);
    }

    #[test]
    fn claude_file_parse_failures_preserve_successful_files() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let valid = tmp.path().join("valid.jsonl");
        let vanished = tmp.path().join("vanished.jsonl");
        std::fs::write(
            &valid,
            concat!(
                r#"{"type":"assistant","timestamp":1000,"message":{"id":"valid","usage":{"inputTokens":10}}}"#,
                "\n"
            ),
        )
        .expect("write valid transcript");
        std::fs::write(&vanished, "").expect("write disappearing transcript");
        let valid_metadata = usage_file_metadata(&valid).expect("valid metadata");
        let vanished_metadata = usage_file_metadata(&vanished).expect("vanished metadata");
        std::fs::remove_file(&vanished).expect("remove transcript");
        let missing = vec![
            (0, valid, valid_metadata),
            (1, vanished.clone(), vanished_metadata),
        ];
        let mut warnings = Vec::new();

        let parsed =
            parse_missing_usage_files("claude", &missing, &mut warnings, &|path: &Path| {
                scan_claude_file(path).map(FileParse::cacheable)
            });

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].index, 0);
        assert_eq!(parsed[0].events.len(), 1);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains(vanished.to_string_lossy().as_ref()));
    }

    #[test]
    fn codex_scanner_caches_events_by_file_metadata() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let sessions = tmp.path().join("sessions/2026/07/03");
        std::fs::create_dir_all(&sessions).expect("create sessions");
        std::fs::write(
            sessions.join("rollout-2026-07-03-session.jsonl"),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-03T01:02:03Z","payload":{"id":"codex-session","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"turn_context","payload":{"model":"gpt-5.4"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-03T01:02:05Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100,"cached_input_tokens":40,"output_tokens":25},"total_token_usage":{"input_tokens":100,"cached_input_tokens":40,"output_tokens":25}}}}"#,
                "\n"
            ),
        )
        .expect("write session");
        let _env = EnvVarGuard::set_os(&[("CODEX_HOME", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Codex),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            ..UsageQuery::default()
        };

        let cold = scan_usage(&query).expect("cold scan");
        let warm = scan_usage(&query).expect("warm scan");
        let cache = Connection::open(query.cache_path.as_ref().expect("cache path"))
            .expect("open usage cache");
        let cached_files: u64 = cache
            .query_row(
                "SELECT count(*) FROM usage_file_cache WHERE source = 'codex'",
                [],
                |row| row.get(0),
            )
            .expect("count cached files");

        assert_eq!(cold.events, 1);
        assert_eq!(cold.details[0].tokens.total(), 125);
        assert_eq!(cold.details[0].model.as_deref(), Some("gpt-5.4"));
        assert_eq!(cold.details[0].session_id.as_deref(), Some("codex-session"));
        assert_eq!(cold.details[0].project.as_deref(), Some("/repo/memex"));
        assert_eq!(warm.events, cold.events);
        assert_eq!(warm.total_tokens, cold.total_tokens);
        assert_eq!(cached_files, 1);
    }

    #[test]
    fn codex_fork_children_inherit_parent_baselines() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let parent_dir = tmp.path().join("sessions/2026/07/14");
        let child_dir = tmp.path().join("sessions/2026/07/15");
        std::fs::create_dir_all(&parent_dir).expect("create parent dir");
        std::fs::create_dir_all(&child_dir).expect("create child dir");
        std::fs::write(
            parent_dir.join("rollout-2026-07-14T10-00-00-019f0000-0000-7000-8000-000000000001.jsonl"),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-14T10:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:01:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:02:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":200},"total_token_usage":{"input_tokens":300}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:03:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":300},"total_token_usage":{"input_tokens":600}}}}"#,
                "\n"
            ),
        )
        .expect("write parent rollout");
        // The child replays a TRUNCATED parent history (the total=300 snapshot is missing)
        // under its own session id, so cross-file tuple dedupe cannot suppress it; only the
        // inherited parent baseline can.
        std::fs::write(
            child_dir.join("rollout-2026-07-15T09-00-00-019f0000-0000-7000-8000-000000000002.jsonl"),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-15T09:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000002","forked_from_id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:00:01Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:00:02Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":300},"total_token_usage":{"input_tokens":600}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:05:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":150},"total_token_usage":{"input_tokens":750}}}}"#,
                "\n"
            ),
        )
        .expect("write fork rollout");
        let _env = EnvVarGuard::set_os(&[("CODEX_HOME", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Codex),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            ..UsageQuery::default()
        };

        let report = scan_usage(&query).expect("scan usage");

        // Parent turns: 100 + 200 + 300. Child: only the post-fork turn of 150.
        assert_eq!(report.total_tokens, 750);
        assert_eq!(report.events, 4);
        let child_events: Vec<_> = report
            .details
            .iter()
            .filter(|event| event.source_path.contains("2026-07-15T09-00-00"))
            .collect();
        assert_eq!(child_events.len(), 1);
        assert_eq!(child_events[0].tokens.total(), 150);
        assert!(!child_events[0].conservative_undercount);
    }

    #[test]
    fn codex_unresolved_fork_is_not_cached_until_parent_appears() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let sessions = tmp.path().join("sessions");
        let parent_dir = sessions.join("2026/07/14");
        let child_dir = sessions.join("2026/07/15");
        std::fs::create_dir_all(&child_dir).expect("create child dir");
        let child = child_dir
            .join("rollout-2026-07-15T09-00-00-019f0000-0000-7000-8000-000000000002.jsonl");
        // Child replays the parent's total=100 and total=600 snapshots, then does one new
        // turn (total=750). With the parent absent the replay is counted via the guessed
        // baseline; with the parent present only the +150 turn should remain.
        std::fs::write(
            &child,
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-15T09:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000002","forked_from_id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:00:01Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:00:02Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":500},"total_token_usage":{"input_tokens":600}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:05:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":150},"total_token_usage":{"input_tokens":750}}}}"#,
                "\n"
            ),
        )
        .expect("write fork rollout");
        let _env = EnvVarGuard::set_os(&[("CODEX_HOME", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Codex),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            ..UsageQuery::default()
        };

        // Parent not yet on disk: fork is unresolved, so nothing is cached for it. Had the
        // guessed result been cached, the next scan would serve it and double-count the 500
        // replayed tokens on top of the parent's own count.
        scan_usage(&query).expect("scan without parent");
        let cache = Connection::open(query.cache_path.as_ref().expect("cache path"))
            .expect("open usage cache");
        let cached_files: u64 = cache
            .query_row(
                "SELECT count(*) FROM usage_file_cache WHERE source = 'codex'",
                [],
                |row| row.get(0),
            )
            .expect("count cached files");
        assert_eq!(cached_files, 0, "unresolved fork must not be cached");

        // Parent appears; the child file is byte-for-byte unchanged. Because the unresolved
        // result was never cached, this scan re-parses and resolves the baseline.
        std::fs::create_dir_all(&parent_dir).expect("create parent dir");
        std::fs::write(
            parent_dir
                .join("rollout-2026-07-14T10-00-00-019f0000-0000-7000-8000-000000000001.jsonl"),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-14T10:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:01:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:02:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":500},"total_token_usage":{"input_tokens":600}}}}"#,
                "\n"
            ),
        )
        .expect("write parent rollout");

        let after = scan_usage(&query).expect("scan with parent");

        // Parent contributes 100 + 500; child only its new +150 turn.
        assert_eq!(after.total_tokens, 750);
        let child_after: u64 = after
            .details
            .iter()
            .filter(|event| {
                event
                    .source_path
                    .contains("019f0000-0000-7000-8000-000000000002")
            })
            .map(|event| event.tokens.total())
            .sum();
        assert_eq!(child_after, 150);
    }

    #[test]
    fn codex_nested_thread_spawn_parent_is_resolved() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let parent_dir = tmp.path().join("sessions/2026/07/14");
        let child_dir = tmp.path().join("sessions/2026/07/15");
        std::fs::create_dir_all(&parent_dir).expect("create parent dir");
        std::fs::create_dir_all(&child_dir).expect("create child dir");
        std::fs::write(
            parent_dir
                .join("rollout-2026-07-14T10-00-00-019f0000-0000-7000-8000-000000000001.jsonl"),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-14T10:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:01:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:02:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":500},"total_token_usage":{"input_tokens":600}}}}"#,
                "\n"
            ),
        )
        .expect("write parent rollout");
        // The parent link is only present in the nested subagent thread_spawn shape.
        std::fs::write(
            child_dir
                .join("rollout-2026-07-15T09-00-00-019f0000-0000-7000-8000-000000000002.jsonl"),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-15T09:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000002","source":{"subagent":{"thread_spawn":{"parent_thread_id":"019f0000-0000-7000-8000-000000000001"}}},"cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:00:01Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:00:02Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":500},"total_token_usage":{"input_tokens":600}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:05:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":150},"total_token_usage":{"input_tokens":750}}}}"#,
                "\n"
            ),
        )
        .expect("write nested fork rollout");
        let _env = EnvVarGuard::set_os(&[("CODEX_HOME", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Codex),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            ..UsageQuery::default()
        };

        let report = scan_usage(&query).expect("scan usage");

        // Parent 100 + 500; child replays both and adds only its 150 turn.
        assert_eq!(report.total_tokens, 750);
        let child: u64 = report
            .details
            .iter()
            .filter(|event| {
                event
                    .source_path
                    .contains("019f0000-0000-7000-8000-000000000002")
            })
            .map(|event| event.tokens.total())
            .sum();
        assert_eq!(child, 150);
    }

    #[test]
    fn codex_fork_merges_snapshots_from_duplicate_parent_copies() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        // The parent session exists in two roots: an archived copy truncated to the first
        // snapshot, and an active copy with the full pre-fork history. The child must inherit
        // the merged (fuller) baseline, not whichever copy is indexed first.
        let archived_dir = tmp.path().join("archived_sessions/2026/07/14");
        let active_dir = tmp.path().join("sessions/2026/07/14");
        let child_dir = tmp.path().join("sessions/2026/07/15");
        std::fs::create_dir_all(&archived_dir).expect("create archived dir");
        std::fs::create_dir_all(&active_dir).expect("create active dir");
        std::fs::create_dir_all(&child_dir).expect("create child dir");
        let parent_name = "rollout-2026-07-14T10-00-00-019f0000-0000-7000-8000-000000000001.jsonl";
        std::fs::write(
            archived_dir.join(parent_name),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-14T10:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:01:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n"
            ),
        )
        .expect("write archived parent copy");
        std::fs::write(
            active_dir.join(parent_name),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-14T10:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:01:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:02:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":500},"total_token_usage":{"input_tokens":600}}}}"#,
                "\n"
            ),
        )
        .expect("write active parent copy");
        std::fs::write(
            child_dir
                .join("rollout-2026-07-15T09-00-00-019f0000-0000-7000-8000-000000000002.jsonl"),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-15T09:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000002","forked_from_id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:00:01Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:00:02Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":500},"total_token_usage":{"input_tokens":600}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:05:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":150},"total_token_usage":{"input_tokens":750}}}}"#,
                "\n"
            ),
        )
        .expect("write fork rollout");
        let _env = EnvVarGuard::set_os(&[("CODEX_HOME", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Codex),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            ..UsageQuery::default()
        };

        let report = scan_usage(&query).expect("scan usage");

        // The child replays both parent snapshots (100 and 600) and adds only its 150 turn.
        // Had it inherited from the truncated archived copy alone, the 500 would recount.
        let child: u64 = report
            .details
            .iter()
            .filter(|event| {
                event
                    .source_path
                    .contains("019f0000-0000-7000-8000-000000000002")
            })
            .map(|event| event.tokens.total())
            .sum();
        assert_eq!(child, 150);
    }

    #[test]
    fn codex_fork_reparses_when_a_new_parent_copy_appears() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let active_dir = tmp.path().join("sessions/2026/07/14");
        let archived_dir = tmp.path().join("archived_sessions/2026/07/14");
        let child_dir = tmp.path().join("sessions/2026/07/15");
        std::fs::create_dir_all(&active_dir).expect("create active dir");
        std::fs::create_dir_all(&child_dir).expect("create child dir");
        let parent_name = "rollout-2026-07-14T10-00-00-019f0000-0000-7000-8000-000000000001.jsonl";
        // At first only a truncated parent copy exists (just the total=100 snapshot).
        std::fs::write(
            active_dir.join(parent_name),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-14T10:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:01:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n"
            ),
        )
        .expect("write truncated parent copy");
        std::fs::write(
            child_dir
                .join("rollout-2026-07-15T09-00-00-019f0000-0000-7000-8000-000000000002.jsonl"),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-15T09:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000002","forked_from_id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:00:01Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:00:02Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":500},"total_token_usage":{"input_tokens":600}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:05:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":150},"total_token_usage":{"input_tokens":750}}}}"#,
                "\n"
            ),
        )
        .expect("write fork rollout");
        let _env = EnvVarGuard::set_os(&[("CODEX_HOME", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Codex),
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            ..UsageQuery::default()
        };

        // First scan: the only parent copy is truncated, so the child treats the not-yet-seen
        // total=600 snapshot as new. The child is cached with a dependency on that one copy.
        let before = scan_usage(&query).expect("first scan");
        assert_eq!(before.total_tokens, 750);

        // A fuller parent copy lands at a new (archived) path. The originally recorded copy is
        // untouched, so only the changed candidate set can trigger the child to re-parse.
        std::fs::create_dir_all(&archived_dir).expect("create archived dir");
        std::fs::write(
            archived_dir.join(parent_name),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-14T10:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:01:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:02:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":500},"total_token_usage":{"input_tokens":600}}}}"#,
                "\n"
            ),
        )
        .expect("write fuller parent copy");

        let after = scan_usage(&query).expect("second scan");

        // Without candidate-set invalidation the child would stay cached and the fuller copy's
        // 500 would be counted twice (total 1250); re-parsing merges both copies and keeps 750.
        assert_eq!(after.total_tokens, 750);
    }

    #[test]
    fn codex_fork_reparses_when_partial_parent_is_extended() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let parent_dir = tmp.path().join("sessions/2026/07/14");
        let child_dir = tmp.path().join("sessions/2026/07/15");
        std::fs::create_dir_all(&parent_dir).expect("create parent dir");
        std::fs::create_dir_all(&child_dir).expect("create child dir");
        let parent = parent_dir
            .join("rollout-2026-07-14T10-00-00-019f0000-0000-7000-8000-000000000001.jsonl");
        // Parent is only partially synced: it has the total=100 snapshot but not yet the
        // total=600 snapshot the child replays.
        std::fs::write(
            &parent,
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-14T10:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:01:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n"
            ),
        )
        .expect("write partial parent");
        std::fs::write(
            child_dir
                .join("rollout-2026-07-15T09-00-00-019f0000-0000-7000-8000-000000000002.jsonl"),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-15T09:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000002","forked_from_id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:00:01Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:00:02Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":500},"total_token_usage":{"input_tokens":600}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-15T09:05:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":150},"total_token_usage":{"input_tokens":750}}}}"#,
                "\n"
            ),
        )
        .expect("write fork rollout");
        let _env = EnvVarGuard::set_os(&[("CODEX_HOME", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Codex),
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            ..UsageQuery::default()
        };

        // Partial parent: it emits only 100, and the child counts the not-yet-synced
        // total=600 snapshot as new (its baseline is the partial 100). The child result is
        // cached against the parent's current metadata.
        let partial = scan_usage(&query).expect("scan with partial parent");
        assert_eq!(partial.total_tokens, 750);

        // Parent finishes syncing the total=600 snapshot. The child file is unchanged, but
        // its cached dependency on the parent is now stale, so it must re-parse.
        std::fs::write(
            &parent,
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-14T10:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:01:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:02:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":500},"total_token_usage":{"input_tokens":600}}}}"#,
                "\n"
            ),
        )
        .expect("extend parent");

        let extended = scan_usage(&query).expect("scan with extended parent");

        // Without dependency invalidation the child would stay cached and the parent's newly
        // synced 500 would be counted twice (total 1250); re-parsing keeps it at 750.
        assert_eq!(extended.total_tokens, 750);
    }

    #[test]
    fn opencode_message_file_changes_bypass_volatile_reuse() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let message_dir = tmp.path().join("storage/message/ses_test");
        std::fs::create_dir_all(&message_dir).expect("create message directory");
        let message_path = message_dir.join("msg_test.json");
        let message = |output: u64| {
            serde_json::to_vec(&serde_json::json!({
                "id": "msg_test",
                "sessionID": "ses_test",
                "time": { "created": 1_750_000_000_000u64 },
                "tokens": {
                    "input": 10,
                    "output": output,
                    "reasoning": 0,
                    "cache": { "read": 0, "write": 0 }
                }
            }))
            .expect("serialize message")
        };
        std::fs::write(&message_path, message(5)).expect("write message");
        let _env = EnvVarGuard::set_os(&[("OPENCODE_DATA_DIR", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Opencode),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            ..UsageQuery::default()
        };

        let initial = scan_usage(&query).expect("initial scan");
        // The message file is rewritten while a response streams; unlike the opencode
        // databases it must not be served from the 60s volatile window.
        std::fs::write(&message_path, message(500)).expect("rewrite message");
        let updated = scan_usage(&query).expect("updated scan");

        assert_eq!(initial.total_tokens, 15);
        assert_eq!(updated.total_tokens, 510);
    }

    #[test]
    fn memoized_scan_reuses_assembled_events_within_ttl() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        let transcript = projects.join("session.jsonl");
        let line = |input: u64| {
            format!(
                r#"{{"type":"assistant","sessionId":"session","timestamp":1000,"message":{{"id":"m-{input}","usage":{{"inputTokens":{input}}}}}}}"#
            ) + "\n"
        };
        std::fs::write(&transcript, line(10)).expect("write transcript");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Claude),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            memo_ttl_ms: 60_000,
            ..UsageQuery::default()
        };

        let first = scan_usage(&query).expect("first scan");
        std::fs::write(&transcript, format!("{}{}", line(10), line(70))).expect("grow transcript");
        let memoized = scan_usage(&query).expect("memoized scan");
        let fresh = scan_usage(&UsageQuery {
            memo_ttl_ms: 0,
            ..query.clone()
        })
        .expect("fresh scan");

        assert_eq!(first.total_tokens, 10);
        assert_eq!(memoized.total_tokens, 10);
        assert_eq!(fresh.total_tokens, 80);
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
        );
        apply_cursor_projects(&mut events, &project_by_session);

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
    fn cursor_project_mapping_is_recomputed_on_cache_hits() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("state.vscdb");
        let conn = Connection::open(&db_path).expect("create cursor db");
        conn.execute_batch(
            "CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY, value TEXT); \
             INSERT INTO cursorDiskKV VALUES ('composerData:composer-main', \
             '{\"generationUUID\":\"gen-1\",\"inputTokens\":10,\"outputTokens\":5}');",
        )
        .expect("populate cursor db");
        drop(conn);
        let cache_path = tmp.path().join("usage-cache.sqlite3");
        let files = vec![db_path];
        let run = |project_by_session: &HashMap<String, String>| {
            let mut cache = UsageCache::open(&cache_path).expect("open cache");
            let mut warnings = Vec::new();
            let mut events = Vec::new();
            scan_files_cached(
                SourceScan {
                    source: "cursor",
                    parser_version: CURSOR_PARSER_VERSION,
                    volatile_reuse_ms: |_| Some(VOLATILE_DB_REUSE_MS),
                },
                &files,
                Some(&mut cache),
                &mut warnings,
                &mut events,
                |path| scan_cursor_db(path).map(FileParse::cacheable),
            );
            assert_eq!(warnings, Vec::<String>::new());
            apply_cursor_projects(&mut events, project_by_session);
            events
        };

        // Cold scan before any transcript is indexed: no attribution.
        let cold = run(&HashMap::new());
        // The database is unchanged, so this scan is served from the cache; a transcript
        // mapping discovered afterwards must still take effect.
        let warm = run(&HashMap::from([(
            "composer-main".to_string(),
            "memex".to_string(),
        )]));

        assert_eq!(cold.len(), 1);
        assert_eq!(cold[0].project, None);
        assert_eq!(warm.len(), 1);
        assert_eq!(warm[0].project.as_deref(), Some("memex"));
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
