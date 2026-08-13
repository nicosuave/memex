//! Reconstructed local token usage.
//!
//! This module intentionally does not model provider quota percentages. Local logs are useful for
//! request-level accounting, but they are not authoritative subscription-limit telemetry.

use crate::analytics::ProjectGrouping;
use crate::types::SourceFilter;
use anyhow::Result;
use clap::ValueEnum;
use once_cell::sync::Lazy;
use rayon::prelude::*;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, ValueEnum)]
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
    pub(crate) fn additive_total(&self) -> u64 {
        self.uncached_input
            .saturating_add(self.cache_read)
            .saturating_add(self.cache_write)
            .saturating_add(self.output)
    }

    pub fn total(&self) -> u64 {
        self.additive_total()
    }

    pub(crate) fn codex(input: u64, cached: u64, output: u64, reasoning: u64) -> Self {
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

    pub(crate) fn disjoint(input: u64, cache_read: u64, cache_write: u64, output: u64) -> Self {
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
    pub source: &'static str,
    /// Shared across every event of a file: assembled scans materialize millions of
    /// events, and per-event owned paths dominated allocation time.
    pub source_path: Arc<str>,
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
    pub(crate) sidechain: bool,
    #[serde(skip)]
    pub(crate) source_order: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
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
    pub cache_waste: CacheWaste,
}

/// Estimated prompt-cache waste: prompt tokens that were in the previous request's prompt
/// (so a warm cache would have served them as cache reads) but were re-billed at
/// input/cache-write rates instead.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct CacheWaste {
    pub missed_tokens: u64,
    /// Extra USD paid vs. a full cache hit, at catalog rates; misses on unpriced models
    /// contribute tokens but no cost.
    pub missed_cost_usd: f64,
    /// Misses above the per-request noise floor.
    pub miss_count: u64,
    /// Misses following an idle gap of at least the cache TTL (same model).
    pub idle_misses: u64,
    /// Misses where the model changed relative to the previous request.
    pub model_switch_misses: u64,
}

impl CacheWaste {
    fn absorb(&mut self, other: &CacheWaste) {
        self.missed_tokens = self.missed_tokens.saturating_add(other.missed_tokens);
        self.missed_cost_usd += other.missed_cost_usd;
        self.miss_count += other.miss_count;
        self.idle_misses += other.idle_misses;
        self.model_switch_misses += other.model_switch_misses;
    }
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
    pub cache_waste: CacheWaste,
    pub by_source: Vec<UsageSummary>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<UsageEvent>,
    pub warnings: Vec<String>,
}

/// One filtered usage event projected to what activity charts need.
#[derive(Clone, Copy, Debug)]
pub struct UsageActivityPoint {
    pub source: &'static str,
    pub timestamp_ms: u64,
    pub total_tokens: u64,
}

/// Filters the assembled events exactly like `scan_usage`, but returns lightweight chart
/// points instead of deep-cloning full events out of the memoized assembly. The boolean is
/// true when any scanner reported a warning, i.e. the totals may be partial.
pub fn scan_usage_activity(query: &UsageQuery) -> Result<(Vec<UsageActivityPoint>, bool)> {
    let _scan_guard = USAGE_SCAN_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (assembled, warnings) = memoized_usage_events(query);
    let points = filtered_events(&assembled, query)
        .map(|event| UsageActivityPoint {
            source: event.source,
            timestamp_ms: event.timestamp_ms,
            total_tokens: event.tokens.total(),
        })
        .collect();
    Ok((points, !warnings.is_empty()))
}

/// Assembled events are already sorted; filtering preserves that order.
fn filtered_events<'a>(
    assembled: &'a [UsageEvent],
    query: &'a UsageQuery,
) -> impl Iterator<Item = &'a UsageEvent> + 'a {
    let mut project_cache = HashMap::new();
    assembled.iter().filter(move |event| {
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
                    session_keys.contains(&(event.source.to_string(), session_id.clone()))
                })
            })
    })
}

pub fn scan_usage(query: &UsageQuery) -> Result<UsageReport> {
    let _scan_guard = USAGE_SCAN_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (assembled, warnings) = memoized_usage_events(query);
    let events: Vec<&UsageEvent> = filtered_events(&assembled, query).collect();

    let mut by_source: HashMap<&'static str, UsageSummary> = HashMap::new();
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
            .entry(event.source)
            .or_insert_with(|| UsageSummary {
                source: event.source.to_string(),
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
    for (source, waste) in compute_cache_waste(events.iter().copied()) {
        report.cache_waste.absorb(&waste);
        if let Some(row) = by_source.get_mut(&source) {
            row.cache_waste = waste;
        }
    }
    report.by_source = by_source.into_values().collect();
    report.by_source.sort_by(|a, b| a.source.cmp(&b.source));
    if query.include_events {
        report.details = events.into_iter().cloned().collect();
    }
    Ok(report)
}

/// Prompt-cache TTL: misses after idle gaps at least this long are attributed to expiry.
/// Anthropic's default cache TTL is 5 minutes.
const CACHE_TTL_MS: u64 = 5 * 60 * 1000;

/// Per-request misses at or below this are cache breakpoint granularity noise.
const CACHE_MISS_NOISE_FLOOR_TOKENS: u64 = 1024;

/// The last request seen in a session chain; everything in its prompt should be cached.
struct CacheChainState<'a> {
    prompt_tokens: u64,
    /// (provider, model); a change re-bills the full prompt and is counted as a miss.
    model: (&'a str, &'a str),
    timestamp_ms: u64,
    /// Sticky: some earlier request in this chain reported cache activity. Distinguishes a
    /// total miss on a read-only-reporting provider (OpenAI-style, writes unreported) from
    /// a provider that never reports caching at all.
    reported_cache: bool,
}

/// Estimate per-source cache waste by chaining each session's requests in order and
/// comparing every request's cache reads against the previous request's prompt.
///
/// This follows pi's cache-stats algorithm (earendil-works/pi, core/cache-stats.ts) with
/// adaptations for reconstructed logs: sidechain requests are excluded (subagents have
/// their own prompt caches), conservatively undercounted events break the chain (their
/// buckets are clamped dedupe deltas, not a real request's shape), and a prompt shrinking
/// below half of its predecessor stands in for the compaction/clear markers the logs don't
/// carry — the context legitimately changed, so the re-billing is not counted as waste.
/// Chains start at the first event a caller passes in, so window filters only undercount at
/// their leading edge.
fn compute_cache_waste<'a>(
    events: impl IntoIterator<Item = &'a UsageEvent>,
) -> HashMap<&'static str, CacheWaste> {
    let mut chains: HashMap<(&'a str, &'a str, &'a str), CacheChainState<'a>> = HashMap::new();
    let mut by_source: HashMap<&'static str, CacheWaste> = HashMap::new();
    for event in events {
        if event.sidechain {
            continue;
        }
        let Some(session_id) = event.session_id.as_deref() else {
            continue;
        };
        // A chain is one process's linear request stream, which is the transcript file, not
        // the session: codex spawned/resumed threads share a session id across rollout
        // files, and interleaving them fabricates misses. OpenCode is the exception — it
        // persists one file per message, so there the session is the stream.
        let thread = if event.source == "opencode" {
            ""
        } else {
            event.source_path.as_ref()
        };
        let key = (event.source, session_id, thread);
        if event.conservative_undercount {
            chains.remove(&key);
            continue;
        }
        let tokens = &event.tokens;
        let prompt_tokens = tokens
            .uncached_input
            .saturating_add(tokens.cache_read)
            .saturating_add(tokens.cache_write);
        if prompt_tokens == 0 {
            continue;
        }
        let cached = tokens.cache_read.saturating_add(tokens.cache_write);
        let model = (
            event.provider.as_deref().unwrap_or(""),
            event.model.as_deref().unwrap_or(""),
        );
        let mut reported_cache = cached > 0;
        if let Some(prev) = chains.get(&key) {
            reported_cache |= prev.reported_cache;
            // A current cache write alone doesn't qualify: the chain's first write creates
            // the cache, so the previous prompt could not have been served from it. A read
            // proves a cache already existed (OpenAI-style writes are unreported), as does
            // earlier reported activity.
            if (tokens.cache_read > 0 || prev.reported_cache)
                && prompt_tokens.saturating_mul(2) >= prev.prompt_tokens
            {
                let missed = prev
                    .prompt_tokens
                    .min(prompt_tokens)
                    .saturating_sub(tokens.cache_read);
                if missed > CACHE_MISS_NOISE_FLOOR_TOKENS {
                    let waste = by_source.entry(event.source).or_default();
                    waste.miss_count += 1;
                    waste.missed_tokens = waste.missed_tokens.saturating_add(missed);
                    waste.missed_cost_usd += cache_miss_cost_usd(event, missed);
                    if model != prev.model {
                        waste.model_switch_misses += 1;
                    } else if event.timestamp_ms.saturating_sub(prev.timestamp_ms) >= CACHE_TTL_MS {
                        waste.idle_misses += 1;
                    }
                }
            }
        }
        chains.insert(
            key,
            CacheChainState {
                prompt_tokens,
                model,
                timestamp_ms: event.timestamp_ms,
                reported_cache,
            },
        );
    }
    by_source
}

/// Extra USD paid for `missed_tokens` vs. reading them from cache. Missed tokens can only
/// land in the uncached-input or cache-write buckets, so the paid rate is the blend of this
/// event's own paid buckets at catalog rates; 0 when the model is unpriced.
fn cache_miss_cost_usd(event: &UsageEvent, missed_tokens: u64) -> f64 {
    let Some(model) = event.model.as_deref() else {
        return 0.0;
    };
    let Some(rates) = rates_for(event.provider.as_deref(), model) else {
        return 0.0;
    };
    let cache_write_1h = event.tokens.cache_write_1h.min(event.tokens.cache_write);
    let cache_write_5m = event.tokens.cache_write - cache_write_1h;
    let paid_tokens = event
        .tokens
        .uncached_input
        .saturating_add(event.tokens.cache_write);
    if paid_tokens == 0 {
        return 0.0;
    }
    // Rates are nano-USD per million tokens; dividing by a million yields nano-USD per token.
    let paid_nanos = ((event.tokens.uncached_input as u128) * (rates.input as u128)
        + (cache_write_5m as u128) * (rates.cache_write_5m as u128)
        + (cache_write_1h as u128) * (rates.cache_write_1h as u128)) as f64
        / 1_000_000.0;
    let paid_per_token = paid_nanos / paid_tokens as f64;
    let read_per_token = rates.cache_read as f64 / 1_000_000.0;
    missed_tokens as f64 * (paid_per_token - read_per_token).max(0.0) / 1_000_000_000.0
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
    let assembly_start = Instant::now();
    let (events, warnings) = assemble_usage_events(query.source, query.cache_path.as_deref());
    usage_timing(assembly_start, || {
        format!("assemble total ({} events)", events.len())
    });
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
    const SCANNERS: [(SourceFilter, SourceScanner); 8] = [
        (SourceFilter::Claude, scan_claude),
        (SourceFilter::Codex, scan_codex),
        (SourceFilter::Opencode, scan_opencode),
        (SourceFilter::Pi, scan_pi),
        (SourceFilter::Omp, scan_omp),
        (SourceFilter::OpenClaw, scan_openclaw),
        (SourceFilter::Cursor, scan_cursor),
        (SourceFilter::Copilot, scan_copilot),
    ];
    for (filter, scanner) in SCANNERS {
        if source.is_none_or(|selected| selected == filter) {
            let scanner_start = Instant::now();
            if let Err(error) = scanner(&mut events, &mut warnings, cache.as_mut()) {
                warnings.push(format!("{} scanner: {error:#}", filter.as_str()));
            }
            usage_timing(scanner_start, || format!("{} scanner", filter.as_str()));
        }
    }
    publish_scan_progress(None);

    let reconcile_start = Instant::now();
    crate::sources::claude::reconcile_usage(&mut events);
    crate::sources::codex::reconcile_usage(&mut events);
    crate::sources::cursor::reconcile_usage(&mut events);
    crate::sources::copilot::reconcile_usage(&mut events);
    crate::sources::opencode::reconcile_usage(&mut events);
    usage_timing(reconcile_start, || {
        format!("reconcile ({} events kept)", events.len())
    });
    let sort_start = Instant::now();
    events.par_sort_by(|a, b| {
        (a.timestamp_ms, &a.source_path, a.source_order).cmp(&(
            b.timestamp_ms,
            &b.source_path,
            b.source_order,
        ))
    });
    usage_timing(sort_start, || "sort".to_string());
    (events, warnings)
}

/// When `MEMEX_USAGE_TIMING` is set (and not "0"), prints per-phase scan timings to
/// stderr. In the TUI, redirect stderr to a file (`MEMEX_USAGE_TIMING=1 memex 2>/tmp/t.log`)
/// so the lines don't corrupt the terminal.
fn usage_timing(start: Instant, message: impl FnOnce() -> String) {
    static ENABLED: Lazy<bool> =
        Lazy::new(|| std::env::var_os("MEMEX_USAGE_TIMING").is_some_and(|value| value != "0"));
    if *ENABLED {
        eprintln!(
            "usage-timing {} {}ms",
            message(),
            start.elapsed().as_millis()
        );
    }
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

    fn into_event(self, source: &'static str, source_path: Arc<str>) -> UsageEvent {
        UsageEvent {
            source,
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

type FileParse = crate::sources::UsageParseOutput;
type UsageFileDep = crate::sources::UsageDependency;

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
        Self::maybe_compact(&connection);
        Ok(Self { connection })
    }

    /// Chunked saves rewrite blob rows continuously and freed pages are never returned to
    /// the filesystem, so the cache file can grow to a large multiple of its live data.
    /// Reclaim it once free pages dominate; a failure (e.g. another process holds the
    /// database) just leaves the file bloated until a later open.
    fn maybe_compact(connection: &Connection) {
        let stats = (|| -> rusqlite::Result<(i64, i64, i64)> {
            let single = |pragma: &str| connection.query_row(pragma, [], |row| row.get(0));
            Ok((
                single("PRAGMA page_count")?,
                single("PRAGMA freelist_count")?,
                single("PRAGMA page_size")?,
            ))
        })();
        if let Ok((page_count, freelist_count, page_size)) = stats
            && freelist_count.saturating_mul(page_size) >= 64 * 1024 * 1024
            && freelist_count >= page_count / 4
        {
            let _ = connection.execute_batch("VACUUM;");
        }
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
    let load_start = Instant::now();
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
    usage_timing(load_start, || {
        format!("{source} cache load ({} rows)", rows.len())
    });
    let stat_start = Instant::now();
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
    usage_timing(stat_start, || {
        format!("{source} stat ({} files)", files.len())
    });
    let decode_start = Instant::now();
    let hit_count = hits.len();
    let decoded = hits
        .into_par_iter()
        .map(|(index, key, blob)| {
            let source_path: Arc<str> = Arc::from(key.as_str());
            let events = postcard::from_bytes::<Vec<CachedUsageEvent>>(&blob).map(|events| {
                events
                    .into_iter()
                    .map(|event| event.into_event(source, source_path.clone()))
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
    usage_timing(decode_start, || {
        format!("{source} decode ({hit_count} cached files)")
    });
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
    let parse_start = Instant::now();
    let missing_count = missing.len();
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
    if missing_count > 0 {
        usage_timing(parse_start, || {
            format!("{source} parse ({missing_count} changed files)")
        });
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
    let files = crate::sources::claude::usage_files();
    scan_files_cached(
        SourceScan {
            source: "claude",
            parser_version: crate::sources::claude::VERSIONS.usage,
            volatile_reuse_ms: |_| None,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::claude::parse_usage_file(path).map(FileParse::cacheable),
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

fn scan_codex(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = crate::sources::codex::discover_rollouts()
        .into_iter()
        .map(|file| file.path)
        .collect::<Vec<_>>();
    let parents = crate::sources::codex::UsageParentIndex::new(&files);
    scan_files_cached_with(
        SourceScan {
            source: "codex",
            parser_version: crate::sources::codex::VERSIONS.usage,
            volatile_reuse_ms: |_| None,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::codex::parse_usage_file(path, &parents),
        |deps| parents.deps_match_current_candidates(deps),
    );
    Ok(())
}

fn scan_pi(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = crate::sources::pi::discover()
        .into_iter()
        .map(|file| file.path)
        .collect::<Vec<_>>();
    scan_files_cached(
        SourceScan {
            source: "pi",
            parser_version: crate::sources::pi::VERSIONS.usage,
            volatile_reuse_ms: |_| None,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::pi::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

fn scan_omp(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = crate::sources::omp::discover()
        .into_iter()
        .map(|file| file.path)
        .collect::<Vec<_>>();
    scan_files_cached(
        SourceScan {
            source: "omp",
            parser_version: crate::sources::omp::VERSIONS.usage,
            volatile_reuse_ms: |_| None,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::omp::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

fn scan_openclaw(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = crate::sources::openclaw::discover()
        .into_iter()
        .map(|file| file.path)
        .collect::<Vec<_>>();
    scan_files_cached(
        SourceScan {
            source: "openclaw",
            parser_version: crate::sources::openclaw::VERSIONS.usage,
            volatile_reuse_ms: |_| None,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::openclaw::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

fn scan_opencode(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = crate::sources::opencode::usage_files();
    scan_files_cached(
        SourceScan {
            source: "opencode",
            parser_version: crate::sources::opencode::VERSIONS.usage,
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
        |path| crate::sources::opencode::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

fn scan_cursor(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let databases = crate::sources::cursor::usage_databases();
    let start = out.len();
    scan_files_cached(
        SourceScan {
            source: "cursor",
            parser_version: crate::sources::cursor::VERSIONS.usage,
            volatile_reuse_ms: |_| Some(VOLATILE_DB_REUSE_MS),
        },
        &databases,
        cache,
        warnings,
        out,
        |path| crate::sources::cursor::parse_usage_database(path).map(FileParse::cacheable),
    );
    crate::sources::cursor::apply_projects(
        &mut out[start..],
        &crate::sources::cursor::project_by_session(),
    );
    Ok(())
}

fn scan_copilot(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = crate::sources::copilot::usage_files();
    scan_files_cached(
        SourceScan {
            source: "copilot",
            parser_version: crate::sources::copilot::VERSIONS.usage,
            volatile_reuse_ms: |_| None,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::copilot::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
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

    #[test]
    fn usage_parser_version_change_invalidates_cached_rows() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("usage-cache.sqlite3");
        let cache = UsageCache::open(&path).expect("open cache");
        cache
            .connection
            .execute(
                "INSERT INTO usage_file_cache(
                    source, path, parser_version, size, mtime_ns, scanned_at_ms,
                    events_blob, deps_blob
                 ) VALUES ('claude', '/tmp/session.jsonl', 1, 10, 20, 30, ?1, ?2)",
                params![
                    postcard::to_stdvec(&Vec::<CachedUsageEvent>::new()).unwrap(),
                    postcard::to_stdvec(&Vec::<UsageFileDep>::new()).unwrap()
                ],
            )
            .expect("seed stale cache row");

        assert!(
            cache
                .load_source("claude", 2)
                .expect("load new parser version")
                .is_empty()
        );
        let rows: i64 = cache
            .connection
            .query_row(
                "SELECT count(*) FROM usage_file_cache WHERE source = 'claude'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn openai_cached_input_is_a_subset() {
        let tokens = TokenBuckets::codex(100, 80, 10, 4);
        assert_eq!(tokens.uncached_input, 20);
        assert_eq!(tokens.cache_read, 80);
        assert_eq!(tokens.additive_total(), 110);
    }

    fn cache_event(
        session: &str,
        timestamp_ms: u64,
        model: &str,
        uncached: u64,
        read: u64,
        write: u64,
    ) -> UsageEvent {
        UsageEvent {
            source: "claude",
            source_path: Arc::from("log.jsonl"),
            source_record_id: None,
            session_id: Some(session.to_string()),
            request_id: None,
            message_id: None,
            timestamp_ms,
            project: None,
            provider: Some("anthropic".to_string()),
            model: Some(model.to_string()),
            tokens: TokenBuckets {
                raw_input: uncached,
                uncached_input: uncached,
                cache_read: read,
                cache_write: write,
                cache_write_1h: 0,
                output: 10,
                reasoning: 0,
            },
            source_cost_usd: None,
            dedupe_confidence: "exact",
            conservative_undercount: false,
            sidechain: false,
            source_order: 0,
        }
    }

    fn waste_for(events: &[UsageEvent]) -> Option<CacheWaste> {
        compute_cache_waste(events.iter()).remove("claude")
    }

    #[test]
    fn cache_idle_gap_miss_is_counted_and_attributed() {
        let events = vec![
            cache_event("s", 0, "claude-sonnet-4-6", 0, 0, 100_000),
            cache_event("s", 10 * 60 * 1000, "claude-sonnet-4-6", 0, 0, 100_500),
        ];
        let waste = waste_for(&events).expect("miss counted");
        assert_eq!(waste.miss_count, 1);
        assert_eq!(waste.missed_tokens, 100_000);
        assert_eq!(waste.idle_misses, 1);
        assert_eq!(waste.model_switch_misses, 0);
        // 100k tokens re-billed at the 5m cache-write rate ($3.75/M) vs read ($0.30/M).
        assert!((waste.missed_cost_usd - 0.345).abs() < 1e-9);
    }

    #[test]
    fn cache_warm_hit_is_not_a_miss() {
        let events = vec![
            cache_event("s", 0, "claude-sonnet-4-6", 0, 0, 100_000),
            cache_event("s", 60_000, "claude-sonnet-4-6", 0, 100_000, 500),
        ];
        assert!(waste_for(&events).is_none());
    }

    #[test]
    fn cache_model_switch_miss_is_attributed_to_the_switch() {
        let events = vec![
            cache_event("s", 0, "claude-sonnet-4-6", 0, 0, 100_000),
            cache_event("s", 60_000, "claude-opus-4-8", 0, 0, 100_000),
        ];
        let waste = waste_for(&events).expect("miss counted");
        assert_eq!(waste.miss_count, 1);
        assert_eq!(waste.model_switch_misses, 1);
        assert_eq!(waste.idle_misses, 0);
    }

    #[test]
    fn cache_prompt_shrink_is_treated_as_context_reset() {
        // A prompt below half of its predecessor stands in for compaction/clear: the first
        // post-shrink request is exempt, and the chain rebases onto the shrunk prompt.
        let events = vec![
            cache_event("s", 0, "claude-sonnet-4-6", 0, 0, 100_000),
            cache_event("s", 60_000, "claude-sonnet-4-6", 0, 0, 20_000),
            cache_event("s", 120_000, "claude-sonnet-4-6", 0, 0, 20_500),
        ];
        let waste = waste_for(&events).expect("post-reset miss counted");
        assert_eq!(waste.miss_count, 1);
        assert_eq!(waste.missed_tokens, 20_000);
    }

    #[test]
    fn cache_miss_below_noise_floor_is_ignored() {
        let events = vec![
            cache_event("s", 0, "claude-sonnet-4-6", 0, 0, 10_000),
            cache_event("s", 60_000, "claude-sonnet-4-6", 0, 9_500, 1_000),
        ];
        assert!(waste_for(&events).is_none());
    }

    #[test]
    fn cache_first_write_after_uncached_prompts_is_not_a_miss() {
        // The chain's first cache write creates the cache; the earlier uncached prompt
        // could not have been served from it. Once the chain has reported cache activity,
        // a later write-only turn is a genuine full miss.
        let events = vec![
            cache_event("s", 0, "claude-sonnet-4-6", 50_000, 0, 0),
            cache_event("s", 60_000, "claude-sonnet-4-6", 0, 0, 52_000),
            cache_event("s", 120_000, "claude-sonnet-4-6", 0, 0, 53_000),
        ];
        let waste = waste_for(&events).expect("post-write miss counted");
        assert_eq!(waste.miss_count, 1);
        assert_eq!(waste.missed_tokens, 52_000);
    }

    #[test]
    fn cache_never_reported_provider_is_not_counted() {
        let events = vec![
            cache_event("s", 0, "claude-sonnet-4-6", 50_000, 0, 0),
            cache_event("s", 60_000, "claude-sonnet-4-6", 50_500, 0, 0),
        ];
        assert!(waste_for(&events).is_none());
    }

    #[test]
    fn cache_read_only_provider_total_miss_counts_after_reported_cache() {
        // OpenAI-style: reads reported, writes not. Once cache activity has been seen, a
        // zero-cache request is a total miss.
        let mut first = cache_event("s", 0, "gpt-5.4", 10_000, 40_000, 0);
        first.provider = Some("openai".to_string());
        let mut second = cache_event("s", 60_000, "gpt-5.4", 50_500, 0, 0);
        second.provider = Some("openai".to_string());
        let waste = waste_for(&[first, second]).expect("total miss counted");
        assert_eq!(waste.miss_count, 1);
        assert_eq!(waste.missed_tokens, 50_000);
        // 50k tokens at gpt-5.4 input ($2.50/M) vs cached ($0.25/M).
        assert!((waste.missed_cost_usd - 0.1125).abs() < 1e-9);
    }

    #[test]
    fn cache_sidechain_events_are_excluded_from_chains() {
        let mut sidechain = cache_event("s", 30_000, "claude-sonnet-4-6", 0, 0, 5_000);
        sidechain.sidechain = true;
        let events = vec![
            cache_event("s", 0, "claude-sonnet-4-6", 0, 0, 100_000),
            sidechain,
            cache_event("s", 60_000, "claude-sonnet-4-6", 0, 100_000, 500),
        ];
        assert!(waste_for(&events).is_none());
    }

    #[test]
    fn cache_conservative_events_break_the_chain() {
        // Clamped dedupe deltas do not describe a real request's prompt; neither the
        // conservative event nor its successor may be counted against the chain.
        let mut clamped = cache_event("s", 30_000, "claude-sonnet-4-6", 0, 0, 40_000);
        clamped.conservative_undercount = true;
        let events = vec![
            cache_event("s", 0, "claude-sonnet-4-6", 0, 0, 100_000),
            clamped,
            cache_event("s", 60_000, "claude-sonnet-4-6", 0, 0, 100_500),
        ];
        assert!(waste_for(&events).is_none());
    }

    #[test]
    fn cache_sessions_chain_independently() {
        let events = vec![
            cache_event("a", 0, "claude-sonnet-4-6", 0, 0, 100_000),
            cache_event("b", 60_000, "claude-sonnet-4-6", 0, 0, 100_000),
        ];
        assert!(waste_for(&events).is_none());
    }

    #[test]
    fn cache_parallel_threads_sharing_a_session_chain_per_file() {
        // Codex spawned/resumed threads share a session id across rollout files; comparing
        // across files fabricates misses.
        let mut thread = cache_event("s", 30_000, "claude-sonnet-4-6", 0, 0, 90_000);
        thread.source_path = Arc::from("thread.jsonl");
        let events = vec![
            cache_event("s", 0, "claude-sonnet-4-6", 0, 0, 100_000),
            thread,
            cache_event("s", 60_000, "claude-sonnet-4-6", 0, 100_000, 500),
        ];
        assert!(waste_for(&events).is_none());
    }

    #[test]
    fn cache_opencode_chains_across_per_message_files() {
        let mut first = cache_event("s", 0, "claude-sonnet-4-6", 0, 0, 100_000);
        first.source = "opencode";
        first.source_path = Arc::from("msg-1.json");
        let mut second = cache_event("s", 10 * 60 * 1000, "claude-sonnet-4-6", 0, 0, 100_500);
        second.source = "opencode";
        second.source_path = Arc::from("msg-2.json");
        let waste = compute_cache_waste([&first, &second])
            .remove("opencode")
            .expect("miss counted");
        assert_eq!(waste.miss_count, 1);
        assert_eq!(waste.idle_misses, 1);
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

        let events =
            crate::sources::claude::parse_usage_file(&transcript).expect("scan transcript");

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
                crate::sources::claude::parse_usage_file(path).map(FileParse::cacheable)
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
                    parser_version: crate::sources::cursor::VERSIONS.usage,
                    volatile_reuse_ms: |_| Some(VOLATILE_DB_REUSE_MS),
                },
                &files,
                Some(&mut cache),
                &mut warnings,
                &mut events,
                |path| crate::sources::cursor::parse_usage_database(path).map(FileParse::cacheable),
            );
            assert_eq!(warnings, Vec::<String>::new());
            crate::sources::cursor::apply_projects(&mut events, project_by_session);
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
        let omp_root = tmp.path().join("omp");
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
            ("PI_CONFIG_DIR", Some(omp_root.as_os_str())),
            ("XDG_DATA_HOME", None),
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
        let omp_root = tmp.path().join("omp");
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

        let _env = EnvVarGuard::set_os(&[
            ("PI_CODING_AGENT_SESSION_DIR", Some(tmp.path().as_os_str())),
            ("PI_CODING_AGENT_DIR", None),
            ("PI_CONFIG_DIR", Some(omp_root.as_os_str())),
            ("XDG_DATA_HOME", None),
        ]);
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
            source: "claude",
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
            source: "claude",
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
