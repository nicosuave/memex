//! Reconstructed local token usage.
//!
//! This module intentionally does not model provider quota percentages. Local logs are useful for
//! request-level accounting, but they are not authoritative subscription-limit telemetry.

use crate::analytics::ProjectGrouping;
use crate::types::SourceFilter;
use clap::ValueEnum;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

mod cache;
mod compact;
mod facts;
mod filter;
#[cfg(test)]
mod lifecycle_tests;
mod merge;
mod pricing;
mod progress;
mod query;
mod scan;
mod snapshot;
#[cfg(test)]
pub(crate) use self::pricing::event_cost_nanos;
pub use self::progress::{UsageScanProgress, usage_scan_progress};
pub(crate) use self::progress::{publish_remote_scan_progress, with_usage_progress};
pub use self::query::{scan_usage, scan_usage_activity, visit_usage_activity};
/// Shared event factory for unit tests across usage submodules.
#[cfg(test)]
pub(crate) fn cache_event(
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
        cost_authoritative: false,
        dedupe_confidence: "exact",
        conservative_undercount: false,
        cache_chain_excluded: false,
        sidechain: false,
        permission_review: false,
        source_order: 0,
    }
}

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
    /// Include internal AI permission-review sessions in reconstructed usage.
    pub include_reviews: bool,
    pub cache_path: Option<PathBuf>,
    /// How long a retained snapshot may be served before its freshness is re-checked.
    /// A re-check that finds nothing changed reuses the snapshot without any decode,
    /// so this is a check interval, not a discard deadline. Filters (`since_ms`,
    /// `project`, `session_keys`, ...) apply after assembly, so repeated queries over
    /// the same corpus share one scan. Zero bypasses retention entirely (one-shot).
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

pub type UsageEvent = UsageEventData<String, Arc<str>>;

/// The same event fields are used by parsers, borrowed report views, and compact storage.
/// Only the text representation changes; the public event and wire formats stay owned.
#[derive(Clone, Debug, Serialize)]
pub struct UsageEventData<S, P> {
    pub source: &'static str,
    /// Shared across every event of a file: assembled scans materialize millions of
    /// events, and per-event owned paths dominated allocation time.
    pub source_path: P,
    pub source_record_id: Option<S>,
    pub session_id: Option<S>,
    pub request_id: Option<S>,
    pub message_id: Option<S>,
    pub timestamp_ms: u64,
    pub project: Option<S>,
    pub provider: Option<S>,
    pub model: Option<S>,
    pub tokens: TokenBuckets,
    pub source_cost_usd: Option<f64>,
    /// A missing source cost is intentionally covered by an authoritative aggregate.
    #[serde(skip)]
    pub(crate) cost_authoritative: bool,
    pub dedupe_confidence: &'static str,
    pub conservative_undercount: bool,
    /// The source row is an aggregate rather than one request in a cache chain.
    #[serde(skip)]
    pub(crate) cache_chain_excluded: bool,
    #[serde(skip)]
    pub(crate) sidechain: bool,
    #[serde(skip)]
    pub(crate) permission_review: bool,
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UsageActivityPoint {
    pub source: &'static str,
    pub timestamp_ms: u64,
    pub total_tokens: u64,
}

/// When `MEMEX_USAGE_TIMING` is set (and not "0"), prints per-phase scan timings to
/// stderr. In the TUI, redirect stderr to a file (`MEMEX_USAGE_TIMING=1 memex 2>/tmp/t.log`)
/// so the lines don't corrupt the terminal.
pub(crate) fn usage_timing(start: Instant, message: impl FnOnce() -> String) {
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
