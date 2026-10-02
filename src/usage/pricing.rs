//! Model pricing catalog and cache-waste estimation.
//!
//! The catalog is deliberately small and versioned: unknown models stay
//! unpriced instead of inheriting a guessed family rate.

use super::compact::UsageEventView;
use super::{CacheWaste, CostMode, UsageEventData, UsageReport, UsageSummary};
use std::collections::HashMap;

impl CacheWaste {
    pub(crate) fn absorb(&mut self, other: &CacheWaste) {
        self.missed_tokens = self.missed_tokens.saturating_add(other.missed_tokens);
        self.missed_cost_usd += other.missed_cost_usd;
        self.miss_count += other.miss_count;
        self.idle_misses += other.idle_misses;
        self.model_switch_misses += other.model_switch_misses;
    }
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
pub(crate) fn compute_cache_waste<'a>(
    events: impl IntoIterator<Item = UsageEventView<'a>>,
) -> HashMap<&'static str, CacheWaste> {
    let mut chains: HashMap<(&'a str, &'a str, &'a str), CacheChainState<'a>> = HashMap::new();
    let mut by_source: HashMap<&'static str, CacheWaste> = HashMap::new();
    let mut rate_cache = RateCache::default();
    for event in events {
        if event.sidechain {
            continue;
        }
        let Some(session_id) = event.session_id else {
            continue;
        };
        // A chain is one process's linear request stream, which is the transcript file, not
        // the session: codex spawned/resumed threads share a session id across rollout
        // files, and interleaving them fabricates misses. OpenCode is the exception — it
        // persists one file per message, so there the session is the stream.
        let thread = if event.source == "opencode" {
            ""
        } else {
            event.source_path
        };
        let key = (event.source, session_id, thread);
        if event.cache_chain_excluded {
            chains.remove(&key);
            continue;
        }
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
        let model = (event.provider.unwrap_or(""), event.model.unwrap_or(""));
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
                    waste.missed_cost_usd +=
                        cache_miss_cost_usd_cached(&event, missed, &mut rate_cache);
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
#[allow(dead_code)]
fn cache_miss_cost_usd(event: &UsageEventView<'_>, missed_tokens: u64) -> f64 {
    let mut cache = RateCache::default();
    cache_miss_cost_usd_cached(event, missed_tokens, &mut cache)
}

fn cache_miss_cost_usd_cached(
    event: &UsageEventView<'_>,
    missed_tokens: u64,
    cache: &mut RateCache,
) -> f64 {
    let Some(model) = event.model else {
        return 0.0;
    };
    let Some(rates) = cache.rates_for(event.provider, model) else {
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

// Rates are nano-USD per million tokens. The catalog is deliberately small and versioned:
// unknown models remain unpriced instead of silently inheriting a guessed family rate.
pub(crate) const PRICE_CATALOG_ID: &str = "official-api-prices-2026-07-15";

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

#[allow(dead_code)]
pub(crate) fn event_cost_nanos<S: std::ops::Deref<Target = str>, P>(
    event: &UsageEventData<S, P>,
    mode: CostMode,
) -> Option<u64> {
    let mut cache = RateCache::default();
    event_cost_nanos_cached(event, mode, &mut cache)
}

pub(crate) fn event_cost_nanos_cached<S: std::ops::Deref<Target = str>, P>(
    event: &UsageEventData<S, P>,
    mode: CostMode,
    rates: &mut RateCache,
) -> Option<u64> {
    let source = event
        .source_cost_usd
        .filter(|value| value.is_finite() && *value >= 0.0)
        .and_then(|value| {
            let nanos = value * 1_000_000_000.0;
            (nanos <= u64::MAX as f64).then_some(nanos.round() as u64)
        });
    match mode {
        CostMode::Source => source,
        CostMode::Auto => source.or_else(|| {
            (!event.cost_authoritative)
                .then(|| calculated_cost_nanos_cached(event, rates))
                .flatten()
        }),
        CostMode::Reprice => calculated_cost_nanos_cached(event, rates),
    }
}

/// Caches catalog lookups per distinct raw provider/model pair so the event loop
/// avoids normalizing (allocating lowercase strings) on every event.
#[derive(Default)]
pub(crate) struct RateCache {
    entries: Vec<(Option<String>, String, Option<Rates>)>,
}

impl RateCache {
    fn rates_for(&mut self, provider: Option<&str>, model: &str) -> Option<Rates> {
        for (cached_provider, cached_model, rates) in &self.entries {
            if cached_provider.as_deref() == provider && cached_model == model {
                return *rates;
            }
        }
        let rates = rates_for(provider, model);
        self.entries
            .push((provider.map(str::to_owned), model.to_owned(), rates));
        rates
    }
}

#[allow(dead_code)]
fn calculated_cost_nanos<S: std::ops::Deref<Target = str>, P>(
    event: &UsageEventData<S, P>,
) -> Option<u64> {
    let mut cache = RateCache::default();
    calculated_cost_nanos_cached(event, &mut cache)
}

fn calculated_cost_nanos_cached<S: std::ops::Deref<Target = str>, P>(
    event: &UsageEventData<S, P>,
    cache: &mut RateCache,
) -> Option<u64> {
    let rates = cache.rates_for(event.provider.as_deref(), event.model.as_deref()?)?;
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

/// Shared per-event totals aggregation so single and merged reports cannot drift.
pub(crate) fn accumulate_usage_event(
    report: &mut UsageReport,
    by_source: &mut HashMap<&'static str, UsageSummary>,
    event: &UsageEventView<'_>,
    cost_mode: CostMode,
    rate_cache: &mut RateCache,
) {
    let total = event.tokens.additive_total();
    report.events += 1;
    if !event.token_usage_available {
        report.unavailable_token_events += 1;
        report.credits = Some(report.credits.unwrap_or(0.0) + event.credits.unwrap_or(0.0));
        let row = by_source
            .entry(event.source)
            .or_insert_with(|| UsageSummary {
                source: event.source.to_string(),
                ..UsageSummary::default()
            });
        row.events += 1;
        row.unavailable_token_events += 1;
        row.credits = Some(row.credits.unwrap_or(0.0) + event.credits.unwrap_or(0.0));
        return;
    }
    report.total_tokens = report.total_tokens.saturating_add(total);
    report.unknown_model_events += u64::from(event.model.is_none());
    report.conservative_events += u64::from(event.conservative_undercount);
    let cost = event_cost_nanos_cached(event, cost_mode, rate_cache);
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

#[cfg(test)]
mod tests {
    use super::super::cache_event;
    use super::super::compact::UsageAssembly;
    use super::super::{TokenBuckets, UsageEvent};
    use super::*;
    use std::sync::Arc;

    fn waste_for(events: &[UsageEvent]) -> Option<CacheWaste> {
        let assembly = UsageAssembly::new(events.to_vec(), None);
        compute_cache_waste((0..assembly.len()).map(|index| assembly.view(index))).remove("claude")
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
    fn hermes_aggregate_rows_are_excluded_from_cache_chains() {
        let mut first = cache_event("s", 0, "claude-sonnet-4-6", 0, 0, 100_000);
        first.source = "hermes";
        first.source_path = Arc::from("hermes.db");
        first.cache_chain_excluded = true;
        let mut second = cache_event("s", 60_000, "claude-sonnet-4-6", 0, 0, 100_500);
        second.source = "hermes";
        second.source_path = Arc::from("hermes.db");
        second.cache_chain_excluded = true;
        let assembly = UsageAssembly::new(vec![first, second], None);
        let waste = compute_cache_waste([assembly.view(0), assembly.view(1)]);
        assert!(!waste.contains_key("hermes"));
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
        let assembly = UsageAssembly::new(vec![first, second], None);
        let waste = compute_cache_waste([assembly.view(0), assembly.view(1)])
            .remove("opencode")
            .expect("miss counted");
        assert_eq!(waste.miss_count, 1);
        assert_eq!(waste.idle_misses, 1);
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
            credits: None,
            token_usage_available: true,
            source_cost_usd: None,
            cost_authoritative: false,
            dedupe_confidence: "exact",
            conservative_undercount: false,
            cache_chain_excluded: false,
            sidechain: false,
            permission_review: false,
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
            credits: None,
            token_usage_available: true,
            source_cost_usd: Some(0.0),
            cost_authoritative: false,
            dedupe_confidence: "exact",
            conservative_undercount: false,
            cache_chain_excluded: false,
            sidechain: false,
            permission_review: false,
            source_order: 0,
        };
        assert_eq!(event_cost_nanos(&event, CostMode::Auto), Some(0));
        assert_eq!(event_cost_nanos(&event, CostMode::Reprice), Some(300_000));
    }
}
