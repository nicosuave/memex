//! Report construction over snapshots and facts.
//!
//! Single-source queries read one partition; combined queries merge shared
//! partitions; cold starts reuse valid facts to populate snapshots or serve one-shots.

use super::cache::UsageCache;
use super::compact::UsageAssembly;
use super::facts::{read_fact_assembly, read_fact_points, scan_usage_from_facts};
use super::filter::filtered_events;
use super::merge::{MergedPos, MergedView, filtered_merged_positions};
use super::pricing::{PRICE_CATALOG_ID, RateCache, accumulate_usage_event, compute_cache_waste};
use super::scan::{FileFingerprint, SCANNERS};
use super::snapshot::{
    MAX_PARTITIONS, MergedSnapshot, PartitionEntry, Snapshot, USAGE_SCAN_LOCK, ensure_snapshot,
    evict_oldest, lock_merged, lock_partitions, populate_after_failed_validation, refresh_merged,
    validate_partition,
};
use super::usage_timing;
use super::{UsageActivityPoint, UsageEvent, UsageQuery, UsageReport, UsageSummary};
use crate::types::SourceFilter;
use anyhow::Result;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Filters the assembled events exactly like `scan_usage`, but returns lightweight chart
/// points instead of deep-cloning full events out of the memoized assembly. The boolean is
/// true when any scanner reported a warning, i.e. the totals may be partial.
pub fn scan_usage_activity(query: &UsageQuery) -> Result<(Vec<UsageActivityPoint>, bool)> {
    let (points, warnings) = scan_usage_activity_with_warnings(query)?;
    Ok((points, !warnings.is_empty()))
}

pub(crate) fn scan_usage_activity_with_warnings(
    query: &UsageQuery,
) -> Result<(Vec<UsageActivityPoint>, Vec<String>)> {
    let mut points = Vec::new();
    let warnings = visit_usage_activity(query, |point| points.push(point))?;
    Ok((points, warnings))
}

/// Visit matching chart points without allocating a per-event result vector.
/// The callback runs outside any usage lock over a shared snapshot, so it may safely
/// start another usage query (each acquisition is short); it must still avoid
/// mutating state the scan itself reads.
pub fn visit_usage_activity(
    query: &UsageQuery,
    mut visit: impl FnMut(UsageActivityPoint),
) -> Result<Vec<String>> {
    // Cold-start fast path before touching snapshots (verification mode below
    // always computes both paths instead).
    if !facts_read_enabled()
        && let Some((points, warnings)) = try_cold_serve_points(query)?
    {
        for point in points {
            visit(point);
        }
        return Ok(warnings);
    }
    // Order-4A verification: answer from facts when enabled, self-checked
    // against the assembly points with fallback.
    if facts_read_enabled()
        && let Some(cache_path) = query.cache_path.as_deref()
    {
        let mut assembly_points = Vec::new();
        let warnings = visit_inner(query, &mut |point| assembly_points.push(point))?;
        match read_fact_points(query, cache_path) {
            Ok(facts_points) if facts_points == assembly_points => {
                for point in facts_points {
                    visit(point);
                }
            }
            _ => {
                for point in assembly_points {
                    visit(point);
                }
            }
        }
        return Ok(warnings);
    }
    visit_inner(query, visit)
}

/// Cold-start fast path for activity points. Warms snapshots for retaining
/// queries; one-shots serve without retaining.
fn try_cold_serve_points(
    query: &UsageQuery,
) -> Result<Option<(Vec<UsageActivityPoint>, Vec<String>)>> {
    let Some(ready) = cold_facts_ready(query) else {
        return Ok(None);
    };
    let cache_path = query
        .cache_path
        .as_deref()
        .expect("cold serve needs a cache");
    if query.memo_ttl_ms != 0 {
        // Retaining callers answer from the assembly we load here. Reading
        // points first would scan the same facts twice on the first query.
        populate_snapshots_from_facts(query.source, cache_path, &ready.per_source);
        return Ok(None);
    }
    let Ok(points) = read_fact_points(query, cache_path) else {
        return Ok(None);
    };
    if !ready.generations_current(cache_path) {
        return Ok(None);
    }
    Ok(Some((points, ready.warnings)))
}

/// Cold-start prerequisites: no usable in-memory snapshot, but facts current on
/// disk for the whole scope, plus the stored warnings to answer with.
struct ColdFacts {
    warnings: Vec<String>,
    per_source: Vec<ColdSource>,
}

struct ColdSource {
    filter: SourceFilter,
    warnings: Vec<String>,
    fingerprint: FileFingerprint,
    generation: String,
}

impl ColdFacts {
    fn generations_current(&self, path: &Path) -> bool {
        let Ok(cache) = UsageCache::open(path) else {
            return false;
        };
        self.per_source.iter().all(|source| {
            cache
                .fact_generation(source.filter.as_str())
                .is_ok_and(|current| current.as_deref() == Some(source.generation.as_str()))
        })
    }
}

/// Cold-start fast path prerequisites: no usable in-memory snapshot, but facts
/// current on disk for the whole scope.
/// Validity here mirrors what a rebuild would find (same fingerprint scheme);
/// any doubt returns `None` and the normal path rebuilds instead.
fn cold_facts_ready(query: &UsageQuery) -> Option<ColdFacts> {
    let cache_path = query.cache_path.as_deref()?;
    let ttl = Duration::from_millis(query.memo_ttl_ms);
    if !ttl.is_zero() {
        // Usable snapshots beat facts reads (borrowed views, no mapping), so
        // cold-serve only when the normal path would have to build.
        match query.source {
            Some(filter) => {
                if lock_partitions().contains_key(&(filter, query.cache_path.clone())) {
                    return None;
                }
            }
            None => {
                if lock_merged().contains_key(&query.cache_path) {
                    return None;
                }
                let store = lock_partitions();
                if SCANNERS
                    .iter()
                    .all(|(filter, _)| store.contains_key(&(*filter, query.cache_path.clone())))
                {
                    // All partitions resident: the normal path only re-checks and
                    // merges, which is cheaper than a facts read.
                    return None;
                }
            }
        }
    }
    // Capture the generation before validating/reading. Recheck after reads so
    // a concurrent refresh cannot pair old rows with a newer checkpoint.
    let cache = UsageCache::open(cache_path).ok()?;
    let mut per_source = Vec::new();
    for (filter, _) in SCANNERS {
        if query.source.is_some_and(|selected| selected != filter) {
            continue;
        }
        let generation = cache.fact_generation(filter.as_str()).ok()??;
        let observed = validate_partition(filter, Some(cache_path), None)?;
        if !observed.valid {
            if !ttl.is_zero() && query.source == Some(filter) {
                populate_after_failed_validation(filter, cache_path, observed.fingerprint);
            }
            return None;
        }
        let fingerprint = observed.fingerprint;
        let (_, _, warnings) = cache.fact_sync(filter.as_str()).ok()??;
        per_source.push(ColdSource {
            filter,
            warnings,
            fingerprint,
            generation,
        });
    }
    let warnings = per_source
        .iter()
        .flat_map(|source| source.warnings.iter().cloned())
        .collect();
    Some(ColdFacts {
        warnings,
        per_source,
    })
}

/// Build snapshot assemblies from canonical, ordered facts, interning borrowed
/// SQLite text directly without intermediate owned events or reconciliation.
/// Best-effort and non-blocking — if a refresh is already running, it will
/// publish anyway. Failures simply leave the store empty for the next attempt.
fn populate_snapshots_from_facts(
    source: Option<SourceFilter>,
    cache_path: &Path,
    per_source: &[ColdSource],
) {
    let Ok(_refresh) = USAGE_SCAN_LOCK.try_lock() else {
        return;
    };
    let Ok(cache) = UsageCache::open(cache_path) else {
        return;
    };
    for source in per_source {
        let filter = &source.filter;
        if lock_partitions().contains_key(&(*filter, Some(cache_path.to_path_buf()))) {
            continue;
        }
        let Ok(assembly) = read_fact_assembly(&cache.connection, *filter) else {
            continue;
        };
        if cache
            .fact_generation(filter.as_str())
            .ok()
            .flatten()
            .as_deref()
            != Some(source.generation.as_str())
        {
            continue;
        }
        let assembly = Arc::new(assembly);
        let mut store = lock_partitions();
        if store.len() >= MAX_PARTITIONS {
            let key = (*filter, Some(cache_path.to_path_buf()));
            evict_oldest(&mut store, &key, |entry| entry.checked_at);
        }
        store.insert(
            (*filter, Some(cache_path.to_path_buf())),
            PartitionEntry {
                checked_at: Instant::now(),
                fingerprint: source.fingerprint.clone(),
                fact_generation: Some(source.generation.clone()),
                assembly,
                warnings: Arc::new(source.warnings.clone()),
            },
        );
    }
    if source.is_none() {
        // Index over the fresh assemblies; partition checks inside reuse them.
        refresh_merged(&Some(cache_path.to_path_buf()));
    }
}

pub fn scan_usage(query: &UsageQuery) -> Result<UsageReport> {
    // Cold-start fast path: valid facts on disk but nothing usable in memory —
    // answer without decoding, sorting, or compacting anything. Verification
    // mode always computes both paths instead, so it stays below that gate.
    if !facts_read_enabled()
        && let Some(report) = try_cold_serve_report(query)?
    {
        return Ok(report);
    }
    let snapshot_start = Instant::now();
    let snapshot = ensure_snapshot(query)?;
    usage_timing(snapshot_start, || "snapshot ensure".to_string());
    let report = match snapshot {
        Snapshot::Partition(assembled, warnings) => {
            scan_single(query, &assembled, warnings.as_ref())
        }
        Snapshot::Merged(merged) => scan_merged(query, merged),
    }?;
    // Order-4A verification: when enabled, answer from canonical facts and
    // self-check the totals against the assembly report. Any incompleteness
    // (e.g. facts predating this binary) falls back to the assembly report.
    if facts_read_enabled()
        && let Some(cache_path) = query.cache_path.as_deref()
    {
        let facts_start = Instant::now();
        let facts_report = scan_usage_from_facts(query, cache_path, &report.warnings);
        usage_timing(facts_start, || "facts report".to_string());
        match facts_report {
            Ok(facts_report)
                if serde_json::to_value(&facts_report)? == serde_json::to_value(&report)? =>
            {
                return Ok(facts_report);
            }
            // Incomplete facts (e.g. rows predating this binary): the assembly
            // report stands. Order 4B makes facts primary with backfill.
            _ => {
                usage_timing(facts_start, || "facts fallback".to_string());
            }
        }
    }
    Ok(report)
}

/// Whether reports read canonical facts instead of assembled events. Order-4A
/// verification path: populate-then-read must digest-match the assembly path.
fn facts_read_enabled() -> bool {
    static ENABLED: Lazy<bool> =
        Lazy::new(|| std::env::var_os("MEMEX_USAGE_FACTS_READ").is_some_and(|value| value != "0"));
    *ENABLED
}

/// Cold-start fast path for reports: when no usable snapshot exists but facts
/// are current on disk, answer from facts without decoding, sorting, or
/// compacting anything. Warms the snapshot store for follow-up queries when the
/// query retains (non-zero TTL); one-shot queries serve without retaining.
fn try_cold_serve_report(query: &UsageQuery) -> Result<Option<UsageReport>> {
    let Some(ready) = cold_facts_ready(query) else {
        return Ok(None);
    };
    let cache_path = query
        .cache_path
        .as_deref()
        .expect("cold serve needs a cache");
    if query.memo_ttl_ms != 0 {
        populate_snapshots_from_facts(query.source, cache_path, &ready.per_source);
        return Ok(None);
    }
    let Ok(report) = scan_usage_from_facts(query, cache_path, &ready.warnings) else {
        return Ok(None);
    };
    if !ready.generations_current(cache_path) {
        return Ok(None);
    }
    Ok(Some(report))
}

fn scan_single(
    query: &UsageQuery,
    assembled: &UsageAssembly,
    warnings: &[String],
) -> Result<UsageReport> {
    let filter_start = Instant::now();
    let events: Vec<usize> = filtered_events(assembled, query).collect();
    usage_timing(filter_start, || {
        format!("filter ({} of {} events)", events.len(), assembled.len())
    });

    let mut by_source: HashMap<&'static str, UsageSummary> = HashMap::new();
    let mut report = UsageReport {
        authority: "local_log",
        cost_mode: query.cost_mode,
        price_catalog: PRICE_CATALOG_ID,
        warnings: warnings.to_vec(),
        ..UsageReport::default()
    };
    let mut rate_cache = RateCache::default();
    let pricing_start = Instant::now();
    for event in events.iter().map(|&index| assembled.view(index)) {
        accumulate_usage_event(
            &mut report,
            &mut by_source,
            &event,
            query.cost_mode,
            &mut rate_cache,
        );
    }
    usage_timing(pricing_start, || {
        format!("pricing ({} events)", events.len())
    });
    let waste_start = Instant::now();
    for (source, waste) in compute_cache_waste(events.iter().map(|&index| assembled.view(index))) {
        report.cache_waste.absorb(&waste);
        if let Some(row) = by_source.get_mut(&source) {
            row.cache_waste = waste;
        }
    }
    usage_timing(waste_start, || "cache waste".to_string());
    report.by_source = by_source.into_values().collect();
    report.by_source.sort_by(|a, b| a.source.cmp(&b.source));
    if query.include_events {
        report.details = assembled.details(events.into_iter());
    }
    Ok(report)
}

fn scan_merged(query: &UsageQuery, merged: MergedSnapshot) -> Result<UsageReport> {
    let MergedSnapshot {
        parts,
        order,
        warnings,
    } = merged;
    let view = MergedView {
        parts: &parts,
        order: &order,
    };
    let total = view.len();
    let filter_start = Instant::now();
    let matches: Vec<MergedPos> = filtered_merged_positions(view, query).collect();
    usage_timing(filter_start, || {
        format!("filter ({} of {} events)", matches.len(), total)
    });

    let mut by_source: HashMap<&'static str, UsageSummary> = HashMap::new();
    let mut report = UsageReport {
        authority: "local_log",
        cost_mode: query.cost_mode,
        price_catalog: PRICE_CATALOG_ID,
        warnings: warnings.as_ref().clone(),
        ..UsageReport::default()
    };
    let view_at = |pos: &MergedPos| parts[pos.part as usize].view(pos.index as usize);
    let mut rate_cache = RateCache::default();
    let pricing_start = Instant::now();
    for event in matches.iter().map(view_at) {
        accumulate_usage_event(
            &mut report,
            &mut by_source,
            &event,
            query.cost_mode,
            &mut rate_cache,
        );
    }
    usage_timing(pricing_start, || {
        format!("pricing ({} events)", matches.len())
    });
    let waste_start = Instant::now();
    for (source, waste) in compute_cache_waste(matches.iter().map(view_at)) {
        report.cache_waste.absorb(&waste);
        if let Some(row) = by_source.get_mut(&source) {
            row.cache_waste = waste;
        }
    }
    usage_timing(waste_start, || "cache waste".to_string());
    report.by_source = by_source.into_values().collect();
    report.by_source.sort_by(|a, b| a.source.cmp(&b.source));
    if query.include_events {
        report.details = merged_details(&parts, &matches);
    }
    Ok(report)
}

/// Detail rows for merged matches in global order. Matches are grouped per
/// partition (each sublist stays ascending) for batched details — preserving the
/// per-file path sharing — then interleaved back into merged order with moves.
fn merged_details(parts: &[Arc<UsageAssembly>], matches: &[MergedPos]) -> Vec<UsageEvent> {
    use std::collections::VecDeque;
    let mut per_part: Vec<Vec<usize>> = vec![Vec::new(); parts.len()];
    for pos in matches {
        per_part[pos.part as usize].push(pos.index as usize);
    }
    let mut batched: Vec<VecDeque<UsageEvent>> = parts
        .iter()
        .zip(per_part)
        .map(|(assembly, indices)| assembly.details(indices.into_iter()).into())
        .collect();
    let mut details = Vec::with_capacity(matches.len());
    for pos in matches {
        details.push(
            batched[pos.part as usize]
                .pop_front()
                .expect("batched details cover every match"),
        );
    }
    details
}

pub(crate) fn visit_inner(
    query: &UsageQuery,
    mut visit: impl FnMut(UsageActivityPoint),
) -> Result<Vec<String>> {
    match ensure_snapshot(query)? {
        Snapshot::Partition(assembled, warnings) => {
            for index in filtered_events(&assembled, query) {
                visit(assembled.activity_point(index));
            }
            Ok(warnings.as_ref().clone())
        }
        Snapshot::Merged(merged) => {
            let view = MergedView {
                parts: &merged.parts,
                order: &merged.order,
            };
            for pos in filtered_merged_positions(view, query) {
                visit(merged.parts[pos.part as usize].activity_point(pos.index as usize));
            }
            Ok(merged.warnings.as_ref().clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::snapshot::{check_partition_valid, lock_merged, lock_partitions};
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn cold_retaining_activity_keeps_events_outside_the_first_query_range() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let temp = tempfile::tempdir().unwrap();
        let projects = temp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).unwrap();
        let lines: String = [("early", 1000, 10), ("late", 2000, 70)]
            .into_iter()
            .map(|(id, timestamp, input)| {
                serde_json::json!({
                    "type": "assistant", "sessionId": "session", "timestamp": timestamp,
                    "message": { "id": id, "model": "claude-sonnet-4-6",
                        "usage": { "inputTokens": input } }
                })
                .to_string()
                    + "\n"
            })
            .collect();
        std::fs::write(projects.join("session.jsonl"), lines).unwrap();
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(temp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Claude),
            cache_path: Some(temp.path().join("cache.sqlite3")),
            memo_ttl_ms: 60_000,
            ..UsageQuery::default()
        };
        let key = (SourceFilter::Claude, query.cache_path.clone());
        let expected = scan_usage_activity_with_warnings(&query).unwrap();
        assert_eq!(expected.0.len(), 2);
        lock_partitions().remove(&key);
        let narrow = UsageQuery {
            since_ms: Some(expected.0[1].timestamp_ms),
            ..query.clone()
        };
        let cold = scan_usage_activity_with_warnings(&narrow).unwrap();
        assert_eq!(cold.0, expected.0[1..]);
        assert_eq!(cold.1, expected.1);
        assert!(lock_partitions().contains_key(&key));
        assert_eq!(scan_usage_activity_with_warnings(&query).unwrap(), expected);
        // The visitor must run after cold population releases refresh locks.
        lock_partitions().remove(&key);
        visit_usage_activity(&narrow, |_| {
            let _refresh = USAGE_SCAN_LOCK
                .try_lock()
                .expect("callback outside refresh lock");
            let _partitions = lock_partitions();
        })
        .unwrap();
    }

    #[test]
    fn cold_population_rejects_a_stale_generation_and_skips_a_busy_refresh() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let temp = tempfile::tempdir().unwrap();
        let projects = temp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).unwrap();
        let transcript = projects.join("session.jsonl");
        let line = |input| {
            serde_json::json!({
                "type": "assistant", "sessionId": "session", "timestamp": 1000,
                "message": { "id": "message", "usage": { "inputTokens": input } }
            })
            .to_string()
                + "\n"
        };
        std::fs::write(&transcript, line(10)).unwrap();
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(temp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Claude),
            cache_path: Some(temp.path().join("cache.sqlite3")),
            memo_ttl_ms: 60_000,
            ..UsageQuery::default()
        };
        let key = (SourceFilter::Claude, query.cache_path.clone());
        assert_eq!(scan_usage(&query).unwrap().total_tokens, 10);
        lock_partitions().remove(&key);
        let ready = cold_facts_ready(&query).expect("valid cold facts");
        let cache_path = query.cache_path.as_deref().unwrap();
        {
            let _refresh = USAGE_SCAN_LOCK.lock().unwrap();
            populate_snapshots_from_facts(query.source, cache_path, &ready.per_source);
            assert!(!lock_partitions().contains_key(&key));
        }
        std::fs::write(&transcript, line(800)).unwrap();
        assert_eq!(
            scan_usage(&UsageQuery {
                memo_ttl_ms: 0,
                ..query.clone()
            })
            .unwrap()
            .total_tokens,
            800,
        );
        assert!(!ready.generations_current(cache_path));
        populate_snapshots_from_facts(query.source, cache_path, &ready.per_source);
        assert!(!lock_partitions().contains_key(&key));
        assert_eq!(scan_usage(&query).unwrap().total_tokens, 800);
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
        // An uncached query must not evict another caller's still-valid memo.
        assert_eq!(scan_usage(&query).unwrap().total_tokens, 10);
    }

    #[test]
    fn alternating_source_filters_reuse_snapshots() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        std::fs::write(
            projects.join("session.jsonl"),
            r#"{"type":"assistant","sessionId":"s","timestamp":1000,"cwd":"/repo/memex","message":{"id":"m","model":"claude-sonnet-4-6","usage":{"inputTokens":10}}}"#
                .to_string()
                + "\n",
        )
        .expect("write transcript");
        let sessions = tmp.path().join("sessions/2026/07/14");
        std::fs::create_dir_all(&sessions).expect("create sessions");
        std::fs::write(
            sessions.join(
                "rollout-2026-07-14T10-00-00-019f0000-0000-7000-8000-000000000001.jsonl",
            ),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-14T10:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                r#"{"type":"event_msg","timestamp":"2026-07-14T10:01:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
                "\n",
            ),
        )
        .expect("write rollout");
        let _env = EnvVarGuard::set_os(&[
            ("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str())),
            ("CODEX_HOME", Some(tmp.path().as_os_str())),
        ]);
        let cache = tmp.path().join("usage-cache.sqlite3");
        let base = UsageQuery {
            cache_path: Some(cache.clone()),
            memo_ttl_ms: 60_000,
            ..UsageQuery::default()
        };
        let codex = UsageQuery {
            source: Some(SourceFilter::Codex),
            ..base.clone()
        };
        let claude = UsageQuery {
            source: Some(SourceFilter::Claude),
            ..base.clone()
        };

        assert_eq!(scan_usage(&codex).expect("codex").events, 1);
        assert_eq!(scan_usage(&claude).expect("claude").events, 1);
        // Alternating back reuses both snapshots; a single slot would have evicted
        // codex when claude was queried and would rescan here.
        assert_eq!(scan_usage(&codex).expect("codex again").events, 1);
        assert_eq!(scan_usage(&claude).expect("claude again").events, 1);
        let store = lock_partitions();
        assert!(
            store.contains_key(&(SourceFilter::Codex, Some(cache.clone()))),
            "codex snapshot retained"
        );
        assert!(
            store.contains_key(&(SourceFilter::Claude, Some(cache))),
            "claude snapshot retained"
        );
    }

    #[test]
    fn stale_snapshot_revalidates_without_rebuild_when_unchanged() {
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
        let cache = tmp.path().join("usage-cache.sqlite3");
        let query = UsageQuery {
            source: Some(SourceFilter::Claude),
            cache_path: Some(cache.clone()),
            memo_ttl_ms: 1,
            ..UsageQuery::default()
        };
        let key = (SourceFilter::Claude, Some(cache));

        assert_eq!(scan_usage(&query).expect("cold scan").total_tokens, 10);
        let fingerprint = lock_partitions()
            .get(&key)
            .map(|entry| entry.fingerprint.clone())
            .expect("snapshot published");
        assert!(
            check_partition_valid(key.0, key.1.as_deref(), Some(&fingerprint)),
            "unchanged corpus revalidates"
        );
        std::fs::write(&transcript, format!("{}{}", line(10), line(70))).expect("grow transcript");
        assert!(
            !check_partition_valid(key.0, key.1.as_deref(), Some(&fingerprint)),
            "appended transcript invalidates"
        );
        // Let the 1ms TTL lapse so the next query revalidates instead of reusing.
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert_eq!(scan_usage(&query).expect("refresh").total_tokens, 80);
        let fingerprint = lock_partitions()
            .get(&key)
            .map(|entry| entry.fingerprint.clone())
            .expect("snapshot republished");
        assert!(
            check_partition_valid(key.0, key.1.as_deref(), Some(&fingerprint)),
            "refreshed corpus revalidates"
        );
    }

    #[test]
    fn hermes_wal_only_change_invalidates_partition() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join("hermes");
        std::fs::create_dir_all(&root).expect("create hermes root");
        let db_path = root.join("state.db");
        let conn = Connection::open(&db_path).expect("create db");
        conn.execute_batch(
            "CREATE TABLE sessions (id TEXT, model TEXT, started_at INTEGER, input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER, billing_provider TEXT, estimated_cost_usd REAL, cwd TEXT, git_repo_root TEXT, profile_name TEXT); \
             INSERT INTO sessions VALUES ('s1', 'model', 1000, 10, 5, 0, 0, 0, NULL, NULL, '/repo/memex', NULL, NULL);",
        )
        .expect("seed session");
        drop(conn);
        let _env = EnvVarGuard::set_os(&[("HERMES_PROFILE_ROOTS", Some(root.as_os_str()))]);
        let cache = tmp.path().join("usage-cache.sqlite3");
        let query = UsageQuery {
            source: Some(SourceFilter::Hermes),
            cache_path: Some(cache.clone()),
            memo_ttl_ms: 60_000,
            ..UsageQuery::default()
        };
        let key = (SourceFilter::Hermes, Some(cache));

        let first = scan_usage(&query).expect("warm scan");
        assert!(first.events > 0, "seeded session is counted");
        let fingerprint = lock_partitions()
            .get(&key)
            .map(|entry| entry.fingerprint.clone())
            .expect("snapshot published");
        assert!(check_partition_valid(
            key.0,
            key.1.as_deref(),
            Some(&fingerprint)
        ));
        // Commits landing entirely in the WAL leave the main file's metadata
        // untouched: the listing fingerprint cannot see them, so only the
        // recorded WAL dependency keeps the check honest.
        let wal = db_path.with_extension("db-wal");
        std::fs::write(&wal, "wal-1").expect("write wal");
        assert!(
            !check_partition_valid(key.0, key.1.as_deref(), Some(&fingerprint)),
            "WAL-only change invalidates"
        );
        std::fs::remove_file(&wal).expect("remove wal");
        assert!(check_partition_valid(
            key.0,
            key.1.as_deref(),
            Some(&fingerprint)
        ));
    }

    #[test]
    fn cold_serve_matches_normal_report_and_repopulates() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        lock_partitions().clear();
        lock_merged().clear();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        std::fs::write(
            &projects.join("session.jsonl"),
            r#"{"type":"assistant","sessionId":"session","timestamp":1000,"cwd":"/repo/memex","message":{"id":"m","model":"claude-sonnet-4-6","usage":{"inputTokens":10}}}"#
                .to_string()
                + "\n",
        )
        .expect("write transcript");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let cache = tmp.path().join("usage-cache.sqlite3");
        let query = UsageQuery {
            source: Some(SourceFilter::Claude),
            include_events: true,
            cache_path: Some(cache.clone()),
            memo_ttl_ms: 60_000,
            ..UsageQuery::default()
        };
        let key = (SourceFilter::Claude, Some(cache));

        let first = scan_usage(&query).expect("first scan");
        assert!(lock_partitions().contains_key(&key));
        // Simulate a fresh process: drop all retained state, keep the database.
        lock_partitions().clear();
        lock_merged().clear();
        let cold = scan_usage(&query).expect("cold scan");
        assert_eq!(
            serde_json::to_value(&cold).unwrap(),
            serde_json::to_value(&first).unwrap(),
            "cold facts serve matches the normal report"
        );
        assert!(
            lock_partitions().contains_key(&key),
            "cold serve repopulates for retaining queries"
        );
    }

    #[test]
    fn cold_serve_oneshot_retains_nothing() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        lock_partitions().clear();
        lock_merged().clear();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        std::fs::write(
            &projects.join("session.jsonl"),
            r#"{"type":"assistant","sessionId":"session","timestamp":1000,"cwd":"/repo/memex","message":{"id":"m","model":"claude-sonnet-4-6","usage":{"inputTokens":10}}}"#
                .to_string()
                + "\n",
        )
        .expect("write transcript");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let cache = tmp.path().join("usage-cache.sqlite3");
        let query = UsageQuery {
            source: Some(SourceFilter::Claude),
            cache_path: Some(cache.clone()),
            ..UsageQuery::default()
        };
        let key = (SourceFilter::Claude, Some(cache));

        // Warm the database through a retaining query, then forget everything.
        let retaining = UsageQuery {
            memo_ttl_ms: 60_000,
            ..query.clone()
        };
        let expected = scan_usage(&retaining).expect("retaining scan");
        lock_partitions().clear();
        lock_merged().clear();
        let cold = scan_usage(&query).expect("one-shot cold scan");
        assert_eq!(cold.total_tokens, expected.total_tokens);
        assert!(
            !lock_partitions().contains_key(&key),
            "one-shot cold serve retains nothing"
        );
    }

    #[test]
    fn cold_serve_returns_stored_warnings() {
        use super::super::cache::write_fact_sync;
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        lock_partitions().clear();
        lock_merged().clear();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        std::fs::write(
            &projects.join("session.jsonl"),
            r#"{"type":"assistant","sessionId":"session","timestamp":1000,"cwd":"/repo/memex","message":{"id":"m","model":"claude-sonnet-4-6","usage":{"inputTokens":10}}}"#
                .to_string()
                + "\n",
        )
        .expect("write transcript");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let cache = tmp.path().join("usage-cache.sqlite3");
        let query = UsageQuery {
            source: Some(SourceFilter::Claude),
            cache_path: Some(cache.clone()),
            memo_ttl_ms: 60_000,
            ..UsageQuery::default()
        };
        let key = (SourceFilter::Claude, Some(cache.clone()));

        assert_eq!(scan_usage(&query).expect("scan").total_tokens, 10);
        // Stamp stored warnings directly, then forget all retained state.
        let fingerprint = lock_partitions()
            .get(&key)
            .map(|entry| entry.fingerprint.clone())
            .expect("snapshot published");
        let stored = Connection::open(&cache).expect("open cache");
        write_fact_sync(
            &stored,
            SourceFilter::Claude,
            &fingerprint,
            &["nightly lint".to_string()],
        )
        .expect("store warnings");
        drop(stored);
        lock_partitions().clear();
        lock_merged().clear();
        let cold = scan_usage(&query).expect("cold scan");
        assert_eq!(cold.total_tokens, 10);
        assert_eq!(cold.warnings, vec!["nightly lint".to_string()]);
    }
}
