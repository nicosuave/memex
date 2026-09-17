//! Canonical usage facts: queryable rows shadowing the blob cache.
//!
//! Facts carry every field reports need, indexed for time-bounded reads, so
//! refreshes and cold starts can skip blob decoding. Writes are atomic per
//! partition (or per changed-file set) together with the freshness fingerprint.

use super::compact::{FilterFields, UsageAssembly, UsageAssemblyBuilder, UsageEventView};
use super::filter::FilterPlan;
use super::merge::{MergedPos, merge_runs};
use super::pricing::{PRICE_CATALOG_ID, RateCache, accumulate_usage_event, compute_cache_waste};
use super::scan::{SCANNERS, source_ordinal};
use super::usage_timing;
use super::{
    TokenBuckets, UsageActivityPoint, UsageEvent, UsageEventData, UsageQuery, UsageReport,
    UsageSummary,
};
use crate::types::SourceFilter;
use anyhow::Result;
use rusqlite::{Connection, params_from_iter};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
/// Chart projection: sort key plus only what activity points need. Project and
/// session predicates need the full row, so filtered charts use full runs.
struct FactPoint {
    source: &'static str,
    path: String,
    source_order: u64,
    timestamp_ms: u64,
    permission_review: bool,
    total_tokens: u64,
}

fn read_fact_point_runs(
    cache_path: &Path,
    source: Option<SourceFilter>,
    since_ms: Option<u64>,
    until_ms: Option<u64>,
) -> Result<Vec<Vec<FactPoint>>> {
    let connection = Connection::open(cache_path)?;
    connection.busy_timeout(Duration::from_secs(2))?;
    let mut runs = Vec::new();
    for (filter, _) in SCANNERS {
        if source.is_none_or(|selected| selected == filter) {
            let mut query = String::from(
                "SELECT ordinal, timestamp_ms, path, source_order, permission_review,
                        uncached_input, cache_read, cache_write, output
                 FROM usage_facts WHERE ordinal = ?",
            );
            let mut params: Vec<rusqlite::types::Value> =
                vec![(source_ordinal(filter) as i64).into()];
            if let Some(since) = since_ms {
                query.push_str(" AND timestamp_ms >= ?");
                params.push((since as i64).into());
            }
            if let Some(until) = until_ms {
                query.push_str(" AND timestamp_ms < ?");
                params.push((until as i64).into());
            }
            query.push_str(" ORDER BY ordinal, timestamp_ms, path, source_order, row_idx");
            let mut statement = connection.prepare(&query)?;
            let rows = statement
                .query_map(params_from_iter(params), |row| {
                    let ordinal: i64 = row.get(0)?;
                    // Same buckets as TokenBuckets::additive_total (reasoning is
                    // an output subset, not billed twice).
                    let total = (row.get::<_, i64>(5)? as u64)
                        .saturating_add(row.get::<_, i64>(6)? as u64)
                        .saturating_add(row.get::<_, i64>(7)? as u64)
                        .saturating_add(row.get::<_, i64>(8)? as u64);
                    Ok((
                        ordinal,
                        FactPoint {
                            source: "",
                            path: row.get(2)?,
                            source_order: row.get::<_, i64>(3)? as u64,
                            timestamp_ms: row.get::<_, i64>(1)? as u64,
                            permission_review: row.get::<_, i64>(4)? != 0,
                            total_tokens: total,
                        },
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let mut run = Vec::with_capacity(rows.len());
            for (ordinal, mut point) in rows {
                let Some(label) = fact_source_label(ordinal) else {
                    continue;
                };
                point.source = label;
                run.push(point);
            }
            runs.push(run);
        }
    }
    Ok(runs)
}

pub(crate) fn read_fact_points(
    query: &UsageQuery,
    cache_path: &Path,
) -> Result<Vec<UsageActivityPoint>> {
    // Unfiltered charts (no project/session predicates) use the narrow
    // projection: same merged order, far less mapping per row.
    if query.project.is_none() && query.session_keys.is_none() {
        let runs = read_fact_point_runs(cache_path, query.source, query.since_ms, query.until_ms)?;
        let order = merge_runs(
            &runs,
            |point| (point.timestamp_ms, point.path.as_str(), point.source_order),
            "fact points merge",
        );
        let mut plan = FilterPlan::new(query);
        return Ok(order
            .iter()
            .map(|pos| &runs[pos.part as usize][pos.index as usize])
            .filter(|point| {
                plan.matches(FilterFields {
                    source: point.source,
                    permission_review: point.permission_review,
                    project: None,
                    session_id: None,
                })
            })
            .map(|point| UsageActivityPoint {
                source: point.source,
                timestamp_ms: point.timestamp_ms,
                total_tokens: point.total_tokens,
            })
            .collect());
    }
    let runs = read_fact_runs(cache_path, query.source, query.since_ms, query.until_ms)?;
    let order = merge_fact_runs(&runs);
    let mut plan = FilterPlan::new(query);
    Ok(order
        .iter()
        .map(|pos| &runs[pos.part as usize][pos.index as usize])
        .filter(|row| plan.matches(row.filter_fields()))
        .map(|row| UsageActivityPoint {
            source: row.source,
            timestamp_ms: row.timestamp_ms,
            total_tokens: row.tokens.total(),
        })
        .collect())
}

/// One canonical usage fact row: every field reports need, in owned form.
pub(crate) struct FactRow {
    pub(crate) source: &'static str,
    pub(crate) path: String,
    pub(crate) source_order: u64,
    pub(crate) timestamp_ms: u64,
    pub(crate) session_id: Option<String>,
    pub(crate) project: Option<String>,
    pub(crate) provider: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) source_record_id: Option<String>,
    pub(crate) request_id: Option<String>,
    pub(crate) message_id: Option<String>,
    pub(crate) tokens: TokenBuckets,
    pub(crate) source_cost_usd: Option<f64>,
    pub(crate) cost_authoritative: bool,
    pub(crate) dedupe_confidence: &'static str,
    pub(crate) conservative_undercount: bool,
    pub(crate) cache_chain_excluded: bool,
    pub(crate) sidechain: bool,
    pub(crate) permission_review: bool,
}

fn fact_source_label(ordinal: i64) -> Option<&'static str> {
    usize::try_from(ordinal)
        .ok()
        .and_then(|index| SCANNERS.get(index))
        .map(|(filter, _)| filter.as_str())
}

const FACT_COLUMNS: &str =
    "ordinal, path, source_order, timestamp_ms, session_id, project, provider,
        model, source_record_id, request_id, message_id, raw_input, uncached_input,
        cache_read, cache_write, cache_write_1h, output, reasoning, source_cost_usd,
        cost_authoritative, dedupe_confidence, conservative_undercount,
        cache_chain_excluded, sidechain, permission_review";

/// Retained cold reads intern SQLite's borrowed text directly, avoiding an
/// owned fact row and event (including a separately allocated path) per record.
pub(crate) fn read_fact_assembly(
    connection: &Connection,
    filter: SourceFilter,
) -> Result<UsageAssembly> {
    let read_start = Instant::now();
    // Ordered index reads revisit table pages across files. Let SQLite borrow
    // up to 256 MiB from the OS page cache instead of repeatedly copying pages
    // through its small default cache. Unsupported platforms ignore this.
    connection.pragma_update(None, "mmap_size", 256 * 1024 * 1024)?;
    let mut statement = connection.prepare(&format!(
        "SELECT {FACT_COLUMNS} FROM usage_facts WHERE ordinal = ?
         ORDER BY ordinal, timestamp_ms, path, source_order, row_idx"
    ))?;
    let mut rows = statement.query([source_ordinal(filter) as i64])?;
    let mut builder = UsageAssemblyBuilder::default();
    while let Some(row) = rows.next()? {
        let text = |index| row.get_ref(index)?.as_str().map_err(rusqlite::Error::from);
        let optional_text = |index| -> rusqlite::Result<Option<&str>> {
            match row.get_ref(index)? {
                rusqlite::types::ValueRef::Null => Ok(None),
                value => Ok(Some(value.as_str()?)),
            }
        };
        let as_u64 = |index| row.get::<_, i64>(index).map(|value| value as u64);
        builder.push(UsageEventData {
            source: filter.as_str(),
            source_path: text(1)?,
            source_order: as_u64(2)?,
            timestamp_ms: as_u64(3)?,
            session_id: optional_text(4)?,
            project: optional_text(5)?,
            provider: optional_text(6)?,
            model: optional_text(7)?,
            source_record_id: optional_text(8)?,
            request_id: optional_text(9)?,
            message_id: optional_text(10)?,
            tokens: TokenBuckets {
                raw_input: as_u64(11)?,
                uncached_input: as_u64(12)?,
                cache_read: as_u64(13)?,
                cache_write: as_u64(14)?,
                cache_write_1h: as_u64(15)?,
                output: as_u64(16)?,
                reasoning: as_u64(17)?,
            },
            source_cost_usd: row.get(18)?,
            cost_authoritative: row.get::<_, i64>(19)? != 0,
            dedupe_confidence: match text(20)? {
                "exact" => "exact",
                "strong" => "strong",
                _ => "heuristic",
            },
            conservative_undercount: row.get::<_, i64>(21)? != 0,
            cache_chain_excluded: row.get::<_, i64>(22)? != 0,
            sidechain: row.get::<_, i64>(23)? != 0,
            permission_review: row.get::<_, i64>(24)? != 0,
        });
    }
    let assembly = builder.finish();
    usage_timing(read_start, || {
        format!("facts compact read ({} rows)", assembly.len())
    });
    Ok(assembly)
}

fn map_fact_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(i64, FactRow)> {
    let ordinal: i64 = row.get(0)?;
    let as_u64 = |index: usize| row.get::<_, i64>(index).map(|value| value as u64);
    let dedupe: String = row.get(20)?;
    Ok((
        ordinal,
        FactRow {
            // Resolved by the caller; unknown ordinals are quarantined.
            source: "",
            path: row.get(1)?,
            source_order: as_u64(2)?,
            timestamp_ms: as_u64(3)?,
            session_id: row.get(4)?,
            project: row.get(5)?,
            provider: row.get(6)?,
            model: row.get(7)?,
            source_record_id: row.get(8)?,
            request_id: row.get(9)?,
            message_id: row.get(10)?,
            tokens: TokenBuckets {
                raw_input: as_u64(11)?,
                uncached_input: as_u64(12)?,
                cache_read: as_u64(13)?,
                cache_write: as_u64(14)?,
                cache_write_1h: as_u64(15)?,
                output: as_u64(16)?,
                reasoning: as_u64(17)?,
            },
            source_cost_usd: row.get(18)?,
            cost_authoritative: row.get::<_, i64>(19)? != 0,
            dedupe_confidence: match dedupe.as_str() {
                "exact" => "exact",
                "strong" => "strong",
                _ => "heuristic",
            },
            conservative_undercount: row.get::<_, i64>(21)? != 0,
            cache_chain_excluded: row.get::<_, i64>(22)? != 0,
            sidechain: row.get::<_, i64>(23)? != 0,
            permission_review: row.get::<_, i64>(24)? != 0,
        },
    ))
}

/// One source's facts in report order, streamed straight from the composite
/// index: no SQLite sort step at any size. Runs are positional; for combined
/// queries every source is read in scanner order so positions are ordinals.
pub(crate) fn read_fact_runs(
    cache_path: &Path,
    source: Option<SourceFilter>,
    since_ms: Option<u64>,
    until_ms: Option<u64>,
) -> Result<Vec<Vec<FactRow>>> {
    let read_start = Instant::now();
    let connection = Connection::open(cache_path)?;
    connection.busy_timeout(Duration::from_secs(2))?;
    let mut runs = Vec::new();
    for (filter, _) in SCANNERS {
        if source.is_none_or(|selected| selected == filter) {
            runs.push(read_ordinal_run(
                &connection,
                source_ordinal(filter) as i64,
                since_ms,
                until_ms,
            )?);
        }
    }
    usage_timing(read_start, || {
        format!(
            "facts read ({} rows)",
            runs.iter().map(Vec::len).sum::<usize>()
        )
    });
    Ok(runs)
}

/// One source's facts in report order from an open connection. Shared by report
/// reads and refreshes rebuilding a partition without decoding blobs.
pub(crate) fn read_ordinal_run(
    connection: &Connection,
    ordinal: i64,
    since_ms: Option<u64>,
    until_ms: Option<u64>,
) -> Result<Vec<FactRow>> {
    let mut query = format!("SELECT {FACT_COLUMNS} FROM usage_facts WHERE ordinal = ?");
    let mut params: Vec<rusqlite::types::Value> = vec![ordinal.into()];
    if let Some(since) = since_ms {
        query.push_str(" AND timestamp_ms >= ?");
        params.push((since as i64).into());
    }
    if let Some(until) = until_ms {
        query.push_str(" AND timestamp_ms < ?");
        params.push((until as i64).into());
    }
    query.push_str(" ORDER BY ordinal, timestamp_ms, path, source_order, row_idx");
    let mut statement = connection.prepare(&query)?;
    let rows = statement
        .query_map(params_from_iter(params), map_fact_row)?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut run = Vec::with_capacity(rows.len());
    for (row_ordinal, mut fact) in rows {
        // Unknown ordinals (foreign or corrupt rows) are quarantined, like
        // corrupt blobs: skipping them can only undercount on damage.
        let Some(label) = fact_source_label(row_ordinal) else {
            continue;
        };
        fact.source = label;
        run.push(fact);
    }
    Ok(run)
}

/// K-way merge of per-source ordered full fact runs into global order.
fn merge_fact_runs(runs: &[Vec<FactRow>]) -> Vec<MergedPos> {
    merge_runs(
        runs,
        |row| (row.timestamp_ms, row.path.as_str(), row.source_order),
        "facts merge",
    )
}

pub(crate) fn scan_usage_from_facts(
    query: &UsageQuery,
    cache_path: &Path,
    warnings: &[String],
) -> Result<UsageReport> {
    let runs = read_fact_runs(cache_path, query.source, query.since_ms, query.until_ms)?;
    let order = merge_fact_runs(&runs);
    let row_at = |pos: &MergedPos| &runs[pos.part as usize][pos.index as usize];
    let mut plan = FilterPlan::new(query);
    let mut report = UsageReport {
        authority: "local_log",
        cost_mode: query.cost_mode,
        price_catalog: PRICE_CATALOG_ID,
        warnings: warnings.to_vec(),
        ..UsageReport::default()
    };
    let mut by_source: HashMap<&'static str, UsageSummary> = HashMap::new();
    let mut rate_cache = RateCache::default();
    // Time bounds already applied in SQL; remaining predicates match row-for-row
    // with the assembly path, in the same order.
    let matched: Vec<MergedPos> = order
        .iter()
        .copied()
        .filter(|pos| plan.matches(row_at(pos).filter_fields()))
        .collect();
    for event in matched.iter().map(|pos| row_at(pos).view()) {
        accumulate_usage_event(
            &mut report,
            &mut by_source,
            &event,
            query.cost_mode,
            &mut rate_cache,
        );
    }
    for (source, waste) in compute_cache_waste(matched.iter().map(|pos| row_at(pos).view())) {
        report.cache_waste.absorb(&waste);
        if let Some(row) = by_source.get_mut(&source) {
            row.cache_waste = waste;
        }
    }
    report.by_source = by_source.into_values().collect();
    report.by_source.sort_by(|a, b| a.source.cmp(&b.source));
    if query.include_events {
        report.details = fact_details(runs, &matched);
    }
    Ok(report)
}

/// Detail rows for merged fact matches in global order. Matches are grouped per
/// run (each sublist stays ascending) and converted in batches, then interleaved
/// back into merged order with moves instead of clones.
fn fact_details(runs: Vec<Vec<FactRow>>, matches: &[MergedPos]) -> Vec<UsageEvent> {
    use std::collections::VecDeque;
    let mut per_run: Vec<Vec<usize>> = vec![Vec::new(); runs.len()];
    for pos in matches {
        per_run[pos.part as usize].push(pos.index as usize);
    }
    let mut batched: Vec<VecDeque<UsageEvent>> = runs
        .into_iter()
        .zip(per_run)
        .map(|(run, indices)| {
            run.into_iter()
                .enumerate()
                .filter(|(index, _)| indices.binary_search(index).is_ok())
                .map(|(_, row)| row.into_event())
                .collect()
        })
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

impl FactRow {
    fn filter_fields(&self) -> FilterFields<'_> {
        FilterFields {
            source: self.source,
            permission_review: self.permission_review,
            project: self.project.as_deref(),
            session_id: self.session_id.as_deref(),
        }
    }

    fn view(&self) -> UsageEventView<'_> {
        UsageEventData {
            source: self.source,
            source_path: self.path.as_str(),
            source_record_id: self.source_record_id.as_deref(),
            session_id: self.session_id.as_deref(),
            request_id: self.request_id.as_deref(),
            message_id: self.message_id.as_deref(),
            timestamp_ms: self.timestamp_ms,
            project: self.project.as_deref(),
            provider: self.provider.as_deref(),
            model: self.model.as_deref(),
            tokens: self.tokens.clone(),
            source_cost_usd: self.source_cost_usd,
            cost_authoritative: self.cost_authoritative,
            dedupe_confidence: self.dedupe_confidence,
            conservative_undercount: self.conservative_undercount,
            cache_chain_excluded: self.cache_chain_excluded,
            sidechain: self.sidechain,
            permission_review: self.permission_review,
            source_order: self.source_order,
        }
    }

    pub(crate) fn into_event(self) -> UsageEvent {
        UsageEvent {
            source: self.source,
            source_path: Arc::from(self.path.as_str()),
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
            cost_authoritative: self.cost_authoritative,
            dedupe_confidence: self.dedupe_confidence,
            conservative_undercount: self.conservative_undercount,
            cache_chain_excluded: self.cache_chain_excluded,
            sidechain: self.sidechain,
            permission_review: self.permission_review,
            source_order: self.source_order,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::super::{CostMode, SourceFilter, UsageQuery, query::visit_inner, scan_usage};
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn borrowed_fact_assembly_preserves_ties_text_and_accounting() {
        let temp = tempfile::tempdir().unwrap();
        let mut cache =
            super::super::cache::UsageCache::open(&temp.path().join("cache.sqlite3")).unwrap();
        let mut events = Vec::new();
        for index in 0..12 {
            let mut event = super::super::cache_event("会話", index / 6, "モデル", index, 2, 3);
            event.source_path = Arc::from(if index % 2 == 0 { "a" } else { "会話.jsonl" });
            event.source_order = 0;
            event.source_record_id = Some(format!("record-{index}"));
            event.request_id = (index % 2 == 0).then(|| "要求".into());
            event.message_id = Some(String::new());
            event.project = Some("/projects/é".into());
            event.provider = None;
            if index == 0 {
                event.session_id = None;
                event.project = None;
                event.model = None;
                event.source_record_id = None;
                event.message_id = None;
            }
            event.source_cost_usd = Some(0.125);
            event.cost_authoritative = index % 2 == 0;
            event.cache_chain_excluded = index % 3 == 0;
            event.sidechain = index % 4 == 0;
            event.permission_review = index % 5 == 0;
            event.conservative_undercount = index % 2 != 0;
            event.dedupe_confidence = ["exact", "strong", "heuristic"][index as usize % 3];
            events.push(event);
        }
        super::super::snapshot::sort_usage_events(&mut events);
        cache
            .replace_partition_facts(SourceFilter::Claude, &events, &[], &[])
            .unwrap();
        let assembly = read_fact_assembly(&cache.connection, SourceFilter::Claude).unwrap();
        let expected = UsageAssembly::new(events, None);
        assert_eq!(assembly.len(), expected.len());
        for index in 0..expected.len() {
            let actual = assembly.view(index);
            let expected = expected.view(index);
            assert_eq!(
                serde_json::to_value(&actual).unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
            assert_eq!(actual.cost_authoritative, expected.cost_authoritative);
            assert_eq!(actual.cache_chain_excluded, expected.cache_chain_excluded);
            assert_eq!(actual.sidechain, expected.sidechain);
            assert_eq!(actual.permission_review, expected.permission_review);
            assert_eq!(actual.source_order, expected.source_order);
        }
        assert_eq!(
            read_fact_assembly(&cache.connection, SourceFilter::Codex)
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn facts_report_matches_assembly_report() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        let line = |id: &str, timestamp_ms: u64, input: u64| {
            format!(
                r#"{{"type":"assistant","sessionId":"session","timestamp":{timestamp_ms},"cwd":"/repo/memex","message":{{"id":"{id}","model":"claude-sonnet-4-6","usage":{{"inputTokens":{input}}}}}}}"#
            ) + "\n"
        };
        std::fs::write(&projects.join("session.jsonl"), line("m-10", 1000, 10))
            .expect("write transcript");
        std::fs::write(&projects.join("later.jsonl"), line("m-70", 3000, 70))
            .expect("write later transcript");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let cache = tmp.path().join("usage-cache.sqlite3");
        let base = UsageQuery {
            source: Some(SourceFilter::Claude),
            include_events: true,
            cache_path: Some(cache.clone()),
            memo_ttl_ms: 60_000,
            ..UsageQuery::default()
        };

        // Populate facts through a normal scan first.
        let assembly_report = scan_usage(&base).expect("assembly report");
        assert_eq!(assembly_report.events, 2);
        let warnings = assembly_report.warnings.clone();
        let facts_report = scan_usage_from_facts(&base, &cache, &warnings).expect("facts report");
        assert_eq!(
            serde_json::to_value(&facts_report).unwrap(),
            serde_json::to_value(&assembly_report).unwrap(),
            "facts report is byte-identical to the assembly report"
        );
        // Same across cost modes, time bounds, and session filters.
        for query in [
            UsageQuery {
                cost_mode: CostMode::Source,
                ..base.clone()
            },
            UsageQuery {
                cost_mode: CostMode::Reprice,
                ..base.clone()
            },
            UsageQuery {
                since_ms: Some(2_000_000),
                ..base.clone()
            },
            UsageQuery {
                session_keys: Some(
                    [("claude".to_string(), "session".to_string())]
                        .into_iter()
                        .collect(),
                ),
                ..base.clone()
            },
        ] {
            let assembly_report = scan_usage(&query).expect("assembly report");
            let facts_report = scan_usage_from_facts(&query, &cache, &assembly_report.warnings)
                .expect("facts report");
            assert_eq!(
                serde_json::to_value(&facts_report).unwrap(),
                serde_json::to_value(&assembly_report).unwrap(),
                "facts report matches for {query:?}"
            );
        }
        // Activity points match exactly too.
        let mut assembly_points = Vec::new();
        visit_inner(&base, &mut |point| assembly_points.push(point)).expect("visit");
        let facts_points = read_fact_points(&base, &cache).expect("fact points");
        assert_eq!(facts_points, assembly_points);
    }

    #[test]
    fn facts_refresh_matches_legacy_rebuild_after_append() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        let line = |id: &str, timestamp_ms: u64, input: u64| {
            format!(
                r#"{{"type":"assistant","sessionId":"session","timestamp":{timestamp_ms},"cwd":"/repo/memex","message":{{"id":"{id}","model":"claude-sonnet-4-6","usage":{{"inputTokens":{input}}}}}}}"#
            ) + "\n"
        };
        std::fs::write(&projects.join("a.jsonl"), line("m-10", 1000, 10))
            .expect("write transcript");
        std::fs::write(&projects.join("b.jsonl"), line("m-20", 2000, 20))
            .expect("write transcript");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Claude),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            memo_ttl_ms: 1,
            ..UsageQuery::default()
        };

        // Legacy path populates blobs and facts on first build (no sync row yet).
        let cold = scan_usage(&query).expect("cold scan");
        assert_eq!(cold.total_tokens, 30);
        std::thread::sleep(std::time::Duration::from_millis(5));
        // Second scan finds a sync row and valid blobs: still legacy (facts path
        // needs no work), then an append forces a facts-backed refresh.
        std::fs::write(
            &projects.join("a.jsonl"),
            format!("{}{}", line("m-10", 1000, 10), line("m-30", 3000, 30)),
        )
        .expect("grow transcript");
        std::thread::sleep(std::time::Duration::from_millis(5));
        let refreshed = scan_usage(&query).expect("facts refresh");
        assert_eq!(refreshed.total_tokens, 60);
        // A from-scratch legacy rebuild over the same corpus must agree exactly.
        let legacy = scan_usage(&UsageQuery {
            cache_path: Some(tmp.path().join("fresh-cache.sqlite3")),
            ..query.clone()
        })
        .expect("legacy rebuild");
        assert_eq!(
            serde_json::to_value(&refreshed).unwrap(),
            serde_json::to_value(&legacy).unwrap(),
            "facts-backed refresh matches a legacy rebuild"
        );
    }

    #[test]
    fn oneshot_scan_keeps_facts_consistent_for_later_refresh() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        let line = |id: &str, input: u64| {
            format!(
                r#"{{"type":"assistant","sessionId":"session","requestId":"request","timestamp":1000,"cwd":"/repo/memex","message":{{"id":"{id}","model":"claude-sonnet-4-6","usage":{{"inputTokens":{input}}}}}}}"#
            ) + "\n"
        };
        std::fs::write(&projects.join("a.jsonl"), line("m-10", 10)).expect("write transcript");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let retaining = UsageQuery {
            source: Some(SourceFilter::Claude),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            memo_ttl_ms: 1,
            ..UsageQuery::default()
        };
        assert_eq!(scan_usage(&retaining).expect("cold scan").total_tokens, 10);
        std::fs::write(
            &projects.join("a.jsonl"),
            format!("{}{}", line("m-10", 10), line("m-30", 30)),
        )
        .expect("grow transcript");
        std::thread::sleep(std::time::Duration::from_millis(5));
        // A one-shot serves fresh and maintains the facts it touched: blobs
        // and facts must agree afterwards, or the next facts-backed refresh
        // would serve the pre-append rows as current.
        let oneshot = UsageQuery {
            memo_ttl_ms: 0,
            ..retaining.clone()
        };
        assert_eq!(
            scan_usage(&oneshot).expect("one-shot scan").total_tokens,
            40
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
        let refreshed = scan_usage(&retaining).expect("later refresh");
        assert_eq!(refreshed.total_tokens, 40);
        let legacy = scan_usage(&UsageQuery {
            cache_path: Some(tmp.path().join("fresh-cache.sqlite3")),
            ..retaining.clone()
        })
        .expect("legacy rebuild");
        assert_eq!(
            serde_json::to_value(&refreshed).unwrap(),
            serde_json::to_value(&legacy).unwrap(),
            "post-one-shot refresh matches a legacy rebuild"
        );
    }

    #[test]
    fn facts_advanced_elsewhere_invalidate_retained_snapshots() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        let line = |id: &str, input: u64| {
            format!(
                r#"{{"type":"assistant","sessionId":"session","requestId":"request","timestamp":1000,"cwd":"/repo/memex","message":{{"id":"{id}","model":"claude-sonnet-4-6","usage":{{"inputTokens":{input}}}}}}}"#
            ) + "\n"
        };
        std::fs::write(&projects.join("a.jsonl"), line("m-10", 10)).expect("write transcript");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let retaining = UsageQuery {
            source: Some(SourceFilter::Claude),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            memo_ttl_ms: 50,
            ..UsageQuery::default()
        };
        assert_eq!(scan_usage(&retaining).expect("cold scan").total_tokens, 10);
        std::fs::write(
            &projects.join("a.jsonl"),
            format!("{}{}", line("m-10", 10), line("m-30", 30)),
        )
        .expect("grow transcript");
        std::thread::sleep(std::time::Duration::from_millis(5));
        // An external writer (here a one-shot in the same cache database)
        // advances disk and facts past the retained snapshot.
        let oneshot = UsageQuery {
            memo_ttl_ms: 0,
            ..retaining.clone()
        };
        assert_eq!(
            scan_usage(&oneshot).expect("one-shot scan").total_tokens,
            40
        );
        // Past the TTL the old snapshot must not serve: disk-vs-facts
        // agreement alone would reuse it even though its generation is stale.
        std::thread::sleep(std::time::Duration::from_millis(60));
        let refreshed = scan_usage(&retaining).expect("expired refresh");
        assert_eq!(refreshed.total_tokens, 40);
    }

    #[test]
    fn deleted_reconcile_winner_resurrects_the_loser() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        // Same (message, request) in two files: the 70-token copy wins the
        // reconcile, so facts store only the winner.
        let line = |input: u64| {
            format!(
                r#"{{"type":"assistant","sessionId":"session","requestId":"request","timestamp":1000,"cwd":"/repo/memex","message":{{"id":"shared","model":"claude-sonnet-4-6","usage":{{"inputTokens":{input}}}}}}}"#
            ) + "\n"
        };
        std::fs::write(&projects.join("a.jsonl"), line(10)).expect("write loser");
        std::fs::write(&projects.join("b.jsonl"), line(70)).expect("write winner");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let retaining = UsageQuery {
            source: Some(SourceFilter::Claude),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            memo_ttl_ms: 1,
            ..UsageQuery::default()
        };
        assert_eq!(scan_usage(&retaining).expect("cold scan").total_tokens, 70);
        // Deleting the winner must resurrect the loser (which the facts never
        // stored), so the refresh has to fall back to the legacy full rebuild.
        std::fs::remove_file(&projects.join("b.jsonl")).expect("delete winner");
        std::thread::sleep(std::time::Duration::from_millis(5));
        let refreshed = scan_usage(&retaining).expect("refresh after delete");
        assert_eq!(refreshed.total_tokens, 10);
        let legacy = scan_usage(&UsageQuery {
            cache_path: Some(tmp.path().join("fresh-cache.sqlite3")),
            ..retaining.clone()
        })
        .expect("legacy rebuild");
        assert_eq!(
            serde_json::to_value(&refreshed).unwrap(),
            serde_json::to_value(&legacy).unwrap(),
            "post-delete refresh matches a legacy rebuild"
        );
    }

    #[test]
    fn new_file_stealing_the_reconcile_win_rebuilds() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        let line = |input: u64| {
            format!(
                r#"{{"type":"assistant","sessionId":"session","requestId":"request","timestamp":1000,"cwd":"/repo/memex","message":{{"id":"shared","model":"claude-sonnet-4-6","usage":{{"inputTokens":{input}}}}}}}"#
            ) + "\n"
        };
        std::fs::write(&projects.join("a.jsonl"), line(10)).expect("write transcript");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let retaining = UsageQuery {
            source: Some(SourceFilter::Claude),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            memo_ttl_ms: 1,
            ..UsageQuery::default()
        };
        assert_eq!(scan_usage(&retaining).expect("cold scan").total_tokens, 10);
        // A new file claims the same (message, request) with more tokens: the
        // stored hit must lose, so per-file upsert cannot apply and the
        // refresh falls back to the legacy full rebuild (no double count).
        std::fs::write(&projects.join("b.jsonl"), line(70)).expect("write challenger");
        std::thread::sleep(std::time::Duration::from_millis(5));
        let refreshed = scan_usage(&retaining).expect("refresh after steal");
        assert_eq!(refreshed.total_tokens, 70);
        let legacy = scan_usage(&UsageQuery {
            cache_path: Some(tmp.path().join("fresh-cache.sqlite3")),
            ..retaining.clone()
        })
        .expect("legacy rebuild");
        assert_eq!(
            serde_json::to_value(&refreshed).unwrap(),
            serde_json::to_value(&legacy).unwrap(),
            "post-steal refresh matches a legacy rebuild"
        );
    }

    #[test]
    fn tied_reconcile_win_does_not_flip_on_refresh() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        // Same (message, request, tokens) in two files: the reconcile tie
        // breaks toward discovery order. Warm with only the later path, then
        // add a tied copy on a discovery-earlier path: the legacy rebuild
        // flips the win to the newcomer.
        let line = |id: &str, timestamp_ms: u64, input: u64| {
            format!(
                r#"{{"type":"assistant","sessionId":"session","requestId":"request","timestamp":{timestamp_ms},"cwd":"/repo/memex","message":{{"id":"{id}","model":"claude-sonnet-4-6","usage":{{"inputTokens":{input}}}}}}}"#
            ) + "\n"
        };
        std::fs::write(&projects.join("z.jsonl"), line("shared", 2, 10)).expect("write transcript");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let retaining = UsageQuery {
            source: Some(SourceFilter::Claude),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            memo_ttl_ms: 1,
            ..UsageQuery::default()
        };
        let cold = scan_usage(&retaining).expect("cold scan");
        assert_eq!(cold.events, 1);
        assert_eq!(cold.details[0].timestamp_ms, 2000);
        // The newcomer shares the reconcile key, so per-file upsert cannot
        // apply: re-running the reconcile over stored hits first would keep
        // the old winner instead of flipping to the discovery-earlier copy.
        std::fs::write(&projects.join("a.jsonl"), line("shared", 1, 10)).expect("write challenger");
        std::thread::sleep(std::time::Duration::from_millis(5));
        let refreshed = scan_usage(&retaining).expect("refresh");
        assert_eq!(refreshed.events, 1);
        let legacy = scan_usage(&UsageQuery {
            cache_path: Some(tmp.path().join("fresh-cache.sqlite3")),
            ..retaining.clone()
        })
        .expect("legacy rebuild");
        assert_eq!(legacy.details[0].timestamp_ms, 1000);
        assert_eq!(
            serde_json::to_value(&refreshed).unwrap(),
            serde_json::to_value(&legacy).unwrap(),
            "refresh flips to the discovery-earlier winner like a legacy rebuild"
        );
    }

    #[test]
    fn missing_sync_row_migrates_through_legacy_rebuild() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
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
            memo_ttl_ms: 1,
            ..UsageQuery::default()
        };
        assert_eq!(scan_usage(&query).expect("cold scan").total_tokens, 10);
        // Simulate a pre-facts database: blob rows exist, sync row does not.
        Connection::open(&cache)
            .expect("open cache")
            .execute("DELETE FROM usage_fact_sync", [])
            .expect("drop sync row");
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert_eq!(scan_usage(&query).expect("migrating scan").total_tokens, 10);
        // And the sync row is back for the next check.
        let sync_rows: u64 = Connection::open(&cache)
            .expect("reopen cache")
            .query_row(
                "SELECT count(*) FROM usage_fact_sync WHERE source = 'claude'",
                [],
                |row| row.get(0),
            )
            .expect("count sync rows");
        assert_eq!(sync_rows, 1);
    }
}
