//! K-way merge of timestamp-sorted partitions into global order.
//!
//! Ties across partitions break by scanner ordinal then partition index,
//! reproducing the combined assembly stable sort exactly.

use super::UsageQuery;
use super::compact::UsageAssembly;
use super::filter::FilterPlan;
use crate::usage::usage_timing;
use std::sync::Arc;
use std::time::Instant;

/// Position in a merged multi-partition order: which partition and which index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MergedPos {
    pub(crate) part: u32,
    pub(crate) index: u32,
}

/// K-way merge of timestamp-sorted partitions into global order. Ties across
/// partitions break by scanner ordinal then partition index, reproducing the
/// combined assembly's stable sort exactly (partitions are laid out in scanner
/// order there, with `(timestamp, path, order)` unique per event).
pub(crate) fn build_merged_order(parts: &[Arc<UsageAssembly>]) -> Vec<MergedPos> {
    let merge_start = Instant::now();
    // Partitions must arrive sorted (snapshot refreshes sort them); the merge only
    // reproduces the combined stable sort on that precondition.
    debug_assert!(parts.iter().all(|assembly| {
        (1..assembly.len()).all(|index| assembly.sort_key(index - 1) <= assembly.sort_key(index))
    }));
    let total: usize = parts.iter().map(|assembly| assembly.len()).sum();
    let mut order = Vec::with_capacity(total);
    // Current head per non-empty partition; thirteen partitions make a linear
    // minimum scan competitive with a heap, and trivially correct.
    let mut heads: Vec<(usize, usize)> = parts
        .iter()
        .enumerate()
        .filter(|(_, assembly)| assembly.len() > 0)
        .map(|(part, _)| (part, 0))
        .collect();
    while !heads.is_empty() {
        let mut best = 0;
        for candidate in 1..heads.len() {
            let (part, index) = heads[candidate];
            let (best_part, best_index) = heads[best];
            let key = parts[part].sort_key(index);
            let best_key = parts[best_part].sort_key(best_index);
            if (key, part, index) < (best_key, best_part, best_index) {
                best = candidate;
            }
        }
        let (part, index) = heads[best];
        order.push(MergedPos {
            part: part as u32,
            index: index as u32,
        });
        if index + 1 < parts[part].len() {
            heads[best] = (part, index + 1);
        } else {
            heads.swap_remove(best);
        }
    }
    usage_timing(merge_start, || format!("merge ({} events)", order.len()));
    order
}

/// Borrowed view over merged partitions plus their precomputed global order.
pub(crate) struct MergedView<'a> {
    pub(crate) parts: &'a [Arc<UsageAssembly>],
    pub(crate) order: &'a [MergedPos],
}

impl MergedView<'_> {
    pub(crate) fn len(&self) -> usize {
        self.order.len()
    }

    pub(crate) fn lower_bound(&self, since: u64) -> usize {
        self.order.partition_point(|pos| {
            self.parts[pos.part as usize].timestamp_ms(pos.index as usize) < since
        })
    }

    pub(crate) fn upper_bound(&self, until: u64, start: usize) -> usize {
        start
            + self.order[start..].partition_point(|pos| {
                self.parts[pos.part as usize].timestamp_ms(pos.index as usize) < until
            })
    }
}

/// Merged positions matching a query, in global order. Timestamp bounds use binary
/// search over the merged order so narrow queries avoid walking all history.
pub(crate) fn filtered_merged_positions<'a>(
    view: MergedView<'a>,
    query: &'a UsageQuery,
) -> impl Iterator<Item = MergedPos> + 'a {
    let start = query.since_ms.map_or(0, |since| view.lower_bound(since));
    let end = query
        .until_ms
        .map_or(view.len(), |until| view.upper_bound(until, start));
    let start = start.min(view.len());
    let end = end.clamp(start, view.len());
    let mut plan = FilterPlan::new(query);
    let parts = view.parts;
    view.order[start..end]
        .iter()
        .copied()
        .filter(move |pos| plan.matches(parts[pos.part as usize].filter_fields(pos.index as usize)))
}

/// K-way merge of per-source ordered runs into global order. Same key and
/// tiebreaks as the assembly merge (runs arrive in scanner order, so positional
/// tiebreaks are scanner ordinals), so float sums and chains match exactly.
pub(crate) fn merge_runs<T>(
    runs: &[Vec<T>],
    key: impl for<'row> Fn(&'row T) -> (u64, &'row str, u64),
    label: &'static str,
) -> Vec<MergedPos> {
    let merge_start = Instant::now();
    let total: usize = runs.iter().map(Vec::len).sum();
    let mut order = Vec::with_capacity(total);
    let mut heads: Vec<(usize, usize)> = runs
        .iter()
        .enumerate()
        .filter(|(_, run)| !run.is_empty())
        .map(|(part, _)| (part, 0))
        .collect();
    while !heads.is_empty() {
        let mut best = 0;
        for candidate in 1..heads.len() {
            let (part, index) = heads[candidate];
            let (best_part, best_index) = heads[best];
            let row_key = key(&runs[part][index]);
            let best_key = key(&runs[best_part][best_index]);
            if (row_key, part, index) < (best_key, best_part, best_index) {
                best = candidate;
            }
        }
        let (part, index) = heads[best];
        order.push(MergedPos {
            part: part as u32,
            index: index as u32,
        });
        if index + 1 < runs[part].len() {
            heads[best] = (part, index + 1);
        } else {
            heads.swap_remove(best);
        }
    }
    usage_timing(merge_start, || format!("{label} ({} rows)", order.len()));
    order
}

#[cfg(test)]
mod tests {
    use super::super::cache_event;
    use super::super::compact::UsageAssembly;
    use super::super::snapshot::sort_usage_events;
    use super::super::{SourceFilter, UsageQuery, scan_usage, scan_usage_activity};
    use super::*;

    #[test]
    fn merged_order_matches_combined_sort() {
        // Interleaved timestamps with cross-partition ties on (timestamp, path,
        // order): the ordinal tiebreak must reproduce the combined stable sort.
        let event = |timestamp_ms: u64, source_order: u64| {
            let mut event = cache_event("session", timestamp_ms, "model", 10, 0, 0);
            event.source_order = source_order;
            event
        };
        // Partitions arrive sorted, exactly as snapshot refreshes leave them.
        let mut a = vec![event(5, 2), event(5, 0), event(3, 1)];
        let mut b = vec![event(5, 0), event(1, 3)];
        sort_usage_events(&mut a);
        sort_usage_events(&mut b);
        let parts = vec![
            Arc::new(UsageAssembly::new(a.clone(), None)),
            Arc::new(UsageAssembly::new(b.clone(), None)),
        ];
        let order = build_merged_order(&parts);
        let mut combined = [a, b].concat();
        sort_usage_events(&mut combined);
        assert_eq!(order.len(), combined.len());
        for (position, pos) in order.iter().enumerate() {
            let view = parts[pos.part as usize].view(pos.index as usize);
            let expected = &combined[position];
            assert_eq!(
                (
                    view.timestamp_ms,
                    view.source_path,
                    view.source_order,
                    view.tokens.additive_total()
                ),
                (
                    expected.timestamp_ms,
                    expected.source_path.as_ref(),
                    expected.source_order,
                    expected.tokens.additive_total()
                ),
                "merged position {position} (part {}, index {})",
                pos.part,
                pos.index
            );
        }
        // Empty partitions merge to the other partition's order, unchanged.
        let empty = vec![
            Arc::new(UsageAssembly::new(Vec::new(), None)),
            parts[1].clone(),
        ];
        let order = build_merged_order(&empty);
        assert!(order.iter().all(|pos| pos.part == 1));
        assert_eq!(order.len(), parts[1].len());
    }

    #[test]
    fn combined_query_merges_partitions_in_global_order() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let empty = tmp.path().join("empty");
        std::fs::create_dir_all(&empty).expect("create empty dir");
        let projects = tmp.path().join("claude/projects/memex");
        std::fs::create_dir_all(&projects).expect("create projects");
        let line = |id: &str, timestamp_ms: u64, input: u64| {
            format!(
                r#"{{"type":"assistant","sessionId":"session","timestamp":{timestamp_ms},"cwd":"/repo/memex","message":{{"id":"{id}","model":"claude-sonnet-4-6","usage":{{"inputTokens":{input}}}}}}}"#
            ) + "\n"
        };
        // Timestamps interleave with the codex event below: 1M, 3M vs 2M.
        std::fs::write(&projects.join("session.jsonl"), line("m-10", 1000, 10))
            .expect("write transcript");
        std::fs::write(&projects.join("later.jsonl"), line("m-70", 3000, 70))
            .expect("write later transcript");
        let sessions = tmp.path().join("codex/sessions/2026/07/14");
        std::fs::create_dir_all(&sessions).expect("create sessions");
        std::fs::write(
            sessions.join(
                "rollout-2026-07-14T10-00-00-019f0000-0000-7000-8000-000000000001.jsonl",
            ),
            concat!(
                r#"{"type":"session_meta","timestamp":"2026-07-14T10:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000001","cwd":"/repo/memex"}}"#,
                "\n",
                // Numeric 2000 means epoch seconds here: 2_000_000 ms, between the two.
                r#"{"type":"event_msg","timestamp":2000,"payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":12345},"total_token_usage":{"input_tokens":12345}}}}"#,
                "\n",
            ),
        )
        .expect("write rollout");
        let _env = EnvVarGuard::set_os(&[
            (
                "CLAUDE_CONFIG_DIR",
                Some(tmp.path().join("claude").as_os_str()),
            ),
            ("CODEX_HOME", Some(tmp.path().join("codex").as_os_str())),
            ("OPENCODE_DATA_DIR", Some(empty.as_os_str())),
            ("COPILOT_HOME", Some(empty.as_os_str())),
            ("PI_CODING_AGENT_DIR", Some(empty.as_os_str())),
            ("OPENCLAW_STATE_DIR", Some(empty.as_os_str())),
            ("GROK_HOME", Some(empty.as_os_str())),
            ("HERMES_PROFILE_ROOTS", Some(empty.as_os_str())),
            ("JCODE_HOME", Some(empty.as_os_str())),
            ("MUSE_HOME", Some(empty.as_os_str())),
            ("ANTIGRAVITY_HOME", Some(empty.as_os_str())),
        ]);
        let cache = tmp.path().join("usage-cache.sqlite3");
        let base = UsageQuery {
            include_events: true,
            cache_path: Some(cache),
            memo_ttl_ms: 60_000,
            ..UsageQuery::default()
        };
        let claude = UsageQuery {
            source: Some(SourceFilter::Claude),
            ..base.clone()
        };
        let codex = UsageQuery {
            source: Some(SourceFilter::Codex),
            ..base.clone()
        };
        let combined = UsageQuery {
            source: None,
            ..base.clone()
        };

        let claude_report = scan_usage(&claude).expect("claude report");
        let codex_report = scan_usage(&codex).expect("codex report");
        assert_eq!(claude_report.events, 2);
        assert_eq!(claude_report.total_tokens, 80);
        assert_eq!(codex_report.events, 1);
        assert_eq!(codex_report.total_tokens, 12345);

        let report = scan_usage(&combined).expect("combined report");
        // Per-source rows through the merged path match the single-source reports
        // built from the same partitions.
        for single in [&claude_report, &codex_report] {
            let source = single.by_source[0].source.clone();
            let merged_row = report
                .by_source
                .iter()
                .find(|row| row.source == source)
                .expect("source row in combined report");
            assert_eq!(
                serde_json::to_value(merged_row).unwrap(),
                serde_json::to_value(&single.by_source[0]).unwrap(),
                "merged aggregation matches single-source aggregation for {source}"
            );
        }
        // Our three events interleave across partitions in global timestamp order.
        // (Other sources may contribute their own events on a dev machine; the
        // cursor home cannot be overridden, so only assert on our sessions.)
        let ours: Vec<u64> = report
            .details
            .iter()
            .filter(|event| {
                event.session_id.as_deref() == Some("session")
                    || event.session_id.as_deref() == Some("019f0000-0000-7000-8000-000000000001")
            })
            .map(|event| event.timestamp_ms)
            .collect();
        assert_eq!(ours, vec![1_000_000, 2_000_000, 3_000_000]);
        // The whole detail stream (whatever it contains) is globally ordered.
        let mut ordered = report.details.clone();
        ordered.sort_by(|a, b| {
            (a.timestamp_ms, &a.source_path, a.source_order).cmp(&(
                b.timestamp_ms,
                &b.source_path,
                b.source_order,
            ))
        });
        assert_eq!(
            serde_json::to_value(&report.details).unwrap(),
            serde_json::to_value(&ordered).unwrap(),
            "combined details are globally ordered"
        );
        // Activity points visit in the same merged order.
        let (points, _) = scan_usage_activity(&combined).expect("combined activity");
        let point_order: Vec<u64> = points.iter().map(|point| point.timestamp_ms).collect();
        let sorted = {
            let mut sorted = point_order.clone();
            sorted.sort_unstable();
            sorted
        };
        assert_eq!(point_order, sorted, "activity points are globally ordered");
    }
}
