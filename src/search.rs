//! Shared search ranking and conversation selection for every user-facing surface.
use crate::analytics::{AnalyticsStore, SessionKindFilter, analytics_path};
use crate::config::{Paths, UserConfig};
use crate::machine::{LOCAL_MACHINE_ID, LocatedRecord, SearchSpec, federated_search};
use crate::retrieval_eval::fuse_ranked_queries;
use anyhow::Result;
use std::collections::{HashMap, HashSet};

pub(crate) const DEFAULT_RECENCY_WEIGHT: f32 = 0.0;

#[derive(Clone, Copy)]
pub(crate) enum Sort {
    Score,
    Timestamp,
}

pub(crate) struct Selection {
    pub sort: Sort,
    pub top_n_per_session: Option<usize>,
    pub limit: usize,
    pub kind_filter: SessionKindFilter,
}
impl Selection {
    pub fn conversations(limit: usize, kind_filter: SessionKindFilter) -> Self {
        Self {
            sort: Sort::Score,
            top_n_per_session: Some(1),
            limit,
            kind_filter,
        }
    }
}
pub(crate) struct SearchResults {
    pub items: Vec<LocatedRecord>,
    pub failures: Vec<(String, String)>,
    pub query_candidate_counts: Vec<usize>,
}

pub(crate) fn compare_records(left: &LocatedRecord, right: &LocatedRecord) -> std::cmp::Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| right.record.ts.cmp(&left.record.ts))
        .then_with(|| left.machine.cmp(&right.machine))
        .then_with(|| left.record.doc_id.cmp(&right.record.doc_id))
        .then_with(|| {
            left.record
                .source
                .storage_label()
                .cmp(right.record.source.storage_label())
        })
        .then_with(|| left.record.session_id.cmp(&right.record.session_id))
        .then_with(|| left.record.source_path.cmp(&right.record.source_path))
}

pub(crate) fn collect(
    paths: &Paths,
    config: &UserConfig,
    machines: &[String],
    queries: &[String],
    spec: &SearchSpec,
    selection: &Selection,
    auto_index_local: bool,
) -> Result<SearchResults> {
    let mut candidate_limit = selection.limit.saturating_mul(5).max(100);
    let mut failures = Vec::new();
    let mut seen_failures = HashSet::new();
    let mut first_round = true;
    loop {
        let mut ranked = Vec::new();
        let mut counts = Vec::new();
        let mut capped = false;
        for (query_index, query) in queries.iter().enumerate() {
            let request = SearchSpec {
                query: query.clone(),
                limit: candidate_limit,
                ..spec.clone()
            };
            let result = federated_search(
                paths,
                config,
                machines,
                &request,
                auto_index_local && first_round && query_index == 0,
            )?;
            capped |= result.candidate_count >= candidate_limit;
            counts.push(result.candidate_count);
            for failure in result.failures {
                if seen_failures.insert(failure.clone()) {
                    failures.push(failure);
                }
            }
            ranked.push(result.items);
        }
        let fused = if ranked.len() == 1 {
            ranked.pop().unwrap_or_default()
        } else {
            fuse_ranked_queries(ranked, crate::retrieval_eval::DEFAULT_RRF_K)
        };
        let kinds = stored_session_kinds(
            paths,
            &fused,
            selection.kind_filter != SessionKindFilter::All,
        );
        let items = select_results(fused, selection, &kinds);
        if items.len() >= selection.limit || !capped || candidate_limit == usize::MAX {
            return Ok(SearchResults {
                items,
                failures,
                query_candidate_counts: counts,
            });
        }
        candidate_limit = candidate_limit.saturating_mul(2);
        first_round = false;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConversationKey {
    machine: String,
    source: String,
    session_id: String,
    source_path: String,
}
impl ConversationKey {
    fn of(result: &LocatedRecord) -> Self {
        Self {
            machine: result.machine.clone(),
            source: result.record.source.storage_label().to_string(),
            session_id: result.record.session_id.clone(),
            source_path: result.record.source_path.clone(),
        }
    }
}

/// Stored session kinds for the candidate groups, keyed by
/// (machine, source label, session id, source path). Remote
/// machines and sessions missing from the analytics cache simply have no
/// entry, and grouping falls back to the matched records. Empty unless an
/// origin filter is active.
fn stored_session_kinds(
    paths: &Paths,
    results: &[LocatedRecord],
    origin_filtered: bool,
) -> HashMap<ConversationKey, Option<String>> {
    let mut out = HashMap::new();
    if !origin_filtered {
        return out;
    }
    let Ok(store) = AnalyticsStore::open_read_only(analytics_path(&paths.state)) else {
        return out;
    };
    for result in results {
        if result.machine != LOCAL_MACHINE_ID {
            continue;
        }
        let key = ConversationKey::of(result);
        out.entry(key).or_insert_with(|| {
            store.session_conversation_kind(
                result.record.source.storage_label(),
                &result.record.session_id,
                &result.record.source_path,
            )
        });
    }
    out
}

fn select_results(
    mut results: Vec<LocatedRecord>,
    selection: &Selection,
    stored_kinds: &HashMap<ConversationKey, Option<String>>,
) -> Vec<LocatedRecord> {
    // Session-grouped origin filter with prefer-main: a sidechain hit inside
    // a primary session must not hide the session (mirrors the TUI and the
    // analytics accumulator). Groups resolve against the stored session kind
    // first — complete-session truth — and only fall back to the matched
    // records when the analytics cache has no row (remote machines, stale
    // caches).
    if selection.kind_filter != crate::analytics::SessionKindFilter::All {
        let mut group_kind: HashMap<ConversationKey, Option<String>> = HashMap::new();
        for result in &results {
            let key = ConversationKey::of(result);
            let dominated = result.record.links.conversation_kind.as_deref() == Some("main");
            group_kind
                .entry(key)
                .and_modify(|kind| {
                    if dominated {
                        *kind = Some("main".to_string());
                    }
                })
                .or_insert_with(|| result.record.links.conversation_kind.clone());
        }
        results.retain(|result| {
            let key = ConversationKey::of(result);
            let kind = stored_kinds
                .get(&key)
                .and_then(|stored| stored.as_deref())
                .or_else(|| group_kind.get(&key).and_then(|kind| kind.as_deref()));
            selection.kind_filter.matches_kind(kind)
        });
    }

    match selection.sort {
        Sort::Score => {
            results.sort_by(compare_records);
        }
        Sort::Timestamp => {
            results.sort_by(|left, right| {
                right
                    .record
                    .ts
                    .cmp(&left.record.ts)
                    .then_with(|| compare_records(left, right))
            });
        }
    }

    if let Some(k) = selection.top_n_per_session {
        let mut per_session: HashMap<ConversationKey, usize> = HashMap::new();
        results.retain(|result| {
            let count = per_session.entry(ConversationKey::of(result)).or_default();
            if *count >= k {
                return false;
            }
            *count += 1;
            true
        });
    }

    results.truncate(selection.limit);
    results
}
