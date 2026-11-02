//! Shared search ranking and conversation selection for every user-facing surface.
use crate::analytics::{AnalyticsStore, SessionKindFilter, analytics_path};
use crate::config::{Paths, UserConfig};
use crate::machine::{LOCAL_MACHINE_ID, LocatedRecord, SearchSpec, federated_search};
use crate::retrieval_eval::fuse_ranked_queries;
use crate::types::Record;
use anyhow::Result;
use serde::{Deserialize, Serialize};
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
    let store = (selection.kind_filter != SessionKindFilter::All)
        .then(|| AnalyticsStore::open_read_only(analytics_path(&paths.state)).ok())
        .flatten();
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
        let items = select_results(fused, selection, store.as_ref());
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

/// Attribution for a hit found in a child session and shown on the session it
/// worked for. The located record stays the child's own, so its record id
/// still reads the child, while reading and resuming go to this root session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRollup {
    pub session_id: String,
    /// Root transcript when local analytics or a root hit in the same results
    /// names it; empty selects every transcript of the root session id.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source_path: String,
    /// Kind of the child session the hit came from, such as `subagent`.
    pub matched_kind: String,
}

/// Backstop for the visited set when walking parent links; real chains are a
/// few links long.
const MAX_ROLLUP_DEPTH: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConversationKey {
    machine: String,
    source: String,
    session_id: String,
    source_path: String,
}
impl ConversationKey {
    fn of(result: &LocatedRecord) -> Self {
        Self::of_record(&result.machine, &result.record)
    }

    fn of_record(machine: &str, record: &Record) -> Self {
        Self {
            machine: machine.to_string(),
            source: record.source.storage_label().to_string(),
            session_id: record.session_id.clone(),
            source_path: record.source_path.clone(),
        }
    }

    /// The conversation a hit is shown under: its root session when it was
    /// rolled up, otherwise its own transcript.
    fn shown(result: &LocatedRecord) -> Self {
        match &result.rollup {
            Some(rollup) => Self {
                session_id: rollup.session_id.clone(),
                source_path: rollup.source_path.clone(),
                ..Self::of(result)
            },
            None => Self::of(result),
        }
    }
}

/// What one candidate transcript is and which session it works for.
#[derive(Debug, Clone, Default)]
struct SessionFacts {
    source_path: String,
    kind: Option<String>,
    parent_session_id: Option<String>,
}

/// Session facts for every candidate transcript. Groups resolve against the
/// stored session row first — complete-session truth — and fall back to the
/// matched records when the analytics cache has no row (remote machines, stale
/// caches). Record kinds prefer main: a sidechain line inside a primary
/// session must not decide the session's kind (mirrors the TUI and the
/// analytics accumulator).
fn candidate_facts<'r>(
    candidates: impl IntoIterator<Item = (&'r str, &'r Record)>,
    store: Option<&AnalyticsStore>,
) -> HashMap<ConversationKey, SessionFacts> {
    let mut facts: HashMap<ConversationKey, SessionFacts> = HashMap::new();
    for (machine, record) in candidates {
        let links = &record.links;
        let entry = facts
            .entry(ConversationKey::of_record(machine, record))
            .or_insert_with(|| SessionFacts {
                source_path: record.source_path.clone(),
                kind: links.conversation_kind.clone(),
                parent_session_id: None,
            });
        if links.conversation_kind.as_deref() == Some("main") {
            entry.kind = Some("main".to_string());
        }
        if entry.parent_session_id.is_none() {
            entry.parent_session_id = links.parent_session_id.clone().filter(|id| !id.is_empty());
        }
    }
    if let Some(store) = store {
        for (key, entry) in &mut facts {
            if key.machine != LOCAL_MACHINE_ID {
                continue;
            }
            let Some(stored) =
                store.session_lineage(&key.source, &key.session_id, &key.source_path)
            else {
                continue;
            };
            if stored.conversation_kind.is_some() {
                entry.kind = stored.conversation_kind;
            }
            if stored.parent_session_id.is_some() {
                entry.parent_session_id = stored.parent_session_id;
            }
        }
    }
    facts
}

/// Origin filter over one candidate set: walks child hits up their parent
/// links to the session the filter shows.
pub(crate) struct Rollups<'a> {
    filter: SessionKindFilter,
    store: Option<&'a AnalyticsStore>,
    candidates: HashMap<ConversationKey, SessionFacts>,
    by_id: HashMap<(String, String, String), SessionFacts>,
    roots: HashMap<ConversationKey, Option<SessionRollup>>,
}

impl<'a> Rollups<'a> {
    /// `candidates` are every (machine, record) hit the filter will see, so
    /// a parent's own hits can supply its facts and dedupe with its children.
    pub(crate) fn new<'r>(
        candidates: impl IntoIterator<Item = (&'r str, &'r Record)>,
        filter: SessionKindFilter,
        store: Option<&'a AnalyticsStore>,
    ) -> Self {
        Self {
            filter,
            store,
            candidates: candidate_facts(candidates, store),
            by_id: HashMap::new(),
            roots: HashMap::new(),
        }
    }

    /// Whether the filter keeps this hit, attributing it to its root session
    /// when it comes from a child the filter rolls up.
    pub(crate) fn admit(&mut self, result: &mut LocatedRecord) -> bool {
        match self.attribute(&result.machine, &result.record) {
            Some(rollup) => {
                result.rollup = rollup;
                true
            }
            None => false,
        }
    }

    /// None drops the hit; otherwise the root it rolls up to, if any.
    pub(crate) fn attribute(
        &mut self,
        machine: &str,
        record: &Record,
    ) -> Option<Option<SessionRollup>> {
        let key = ConversationKey::of_record(machine, record);
        let kind = self
            .candidates
            .get(&key)
            .and_then(|facts| facts.kind.clone());
        let own_row = self.filter.matches_kind(kind.as_deref()).then_some(None);
        if !self.filter.rolls_up(kind.as_deref()) {
            return own_row;
        }
        // A child with no visible root (no parent link, say) keeps whatever
        // the filter did before roll-up: its own row, or nothing.
        self.root_for(machine, record).map(Some).or(own_row)
    }

    /// The root a hit from a rolled-up child is shown under.
    fn root_for(&mut self, machine: &str, record: &Record) -> Option<SessionRollup> {
        let key = ConversationKey::of_record(machine, record);
        if let Some(root) = self.roots.get(&key) {
            return root.clone();
        }
        let root = self.root(&key);
        self.roots.insert(key, root.clone());
        root
    }

    /// The root a child hit rolls up to, or None when the hit has no visible
    /// root: an orphan child, a cycle, or a root the filter excludes.
    fn root(&mut self, key: &ConversationKey) -> Option<SessionRollup> {
        let start = self.candidates.get(key)?.clone();
        let matched_kind = start.kind.clone().unwrap_or_default();
        let mut session_id = key.session_id.clone();
        let mut current = start;
        let mut visited = HashSet::from([session_id.clone()]);
        for _ in 0..MAX_ROLLUP_DEPTH {
            if !self.filter.rolls_up(current.kind.as_deref()) {
                break;
            }
            let Some(parent) = current.parent_session_id.clone() else {
                // A sidechain without a parent link is a branch inside its own
                // transcript (Claude writes those into the main session file).
                if current.kind.as_deref() == Some("sidechain") {
                    current.kind = None;
                    break;
                }
                return None;
            };
            if !visited.insert(parent.clone()) {
                return None;
            }
            current = self.parent(&key.machine, &key.source, &parent);
            session_id = parent;
        }
        if self.filter.rolls_up(current.kind.as_deref())
            || !self.filter.matches_kind(current.kind.as_deref())
        {
            return None;
        }
        Some(SessionRollup {
            session_id,
            source_path: current.source_path,
            matched_kind,
        })
    }

    /// Facts for a parent known only by id: a transcript of it among the
    /// candidates first (so its own hits dedupe with its children's), then
    /// local analytics. A remote parent missing from the candidates is assumed
    /// to be a root conversation with an unknown transcript path.
    fn parent(&mut self, machine: &str, source: &str, session_id: &str) -> SessionFacts {
        let id = (
            machine.to_string(),
            source.to_string(),
            session_id.to_string(),
        );
        if let Some(found) = self.by_id.get(&id) {
            return found.clone();
        }
        let candidate = self
            .candidates
            .iter()
            .filter(|(key, _)| {
                key.machine == machine && key.source == source && key.session_id == session_id
            })
            .min_by(|(left_key, left), (right_key, right)| {
                let is_child =
                    |facts: &SessionFacts| facts.kind.as_deref().is_some_and(|kind| kind != "main");
                is_child(left)
                    .cmp(&is_child(right))
                    .then_with(|| left_key.source_path.cmp(&right_key.source_path))
            })
            .map(|(_, facts)| facts.clone());
        let found = candidate
            .or_else(|| {
                (machine == LOCAL_MACHINE_ID)
                    .then(|| self.store?.session_lineage_by_id(source, session_id))
                    .flatten()
                    .map(|stored| SessionFacts {
                        source_path: stored.source_path,
                        kind: stored.conversation_kind,
                        parent_session_id: stored.parent_session_id,
                    })
            })
            .unwrap_or_default();
        self.by_id.insert(id, found.clone());
        found
    }
}

fn select_results(
    mut results: Vec<LocatedRecord>,
    selection: &Selection,
    store: Option<&AnalyticsStore>,
) -> Vec<LocatedRecord> {
    // Session-grouped origin filter. Hits in child sessions the filter rolls
    // up (subagents and sidechains, plus forks for the interactive filter) are
    // attributed to their root session instead of being dropped, so a match
    // only a subagent said still surfaces the conversation it worked for.
    if selection.kind_filter != SessionKindFilter::All {
        let candidates = results
            .iter()
            .map(|result| (result.machine.as_str(), &result.record));
        let mut rollups = Rollups::new(candidates, selection.kind_filter, store);
        results.retain_mut(|result| rollups.admit(result));
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
            let count = per_session
                .entry(ConversationKey::shown(result))
                .or_default();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RecordLinks, SourceKind};

    fn hit(
        machine: &str,
        session_id: &str,
        kind: &str,
        parent: Option<&str>,
        doc_id: u64,
        score: f32,
    ) -> LocatedRecord {
        LocatedRecord {
            machine: machine.to_string(),
            score,
            record: Record {
                source: SourceKind::Claude,
                doc_id,
                ts: doc_id,
                project: "memex".to_string(),
                session_id: session_id.to_string(),
                turn_id: doc_id as u32,
                role: "assistant".to_string(),
                text: format!("record {doc_id}"),
                tool_name: None,
                tool_input: None,
                tool_output: None,
                links: RecordLinks {
                    conversation_kind: Some(kind.to_string()),
                    parent_session_id: parent.map(str::to_string),
                    ..RecordLinks::default()
                },
                source_path: format!("/sessions/{session_id}.jsonl"),
            },
            rollup: None,
        }
    }

    fn conversations(kind_filter: SessionKindFilter) -> Selection {
        Selection::conversations(10, kind_filter)
    }

    fn shown(results: &[LocatedRecord]) -> Vec<(String, Option<SessionRollup>)> {
        results
            .iter()
            .map(|result| (result.record.session_id.clone(), result.rollup.clone()))
            .collect()
    }

    fn rollup(session_id: &str, source_path: &str, matched_kind: &str) -> Option<SessionRollup> {
        Some(SessionRollup {
            session_id: session_id.to_string(),
            source_path: source_path.to_string(),
            matched_kind: matched_kind.to_string(),
        })
    }

    /// Analytics rows for the given (session, kind, parent) transcripts, as
    /// local ingest stores them.
    fn store(sessions: &[(&str, &str, Option<&str>)]) -> (tempfile::TempDir, AnalyticsStore) {
        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("analytics.sqlite");
        let records = sessions
            .iter()
            .enumerate()
            .map(|(index, (session, kind, parent))| {
                hit(
                    LOCAL_MACHINE_ID,
                    session,
                    kind,
                    *parent,
                    index as u64 + 1,
                    0.0,
                )
                .record
            });
        crate::analytics::rebuild_from_records(&db, records).unwrap();
        let store = AnalyticsStore::open_read_only(&db).unwrap();
        (tmp, store)
    }

    #[test]
    fn child_only_match_returns_the_parent_with_child_attribution() {
        let (_tmp, store) = store(&[
            ("parent", "main", None),
            ("agent-a", "subagent", Some("parent")),
        ]);
        // The indexed child record carries no parent link of its own here, so
        // the stored lineage is what finds the root.
        let child = hit(LOCAL_MACHINE_ID, "agent-a", "subagent", None, 7, 3.0);
        for filter in [SessionKindFilter::Primary, SessionKindFilter::Regular] {
            let results = select_results(vec![child.clone()], &conversations(filter), Some(&store));
            assert_eq!(
                shown(&results),
                vec![(
                    "agent-a".to_string(),
                    rollup("parent", "/sessions/parent.jsonl", "subagent")
                )],
                "{filter:?}"
            );
        }

        // Explicit subagent and all filters list the child itself.
        for filter in [SessionKindFilter::Subagent, SessionKindFilter::All] {
            let results = select_results(vec![child.clone()], &conversations(filter), Some(&store));
            assert_eq!(shown(&results), vec![("agent-a".to_string(), None)]);
        }
    }

    #[test]
    fn parent_and_child_matches_dedupe_to_one_row() {
        let (_tmp, store) = store(&[
            ("parent", "main", None),
            ("agent-a", "subagent", Some("parent")),
            ("agent-b", "subagent", Some("agent-a")),
        ]);
        let results = vec![
            hit(LOCAL_MACHINE_ID, "parent", "main", None, 1, 1.0),
            hit(
                LOCAL_MACHINE_ID,
                "agent-a",
                "subagent",
                Some("parent"),
                2,
                2.0,
            ),
            hit(
                LOCAL_MACHINE_ID,
                "agent-b",
                "subagent",
                Some("agent-a"),
                3,
                3.0,
            ),
        ];
        let selected = select_results(
            results.clone(),
            &conversations(SessionKindFilter::Primary),
            Some(&store),
        );
        // The best hit, two subagents deep, represents the root conversation.
        assert_eq!(
            shown(&selected),
            vec![(
                "agent-b".to_string(),
                rollup("parent", "/sessions/parent.jsonl", "subagent")
            )]
        );

        // Record-level listings keep every hit, each attributed to the root.
        let selection = Selection {
            top_n_per_session: None,
            ..conversations(SessionKindFilter::Regular)
        };
        let selected = select_results(results, &selection, Some(&store));
        assert_eq!(
            selected
                .iter()
                .map(|result| ConversationKey::shown(result).session_id)
                .collect::<Vec<_>>(),
            vec!["parent", "parent", "parent"]
        );
    }

    #[test]
    fn remote_hits_roll_up_on_their_own_parent_links() {
        let remote = "devbox";
        // With the parent's own hit present, the child dedupes onto that
        // transcript even though no analytics describe the remote machine.
        let selected = select_results(
            vec![
                hit(remote, "parent", "main", None, 1, 1.0),
                hit(remote, "agent-a", "subagent", Some("parent"), 2, 2.0),
            ],
            &conversations(SessionKindFilter::Primary),
            None,
        );
        assert_eq!(
            shown(&selected),
            vec![(
                "agent-a".to_string(),
                rollup("parent", "/sessions/parent.jsonl", "subagent")
            )]
        );

        // A child-only remote hit names the parent by id alone; the same
        // session id on another machine stays a separate conversation.
        let selected = select_results(
            vec![
                hit(remote, "agent-a", "subagent", Some("parent"), 2, 2.0),
                hit(LOCAL_MACHINE_ID, "parent", "main", None, 1, 1.0),
            ],
            &conversations(SessionKindFilter::Primary),
            None,
        );
        assert_eq!(
            shown(&selected),
            vec![
                ("agent-a".to_string(), rollup("parent", "", "subagent")),
                ("parent".to_string(), None),
            ]
        );
    }

    #[test]
    fn filters_decide_which_child_kinds_roll_up() {
        let results = || {
            vec![
                hit("devbox", "fork", "fork", Some("root"), 1, 4.0),
                hit("devbox", "review", "guardian_review", Some("root"), 2, 3.0),
                hit("devbox", "orphan", "subagent", None, 3, 2.0),
                hit("devbox", "side", "sidechain", None, 4, 1.0),
            ]
        };
        // Interactive folds forks into their root and drops orphans and reviews.
        let primary = select_results(results(), &conversations(SessionKindFilter::Primary), None);
        assert_eq!(
            shown(&primary),
            vec![
                ("fork".to_string(), rollup("root", "", "fork")),
                (
                    "side".to_string(),
                    rollup("side", "/sessions/side.jsonl", "sidechain")
                ),
            ]
        );
        // Regular keeps resumable forks and orphan subagents as their own rows.
        let regular = select_results(results(), &conversations(SessionKindFilter::Regular), None);
        assert_eq!(
            shown(&regular),
            vec![
                ("fork".to_string(), None),
                ("orphan".to_string(), None),
                (
                    "side".to_string(),
                    rollup("side", "/sessions/side.jsonl", "sidechain")
                ),
            ]
        );
    }

    #[test]
    fn parent_cycles_do_not_hide_or_loop() {
        let selected = select_results(
            vec![
                hit("devbox", "a", "subagent", Some("b"), 1, 2.0),
                hit("devbox", "b", "subagent", Some("a"), 2, 1.0),
            ],
            &conversations(SessionKindFilter::Regular),
            None,
        );
        assert_eq!(
            shown(&selected),
            vec![("a".to_string(), None), ("b".to_string(), None)]
        );
    }
}
