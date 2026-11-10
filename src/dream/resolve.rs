//! Finds a citation's source again, cheapest exact match first:
//!
//! 1. the canonical record ID within its session, which survives every reindex;
//! 2. the source's native event ID, which survives an ID scheme change and a moved file;
//! 3. identical text within the session, which survives a record that changed identity;
//! 4. a search for the excerpt, recorded as `remapped` because it is not an exact match.
//!
//! Only when all of them fail is the evidence marked lost, and it is marked, never dropped:
//! the dream keeps its inline excerpt. A machine that cannot be reached leaves the citation
//! `unverified`, which is not evidence of loss.

use super::apply::Change;
use super::gather::redact;
use super::model::{Citation, CitationState, CitationTarget, Dream, normalize_text, text_sha256};
use crate::config::{Paths, UserConfig};
use crate::index::{QueryOptions, SearchIndex};
use crate::machine::{self, LOCAL_MACHINE_ID, SearchMode, SearchSpec};
use crate::memory::{MemoryDocument, MemoryStore};
use crate::redact::Redactor;
use crate::retrieval::{CANONICAL_RECORD_ID_VERSION, ContextSelector, canonical_record_id};
use crate::types::{Record, SourceFilter, SourceKind};
use serde::Serialize;
use std::cell::OnceCell;

/// Words of the excerpt used as the fallback search query.
const EXCERPT_QUERY_WORDS: usize = 8;
const EXCERPT_SEARCH_LIMIT: usize = 10;
/// Share of the excerpt's words a same-session candidate must contain to count as a remap
/// when the projected text no longer contains the excerpt verbatim.
const REMAP_WORD_COVERAGE: f32 = 0.85;

pub enum Lookup<T> {
    Found(T),
    Missing,
    /// The machine holding the source could not be asked.
    Unreachable(String),
}

/// Where citations are looked up. The live implementation reads the local index directly
/// and other machines over the existing RPC; tests substitute fixed records.
pub trait EvidenceSource {
    fn record_by_rid(
        &self,
        machine: &str,
        rid: &str,
        session_id: &str,
        source: SourceKind,
    ) -> Lookup<Record>;

    fn records_by_event(
        &self,
        machine: &str,
        event_id: &str,
        session_id: &str,
        source: SourceKind,
    ) -> Lookup<Vec<Record>>;

    fn session_records(
        &self,
        machine: &str,
        session_id: &str,
        source: SourceKind,
    ) -> Lookup<Vec<Record>>;

    /// Lexical search, scoped to `session_id` when given.
    fn search(
        &self,
        machine: &str,
        query: &str,
        session_id: Option<&str>,
        source: SourceKind,
    ) -> Lookup<Vec<Record>>;

    fn memory(&self, machine: &str, memory_id: &str) -> Lookup<MemoryDocument>;
}

pub struct LiveEvidence<'a> {
    paths: &'a Paths,
    config: &'a UserConfig,
    index: OnceCell<Option<SearchIndex>>,
}

impl<'a> LiveEvidence<'a> {
    pub fn new(paths: &'a Paths, config: &'a UserConfig) -> Self {
        Self {
            paths,
            config,
            index: OnceCell::new(),
        }
    }

    /// The local index, opened once. A root that was never indexed has no records to find.
    fn local_index(&self) -> Option<&SearchIndex> {
        self.index
            .get_or_init(|| {
                SearchIndex::exists(&self.paths.index)
                    .then(|| SearchIndex::open_or_create(&self.paths.index).ok())
                    .flatten()
            })
            .as_ref()
    }
}

fn from_local<T>(result: anyhow::Result<Vec<T>>) -> Lookup<Vec<T>> {
    match result {
        Ok(items) if items.is_empty() => Lookup::Missing,
        Ok(items) => Lookup::Found(items),
        Err(error) => Lookup::Unreachable(format!("{error:#}")),
    }
}

/// A peer that answered with an error was reached, so the source is missing there; any
/// other failure (SSH, timeout, protocol) says nothing about the source.
fn from_remote<T>(result: anyhow::Result<T>, answered_prefix: &str) -> Lookup<T> {
    match result {
        Ok(value) => Lookup::Found(value),
        Err(error) if error.to_string().starts_with(answered_prefix) => Lookup::Missing,
        Err(error) => Lookup::Unreachable(format!("{error:#}")),
    }
}

impl EvidenceSource for LiveEvidence<'_> {
    fn record_by_rid(
        &self,
        machine: &str,
        rid: &str,
        session_id: &str,
        source: SourceKind,
    ) -> Lookup<Record> {
        if machine == LOCAL_MACHINE_ID {
            let Some(index) = self.local_index() else {
                return Lookup::Missing;
            };
            return match from_local(index.records_by_canonical_id_in_session_scope(
                rid,
                session_id,
                None,
                Some(source),
            )) {
                Lookup::Found(mut records) => Lookup::Found(records.remove(0)),
                Lookup::Missing => Lookup::Missing,
                Lookup::Unreachable(error) => Lookup::Unreachable(error),
            };
        }
        from_remote(
            machine::record_by_selector(
                self.paths,
                self.config,
                machine,
                &ContextSelector::record_id(rid)
                    .with_scope(Some(session_id.to_string()), Some(source)),
            ),
            "record resolution failed:",
        )
    }

    fn records_by_event(
        &self,
        machine: &str,
        event_id: &str,
        session_id: &str,
        source: SourceKind,
    ) -> Lookup<Vec<Record>> {
        if machine == LOCAL_MACHINE_ID {
            let Some(index) = self.local_index() else {
                return Lookup::Missing;
            };
            return from_local(index.records_by_event_id(event_id).map(|records| {
                records
                    .into_iter()
                    .filter(|record| record.session_id == session_id && record.source == source)
                    .collect()
            }));
        }
        // A peer resolves one record per selector; several records sharing the event ID
        // read as an error there and fall through to the text tiers.
        match from_remote(
            machine::record_by_selector(
                self.paths,
                self.config,
                machine,
                &ContextSelector::event_id(event_id)
                    .with_scope(Some(session_id.to_string()), Some(source)),
            ),
            "record resolution failed:",
        ) {
            Lookup::Found(record) => Lookup::Found(vec![record]),
            Lookup::Missing => Lookup::Missing,
            Lookup::Unreachable(error) => Lookup::Unreachable(error),
        }
    }

    fn session_records(
        &self,
        machine: &str,
        session_id: &str,
        source: SourceKind,
    ) -> Lookup<Vec<Record>> {
        if machine == LOCAL_MACHINE_ID {
            let Some(index) = self.local_index() else {
                return Lookup::Missing;
            };
            return from_local(index.records_by_session_id(session_id).map(|records| {
                records
                    .into_iter()
                    .filter(|record| record.source == source)
                    .collect()
            }));
        }
        match from_remote(
            machine::session_records(self.paths, self.config, machine, session_id, ""),
            "session failed:",
        ) {
            Lookup::Found(records) => {
                let records = records
                    .into_iter()
                    .filter(|record| record.source == source)
                    .collect::<Vec<_>>();
                if records.is_empty() {
                    Lookup::Missing
                } else {
                    Lookup::Found(records)
                }
            }
            other => other,
        }
    }

    fn search(
        &self,
        machine: &str,
        query: &str,
        session_id: Option<&str>,
        source: SourceKind,
    ) -> Lookup<Vec<Record>> {
        let source_filter = <SourceFilter as clap::ValueEnum>::from_str(source.label(), true).ok();
        if machine == LOCAL_MACHINE_ID {
            let Some(index) = self.local_index() else {
                return Lookup::Missing;
            };
            return from_local(
                index
                    .search(&QueryOptions {
                        query: query.to_string(),
                        project: None,
                        role: None,
                        tool: None,
                        session_id: session_id.map(str::to_string),
                        session_scope: None,
                        source: source_filter,
                        since: None,
                        until: None,
                        limit: EXCERPT_SEARCH_LIMIT,
                    })
                    .map(|hits| hits.into_iter().map(|(_, record)| record).collect()),
            );
        }
        let spec = SearchSpec {
            query: query.to_string(),
            project: None,
            role: None,
            tool: None,
            session_id: session_id.map(str::to_string),
            session_scope: None,
            cwd: None,
            source: source_filter,
            since: None,
            until: None,
            limit: EXCERPT_SEARCH_LIMIT,
            mode: SearchMode::Lexical,
            recency_weight: 0.0,
            recency_half_life_days: 30.0,
            min_score: None,
            project_grouping: None,
            text_limit: None,
            tags: Vec::new(),
        };
        match machine::federated_search(
            self.paths,
            self.config,
            &[machine.to_string()],
            &spec,
            false,
        ) {
            Ok(found) if found.items.is_empty() => Lookup::Missing,
            Ok(found) => Lookup::Found(found.items.into_iter().map(|item| item.record).collect()),
            Err(error) => Lookup::Unreachable(format!("{error:#}")),
        }
    }

    fn memory(&self, machine: &str, memory_id: &str) -> Lookup<MemoryDocument> {
        if machine != LOCAL_MACHINE_ID {
            return Lookup::Unreachable(
                "memory citations are only re-checked on the machine that wrote them".to_string(),
            );
        }
        match MemoryStore::new(self.paths.root.join("memory/documents.json")).get(memory_id) {
            Ok(Some(document)) => Lookup::Found(document),
            Ok(None) => Lookup::Missing,
            Err(error) => Lookup::Unreachable(format!("{error:#}")),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct LintReport {
    pub dreams: usize,
    pub citations: usize,
    pub resolved: usize,
    pub remapped: usize,
    pub unverified: usize,
    pub evidence_lost: usize,
    pub source_revised: usize,
    /// Citations whose state or identity changed in this pass.
    pub changed: usize,
    /// Dreams left with no readable evidence.
    pub dreams_without_evidence: usize,
}

/// Re-resolve every citation in place and report what moved.
pub fn lint_dreams(
    dreams: &mut [Dream],
    source: &dyn EvidenceSource,
    redactor: Option<&Redactor>,
    now_ms: u64,
) -> (LintReport, Vec<Change>) {
    let mut report = LintReport {
        dreams: dreams.len(),
        ..LintReport::default()
    };
    let mut changes = Vec::new();
    for dream in dreams.iter_mut() {
        for citation in &mut dream.citations {
            let before = citation.clone();
            let detail = resolve_citation(citation, source, redactor);
            citation.checked_at_ms = Some(now_ms);
            report.citations += 1;
            match citation.state {
                CitationState::Resolved => report.resolved += 1,
                CitationState::Remapped => report.remapped += 1,
                CitationState::Unverified => report.unverified += 1,
                CitationState::EvidenceLost => report.evidence_lost += 1,
                CitationState::SourceRevised => report.source_revised += 1,
            }
            if before.state != citation.state || before.target != citation.target {
                report.changed += 1;
                changes.push(Change::CitationChanged {
                    dream_id: dream.id.clone(),
                    citation: citation_label(citation),
                    from: before.state,
                    to: citation.state,
                    detail,
                });
            }
        }
        if !dream
            .citations
            .iter()
            .any(|citation| citation.state.supports())
        {
            report.dreams_without_evidence += 1;
        }
    }
    (report, changes)
}

pub fn citation_label(citation: &Citation) -> String {
    match &citation.target {
        CitationTarget::Record { rid, .. } => format!("{}:{rid}", citation.machine),
        CitationTarget::Memory { memory_id, .. } => format!("{}:{memory_id}", citation.machine),
    }
}

/// Update `citation` to where its source is now. Returns a short account of the outcome.
fn resolve_citation(
    citation: &mut Citation,
    source: &dyn EvidenceSource,
    redactor: Option<&Redactor>,
) -> String {
    let machine = citation.machine.clone();
    match citation.target.clone() {
        CitationTarget::Record {
            rid,
            rid_version,
            source: kind,
            session_id,
            event_id,
            role,
            ..
        } => {
            let outcome = resolve_record(
                source,
                &machine,
                &RecordQuery {
                    rid: &rid,
                    rid_version: &rid_version,
                    source: kind,
                    session_id: &session_id,
                    event_id: event_id.as_deref(),
                    role: &role,
                    text_sha256: &citation.text_sha256,
                    excerpt: &citation.excerpt,
                },
                redactor,
            );
            match outcome {
                RecordOutcome::Found { record, via, exact } => {
                    let new_rid = canonical_record_id(&record);
                    if let CitationTarget::Record {
                        rid,
                        rid_version,
                        rid_prev,
                        session_id,
                        source_path,
                        event_id,
                        ..
                    } = &mut citation.target
                    {
                        if *rid != new_rid {
                            // Rewrite to the current ID so the next lookup is the cheap one.
                            *rid_prev = Some(std::mem::replace(rid, new_rid));
                        }
                        *rid_version = CANONICAL_RECORD_ID_VERSION.to_string();
                        *session_id = record.session_id.clone();
                        *source_path = record.source_path.clone();
                        if event_id.is_none() {
                            *event_id = record.links.event_id.clone();
                        }
                    }
                    citation.state = if exact {
                        CitationState::Resolved
                    } else {
                        CitationState::Remapped
                    };
                    citation.resolved_via = Some(via.to_string());
                    format!("found by {via}")
                }
                RecordOutcome::Unreachable(error) => {
                    citation.state = CitationState::Unverified;
                    citation.resolved_via = None;
                    format!("machine unreachable: {error}")
                }
                RecordOutcome::Lost => {
                    citation.state = CitationState::EvidenceLost;
                    citation.resolved_via = None;
                    "source record no longer indexed".to_string()
                }
            }
        }
        CitationTarget::Memory {
            memory_id,
            content_version,
            ..
        } => match source.memory(&machine, &memory_id) {
            Lookup::Found(document) => {
                if document.version_sha256 == content_version {
                    citation.state = CitationState::Resolved;
                    citation.resolved_via = Some("content_version".to_string());
                    return "memory unchanged".to_string();
                }
                let current = normalize_text(&redact(redactor, &document.content));
                if !citation.excerpt.is_empty() && current.contains(&citation.excerpt) {
                    if let CitationTarget::Memory {
                        content_version,
                        content_version_prev,
                        ..
                    } = &mut citation.target
                    {
                        *content_version_prev = Some(std::mem::replace(
                            content_version,
                            document.version_sha256.clone(),
                        ));
                    }
                    citation.state = CitationState::Resolved;
                    citation.resolved_via = Some("excerpt".to_string());
                    "memory edited; cited passage still present".to_string()
                } else {
                    // The user may have corrected the note on purpose: the dream needs a
                    // fresh look, which is different from its evidence having vanished.
                    citation.state = CitationState::SourceRevised;
                    citation.resolved_via = None;
                    "memory edited; cited passage is gone".to_string()
                }
            }
            Lookup::Missing => {
                citation.state = CitationState::EvidenceLost;
                citation.resolved_via = None;
                "memory document no longer indexed".to_string()
            }
            Lookup::Unreachable(error) => {
                citation.state = CitationState::Unverified;
                citation.resolved_via = None;
                format!("memory unavailable: {error}")
            }
        },
    }
}

struct RecordQuery<'a> {
    rid: &'a str,
    rid_version: &'a str,
    source: SourceKind,
    session_id: &'a str,
    event_id: Option<&'a str>,
    role: &'a str,
    text_sha256: &'a str,
    excerpt: &'a str,
}

enum RecordOutcome {
    Found {
        record: Box<Record>,
        via: &'static str,
        exact: bool,
    },
    Unreachable(String),
    Lost,
}

fn resolve_record(
    source: &dyn EvidenceSource,
    machine: &str,
    query: &RecordQuery<'_>,
    redactor: Option<&Redactor>,
) -> RecordOutcome {
    let found = |record: Record, via, exact| RecordOutcome::Found {
        record: Box::new(record),
        via,
        exact,
    };

    // An ID from an older scheme cannot match; skip straight to the keys that outlive it.
    if query.rid_version == CANONICAL_RECORD_ID_VERSION {
        match source.record_by_rid(machine, query.rid, query.session_id, query.source) {
            Lookup::Found(record) => return found(record, "rid", true),
            Lookup::Unreachable(error) => return RecordOutcome::Unreachable(error),
            Lookup::Missing => {}
        }
    }

    if let Some(event_id) = query.event_id {
        match source.records_by_event(machine, event_id, query.session_id, query.source) {
            Lookup::Found(records) => {
                // One provider event can project to several records (text plus tool calls);
                // the role, then the text, picks the cited one.
                let mut same_role = records
                    .into_iter()
                    .filter(|record| record.role == query.role)
                    .collect::<Vec<_>>();
                if let Some(position) = same_role
                    .iter()
                    .position(|record| text_sha256(&record.text) == query.text_sha256)
                {
                    return found(same_role.swap_remove(position), "event_id", true);
                }
                if same_role.len() == 1 {
                    return found(same_role.remove(0), "event_id", true);
                }
            }
            Lookup::Unreachable(error) => return RecordOutcome::Unreachable(error),
            Lookup::Missing => {}
        }
    }

    match source.session_records(machine, query.session_id, query.source) {
        Lookup::Found(records) => {
            if let Some(record) = records.into_iter().find(|record| {
                record.role == query.role && text_sha256(&record.text) == query.text_sha256
            }) {
                return found(record, "text_hash", true);
            }
        }
        Lookup::Unreachable(error) => return RecordOutcome::Unreachable(error),
        Lookup::Missing => {}
    }

    let Some(search_query) = excerpt_query(query.excerpt) else {
        return RecordOutcome::Lost;
    };
    let contains_excerpt =
        |record: &Record| normalize_text(&redact(redactor, &record.text)).contains(query.excerpt);
    match source.search(machine, &search_query, Some(query.session_id), query.source) {
        Lookup::Found(records) => {
            let candidates = records
                .into_iter()
                .filter(|record| record.role == query.role)
                .collect::<Vec<_>>();
            if let Some(record) = candidates.iter().find(|record| contains_excerpt(record)) {
                return found(record.clone(), "excerpt", false);
            }
            // Same session and role with nearly all of the excerpt's words: the parser
            // changed how it projects the text, but this is still the turn.
            if let Some(record) = candidates
                .into_iter()
                .find(|record| word_coverage(query.excerpt, &record.text) >= REMAP_WORD_COVERAGE)
            {
                return found(record, "excerpt", false);
            }
        }
        Lookup::Unreachable(error) => return RecordOutcome::Unreachable(error),
        Lookup::Missing => {}
    }
    // Outside the session only a verbatim excerpt is accepted: a loose match there could
    // attach the dream to an unrelated conversation.
    match source.search(machine, &search_query, None, query.source) {
        Lookup::Found(records) => {
            if let Some(record) = records
                .into_iter()
                .find(|record| record.role == query.role && contains_excerpt(record))
            {
                return found(record, "excerpt", false);
            }
            RecordOutcome::Lost
        }
        Lookup::Unreachable(error) => RecordOutcome::Unreachable(error),
        Lookup::Missing => RecordOutcome::Lost,
    }
}

fn words(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|word| word.chars().count() >= 3)
        .map(str::to_lowercase)
}

fn excerpt_query(excerpt: &str) -> Option<String> {
    let query = words(excerpt)
        .take(EXCERPT_QUERY_WORDS)
        .collect::<Vec<_>>()
        .join(" ");
    (!query.is_empty()).then_some(query)
}

fn word_coverage(excerpt: &str, text: &str) -> f32 {
    let wanted = words(excerpt).collect::<Vec<_>>();
    if wanted.is_empty() {
        return 0.0;
    }
    let present = words(text).collect::<std::collections::HashSet<_>>();
    wanted.iter().filter(|word| present.contains(*word)).count() as f32 / wanted.len() as f32
}

#[cfg(test)]
mod tests {
    use super::super::apply::tests::{input, record};
    use super::super::apply::{ApplyContext, apply_ops, parse_response};
    use super::*;
    use crate::memory::{MemoryDocumentKind, MemoryFreshness, MemoryScope};
    use std::cell::RefCell;

    /// Records and memories as one machine currently holds them.
    #[derive(Default)]
    struct Fixture {
        records: RefCell<Vec<Record>>,
        memories: RefCell<Vec<MemoryDocument>>,
        unreachable: RefCell<bool>,
    }

    impl Fixture {
        fn guard<T>(&self, lookup: Lookup<T>) -> Lookup<T> {
            if *self.unreachable.borrow() {
                Lookup::Unreachable("ssh: connect timed out".into())
            } else {
                lookup
            }
        }

        fn some(records: Vec<Record>) -> Lookup<Vec<Record>> {
            if records.is_empty() {
                Lookup::Missing
            } else {
                Lookup::Found(records)
            }
        }
    }

    impl EvidenceSource for Fixture {
        fn record_by_rid(
            &self,
            _machine: &str,
            rid: &str,
            session_id: &str,
            _source: SourceKind,
        ) -> Lookup<Record> {
            self.guard(
                self.records
                    .borrow()
                    .iter()
                    .find(|record| {
                        record.session_id == session_id && canonical_record_id(record) == rid
                    })
                    .cloned()
                    .map_or(Lookup::Missing, Lookup::Found),
            )
        }

        fn records_by_event(
            &self,
            _machine: &str,
            event_id: &str,
            session_id: &str,
            _source: SourceKind,
        ) -> Lookup<Vec<Record>> {
            self.guard(Self::some(
                self.records
                    .borrow()
                    .iter()
                    .filter(|record| {
                        record.session_id == session_id
                            && record.links.event_id.as_deref() == Some(event_id)
                    })
                    .cloned()
                    .collect(),
            ))
        }

        fn session_records(
            &self,
            _machine: &str,
            session_id: &str,
            _source: SourceKind,
        ) -> Lookup<Vec<Record>> {
            self.guard(Self::some(
                self.records
                    .borrow()
                    .iter()
                    .filter(|record| record.session_id == session_id)
                    .cloned()
                    .collect(),
            ))
        }

        fn search(
            &self,
            _machine: &str,
            query: &str,
            session_id: Option<&str>,
            _source: SourceKind,
        ) -> Lookup<Vec<Record>> {
            let wanted = query.split(' ').collect::<Vec<_>>();
            self.guard(Self::some(
                self.records
                    .borrow()
                    .iter()
                    .filter(|record| session_id.is_none_or(|id| record.session_id == id))
                    .filter(|record| {
                        let text = record.text.to_lowercase();
                        wanted.iter().filter(|word| text.contains(**word)).count() * 2
                            >= wanted.len()
                    })
                    .cloned()
                    .collect(),
            ))
        }

        fn memory(&self, _machine: &str, memory_id: &str) -> Lookup<MemoryDocument> {
            self.guard(
                self.memories
                    .borrow()
                    .iter()
                    .find(|document| document.stable_id == memory_id)
                    .cloned()
                    .map_or(Lookup::Missing, Lookup::Found),
            )
        }
    }

    const CITED_TEXT: &str = "No, always use bun in this repository, never npm or yarn.";

    fn cited_dream(evidence: &str) -> (Vec<Dream>, Record) {
        let cited = record(1, "user", CITED_TEXT);
        let input = input(vec![cited.clone()]);
        let kind = if evidence.contains("m1") {
            "contradiction"
        } else {
            "preference"
        };
        let dreams = apply_ops(
            &[],
            parse_response(&format!(
                r#"{{"ops":[{{"op":"add","kind":"{kind}","text":"Use bun.","evidence":{evidence}}}]}}"#
            ))
            .unwrap(),
            &ApplyContext {
                input: &input,
                redactor: None,
                generation_id: "g0",
                now_ms: 1,
            },
        )
        .dreams;
        assert_eq!(dreams.len(), 1);
        (dreams, cited)
    }

    fn lint(dreams: &mut [Dream], fixture: &Fixture) -> LintReport {
        lint_dreams(dreams, fixture, None, 99).0
    }

    fn rid_of(dream: &Dream) -> (String, Option<String>) {
        match &dream.citations[0].target {
            CitationTarget::Record { rid, rid_prev, .. } => (rid.clone(), rid_prev.clone()),
            CitationTarget::Memory { .. } => panic!("record citation expected"),
        }
    }

    #[test]
    fn an_indexed_record_resolves_by_its_id_and_changes_nothing() {
        let (mut dreams, cited) = cited_dream(r#"["r1"]"#);
        let before = dreams.clone();
        let fixture = Fixture::default();
        fixture.records.borrow_mut().push(cited);
        let report = lint(&mut dreams, &fixture);
        assert_eq!((report.resolved, report.changed), (1, 0));
        assert_eq!(dreams[0].citations[0].target, before[0].citations[0].target);
        assert_eq!(dreams[0].citations[0].resolved_via.as_deref(), Some("rid"));
    }

    #[test]
    fn a_record_whose_id_changed_is_found_by_event_id_and_rewritten() {
        let (mut dreams, cited) = cited_dream(r#"["r1"]"#);
        let (old_rid, _) = rid_of(&dreams[0]);
        // A role change gives the same source event a different canonical ID.
        let mut moved = cited;
        moved.tool_name = Some("renamed".into());
        let fixture = Fixture::default();
        fixture.records.borrow_mut().push(moved.clone());
        let report = lint(&mut dreams, &fixture);
        assert_eq!((report.resolved, report.changed), (1, 1));
        assert_eq!(
            rid_of(&dreams[0]),
            (canonical_record_id(&moved), Some(old_rid))
        );
        assert_eq!(
            dreams[0].citations[0].resolved_via.as_deref(),
            Some("event_id")
        );
        // The rewritten ID makes the next lookup the cheap one.
        assert_eq!(lint(&mut dreams, &fixture).changed, 0);
        assert_eq!(dreams[0].citations[0].resolved_via.as_deref(), Some("rid"));
    }

    #[test]
    fn identical_text_in_the_session_resolves_a_record_with_no_stable_key() {
        let (mut dreams, cited) = cited_dream(r#"["r1"]"#);
        let mut reprojected = cited;
        reprojected.links.event_id = Some("evt-renumbered".into());
        reprojected.turn_id = 40;
        let fixture = Fixture::default();
        fixture.records.borrow_mut().push(reprojected);
        let report = lint(&mut dreams, &fixture);
        assert_eq!(report.resolved, 1);
        assert_eq!(
            dreams[0].citations[0].resolved_via.as_deref(),
            Some("text_hash")
        );
    }

    #[test]
    fn a_match_found_only_by_excerpt_is_remapped_not_exact() {
        let (mut dreams, cited) = cited_dream(r#"["r1"]"#);
        let mut enriched = cited;
        enriched.links.event_id = Some("evt-renumbered".into());
        enriched.text = format!("[pasted image] {CITED_TEXT}");
        let fixture = Fixture::default();
        fixture.records.borrow_mut().push(enriched);
        let report = lint(&mut dreams, &fixture);
        assert_eq!((report.remapped, report.resolved), (1, 0));
        assert_eq!(dreams[0].citations[0].state, CitationState::Remapped);
        assert!(dreams[0].citations[0].state.supports());
    }

    #[test]
    fn an_excerpt_elsewhere_must_match_verbatim_and_keep_its_role() {
        let (mut dreams, cited) = cited_dream(r#"["r1"]"#);
        let mut other_session = cited.clone();
        other_session.session_id = "s2".into();
        other_session.links.event_id = None;
        other_session.text = "always use bun in this repository maybe npm yarn".into();
        let mut wrong_role = cited;
        wrong_role.session_id = "s3".into();
        wrong_role.role = "assistant".into();
        let fixture = Fixture::default();
        fixture
            .records
            .borrow_mut()
            .extend([other_session, wrong_role]);
        assert_eq!(lint(&mut dreams, &fixture).evidence_lost, 1);
    }

    #[test]
    fn a_deleted_transcript_marks_evidence_lost_and_keeps_the_excerpt() {
        let (mut dreams, _) = cited_dream(r#"["r1"]"#);
        let report = lint(&mut dreams, &Fixture::default());
        assert_eq!(
            (report.evidence_lost, report.dreams_without_evidence),
            (1, 1)
        );
        let citation = &dreams[0].citations[0];
        assert_eq!(citation.state, CitationState::EvidenceLost);
        assert_eq!(citation.excerpt, CITED_TEXT);
        assert_eq!(dreams.len(), 1);
    }

    #[test]
    fn an_unreachable_machine_is_unverified_never_lost() {
        let (mut dreams, _) = cited_dream(r#"["r1"]"#);
        let fixture = Fixture::default();
        *fixture.unreachable.borrow_mut() = true;
        let report = lint(&mut dreams, &fixture);
        assert_eq!((report.unverified, report.evidence_lost), (1, 0));
    }

    #[test]
    fn an_edited_memory_is_supported_only_while_the_passage_remains() {
        let (mut dreams, cited) = cited_dream(r#"["r1","m1"]"#);
        let document = |version: &str, content: &str| MemoryDocument {
            provider: SourceKind::Claude,
            stable_id: "memory-claude-1".into(),
            version_sha256: version.into(),
            source_path: "/m/MEMORY.md".into(),
            scope: MemoryScope::default(),
            kind: MemoryDocumentKind::Index,
            mtime_ms: 1,
            size: 0,
            changed_ns: None,
            event_dates: Vec::new(),
            title: None,
            content: content.into(),
            sections: Vec::new(),
            refs: Vec::new(),
            freshness: MemoryFreshness::Fresh,
        };
        let fixture = Fixture::default();
        fixture.records.borrow_mut().push(cited);
        let state = |dreams: &[Dream]| dreams[0].citations[1].state;

        *fixture.memories.borrow_mut() = vec![document("v1", "Use npm for installs.")];
        lint(&mut dreams, &fixture);
        assert_eq!(state(&dreams), CitationState::Resolved);

        *fixture.memories.borrow_mut() =
            vec![document("v2", "# Notes\n\nUse npm for installs.\n\nMore.")];
        lint(&mut dreams, &fixture);
        assert_eq!(state(&dreams), CitationState::Resolved);
        let CitationTarget::Memory {
            content_version,
            content_version_prev,
            ..
        } = &dreams[0].citations[1].target
        else {
            panic!("memory citation expected");
        };
        assert_eq!(
            (content_version.as_str(), content_version_prev.as_deref()),
            ("v2", Some("v1"))
        );

        *fixture.memories.borrow_mut() = vec![document("v3", "Use bun for installs.")];
        lint(&mut dreams, &fixture);
        assert_eq!(state(&dreams), CitationState::SourceRevised);

        fixture.memories.borrow_mut().clear();
        lint(&mut dreams, &fixture);
        assert_eq!(state(&dreams), CitationState::EvidenceLost);
    }
}
