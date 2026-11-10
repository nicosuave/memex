//! Turns the executor's answer into dream changes. The answer is a proposal, not a write:
//! each operation is checked against the evidence memex actually offered, citations are
//! snapshotted from the index rather than taken from the model, and anything that cannot be
//! tied to a real record is rejected and reported.

use super::gather::{DreamInput, Evidence, MemoryEvidence, RecordEvidence, redact};
use super::model::{
    Citation, CitationState, CitationTarget, Dream, DreamKind, DreamStatus, EXCERPT_CHARS,
    RejectedOp, Retraction, RunCounts, normalize_text, text_sha256,
};
use crate::machine::LOCAL_MACHINE_ID;
use crate::redact::Redactor;
use crate::retrieval::CANONICAL_RECORD_ID_VERSION;
use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashSet;

const MAX_DREAM_CHARS: usize = 600;
const MAX_REASON_CHARS: usize = 300;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(untagged)]
enum EvidenceRef {
    Handle(String),
    Quoted {
        #[serde(rename = "ref")]
        handle: String,
        #[serde(default)]
        quote: Option<String>,
    },
}

impl EvidenceRef {
    fn parts(&self) -> (&str, Option<&str>) {
        match self {
            EvidenceRef::Handle(handle) => (handle, None),
            EvidenceRef::Quoted { handle, quote } => (handle, quote.as_deref()),
        }
    }
}

/// One proposed operation, as loosely as the model may have written it.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct ProposedOp {
    #[serde(default)]
    op: String,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    dream: Option<String>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    evidence: Vec<EvidenceRef>,
}

/// What changed, in the order it happened; the generation's diff is rendered from this.
#[derive(Clone, Debug, PartialEq)]
pub enum Change {
    Added {
        dream_id: String,
    },
    Reinforced {
        dream_id: String,
        citations: usize,
    },
    Revised {
        old_id: String,
        new_id: String,
    },
    Retracted {
        dream_id: String,
        reason: String,
    },
    CitationChanged {
        dream_id: String,
        citation: String,
        from: CitationState,
        to: CitationState,
        detail: String,
    },
}

#[derive(Clone, Debug, Default)]
pub struct ApplyOutcome {
    pub dreams: Vec<Dream>,
    pub counts: RunCounts,
    pub rejected: Vec<RejectedOp>,
    pub changes: Vec<Change>,
}

pub struct ApplyContext<'a> {
    pub input: &'a DreamInput,
    pub redactor: Option<&'a Redactor>,
    pub generation_id: &'a str,
    pub now_ms: u64,
}

/// Find the JSON object in the executor's answer. Models wrap it in prose or a code fence
/// often enough that only the outermost braces are trusted.
pub fn parse_response(answer: &str) -> Result<Vec<ProposedOp>> {
    let value = extract_json(answer)
        .ok_or_else(|| anyhow!("executor answer does not contain a JSON object"))?;
    let ops = match value {
        Value::Object(mut object) => object
            .remove("ops")
            .ok_or_else(|| anyhow!("executor answer has no \"ops\" array"))?,
        other => other,
    };
    let Value::Array(items) = ops else {
        return Err(anyhow!("executor \"ops\" is not an array"));
    };
    items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            serde_json::from_value::<ProposedOp>(item)
                .with_context(|| format!("executor op {index} is malformed"))
        })
        .collect()
}

fn extract_json(answer: &str) -> Option<Value> {
    let trimmed = answer.trim();
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return Some(value);
    }
    let start = trimmed.find('{')?;
    let end = trimmed.rfind('}')?;
    (start < end)
        .then(|| serde_json::from_str::<Value>(&trimmed[start..=end]).ok())
        .flatten()
}

pub fn apply_ops(
    previous: &[Dream],
    ops: Vec<ProposedOp>,
    context: &ApplyContext<'_>,
) -> ApplyOutcome {
    let mut outcome = ApplyOutcome {
        dreams: previous.to_vec(),
        ..ApplyOutcome::default()
    };
    for (index, op) in ops.into_iter().enumerate() {
        let label = op.op.trim().to_ascii_lowercase();
        let text = op.text.as_deref().map(normalize_text).unwrap_or_default();
        if let Err(reason) = apply_op(&mut outcome, &label, &op, index, context) {
            outcome.counts.rejected += 1;
            outcome.rejected.push(RejectedOp {
                op: if label.is_empty() {
                    "(missing)".to_string()
                } else {
                    label
                },
                reason,
                text: clip(&text, MAX_DREAM_CHARS),
            });
        }
    }
    outcome
}

fn apply_op(
    outcome: &mut ApplyOutcome,
    label: &str,
    op: &ProposedOp,
    index: usize,
    context: &ApplyContext<'_>,
) -> Result<(), String> {
    let citations = build_citations(&op.evidence, context);
    match label {
        "add" => {
            let (kind, text) = dream_body(op)?;
            require_evidence(&citations, kind)?;
            let project = clean_project(op.project.as_deref());
            if let Some(existing) = find_duplicate(&outcome.dreams, &text, project.as_deref()) {
                // The same statement again is more evidence for it, not a second dream.
                return reinforce(outcome, existing, citations, context);
            }
            let id = new_dream_id(context.generation_id, index, &text);
            outcome.dreams.push(Dream {
                id: id.clone(),
                kind,
                text,
                project,
                status: DreamStatus::Active,
                created_generation: context.generation_id.to_string(),
                created_at_ms: context.now_ms,
                updated_generation: context.generation_id.to_string(),
                updated_at_ms: context.now_ms,
                reinforced: 0,
                supersedes: None,
                superseded_by: None,
                retraction: None,
                citations,
            });
            outcome.counts.added += 1;
            outcome.changes.push(Change::Added { dream_id: id });
            Ok(())
        }
        "reinforce" => {
            let target = target_dream(outcome, op, context)?;
            require_record(&citations)?;
            reinforce(outcome, target, citations, context)
        }
        "revise" => {
            let target = target_dream(outcome, op, context)?;
            let old = outcome.dreams[target].clone();
            let (kind, text) = dream_body(&ProposedOp {
                kind: op.kind.clone().or(Some(old.kind.label().to_string())),
                ..op.clone()
            })?;
            require_evidence(&citations, kind)?;
            if normalize_key(&text) == normalize_key(&old.text) {
                return reinforce(outcome, target, citations, context);
            }
            let id = new_dream_id(context.generation_id, index, &text);
            outcome.dreams[target].status = DreamStatus::Superseded;
            outcome.dreams[target].superseded_by = Some(id.clone());
            outcome.dreams[target].updated_generation = context.generation_id.to_string();
            outcome.dreams[target].updated_at_ms = context.now_ms;
            outcome.dreams.push(Dream {
                id: id.clone(),
                kind,
                text,
                project: match op.project.as_deref() {
                    Some(project) => clean_project(Some(project)),
                    None => old.project.clone(),
                },
                status: DreamStatus::Active,
                created_generation: context.generation_id.to_string(),
                created_at_ms: context.now_ms,
                updated_generation: context.generation_id.to_string(),
                updated_at_ms: context.now_ms,
                reinforced: 0,
                supersedes: Some(old.id.clone()),
                superseded_by: None,
                retraction: None,
                citations,
            });
            outcome.counts.revised += 1;
            outcome.changes.push(Change::Revised {
                old_id: old.id,
                new_id: id,
            });
            Ok(())
        }
        "retract" => {
            let target = target_dream(outcome, op, context)?;
            require_record(&citations)?;
            let reason = clip(
                &op.reason.as_deref().map(normalize_text).unwrap_or_default(),
                MAX_REASON_CHARS,
            );
            if reason.is_empty() {
                return Err("retract needs a reason".to_string());
            }
            let dream = &mut outcome.dreams[target];
            dream.status = DreamStatus::Retracted;
            dream.updated_generation = context.generation_id.to_string();
            dream.updated_at_ms = context.now_ms;
            dream.retraction = Some(Retraction {
                reason: reason.clone(),
                generation: context.generation_id.to_string(),
                citations,
            });
            outcome.counts.retracted += 1;
            outcome.changes.push(Change::Retracted {
                dream_id: dream.id.clone(),
                reason,
            });
            Ok(())
        }
        other => Err(format!("unknown op {other:?}")),
    }
}

fn dream_body(op: &ProposedOp) -> Result<(DreamKind, String), String> {
    let kind = op
        .kind
        .as_deref()
        .and_then(DreamKind::parse)
        .ok_or_else(|| format!("unknown dream kind {:?}", op.kind.as_deref().unwrap_or("")))?;
    let text = clip(
        &op.text.as_deref().map(normalize_text).unwrap_or_default(),
        MAX_DREAM_CHARS,
    );
    if text.is_empty() {
        return Err("dream text is empty".to_string());
    }
    Ok((kind, text))
}

/// A dream must rest on something the developer or an agent actually said in a session.
/// A saved memory alone is not new ground truth, with one exception: two or more sources
/// that disagree are themselves the finding.
fn require_evidence(citations: &[Citation], kind: DreamKind) -> Result<(), String> {
    if citations.iter().any(Citation::is_record)
        || (kind == DreamKind::Contradiction && citations.len() >= 2)
    {
        return Ok(());
    }
    Err(if citations.is_empty() {
        "no evidence handle resolved to an offered record or memory".to_string()
    } else {
        "evidence cites saved memories only; at least one conversation record is required"
            .to_string()
    })
}

fn require_record(citations: &[Citation]) -> Result<(), String> {
    if citations.iter().any(Citation::is_record) {
        Ok(())
    } else {
        Err("at least one conversation record is required as evidence".to_string())
    }
}

fn target_dream(
    outcome: &ApplyOutcome,
    op: &ProposedOp,
    context: &ApplyContext<'_>,
) -> Result<usize, String> {
    let handle = op
        .dream
        .as_deref()
        .ok_or_else(|| "op names no existing dream".to_string())?;
    let dream_id = context
        .input
        .dream_id(handle)
        .ok_or_else(|| format!("dream {handle:?} was not offered to this run"))?;
    let position = outcome
        .dreams
        .iter()
        .position(|dream| dream.id == dream_id)
        .ok_or_else(|| format!("dream {handle:?} does not exist"))?;
    if outcome.dreams[position].status != DreamStatus::Active {
        return Err(format!("dream {handle:?} is no longer active"));
    }
    Ok(position)
}

fn reinforce(
    outcome: &mut ApplyOutcome,
    target: usize,
    citations: Vec<Citation>,
    context: &ApplyContext<'_>,
) -> Result<(), String> {
    let dream = &mut outcome.dreams[target];
    let known = dream
        .citations
        .iter()
        .map(Citation::identity)
        .collect::<HashSet<_>>();
    let fresh = citations
        .into_iter()
        .filter(|citation| !known.contains(&citation.identity()))
        .collect::<Vec<_>>();
    // Re-citing a source the dream already has is not reinforcement: nothing new was seen.
    if !fresh.iter().any(Citation::is_record) {
        return Err("no new conversation record beyond what the dream already cites".to_string());
    }
    let added = fresh.len();
    dream.citations.extend(fresh);
    dream.reinforced += 1;
    dream.updated_generation = context.generation_id.to_string();
    dream.updated_at_ms = context.now_ms;
    outcome.counts.reinforced += 1;
    outcome.changes.push(Change::Reinforced {
        dream_id: dream.id.clone(),
        citations: added,
    });
    Ok(())
}

fn find_duplicate(dreams: &[Dream], text: &str, project: Option<&str>) -> Option<usize> {
    let key = normalize_key(text);
    dreams.iter().position(|dream| {
        dream.status == DreamStatus::Active
            && dream.project.as_deref() == project
            && normalize_key(&dream.text) == key
    })
}

fn normalize_key(text: &str) -> String {
    normalize_text(text)
        .to_lowercase()
        .trim_end_matches('.')
        .to_string()
}

fn clean_project(project: Option<&str>) -> Option<String> {
    let project = project?.trim();
    (!project.is_empty()
        && !matches!(
            project.to_ascii_lowercase().as_str(),
            "null" | "none" | "all projects" | "no project"
        ))
    .then(|| project.to_string())
}

fn clip(text: &str, max_chars: usize) -> String {
    text.chars().take(max_chars).collect()
}

fn new_dream_id(generation_id: &str, index: usize, text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(generation_id.as_bytes());
    hasher.update([0]);
    hasher.update((index as u64).to_le_bytes());
    hasher.update(text.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    format!("drm_{}", &digest[..16])
}

/// Map evidence handles to citations. Unknown handles are dropped here, so an op that
/// cited only invented handles arrives at the evidence rules with nothing.
fn build_citations(evidence: &[EvidenceRef], context: &ApplyContext<'_>) -> Vec<Citation> {
    let mut seen = HashSet::new();
    let mut citations = Vec::new();
    for reference in evidence {
        let (handle, quote) = reference.parts();
        let citation = match context.input.evidence(handle) {
            Some(Evidence::Record(record)) => record_citation(record, quote, context),
            Some(Evidence::Memory(memory)) => memory_citation(memory, quote, context),
            None => continue,
        };
        if seen.insert(citation.identity()) {
            citations.push(citation);
        }
    }
    citations
}

fn record_citation(
    evidence: &RecordEvidence,
    quote: Option<&str>,
    context: &ApplyContext<'_>,
) -> Citation {
    let record = &evidence.record;
    Citation {
        machine: evidence.machine.clone(),
        target: CitationTarget::Record {
            rid: evidence.rid.clone(),
            rid_version: CANONICAL_RECORD_ID_VERSION.to_string(),
            rid_prev: None,
            source: record.source,
            session_id: record.session_id.clone(),
            source_path: record.source_path.clone(),
            event_id: record.links.event_id.clone(),
            role: record.role.clone(),
        },
        ts: record.ts,
        text_sha256: text_sha256(&record.text),
        excerpt: excerpt(&redact(context.redactor, &record.text), quote),
        state: CitationState::Resolved,
        resolved_via: Some("rid".to_string()),
        checked_at_ms: Some(context.now_ms),
    }
}

fn memory_citation(
    evidence: &MemoryEvidence,
    quote: Option<&str>,
    context: &ApplyContext<'_>,
) -> Citation {
    Citation {
        machine: LOCAL_MACHINE_ID.to_string(),
        target: CitationTarget::Memory {
            memory_id: evidence.memory_id.clone(),
            content_version: evidence.content_version.clone(),
            content_version_prev: None,
            section_id: evidence.section_id.clone(),
            source: evidence.source,
            source_path: evidence.source_path.to_string_lossy().to_string(),
        },
        ts: evidence.mtime_ms,
        text_sha256: text_sha256(&evidence.text),
        excerpt: excerpt(&redact(context.redactor, &evidence.text), quote),
        state: CitationState::Resolved,
        resolved_via: Some("content_version".to_string()),
        checked_at_ms: Some(context.now_ms),
    }
}

/// A verbatim slice of the normalized source text: around the model's quote when the quote
/// really occurs in the source, otherwise the opening. The slice is always cut from the
/// source, never copied from the model, so an excerpt cannot say what the source did not.
pub fn excerpt(source_text: &str, quote: Option<&str>) -> String {
    let normalized = normalize_text(source_text);
    let start = quote
        .map(normalize_text)
        .filter(|quote| !quote.is_empty())
        .and_then(|quote| find_ignore_case(&normalized, &quote))
        .unwrap_or(0);
    normalized[start..].chars().take(EXCERPT_CHARS).collect()
}

/// Byte offset of `needle` in `haystack`, ignoring ASCII case, on a char boundary.
fn find_ignore_case(haystack: &str, needle: &str) -> Option<usize> {
    let haystack_lower = haystack.to_ascii_lowercase();
    let needle_lower = needle.to_ascii_lowercase();
    haystack_lower.find(&needle_lower)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::super::gather::{OfferedDream, SessionInput};
    use super::*;
    use crate::retrieval::canonical_record_id;
    use crate::types::{Record, RecordLinks, SourceKind};

    pub(crate) fn record(turn: u32, role: &str, text: &str) -> Record {
        Record {
            source: SourceKind::Claude,
            doc_id: turn as u64,
            ts: 1_000 + turn as u64,
            project: "memex".into(),
            session_id: "s1".into(),
            turn_id: turn,
            role: role.into(),
            text: text.into(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks {
                event_id: Some(format!("evt-{turn}")),
                ..RecordLinks::default()
            },
            source_path: "/t/s1.jsonl".into(),
        }
    }

    pub(crate) fn input(records: Vec<Record>) -> DreamInput {
        DreamInput {
            sessions: vec![SessionInput {
                machine: LOCAL_MACHINE_ID.into(),
                source: SourceKind::Claude,
                session_id: "s1".into(),
                project: "memex".into(),
                last_at: 2_000,
                records: records
                    .into_iter()
                    .enumerate()
                    .map(|(index, record)| RecordEvidence {
                        handle: format!("r{}", index + 1),
                        machine: LOCAL_MACHINE_ID.into(),
                        rid: canonical_record_id(&record),
                        shown: record.text.clone(),
                        record,
                    })
                    .collect(),
                omitted: 0,
            }],
            memories: vec![MemoryEvidence {
                handle: "m1".into(),
                memory_id: "memory-claude-1".into(),
                content_version: "v1".into(),
                section_id: "sec".into(),
                heading: None,
                title: None,
                source: SourceKind::Claude,
                source_path: "/m/MEMORY.md".into(),
                project: Some("memex".into()),
                mtime_ms: 500,
                stale: false,
                text: "Use npm for installs.".into(),
                shown: "Use npm for installs.".into(),
            }],
            ..DreamInput::default()
        }
    }

    fn apply(
        previous: &[Dream],
        input: &DreamInput,
        generation: &str,
        answer: &str,
    ) -> ApplyOutcome {
        apply_ops(
            previous,
            parse_response(answer).unwrap(),
            &ApplyContext {
                input,
                redactor: None,
                generation_id: generation,
                now_ms: 5_000,
            },
        )
    }

    fn offer(input: &mut DreamInput, dreams: &[Dream]) {
        input.dreams = dreams
            .iter()
            .filter(|dream| dream.status == DreamStatus::Active)
            .enumerate()
            .map(|(index, dream)| OfferedDream {
                handle: format!("d{}", index + 1),
                dream_id: dream.id.clone(),
            })
            .collect();
    }

    #[test]
    fn the_answer_is_found_inside_prose_and_code_fences() {
        let ops = parse_response(
            "Here you go:\n```json\n{\"ops\":[{\"op\":\"add\",\"kind\":\"preference\",\"text\":\"t\",\"evidence\":[\"r1\",{\"ref\":\"r2\",\"quote\":\"q\"}]}]}\n```\n",
        )
        .unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].evidence[1].parts(), ("r2", Some("q")));
        assert!(parse_response("no json here").is_err());
        assert!(parse_response("{\"dreams\":[]}").is_err());
    }

    #[test]
    fn a_dream_is_written_only_with_evidence_memex_offered() {
        let input = input(vec![
            record(1, "user", "No, always use bun here, never npm."),
            record(2, "assistant", "Switching to bun."),
        ]);
        let outcome = apply(
            &[],
            &input,
            "g1",
            r#"{"ops":[
              {"op":"add","kind":"preference","text":"Use bun, not npm.","project":"memex",
               "evidence":[{"ref":"r1","quote":"always use bun"}]},
              {"op":"add","kind":"preference","text":"Invented handle.","evidence":["r99"]},
              {"op":"add","kind":"preference","text":"Memory only.","evidence":["m1"]},
              {"op":"add","kind":"preference","text":"No evidence."},
              {"op":"add","kind":"vibes","text":"Unknown kind.","evidence":["r1"]},
              {"op":"explode","text":"Unknown op.","evidence":["r1"]}
            ]}"#,
        );
        assert_eq!(outcome.dreams.len(), 1);
        assert_eq!(outcome.counts.added, 1);
        assert_eq!(outcome.counts.rejected, 5);
        let dream = &outcome.dreams[0];
        assert_eq!(dream.project.as_deref(), Some("memex"));
        let citation = &dream.citations[0];
        assert_eq!(citation.excerpt, "always use bun here, never npm.");
        assert_eq!(
            citation.text_sha256,
            text_sha256("No, always use bun here, never npm.")
        );
        let CitationTarget::Record { rid, event_id, .. } = &citation.target else {
            panic!("record citation expected");
        };
        assert!(rid.starts_with("rid1_"));
        assert_eq!(event_id.as_deref(), Some("evt-1"));
        assert!(outcome.rejected[1].reason.contains("saved memories only"));
    }

    #[test]
    fn a_contradiction_may_rest_on_two_sources_without_a_record() {
        let mut input = input(vec![record(1, "user", "we use bun")]);
        input.memories.push(MemoryEvidence {
            handle: "m2".into(),
            memory_id: "memory-codex-2".into(),
            section_id: "sec".into(),
            text: "Use bun for installs.".into(),
            ..input.memories[0].clone()
        });
        let outcome = apply(
            &[],
            &input,
            "g1",
            r#"{"ops":[
              {"op":"add","kind":"contradiction","text":"Claude memory says npm; Codex memory says bun.","evidence":["m1","m2"]},
              {"op":"add","kind":"contradiction","text":"One memory is not a contradiction.","evidence":["m1"]}
            ]}"#,
        );
        assert_eq!(outcome.counts.added, 1);
        assert_eq!(outcome.counts.rejected, 1);
    }

    #[test]
    fn a_quote_the_source_does_not_contain_falls_back_to_the_source_itself() {
        assert_eq!(
            excerpt("alpha  beta\ngamma", Some("BETA gamma")),
            "beta gamma"
        );
        assert_eq!(excerpt("alpha beta", Some("never said this")), "alpha beta");
        assert_eq!(
            excerpt(&"é".repeat(500), None).chars().count(),
            EXCERPT_CHARS
        );
    }

    #[test]
    fn omitted_dreams_survive_any_number_of_dreams() {
        let seed_input = input(vec![record(1, "user", "Always use bun.")]);
        let mut dreams = apply(
            &[],
            &seed_input,
            "g0",
            r#"{"ops":[{"op":"add","kind":"preference","text":"Use bun.","evidence":["r1"]}]}"#,
        )
        .dreams;
        let seeded = dreams[0].clone();
        for round in 1..=10 {
            // Each later run floods near-duplicates and never mentions the seeded dream.
            let mut input = input(vec![record(
                round + 1,
                "assistant",
                &format!("npm is fine, round {round}"),
            )]);
            offer(&mut input, &dreams);
            dreams = apply(
                &dreams,
                &input,
                &format!("g{round}"),
                &format!(
                    r#"{{"ops":[{{"op":"add","kind":"preference","text":"Use npm, round {round}.","evidence":["r1"]}}]}}"#
                ),
            )
            .dreams;
        }
        assert_eq!(dreams.len(), 11);
        assert_eq!(dreams[0], seeded);
    }

    #[test]
    fn reinforcing_needs_a_record_the_dream_does_not_already_cite() {
        let first = input(vec![record(1, "user", "Always use bun.")]);
        let dreams = apply(
            &[],
            &first,
            "g0",
            r#"{"ops":[{"op":"add","kind":"preference","text":"Use bun.","evidence":["r1"]}]}"#,
        )
        .dreams;

        let mut same = input(vec![record(1, "user", "Always use bun.")]);
        offer(&mut same, &dreams);
        let repeated = apply(
            &dreams,
            &same,
            "g1",
            r#"{"ops":[{"op":"reinforce","dream":"d1","evidence":["r1"]},
                       {"op":"reinforce","dream":"d1","evidence":["m1"]},
                       {"op":"reinforce","dream":"d9","evidence":["r1"]}]}"#,
        );
        assert_eq!(repeated.counts.reinforced, 0);
        assert_eq!(repeated.counts.rejected, 3);
        assert_eq!(repeated.dreams, dreams);

        let mut later = input(vec![record(7, "user", "bun again please")]);
        offer(&mut later, &dreams);
        let reinforced = apply(
            &dreams,
            &later,
            "g2",
            r#"{"ops":[{"op":"add","kind":"preference","text":"use bun","evidence":["r1"]}]}"#,
        );
        assert_eq!(reinforced.counts.added, 0);
        assert_eq!(reinforced.counts.reinforced, 1);
        assert_eq!(reinforced.dreams.len(), 1);
        assert_eq!(reinforced.dreams[0].citations.len(), 2);
        assert_eq!(reinforced.dreams[0].reinforced, 1);
    }

    #[test]
    fn revising_and_retracting_keep_the_old_dream_on_record() {
        let first = input(vec![record(1, "user", "Use tabs.")]);
        let dreams = apply(
            &[],
            &first,
            "g0",
            r#"{"ops":[
              {"op":"add","kind":"preference","text":"Prefer tabs.","evidence":["r1"]},
              {"op":"add","kind":"project_fact","text":"CI runs on Jenkins.","project":"memex","evidence":["r1"]}
            ]}"#,
        )
        .dreams;

        let mut next = input(vec![record(
            9,
            "user",
            "I no longer prefer tabs; CI moved off Jenkins.",
        )]);
        offer(&mut next, &dreams);
        let outcome = apply(
            &dreams,
            &next,
            "g1",
            r#"{"ops":[
              {"op":"revise","dream":"d1","text":"No longer prefers tabs (as of 1970-01-01).","evidence":["r1"]},
              {"op":"retract","dream":"d2","reason":"CI moved off Jenkins.","evidence":["r1"]},
              {"op":"retract","dream":"d1","reason":"Already superseded.","evidence":["r1"]}
            ]}"#,
        );
        assert_eq!(outcome.counts.revised, 1);
        assert_eq!(outcome.counts.retracted, 1);
        assert_eq!(outcome.counts.rejected, 1);
        assert_eq!(outcome.dreams.len(), 3);
        let old = &outcome.dreams[0];
        let replacement = &outcome.dreams[2];
        assert_eq!(old.status, DreamStatus::Superseded);
        assert_eq!(old.text, "Prefer tabs.");
        assert_eq!(old.superseded_by.as_deref(), Some(replacement.id.as_str()));
        assert_eq!(replacement.supersedes.as_deref(), Some(old.id.as_str()));
        assert_eq!(replacement.kind, DreamKind::Preference);
        let retracted = &outcome.dreams[1];
        assert_eq!(retracted.status, DreamStatus::Retracted);
        assert_eq!(retracted.text, "CI runs on Jenkins.");
        assert_eq!(retracted.retraction.as_ref().unwrap().citations.len(), 1);
    }
}
