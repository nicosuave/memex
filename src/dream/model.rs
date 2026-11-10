//! Durable shapes of the dream store: dreams, their citations, and the generation that
//! holds one consistent set of them.

use crate::types::SourceKind;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const DREAM_SCHEMA_VERSION: u32 = 1;

/// Characters of source text kept inline with a citation, so a dream still shows its
/// evidence after the transcript it came from is deleted.
pub const EXCERPT_CHARS: usize = 240;

/// Bytes of normalized text covered by a citation hash; the same cap vector transfer uses.
const HASHED_TEXT_BYTES: usize = 8192;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DreamKind {
    Preference,
    Correction,
    RepeatedMistake,
    Contradiction,
    ProjectFact,
    Lesson,
}

impl DreamKind {
    pub const ALL: [DreamKind; 6] = [
        DreamKind::Preference,
        DreamKind::Correction,
        DreamKind::RepeatedMistake,
        DreamKind::Contradiction,
        DreamKind::ProjectFact,
        DreamKind::Lesson,
    ];

    pub fn label(self) -> &'static str {
        match self {
            DreamKind::Preference => "preference",
            DreamKind::Correction => "correction",
            DreamKind::RepeatedMistake => "repeated_mistake",
            DreamKind::Contradiction => "contradiction",
            DreamKind::ProjectFact => "project_fact",
            DreamKind::Lesson => "lesson",
        }
    }

    pub fn heading(self) -> &'static str {
        match self {
            DreamKind::Preference => "Preferences",
            DreamKind::Correction => "Corrections",
            DreamKind::RepeatedMistake => "Repeated mistakes",
            DreamKind::Contradiction => "Contradictions",
            DreamKind::ProjectFact => "Project facts",
            DreamKind::Lesson => "Lessons",
        }
    }

    pub fn parse(label: &str) -> Option<Self> {
        let label = label.trim().to_ascii_lowercase().replace(['-', ' '], "_");
        Self::ALL.into_iter().find(|kind| kind.label() == label)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DreamStatus {
    Active,
    /// Replaced by the dream named in `superseded_by`.
    Superseded,
    /// Withdrawn on cited evidence; kept so the withdrawal stays auditable.
    Retracted,
}

impl DreamStatus {
    pub fn label(self) -> &'static str {
        match self {
            DreamStatus::Active => "active",
            DreamStatus::Superseded => "superseded",
            DreamStatus::Retracted => "retracted",
        }
    }
}

/// Outcome of the last attempt to find a citation's source again.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CitationState {
    /// Found by identity (record ID or native event ID) or by identical text in its session.
    Resolved,
    /// Found only by searching for the excerpt: plausible, not an exact match.
    Remapped,
    /// The machine that holds the source could not be asked. Not evidence of loss.
    Unverified,
    /// The source is gone. The inline excerpt is all that remains.
    EvidenceLost,
    /// The cited memory document changed and no longer contains the excerpt.
    SourceRevised,
}

impl CitationState {
    pub fn label(self) -> &'static str {
        match self {
            CitationState::Resolved => "resolved",
            CitationState::Remapped => "remapped",
            CitationState::Unverified => "unverified",
            CitationState::EvidenceLost => "evidence_lost",
            CitationState::SourceRevised => "source_revised",
        }
    }

    /// Whether the source can still be read to back the dream.
    pub fn supports(self) -> bool {
        matches!(self, CitationState::Resolved | CitationState::Remapped)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CitationTarget {
    Record {
        /// Canonical record ID, as `memex show --record-id` takes it.
        rid: String,
        /// Scheme of `rid` (`rid1`); a different current scheme means every ID needs a remap.
        rid_version: String,
        /// The ID this citation carried before a lint found the record under a new one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        rid_prev: Option<String>,
        source: SourceKind,
        session_id: String,
        source_path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_id: Option<String>,
        role: String,
    },
    Memory {
        memory_id: String,
        content_version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_version_prev: Option<String>,
        section_id: String,
        source: SourceKind,
        source_path: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Citation {
    pub machine: String,
    #[serde(flatten)]
    pub target: CitationTarget,
    /// Record timestamp, or the memory file's modification time, in Unix milliseconds.
    pub ts: u64,
    /// SHA-256 of the normalized source text; survives ID scheme changes and file moves.
    pub text_sha256: String,
    /// Redacted slice of the source text, starting at the executor's quote where it gave one.
    pub excerpt: String,
    pub state: CitationState,
    /// How the last lint found the source: `rid`, `event_id`, `text_hash`, or `excerpt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_via: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checked_at_ms: Option<u64>,
}

impl Citation {
    pub fn is_record(&self) -> bool {
        matches!(self.target, CitationTarget::Record { .. })
    }

    /// The agent whose transcript or memory file is cited.
    pub fn source(&self) -> SourceKind {
        match &self.target {
            CitationTarget::Record { source, .. } | CitationTarget::Memory { source, .. } => {
                *source
            }
        }
    }

    /// Identity used to keep one citation per source within a dream.
    pub fn identity(&self) -> String {
        match &self.target {
            CitationTarget::Record { rid, .. } => format!("{}\0record\0{rid}", self.machine),
            CitationTarget::Memory {
                memory_id,
                section_id,
                ..
            } => format!("{}\0memory\0{memory_id}\0{section_id}", self.machine),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Retraction {
    pub reason: String,
    pub generation: String,
    pub citations: Vec<Citation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Dream {
    pub id: String,
    pub kind: DreamKind,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub status: DreamStatus,
    /// Generation that first wrote this dream.
    pub created_generation: String,
    pub created_at_ms: u64,
    /// Generation that last added evidence or changed status.
    pub updated_generation: String,
    pub updated_at_ms: u64,
    /// Runs, after the first, that added new record evidence.
    #[serde(default)]
    pub reinforced: u32,
    /// The dream this one replaced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retraction: Option<Retraction>,
    pub citations: Vec<Citation>,
}

/// How much of a dream's evidence can still be read.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceHealth {
    /// Every citation still resolves.
    Supported,
    /// Some citations resolve.
    Partial,
    /// No citation resolves, and at least one could not be checked.
    Unverified,
    /// No citation resolves; only the inline excerpts remain.
    Lost,
}

impl EvidenceHealth {
    pub fn label(self) -> &'static str {
        match self {
            EvidenceHealth::Supported => "supported",
            EvidenceHealth::Partial => "partial",
            EvidenceHealth::Unverified => "unverified",
            EvidenceHealth::Lost => "lost",
        }
    }
}

impl Dream {
    pub fn evidence_health(&self) -> EvidenceHealth {
        let supporting = self
            .citations
            .iter()
            .filter(|citation| citation.state.supports())
            .count();
        if supporting == self.citations.len() && supporting > 0 {
            EvidenceHealth::Supported
        } else if supporting > 0 {
            EvidenceHealth::Partial
        } else if self
            .citations
            .iter()
            .any(|citation| citation.state == CitationState::Unverified)
        {
            EvidenceHealth::Unverified
        } else {
            EvidenceHealth::Lost
        }
    }

    pub fn last_evidence_ts(&self) -> u64 {
        self.citations
            .iter()
            .map(|citation| citation.ts)
            .max()
            .unwrap_or(self.created_at_ms)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationKind {
    /// Produced by an executor run over new sessions.
    Run,
    /// Produced by re-resolving citations; no dream text changed.
    Lint,
}

/// An operation the executor proposed that memex refused to apply.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RejectedOp {
    pub op: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunCounts {
    pub added: usize,
    pub reinforced: usize,
    pub revised: usize,
    pub retracted: usize,
    pub rejected: usize,
    /// Citations whose state or identity a lint changed.
    #[serde(default)]
    pub citations_changed: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunMeta {
    pub started_at_ms: u64,
    pub finished_at_ms: u64,
    pub memex_version: String,
    /// Canonical record ID scheme in force; a later mismatch means citations need a remap.
    pub rid_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Records at or before this time were already offered to an earlier dream run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since_ms: Option<u64>,
    #[serde(default)]
    pub machines: Vec<String>,
    #[serde(default)]
    pub sessions: usize,
    /// New sessions left out by the session or prompt budget.
    #[serde(default)]
    pub sessions_skipped: usize,
    #[serde(default)]
    pub records: usize,
    #[serde(default)]
    pub memory_sections: usize,
    #[serde(default)]
    pub counts: RunCounts,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rejected: Vec<RejectedOp>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<String>,
}

/// One immutable published version of the dream store.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Generation {
    pub schema_version: u32,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub kind: GenerationKind,
    pub created_at_ms: u64,
    /// Activity up to this time has been read; the next dream run starts after it.
    pub watermark_ms: u64,
    pub run: RunMeta,
    pub dreams: Vec<Dream>,
}

impl Generation {
    pub fn dream(&self, id: &str) -> Option<&Dream> {
        self.dreams.iter().find(|dream| dream.id == id)
    }

    pub fn active(&self) -> impl Iterator<Item = &Dream> {
        self.dreams
            .iter()
            .filter(|dream| dream.status == DreamStatus::Active)
    }
}

/// Collapse whitespace and trim, so cosmetic projection changes keep the same hash.
pub fn normalize_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Hash of the normalized text, capped so a huge record costs a bounded amount to compare.
pub fn text_sha256(text: &str) -> String {
    let normalized = normalize_text(text);
    let mut end = normalized.len().min(HASHED_TEXT_BYTES);
    while !normalized.is_char_boundary(end) {
        end -= 1;
    }
    format!("{:x}", Sha256::digest(&normalized.as_bytes()[..end]))
}

/// Up to `max_chars` characters of normalized text, with an ellipsis when cut.
pub fn clip_chars(text: &str, max_chars: usize) -> String {
    let normalized = normalize_text(text);
    if normalized.chars().count() <= max_chars {
        return normalized;
    }
    let mut clipped = normalized.chars().take(max_chars).collect::<String>();
    clipped.push('…');
    clipped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_hash_ignores_whitespace_and_caps_at_a_char_boundary() {
        assert_eq!(
            text_sha256("use  bun\n not npm"),
            text_sha256("use bun not npm")
        );
        assert_ne!(text_sha256("use bun"), text_sha256("use npm"));
        let long = "é".repeat(HASHED_TEXT_BYTES);
        assert_eq!(text_sha256(&long), text_sha256(&format!("{long}tail")));
    }

    #[test]
    fn evidence_health_separates_unverified_from_lost() {
        let citation = |state| Citation {
            machine: "local".into(),
            target: CitationTarget::Record {
                rid: "rid1_a".into(),
                rid_version: "rid1".into(),
                rid_prev: None,
                source: SourceKind::Claude,
                session_id: "s".into(),
                source_path: "/p".into(),
                event_id: None,
                role: "user".into(),
            },
            ts: 1,
            text_sha256: String::new(),
            excerpt: String::new(),
            state,
            resolved_via: None,
            checked_at_ms: None,
        };
        let dream = |states: &[CitationState]| Dream {
            id: "drm_1".into(),
            kind: DreamKind::Preference,
            text: "t".into(),
            project: None,
            status: DreamStatus::Active,
            created_generation: "g".into(),
            created_at_ms: 0,
            updated_generation: "g".into(),
            updated_at_ms: 0,
            reinforced: 0,
            supersedes: None,
            superseded_by: None,
            retraction: None,
            citations: states.iter().copied().map(citation).collect(),
        };
        use CitationState::*;
        assert_eq!(
            dream(&[Resolved, Remapped]).evidence_health(),
            EvidenceHealth::Supported
        );
        assert_eq!(
            dream(&[Resolved, EvidenceLost]).evidence_health(),
            EvidenceHealth::Partial
        );
        assert_eq!(
            dream(&[Unverified, EvidenceLost]).evidence_health(),
            EvidenceHealth::Unverified
        );
        assert_eq!(
            dream(&[EvidenceLost, SourceRevised]).evidence_health(),
            EvidenceHealth::Lost
        );
    }

    #[test]
    fn citations_round_trip_with_a_flattened_target() {
        let citation = Citation {
            machine: "mini".into(),
            target: CitationTarget::Memory {
                memory_id: "memory-claude-1".into(),
                content_version: "v1".into(),
                content_version_prev: None,
                section_id: "s1".into(),
                source: SourceKind::Claude,
                source_path: "/m.md".into(),
            },
            ts: 5,
            text_sha256: "h".into(),
            excerpt: "use npm".into(),
            state: CitationState::SourceRevised,
            resolved_via: None,
            checked_at_ms: Some(9),
        };
        let json = serde_json::to_value(&citation).unwrap();
        assert_eq!(json["kind"], "memory");
        assert_eq!(json["state"], "source_revised");
        assert_eq!(serde_json::from_value::<Citation>(json).unwrap(), citation);
    }
}
