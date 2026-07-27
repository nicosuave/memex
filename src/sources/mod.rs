//! Source-owned transcript discovery, identity, and projection adapters.
//!
//! Indexing and usage reconstruction deliberately remain independent projections.  The
//! adapters in this module make them share source identity, discovery, hierarchy,
//! parser-version rules, and the relation facts persisted by the canonical catalog.

pub mod audit;
pub mod claude;
pub mod codex;
pub mod common;
pub mod copilot;
pub mod cursor;
pub mod openclaw;
pub mod opencode;
pub mod pi;

use crate::state::{ParserStreamState, PendingToolCall};
use crate::types::SourceKind;
use crate::usage::UsageEvent;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationKind {
    Main,
    Subagent,
    Sidechain,
    Fork,
    Branch,
    Compaction,
}

impl ConversationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::Subagent => "subagent",
            Self::Sidechain => "sidechain",
            Self::Fork => "fork",
            Self::Branch => "branch",
            Self::Compaction => "compaction",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceFile {
    pub source: SourceKind,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionIdentity {
    pub source: SourceKind,
    pub session_id: String,
    pub parent_session_id: Option<String>,
    pub conversation_kind: ConversationKind,
    pub source_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceMetadata {
    pub session: SessionIdentity,
    pub cwd: Option<PathBuf>,
    pub project: Option<String>,
    pub git_branch: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParserVersions {
    /// Rules shared by both projections: discovery, logical session, hierarchy, and project.
    pub identity: u32,
    /// Byte-offset/Tantivy record projection.
    pub index: u32,
    /// Request/token projection.
    pub usage: i64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct IndexParseState {
    pub offset: u64,
    pub turn_id: u32,
    pub pending_tool_calls: std::collections::HashMap<String, PendingToolCall>,
    pub parser_stream: ParserStreamState,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexParseOutput {
    pub offset: u64,
    pub turn_id: u32,
    pub pending_tool_calls: std::collections::HashMap<String, PendingToolCall>,
    pub parser_stream: ParserStreamState,
    pub session_id: Option<String>,
    pub diagnostics: ParseDiagnostics,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ParseDiagnostics {
    pub malformed_json_lines: u64,
    pub non_object_json_lines: u64,
    pub unknown_top_level_types: HashMap<String, u64>,
    pub unknown_semantic_types: HashMap<String, u64>,
    pub orphan_tool_results: u64,
    pub duplicate_tool_calls: u64,
    pub encrypted_reasoning_dropped: u64,
    pub truncated_tool_inputs: u64,
    pub truncated_tool_outputs: u64,
}

impl ParseDiagnostics {
    pub fn increment_unknown_top_level(&mut self, value: &str) {
        if !value.is_empty() {
            *self
                .unknown_top_level_types
                .entry(value.to_string())
                .or_default() += 1;
        }
    }

    pub fn increment_unknown_semantic(&mut self, value: &str) {
        if !value.is_empty() {
            *self
                .unknown_semantic_types
                .entry(value.to_string())
                .or_default() += 1;
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.malformed_json_lines += other.malformed_json_lines;
        self.non_object_json_lines += other.non_object_json_lines;
        self.orphan_tool_results += other.orphan_tool_results;
        self.duplicate_tool_calls += other.duplicate_tool_calls;
        self.encrypted_reasoning_dropped += other.encrypted_reasoning_dropped;
        self.truncated_tool_inputs += other.truncated_tool_inputs;
        self.truncated_tool_outputs += other.truncated_tool_outputs;
        for (key, count) in other.unknown_top_level_types {
            *self.unknown_top_level_types.entry(key).or_default() += count;
        }
        for (key, count) in other.unknown_semantic_types {
            *self.unknown_semantic_types.entry(key).or_default() += count;
        }
    }

    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// A file whose content a cached usage projection depends on. Sources that reconstruct
/// cross-file state (currently Codex forks) attach these fingerprints to their parse result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct UsageDependency {
    pub path: String,
    pub size: u64,
    pub mtime_ns: i64,
}

impl UsageDependency {
    pub fn from_path(path: &Path) -> std::io::Result<Self> {
        let metadata = path.metadata()?;
        let mtime_ns = metadata
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(i64::MAX as u128) as i64;
        Ok(Self {
            path: path.to_string_lossy().to_string(),
            size: metadata.len(),
            mtime_ns,
        })
    }

    pub fn is_current(&self) -> bool {
        Self::from_path(Path::new(&self.path))
            .is_ok_and(|current| current.size == self.size && current.mtime_ns == self.mtime_ns)
    }
}

/// Source-owned usage parsing output. The shared usage pipeline only caches and assembles it.
pub(crate) struct UsageParseOutput {
    pub events: Vec<UsageEvent>,
    pub cacheable: bool,
    pub deps: Vec<UsageDependency>,
}

impl UsageParseOutput {
    pub fn cacheable(events: Vec<UsageEvent>) -> Self {
        Self {
            events,
            cacheable: true,
            deps: Vec::new(),
        }
    }
}

pub fn versions(source: SourceKind) -> ParserVersions {
    match source {
        SourceKind::Claude => claude::VERSIONS,
        SourceKind::Codex => codex::VERSIONS,
        SourceKind::Cursor => cursor::VERSIONS,
        SourceKind::Opencode => opencode::VERSIONS,
        SourceKind::Pi => pi::VERSIONS,
        SourceKind::OpenClaw => openclaw::VERSIONS,
        SourceKind::Copilot => copilot::VERSIONS,
    }
}

pub fn index_state_version(source: SourceKind) -> u32 {
    index_state_version_for(source, false)
}

pub fn index_state_version_for(source: SourceKind, include_reasoning: bool) -> u32 {
    let versions = versions(source);
    let reasoning_mode = include_reasoning
        && matches!(
            source,
            SourceKind::Claude | SourceKind::Codex | SourceKind::Pi | SourceKind::OpenClaw
        );
    (versions.identity.saturating_mul(10_000) + versions.index)
        .saturating_mul(2)
        .saturating_add(u32::from(reasoning_mode))
}

/// Compatibility classification for persisted records that only carry a source path.
/// Individual path rules stay beside the source discovery code that defines them.
pub fn classify_path(path: &str) -> SourceKind {
    if let Some(source) = codex::classify_path(path) {
        source
    } else if opencode::matches_path(path) {
        SourceKind::Opencode
    } else if cursor::matches_path(path) {
        SourceKind::Cursor
    } else if pi::matches_path(path) {
        SourceKind::Pi
    } else if openclaw::matches_path(path) {
        SourceKind::OpenClaw
    } else if copilot::matches_path(path) {
        SourceKind::Copilot
    } else {
        SourceKind::Claude
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reasoning_mode_is_part_of_index_state_version() {
        for source in [
            SourceKind::Claude,
            SourceKind::Codex,
            SourceKind::Pi,
            SourceKind::OpenClaw,
        ] {
            assert_ne!(
                index_state_version_for(source, false),
                index_state_version_for(source, true)
            );
        }
    }
}
