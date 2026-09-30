//! Source-owned transcript discovery, identity, and projection adapters.
//!
//! Indexing and usage reconstruction deliberately remain independent projections.  The
//! adapters in this module make them share source identity, discovery, hierarchy, and
//! parser-version rules without introducing a persisted normalized transcript store.

pub mod antigravity;
pub mod audit;
pub mod bob;
pub mod claude;
pub mod codex;
pub mod common;
pub mod copilot;
pub mod cursor;
pub mod grok;
pub mod hermes;
pub mod jcode;
mod jsonl;
pub mod kilocode;
pub mod kiro;
pub mod muse;
pub mod omp;
pub mod openclaw;
pub mod opencode;
pub mod pi;
pub mod zcode;

use crate::state::PendingToolCall;
use crate::types::SourceKind;
use crate::usage::UsageEvent;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationKind {
    Main,
    Subagent,
    GuardianReview,
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
            Self::GuardianReview => "guardian_review",
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
    pub legacy_turn_id: Option<u32>,
    pub pending_tool_calls: std::collections::HashMap<String, PendingToolCall>,
}

#[derive(Clone, Debug)]
pub(crate) struct IndexParseOutput {
    pub offset: u64,
    pub turn_id: u32,
    pub legacy_turn_id: Option<u32>,
    pub pending_tool_calls: std::collections::HashMap<String, PendingToolCall>,
    pub session_id: Option<String>,
    pub diagnostics: ParseDiagnostics,
    /// Working directory the transcript records for its session, when the format carries one.
    /// Analytics resolves repositories from it instead of re-reading the transcript.
    pub session_cwd: Option<String>,
}

impl IndexParseState {
    fn legacy_ordinal(&self) -> anyhow::Result<u32> {
        if self.offset == 0 {
            return Ok(0);
        }
        self.legacy_turn_id.ok_or_else(|| {
            anyhow::anyhow!("missing legacy record ordinal; reparse the source from offset zero")
        })
    }
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
    /// Source stores discovery could not read this refresh (locked, corrupt, or on an
    /// unsupported schema). Their previously indexed records are kept until they read again.
    pub unreadable_sources: Vec<String>,
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
        self.unreadable_sources.extend(other.unreadable_sources);
        self.unreadable_sources.sort();
        self.unreadable_sources.dedup();
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
    pub native_path: Vec<u8>,
    pub size: u64,
    pub mtime_ns: i64,
    pub exists: bool,
}

impl UsageDependency {
    fn native_path(path: &Path) -> Vec<u8> {
        #[cfg(unix)]
        {
            path.as_os_str().as_bytes().to_vec()
        }
        #[cfg(not(unix))]
        {
            path.to_string_lossy().as_bytes().to_vec()
        }
    }

    fn path_from_native(&self) -> PathBuf {
        #[cfg(unix)]
        {
            PathBuf::from(OsString::from_vec(self.native_path.clone()))
        }
        #[cfg(not(unix))]
        {
            PathBuf::from(String::from_utf8_lossy(&self.native_path).into_owned())
        }
    }

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
            native_path: Self::native_path(path),
            size: metadata.len(),
            mtime_ns,
            exists: true,
        })
    }

    pub fn from_path_or_absent(path: &Path) -> Self {
        Self::from_path(path).unwrap_or_else(|_| Self {
            path: path.to_string_lossy().to_string(),
            native_path: Self::native_path(path),
            size: 0,
            mtime_ns: 0,
            exists: false,
        })
    }

    #[allow(dead_code)]
    pub fn is_current(&self) -> bool {
        Self::from_path_or_absent(&self.path_from_native()) == *self
    }

    /// Current on-disk fingerprint for this dependency's path, without comparing.
    /// Scans cache one observation per distinct path so shared parent rollouts are
    /// stat'd once per scan instead of once per dependent file.
    pub(crate) fn observed(&self) -> (u64, i64, bool) {
        let current = Self::from_path_or_absent(&self.path_from_native());
        (current.size, current.mtime_ns, current.exists)
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
        SourceKind::Omp => omp::VERSIONS,
        SourceKind::OpenClaw => openclaw::VERSIONS,
        SourceKind::Copilot => copilot::VERSIONS,
        SourceKind::Grok => grok::VERSIONS,
        SourceKind::Hermes => hermes::VERSIONS,
        SourceKind::Jcode => jcode::VERSIONS,
        SourceKind::Muse => muse::VERSIONS,
        SourceKind::Antigravity => antigravity::VERSIONS,
        SourceKind::Bob => bob::VERSIONS,
        SourceKind::Zcode => zcode::VERSIONS,
        SourceKind::Kiro => kiro::VERSIONS,
        SourceKind::Kilocode => kilocode::VERSIONS,
    }
}

/// Best-effort working directory for a session transcript.
///
/// Sources that record the cwd somewhere other than a JSONL line (SQLite
/// stores, protobuf payloads, sidecar files) extract it themselves; every
/// other format shares the generic `cwd` scan. Callers fall back to their own
/// last resort when this returns `None`.
pub fn session_cwd(source: SourceKind, path: &Path, session_id: &str) -> Option<String> {
    match source {
        SourceKind::Antigravity => {
            antigravity::session_cwd(path).map(|cwd| cwd.to_string_lossy().into_owned())
        }
        SourceKind::Bob => bob::session_cwd(path).map(|cwd| cwd.to_string_lossy().into_owned()),
        SourceKind::Copilot => copilot::session_cwd(path),
        SourceKind::Kilocode => {
            kilocode::session_cwd(path, session_id).map(|cwd| cwd.to_string_lossy().into_owned())
        }
        SourceKind::Hermes => hermes::session_cwd(path, session_id),
        _ => jsonl::scan_session_cwd(path, session_id),
    }
}

/// True when a directory lies inside a transcript store owned by a supported
/// source rather than in a user workspace. Resume commands must never treat
/// such a directory as a session cwd: CLIs such as antigravity would ask the
/// user to trust an agent's own state store as a project.
pub fn is_state_store_dir(dir: &Path) -> bool {
    let home = common::home();
    state_store_roots()
        .into_iter()
        // A source configured to keep transcripts directly in `$HOME` (pi's
        // `sessionDir = "~"`) must not make the whole home directory internal.
        .filter(|root| root != &home && root.as_os_str() != "/")
        .any(|root| dir.starts_with(root))
}

fn state_store_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    roots.extend(crate::config::default_claude_sources());
    // CODEX_HOME also owns real workspaces under worktrees/. Only its
    // transcript directories are unsafe resume destinations.
    roots.extend(
        codex::homes()
            .into_iter()
            .flat_map(|home| [home.join("sessions"), home.join("archived_sessions")]),
    );
    roots.push(cursor::projects_root());
    roots.extend(opencode::data_roots());
    roots.push(pi::sessions_root());
    roots.extend(omp::session_roots());
    roots.extend(openclaw::state_dirs());
    roots.push(copilot::root());
    roots.push(grok::root());
    roots.extend(hermes::profile_roots());
    roots.push(jcode::sessions_root());
    roots.push(muse::sessions_root());
    roots.extend(antigravity::profile_roots());
    roots.extend(bob::roots());
    roots.extend(kilocode::roots());
    roots
}

pub fn index_state_version(source: SourceKind) -> u32 {
    index_state_version_for(source, false)
}

pub fn index_state_version_for(source: SourceKind, include_reasoning: bool) -> u32 {
    let versions = versions(source);
    let reasoning_mode = include_reasoning
        && matches!(
            source,
            SourceKind::Claude
                | SourceKind::Codex
                | SourceKind::Pi
                | SourceKind::Omp
                | SourceKind::OpenClaw
                | SourceKind::Opencode
                | SourceKind::Jcode
                | SourceKind::Muse
                | SourceKind::Grok
                | SourceKind::Hermes
                | SourceKind::Antigravity
                | SourceKind::Zcode
                | SourceKind::Kiro
                | SourceKind::Kilocode
        );
    (versions.identity.saturating_mul(10_000) + versions.index)
        .saturating_mul(2)
        .saturating_add(u32::from(reasoning_mode))
}

/// Compatibility classification for persisted records that only carry a source path.
/// Individual path rules stay beside the source discovery code that defines them.
pub fn classify_path(path: &str) -> SourceKind {
    if bob::matches_path(path) {
        SourceKind::Bob
    } else if zcode::matches_path(path) {
        SourceKind::Zcode
    } else if let Some(source) = codex::classify_path(path) {
        source
    } else if opencode::matches_path(path) {
        SourceKind::Opencode
    } else if jcode::matches_path(path) {
        SourceKind::Jcode
    } else if muse::matches_path(path) {
        SourceKind::Muse
    } else if antigravity::matches_path(path) {
        SourceKind::Antigravity
    } else if kiro::matches_path(path) {
        SourceKind::Kiro
    } else if kilocode::matches_path(path) {
        SourceKind::Kilocode
    } else if grok::matches_path(path) {
        SourceKind::Grok
    } else if cursor::matches_path(path) {
        SourceKind::Cursor
    } else if omp::matches_path(path) {
        SourceKind::Omp
    } else if pi::matches_path(path) {
        SourceKind::Pi
    } else if openclaw::matches_path(path) {
        SourceKind::OpenClaw
    } else if copilot::matches_path(path) {
        SourceKind::Copilot
    } else if hermes::matches_path(path) {
        SourceKind::Hermes
    } else {
        SourceKind::Claude
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{EnvVarGuard, env_lock, pin_source_roots};

    #[test]
    fn session_cwd_dispatches_source_specific_extraction() {
        let temp = tempfile::tempdir().unwrap();
        let _guard = env_lock();
        let _env = pin_source_roots(temp.path());
        // Antigravity keeps the cwd inside tool-call arguments, not in a
        // top-level `cwd` key the generic scan could find.
        let transcript = temp.path().join("transcript.jsonl");
        std::fs::write(
            &transcript,
            r#"{"type":"PLANNER_RESPONSE","tool_calls":[{"name":"run_command","args":{"Cwd":"/work/repo"}}]}"#,
        )
        .unwrap();
        assert_eq!(
            session_cwd(SourceKind::Antigravity, &transcript, "any"),
            Some("/work/repo".to_string())
        );
        // Formats with a plain top-level `cwd` use the generic scan.
        let jsonl = temp.path().join("session.jsonl");
        std::fs::write(&jsonl, r#"{"sessionId":"s1","cwd":"/work/claude"}"#).unwrap();
        assert_eq!(
            session_cwd(SourceKind::Claude, &jsonl, "s1"),
            Some("/work/claude".to_string())
        );
    }

    #[test]
    fn state_store_dirs_are_detected_from_source_roots() {
        let temp = tempfile::tempdir().unwrap();
        let _guard = env_lock();
        let _env = pin_source_roots(temp.path());
        // Transcript stores of any source are internal, wherever the roots
        // are configured to live...
        assert!(is_state_store_dir(&temp.path().join(
            "ANTIGRAVITY_HOME/antigravity-cli/brain/id/.system_generated/logs"
        )));
        assert!(is_state_store_dir(
            &temp.path().join("CODEX_HOME/sessions/2026/09")
        ));
        assert!(is_state_store_dir(
            &temp.path().join("CODEX_HOME/archived_sessions/2026/09")
        ));
        assert!(!is_state_store_dir(
            &temp.path().join("CODEX_HOME/worktrees/24d4/memex")
        ));
        assert!(is_state_store_dir(
            &temp.path().join("CLAUDE_CONFIG_DIR/projects/encoded-slug")
        ));
        // ...while user workspaces stay resumable, even inside the same parent.
        assert!(!is_state_store_dir(&temp.path().join("my-repo")));
        // A source configured to keep transcripts directly in $HOME must not
        // make the whole home directory internal.
        let home = common::home();
        let _env_home = EnvVarGuard::set(&[("PI_CODING_AGENT_SESSION_DIR", home.to_str())]);
        assert!(!is_state_store_dir(&home));
    }

    #[test]
    fn reasoning_mode_is_part_of_index_state_version() {
        for source in [
            SourceKind::Claude,
            SourceKind::Codex,
            SourceKind::Pi,
            SourceKind::Omp,
            SourceKind::OpenClaw,
            SourceKind::Opencode,
            SourceKind::Jcode,
            SourceKind::Muse,
            SourceKind::Kiro,
            SourceKind::Kilocode,
            SourceKind::Grok,
        ] {
            assert_ne!(
                index_state_version_for(source, false),
                index_state_version_for(source, true)
            );
        }
    }
}
