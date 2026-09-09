use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceKind {
    #[default]
    Claude,
    Codex,
    Opencode,
    Cursor,
    Pi,
    OpenClaw,
    Copilot,
    Omp,
    Grok,
    Hermes,
    Jcode,
    Muse,
}

impl SourceKind {
    pub const ALL: [SourceKind; 12] = [
        SourceKind::Claude,
        SourceKind::Codex,
        SourceKind::Opencode,
        SourceKind::Cursor,
        SourceKind::Pi,
        SourceKind::OpenClaw,
        SourceKind::Copilot,
        SourceKind::Omp,
        SourceKind::Grok,
        SourceKind::Hermes,
        SourceKind::Jcode,
        SourceKind::Muse,
    ];
    pub const COUNT: usize = Self::ALL.len();

    pub fn idx(self) -> usize {
        match self {
            SourceKind::Claude => 0,
            SourceKind::Codex => 1,
            SourceKind::Opencode => 2,
            SourceKind::Cursor => 3,
            SourceKind::Pi => 4,
            SourceKind::OpenClaw => 5,
            SourceKind::Copilot => 6,
            SourceKind::Omp => 7,
            SourceKind::Grok => 8,
            SourceKind::Hermes => 9,
            SourceKind::Jcode => 10,
            SourceKind::Muse => 11,
        }
    }

    pub fn from_idx(idx: usize) -> Option<Self> {
        match idx {
            0 => Some(SourceKind::Claude),
            1 => Some(SourceKind::Codex),
            2 => Some(SourceKind::Opencode),
            3 => Some(SourceKind::Cursor),
            4 => Some(SourceKind::Pi),
            5 => Some(SourceKind::OpenClaw),
            6 => Some(SourceKind::Copilot),
            7 => Some(SourceKind::Omp),
            8 => Some(SourceKind::Grok),
            9 => Some(SourceKind::Hermes),
            10 => Some(SourceKind::Jcode),
            11 => Some(SourceKind::Muse),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            SourceKind::Claude => "claude",
            SourceKind::Codex => "codex",
            SourceKind::Opencode => "opencode",
            SourceKind::Cursor => "cursor",
            SourceKind::Pi => "pi",
            SourceKind::OpenClaw => "openclaw",
            SourceKind::Copilot => "copilot",
            SourceKind::Omp => "omp",
            SourceKind::Grok => "grok",
            SourceKind::Hermes => "hermes",
            SourceKind::Jcode => "jcode",
            SourceKind::Muse => "muse",
        }
    }

    pub fn storage_label(self) -> &'static str {
        match self {
            SourceKind::Claude => "claude",
            SourceKind::Codex => "codex",
            SourceKind::Opencode => "opencode",
            SourceKind::Cursor => "cursor",
            SourceKind::Pi => "pi",
            SourceKind::OpenClaw => "openclaw",
            SourceKind::Copilot => "copilot",
            SourceKind::Omp => "omp",
            SourceKind::Grok => "grok",
            SourceKind::Hermes => "hermes",
            SourceKind::Jcode => "jcode",
            SourceKind::Muse => "muse",
        }
    }

    pub fn from_path(path: &str) -> Self {
        crate::sources::classify_path(path)
    }

    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "claude" => Some(SourceKind::Claude),
            "codex" | "codex-session" | "codex-history" => Some(SourceKind::Codex),
            "opencode" => Some(SourceKind::Opencode),
            "cursor" => Some(SourceKind::Cursor),
            "pi" => Some(SourceKind::Pi),
            "openclaw" => Some(SourceKind::OpenClaw),
            "copilot" => Some(SourceKind::Copilot),
            "omp" => Some(SourceKind::Omp),
            "grok" => Some(SourceKind::Grok),
            "hermes" => Some(SourceKind::Hermes),
            "jcode" => Some(SourceKind::Jcode),
            "muse" => Some(SourceKind::Muse),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[value(rename_all = "kebab-case")]
#[serde(rename_all = "kebab-case")]
pub enum SourceFilter {
    Claude,
    Codex,
    Opencode,
    Cursor,
    Pi,
    #[value(name = "openclaw", alias = "open-claw")]
    OpenClaw,
    Copilot,
    Omp,
    Grok,
    Hermes,
    Jcode,
    Muse,
}

impl SourceFilter {
    pub fn matches(self, source: SourceKind) -> bool {
        match self {
            SourceFilter::Claude => source == SourceKind::Claude,
            SourceFilter::Codex => source == SourceKind::Codex,
            SourceFilter::Opencode => source == SourceKind::Opencode,
            SourceFilter::Cursor => source == SourceKind::Cursor,
            SourceFilter::Pi => source == SourceKind::Pi,
            SourceFilter::OpenClaw => source == SourceKind::OpenClaw,
            SourceFilter::Copilot => source == SourceKind::Copilot,
            SourceFilter::Omp => source == SourceKind::Omp,
            SourceFilter::Grok => source == SourceKind::Grok,
            SourceFilter::Hermes => source == SourceKind::Hermes,
            SourceFilter::Jcode => source == SourceKind::Jcode,
            SourceFilter::Muse => source == SourceKind::Muse,
        }
    }

    pub fn storage_labels(self) -> &'static [&'static str] {
        match self {
            SourceFilter::Claude => &["claude"],
            SourceFilter::Codex => &["codex", "codex-session", "codex-history"],
            SourceFilter::Opencode => &["opencode"],
            SourceFilter::Cursor => &["cursor"],
            SourceFilter::Pi => &["pi"],
            SourceFilter::OpenClaw => &["openclaw"],
            SourceFilter::Copilot => &["copilot"],
            SourceFilter::Omp => &["omp"],
            SourceFilter::Grok => &["grok"],
            SourceFilter::Hermes => &["hermes"],
            SourceFilter::Jcode => &["jcode"],
            SourceFilter::Muse => &["muse"],
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SourceFilter::Claude => "claude",
            SourceFilter::Codex => "codex",
            SourceFilter::Opencode => "opencode",
            SourceFilter::Cursor => "cursor",
            SourceFilter::Pi => "pi",
            SourceFilter::OpenClaw => "openclaw",
            SourceFilter::Copilot => "copilot",
            SourceFilter::Omp => "omp",
            SourceFilter::Grok => "grok",
            SourceFilter::Hermes => "hermes",
            SourceFilter::Jcode => "jcode",
            SourceFilter::Muse => "muse",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecordLinks {
    /// Provider turn identity; `Record::turn_id` remains the ordered record sequence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_turn_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assistant_phase: Option<String>,
    /// Explicit provider event, never inferred from an answer or missing output.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle_event: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_record_type: Option<String>,
    /// Typed display content when a message includes attachments, serialized as JSON.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical_parent_event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_tool_use_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_tool_use_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_tool_assistant_uuid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    #[serde(default)]
    pub source: SourceKind,
    pub doc_id: u64,
    pub ts: u64,
    pub project: String,
    pub session_id: String,
    pub turn_id: u32,
    pub role: String,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_input: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_output: Option<String>,
    #[serde(flatten)]
    pub links: RecordLinks,
    pub source_path: String,
}

/// Lowercase substrings marking a first user message as a spawned-worker
/// directive rather than an interactive prompt. Shared by the jcode parser
/// and the analytics backstop so the two can never disagree.
pub const JCODE_SUBAGENT_MARKERS: &[&str] = &[
    "you are a low-effort fact-checker",
    "you are a low effort",
    "you are a subagent",
    "you are downstream",
    "you are the downstream",
    "investigation subagent",
    "deep validation:",
    "bossmode task",
    // The trailing colon is load-bearing: without it, ordinary prose such
    // as "add a `role: manager` column" would match. Only the colon
    // form ("Role: Manager: ...") marks a spawned worker.
    "role: manager:",
];

/// Leaf-name tokens marking a /tmp working directory as a spawned-worker
/// sandbox (e.g. `/private/tmp/bossmode-hygiene-v2-worker-docs`). A bare
/// /tmp cwd alone is not evidence — users legitimately work in /tmp — so
/// the sandbox cue needs one of these tokens in the leaf directory name.
pub const JCODE_WORKER_SANDBOX_TOKENS: &[&str] =
    &["worker", "agent", "swarm", "sandbox", "spawn", "subagent"];

/// Returns true when `cwd` is a spawned-worker sandbox under the system
/// temp dir. Shared by the jcode parser and the analytics backstop so the
/// two can never disagree.
pub fn jcode_tmp_cwd_is_worker_sandbox(cwd: &str) -> bool {
    let under_tmp = cwd == "/tmp"
        || cwd == "/private/tmp"
        || cwd.starts_with("/tmp/")
        || cwd.starts_with("/private/tmp/");
    if !under_tmp {
        return false;
    }
    let leaf = cwd.rsplit('/').next().unwrap_or("").to_lowercase();
    JCODE_WORKER_SANDBOX_TOKENS
        .iter()
        .any(|token| leaf.contains(token))
}

/// Returns true when lowercase first-directive text identifies a spawned
/// worker. Compound rules keep precision against ordinary prompts:
/// - "implementation worker" needs a role assignment ("for <task>" or
///   "you are") so "review the implementation worker pool sizing" stays out.
/// - "0 repo writes" needs the read-only constraint form ("outside …" or
///   sentence-final) so "0 repo writes from forks" stays out.
pub fn jcode_text_is_subagent_directive(lower: &str) -> bool {
    if JCODE_SUBAGENT_MARKERS
        .iter()
        .any(|marker| lower.contains(marker))
    {
        return true;
    }
    if lower.contains("implementation worker")
        && (lower.contains("implementation worker for") || lower.contains("you are"))
    {
        return true;
    }
    if let Some(pos) = lower.find("0 repo writes") {
        let rest = lower[pos + "0 repo writes".len()..].trim_start();
        if rest.starts_with("outside") || rest.starts_with('.') {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{
        SourceFilter, SourceKind, jcode_text_is_subagent_directive, jcode_tmp_cwd_is_worker_sandbox,
    };
    use clap::ValueEnum;
    use std::collections::HashSet;

    #[test]
    fn subagent_directive_markers_match_worker_roles() {
        for text in [
            "You are downstream A8 mapper. Read-only.",
            "You are the focused implementation worker for Bossmode task task_abc.",
            "You are an investigation subagent. Triage the failure.",
            "Deep validation: Orchestration ORIGIN steady. Read-only.",
            "Act as an independent reviewer for Bossmode task task_abc.",
            "Role: Manager: Repo Hygiene. You are the coordinator.",
            "FRESH run. 0 repo writes outside /tmp.",
            "FRESH run. 0 repo writes.",
        ] {
            assert!(
                jcode_text_is_subagent_directive(&text.to_lowercase()),
                "should match: {text}"
            );
        }
    }

    #[test]
    fn subagent_directive_markers_reject_ordinary_prose() {
        for text in [
            "Add a `role: manager` column to the users table.",
            "Please review the implementation worker pool sizing.",
            "Our CI policy says 0 repo writes from forks.",
            "take over and fully complete the interrupted session",
            "Perform a final independent read-only review of the release.",
        ] {
            assert!(
                !jcode_text_is_subagent_directive(&text.to_lowercase()),
                "should not match: {text}"
            );
        }
    }

    #[test]
    fn worker_sandbox_cwd_needs_token_leaf_under_tmp() {
        for cwd in [
            "/private/tmp/bossmode-hygiene-v2-worker-docs",
            "/tmp/swarm-run-12",
            "/tmp/agent-scratch",
        ] {
            assert!(jcode_tmp_cwd_is_worker_sandbox(cwd), "should match: {cwd}");
        }
        for cwd in ["/tmp", "/private/tmp", "/tmp/work", "/repo/example", ""] {
            assert!(
                !jcode_tmp_cwd_is_worker_sandbox(cwd),
                "should not match: {cwd}"
            );
        }
    }

    #[test]
    fn source_indices_and_storage_labels_are_unique() {
        assert_eq!(SourceKind::COUNT, SourceKind::ALL.len());
        let mut indices = HashSet::new();
        let mut labels = HashSet::new();
        for source in SourceKind::ALL {
            assert!(indices.insert(source.idx()));
            assert!(labels.insert(source.storage_label()));
            assert_eq!(SourceKind::from_label(source.storage_label()), Some(source));
        }
    }

    #[test]
    fn hermes_is_a_stable_first_class_source() {
        assert_eq!(SourceKind::Hermes.label(), "hermes");
        assert_eq!(SourceKind::Hermes.storage_label(), "hermes");
        assert_eq!(SourceKind::from_label("hermes"), Some(SourceKind::Hermes));
        assert_eq!(
            SourceKind::from_idx(SourceKind::Hermes.idx()),
            Some(SourceKind::Hermes)
        );
        assert!(SourceFilter::Hermes.matches(SourceKind::Hermes));
        assert_eq!(SourceFilter::Hermes.as_str(), "hermes");
        assert_eq!(SourceFilter::Hermes.storage_labels(), &["hermes"]);
    }

    #[test]
    fn grok_is_a_stable_first_class_source() {
        assert_eq!(SourceKind::Grok.label(), "grok");
        assert_eq!(SourceKind::Grok.storage_label(), "grok");
        assert_eq!(SourceKind::from_label("grok"), Some(SourceKind::Grok));
        assert_eq!(
            SourceKind::from_idx(SourceKind::Grok.idx()),
            Some(SourceKind::Grok)
        );
        assert!(SourceFilter::Grok.matches(SourceKind::Grok));
        assert_eq!(SourceFilter::Grok.as_str(), "grok");
        assert_eq!(SourceFilter::Grok.storage_labels(), &["grok"]);
    }

    #[test]
    fn legacy_codex_labels_converge_to_codex() {
        for label in ["codex", "codex-session", "codex-history"] {
            assert_eq!(SourceKind::from_label(label), Some(SourceKind::Codex));
        }
        assert_eq!(SourceKind::Codex.storage_label(), "codex");
    }

    #[test]
    fn openclaw_source_filter_uses_unhyphenated_cli_name() {
        assert_eq!(
            SourceFilter::from_str("openclaw", true),
            Ok(SourceFilter::OpenClaw)
        );
        assert_eq!(
            SourceFilter::from_str("open-claw", true),
            Ok(SourceFilter::OpenClaw)
        );
    }

    #[test]
    fn from_path_recognizes_archived_codex_sessions() {
        let unix_path = "/tmp/.codex/archived_sessions/rollout-2026-02-10T11-16-28-abc.jsonl";
        let windows_path =
            "C:\\tmp\\.codex\\archived_sessions\\rollout-2026-02-10T11-16-28-abc.jsonl";

        assert_eq!(SourceKind::from_path(unix_path), SourceKind::Codex);
        assert_eq!(SourceKind::from_path(windows_path), SourceKind::Codex);
    }

    #[test]
    fn from_path_recognizes_codex_history_as_codex() {
        assert_eq!(
            SourceKind::from_path("/tmp/.codex/history.jsonl"),
            SourceKind::Codex
        );
        assert_eq!(
            SourceKind::from_path("C:\\tmp\\.codex\\history.jsonl"),
            SourceKind::Codex
        );
    }

    #[test]
    fn from_path_recognizes_cursor_agent_transcripts() {
        let unix_path =
            "/Users/nico/.cursor/projects/Users-nico-Code-app/agent-transcripts/abc/abc.jsonl";
        let windows_path =
            "C:\\Users\\nico\\.cursor\\projects\\app\\agent-transcripts\\abc\\abc.jsonl";

        assert_eq!(SourceKind::from_path(unix_path), SourceKind::Cursor);
        assert_eq!(SourceKind::from_path(windows_path), SourceKind::Cursor);
    }

    #[test]
    fn from_path_recognizes_pi_sessions() {
        let unix_path = "/tmp/.pi/agent/sessions/--Users-nico-Code/20260703_session.jsonl";
        let windows_path =
            "C:\\tmp\\.pi\\agent\\sessions\\--Users-nico-Code\\20260703_session.jsonl";

        assert_eq!(SourceKind::from_path(unix_path), SourceKind::Pi);
        assert_eq!(SourceKind::from_path(windows_path), SourceKind::Pi);
    }

    #[test]
    fn from_path_recognizes_copilot_sessions() {
        let unix_path =
            "/Users/nico/.copilot/session-state/11111111-1111-4111-8111-111111111111/events.jsonl";
        let windows_path = "C:\\Users\\nico\\.copilot\\session-state\\11111111-1111-4111-8111-111111111111\\events.jsonl";

        assert_eq!(SourceKind::from_path(unix_path), SourceKind::Copilot);
        assert_eq!(SourceKind::from_path(windows_path), SourceKind::Copilot);
    }

    #[test]
    fn from_path_recognizes_grok_sessions() {
        let unix_path = "/Users/nico/.grok/sessions/%2Fwork/session-id/updates.jsonl";
        let windows_path =
            "C:\\Users\\nico\\.grok\\sessions\\C%3A%5Cwork\\session-id\\updates.jsonl";

        assert_eq!(SourceKind::from_path(unix_path), SourceKind::Grok);
        assert_eq!(SourceKind::from_path(windows_path), SourceKind::Grok);
    }
}
