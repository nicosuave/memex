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
}

impl SourceKind {
    pub const ALL: [SourceKind; 10] = [
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
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecordLinks {
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

#[cfg(test)]
mod tests {
    use super::{SourceFilter, SourceKind};
    use clap::ValueEnum;
    use std::collections::HashSet;

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
