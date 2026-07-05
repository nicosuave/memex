use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SourceKind {
    #[default]
    Claude,
    CodexSession,
    CodexHistory,
    Opencode,
    Cursor,
    Pi,
    Copilot,
}

impl SourceKind {
    pub const ALL: [SourceKind; 7] = [
        SourceKind::Claude,
        SourceKind::CodexSession,
        SourceKind::CodexHistory,
        SourceKind::Opencode,
        SourceKind::Cursor,
        SourceKind::Pi,
        SourceKind::Copilot,
    ];
    pub const COUNT: usize = Self::ALL.len();

    pub fn idx(self) -> usize {
        match self {
            SourceKind::Claude => 0,
            SourceKind::CodexSession => 1,
            SourceKind::CodexHistory => 2,
            SourceKind::Opencode => 3,
            SourceKind::Cursor => 4,
            SourceKind::Pi => 5,
            SourceKind::Copilot => 6,
        }
    }

    pub fn from_idx(idx: usize) -> Option<Self> {
        match idx {
            0 => Some(SourceKind::Claude),
            1 => Some(SourceKind::CodexSession),
            2 => Some(SourceKind::CodexHistory),
            3 => Some(SourceKind::Opencode),
            4 => Some(SourceKind::Cursor),
            5 => Some(SourceKind::Pi),
            6 => Some(SourceKind::Copilot),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            SourceKind::Claude => "claude",
            SourceKind::CodexSession | SourceKind::CodexHistory => "codex",
            SourceKind::Opencode => "opencode",
            SourceKind::Cursor => "cursor",
            SourceKind::Pi => "pi",
            SourceKind::Copilot => "copilot",
        }
    }

    pub fn storage_label(self) -> &'static str {
        match self {
            SourceKind::Claude => "claude",
            SourceKind::CodexSession => "codex-session",
            SourceKind::CodexHistory => "codex-history",
            SourceKind::Opencode => "opencode",
            SourceKind::Cursor => "cursor",
            SourceKind::Pi => "pi",
            SourceKind::Copilot => "copilot",
        }
    }

    pub fn from_path(path: &str) -> Self {
        if path.contains(".codex/sessions")
            || path.contains(".codex\\sessions")
            || path.contains(".codex/archived_sessions")
            || path.contains(".codex\\archived_sessions")
        {
            SourceKind::CodexSession
        } else if path.contains(".codex/history.jsonl") || path.contains(".codex\\history.jsonl") {
            SourceKind::CodexHistory
        } else if path.contains("opencode/storage/message")
            || path.contains("opencode\\storage\\message")
        {
            SourceKind::Opencode
        } else if path.contains(".cursor/projects")
            || path.contains(".cursor\\projects")
            || path.contains("agent-transcripts")
        {
            SourceKind::Cursor
        } else if path.contains(".pi/agent/sessions")
            || path.contains(".pi\\agent\\sessions")
            || path.contains("pi/agent/sessions")
            || path.contains("pi\\agent\\sessions")
        {
            SourceKind::Pi
        } else if path.contains(".copilot/session-state")
            || path.contains(".copilot\\session-state")
            || path.contains("/session-state/")
            || path.contains("\\session-state\\")
        {
            SourceKind::Copilot
        } else {
            SourceKind::Claude
        }
    }

    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "claude" => Some(SourceKind::Claude),
            "codex" | "codex-session" => Some(SourceKind::CodexSession),
            "codex-history" => Some(SourceKind::CodexHistory),
            "opencode" => Some(SourceKind::Opencode),
            "cursor" => Some(SourceKind::Cursor),
            "pi" => Some(SourceKind::Pi),
            "copilot" => Some(SourceKind::Copilot),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum SourceFilter {
    Claude,
    Codex,
    Opencode,
    Cursor,
    Pi,
    Copilot,
}

impl SourceFilter {
    pub fn matches(self, source: SourceKind) -> bool {
        match self {
            SourceFilter::Claude => source == SourceKind::Claude,
            SourceFilter::Codex => {
                source == SourceKind::CodexSession || source == SourceKind::CodexHistory
            }
            SourceFilter::Opencode => source == SourceKind::Opencode,
            SourceFilter::Cursor => source == SourceKind::Cursor,
            SourceFilter::Pi => source == SourceKind::Pi,
            SourceFilter::Copilot => source == SourceKind::Copilot,
        }
    }

    pub fn storage_labels(self) -> &'static [&'static str] {
        match self {
            SourceFilter::Claude => &["claude"],
            SourceFilter::Codex => &["codex", "codex-session", "codex-history"],
            SourceFilter::Opencode => &["opencode"],
            SourceFilter::Cursor => &["cursor"],
            SourceFilter::Pi => &["pi"],
            SourceFilter::Copilot => &["copilot"],
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SourceFilter::Claude => "claude",
            SourceFilter::Codex => "codex",
            SourceFilter::Opencode => "opencode",
            SourceFilter::Cursor => "cursor",
            SourceFilter::Pi => "pi",
            SourceFilter::Copilot => "copilot",
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
    #[serde(skip)]
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
    use super::SourceKind;
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
    fn from_path_recognizes_archived_codex_sessions() {
        let unix_path = "/tmp/.codex/archived_sessions/rollout-2026-02-10T11-16-28-abc.jsonl";
        let windows_path =
            "C:\\tmp\\.codex\\archived_sessions\\rollout-2026-02-10T11-16-28-abc.jsonl";

        assert_eq!(SourceKind::from_path(unix_path), SourceKind::CodexSession);
        assert_eq!(
            SourceKind::from_path(windows_path),
            SourceKind::CodexSession
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
}
