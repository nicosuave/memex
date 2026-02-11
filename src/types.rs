use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SourceKind {
    #[default]
    Claude,
    CodexSession,
    CodexHistory,
    Opencode,
}

impl SourceKind {
    pub fn idx(self) -> usize {
        match self {
            SourceKind::Claude => 0,
            SourceKind::CodexSession => 1,
            SourceKind::CodexHistory => 2,
            SourceKind::Opencode => 3,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            SourceKind::Claude => "claude",
            SourceKind::CodexSession | SourceKind::CodexHistory => "codex",
            SourceKind::Opencode => "opencode",
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
        } else {
            SourceKind::Claude
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub enum SourceFilter {
    Claude,
    Codex,
    Opencode,
}

impl SourceFilter {
    pub fn matches(self, source: SourceKind) -> bool {
        match self {
            SourceFilter::Claude => source == SourceKind::Claude,
            SourceFilter::Codex => {
                source == SourceKind::CodexSession || source == SourceKind::CodexHistory
            }
            SourceFilter::Opencode => source == SourceKind::Opencode,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SourceFilter::Claude => "claude",
            SourceFilter::Codex => "codex",
            SourceFilter::Opencode => "opencode",
        }
    }
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
    pub source_path: String,
}

#[cfg(test)]
mod tests {
    use super::SourceKind;

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
}
