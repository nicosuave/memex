use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SourceKind {
    #[default]
    Claude,
    CodexSession,
    CodexHistory,
}

impl SourceKind {
    pub fn idx(self) -> usize {
        match self {
            SourceKind::Claude => 0,
            SourceKind::CodexSession => 1,
            SourceKind::CodexHistory => 2,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            SourceKind::Claude => "claude",
            SourceKind::CodexSession | SourceKind::CodexHistory => "codex",
        }
    }

    pub fn from_path(path: &str) -> Self {
        if path.contains(".codex/sessions") || path.contains(".codex\\sessions") {
            SourceKind::CodexSession
        } else if path.contains(".codex/history.jsonl") || path.contains(".codex\\history.jsonl") {
            SourceKind::CodexHistory
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
}

impl SourceFilter {
    pub fn matches(self, source: SourceKind) -> bool {
        match self {
            SourceFilter::Claude => source == SourceKind::Claude,
            SourceFilter::Codex => {
                source == SourceKind::CodexSession || source == SourceKind::CodexHistory
            }
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            SourceFilter::Claude => "claude",
            SourceFilter::Codex => "codex",
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
