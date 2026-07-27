use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
}

impl SourceKind {
    pub const ALL: [SourceKind; 7] = [
        SourceKind::Claude,
        SourceKind::Codex,
        SourceKind::Opencode,
        SourceKind::Cursor,
        SourceKind::Pi,
        SourceKind::OpenClaw,
        SourceKind::Copilot,
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
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecordLinks {
    /// Stable order of the source message within its session. Every record emitted from the
    /// same source message carries the same value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_ordinal: Option<u32>,
    /// Zero-based order of this tool call within its owning message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_index: Option<u32>,
    /// Zero-based order of this result update within its owning tool call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interaction_id: Option<String>,
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
    /// Normalized source-provided lifecycle status.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Original status spelling when the source provides one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_write_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skill_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subagent_session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    #[serde(default)]
    pub source: SourceKind,
    /// Stable source-derived identity. Unlike `doc_id`, this survives a full index rebuild.
    #[serde(default)]
    pub record_key: String,
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

impl Record {
    pub fn ensure_record_key(&mut self) {
        if self.record_key.is_empty() {
            self.record_key = self.computed_record_key();
        }
    }

    pub fn computed_record_key(&self) -> String {
        let mut hasher = Sha256::new();
        hash_identity_part(&mut hasher, "version", "2");
        hash_identity_part(&mut hasher, "source", self.source.storage_label());
        hash_identity_part(
            &mut hasher,
            "session",
            if self.session_id.is_empty() {
                &self.source_path
            } else {
                &self.session_id
            },
        );

        // A parent tool ID identifies the relationship, not the result event itself. Falling
        // back to it would collapse multiple lifecycle/result updates for one call.
        let native_id = self.links.event_id.as_deref().or_else(|| {
            (self.role == "tool_use")
                .then_some(self.links.source_tool_use_id.as_deref())
                .flatten()
        });
        if let Some(native_id) = native_id {
            hash_identity_part(&mut hasher, "native", native_id);
        } else {
            hash_identity_part(&mut hasher, "path", &self.source_path);
            hash_identity_part(&mut hasher, "turn", &self.turn_id.to_string());
            hash_identity_part(
                &mut hasher,
                "source_tool_use_id",
                self.links.source_tool_use_id.as_deref().unwrap_or(""),
            );
            hash_identity_part(
                &mut hasher,
                "parent_tool_use_id",
                self.links.parent_tool_use_id.as_deref().unwrap_or(""),
            );
            hash_identity_part(
                &mut hasher,
                "message_ordinal",
                &self
                    .links
                    .message_ordinal
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            );
            hash_identity_part(
                &mut hasher,
                "call_index",
                &self
                    .links
                    .call_index
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            );
            hash_identity_part(
                &mut hasher,
                "event_index",
                &self
                    .links
                    .event_index
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            );
        }

        hash_identity_part(&mut hasher, "role", &self.role);
        hash_identity_part(
            &mut hasher,
            "subtype",
            self.tool_name.as_deref().unwrap_or(""),
        );
        format!("rk2_{:x}", hasher.finalize())
    }

    pub fn computed_content_hash(&self) -> String {
        let mut hasher = Sha256::new();
        hash_identity_part(&mut hasher, "version", "1");
        hash_identity_part(&mut hasher, "role", &self.role);
        hash_identity_part(&mut hasher, "text", &self.text);
        hash_identity_part(
            &mut hasher,
            "tool_name",
            self.tool_name.as_deref().unwrap_or(""),
        );
        hash_identity_part(
            &mut hasher,
            "tool_input",
            self.tool_input.as_deref().unwrap_or(""),
        );
        hash_identity_part(
            &mut hasher,
            "tool_output",
            self.tool_output.as_deref().unwrap_or(""),
        );
        hash_identity_part(
            &mut hasher,
            "status",
            self.links.status.as_deref().unwrap_or(""),
        );
        hash_identity_part(
            &mut hasher,
            "source_status",
            self.links.source_status.as_deref().unwrap_or(""),
        );
        format!("ch1_{:x}", hasher.finalize())
    }
}

fn hash_identity_part(hasher: &mut Sha256, label: &str, value: &str) {
    hasher.update((label.len() as u64).to_le_bytes());
    hasher.update(label.as_bytes());
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::{Record, RecordLinks, SourceFilter, SourceKind};
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
    fn legacy_codex_labels_converge_to_codex() {
        for label in ["codex", "codex-session", "codex-history"] {
            assert_eq!(SourceKind::from_label(label), Some(SourceKind::Codex));
        }
        assert_eq!(SourceKind::Codex.storage_label(), "codex");
    }

    fn record(doc_id: u64, source_path: &str) -> Record {
        Record {
            source: SourceKind::Codex,
            record_key: String::new(),
            doc_id,
            ts: 1,
            project: "memex".to_string(),
            session_id: "session-1".to_string(),
            turn_id: 2,
            role: "assistant".to_string(),
            text: "same projected content".to_string(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks {
                event_id: Some("event-1".to_string()),
                ..RecordLinks::default()
            },
            source_path: source_path.to_string(),
        }
    }

    #[test]
    fn record_key_survives_doc_id_and_source_path_changes_with_native_identity() {
        let first = record(1, "/old/session.jsonl");
        let rebuilt = record(999, "/moved/session.jsonl");

        assert_eq!(first.computed_record_key(), rebuilt.computed_record_key());
        assert!(first.computed_record_key().starts_with("rk2_"));
    }

    #[test]
    fn record_key_is_independent_of_projection_content() {
        let first = record(1, "/session.jsonl");
        let mut changed = first.clone();
        changed.text = "different projected content".to_string();
        changed.tool_input = Some("corrected input".to_string());
        changed.tool_output = Some("corrected output".to_string());

        assert_eq!(first.computed_record_key(), changed.computed_record_key());
        assert_ne!(
            first.computed_content_hash(),
            changed.computed_content_hash()
        );
    }

    #[test]
    fn result_updates_do_not_use_the_parent_call_as_their_identity() {
        let mut first = record(1, "/session.jsonl");
        first.role = "tool_result".to_string();
        first.links.event_id = None;
        first.links.parent_tool_use_id = Some("call-1".to_string());
        first.turn_id = 4;
        let mut second = first.clone();
        second.turn_id = 5;

        assert_ne!(first.computed_record_key(), second.computed_record_key());
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
}
