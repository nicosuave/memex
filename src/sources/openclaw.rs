use super::{IndexParseOutput, IndexParseState, ParserVersions, SourceFile};
use crate::types::{Record, SourceKind};
use crate::usage::UsageEvent;
use anyhow::Result;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 1,
    // OpenClaw delegates the SessionManager projection to the Pi parser.
    index: 4,
    usage: 4,
};

const DELIVERY_MIRROR_MODEL: &str = "delivery-mirror";

pub fn matches_path(path: &str) -> bool {
    (path.contains(".openclaw/agents") || path.contains(".openclaw\\agents"))
        || (path.contains(".clawdbot/agents") || path.contains(".clawdbot\\agents"))
}

pub fn state_dirs() -> Vec<PathBuf> {
    if let Some(path) =
        std::env::var_os("OPENCLAW_STATE_DIR").or_else(|| std::env::var_os("CLAWDBOT_STATE_DIR"))
    {
        return vec![PathBuf::from(path)];
    }
    let home = super::common::home();
    vec![home.join(".openclaw"), home.join(".clawdbot")]
}

pub fn discover() -> Vec<SourceFile> {
    discover_from_state_dirs(&state_dirs())
}

fn discover_from_state_dirs(roots: &[PathBuf]) -> Vec<SourceFile> {
    let mut seen = HashSet::new();
    let mut files = roots
        .iter()
        .flat_map(|root| discover_from_state_dir(root))
        .filter(|file| seen.insert(file.path.clone()))
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.path.cmp(&right.path));
    files
}

fn discover_from_state_dir(root: &Path) -> Vec<SourceFile> {
    let agents = root.join("agents");
    let Ok(agent_entries) = std::fs::read_dir(agents) else {
        return Vec::new();
    };
    let mut files = Vec::new();
    for agent in agent_entries.flatten() {
        let sessions = agent.path().join("sessions");
        let Ok(session_entries) = std::fs::read_dir(sessions) else {
            continue;
        };
        for entry in session_entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
                files.push(SourceFile {
                    source: SourceKind::OpenClaw,
                    path,
                });
            }
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    files
}

pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    super::pi::parse_index_records_for(
        path,
        state,
        SourceKind::OpenClaw,
        include_reasoning,
        next_doc_id,
        emit,
    )
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<Vec<UsageEvent>> {
    super::pi::parse_usage_file_for(path, "openclaw", &[DELIVERY_MIRROR_MODEL])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn discovery_scans_agent_session_files_only() {
        let temp = tempfile::tempdir().unwrap();
        let sessions = temp.path().join("agents/main/sessions");
        fs::create_dir_all(&sessions).unwrap();
        fs::write(sessions.join("one.jsonl"), "{}\n").unwrap();
        fs::write(sessions.join("one.json"), "{}\n").unwrap();
        fs::write(temp.path().join("journal.jsonl"), "{}\n").unwrap();

        let files = discover_from_state_dir(temp.path());
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].source, SourceKind::OpenClaw);
        assert_eq!(
            files[0].path.file_name().and_then(|name| name.to_str()),
            Some("one.jsonl")
        );
    }

    #[test]
    fn discovery_scans_current_and_legacy_stores_together() {
        let temp = tempfile::tempdir().unwrap();
        let current = temp.path().join(".openclaw");
        let legacy = temp.path().join(".clawdbot");
        for (root, name) in [(&current, "current.jsonl"), (&legacy, "legacy.jsonl")] {
            let sessions = root.join("agents/main/sessions");
            fs::create_dir_all(&sessions).unwrap();
            fs::write(sessions.join(name), "{}\n").unwrap();
        }

        let files = discover_from_state_dirs(&[current, legacy]);
        assert_eq!(files.len(), 2);
        assert!(
            files
                .iter()
                .any(|file| file.path.ends_with("current.jsonl"))
        );
        assert!(files.iter().any(|file| file.path.ends_with("legacy.jsonl")));
    }

    #[test]
    fn path_classification_accepts_current_and_legacy_stores() {
        assert!(matches_path(
            "/Users/test/.openclaw/agents/main/sessions/one.jsonl"
        ));
        assert!(matches_path(
            r"C:\Users\test\.clawdbot\agents\main\sessions\one.jsonl"
        ));
    }

    #[test]
    fn parity_fixture_uses_pi_projection_and_filters_placeholder_model() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("openclaw.jsonl");
        fs::write(
            &path,
            include_str!("../../fixtures/trajectory_parity/openclaw.jsonl"),
        )
        .unwrap();

        let mut records = Vec::new();
        parse_index_records(
            &path,
            IndexParseState::default(),
            false,
            &AtomicU64::new(1),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source, SourceKind::OpenClaw);
        assert_eq!(records[0].text, "Mirrored assistant prose");
        assert_eq!(records[0].links.message_ordinal, Some(records[0].turn_id));

        let usage = parse_usage_file(&path).unwrap();
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].source, "openclaw");
        assert_eq!(usage[0].model, None);
    }
}
