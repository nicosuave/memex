use super::{IndexParseOutput, IndexParseState, ParserVersions, SourceFile};
use crate::types::{Record, SourceKind};
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 1,
    index: super::pi::VERSIONS.index,
    usage: super::pi::VERSIONS.usage,
};

pub fn matches_path(path: &str) -> bool {
    let mut previous = "";
    let mut two_back = "";
    let mut three_back = "";
    let mut four_back = "";
    for component in path
        .split(['/', '\\'])
        .filter(|component| !component.is_empty())
    {
        let root_session = (two_back == ".omp" || two_back == "omp")
            && previous == "agent"
            && component == "sessions";
        let data_session = previous == "omp" && component == "sessions";
        let profile_session = (four_back == ".omp" || four_back == "omp")
            && three_back == "profiles"
            && previous == "agent"
            && component == "sessions";
        if root_session || data_session || profile_session {
            return true;
        }
        four_back = three_back;
        three_back = two_back;
        two_back = previous;
        previous = component;
    }
    false
}

fn config_root() -> PathBuf {
    let home = super::common::home();
    let config_dir = std::env::var_os("PI_CONFIG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".omp"));
    if config_dir.is_absolute() {
        let relative = config_dir
            .components()
            .filter(|component| {
                !matches!(
                    component,
                    std::path::Component::Prefix(_) | std::path::Component::RootDir
                )
            })
            .collect::<PathBuf>();
        home.join(relative)
    } else {
        home.join(config_dir)
    }
}

pub fn agent_root() -> PathBuf {
    if let Some(root) = std::env::var_os("PI_CODING_AGENT_DIR").filter(|value| !value.is_empty()) {
        return PathBuf::from(root);
    }
    let root = config_root();
    let profile = match std::env::var_os("OMP_PROFILE") {
        Some(value) => (!value.is_empty()).then_some(value),
        None => std::env::var_os("PI_PROFILE").filter(|value| !value.is_empty()),
    };
    if let Some(profile) = profile {
        root.join("profiles").join(profile).join("agent")
    } else {
        root.join("agent")
    }
}

fn profile_session_roots(profiles_root: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(profiles_root) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            entry
                .file_type()
                .ok()
                .filter(|kind| kind.is_dir())
                .map(|_| entry.path().join("agent/sessions"))
        })
        .collect()
}

fn session_roots() -> Vec<PathBuf> {
    if std::env::var_os("PI_CODING_AGENT_DIR").is_some_and(|value| !value.is_empty()) {
        return vec![agent_root().join("sessions")];
    }
    let root = config_root();
    let mut roots = vec![root.join("agent/sessions")];
    roots.extend(profile_session_roots(&root.join("profiles")));

    if let Some(data_home) = std::env::var_os("XDG_DATA_HOME").filter(|value| !value.is_empty()) {
        let data_root = PathBuf::from(data_home).join("omp");
        roots.push(data_root.join("sessions"));
        roots.extend(profile_session_roots(&data_root.join("profiles")));
    }
    roots.sort();
    roots.dedup();
    roots
}

pub fn discover() -> Vec<SourceFile> {
    super::common::jsonl_files(session_roots())
        .into_iter()
        .map(|path| SourceFile {
            source: SourceKind::Omp,
            path,
        })
        .collect()
}

pub fn session_id_from_path(path: &Path) -> String {
    super::pi::session_id_from_path(path)
}

pub fn project_from_path(path: &Path) -> String {
    super::pi::project_from_path(path)
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
        SourceKind::Omp,
        include_reasoning,
        next_doc_id,
        emit,
    )
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<Vec<crate::usage::UsageEvent>> {
    super::pi::parse_usage_file_for(path, "omp", &[])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_root_matches_omp_environment_semantics() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let home = super::super::common::home();

        {
            let _env = EnvVarGuard::set_os(&[
                ("PI_CODING_AGENT_DIR", None),
                (
                    "PI_CONFIG_DIR",
                    Some(std::ffi::OsStr::new("/tmp/absconfig")),
                ),
                ("OMP_PROFILE", None),
                ("PI_PROFILE", None),
            ]);
            assert_eq!(agent_root(), home.join("tmp/absconfig/agent"));
        }
        {
            let _env = EnvVarGuard::set_os(&[
                (
                    "PI_CODING_AGENT_DIR",
                    Some(std::ffi::OsStr::new("/tmp/customagent")),
                ),
                ("PI_CONFIG_DIR", Some(std::ffi::OsStr::new("customcfg"))),
                ("OMP_PROFILE", Some(std::ffi::OsStr::new("audit"))),
                ("PI_PROFILE", None),
            ]);
            assert_eq!(agent_root(), PathBuf::from("/tmp/customagent"));
        }
        {
            let _env = EnvVarGuard::set_os(&[
                ("PI_CODING_AGENT_DIR", None),
                ("PI_CONFIG_DIR", None),
                ("OMP_PROFILE", Some(std::ffi::OsStr::new("audit"))),
                ("PI_PROFILE", None),
            ]);
            assert_eq!(agent_root(), home.join(".omp/profiles/audit/agent"));
        }
        {
            let _env = EnvVarGuard::set_os(&[
                ("PI_CODING_AGENT_DIR", None),
                ("PI_CONFIG_DIR", None),
                ("OMP_PROFILE", Some(std::ffi::OsStr::new(""))),
                ("PI_PROFILE", Some(std::ffi::OsStr::new("ignored"))),
            ]);
            assert_eq!(agent_root(), home.join(".omp/agent"));
        }
    }

    #[test]
    fn discovery_honors_full_agent_directory_override() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let temp = tempfile::tempdir().unwrap();
        let sessions = temp.path().join("sessions/project");
        std::fs::create_dir_all(&sessions).unwrap();
        let transcript = sessions.join("session.jsonl");
        std::fs::write(&transcript, "{}\n").unwrap();
        let _env = EnvVarGuard::set_os(&[("PI_CODING_AGENT_DIR", Some(temp.path().as_os_str()))]);

        assert_eq!(
            discover(),
            vec![SourceFile {
                source: SourceKind::Omp,
                path: transcript
            }]
        );
    }

    #[test]
    fn omp_paths_are_not_pi_paths() {
        let path = "/Users/nico/.omp/agent/sessions/project/session.jsonl";
        assert!(matches_path(path));
        assert!(!crate::sources::pi::matches_path(path));
    }
    #[test]
    fn omp_profile_paths_are_classified() {
        let path = "/Users/nico/.local/share/omp/profiles/work/agent/sessions/session.jsonl";
        assert!(matches_path(path));
        assert_eq!(crate::types::SourceKind::from_path(path), SourceKind::Omp);
    }

    #[test]
    fn omp_fixture_uses_distinct_source_identity() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("omp.jsonl");
        std::fs::write(
            &path,
            include_str!("../../fixtures/trajectory_parity/omp.jsonl"),
        )
        .unwrap();

        let mut records = Vec::new();
        let parsed = parse_index_records(
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
        assert_eq!(parsed.session_id.as_deref(), Some("omp-session"));
        assert!(
            records
                .iter()
                .all(|record| record.source == SourceKind::Omp)
        );
        assert!(records.iter().all(|record| record.project == "omp-project"));

        let usage = parse_usage_file(&path).unwrap();
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].source, "omp");
        assert_eq!(usage[0].session_id.as_deref(), Some("omp-session"));
    }
    #[test]
    fn omp_usage_falls_back_to_session_timestamp() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("omp.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"type\":\"session\",\"version\":3,\"id\":\"omp-session\",\"timestamp\":1786665600000,\"cwd\":\"/workspace/omp-project\"}\n",
                "{\"type\":\"message\",\"id\":\"assistant-1\",\"timestamp\":0,\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"hello\"}],\"usage\":{\"input\":12,\"output\":8}}}\n"
            ),
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
        assert_eq!(records[0].ts, 1_786_665_600_000);

        let usage = parse_usage_file(&path).unwrap();
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].timestamp_ms, 1_786_665_600_000);
    }
}
