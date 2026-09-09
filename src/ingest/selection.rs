//! Resolve event hints without walking transcript trees. Root aliases are
//! translated back to discovery spelling so state and index keys stay stable.

use super::{IngestOptions, PathExcluder, build_path_excluder};
use crate::sources::{self, SourceFile};
use crate::state::IngestState;
use crate::types::SourceKind;
use anyhow::Result;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(super) enum DirtySelection {
    Paths {
        files: Vec<SourceFile>,
        databases: Vec<SourceFile>,
    },
    Resync,
}

#[derive(Clone, Copy)]
enum Shape {
    Claude,
    Jsonl(SourceKind),
    CodexHome,
    Opencode,
    Cursor,
    PiSettings,
    OpenClaw,
    Copilot,
    Grok,
    Jcode,
    Muse,
    Antigravity,
}

struct Root {
    lexical: PathBuf,
    canonical: PathBuf,
    shape: Shape,
}

impl Root {
    fn new(lexical: PathBuf, shape: Shape) -> Self {
        let canonical = lexical.canonicalize().unwrap_or_else(|_| lexical.clone());
        Self {
            lexical,
            canonical,
            shape,
        }
    }

    fn remap(&self, path: &Path) -> Option<PathBuf> {
        path.strip_prefix(&self.lexical)
            .or_else(|_| path.strip_prefix(&self.canonical))
            .ok()
            .map(|relative| self.lexical.join(relative))
    }
}

fn roots(options: &IngestOptions) -> Vec<Root> {
    let mut roots = Vec::new();
    for root in &options.claude_sources {
        roots.push(Root::new(root.clone(), Shape::Claude));
    }
    if options.include_codex {
        roots.extend(
            sources::codex::rollout_roots()
                .into_iter()
                .map(|root| Root::new(root, Shape::Jsonl(SourceKind::Codex))),
        );
        roots.extend(
            sources::codex::homes()
                .into_iter()
                .map(|root| Root::new(root, Shape::CodexHome)),
        );
    }
    if options.include_opencode {
        roots.extend(
            sources::opencode::data_roots()
                .into_iter()
                .map(|root| Root::new(root, Shape::Opencode)),
        );
    }
    if options.include_cursor {
        roots.push(Root::new(sources::cursor::projects_root(), Shape::Cursor));
    }
    if options.include_pi {
        roots.push(Root::new(
            sources::pi::sessions_root(),
            Shape::Jsonl(SourceKind::Pi),
        ));
        roots.push(Root::new(sources::pi::agent_root(), Shape::PiSettings));
    }
    if options.include_omp {
        // This helper reads only profile entries, never transcript trees.
        roots.extend(
            sources::omp::session_roots()
                .into_iter()
                .map(|root| Root::new(root, Shape::Jsonl(SourceKind::Omp))),
        );
    }
    if options.include_openclaw {
        roots.extend(
            sources::openclaw::state_dirs()
                .into_iter()
                .map(|root| Root::new(root, Shape::OpenClaw)),
        );
    }
    if options.include_copilot {
        roots.push(Root::new(sources::copilot::session_root(), Shape::Copilot));
    }
    if options.include_grok {
        roots.push(Root::new(sources::grok::session_root(), Shape::Grok));
    }
    if options.include_jcode {
        roots.push(Root::new(sources::jcode::sessions_root(), Shape::Jcode));
    }
    if options.include_muse {
        roots.push(Root::new(sources::muse::sessions_root(), Shape::Muse));
    }
    if options.include_antigravity {
        roots.push(Root::new(
            sources::antigravity::sessions_root(),
            Shape::Antigravity,
        ));
    }
    roots
}

pub(super) fn resolve_dirty(
    options: &IngestOptions,
    dirty: &HashSet<PathBuf>,
    state: &IngestState,
) -> Result<DirtySelection> {
    let excluder = build_path_excluder(options)?;
    resolve(&roots(options), dirty, state, &excluder)
}

enum Match {
    Ignore,
    File(SourceKind),
    Database(PathBuf),
    Resync,
}

fn classify(root: &Root, path: &Path) -> Match {
    let relative = path.strip_prefix(&root.lexical).expect("remapped path");
    let parts = relative.iter().collect::<Vec<_>>();
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    let jsonl = path
        .extension()
        .is_some_and(|extension| extension == "jsonl");
    let source = match root.shape {
        Shape::Claude => {
            let subagents = path
                .ancestors()
                .any(|ancestor| ancestor.file_name().is_some_and(|name| name == "subagents"));
            (jsonl && (!subagents || name.starts_with("agent-")) && (parts.len() <= 2 || subagents))
                .then_some(SourceKind::Claude)
        }
        Shape::Jsonl(source) => jsonl.then_some(source),
        Shape::CodexHome => {
            return if relative == Path::new("history.jsonl") {
                Match::File(SourceKind::Codex)
            } else {
                Match::Ignore
            };
        }
        Shape::Opencode => {
            if parts.first().is_some_and(|part| *part == "storage") {
                // Legacy messages depend on sibling part trees, global session
                // metadata, and database ownership across configured roots.
                return Match::Resync;
            }
            if parts.len() == 1 {
                let database_name = name.strip_suffix("-wal").unwrap_or(name);
                if sources::opencode::is_database_path(database_name) {
                    return Match::Database(path.with_file_name(database_name));
                }
            }
            None
        }
        Shape::Cursor => (jsonl && parts.iter().any(|part| *part == "agent-transcripts"))
            .then_some(SourceKind::Cursor),
        Shape::PiSettings => {
            return if relative == Path::new("settings.json") {
                Match::Resync
            } else {
                Match::Ignore
            };
        }
        Shape::OpenClaw => {
            (parts.len() == 4 && parts[0] == "agents" && parts[2] == "sessions" && jsonl)
                .then_some(SourceKind::OpenClaw)
        }
        Shape::Copilot => {
            if name == "workspace.yaml" {
                return Match::Resync;
            }
            (name == "events.jsonl").then_some(SourceKind::Copilot)
        }
        Shape::Grok => {
            if (2..=3).contains(&parts.len()) && name == "summary.json" {
                return Match::Resync;
            }
            ((2..=3).contains(&parts.len()) && name == "updates.jsonl").then_some(SourceKind::Grok)
        }
        Shape::Jcode => {
            (name.starts_with("session_") && name.ends_with(".json")).then_some(SourceKind::Jcode)
        }
        Shape::Muse => (name == "session.jsonl").then_some(SourceKind::Muse),
        Shape::Antigravity => {
            let in_profile = parts.first().is_some_and(|part| {
                *part == "antigravity-ide"
                    || *part == "antigravity"
                    || *part == "antigravity-backup"
            });
            if !in_profile {
                None
            } else if (name.ends_with(".db")
                && !name.ends_with("-wal.db")
                && !name.ends_with("-shm.db")
                && parts.get(1).is_some_and(|part| *part == "conversations"))
                || (name == "overview.txt"
                    && path.to_string_lossy().contains(".system_generated/logs/"))
            {
                Some(SourceKind::Antigravity)
            } else {
                None
            }
        }
    };
    source.map_or(Match::Ignore, Match::File)
}

fn resolve(
    roots: &[Root],
    dirty: &HashSet<PathBuf>,
    state: &IngestState,
    excluder: &PathExcluder,
) -> Result<DirtySelection> {
    let mut files = Vec::new();
    let mut databases = Vec::new();
    for hint in dirty {
        let mut matches = Vec::new();
        let mut known_unmatched = false;
        for root in roots {
            let Some(path) = root.remap(hint) else {
                continue;
            };
            if excluder.is_excluded(&path) || excluder.is_excluded(hint) {
                continue;
            }
            let path = if matches!(root.shape, Shape::Antigravity) {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.strip_suffix("-wal"))
                    .filter(|name| name.ends_with(".db"))
                    .map(|name| path.with_file_name(name))
                    .unwrap_or(path)
            } else {
                path
            };
            if excluder.is_excluded(&path) {
                continue;
            }
            if path == root.lexical {
                return Ok(DirtySelection::Resync);
            }
            let classification = classify(root, &path);
            let metadata = std::fs::symlink_metadata(&path);
            if matches!(classification, Match::Ignore) {
                // These broad watches only own a specific file (history or
                // settings), or OpenCode's direct DB files and storage tree.
                // A build/worktree/cache directory elsewhere under an agent
                // home must not turn ordinary activity into global discovery.
                if matches!(
                    root.shape,
                    Shape::CodexHome | Shape::PiSettings | Shape::Opencode
                ) && metadata.as_ref().is_ok_and(|metadata| metadata.is_dir())
                {
                    continue;
                }
                // A deleted lock/cache file is noise. A vanished directory
                // containing indexed keys still requires tombstone recovery.
                let key = path.to_string_lossy();
                known_unmatched |= state.files.contains_key(key.as_ref())
                    || state.opencode_databases.contains_key(key.as_ref());
                if metadata.is_err() {
                    known_unmatched |= state
                        .files
                        .keys()
                        .chain(state.opencode_databases.keys())
                        .any(|key| Path::new(key).starts_with(&path));
                }
                if !metadata.as_ref().is_ok_and(|metadata| metadata.is_dir()) {
                    continue;
                }
            }
            // Watchers may coalesce a tree rename into its parent directory.
            // A partial batch cannot infer its complete set of affected keys.
            match metadata {
                Ok(metadata) if metadata.is_file() => {}
                _ => return Ok(DirtySelection::Resync),
            }
            let (candidate, database) = match classification {
                Match::Ignore => continue,
                Match::Resync => return Ok(DirtySelection::Resync),
                Match::File(source) => (SourceFile { source, path }, false),
                Match::Database(path) => {
                    if excluder.is_excluded(&path) {
                        continue;
                    }
                    if !path.is_file() {
                        return Ok(DirtySelection::Resync);
                    }
                    (
                        SourceFile {
                            source: SourceKind::Opencode,
                            path,
                        },
                        true,
                    )
                }
            };
            // WalkDir does not follow nested symlinks. A root symlink is valid,
            // but an interior alias must not introduce an undiscovered file.
            for ancestor in candidate.path.ancestors().skip(1) {
                if ancestor == root.lexical {
                    break;
                }
                if std::fs::symlink_metadata(ancestor)
                    .map(|metadata| metadata.file_type().is_symlink())
                    .unwrap_or(true)
                {
                    return Ok(DirtySelection::Resync);
                }
            }
            if !matches.contains(&(candidate.clone(), database)) {
                matches.push((candidate, database));
            }
        }
        if matches.len() > 1 || (matches.is_empty() && known_unmatched) {
            return Ok(DirtySelection::Resync);
        }
        if let Some((candidate, database)) = matches.pop() {
            let target = if database { &mut databases } else { &mut files };
            if !target.contains(&candidate) {
                target.push(candidate);
            }
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    databases.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(DirtySelection::Paths { files, databases })
}

/// Preserve history's rollout preference using already-known keys plus this
/// batch. No transcript discovery or per-file stats are needed here.
pub(super) fn codex_session_ids(
    options: &IngestOptions,
    state: &IngestState,
    files: &[SourceFile],
) -> HashSet<String> {
    if !options.include_codex {
        return HashSet::new();
    }
    let Ok(excluder) = build_path_excluder(options) else {
        // The caller validates these same patterns before resolving a batch.
        return HashSet::new();
    };
    let rollout_roots = sources::codex::rollout_roots()
        .into_iter()
        .map(|root| Root::new(root, Shape::Jsonl(SourceKind::Codex)))
        .collect::<Vec<_>>();
    state
        .files
        .keys()
        .map(Path::new)
        .chain(
            files
                .iter()
                .filter(|file| file.source == SourceKind::Codex)
                .map(|file| file.path.as_path()),
        )
        .filter(|path| !sources::codex::is_history_path(path))
        .filter(|path| {
            rollout_roots.iter().any(|root| {
                root.remap(path).is_some_and(|mapped| {
                    mapped
                        .extension()
                        .is_some_and(|extension| extension == "jsonl")
                        && !excluder.is_excluded(&mapped)
                })
            })
        })
        .filter_map(sources::codex::session_id_from_path)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{IndexedToolContentLimits, UserConfig};
    use crate::state::FileState;
    use crate::test_support::{EnvVarGuard, env_lock};

    fn options() -> IngestOptions {
        IngestOptions {
            claude_sources: Vec::new(),
            include_agents: false,
            include_reasoning: false,
            include_codex: false,
            include_opencode: false,
            include_cursor: false,
            include_pi: false,
            include_omp: false,
            include_openclaw: false,
            include_copilot: false,
            include_grok: false,
            include_jcode: false,
            include_muse: false,
            include_antigravity: false,
            exclude_patterns: Vec::new(),
            embeddings: false,
            backfill_embeddings: false,
            model: Default::default(),
            embed_runtime: UserConfig::default().resolve_embed_runtime().unwrap(),
            tool_content_limits: IndexedToolContentLimits::default(),
        }
    }

    fn write(path: &Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "{}\n").unwrap();
    }

    fn select(roots: &[Root], path: &Path) -> DirtySelection {
        resolve(
            roots,
            &HashSet::from([path.to_path_buf()]),
            &IngestState::default(),
            &PathExcluder::build(&[]).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn direct_selection_matches_all_source_discovery_shapes() {
        let cases = [
            (Shape::Claude, "project/session.jsonl", SourceKind::Claude),
            (
                Shape::Claude,
                "project/session/subagents/agent-worker.jsonl",
                SourceKind::Claude,
            ),
            (
                Shape::Jsonl(SourceKind::Codex),
                "2026/rollout.jsonl",
                SourceKind::Codex,
            ),
            (Shape::CodexHome, "history.jsonl", SourceKind::Codex),
            (
                Shape::Cursor,
                "project/agent-transcripts/id/worker.jsonl",
                SourceKind::Cursor,
            ),
            (
                Shape::Jsonl(SourceKind::Pi),
                "project/session.jsonl",
                SourceKind::Pi,
            ),
            (
                Shape::Jsonl(SourceKind::Omp),
                "project/session.jsonl",
                SourceKind::Omp,
            ),
            (
                Shape::OpenClaw,
                "agents/main/sessions/session.jsonl",
                SourceKind::OpenClaw,
            ),
            (Shape::Copilot, "session/events.jsonl", SourceKind::Copilot),
            (Shape::Grok, "session/updates.jsonl", SourceKind::Grok),
            (
                Shape::Grok,
                "project/session/updates.jsonl",
                SourceKind::Grok,
            ),
            (Shape::Jcode, "project/session_one.json", SourceKind::Jcode),
            (Shape::Muse, "project/id/session.jsonl", SourceKind::Muse),
            (
                Shape::Antigravity,
                "antigravity-ide/conversations/abc.db",
                SourceKind::Antigravity,
            ),
            (
                Shape::Antigravity,
                "antigravity-ide/brain/comp/.system_generated/logs/overview.txt",
                SourceKind::Antigravity,
            ),
        ];
        for (shape, relative, source) in cases {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(relative);
            write(&path);
            let DirtySelection::Paths { files, databases } =
                select(&[Root::new(temp.path().to_path_buf(), shape)], &path)
            else {
                panic!("unexpected resync for {relative}")
            };
            assert_eq!(files, vec![SourceFile { source, path }]);
            assert!(databases.is_empty());
        }
        for name in ["opencode.db", "opencode-work.db"] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(name);
            write(&path);
            let wal = temp.path().join(format!("{name}-wal"));
            write(&wal);
            for hint in [&path, &wal] {
                let DirtySelection::Paths { files, databases } = select(
                    &[Root::new(temp.path().to_path_buf(), Shape::Opencode)],
                    hint,
                ) else {
                    panic!("unexpected database resync")
                };
                assert!(files.is_empty());
                assert_eq!(
                    databases,
                    vec![SourceFile {
                        source: SourceKind::Opencode,
                        path: path.clone()
                    }]
                );
            }
        }
    }

    #[test]
    fn selection_ignores_files_outside_discovery_shapes() {
        let cases = [
            (Shape::Claude, "project/deep/session.jsonl"),
            (Shape::Claude, "project/id/subagents/workflow.jsonl"),
            (Shape::Cursor, "project/session.jsonl"),
            (Shape::OpenClaw, "agents/main/sessions/nested/session.jsonl"),
            (Shape::Copilot, "id/notes.jsonl"),
            (Shape::Grok, "updates.jsonl"),
            (Shape::Grok, "a/b/c/updates.jsonl"),
            (Shape::Jcode, "notes.json"),
            (Shape::Muse, "notes.jsonl"),
            (Shape::Opencode, "nested/opencode.db"),
            (Shape::Opencode, "other.db-wal"),
            (Shape::CodexHome, "settings.json"),
            (Shape::PiSettings, "other.json"),
        ];
        for (shape, relative) in cases {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(relative);
            write(&path);
            assert!(
                matches!(select(&[Root::new(temp.path().to_path_buf(), shape)], &path),
                DirtySelection::Paths { files, databases } if files.is_empty() && databases.is_empty())
            );
        }
    }

    #[test]
    fn unrelated_directories_under_agent_homes_do_not_force_resync() {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("worktrees/project/target");
        std::fs::create_dir_all(&directory).unwrap();
        for shape in [Shape::CodexHome, Shape::PiSettings, Shape::Opencode] {
            assert!(
                matches!(select(&[Root::new(temp.path().to_path_buf(), shape)], &directory),
                DirtySelection::Paths { files, databases } if files.is_empty() && databases.is_empty())
            );
        }
    }

    #[test]
    fn selection_reconciles_dependencies_deletions_and_overlap() {
        for (shape, relative) in [
            (Shape::Opencode, "storage/message/ses_a/message.json"),
            (Shape::Opencode, "storage/part/msg_a/part.json"),
            (Shape::Opencode, "storage/session/project/ses_a.json"),
            (Shape::PiSettings, "settings.json"),
            (Shape::Copilot, "id/workspace.yaml"),
            (Shape::Grok, "id/summary.json"),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(relative);
            write(&path);
            assert!(matches!(
                select(&[Root::new(temp.path().to_path_buf(), shape)], &path),
                DirtySelection::Resync
            ));
        }
        let temp = tempfile::tempdir().unwrap();
        let root = Root::new(temp.path().to_path_buf(), Shape::Jsonl(SourceKind::Pi));
        assert!(matches!(
            select(&[root], &temp.path().join("removed.jsonl")),
            DirtySelection::Resync
        ));
        let path = temp.path().join("session.jsonl");
        write(&path);
        let roots = [
            Root::new(temp.path().to_path_buf(), Shape::Jsonl(SourceKind::Pi)),
            Root::new(temp.path().to_path_buf(), Shape::Jsonl(SourceKind::Omp)),
        ];
        assert!(matches!(select(&roots, &path), DirtySelection::Resync));
        assert!(matches!(
            select(&roots, temp.path()),
            DirtySelection::Resync
        ));
    }

    #[cfg(unix)]
    #[test]
    fn selection_preserves_lexical_root_alias_and_exclusions() {
        let temp = tempfile::tempdir().unwrap();
        let real = temp.path().join("real");
        let path = real.join("session.jsonl");
        write(&path);
        let alias = temp.path().join("alias");
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        let roots = [Root::new(alias.clone(), Shape::Jsonl(SourceKind::Pi))];
        let canonical_hint = path.canonicalize().unwrap();
        let DirtySelection::Paths { files, .. } = select(&roots, &canonical_hint) else {
            panic!("root alias should resolve")
        };
        assert_eq!(files[0].path, alias.join("session.jsonl"));
        let exclusions =
            PathExcluder::build(&[alias.join("**").to_string_lossy().into_owned()]).unwrap();
        assert!(matches!(resolve(&roots, &HashSet::from([canonical_hint]),
            &IngestState::default(), &exclusions).unwrap(), DirtySelection::Paths { files, databases }
            if files.is_empty() && databases.is_empty()));
    }

    #[test]
    fn selection_uses_overrides_and_preserves_known_codex_history_dedupe() {
        let _lock = env_lock();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("custom-codex");
        let sessions = temp.path().join("custom-pi");
        let _env = EnvVarGuard::set(&[
            ("CODEX_HOME", Some(home.to_str().unwrap())),
            (
                "PI_CODING_AGENT_SESSION_DIR",
                Some(sessions.to_str().unwrap()),
            ),
        ]);
        let known_id = "11111111-1111-1111-1111-111111111111";
        let new_id = "22222222-2222-2222-2222-222222222222";
        let known = home.join(format!("sessions/rollout-{known_id}.jsonl"));
        let new = home.join(format!("archived_sessions/rollout-{new_id}.jsonl"));
        let history = home.join("history.jsonl");
        let pi = sessions.join("project/session.jsonl");
        for path in [&known, &new, &history, &pi] {
            write(path);
        }
        let mut options = options();
        options.include_codex = true;
        options.include_pi = true;
        let mut state = IngestState::default();
        state.files.insert(
            known.to_string_lossy().into_owned(),
            FileState {
                size: 3,
                mtime: 0,
                offset: 3,
                turn_id: 0,
                parser_version: 0,
                pending_tool_calls: Default::default(),
                identity: Default::default(),
            },
        );
        let DirtySelection::Paths { files, databases } = resolve_dirty(
            &options,
            &HashSet::from([history.clone(), new.clone(), pi.clone()]),
            &state,
        )
        .unwrap() else {
            panic!("overridden sources should resolve directly")
        };
        assert!(databases.is_empty());
        assert_eq!(files.len(), 3);
        assert!(files.contains(&SourceFile {
            source: SourceKind::Pi,
            path: pi
        }));
        assert_eq!(
            codex_session_ids(&options, &state, &files),
            HashSet::from([known_id.to_string(), new_id.to_string()])
        );
        options.exclude_patterns = vec![known.to_string_lossy().into_owned()];
        assert_eq!(
            codex_session_ids(&options, &state, &files),
            HashSet::from([new_id.to_string()])
        );
    }

    #[test]
    fn selection_matches_actual_discovery_for_overridden_sources() {
        let _lock = env_lock();
        let temp = tempfile::tempdir().unwrap();
        let claude = temp.path().join("claude");
        let codex = temp.path().join("codex");
        let pi = temp.path().join("pi");
        let opencode = temp.path().join("opencode");
        let _env = EnvVarGuard::set(&[
            ("CODEX_HOME", Some(codex.to_str().unwrap())),
            ("PI_CODING_AGENT_SESSION_DIR", Some(pi.to_str().unwrap())),
            ("OPENCODE_DATA_DIR", Some(opencode.to_str().unwrap())),
        ]);
        let mut dirty = HashSet::new();
        for (root, relative) in [
            (&claude, "project/main.jsonl"),
            (&claude, "project/deep/ignored.jsonl"),
            (&claude, "project/id/subagents/agent-child.jsonl"),
            (&claude, "project/id/subagents/workflow.jsonl"),
            (&claude, "project/notes.txt"),
            (&codex, "sessions/2026/rollout.jsonl"),
            (&codex, "archived_sessions/old.jsonl"),
            (&codex, "outside.jsonl"),
            (&codex, "history.jsonl"),
            (&pi, "project/deep/session.jsonl"),
            (&pi, "project/notes.txt"),
            (&opencode, "opencode.db"),
            (&opencode, "opencode-work.db"),
            (&opencode, "nested/opencode-ignored.db"),
            (&opencode, "other.db"),
        ] {
            let path = root.join(relative);
            write(&path);
            dirty.insert(path);
        }
        let mut options = options();
        options.claude_sources = vec![claude.clone()];
        options.include_codex = true;
        options.include_pi = true;
        options.include_opencode = true;
        let DirtySelection::Paths { files, databases } =
            resolve_dirty(&options, &dirty, &IngestState::default()).unwrap()
        else {
            panic!("regular source files should resolve directly")
        };
        let mut discovered = sources::claude::discover(&claude, false).unwrap();
        discovered.extend(sources::codex::discover_rollouts());
        discovered.extend(
            sources::codex::history_paths()
                .into_iter()
                .map(|path| SourceFile {
                    source: SourceKind::Codex,
                    path,
                }),
        );
        discovered.extend(sources::pi::discover());
        discovered.sort_by(|left, right| left.path.cmp(&right.path));
        assert_eq!(files, discovered);
        assert_eq!(databases, sources::opencode::discover_databases().unwrap());
    }

    #[test]
    fn selection_matches_codex_discovery_without_sessions_subdirectory() {
        let _lock = env_lock();
        let temp = tempfile::tempdir().unwrap();
        let _env = EnvVarGuard::set(&[("CODEX_HOME", Some(temp.path().to_str().unwrap()))]);
        let dirty = HashSet::from([
            temp.path().join("rollout.jsonl"),
            temp.path().join("history.jsonl"),
        ]);
        for path in &dirty {
            write(path);
        }
        let mut options = options();
        options.include_codex = true;
        let DirtySelection::Paths { files, databases } =
            resolve_dirty(&options, &dirty, &IngestState::default()).unwrap()
        else {
            panic!("fallback Codex root should resolve directly")
        };
        assert_eq!(files, sources::codex::discover_rollouts());
        assert!(databases.is_empty());
    }

    #[test]
    fn nonexistent_noise_is_ignored_but_indexed_deletions_reconcile() {
        let temp = tempfile::tempdir().unwrap();
        let root = Root::new(temp.path().to_path_buf(), Shape::CodexHome);
        let noise = temp.path().join("removed-cache.tmp");
        assert!(
            matches!(select(&[root], &noise), DirtySelection::Paths { files, databases }
            if files.is_empty() && databases.is_empty())
        );
        let mut state = IngestState::default();
        state
            .opencode_databases
            .insert(noise.to_string_lossy().into_owned(), Default::default());
        let root = Root::new(temp.path().to_path_buf(), Shape::CodexHome);
        assert!(matches!(
            resolve(
                &[root],
                &HashSet::from([noise]),
                &state,
                &PathExcluder::build(&[]).unwrap()
            )
            .unwrap(),
            DirtySelection::Resync
        ));
    }
}
