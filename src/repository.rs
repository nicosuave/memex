use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RepositoryFacts {
    pub worktree: Option<PathBuf>,
    pub git_dir: PathBuf,
    pub common_dir: PathBuf,
    pub project: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RepositoryResolution {
    Found(RepositoryFacts),
    NotRepository {
        project_hint: Option<String>,
    },
    Missing {
        project_hint: Option<String>,
    },
    Unavailable {
        project_hint: Option<String>,
        error: String,
    },
}

impl RepositoryResolution {
    pub fn project(&self) -> Option<&str> {
        match self {
            Self::Found(facts) => facts.project.as_deref(),
            Self::NotRepository { project_hint }
            | Self::Missing { project_hint }
            | Self::Unavailable { project_hint, .. } => project_hint.as_deref(),
        }
    }
}

type ResolutionSlot = Arc<OnceLock<RepositoryResolution>>;

#[derive(Default)]
pub(crate) struct RepositoryResolver {
    directories: Mutex<HashMap<PathBuf, ResolutionSlot>>,
}

impl RepositoryResolver {
    pub fn resolve(&self, cwd: &Path) -> RepositoryResolution {
        let slot = {
            let mut directories = self.directories.lock().unwrap();
            Arc::clone(directories.entry(cwd.to_path_buf()).or_default())
        };
        slot.get_or_init(|| discover(cwd)).clone()
    }
}

fn discover(cwd: &Path) -> RepositoryResolution {
    crate::profiling::span!("repository.discover");
    crate::profiling::count!("repository.discoveries", 1);
    let project_hint =
        claude_worktree_repo_project(cwd).or_else(|| codex_worktree_repo_project(cwd));
    let directory = match std::fs::canonicalize(cwd) {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return RepositoryResolution::Missing { project_hint };
        }
        Err(error) => {
            return RepositoryResolution::Unavailable {
                project_hint,
                error: error.to_string(),
            };
        }
    };
    match gix::discover(&directory) {
        Ok(repo) => {
            let worktree = repo.workdir().map(canonical);
            let common_dir = canonical(repo.common_dir());
            let project = common_dir_project_name(&common_dir)
                .or_else(|| worktree.as_deref().and_then(path_name))
                .or(project_hint);
            RepositoryResolution::Found(RepositoryFacts {
                worktree,
                git_dir: canonical(repo.git_dir()),
                common_dir,
                project,
            })
        }
        Err(gix::discover::Error::Discover(
            gix::discover::upwards::Error::NoGitRepository { .. }
            | gix::discover::upwards::Error::NoGitRepositoryWithinCeiling { .. }
            | gix::discover::upwards::Error::NoGitRepositoryWithinFs { .. },
        )) => RepositoryResolution::NotRepository { project_hint },
        Err(error) => RepositoryResolution::Unavailable {
            project_hint,
            error: error.to_string(),
        },
    }
}

fn canonical(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

pub(crate) fn claude_worktree_repo_project(cwd: &Path) -> Option<String> {
    for ancestor in cwd.ancestors() {
        if ancestor.file_name().and_then(|name| name.to_str()) != Some("worktrees") {
            continue;
        }
        let parent = ancestor.parent()?;
        if parent.file_name()?.to_str()? == ".claude" {
            return parent.parent().and_then(path_name);
        }
    }
    None
}

pub(crate) fn codex_worktree_repo_project(cwd: &Path) -> Option<String> {
    for ancestor in cwd.ancestors() {
        if ancestor.file_name().and_then(|name| name.to_str()) != Some("worktrees") {
            continue;
        }
        let parent = ancestor.parent()?;
        if parent.file_name()?.to_str()? == ".codex" {
            let mut relative = cwd.strip_prefix(ancestor).ok()?.components();
            relative.next()?;
            return relative.next()?.as_os_str().to_str().map(str::to_owned);
        }
    }
    None
}

pub(crate) fn common_dir_project_name(path: &Path) -> Option<String> {
    if path.file_name().is_some_and(|name| name == ".git") {
        path.parent().and_then(path_name)
    } else {
        path_name(path)
    }
}

fn path_name(path: &Path) -> Option<String> {
    path.file_name()?
        .to_str()
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn git(root: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn normal_unborn_bare_and_nested_repositories() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("project");
        std::fs::create_dir_all(root.join("nested")).unwrap();
        git(&root, &["init", "-q"]);
        let resolver = RepositoryResolver::default();
        let RepositoryResolution::Found(facts) = resolver.resolve(&root.join("nested")) else {
            panic!("repository")
        };
        assert_eq!(facts.worktree, Some(root.canonicalize().unwrap()));
        assert_eq!(facts.project.as_deref(), Some("project"));
        git(temp.path(), &["init", "--bare", "-q", "bare.git"]);
        let RepositoryResolution::Found(bare) = resolver.resolve(&temp.path().join("bare.git"))
        else {
            panic!("bare repository")
        };
        assert!(bare.worktree.is_none());
        assert_eq!(bare.project.as_deref(), Some("bare.git"));
    }

    #[test]
    fn linked_worktree_uses_shared_repository_name() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("project");
        std::fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        git(
            &root,
            &[
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "--allow-empty",
                "-qm",
                "initial",
            ],
        );
        let worktree = temp.path().join("checkout");
        git(
            &root,
            &[
                "worktree",
                "add",
                "-qb",
                "fixture",
                worktree.to_str().unwrap(),
            ],
        );
        let resolver = RepositoryResolver::default();
        let RepositoryResolution::Found(facts) = resolver.resolve(&worktree) else {
            panic!("worktree")
        };
        assert_eq!(facts.project.as_deref(), Some("project"));
        assert_eq!(facts.worktree, Some(worktree.canonicalize().unwrap()));
        assert_eq!(facts.common_dir, root.join(".git").canonicalize().unwrap());
    }

    #[test]
    fn submodule_has_its_own_worktree_and_common_directory() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("parent");
        let child = temp.path().join("source");
        std::fs::create_dir(&parent).unwrap();
        std::fs::create_dir(&child).unwrap();
        for root in [&parent, &child] {
            git(root, &["init", "-q"]);
            git(
                root,
                &[
                    "-c",
                    "user.name=Fixture",
                    "-c",
                    "user.email=fixture@example.invalid",
                    "commit",
                    "--allow-empty",
                    "-qm",
                    "initial",
                ],
            );
        }
        git(
            &parent,
            &[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                "-q",
                child.to_str().unwrap(),
                "component",
            ],
        );
        let root = parent.join("component");
        let RepositoryResolution::Found(facts) = RepositoryResolver::default().resolve(&root)
        else {
            panic!("submodule")
        };
        assert_eq!(facts.worktree, Some(root.canonicalize().unwrap()));
        assert_eq!(
            facts.common_dir,
            parent
                .join(".git/modules/component")
                .canonicalize()
                .unwrap()
        );
        assert_eq!(facts.project.as_deref(), Some("component"));
    }

    #[test]
    fn discovery_does_not_require_a_git_executable() {
        let temp = tempfile::tempdir().unwrap();
        git(temp.path(), &["init", "-q"]);
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "repository::tests::resolve_without_git_child"])
            .env("MEMEX_TEST_REPOSITORY", temp.path())
            .env("PATH", "")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    #[test]
    fn resolve_without_git_child() {
        let Some(path) = std::env::var_os("MEMEX_TEST_REPOSITORY") else {
            return;
        };
        assert!(matches!(
            RepositoryResolver::default().resolve(Path::new(&path)),
            RepositoryResolution::Found(_)
        ));
    }

    #[test]
    fn missing_and_ordinary_directories_are_distinct() {
        let temp = tempfile::tempdir().unwrap();
        let resolver = RepositoryResolver::default();
        assert!(matches!(
            resolver.resolve(temp.path()),
            RepositoryResolution::NotRepository { .. }
        ));
        let missing = temp.path().join("project/.claude/worktrees/branch/subdir");
        let facts = resolver.resolve(&missing);
        assert!(matches!(facts, RepositoryResolution::Missing { .. }));
        assert_eq!(facts.project(), Some("project"));
    }

    #[test]
    fn resolution_is_shared_by_concurrent_consumers_within_one_operation() {
        let temp = tempfile::tempdir().unwrap();
        let resolver = RepositoryResolver::default();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    resolver.resolve(temp.path());
                });
            }
        });
        assert_eq!(resolver.directories.lock().unwrap().len(), 1);
        git(temp.path(), &["init", "-q"]);
        assert!(matches!(
            resolver.resolve(temp.path()),
            RepositoryResolution::NotRepository { .. }
        ));
        assert!(matches!(
            RepositoryResolver::default().resolve(temp.path()),
            RepositoryResolution::Found(_)
        ));
    }
}
