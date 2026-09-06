use memex::{
    config::Paths,
    memory::{MemoryDiscoveryOptions, MemoryStore},
    memory_search::{MemoryReadRequest, MemorySearchOptions, read_memory, search_memory},
    types::SourceKind,
};
use serde_json::json;
use std::{collections::HashSet, fs, path::Path, process::Command};

fn write(path: &Path, content: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, content).unwrap();
}

fn git(cwd: &Path, args: &[&str]) {
    let result = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&result.stderr)
    );
}

fn options(root: &Path) -> MemoryDiscoveryOptions {
    MemoryDiscoveryOptions {
        claude_project_roots: vec![root.join("claude/projects")],
        codex_homes: vec![root.join("codex")],
        enabled_sources: HashSet::from([SourceKind::Claude, SourceKind::Codex]),
        exclude_patterns: vec![],
    }
}

fn claude_note(root: &Path, name: &str, cwd: &Path) {
    let project = root.join("claude/projects").join(name);
    write(
        &project.join("session.jsonl"),
        &format!(
            "{}\n",
            json!({
                "type": "user", "sessionId": name, "cwd": cwd,
                "message": {"role": "user", "content": "scope"}
            })
        ),
    );
    write(
        &project.join("memory/MEMORY.md"),
        "# Decision\n\ncontinuity evidence\n",
    );
}

fn codex_note(root: &Path, name: &str, cwd: &Path) {
    write(
        &root
            .join("codex/memories/rollout_summaries")
            .join(format!("{name}.md")),
        &format!(
            "cwd: {}\n\n# Decision\n\ncontinuity evidence\n",
            cwd.display()
        ),
    );
}

#[test]
fn memory_project_search_groups_worktrees_and_refreshes_existing_scopes() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let repo = root.join("memex");
    fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--allow-empty",
            "-m",
            "fixture",
            "--quiet",
        ],
    );
    let first = root.join("exciting-morse");
    let second = root.join("sleepy-curie");
    for (branch, path) in [("first", &first), ("second", &second)] {
        git(
            &repo,
            &[
                "worktree",
                "add",
                "--quiet",
                "-b",
                branch,
                path.to_str().unwrap(),
            ],
        );
    }
    for (name, cwd) in [("main", &repo), ("first", &first), ("second", &second)] {
        claude_note(root, name, cwd);
        codex_note(root, name, cwd);
    }
    let paths = Paths::new(Some(root.join("store"))).unwrap();
    let store = MemoryStore::new(paths.root.join("memory/documents.json"));
    let discovery = options(root);
    let original = store.refresh(&discovery).unwrap().snapshot;
    assert_eq!(original.documents.len(), 6);
    assert!(
        original
            .documents
            .iter()
            .all(|doc| doc.scope.project.as_deref() == Some("memex"))
    );
    let query = MemorySearchOptions {
        query: "continuity".into(),
        project: Some("memex".into()),
        recency_weight: 0.0,
        ..Default::default()
    };
    let all = search_memory(&paths, &query).unwrap();
    assert_eq!(all.len(), 6);
    assert_eq!(
        all.iter()
            .map(|hit| &hit.memory_id)
            .collect::<HashSet<_>>()
            .len(),
        6
    );
    let checkout = search_memory(
        &paths,
        &MemorySearchOptions {
            cwd: Some(first.canonicalize().unwrap()),
            ..query
        },
    )
    .unwrap();
    assert_eq!(checkout.len(), 2);
    for hit in &checkout {
        let read = read_memory(
            &paths,
            &MemoryReadRequest {
                memory_id: hit.memory_id.clone(),
                section_ref: Some(hit.section_ref.clone()),
                content_version: Some(hit.content_version.clone()),
                offset_chars: 0,
                max_chars: 128,
            },
        )
        .unwrap();
        assert_eq!(read.project.as_deref(), Some("memex"));
        assert_eq!(read.cwd, Some(first.canonicalize().unwrap()));
        assert!(read.text.contains("continuity"));
    }

    // Upgrade a pre-resolver snapshot without touching the source memories.
    let mut legacy = original.clone();
    for doc in &mut legacy.documents {
        doc.scope.project = doc
            .scope
            .cwd
            .as_ref()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .map(str::to_string);
    }
    fs::write(store.snapshot_path(), serde_json::to_vec(&legacy).unwrap()).unwrap();
    let refreshed = store.refresh(&discovery).unwrap();
    assert!(refreshed.changed);
    assert_eq!(refreshed.unchanged, 6);
    assert_eq!(refreshed.parsed, 0);
    assert_eq!(refreshed.snapshot, original);
    assert!(!store.refresh(&discovery).unwrap().changed);
}

#[test]
fn memory_scope_preserves_non_git_and_global_notes_with_deleted_worktree_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let missing = root.join("atm-backend/.claude/worktrees/exciting-morse");
    let ordinary = root.join("research-notes");
    fs::create_dir(&ordinary).unwrap();
    claude_note(root, "missing", &missing);
    claude_note(root, "ordinary", &ordinary);
    codex_note(root, "missing", &missing);
    codex_note(root, "ordinary", &ordinary);
    write(
        &root.join("codex/memories/MEMORY.md"),
        "# Global\n\ncontinuity evidence\n",
    );
    let paths = Paths::new(Some(root.join("store"))).unwrap();
    let store = MemoryStore::new(paths.root.join("memory/documents.json"));
    let docs = store.refresh(&options(root)).unwrap().snapshot.documents;
    assert_eq!(
        docs.iter()
            .filter(|doc| doc.scope.project.as_deref() == Some("atm-backend"))
            .count(),
        2
    );
    assert_eq!(
        docs.iter()
            .filter(|doc| doc.scope.project.as_deref() == Some("research-notes"))
            .count(),
        2
    );
    let global = docs.iter().find(|doc| doc.scope.cwd.is_none()).unwrap();
    assert_eq!(global.scope.project, None);
}

#[test]
fn live_codex_read_uses_current_summary_scope() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let old = root.join("old-project");
    let new = root.join("new-project");
    fs::create_dir(&old).unwrap();
    fs::create_dir(&new).unwrap();
    codex_note(root, "summary", &old);
    let paths = Paths::new(Some(root.join("store"))).unwrap();
    let store = MemoryStore::new(paths.root.join("memory/documents.json"));
    let doc = store
        .refresh(&options(root))
        .unwrap()
        .snapshot
        .documents
        .remove(0);
    codex_note(root, "summary", &new);
    let current = read_memory(
        &paths,
        &MemoryReadRequest {
            memory_id: doc.stable_id,
            section_ref: None,
            content_version: Some(doc.version_sha256),
            offset_chars: 0,
            max_chars: 256,
        },
    )
    .unwrap();
    assert!(current.changed_since_search);
    assert_eq!(current.project.as_deref(), Some("new-project"));
    assert_eq!(current.cwd, Some(new.canonicalize().unwrap()));
}
