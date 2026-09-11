use super::*;
use crate::directory_inventory::DiscoveryInventory;

#[test]
fn directory_inventory_discovery_matches_full_walk_after_tree_changes() {
    let _lock = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("projects");
    let codex = temp.path().join("codex");
    let pi = temp.path().join("pi");
    let omp = temp.path().join("omp");
    let _env = EnvVarGuard::set_os(&[
        ("CODEX_HOME", Some(codex.as_os_str())),
        ("PI_CODING_AGENT_SESSION_DIR", Some(pi.as_os_str())),
        ("PI_CODING_AGENT_DIR", Some(omp.as_os_str())),
    ]);
    for path in [
        root.join("project/session.jsonl"),
        root.join("project/subagents/agent-one.jsonl"),
        root.join("project/subagents/journal.jsonl"),
        root.join("project/ignored/deep.jsonl"),
        codex.join("sessions/2026/09/session.jsonl"),
        codex.join("archived_sessions/empty.jsonl"),
        pi.join("project/session.jsonl"),
        omp.join("sessions/project/session.jsonl"),
    ] {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, "").unwrap();
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(root.join("project"), root.join("linked")).unwrap();
        std::os::unix::fs::symlink(root.join("project/session.jsonl"), root.join("link.jsonl"))
            .unwrap();
    }
    let compare = |inventory: &mut DiscoveryInventory| {
        let mut expected = crate::sources::claude::discover(&root, false).unwrap();
        expected.extend(crate::sources::codex::discover_rollouts());
        expected.extend(crate::sources::pi::discover());
        expected.extend(crate::sources::omp::discover());
        let mut actual =
            crate::sources::claude::discover_with_inventory(&root, false, inventory).unwrap();
        actual.extend(crate::sources::codex::discover_rollouts_with_inventory(inventory).unwrap());
        actual.extend(crate::sources::pi::discover_with_inventory(inventory).unwrap());
        actual.extend(crate::sources::omp::discover_with_inventory(inventory).unwrap());
        let keys = |files: Vec<crate::sources::SourceFile>| {
            files
                .into_iter()
                .map(|file| (file.source, file.path))
                .collect::<Vec<_>>()
        };
        assert_eq!(keys(actual), keys(expected));
    };
    std::thread::sleep(Duration::from_secs(2));
    let mut inventory = DiscoveryInventory::new(None, b"fixture");
    compare(&mut inventory);
    let mut inventory = DiscoveryInventory::new(inventory.finish(), b"fixture");
    compare(&mut inventory);
    #[cfg(target_os = "macos")]
    assert!(inventory.counters().directories_reused > 0);
    let previous = inventory.finish();
    fs::rename(
        root.join("project/session.jsonl"),
        root.join("project/renamed.jsonl"),
    )
    .unwrap();
    fs::create_dir_all(root.join("new-project")).unwrap();
    fs::write(root.join("new-project/new.jsonl"), "").unwrap();
    fs::rename(
        codex.join("sessions/2026/09"),
        codex.join("sessions/2026/10"),
    )
    .unwrap();
    fs::write(pi.join("new.jsonl"), "").unwrap();
    compare(&mut DiscoveryInventory::new(previous, b"fixture"));
}

#[test]
fn directory_inventory_full_scan_persists_and_targeted_scan_preserves_cache() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("projects");
    fs::create_dir_all(root.join("project")).unwrap();
    let path = root.join("project/session.jsonl");
    fs::write(
        &path,
        "{\"type\":\"user\",\"message\":{\"content\":\"original\"}}\n",
    )
    .unwrap();
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let lease = ingest_lease(&paths);
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![root];
    std::thread::sleep(Duration::from_secs(2));
    ingest_all(&paths, &open_search_index(&paths), &options, &lease).unwrap();
    let cache_path = paths.state.join("scan_cache.json");
    let cache = ScanCache::load(&cache_path).unwrap();
    #[cfg(target_os = "macos")]
    assert!(cache.directory_inventory.is_some());
    assert_eq!(cache.file_count, 1);
    let before = fs::read(&cache_path).unwrap();
    let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(
        file,
        "{{\"type\":\"user\",\"message\":{{\"content\":\"appended\"}}}}"
    )
    .unwrap();
    let report = ingest_dirty(
        &paths,
        &open_search_index(&paths),
        &options,
        &lease,
        &HashSet::from([path.clone()]),
    )
    .unwrap();
    assert!(!report.full_scan);
    assert_eq!(report.report.records_added, 1);
    assert_eq!(fs::read(&cache_path).unwrap(), before);
    let report = ingest_if_stale(&paths, &open_search_index(&paths), &options, 0, &lease)
        .unwrap()
        .unwrap();
    assert_eq!(report.files_scanned, 1);
    assert_eq!(report.records_added, 0);
    assert_eq!(indexed_texts(&paths), ["appended", "original"]);
    let modified = fs::metadata(&path).unwrap().modified().unwrap();
    let rewritten = fs::read_to_string(&path)
        .unwrap()
        .replace("original", "modified");
    fs::write(&path, rewritten).unwrap();
    file.set_times(fs::FileTimes::new().set_modified(modified))
        .unwrap();
    let report = ingest_if_stale(&paths, &open_search_index(&paths), &options, 0, &lease)
        .unwrap()
        .unwrap();
    assert_eq!(report.records_added, 2);
    assert_eq!(indexed_texts(&paths), ["appended", "modified"]);
}
