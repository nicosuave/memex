use super::*;
use std::cell::Cell;

#[test]
fn cleanup_current_only_generation_does_not_sync() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir_all(temp.path().join(GENERATIONS_DIR).join("current")).unwrap();
    prune_superseded_generations_with_sync(temp.path(), "current", |_| {
        panic!("unchanged generations must not synchronize")
    })
    .unwrap();
}

#[cfg(unix)]
#[test]
fn cleanup_keeps_leased_and_ambiguous_generations_without_syncing() {
    let temp = tempfile::tempdir().unwrap();
    let generations = temp.path().join(GENERATIONS_DIR);
    fs::create_dir_all(generations.join("current")).unwrap();
    let old = generations.join(new_generation_name());
    let staging = generations.join(format!(".{}.tmp", new_generation_name()));
    let ambiguous = generations.join(format!(".{}.tmp", new_generation_name()));
    let offline = generations.join(".gc-reserved");
    for path in [&old, &staging, &ambiguous, &offline] {
        fs::create_dir(path).unwrap();
    }
    for path in [&old, &staging, &offline] {
        create_generation_lease_file(path).unwrap();
    }
    let old_lease = acquire_generation_lease(&old).unwrap();
    let staging_lease = acquire_generation_lease(&staging).unwrap();
    prune_superseded_generations_with_sync(temp.path(), "current", |_| {
        panic!("retained generations must not synchronize")
    })
    .unwrap();
    assert!(old.exists() && staging.exists() && ambiguous.exists() && offline.exists());

    drop(old_lease);
    drop(staging_lease);
    let syncs = Cell::new(0);
    prune_superseded_generations_with_sync(temp.path(), "current", |path| {
        assert_eq!(path, generations);
        syncs.set(syncs.get() + 1);
        Ok(())
    })
    .unwrap();
    assert_eq!(syncs.get(), 1);
    assert!(!old.exists() && !staging.exists());
    assert!(ambiguous.exists() && offline.exists());
}

#[test]
fn cleanup_legacy_syncs_only_after_removal_and_propagates_sync_errors() {
    let temp = tempfile::tempdir().unwrap();
    fs::write(temp.path().join(CURRENT_FILE), b"current\n").unwrap();
    fs::create_dir(temp.path().join(GENERATIONS_DIR)).unwrap();
    prune_legacy_index_files_with_sync(temp.path(), |_| {
        panic!("unchanged root must not synchronize")
    })
    .unwrap();

    let legacy = temp.path().join("old.store");
    fs::write(&legacy, b"unreachable").unwrap();
    let syncs = Cell::new(0);
    let error = prune_legacy_index_files_with_sync(temp.path(), |path| {
        assert_eq!(path, temp.path());
        syncs.set(syncs.get() + 1);
        Err(io::Error::other("cleanup sync fault"))
    })
    .unwrap_err();
    assert_eq!(error.to_string(), "cleanup sync fault");
    assert_eq!(syncs.get(), 1);
    assert!(!legacy.exists());
    assert_eq!(
        fs::read(temp.path().join(CURRENT_FILE)).unwrap(),
        b"current\n"
    );
}

#[test]
fn cleanup_generation_inventory_error_does_not_sync() {
    let temp = tempfile::tempdir().unwrap();
    assert!(
        prune_superseded_generations_with_sync(temp.path(), "current", |_| {
            panic!("failed inventory must not introduce synchronization")
        })
        .is_err()
    );
}

#[cfg(unix)]
struct RestorePermissions {
    path: PathBuf,
    permissions: fs::Permissions,
}

#[cfg(unix)]
impl RestorePermissions {
    fn read_only(path: &Path) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let permissions = fs::metadata(path).unwrap().permissions();
        fs::set_permissions(path, fs::Permissions::from_mode(0o555)).unwrap();
        Self {
            path: path.to_path_buf(),
            permissions,
        }
    }
}

#[cfg(unix)]
impl Drop for RestorePermissions {
    fn drop(&mut self) {
        fs::set_permissions(&self.path, self.permissions.clone()).unwrap();
    }
}

#[cfg(unix)]
#[test]
fn cleanup_denied_removals_still_request_sync() {
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("permission-denial cleanup fixture requires an unprivileged user; skipped");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let legacy = temp.path().join("old.store");
    fs::write(&legacy, b"legacy").unwrap();
    {
        let _permissions = RestorePermissions::read_only(temp.path());
        let error = prune_legacy_index_files_with_sync(temp.path(), |path| {
            assert_eq!(path, temp.path());
            Err(io::Error::other("legacy sync fault"))
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "legacy sync fault");
        assert!(legacy.exists());
    }

    let generations = temp.path().join(GENERATIONS_DIR);
    let old = generations.join("old");
    fs::create_dir_all(&old).unwrap();
    fs::write(old.join("payload"), b"unreachable").unwrap();
    let _permissions = RestorePermissions::read_only(&generations);
    let error = prune_superseded_generations_with_sync(temp.path(), "current", |path| {
        assert_eq!(path, generations);
        Err(io::Error::other("generation sync fault"))
    })
    .unwrap_err();
    assert_eq!(error.to_string(), "generation sync fault");
    assert!(old.exists());
}
