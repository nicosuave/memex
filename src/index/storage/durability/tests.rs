use super::*;
use std::time::Duration;

fn staging() -> (tempfile::TempDir, SharedDirectory, Arc<StagingDurability>) {
    let temp = tempfile::tempdir().unwrap();
    let owner = "00000000000000000000000000000001-00000001";
    let path = temp
        .path()
        .join(GENERATIONS_DIR)
        .join(format!(".{owner}.tmp"));
    fs::create_dir_all(&path).unwrap();
    let _guard = lock_store(temp.path()).unwrap();
    let durability = StagingDurability::prepare(temp.path(), &path)
        .unwrap()
        .unwrap();
    let directory = SharedDirectory::stage(temp.path(), &path, None, owner).unwrap();
    directory.set_durability(Some(Arc::clone(&durability)));
    (temp, directory, durability)
}

fn add(index: &SearchIndex, id: u64) {
    let mut writer = index.writer_for_ingest(Some(128)).unwrap();
    writer
        .add_document(
            tantivy::doc!(index.fields.doc_id => id, index.fields.text => "durable record"),
        )
        .unwrap();
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
}

#[test]
fn buffered_and_atomic_writes_wait_for_publication_barrier() {
    let (_temp, directory, durability) = staging();
    let mut writer = directory.open_write(Path::new("new.store")).unwrap();
    writer.write_all(b"buffered segment").unwrap();
    writer.terminate().unwrap();
    assert_eq!(
        directory.atomic_read(Path::new("new.store")).unwrap(),
        b"buffered segment"
    );
    assert!(matches!(
        directory.open_write(Path::new("new.store")),
        Err(OpenWriteError::FileAlreadyExists(_))
    ));
    directory
        .atomic_write(Path::new("meta.json"), b"metadata")
        .unwrap();
    directory.sync_directory().unwrap();
    assert!(
        directory
            .view
            .read()
            .unwrap()
            .durable_metadata
            .contains(Path::new("meta.json"))
    );
    assert_eq!(*durability.calls.lock().unwrap(), [false, false, false]);
    directory.sync_for_publication().unwrap();
    assert_eq!(
        *durability.calls.lock().unwrap(),
        [false, false, false, true]
    );
}

#[test]
fn failed_atomic_sync_keeps_previous_bytes_and_removes_temporary_file() {
    let (_temp, directory, durability) = staging();
    directory
        .atomic_write(Path::new("meta.json"), b"old")
        .unwrap();
    let path = directory.view.read().unwrap().path.clone();
    let before = fs::read_dir(&path).unwrap().count();
    *durability.fail.lock().unwrap() = Some(false);
    assert_eq!(
        directory
            .atomic_write(Path::new("meta.json"), b"new")
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EIO)
    );
    assert_eq!(
        directory.atomic_read(Path::new("meta.json")).unwrap(),
        b"old"
    );
    assert_eq!(fs::read_dir(&path).unwrap().count(), before);
    let mut writer = directory.open_write(Path::new("failed.store")).unwrap();
    writer.write_all(b"uncommitted").unwrap();
    assert_eq!(
        writer.terminate().unwrap_err().raw_os_error(),
        Some(libc::EIO)
    );
}

#[test]
fn failed_publication_sync_keeps_current_and_old_segments_until_successful_retry() {
    for fail_full in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let first = SearchIndex::open_or_create_for_ingest(temp.path()).unwrap();
        add(&first, 1);
        first.publish_generation().unwrap();
        let old_current = fs::read(temp.path().join(CURRENT_FILE)).unwrap();
        let old_generation = resolve_current_generation(temp.path()).unwrap();
        let update = SearchIndex::open_or_create_for_ingest(temp.path()).unwrap();
        add(&update, 2);
        let pending = update.pending_generation.as_ref().unwrap();
        let durability = pending
            .directory
            .view
            .read()
            .unwrap()
            .durability
            .clone()
            .unwrap();
        *durability.fail.lock().unwrap() = Some(fail_full);
        assert!(update.publish_generation().is_err());
        assert_eq!(
            fs::read(temp.path().join(CURRENT_FILE)).unwrap(),
            old_current
        );
        assert!(pending.staging_dir.exists());
        assert!(old_generation.is_dir());
        assert!(!pending.published.load(AtomicOrdering::Acquire));
        assert_eq!(
            SearchIndex::open_or_create(temp.path())
                .unwrap()
                .doc_count()
                .unwrap(),
            1
        );
        assert_eq!(first.doc_count().unwrap(), 1);
        assert_eq!(durability.calls.lock().unwrap().last(), Some(&fail_full));
        *durability.fail.lock().unwrap() = None;
        update.publish_generation().unwrap();
        assert_ne!(
            fs::read(temp.path().join(CURRENT_FILE)).unwrap(),
            old_current
        );
        assert_eq!(
            SearchIndex::open_or_create(temp.path())
                .unwrap()
                .doc_count()
                .unwrap(),
            2
        );
        assert_eq!(first.doc_count().unwrap(), 1);
        assert_eq!(
            durability
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|&&full| full)
                .count(),
            1 + usize::from(fail_full)
        );
        assert!(
            pending
                .directory
                .open_write(Path::new("sealed.store"))
                .is_err()
        );
        assert!(
            pending
                .directory
                .atomic_write(Path::new("meta.json"), b"sealed")
                .is_err()
        );
    }
}

#[test]
fn preparation_skips_already_synchronized_metadata_before_the_publication_barrier() {
    let temp = tempfile::tempdir().unwrap();
    let index = SearchIndex::open_or_create_for_ingest(temp.path()).unwrap();
    add(&index, 1);
    let pending = index.pending_generation.as_ref().unwrap();
    let durability = pending
        .directory
        .view
        .read()
        .unwrap()
        .durability
        .clone()
        .unwrap();
    durability.calls.lock().unwrap().clear();
    index.publish_generation().unwrap();
    assert_eq!(
        *durability.calls.lock().unwrap(),
        [false, false, false, false, false, true]
    );
    assert_eq!(
        SearchIndex::open_or_create(temp.path())
            .unwrap()
            .doc_count()
            .unwrap(),
        1
    );
}

#[test]
fn failed_manifest_sync_preserves_previous_manifest_and_cleans_temporary_file() {
    let (_temp, directory, durability) = staging();
    let path = directory.view.read().unwrap().path.clone();
    let previous = fs::read(path.join(MANIFEST)).unwrap();
    let files_before = fs::read_dir(&path).unwrap().count();
    let sync = PublicationSync {
        durability: Some(Arc::clone(&durability)),
    };
    *durability.fail.lock().unwrap() = Some(false);
    assert!(Manifest::empty().write(&path, Some(&sync)).is_err());
    assert_eq!(fs::read(path.join(MANIFEST)).unwrap(), previous);
    assert_eq!(fs::read_dir(&path).unwrap().count(), files_before);
    assert_eq!(*durability.calls.lock().unwrap(), [false]);
}

#[test]
fn replaced_lease_prevents_publication() {
    let temp = tempfile::tempdir().unwrap();
    let index = SearchIndex::open_or_create_for_ingest(temp.path()).unwrap();
    add(&index, 1);
    let pending = index.pending_generation.as_ref().unwrap();
    let replacement = tempfile::NamedTempFile::new_in(&pending.staging_dir).unwrap();
    replacement
        .persist(pending.staging_dir.join(GENERATION_LEASE_FILE))
        .unwrap();
    assert!(index.publish_generation().is_err());
    assert!(!temp.path().join(CURRENT_FILE).exists());
    assert!(pending.staging_dir.exists());
}

#[test]
fn changed_device_rejects_writes_and_full_sync() {
    let (temp, directory, durability) = staging();
    directory.set_durability(None);
    let mut durability = Arc::try_unwrap(durability).unwrap();
    durability.device ^= 1;
    assert!(durability.check_file(&durability.lease).is_err());
    let path = directory.view.read().unwrap().path.clone();
    assert!(durability.sync_directory(&path).is_err());
    assert!(durability.publish(&path).is_err());
    assert!(durability.calls.lock().unwrap().is_empty());
    let durability = Arc::new(durability);
    directory.set_durability(Some(Arc::clone(&durability)));
    let owner = "00000000000000000000000000000001-00000001";
    fs::create_dir_all(temp.path().join(STORE).join(owner)).unwrap();
    assert!(
        directory
            .prepare_publication(temp.path(), owner, &HashSet::new())
            .is_err()
    );
    assert!(durability.calls.lock().unwrap().is_empty());
}

#[test]
fn unsupported_probe_uses_original_sync_but_io_errors_propagate() {
    let file = tempfile::tempfile().unwrap();
    for code in [libc::EINVAL, libc::ENOTSUP, libc::ENOSYS, libc::ENOTTY] {
        assert!(!finish_probe(&file, Err(io::Error::from_raw_os_error(code))).unwrap());
    }
    assert_eq!(
        finish_probe(&file, Err(io::Error::from_raw_os_error(libc::EIO)))
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EIO)
    );
    let (temp, directory, durability) = staging();
    directory.set_durability(None);
    directory
        .atomic_write(Path::new("meta.json"), b"original path")
        .unwrap();
    directory.sync_directory().unwrap();
    directory
        .prepare_publication(
            temp.path(),
            "00000000000000000000000000000001-00000001",
            &HashSet::new(),
        )
        .unwrap();
    directory.sync_for_publication().unwrap();
    assert!(durability.calls.lock().unwrap().is_empty());
    assert!(
        directory
            .view
            .read()
            .unwrap()
            .durable_metadata
            .contains(Path::new("meta.json"))
    );
}

#[test]
fn atomic_metadata_replacement_notifies_existing_watcher() {
    let (_temp, directory, _durability) = staging();
    directory
        .atomic_write(Path::new("meta.json"), b"first")
        .unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let watched = directory.clone();
    let _watch = directory
        .watch(WatchCallback::new(move || {
            let _ = send.send(watched.atomic_read(Path::new("meta.json")).unwrap());
        }))
        .unwrap();
    assert_eq!(
        receive.recv_timeout(Duration::from_secs(3)).unwrap(),
        b"first"
    );
    directory
        .atomic_write(Path::new("meta.json"), b"second")
        .unwrap();
    assert_eq!(
        receive.recv_timeout(Duration::from_secs(3)).unwrap(),
        b"second"
    );
}

#[test]
fn interrupted_sync_retries_but_io_failure_does_not() {
    let mut attempts = 0;
    retry_sync(|| {
        attempts += 1;
        if attempts == 1 {
            unsafe {
                *libc::__error() = libc::EINTR;
            }
            -1
        } else {
            0
        }
    })
    .unwrap();
    assert_eq!(attempts, 2);
    assert_eq!(
        retry_sync(|| {
            unsafe {
                *libc::__error() = libc::EIO;
            }
            -1
        })
        .unwrap_err()
        .raw_os_error(),
        Some(libc::EIO)
    );
}
