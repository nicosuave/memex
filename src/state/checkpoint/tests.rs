use super::*;
use crate::config::Paths;
use crate::lease::LeaseAttempt;
use crate::state::{FileIdentity, PendingToolCall};
use std::time::Duration;

fn fixture() -> (tempfile::TempDir, PathBuf, IngestLease) {
    let temp = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(temp.path().join("root"))).unwrap();
    let lease = match IngestLease::try_acquire(&paths, "checkpoint test").unwrap() {
        LeaseAttempt::Acquired(lease) => lease,
        LeaseAttempt::Busy(_) => panic!("unexpected lease contention"),
    };
    (temp, paths.state.join("ingest.json"), lease)
}

fn file(mtime: i64) -> FileState {
    FileState {
        size: u64::MAX,
        mtime,
        offset: u64::MAX,
        turn_id: u32::MAX,
        legacy_turn_id: Some(u32::MAX),
        claude_background: None,
        parser_version: u32::MAX,
        pending_tool_calls: HashMap::from([(
            "call".into(),
            PendingToolCall {
                tool_use_doc_id: Some(u64::MAX),
                timestamp: u64::MAX,
                argument_bytes: Some(u64::MAX),
                ..Default::default()
            },
        )]),
        identity: FileIdentity {
            device: Some(u64::MAX),
            inode: Some(u64::MAX),
            prefix_bytes: u64::MAX,
            modified_ns: Some(i64::MIN),
            changed_ns: Some(i64::MAX),
            ..Default::default()
        },
        codex_metadata_offsets: Some(vec![0, u64::MAX]),
    }
}

fn database() -> OpencodeDatabaseState {
    OpencodeDatabaseState {
        parser_version: u32::MAX,
        event_rowid: i64::MIN,
        event_id: Some("event".into()),
        owned_session_ids: HashSet::from(["session".into()]),
    }
}

fn extended_legacy(path: &Path) -> Value {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut value = serde_json::to_value(IngestState {
        next_doc_id: u64::MAX,
        files: HashMap::from([("./odd/../a\n\0é.jsonl".into(), file(i64::MIN))]),
        opencode_databases: HashMap::from([("database".into(), database())]),
    })
    .unwrap();
    value["extension"] = serde_json::json!({"unsigned": u64::MAX});
    let row = &mut value["files"]["./odd/../a\n\0é.jsonl"];
    row["extension"] = serde_json::json!(["opaque", u64::MAX]);
    row["identity"]["extension"] = serde_json::json!({"id": u64::MAX});
    row["pending_tool_calls"]["call"]["extension"] = serde_json::json!({"future": true});
    value["opencode_databases"]["database"]["extension"] = serde_json::json!("owner");
    fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    value
}

fn connection(writer: &CheckpointWriter) -> &Connection {
    match &writer.reader.backend {
        Backend::Sqlite { connection, .. } => connection,
        Backend::Legacy(_) => panic!("not SQLite"),
    }
}

#[test]
fn migration_preserves_exact_unsigned_values_extensions_paths_and_raw_backup() {
    let (_temp, path, lease) = fixture();
    let original = extended_legacy(&path);
    let raw = fs::read(&path).unwrap();
    let mut writer = CheckpointWriter::open(&path, &lease, false).unwrap();
    assert_eq!(writer.reader().export_json().unwrap(), original);
    assert_eq!(writer.reader().header().unwrap().next_doc_id, u64::MAX);
    assert_eq!(
        writer
            .reader()
            .snapshot()
            .unwrap()
            .files
            .values()
            .next()
            .unwrap()
            .offset,
        u64::MAX
    );
    assert!(serde_json::from_slice::<IngestState>(&fs::read(&path).unwrap()).is_err());
    let backups: Vec<_> = fs::read_dir(path.parent().unwrap())
        .unwrap()
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("ingest.legacy-")
        })
        .collect();
    assert_eq!(backups.len(), 1);
    assert_eq!(fs::read(backups[0].path()).unwrap(), raw);
    writer.checkpoint().unwrap();
}

#[test]
fn delta_updates_preserve_private_nested_extensions_and_remove_deleted_known_fields() {
    let (_temp, path, lease) = fixture();
    let original = extended_legacy(&path);
    let mut writer = CheckpointWriter::open(&path, &lease, false).unwrap();
    let mut state = writer.reader().snapshot().unwrap();
    let row = state.files.values_mut().next().unwrap();
    row.offset = 7;
    row.identity.inode = None;
    row.pending_tool_calls
        .get_mut("call")
        .unwrap()
        .tool_use_doc_id = None;
    row.codex_metadata_offsets = None;
    state
        .opencode_databases
        .get_mut("database")
        .unwrap()
        .event_rowid = 19;
    writer.replace_snapshot(&state).unwrap();
    let exported = writer.reader().export_json().unwrap();
    assert_eq!(exported["extension"], original["extension"]);
    let key = "./odd/../a\n\0é.jsonl";
    for nested in [
        "/extension",
        "/identity/extension",
        "/pending_tool_calls/call/extension",
    ] {
        assert_eq!(
            exported["files"][key].pointer(nested),
            original["files"][key].pointer(nested)
        );
    }
    assert!(exported["files"][key]["identity"].get("inode").is_none());
    assert!(
        exported["files"][key]["pending_tool_calls"]["call"]
            .get("tool_use_doc_id")
            .is_none()
    );
    assert!(
        exported["files"][key]
            .get("codex_metadata_offsets")
            .is_none()
    );
    assert_eq!(
        exported["opencode_databases"]["database"]["extension"],
        "owner"
    );
}

#[test]
fn legacy_defaults_remain_readable_after_migration() {
    let (_temp, path, lease) = fixture();
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        r#"{"next_doc_id":9,"files":{"legacy":{"size":4,"mtime":-1,"offset":3,"turn_id":2}}}"#,
    )
    .unwrap();
    let writer = CheckpointWriter::open(&path, &lease, false).unwrap();
    let state = writer.reader().snapshot().unwrap();
    assert_eq!(state.next_doc_id, 9);
    assert_eq!(state.files["legacy"].identity, FileIdentity::default());
    assert!(state.files["legacy"].pending_tool_calls.is_empty());
    assert!(state.opencode_databases.is_empty());
}

#[test]
fn every_writer_verifies_durable_sqlite_pragmas() {
    let (_temp, path, lease) = fixture();
    for _ in 0..2 {
        let writer = CheckpointWriter::open(&path, &lease, true).unwrap();
        let connection = connection(&writer);
        assert_eq!(
            connection
                .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
        for (name, expected) in [
            ("synchronous", 2),
            ("fullfsync", 0),
            ("checkpoint_fullfsync", 0),
            ("wal_autocheckpoint", 1000),
            ("journal_size_limit", 16 * 1024 * 1024),
        ] {
            assert_eq!(
                connection
                    .pragma_query_value(None, name, |row| row.get::<_, i64>(0))
                    .unwrap(),
                expected
            );
        }
        assert!(
            connection
                .db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE)
                .unwrap()
        );
    }
}

#[test]
fn schema_validates_generated_mtime_and_uses_its_index() {
    let (_temp, path, lease) = fixture();
    let writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    let connection = connection(&writer);
    for payload in [
        r#"{"mtime":"5"}"#,
        r#"{"mtime":1.5}"#,
        r#"{"mtime":18446744073709551615}"#,
        r#"{}"#,
    ] {
        assert!(
            connection
                .execute(
                    "INSERT INTO files(path,payload) VALUES('bad',?1)",
                    [payload]
                )
                .is_err()
        );
    }
    let plan: String = connection
        .query_row(
            "EXPLAIN QUERY PLAN SELECT path,payload FROM files WHERE mtime>=?1",
            [1],
            |row| row.get(3),
        )
        .unwrap();
    assert!(plan.contains("files_mtime"), "{plan}");
    let path_type: String = connection
        .query_row(
            "SELECT type FROM pragma_table_info('files') WHERE name='path'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(path_type, "TEXT");
}

#[test]
fn sparse_reads_and_delta_leave_unrelated_payloads_identical() {
    let (_temp, path, lease) = fixture();
    let mut writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    writer
        .commit_delta(&CheckpointDelta {
            upserts: (0..2000)
                .map(|id| (format!("file-{id}"), file(id)))
                .collect(),
            next_doc_id: Some(u64::MAX),
            ..Default::default()
        })
        .unwrap();
    let before: String = connection(&writer)
        .query_row(
            "SELECT payload FROM files WHERE path='file-1000'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let loaded = writer
        .reader()
        .load_files(&["file-1".into(), "absent".into()])
        .unwrap();
    assert_eq!(loaded.len(), 2);
    assert!(loaded["file-1"].is_some());
    assert_eq!(loaded["absent"], None);
    writer
        .commit_delta(&CheckpointDelta {
            upserts: HashMap::from([("file-1".into(), file(-8))]),
            deletes: HashSet::from(["file-2".into()]),
            ..Default::default()
        })
        .unwrap();
    let after: String = connection(&writer)
        .query_row(
            "SELECT payload FROM files WHERE path='file-1000'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(before, after);
    assert!(!writer.reader().contains_file("file-2").unwrap());
    assert_eq!(writer.reader().file_keys().unwrap().len(), 1999);
    assert_eq!(writer.reader().header().unwrap().next_doc_id, u64::MAX);
}

#[test]
fn empty_delta_starts_no_transaction_and_changes_no_pages() {
    let (_temp, path, lease) = fixture();
    let mut writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    connection(&writer).execute_batch("BEGIN").unwrap();
    assert!(!writer.commit_delta(&CheckpointDelta::default()).unwrap());
    assert!(!connection(&writer).is_autocommit());
    connection(&writer).execute_batch("ROLLBACK").unwrap();
    let reader = CheckpointReader::open(&path).unwrap();
    let Backend::Sqlite {
        connection: observer,
        ..
    } = &reader.backend
    else {
        panic!()
    };
    let before: i64 = observer
        .pragma_query_value(None, "data_version", |row| row.get(0))
        .unwrap();
    assert!(!writer.commit_delta(&CheckpointDelta::default()).unwrap());
    assert_eq!(
        before,
        observer
            .pragma_query_value(None, "data_version", |row| row.get::<_, i64>(0))
            .unwrap()
    );
}

#[test]
fn hot_and_key_queries_do_not_decode_cold_payloads() {
    let (_temp, path, lease) = fixture();
    let mut writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    writer
        .commit_delta(&CheckpointDelta {
            upserts: HashMap::from([("hot".into(), file(50))]),
            ..Default::default()
        })
        .unwrap();
    connection(&writer)
        .execute(
            "INSERT INTO files(path,payload) VALUES('cold',?1)",
            [r#"{"mtime":-50,"size":"invalid FileState"}"#],
        )
        .unwrap();
    assert_eq!(writer.reader().hot_files_since(0).unwrap().len(), 1);
    assert_eq!(writer.reader().file_keys().unwrap().len(), 2);
    assert!(writer.reader().contains_file("cold").unwrap());
    assert!(
        writer
            .reader()
            .has_files_excluding(&HashSet::from(["hot".into()]))
            .unwrap()
    );
    assert!(
        !writer
            .reader()
            .has_files_excluding(&HashSet::from(["hot".into(), "cold".into()]))
            .unwrap()
    );
    assert!(writer.reader().snapshot().is_err());
}

#[test]
fn live_reader_observes_wal_commits_without_database_mtime_changes() {
    let (_temp, path, lease) = fixture();
    let mut writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    let reader = CheckpointReader::open(&path).unwrap();
    let database_path = path.with_file_name(DATABASE);
    let modified = fs::metadata(&database_path).unwrap().modified().unwrap();
    assert!(reader.hot_files_since(0).unwrap().is_empty());
    writer
        .commit_delta(&CheckpointDelta {
            upserts: HashMap::from([("new".into(), file(1))]),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        fs::metadata(&database_path).unwrap().modified().unwrap(),
        modified
    );
    assert_eq!(reader.hot_files_since(0).unwrap()["new"].mtime, 1);
    assert!(reader.contains_file("new").unwrap());
}

#[test]
fn truncate_reports_pinned_readers_and_completes_after_release() {
    let (_temp, path, lease) = fixture();
    let mut writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    writer
        .commit_delta(&CheckpointDelta {
            upserts: HashMap::from([("first".into(), file(1))]),
            ..Default::default()
        })
        .unwrap();
    let reader = CheckpointReader::open(&path).unwrap();
    let Backend::Sqlite {
        connection: pinned, ..
    } = &reader.backend
    else {
        panic!()
    };
    pinned.execute_batch("BEGIN; SELECT * FROM files;").unwrap();
    writer
        .commit_delta(&CheckpointDelta {
            next_doc_id: Some(3),
            ..Default::default()
        })
        .unwrap();
    assert!(writer.checkpoint().is_err());
    pinned.execute_batch("ROLLBACK").unwrap();
    writer.checkpoint().unwrap();
    assert_eq!(
        fs::metadata(path.with_file_name("checkpoints.sqlite-wal"))
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn migration_failure_boundaries_preserve_legacy_or_activated_authority() {
    for point in [
        "before_backup",
        "after_backup",
        "before_import",
        "before_import_commit",
        "after_import_commit",
        "before_database_sync",
        "after_database_sync",
        "before_marker",
        "after_marker",
    ] {
        let (_temp, path, lease) = fixture();
        let original = extended_legacy(&path);
        let raw = fs::read(&path).unwrap();
        assert!(
            lifecycle::open_writer(&path, &lease, false, lifecycle::MigrationFailure::At(point))
                .is_err(),
            "{point}"
        );
        if point != "after_marker" {
            assert_eq!(fs::read(&path).unwrap(), raw, "{point}");
        }
        assert_eq!(
            CheckpointReader::open(&path)
                .unwrap()
                .export_json()
                .unwrap(),
            original,
            "{point}"
        );
        let writer = CheckpointWriter::open(&path, &lease, false).unwrap();
        assert_eq!(writer.reader().export_json().unwrap(), original, "{point}");
    }
}

#[test]
fn legacy_changes_after_partial_import_are_reimported_not_replaced_by_backup() {
    let (_temp, path, lease) = fixture();
    extended_legacy(&path);
    assert!(
        lifecycle::open_writer(
            &path,
            &lease,
            false,
            lifecycle::MigrationFailure::At("after_import_commit")
        )
        .is_err()
    );
    let replacement =
        serde_json::json!({"next_doc_id":72,"files":{},"opencode_databases":{},"future":"new"});
    fs::write(&path, serde_json::to_vec(&replacement).unwrap()).unwrap();
    let writer = CheckpointWriter::open(&path, &lease, false).unwrap();
    assert_eq!(writer.reader().export_json().unwrap(), replacement);
    assert_eq!(
        fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .filter(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with("ingest.legacy-"))
            .count(),
        2
    );
}

#[test]
fn backup_collision_is_an_error_and_never_overwrites_bytes() {
    use sha2::{Digest, Sha256};
    let (_temp, path, lease) = fixture();
    extended_legacy(&path);
    let raw = fs::read(&path).unwrap();
    let backup = path.with_file_name(format!("ingest.legacy-{:x}.json", Sha256::digest(&raw)));
    fs::write(&backup, "wrong bytes").unwrap();
    assert!(CheckpointWriter::open(&path, &lease, false).is_err());
    assert_eq!(fs::read(&path).unwrap(), raw);
    assert_eq!(fs::read(&backup).unwrap(), b"wrong bytes");
}

#[test]
fn read_only_missing_state_creates_nothing_and_initialization_requires_permission() {
    let (_temp, path, lease) = fixture();
    assert!(!path.parent().unwrap().exists());
    assert!(!has_authority(&path).unwrap());
    assert_eq!(
        CheckpointReader::open(&path)
            .unwrap()
            .snapshot()
            .unwrap()
            .next_doc_id,
        1
    );
    assert!(!path.parent().unwrap().exists());
    assert!(CheckpointWriter::open(&path, &lease, false).is_err());
    assert!(!path.with_file_name(DATABASE).exists());
    assert!(CheckpointWriter::open(&path, &lease, true).is_ok());
}

#[test]
fn empty_bootstrap_retry_requires_matching_receipt_and_initialization_permission() {
    let (_temp, path, lease) = fixture();
    assert!(
        lifecycle::open_writer(
            &path,
            &lease,
            true,
            lifecycle::MigrationFailure::At("after_import_commit")
        )
        .is_err()
    );
    assert!(!has_authority(&path).unwrap());
    assert!(CheckpointWriter::open(&path, &lease, false).is_err());
    let receipt = fs::read(path.with_file_name(LOCK)).unwrap();
    fs::write(
        path.with_file_name(LOCK),
        format!("bootstrap:{}", "0".repeat(64)),
    )
    .unwrap();
    assert!(CheckpointReader::open(&path).is_err());
    fs::write(path.with_file_name(LOCK), receipt).unwrap();
    assert!(CheckpointWriter::open(&path, &lease, true).is_ok());
}

#[test]
fn missing_authority_rejects_populated_database_even_with_archived_backup() {
    let (_temp, path, lease) = fixture();
    extended_legacy(&path);
    drop(CheckpointWriter::open(&path, &lease, false).unwrap());
    fs::remove_file(&path).unwrap();
    assert!(has_authority(&path).is_err());
    assert!(IngestState::load(&path).is_err());
    assert!(CheckpointWriter::open(&path, &lease, true).is_err());
}

#[test]
fn malformed_marker_missing_database_missing_lock_and_unknown_versions_fail_closed() {
    for broken in [
        "marker",
        "database",
        "lock",
        "version",
        "identity",
        "schema_version",
        "next_id",
    ] {
        let (_temp, path, lease) = fixture();
        drop(CheckpointWriter::open(&path, &lease, true).unwrap());
        match broken {
            "marker" => fs::write(&path, "\"memex-checkpoints:broken\"").unwrap(),
            "database" => fs::remove_file(path.with_file_name(DATABASE)).unwrap(),
            "lock" => fs::remove_file(path.with_file_name(LOCK)).unwrap(),
            "version" => {
                let raw = fs::read_to_string(&path)
                    .unwrap()
                    .replace("checkpoints:2:", "checkpoints:99:");
                fs::write(&path, raw).unwrap();
            }
            "identity" => fs::write(
                &path,
                serde_json::to_vec(&format!("{MARKER_PREFIX}1:{}", "0".repeat(64))).unwrap(),
            )
            .unwrap(),
            "schema_version" | "next_id" => {
                let connection = Connection::open(path.with_file_name(DATABASE)).unwrap();
                connection
                    .execute_batch(if broken == "schema_version" {
                        "UPDATE metadata SET format_version=99"
                    } else {
                        "UPDATE metadata SET next_doc_id='01'"
                    })
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(CheckpointReader::open(&path).is_err(), "{broken}");
        assert!(has_authority(&path).is_err(), "{broken}");
        assert!(
            CheckpointWriter::open(&path, &lease, true).is_err(),
            "{broken}"
        );
    }
}

#[test]
fn legacy_save_cannot_overwrite_marker_and_explicit_snapshot_keeps_extensions() {
    let (_temp, path, lease) = fixture();
    let original = extended_legacy(&path);
    drop(CheckpointWriter::open(&path, &lease, false).unwrap());
    let marker = fs::read(&path).unwrap();
    let mut state = IngestState::load(&path).unwrap();
    state.next_doc_id = 17;
    assert!(state.save(&path).is_err());
    assert_eq!(fs::read(&path).unwrap(), marker);
    state.save_with_lease(&path, &lease).unwrap();
    let exported = CheckpointReader::open(&path)
        .unwrap()
        .export_json()
        .unwrap();
    assert_eq!(exported["next_doc_id"], 17);
    assert_eq!(exported["extension"], original["extension"]);
}

#[test]
fn reset_waits_for_readers_preserves_lock_inode_and_archives() {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    let (_temp, path, lease) = fixture();
    extended_legacy(&path);
    drop(CheckpointWriter::open(&path, &lease, false).unwrap());
    let reader = CheckpointReader::open(&path).unwrap();
    #[cfg(unix)]
    let inode = fs::metadata(path.with_file_name(LOCK)).unwrap().ino();
    assert!(reset(&path, &lease).is_err());
    assert!(path.exists());
    drop(reader);
    reset(&path, &lease).unwrap();
    assert!(!path.exists());
    assert!(!path.with_file_name(DATABASE).exists());
    assert!(path.with_file_name(LOCK).exists());
    #[cfg(unix)]
    assert_eq!(
        fs::metadata(path.with_file_name(LOCK)).unwrap().ino(),
        inode
    );
    assert!(
        fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .any(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with("ingest.legacy-"))
    );
    assert!(!has_authority(&path).unwrap());
    drop(CheckpointWriter::open(&path, &lease, true).unwrap());
}

#[test]
fn reset_serializes_with_concurrent_reader_lifetime() {
    let (_temp, path, lease) = fixture();
    drop(CheckpointWriter::open(&path, &lease, true).unwrap());
    let reader = CheckpointReader::open(&path).unwrap();
    let held = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(70));
        assert_eq!(reader.header().unwrap().next_doc_id, 1);
    });
    reset(&path, &lease).unwrap();
    held.join().unwrap();
    assert!(!path.exists());
}

#[test]
fn clear_all_preserves_allocator_and_small_database_map_unless_explicitly_changed() {
    let (_temp, path, lease) = fixture();
    let mut writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    writer
        .commit_delta(&CheckpointDelta {
            upserts: HashMap::from([("old".into(), file(0))]),
            next_doc_id: Some(900),
            opencode_databases: Some(HashMap::from([("database".into(), database())])),
            ..Default::default()
        })
        .unwrap();
    writer
        .commit_delta(&CheckpointDelta {
            clear_files: true,
            upserts: HashMap::from([("new".into(), file(1))]),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(writer.reader().file_keys().unwrap(), ["new"]);
    let header = writer.reader().header().unwrap();
    assert_eq!(header.next_doc_id, 900);
    assert_eq!(header.opencode_databases["database"], database());
}

#[test]
fn checkpoint_artifact_names_are_canonical_and_bounded() {
    for name in [
        "checkpoints.sqlite",
        "checkpoints.sqlite-wal",
        "checkpoints.sqlite-shm",
        "checkpoints.sqlite-journal",
        ".checkpoints.lock",
        "ingest.json",
    ] {
        assert!(is_checkpoint_artifact_name(OsStr::new(name)));
    }
    assert!(is_checkpoint_artifact_name(OsStr::new(&format!(
        "ingest.legacy-{}.json",
        "a".repeat(64)
    ))));
    for name in [
        "checkpoints.sqlite-session.jsonl",
        "ingest.legacy-x.json",
        "my-checkpoints.sqlite",
        "session.jsonl",
    ] {
        assert!(!is_checkpoint_artifact_name(OsStr::new(name)));
    }
}

fn pending() -> PendingIngest {
    PendingIngest {
        next_doc_id: 19,
        source_paths: vec!["source".into()],
        session_scopes: vec![super::super::SessionScope {
            source_path: "source".into(),
            session_id: "session".into(),
        }],
        vector_delete_paths: Vec::new(),
        vector_publication: true,
        embedding_publication: Some(true),
    }
}

fn cache() -> ScanCache {
    ScanCache {
        last_scan_ts: 17,
        file_count: 3,
        total_bytes: u64::MAX,
    }
}

fn extended_sidecars(path: &Path) -> (Value, Value) {
    let mut pending = serde_json::to_value(pending()).unwrap();
    pending["extension"] = serde_json::json!({"future":u64::MAX});
    pending["session_scopes"][0]["extension"] = serde_json::json!({"owner":"session"});
    let mut cache = serde_json::to_value(cache()).unwrap();
    cache["extension"] = serde_json::json!([u64::MAX, "cache"]);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    for (name, value) in [(PENDING, &pending), (SCAN_CACHE, &cache)] {
        fs::write(
            path.with_file_name(name),
            serde_json::to_vec_pretty(value).unwrap(),
        )
        .unwrap();
    }
    (pending, cache)
}

fn v1_fixture(path: &Path) -> Value {
    let original = extended_legacy(path);
    let identity = "a".repeat(64);
    fs::write(path.with_file_name(LOCK), "").unwrap();
    let connection = Connection::open(path.with_file_name(DATABASE)).unwrap();
    connection.execute_batch("
        PRAGMA journal_mode=WAL;
        CREATE TABLE metadata (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            format_version INTEGER NOT NULL,
            store_id TEXT NOT NULL,
            origin TEXT NOT NULL,
            next_doc_id TEXT NOT NULL CHECK(typeof(next_doc_id)='text'),
            opencode_databases TEXT NOT NULL CHECK(json_valid(opencode_databases) AND json_type(opencode_databases)='object'),
            legacy_extras TEXT NOT NULL CHECK(json_valid(legacy_extras) AND json_type(legacy_extras)='object')
        );
        CREATE TABLE files (
            path TEXT PRIMARY KEY NOT NULL,
            payload TEXT NOT NULL CHECK(json_valid(payload) AND json_type(payload)='object'),
            mtime INTEGER GENERATED ALWAYS AS (json_extract(payload,'$.mtime')) STORED NOT NULL
                CHECK(json_type(payload,'$.mtime')='integer' AND typeof(mtime)='integer')
        );
        CREATE INDEX files_mtime ON files(mtime);
    ").unwrap();
    connection
        .execute(
            "INSERT INTO metadata VALUES(1,1,?1,'legacy:fixture',?2,?3,?4)",
            params![
                identity,
                u64::MAX.to_string(),
                serde_json::to_string(&original["opencode_databases"]).unwrap(),
                codec::legacy_extras(&original).unwrap()
            ],
        )
        .unwrap();
    for (key, value) in original["files"].as_object().unwrap() {
        connection
            .execute(
                "INSERT INTO files(path,payload) VALUES(?1,?2)",
                params![key, serde_json::to_string(value).unwrap()],
            )
            .unwrap();
    }
    lifecycle::checkpoint(&connection).unwrap();
    fs::write(
        path,
        serde_json::to_vec(&format!("{MARKER_PREFIX}1:{identity}")).unwrap(),
    )
    .unwrap();
    original
}

#[test]
fn direct_legacy_and_v1_upgrade_import_all_documents_and_archive_raw_sidecars() {
    use sha2::{Digest, Sha256};
    for v1 in [false, true] {
        let (_temp, path, lease) = fixture();
        let original = if v1 {
            v1_fixture(&path)
        } else {
            extended_legacy(&path)
        };
        let (pending, cache) = extended_sidecars(&path);
        let raw: Vec<_> = [PENDING, SCAN_CACHE]
            .iter()
            .map(|name| (*name, fs::read(path.with_file_name(name)).unwrap()))
            .collect();
        let reader = CheckpointReader::open(&path).unwrap();
        assert_eq!(reader.export_pending_json().unwrap(), Some(pending.clone()));
        assert_eq!(
            reader.export_scan_cache_json().unwrap(),
            Some(cache.clone())
        );
        assert_eq!(
            reader.header().unwrap().pending,
            Some(serde_json::from_value(pending.clone()).unwrap())
        );
        drop(reader);
        let writer = CheckpointWriter::open(&path, &lease, false).unwrap();
        assert_eq!(writer.reader().export_json().unwrap(), original);
        assert_eq!(
            writer.reader().export_pending_json().unwrap(),
            Some(pending)
        );
        assert_eq!(
            writer.reader().export_scan_cache_json().unwrap(),
            Some(cache)
        );
        assert_eq!(
            connection(&writer)
                .query_row("SELECT format_version FROM metadata", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        for (name, raw) in raw {
            assert!(!path.with_file_name(name).exists());
            let archive = path.with_file_name(format!(
                "{}.legacy-{:x}.json",
                name.trim_end_matches(".json"),
                Sha256::digest(&raw)
            ));
            assert_eq!(fs::read(archive).unwrap(), raw);
        }
    }
}

#[test]
fn v1_upgrade_failure_boundaries_preserve_one_authority_and_resume_without_stale_import() {
    for point in [
        "before_sidecar_backup",
        "after_pending_backup",
        "after_cache_backup",
        "after_sidecar_backup",
        "before_import",
        "before_import_commit",
        "after_import_commit",
        "before_checkpoint",
        "after_checkpoint",
        "before_database_sync",
        "after_database_file_sync",
        "after_database_sync",
        "before_marker",
        "after_marker",
        "before_cleanup",
        "after_pending_cleanup",
        "after_cache_cleanup",
        "after_cleanup",
    ] {
        let (_temp, path, lease) = fixture();
        let original = v1_fixture(&path);
        let (pending, cache) = extended_sidecars(&path);
        assert!(
            lifecycle::open_writer(&path, &lease, false, lifecycle::MigrationFailure::At(point))
                .is_err(),
            "{point}"
        );
        let reader = CheckpointReader::open(&path).unwrap();
        assert_eq!(reader.export_json().unwrap(), original, "{point}");
        assert_eq!(
            reader.export_pending_json().unwrap(),
            Some(pending.clone()),
            "{point}"
        );
        assert_eq!(
            reader.export_scan_cache_json().unwrap(),
            Some(cache.clone()),
            "{point}"
        );
        let upgraded = reader.is_v2();
        drop(reader);
        if upgraded {
            fs::write(path.with_file_name(PENDING), "stale invalid intent").unwrap();
            fs::write(path.with_file_name(SCAN_CACHE), "stale invalid cache").unwrap();
        }
        let writer = CheckpointWriter::open(&path, &lease, false).unwrap();
        assert_eq!(writer.reader().export_json().unwrap(), original, "{point}");
        assert_eq!(
            writer.reader().export_pending_json().unwrap(),
            Some(pending),
            "{point}"
        );
        assert_eq!(
            writer.reader().export_scan_cache_json().unwrap(),
            Some(cache),
            "{point}"
        );
        assert!(!path.with_file_name(PENDING).exists(), "{point}");
        assert!(!path.with_file_name(SCAN_CACHE).exists(), "{point}");
        assert!(
            fs::read_to_string(&path)
                .unwrap()
                .contains("checkpoints:2:")
        );
    }
}

#[test]
fn db2_marker1_reader_is_read_only_and_writer_finishes_activation() {
    let (_temp, path, lease) = fixture();
    let original = v1_fixture(&path);
    let (pending, cache) = extended_sidecars(&path);
    assert!(
        lifecycle::open_writer(
            &path,
            &lease,
            false,
            lifecycle::MigrationFailure::At("after_import_commit")
        )
        .is_err()
    );
    let marker = fs::read(&path).unwrap();
    let database = fs::read(path.with_file_name(DATABASE)).unwrap();
    fs::write(path.with_file_name(PENDING), "stale").unwrap();
    fs::write(path.with_file_name(SCAN_CACHE), "stale").unwrap();
    let reader = CheckpointReader::open(&path).unwrap();
    assert_eq!(reader.export_json().unwrap(), original);
    assert_eq!(reader.export_pending_json().unwrap(), Some(pending.clone()));
    assert_eq!(
        reader.export_scan_cache_json().unwrap(),
        Some(cache.clone())
    );
    assert_eq!(
        reader.header().unwrap().pending,
        Some(serde_json::from_value(pending.clone()).unwrap())
    );
    assert_eq!(
        PendingIngest::load(&path.with_file_name(PENDING)).unwrap(),
        reader.header().unwrap().pending
    );
    assert_eq!(
        ScanCache::load(&path.with_file_name(SCAN_CACHE))
            .unwrap()
            .last_scan_ts,
        17
    );
    drop(reader);
    assert_eq!(fs::read(&path).unwrap(), marker);
    assert_eq!(fs::read(path.with_file_name(DATABASE)).unwrap(), database);
    let writer = CheckpointWriter::open(&path, &lease, false).unwrap();
    assert_eq!(
        writer.reader().export_pending_json().unwrap(),
        Some(pending)
    );
    assert_eq!(
        writer.reader().export_scan_cache_json().unwrap(),
        Some(cache)
    );
}

#[test]
fn marker2_db1_is_fatal_without_mutating_the_database() {
    let (_temp, path, lease) = fixture();
    v1_fixture(&path);
    let marker = fs::read_to_string(&path)
        .unwrap()
        .replace("checkpoints:1:", "checkpoints:2:");
    fs::write(&path, marker).unwrap();
    let before = fs::read(path.with_file_name(DATABASE)).unwrap();
    assert!(CheckpointReader::open(&path).is_err());
    assert!(CheckpointWriter::open(&path, &lease, false).is_err());
    assert!(PendingIngest::load(&path.with_file_name(PENDING)).is_err());
    assert!(ScanCache::load(&path.with_file_name(SCAN_CACHE)).is_err());
    assert_eq!(fs::read(path.with_file_name(DATABASE)).unwrap(), before);
}

#[test]
fn pending_is_strict_but_cache_is_lenient_before_and_after_upgrade() {
    for v1 in [false, true] {
        let (_temp, path, lease) = fixture();
        if v1 {
            v1_fixture(&path);
        } else {
            extended_legacy(&path);
        }
        fs::write(path.with_file_name(PENDING), "{broken").unwrap();
        fs::write(path.with_file_name(SCAN_CACHE), "{broken").unwrap();
        assert!(CheckpointReader::open(&path).unwrap().header().is_err());
        assert!(PendingIngest::load(&path.with_file_name(PENDING)).is_err());
        assert_eq!(
            ScanCache::load(&path.with_file_name(SCAN_CACHE))
                .unwrap()
                .last_scan_ts,
            0
        );
        assert!(CheckpointWriter::open(&path, &lease, false).is_err());
        pending().save(&path.with_file_name(PENDING)).unwrap();
        let writer = CheckpointWriter::open(&path, &lease, false).unwrap();
        assert_eq!(writer.reader().header().unwrap().scan_cache.last_scan_ts, 0);
        connection(&writer)
            .execute(
                "UPDATE metadata SET scancache_json=?1",
                [r#"{"last_scan_ts":"invalid","extension":7}"#],
            )
            .unwrap();
        assert_eq!(
            CheckpointReader::open(&path)
                .unwrap()
                .header()
                .unwrap()
                .scan_cache
                .last_scan_ts,
            0
        );
        connection(&writer)
            .execute("UPDATE metadata SET pending_json='{}'", [])
            .unwrap();
        assert!(CheckpointReader::open(&path).is_err());
        assert!(PendingIngest::load(&path.with_file_name(PENDING)).is_err());
    }
}

#[test]
fn early_intent_transaction_does_not_flush_staged_delta_and_failed_final_rolls_back_every_field() {
    let (_temp, path, lease) = fixture();
    extended_legacy(&path);
    extended_sidecars(&path);
    let mut writer = CheckpointWriter::open(&path, &lease, false).unwrap();
    let before = writer.reader().export_json().unwrap();
    let before_cache = writer.reader().export_scan_cache_json().unwrap();
    let final_delta = CheckpointDelta {
        clear_files: true,
        upserts: HashMap::from([("new".into(), file(90))]),
        next_doc_id: Some(80),
        opencode_databases: Some(HashMap::new()),
        pending: PendingChange::Clear,
        scan_cache: Some(cache()),
        ..Default::default()
    };
    let mut intent = pending();
    intent.next_doc_id = 80;
    writer.commit_intent(&intent).unwrap();
    assert_eq!(writer.reader().export_json().unwrap(), before);
    assert_eq!(
        writer.reader().export_scan_cache_json().unwrap(),
        before_cache
    );
    assert_eq!(
        writer.reader().header().unwrap().pending,
        Some(intent.clone())
    );
    connection(&writer).execute_batch("CREATE TEMP TRIGGER fail_final BEFORE UPDATE OF scancache_json ON metadata BEGIN SELECT RAISE(ABORT, 'final failure'); END;").unwrap();
    assert!(writer.commit_delta(&final_delta).is_err());
    assert_eq!(writer.reader().export_json().unwrap(), before);
    assert_eq!(
        writer.reader().export_scan_cache_json().unwrap(),
        before_cache
    );
    assert_eq!(writer.reader().header().unwrap().pending, Some(intent));
    connection(&writer)
        .execute_batch("DROP TRIGGER fail_final;")
        .unwrap();
    assert!(writer.commit_delta(&final_delta).unwrap());
    assert_eq!(writer.reader().header().unwrap().pending, None);
    assert_eq!(writer.reader().header().unwrap().next_doc_id, 80);
    assert_eq!(writer.reader().file_keys().unwrap(), ["new"]);
    assert!(
        writer
            .reader()
            .header()
            .unwrap()
            .opencode_databases
            .is_empty()
    );
}

#[test]
fn vector_only_clear_cache_only_and_early_failure_are_independent_transactions() {
    let (_temp, path, lease) = fixture();
    let mut writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    let before = writer.reader().export_json().unwrap();
    let mut intent = pending();
    intent.source_paths.clear();
    intent.session_scopes.clear();
    writer.commit_intent(&intent).unwrap();
    connection(&writer).execute_batch("CREATE TEMP TRIGGER fail_intent BEFORE UPDATE OF pending_json ON metadata BEGIN SELECT RAISE(ABORT, 'intent failure'); END;").unwrap();
    assert!(writer.commit_intent(&pending()).is_err());
    assert_eq!(writer.reader().header().unwrap().pending, Some(intent));
    connection(&writer)
        .execute_batch("DROP TRIGGER fail_intent;")
        .unwrap();
    assert!(
        writer
            .commit_delta(&CheckpointDelta {
                pending: PendingChange::Clear,
                ..Default::default()
            })
            .unwrap()
    );
    assert_eq!(writer.reader().header().unwrap().pending, None);
    assert!(
        writer
            .commit_delta(&CheckpointDelta {
                scan_cache: Some(cache()),
                ..Default::default()
            })
            .unwrap()
    );
    assert_eq!(
        writer.reader().header().unwrap().scan_cache.last_scan_ts,
        17
    );
    assert_eq!(writer.reader().export_json().unwrap(), before);
    assert!(!writer.commit_delta(&CheckpointDelta::default()).unwrap());
}

#[test]
fn reordered_pending_scopes_keep_extensions_by_identity_and_cache_updates_keep_unknown_fields() {
    let (_temp, path, lease) = fixture();
    extended_legacy(&path);
    let (mut original_pending, original_cache) = extended_sidecars(&path);
    original_pending["session_scopes"].as_array_mut().unwrap().push(serde_json::json!({"source_path":"another","session_id":"session","extension":"second"}));
    fs::write(
        path.with_file_name(PENDING),
        serde_json::to_vec(&original_pending).unwrap(),
    )
    .unwrap();
    let mut writer = CheckpointWriter::open(&path, &lease, false).unwrap();
    let mut intent = writer.reader().header().unwrap().pending.unwrap();
    intent.session_scopes.reverse();
    writer.commit_intent(&intent).unwrap();
    let exported = writer.reader().export_pending_json().unwrap().unwrap();
    assert_eq!(exported["extension"], original_pending["extension"]);
    assert_eq!(exported["session_scopes"][0]["extension"], "second");
    assert_eq!(
        exported["session_scopes"][1]["extension"],
        original_pending["session_scopes"][0]["extension"]
    );
    intent.session_scopes.remove(0);
    writer
        .commit_delta(&CheckpointDelta {
            pending: PendingChange::Replace(intent),
            scan_cache: Some(ScanCache::default()),
            ..Default::default()
        })
        .unwrap();
    let exported = writer.reader().export_pending_json().unwrap().unwrap();
    assert_eq!(exported["session_scopes"].as_array().unwrap().len(), 1);
    assert_eq!(
        exported["session_scopes"][0]["extension"],
        original_pending["session_scopes"][0]["extension"]
    );
    assert_eq!(
        writer.reader().export_scan_cache_json().unwrap().unwrap()["extension"],
        original_cache["extension"]
    );
}

#[test]
fn lease_aware_sidecar_adapters_route_v2_while_unleased_setters_refuse() {
    let (_temp, path, lease) = fixture();
    v1_fixture(&path);
    let pending_path = path.with_file_name(PENDING);
    let cache_path = path.with_file_name(SCAN_CACHE);
    pending().save(&pending_path).unwrap();
    cache().save(&cache_path).unwrap();
    assert!(pending_path.exists());
    drop(CheckpointWriter::open(&path, &lease, false).unwrap());
    assert!(pending().save(&pending_path).is_err());
    assert!(PendingIngest::clear(&pending_path).is_err());
    assert!(cache().save(&cache_path).is_err());
    PendingIngest::clear_with_lease(&pending_path, &lease).unwrap();
    assert_eq!(PendingIngest::load(&pending_path).unwrap(), None);
    pending().save_with_lease(&pending_path, &lease).unwrap();
    ScanCache::default()
        .save_with_lease(&cache_path, &lease)
        .unwrap();
    assert_eq!(PendingIngest::load(&pending_path).unwrap(), Some(pending()));
    assert_eq!(ScanCache::load(&cache_path).unwrap().last_scan_ts, 0);
    assert!(!pending_path.exists());
    assert!(!cache_path.exists());
}

#[test]
fn unexplained_bootstrap_pending_or_cache_cannot_be_reinitialized() {
    for column in ["pending_json", "scancache_json"] {
        let (_temp, path, lease) = fixture();
        assert!(
            lifecycle::open_writer(
                &path,
                &lease,
                true,
                lifecycle::MigrationFailure::At("after_import_commit")
            )
            .is_err()
        );
        let connection = Connection::open(path.with_file_name(DATABASE)).unwrap();
        let payload = if column == "pending_json" {
            serde_json::to_string(&pending()).unwrap()
        } else {
            "{}".into()
        };
        connection
            .execute(&format!("UPDATE metadata SET {column}=?1"), [payload])
            .unwrap();
        drop(connection);
        assert!(CheckpointReader::open(&path).is_err());
        assert!(CheckpointWriter::open(&path, &lease, true).is_err());
    }
}

#[test]
fn reset_removes_active_sidecars_but_keeps_all_archives_and_lock_inode() {
    let (_temp, path, lease) = fixture();
    extended_legacy(&path);
    extended_sidecars(&path);
    drop(CheckpointWriter::open(&path, &lease, false).unwrap());
    extended_sidecars(&path);
    let archives: Vec<_> = fs::read_dir(path.parent().unwrap())
        .unwrap()
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().contains(".legacy-"))
        .map(|entry| entry.path())
        .collect();
    assert_eq!(archives.len(), 3);
    for archive in &archives {
        assert!(is_checkpoint_artifact_name(archive.file_name().unwrap()));
    }
    assert!(is_checkpoint_artifact_name(OsStr::new(PENDING)));
    assert!(is_checkpoint_artifact_name(OsStr::new(SCAN_CACHE)));
    reset(&path, &lease).unwrap();
    for name in ACTIVE_ARTIFACTS {
        assert!(!path.with_file_name(name).exists());
    }
    for archive in archives {
        assert!(archive.exists());
    }
    assert!(path.with_file_name(LOCK).exists());
}

#[test]
fn marker_backed_mutations_never_recreate_a_missing_lifecycle_lock() {
    for mode in ["v1", "transition", "v2-stale"] {
        let (_temp, path, lease) = fixture();
        let original = v1_fixture(&path);
        let (original_pending, original_cache) = extended_sidecars(&path);
        match mode {
            "transition" => assert!(
                lifecycle::open_writer(
                    &path,
                    &lease,
                    false,
                    lifecycle::MigrationFailure::At("after_import_commit"),
                )
                .is_err()
            ),
            "v2-stale" => {
                drop(CheckpointWriter::open(&path, &lease, false).unwrap());
                extended_sidecars(&path);
            }
            _ => {}
        }
        let held_reader = CheckpointReader::open(&path).unwrap();
        assert_eq!(held_reader.export_json().unwrap(), original);
        assert_eq!(
            held_reader.export_pending_json().unwrap(),
            Some(original_pending.clone())
        );
        assert_eq!(
            held_reader.export_scan_cache_json().unwrap(),
            Some(original_cache.clone())
        );
        let lock_path = path.with_file_name(LOCK);
        fs::remove_file(&lock_path).unwrap();
        let snapshot = || -> HashMap<PathBuf, Vec<u8>> {
            fs::read_dir(path.parent().unwrap())
                .unwrap()
                .map(|entry| {
                    let path = entry.unwrap().path();
                    let raw = fs::read(&path).unwrap();
                    (path, raw)
                })
                .collect()
        };
        let before = snapshot();
        let pending_path = path.with_file_name(PENDING);
        let cache_path = path.with_file_name(SCAN_CACHE);
        for operation in [
            "writer",
            "pending-save",
            "pending-clear",
            "cache-save",
            "leased-pending-save",
            "leased-pending-clear",
            "leased-cache-save",
            "legacy-save",
            "reset",
        ] {
            let result = match operation {
                "writer" => CheckpointWriter::open(&path, &lease, false).map(drop),
                "pending-save" => pending().save(&pending_path),
                "pending-clear" => PendingIngest::clear(&pending_path),
                "cache-save" => cache().save(&cache_path),
                "leased-pending-save" => pending().save_with_lease(&pending_path, &lease),
                "leased-pending-clear" => PendingIngest::clear_with_lease(&pending_path, &lease),
                "leased-cache-save" => cache().save_with_lease(&cache_path, &lease),
                "legacy-save" => IngestState::default().save(&path),
                "reset" => reset(&path, &lease),
                _ => unreachable!(),
            };
            assert!(result.is_err(), "{mode}: {operation}");
            assert!(!lock_path.exists(), "{mode}: {operation} recreated lock");
            assert_eq!(
                snapshot(),
                before,
                "{mode}: {operation} mutated checkpoint artifacts"
            );
        }
        assert_eq!(held_reader.export_json().unwrap(), original);
        assert_eq!(
            held_reader.export_pending_json().unwrap(),
            Some(original_pending)
        );
        assert_eq!(
            held_reader.export_scan_cache_json().unwrap(),
            Some(original_cache)
        );
    }
}

#[test]
fn missing_authority_cannot_recreate_a_lost_database_bootstrap_lock() {
    let (_temp, path, lease) = fixture();
    assert!(
        lifecycle::open_writer(
            &path,
            &lease,
            true,
            lifecycle::MigrationFailure::At("after_import_commit"),
        )
        .is_err()
    );
    let lock_path = path.with_file_name(LOCK);
    let held_lock = File::open(&lock_path).unwrap();
    held_lock.try_lock_shared().unwrap();
    fs::remove_file(&lock_path).unwrap();
    let snapshot = || -> HashMap<PathBuf, Vec<u8>> {
        fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                let raw = fs::read(&path).unwrap();
                (path, raw)
            })
            .collect()
    };
    let before = snapshot();
    assert!(CheckpointWriter::open(&path, &lease, true).is_err());
    assert!(!lock_path.exists());
    assert_eq!(snapshot(), before);
    assert!(pending().save(&path.with_file_name(PENDING)).is_err());
    assert!(PendingIngest::clear(&path.with_file_name(PENDING)).is_err());
    assert!(cache().save(&path.with_file_name(SCAN_CACHE)).is_err());
    assert!(
        pending()
            .save_with_lease(&path.with_file_name(PENDING), &lease)
            .is_err()
    );
    assert!(PendingIngest::clear_with_lease(&path.with_file_name(PENDING), &lease).is_err());
    assert!(
        cache()
            .save_with_lease(&path.with_file_name(SCAN_CACHE), &lease)
            .is_err()
    );
    assert!(IngestState::default().save(&path).is_err());
    assert!(reset(&path, &lease).is_err());
    assert!(!lock_path.exists());
    assert_eq!(snapshot(), before);
}

#[test]
fn optional_scan_cache_oversize_does_not_block_checkpoint_open() {
    let (_temp, path, lease) = fixture();
    drop(CheckpointWriter::open(&path, &lease, true).unwrap());
    let database = Connection::open(path.with_file_name(DATABASE)).unwrap();
    database.execute(
        r#"UPDATE metadata SET next_doc_id='19', scancache_json='{"padding":"' || printf('%.*c', ?1, 'x') || CAST(x'ff' AS TEXT) || '"}'"#,
        [80 * 1024 * 1024],
    ).unwrap();
    drop(database);
    let reader = CheckpointReader::open(&path).expect("oversize optional cache must expire");
    let header = reader.header().unwrap();
    assert_eq!(header.next_doc_id, 19);
    assert_eq!(header.scan_cache.last_scan_ts, 0);
    assert_eq!(reader.export_scan_cache_json().unwrap(), None);
    assert_eq!(
        ScanCache::load(&path.with_file_name(SCAN_CACHE))
            .unwrap()
            .last_scan_ts,
        0
    );
}

#[test]
fn optional_scan_cache_oversize_archival_survives_failure_and_retry() {
    use sha2::{Digest, Sha256};
    use std::io::{Read, Write};
    for v1 in [false, true] {
        let (_temp, path, lease) = fixture();
        if v1 {
            v1_fixture(&path);
        } else {
            extended_legacy(&path);
        }
        let cache_path = path.with_file_name(SCAN_CACHE);
        let mut source = File::create(&cache_path).unwrap();
        let block = [b'x'; 64 * 1024];
        let mut digest = Sha256::new();
        for _ in 0..1280 {
            source.write_all(&block).unwrap();
            digest.update(block);
        }
        source.write_all(b"last byte").unwrap();
        digest.update(b"last byte");
        drop(source);
        let digest = digest.finalize();
        let archive = path.with_file_name(format!("scan_cache.legacy-{digest:x}.json"));
        let marker = fs::read(&path).unwrap();
        assert!(
            lifecycle::open_writer(
                &path,
                &lease,
                false,
                lifecycle::MigrationFailure::At("after_cache_backup")
            )
            .is_err()
        );
        assert!(
            archive.exists(),
            "oversize cache must be archived before activation"
        );
        assert_eq!(fs::read(&path).unwrap(), marker);
        assert!(cache_path.exists());
        let mut archived = File::open(&archive).unwrap();
        let mut observed = Sha256::new();
        let mut buffer = [0; 64 * 1024];
        loop {
            let len = archived.read(&mut buffer).unwrap();
            if len == 0 {
                break;
            }
            observed.update(&buffer[..len]);
        }
        assert_eq!(observed.finalize(), digest);
        drop(archived);
        OpenOptions::new()
            .write(true)
            .open(&archive)
            .unwrap()
            .write_all(b"wrong")
            .unwrap();
        assert!(CheckpointWriter::open(&path, &lease, false).is_err());
        assert_eq!(fs::read(&path).unwrap(), marker);
        assert!(cache_path.exists());
        fs::copy(&cache_path, &archive).unwrap();
        let writer = CheckpointWriter::open(&path, &lease, false).unwrap();
        assert_eq!(writer.reader().header().unwrap().scan_cache.last_scan_ts, 0);
        assert_eq!(writer.reader().export_scan_cache_json().unwrap(), None);
        assert!(!cache_path.exists());
        assert_eq!(fs::metadata(&archive).unwrap().len(), 80 * 1024 * 1024 + 9);
    }
}
