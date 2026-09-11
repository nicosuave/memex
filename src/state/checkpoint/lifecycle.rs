use super::*;
use rusqlite::config::DbConfig;
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS metadata (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    format_version INTEGER NOT NULL,
    store_id TEXT NOT NULL,
    origin TEXT NOT NULL,
    next_doc_id TEXT NOT NULL CHECK(typeof(next_doc_id)='text'),
    opencode_databases TEXT NOT NULL CHECK(json_valid(opencode_databases) AND json_type(opencode_databases)='object'),
    legacy_extras TEXT NOT NULL CHECK(json_valid(legacy_extras) AND json_type(legacy_extras)='object'),
    pending_json TEXT CHECK(pending_json IS NULL OR (json_valid(pending_json) AND json_type(pending_json)='object')),
    scancache_json TEXT CHECK(scancache_json IS NULL OR (json_valid(scancache_json) AND json_type(scancache_json)='object'))
);
CREATE TABLE IF NOT EXISTS files (
    path TEXT PRIMARY KEY NOT NULL,
    payload TEXT NOT NULL CHECK(json_valid(payload) AND json_type(payload)='object'),
    mtime INTEGER GENERATED ALWAYS AS (json_extract(payload,'$.mtime')) STORED NOT NULL
        CHECK(json_type(payload,'$.mtime')='integer' AND typeof(mtime)='integer')
);
CREATE INDEX IF NOT EXISTS files_mtime ON files(mtime);
";

pub(super) enum MigrationFailure {
    None,
    #[cfg(test)]
    At(&'static str),
}

impl MigrationFailure {
    fn check(&self, _point: &str) -> Result<()> {
        #[cfg(test)]
        if matches!(self, Self::At(point) if *point == _point) {
            bail!("injected migration failure at {_point}");
        }
        Ok(())
    }
}

enum Authority {
    Missing,
    Legacy { raw: Vec<u8>, value: Value },
    Marker { identity: String, version: i64 },
}

fn authority(path: &Path) -> Result<Authority> {
    let raw = match fs::read(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Authority::Missing);
        }
        Err(error) => return Err(error.into()),
    };
    let value: Value =
        serde_json::from_slice(&raw).context("invalid ingest checkpoint authority")?;
    if let Some(marker) = value.as_str() {
        let marker = marker
            .strip_prefix(MARKER_PREFIX)
            .context("unknown ingest checkpoint marker")?;
        let (version, store_id) = marker
            .split_once(':')
            .context("malformed ingest checkpoint marker")?;
        ensure!(
            matches!(version, "1" | "2"),
            "unsupported checkpoint marker version {version}"
        );
        ensure!(
            valid_identity(store_id),
            "invalid checkpoint store identity"
        );
        return Ok(Authority::Marker {
            identity: store_id.to_owned(),
            version: version.parse()?,
        });
    }
    serde_json::from_slice::<IngestState>(&raw).context("invalid legacy ingest checkpoint")?;
    Ok(Authority::Legacy { raw, value })
}

fn valid_identity(identity: &str) -> bool {
    identity.len() == 64
        && identity
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn location(state_path: &Path, name: &str) -> Result<PathBuf> {
    Ok(super::super::parent_directory(state_path)?.join(name))
}

fn lifecycle_lock(state_path: &Path, exclusive: bool, create: bool) -> Result<File> {
    let path = location(state_path, LOCK)?;
    let open_existing = || OpenOptions::new().read(true).write(create).open(&path);
    let file = match open_existing() {
        Err(error) if create && error.kind() == std::io::ErrorKind::NotFound => {
            match fs::read(state_path) {
                Ok(raw) => {
                    let value = serde_json::from_slice::<Value>(&raw);
                    ensure!(
                        !matches!(value, Ok(Value::String(_))),
                        "missing checkpoint lifecycle lock for marker-backed state: {}",
                        path.display()
                    );
                    if location(state_path, DATABASE)?.try_exists()? {
                        serde_json::from_slice::<IngestState>(&raw).context(
                            "cannot create lifecycle lock for invalid checkpoint authority",
                        )?;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    ensure!(
                        !location(state_path, DATABASE)?.try_exists()?,
                        "checkpoint database has no authority or lifecycle lock"
                    );
                }
                Err(error) => return Err(error.into()),
            }
            fs::create_dir_all(super::super::parent_directory(state_path)?)?;
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => open_existing(),
                result => result,
            }
        }
        result => result,
    }
    .with_context(|| {
        format!(
            "missing or inaccessible checkpoint lifecycle lock {}",
            path.display()
        )
    })?;
    crate::profiling::span!("state.checkpoint.lock_wait");
    let started = Instant::now();
    loop {
        let result = if exclusive {
            file.try_lock()
        } else {
            file.try_lock_shared()
        };
        match result {
            Ok(()) => return Ok(file),
            Err(TryLockError::WouldBlock) => {
                crate::profiling::count!("state.checkpoint.lock_contention", 1);
                if started.elapsed() >= Duration::from_millis(500) {
                    bail!("checkpoint lifecycle lock is busy: {}", path.display());
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(TryLockError::Error(error)) => {
                return Err(error).context("failed to lock checkpoint lifecycle");
            }
        }
    }
}

fn open_connection(state_path: &Path, writable: bool, create: bool) -> Result<Connection> {
    let mut flags = if writable {
        OpenFlags::SQLITE_OPEN_READ_WRITE
    } else {
        OpenFlags::SQLITE_OPEN_READ_ONLY
    };
    flags |= OpenFlags::SQLITE_OPEN_NO_MUTEX;
    if create {
        flags |= OpenFlags::SQLITE_OPEN_CREATE;
    }
    let connection = Connection::open_with_flags(location(state_path, DATABASE)?, flags)
        .context("cannot open authoritative checkpoint database")?;
    connection.busy_timeout(Duration::from_millis(500))?;
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)?;
    Ok(connection)
}

fn configure_writer(connection: &Connection) -> Result<()> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "fullfsync", false)?;
    connection.pragma_update(None, "checkpoint_fullfsync", false)?;
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)?;
    connection.pragma_update(None, "wal_autocheckpoint", 1000)?;
    connection.pragma_update(None, "journal_size_limit", 16 * 1024 * 1024)?;
    let mode: String = connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    ensure!(
        mode.eq_ignore_ascii_case("wal"),
        "checkpoint journal_mode is not WAL"
    );
    for (name, expected) in [
        ("synchronous", 2),
        ("fullfsync", 0),
        ("checkpoint_fullfsync", 0),
        ("wal_autocheckpoint", 1000),
        ("journal_size_limit", 16 * 1024 * 1024),
    ] {
        let value: i64 = connection.pragma_query_value(None, name, |row| row.get(0))?;
        ensure!(
            value == expected,
            "checkpoint {name} is {value}, expected {expected}"
        );
    }
    ensure!(
        connection.db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE)?,
        "checkpoint close maintenance could not be disabled"
    );
    Ok(())
}

fn validate(connection: &Connection, expected_identity: &str, marker_version: i64) -> Result<i64> {
    crate::profiling::span!("state.checkpoint.validate");
    let (version, identity, next_id, databases, extras): (i64, String, String, String, String) = connection.query_row(
        "SELECT format_version,store_id,next_doc_id,opencode_databases,legacy_extras FROM metadata WHERE singleton=1", [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
    ).context("invalid checkpoint metadata")?;
    ensure!(
        matches!(version, 1 | 2) && version >= marker_version,
        "unsupported checkpoint database format {version}"
    );
    ensure!(
        identity == expected_identity,
        "checkpoint store identity mismatch"
    );
    parse_next_id(&next_id)?;
    serde_json::from_str::<HashMap<String, OpencodeDatabaseState>>(&databases)?;
    serde_json::from_str::<Map<String, Value>>(&extras)?;
    connection
        .prepare("SELECT path,payload,mtime FROM files INDEXED BY files_mtime WHERE mtime>=?1")?;
    if version == 2 {
        let (pending, cache_type): (Option<String>, String) = connection
            .query_row(
                "SELECT pending_json,typeof(scancache_json) FROM metadata WHERE singleton=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .context("incomplete checkpoint v2 metadata")?;
        ensure!(
            matches!(cache_type.as_str(), "text" | "null"),
            "invalid checkpoint scan cache column type"
        );
        codec::pending_document(pending.as_deref().map(str::as_bytes))?;
    }
    Ok(version)
}

pub(super) fn open_reader(state_path: &Path) -> Result<CheckpointReader> {
    crate::profiling::span!("state.checkpoint.open_reader");
    match authority(state_path)? {
        Authority::Legacy { value, .. } => Ok(CheckpointReader {
            backend: Backend::Legacy(value),
            state_path: state_path.to_owned(),
        }),
        Authority::Missing => {
            validate_missing(state_path)?;
            Ok(CheckpointReader {
                backend: Backend::Legacy(serde_json::to_value(IngestState::default())?),
                state_path: state_path.to_owned(),
            })
        }
        Authority::Marker { identity, version } => {
            open_active(state_path, &identity, version, false)
        }
    }
}

fn open_active(
    state_path: &Path,
    identity: &str,
    marker_version: i64,
    writable: bool,
) -> Result<CheckpointReader> {
    let lease = lifecycle_lock(state_path, false, false)?;
    ensure!(
        matches!(authority(state_path)?, Authority::Marker { identity: current, version } if current == identity && version == marker_version),
        "checkpoint authority changed while opening"
    );
    let connection = open_connection(state_path, writable, false)?;
    let version = validate(&connection, identity, marker_version)?;
    if writable {
        ensure!(
            version == FORMAT_VERSION && marker_version == FORMAT_VERSION,
            "checkpoint upgrade required"
        );
        configure_writer(&connection)?;
    }
    Ok(CheckpointReader {
        backend: Backend::Sqlite {
            connection,
            version,
            _lease: lease,
        },
        state_path: state_path.to_owned(),
    })
}

pub(super) fn has_authority(state_path: &Path) -> Result<bool> {
    match authority(state_path)? {
        Authority::Missing => {
            validate_missing(state_path)?;
            Ok(false)
        }
        Authority::Legacy { .. } => Ok(true),
        Authority::Marker { identity, version } => {
            open_active(state_path, &identity, version, false)?;
            Ok(true)
        }
    }
}

fn validate_missing(state_path: &Path) -> Result<()> {
    if !location(state_path, DATABASE)?.try_exists()? {
        return Ok(());
    }
    let mut lease = lifecycle_lock(state_path, false, false)?;
    ensure!(
        matches!(authority(state_path)?, Authority::Missing),
        "checkpoint authority changed while opening"
    );
    validate_bootstrap(state_path, &mut lease)
}

fn bootstrap_identity(lease: &mut File) -> Result<String> {
    lease.seek(SeekFrom::Start(0))?;
    let mut receipt = String::new();
    lease.read_to_string(&mut receipt)?;
    let identity = receipt
        .strip_prefix("bootstrap:")
        .context("checkpoint database has no authority or bootstrap receipt")?;
    ensure!(
        valid_identity(identity),
        "invalid checkpoint bootstrap identity"
    );
    Ok(identity.to_owned())
}

fn validate_bootstrap(state_path: &Path, lease: &mut File) -> Result<()> {
    let identity = bootstrap_identity(lease)?;
    let connection = open_connection(state_path, false, false)?;
    let tables: i64 = connection.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type='table'",
        [],
        |row| row.get(0),
    )?;
    if tables == 0 {
        return Ok(());
    }
    let version = validate(&connection, &identity, 1)?;
    let sidecars_empty: bool = version == 1 || connection.query_row(
        "SELECT pending_json IS NULL AND scancache_json IS NULL FROM metadata WHERE singleton=1", [], |row| row.get(0),
    )?;
    let empty: bool = connection.query_row(
        "SELECT origin=?1 AND next_doc_id='1' AND opencode_databases='{}' AND legacy_extras='{}' AND NOT EXISTS(SELECT 1 FROM files) FROM metadata WHERE singleton=1",
        [format!("bootstrap:{identity}")], |row| row.get(0),
    )?;
    ensure!(
        empty && sidecars_empty,
        "populated checkpoint database has no authority; restore a consistent snapshot or rebuild"
    );
    Ok(())
}

pub(super) fn open_writer(
    state_path: &Path,
    _ingest_lease: &IngestLease,
    allow_initialize: bool,
    failure: MigrationFailure,
) -> Result<CheckpointWriter> {
    crate::profiling::span!("state.checkpoint.open_writer");
    match authority(state_path)? {
        Authority::Marker {
            identity,
            version: 2,
        } if !sidecars_exist(state_path)? => {
            return Ok(CheckpointWriter {
                reader: open_active(state_path, &identity, 2, true)?,
            });
        }
        Authority::Missing if !allow_initialize => {
            bail!(
                "missing checkpoint authority; initialization requires empty-root or pending recovery validation"
            );
        }
        _ => {}
    }
    crate::profiling::span!("state.checkpoint.upgrade");
    let mut lease = lifecycle_lock(state_path, true, true)?;
    let authority = authority(state_path)?;
    if let Authority::Marker {
        identity,
        version: marker_version,
    } = authority
    {
        let mut connection = open_connection(state_path, true, false)?;
        let version = validate(&connection, &identity, marker_version)?;
        configure_writer(&connection)?;
        if version == 1 {
            let (pending, cache) = archive_sidecars(state_path, &failure)?;
            failure.check("before_import")?;
            let transaction = connection.transaction()?;
            add_v2_columns(&transaction)?;
            transaction.execute(
                "UPDATE metadata SET pending_json=?1,scancache_json=?2,format_version=2 WHERE singleton=1",
                params![pending, cache],
            )?;
            failure.check("before_import_commit")?;
            transaction.commit()?;
            failure.check("after_import_commit")?;
        }
        if marker_version == 1 {
            activate(state_path, &connection, &identity, &failure)?;
        }
        cleanup_sidecars(state_path, &failure)?;
        drop(connection);
        drop(lease);
        return Ok(CheckpointWriter {
            reader: open_active(state_path, &identity, 2, true)?,
        });
    }
    let (pending, cache) = archive_sidecars(state_path, &failure)?;
    let (value, identity, origin) = match authority {
        Authority::Legacy { raw, value } => {
            failure.check("before_backup")?;
            let digest = format!("{:x}", Sha256::digest(&raw));
            backup(state_path, &raw, "ingest", &digest)?;
            failure.check("after_backup")?;
            (value, new_identity()?, format!("legacy:{digest}"))
        }
        Authority::Missing => {
            ensure!(
                allow_initialize,
                "missing checkpoint authority; initialization requires empty-root or pending recovery validation"
            );
            let identity = if location(state_path, DATABASE)?.try_exists()? {
                validate_bootstrap(state_path, &mut lease)?;
                bootstrap_identity(&mut lease)?
            } else {
                let identity = new_identity()?;
                lease.set_len(0)?;
                lease.seek(SeekFrom::Start(0))?;
                write!(lease, "bootstrap:{identity}")?;
                lease.sync_all()?;
                super::super::sync_directory(super::super::parent_directory(state_path)?)?;
                identity
            };
            (
                serde_json::to_value(IngestState::default())?,
                identity.clone(),
                format!("bootstrap:{identity}"),
            )
        }
        Authority::Marker { .. } => unreachable!(),
    };
    let mut connection = open_connection(state_path, true, true)?;
    configure_writer(&connection)?;
    let state: IngestState = serde_json::from_value(value.clone())?;
    failure.check("before_import")?;
    {
        let transaction = connection.transaction()?;
        transaction.execute_batch(SCHEMA)?;
        let columns: i64 = transaction.query_row(
            "SELECT count(*) FROM pragma_table_info('metadata') WHERE name IN ('pending_json','scancache_json')", [], |row| row.get(0),
        )?;
        if columns == 0 {
            add_v2_columns(&transaction)?;
        }
        ensure!(
            columns == 0 || columns == 2,
            "incomplete checkpoint v2 schema"
        );
        transaction.execute("DELETE FROM files", [])?;
        transaction.execute("DELETE FROM metadata", [])?;
        transaction.execute(
            "INSERT INTO metadata(singleton,format_version,store_id,origin,next_doc_id,opencode_databases,legacy_extras,pending_json,scancache_json) VALUES(1,?1,?2,?3,?4,?5,?6,?7,?8)",
            params![FORMAT_VERSION, identity, origin, state.next_doc_id.to_string(), serde_json::to_string(value.get("opencode_databases").unwrap_or(&serde_json::json!({})))?, codec::legacy_extras(&value)?, pending, cache],
        )?;
        {
            let mut insert =
                transaction.prepare("INSERT INTO files(path,payload) VALUES(?1,?2)")?;
            for (path, payload) in value["files"].as_object().context("invalid legacy files")? {
                insert.execute(params![path, serde_json::to_string(payload)?])?;
            }
        }
        failure.check("before_import_commit")?;
        transaction.commit()?;
    }
    failure.check("after_import_commit")?;
    activate(state_path, &connection, &identity, &failure)?;
    cleanup_sidecars(state_path, &failure)?;
    drop(connection);
    drop(lease);
    Ok(CheckpointWriter {
        reader: open_active(state_path, &identity, 2, true)?,
    })
}

fn add_v2_columns(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "ALTER TABLE metadata ADD COLUMN pending_json TEXT CHECK(pending_json IS NULL OR (json_valid(pending_json) AND json_type(pending_json)='object'));
         ALTER TABLE metadata ADD COLUMN scancache_json TEXT CHECK(scancache_json IS NULL OR (json_valid(scancache_json) AND json_type(scancache_json)='object'));",
    )?;
    Ok(())
}

pub(super) fn read_sidecar(state_path: &Path, name: &str) -> Result<Option<Vec<u8>>> {
    use std::io::Read;
    let mut file = match File::open(location(state_path, name)?) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut raw = Vec::new();
    if name == SCAN_CACHE {
        if file.metadata()?.len() > ScanCache::MAX_JSON_BYTES {
            return Ok(None);
        }
        file.take(ScanCache::MAX_JSON_BYTES + 1)
            .read_to_end(&mut raw)?;
        if raw.len() as u64 > ScanCache::MAX_JSON_BYTES {
            return Ok(None);
        }
    } else {
        file.read_to_end(&mut raw)?;
    }
    Ok(Some(raw))
}

fn archive_sidecars(
    state_path: &Path,
    failure: &MigrationFailure,
) -> Result<(Option<String>, Option<String>)> {
    let pending = read_sidecar(state_path, PENDING)?;
    let cache = read_sidecar(state_path, SCAN_CACHE)?;
    let pending_value = codec::pending_document(pending.as_deref())?;
    let cache_value = codec::scan_cache_document(cache.as_deref());
    failure.check("before_sidecar_backup")?;
    for (name, raw) in [(PENDING, pending), (SCAN_CACHE, cache)] {
        if let Some(raw) = raw {
            let digest = format!("{:x}", Sha256::digest(&raw));
            backup(state_path, &raw, name.trim_end_matches(".json"), &digest)?;
        } else if name == SCAN_CACHE && location(state_path, name)?.try_exists()? {
            backup_sidecar_stream(state_path, name)?;
        }
        failure.check(if name == PENDING {
            "after_pending_backup"
        } else {
            "after_cache_backup"
        })?;
    }
    failure.check("after_sidecar_backup")?;
    Ok((
        pending_value
            .map(|value| serde_json::to_string(&value))
            .transpose()?,
        cache_value
            .map(|value| serde_json::to_string(&value))
            .transpose()?,
    ))
}

fn sidecars_exist(state_path: &Path) -> Result<bool> {
    Ok(location(state_path, PENDING)?.try_exists()?
        || location(state_path, SCAN_CACHE)?.try_exists()?)
}

fn activate(
    state_path: &Path,
    connection: &Connection,
    identity: &str,
    failure: &MigrationFailure,
) -> Result<()> {
    failure.check("before_checkpoint")?;
    checkpoint(connection)?;
    failure.check("after_checkpoint")?;
    failure.check("before_database_sync")?;
    File::open(location(state_path, DATABASE)?)?.sync_all()?;
    failure.check("after_database_file_sync")?;
    super::super::sync_directory(super::super::parent_directory(state_path)?)?;
    failure.check("after_database_sync")?;
    let marker = serde_json::to_vec(&format!("{MARKER_PREFIX}{FORMAT_VERSION}:{identity}"))?;
    failure.check("before_marker")?;
    super::super::atomic_write(state_path, &marker)?;
    failure.check("after_marker")
}

fn cleanup_sidecars(state_path: &Path, failure: &MigrationFailure) -> Result<()> {
    failure.check("before_cleanup")?;
    let mut attempted = false;
    let result: Result<()> = (|| {
        for name in [PENDING, SCAN_CACHE] {
            let path = location(state_path, name)?;
            if path.try_exists()? {
                attempted = true;
                match fs::remove_file(path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                failure.check(if name == PENDING {
                    "after_pending_cleanup"
                } else {
                    "after_cache_cleanup"
                })?;
            }
        }
        Ok(())
    })();
    if attempted {
        super::super::sync_directory(super::super::parent_directory(state_path)?)?;
    }
    result?;
    failure.check("after_cleanup")
}

fn new_identity() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|error| anyhow::anyhow!("checkpoint store identity: {error}"))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn backup(state_path: &Path, raw: &[u8], stem: &str, digest: &str) -> Result<()> {
    let mut temporary =
        tempfile::NamedTempFile::new_in(super::super::parent_directory(state_path)?)?;
    temporary.write_all(raw)?;
    publish_backup(state_path, temporary, stem, digest)
}

fn backup_sidecar_stream(state_path: &Path, name: &str) -> Result<()> {
    let mut source = File::open(location(state_path, name)?)?;
    let mut temporary =
        tempfile::NamedTempFile::new_in(super::super::parent_directory(state_path)?)?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let len = source.read(&mut buffer)?;
        if len == 0 {
            break;
        }
        temporary.write_all(&buffer[..len])?;
        digest.update(&buffer[..len]);
    }
    publish_backup(
        state_path,
        temporary,
        name.trim_end_matches(".json"),
        &format!("{:x}", digest.finalize()),
    )
}

fn publish_backup(
    state_path: &Path,
    temporary: tempfile::NamedTempFile,
    stem: &str,
    digest: &str,
) -> Result<()> {
    let path = location(state_path, &format!("{stem}.legacy-{digest}.json"))?;
    temporary.as_file().sync_all()?;
    if let Err(mut error) = temporary.persist_noclobber(&path) {
        if error.error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(error.error.into());
        }
        verify_backup(&path, error.file.as_file_mut())?;
        File::open(&path)?.sync_all()?;
    }
    super::super::sync_directory(super::super::parent_directory(state_path)?)
}

fn verify_backup(path: &Path, expected: &mut File) -> Result<()> {
    let mut actual = File::open(path)?;
    ensure!(
        actual.metadata()?.len() == expected.metadata()?.len(),
        "legacy checkpoint backup content mismatch"
    );
    expected.seek(SeekFrom::Start(0))?;
    let mut expected_bytes = [0; 64 * 1024];
    let mut actual_bytes = [0; 64 * 1024];
    loop {
        let len = expected.read(&mut expected_bytes)?;
        if len == 0 {
            break;
        }
        actual.read_exact(&mut actual_bytes[..len])?;
        ensure!(
            actual_bytes[..len] == expected_bytes[..len],
            "legacy checkpoint backup content mismatch"
        );
    }
    ensure!(
        actual.read(&mut actual_bytes[..1])? == 0,
        "legacy checkpoint backup content mismatch"
    );
    Ok(())
}

pub(super) fn checkpoint(connection: &Connection) -> Result<()> {
    crate::profiling::span!("state.checkpoint.maintenance");
    let (busy, log, checkpointed): (i64, i64, i64) =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    ensure!(
        busy == 0 && log == 0 && checkpointed == 0,
        "checkpoint WAL truncate did not complete (busy={busy}, log={log}, checkpointed={checkpointed})"
    );
    Ok(())
}

pub(super) fn save_sidecar(path: &Path, data: Option<&[u8]>) -> Result<()> {
    let state_path = path.with_file_name("ingest.json");
    let _lease = lifecycle_lock(&state_path, false, true)?;
    ensure!(
        super::sidecar_reader(path)?.is_none(),
        "cannot write checkpoint v2 sidecar; use the lease-bearing API"
    );
    if let Some(data) = data {
        return super::super::atomic_write(path, data);
    }
    match fs::remove_file(path) {
        Ok(()) => super::super::sync_directory(super::super::parent_directory(path)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn save_legacy(state: &IngestState, state_path: &Path) -> Result<()> {
    let _lease = lifecycle_lock(state_path, false, true)?;
    if let Ok(raw) = fs::read(state_path) {
        if let Ok(Value::String(_)) = serde_json::from_slice::<Value>(&raw) {
            bail!("cannot overwrite checkpoint marker with legacy JSON; use save_with_lease");
        }
        if location(state_path, DATABASE)?.try_exists()? {
            serde_json::from_slice::<IngestState>(&raw)
                .context("cannot overwrite invalid checkpoint authority with legacy JSON")?;
        }
    } else {
        ensure!(
            !location(state_path, DATABASE)?.try_exists()?,
            "cannot replace missing checkpoint authority with legacy JSON"
        );
    }
    super::super::atomic_write(state_path, serde_json::to_string_pretty(state)?.as_bytes())
}

pub(super) fn reset(state_path: &Path, _ingest_lease: &IngestLease) -> Result<()> {
    let _lease = lifecycle_lock(state_path, true, true)?;
    for path in std::iter::once(state_path.to_owned()).chain(
        ACTIVE_ARTIFACTS
            .iter()
            .map(|name| state_path.with_file_name(name)),
    ) {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    super::super::sync_directory(super::super::parent_directory(state_path)?)
}
