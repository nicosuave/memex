use crate::config::Paths;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const INGEST_LEASE_TIMEOUT: Duration = Duration::from_secs(30);
const INGEST_LEASE_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug)]
pub enum LeaseAttempt {
    Acquired(IngestLease),
    Busy(Option<LeaseHolder>),
}

#[derive(Debug)]
pub struct IngestLease {
    file: File,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LeaseHolder {
    pub pid: u32,
    pub operation: String,
    pub started_at: u64,
}

impl IngestLease {
    pub fn try_acquire(paths: &Paths, operation: impl Into<String>) -> Result<LeaseAttempt> {
        let path = lease_path(paths);
        let mut file = open_lease_file(&path)?;
        match file.try_lock() {
            Ok(()) => {
                write_holder(&mut file, operation.into())?;
                Ok(LeaseAttempt::Acquired(Self { file }))
            }
            Err(TryLockError::WouldBlock) => Ok(LeaseAttempt::Busy(read_holder(&path))),
            Err(TryLockError::Error(error)) => Err(error)
                .with_context(|| format!("failed to acquire index lease {}", path.display())),
        }
    }

    pub fn acquire(paths: &Paths, operation: impl Into<String>, timeout: Duration) -> Result<Self> {
        crate::profiling::span!("ingest.lease_wait");
        let path = lease_path(paths);
        let operation = operation.into();
        let started = Instant::now();
        loop {
            match Self::try_acquire(paths, operation.clone())? {
                LeaseAttempt::Acquired(lease) => return Ok(lease),
                LeaseAttempt::Busy(_) if started.elapsed() < timeout => {
                    thread::sleep(INGEST_LEASE_POLL_INTERVAL);
                }
                LeaseAttempt::Busy(holder) => {
                    return Err(lease_timeout_error(&path, timeout, holder));
                }
            }
        }
    }
}

impl Drop for IngestLease {
    fn drop(&mut self) {
        let _ = self.file.set_len(0);
        let _ = self.file.unlock();
    }
}

/// Serializes the merge itself, which the ingest lease cannot: a compaction releases that
/// lease while it merges so searches keep working, which would otherwise let two detached
/// children merge the same segments at once.
#[derive(Debug)]
pub struct CompactionLock {
    _file: File,
}

impl CompactionLock {
    pub fn try_acquire(paths: &Paths) -> Result<Option<Self>> {
        let path = compaction_lock_path(paths);
        let file = open_lease_file(&path)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Self { _file: file })),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Error(error)) => Err(error)
                .with_context(|| format!("failed to acquire compaction lock {}", path.display())),
        }
    }
}

fn compaction_lock_path(paths: &Paths) -> PathBuf {
    let path = lease_path(paths);
    path.with_file_name(
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(".memex.ingest.lock")
            .replace("ingest.lock", "compaction.lock"),
    )
}

fn lease_path(paths: &Paths) -> PathBuf {
    let parent = paths.root.parent().unwrap_or_else(|| Path::new("."));
    let root_name = paths
        .root
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("memex");
    parent.join(format!(".{root_name}.ingest.lock"))
}

fn open_lease_file(path: &Path) -> Result<File> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("index lease path has no parent: {}", path.display()))?;
    std::fs::create_dir_all(parent)?;
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open index lease {}", path.display()))
}

fn write_holder(file: &mut File, operation: String) -> Result<()> {
    let holder = LeaseHolder {
        pid: std::process::id(),
        operation,
        started_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0),
    };
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    serde_json::to_writer(&mut *file, &holder)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

fn read_holder(path: &Path) -> Option<LeaseHolder> {
    let contents = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(contents.trim()).ok()
}

fn lease_timeout_error(
    path: &Path,
    timeout: Duration,
    holder: Option<LeaseHolder>,
) -> anyhow::Error {
    match holder {
        Some(holder) => anyhow!(
            "timed out after {:.1}s waiting for index lease held by pid {} for '{}' since unix timestamp {} ({})",
            timeout.as_secs_f32(),
            holder.pid,
            holder.operation,
            holder.started_at,
            path.display()
        ),
        None => anyhow!(
            "timed out after {:.1}s waiting for index lease {}",
            timeout.as_secs_f32(),
            path.display()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contended_lease_reports_holder() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(temp.path().join("memex"))).expect("paths");
        let first = match IngestLease::try_acquire(&paths, "first").expect("first lease") {
            LeaseAttempt::Acquired(lease) => lease,
            LeaseAttempt::Busy(_) => panic!("first lease should be available"),
        };

        let holder = match IngestLease::try_acquire(&paths, "second").expect("second lease") {
            LeaseAttempt::Acquired(_) => panic!("second lease should be contended"),
            LeaseAttempt::Busy(holder) => holder.expect("holder metadata"),
        };

        assert_eq!(holder.pid, std::process::id());
        assert_eq!(holder.operation, "first");
        drop(first);
        assert!(matches!(
            IngestLease::try_acquire(&paths, "third").expect("third lease"),
            LeaseAttempt::Acquired(_)
        ));
    }

    #[test]
    fn a_second_compaction_is_refused_while_the_first_holds_its_own_lock() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(temp.path().join("memex"))).expect("paths");
        assert_ne!(compaction_lock_path(&paths), lease_path(&paths));

        let first = CompactionLock::try_acquire(&paths)
            .expect("first compaction lock")
            .expect("first compaction should be available");
        assert!(
            CompactionLock::try_acquire(&paths)
                .expect("second compaction lock")
                .is_none()
        );

        let ingest = match IngestLease::try_acquire(&paths, "refresh").expect("ingest lease") {
            LeaseAttempt::Acquired(lease) => lease,
            LeaseAttempt::Busy(_) => panic!("compaction lock must not hold the ingest lease"),
        };
        drop(ingest);

        drop(first);
        assert!(
            CompactionLock::try_acquire(&paths)
                .expect("third compaction lock")
                .is_some()
        );
    }

    #[test]
    fn timed_out_lease_names_the_holder() {
        let temp = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(temp.path().join("memex"))).expect("paths");
        let _first = match IngestLease::try_acquire(&paths, "long reindex").expect("first lease") {
            LeaseAttempt::Acquired(lease) => lease,
            LeaseAttempt::Busy(_) => panic!("first lease should be available"),
        };

        let error = IngestLease::acquire(&paths, "second index", Duration::from_millis(1))
            .expect_err("lease should time out");
        let message = error.to_string();

        assert!(message.contains(&format!("pid {}", std::process::id())));
        assert!(message.contains("long reindex"));
    }
}
