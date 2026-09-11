//! Root-scoped daemon lifetime and readiness information.
//!
//! The OS lock establishes liveness; the JSON file alone is never evidence of a
//! running process. This information is for status and activation checks, not
//! authority to send signals to a PID.

use crate::config::Paths;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RuntimeInfo {
    pub pid: u32,
    pub version: String,
    pub executable: PathBuf,
    pub executable_id: String,
    pub started_at: u64,
    pub ready: bool,
    pub instance_id: String,
}

/// Hold this guard for the entire daemon lifetime, including initialization.
#[derive(Debug)]
pub struct DaemonRuntime {
    lock: File,
    state_path: PathBuf,
    info: RuntimeInfo,
}

impl DaemonRuntime {
    pub fn start(paths: &Paths) -> Result<Self> {
        std::fs::create_dir_all(&paths.state)?;
        let mut lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path(paths))
            .context("failed to open daemon runtime lock")?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(anyhow!(
                    "a daemon is already running for {}",
                    paths.root.display()
                ));
            }
            Err(TryLockError::Error(error)) => {
                return Err(error).context("failed to acquire daemon runtime lock");
            }
        }

        let mut nonce = [0_u8; 16];
        getrandom::fill(&mut nonce).map_err(|error| anyhow!("runtime identity: {error}"))?;
        let instance_id = hex(&nonce);
        // Change the token before publishing the new state, so readers cannot
        // mistake a previous daemon's JSON for this lock holder's information.
        lock.set_len(0)?;
        lock.seek(SeekFrom::Start(0))?;
        lock.write_all(instance_id.as_bytes())?;
        lock.flush()?;

        let runtime = Self {
            lock,
            state_path: state_path(paths),
            info: RuntimeInfo {
                pid: std::process::id(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                executable: std::env::current_exe().context("resolve daemon executable")?,
                executable_id: current_executable_identity()?,
                started_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                ready: false,
                instance_id,
            },
        };
        runtime.publish()?;
        Ok(runtime)
    }

    /// Call only after the daemon has initialized its requested services.
    pub fn mark_ready(&mut self) -> Result<()> {
        self.info.ready = true;
        self.publish()
    }

    fn publish(&self) -> Result<()> {
        let parent = self.state_path.parent().context("runtime state parent")?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        serde_json::to_writer(&mut temporary, &self.info)?;
        temporary.write_all(b"\n")?;
        temporary.flush()?;
        temporary.persist(&self.state_path)?;
        Ok(())
    }
}

impl Drop for DaemonRuntime {
    fn drop(&mut self) {
        // Remove metadata while still holding the lock. Never unlink the lock
        // file: another opener must always contend on the same inode.
        let _ = std::fs::remove_file(&self.state_path);
        let _ = self.lock.unlock();
    }
}

/// Read a snapshot only when a daemon holds the runtime lock. A stale PID in a
/// file cannot establish liveness, including when the OS has reused that PID.
pub fn read(paths: &Paths) -> Result<Option<RuntimeInfo>> {
    let lock_path = lock_path(paths);
    let lock = match OpenOptions::new().read(true).write(true).open(&lock_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("open daemon runtime lock"),
    };
    if !is_locked(&lock)? {
        return Ok(None);
    }
    let token = std::fs::read_to_string(&lock_path)?;
    let contents = match std::fs::read(state_path(paths)) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("read daemon runtime state"),
    };
    let info: RuntimeInfo =
        serde_json::from_slice(&contents).context("parse daemon runtime state")?;
    // A restart may happen between the reads. Reject metadata from a different
    // lock generation and check that the lock is still held before returning.
    if token.is_empty()
        || info.instance_id != token
        || std::fs::read_to_string(&lock_path)? != token
        || !is_locked(&lock)?
    {
        return Ok(None);
    }
    Ok(Some(info))
}

fn is_locked(file: &File) -> Result<bool> {
    match file.try_lock() {
        Ok(()) => {
            file.unlock()?;
            Ok(false)
        }
        Err(TryLockError::WouldBlock) => Ok(true),
        Err(TryLockError::Error(error)) => Err(error).context("check daemon runtime lock"),
    }
}

/// Identify the installed file without reading the executable's contents.
///
/// Unix inode and change timestamps distinguish ordinary replacements and
/// same-version rebuilds, including replacements preserving size and mtime.
/// This is advisory file identity, not proof of content equality: copying the
/// same bytes to a new inode can trigger a harmless extra daemon handoff.
pub fn executable_identity(path: &Path) -> Result<String> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("read executable identity: {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(format!(
            "unix:{}:{}:{}:{}:{}:{}:{}",
            metadata.dev(),
            metadata.ino(),
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        ))
    }
    #[cfg(not(unix))]
    {
        let modified = metadata.modified()?.duration_since(UNIX_EPOCH)?.as_nanos();
        Ok(format!("file:{}:{modified}", metadata.len()))
    }
}

/// Capture once at startup, before an updater can replace the executable path.
pub fn current_executable_identity() -> Result<String> {
    static IDENTITY: OnceLock<std::result::Result<String, String>> = OnceLock::new();
    IDENTITY
        .get_or_init(|| {
            std::env::current_exe()
                .map_err(anyhow::Error::from)
                .and_then(|path| executable_identity(&path))
                .map_err(|error| format!("{error:#}"))
        })
        .clone()
        .map_err(anyhow::Error::msg)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn lock_path(paths: &Paths) -> PathBuf {
    paths.state.join("daemon-runtime.lock")
}

fn state_path(paths: &Paths) -> PathBuf {
    paths.state.join("daemon-runtime.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_tracks_readiness_and_releases_ownership() {
        let temp = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        assert!(read(&paths).unwrap().is_none());
        let mut runtime = DaemonRuntime::start(&paths).unwrap();
        let initial = read(&paths).unwrap().unwrap();
        assert_eq!(initial.pid, std::process::id());
        assert!(!initial.ready);
        assert!(DaemonRuntime::start(&paths).is_err());
        runtime.mark_ready().unwrap();
        assert!(read(&paths).unwrap().unwrap().ready);
        drop(runtime);
        assert!(read(&paths).unwrap().is_none());
        let _next = DaemonRuntime::start(&paths).unwrap();
        assert_ne!(
            read(&paths).unwrap().unwrap().instance_id,
            initial.instance_id
        );
    }

    #[test]
    fn stale_state_with_live_pid_is_not_a_running_daemon() {
        let temp = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        let runtime = DaemonRuntime::start(&paths).unwrap();
        let stale = std::fs::read(state_path(&paths)).unwrap();
        drop(runtime);
        std::fs::write(state_path(&paths), &stale).unwrap();
        assert!(read(&paths).unwrap().is_none());
        let _next = DaemonRuntime::start(&paths).unwrap();
        // Even an active lock cannot authenticate JSON from the old instance.
        std::fs::write(state_path(&paths), stale).unwrap();
        assert!(read(&paths).unwrap().is_none());
    }

    #[test]
    fn independent_roots_can_each_run_a_daemon() {
        let temp = tempfile::tempdir().unwrap();
        let first = Paths::new(Some(temp.path().join("first"))).unwrap();
        let second = Paths::new(Some(temp.path().join("second"))).unwrap();
        let _first = DaemonRuntime::start(&first).unwrap();
        let _second = DaemonRuntime::start(&second).unwrap();
        assert!(read(&first).unwrap().is_some());
        assert!(read(&second).unwrap().is_some());
    }

    #[test]
    fn executable_identity_distinguishes_same_version_rebuilds() {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("memex");
        std::fs::write(&executable, b"version 1.0 build A").unwrap();
        let first = executable_identity(&executable).unwrap();
        assert_eq!(first, executable_identity(&executable).unwrap());
        // Ensure successive writes fall in different filesystem timestamp ticks.
        std::thread::sleep(std::time::Duration::from_millis(2));
        std::fs::write(&executable, b"version 1.0 build B").unwrap();
        assert_ne!(first, executable_identity(&executable).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn executable_identity_detects_replacement_preserving_size_and_mtime() {
        use std::os::unix::fs::MetadataExt;

        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("memex");
        std::fs::write(&executable, b"version 1.0 build A").unwrap();
        let original = std::fs::metadata(&executable).unwrap();
        let first = executable_identity(&executable).unwrap();

        let mut replacement = tempfile::NamedTempFile::new_in(temp.path()).unwrap();
        replacement.write_all(b"version 1.0 build B").unwrap();
        replacement
            .as_file()
            .set_times(std::fs::FileTimes::new().set_modified(original.modified().unwrap()))
            .unwrap();
        replacement.persist(&executable).unwrap();

        let replaced = std::fs::metadata(&executable).unwrap();
        assert_eq!(original.len(), replaced.len());
        assert_eq!(original.modified().unwrap(), replaced.modified().unwrap());
        assert_ne!(original.ino(), replaced.ino());
        assert_ne!(first, executable_identity(&executable).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn executable_identity_detects_in_place_write_with_restored_mtime() {
        use std::os::unix::fs::MetadataExt;

        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("memex");
        std::fs::write(&executable, b"version 1.0 build A").unwrap();
        let original = std::fs::metadata(&executable).unwrap();
        let first = executable_identity(&executable).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        std::fs::write(&executable, b"version 1.0 build B").unwrap();
        File::options()
            .write(true)
            .open(&executable)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(original.modified().unwrap()))
            .unwrap();

        let updated = std::fs::metadata(&executable).unwrap();
        assert_eq!(original.ino(), updated.ino());
        assert_eq!(original.len(), updated.len());
        assert_eq!(original.modified().unwrap(), updated.modified().unwrap());
        assert_ne!(
            (original.ctime(), original.ctime_nsec()),
            (updated.ctime(), updated.ctime_nsec())
        );
        assert_ne!(first, executable_identity(&executable).unwrap());
    }
}
