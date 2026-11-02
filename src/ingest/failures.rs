//! Per-file ingest failure isolation.
//!
//! A transcript that fails to parse is skipped instead of cancelling the whole refresh: its
//! previously indexed records and checkpoint stay as they were, and the failure is recorded
//! here. Discovery defers the file until its backoff expires or its observed identity changes,
//! so one corrupt file neither blocks other sources nor re-fails on every refresh.
//!
//! The daemon keeps its own record of consecutive refresh failures so `memex stats` and
//! `memex daemon status` can show a stuck ingest.

use super::FileTask;
use crate::types::SourceKind;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const FILE_FAILURES: &str = "ingest-failures.json";
const DAEMON_STATUS: &str = "daemon-ingest.json";
/// First retry delay for a failing file; it doubles per failed attempt up to the cap.
const FILE_RETRY_BASE: Duration = Duration::from_secs(30);
const FILE_RETRY_MAX: Duration = Duration::from_secs(60 * 60);
/// Daemon refreshes that fail as a whole back off from 2 s to 60 s.
pub(crate) const DAEMON_RETRY_BASE: Duration = Duration::from_secs(2);
pub(crate) const DAEMON_RETRY_MAX: Duration = Duration::from_secs(60);

/// `base * 2^(attempts - 1)`, capped at `max`.
pub(crate) fn backoff(attempts: u32, base: Duration, max: Duration) -> Duration {
    let doublings = attempts.saturating_sub(1).min(31);
    base.saturating_mul(1u32 << doublings).min(max)
}

pub(crate) fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// The stat fields a watcher can compare without reading the file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatStamp {
    pub size: u64,
    pub modified_ns: Option<i64>,
    pub device: Option<u64>,
    pub inode: Option<u64>,
}

impl StatStamp {
    fn of_metadata(metadata: &std::fs::Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Self {
            size: metadata.len(),
            modified_ns: super::discovery::modified_ns(metadata),
            #[cfg(unix)]
            device: Some(metadata.dev()),
            #[cfg(not(unix))]
            device: None,
            #[cfg(unix)]
            inode: Some(metadata.ino()),
            #[cfg(not(unix))]
            inode: None,
        }
    }
}

/// A file a refresh skipped because it could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileFailure {
    pub source: SourceKind,
    pub path: String,
    pub error: String,
    /// Unix seconds of the first and latest failed attempt.
    pub first_seen: u64,
    pub last_seen: u64,
    pub attempts: u32,
    /// Unix seconds after which discovery parses the file again even if it is unchanged.
    pub retry_at: u64,
    /// Hash of the observed size, mtime, and identity the failure was recorded against; any
    /// change retries the file immediately.
    fingerprint: String,
    /// Present for files on disk, so the daemon can veto a dirty hint without a refresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stat: Option<StatStamp>,
}

/// A parse failure observed by one refresh attempt.
#[derive(Debug, Clone)]
pub(super) struct ParseFailure {
    source: SourceKind,
    path: String,
    error: String,
    fingerprint: String,
    stat: Option<StatStamp>,
}

impl ParseFailure {
    pub(super) fn new(task: &FileTask, error: &anyhow::Error) -> Self {
        Self {
            source: task.source,
            path: task.path.to_string_lossy().into_owned(),
            error: format!("{error:#}"),
            fingerprint: fingerprint(task),
            stat: task_stat(task),
        }
    }
}

/// Returned by an execution attempt whose parse phase isolated failing files. Nothing was
/// published; the caller records the failures and retries without them.
#[derive(Debug)]
pub(super) struct ParseFailures(pub Vec<ParseFailure>);

impl std::fmt::Display for ParseFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} transcript(s) failed to parse", self.0.len())?;
        for failure in &self.0 {
            write!(formatter, "; {}: {}", failure.path, failure.error)?;
        }
        Ok(())
    }
}

impl std::error::Error for ParseFailures {}

fn fingerprint(task: &FileTask) -> String {
    let observed = serde_json::to_vec(&(task.size, task.mtime, &task.identity))
        .expect("file identity serializes");
    format!("{:x}", Sha256::digest(observed))
}

fn task_stat(task: &FileTask) -> Option<StatStamp> {
    // Virtual database sessions have no inode; their size is a message count.
    task.identity.inode.map(|inode| StatStamp {
        size: task.size,
        modified_ns: task.identity.modified_ns,
        device: task.identity.device,
        inode: Some(inode),
    })
}

/// Files currently skipped for parse failures, persisted under the state directory. Written
/// only under the ingest lease.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct FailureLog {
    #[serde(default)]
    files: BTreeMap<String, FileFailure>,
}

impl FailureLog {
    /// A missing log is empty. An unreadable one is treated as empty too: the worst case is
    /// that failing files are retried once more.
    pub fn load(state_dir: &Path) -> Result<Self> {
        let path = state_dir.join(FILE_FAILURES);
        match std::fs::read(&path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes).unwrap_or_else(|error| {
                eprintln!(
                    "warning: ignoring unreadable ingest failure log {}: {error}",
                    path.display()
                );
                Self::default()
            })),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
        }
    }

    pub(super) fn save(&self, state_dir: &Path) -> Result<()> {
        let path = state_dir.join(FILE_FAILURES);
        if self.files.is_empty() {
            return match std::fs::remove_file(&path) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    Err(error).with_context(|| format!("remove {}", path.display()))
                }
                _ => Ok(()),
            };
        }
        crate::state::atomic_write(&path, &serde_json::to_vec_pretty(self)?)
            .with_context(|| format!("write {}", path.display()))
    }

    pub fn files(&self) -> impl Iterator<Item = &FileFailure> {
        self.files.values()
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Whether discovery should leave `task` alone this refresh: it failed before, has not
    /// changed since, and its backoff has not expired.
    pub(super) fn defers(&self, task: &FileTask, now: u64) -> bool {
        self.files
            .get(task.path.to_string_lossy().as_ref())
            .is_some_and(|failure| {
                now < failure.retry_at && failure.fingerprint == fingerprint(task)
            })
    }

    /// The watcher's cheap form of [`Self::defers`] for a file on disk.
    pub(crate) fn defers_metadata(
        &self,
        path: &str,
        metadata: &std::fs::Metadata,
        now: u64,
    ) -> bool {
        self.files.get(path).is_some_and(|failure| {
            now < failure.retry_at
                && failure.stat.as_ref() == Some(&StatStamp::of_metadata(metadata))
        })
    }

    pub(super) fn record(&mut self, failure: ParseFailure, now: u64) -> FileFailure {
        let entry = self
            .files
            .entry(failure.path.clone())
            .or_insert_with(|| FileFailure {
                source: failure.source,
                path: failure.path.clone(),
                error: String::new(),
                first_seen: now,
                last_seen: now,
                attempts: 0,
                retry_at: now,
                fingerprint: String::new(),
                stat: None,
            });
        entry.source = failure.source;
        entry.error = failure.error;
        entry.last_seen = now;
        entry.attempts = entry.attempts.saturating_add(1);
        entry.retry_at = now + backoff(entry.attempts, FILE_RETRY_BASE, FILE_RETRY_MAX).as_secs();
        entry.fingerprint = failure.fingerprint;
        entry.stat = failure.stat;
        entry.clone()
    }

    /// Forget files a successful refresh parsed or deleted. A full scan sees every file, so it
    /// also forgets entries it neither parsed nor deferred: those files were removed,
    /// excluded, or reverted to their indexed state.
    pub(super) fn settle<'a>(
        &mut self,
        settled: impl IntoIterator<Item = &'a str>,
        full_scan_deferred: Option<&[String]>,
    ) {
        for path in settled {
            self.files.remove(path);
        }
        if let Some(deferred) = full_scan_deferred {
            self.files.retain(|path, _| deferred.contains(path));
        }
    }
}

/// Consecutive whole-refresh failures of the daemon. Written only by the daemon loop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonIngestStatus {
    pub consecutive_failures: u32,
    /// Unix seconds of the first and latest failure in the current streak.
    pub first_failed_at: u64,
    pub last_failed_at: u64,
    pub last_error: String,
    /// Unix seconds when the daemon tries again.
    pub next_retry_at: u64,
}

impl DaemonIngestStatus {
    pub fn load(state_dir: &Path) -> Result<Option<Self>> {
        let path = state_dir.join(DAEMON_STATUS);
        match std::fs::read(&path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
        }
    }

    /// Extend the failure streak with `error`; the daemon retries after `delay`.
    pub(crate) fn record_failure(
        state_dir: &Path,
        failures: u32,
        error: &anyhow::Error,
        delay: Duration,
    ) -> Result<()> {
        let now = now_secs();
        let first_failed_at = Self::load(state_dir)?
            .filter(|previous| failures > 1 && previous.consecutive_failures < failures)
            .map_or(now, |previous| previous.first_failed_at);
        let status = Self {
            consecutive_failures: failures,
            first_failed_at,
            last_failed_at: now,
            last_error: format!("{error:#}"),
            next_retry_at: now + delay.as_secs(),
        };
        crate::state::atomic_write(
            &state_dir.join(DAEMON_STATUS),
            &serde_json::to_vec_pretty(&status)?,
        )
    }

    pub(crate) fn clear(state_dir: &Path) -> Result<()> {
        match std::fs::remove_file(state_dir.join(DAEMON_STATUS)) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error.into()),
            _ => Ok(()),
        }
    }
}

/// Human-readable lines describing skipped files and a failing daemon, empty when healthy.
pub fn status_lines(state_dir: &Path) -> Result<Vec<String>> {
    let now = now_secs();
    let mut lines = Vec::new();
    if let Some(daemon) = DaemonIngestStatus::load(state_dir)? {
        lines.push(format!(
            "ingest: failing ({} consecutive attempts over {}; next retry {}): {}",
            daemon.consecutive_failures,
            relative(daemon.last_failed_at.saturating_sub(daemon.first_failed_at)),
            until(daemon.next_retry_at, now),
            daemon.last_error
        ));
    }
    let log = FailureLog::load(state_dir)?;
    if !log.is_empty() {
        lines.push(format!(
            "ingest: {} file(s) skipped after parse failures; indexed records are kept",
            log.files.len()
        ));
        for failure in log.files() {
            lines.push(format!(
                "  {} {} ({} attempt(s) since {} ago; retry {}): {}",
                failure.source.label(),
                failure.path,
                failure.attempts,
                relative(now.saturating_sub(failure.first_seen)),
                until(failure.retry_at, now),
                failure.error
            ));
        }
    }
    Ok(lines)
}

fn until(at: u64, now: u64) -> String {
    if at <= now {
        "due".to_string()
    } else {
        format!("in {}", relative(at - now))
    }
}

fn relative(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3600 => format!("{}m", seconds / 60),
        3600..86400 => format!("{}h", seconds / 3600),
        _ => format!("{}d", seconds / 86400),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_to_its_cap() {
        let base = Duration::from_secs(2);
        let max = Duration::from_secs(60);
        let delays = (1..=7)
            .map(|attempt| backoff(attempt, base, max).as_secs())
            .collect::<Vec<_>>();
        assert_eq!(delays, [2, 4, 8, 16, 32, 60, 60]);
        assert_eq!(backoff(u32::MAX, base, max), max);
    }

    #[test]
    fn daemon_status_keeps_the_start_of_a_streak() {
        let temp = tempfile::tempdir().unwrap();
        let error = anyhow::anyhow!("boom");
        DaemonIngestStatus::record_failure(temp.path(), 1, &error, Duration::from_secs(2)).unwrap();
        let first = DaemonIngestStatus::load(temp.path()).unwrap().unwrap();
        DaemonIngestStatus::record_failure(temp.path(), 2, &error, Duration::from_secs(4)).unwrap();
        let second = DaemonIngestStatus::load(temp.path()).unwrap().unwrap();
        assert_eq!(second.consecutive_failures, 2);
        assert_eq!(second.first_failed_at, first.first_failed_at);
        assert!(status_lines(temp.path()).unwrap()[0].contains("2 consecutive attempts"));
        DaemonIngestStatus::clear(temp.path()).unwrap();
        assert!(DaemonIngestStatus::load(temp.path()).unwrap().is_none());
        assert!(status_lines(temp.path()).unwrap().is_empty());
    }
}
