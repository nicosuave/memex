//! The dream store on disk: immutable generations under `<root>/dreams/generations/` and a
//! `CURRENT` pointer switched atomically, so a run never rewrites what an earlier one
//! published and a reader never sees a half-written set of dreams.

use super::model::{DREAM_SCHEMA_VERSION, Generation};
use crate::config::Paths;
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::{Path, PathBuf};

const CURRENT_FILE: &str = "CURRENT";
const GENERATIONS_DIR: &str = "generations";
const DREAMS_FILE: &str = "dreams.json";
pub const MARKDOWN_FILE: &str = "DREAMS.md";
pub const DIFF_FILE: &str = "diff.md";
const RESPONSE_FILE: &str = "response.txt";
const STATE_FILE: &str = "state.json";
const LOCK_FILE: &str = "lock";

/// Bookkeeping that is not part of any generation: the scheduler reads it to back off
/// after a run that published nothing.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DreamState {
    #[serde(default)]
    pub last_attempt_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DreamStore {
    dir: PathBuf,
}

/// Held for a whole run or lint, so two writers never derive from the same parent.
#[derive(Debug)]
pub struct DreamLock {
    file: File,
}

impl Drop for DreamLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Files published with a generation besides its dreams.
pub struct GenerationFiles<'a> {
    pub markdown: &'a str,
    pub diff: &'a str,
    /// Raw executor output, kept so a rejected operation can be traced to what was said.
    pub response: Option<&'a str>,
}

impl DreamStore {
    pub fn new(paths: &Paths) -> Self {
        Self {
            dir: paths.root.join("dreams"),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Working directory handed to the executor: empty, and never a project checkout.
    pub fn work_dir(&self) -> PathBuf {
        self.dir.join("work")
    }

    pub fn generation_dir(&self, id: &str) -> Result<PathBuf> {
        validate_generation_id(id)?;
        Ok(self.dir.join(GENERATIONS_DIR).join(id))
    }

    pub fn try_lock(&self) -> Result<Option<DreamLock>> {
        fs::create_dir_all(&self.dir)?;
        let path = self.dir.join(LOCK_FILE);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("open dream lock {}", path.display()))?;
        match file.try_lock() {
            Ok(()) => Ok(Some(DreamLock { file })),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Error(error)) => {
                Err(error).with_context(|| format!("acquire dream lock {}", path.display()))
            }
        }
    }

    pub fn lock(&self) -> Result<DreamLock> {
        self.try_lock()?
            .ok_or_else(|| anyhow!("another dream run or lint is in progress on this store"))
    }

    /// Whether a run holds the lock now. Read-only: a store that was never written is not
    /// created just to be asked.
    pub fn is_locked(&self) -> bool {
        self.dir.join(LOCK_FILE).exists() && matches!(self.try_lock(), Ok(None))
    }

    pub fn current_id(&self) -> Result<Option<String>> {
        match fs::read_to_string(self.dir.join(CURRENT_FILE)) {
            Ok(contents) => {
                let id = contents.trim();
                if id.is_empty() {
                    return Ok(None);
                }
                validate_generation_id(id)?;
                Ok(Some(id.to_string()))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).context("read dream CURRENT pointer"),
        }
    }

    pub fn current(&self) -> Result<Option<Generation>> {
        self.current_id()?.map(|id| self.load(&id)).transpose()
    }

    pub fn load(&self, id: &str) -> Result<Generation> {
        let path = self.generation_dir(id)?.join(DREAMS_FILE);
        let bytes =
            fs::read(&path).with_context(|| format!("read dream generation {}", path.display()))?;
        let generation: Generation = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse dream generation {}", path.display()))?;
        if generation.schema_version != DREAM_SCHEMA_VERSION {
            bail!(
                "unsupported dream schema version {} in {} (expected {DREAM_SCHEMA_VERSION})",
                generation.schema_version,
                path.display()
            );
        }
        Ok(generation)
    }

    pub fn read_generation_file(&self, id: &str, name: &str) -> Result<Option<String>> {
        match fs::read_to_string(self.generation_dir(id)?.join(name)) {
            Ok(contents) => Ok(Some(contents)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Published generation IDs, oldest first. IDs sort by creation time.
    pub fn generation_ids(&self) -> Result<Vec<String>> {
        let entries = match fs::read_dir(self.dir.join(GENERATIONS_DIR)) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut ids = Vec::new();
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if validate_generation_id(&name).is_ok() && entry.path().join(DREAMS_FILE).is_file() {
                ids.push(name);
            }
        }
        ids.sort();
        Ok(ids)
    }

    /// Write `generation` beside the existing ones and switch `CURRENT` to it. The parent
    /// generation is left exactly as published.
    pub fn publish(
        &self,
        _lock: &DreamLock,
        generation: &Generation,
        files: &GenerationFiles<'_>,
    ) -> Result<()> {
        let generations = self.dir.join(GENERATIONS_DIR);
        fs::create_dir_all(&generations)?;
        let target = self.generation_dir(&generation.id)?;
        if target.exists() {
            bail!("dream generation {} already exists", generation.id);
        }
        let staging = tempfile::Builder::new()
            .prefix(".staging-")
            .tempdir_in(&generations)?;
        write_synced(
            &staging.path().join(DREAMS_FILE),
            &serde_json::to_vec_pretty(generation)?,
        )?;
        write_synced(
            &staging.path().join(MARKDOWN_FILE),
            files.markdown.as_bytes(),
        )?;
        write_synced(&staging.path().join(DIFF_FILE), files.diff.as_bytes())?;
        if let Some(response) = files.response {
            write_synced(&staging.path().join(RESPONSE_FILE), response.as_bytes())?;
        }
        fs::rename(staging.keep(), &target)
            .with_context(|| format!("publish dream generation {}", target.display()))?;
        sync_directory(&generations)?;
        atomic_write(&self.dir.join(CURRENT_FILE), generation.id.as_bytes())
    }

    pub fn state(&self) -> DreamState {
        fs::read(self.dir.join(STATE_FILE))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    pub fn write_state(&self, state: &DreamState) -> Result<()> {
        fs::create_dir_all(&self.dir)?;
        atomic_write(
            &self.dir.join(STATE_FILE),
            &serde_json::to_vec_pretty(state)?,
        )
    }
}

/// Sortable by creation time; the suffix keeps two generations in one second apart.
pub fn new_generation_id(now_ms: u64) -> String {
    let stamp = chrono::DateTime::from_timestamp_millis(now_ms as i64)
        .unwrap_or_default()
        .format("%Y%m%dT%H%M%SZ");
    let mut suffix = [0u8; 2];
    if getrandom::fill(&mut suffix).is_err() {
        suffix = (std::process::id() as u16).to_le_bytes();
    }
    format!("{stamp}-{:02x}{:02x}", suffix[0], suffix[1])
}

fn validate_generation_id(id: &str) -> Result<()> {
    let valid = !id.is_empty()
        && id.len() <= 64
        && !id.starts_with('.')
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if valid {
        Ok(())
    } else {
        Err(anyhow!("invalid dream generation id {id:?}"))
    }
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("dream path has no parent: {}", path.display()))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.write_all(b"\n")?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("publish {}", path.display()))?;
    sync_directory(parent)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::model::{GenerationKind, RunMeta};
    use super::*;

    fn generation(id: &str, parent: Option<&str>) -> Generation {
        Generation {
            schema_version: DREAM_SCHEMA_VERSION,
            id: id.to_string(),
            parent: parent.map(str::to_string),
            kind: GenerationKind::Run,
            created_at_ms: 1,
            watermark_ms: 1,
            run: RunMeta::default(),
            dreams: Vec::new(),
        }
    }

    fn files() -> GenerationFiles<'static> {
        GenerationFiles {
            markdown: "# Dreams\n",
            diff: "# Diff\n",
            response: Some("{}"),
        }
    }

    #[test]
    fn publishing_keeps_the_parent_generation_and_switches_current() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = DreamStore::new(&Paths::new(Some(root.path().to_path_buf()))?);
        assert!(store.current()?.is_none());
        let lock = store.lock()?;
        store.publish(&lock, &generation("20260101T000000Z-0001", None), &files())?;
        let first = fs::read(
            store
                .generation_dir("20260101T000000Z-0001")?
                .join(DREAMS_FILE),
        )?;
        store.publish(
            &lock,
            &generation("20260102T000000Z-0002", Some("20260101T000000Z-0001")),
            &files(),
        )?;
        assert_eq!(
            store.current_id()?.as_deref(),
            Some("20260102T000000Z-0002")
        );
        assert_eq!(
            store.generation_ids()?,
            ["20260101T000000Z-0001", "20260102T000000Z-0002"]
        );
        assert_eq!(
            fs::read(
                store
                    .generation_dir("20260101T000000Z-0001")?
                    .join(DREAMS_FILE)
            )?,
            first
        );
        assert!(
            store
                .publish(&lock, &generation("20260102T000000Z-0002", None), &files())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn a_second_writer_cannot_take_the_lock() -> Result<()> {
        let root = tempfile::tempdir()?;
        let store = DreamStore::new(&Paths::new(Some(root.path().to_path_buf()))?);
        let lock = store.lock()?;
        assert!(store.try_lock()?.is_none());
        assert!(store.is_locked());
        drop(lock);
        assert!(store.try_lock()?.is_some());
        Ok(())
    }

    #[test]
    fn generation_ids_cannot_leave_the_store() {
        for id in ["../x", "a/b", ".staging-1", ""] {
            assert!(validate_generation_id(id).is_err(), "{id:?}");
        }
        assert!(validate_generation_id(&new_generation_id(1_800_000_000_000)).is_ok());
    }
}
