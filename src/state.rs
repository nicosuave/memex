use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileIdentity {
    /// SQLite commits can change only the WAL while the main file stays unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sqlite_wal: Option<SqliteWalIdentity>,
    /// Stable filesystem identity when the platform exposes one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inode: Option<u64>,
    /// Hash of a bounded prefix, used to detect in-place replacement without rescanning a file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_sha256: Option<String>,
    /// Number of leading bytes covered by `prefix_sha256`. Keeping this stable across appends
    /// prevents a short file's fingerprint from changing merely because its prefix grew.
    #[serde(default)]
    pub prefix_bytes: u64,
    /// Nanosecond-resolution modification marker for detecting same-size rewrites.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_ns: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub changed_ns: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SqliteWalIdentity {
    pub exists: bool,
    pub size: u64,
    pub modified_ns: Option<i64>,
}

impl SqliteWalIdentity {
    pub fn read(database: &Path) -> Self {
        let mut wal = database.as_os_str().to_os_string();
        wal.push("-wal");
        let Ok(metadata) = fs::metadata(Path::new(&wal)) else {
            return Self::default();
        };
        // Opening a checkpointed WAL-mode database can create an empty WAL.
        // Its creation/removal contains no commits and must not cause a reparse loop.
        if metadata.len() == 0 {
            return Self::default();
        }
        Self {
            exists: true,
            size: metadata.len(),
            modified_ns: metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingToolCall {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_use_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_use_doc_id: Option<u64>,
    #[serde(default)]
    pub timestamp: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub argument_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub argument_bytes: Option<u64>,
    /// Source-native parent event for formats whose result event does not repeat it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_tool_use_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_tool_assistant_uuid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileState {
    pub size: u64,
    pub mtime: i64,
    pub offset: u64,
    pub turn_id: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_turn_id: Option<u32>,
    #[serde(default)]
    pub parser_version: u32,
    #[serde(default)]
    pub pending_tool_calls: HashMap<String, PendingToolCall>,
    #[serde(default)]
    pub identity: FileIdentity,
    /// Claude's file-level `sessionKind: "bg"` classification. `None` is
    /// retained for states written before this was tracked, so they can be
    /// migrated safely on their next ingest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_background: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codex_metadata_offsets: Option<Vec<u64>>,
}

/// Tracks when we last scanned for changes, allowing us to skip
/// redundant scans if called again within a short TTL.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScanCache {
    /// Unix timestamp (seconds) of last successful scan
    pub last_scan_ts: u64,
    /// Number of files found in last scan
    pub file_count: usize,
    /// Total bytes across all source files
    pub total_bytes: u64,
}

impl ScanCache {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        crate::profiling::span!("state.scan_cache.load");
        if !path.exists() {
            return Ok(Self::default());
        }
        let data = fs::read_to_string(path)?;
        let cache = serde_json::from_str(&data).unwrap_or_default();
        Ok(cache)
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        crate::profiling::span!("state.scan_cache.save");
        let data = serde_json::to_string(self)?;
        atomic_write(path, data.as_bytes())
    }

    /// Check if the cache is still valid (within TTL seconds)
    pub fn is_fresh(&self, ttl_seconds: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        now.saturating_sub(self.last_scan_ts) < ttl_seconds
    }

    /// Update cache with current scan results
    pub fn update(&mut self, file_count: usize, total_bytes: u64) {
        self.last_scan_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.file_count = file_count;
        self.total_bytes = total_bytes;
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpencodeDatabaseState {
    pub parser_version: u32,
    pub event_rowid: i64,
    pub event_id: Option<String>,
    pub owned_session_ids: HashSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestState {
    pub next_doc_id: u64,
    pub files: HashMap<String, FileState>,
    #[serde(default)]
    pub opencode_databases: HashMap<String, OpencodeDatabaseState>,
}

/// A precise source/session target for replacement and deletion work.
#[derive(Debug, Clone, Hash, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionScope {
    pub source_path: String,
    pub session_id: String,
}

/// Durable intent for an ingest batch that may have crossed one publication boundary.
///
/// Tantivy and SQLite cannot commit atomically. While this marker exists, the listed source
/// paths must be removed from both stores and reparsed before their ingest state is trusted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingIngest {
    pub next_doc_id: u64,
    pub source_paths: Vec<String>,
    /// Source paths whose records must also be removed from the vector store.
    /// This is deliberately narrower than `source_paths`: parser replacement
    /// only republishes lexical and analytics state.
    #[serde(default)]
    pub vector_delete_paths: Vec<String>,
    #[serde(default)]
    pub session_scopes: Vec<SessionScope>,
    #[serde(default)]
    pub vector_publication: bool,
    /// Whether the interrupted vector publication required embeddings. Older
    /// markers omitted this field, and therefore retain the historical
    /// `vector_publication` behavior during recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_publication: Option<bool>,
}

impl Default for IngestState {
    fn default() -> Self {
        Self {
            next_doc_id: 1,
            files: HashMap::new(),
            opencode_databases: HashMap::new(),
        }
    }
}

impl IngestState {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        crate::profiling::span!("state.ingest.load");
        if !path.exists() {
            return Ok(Self::default());
        }
        let data = fs::read_to_string(path)?;
        let state = serde_json::from_str(&data)?;
        Ok(state)
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        crate::profiling::span!("state.ingest.save");
        let data = serde_json::to_string_pretty(self)?;
        atomic_write(path, data.as_bytes())
    }
}

impl PendingIngest {
    pub fn load(path: &Path) -> anyhow::Result<Option<Self>> {
        crate::profiling::span!("state.pending.load");
        if !path.exists() {
            return Ok(None);
        }
        let data = fs::read_to_string(path)?;
        Ok(Some(serde_json::from_str(&data)?))
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        crate::profiling::span!("state.pending.save");
        let data = serde_json::to_string_pretty(self)?;
        atomic_write(path, data.as_bytes())
    }

    pub fn clear(path: &Path) -> anyhow::Result<()> {
        let parent = parent_directory(path)?;
        match fs::remove_file(path) {
            Ok(()) => sync_directory(parent),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn atomic_write(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    let parent = parent_directory(path)?;
    fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(data)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    sync_directory(parent)
}

fn parent_directory(path: &Path) -> anyhow::Result<&Path> {
    match path.parent() {
        Some(parent) if parent.as_os_str().is_empty() => Ok(Path::new(".")),
        Some(parent) => Ok(parent),
        None => Err(anyhow::anyhow!(
            "state path has no parent: {}",
            path.display()
        )),
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> anyhow::Result<()> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_saves_replace_existing_files_atomically() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("ingest.json");
        fs::write(&path, "old state").expect("seed state");

        let state = IngestState {
            next_doc_id: 42,
            files: HashMap::new(),
            opencode_databases: HashMap::new(),
        };
        state.save(&path).expect("save state");

        assert_eq!(
            IngestState::load(&path).expect("load state").next_doc_id,
            42
        );
        assert!(
            fs::read_dir(temp.path())
                .expect("read tempdir")
                .all(|entry| entry.expect("directory entry").path() == path)
        );
    }

    #[test]
    fn scan_cache_saves_replace_existing_files_atomically() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("scan_cache.json");
        fs::write(&path, "old cache").expect("seed cache");

        let cache = ScanCache {
            last_scan_ts: 12,
            file_count: 3,
            total_bytes: 99,
        };
        cache.save(&path).expect("save cache");

        assert_eq!(ScanCache::load(&path).expect("load cache").file_count, 3);
    }

    #[test]
    fn malformed_scan_cache_loads_as_default() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("scan_cache.json");
        fs::write(&path, "{\"last_scan_ts\":").expect("seed malformed cache");

        let cache = ScanCache::load(&path).expect("load malformed cache");

        assert_eq!(cache.last_scan_ts, 0);
        assert_eq!(cache.file_count, 0);
        assert_eq!(cache.total_bytes, 0);
    }

    #[test]
    fn pending_ingest_round_trips_and_clears() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("ingest.pending.json");
        let pending = PendingIngest {
            next_doc_id: 17,
            source_paths: vec!["session.jsonl".to_string()],
            vector_delete_paths: vec!["session.jsonl".to_string()],
            session_scopes: Vec::new(),
            vector_publication: true,
            embedding_publication: Some(true),
        };

        pending.save(&path).expect("save pending ingest");
        assert_eq!(
            PendingIngest::load(&path).expect("load pending ingest"),
            Some(pending)
        );

        PendingIngest::clear(&path).expect("clear pending ingest");
        PendingIngest::clear(&path).expect("clear missing pending ingest");
        assert_eq!(
            PendingIngest::load(&path).expect("load cleared ingest"),
            None
        );
    }

    #[test]
    fn pending_ingest_without_vector_flag_is_lexical_only() {
        let pending: PendingIngest =
            serde_json::from_str(r#"{"next_doc_id":17,"source_paths":["session.jsonl"]}"#)
                .expect("load legacy pending ingest");

        assert!(!pending.vector_publication);
    }

    #[test]
    fn ingest_state_without_opencode_databases_remains_compatible() {
        let state: IngestState =
            serde_json::from_str(r#"{"next_doc_id":9,"files":{}}"#).expect("legacy state");
        assert_eq!(state.next_doc_id, 9);
        assert!(state.opencode_databases.is_empty());

        let database = OpencodeDatabaseState {
            parser_version: 1,
            event_rowid: 12,
            event_id: Some("event".to_string()),
            owned_session_ids: HashSet::from(["session".to_string()]),
        };
        let round_trip = serde_json::to_string(&database).expect("serialize database state");
        assert_eq!(
            serde_json::from_str::<OpencodeDatabaseState>(&round_trip).unwrap(),
            database
        );
    }
}
