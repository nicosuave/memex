use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileIdentity {
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParserStreamState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_assistant_message_ordinal: Option<u32>,
    #[serde(default)]
    pub next_tool_call_index: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileState {
    pub size: u64,
    pub mtime: i64,
    pub offset: u64,
    pub turn_id: u32,
    #[serde(default)]
    pub parser_version: u32,
    #[serde(default)]
    pub pending_tool_calls: HashMap<String, PendingToolCall>,
    #[serde(default)]
    pub parser_stream: ParserStreamState,
    #[serde(default)]
    pub identity: FileIdentity,
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
        if !path.exists() {
            return Ok(Self::default());
        }
        let data = fs::read_to_string(path)?;
        let cache = serde_json::from_str(&data).unwrap_or_default();
        Ok(cache)
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestState {
    pub next_doc_id: u64,
    pub files: HashMap<String, FileState>,
}

impl Default for IngestState {
    fn default() -> Self {
        Self {
            next_doc_id: 1,
            files: HashMap::new(),
        }
    }
}

impl IngestState {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let data = fs::read_to_string(path)?;
        let state = serde_json::from_str(&data)?;
        Ok(state)
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        let data = serde_json::to_string_pretty(self)?;
        atomic_write(path, data.as_bytes())
    }
}

fn atomic_write(path: &Path, data: &[u8]) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("state path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(data)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
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
}
