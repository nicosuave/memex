//! Claude session titles cached by transcript identity.
//!
//! Claude writes a session's saved and generated names as metadata lines anywhere in its
//! transcript, and later renames append more, so a title is only known after reading the
//! whole file. Session listings resolve titles on every refresh. This cache keeps each
//! transcript's title together with the file identity it was read at: an unchanged
//! transcript is not read again, and a changed one is read whole, exactly as without the
//! cache. Resuming a grown transcript from its old end would be cheaper but cannot tell an
//! append from an in-place edit of the part already read without reading that part.
//!
//! The cache is derived only from the transcripts, so it may be lost or deleted at any
//! time; every failure to use it falls back to reading the transcript.

use crate::sources::claude::SessionTitles;
use rusqlite::{Connection, OptionalExtension, params};
use std::fs::File;
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

const CACHE_FILE: &str = "session-titles.sqlite3";
/// Bump when the rules of [`SessionTitles`] or the table change; older caches are dropped.
const RULES_VERSION: i64 = 2;

pub(crate) struct TitleCache {
    conn: Connection,
}

/// What identifies one version of a transcript without reading it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileStamp {
    size: u64,
    modified_ns: Option<i64>,
    device: Option<u64>,
    inode: Option<u64>,
}

impl FileStamp {
    fn of(metadata: &std::fs::Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Self {
            size: metadata.len(),
            modified_ns: metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|time| time.as_nanos().min(i64::MAX as u128) as i64),
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

struct CachedTitle {
    stamp: FileStamp,
    /// The title of the whole file at `stamp`.
    title: Option<String>,
}

impl TitleCache {
    /// Open or create the cache in `state_dir`, or `None` when it cannot be used (for
    /// example a read-only state directory).
    pub(crate) fn open(state_dir: &Path) -> Option<Self> {
        let conn = Connection::open(state_dir.join(CACHE_FILE)).ok()?;
        // Listings never wait long on another process's write; a miss only costs a read.
        conn.busy_timeout(Duration::from_millis(250)).ok()?;
        // A lost write after a crash only costs a rescan, so commits need not reach disk.
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")
            .ok()?;
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .ok()?;
        if version != RULES_VERSION {
            conn.execute_batch(&format!(
                "BEGIN IMMEDIATE;
                 DROP TABLE IF EXISTS claude_titles;
                 CREATE TABLE claude_titles (
                     source_path TEXT NOT NULL,
                     session_id TEXT NOT NULL,
                     size INTEGER NOT NULL,
                     modified_ns INTEGER,
                     device INTEGER,
                     inode INTEGER,
                     title TEXT,
                     PRIMARY KEY (source_path, session_id)
                 ) WITHOUT ROWID;
                 PRAGMA user_version = {RULES_VERSION};
                 COMMIT;"
            ))
            .ok()?;
        }
        Some(Self { conn })
    }

    /// The title [`crate::sources::claude::session_title`] returns for this transcript,
    /// read from the transcript only when it changed since the cached version. `None`
    /// when the transcript cannot be found or read.
    pub(crate) fn claude_title(&self, path: &Path, session_id: &str) -> Option<String> {
        let source_path = path.to_string_lossy();
        let stamp = FileStamp::of(&std::fs::metadata(path).ok()?);
        if let Some(cached) = self.load(&source_path, session_id)
            && cached.stamp == stamp
        {
            return cached.title;
        }
        let file = File::open(path).ok()?;
        // SAFETY: as in `session_title`, the map is only read; transcripts are appended to.
        let bytes = unsafe { memmap2::Mmap::map(&file).ok()? };
        let title = SessionTitles::of_transcript(&bytes, session_id).title();
        // A file that changed between the stat and the map has no single identity to cache.
        if bytes.len() as u64 == stamp.size {
            self.store(
                &source_path,
                session_id,
                &CachedTitle {
                    stamp,
                    title: title.clone(),
                },
            );
        }
        title
    }

    fn load(&self, source_path: &str, session_id: &str) -> Option<CachedTitle> {
        let mut statement = self
            .conn
            .prepare_cached(
                "SELECT size, modified_ns, device, inode, title
                 FROM claude_titles WHERE source_path = ?1 AND session_id = ?2",
            )
            .ok()?;
        statement
            .query_row(params![source_path, session_id], |row| {
                Ok(CachedTitle {
                    stamp: FileStamp {
                        size: row.get::<_, i64>(0)? as u64,
                        modified_ns: row.get(1)?,
                        device: row.get::<_, Option<i64>>(2)?.map(|value| value as u64),
                        inode: row.get::<_, Option<i64>>(3)?.map(|value| value as u64),
                    },
                    title: row.get(4)?,
                })
            })
            .optional()
            .ok()
            .flatten()
    }

    /// Best effort: a failed write leaves the next listing to read the transcript again.
    fn store(&self, source_path: &str, session_id: &str, cached: &CachedTitle) {
        let Ok(mut statement) = self.conn.prepare_cached(
            "INSERT OR REPLACE INTO claude_titles(
                 source_path, session_id, size, modified_ns, device, inode, title
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        ) else {
            return;
        };
        let _ = statement.execute(params![
            source_path,
            session_id,
            cached.stamp.size as i64,
            cached.stamp.modified_ns,
            cached.stamp.device.map(|value| value as i64),
            cached.stamp.inode.map(|value| value as i64),
            cached.title,
        ]);
    }
}
