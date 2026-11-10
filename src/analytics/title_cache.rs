//! Claude session titles cached by transcript identity.
//!
//! Claude writes a session's saved and generated names as metadata lines anywhere in its
//! transcript, and later renames append more, so a title is only known after reading the
//! whole file. Session listings resolve titles on every refresh. This cache keeps what a
//! transcript's title lines folded to, together with the file identity they were read at:
//! an unchanged transcript is not read again, and one that only grew is read from where
//! the previous read stopped. Anything else is read whole, exactly as without the cache.
//!
//! The cache is derived only from the transcripts, so it may be lost or deleted at any
//! time; every failure to use it falls back to reading the transcript.

use crate::sources::claude::SessionTitles;
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

const CACHE_FILE: &str = "session-titles.sqlite3";
/// Bump when the rules of [`SessionTitles`] change; titles cached under older rules are dropped.
const RULES_VERSION: i64 = 1;
/// Bytes hashed from each end of the scanned part of a transcript. A grown transcript is
/// read from the previous end only while both ends still match, so a rewrite is read whole.
const FINGERPRINT_WINDOW: usize = 4096;

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

struct CachedTitles {
    stamp: FileStamp,
    /// End of the last newline-terminated line read; a grown file resumes here.
    resume_offset: u64,
    /// [`fingerprint`] of the bytes before `resume_offset`.
    fingerprint: Vec<u8>,
    /// Title lines folded up to `resume_offset`.
    folded: SessionTitles,
    /// The title of the whole file at `stamp`, including an unterminated final line.
    title: Option<String>,
}

impl CachedTitles {
    /// Whether `bytes` are this version of the file with only lines appended.
    fn extends_to(&self, stamp: &FileStamp, bytes: &[u8]) -> bool {
        let resume_offset = self.resume_offset as usize;
        stamp.device == self.stamp.device
            && stamp.inode == self.stamp.inode
            && bytes.len() as u64 > self.stamp.size
            && stamp.modified_ns >= self.stamp.modified_ns
            && resume_offset <= bytes.len()
            && fingerprint(&bytes[..resume_offset]) == self.fingerprint
    }
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
                     resume_offset INTEGER NOT NULL,
                     fingerprint BLOB NOT NULL,
                     custom_title TEXT,
                     ai_title TEXT,
                     agent_name TEXT,
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
    /// reading only what changed since the cached version. `None` when the transcript
    /// cannot be found or read and no unchanged version is cached.
    pub(crate) fn claude_title(&self, path: &Path, session_id: &str) -> Option<String> {
        let source_path = path.to_string_lossy();
        let stamp = FileStamp::of(&std::fs::metadata(path).ok()?);
        let cached = self.load(&source_path, session_id);
        if let Some(cached) = &cached
            && cached.stamp == stamp
        {
            return cached.title.clone();
        }
        let file = File::open(path).ok()?;
        // SAFETY: as in `session_title`, the map is only read; transcripts are appended to.
        let bytes = unsafe { memmap2::Mmap::map(&file).ok()? };
        let (mut folded, start) = match cached {
            Some(cached) if cached.extends_to(&stamp, &bytes) => {
                (cached.folded, cached.resume_offset as usize)
            }
            _ => (SessionTitles::default(), 0),
        };
        let resume_offset = start + folded.fold_lines(&bytes[start..], session_id);
        let mut whole = folded.clone();
        whole.fold_line(&bytes[resume_offset..], session_id);
        let title = whole.title();
        // A file that changed between the stat and the map has no single identity to cache.
        if bytes.len() as u64 == stamp.size {
            self.store(
                &source_path,
                session_id,
                &CachedTitles {
                    stamp,
                    resume_offset: resume_offset as u64,
                    fingerprint: fingerprint(&bytes[..resume_offset]),
                    folded,
                    title: title.clone(),
                },
            );
        }
        title
    }

    fn load(&self, source_path: &str, session_id: &str) -> Option<CachedTitles> {
        let mut statement = self
            .conn
            .prepare_cached(
                "SELECT size, modified_ns, device, inode, resume_offset, fingerprint,
                        custom_title, ai_title, agent_name, title
                 FROM claude_titles WHERE source_path = ?1 AND session_id = ?2",
            )
            .ok()?;
        statement
            .query_row(params![source_path, session_id], |row| {
                Ok(CachedTitles {
                    stamp: FileStamp {
                        size: row.get::<_, i64>(0)? as u64,
                        modified_ns: row.get(1)?,
                        device: row.get::<_, Option<i64>>(2)?.map(|value| value as u64),
                        inode: row.get::<_, Option<i64>>(3)?.map(|value| value as u64),
                    },
                    resume_offset: row.get::<_, i64>(4)? as u64,
                    fingerprint: row.get(5)?,
                    folded: SessionTitles {
                        custom_title: row.get(6)?,
                        ai_title: row.get(7)?,
                        agent_name: row.get(8)?,
                    },
                    title: row.get(9)?,
                })
            })
            .optional()
            .ok()
            .flatten()
    }

    /// Best effort: a failed write leaves the next listing to read the transcript again.
    fn store(&self, source_path: &str, session_id: &str, cached: &CachedTitles) {
        let Ok(mut statement) = self.conn.prepare_cached(
            "INSERT OR REPLACE INTO claude_titles(
                 source_path, session_id, size, modified_ns, device, inode, resume_offset,
                 fingerprint, custom_title, ai_title, agent_name, title
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
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
            cached.resume_offset as i64,
            cached.fingerprint,
            cached.folded.custom_title,
            cached.folded.ai_title,
            cached.folded.agent_name,
            cached.title,
        ]);
    }
}

/// Hash of the length and both ends of `scanned`.
fn fingerprint(scanned: &[u8]) -> Vec<u8> {
    let window = FINGERPRINT_WINDOW.min(scanned.len());
    let mut hash = Sha256::new();
    hash.update((scanned.len() as u64).to_le_bytes());
    hash.update(&scanned[..window]);
    hash.update(&scanned[scanned.len() - window..]);
    hash.finalize().to_vec()
}
