//! Directory stamps let a full discovery skip `read_dir` for directories whose entries have not
//! changed since the last successful refresh. A directory's mtime moves whenever an entry is
//! created, removed, or renamed, and never when a file inside it is appended to, so an unchanged
//! stamp means the transcript files it held last time are exactly the ones it holds now. Known
//! files are still stat-checked individually by the caller.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectoryStamp {
    pub device: u64,
    pub inode: u64,
    pub mtime_secs: i64,
    pub mtime_nanos: i64,
    /// Tools that preserve timestamps restore mtime after changing entries; ctime still moves.
    pub ctime_secs: i64,
    pub ctime_nanos: i64,
}

impl DirectoryStamp {
    #[cfg(unix)]
    fn of(metadata: &fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            mtime_secs: metadata.mtime(),
            mtime_nanos: metadata.mtime_nsec(),
            ctime_secs: metadata.ctime(),
            ctime_nanos: metadata.ctime_nsec(),
        }
    }

    #[cfg(not(unix))]
    fn of(metadata: &fs::Metadata) -> Self {
        let modified = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok());
        Self {
            device: 0,
            inode: 0,
            mtime_secs: modified.map_or(0, |d| d.as_secs() as i64),
            mtime_nanos: modified.map_or(0, |d| d.subsec_nanos() as i64),
            ctime_secs: 0,
            ctime_nanos: 0,
        }
    }
}

/// Stamp rows to persist with the refresh that observed them.
#[derive(Debug, Default)]
pub struct DirectoryStampUpdate {
    pub fingerprint: String,
    pub upserts: Vec<(PathBuf, DirectoryStamp)>,
    pub deletes: Vec<PathBuf>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct WalkCounters {
    pub reused: usize,
    pub enumerated: usize,
}

pub struct StampedWalk {
    previous: HashMap<PathBuf, DirectoryStamp>,
    child_directories: HashMap<PathBuf, Vec<PathBuf>>,
    known_files: HashMap<PathBuf, Vec<PathBuf>>,
    next: HashMap<PathBuf, DirectoryStamp>,
    roots: Vec<PathBuf>,
    counters: WalkCounters,
}

impl StampedWalk {
    /// `previous` holds the stamps persisted by the last successful refresh under the same
    /// fingerprint; `known` holds every file path the checkpoint tracks.
    pub fn new(
        previous: HashMap<PathBuf, DirectoryStamp>,
        known: impl IntoIterator<Item = PathBuf>,
    ) -> Self {
        let mut child_directories: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        for directory in previous.keys() {
            if let Some(parent) = directory.parent() {
                child_directories
                    .entry(parent.to_path_buf())
                    .or_default()
                    .push(directory.clone());
            }
        }
        let mut known_files: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        for file in known {
            if let Some(parent) = file.parent() {
                known_files
                    .entry(parent.to_path_buf())
                    .or_default()
                    .push(file);
            }
        }
        Self {
            previous,
            child_directories,
            known_files,
            next: HashMap::new(),
            roots: Vec::new(),
            counters: WalkCounters::default(),
        }
    }

    pub fn counters(&self) -> WalkCounters {
        self.counters
    }

    /// Every regular file below `root`, the set `WalkDir` would yield without following
    /// symlinks: a symlinked root is followed, symlinks below it are not.
    pub fn files(&mut self, root: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        let Ok(root_metadata) = fs::metadata(root) else {
            return files;
        };
        if root_metadata.is_file() {
            files.push(root.to_path_buf());
            return files;
        }
        if !root_metadata.is_dir() {
            return files;
        }
        self.roots.push(root.to_path_buf());
        let mut pending = vec![(root.to_path_buf(), Some(root_metadata))];
        while let Some((directory, metadata)) = pending.pop() {
            let metadata = match metadata {
                Some(metadata) => metadata,
                None => match fs::symlink_metadata(&directory) {
                    Ok(metadata) => metadata,
                    Err(_) => {
                        self.forget_ancestors(&directory);
                        continue;
                    }
                },
            };
            if !metadata.is_dir() {
                continue;
            }
            // Captured before the read so an entry added mid-enumeration invalidates it.
            let stamp = DirectoryStamp::of(&metadata);
            if self.previous.get(&directory) == Some(&stamp) {
                self.counters.reused += 1;
                if let Some(known) = self.known_files.get(&directory) {
                    files.extend(known.iter().cloned());
                }
                if let Some(children) = self.child_directories.get(&directory) {
                    pending.extend(children.iter().map(|child| (child.clone(), None)));
                }
                self.next.insert(directory, stamp);
                continue;
            }
            self.counters.enumerated += 1;
            let Ok(entries) = fs::read_dir(&directory) else {
                // The stamp of an ancestor certifies its own entries, not its descendants'.
                // Leaving this directory unstamped while its parent stays reusable would
                // hide it from every later refresh, so drop the parents' stamps too.
                self.forget_ancestors(&directory);
                continue;
            };
            let mut complete = true;
            for entry in entries {
                let Ok(entry) = entry else {
                    complete = false;
                    continue;
                };
                let Ok(kind) = entry.file_type() else {
                    complete = false;
                    continue;
                };
                if kind.is_dir() {
                    pending.push((entry.path(), None));
                } else if kind.is_file() {
                    files.push(entry.path());
                }
            }
            if complete {
                self.next.insert(directory, stamp);
            } else {
                self.forget_ancestors(&directory);
            }
        }
        files
    }

    /// Removes every stamp on the path from a root down to `directory`, so a refresh that
    /// could not enumerate it cannot be skipped through an ancestor next time.
    fn forget_ancestors(&mut self, directory: &Path) {
        let mut parent = directory.parent();
        while let Some(path) = parent {
            if self.next.remove(path).is_none() && !self.previous.contains_key(path) {
                break;
            }
            parent = path.parent();
        }
    }

    /// Rows to write with the refresh: stamps observed under the walked roots, and previous
    /// stamps under those roots that were not observed again.
    pub fn finish(self, fingerprint: String) -> DirectoryStampUpdate {
        let under_roots = |path: &Path| self.roots.iter().any(|root| path.starts_with(root));
        let mut update = DirectoryStampUpdate {
            fingerprint,
            ..Default::default()
        };
        let observed = self.next.keys().cloned().collect::<HashSet<_>>();
        for (directory, stamp) in self.next {
            if self.previous.get(&directory) != Some(&stamp) {
                update.upserts.push((directory, stamp));
            }
        }
        for directory in self.previous.into_keys() {
            if under_roots(&directory) && !observed.contains(&directory) {
                update.deletes.push(directory);
            }
        }
        update.upserts.sort_by(|a, b| a.0.cmp(&b.0));
        update.deletes.sort();
        update
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn touch(path: &Path, text: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = fs::File::create(path).unwrap();
        file.write_all(text.as_bytes()).unwrap();
    }

    fn sorted(mut files: Vec<PathBuf>) -> Vec<PathBuf> {
        files.sort();
        files
    }

    fn stamps(update: &DirectoryStampUpdate) -> HashMap<PathBuf, DirectoryStamp> {
        update.upserts.iter().cloned().collect()
    }

    /// The checkpoint keeps every stamp and applies each refresh as a delta.
    fn persist(
        mut rows: HashMap<PathBuf, DirectoryStamp>,
        update: &DirectoryStampUpdate,
    ) -> HashMap<PathBuf, DirectoryStamp> {
        for directory in &update.deletes {
            rows.remove(directory);
        }
        rows.extend(update.upserts.iter().cloned());
        rows
    }

    fn settle() {
        // Directory mtimes carry nanoseconds on APFS; a short pause keeps the test honest on
        // filesystems that truncate them.
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    #[test]
    fn unchanged_tree_is_reused_without_enumeration_and_yields_the_known_files() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        touch(&root.join("a/one.jsonl"), "1");
        touch(&root.join("a/sub/agent-two.jsonl"), "2");
        touch(&root.join("b/three.jsonl"), "3");
        touch(&root.join("b/notes.txt"), "x");
        let mut first = StampedWalk::new(HashMap::new(), []);
        let full = sorted(first.files(&root));
        assert_eq!(first.counters().enumerated, 4);
        let update = first.finish("fp".into());
        assert_eq!(update.upserts.len(), 4);
        let known = full
            .iter()
            .filter(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
            .cloned()
            .collect::<Vec<_>>();
        let mut second = StampedWalk::new(stamps(&update), known.clone());
        assert_eq!(sorted(second.files(&root)), known);
        assert_eq!(second.counters().enumerated, 0);
        assert_eq!(second.counters().reused, 4);
        let update = second.finish("fp".into());
        assert!(update.upserts.is_empty() && update.deletes.is_empty());
    }

    #[test]
    fn a_new_file_reenumerates_only_its_directory() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        touch(&root.join("a/one.jsonl"), "1");
        touch(&root.join("b/two.jsonl"), "2");
        let mut first = StampedWalk::new(HashMap::new(), []);
        let known = sorted(first.files(&root));
        let update = first.finish("fp".into());
        settle();
        touch(&root.join("b/three.jsonl"), "3");
        let mut second = StampedWalk::new(stamps(&update), known);
        let files = sorted(second.files(&root));
        assert!(files.contains(&root.join("b/three.jsonl")));
        assert!(files.contains(&root.join("a/one.jsonl")));
        assert_eq!(second.counters().enumerated, 1);
        assert_eq!(second.counters().reused, 2);
        let update = second.finish("fp".into());
        assert_eq!(update.upserts.len(), 1);
        assert_eq!(update.upserts[0].0, root.join("b"));
    }

    #[test]
    fn new_nested_directories_and_deletions_are_seen() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        touch(&root.join("a/one.jsonl"), "1");
        let mut first = StampedWalk::new(HashMap::new(), []);
        let known = sorted(first.files(&root));
        let update = first.finish("fp".into());
        settle();
        touch(&root.join("a/deep/er/four.jsonl"), "4");
        fs::remove_file(root.join("a/one.jsonl")).unwrap();
        let mut second = StampedWalk::new(stamps(&update), known);
        let files = sorted(second.files(&root));
        assert_eq!(files, vec![root.join("a/deep/er/four.jsonl")]);
        let update = second.finish("fp".into());
        assert_eq!(update.upserts.len(), 3);
    }

    #[cfg(unix)]
    #[test]
    fn an_entry_added_under_a_restored_mtime_is_still_enumerated() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        touch(&root.join("a/one.jsonl"), "1");
        let mut first = StampedWalk::new(HashMap::new(), []);
        let known = sorted(first.files(&root));
        let update = first.finish("fp".into());
        let before = fs::metadata(root.join("a")).unwrap();
        settle();

        touch(&root.join("a/two.jsonl"), "2");
        let restored = fs::FileTimes::new()
            .set_accessed(before.accessed().unwrap())
            .set_modified(before.modified().unwrap());
        fs::File::options()
            .write(true)
            .open(root.join("a"))
            .or_else(|_| fs::File::open(root.join("a")))
            .unwrap()
            .set_times(restored)
            .unwrap();

        let mut second = StampedWalk::new(stamps(&update), known);
        assert!(
            sorted(second.files(&root)).contains(&root.join("a/two.jsonl")),
            "a restored mtime must not certify changed entries"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_child_that_could_not_be_stat_ed_is_rediscovered_once_it_is_readable_again() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        touch(&root.join("a/one.jsonl"), "1");
        touch(&root.join("a/hidden/two.jsonl"), "2");
        let mut first = StampedWalk::new(HashMap::new(), []);
        let known = sorted(first.files(&root));
        let update = first.finish("fp".into());
        settle();

        // Losing search permission on `a` fails symlink_metadata for its children only.
        let opaque = fs::Permissions::from_mode(0o600);
        let readable = fs::metadata(root.join("a")).unwrap().permissions();
        fs::set_permissions(root.join("a"), opaque).unwrap();
        let mut persisted = stamps(&update);
        let mut blocked = StampedWalk::new(persisted.clone(), known.clone());
        blocked.files(&root);
        persisted = persist(persisted, &blocked.finish("fp".into()));
        fs::set_permissions(root.join("a"), readable).unwrap();

        let mut recovered = StampedWalk::new(persisted, known);
        assert!(
            sorted(recovered.files(&root)).contains(&root.join("a/hidden/two.jsonl")),
            "a subtree hidden by a stat failure must be enumerated again"
        );
    }

    #[test]
    fn removed_directories_are_deleted_from_the_update_and_missing_roots_are_empty() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        touch(&root.join("a/one.jsonl"), "1");
        let mut first = StampedWalk::new(HashMap::new(), []);
        let known = sorted(first.files(&root));
        let update = first.finish("fp".into());
        settle();
        fs::remove_dir_all(root.join("a")).unwrap();
        let mut second = StampedWalk::new(stamps(&update), known);
        assert!(second.files(&root).is_empty());
        let update = second.finish("fp".into());
        assert_eq!(update.deletes, vec![root.join("a")]);
        let mut absent = StampedWalk::new(HashMap::new(), []);
        assert!(absent.files(&temp.path().join("missing")).is_empty());
        assert!(absent.finish("fp".into()).upserts.is_empty());
    }
}
