use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::ffi::OsString;
use std::fs::{self, File};
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const VERSION: u32 = 1;
const MAX_DIRECTORIES: usize = 4_096;
const MAX_CHILDREN: usize = 65_536;
const MAX_PATH_BYTES: usize = 4_096;
const MAX_NAME_BYTES: usize = 1_024;
const MAX_PROJECTION_BYTES: usize = 65_536;
const MAX_TOTAL_BYTES: usize = 16 * 1024 * 1024;
const RECENCY_SECONDS: i64 = 1;

type Bytes<const N: usize> = BoundedVec<u8, N>;
type Key = (Vec<u8>, Vec<u8>);

#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
struct BoundedVec<T, const N: usize>(Vec<T>);

impl<'de, T: Deserialize<'de>, const N: usize> Deserialize<'de> for BoundedVec<T, N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor<T, const N: usize>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>, const N: usize> serde::de::Visitor<'de> for Visitor<T, N> {
            type Value = BoundedVec<T, N>;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "at most {N} inventory elements")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                if seq.size_hint().is_some_and(|size| size > N) {
                    return Err(serde::de::Error::custom("oversized directory inventory"));
                }
                let mut values = Vec::new();
                while let Some(value) = seq.next_element()? {
                    if values.len() == N {
                        return Err(serde::de::Error::custom("oversized directory inventory"));
                    }
                    values.push(value);
                }
                Ok(BoundedVec(values))
            }
        }
        deserializer.deserialize_seq(Visitor::<T, N>(std::marker::PhantomData))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
struct Timestamp {
    seconds: i64,
    nanos: u32,
}

impl Timestamp {
    fn now() -> Option<Self> {
        let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
        Some(Self {
            seconds: elapsed.as_secs().try_into().ok()?,
            nanos: elapsed.subsec_nanos(),
        })
    }

    fn valid(self) -> bool {
        self.seconds >= 0 && self.nanos < 1_000_000_000
    }

    fn settled(self, now: Self) -> bool {
        self.valid()
            && self.nanos != 0
            && now.valid()
            && now
                .seconds
                .checked_sub(self.seconds)
                .is_some_and(|age| age > RECENCY_SECONDS)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Epoch {
    boot: [u8; 32],
    mounts: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Stamp {
    device: u64,
    inode: u64,
    birth: Timestamp,
    modified: Timestamp,
    changed: Timestamp,
    generation: u32,
    volume: [u8; 16],
    fsid: [i32; 2],
}

impl Stamp {
    fn eligible(&self, now: Timestamp) -> bool {
        self.generation != 0
            && self.birth.valid()
            && self.birth <= now
            && self.modified.settled(now)
            && self.changed.settled(now)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum EntryType {
    File,
    Directory,
    Symlink,
    Other,
}

impl EntryType {
    fn of(kind: fs::FileType) -> Self {
        if kind.is_file() {
            Self::File
        } else if kind.is_dir() {
            Self::Directory
        } else if kind.is_symlink() {
            Self::Symlink
        } else {
            Self::Other
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Child {
    name: Bytes<MAX_NAME_BYTES>,
    kind: EntryType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Directory {
    root: Bytes<MAX_PATH_BYTES>,
    relative: Bytes<MAX_PATH_BYTES>,
    stamp: Stamp,
    start: usize,
    len: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DirectoryInventory {
    version: u32,
    projection: Bytes<MAX_PROJECTION_BYTES>,
    epoch: Epoch,
    observed: Timestamp,
    directories: BoundedVec<Directory, MAX_DIRECTORIES>,
    children: BoundedVec<Child, MAX_CHILDREN>,
}

impl DirectoryInventory {
    fn valid(&self, projection: &[u8], epoch: &Epoch, now: Timestamp) -> bool {
        if self.version != VERSION
            || self.projection.0 != projection
            || &self.epoch != epoch
            || !self.observed.valid()
            || self.observed > now
            || self.directories.0.len() > MAX_DIRECTORIES
            || self.children.0.len() > MAX_CHILDREN
        {
            return false;
        }
        let mut keys = HashSet::new();
        let mut next = 0usize;
        let mut bytes = self.projection.0.len();
        for directory in &self.directories.0 {
            let Some(root) = decode_path(&directory.root.0) else {
                return false;
            };
            let Some(relative) = decode_path(&directory.relative.0) else {
                return false;
            };
            if !root.is_absolute()
                || !relative
                    .components()
                    .all(|c| matches!(c, Component::Normal(_)))
                || !keys.insert((&directory.root.0, &directory.relative.0))
                || directory.start != next
                || !directory.stamp.eligible(now)
            {
                return false;
            }
            let Some(end) = next.checked_add(directory.len) else {
                return false;
            };
            let Some(children) = self.children.0.get(next..end) else {
                return false;
            };
            let mut names = HashSet::new();
            for child in children {
                if !valid_name(&child.name.0) || !names.insert(&child.name.0) {
                    return false;
                }
                bytes = bytes.saturating_add(child.name.0.len());
            }
            bytes = bytes
                .saturating_add(directory.root.0.len())
                .saturating_add(directory.relative.0.len());
            next = end;
        }
        next == self.children.0.len() && bytes <= MAX_TOTAL_BYTES
    }
}

#[cfg(any(test, feature = "profiling"))]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct InventoryCounters {
    pub directories_checked: usize,
    pub directories_reused: usize,
    pub directories_enumerated: usize,
    pub fallback_walks: usize,
    pub metadata_checks: usize,
}

pub(crate) struct DiscoveredEntry {
    pub path: PathBuf,
    pub depth: usize,
    pub file_type: EntryType,
}

trait Observer {
    fn epoch(&mut self) -> Option<Epoch>;
    fn stamp(&mut self, directory: &File) -> Option<Stamp>;
    fn now(&self) -> Option<Timestamp> {
        Timestamp::now()
    }
}

#[derive(Default)]
struct PlatformObserver {
    #[cfg(target_os = "macos")]
    volumes: BTreeMap<u64, ([u8; 16], [i32; 2])>,
}

impl Observer for PlatformObserver {
    fn epoch(&mut self) -> Option<Epoch> {
        platform::epoch()
    }
    fn stamp(&mut self, directory: &File) -> Option<Stamp> {
        #[cfg(target_os = "macos")]
        {
            platform::stamp(directory, &mut self.volumes)
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = directory;
            None
        }
    }
}

macro_rules! count {
    ($inventory:ident, $field:ident) => {
        #[cfg(any(test, feature = "profiling"))]
        {
            $inventory.counters.$field += 1;
        }
    };
}

pub(crate) struct DiscoveryInventory {
    previous: BTreeMap<Key, (Stamp, Vec<Child>)>,
    next: BTreeMap<Key, (Stamp, Vec<Child>)>,
    projection: Vec<u8>,
    epoch: Option<Epoch>,
    started: Option<Timestamp>,
    observer: Box<dyn Observer>,
    #[cfg(any(test, feature = "profiling"))]
    counters: InventoryCounters,
    collecting: bool,
    stored_children: usize,
    stored_bytes: usize,
    walk_failed: bool,
}

impl DiscoveryInventory {
    pub(crate) fn new(previous: Option<DirectoryInventory>, projection: &[u8]) -> Self {
        Self::with_observer(previous, projection, Box::<PlatformObserver>::default())
    }

    fn with_observer(
        previous: Option<DirectoryInventory>,
        projection: &[u8],
        mut observer: Box<dyn Observer>,
    ) -> Self {
        let epoch = observer.epoch();
        let started = observer.now();
        let mut listings = BTreeMap::new();
        if let (Some(previous), Some(epoch), Some(now)) = (previous, epoch.as_ref(), started)
            && previous.valid(projection, epoch, now)
        {
            for directory in previous.directories.0 {
                let children =
                    previous.children.0[directory.start..directory.start + directory.len].to_vec();
                listings.insert(
                    (directory.root.0, directory.relative.0),
                    (directory.stamp, children),
                );
            }
        }
        Self {
            previous: listings,
            next: BTreeMap::new(),
            projection: projection.to_vec(),
            epoch,
            started,
            observer,
            #[cfg(any(test, feature = "profiling"))]
            counters: InventoryCounters::default(),
            collecting: projection.len() <= MAX_PROJECTION_BYTES,
            stored_children: 0,
            stored_bytes: projection.len(),
            walk_failed: false,
        }
    }

    #[cfg(any(test, feature = "profiling"))]
    pub(crate) fn counters(&self) -> InventoryCounters {
        self.counters
    }

    pub(crate) fn walk(&mut self, root: &Path) -> Vec<io::Result<DiscoveredEntry>> {
        if !root.is_absolute() {
            return self.full_walk(root);
        }
        let root = root.to_path_buf();
        let initial = self.observer.epoch();
        let current_time = self.observer.now();
        if initial.is_none()
            || initial != self.epoch
            || current_time < self.started
            || current_time.is_none()
            || !self.collecting
        {
            self.invalidate();
            return self.full_walk(&root);
        }
        count!(self, metadata_checks);
        let metadata = match fs::symlink_metadata(&root) {
            Ok(metadata) if !metadata.file_type().is_symlink() => metadata,
            _ => return self.full_walk(&root),
        };
        self.walk_failed = false;
        let mut pending = vec![Ok(DiscoveredEntry {
            path: root.clone(),
            depth: 0,
            file_type: EntryType::of(metadata.file_type()),
        })];
        let mut result = Vec::new();
        while let Some(entry) = pending.pop() {
            match entry {
                Ok(entry) => {
                    if entry.file_type == EntryType::Directory {
                        let children = self.children(&root, &entry.path, entry.depth + 1);
                        pending.extend(children.into_iter().rev());
                    }
                    result.push(Ok(entry));
                }
                Err(error) => result.push(Err(error)),
            }
        }
        if self.walk_failed
            || self.observer.epoch() != initial
            || self.observer.now() < current_time
        {
            self.invalidate();
            return self.full_walk(&root);
        }
        result
    }

    fn invalidate(&mut self) {
        self.previous.clear();
        self.next.clear();
        self.collecting = false;
    }

    fn full_walk(&mut self, root: &Path) -> Vec<io::Result<DiscoveredEntry>> {
        count!(self, fallback_walks);
        walkdir::WalkDir::new(root)
            .into_iter()
            .map(|entry| {
                let entry = entry.map_err(|error| {
                    let kind = error
                        .io_error()
                        .map_or(io::ErrorKind::Other, io::Error::kind);
                    io::Error::new(kind, error)
                })?;
                let file_type = EntryType::of(entry.file_type());
                if file_type == EntryType::Directory {
                    count!(self, directories_enumerated);
                }
                Ok(DiscoveredEntry {
                    path: entry.path().to_path_buf(),
                    depth: entry.depth(),
                    file_type,
                })
            })
            .collect()
    }

    fn children(
        &mut self,
        root: &Path,
        path: &Path,
        depth: usize,
    ) -> Vec<io::Result<DiscoveredEntry>> {
        count!(self, directories_checked);
        let key = path
            .strip_prefix(root)
            .ok()
            .and_then(|relative| Some((encode_path(root)?, encode_path(relative)?)));
        let anchor = platform::open_directory(path).ok();
        let before = anchor.as_ref().and_then(|file| self.observer.stamp(file));
        let eligible = before
            .as_ref()
            .zip(self.observer.now())
            .is_some_and(|(stamp, now)| stamp.eligible(now));
        // Sources can share a root, so a listing this same request already verified counts
        // as well as one loaded from the cache.
        let cached = key
            .as_ref()
            .and_then(|key| self.next.get(key).or_else(|| self.previous.get(key)))
            .filter(|(stamp, _)| eligible && before.as_ref() == Some(stamp))
            .map(|(_, children)| children.clone());
        if let Some(cached) = cached
            && self.stable(path, anchor.as_ref(), &before)
        {
            let entries = self.inspect(
                path,
                depth,
                cached
                    .iter()
                    .map(|child| (decode_name(&child.name.0).unwrap(), child.kind))
                    .collect(),
            );
            count!(self, directories_reused);
            self.store(key, before, cached);
            return entries;
        }
        count!(self, directories_enumerated);
        let names = match platform::names(anchor.as_ref(), path) {
            Ok(names) => names,
            Err(error) => {
                self.walk_failed = true;
                return vec![Err(error)];
            }
        };
        let entries = self.inspect(path, depth, names);
        if eligible {
            if !self.stable(path, anchor.as_ref(), &before) {
                self.walk_failed = true;
                return entries;
            }
            let children = entries
                .iter()
                .map(|entry| {
                    let entry = entry.as_ref().ok()?;
                    let name = encode_name(entry.path.file_name()?)?;
                    Some(Child {
                        name: BoundedVec(name),
                        kind: entry.file_type,
                    })
                })
                .collect::<Option<Vec<_>>>();
            if let Some(children) = children {
                self.store(key, before, children);
            }
        }
        entries
    }

    fn inspect(
        &self,
        path: &Path,
        depth: usize,
        names: Vec<(OsString, EntryType)>,
    ) -> Vec<io::Result<DiscoveredEntry>> {
        names
            .into_iter()
            .map(|(name, file_type)| {
                Ok(DiscoveredEntry {
                    path: path.join(name),
                    depth,
                    file_type,
                })
            })
            .collect()
    }

    fn stable(&mut self, path: &Path, anchor: Option<&File>, before: &Option<Stamp>) -> bool {
        let Some(anchor) = anchor else { return false };
        if before.is_none() || self.observer.stamp(anchor) != *before {
            return false;
        }
        let Ok(current) = platform::open_directory(path) else {
            return false;
        };
        self.observer.stamp(&current) == *before
    }

    fn store(&mut self, key: Option<Key>, stamp: Option<Stamp>, children: Vec<Child>) {
        let (Some(key), Some(stamp)) = (key, stamp) else {
            return;
        };
        if !self.collecting {
            return;
        }
        if let Some((_, old)) = self.next.remove(&key) {
            self.stored_children -= old.len();
            self.stored_bytes -= key.0.len()
                + key.1.len()
                + old.iter().map(|child| child.name.0.len()).sum::<usize>();
        }
        let bytes = key.0.len()
            + key.1.len()
            + children
                .iter()
                .map(|child| child.name.0.len())
                .sum::<usize>();
        if self.next.len() == MAX_DIRECTORIES
            || self.stored_children.saturating_add(children.len()) > MAX_CHILDREN
            || self.stored_bytes.saturating_add(bytes) > MAX_TOTAL_BYTES
        {
            self.invalidate();
            return;
        }
        self.stored_children += children.len();
        self.stored_bytes += bytes;
        self.next.insert(key, (stamp, children));
    }

    /// Persist `None` as an explicit null, not an omitted field.
    pub(crate) fn finish(mut self) -> Option<DirectoryInventory> {
        if !self.collecting || self.next.is_empty() || self.observer.epoch() != self.epoch {
            return None;
        }
        let observed = self.observer.now()?;
        if Some(observed) < self.started {
            return None;
        }
        let mut directories = Vec::new();
        let mut children = Vec::new();
        for ((root, relative), (stamp, listing)) in self.next {
            directories.push(Directory {
                root: BoundedVec(root),
                relative: BoundedVec(relative),
                stamp,
                start: children.len(),
                len: listing.len(),
            });
            children.extend(listing);
        }
        Some(DirectoryInventory {
            version: VERSION,
            projection: BoundedVec(self.projection),
            epoch: self.epoch?,
            observed,
            directories: BoundedVec(directories),
            children: BoundedVec(children),
        })
    }
}

fn valid_name(name: &[u8]) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_BYTES
        && name != b"."
        && name != b".."
        && !name.contains(&0)
        && !name.contains(&b'/')
}

fn encode_name(name: &std::ffi::OsStr) -> Option<Vec<u8>> {
    let bytes = encode_path(Path::new(name))?;
    valid_name(&bytes).then_some(bytes)
}

fn decode_name(name: &[u8]) -> Option<OsString> {
    valid_name(name)
        .then(|| decode_path(name).map(PathBuf::into_os_string))
        .flatten()
}

#[cfg(unix)]
fn encode_path(path: &Path) -> Option<Vec<u8>> {
    use std::os::unix::ffi::OsStrExt;
    let bytes = path.as_os_str().as_bytes();
    (bytes.len() <= MAX_PATH_BYTES && !bytes.contains(&0)).then(|| bytes.to_vec())
}

#[cfg(unix)]
fn decode_path(bytes: &[u8]) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    (bytes.len() <= MAX_PATH_BYTES && !bytes.contains(&0))
        .then(|| PathBuf::from(OsString::from_vec(bytes.to_vec())))
}

#[cfg(not(unix))]
fn encode_path(_: &Path) -> Option<Vec<u8>> {
    None
}
#[cfg(not(unix))]
fn decode_path(_: &[u8]) -> Option<PathBuf> {
    None
}

#[cfg(not(target_os = "macos"))]
mod platform {
    use super::*;
    pub(super) fn epoch() -> Option<Epoch> {
        None
    }
    pub(super) fn open_directory(path: &Path) -> io::Result<File> {
        File::open(path)
    }
    pub(super) fn names(_: Option<&File>, path: &Path) -> io::Result<Vec<(OsString, EntryType)>> {
        fs::read_dir(path)?
            .map(|entry| {
                let entry = entry?;
                Ok((entry.file_name(), EntryType::of(entry.file_type()?)))
            })
            .collect()
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::*;
    use std::ffi::CStr;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    fn sysctl(name: &CStr, output: &mut [u8]) -> bool {
        let mut len = output.len();
        unsafe {
            libc::sysctlbyname(
                name.as_ptr(),
                output.as_mut_ptr().cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            ) == 0
                && len == output.len()
        }
    }

    pub(super) fn epoch() -> Option<Epoch> {
        let mut boot = [0; 37];
        let mut mounts = [0; 4];
        if !sysctl(c"kern.bootsessionuuid", &mut boot) || !sysctl(c"vfs.nummntops", &mut mounts) {
            return None;
        }
        if boot[36] != 0 {
            return None;
        }
        let boot = boot[..36]
            .iter()
            .copied()
            .filter(|byte| *byte != b'-')
            .collect::<Vec<_>>();
        if !boot.iter().all(u8::is_ascii_hexdigit) {
            return None;
        }
        Some(Epoch {
            boot: boot.try_into().ok()?,
            mounts: u32::from_ne_bytes(mounts),
        })
    }

    pub(super) fn open_directory(path: &Path) -> io::Result<File> {
        fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(path)
    }

    fn timestamp(seconds: i64, nanos: i64) -> Option<Timestamp> {
        let stamp = Timestamp {
            seconds,
            nanos: nanos.try_into().ok()?,
        };
        stamp.valid().then_some(stamp)
    }

    fn attributes(file: &File, common: u32, volume: u32, buffer: &mut [u8]) -> bool {
        let mut attrs = libc::attrlist {
            bitmapcount: libc::ATTR_BIT_MAP_COUNT,
            reserved: 0,
            commonattr: common | libc::ATTR_CMN_RETURNED_ATTRS,
            volattr: volume,
            dirattr: 0,
            fileattr: 0,
            forkattr: 0,
        };
        unsafe {
            libc::fgetattrlist(
                file.as_raw_fd(),
                (&mut attrs as *mut libc::attrlist).cast(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                libc::FSOPT_ATTR_CMN_EXTENDED,
            ) == 0
        }
    }

    fn word(buffer: &[u8], offset: usize) -> Option<u32> {
        Some(u32::from_ne_bytes(
            buffer.get(offset..offset + 4)?.try_into().ok()?,
        ))
    }

    fn volume(file: &File) -> Option<([u8; 16], [i32; 2])> {
        let mut filesystem = std::mem::MaybeUninit::<libc::statfs>::uninit();
        if unsafe { libc::fstatfs(file.as_raw_fd(), filesystem.as_mut_ptr()) } != 0 {
            return None;
        }
        let filesystem = unsafe { filesystem.assume_init() };
        if filesystem.f_flags & libc::MNT_LOCAL as u32 == 0
            || unsafe { CStr::from_ptr(filesystem.f_fstypename.as_ptr()) }.to_bytes() != b"apfs"
        {
            return None;
        }
        let mount = unsafe { CStr::from_ptr(filesystem.f_mntonname.as_ptr()) };
        let mount = PathBuf::from(OsString::from_vec(mount.to_bytes().to_vec()));
        let volume_root = open_directory(&mount).ok()?;
        let mut volume = [0; 40];
        if !attributes(
            &volume_root,
            0,
            libc::ATTR_VOL_INFO | libc::ATTR_VOL_UUID,
            &mut volume,
        ) || word(&volume, 0)? != 40
            || word(&volume, 8)? & libc::ATTR_VOL_UUID == 0
        {
            return None;
        }
        let fsid = unsafe { std::mem::transmute::<libc::fsid_t, [i32; 2]>(filesystem.f_fsid) };
        Some((volume[24..40].try_into().ok()?, fsid))
    }

    pub(super) fn stamp(
        file: &File,
        volumes: &mut BTreeMap<u64, ([u8; 16], [i32; 2])>,
    ) -> Option<Stamp> {
        let metadata = file.metadata().ok()?;
        if !metadata.is_dir() {
            return None;
        }
        let (volume, fsid) = if let Some(volume) = volumes.get(&metadata.dev()) {
            *volume
        } else {
            let identity = volume(file)?;
            volumes.insert(metadata.dev(), identity);
            identity
        };
        let birth = metadata.created().ok()?.duration_since(UNIX_EPOCH).ok()?;
        let mut generation = [0; 28];
        if !attributes(file, libc::ATTR_CMN_GEN_COUNT, 0, &mut generation)
            || word(&generation, 0)? != 28
            || word(&generation, 4)? & libc::ATTR_CMN_GEN_COUNT == 0
        {
            return None;
        }
        Some(Stamp {
            device: metadata.dev(),
            inode: metadata.ino(),
            birth: Timestamp {
                seconds: birth.as_secs().try_into().ok()?,
                nanos: birth.subsec_nanos(),
            },
            modified: timestamp(metadata.mtime(), metadata.mtime_nsec())?,
            changed: timestamp(metadata.ctime(), metadata.ctime_nsec())?,
            generation: word(&generation, 24)?,
            volume,
            fsid,
        })
    }

    pub(super) fn names(
        anchor: Option<&File>,
        path: &Path,
    ) -> io::Result<Vec<(OsString, EntryType)>> {
        let Some(anchor) = anchor else {
            // The no-follow open failed, which is what happens when the entry became a
            // symlink after it was seen as a directory. Reading it here would follow that
            // link and enumerate outside the root, so fail and let the caller fall back to
            // the non-following walker.
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("no directory handle for {}", path.display()),
            ));
        };
        // fdopendir owns the duplicate; the validation handle remains open.
        let descriptor = unsafe { libc::dup(anchor.as_raw_fd()) };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        let directory = unsafe { libc::fdopendir(descriptor) };
        if directory.is_null() {
            let error = io::Error::last_os_error();
            unsafe {
                libc::close(descriptor);
            }
            return Err(error);
        }
        struct DirectoryStream(*mut libc::DIR);
        impl Drop for DirectoryStream {
            fn drop(&mut self) {
                unsafe {
                    libc::closedir(self.0);
                }
            }
        }
        let directory = DirectoryStream(directory);
        let mut names = Vec::new();
        loop {
            unsafe {
                *libc::__error() = 0;
            }
            let entry = unsafe { libc::readdir(directory.0) };
            if entry.is_null() {
                let error = io::Error::last_os_error();
                return if error.raw_os_error() == Some(0) {
                    Ok(names)
                } else {
                    Err(error)
                };
            }
            let entry = unsafe { &*entry };
            let bytes = unsafe { CStr::from_ptr(entry.d_name.as_ptr()) }.to_bytes();
            if bytes == b"." || bytes == b".." {
                continue;
            }
            let name = OsString::from_vec(bytes.to_vec());
            let kind = match entry.d_type {
                libc::DT_REG => EntryType::File,
                libc::DT_DIR => EntryType::Directory,
                libc::DT_LNK => EntryType::Symlink,
                libc::DT_UNKNOWN => {
                    EntryType::of(fs::symlink_metadata(path.join(&name))?.file_type())
                }
                _ => EntryType::Other,
            };
            names.push((name, kind));
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::os::unix::fs::{MetadataExt, symlink};
    use std::rc::Rc;

    struct FakeState {
        epoch: Option<Epoch>,
        now: Timestamp,
        generations: BTreeMap<u64, u32>,
        unstable: bool,
        calls: u32,
    }

    impl Default for FakeState {
        fn default() -> Self {
            Self {
                epoch: Some(Epoch {
                    boot: [b'a'; 32],
                    mounts: 1,
                }),
                now: Timestamp {
                    seconds: 1_000,
                    nanos: 1,
                },
                generations: BTreeMap::new(),
                unstable: false,
                calls: 0,
            }
        }
    }

    struct FakeObserver(Rc<RefCell<FakeState>>);

    impl Observer for FakeObserver {
        fn epoch(&mut self) -> Option<Epoch> {
            self.0.borrow().epoch.clone()
        }
        fn now(&self) -> Option<Timestamp> {
            Some(self.0.borrow().now)
        }
        fn stamp(&mut self, directory: &File) -> Option<Stamp> {
            let metadata = directory.metadata().ok()?;
            let mut state = self.0.borrow_mut();
            state.calls += 1;
            let generation = if state.unstable {
                state.calls
            } else {
                *state.generations.get(&metadata.ino()).unwrap_or(&1)
            };
            Some(Stamp {
                device: metadata.dev(),
                inode: metadata.ino(),
                birth: Timestamp {
                    seconds: 90,
                    nanos: 1,
                },
                modified: Timestamp {
                    seconds: 100,
                    nanos: 1,
                },
                changed: Timestamp {
                    seconds: 100,
                    nanos: 1,
                },
                generation,
                volume: [1; 16],
                fsid: [1, 2],
            })
        }
    }

    fn request(
        previous: Option<DirectoryInventory>,
        state: &Rc<RefCell<FakeState>>,
    ) -> DiscoveryInventory {
        DiscoveryInventory::with_observer(
            previous,
            b"roots-and-options",
            Box::new(FakeObserver(state.clone())),
        )
    }

    fn paths(entries: Vec<io::Result<DiscoveredEntry>>) -> Vec<PathBuf> {
        let mut paths = entries
            .into_iter()
            .map(|entry| entry.unwrap().path)
            .collect::<Vec<_>>();
        paths.sort();
        paths
    }

    fn oracle(root: &Path) -> Vec<PathBuf> {
        let mut paths = walkdir::WalkDir::new(root)
            .into_iter()
            .map(|entry| entry.unwrap().into_path())
            .collect::<Vec<_>>();
        paths.sort();
        paths
    }

    fn populate(root: &Path, state: &Rc<RefCell<FakeState>>) -> DirectoryInventory {
        let mut inventory = request(None, state);
        assert_eq!(paths(inventory.walk(root)), oracle(root));
        inventory.finish().unwrap()
    }

    fn bump(path: &Path, state: &Rc<RefCell<FakeState>>) {
        let inode = fs::metadata(path).unwrap().ino();
        *state.borrow_mut().generations.entry(inode).or_insert(1) += 1;
    }

    #[test]
    fn unchanged_inventory_reuses_every_directory_without_candidate_stats() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("empty")).unwrap();
        fs::create_dir(root.path().join("nested")).unwrap();
        fs::write(root.path().join("nested/file.jsonl"), b"{}").unwrap();
        fs::write(root.path().join("unindexed.txt"), b"").unwrap();
        let state = Rc::new(RefCell::new(FakeState::default()));
        let cache = populate(root.path(), &state);
        let encoded = serde_json::to_vec(&cache).unwrap();
        let cache = serde_json::from_slice(&encoded).unwrap();
        let mut inventory = request(Some(cache), &state);
        assert_eq!(paths(inventory.walk(root.path())), oracle(root.path()));
        assert_eq!(inventory.counters().directories_reused, 3);
        assert_eq!(inventory.counters().directories_enumerated, 0);
        assert_eq!(inventory.counters().metadata_checks, 1);
    }

    #[test]
    fn unchanged_ancestor_does_not_hide_new_nested_descendants() {
        let root = tempfile::tempdir().unwrap();
        let nested = root.path().join("nested");
        fs::create_dir(&nested).unwrap();
        let state = Rc::new(RefCell::new(FakeState::default()));
        let cache = populate(root.path(), &state);
        fs::create_dir(nested.join("new")).unwrap();
        fs::write(nested.join("new/session.jsonl"), b"{}").unwrap();
        bump(&nested, &state);
        let mut inventory = request(Some(cache), &state);
        assert_eq!(paths(inventory.walk(root.path())), oracle(root.path()));
        assert_eq!(inventory.counters().directories_reused, 1);
        assert_eq!(inventory.counters().directories_enumerated, 2);
    }

    #[test]
    fn same_count_rename_and_type_replacement_reenumerate_membership() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("old"), b"").unwrap();
        let state = Rc::new(RefCell::new(FakeState::default()));
        let cache = populate(root.path(), &state);
        fs::rename(root.path().join("old"), root.path().join("new")).unwrap();
        bump(root.path(), &state);
        let mut inventory = request(Some(cache), &state);
        assert_eq!(paths(inventory.walk(root.path())), oracle(root.path()));
        assert_eq!(inventory.counters().directories_reused, 0);
        let cache = inventory.finish().unwrap();
        fs::rename(root.path().join("new"), root.path().join("moved")).unwrap();
        fs::create_dir(root.path().join("new")).unwrap();
        fs::write(root.path().join("new/nested"), b"").unwrap();
        bump(root.path(), &state);
        let mut inventory = request(Some(cache), &state);
        assert_eq!(paths(inventory.walk(root.path())), oracle(root.path()));
    }

    #[test]
    fn mutation_during_observation_uses_original_walk_and_invalidates() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("visible.jsonl"), b"{}").unwrap();
        let state = Rc::new(RefCell::new(FakeState::default()));
        let cache = populate(root.path(), &state);
        state.borrow_mut().unstable = true;
        let mut inventory = request(Some(cache), &state);
        assert_eq!(paths(inventory.walk(root.path())), oracle(root.path()));
        assert_eq!(inventory.counters().directories_reused, 0);
        assert_eq!(inventory.counters().fallback_walks, 1);
        assert!(inventory.finish().is_none());
    }

    #[test]
    fn reboot_remount_and_projection_changes_reject_prior_inventory() {
        let root = tempfile::tempdir().unwrap();
        let state = Rc::new(RefCell::new(FakeState::default()));
        let cache = populate(root.path(), &state);
        state.borrow_mut().epoch.as_mut().unwrap().mounts += 1;
        let mut inventory = request(Some(cache.clone()), &state);
        assert_eq!(paths(inventory.walk(root.path())), oracle(root.path()));
        assert_eq!(inventory.counters().directories_reused, 0);
        state.borrow_mut().epoch = Some(cache.epoch.clone());
        state.borrow_mut().epoch.as_mut().unwrap().boot[0] = b'b';
        let mut inventory = request(Some(cache.clone()), &state);
        inventory.walk(root.path());
        assert_eq!(inventory.counters().directories_reused, 0);
        state.borrow_mut().epoch = Some(cache.epoch.clone());
        let mut inventory = DiscoveryInventory::with_observer(
            Some(cache),
            b"different-roots",
            Box::new(FakeObserver(state)),
        );
        inventory.walk(root.path());
        assert_eq!(inventory.counters().directories_reused, 0);
    }

    #[test]
    fn unsupported_epoch_and_clock_regression_preserve_full_discovery() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("file"), b"").unwrap();
        let state = Rc::new(RefCell::new(FakeState::default()));
        let cache = populate(root.path(), &state);
        let mut inventory = request(Some(cache.clone()), &state);
        state.borrow_mut().now.seconds -= 1;
        assert_eq!(paths(inventory.walk(root.path())), oracle(root.path()));
        assert!(inventory.finish().is_none());
        state.borrow_mut().epoch = None;
        let mut inventory = request(Some(cache), &state);
        assert_eq!(paths(inventory.walk(root.path())), oracle(root.path()));
        assert_eq!(inventory.counters().directories_reused, 0);
        assert!(inventory.finish().is_none());
    }

    #[test]
    fn uncertain_timestamps_and_zero_generation_are_ineligible() {
        let root = tempfile::tempdir().unwrap();
        let state = Rc::new(RefCell::new(FakeState::default()));
        let mut observer = FakeObserver(state.clone());
        let stamp = observer.stamp(&File::open(root.path()).unwrap()).unwrap();
        let now = state.borrow().now;
        assert!(stamp.eligible(now));
        let mut zero = stamp.clone();
        zero.generation = 0;
        assert!(!zero.eligible(now));
        let mut coarse = stamp.clone();
        coarse.changed.nanos = 0;
        assert!(!coarse.eligible(now));
        let mut recent = stamp.clone();
        recent.changed = now;
        assert!(!recent.eligible(now));
        let mut future = stamp.clone();
        future.modified.seconds = now.seconds + 1;
        assert!(!future.eligible(now));
        let mut invalid = stamp;
        invalid.changed.nanos = 1_000_000_000;
        assert!(!invalid.eligible(now));
    }

    #[test]
    fn cache_paths_cannot_escape_and_invalid_inventory_is_explicit_null() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("safe"), b"").unwrap();
        let state = Rc::new(RefCell::new(FakeState::default()));
        let cache = populate(root.path(), &state);
        for name in [
            b"../escape".as_slice(),
            b"/absolute",
            b".",
            b"..",
            b"",
            b"nul\0name",
        ] {
            let mut malformed = cache.clone();
            malformed.children.0[0].name = BoundedVec(name.to_vec());
            let mut inventory = request(Some(malformed), &state);
            assert_eq!(paths(inventory.walk(root.path())), oracle(root.path()));
            assert_eq!(inventory.counters().directories_reused, 0);
        }
        let mut duplicate = cache.clone();
        duplicate
            .directories
            .0
            .push(duplicate.directories.0[0].clone());
        assert!(!duplicate.valid(b"roots-and-options", &cache.epoch, state.borrow().now));
        let mut unknown = cache.clone();
        unknown.version += 1;
        assert!(!unknown.valid(b"roots-and-options", &cache.epoch, state.borrow().now));
        assert_eq!(
            serde_json::to_value(Option::<DirectoryInventory>::None).unwrap(),
            serde_json::Value::Null
        );
        assert!(serde_json::from_str::<BoundedVec<u8, 2>>("[1,2,3]").is_err());
    }

    #[test]
    fn raw_names_round_trip_without_utf8_conversion() {
        let name = b"session-\xff.jsonl";
        assert!(valid_name(name));
        let native = decode_name(name).unwrap();
        assert_eq!(encode_name(&native).unwrap(), name);
        let encoded = serde_json::to_vec(&BoundedVec::<u8, MAX_NAME_BYTES>(name.to_vec())).unwrap();
        let decoded: Bytes<MAX_NAME_BYTES> = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.0, name);
    }

    #[test]
    fn root_symlink_is_followed_but_child_symlinks_are_not() {
        let owned = tempfile::tempdir().unwrap();
        let target = owned.path().join("target");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("file.jsonl"), b"{}").unwrap();
        symlink(&target, target.join("loop")).unwrap();
        let root = owned.path().join("root-link");
        symlink(&target, &root).unwrap();
        let state = Rc::new(RefCell::new(FakeState::default()));
        let mut inventory = request(None, &state);
        let entries = inventory.walk(&root);
        assert_eq!(entries[0].as_ref().unwrap().file_type, EntryType::Symlink);
        assert_eq!(paths(entries), oracle(&root));
        assert_eq!(inventory.counters().fallback_walks, 1);
        let mut inventory = request(None, &state);
        assert_eq!(paths(inventory.walk(&target)), oracle(&target));
    }

    #[test]
    fn missing_root_can_appear_on_the_next_request() {
        let owned = tempfile::tempdir().unwrap();
        let root = owned.path().join("missing");
        let state = Rc::new(RefCell::new(FakeState::default()));
        let mut inventory = request(None, &state);
        assert_eq!(
            inventory.walk(&root)[0].as_ref().err().unwrap().kind(),
            io::ErrorKind::NotFound
        );
        assert!(inventory.finish().is_none());
        fs::create_dir(&root).unwrap();
        fs::write(root.join("new.jsonl"), b"{}").unwrap();
        let mut inventory = request(None, &state);
        assert_eq!(paths(inventory.walk(&root)), oracle(&root));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn native_apfs_generation_tracks_directory_membership_not_descendants() {
        let root = tempfile::tempdir().unwrap();
        let anchor = platform::open_directory(root.path()).unwrap();
        let mut observer = PlatformObserver::default();
        let Some(initial) = observer.stamp(&anchor) else {
            let mut inventory = DiscoveryInventory::new(None, b"unsupported-volume");
            assert_eq!(paths(inventory.walk(root.path())), oracle(root.path()));
            assert_eq!(inventory.counters().directories_reused, 0);
            return;
        };
        assert_ne!(initial.generation, 0);
        fs::write(root.path().join("old"), b"").unwrap();
        let created = observer.stamp(&anchor).unwrap();
        assert_ne!(created.generation, initial.generation);
        fs::rename(root.path().join("old"), root.path().join("new")).unwrap();
        let renamed = observer.stamp(&anchor).unwrap();
        assert_ne!(renamed.generation, created.generation);
        fs::create_dir(root.path().join("nested")).unwrap();
        let parent = observer.stamp(&anchor).unwrap();
        let child = platform::open_directory(&root.path().join("nested")).unwrap();
        let before_child = observer.stamp(&child).unwrap();
        fs::write(root.path().join("nested/child"), b"").unwrap();
        assert_eq!(observer.stamp(&anchor).unwrap(), parent);
        assert_ne!(
            observer.stamp(&child).unwrap().generation,
            before_child.generation
        );
    }
}
