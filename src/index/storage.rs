#[cfg(target_os = "macos")]
mod durability;
#[cfg(target_os = "macos")]
pub(super) use durability::StagingDurability;

use super::*;
use std::collections::BTreeMap;
use std::sync::RwLock;

pub(super) const MANIFEST: &str = ".segments.json";
pub(super) const FORMAT: &str = ".storage-format";
const FORMAT_VERSION: &str = "memex-shared-segments-v1\n";
const STORE: &str = "segments";

#[derive(Default)]
struct PublicationSync {
    #[cfg(target_os = "macos")]
    durability: Option<Arc<StagingDurability>>,
}

impl PublicationSync {
    fn file(&self, file: &File) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        if let Some(durability) = &self.durability {
            return durability.sync_file(file);
        }
        file.sync_data()
    }

    fn directory(&self, path: &Path) -> io::Result<()> {
        #[cfg(target_os = "macos")]
        if let Some(durability) = &self.durability {
            return durability.sync_directory(path);
        }
        sync_directory(path)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    files: BTreeMap<String, String>,
}

/// Abandoned generations are removed without the store lock, and hold no published references.
fn removed_concurrently(directory: &Path, error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound && !directory.exists()
}

impl Manifest {
    fn empty() -> Self {
        Self {
            version: 1,
            files: BTreeMap::new(),
        }
    }

    fn read(directory: &Path) -> Result<Option<Self>> {
        let marker = directory.join(FORMAT);
        let manifest = directory.join(MANIFEST);
        if !marker.try_exists()? && !manifest.try_exists()? {
            return Ok(None);
        }
        let format = match fs::read_to_string(&marker) {
            Ok(format) => format,
            Err(error) if removed_concurrently(directory, &error) => return Ok(None),
            Err(error) => return Err(error).context("read shared-segment format"),
        };
        if format != FORMAT_VERSION {
            bail!(
                "unsupported shared-segment format in {}",
                directory.display()
            );
        }
        let references = match fs::read(&manifest) {
            Ok(references) => references,
            Err(error) if removed_concurrently(directory, &error) => return Ok(None),
            Err(error) => return Err(error).context("read segment references"),
        };
        let parsed: Self = serde_json::from_slice(&references)?;
        if parsed.version != 1 {
            bail!("unsupported segment-reference version {}", parsed.version);
        }
        for (name, owner) in &parsed.files {
            if !safe_component(name) || metadata_file(Path::new(name)) || !safe_owner(owner) {
                bail!("invalid shared-segment reference");
            }
        }
        Ok(Some(parsed))
    }

    fn write(&self, directory: &Path, sync: Option<&PublicationSync>) -> Result<()> {
        let mut file = tempfile::NamedTempFile::new_in(directory)?;
        serde_json::to_writer(file.as_file_mut(), self)?;
        if let Some(sync) = sync {
            sync.file(file.as_file())?;
        }
        file.persist(directory.join(MANIFEST))
            .map_err(|error| error.error)?;
        if !directory.join(FORMAT).exists() {
            fs::write(directory.join(FORMAT), FORMAT_VERSION)?;
        }
        if let Some(sync) = sync {
            sync.file(&File::open(directory.join(FORMAT))?)?;
            sync.directory(directory)?;
        }
        Ok(())
    }
}

fn safe_component(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\', ':', '\0'])
}

fn safe_owner(owner: &str) -> bool {
    let Some((stamp, pid)) = owner.split_once('-') else {
        return false;
    };
    stamp.len() == 32
        && pid.len() == 8
        && stamp
            .bytes()
            .chain(pid.bytes())
            .all(|byte| byte.is_ascii_hexdigit())
}

fn metadata_file(path: &Path) -> bool {
    path.to_str()
        .is_some_and(|name| name == "meta.json" || name.starts_with('.'))
}

fn checked_name(path: &Path) -> io::Result<&str> {
    path.to_str()
        .filter(|name| safe_component(name))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid index filename"))
}

pub(super) struct StoreGuard {
    _file: File,
}

pub(super) fn lock_store(root: &Path) -> Result<StoreGuard> {
    let store = root.join(STORE);
    fs::create_dir_all(&store)?;
    if fs::symlink_metadata(&store)?.file_type().is_symlink() {
        bail!("segment store must not be a symlink");
    }
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(store.join(".lock"))?;
    file.lock()?;
    Ok(StoreGuard { _file: file })
}

pub(super) fn lock_existing_store(root: &Path) -> Result<Option<StoreGuard>> {
    let store = root.join(STORE);
    match fs::symlink_metadata(&store) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("segment store must not be a symlink");
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    let file = match File::open(store.join(".lock")) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    file.lock_shared()?;
    Ok(Some(StoreGuard { _file: file }))
}

#[derive(Debug)]
struct View {
    local: MmapDirectory,
    path: PathBuf,
    manifest: Manifest,
    created: HashSet<PathBuf>,
    durable_metadata: HashSet<PathBuf>,
    deleted: HashSet<PathBuf>,
    sealed: bool,
    _generation_lease: Option<Arc<GenerationLease>>,
    #[cfg(target_os = "macos")]
    durability: Option<Arc<StagingDurability>>,
}

#[derive(Clone, Debug)]
pub(super) struct SharedDirectory {
    store: MmapDirectory,
    store_path: PathBuf,
    view: Arc<RwLock<View>>,
}

impl SharedDirectory {
    pub fn open(root: &Path, path: &Path, sealed: bool) -> Result<Option<Self>> {
        let Some(manifest) = Manifest::read(path)? else {
            return Ok(None);
        };
        let store_path = root.join(STORE);
        if fs::symlink_metadata(&store_path)?.file_type().is_symlink() {
            bail!("segment store must not be a symlink");
        }
        let store = MmapDirectory::open(&store_path)?;
        let directory = Self {
            store,
            store_path,
            view: Arc::new(RwLock::new(View {
                local: MmapDirectory::open(path)?,
                path: path.to_path_buf(),
                manifest,
                created: HashSet::new(),
                durable_metadata: HashSet::new(),
                deleted: HashSet::new(),
                sealed,
                _generation_lease: None,
                #[cfg(target_os = "macos")]
                durability: None,
            })),
        };
        {
            let view = directory.view.read().unwrap();
            for (name, owner) in &view.manifest.files {
                directory.shared_path(owner, name)?;
            }
        }
        Ok(Some(directory))
    }

    pub fn stage(
        root: &Path,
        destination: &Path,
        source: Option<&Path>,
        owner: &str,
    ) -> Result<Self> {
        let mut manifest = Manifest::empty();
        if let Some(source) = source {
            if let Some(inherited) = Manifest::read(source)? {
                manifest = inherited;
                crate::profiling::count!("lexical.inherited_references", manifest.files.len());
            } else {
                let files = committed_generation_files(source)?;
                for name in files {
                    if metadata_file(&name) || !source.join(&name).is_file() {
                        continue;
                    }
                    adopt_file(root, owner, &name, &source.join(&name))?;
                    manifest
                        .files
                        .insert(checked_name(&name)?.to_owned(), owner.to_owned());
                    crate::profiling::count!("lexical.migrated_segment_files", 1);
                }
            }
            for name in ["meta.json", ".managed.json"] {
                if source.join(name).is_file() {
                    fs::copy(source.join(name), destination.join(name))?;
                }
            }
        }
        manifest.write(destination, None)?;
        Self::open(root, destination, false)?.ok_or_else(|| anyhow!("missing staging manifest"))
    }

    pub fn reset(&self) -> Result<()> {
        let mut view = self.view.write().unwrap();
        for entry in fs::read_dir(&view.path)? {
            let entry = entry?;
            if entry.file_name() != GENERATION_LEASE_FILE && entry.file_type()?.is_file() {
                fs::remove_file(entry.path())?;
            }
        }
        view.manifest = Manifest::empty();
        view.created.clear();
        view.deleted.clear();
        view.durable_metadata.clear();
        view.manifest.write(&view.path, None)
    }

    pub fn prepare_publication(
        &self,
        root: &Path,
        owner: &str,
        committed: &HashSet<PathBuf>,
    ) -> Result<()> {
        let mut view = self.view.write().unwrap();
        let sync = PublicationSync {
            #[cfg(target_os = "macos")]
            durability: view.durability.clone(),
        };
        let mut next = Manifest::empty();
        for path in committed {
            if metadata_file(path) {
                continue;
            }
            let name = checked_name(path)?;
            if view.created.contains(path) {
                if !view.local.exists(path)? {
                    bail!("new committed segment file is missing: {}", path.display());
                }
                adopt_file(root, owner, path, &view.path.join(path))?;
                #[cfg(target_os = "macos")]
                if let Some(durability) = &view.durability {
                    durability.check_path(&root.join(STORE).join(owner))?;
                    durability.check_path(&root.join(STORE).join(owner).join(path))?;
                }
                next.files.insert(name.to_owned(), owner.to_owned());
                crate::profiling::count!("lexical.new_shared_files", 1);
            } else if let Some(inherited) = view.manifest.files.get(name) {
                next.files.insert(name.to_owned(), inherited.clone());
            }
        }
        sync_owner(root, owner, &sync)?;
        for name in ["meta.json", ".managed.json"] {
            if !view.durable_metadata.contains(Path::new(name)) && view.path.join(name).is_file() {
                sync.file(&File::open(view.path.join(name))?)?;
            }
        }
        next.write(&view.path, Some(&sync))?;
        view.manifest = next;
        Ok(())
    }

    #[cfg(target_os = "macos")]
    pub(super) fn set_durability(&self, durability: Option<Arc<StagingDurability>>) {
        self.view.write().unwrap().durability = durability;
    }

    pub(super) fn sync_for_publication(&self) -> Result<()> {
        let view = self.view.read().unwrap();
        #[cfg(target_os = "macos")]
        if let Some(durability) = &view.durability {
            return Ok(durability.publish(&view.path)?);
        }
        create_generation_lease_file(&view.path)
    }

    pub fn pin_generation(&self, lease: Arc<GenerationLease>) {
        self.view.write().unwrap()._generation_lease = Some(lease);
    }

    pub fn seal_at(&self, path: &Path) -> Result<()> {
        let mut view = self.view.write().unwrap();
        view.local = MmapDirectory::open(path)?;
        view.path = path.to_path_buf();
        view.sealed = true;
        view.created.clear();
        view.deleted.clear();
        Ok(())
    }

    fn shared_path(&self, owner: &str, name: &str) -> io::Result<PathBuf> {
        let directory = self.store_path.join(owner);
        if fs::symlink_metadata(&directory)?.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shared-segment owner is a symlink",
            ));
        }
        let path = directory.join(name);
        if !fs::symlink_metadata(&path)?.file_type().is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shared segment is not a regular file",
            ));
        }
        Ok(PathBuf::from(owner).join(name))
    }
}

fn adopt_file(root: &Path, owner: &str, name: &Path, source: &Path) -> Result<()> {
    if !safe_owner(owner) {
        bail!("invalid segment owner");
    }
    if !fs::symlink_metadata(source)?.file_type().is_file() {
        bail!("segment source is not a regular file");
    }
    let directory = root.join(STORE).join(owner);
    fs::create_dir_all(&directory)?;
    if fs::symlink_metadata(&directory)?.file_type().is_symlink() {
        bail!("segment owner must not be a symlink");
    }
    let target = directory.join(checked_name(name)?);
    // `try_exists` follows links, so a dangling symlink here would look absent and the
    // copy below would write through it, outside the store. Any existing entry counts.
    match fs::symlink_metadata(&target) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() || fs::read(&target)? != fs::read(source)? {
                bail!("immutable segment collision at {}", target.display());
            }
            return Ok(());
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    if fs::hard_link(source, &target).is_err() {
        let mut destination = File::create_new(&target)?;
        io::copy(&mut File::open(source)?, &mut destination)?;
        destination.sync_all()?;
    }
    Ok(())
}

fn sync_owner(root: &Path, owner: &str, sync: &PublicationSync) -> Result<()> {
    let directory = root.join(STORE).join(owner);
    if directory.is_dir() {
        sync.directory(&directory)?;
        sync.directory(&root.join(STORE))?;
    }
    Ok(())
}

impl Directory for SharedDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let name = checked_name(path)
            .map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?;
        let view = self.view.read().unwrap();
        if view.deleted.contains(path) {
            return Err(OpenReadError::FileDoesNotExist(path.to_path_buf()));
        }
        if metadata_file(path) || view.created.contains(path) {
            return view.local.get_file_handle(path);
        }
        let Some(owner) = view.manifest.files.get(name) else {
            return Err(OpenReadError::FileDoesNotExist(path.to_path_buf()));
        };
        let shared = self
            .shared_path(owner, name)
            .map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?;
        self.store.get_file_handle(&shared)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        checked_name(path).map_err(|error| DeleteError::IoError {
            io_error: Arc::new(error),
            filepath: path.to_path_buf(),
        })?;
        let mut view = self.view.write().unwrap();
        if view.sealed {
            return Err(DeleteError::IoError {
                io_error: Arc::new(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "sealed generation",
                )),
                filepath: path.to_path_buf(),
            });
        }
        let local_exists = view
            .local
            .exists(path)
            .map_err(|error| DeleteError::IoError {
                io_error: Arc::new(io::Error::other(error)),
                filepath: path.to_path_buf(),
            })?;
        if local_exists {
            view.local.delete(path)?;
        } else if !view
            .manifest
            .files
            .contains_key(&path.to_string_lossy().into_owned())
        {
            return Err(DeleteError::FileDoesNotExist(path.to_path_buf()));
        }
        view.created.remove(path);
        view.deleted.insert(path.to_path_buf());
        Ok(())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        let name = checked_name(path)
            .map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?;
        let view = self.view.read().unwrap();
        if view.deleted.contains(path) {
            return Ok(false);
        }
        if metadata_file(path) || view.created.contains(path) {
            return view.local.exists(path);
        }
        let Some(owner) = view.manifest.files.get(name) else {
            return Ok(false);
        };
        let shared = self
            .shared_path(owner, name)
            .map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?;
        self.store.exists(&shared)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        checked_name(path)
            .map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?;
        let mut view = self.view.write().unwrap();
        if view.sealed {
            return Err(OpenWriteError::wrap_io_error(
                io::Error::new(io::ErrorKind::PermissionDenied, "sealed generation"),
                path.to_path_buf(),
            ));
        }
        #[cfg(target_os = "macos")]
        let result = match &view.durability {
            Some(durability) => durability.open_write(&view.path, path)?,
            None => view.local.open_write(path)?,
        };
        #[cfg(not(target_os = "macos"))]
        let result = view.local.open_write(path)?;
        view.deleted.remove(path);
        view.created.insert(path.to_path_buf());
        Ok(result)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        checked_name(path)
            .map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?;
        if metadata_file(path) {
            return self.view.read().unwrap().local.atomic_read(path);
        }
        let handle = self.get_file_handle(path)?;
        Ok(handle
            .read_bytes(0..handle.len())
            .map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?
            .to_vec())
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        checked_name(path)?;
        let view = self.view.read().unwrap();
        if view.sealed {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "sealed generation",
            ));
        }
        let local = view.local.clone();
        #[cfg(target_os = "macos")]
        let staging = view
            .durability
            .clone()
            .map(|durability| (durability, view.path.clone()));
        drop(view);
        #[cfg(target_os = "macos")]
        let durable = if let Some((durability, directory)) = staging {
            durability.atomic_write(&directory, path, data)?;
            true
        } else {
            local.atomic_write(path, data)?;
            true
        };
        #[cfg(not(target_os = "macos"))]
        let durable = {
            local.atomic_write(path, data)?;
            true
        };
        let mut view = self.view.write().unwrap();
        view.deleted.remove(path);
        if metadata_file(path) {
            if durable {
                view.durable_metadata.insert(path.to_path_buf());
            } else {
                view.durable_metadata.remove(path);
            }
        } else {
            view.created.insert(path.to_path_buf());
        }
        Ok(())
    }

    fn sync_directory(&self) -> io::Result<()> {
        let view = self.view.read().unwrap();
        #[cfg(target_os = "macos")]
        if !view.sealed
            && let Some(durability) = &view.durability
        {
            return durability.sync_directory(&view.path);
        }
        view.local.sync_directory()
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        let (local, sealed) = {
            let view = self.view.read().unwrap();
            (view.local.clone(), view.sealed)
        };
        if sealed {
            Ok(DirectoryLock::from(Box::new(())))
        } else {
            local.acquire_lock(lock)
        }
    }

    fn watch(&self, callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.view.read().unwrap().local.watch(callback)
    }
}

pub(super) fn open_directory(root: &Path, path: &Path) -> Result<Box<dyn Directory>> {
    if let Some(directory) = SharedDirectory::open(root, path, true)? {
        Ok(Box::new(directory))
    } else {
        Ok(Box::new(SealedDirectory {
            directory: MmapDirectory::open(path)?,
            _generation_lease: None,
        }))
    }
}

pub(super) fn index_root(generation: &Path) -> &Path {
    if generation
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == GENERATIONS_DIR)
    {
        generation
            .parent()
            .and_then(Path::parent)
            .unwrap_or(generation)
    } else {
        generation
    }
}

pub(super) fn collect_unreachable(root: &Path, dry_run: bool) -> Result<usize> {
    collect_unreachable_excluding(root, dry_run, &[])
}

/// `doomed` names generation directories the caller is about to remove. A dry run has not
/// removed them yet, so without this their manifests would keep their payloads reachable and
/// the reported count would be far lower than what a real run reclaims.
pub(super) fn collect_unreachable_excluding(
    root: &Path,
    dry_run: bool,
    doomed: &[PathBuf],
) -> Result<usize> {
    collect_unreachable_with_sync(root, dry_run, doomed, super::fsync_directory)
}

fn collect_unreachable_with_sync(
    root: &Path,
    dry_run: bool,
    doomed: &[PathBuf],
    synchronize: impl FnOnce(&Path) -> io::Result<()>,
) -> Result<usize> {
    crate::profiling::span!("lexical.cleanup.shared");
    crate::profiling::count!("lexical.cleanup.shared.calls", 1);
    let store = root.join(STORE);
    if !store.exists() {
        crate::profiling::count!("lexical.cleanup.shared.sync_skips", 1);
        return Ok(0);
    }
    let mut reachable = HashSet::new();
    for entry in fs::read_dir(root.join(GENERATIONS_DIR))? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() || doomed.iter().any(|path| *path == entry.path()) {
            continue;
        }
        if let Some(manifest) = Manifest::read(&entry.path())? {
            reachable.extend(
                manifest
                    .files
                    .into_iter()
                    .map(|(name, owner)| PathBuf::from(owner).join(name)),
            );
        }
    }
    let mut removed = 0;
    let mut removal_attempted = false;
    for owner in fs::read_dir(&store)? {
        let owner = owner?;
        if owner.file_name() == ".lock" {
            continue;
        }
        if !owner.file_type()?.is_dir() || !safe_owner(&owner.file_name().to_string_lossy()) {
            bail!("unexpected segment-store entry");
        }
        for entry in fs::read_dir(owner.path())? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                bail!("unexpected segment-store file");
            }
            let key = PathBuf::from(owner.file_name()).join(entry.file_name());
            if !reachable.contains(&key) {
                removed += 1;
                if !dry_run {
                    removal_attempted = true;
                    fs::remove_file(entry.path())?;
                }
            }
        }
        if !dry_run && fs::read_dir(owner.path())?.next().is_none() {
            removal_attempted = true;
            fs::remove_dir(owner.path())?;
        }
    }
    if removal_attempted {
        crate::profiling::count!("lexical.cleanup.shared.sync_requests", 1);
        synchronize(&store)?;
    } else {
        crate::profiling::count!("lexical.cleanup.shared.sync_skips", 1);
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stage(root: &Path, owner: &str, source: Option<&Path>) -> SharedDirectory {
        let path = root.join(GENERATIONS_DIR).join(owner);
        fs::create_dir_all(&path).unwrap();
        SharedDirectory::stage(root, &path, source, owner).unwrap()
    }

    fn bytes(directory: &SharedDirectory, path: &str) -> Vec<u8> {
        directory.atomic_read(Path::new(path)).unwrap()
    }

    #[test]
    fn a_generation_removed_between_listing_and_read_contributes_no_references() {
        let temp = tempfile::tempdir().unwrap();
        let removed = temp.path().join("generations/.pending.tmp");
        assert!(Manifest::read(&removed).unwrap().is_none());
    }

    #[test]
    fn a_present_generation_missing_its_format_marker_still_fails() {
        let temp = tempfile::tempdir().unwrap();
        let generation = temp.path().join("generations/live");
        fs::create_dir_all(&generation).unwrap();
        fs::write(generation.join(MANIFEST), b"{\"version\":1,\"files\":{}}").unwrap();
        assert!(Manifest::read(&generation).is_err());
    }

    #[test]
    fn cleanup_shared_empty_owner_syncs_despite_zero_file_count() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let _guard = lock_store(root).unwrap();
        fs::create_dir(root.join(GENERATIONS_DIR)).unwrap();
        let owner = root.join(STORE).join(new_generation_name());
        fs::create_dir(&owner).unwrap();
        assert_eq!(
            collect_unreachable_with_sync(root, true, &[], |_| {
                panic!("dry run must not synchronize")
            })
            .unwrap(),
            0
        );
        assert!(owner.exists());
        let syncs = std::cell::Cell::new(0);
        assert_eq!(
            collect_unreachable_with_sync(root, false, &[], |path| {
                assert_eq!(path, root.join(STORE));
                syncs.set(syncs.get() + 1);
                Ok(())
            })
            .unwrap(),
            0
        );
        assert_eq!(syncs.get(), 1);
        assert!(!owner.exists());
        assert_eq!(
            collect_unreachable_with_sync(root, false, &[], |_| {
                panic!("unchanged store must not synchronize")
            })
            .unwrap(),
            0
        );
    }

    #[test]
    fn cleanup_shared_dry_run_preserves_files_and_removal_propagates_sync_failure() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let _guard = lock_store(root).unwrap();
        fs::create_dir(root.join(GENERATIONS_DIR)).unwrap();
        let owner = root.join(STORE).join(new_generation_name());
        fs::create_dir(&owner).unwrap();
        let file = owner.join("orphan.store");
        fs::write(&file, b"unreachable").unwrap();
        assert_eq!(
            collect_unreachable_with_sync(root, true, &[], |_| {
                panic!("dry run must not synchronize")
            })
            .unwrap(),
            1
        );
        assert!(file.exists());
        let syncs = std::cell::Cell::new(0);
        let error = collect_unreachable_with_sync(root, false, &[], |path| {
            assert_eq!(path, root.join(STORE));
            syncs.set(syncs.get() + 1);
            Err(io::Error::other("shared sync fault"))
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "shared sync fault");
        assert_eq!(syncs.get(), 1);
        assert!(!owner.exists());
    }

    #[test]
    fn cleanup_shared_reachable_files_need_no_sync() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let _guard = lock_store(root).unwrap();
        let owner = new_generation_name();
        let directory = stage(root, &owner, None);
        let file = Path::new("live.store");
        directory.atomic_write(file, b"reachable").unwrap();
        directory
            .prepare_publication(root, &owner, &HashSet::from([file.to_path_buf()]))
            .unwrap();
        assert_eq!(
            collect_unreachable_with_sync(root, false, &[], |_| {
                panic!("reachable store must not synchronize")
            })
            .unwrap(),
            0
        );
        assert_eq!(bytes(&directory, "live.store"), b"reachable");
    }

    #[test]
    fn cleanup_shared_invalid_owner_keeps_error_without_sync() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let _guard = lock_store(root).unwrap();
        fs::create_dir(root.join(GENERATIONS_DIR)).unwrap();
        fs::create_dir(root.join(STORE).join("invalid-owner")).unwrap();
        let error = collect_unreachable_with_sync(root, false, &[], |_| {
            panic!("invalid inventory must not introduce synchronization")
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "unexpected segment-store entry");
    }

    #[test]
    fn inherited_files_are_referenced_and_branch_writes_are_isolated() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let _guard = lock_store(root).unwrap();
        let first = new_generation_name();
        let original = stage(root, &first, None);
        let file = Path::new("segment.1.del");
        original.atomic_write(file, b"original").unwrap();
        let committed = HashSet::from([file.to_path_buf()]);
        original
            .prepare_publication(root, &first, &committed)
            .unwrap();
        let original_path = root.join(GENERATIONS_DIR).join(&first);
        original.seal_at(&original_path).unwrap();
        assert!(original.atomic_write(file, b"forbidden").is_err());
        assert!(original.delete(file).is_err());

        let left_id = new_generation_name();
        let left = stage(root, &left_id, Some(&original_path));
        let right_id = new_generation_name();
        let right = stage(root, &right_id, Some(&original_path));
        assert!(
            !root
                .join(GENERATIONS_DIR)
                .join(&left_id)
                .join(file)
                .exists()
        );
        assert_eq!(bytes(&left, "segment.1.del"), b"original");
        left.atomic_write(file, b"left").unwrap();
        right.atomic_write(file, b"right").unwrap();
        left.prepare_publication(root, &left_id, &committed)
            .unwrap();
        right
            .prepare_publication(root, &right_id, &committed)
            .unwrap();
        assert_eq!(bytes(&original, "segment.1.del"), b"original");
        assert_eq!(bytes(&left, "segment.1.del"), b"left");
        assert_eq!(bytes(&right, "segment.1.del"), b"right");
        assert!(root.join(STORE).join(&left_id).join(file).exists());
        assert!(root.join(STORE).join(&right_id).join(file).exists());
        left.delete(file).unwrap();
        assert!(!left.exists(file).unwrap());
        assert_eq!(bytes(&original, "segment.1.del"), b"original");
    }

    #[cfg(unix)]
    #[test]
    fn shared_files_survive_reader_and_staging_leases_until_last_reference() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let _store = lock_store(root).unwrap();
        let owner = new_generation_name();
        let original = stage(root, &owner, None);
        let path = Path::new("segment.store");
        original.atomic_write(path, b"retained data").unwrap();
        original
            .prepare_publication(root, &owner, &HashSet::from([path.to_path_buf()]))
            .unwrap();
        let original_path = root.join(GENERATIONS_DIR).join(&owner);
        create_generation_lease_file(&original_path).unwrap();
        let reader = acquire_generation_lease(&original_path).unwrap();
        original.seal_at(&original_path).unwrap();

        let staging_id = new_generation_name();
        let staging_path = root
            .join(GENERATIONS_DIR)
            .join(format!(".{staging_id}.tmp"));
        fs::create_dir(&staging_path).unwrap();
        create_generation_lease_file(&staging_path).unwrap();
        let staging_lease = acquire_generation_lease(&staging_path).unwrap();
        let staging =
            SharedDirectory::stage(root, &staging_path, Some(&original_path), &staging_id).unwrap();
        let current = new_generation_name();
        stage(root, &current, None);
        atomic_write_current(root, &current).unwrap();
        prune_superseded_generations(root, &current).unwrap();
        assert_eq!(collect_unreachable(root, false).unwrap(), 0);
        assert_eq!(bytes(&original, "segment.store"), b"retained data");
        drop(reader);
        prune_superseded_generations(root, &current).unwrap();
        assert_eq!(collect_unreachable(root, false).unwrap(), 0);
        assert_eq!(bytes(&staging, "segment.store"), b"retained data");
        drop(staging_lease);
        prune_superseded_generations(root, &current).unwrap();
        assert_eq!(collect_unreachable(root, true).unwrap(), 1);
        assert_eq!(collect_unreachable(root, false).unwrap(), 1);
        assert!(!root.join(STORE).join(&owner).exists());
    }

    #[test]
    fn malformed_missing_and_traversing_manifests_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join(FORMAT), FORMAT_VERSION).unwrap();
        assert!(Manifest::read(temp.path()).is_err());
        for value in [
            serde_json::json!({"version":2,"files":{}}),
            serde_json::json!({"version":1,"files":{"../escape":"owner"}}),
            serde_json::json!({"version":1,"files":{"segment.store":"../owner"}}),
            serde_json::json!({"version":1,"files":{"meta.json":"00000000000000000000000000000000-00000001"}}),
        ] {
            fs::write(
                temp.path().join(MANIFEST),
                serde_json::to_vec(&value).unwrap(),
            )
            .unwrap();
            assert!(Manifest::read(temp.path()).is_err());
        }
    }

    #[test]
    fn missing_shared_files_are_errors_not_absent_optional_components() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let _guard = lock_store(root).unwrap();
        let owner = new_generation_name();
        let directory = stage(root, &owner, None);
        let path = Path::new("segment.store");
        directory.atomic_write(path, b"data").unwrap();
        directory
            .prepare_publication(root, &owner, &HashSet::from([path.to_path_buf()]))
            .unwrap();
        directory
            .seal_at(&root.join(GENERATIONS_DIR).join(&owner))
            .unwrap();
        fs::remove_file(root.join(STORE).join(&owner).join(path)).unwrap();
        assert!(directory.get_file_handle(path).is_err());
        assert!(directory.exists(path).is_err());
        let generation = root.join(GENERATIONS_DIR).join(&owner);
        assert!(SharedDirectory::open(root, &generation, true).is_err());
        let next = new_generation_name();
        let destination = root.join(GENERATIONS_DIR).join(&next);
        fs::create_dir(&destination).unwrap();
        assert!(SharedDirectory::stage(root, &destination, Some(&generation), &next).is_err());
    }
}
