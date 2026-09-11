#[cfg(test)]
mod tests;

use super::*;
use std::ffi::CStr;
use std::io::BufWriter;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use tantivy::directory::{AntiCallToken, TerminatingWrite};

#[derive(Debug)]
pub(crate) struct StagingDurability {
    device: u64,
    directories: Vec<(PathBuf, File)>,
    lease: File,
    #[cfg(test)]
    pub(super) calls: std::sync::Mutex<Vec<bool>>,
    #[cfg(test)]
    pub(super) fail: std::sync::Mutex<Option<bool>>,
}

impl StagingDurability {
    pub(crate) fn prepare(root: &Path, staging: &Path) -> io::Result<Option<Arc<Self>>> {
        let lease = OpenOptions::new()
            .create(true)
            .append(true)
            .open(staging.join(GENERATION_LEASE_FILE))?;
        let device = lease.metadata()?.dev();
        let mut directories = Vec::new();
        for path in [
            root.to_path_buf(),
            root.join(GENERATIONS_DIR),
            staging.to_path_buf(),
            root.join(STORE),
        ] {
            let directory = File::open(&path)?;
            if directory.metadata()?.dev() != device || !supported_filesystem(&directory)? {
                lease.sync_all()?;
                return Ok(None);
            }
            directories.push((path, directory));
        }
        if !probe_full_sync(&lease)? {
            return Ok(None);
        }
        Ok(Some(Arc::new(Self {
            device,
            directories,
            lease,
            #[cfg(test)]
            calls: Default::default(),
            #[cfg(test)]
            fail: Default::default(),
        })))
    }

    pub(super) fn check_file(&self, file: &File) -> io::Result<()> {
        if file.metadata()?.dev() != self.device {
            return Err(io::Error::other("staging durability device changed"));
        }
        Ok(())
    }

    pub(super) fn check_path(&self, path: &Path) -> io::Result<()> {
        self.check_file(&File::open(path)?)
    }

    pub(super) fn publish(&self, staging: &Path) -> io::Result<()> {
        for (path, directory) in &self.directories {
            let expected = directory.metadata()?;
            let actual = fs::metadata(path)?;
            if actual.dev() != expected.dev() || actual.ino() != expected.ino() {
                return Err(io::Error::other("staging durability directory changed"));
            }
        }
        let lease = fs::symlink_metadata(staging.join(GENERATION_LEASE_FILE))?;
        let expected = self.lease.metadata()?;
        if !lease.is_file() || lease.dev() != expected.dev() || lease.ino() != expected.ino() {
            return Err(io::Error::other("staging durability lease changed"));
        }
        // F_FULLFSYNC drains earlier fsyncs on this device before the generation is reachable.
        crate::profiling::span!("lexical.publication_fullsync");
        self.synchronize(&self.lease, true)
    }

    fn synchronize(&self, file: &File, full: bool) -> io::Result<()> {
        self.check_file(file)?;
        #[cfg(test)]
        {
            self.calls.lock().unwrap().push(full);
            if *self.fail.lock().unwrap() == Some(full) {
                return Err(io::Error::from_raw_os_error(libc::EIO));
            }
        }
        if full {
            crate::profiling::count!("lexical.full_syncs", 1);
            full_sync(file)
        } else {
            crate::profiling::count!("lexical.staging_fsyncs", 1);
            retry_sync(|| unsafe { libc::fsync(file.as_raw_fd()) })
        }
    }

    pub(super) fn open_write(
        self: &Arc<Self>,
        directory: &Path,
        path: &Path,
    ) -> Result<WritePtr, OpenWriteError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join(path))
            .map_err(|error| {
                if error.kind() == io::ErrorKind::AlreadyExists {
                    OpenWriteError::FileAlreadyExists(path.to_path_buf())
                } else {
                    OpenWriteError::wrap_io_error(error, path.to_path_buf())
                }
            })?;
        self.check_file(&file)
            .and_then(|()| file.flush())
            .map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?;
        Ok(BufWriter::new(Box::new(StagingWriter {
            file,
            durability: Arc::clone(self),
        })))
    }

    pub(super) fn atomic_write(
        &self,
        directory: &Path,
        path: &Path,
        data: &[u8],
    ) -> io::Result<()> {
        let mut file = tempfile::NamedTempFile::new_in(directory)?;
        self.check_file(file.as_file())?;
        file.write_all(data)?;
        file.flush()?;
        self.synchronize(file.as_file(), false)?;
        file.persist(directory.join(path))
            .map_err(|error| error.error)?;
        Ok(())
    }

    pub(super) fn sync_file(&self, file: &File) -> io::Result<()> {
        self.synchronize(file, false)
    }

    pub(super) fn sync_directory(&self, directory: &Path) -> io::Result<()> {
        self.sync_file(&File::open(directory)?)
    }
}

struct StagingWriter {
    file: File,
    durability: Arc<StagingDurability>,
}

impl Write for StagingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.file.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl TerminatingWrite for StagingWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.file.flush()?;
        self.durability.synchronize(&self.file, false)
    }
}

fn supported_filesystem(file: &File) -> io::Result<bool> {
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
    if unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    let name = unsafe { CStr::from_ptr(stat.f_fstypename.as_ptr()) }.to_bytes();
    Ok(stat.f_flags & libc::MNT_LOCAL as u32 != 0 && matches!(name, b"apfs" | b"hfs"))
}

fn probe_full_sync(file: &File) -> io::Result<bool> {
    finish_probe(file, full_sync(file))
}

fn finish_probe(file: &File, result: io::Result<()>) -> io::Result<bool> {
    match result {
        Ok(()) => Ok(true),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::EINVAL | libc::ENOTSUP | libc::ENOSYS | libc::ENOTTY)
            ) =>
        {
            file.sync_all()?;
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

fn full_sync(file: &File) -> io::Result<()> {
    retry_sync(|| unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) })
}

fn retry_sync(mut call: impl FnMut() -> libc::c_int) -> io::Result<()> {
    loop {
        if call() == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}
