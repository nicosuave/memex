use crate::config::Paths;
use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

const ACCESS_TOKEN_BYTES: usize = 32;
const BOOTSTRAP_NONCE_BYTES: usize = 16;
const BOOTSTRAP_PAYLOAD_BYTES: usize = 8 + BOOTSTRAP_NONCE_BYTES;
const BOOTSTRAP_TOKEN_BYTES: usize = BOOTSTRAP_PAYLOAD_BYTES + 32;
const BOOTSTRAP_TTL: Duration = Duration::from_secs(60);
const SESSION_TTL: Duration = Duration::from_secs(12 * 60 * 60);
const TOKEN_FILE: &str = "web-auth-token";
type HmacSha256 = Hmac<Sha256>;

pub struct WebAuth {
    secret: [u8; ACCESS_TOKEN_BYTES],
    sessions: Mutex<HashMap<[u8; 32], Instant>>,
    used_bootstrap_tokens: Mutex<HashMap<[u8; 32], u64>>,
}

impl WebAuth {
    pub fn load_or_create(paths: &Paths) -> Result<Self> {
        Ok(Self {
            secret: load_or_create_secret(paths)?,
            sessions: Mutex::new(HashMap::new()),
            used_bootstrap_tokens: Mutex::new(HashMap::new()),
        })
    }

    pub fn create_bootstrap_token(&self) -> Result<String> {
        let expires_at = SystemTime::now()
            .checked_add(BOOTSTRAP_TTL)
            .ok_or_else(|| anyhow!("bootstrap expiry overflow"))?
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_secs();
        let mut payload = [0_u8; BOOTSTRAP_PAYLOAD_BYTES];
        payload[..8].copy_from_slice(&expires_at.to_be_bytes());
        fill_random(&mut payload[8..])?;

        let signature = sign_bootstrap(&self.secret, &payload);
        let mut token = [0_u8; BOOTSTRAP_TOKEN_BYTES];
        token[..BOOTSTRAP_PAYLOAD_BYTES].copy_from_slice(&payload);
        token[BOOTSTRAP_PAYLOAD_BYTES..].copy_from_slice(&signature);
        Ok(URL_SAFE_NO_PAD.encode(token))
    }

    pub fn exchange_bootstrap_token(&self, encoded: &str) -> Result<String> {
        let decoded = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| anyhow!("invalid bootstrap token"))?;
        let token: [u8; BOOTSTRAP_TOKEN_BYTES] = decoded
            .try_into()
            .map_err(|_| anyhow!("invalid bootstrap token"))?;
        let (payload, signature) = token.split_at(BOOTSTRAP_PAYLOAD_BYTES);
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("valid HMAC key");
        mac.update(payload);
        if mac.verify_slice(signature).is_err() {
            bail!("invalid bootstrap token");
        }

        let expires_at = u64::from_be_bytes(payload[..8].try_into().expect("fixed timestamp"));
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_secs();
        if expires_at < now || expires_at > now.saturating_add(BOOTSTRAP_TTL.as_secs()) {
            bail!("expired bootstrap token");
        }

        let fingerprint: [u8; 32] = Sha256::digest(token).into();
        let mut used = self
            .used_bootstrap_tokens
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        used.retain(|_, expiry| *expiry >= now);
        if used.insert(fingerprint, expires_at).is_some() {
            bail!("bootstrap token has already been used");
        }
        drop(used);

        let mut session = [0_u8; ACCESS_TOKEN_BYTES];
        fill_random(&mut session)?;
        let session_fingerprint: [u8; 32] = Sha256::digest(session).into();
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(session_fingerprint, Instant::now());
        Ok(URL_SAFE_NO_PAD.encode(session))
    }

    pub fn authorize_bearer(&self, encoded: &str) -> bool {
        let Ok(decoded) = URL_SAFE_NO_PAD.decode(encoded) else {
            return false;
        };
        let Ok(candidate) = <[u8; ACCESS_TOKEN_BYTES]>::try_from(decoded) else {
            return false;
        };
        bool::from(self.secret.ct_eq(&candidate))
    }

    pub fn authorize_session(&self, encoded: &str) -> bool {
        let Ok(decoded) = URL_SAFE_NO_PAD.decode(encoded) else {
            return false;
        };
        let Ok(session) = <[u8; ACCESS_TOKEN_BYTES]>::try_from(decoded) else {
            return false;
        };
        let fingerprint: [u8; 32] = Sha256::digest(session).into();
        let now = Instant::now();
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        sessions.retain(|_, created_at| now.duration_since(*created_at) <= SESSION_TTL);
        sessions.contains_key(&fingerprint)
    }
}

pub fn token_path(paths: &Paths) -> PathBuf {
    paths.root.join(TOKEN_FILE)
}

fn load_or_create_secret(paths: &Paths) -> Result<[u8; ACCESS_TOKEN_BYTES]> {
    std::fs::create_dir_all(&paths.root)
        .with_context(|| format!("failed to create {}", paths.root.display()))?;
    let path = token_path(paths);
    match publish_new_secret(&path)? {
        Some(secret) => Ok(secret),
        None => read_secret(&path),
    }
}

fn publish_new_secret(path: &Path) -> Result<Option<[u8; ACCESS_TOKEN_BYTES]>> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("web auth token path has no parent: {}", path.display()))?;
    let mut secret = [0_u8; ACCESS_TOKEN_BYTES];
    fill_random(&mut secret)?;
    let encoded = URL_SAFE_NO_PAD.encode(secret);

    for _ in 0..16 {
        let mut suffix = [0_u8; 16];
        fill_random(&mut suffix)?;
        let temp_path = parent.join(format!(
            ".{TOKEN_FILE}.{}.tmp",
            URL_SAFE_NO_PAD.encode(suffix)
        ));
        let mut file = match open_secret_for_create(&temp_path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to create {}", temp_path.display()));
            }
        };

        let write_result = (|| -> std::io::Result<()> {
            file.write_all(encoded.as_bytes())?;
            file.write_all(b"\n")?;
            file.sync_all()
        })();
        if let Err(err) = write_result {
            drop(file);
            let _ = std::fs::remove_file(&temp_path);
            return Err(err).with_context(|| format!("failed to write {}", temp_path.display()));
        }
        drop(file);

        match std::fs::hard_link(&temp_path, path) {
            Ok(()) => {
                std::fs::remove_file(&temp_path).with_context(|| {
                    format!("failed to remove temporary token {}", temp_path.display())
                })?;
                sync_directory(parent)?;
                return Ok(Some(secret));
            }
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                let _ = std::fs::remove_file(&temp_path);
                return Ok(None);
            }
            Err(err) => {
                let _ = std::fs::remove_file(&temp_path);
                return Err(err).with_context(|| format!("failed to publish {}", path.display()));
            }
        }
    }

    bail!("failed to allocate a temporary web auth token file")
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("failed to sync {}", path.display()))
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn read_secret(path: &Path) -> Result<[u8; ACCESS_TOKEN_BYTES]> {
    let mut file =
        open_secret_for_read(path).with_context(|| format!("failed to open {}", path.display()))?;
    validate_secret_file(&file, path)?;
    let mut encoded = String::new();
    file.read_to_string(&mut encoded)?;
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded.trim())
        .with_context(|| format!("invalid token in {}", path.display()))?;
    decoded
        .try_into()
        .map_err(|_| anyhow!("invalid token length in {}", path.display()))
}

#[cfg(unix)]
fn open_secret_for_create(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(not(unix))]
fn open_secret_for_create(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

#[cfg(unix)]
fn open_secret_for_read(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(not(unix))]
fn open_secret_for_read(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().read(true).open(path)
}

fn validate_secret_file(file: &File, path: &Path) -> Result<()> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        bail!("web auth token is not a regular file: {}", path.display());
    }
    #[cfg(unix)]
    {
        // SAFETY: geteuid has no arguments and only reads process credentials.
        if metadata.uid() != unsafe { libc::geteuid() } {
            bail!(
                "web auth token is not owned by the current user: {}",
                path.display()
            );
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!(
                "web auth token permissions are too broad: {} (run chmod 600 {})",
                path.display(),
                path.display()
            );
        }
    }
    Ok(())
}

fn fill_random(bytes: &mut [u8]) -> Result<()> {
    getrandom::fill(bytes).map_err(|err| anyhow!("secure random generation failed: {err}"))
}

fn sign_bootstrap(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("valid HMAC key");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn creates_and_reuses_restricted_access_token() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        let first = WebAuth::load_or_create(&paths).unwrap();
        let token = std::fs::read_to_string(token_path(&paths)).unwrap();
        assert!(first.authorize_bearer(token.trim()));

        let second = WebAuth::load_or_create(&paths).unwrap();
        assert!(second.authorize_bearer(token.trim()));
        #[cfg(unix)]
        assert_eq!(
            std::fs::metadata(token_path(&paths))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[test]
    fn concurrent_first_use_publishes_one_complete_access_token() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().to_path_buf();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(16));
        let threads: Vec<_> = (0..16)
            .map(|_| {
                let root = root.clone();
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let paths = Paths::new(Some(root)).unwrap();
                    barrier.wait();
                    WebAuth::load_or_create(&paths).unwrap()
                })
            })
            .collect();
        let auths: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        let paths = Paths::new(Some(root)).unwrap();
        let token = std::fs::read_to_string(token_path(&paths)).unwrap();

        assert!(auths.iter().all(|auth| auth.authorize_bearer(token.trim())));
        assert_eq!(
            std::fs::read_dir(&paths.root)
                .unwrap()
                .filter_map(Result::ok)
                .count(),
            1
        );
    }

    #[test]
    fn bootstrap_tokens_are_short_lived_and_single_use() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        let auth = WebAuth::load_or_create(&paths).unwrap();
        let bootstrap = auth.create_bootstrap_token().unwrap();
        let session = auth.exchange_bootstrap_token(&bootstrap).unwrap();

        assert!(auth.authorize_session(&session));
        assert!(auth.exchange_bootstrap_token(&bootstrap).is_err());
    }

    #[test]
    #[cfg(unix)]
    fn rejects_access_token_with_broad_permissions() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        let auth = WebAuth::load_or_create(&paths).unwrap();
        drop(auth);
        std::fs::set_permissions(token_path(&paths), std::fs::Permissions::from_mode(0o644))
            .unwrap();

        let error = WebAuth::load_or_create(&paths).err().unwrap();
        assert!(error.to_string().contains("permissions are too broad"));
    }
}
