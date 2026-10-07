//! Explicit execution-host gateway. Retrieval credentials never authorize this endpoint.
use crate::config::Paths;
use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::time::Duration;

pub const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

/// A reverse proxy may terminate TLS while the upstream Host remains loopback.
/// Only an explicitly configured HTTPS origin is accepted, never arbitrary
/// Forwarded/X-Forwarded-* headers supplied by a requester.
pub fn allows_public_origin(origin: &str, configured: Option<&str>) -> bool {
    let Some(configured) = configured else {
        return false;
    };
    let Ok(url) = url::Url::parse(configured) else {
        return false;
    };
    url.scheme() == "https"
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && matches!(url.path(), "" | "/")
        && url.query().is_none()
        && url.fragment().is_none()
        && url.origin().ascii_serialization() == origin
}

#[cfg(unix)]
pub fn authorize(paths: &Paths, token: &str) -> Result<bool> {
    use std::fs::OpenOptions;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use subtle::ConstantTimeEq;

    if token.len() != 64 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(false);
    }
    let directory = paths.state.join("execution");
    let metadata = std::fs::symlink_metadata(&directory)
        .context("execution host is not configured for this Memex root")?;
    ensure!(
        metadata.is_dir()
            && metadata.uid() == unsafe { libc::getuid() }
            && metadata.mode() & 0o077 == 0,
        "execution directory is not private to this user"
    );
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(directory.join("control-token"))
        .context("execution control pairing is unavailable")?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file()
            && metadata.uid() == unsafe { libc::getuid() }
            && metadata.mode() & 0o077 == 0,
        "execution control token is not private to this user"
    );
    let mut expected = String::new();
    std::io::Read::by_ref(&mut file)
        .take(67)
        .read_to_string(&mut expected)?;
    let expected = expected.trim();
    ensure!(expected.len() == 64, "execution pairing token is invalid");
    Ok(bool::from(expected.as_bytes().ct_eq(token.as_bytes())))
}

#[cfg(not(unix))]
pub fn authorize(_paths: &Paths, _token: &str) -> Result<bool> {
    bail!("execution hosts require Unix domain socket support")
}

/// A local stdio MCP caller already has same-user execution authority. HTTP callers
/// must pass the separate bearer check before reaching this function.
#[cfg(unix)]
pub fn request(paths: &Paths, request: &Value) -> Result<Value> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    use std::os::unix::net::UnixStream;

    ensure!(request.is_object(), "execution request must be an object");
    ensure!(
        request["method"].is_string(),
        "execution request requires method"
    );
    ensure!(
        request["params"].is_object(),
        "execution request requires params"
    );
    let mut body = serde_json::to_vec(request)?;
    ensure!(
        body.len() <= MAX_REQUEST_BYTES,
        "execution request exceeds 4 MiB"
    );
    body.push(b'\n');
    let directory = paths.state.join("execution");
    let metadata =
        std::fs::symlink_metadata(&directory).context("execution host is not running")?;
    ensure!(
        metadata.is_dir()
            && metadata.uid() == unsafe { libc::getuid() }
            && metadata.mode() & 0o077 == 0,
        "execution directory is not private to this user"
    );
    let endpoint = directory.join("control.sock");
    let metadata = std::fs::symlink_metadata(&endpoint).context("execution host is not running")?;
    ensure!(
        metadata.file_type().is_socket()
            && metadata.uid() == unsafe { libc::getuid() }
            && metadata.mode() & 0o077 == 0,
        "execution endpoint is not a private socket owned by this user"
    );
    let mut stream = UnixStream::connect(endpoint).context("connect to the execution host")?;
    stream.set_read_timeout(Some(Duration::from_secs(65)))?;
    stream.set_write_timeout(Some(Duration::from_secs(65)))?;
    stream.write_all(&body)?;
    let mut response = Vec::new();
    BufReader::new(stream)
        .take((MAX_RESPONSE_BYTES + 1) as u64)
        .read_until(b'\n', &mut response)?;
    ensure!(
        response.len() <= MAX_RESPONSE_BYTES,
        "execution response exceeds 32 MiB"
    );
    ensure!(
        response.last() == Some(&b'\n'),
        "execution response was interrupted; retain the original command ID"
    );
    serde_json::from_slice(&response).context("decode execution host response")
}

#[cfg(not(unix))]
pub fn request(_paths: &Paths, _request: &Value) -> Result<Value> {
    bail!("execution hosts require Unix domain socket support")
}

pub fn decode_request(body: &[u8]) -> Result<Value> {
    ensure!(
        body.len() <= MAX_REQUEST_BYTES,
        "execution request exceeds 4 MiB"
    );
    let value: Value = serde_json::from_slice(body)?;
    if !value.is_object() || !value["method"].is_string() || !value["params"].is_object() {
        bail!("execution request must contain method and params");
    }
    Ok(value)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::os::unix::net::UnixListener;

    #[test]
    fn public_origin_requires_an_exact_explicit_https_origin() {
        assert!(allows_public_origin(
            "https://host.example",
            Some("https://host.example/")
        ));
        assert!(!allows_public_origin(
            "https://other.example",
            Some("https://host.example")
        ));
        assert!(!allows_public_origin("https://host.example", None));
        assert!(!allows_public_origin(
            "http://host.example",
            Some("http://host.example")
        ));
        assert!(!allows_public_origin(
            "https://host.example",
            Some("https://user@host.example")
        ));
        assert!(!allows_public_origin(
            "https://host.example",
            Some("https://host.example/path")
        ));
    }

    fn private_root() -> (tempfile::TempDir, Paths) {
        let temp = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        let directory = paths.state.join("execution");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        (temp, paths)
    }

    #[test]
    fn execution_pairing_is_separate_and_requires_private_token() {
        let (_temp, paths) = private_root();
        let path = paths.state.join("execution/control-token");
        let token = "a".repeat(64);
        std::fs::write(&path, format!("{token}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(authorize(&paths, &token).unwrap());
        assert!(!authorize(&paths, &"b".repeat(64)).unwrap());
        assert!(!authorize(&paths, "history-session-cookie").unwrap());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(authorize(&paths, &token).is_err());
        std::fs::remove_file(&path).unwrap();
        let elsewhere = paths.root.join("elsewhere");
        std::fs::write(&elsewhere, &token).unwrap();
        symlink(elsewhere, path).unwrap();
        assert!(authorize(&paths, &token).is_err());
    }

    #[test]
    fn gateway_preserves_command_identity_and_response() {
        let (_temp, paths) = private_root();
        let path = paths.state.join("execution/control.sock");
        let listener = UnixListener::bind(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let request = serde_json::json!({"id":"request", "method":"conversation.send", "params":{"hostId":"host", "commandId":"stable", "text":"hello"}});
        let expected = request.clone();
        let worker = std::thread::spawn(move || {
            let (mut client, _) = listener.accept().unwrap();
            let mut line = String::new();
            BufReader::new(client.try_clone().unwrap())
                .read_line(&mut line)
                .unwrap();
            assert_eq!(serde_json::from_str::<Value>(&line).unwrap(), expected);
            client
                .write_all(b"{\"id\":\"request\",\"result\":{\"accepted\":true}}\n")
                .unwrap();
        });
        assert_eq!(
            super::request(&paths, &request).unwrap()["result"]["accepted"],
            true
        );
        worker.join().unwrap();
    }
}
