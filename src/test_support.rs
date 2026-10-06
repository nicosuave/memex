use std::ffi::{OsStr, OsString};
use std::sync::{Mutex, MutexGuard, OnceLock};

pub fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .expect("lock test env")
}

pub struct EnvVarGuard {
    prev: Vec<(&'static str, Option<OsString>)>,
}

impl EnvVarGuard {
    pub fn set(vars: &[(&'static str, Option<&str>)]) -> Self {
        let vars = vars
            .iter()
            .map(|(key, value)| (*key, value.map(OsStr::new)))
            .collect::<Vec<_>>();
        Self::set_os(&vars)
    }

    pub fn set_os(vars: &[(&'static str, Option<&OsStr>)]) -> Self {
        let prev = vars
            .iter()
            .map(|(key, _)| (*key, std::env::var_os(key)))
            .collect::<Vec<_>>();
        unsafe {
            for (key, value) in vars {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
        Self { prev }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        unsafe {
            for (key, value) in &self.prev {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

/// Pin `HOME` and every source's storage-root environment variable to
/// directories inside `base`, so tests that resolve session cwds or state
/// stores never depend on the machine they run on. Hold `env_lock()` for the
/// duration of the test alongside the returned guard.
pub fn pin_source_roots(base: &std::path::Path) -> EnvVarGuard {
    const ROOT_VARS: [&str; 15] = [
        "HOME",
        "CLAUDE_CONFIG_DIR",
        "CODEX_HOME",
        "OPENCODE_DATA_DIR",
        "PI_CODING_AGENT_DIR",
        "PI_CODING_AGENT_SESSION_DIR",
        "XDG_DATA_HOME",
        "OPENCLAW_STATE_DIR",
        "COPILOT_HOME",
        "GROK_HOME",
        "HERMES_PROFILE_ROOTS",
        "JCODE_SESSIONS_DIR",
        "MUSE_SESSIONS_DIR",
        "ANTIGRAVITY_HOME",
        "MEMEX_BOB_DB",
    ];
    let values: Vec<Option<OsString>> = ROOT_VARS
        .iter()
        .map(|name| {
            // Bob's root is the parent of its database file, so give it a
            // file-shaped path like a real configuration would.
            let path = if *name == "MEMEX_BOB_DB" {
                base.join(name).join("bob.db")
            } else {
                base.join(name)
            };
            Some(OsString::from(path))
        })
        .collect();
    let vars: Vec<(&'static str, Option<&OsStr>)> = ROOT_VARS
        .iter()
        .zip(values.iter())
        .map(|(name, value)| (*name, value.as_deref()))
        .collect();
    EnvVarGuard::set_os(&vars)
}

/// Local HTTP server and fixtures for the remote model client tests.
pub mod remote_server {
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    use anyhow::{Result, anyhow, bail};
    use serde_json::Value;
    use tiny_http::{Header, Response, Server};

    use crate::remote_http::RemoteEndpoint;

    pub const KEY: &str = "sk-memex-SECRET-7f3a9c41";

    pub struct Captured {
        pub path: String,
        pub auth: Option<String>,
        pub body: Value,
    }

    pub struct Reply {
        pub status: u16,
        pub body: String,
        pub retry_after: Option<&'static str>,
    }

    pub fn reply(status: u16, body: Value) -> Reply {
        Reply {
            status,
            body: body.to_string(),
            retry_after: None,
        }
    }

    /// Serves requests until 500ms pass without one, returning what was received.
    ///
    /// The returned base URL is `http://127.0.0.1:{port}/v1`.
    pub fn serve<F>(mut handler: F) -> Result<(String, JoinHandle<Vec<Captured>>)>
    where
        F: FnMut(usize, &Value) -> Reply + Send + 'static,
    {
        let server = Server::http("127.0.0.1:0").map_err(|e| anyhow!("bind: {e}"))?;
        let addr = server
            .server_addr()
            .to_ip()
            .ok_or_else(|| anyhow!("no ip address"))?;
        let handle = thread::spawn(move || {
            let mut captured = Vec::new();
            while let Ok(Some(mut request)) = server.recv_timeout(Duration::from_millis(500)) {
                let mut raw = String::new();
                let _ = request.as_reader().read_to_string(&mut raw);
                let body: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
                let auth = request
                    .headers()
                    .iter()
                    .find(|h| h.field.equiv("Authorization"))
                    .map(|h| h.value.as_str().to_owned());
                let out = handler(captured.len(), &body);
                let mut response = Response::from_string(out.body).with_status_code(out.status);
                if let Ok(h) = Header::from_bytes("Content-Type", "application/json") {
                    response = response.with_header(h);
                }
                if let Some(value) = out.retry_after
                    && let Ok(h) = Header::from_bytes("Retry-After", value)
                {
                    response = response.with_header(h);
                }
                captured.push(Captured {
                    path: request.url().to_owned(),
                    auth,
                    body,
                });
                let _ = request.respond(response);
            }
            captured
        });
        Ok((format!("http://{addr}/v1"), handle))
    }

    pub fn join(handle: JoinHandle<Vec<Captured>>) -> Result<Vec<Captured>> {
        handle.join().map_err(|_| anyhow!("server thread panicked"))
    }

    pub fn endpoint(base: &str, key: Option<&str>, max_retries: u32) -> RemoteEndpoint {
        RemoteEndpoint {
            base_url: base.to_owned(),
            api_key: key.map(str::to_owned),
            timeout: Duration::from_secs(5),
            max_retries,
        }
    }

    pub fn expect_err<T>(result: Result<T>) -> Result<String> {
        match result {
            Ok(_) => bail!("expected an error"),
            Err(e) => Ok(format!("{e:#}")),
        }
    }
}
