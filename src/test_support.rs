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
    const ROOT_VARS: [&str; 16] = [
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
        "KIMI_CODE_HOME",
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
