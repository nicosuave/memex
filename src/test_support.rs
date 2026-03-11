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
