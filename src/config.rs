use crate::embed::ModelChoice;
use anyhow::{Result, anyhow};
use directories::BaseDirs;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Paths {
    pub root: PathBuf,
    pub index: PathBuf,
    pub vectors: PathBuf,
    pub state: PathBuf,
}

impl Paths {
    pub fn new(root_override: Option<PathBuf>) -> Result<Self> {
        let root = match root_override {
            Some(path) => path,
            None => {
                let base = BaseDirs::new().ok_or_else(|| anyhow!("missing home dir"))?;
                base.home_dir().join(".memex")
            }
        };

        Ok(Self {
            index: root.join("index"),
            vectors: root.join("vectors"),
            state: root.join("state"),
            root,
        })
    }

    pub fn ensure_dirs(&self) -> Result<()> {
        std::fs::create_dir_all(&self.index)?;
        std::fs::create_dir_all(&self.vectors)?;
        std::fs::create_dir_all(&self.state)?;
        Ok(())
    }
}

pub fn default_claude_source() -> PathBuf {
    let home = directories::BaseDirs::new()
        .map(|b| b.home_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("/"));
    home.join(".claude").join("projects")
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct UserConfig {
    pub embeddings: Option<bool>,
    pub auto_index_on_search: Option<bool>,
    /// Embedding model: minilm, bge, nomic, gemma (default), potion
    pub model: Option<String>,
    /// Embedding runtime compute units on macOS: ane, gpu, cpu, all
    pub compute_units: Option<String>,
    /// Scan cache TTL in seconds. If a scan was done within this time,
    /// skip re-scanning on search. Default: 3600 seconds (1 hour).
    pub scan_cache_ttl: Option<u64>,
    /// Background index service mode: "interval" or "continuous".
    pub index_service_mode: Option<String>,
    /// Run background index service continuously (legacy).
    #[serde(alias = "index_service_watch")]
    pub index_service_continuous: Option<bool>,
    /// Background index service interval in seconds (ignored when continuous is true).
    pub index_service_interval: Option<u64>,
    /// Background index service poll interval in seconds.
    #[serde(alias = "index_service_watch_interval")]
    pub index_service_poll_interval: Option<u64>,
    /// Background index service launchd label.
    pub index_service_label: Option<String>,
    /// Background index service stdout log path.
    pub index_service_stdout: Option<PathBuf>,
    /// Background index service stderr log path (macOS).
    pub index_service_stderr: Option<PathBuf>,
    /// Background index service plist path (macOS).
    pub index_service_plist: Option<PathBuf>,
    /// Background index service systemd user directory (Linux).
    pub index_service_systemd_dir: Option<PathBuf>,
    /// Resume command template for Claude sessions.
    pub claude_resume_cmd: Option<String>,
    /// Resume command template for Codex sessions.
    pub codex_resume_cmd: Option<String>,
    /// Resume command template for Opencode sessions.
    pub opencode_resume_cmd: Option<String>,
}

impl UserConfig {
    pub fn load(paths: &Paths) -> Result<Self> {
        let path = paths.root.join("config.toml");
        if !path.exists() {
            return Ok(Self::default());
        }
        let contents = std::fs::read_to_string(path)?;
        let config: UserConfig = toml::from_str(&contents)?;
        Ok(config)
    }

    pub fn embeddings_default(&self) -> bool {
        self.embeddings.unwrap_or(false)
    }

    pub fn auto_index_on_search_default(&self) -> bool {
        self.auto_index_on_search.unwrap_or(true)
    }

    pub fn resolve_model(&self, cli_model: Option<String>) -> Result<ModelChoice> {
        if let Some(model) = cli_model {
            return ModelChoice::parse(&model);
        }
        if let Some(model) = self.model.as_deref() {
            return ModelChoice::parse(model);
        }
        if let Ok(model) = std::env::var("MEMEX_MODEL") {
            return ModelChoice::parse(&model);
        }
        Ok(ModelChoice::default())
    }

    pub fn resolve_compute_units(&self) -> Option<String> {
        if let Some(units) = self.compute_units.as_deref() {
            return Some(units.to_string());
        }
        std::env::var("MEMEX_COMPUTE_UNITS").ok()
    }

    pub fn apply_embed_runtime_env(&self) {
        if let Some(units) = self.resolve_compute_units() {
            unsafe {
                std::env::set_var("MEMEX_COMPUTE_UNITS", units);
            }
        }
    }

    pub fn scan_cache_ttl(&self) -> u64 {
        self.scan_cache_ttl.unwrap_or(3600)
    }

    pub fn index_service_mode(&self) -> Option<&str> {
        self.index_service_mode.as_deref()
    }

    pub fn index_service_continuous_default(&self) -> bool {
        self.index_service_continuous.unwrap_or(false)
    }

    pub fn index_service_interval(&self) -> u64 {
        self.index_service_interval.unwrap_or(3600)
    }

    pub fn index_service_poll_interval(&self) -> u64 {
        self.index_service_poll_interval.unwrap_or(30)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    struct EnvVarGuard {
        prev: Option<String>,
    }

    impl EnvVarGuard {
        fn set(value: Option<&str>) -> Self {
            let prev = std::env::var("MEMEX_COMPUTE_UNITS").ok();
            unsafe {
                match value {
                    Some(v) => std::env::set_var("MEMEX_COMPUTE_UNITS", v),
                    None => std::env::remove_var("MEMEX_COMPUTE_UNITS"),
                }
            }
            Self { prev }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            unsafe {
                match self.prev.as_deref() {
                    Some(v) => std::env::set_var("MEMEX_COMPUTE_UNITS", v),
                    None => std::env::remove_var("MEMEX_COMPUTE_UNITS"),
                }
            }
        }
    }

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("lock MEMEX_COMPUTE_UNITS env")
    }

    #[test]
    fn resolve_compute_units_prefers_config_over_env() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(Some("gpu"));
        let config = UserConfig {
            compute_units: Some("ane".to_string()),
            ..UserConfig::default()
        };
        assert_eq!(config.resolve_compute_units().as_deref(), Some("ane"));
    }

    #[test]
    fn resolve_compute_units_uses_env_fallback() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(Some("cpu"));
        let config = UserConfig::default();
        assert_eq!(config.resolve_compute_units().as_deref(), Some("cpu"));
    }

    #[test]
    fn resolve_compute_units_none_when_unset() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(None);
        let config = UserConfig::default();
        assert_eq!(config.resolve_compute_units(), None);
    }
}
