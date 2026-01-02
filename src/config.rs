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

#[derive(Debug, Clone, Default, Deserialize)]
pub struct UserConfig {
    pub embeddings: Option<bool>,
    pub auto_index_on_search: Option<bool>,
    /// Embedding model: minilm, bge, nomic, gemma, potion (default)
    pub model: Option<String>,
    /// Scan cache TTL in seconds. If a scan was done within this time,
    /// skip re-scanning on search. Default: 3600 seconds (1 hour).
    pub scan_cache_ttl: Option<u64>,
    /// Run background index service in watch mode (continuous).
    pub index_service_watch: Option<bool>,
    /// Background index service interval in seconds (ignored when watch is true).
    pub index_service_interval: Option<u64>,
    /// Background index service watch interval in seconds.
    pub index_service_watch_interval: Option<u64>,
    /// Background index service launchd label.
    pub index_service_label: Option<String>,
    /// Background index service stdout log path.
    pub index_service_stdout: Option<PathBuf>,
    /// Background index service stderr log path.
    pub index_service_stderr: Option<PathBuf>,
    /// Background index service plist path.
    pub index_service_plist: Option<PathBuf>,
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
        self.embeddings.unwrap_or(true)
    }

    pub fn auto_index_on_search_default(&self) -> bool {
        self.auto_index_on_search.unwrap_or(true)
    }

    pub fn model(&self) -> Option<&str> {
        Some(self.model.as_deref().unwrap_or("potion"))
    }

    pub fn scan_cache_ttl(&self) -> u64 {
        self.scan_cache_ttl.unwrap_or(3600)
    }

    pub fn index_service_watch_default(&self) -> bool {
        self.index_service_watch.unwrap_or(false)
    }

    pub fn index_service_interval(&self) -> u64 {
        self.index_service_interval.unwrap_or(3600)
    }

    pub fn index_service_watch_interval(&self) -> u64 {
        self.index_service_watch_interval.unwrap_or(30)
    }
}
