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
    /// Embedding model: minilm, bge, nomic, gemma (default)
    pub model: Option<String>,
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
        self.model.as_deref()
    }
}
