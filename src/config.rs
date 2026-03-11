use crate::embed::{EmbedRuntimeConfig, ExecutionProviderChoice, ModelChoice};
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
    /// Execution provider: auto, cpu, coreml, cuda
    pub execution_provider: Option<String>,
    /// CUDA device index when execution_provider is "cuda"
    pub cuda_device_id: Option<i32>,
    /// Additional search paths for CUDA runtime libraries
    pub cuda_library_paths: Option<Vec<PathBuf>>,
    /// Additional search paths for cuDNN runtime libraries
    pub cudnn_library_paths: Option<Vec<PathBuf>>,
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

    pub fn resolve_execution_provider(&self) -> Result<ExecutionProviderChoice> {
        if let Some(provider) = self.execution_provider.as_deref() {
            return ExecutionProviderChoice::parse(provider);
        }
        match std::env::var("MEMEX_EXECUTION_PROVIDER") {
            Ok(provider) => ExecutionProviderChoice::parse(&provider),
            Err(std::env::VarError::NotPresent) => Ok(ExecutionProviderChoice::Auto),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(anyhow!("MEMEX_EXECUTION_PROVIDER is not valid unicode"))
            }
        }
    }

    pub fn resolve_cuda_device_id(&self) -> Result<Option<i32>> {
        if let Some(device_id) = self.cuda_device_id {
            return Ok(Some(device_id));
        }
        match std::env::var("MEMEX_CUDA_DEVICE_ID") {
            Ok(device_id) => {
                let parsed = device_id
                    .parse::<i32>()
                    .map_err(|err| anyhow!("MEMEX_CUDA_DEVICE_ID must be an integer: {err}"))?;
                Ok(Some(parsed))
            }
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(anyhow!("MEMEX_CUDA_DEVICE_ID is not valid unicode"))
            }
        }
    }

    pub fn resolve_cuda_library_paths(&self) -> Result<Vec<PathBuf>> {
        if let Some(paths) = &self.cuda_library_paths {
            return Ok(paths.clone());
        }
        match std::env::var_os("MEMEX_CUDA_LIBRARY_PATHS") {
            Some(paths) => Ok(std::env::split_paths(&paths).collect()),
            None => Ok(Vec::new()),
        }
    }

    pub fn resolve_cudnn_library_paths(&self) -> Result<Vec<PathBuf>> {
        if let Some(paths) = &self.cudnn_library_paths {
            return Ok(paths.clone());
        }
        match std::env::var_os("MEMEX_CUDNN_LIBRARY_PATHS") {
            Some(paths) => Ok(std::env::split_paths(&paths).collect()),
            None => Ok(Vec::new()),
        }
    }

    pub fn resolve_compute_units(&self) -> Option<String> {
        if let Some(units) = self.compute_units.as_deref() {
            return Some(units.to_string());
        }
        std::env::var("MEMEX_COMPUTE_UNITS").ok()
    }

    pub fn resolve_embed_runtime(&self) -> Result<EmbedRuntimeConfig> {
        Ok(EmbedRuntimeConfig {
            execution_provider: self.resolve_execution_provider()?,
            compute_units: self.resolve_compute_units(),
            cuda_device_id: self.resolve_cuda_device_id()?,
            cuda_library_paths: self.resolve_cuda_library_paths()?,
            cudnn_library_paths: self.resolve_cudnn_library_paths()?,
        })
    }

    pub fn apply_embed_runtime_env(&self) -> Result<()> {
        self.resolve_embed_runtime()?.apply_env()?;
        Ok(())
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
    use crate::test_support::{EnvVarGuard, env_lock};

    #[test]
    fn resolve_compute_units_prefers_config_over_env() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("MEMEX_COMPUTE_UNITS", Some("gpu"))]);
        let config = UserConfig {
            compute_units: Some("ane".to_string()),
            ..UserConfig::default()
        };
        assert_eq!(config.resolve_compute_units().as_deref(), Some("ane"));
    }

    #[test]
    fn resolve_compute_units_uses_env_fallback() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("MEMEX_COMPUTE_UNITS", Some("cpu"))]);
        let config = UserConfig::default();
        assert_eq!(config.resolve_compute_units().as_deref(), Some("cpu"));
    }

    #[test]
    fn resolve_compute_units_none_when_unset() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("MEMEX_COMPUTE_UNITS", None)]);
        let config = UserConfig::default();
        assert_eq!(config.resolve_compute_units(), None);
    }

    #[test]
    fn resolve_execution_provider_prefers_config_over_env() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("MEMEX_EXECUTION_PROVIDER", Some("cpu"))]);
        let config = UserConfig {
            execution_provider: Some("cuda".to_string()),
            ..UserConfig::default()
        };
        assert_eq!(
            config
                .resolve_execution_provider()
                .expect("resolve execution provider"),
            ExecutionProviderChoice::Cuda
        );
    }

    #[test]
    fn resolve_execution_provider_uses_env_fallback() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("MEMEX_EXECUTION_PROVIDER", Some("coreml"))]);
        let config = UserConfig::default();
        assert_eq!(
            config
                .resolve_execution_provider()
                .expect("resolve execution provider"),
            ExecutionProviderChoice::CoreML
        );
    }

    #[test]
    fn resolve_execution_provider_defaults_to_auto() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("MEMEX_EXECUTION_PROVIDER", None)]);
        let config = UserConfig::default();
        assert_eq!(
            config
                .resolve_execution_provider()
                .expect("resolve execution provider"),
            ExecutionProviderChoice::Auto
        );
    }

    #[test]
    fn resolve_cuda_device_id_prefers_config_over_env() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("MEMEX_CUDA_DEVICE_ID", Some("1"))]);
        let config = UserConfig {
            cuda_device_id: Some(3),
            ..UserConfig::default()
        };
        assert_eq!(
            config
                .resolve_cuda_device_id()
                .expect("resolve cuda device id"),
            Some(3)
        );
    }

    #[test]
    fn resolve_cuda_device_id_uses_env_fallback() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("MEMEX_CUDA_DEVICE_ID", Some("2"))]);
        let config = UserConfig::default();
        assert_eq!(
            config
                .resolve_cuda_device_id()
                .expect("resolve cuda device id"),
            Some(2)
        );
    }

    #[test]
    fn resolve_cuda_device_id_none_when_unset() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("MEMEX_CUDA_DEVICE_ID", None)]);
        let config = UserConfig::default();
        assert_eq!(
            config
                .resolve_cuda_device_id()
                .expect("resolve cuda device id"),
            None
        );
    }

    #[test]
    fn resolve_cuda_library_paths_prefers_config_over_env() {
        let _guard = env_lock();
        let env_paths = std::env::join_paths(["/env/cuda/lib64"]).expect("join env paths");
        let _env = EnvVarGuard::set_os(&[("MEMEX_CUDA_LIBRARY_PATHS", Some(&env_paths))]);
        let config = UserConfig {
            cuda_library_paths: Some(vec![PathBuf::from("/config/cuda/lib64")]),
            ..UserConfig::default()
        };
        assert_eq!(
            config
                .resolve_cuda_library_paths()
                .expect("resolve cuda library paths"),
            vec![PathBuf::from("/config/cuda/lib64")]
        );
    }

    #[test]
    fn resolve_cuda_library_paths_uses_env_fallback() {
        let _guard = env_lock();
        let env_paths =
            std::env::join_paths(["/env/cuda/lib64", "/env/cuda/extras"]).expect("join env paths");
        let _env = EnvVarGuard::set_os(&[("MEMEX_CUDA_LIBRARY_PATHS", Some(&env_paths))]);
        let config = UserConfig::default();
        assert_eq!(
            config
                .resolve_cuda_library_paths()
                .expect("resolve cuda library paths"),
            vec![
                PathBuf::from("/env/cuda/lib64"),
                PathBuf::from("/env/cuda/extras")
            ]
        );
    }

    #[test]
    fn resolve_cudnn_library_paths_prefers_config_over_env() {
        let _guard = env_lock();
        let env_paths = std::env::join_paths(["/env/cudnn/lib64"]).expect("join env paths");
        let _env = EnvVarGuard::set_os(&[("MEMEX_CUDNN_LIBRARY_PATHS", Some(&env_paths))]);
        let config = UserConfig {
            cudnn_library_paths: Some(vec![PathBuf::from("/config/cudnn/lib64")]),
            ..UserConfig::default()
        };
        assert_eq!(
            config
                .resolve_cudnn_library_paths()
                .expect("resolve cudnn library paths"),
            vec![PathBuf::from("/config/cudnn/lib64")]
        );
    }

    #[test]
    fn resolve_cudnn_library_paths_uses_env_fallback() {
        let _guard = env_lock();
        let env_paths = std::env::join_paths(["/env/cudnn/lib64", "/env/cudnn/extras"])
            .expect("join env paths");
        let _env = EnvVarGuard::set_os(&[("MEMEX_CUDNN_LIBRARY_PATHS", Some(&env_paths))]);
        let config = UserConfig::default();
        assert_eq!(
            config
                .resolve_cudnn_library_paths()
                .expect("resolve cudnn library paths"),
            vec![
                PathBuf::from("/env/cudnn/lib64"),
                PathBuf::from("/env/cudnn/extras")
            ]
        );
    }
}
