use crate::embed::{EmbedRuntimeConfig, ExecutionProviderChoice, ModelChoice};
use anyhow::{Result, anyhow};
use directories::BaseDirs;
use serde::{Deserialize, Serialize};
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

pub fn default_claude_sources() -> Vec<PathBuf> {
    if let Some(value) = std::env::var_os("CLAUDE_CONFIG_DIR") {
        let roots: Vec<_> = value
            .to_string_lossy()
            .split(',')
            .filter_map(|value| {
                let value = value.trim();
                if value.is_empty() {
                    None
                } else {
                    let root = PathBuf::from(value);
                    Some(
                        if root.file_name().and_then(|name| name.to_str()) == Some("projects") {
                            root
                        } else {
                            root.join("projects")
                        },
                    )
                }
            })
            .collect();
        if !roots.is_empty() {
            return roots;
        }
    }

    let home = directories::BaseDirs::new()
        .map(|b| b.home_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("/"));
    vec![
        home.join(".claude").join("projects"),
        home.join(".config").join("claude").join("projects"),
    ]
}

pub fn default_claude_source() -> PathBuf {
    default_claude_sources()
        .into_iter()
        .next()
        .expect("default Claude sources are never empty")
}

pub const DEFAULT_MAX_INDEXED_TOOL_INPUT_BYTES: usize = 64 * 1024;
pub const DEFAULT_MAX_INDEXED_TOOL_OUTPUT_BYTES: usize = 256 * 1024;
const MIN_INDEXED_TOOL_CONTENT_BYTES: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedToolContentLimits {
    pub input_bytes: usize,
    pub output_bytes: usize,
}

impl Default for IndexedToolContentLimits {
    fn default() -> Self {
        Self {
            input_bytes: DEFAULT_MAX_INDEXED_TOOL_INPUT_BYTES,
            output_bytes: DEFAULT_MAX_INDEXED_TOOL_OUTPUT_BYTES,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct UserConfig {
    pub embeddings: Option<bool>,
    pub auto_index_on_search: Option<bool>,
    /// Index plaintext model reasoning. Encrypted/redacted reasoning is always excluded.
    pub include_reasoning: Option<bool>,
    /// Reconstruct token usage from local agent logs (disabled by default).
    pub token_usage: Option<bool>,
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
    /// Maximum indexed bytes for tool-call input.
    pub max_indexed_tool_input_bytes: Option<usize>,
    /// Maximum indexed bytes for tool-call output.
    pub max_indexed_tool_output_bytes: Option<usize>,
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
    /// Serve the local Web UI from the continuous background index service.
    pub index_service_web_ui: Option<bool>,
    /// Address and port for the background Web UI.
    pub index_service_web_listen: Option<String>,
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
    /// Resume command template for Cursor sessions.
    pub cursor_resume_cmd: Option<String>,
    /// Resume command template for Pi sessions.
    pub pi_resume_cmd: Option<String>,
    /// Resume command template for Oh My Pi sessions.
    pub omp_resume_cmd: Option<String>,
    /// Resume command template for GitHub Copilot CLI sessions.
    pub copilot_resume_cmd: Option<String>,
    /// Resume command template for Grok sessions.
    pub grok_resume_cmd: Option<String>,
    /// How resume behaves inside a herdr pane: "tab" (default), "split", or "off".
    pub herdr_resume: Option<String>,
    /// Glob patterns matched against transcript source paths; matched files are
    /// never indexed. Applied during source discovery, not at query time.
    pub exclude_paths: Option<Vec<String>>,
    /// Multi-machine search and control defaults.
    #[serde(default)]
    pub multi_machine: MultiMachineConfig,
    /// Other machines whose indexes can be queried through a backend.
    #[serde(default)]
    pub machines: Vec<MachineConfig>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct MultiMachineConfig {
    /// Machines searched when --machine is not supplied. "local" is the current machine.
    #[serde(default)]
    pub default: Vec<String>,
    /// Per-machine SSH request timeout.
    pub timeout_seconds: Option<u64>,
}

impl MultiMachineConfig {
    pub fn timeout_seconds(&self) -> u64 {
        self.timeout_seconds.unwrap_or(10).max(1)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MachineConfig {
    pub id: String,
    pub label: Option<String>,
    /// Backwards-compatible shorthand for an SSH control transport.
    pub ssh: Option<String>,
    /// Executable used by the SSH RPC command. Defaults to "memex".
    pub command: Option<String>,
    pub enabled: Option<bool>,
    pub control: Option<ControlConfig>,
    pub index: Option<IndexBackendConfig>,
}

impl MachineConfig {
    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(true)
    }

    pub fn ssh_target(&self) -> Option<&str> {
        self.control
            .as_ref()
            .filter(|control| control.kind == "ssh")
            .map(|control| control.host.as_str())
            .or(self.ssh.as_deref())
    }

    pub fn command(&self) -> &str {
        self.command.as_deref().unwrap_or("memex")
    }

    pub fn uses_remote_index(&self) -> bool {
        self.index
            .as_ref()
            .map(|index| index.kind == "remote")
            .unwrap_or(true)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ControlConfig {
    #[serde(rename = "type")]
    pub kind: String,
    pub host: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IndexBackendConfig {
    #[serde(rename = "type")]
    pub kind: String,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub cache: Option<PathBuf>,
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

    pub fn include_reasoning_default(&self) -> bool {
        self.include_reasoning.unwrap_or(false)
    }

    pub fn token_usage_enabled(&self) -> bool {
        self.token_usage.unwrap_or(false)
    }

    /// Exclusion patterns from config, with a leading `~/` expanded to the
    /// user's home directory so patterns match absolute transcript paths.
    pub fn exclude_path_patterns(&self) -> Vec<String> {
        expand_exclude_patterns(self.exclude_paths.clone().unwrap_or_default())
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

    pub fn indexed_tool_content_limits(&self) -> Result<IndexedToolContentLimits> {
        Ok(IndexedToolContentLimits {
            input_bytes: indexed_tool_content_limit(
                self.max_indexed_tool_input_bytes,
                DEFAULT_MAX_INDEXED_TOOL_INPUT_BYTES,
                "max_indexed_tool_input_bytes",
            )?,
            output_bytes: indexed_tool_content_limit(
                self.max_indexed_tool_output_bytes,
                DEFAULT_MAX_INDEXED_TOOL_OUTPUT_BYTES,
                "max_indexed_tool_output_bytes",
            )?,
        })
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

    pub fn index_service_web_ui_default(&self) -> bool {
        self.index_service_web_ui.unwrap_or(false)
    }
}

/// Expand a leading `~/` (or bare `~`) in exclusion patterns to the current
/// home directory. Other patterns are returned unchanged.
pub fn expand_exclude_patterns(patterns: Vec<String>) -> Vec<String> {
    let home = directories::BaseDirs::new().map(|b| b.home_dir().to_path_buf());
    patterns
        .into_iter()
        .map(|pattern| {
            if pattern == "~" {
                home.as_ref()
                    .map(|h| h.to_string_lossy().to_string())
                    .unwrap_or(pattern)
            } else if let Some(rest) = pattern.strip_prefix("~/") {
                match &home {
                    Some(h) => format!("{}/{rest}", h.to_string_lossy()),
                    None => pattern,
                }
            } else {
                pattern
            }
        })
        .collect()
}

fn indexed_tool_content_limit(value: Option<usize>, default: usize, key: &str) -> Result<usize> {
    let value = value.unwrap_or(default);
    if value < MIN_INDEXED_TOOL_CONTENT_BYTES {
        return Err(anyhow!(
            "{key} must be at least {MIN_INDEXED_TOOL_CONTENT_BYTES} bytes"
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{EnvVarGuard, env_lock};

    #[test]
    fn claude_sources_honor_config_dir_and_multiple_roots() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[(
            "CLAUDE_CONFIG_DIR",
            Some(" /tmp/claude-one, /tmp/claude-two/projects ,, "),
        )]);

        assert_eq!(
            default_claude_sources(),
            vec![
                PathBuf::from("/tmp/claude-one/projects"),
                PathBuf::from("/tmp/claude-two/projects"),
            ]
        );
        assert_eq!(
            default_claude_source(),
            PathBuf::from("/tmp/claude-one/projects")
        );
    }

    #[test]
    fn exclude_paths_parse_and_expand_tilde() {
        let config: UserConfig = toml::from_str(
            r#"
                exclude_paths = ["~/.claude/projects/*-client-*", "/opt/work/**"]
            "#,
        )
        .expect("parse config");
        let patterns = config.exclude_path_patterns();
        assert_eq!(patterns.len(), 2);
        let home = directories::BaseDirs::new()
            .expect("home dir")
            .home_dir()
            .to_string_lossy()
            .to_string();
        assert_eq!(patterns[0], format!("{home}/.claude/projects/*-client-*"));
        assert_eq!(patterns[1], "/opt/work/**");
    }

    #[test]
    fn exclude_paths_default_to_empty() {
        assert!(UserConfig::default().exclude_path_patterns().is_empty());
    }

    #[test]
    fn token_usage_is_disabled_by_default() {
        assert!(!UserConfig::default().token_usage_enabled());
    }

    #[test]
    fn parses_ssh_machine_backend_configuration() {
        let config: UserConfig = toml::from_str(
            r#"
                [multi_machine]
                default = ["local", "mini"]
                timeout_seconds = 7

                [[machines]]
                id = "mini"
                label = "Mac mini"

                [machines.control]
                type = "ssh"
                host = "mini.local"

                [machines.index]
                type = "remote"
            "#,
        )
        .expect("parse config");

        assert_eq!(config.multi_machine.default, ["local", "mini"]);
        assert_eq!(config.multi_machine.timeout_seconds(), 7);
        assert_eq!(config.machines[0].ssh_target(), Some("mini.local"));
        assert!(config.machines[0].uses_remote_index());
    }

    #[test]
    fn reasoning_is_opt_in() {
        assert!(!UserConfig::default().include_reasoning_default());
        assert!(
            UserConfig {
                include_reasoning: Some(true),
                ..UserConfig::default()
            }
            .include_reasoning_default()
        );
    }

    #[test]
    fn token_usage_can_be_enabled() {
        let config = UserConfig {
            token_usage: Some(true),
            ..UserConfig::default()
        };

        assert!(config.token_usage_enabled());
    }

    #[test]
    fn indexed_tool_content_limits_use_defaults() {
        assert_eq!(
            UserConfig::default()
                .indexed_tool_content_limits()
                .expect("resolve defaults"),
            IndexedToolContentLimits::default()
        );
    }

    #[test]
    fn indexed_tool_content_limits_allow_per_field_overrides() {
        let config = UserConfig {
            max_indexed_tool_input_bytes: Some(96 * 1024),
            max_indexed_tool_output_bytes: Some(384 * 1024),
            ..UserConfig::default()
        };

        assert_eq!(
            config
                .indexed_tool_content_limits()
                .expect("resolve overrides"),
            IndexedToolContentLimits {
                input_bytes: 96 * 1024,
                output_bytes: 384 * 1024,
            }
        );
    }

    #[test]
    fn indexed_tool_content_limits_reject_too_small_values() {
        let config = UserConfig {
            max_indexed_tool_output_bytes: Some(512),
            ..UserConfig::default()
        };

        assert!(
            config
                .indexed_tool_content_limits()
                .expect_err("reject too-small limit")
                .to_string()
                .contains("max_indexed_tool_output_bytes")
        );
    }

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
