use crate::embed::{
    EmbedRuntimeConfig, ExecutionProviderChoice, LocalModel, ModelChoice, RemoteEmbedConfig,
};
use crate::remote_embed::{MAX_BATCH_SIZE, MAX_DIMENSIONS, RemoteEndpoint};
use anyhow::{Result, anyhow};
use directories::BaseDirs;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

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
        let mut roots: Vec<_> = value
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
        let mut seen = HashSet::new();
        roots.retain(|root| seen.insert(root.clone()));
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

const DEFAULT_EMBEDDING_TIMEOUT_SECS: u64 = 60;
const MAX_EMBEDDING_TIMEOUT_SECS: u64 = 600;
const DEFAULT_EMBEDDING_MAX_RETRIES: u32 = 3;
const MAX_EMBEDDING_MAX_RETRIES: u32 = 10;
/// Key variables consulted, in order, when neither `embedding_api_key` nor
/// `embedding_api_key_env` is set.
const DEFAULT_EMBEDDING_API_KEY_ENVS: [&str; 2] = ["MEMEX_EMBEDDING_API_KEY", "OPENAI_API_KEY"];

/// Value of the `embeddings` key: `true`/`"local"`, `"remote"`, or `false`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EmbeddingsMode {
    #[default]
    Off,
    Local,
    Remote,
}

impl EmbeddingsMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

impl<'de> Deserialize<'de> for EmbeddingsMode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct ModeVisitor;

        impl serde::de::Visitor<'_> for ModeVisitor {
            type Value = EmbeddingsMode;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("true, false, \"local\", or \"remote\"")
            }

            fn visit_bool<E: serde::de::Error>(
                self,
                value: bool,
            ) -> std::result::Result<Self::Value, E> {
                Ok(if value {
                    EmbeddingsMode::Local
                } else {
                    EmbeddingsMode::Off
                })
            }

            fn visit_str<E: serde::de::Error>(
                self,
                value: &str,
            ) -> std::result::Result<Self::Value, E> {
                match value {
                    "local" => Ok(EmbeddingsMode::Local),
                    "remote" => Ok(EmbeddingsMode::Remote),
                    other => Err(E::invalid_value(serde::de::Unexpected::Str(other), &self)),
                }
            }
        }

        deserializer.deserialize_any(ModeVisitor)
    }
}

impl Serialize for EmbeddingsMode {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Off => serializer.serialize_bool(false),
            Self::Local => serializer.serialize_bool(true),
            Self::Remote => serializer.serialize_str("remote"),
        }
    }
}

/// A configured credential whose `Debug` output never contains the value.
#[derive(Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct SecretString(String);

impl SecretString {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
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
    /// Embedding mode: true or "local", "remote", or false (default).
    pub embeddings: Option<EmbeddingsMode>,
    pub auto_index_on_search: Option<bool>,
    /// Index plaintext model reasoning. Encrypted/redacted reasoning is always excluded.
    pub include_reasoning: Option<bool>,
    /// Reconstruct token usage from local agent logs (disabled by default).
    pub token_usage: Option<bool>,
    /// Embedding model. Local: minilm, bge, nomic, gemma (default), potion, or a
    /// fastembed model name. Remote: the server's model name, passed verbatim.
    pub model: Option<String>,
    /// Inputs per embedding call and per vector flush, 1..=2048 (default 64).
    pub embedding_batch_size: Option<usize>,
    /// Remote output size, 1..=65536, sent as the API `dimensions` parameter.
    pub embedding_dimensions: Option<usize>,
    /// Base URL of the OpenAI-compatible API; required for remote embeddings.
    pub embedding_base_url: Option<String>,
    /// Environment variable holding the remote API key.
    pub embedding_api_key_env: Option<String>,
    /// Literal remote API key; takes precedence over `embedding_api_key_env`.
    pub embedding_api_key: Option<SecretString>,
    /// Per-attempt remote request timeout in seconds, 1..=600 (default 60).
    pub embedding_timeout_secs: Option<u64>,
    /// Retries for transient remote failures, 0..=10 (default 3).
    pub embedding_max_retries: Option<u32>,
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
    /// Refresh strategy for the continuous background service: "events" or "poll".
    pub index_service_watch_mode: Option<String>,
    /// Full-resync interval in seconds for events mode (the missed-event backstop).
    /// Falls back to `index_service_poll_interval` when unset.
    pub index_service_resync_interval: Option<u64>,
    /// Serve the local Web UI from the continuous background index service.
    pub index_service_web_ui: Option<bool>,
    /// Serve MCP from the continuous background index service.
    pub index_service_mcp: Option<bool>,
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
    /// Resume command template for Jcode sessions.
    pub jcode_resume_cmd: Option<String>,
    /// Resume command template for Muse sessions.
    pub muse_resume_cmd: Option<String>,
    /// Resume command template for Grok sessions.
    pub grok_resume_cmd: Option<String>,
    /// Resume command template for Antigravity sessions.
    pub antigravity_resume_cmd: Option<String>,
    /// Resume command template for IBM Bob tasks.
    pub bob_resume_cmd: Option<String>,
    /// Resume command template for KiloCode CLI sessions.
    pub kilocode_resume_cmd: Option<String>,
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
    /// Shared MCP HTTP configuration for standalone and background service modes.
    #[serde(default)]
    pub mcp: McpConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct McpConfig {
    pub listen: Option<SocketAddr>,
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    pub public_url: Option<String>,
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

    pub fn embeddings_mode(&self) -> EmbeddingsMode {
        self.embeddings.unwrap_or_default()
    }

    pub fn embeddings_default(&self) -> bool {
        self.embeddings_mode() != EmbeddingsMode::Off
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

    /// Resolve the model from the CLI flag, then config, then `MEMEX_MODEL`.
    ///
    /// In remote mode every source names a remote model (an optional `remote:` prefix is
    /// accepted) and there is no default. With embeddings off, a name that is not a local
    /// model resolves to a remote model when it is a valid remote name.
    pub fn resolve_model(&self, cli_model: Option<String>) -> Result<ModelChoice> {
        if self.embeddings_mode() == EmbeddingsMode::Remote {
            let model = cli_model
                .or_else(|| self.model.clone())
                .or_else(|| std::env::var("MEMEX_MODEL").ok())
                .ok_or_else(|| {
                    anyhow!(
                        "embeddings = \"remote\" requires `model` to name the remote embedding model"
                    )
                })?;
            return ModelChoice::remote(model.strip_prefix("remote:").unwrap_or(&model));
        }
        let Some(model) = cli_model
            .or_else(|| self.model.clone())
            .or_else(|| std::env::var("MEMEX_MODEL").ok())
        else {
            return Ok(ModelChoice::default());
        };
        match ModelChoice::parse(&model) {
            // With embeddings off nothing is loaded, so a remote model name left in the
            // config only identifies existing remote vectors.
            Err(error) if self.embeddings_mode() == EmbeddingsMode::Off => {
                ModelChoice::remote(&model).map_err(|_| error)
            }
            result => result,
        }
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

    /// Validate the embedding settings and resolve the runtime for the configured mode.
    ///
    /// Remote mode ignores the ONNX Runtime keys; local mode ignores the remote keys
    /// except `embedding_dimensions`, which is rejected because local models have a
    /// fixed output size.
    pub fn resolve_embed_runtime(&self) -> Result<EmbedRuntimeConfig> {
        let batch_size = self.resolve_embedding_batch_size()?;
        if let Some(dimensions) = self.embedding_dimensions
            && !(1..=MAX_DIMENSIONS).contains(&dimensions)
        {
            return Err(anyhow!(
                "embedding_dimensions must be between 1 and {MAX_DIMENSIONS}, got {dimensions}"
            ));
        }
        if self.embeddings_mode() == EmbeddingsMode::Remote {
            self.resolve_model(None)?;
            return Ok(EmbedRuntimeConfig {
                batch_size,
                remote: Some(self.resolve_remote_embed()?),
                ..EmbedRuntimeConfig::default()
            });
        }
        if self.embeddings_mode() == EmbeddingsMode::Local
            && let Some(dimensions) = self.embedding_dimensions
        {
            return Err(local_dimensions_error(
                &self.resolve_model(None)?,
                dimensions,
            ));
        }
        Ok(EmbedRuntimeConfig {
            execution_provider: self.resolve_execution_provider()?,
            compute_units: self.resolve_compute_units(),
            cuda_device_id: self.resolve_cuda_device_id()?,
            cuda_library_paths: self.resolve_cuda_library_paths()?,
            cudnn_library_paths: self.resolve_cudnn_library_paths()?,
            batch_size,
            remote: None,
        })
    }

    fn resolve_embedding_batch_size(&self) -> Result<Option<usize>> {
        match self.embedding_batch_size {
            Some(size) if !(1..=MAX_BATCH_SIZE).contains(&size) => Err(anyhow!(
                "embedding_batch_size must be between 1 and {MAX_BATCH_SIZE}, got {size}"
            )),
            size => Ok(size),
        }
    }

    fn resolve_remote_embed(&self) -> Result<RemoteEmbedConfig> {
        let base_url = self
            .embedding_base_url
            .as_deref()
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "embeddings = \"remote\" requires embedding_base_url, for example \
                     \"https://api.openai.com/v1\""
                )
            })?;
        let scheme = base_url.split_once("://").map(|(scheme, _)| scheme);
        if !scheme.is_some_and(|scheme| {
            scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
        }) {
            return Err(anyhow!(
                "embedding_base_url must start with http:// or https://"
            ));
        }
        let timeout_secs = self
            .embedding_timeout_secs
            .unwrap_or(DEFAULT_EMBEDDING_TIMEOUT_SECS);
        if !(1..=MAX_EMBEDDING_TIMEOUT_SECS).contains(&timeout_secs) {
            return Err(anyhow!(
                "embedding_timeout_secs must be between 1 and {MAX_EMBEDDING_TIMEOUT_SECS}, got \
                 {timeout_secs}"
            ));
        }
        let max_retries = self
            .embedding_max_retries
            .unwrap_or(DEFAULT_EMBEDDING_MAX_RETRIES);
        if max_retries > MAX_EMBEDDING_MAX_RETRIES {
            return Err(anyhow!(
                "embedding_max_retries must be between 0 and {MAX_EMBEDDING_MAX_RETRIES}, got \
                 {max_retries}"
            ));
        }
        let endpoint = RemoteEndpoint {
            base_url: base_url.to_string(),
            api_key: self.resolve_embedding_api_key()?,
            timeout: Duration::from_secs(timeout_secs),
            max_retries,
        };
        endpoint.validate()?;
        Ok(RemoteEmbedConfig {
            endpoint,
            dimensions: self.embedding_dimensions,
        })
    }

    /// Resolve the remote API key: the literal `embedding_api_key`, else the variable
    /// named by `embedding_api_key_env` (and only that one), else
    /// `MEMEX_EMBEDDING_API_KEY`, then `OPENAI_API_KEY`, for every base URL. Empty values
    /// count as unset.
    pub fn resolve_embedding_api_key(&self) -> Result<Option<String>> {
        if let Some(key) = &self.embedding_api_key
            && !key.expose().is_empty()
        {
            return Ok(Some(key.expose().to_string()));
        }
        if let Some(var) = self.embedding_api_key_env.as_deref() {
            return api_key_from_env(var);
        }
        for var in DEFAULT_EMBEDDING_API_KEY_ENVS {
            if let Some(key) = api_key_from_env(var)? {
                return Ok(Some(key));
            }
        }
        Ok(None)
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

    pub(crate) fn index_service_watch_mode(&self) -> Result<crate::watch::WatchMode> {
        match self.index_service_watch_mode.as_deref() {
            None => Ok(crate::watch::WatchMode::Events),
            Some(mode) => mode.parse(),
        }
    }

    pub fn index_service_resync_interval(&self) -> u64 {
        self.index_service_resync_interval
            .or(self.index_service_poll_interval)
            .unwrap_or(600)
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

fn api_key_from_env(var: &str) -> Result<Option<String>> {
    match std::env::var(var) {
        Ok(key) if key.is_empty() => Ok(None),
        Ok(key) => Ok(Some(key)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(anyhow!("{var} is not valid unicode")),
    }
}

fn local_dimensions_error(model: &ModelChoice, dimensions: usize) -> anyhow::Error {
    let size = match model {
        ModelChoice::Local(LocalModel::Fastembed(_)) => model
            .known_dimensions(&EmbedRuntimeConfig::default())
            .map(|native| format!("always produces {native} dimensions")),
        _ => None,
    }
    .unwrap_or_else(|| "has a fixed output size".to_string());
    anyhow!(
        "embedding_dimensions = {dimensions} applies only to remote embeddings; local model {} {size}",
        model.identity()
    )
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
            Some(" /tmp/claude-one, /tmp/claude-two/projects, /tmp/claude-one/projects ,, "),
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
    fn watch_mode_defaults_to_events_and_rejects_unknown() {
        assert_eq!(
            UserConfig::default()
                .index_service_watch_mode()
                .expect("default watch mode"),
            crate::watch::WatchMode::Events
        );
        let config: UserConfig =
            toml::from_str(r#"index_service_watch_mode = "poll""#).expect("parse config");
        assert_eq!(
            config.index_service_watch_mode().expect("poll watch mode"),
            crate::watch::WatchMode::Poll
        );
        let config: UserConfig =
            toml::from_str(r#"index_service_watch_mode = "fsevents""#).expect("parse config");
        assert!(config.index_service_watch_mode().is_err());
    }

    #[test]
    fn resync_interval_falls_back_to_poll_interval_then_default() {
        assert_eq!(UserConfig::default().index_service_resync_interval(), 600);
        let config: UserConfig =
            toml::from_str("index_service_poll_interval = 30").expect("parse config");
        assert_eq!(config.index_service_resync_interval(), 30);
        let config: UserConfig =
            toml::from_str("index_service_poll_interval = 30\nindex_service_resync_interval = 120")
                .expect("parse config");
        assert_eq!(config.index_service_resync_interval(), 120);
    }

    #[test]
    fn mcp_defaults_preserve_disabled_service_and_empty_options() {
        let config = UserConfig::default();

        assert_eq!(config.index_service_mcp, None);
        assert_eq!(config.mcp.listen, None);
        assert!(config.mcp.allowed_hosts.is_empty());
        assert!(config.mcp.allowed_origins.is_empty());
        assert_eq!(config.mcp.public_url, None);
    }

    #[test]
    fn parses_background_mcp_configuration() {
        let config: UserConfig = toml::from_str(
            r#"
                index_service_mcp = true

                [mcp]
                listen = "127.0.0.1:5363"
                allowed_hosts = ["memex.example", "127.0.0.1:5363"]
                allowed_origins = ["https://chat.example"]
                public_url = "https://memex.example"
            "#,
        )
        .expect("parse MCP config");

        assert_eq!(config.index_service_mcp, Some(true));
        assert_eq!(
            config.mcp.listen,
            Some("127.0.0.1:5363".parse().expect("socket address"))
        );
        assert_eq!(
            config.mcp.allowed_hosts,
            ["memex.example", "127.0.0.1:5363"]
        );
        assert_eq!(config.mcp.allowed_origins, ["https://chat.example"]);
        assert_eq!(
            config.mcp.public_url.as_deref(),
            Some("https://memex.example")
        );
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

    #[test]
    fn embeddings_accepts_bool_or_mode_name() {
        for (value, mode) in [
            ("true", EmbeddingsMode::Local),
            ("false", EmbeddingsMode::Off),
            ("\"local\"", EmbeddingsMode::Local),
            ("\"remote\"", EmbeddingsMode::Remote),
        ] {
            let config: UserConfig =
                toml::from_str(&format!("embeddings = {value}")).expect("parse embeddings");
            assert_eq!(config.embeddings_mode(), mode, "{value}");
            assert_eq!(config.embeddings_default(), mode != EmbeddingsMode::Off);
        }
        assert_eq!(UserConfig::default().embeddings_mode(), EmbeddingsMode::Off);
        assert!(toml::from_str::<UserConfig>(r#"embeddings = "cloud""#).is_err());
        assert!(toml::from_str::<UserConfig>("embeddings = 1").is_err());
    }

    fn remote_config() -> UserConfig {
        toml::from_str(
            r#"
                embeddings = "remote"
                model = "text-embedding-3-small"
                embedding_base_url = "https://api.example.test/v1"
                embedding_api_key = "sk-literal"
                embedding_dimensions = 512
                embedding_batch_size = 16
                execution_provider = "not-a-provider"
            "#,
        )
        .expect("parse remote config")
    }

    #[test]
    fn remote_mode_resolves_endpoint_and_ignores_runtime_keys() {
        let config = remote_config();
        assert_eq!(
            config.resolve_model(None).expect("resolve remote model"),
            ModelChoice::Remote("text-embedding-3-small".to_string())
        );
        let runtime = config
            .resolve_embed_runtime()
            .expect("resolve remote runtime");
        assert_eq!(runtime.batch_size, Some(16));
        assert_eq!(runtime.execution_provider, ExecutionProviderChoice::Auto);
        let remote = runtime.remote.expect("remote endpoint");
        assert_eq!(remote.dimensions, Some(512));
        assert_eq!(remote.endpoint.base_url, "https://api.example.test/v1");
        assert_eq!(remote.endpoint.api_key.as_deref(), Some("sk-literal"));
        assert_eq!(remote.endpoint.timeout, Duration::from_secs(60));
        assert_eq!(remote.endpoint.max_retries, 3);
        assert_eq!(
            config
                .resolve_model(Some("remote:other".to_string()))
                .expect("resolve prefixed remote model"),
            ModelChoice::Remote("other".to_string())
        );
    }

    #[test]
    fn remote_mode_requires_model_and_http_base_url() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("MEMEX_MODEL", None)]);
        let config = UserConfig {
            model: None,
            ..remote_config()
        };
        assert!(
            config
                .resolve_embed_runtime()
                .expect_err("reject remote mode without model")
                .to_string()
                .contains("requires `model`")
        );
        let config = UserConfig {
            embedding_base_url: None,
            ..remote_config()
        };
        assert!(
            config
                .resolve_embed_runtime()
                .expect_err("reject remote mode without base url")
                .to_string()
                .contains("embedding_base_url")
        );
        let config = UserConfig {
            embedding_base_url: Some("ftp://api.example.test".to_string()),
            ..remote_config()
        };
        assert!(
            config
                .resolve_embed_runtime()
                .expect_err("reject non-http base url")
                .to_string()
                .contains("http://")
        );
        let config = UserConfig {
            model: Some(String::new()),
            ..remote_config()
        };
        assert!(config.resolve_embed_runtime().is_err());
    }

    #[test]
    fn embedding_batch_size_and_dimensions_are_validated() {
        for size in [0, MAX_BATCH_SIZE + 1] {
            let config = UserConfig {
                embedding_batch_size: Some(size),
                ..UserConfig::default()
            };
            assert!(
                config
                    .resolve_embed_runtime()
                    .expect_err("reject batch size")
                    .to_string()
                    .contains("embedding_batch_size")
            );
        }
        let config = UserConfig {
            embedding_batch_size: Some(MAX_BATCH_SIZE),
            ..UserConfig::default()
        };
        assert_eq!(
            config
                .resolve_embed_runtime()
                .expect("max batch")
                .batch_size(),
            MAX_BATCH_SIZE
        );
        assert_eq!(
            UserConfig::default()
                .resolve_embed_runtime()
                .expect("default runtime")
                .batch_size(),
            crate::embed::DEFAULT_EMBED_BATCH_SIZE
        );
        for dimensions in [0, MAX_DIMENSIONS + 1] {
            let config = UserConfig {
                embedding_dimensions: Some(dimensions),
                ..remote_config()
            };
            assert!(
                config
                    .resolve_embed_runtime()
                    .expect_err("reject dimensions")
                    .to_string()
                    .contains("embedding_dimensions")
            );
        }
    }

    #[test]
    fn remote_timeout_and_retries_are_bounded() {
        for timeout in [0, 601] {
            let config = UserConfig {
                embedding_timeout_secs: Some(timeout),
                ..remote_config()
            };
            let error = config
                .resolve_embed_runtime()
                .expect_err("reject timeout")
                .to_string();
            assert!(
                error.contains("embedding_timeout_secs must be between 1 and 600"),
                "{error}"
            );
        }
        let config = UserConfig {
            embedding_max_retries: Some(11),
            ..remote_config()
        };
        let error = config
            .resolve_embed_runtime()
            .expect_err("reject retries")
            .to_string();
        assert!(
            error.contains("embedding_max_retries must be between 0 and 10"),
            "{error}"
        );
        let config = UserConfig {
            embedding_timeout_secs: Some(600),
            embedding_max_retries: Some(0),
            ..remote_config()
        };
        let endpoint = config
            .resolve_embed_runtime()
            .expect("accept bounds")
            .remote
            .expect("remote endpoint")
            .endpoint;
        assert_eq!(endpoint.timeout, Duration::from_secs(600));
        assert_eq!(endpoint.max_retries, 0);
        let config = UserConfig {
            embedding_timeout_secs: Some(1),
            embedding_max_retries: Some(10),
            ..remote_config()
        };
        assert!(config.resolve_embed_runtime().is_ok());
    }

    #[test]
    fn unknown_model_name_is_remote_only_while_embeddings_are_off() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("MEMEX_MODEL", None)]);
        let off: UserConfig = toml::from_str(
            r#"
                embeddings = false
                model = "nomic-embed-text"
            "#,
        )
        .expect("parse config");
        assert_eq!(
            off.resolve_model(None)
                .expect("resolve with embeddings off"),
            ModelChoice::Remote("nomic-embed-text".to_string())
        );
        assert_eq!(
            off.resolve_model(Some("bge".to_string()))
                .expect("local alias"),
            ModelChoice::bge_small()
        );
        assert!(off.resolve_model(Some("a b".to_string())).is_err());
        let unset = UserConfig {
            embeddings: None,
            ..off.clone()
        };
        assert!(unset.resolve_model(None).is_ok());
        {
            let _env = EnvVarGuard::set(&[("MEMEX_MODEL", Some("nomic-embed-text"))]);
            assert_eq!(
                UserConfig::default()
                    .resolve_model(None)
                    .expect("resolve env model"),
                ModelChoice::Remote("nomic-embed-text".to_string())
            );
        }

        let local = UserConfig {
            embeddings: Some(EmbeddingsMode::Local),
            ..off
        };
        let error = local
            .resolve_model(None)
            .expect_err("reject unknown local model")
            .to_string();
        assert!(
            error.starts_with("unknown model 'nomic-embed-text', options: minilm"),
            "{error}"
        );
    }

    #[test]
    fn remote_model_name_rejects_whitespace_and_control_characters() {
        for model in ["a b", "remote:a\nb", "a\tb"] {
            let config = UserConfig {
                model: Some(model.to_string()),
                ..remote_config()
            };
            assert!(config.resolve_model(None).is_err(), "{model:?}");
        }
    }

    #[test]
    fn embedding_dimensions_in_local_mode_names_native_size() {
        let config: UserConfig = toml::from_str(
            r#"
                embeddings = true
                model = "bge"
                embedding_dimensions = 256
            "#,
        )
        .expect("parse local config");
        let error = config
            .resolve_embed_runtime()
            .expect_err("reject local dimensions")
            .to_string();
        assert!(
            error.contains("applies only to remote embeddings"),
            "{error}"
        );
        assert!(
            error.contains("local model bge always produces 384 dimensions"),
            "{error}"
        );
    }

    #[test]
    fn local_mode_ignores_remote_only_keys() {
        let _guard = env_lock();
        let config: UserConfig = toml::from_str(
            r#"
                embeddings = "local"
                model = "minilm"
                embedding_base_url = "not a url"
                embedding_timeout_secs = 0
            "#,
        )
        .expect("parse local config");
        let runtime = config
            .resolve_embed_runtime()
            .expect("resolve local runtime");
        assert_eq!(runtime.remote, None);
        assert_eq!(
            config.resolve_model(None).expect("resolve local model"),
            ModelChoice::minilm()
        );
    }

    #[test]
    fn embedding_api_key_lookup_precedence() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[
            ("MEMEX_EMBEDDING_API_KEY", Some("memex-key")),
            ("OPENAI_API_KEY", Some("openai-key")),
            ("MEMEX_TEST_CUSTOM_KEY", Some("custom-key")),
            ("MEMEX_TEST_MISSING_KEY", None),
        ]);
        let literal = UserConfig {
            embedding_api_key: Some(SecretString("literal-key".to_string())),
            embedding_api_key_env: Some("MEMEX_TEST_CUSTOM_KEY".to_string()),
            ..UserConfig::default()
        };
        assert_eq!(
            literal
                .resolve_embedding_api_key()
                .expect("literal key")
                .as_deref(),
            Some("literal-key")
        );
        let named = UserConfig {
            embedding_api_key_env: Some("MEMEX_TEST_CUSTOM_KEY".to_string()),
            ..UserConfig::default()
        };
        assert_eq!(
            named
                .resolve_embedding_api_key()
                .expect("named key")
                .as_deref(),
            Some("custom-key")
        );
        let missing = UserConfig {
            embedding_api_key_env: Some("MEMEX_TEST_MISSING_KEY".to_string()),
            ..UserConfig::default()
        };
        assert_eq!(
            missing
                .resolve_embedding_api_key()
                .expect("missing named key"),
            None
        );
        assert_eq!(
            UserConfig::default()
                .resolve_embedding_api_key()
                .expect("memex key")
                .as_deref(),
            Some("memex-key")
        );
        let hosts = [
            Some("https://api.openai.com/v1"),
            Some("https://embeddings.example.test/v1"),
            Some("http://127.0.0.1:8080/v1"),
            None,
        ]
        .map(|base_url| UserConfig {
            embedding_base_url: base_url.map(str::to_string),
            ..UserConfig::default()
        });
        for config in &hosts {
            assert_eq!(
                config
                    .resolve_embedding_api_key()
                    .expect("memex key wins")
                    .as_deref(),
                Some("memex-key"),
                "{:?}",
                config.embedding_base_url
            );
        }
        let _env = EnvVarGuard::set(&[("MEMEX_EMBEDDING_API_KEY", None)]);
        for config in &hosts {
            assert_eq!(
                config
                    .resolve_embedding_api_key()
                    .expect("openai key")
                    .as_deref(),
                Some("openai-key"),
                "{:?}",
                config.embedding_base_url
            );
        }
        let _env = EnvVarGuard::set(&[("OPENAI_API_KEY", Some(""))]);
        for config in &hosts {
            assert_eq!(
                config.resolve_embedding_api_key().expect("no key"),
                None,
                "{:?}",
                config.embedding_base_url
            );
        }
    }

    #[test]
    fn debug_output_never_contains_the_api_key() {
        let config = remote_config();
        assert!(!format!("{config:?}").contains("sk-literal"));
        let runtime = config
            .resolve_embed_runtime()
            .expect("resolve remote runtime");
        assert!(!format!("{runtime:?}").contains("sk-literal"));
    }
}
