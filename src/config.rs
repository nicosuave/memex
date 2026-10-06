use crate::embed::{
    EmbedRuntimeConfig, ExecutionProviderChoice, LocalModel, ModelChoice, RemoteEmbedConfig,
};
use crate::remote_embed::{MAX_BATCH_SIZE, MAX_DIMENSIONS, RemoteEndpoint};
use crate::remote_http::Purpose;
use crate::remote_rerank::{RemoteRerankConfig, RerankDialect, RerankExtraBody};
use crate::rerank::{
    DEFAULT_RERANK_CANDIDATES, DEFAULT_RERANK_DOC_CHARS, RERANK_CANDIDATES, RERANK_DOC_CHARS,
    RerankEngine, RerankSettings,
};
use anyhow::{Context, Result, anyhow};
use directories::BaseDirs;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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
const DEFAULT_RERANK_TIMEOUT_SECS: u64 = 10;
/// Equal to the rerank time budget, so the budget never cuts a first attempt short.
const MAX_RERANK_TIMEOUT_SECS: u64 = 30;
const DEFAULT_RERANK_MAX_RETRIES: u32 = 1;
const MAX_RERANK_MAX_RETRIES: u32 = 3;
/// Key variable consulted when neither `rerank_api_key` nor `rerank_api_key_env` is set.
const DEFAULT_RERANK_API_KEY_ENV: &str = "MEMEX_RERANK_API_KEY";

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

/// Value of the `rerank` key: `false` (default), `true`/`"local"`, or `"remote"`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RerankMode {
    #[default]
    Off,
    Local,
    Remote,
}

impl RerankMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

impl<'de> Deserialize<'de> for RerankMode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct ModeVisitor;

        impl serde::de::Visitor<'_> for ModeVisitor {
            type Value = RerankMode;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("true, false, \"local\", or \"remote\"")
            }

            fn visit_bool<E: serde::de::Error>(
                self,
                value: bool,
            ) -> std::result::Result<Self::Value, E> {
                Ok(if value {
                    RerankMode::Local
                } else {
                    RerankMode::Off
                })
            }

            fn visit_str<E: serde::de::Error>(
                self,
                value: &str,
            ) -> std::result::Result<Self::Value, E> {
                match value {
                    "local" => Ok(RerankMode::Local),
                    "remote" => Ok(RerankMode::Remote),
                    other => Err(E::invalid_value(serde::de::Unexpected::Str(other), &self)),
                }
            }
        }

        deserializer.deserialize_any(ModeVisitor)
    }
}

impl Serialize for RerankMode {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Off => serializer.serialize_bool(false),
            Self::Local => serializer.serialize_str("local"),
            Self::Remote => serializer.serialize_str("remote"),
        }
    }
}

/// A configured credential or private value whose `Debug` output never contains it.
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
    /// Search reranking: true or "local", "remote", or false (default).
    pub rerank: Option<RerankMode>,
    /// Required when reranking is on. Local: the cross-encoder, jina-turbo, bge-v2-m3,
    /// bge-base, or jina-v2. Remote: the provider's model name, passed verbatim.
    pub rerank_model: Option<String>,
    /// Leading results rescored per search, 5..=100 (default 30).
    pub rerank_candidates: Option<usize>,
    /// Characters of each result sent to the reranker, 200..=8000 (default 1500).
    pub rerank_doc_chars: Option<usize>,
    /// Full URL of the remote rerank endpoint, used verbatim; required for remote
    /// reranking.
    pub rerank_url: Option<String>,
    /// Literal remote rerank API key; takes precedence over `rerank_api_key_env`.
    pub rerank_api_key: Option<SecretString>,
    /// Environment variable holding the remote rerank API key.
    pub rerank_api_key_env: Option<String>,
    /// Per-attempt remote rerank request timeout in seconds, 1..=30 (default 10).
    pub rerank_timeout_secs: Option<u64>,
    /// Retries for transient remote rerank failures, 0..=3 (default 1).
    pub rerank_max_retries: Option<u32>,
    /// Remote request body: "documents" (default) or "texts".
    pub rerank_dialect: Option<RerankDialect>,
    /// JSON object of extra top-level fields for every remote rerank request.
    pub rerank_extra_body: Option<SecretString>,
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
        let Some(contents) = read_config_file(&path)? else {
            return Ok(Self::default());
        };
        Self::parse_file(&contents, &path)
    }

    /// Parse `config.toml` contents.
    ///
    /// Unknown top-level keys are ignored; a near miss of a known key also prints a warning.
    /// Errors name a line and column but never quote the source, which may hold a key.
    pub fn parse(contents: &str) -> Result<Self> {
        Self::parse_file(contents, Path::new("config.toml"))
    }

    fn parse_file(contents: &str, path: &Path) -> Result<Self> {
        let table: toml::Table = toml::from_str(contents)
            .map_err(|error| toml_error(path, contents, error.message(), error.span()))?;
        warn_key_typos(&table, path);
        toml::from_str(contents)
            .map_err(|error| toml_error(path, contents, error.message(), error.span()))
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
            batch_size,
            ..self.resolve_onnx_runtime()?
        })
    }

    /// The ONNX Runtime keys (`execution_provider`, `compute_units`, and the CUDA keys) with
    /// their environment fallbacks, shared by local embedding and local reranking.
    fn resolve_onnx_runtime(&self) -> Result<EmbedRuntimeConfig> {
        Ok(EmbedRuntimeConfig {
            execution_provider: self.resolve_execution_provider()?,
            compute_units: self.resolve_compute_units(),
            cuda_device_id: self.resolve_cuda_device_id()?,
            cuda_library_paths: self.resolve_cuda_library_paths()?,
            cudnn_library_paths: self.resolve_cudnn_library_paths()?,
            batch_size: None,
            remote: None,
        })
    }

    pub fn rerank_mode(&self) -> RerankMode {
        self.rerank.unwrap_or_default()
    }

    /// Validate the reranking keys and resolve them; `None` when reranking is off.
    ///
    /// `rerank_candidates` and `rerank_doc_chars` are checked in every mode. Local
    /// reranking requires a model from `rerank_model`, else `MEMEX_RERANK_MODEL`, runs on
    /// the same ONNX Runtime settings as local embeddings, and ignores the remote keys.
    /// Remote reranking requires `rerank_url`, and `rerank_model` unless the dialect is
    /// `"texts"`; it never reads `MEMEX_RERANK_MODEL` and ignores the ONNX Runtime keys.
    pub fn resolve_rerank(&self) -> Result<Option<RerankSettings>> {
        let candidates = self.rerank_candidates.unwrap_or(DEFAULT_RERANK_CANDIDATES);
        if !RERANK_CANDIDATES.contains(&candidates) {
            return Err(anyhow!(
                "rerank_candidates must be between {} and {}, got {candidates}",
                RERANK_CANDIDATES.start(),
                RERANK_CANDIDATES.end()
            ));
        }
        let doc_chars = self.rerank_doc_chars.unwrap_or(DEFAULT_RERANK_DOC_CHARS);
        if !RERANK_DOC_CHARS.contains(&doc_chars) {
            return Err(anyhow!(
                "rerank_doc_chars must be between {} and {}, got {doc_chars}",
                RERANK_DOC_CHARS.start(),
                RERANK_DOC_CHARS.end()
            ));
        }
        let engine = match self.rerank_mode() {
            RerankMode::Off => return Ok(None),
            RerankMode::Local => {
                let model = match self.rerank_model_name()? {
                    Some((name, source)) => crate::rerank::parse_model(&name)
                        .with_context(|| format!("invalid {source}"))?,
                    None => {
                        return Err(anyhow!(
                            "rerank = \"local\" requires rerank_model, one of: {}",
                            crate::rerank::SUPPORTED_MODEL_NAMES
                        ));
                    }
                };
                RerankEngine::Local {
                    model,
                    runtime: self.resolve_onnx_runtime()?,
                }
            }
            RerankMode::Remote => RerankEngine::Remote(self.resolve_remote_rerank()?),
        };
        Ok(Some(RerankSettings {
            engine,
            candidates,
            doc_chars,
        }))
    }

    /// The configured `rerank_model`, else `MEMEX_RERANK_MODEL`, with the name of its
    /// source for error messages.
    fn rerank_model_name(&self) -> Result<Option<(String, &'static str)>> {
        if let Some(name) = &self.rerank_model {
            return Ok(Some((name.clone(), "rerank_model")));
        }
        match std::env::var("MEMEX_RERANK_MODEL") {
            Ok(name) => Ok(Some((name, "MEMEX_RERANK_MODEL"))),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(anyhow!("MEMEX_RERANK_MODEL is not valid unicode"))
            }
        }
    }

    fn resolve_remote_rerank(&self) -> Result<RemoteRerankConfig> {
        // MEMEX_RERANK_MODEL names a local model, so it never reaches a hosted API.
        let dialect = self.rerank_dialect.unwrap_or_default();
        let model = match (&self.rerank_model, dialect) {
            (Some(name), _) => {
                crate::remote_rerank::validate_model_name(name).context("invalid rerank_model")?;
                Some(name.clone())
            }
            (None, RerankDialect::Texts) => None,
            (None, RerankDialect::Documents) => {
                return Err(anyhow!(
                    "rerank = \"remote\" requires rerank_model to name the provider's rerank \
                     model, unless rerank_dialect = \"texts\""
                ));
            }
        };
        let url = self
            .rerank_url
            .as_deref()
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "rerank = \"remote\" requires rerank_url, the full endpoint URL, for example \
                     \"https://openrouter.ai/api/v1/rerank\""
                )
            })?;
        let timeout_secs = self
            .rerank_timeout_secs
            .unwrap_or(DEFAULT_RERANK_TIMEOUT_SECS);
        if !(1..=MAX_RERANK_TIMEOUT_SECS).contains(&timeout_secs) {
            return Err(anyhow!(
                "rerank_timeout_secs must be between 1 and {MAX_RERANK_TIMEOUT_SECS}, got \
                 {timeout_secs}"
            ));
        }
        let max_retries = self
            .rerank_max_retries
            .unwrap_or(DEFAULT_RERANK_MAX_RETRIES);
        if max_retries > MAX_RERANK_MAX_RETRIES {
            return Err(anyhow!(
                "rerank_max_retries must be between 0 and {MAX_RERANK_MAX_RETRIES}, got \
                 {max_retries}"
            ));
        }
        let endpoint = RemoteEndpoint {
            base_url: url.to_string(),
            api_key: self.resolve_rerank_api_key()?,
            timeout: Duration::from_secs(timeout_secs),
            max_retries,
        };
        endpoint.validate_for(Purpose::Rerank)?;
        let extra_body = match &self.rerank_extra_body {
            Some(source) => {
                RerankExtraBody::parse(source.expose()).context("invalid rerank_extra_body")?
            }
            None => None,
        };
        Ok(RemoteRerankConfig {
            endpoint,
            model,
            dialect,
            extra_body,
        })
    }

    /// Resolve the remote rerank API key: the literal `rerank_api_key`, else the variable
    /// named by `rerank_api_key_env` (and only that one), else `MEMEX_RERANK_API_KEY`.
    /// Empty values count as unset, and no key is allowed.
    pub fn resolve_rerank_api_key(&self) -> Result<Option<String>> {
        if let Some(key) = &self.rerank_api_key
            && !key.expose().is_empty()
        {
            return Ok(Some(key.expose().to_string()));
        }
        api_key_from_env(
            self.rerank_api_key_env
                .as_deref()
                .unwrap_or(DEFAULT_RERANK_API_KEY_ENV),
        )
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

/// Largest `config.toml` accepted, in bytes.
const MAX_CONFIG_BYTES: u64 = 1024 * 1024;

/// Read a config file, following symlinks; `None` when it does not exist.
///
/// Anything other than a regular file of at most [`MAX_CONFIG_BYTES`] is rejected before
/// it is read. On Unix the file is opened non-blocking and inspected through the open
/// handle, so a FIFO never blocks the reader and a swapped path is never read unchecked.
fn read_config_file(path: &Path) -> Result<Option<String>> {
    use std::io::Read;

    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("open {}", path.display())),
    };
    let metadata = file
        .metadata()
        .with_context(|| format!("inspect {}", path.display()))?;
    if !metadata.is_file() {
        return Err(anyhow!("{} is not a regular file", path.display()));
    }
    let too_large = || anyhow!("{} is larger than {MAX_CONFIG_BYTES} bytes", path.display());
    if metadata.len() > MAX_CONFIG_BYTES {
        return Err(too_large());
    }
    let mut contents = String::new();
    file.take(MAX_CONFIG_BYTES + 1)
        .read_to_string(&mut contents)
        .with_context(|| format!("read {}", path.display()))?;
    if contents.len() as u64 > MAX_CONFIG_BYTES {
        return Err(too_large());
    }
    Ok(Some(contents))
}

/// Top-level keys `UserConfig` reads, including serde aliases.
const USER_CONFIG_KEYS: &[&str] = &[
    "embeddings",
    "auto_index_on_search",
    "include_reasoning",
    "token_usage",
    "model",
    "embedding_batch_size",
    "embedding_dimensions",
    "embedding_base_url",
    "embedding_api_key_env",
    "embedding_api_key",
    "embedding_timeout_secs",
    "embedding_max_retries",
    "execution_provider",
    "cuda_device_id",
    "cuda_library_paths",
    "cudnn_library_paths",
    "compute_units",
    "rerank",
    "rerank_model",
    "rerank_candidates",
    "rerank_doc_chars",
    "rerank_url",
    "rerank_api_key",
    "rerank_api_key_env",
    "rerank_timeout_secs",
    "rerank_max_retries",
    "rerank_dialect",
    "rerank_extra_body",
    "scan_cache_ttl",
    "max_indexed_tool_input_bytes",
    "max_indexed_tool_output_bytes",
    "index_service_mode",
    "index_service_continuous",
    "index_service_watch",
    "index_service_interval",
    "index_service_poll_interval",
    "index_service_watch_interval",
    "index_service_watch_mode",
    "index_service_resync_interval",
    "index_service_web_ui",
    "index_service_mcp",
    "index_service_web_listen",
    "index_service_label",
    "index_service_stdout",
    "index_service_stderr",
    "index_service_plist",
    "index_service_systemd_dir",
    "claude_resume_cmd",
    "codex_resume_cmd",
    "opencode_resume_cmd",
    "cursor_resume_cmd",
    "pi_resume_cmd",
    "omp_resume_cmd",
    "copilot_resume_cmd",
    "jcode_resume_cmd",
    "muse_resume_cmd",
    "grok_resume_cmd",
    "antigravity_resume_cmd",
    "bob_resume_cmd",
    "kilocode_resume_cmd",
    "herdr_resume",
    "exclude_paths",
    "multi_machine",
    "machines",
    "mcp",
];

/// Most distinct key typo warnings printed by one process.
const MAX_KEY_TYPO_WARNINGS: usize = 64;

/// Key typo warnings already printed by this process.
static WARNED_KEY_TYPOS: std::sync::LazyLock<std::sync::Mutex<HashSet<String>>> =
    std::sync::LazyLock::new(Default::default);

/// Warnings for unknown top-level keys that are a near miss of a known key, such as
/// `embedding_base_ulr`; other unknown keys are ignored, as is a key that is not a bare
/// key of at most [`MAX_ECHOED_KEY_LEN`] bytes.
fn key_typo_warnings(table: &toml::Table, path: &Path) -> Vec<String> {
    table
        .keys()
        .filter(|key| !USER_CONFIG_KEYS.contains(&key.as_str()) && is_echoable_key(key))
        .filter_map(|key| {
            closest_name(key, USER_CONFIG_KEYS).map(|known| {
                format!(
                    "{}: unknown key `{key}` (did you mean `{known}`?)",
                    path.display()
                )
            })
        })
        .collect()
}

/// Record `warning` in `warned`; `false` when it was printed before or the set is full.
fn admit_key_typo_warning(warned: &mut HashSet<String>, warning: &str) -> bool {
    if warned.contains(warning) || warned.len() >= MAX_KEY_TYPO_WARNINGS {
        return false;
    }
    warned.insert(warning.to_string())
}

/// Print each key typo warning once per process.
fn warn_key_typos(table: &toml::Table, path: &Path) {
    let warnings = key_typo_warnings(table, path);
    if warnings.is_empty() {
        return;
    }
    let mut warned = WARNED_KEY_TYPOS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for warning in warnings {
        if admit_key_typo_warning(&mut warned, &warning) {
            eprintln!("warning: {warning}");
        }
    }
}

/// A TOML error reduced to its message and position, so the offending source line,
/// which may hold a secret, never reaches output.
///
/// Type and value errors keep only what was expected, plus the key whose value holds
/// the error; unknown variants keep only the accepted names, and unknown fields name
/// the offending key only when it is a near miss of an accepted one. No offending value
/// is ever included.
fn toml_error(
    path: &Path,
    contents: &str,
    message: &str,
    span: Option<std::ops::Range<usize>>,
) -> anyhow::Error {
    let before = span.as_ref().and_then(|span| contents.get(..span.start));
    let location = before
        .map(|before| {
            let line = before.matches('\n').count() + 1;
            let column = before
                .rsplit('\n')
                .next()
                .map_or(0, |line| line.chars().count())
                + 1;
            format!(" at line {line}, column {column}")
        })
        .unwrap_or_default();
    let key = span.and_then(|span| key_at(contents, span.start));
    anyhow!(
        "{}: invalid TOML{location}: {}",
        path.display(),
        scrub_toml_message(message.trim_end(), key.as_deref())
    )
}

/// Separator serde places before the description of what a value should be.
const EXPECTED_SEPARATOR: &str = ", expected ";

/// Largest edit distance at which an unknown field is named as a typo of a valid one.
const MAX_FIELD_TYPO_DISTANCE: usize = 2;

/// Drop the offending value or token from a serde message; parser messages pass through.
fn scrub_toml_message(message: &str, key: Option<&str>) -> String {
    let key = key.map(|key| format!("{key}: ")).unwrap_or_default();
    for (prefix, summary) in [
        ("invalid type: ", "wrong value type"),
        ("invalid value: ", "invalid value"),
    ] {
        if message.starts_with(prefix) {
            // What follows the last separator is the static expectation of the visitor.
            return match message.rsplit_once(EXPECTED_SEPARATOR) {
                Some((_, expected)) if !expected.contains('`') => {
                    format!("{key}{summary}, expected {expected}")
                }
                _ => format!("{key}{summary}"),
            };
        }
    }
    for (prefix, summary, none) in [
        (
            "unknown variant ",
            "unknown variant",
            "there are no variants",
        ),
        ("unknown field ", "unknown field", "there are no fields"),
    ] {
        if !message.starts_with(prefix) {
            continue;
        }
        if message.ends_with(none) {
            return summary.to_string();
        }
        let Some((offending, names)) = message
            .rsplit_once(EXPECTED_SEPARATOR)
            .and_then(|(offending, expected)| Some((offending, static_names(expected)?)))
        else {
            return summary.to_string();
        };
        // Only a key close to a static name is echoed; a value never is.
        if summary == "unknown field"
            && let Some(field) = offending
                .strip_prefix("unknown field `")
                .and_then(|field| field.strip_suffix('`'))
                .filter(|field| is_echoable_key(field))
            && let Some(suggestion) = closest_name(field, &names.names)
        {
            return format!("unknown field `{field}` (did you mean `{suggestion}`?)");
        }
        return format!("{summary}, expected {names}");
    }
    redact_quoted_segments(message)
}

/// Replace each backtick-delimited segment of a parser message with `…` unless it is
/// an echoable key or a short run of TOML punctuation such as `` `"` ``; an unpaired
/// backtick hides the rest of the message.
fn redact_quoted_segments(message: &str) -> String {
    let mut output = String::with_capacity(message.len());
    let mut rest = message;
    while let Some((before, after)) = rest.split_once('`') {
        output.push_str(before);
        let Some((segment, after)) = after.split_once('`') else {
            output.push('…');
            return output;
        };
        let punctuation =
            segment.chars().count() <= 3 && segment.chars().all(|c| "\"'[]{}=#,.".contains(c));
        if !segment.is_empty() && (is_echoable_key(segment) || punctuation) {
            output.push('`');
            output.push_str(segment);
            output.push('`');
        } else {
            output.push('…');
        }
        rest = after;
    }
    output.push_str(rest);
    output
}

/// Accepted names parsed from a serde expectation, kept with its wording.
struct StaticNames<'a> {
    lead: &'static str,
    separator: &'static str,
    names: Vec<&'a str>,
}

impl fmt::Display for StaticNames<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.lead, self.names.join(self.separator))
    }
}

/// Serde's list of accepted names (`` `a` ``, `` `a` or `b` ``, or `` one of `a`, `b`, `c` ``)
/// without the backticks; `None` for any other shape.
fn static_names(expected: &str) -> Option<StaticNames<'_>> {
    let (lead, list, separator) = match expected.strip_prefix("one of ") {
        Some(list) => ("one of ", list, ", "),
        None => ("", expected, " or "),
    };
    let names = list
        .split(separator)
        .map(|name| {
            name.strip_prefix('`')
                .and_then(|name| name.strip_suffix('`'))
                .filter(|name| !name.is_empty() && !name.contains(['`', '"']))
        })
        .collect::<Option<Vec<_>>>()?;
    Some(StaticNames {
        lead,
        separator,
        names,
    })
}

/// The accepted name nearest to `field`, when within [`MAX_FIELD_TYPO_DISTANCE`] edits.
fn closest_name<'a>(field: &str, names: &[&'a str]) -> Option<&'a str> {
    names
        .iter()
        .map(|name| (edit_distance(field, name, MAX_FIELD_TYPO_DISTANCE), *name))
        .filter_map(|(distance, name)| distance.map(|distance| (distance, name)))
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, name)| name)
}

/// Levenshtein distance between `a` and `b` in characters, or `None` above `limit`.
fn edit_distance(a: &str, b: &str, limit: usize) -> Option<usize> {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.len().abs_diff(b.len()) > limit {
        return None;
    }
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    for (i, left) in a.iter().enumerate() {
        let mut current = Vec::with_capacity(b.len() + 1);
        current.push(i + 1);
        for (j, right) in b.iter().enumerate() {
            let substitute = previous.get(j)? + usize::from(left != right);
            let delete = previous.get(j + 1)? + 1;
            let insert = current.get(j)? + 1;
            current.push(substitute.min(delete).min(insert));
        }
        previous = current;
    }
    previous
        .last()
        .copied()
        .filter(|distance| *distance <= limit)
}

/// Longest key name echoed in an error.
const MAX_ECHOED_KEY_LEN: usize = 64;

/// The innermost key whose value contains byte `offset` of `contents`, when it is a
/// bare key; keys come from the parsed document, never from scanning the text.
fn key_at(contents: &str, offset: usize) -> Option<String> {
    let document = toml_edit::ImDocument::parse(contents).ok()?;
    key_in_table(document.as_table(), offset).filter(|key| is_echoable_key(key))
}

/// Whether a key from the config may appear in an error: a bare key of at most
/// [`MAX_ECHOED_KEY_LEN`] bytes, so no control character or long token is printed.
fn is_echoable_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_ECHOED_KEY_LEN
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn key_in_table(table: &toml_edit::Table, offset: usize) -> Option<String> {
    table.iter().find_map(|(key, item)| match item {
        toml_edit::Item::Value(value) => key_in_value(key, value, offset),
        toml_edit::Item::Table(table) => key_in_table(table, offset),
        toml_edit::Item::ArrayOfTables(tables) => {
            tables.iter().find_map(|table| key_in_table(table, offset))
        }
        toml_edit::Item::None => None,
    })
}

fn key_in_value(key: &str, value: &toml_edit::Value, offset: usize) -> Option<String> {
    if !value.span().is_some_and(|span| span.contains(&offset)) {
        return None;
    }
    let nested = |table: &toml_edit::InlineTable| {
        table
            .iter()
            .find_map(|(key, value)| key_in_value(key, value, offset))
    };
    let inner = match value {
        toml_edit::Value::InlineTable(table) => nested(table),
        toml_edit::Value::Array(array) => array.iter().find_map(|element| match element {
            toml_edit::Value::InlineTable(table) => nested(table),
            _ => None,
        }),
        _ => None,
    };
    Some(inner.unwrap_or_else(|| key.to_string()))
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
    fn type_and_value_errors_name_the_key_but_never_the_value() {
        for (contents, secret, expected) in [
            (
                "embedding_api_key = 12345678\n",
                "12345678",
                "line 1, column 21: embedding_api_key: wrong value type, expected a string",
            ),
            (
                "cuda_device_id = \"x-secret\"\n",
                "x-secret",
                "cuda_device_id: wrong value type, expected i32",
            ),
            (
                "embeddings = \"sk-secret\"\n",
                "sk-secret",
                "embeddings: invalid value, expected true, false, \"local\", or \"remote\"",
            ),
            (
                "embedding_batch_size = -7\n",
                "-7",
                "embedding_batch_size: invalid value, expected usize",
            ),
            (
                "embeddings = \"sk-x, expected leak\"\n",
                "leak",
                "embeddings: invalid value, expected true, false",
            ),
            (
                "rerank = \"sk-secret\"\n",
                "sk-secret",
                "rerank: invalid value, expected true, false, \"local\", or \"remote\"",
            ),
            (
                "rerank_dialect = \"sk-secret\"\n",
                "sk-secret",
                "unknown variant, expected documents or texts",
            ),
            (
                "rerank_api_key = 12345678\n",
                "12345678",
                "rerank_api_key: wrong value type, expected a string",
            ),
            (
                "rerank_candidates = -3\n",
                "-3",
                "rerank_candidates: invalid value, expected usize",
            ),
            (
                "rerank_extra_body = { provider = \"sk-secret\" }\n",
                "sk-secret",
                "rerank_extra_body: wrong value type, expected a string",
            ),
        ] {
            let error = UserConfig::parse(contents)
                .expect_err("reject mistyped value")
                .to_string();
            assert!(error.contains(expected), "{error}");
            assert!(!error.contains(secret), "{error}");
            assert!(!error.contains('`'), "{error}");
        }
    }

    #[test]
    fn serde_messages_keep_only_static_expectations() {
        for (message, key, scrubbed) in [
            (
                "invalid type: string \"a, expected b\", expected u64",
                Some("x"),
                "x: wrong value type, expected u64",
            ),
            (
                "unknown variant `s`, expected `a` or `b`",
                None,
                "unknown variant, expected a or b",
            ),
            (
                "unknown variant `s, expected `leak``, there are no variants",
                None,
                "unknown variant",
            ),
            (
                "unknown field `s`, expected `only`",
                None,
                "unknown field, expected only",
            ),
            (
                "unknown field `onl`, expected `only`",
                None,
                "unknown field `onl` (did you mean `only`?)",
            ),
            (
                "unknown field `\u{1b}only`, expected `only`",
                None,
                "unknown field, expected only",
            ),
            (
                "unknown variant `locl`, expected `local` or `remote`",
                None,
                "unknown variant, expected local or remote",
            ),
            (
                "invalid table header\nduplicate key `t` in document root",
                None,
                "invalid table header\nduplicate key `t` in document root",
            ),
        ] {
            assert_eq!(scrub_toml_message(message, key), scrubbed);
        }
        assert_eq!(edit_distance("modle", "model", 2), Some(2));
        assert_eq!(edit_distance("cuda_device", "cuda_device_id", 2), None);
    }

    #[test]
    fn errors_inside_arrays_and_tables_name_the_innermost_key() {
        for (contents, expected) in [
            (
                "cuda_library_paths = [1]\n",
                "line 1, column 23: cuda_library_paths: wrong value type, expected ",
            ),
            (
                "cudnn_library_paths = [\n  \"/ok\",\n  2,\n]\n",
                "line 3, column 3: cudnn_library_paths: wrong value type, expected ",
            ),
            (
                "cuda_library_paths = [\n  \"\"\"\ntoken = [\n\"\"\", 3]\n",
                "cuda_library_paths: wrong value type, expected ",
            ),
            (
                "multi_machine.timeout_seconds = \"x\"\n",
                "timeout_seconds: wrong value type, expected u64",
            ),
        ] {
            let error = UserConfig::parse(contents)
                .expect_err("reject mistyped element")
                .to_string();
            assert!(error.contains(expected), "{error}");
            assert!(!error.contains("token"), "{error}");
        }
        assert_eq!(key_at("\"quoted key\" = 5\n", 15), None);
        assert_eq!(
            key_at("x = [\n{ inner = 5 }]\n", 16).as_deref(),
            Some("inner")
        );
    }

    #[test]
    fn oversized_and_non_regular_config_files_are_rejected() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(root.path().to_path_buf())).expect("paths");
        let path = paths.root.join("config.toml");
        let oversized = usize::try_from(MAX_CONFIG_BYTES).expect("size") + 1;
        std::fs::write(&path, "#".repeat(oversized)).expect("write");
        let error = UserConfig::load(&paths)
            .expect_err("reject oversized")
            .to_string();
        assert!(error.contains("is larger than 1048576 bytes"), "{error}");
        std::fs::remove_file(&path).expect("remove");

        std::fs::create_dir(&path).expect("directory in place of the file");
        let error = UserConfig::load(&paths)
            .expect_err("reject directory")
            .to_string();
        assert!(error.contains("is not a regular file"), "{error}");
        std::fs::remove_dir(&path).expect("remove directory");

        #[cfg(unix)]
        {
            let status = std::process::Command::new("mkfifo")
                .arg(&path)
                .status()
                .expect("run mkfifo");
            assert!(status.success());
            let error = UserConfig::load(&paths)
                .expect_err("reject fifo")
                .to_string();
            assert!(error.contains("is not a regular file"), "{error}");
            std::fs::remove_file(&path).expect("remove fifo");
        }

        assert!(
            UserConfig::load(&paths).is_ok(),
            "a missing file is the default"
        );
    }

    #[test]
    fn parse_errors_report_a_position_but_never_the_offending_line() {
        let root = tempfile::tempdir().expect("tempdir");
        let paths = Paths::new(Some(root.path().to_path_buf())).expect("paths");
        let path = paths.root.join("config.toml");
        for (contents, secret) in [
            ("embedding_api_key = sk-abc123secret\n", "sk-abc123secret"),
            (
                "embedding_api_key = \"sk-unterminated-secret\n",
                "sk-unterminated-secret",
            ),
            ("embeddings = \"cloud-secret\"\n", "cloud-secret"),
        ] {
            std::fs::write(&path, contents).expect("write");
            for error in [
                UserConfig::load(&paths)
                    .expect_err("reject from load")
                    .to_string(),
                UserConfig::parse(contents)
                    .expect_err("reject from parse")
                    .to_string(),
            ] {
                assert!(!error.contains(secret), "{error}");
            }
        }
        std::fs::write(
            &path,
            "auto_index_on_search = false\nembedding_api_key = sk-abc\n",
        )
        .expect("write");
        let error = UserConfig::load(&paths).expect_err("reject").to_string();
        assert!(
            error.starts_with(&format!(
                "{}: invalid TOML at line 2, column ",
                path.display()
            )),
            "{error}"
        );
    }

    #[test]
    fn near_miss_top_level_keys_warn_and_still_load() {
        let path = Path::new("config.toml");
        for (contents, typo, known) in [
            (
                "embedding_base_ulr = \"https://x\"\n",
                "embedding_base_ulr",
                "embedding_base_url",
            ),
            ("modle = \"bge\"\n", "modle", "model"),
            ("embedings = true\n", "embedings", "embeddings"),
            (
                "execution_providr = \"cpu\"\n",
                "execution_providr",
                "execution_provider",
            ),
        ] {
            let table: toml::Table = toml::from_str(contents).expect("parse table");
            assert_eq!(
                key_typo_warnings(&table, path),
                [format!(
                    "config.toml: unknown key `{typo}` (did you mean `{known}`?)"
                )]
            );
            assert!(UserConfig::parse(contents).is_ok(), "{contents:?}");
        }
        for contents in [
            "foo = 1\n",
            "future_setting = 1\n",
            "sk-live-0123456789abcdef = 1\n",
            // Not bare keys, so never echoed.
            "\"modl\\u0007\" = 1\n",
            "\"mod el\" = 1\n",
            "index_service_watch = true\nindex_service_watch_interval = 5\n",
        ] {
            let table: toml::Table = toml::from_str(contents).expect("parse table");
            assert!(key_typo_warnings(&table, path).is_empty(), "{contents:?}");
            assert!(UserConfig::parse(contents).is_ok(), "{contents:?}");
        }
    }

    #[test]
    fn key_typo_warnings_print_once_and_stay_bounded() {
        let mut warned = HashSet::new();
        assert!(admit_key_typo_warning(&mut warned, "a"));
        assert!(!admit_key_typo_warning(&mut warned, "a"));
        for index in 0..MAX_KEY_TYPO_WARNINGS * 2 {
            admit_key_typo_warning(&mut warned, &format!("warning {index}"));
        }
        assert_eq!(warned.len(), MAX_KEY_TYPO_WARNINGS);
        assert!(!admit_key_typo_warning(&mut warned, "new"));
    }

    #[test]
    fn parser_messages_hide_quoted_tokens_that_are_not_keys() {
        for (message, redacted) in [
            (
                "duplicate key `sk-live-0123456789abcdef0123456789abcdef0123456789abcdef0123456789` in document root",
                "duplicate key … in document root",
            ),
            (
                "duplicate key `api key=sk-abc` in document root",
                "duplicate key … in document root",
            ),
            (
                "duplicate key `\u{1b}[31mmodel` in document root",
                "duplicate key … in document root",
            ),
            (
                "duplicate key `model` in document root",
                "duplicate key `model` in document root",
            ),
            (
                "invalid string\nexpected `\"`, `'`",
                "invalid string\nexpected `\"`, `'`",
            ),
            ("unpaired `tail secret", "unpaired …"),
        ] {
            assert_eq!(scrub_toml_message(message, None), redacted);
        }
        let error = UserConfig::parse("\"sk-pasted token\" = 1\n\"sk-pasted token\" = 2\n")
            .expect_err("reject duplicate key")
            .to_string();
        assert!(!error.contains("sk-pasted"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn an_unreadable_config_file_is_an_error_not_the_default() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let root = tempfile::tempdir().expect("tempdir");
        // Root reads the file regardless of its mode.
        if std::fs::metadata(root.path()).expect("inspect").uid() == 0 {
            return;
        }
        let paths = Paths::new(Some(root.path().to_path_buf())).expect("paths");
        let path = paths.root.join("config.toml");
        std::fs::write(&path, "embeddings = true\n").expect("write");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000))
            .expect("make unreadable");
        let error = format!(
            "{:#}",
            UserConfig::load(&paths).expect_err("reject unreadable file")
        );
        assert!(
            error.starts_with(&format!("open {}: ", path.display())),
            "{error}"
        );
        assert!(error.contains("ermission denied"), "{error}");
    }

    /// Captures the field names serde derives for a struct.
    struct FieldNames<'a>(&'a mut &'static [&'static str]);

    impl<'de> serde::Deserializer<'de> for FieldNames<'_> {
        type Error = serde::de::value::Error;

        fn deserialize_any<V: serde::de::Visitor<'de>>(
            self,
            _: V,
        ) -> std::result::Result<V::Value, Self::Error> {
            Err(serde::de::Error::custom("only structs are inspected"))
        }

        fn deserialize_struct<V: serde::de::Visitor<'de>>(
            self,
            _: &'static str,
            fields: &'static [&'static str],
            _: V,
        ) -> std::result::Result<V::Value, Self::Error> {
            *self.0 = fields;
            Err(serde::de::Error::custom("fields captured"))
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes
            byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct map enum
            identifier ignored_any
        }
    }

    #[test]
    fn known_key_list_matches_the_config_struct() {
        let mut fields: &'static [&'static str] = &[];
        let _ = UserConfig::deserialize(FieldNames(&mut fields));
        let mut derived: Vec<&str> = fields.to_vec();
        let aliases = ["index_service_watch", "index_service_watch_interval"];
        // Every alias on the struct must be in the list, or a near miss of it would warn.
        let source = include_str!("config.rs");
        let start = source
            .find("pub struct UserConfig {")
            .expect("UserConfig definition");
        let end = start
            + source
                .get(start..)
                .and_then(|definition| definition.find("\n}\n"))
                .expect("end of UserConfig");
        let declared: Vec<&str> = source
            .get(start..end)
            .expect("definition")
            .split("serde(alias = \"")
            .skip(1)
            .filter_map(|rest| rest.split_once('"').map(|(alias, _)| alias))
            .collect();
        assert_eq!(declared, aliases);
        derived.extend(aliases.iter().filter(|alias| !fields.contains(alias)));
        derived.sort_unstable();
        let mut listed = USER_CONFIG_KEYS.to_vec();
        listed.sort_unstable();
        assert_eq!(listed, derived);
    }

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

    #[test]
    fn rerank_accepts_bool_local_or_remote() {
        for (value, mode) in [
            ("true", RerankMode::Local),
            ("false", RerankMode::Off),
            ("\"local\"", RerankMode::Local),
            ("\"remote\"", RerankMode::Remote),
        ] {
            let config: UserConfig =
                toml::from_str(&format!("rerank = {value}")).expect("parse rerank");
            assert_eq!(config.rerank_mode(), mode, "{value}");
        }
        assert_eq!(UserConfig::default().rerank_mode(), RerankMode::Off);
        assert!(toml::from_str::<UserConfig>(r#"rerank = "cloud""#).is_err());
        assert!(toml::from_str::<UserConfig>("rerank = 1").is_err());
    }

    #[test]
    fn rerank_resolves_model_defaults_and_the_shared_onnx_runtime() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[
            ("MEMEX_RERANK_MODEL", None),
            ("MEMEX_EXECUTION_PROVIDER", None),
        ]);
        assert_eq!(UserConfig::default().resolve_rerank().expect("off"), None);
        let config: UserConfig = toml::from_str(
            r#"
                embeddings = "remote"
                model = "text-embedding-3-small"
                embedding_base_url = "https://api.example.test/v1"
                rerank = true
                rerank_model = "BGE-V2-M3"
                execution_provider = "cpu"
                cuda_device_id = 2
            "#,
        )
        .expect("parse rerank config");
        let settings = config
            .resolve_rerank()
            .expect("resolve rerank")
            .expect("rerank on");
        let (model, runtime) = local_engine(&settings);
        assert_eq!(*model, fastembed::RerankerModel::BGERerankerV2M3);
        assert_eq!(settings.candidates, DEFAULT_RERANK_CANDIDATES);
        assert_eq!(settings.doc_chars, DEFAULT_RERANK_DOC_CHARS);
        // Remote embeddings ignore the ONNX keys; the local reranker still uses them.
        assert_eq!(runtime.execution_provider, ExecutionProviderChoice::Cpu);
        assert_eq!(runtime.cuda_device_id, Some(2));
        assert_eq!(runtime.remote, None);
        let local = UserConfig {
            embeddings: Some(EmbeddingsMode::Local),
            embedding_base_url: None,
            ..config.clone()
        };
        let embed_runtime = local.resolve_embed_runtime().expect("embed runtime");
        assert_eq!(
            *runtime,
            EmbedRuntimeConfig {
                batch_size: None,
                ..embed_runtime
            }
        );
    }

    fn local_engine(settings: &RerankSettings) -> (&fastembed::RerankerModel, &EmbedRuntimeConfig) {
        match &settings.engine {
            RerankEngine::Local { model, runtime } => (model, runtime),
            RerankEngine::Remote(_) => panic!("expected a local reranker"),
        }
    }

    fn remote_engine(settings: &RerankSettings) -> &RemoteRerankConfig {
        match &settings.engine {
            RerankEngine::Remote(remote) => remote,
            RerankEngine::Local { .. } => panic!("expected a remote reranker"),
        }
    }

    #[test]
    fn rerank_requires_a_known_model_and_bounded_sizes() {
        let _guard = env_lock();
        let local = UserConfig {
            rerank: Some(RerankMode::Local),
            ..UserConfig::default()
        };
        {
            let _env = EnvVarGuard::set(&[("MEMEX_RERANK_MODEL", None)]);
            let error = local
                .resolve_rerank()
                .expect_err("model required")
                .to_string();
            assert!(error.contains("requires rerank_model"), "{error}");
            assert!(
                error.contains("jina-turbo, bge-v2-m3, bge-base, jina-v2"),
                "{error}"
            );
        }
        {
            let _env = EnvVarGuard::set(&[("MEMEX_RERANK_MODEL", Some("jina-turbo"))]);
            let settings = local.resolve_rerank().expect("env model").expect("on");
            assert_eq!(
                *local_engine(&settings).0,
                fastembed::RerankerModel::JINARerankerV1TurboEn
            );
            let configured = UserConfig {
                rerank_model: Some("bge-base".to_string()),
                ..local.clone()
            };
            let settings = configured
                .resolve_rerank()
                .expect("config model")
                .expect("on");
            assert_eq!(
                *local_engine(&settings).0,
                fastembed::RerankerModel::BGERerankerBase
            );
        }
        {
            let _env = EnvVarGuard::set(&[("MEMEX_RERANK_MODEL", Some("ms-marco"))]);
            let error = format!("{:#}", local.resolve_rerank().expect_err("bad env model"));
            assert!(error.contains("MEMEX_RERANK_MODEL"), "{error}");
            assert!(error.contains("'ms-marco'"), "{error}");
            // Off never reads the variable.
            assert_eq!(UserConfig::default().resolve_rerank().expect("off"), None);
        }
        let _env = EnvVarGuard::set(&[("MEMEX_RERANK_MODEL", None)]);
        // The model name depends on the mode, so it is checked only when reranking is on.
        let unknown = UserConfig {
            rerank_model: Some("ms-marco".to_string()),
            ..UserConfig::default()
        };
        assert_eq!(unknown.resolve_rerank().expect("off"), None);
        let error = format!(
            "{:#}",
            UserConfig {
                rerank: Some(RerankMode::Local),
                ..unknown
            }
            .resolve_rerank()
            .expect_err("unknown local model")
        );
        assert!(error.contains("invalid rerank_model"), "{error}");
        for (candidates, doc_chars, key) in [
            (Some(4), None, "rerank_candidates"),
            (Some(101), None, "rerank_candidates"),
            (None, Some(199), "rerank_doc_chars"),
            (None, Some(8001), "rerank_doc_chars"),
        ] {
            let config = UserConfig {
                rerank_model: Some("jina-turbo".to_string()),
                rerank_candidates: candidates,
                rerank_doc_chars: doc_chars,
                ..local.clone()
            };
            let error = config.resolve_rerank().expect_err(key).to_string();
            assert!(error.starts_with(key), "{error}");
        }
        let bounds = UserConfig {
            rerank_model: Some("jina-turbo".to_string()),
            rerank_candidates: Some(100),
            rerank_doc_chars: Some(200),
            ..local
        };
        let settings = bounds.resolve_rerank().expect("bounds").expect("on");
        assert_eq!((settings.candidates, settings.doc_chars), (100, 200));
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
                .resolve_model(None)
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
        assert!(config.resolve_model(None).is_err());
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

    fn remote_rerank_config() -> UserConfig {
        toml::from_str(
            r#"
                rerank = "remote"
                rerank_url = " https://openrouter.ai/api/v1/rerank "
                rerank_model = "cohere/rerank-v3.5"
                rerank_api_key = "sk-rerank-literal"
                execution_provider = "not-a-provider"
                cuda_device_id = 3
            "#,
        )
        .expect("parse remote rerank config")
    }

    #[test]
    fn remote_rerank_resolves_defaults_and_ignores_runtime_keys() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("MEMEX_RERANK_MODEL", None)]);
        let config = remote_rerank_config();
        let settings = config.resolve_rerank().expect("resolve").expect("on");
        assert_eq!(settings.candidates, DEFAULT_RERANK_CANDIDATES);
        assert_eq!(settings.doc_chars, DEFAULT_RERANK_DOC_CHARS);
        let remote = remote_engine(&settings);
        assert_eq!(remote.model.as_deref(), Some("cohere/rerank-v3.5"));
        assert_eq!(remote.dialect, RerankDialect::Documents);
        assert_eq!(
            remote.endpoint,
            RemoteEndpoint {
                base_url: "https://openrouter.ai/api/v1/rerank".to_string(),
                api_key: Some("sk-rerank-literal".to_string()),
                timeout: Duration::from_secs(10),
                max_retries: 1,
            }
        );
        assert!(!format!("{config:?}").contains("sk-rerank-literal"));
        assert!(!format!("{settings:?}").contains("sk-rerank-literal"));

        let tuned = UserConfig {
            rerank_timeout_secs: Some(30),
            rerank_max_retries: Some(0),
            rerank_dialect: Some(RerankDialect::Texts),
            rerank_candidates: Some(50),
            rerank_doc_chars: Some(4000),
            ..config.clone()
        };
        let settings = tuned.resolve_rerank().expect("resolve").expect("on");
        assert_eq!((settings.candidates, settings.doc_chars), (50, 4000));
        let remote = remote_engine(&settings);
        assert_eq!(remote.dialect, RerankDialect::Texts);
        assert_eq!(remote.endpoint.timeout, Duration::from_secs(30));
        assert_eq!(remote.endpoint.max_retries, 0);
        let parsed: UserConfig = toml::from_str("rerank_dialect = \"texts\"").expect("dialect");
        assert_eq!(parsed.rerank_dialect, Some(RerankDialect::Texts));
    }

    #[test]
    fn remote_rerank_validates_url_model_and_bounds() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[("MEMEX_RERANK_MODEL", None)]);
        let base = remote_rerank_config();
        let error = |config: UserConfig| {
            format!(
                "{:#}",
                config.resolve_rerank().expect_err("invalid remote rerank")
            )
        };
        for (config, expected) in [
            (
                UserConfig {
                    rerank_url: None,
                    ..base.clone()
                },
                "requires rerank_url",
            ),
            (
                UserConfig {
                    rerank_url: Some("  ".to_string()),
                    ..base.clone()
                },
                "requires rerank_url",
            ),
            (
                UserConfig {
                    rerank_url: Some("ftp://rerank.example.test/rerank".to_string()),
                    ..base.clone()
                },
                "rerank URL",
            ),
            (
                UserConfig {
                    rerank_url: Some("http://rerank.example.test/rerank".to_string()),
                    ..base.clone()
                },
                "use https or a loopback host",
            ),
            (
                UserConfig {
                    rerank_model: None,
                    ..base.clone()
                },
                "requires rerank_model",
            ),
            (
                UserConfig {
                    rerank_model: Some(String::new()),
                    ..base.clone()
                },
                "invalid rerank_model",
            ),
            (
                UserConfig {
                    rerank_model: Some("a b".to_string()),
                    ..base.clone()
                },
                "whitespace or control",
            ),
            (
                UserConfig {
                    rerank_model: Some("m".repeat(201)),
                    ..base.clone()
                },
                "longer than 200",
            ),
            (
                UserConfig {
                    rerank_timeout_secs: Some(0),
                    ..base.clone()
                },
                "rerank_timeout_secs must be between 1 and 30, got 0",
            ),
            (
                UserConfig {
                    rerank_timeout_secs: Some(31),
                    ..base.clone()
                },
                "rerank_timeout_secs must be between 1 and 30, got 31",
            ),
            (
                UserConfig {
                    rerank_max_retries: Some(4),
                    ..base.clone()
                },
                "rerank_max_retries must be between 0 and 3, got 4",
            ),
            (
                UserConfig {
                    rerank_candidates: Some(101),
                    ..base.clone()
                },
                "rerank_candidates",
            ),
            (
                UserConfig {
                    rerank_extra_body: Some(SecretString(
                        "{\"provider\": sk-extra-secret}".to_string(),
                    )),
                    ..base.clone()
                },
                "invalid rerank_extra_body: is not valid JSON",
            ),
            (
                UserConfig {
                    rerank_extra_body: Some(SecretString(
                        "{\"query\": \"sk-extra-secret\"}".to_string(),
                    )),
                    ..base.clone()
                },
                "invalid rerank_extra_body: must not set `query`",
            ),
        ] {
            let message = error(config);
            assert!(message.contains(expected), "{message}");
            assert!(!message.contains("sk-rerank-literal"), "{message}");
            assert!(!message.contains("sk-extra-secret"), "{message}");
        }
        let extra = UserConfig {
            rerank_extra_body: Some(SecretString(
                "{\"provider\": {\"data_collection\": \"deny\"}}".to_string(),
            )),
            ..base.clone()
        };
        let settings = extra.resolve_rerank().expect("extra body").expect("on");
        assert!(remote_engine(&settings).extra_body.is_some());
        assert!(!format!("{extra:?} {settings:?}").contains("data_collection"));
        // Plain http is accepted without a key and to loopback hosts with one.
        for (url, key) in [
            ("http://rerank.example.test/rerank", None),
            ("http://127.0.0.1:8080/rerank", Some("sk-rerank-literal")),
        ] {
            let config = UserConfig {
                rerank_url: Some(url.to_string()),
                rerank_api_key: key.map(|key| SecretString(key.to_string())),
                rerank_api_key_env: Some("MEMEX_TEST_UNSET_RERANK_KEY".to_string()),
                ..base.clone()
            };
            assert!(config.resolve_rerank().is_ok(), "{url}");
        }
        {
            // The local model variable never reaches a hosted API.
            let _env = EnvVarGuard::set(&[("MEMEX_RERANK_MODEL", Some("jina-turbo"))]);
            let unnamed = UserConfig {
                rerank_model: None,
                ..base.clone()
            };
            assert!(error(unnamed.clone()).contains("requires rerank_model"));
            let texts = UserConfig {
                rerank_dialect: Some(RerankDialect::Texts),
                ..unnamed
            };
            let settings = texts.resolve_rerank().expect("texts").expect("on");
            assert_eq!(remote_engine(&settings).model, None);
            let message = error(UserConfig {
                rerank_model: Some("bad model".to_string()),
                ..texts
            });
            assert!(message.contains("invalid rerank_model"), "{message}");
        }
        // Local reranking ignores the remote keys.
        let local = UserConfig {
            rerank: Some(RerankMode::Local),
            rerank_model: Some("jina-turbo".to_string()),
            rerank_url: Some("not a url".to_string()),
            rerank_timeout_secs: Some(0),
            rerank_max_retries: Some(99),
            rerank_extra_body: Some(SecretString("not json".to_string())),
            execution_provider: None,
            ..base
        };
        let settings = local.resolve_rerank().expect("local").expect("on");
        assert_eq!(
            *local_engine(&settings).0,
            fastembed::RerankerModel::JINARerankerV1TurboEn
        );
    }

    #[test]
    fn rerank_api_key_lookup_precedence() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[
            ("MEMEX_RERANK_API_KEY", Some("memex-rerank-key")),
            ("MEMEX_TEST_RERANK_KEY", Some("custom-key")),
            ("MEMEX_TEST_MISSING_RERANK_KEY", None),
            ("MEMEX_EMBEDDING_API_KEY", Some("embedding-key")),
            ("OPENAI_API_KEY", Some("openai-key")),
        ]);
        let resolve = |config: UserConfig| config.resolve_rerank_api_key().expect("key");
        assert_eq!(
            resolve(UserConfig {
                rerank_api_key: Some(SecretString("literal-key".to_string())),
                rerank_api_key_env: Some("MEMEX_TEST_RERANK_KEY".to_string()),
                ..UserConfig::default()
            })
            .as_deref(),
            Some("literal-key")
        );
        assert_eq!(
            resolve(UserConfig {
                rerank_api_key: Some(SecretString(String::new())),
                rerank_api_key_env: Some("MEMEX_TEST_RERANK_KEY".to_string()),
                ..UserConfig::default()
            })
            .as_deref(),
            Some("custom-key")
        );
        // A named variable is the only one read.
        assert_eq!(
            resolve(UserConfig {
                rerank_api_key_env: Some("MEMEX_TEST_MISSING_RERANK_KEY".to_string()),
                ..UserConfig::default()
            }),
            None
        );
        assert_eq!(
            resolve(UserConfig::default()).as_deref(),
            Some("memex-rerank-key")
        );
        // The embedding keys are never used, and an empty variable counts as unset.
        let _env = EnvVarGuard::set(&[("MEMEX_RERANK_API_KEY", Some(""))]);
        assert_eq!(resolve(UserConfig::default()), None);
        let _env = EnvVarGuard::set(&[("MEMEX_RERANK_API_KEY", None)]);
        assert_eq!(resolve(UserConfig::default()), None);
    }
}
