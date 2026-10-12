use crate::remote_embed::{RemoteEmbedder, RemoteEndpoint};
use anyhow::{Context, Result, anyhow};
use fastembed::{EmbeddingModel, InitOptions, QuantizationMode, TextEmbedding};
use model2vec_rs::model::StaticModel;
use ort::ep::ExecutionProviderDispatch;
use std::borrow::Cow;
use std::path::PathBuf;
use std::str::FromStr;

#[cfg(feature = "cuda")]
use std::path::Path;
#[cfg(feature = "cuda")]
use std::{collections::HashSet, mem, sync::OnceLock};

const MEMEX_EXECUTION_PROVIDER_ENV: &str = "MEMEX_EXECUTION_PROVIDER";
const MEMEX_CUDA_DEVICE_ID_ENV: &str = "MEMEX_CUDA_DEVICE_ID";
const MEMEX_COMPUTE_UNITS_ENV: &str = "MEMEX_COMPUTE_UNITS";
const MEMEX_CUDA_LIBRARY_PATHS_ENV: &str = "MEMEX_CUDA_LIBRARY_PATHS";
const MEMEX_CUDNN_LIBRARY_PATHS_ENV: &str = "MEMEX_CUDNN_LIBRARY_PATHS";

#[cfg(all(feature = "cuda", windows))]
const CUDA_DYLIBS: &[&str] = &[
    "cublasLt64_13.dll",
    "cublas64_13.dll",
    "cufft64_12.dll",
    "cudart64_13.dll",
];

#[cfg(all(feature = "cuda", not(windows)))]
const CUDA_DYLIBS: &[&str] = &[
    "libcublasLt.so.13",
    "libcublas.so.13",
    "libnvrtc.so.13",
    "libcurand.so.10",
    "libcufft.so.12",
    "libcudart.so.13",
];

#[cfg(all(feature = "cuda", windows))]
const CUDNN_DYLIBS: &[&str] = &[
    "cudnn_engines_runtime_compiled64_9.dll",
    "cudnn_engines_precompiled64_9.dll",
    "cudnn_heuristic64_9.dll",
    "cudnn_ops64_9.dll",
    "cudnn_adv64_9.dll",
    "cudnn_graph64_9.dll",
    "cudnn64_9.dll",
];

#[cfg(all(feature = "cuda", not(windows)))]
const CUDNN_DYLIBS: &[&str] = &[
    "libcudnn_engines_runtime_compiled.so.9",
    "libcudnn_engines_precompiled.so.9",
    "libcudnn_heuristic.so.9",
    "libcudnn_ops.so.9",
    "libcudnn_adv.so.9",
    "libcudnn_graph.so.9",
    "libcudnn.so.9",
];

/// Inputs per embedding call and per vector flush when `embedding_batch_size` is unset.
pub const DEFAULT_EMBED_BATCH_SIZE: usize = 64;

const REMOTE_MODEL_PREFIX: &str = "remote:";

/// A model that runs in-process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalModel {
    /// An ONNX model from the fastembed catalog.
    Fastembed(EmbeddingModel),
    /// PotionBase8M - 8M params, model2vec backend, tiny and fast
    Potion,
}

/// Embedding model selection.
///
/// [`ModelChoice::identity`] is the name stored with vectors and caches; two choices
/// with the same identity produce interchangeable vectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelChoice {
    Local(LocalModel),
    /// A model served by an OpenAI-compatible endpoint, named verbatim.
    Remote(String),
}

impl Default for ModelChoice {
    fn default() -> Self {
        Self::gemma()
    }
}

impl ModelChoice {
    /// EmbeddingGemma300M - 300M params, 768 dims, highest quality but slowest
    pub fn gemma() -> Self {
        Self::Local(LocalModel::Fastembed(EmbeddingModel::EmbeddingGemma300M))
    }

    /// AllMiniLML6V2 - 22M params, 384 dims, very fast
    pub fn minilm() -> Self {
        Self::Local(LocalModel::Fastembed(EmbeddingModel::AllMiniLML6V2))
    }

    /// BGESmallENV15 - 33M params, 384 dims, good balance
    pub fn bge_small() -> Self {
        Self::Local(LocalModel::Fastembed(EmbeddingModel::BGESmallENV15))
    }

    /// NomicEmbedTextV15 - 137M params, 768 dims, good quality
    pub fn nomic() -> Self {
        Self::Local(LocalModel::Fastembed(EmbeddingModel::NomicEmbedTextV15))
    }

    pub fn potion() -> Self {
        Self::Local(LocalModel::Potion)
    }

    /// Parse a legacy alias, a fastembed `EmbeddingModel` variant name (case-insensitive),
    /// or `remote:<model>` (case-sensitive model name).
    pub fn parse(s: &str) -> Result<Self> {
        if let Some(model) = s.strip_prefix(REMOTE_MODEL_PREFIX) {
            return Self::remote(model);
        }
        match s.to_lowercase().as_str() {
            "minilm" | "mini" | "fast" => Ok(Self::minilm()),
            "bge" | "bge-small" | "bgesmall" => Ok(Self::bge_small()),
            "nomic" => Ok(Self::nomic()),
            "gemma" | "embeddinggemma" | "default" => Ok(Self::gemma()),
            "potion" | "potion8m" | "potion-8m" | "potion-base-8m" | "model2vec" => {
                Ok(Self::potion())
            }
            _ => match EmbeddingModel::from_str(s) {
                Ok(model) if is_supported_fastembed_model(&model) => {
                    Ok(Self::Local(LocalModel::Fastembed(model)))
                }
                Ok(_) => Err(anyhow!(
                    "model '{s}' is not supported; options: {}",
                    supported_model_names()
                )),
                Err(_) => Err(anyhow!(
                    "unknown model '{s}', options: {}",
                    supported_model_names()
                )),
            },
        }
    }

    /// A remote model named verbatim (without the `remote:` prefix).
    ///
    /// Rejects an empty name and any control character or ASCII whitespace. The
    /// resulting identity is stored in metadata and caches and must never be used as
    /// a path component.
    pub fn remote(model: &str) -> Result<Self> {
        if model.is_empty() {
            return Err(anyhow!("remote embedding model name must not be empty"));
        }
        if model
            .chars()
            .any(|c| c.is_control() || c.is_ascii_whitespace())
        {
            return Err(anyhow!(
                "remote embedding model name {model:?} must not contain whitespace or control \
                 characters"
            ));
        }
        Ok(Self::Remote(model.to_string()))
    }

    /// Stable name stored with vectors: the legacy alias for the five original models,
    /// the fastembed variant name for other local models, and `remote:<model>`.
    pub fn identity(&self) -> Cow<'static, str> {
        match self {
            Self::Local(LocalModel::Potion) => Cow::Borrowed("potion"),
            Self::Local(LocalModel::Fastembed(model)) => match model {
                EmbeddingModel::AllMiniLML6V2 => Cow::Borrowed("minilm"),
                EmbeddingModel::BGESmallENV15 => Cow::Borrowed("bge"),
                EmbeddingModel::NomicEmbedTextV15 => Cow::Borrowed("nomic"),
                EmbeddingModel::EmbeddingGemma300M => Cow::Borrowed("gemma"),
                other => Cow::Owned(format!("{other:?}")),
            },
            Self::Remote(model) => Cow::Owned(format!("{REMOTE_MODEL_PREFIX}{model}")),
        }
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote(_))
    }

    /// Vector length known without loading the model or contacting a server: the
    /// native size of a fastembed model, or the configured `embedding_dimensions`
    /// for a remote model. `None` means only an embedder can tell.
    pub fn known_dimensions(&self, runtime: &EmbedRuntimeConfig) -> Option<usize> {
        match self {
            Self::Local(LocalModel::Fastembed(model)) => TextEmbedding::get_model_info(model)
                .ok()
                .map(|info| info.dim),
            Self::Local(LocalModel::Potion) => None,
            Self::Remote(_) => runtime.remote.as_ref().and_then(|remote| remote.dimensions),
        }
    }

    /// Reject querying vectors of a local model while remote embeddings are configured,
    /// so a query never loads a local model in remote mode.
    pub fn ensure_query_compatible(&self, runtime: &EmbedRuntimeConfig) -> Result<()> {
        if runtime.remote.is_some() && !self.is_remote() {
            return Err(anyhow!(
                "vectors use local model {} but embeddings = \"remote\"; run `memex embed` to \
                 rebuild with the remote model",
                self.identity()
            ));
        }
        Ok(())
    }
}

fn is_supported_fastembed_model(model: &EmbeddingModel) -> bool {
    TextEmbedding::get_quantization_mode(model) != QuantizationMode::Dynamic
}

fn supported_model_names() -> String {
    let mut catalog = TextEmbedding::list_supported_models()
        .into_iter()
        .map(|info| info.model)
        .filter(is_supported_fastembed_model)
        .map(|model| format!("{model:?}"))
        .collect::<Vec<_>>();
    catalog.sort_unstable();
    format!(
        "minilm, bge, nomic, gemma, potion, remote:<model>, or a fastembed model: {}",
        catalog.join(", ")
    )
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ExecutionProviderChoice {
    #[default]
    Auto,
    Cpu,
    CoreML,
    Cuda,
}

impl ExecutionProviderChoice {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "auto" | "default" => Ok(Self::Auto),
            "cpu" => Ok(Self::Cpu),
            "coreml" | "core-ml" => Ok(Self::CoreML),
            "cuda" => Ok(Self::Cuda),
            _ => Err(anyhow!(
                "unknown execution provider '{s}', options: auto, cpu, coreml, cuda"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Cpu => "cpu",
            Self::CoreML => "coreml",
            Self::Cuda => "cuda",
        }
    }

    fn effective(self) -> Self {
        match self {
            Self::Auto => Self::default_for_platform(),
            other => other,
        }
    }

    fn default_for_platform() -> Self {
        #[cfg(target_os = "macos")]
        {
            Self::CoreML
        }
        #[cfg(not(target_os = "macos"))]
        {
            Self::Cpu
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmbedRuntimeConfig {
    pub execution_provider: ExecutionProviderChoice,
    pub compute_units: Option<String>,
    pub cuda_device_id: Option<i32>,
    pub cuda_library_paths: Vec<PathBuf>,
    pub cudnn_library_paths: Vec<PathBuf>,
    /// Explicit `embedding_batch_size`; `None` uses [`DEFAULT_EMBED_BATCH_SIZE`].
    pub batch_size: Option<usize>,
    /// Endpoint for `remote:` models; `None` when remote embeddings are not configured.
    pub remote: Option<RemoteEmbedConfig>,
}

/// Remote endpoint settings plus the requested output size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteEmbedConfig {
    pub endpoint: RemoteEndpoint,
    /// Sent as the API `dimensions` parameter; `None` keeps the model's native size.
    pub dimensions: Option<usize>,
}

impl EmbedRuntimeConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            execution_provider: resolve_execution_provider_from_env()?,
            compute_units: std::env::var(MEMEX_COMPUTE_UNITS_ENV).ok(),
            cuda_device_id: resolve_cuda_device_id_from_env()?,
            cuda_library_paths: resolve_library_paths_from_env(MEMEX_CUDA_LIBRARY_PATHS_ENV),
            cudnn_library_paths: resolve_library_paths_from_env(MEMEX_CUDNN_LIBRARY_PATHS_ENV),
            batch_size: None,
            remote: None,
        })
    }

    /// Inputs per embedding call and per vector flush.
    pub fn batch_size(&self) -> usize {
        self.batch_size.unwrap_or(DEFAULT_EMBED_BATCH_SIZE)
    }

    pub fn apply_env(&self) -> Result<()> {
        apply_runtime_env(
            self.execution_provider,
            self.compute_units.as_deref(),
            self.cuda_device_id,
            &self.cuda_library_paths,
            &self.cudnn_library_paths,
        )
    }
}

pub fn resolve_execution_provider_from_env() -> Result<ExecutionProviderChoice> {
    match std::env::var(MEMEX_EXECUTION_PROVIDER_ENV) {
        Ok(provider) => ExecutionProviderChoice::parse(&provider),
        Err(std::env::VarError::NotPresent) => Ok(ExecutionProviderChoice::Auto),
        Err(std::env::VarError::NotUnicode(_)) => Err(anyhow!(
            "{MEMEX_EXECUTION_PROVIDER_ENV} is not valid unicode"
        )),
    }
}

pub fn resolve_cuda_device_id_from_env() -> Result<Option<i32>> {
    match std::env::var(MEMEX_CUDA_DEVICE_ID_ENV) {
        Ok(device_id) => {
            let parsed = device_id
                .parse::<i32>()
                .map_err(|err| anyhow!("{MEMEX_CUDA_DEVICE_ID_ENV} must be an integer: {err}"))?;
            Ok(Some(parsed))
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(anyhow!("{MEMEX_CUDA_DEVICE_ID_ENV} is not valid unicode"))
        }
    }
}

pub fn apply_runtime_env(
    execution_provider: ExecutionProviderChoice,
    compute_units: Option<&str>,
    cuda_device_id: Option<i32>,
    cuda_library_paths: &[PathBuf],
    cudnn_library_paths: &[PathBuf],
) -> Result<()> {
    unsafe {
        std::env::set_var(MEMEX_EXECUTION_PROVIDER_ENV, execution_provider.as_str());
        match compute_units {
            Some(units) => std::env::set_var(MEMEX_COMPUTE_UNITS_ENV, units),
            None => std::env::remove_var(MEMEX_COMPUTE_UNITS_ENV),
        }
        match cuda_device_id {
            Some(device_id) => std::env::set_var(MEMEX_CUDA_DEVICE_ID_ENV, device_id.to_string()),
            None => std::env::remove_var(MEMEX_CUDA_DEVICE_ID_ENV),
        }
        if cuda_library_paths.is_empty() {
            std::env::remove_var(MEMEX_CUDA_LIBRARY_PATHS_ENV);
        } else {
            std::env::set_var(
                MEMEX_CUDA_LIBRARY_PATHS_ENV,
                std::env::join_paths(cuda_library_paths)?,
            );
        }
        if cudnn_library_paths.is_empty() {
            std::env::remove_var(MEMEX_CUDNN_LIBRARY_PATHS_ENV);
        } else {
            std::env::set_var(
                MEMEX_CUDNN_LIBRARY_PATHS_ENV,
                std::env::join_paths(cudnn_library_paths)?,
            );
        }
    }
    Ok(())
}

fn resolve_library_paths_from_env(var: &str) -> Vec<PathBuf> {
    std::env::var_os(var)
        .map(|value| std::env::split_paths(&value).collect())
        .unwrap_or_default()
}

#[cfg(feature = "cuda")]
fn preload_dylib(path: impl AsRef<std::ffi::OsStr>) -> std::result::Result<(), libloading::Error> {
    #[cfg(unix)]
    let library = unsafe {
        libloading::os::unix::Library::open(
            Some(path),
            libloading::os::unix::RTLD_LAZY | libloading::os::unix::RTLD_GLOBAL,
        )
    }?;
    #[cfg(not(unix))]
    let library = unsafe { libloading::Library::new(path) }?;
    mem::forget(library);
    Ok(())
}

#[cfg(feature = "cuda")]
fn preload_cuda_dependencies(runtime: &EmbedRuntimeConfig) -> Result<()> {
    static PRELOADED: OnceLock<()> = OnceLock::new();
    if PRELOADED.get().is_some() {
        Ok(())
    } else {
        try_preload_cuda_dependencies(runtime)?;
        let _ = PRELOADED.set(());
        Ok(())
    }
}

#[cfg(feature = "cuda")]
fn try_preload_cuda_dependencies(runtime: &EmbedRuntimeConfig) -> Result<()> {
    let cuda_dirs = candidate_cuda_library_dirs(&runtime.cuda_library_paths);
    let cudnn_dirs = candidate_cudnn_library_dirs(&runtime.cudnn_library_paths);
    preload_library_group(
        "CUDA",
        CUDA_DYLIBS,
        &cuda_dirs,
        "You can set `cuda_library_paths` and `cudnn_library_paths` in config or via \
         `MEMEX_CUDA_LIBRARY_PATHS` / `MEMEX_CUDNN_LIBRARY_PATHS`.",
    )?;
    preload_library_group(
        "cuDNN",
        CUDNN_DYLIBS,
        &cudnn_dirs,
        "You can set `cuda_library_paths` and `cudnn_library_paths` in config or via \
         `MEMEX_CUDA_LIBRARY_PATHS` / `MEMEX_CUDNN_LIBRARY_PATHS`.",
    )?;
    Ok(())
}

#[cfg(feature = "cuda")]
fn preload_library_group(group: &str, dylibs: &[&str], dirs: &[PathBuf], hint: &str) -> Result<()> {
    let mut missing = Vec::new();

    for dylib in dylibs {
        if preload_dylib(dylib).is_ok() {
            continue;
        }
        let Some(path) = find_library_in_dirs(dylib, dirs) else {
            missing.push(*dylib);
            continue;
        };
        preload_dylib(&path).map_err(|err| {
            anyhow!(
                "failed to preload {group} library `{}` from `{}`: {err}",
                dylib,
                path.display()
            )
        })?;
    }

    if missing.is_empty() {
        return Ok(());
    }

    let searched = if dirs.is_empty() {
        "default loader paths only".to_string()
    } else {
        dirs.iter()
            .map(|dir| dir.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };
    Err(anyhow!(
        "missing {group} libraries: {}. Searched default loader paths and candidate directories: {}. {hint}",
        missing.join(", "),
        searched,
    ))
}

#[cfg(feature = "cuda")]
fn find_library_in_dirs(dylib: &str, dirs: &[PathBuf]) -> Option<PathBuf> {
    dirs.iter()
        .map(|dir| dir.join(dylib))
        .find(|candidate| candidate.is_file())
}

#[cfg(feature = "cuda")]
fn candidate_cuda_library_dirs(explicit_paths: &[PathBuf]) -> Vec<PathBuf> {
    let mut dirs = explicit_paths.to_vec();
    append_common_cuda_dirs(&mut dirs);
    append_active_python_nvidia_dirs(&mut dirs);
    normalize_existing_dirs(dirs)
}

#[cfg(feature = "cuda")]
fn candidate_cudnn_library_dirs(explicit_paths: &[PathBuf]) -> Vec<PathBuf> {
    let mut dirs = explicit_paths.to_vec();
    append_common_cudnn_dirs(&mut dirs);
    append_active_python_nvidia_dirs(&mut dirs);
    normalize_existing_dirs(dirs)
}

#[cfg(feature = "cuda")]
fn append_common_cuda_dirs(dirs: &mut Vec<PathBuf>) {
    for root in cuda_roots() {
        append_cuda_root_dirs(dirs, &root);
    }

    #[cfg(target_os = "linux")]
    dirs.extend([
        PathBuf::from("/usr/lib/x86_64-linux-gnu"),
        PathBuf::from("/usr/lib64"),
        PathBuf::from("/usr/lib"),
        PathBuf::from("/lib/x86_64-linux-gnu"),
        PathBuf::from("/lib64"),
    ]);
}

/// Toolkit roots to probe: those named by the environment first, then the
/// conventional install prefixes (`/usr/local/cuda` on NVIDIA's runfile and
/// Debian packages, `/opt/cuda` on Arch and Gentoo).
#[cfg(feature = "cuda")]
fn cuda_roots() -> Vec<PathBuf> {
    let mut roots: Vec<PathBuf> = ["CUDA_PATH", "CUDA_HOME", "CUDA_ROOT"]
        .iter()
        .filter_map(|var| std::env::var_os(var).map(PathBuf::from))
        .collect();

    #[cfg(target_os = "linux")]
    roots.extend([PathBuf::from("/usr/local/cuda"), PathBuf::from("/opt/cuda")]);

    roots
}

#[cfg(feature = "cuda")]
fn append_cuda_root_dirs(dirs: &mut Vec<PathBuf>, root: &Path) {
    dirs.push(root.to_path_buf());
    dirs.push(root.join("lib64"));
    dirs.push(root.join("lib"));
    dirs.extend(cuda_target_lib_dirs(root));
    dirs.push(root.join("bin"));
}

/// A toolkit keeps its per-architecture libraries under `targets/<triple>/lib`,
/// where the triple names the target the libraries are built for (for example
/// `x86_64-linux`, or `sbsa-linux` and `aarch64-linux` on Arm hosts). Enumerate
/// the directory rather than naming a triple, so every host resolves.
#[cfg(feature = "cuda")]
fn cuda_target_lib_dirs(root: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(root.join("targets")) else {
        return Vec::new();
    };
    entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path().join("lib"))
        .collect()
}

#[cfg(feature = "cuda")]
fn append_common_cudnn_dirs(dirs: &mut Vec<PathBuf>) {
    for var in ["CUDNN_HOME", "CUDNN_ROOT", "CUDNN_PATH"] {
        if let Some(root) = std::env::var_os(var).map(PathBuf::from) {
            dirs.push(root.clone());
            dirs.push(root.join("lib64"));
            dirs.push(root.join("lib"));
            dirs.push(root.join("bin"));
        }
    }

    // Some distributions install cuDNN into the CUDA toolkit prefix rather than
    // a standalone one.
    for root in cuda_roots() {
        append_cuda_root_dirs(dirs, &root);
    }

    #[cfg(target_os = "linux")]
    dirs.extend([
        PathBuf::from("/usr/lib/x86_64-linux-gnu"),
        PathBuf::from("/usr/lib64"),
        PathBuf::from("/usr/lib"),
        PathBuf::from("/lib/x86_64-linux-gnu"),
        PathBuf::from("/lib64"),
    ]);
}

#[cfg(feature = "cuda")]
fn append_active_python_nvidia_dirs(dirs: &mut Vec<PathBuf>) {
    for var in ["VIRTUAL_ENV", "CONDA_PREFIX"] {
        if let Some(root) = std::env::var_os(var).map(PathBuf::from) {
            dirs.extend(python_nvidia_lib_dirs(&root));
        }
    }
}

#[cfg(feature = "cuda")]
fn python_nvidia_lib_dirs(root: &Path) -> Vec<PathBuf> {
    let lib_root = root.join("lib");
    let Ok(entries) = std::fs::read_dir(lib_root) else {
        return Vec::new();
    };

    let mut dirs = Vec::new();
    for entry in entries.filter_map(|entry| entry.ok()) {
        let python_dir = entry.path();
        let Some(name) = python_dir.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.starts_with("python") {
            continue;
        }
        let nvidia_dir = python_dir.join("site-packages/nvidia");
        let Ok(packages) = std::fs::read_dir(nvidia_dir) else {
            continue;
        };
        for package in packages.filter_map(|entry| entry.ok()) {
            let lib_dir = package.path().join("lib");
            if lib_dir.is_dir() {
                dirs.push(lib_dir);
            }
        }
    }
    dirs
}

#[cfg(feature = "cuda")]
fn normalize_existing_dirs(dirs: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for dir in dirs {
        if !dir.is_dir() {
            continue;
        }
        let normalized = dir.canonicalize().unwrap_or(dir);
        if seen.insert(normalized.clone()) {
            out.push(normalized);
        }
    }
    out
}

fn init_options_for_model(
    model_type: EmbeddingModel,
    runtime: &EmbedRuntimeConfig,
) -> Result<InitOptions> {
    let opts = InitOptions::new(model_type).with_show_download_progress(false);
    let providers = execution_providers(runtime)?;
    if providers.is_empty() {
        Ok(opts)
    } else {
        Ok(opts.with_execution_providers(providers))
    }
}

/// ONNX Runtime execution providers for `runtime`, shared by every local ONNX model.
///
/// `Auto` resolves to the platform default first. The CPU provider yields an empty list,
/// which leaves ONNX Runtime on its built-in CPU provider. Fails when the selected
/// provider is unavailable on this platform or in this build.
pub(crate) fn execution_providers(
    runtime: &EmbedRuntimeConfig,
) -> Result<Vec<ExecutionProviderDispatch>> {
    match runtime.execution_provider.effective() {
        ExecutionProviderChoice::Auto | ExecutionProviderChoice::Cpu => Ok(Vec::new()),
        ExecutionProviderChoice::CoreML => coreml_execution_providers(runtime),
        ExecutionProviderChoice::Cuda => cuda_execution_providers(runtime),
    }
}

#[cfg(target_os = "macos")]
fn coreml_execution_providers(
    runtime: &EmbedRuntimeConfig,
) -> Result<Vec<ExecutionProviderDispatch>> {
    use ort::ep::coreml::{ComputeUnits, CoreML};

    let compute_units = runtime
        .compute_units
        .as_deref()
        .map(|v| match v.to_lowercase().as_str() {
            "ane" | "neural" | "neuralengine" => ComputeUnits::CPUAndNeuralEngine,
            "gpu" => ComputeUnits::CPUAndGPU,
            "cpu" => ComputeUnits::CPUOnly,
            _ => ComputeUnits::All,
        })
        .unwrap_or(ComputeUnits::All);
    let provider = CoreML::default()
        .with_subgraphs(true)
        .with_compute_units(compute_units);
    let dispatch = if matches!(runtime.execution_provider, ExecutionProviderChoice::CoreML) {
        provider.build().error_on_failure()
    } else {
        provider.build()
    };
    Ok(vec![dispatch])
}

#[cfg(not(target_os = "macos"))]
fn coreml_execution_providers(
    _runtime: &EmbedRuntimeConfig,
) -> Result<Vec<ExecutionProviderDispatch>> {
    Err(anyhow!(
        "execution_provider=coreml is only supported on macOS"
    ))
}

#[cfg(feature = "cuda")]
fn cuda_execution_providers(
    runtime: &EmbedRuntimeConfig,
) -> Result<Vec<ExecutionProviderDispatch>> {
    use ort::ep::{CUDA, ExecutionProvider};

    preload_cuda_dependencies(runtime)?;
    let mut provider = CUDA::default();
    if let Some(device_id) = runtime.cuda_device_id {
        provider = provider.with_device_id(device_id);
    }
    if !provider.is_available()? {
        return Err(anyhow!(
            "CUDA execution provider is not available; ensure this binary was built with \
             `--features cuda` and that the required CUDA 12/cuDNN runtime libraries are installed"
        ));
    }
    Ok(vec![provider.build().error_on_failure()])
}

#[cfg(not(feature = "cuda"))]
fn cuda_execution_providers(
    _runtime: &EmbedRuntimeConfig,
) -> Result<Vec<ExecutionProviderDispatch>> {
    Err(anyhow!(
        "execution_provider=cuda requires a memex binary built with cargo feature `cuda`"
    ))
}

/// Rewrite a model initialization error to name the execution provider that failed.
pub(crate) fn provider_init_error(
    runtime: &EmbedRuntimeConfig,
    err: anyhow::Error,
) -> anyhow::Error {
    let requested_provider = runtime.execution_provider;
    match requested_provider.effective() {
        ExecutionProviderChoice::Cuda => anyhow!(
            "failed to initialize CUDA execution provider: {err}. Ensure the binary was \
             built with `--features cuda` and the required CUDA 12/cuDNN libraries are \
             on the dynamic linker path (for example via LD_LIBRARY_PATH)"
        ),
        ExecutionProviderChoice::CoreML
            if matches!(requested_provider, ExecutionProviderChoice::CoreML) =>
        {
            anyhow!("failed to initialize CoreML execution provider: {err}")
        }
        _ => err,
    }
}

enum EmbedBackend {
    Fastembed(TextEmbedding),
    Model2Vec(StaticModel),
    Remote(RemoteEmbedder),
}

pub struct EmbedderHandle {
    backend: EmbedBackend,
    pub dims: usize,
    batch_size: usize,
}

impl EmbedderHandle {
    pub fn with_model(choice: &ModelChoice) -> Result<Self> {
        let runtime = EmbedRuntimeConfig::from_env()?;
        Self::with_model_and_runtime(choice, &runtime)
    }

    /// Load a local model or connect to the remote endpoint in `runtime`.
    ///
    /// A remote model never initializes ONNX Runtime; it sends one probe request to
    /// learn the vector length and fails when `runtime` has no remote endpoint.
    pub fn with_model_and_runtime(
        choice: &ModelChoice,
        runtime: &EmbedRuntimeConfig,
    ) -> Result<Self> {
        crate::profiling::span!("embeddings.model_init");
        let batch_size = runtime.batch_size();
        match choice {
            ModelChoice::Remote(model) => {
                let mut embedder = remote_embedder(model, runtime, batch_size)?;
                let dims = embedder.probe_dimensions().with_context(|| {
                    format!("failed to initialize remote embedding model `{model}`")
                })?;
                Ok(Self {
                    backend: EmbedBackend::Remote(embedder),
                    dims,
                    batch_size,
                })
            }
            ModelChoice::Local(LocalModel::Fastembed(model_type)) => {
                let dims = TextEmbedding::get_model_info(model_type)?.dim;
                let opts = init_options_for_model(model_type.clone(), runtime)?;
                let model = TextEmbedding::try_new(opts)
                    .map_err(|err| provider_init_error(runtime, err))?;
                Ok(Self {
                    backend: EmbedBackend::Fastembed(model),
                    dims,
                    batch_size,
                })
            }
            ModelChoice::Local(LocalModel::Potion) => {
                let model =
                    StaticModel::from_pretrained("minishlab/potion-base-8M", None, None, None)?;
                let dims = model
                    .encode(&[String::from("dimension_check")])
                    .first()
                    .map(|vec| vec.len())
                    .ok_or_else(|| anyhow!("no embedding returned"))?;
                Ok(Self {
                    backend: EmbedBackend::Model2Vec(model),
                    dims,
                    batch_size,
                })
            }
        }
    }

    /// Build a query embedder for stored vectors of `stored` dimensions.
    ///
    /// Fails before any model load or request when `choice` cannot query in this
    /// runtime or a configured remote size differs from `stored`. A remote model
    /// sends no probe; each response must have `stored` dimensions.
    pub fn for_stored_vectors(
        choice: &ModelChoice,
        runtime: &EmbedRuntimeConfig,
        stored: usize,
    ) -> Result<Self> {
        choice.ensure_query_compatible(runtime)?;
        let ModelChoice::Remote(model) = choice else {
            let handle = Self::with_model_and_runtime(choice, runtime)?;
            handle.ensure_dimensions(stored)?;
            return Ok(handle);
        };
        if let Some(configured) = choice.known_dimensions(runtime) {
            check_dimensions(configured, stored)?;
        }
        let batch_size = runtime.batch_size();
        let mut embedder = remote_embedder(model, runtime, batch_size)?;
        embedder.expect_dimensions(stored);
        Ok(Self {
            backend: EmbedBackend::Remote(embedder),
            dims: stored,
            batch_size,
        })
    }

    /// Inputs per embedding call; callers also use it as their flush size.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Reject a query embedder whose vectors cannot be compared with `stored` dimensions.
    pub fn ensure_dimensions(&self, stored: usize) -> Result<()> {
        check_dimensions(self.dims, stored)
    }

    pub fn embed_texts(&mut self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        crate::profiling::span!("embeddings.batch");
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        match &mut self.backend {
            EmbedBackend::Fastembed(model) => Ok(model.embed(texts, Some(self.batch_size))?),
            EmbedBackend::Model2Vec(model) => {
                let input: Vec<String> = texts.iter().map(|t| t.to_string()).collect();
                Ok(model.encode_with_args(&input, Some(512), self.batch_size))
            }
            EmbedBackend::Remote(embedder) => embedder.embed(texts),
        }
    }
}

fn remote_embedder(
    model: &str,
    runtime: &EmbedRuntimeConfig,
    batch_size: usize,
) -> Result<RemoteEmbedder> {
    let Some(remote) = runtime.remote.as_ref() else {
        return Err(anyhow!(
            "vectors use remote embedding model `{model}`, but no remote endpoint is \
             configured; set embeddings = \"remote\" with embedding_base_url, or run \
             `memex embed` to rebuild the vectors with the configured local model"
        ));
    };
    RemoteEmbedder::new(
        remote.endpoint.clone(),
        model.to_string(),
        remote.dimensions,
        batch_size,
    )
}

fn check_dimensions(produced: usize, stored: usize) -> Result<()> {
    if produced == stored {
        return Ok(());
    }
    Err(anyhow!(
        "the embedding model produces {produced} dimensions but the vector index stores \
         {stored}; run `memex embed` to rebuild the vectors"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{EnvVarGuard, env_lock};
    #[cfg(feature = "cuda")]
    use tempfile::TempDir;

    fn test_embedder_from_env() -> EmbedderHandle {
        let choice = std::env::var("MEMEX_MODEL")
            .ok()
            .map(|s| ModelChoice::parse(&s))
            .transpose()
            .expect("parse MEMEX_MODEL")
            .unwrap_or_default();
        EmbedderHandle::with_model(&choice).expect("failed to init embedder")
    }

    #[test]
    fn test_embedder_init() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[
            ("MEMEX_EXECUTION_PROVIDER", Some("auto")),
            ("MEMEX_CUDA_DEVICE_ID", None),
            ("MEMEX_COMPUTE_UNITS", None),
        ]);
        let env_model = std::env::var("MEMEX_MODEL").ok().map(|s| s.to_lowercase());
        let embedder = test_embedder_from_env();
        // Default is Gemma with 768 dims, but env var could change it
        let is_potion = matches!(
            env_model.as_deref(),
            Some("potion")
                | Some("potion8m")
                | Some("potion-8m")
                | Some("potion-base-8m")
                | Some("model2vec")
        );
        if is_potion {
            assert!(embedder.dims > 0);
        } else {
            assert!(embedder.dims == 384 || embedder.dims == 768);
        }
    }

    #[test]
    fn test_embed_single_text() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[
            ("MEMEX_EXECUTION_PROVIDER", Some("auto")),
            ("MEMEX_CUDA_DEVICE_ID", None),
            ("MEMEX_COMPUTE_UNITS", None),
        ]);
        let mut embedder = test_embedder_from_env();
        let texts = vec!["Hello world"];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");
        assert_eq!(embeddings.len(), 1);
        assert_eq!(embeddings[0].len(), embedder.dims);
    }

    #[test]
    fn test_embed_multiple_texts() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[
            ("MEMEX_EXECUTION_PROVIDER", Some("auto")),
            ("MEMEX_CUDA_DEVICE_ID", None),
            ("MEMEX_COMPUTE_UNITS", None),
        ]);
        let mut embedder = test_embedder_from_env();
        let texts = vec!["Hello world", "How are you?", "Rust is great"];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");
        assert_eq!(embeddings.len(), 3);
        for emb in &embeddings {
            assert_eq!(emb.len(), embedder.dims);
        }
    }

    #[test]
    fn test_embed_empty() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[
            ("MEMEX_EXECUTION_PROVIDER", Some("auto")),
            ("MEMEX_CUDA_DEVICE_ID", None),
            ("MEMEX_COMPUTE_UNITS", None),
        ]);
        let mut embedder = test_embedder_from_env();
        let texts: Vec<&str> = vec![];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");
        assert!(embeddings.is_empty());
    }

    #[test]
    fn test_embeddings_are_different() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[
            ("MEMEX_EXECUTION_PROVIDER", Some("auto")),
            ("MEMEX_CUDA_DEVICE_ID", None),
            ("MEMEX_COMPUTE_UNITS", None),
        ]);
        let mut embedder = test_embedder_from_env();
        let texts = vec!["cats are cute", "dogs are loyal"];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");
        assert_ne!(embeddings[0], embeddings[1]);
    }

    #[test]
    fn test_similar_texts_have_similar_embeddings() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[
            ("MEMEX_EXECUTION_PROVIDER", Some("auto")),
            ("MEMEX_CUDA_DEVICE_ID", None),
            ("MEMEX_COMPUTE_UNITS", None),
        ]);
        let mut embedder = test_embedder_from_env();
        let texts = vec!["the cat sat on the mat", "a cat is sitting on a mat"];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");

        let dot: f32 = embeddings[0]
            .iter()
            .zip(embeddings[1].iter())
            .map(|(a, b)| a * b)
            .sum();
        let norm0: f32 = embeddings[0].iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm1: f32 = embeddings[1].iter().map(|x| x * x).sum::<f32>().sqrt();
        let cosine_sim = dot / (norm0 * norm1);

        assert!(
            cosine_sim > 0.8,
            "expected high similarity, got {cosine_sim}"
        );
    }

    #[test]
    fn test_parse_potion_model() {
        let choice = ModelChoice::parse("potion").expect("parse potion");
        assert_eq!(choice, ModelChoice::potion());
        let choice = ModelChoice::parse("potion-base-8m").expect("parse potion-base-8m");
        assert_eq!(choice, ModelChoice::potion());
        let choice = ModelChoice::parse("model2vec").expect("parse model2vec");
        assert_eq!(choice, ModelChoice::potion());
    }

    #[test]
    fn legacy_identities_round_trip_byte_identical() {
        for name in ["minilm", "bge", "nomic", "gemma", "potion"] {
            let choice = ModelChoice::parse(name).expect("parse legacy name");
            assert_eq!(choice.identity(), name);
            assert_eq!(
                ModelChoice::parse(&choice.identity()).expect("reparse identity"),
                choice
            );
        }
        assert_eq!(ModelChoice::default().identity(), "gemma");
    }

    #[test]
    fn fastembed_names_equal_to_legacy_models_collapse_to_legacy_identity() {
        let choice = ModelChoice::parse("AllMiniLML6V2").expect("parse fastembed name");
        assert_eq!(choice, ModelChoice::minilm());
        assert_eq!(choice.identity(), "minilm");
        assert_eq!(
            ModelChoice::parse("bgesmallenv15")
                .expect("parse lowercase fastembed name")
                .identity(),
            "bge"
        );
    }

    #[test]
    fn fastembed_variant_names_parse_and_round_trip() {
        let choice = ModelChoice::parse("bgelargeenv15").expect("parse fastembed name");
        assert_eq!(
            choice,
            ModelChoice::Local(LocalModel::Fastembed(EmbeddingModel::BGELargeENV15))
        );
        assert_eq!(choice.identity(), "BGELargeENV15");
        assert_eq!(
            ModelChoice::parse(&choice.identity()).expect("reparse identity"),
            choice
        );
        assert_eq!(
            choice.known_dimensions(&EmbedRuntimeConfig::default()),
            Some(1024)
        );
    }

    #[test]
    fn legacy_models_keep_their_native_dimensions() {
        let runtime = EmbedRuntimeConfig::default();
        assert_eq!(ModelChoice::minilm().known_dimensions(&runtime), Some(384));
        assert_eq!(
            ModelChoice::bge_small().known_dimensions(&runtime),
            Some(384)
        );
        assert_eq!(ModelChoice::nomic().known_dimensions(&runtime), Some(768));
        assert_eq!(ModelChoice::gemma().known_dimensions(&runtime), Some(768));
        assert_eq!(ModelChoice::potion().known_dimensions(&runtime), None);
    }

    #[test]
    fn remote_identity_round_trips_and_keeps_case() {
        let choice = ModelChoice::parse("remote:Text-Embedding-3-Small").expect("parse remote");
        assert_eq!(
            choice,
            ModelChoice::Remote("Text-Embedding-3-Small".to_string())
        );
        assert_eq!(choice.identity(), "remote:Text-Embedding-3-Small");
        assert_eq!(
            ModelChoice::parse(&choice.identity()).expect("reparse identity"),
            choice
        );
        assert!(ModelChoice::parse("remote:").is_err());
        assert!(ModelChoice::parse("remote: ").is_err());
    }

    #[test]
    fn remote_model_names_reject_whitespace_and_control_characters() {
        for name in [
            "remote:a\nb",
            "remote:a b",
            "remote:a\tb",
            "remote:a\u{7f}b",
        ] {
            let error = ModelChoice::parse(name)
                .expect_err("reject remote model name")
                .to_string();
            assert!(
                error.contains("whitespace or control characters"),
                "{error}"
            );
        }
        assert!(ModelChoice::remote("org/model-v1.5:latest").is_ok());
    }

    #[test]
    fn query_embedder_for_remote_vectors_sends_no_probe() {
        let runtime = EmbedRuntimeConfig {
            remote: Some(RemoteEmbedConfig {
                endpoint: unroutable_endpoint(),
                dimensions: None,
            }),
            ..EmbedRuntimeConfig::default()
        };
        // A probe against this endpoint would fail construction.
        let embedder = EmbedderHandle::for_stored_vectors(
            &ModelChoice::Remote("m".to_string()),
            &runtime,
            1536,
        )
        .expect("build remote query embedder without a request");
        assert_eq!(embedder.dims, 1536);

        let runtime = EmbedRuntimeConfig {
            remote: Some(RemoteEmbedConfig {
                endpoint: unroutable_endpoint(),
                dimensions: Some(512),
            }),
            ..EmbedRuntimeConfig::default()
        };
        let error = EmbedderHandle::for_stored_vectors(
            &ModelChoice::Remote("m".to_string()),
            &runtime,
            1536,
        )
        .err()
        .expect("reject configured size that differs from stored vectors")
        .to_string();
        assert!(error.contains("produces 512 dimensions"), "{error}");
        assert!(error.contains("memex embed"), "{error}");
    }

    #[test]
    fn remote_known_dimensions_come_from_runtime() {
        let choice = ModelChoice::Remote("m".to_string());
        assert_eq!(
            choice.known_dimensions(&EmbedRuntimeConfig::default()),
            None
        );
        let runtime = EmbedRuntimeConfig {
            remote: Some(RemoteEmbedConfig {
                endpoint: unroutable_endpoint(),
                dimensions: Some(512),
            }),
            ..EmbedRuntimeConfig::default()
        };
        assert_eq!(choice.known_dimensions(&runtime), Some(512));
    }

    #[test]
    fn rejects_unsupported_fastembed_models() {
        let unsupported = TextEmbedding::list_supported_models()
            .into_iter()
            .map(|info| info.model)
            .filter(|model| !is_supported_fastembed_model(model))
            .collect::<Vec<_>>();
        assert!(unsupported.contains(&EmbeddingModel::AllMiniLML6V2Q));
        for model in unsupported {
            let name = format!("{model:?}");
            let error = ModelChoice::parse(&name)
                .expect_err("reject unsupported model")
                .to_string();
            assert!(error.contains("is not supported"), "{error}");
            assert!(!error.contains(&format!(" {name},")), "{error}");
            assert!(!error.ends_with(&format!(" {name}")), "{error}");
        }
        assert_eq!(
            ModelChoice::parse("BGESmallENV15Q").expect("parse supported variant"),
            ModelChoice::Local(LocalModel::Fastembed(EmbeddingModel::BGESmallENV15Q))
        );
        assert!(ModelChoice::parse("EmbeddingGemma300MQ4").is_ok());
    }

    #[test]
    fn unknown_model_error_lists_catalog() {
        let error = ModelChoice::parse("no-such-model")
            .expect_err("reject unknown model")
            .to_string();
        assert!(error.contains("no-such-model"));
        assert!(error.contains("remote:<model>"));
        assert!(error.contains("BGELargeENV15"));
    }

    #[test]
    fn remote_model_without_endpoint_names_the_model() {
        let error = EmbedderHandle::with_model_and_runtime(
            &ModelChoice::Remote("text-embedding-3-small".to_string()),
            &EmbedRuntimeConfig::default(),
        )
        .err()
        .expect("reject remote model without endpoint")
        .to_string();
        assert!(error.contains("text-embedding-3-small"));
        assert!(error.contains("embedding_base_url"));
        assert!(error.contains("memex embed"));
    }

    #[test]
    fn local_vectors_are_rejected_for_queries_in_remote_mode() {
        let runtime = EmbedRuntimeConfig {
            remote: Some(RemoteEmbedConfig {
                endpoint: unroutable_endpoint(),
                dimensions: None,
            }),
            ..EmbedRuntimeConfig::default()
        };
        let error = ModelChoice::gemma()
            .ensure_query_compatible(&runtime)
            .expect_err("reject local vectors in remote mode")
            .to_string();
        assert_eq!(
            error,
            "vectors use local model gemma but embeddings = \"remote\"; run `memex embed` to \
             rebuild with the remote model"
        );
        ModelChoice::Remote("m".to_string())
            .ensure_query_compatible(&runtime)
            .expect("remote vectors in remote mode");
        ModelChoice::gemma()
            .ensure_query_compatible(&EmbedRuntimeConfig::default())
            .expect("local vectors in local mode");
    }

    fn unroutable_endpoint() -> RemoteEndpoint {
        RemoteEndpoint {
            base_url: "http://127.0.0.1:9/v1".to_string(),
            api_key: None,
            timeout: std::time::Duration::from_secs(1),
            max_retries: 0,
        }
    }

    #[test]
    fn test_parse_execution_provider() {
        assert_eq!(
            ExecutionProviderChoice::parse("auto").expect("parse auto"),
            ExecutionProviderChoice::Auto
        );
        assert_eq!(
            ExecutionProviderChoice::parse("cpu").expect("parse cpu"),
            ExecutionProviderChoice::Cpu
        );
        assert_eq!(
            ExecutionProviderChoice::parse("coreml").expect("parse coreml"),
            ExecutionProviderChoice::CoreML
        );
        assert_eq!(
            ExecutionProviderChoice::parse("cuda").expect("parse cuda"),
            ExecutionProviderChoice::Cuda
        );
    }

    #[test]
    fn test_apply_runtime_env_sets_expected_vars() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[
            ("MEMEX_EXECUTION_PROVIDER", None),
            ("MEMEX_CUDA_DEVICE_ID", None),
            ("MEMEX_COMPUTE_UNITS", None),
            ("MEMEX_CUDA_LIBRARY_PATHS", None),
            ("MEMEX_CUDNN_LIBRARY_PATHS", None),
        ]);
        apply_runtime_env(
            ExecutionProviderChoice::Cuda,
            Some("all"),
            Some(2),
            &[PathBuf::from("/opt/cuda/lib64")],
            &[PathBuf::from("/opt/cudnn/lib64")],
        )
        .expect("apply runtime env");
        assert_eq!(
            std::env::var("MEMEX_EXECUTION_PROVIDER").ok().as_deref(),
            Some("cuda")
        );
        assert_eq!(
            std::env::var("MEMEX_COMPUTE_UNITS").ok().as_deref(),
            Some("all")
        );
        assert_eq!(
            std::env::var("MEMEX_CUDA_DEVICE_ID").ok().as_deref(),
            Some("2")
        );
        assert_eq!(
            std::env::var_os("MEMEX_CUDA_LIBRARY_PATHS")
                .map(|value| std::env::split_paths(&value).collect::<Vec<_>>()),
            Some(vec![PathBuf::from("/opt/cuda/lib64")])
        );
        assert_eq!(
            std::env::var_os("MEMEX_CUDNN_LIBRARY_PATHS")
                .map(|value| std::env::split_paths(&value).collect::<Vec<_>>()),
            Some(vec![PathBuf::from("/opt/cudnn/lib64")])
        );
    }

    #[cfg(feature = "cuda")]
    fn temp_virtual_env(packages: &[&str]) -> TempDir {
        let dir = TempDir::new().expect("create temp venv");
        for package in packages {
            std::fs::create_dir_all(
                dir.path()
                    .join("lib")
                    .join("python3.11")
                    .join("site-packages")
                    .join("nvidia")
                    .join(package)
                    .join("lib"),
            )
            .expect("create package lib dir");
        }
        dir
    }

    #[cfg(feature = "cuda")]
    #[test]
    fn test_python_nvidia_lib_dirs_detects_site_packages() {
        let dir = temp_virtual_env(&["cublas", "cuda_runtime", "cudnn"]);
        let libs = python_nvidia_lib_dirs(dir.path());
        assert!(
            libs.contains(
                &dir.path()
                    .join("lib/python3.11/site-packages/nvidia/cublas/lib")
            )
        );
        assert!(
            libs.contains(
                &dir.path()
                    .join("lib/python3.11/site-packages/nvidia/cuda_runtime/lib")
            )
        );
        assert!(
            libs.contains(
                &dir.path()
                    .join("lib/python3.11/site-packages/nvidia/cudnn/lib")
            )
        );
    }

    #[cfg(feature = "cuda")]
    #[test]
    fn test_candidate_cuda_library_dirs_include_virtual_env_packages() {
        let _guard = env_lock();
        let dir = temp_virtual_env(&["cublas", "cuda_runtime"]);
        let venv = dir.path().as_os_str();
        let _env = EnvVarGuard::set_os(&[
            ("VIRTUAL_ENV", Some(venv)),
            ("CONDA_PREFIX", None),
            ("MEMEX_CUDA_LIBRARY_PATHS", None),
        ]);
        let dirs = candidate_cuda_library_dirs(&[]);
        assert!(
            dirs.contains(
                &dir.path()
                    .join("lib/python3.11/site-packages/nvidia/cublas/lib")
                    .canonicalize()
                    .expect("canonical cublas dir")
            )
        );
        assert!(
            dirs.contains(
                &dir.path()
                    .join("lib/python3.11/site-packages/nvidia/cuda_runtime/lib")
                    .canonicalize()
                    .expect("canonical cuda runtime dir")
            )
        );
    }

    #[cfg(feature = "cuda")]
    #[test]
    fn test_candidate_cudnn_library_dirs_include_explicit_env_paths() {
        let _guard = env_lock();
        let dir = TempDir::new().expect("create temp dir");
        let cudnn_dir = dir.path().join("cudnn/lib");
        std::fs::create_dir_all(&cudnn_dir).expect("create cudnn dir");
        let joined = std::env::join_paths([&cudnn_dir]).expect("join cudnn paths");
        let _env = EnvVarGuard::set_os(&[
            ("MEMEX_CUDNN_LIBRARY_PATHS", Some(joined.as_os_str())),
            ("VIRTUAL_ENV", None),
            ("CONDA_PREFIX", None),
        ]);
        let dirs = candidate_cudnn_library_dirs(&resolve_library_paths_from_env(
            MEMEX_CUDNN_LIBRARY_PATHS_ENV,
        ));
        assert!(dirs.contains(&cudnn_dir.canonicalize().expect("canonical cudnn dir")));
    }

    #[test]
    fn test_potion_embedding() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set(&[
            ("MEMEX_EXECUTION_PROVIDER", Some("auto")),
            ("MEMEX_CUDA_DEVICE_ID", None),
            ("MEMEX_COMPUTE_UNITS", None),
        ]);
        let mut embedder =
            EmbedderHandle::with_model(&ModelChoice::potion()).expect("init potion embedder");
        let texts = vec!["potion model smoke test", "another short sentence"];
        let embeddings = embedder.embed_texts(&texts).expect("embed with potion");
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].len(), embedder.dims);
    }
    #[test]
    fn execution_providers_resolve_auto_and_reject_unavailable_providers() {
        let runtime = |execution_provider| EmbedRuntimeConfig {
            execution_provider,
            ..EmbedRuntimeConfig::default()
        };
        assert!(
            execution_providers(&runtime(ExecutionProviderChoice::Cpu))
                .expect("cpu")
                .is_empty()
        );
        let auto = execution_providers(&runtime(ExecutionProviderChoice::Auto));
        #[cfg(not(target_os = "macos"))]
        assert!(auto.expect("auto is cpu").is_empty());
        #[cfg(target_os = "macos")]
        assert_eq!(auto.expect("auto is coreml").len(), 1);
        #[cfg(not(target_os = "macos"))]
        assert_eq!(
            execution_providers(&runtime(ExecutionProviderChoice::CoreML))
                .expect_err("coreml off macOS")
                .to_string(),
            "execution_provider=coreml is only supported on macOS"
        );
        #[cfg(not(feature = "cuda"))]
        assert_eq!(
            execution_providers(&runtime(ExecutionProviderChoice::Cuda))
                .expect_err("cuda without the feature")
                .to_string(),
            "execution_provider=cuda requires a memex binary built with cargo feature `cuda`"
        );
        let options = init_options_for_model(
            EmbeddingModel::AllMiniLML6V2,
            &runtime(ExecutionProviderChoice::Cpu),
        )
        .expect("cpu options");
        assert!(options.execution_providers.is_empty());
        assert!(!options.show_download_progress);
    }

    #[test]
    fn provider_init_errors_name_the_failed_provider() {
        let runtime = |execution_provider| EmbedRuntimeConfig {
            execution_provider,
            ..EmbedRuntimeConfig::default()
        };
        let error = provider_init_error(&runtime(ExecutionProviderChoice::Cuda), anyhow!("boom"));
        assert!(
            error
                .to_string()
                .starts_with("failed to initialize CUDA execution provider: boom.")
        );
        let error = provider_init_error(&runtime(ExecutionProviderChoice::CoreML), anyhow!("boom"));
        assert_eq!(
            error.to_string(),
            "failed to initialize CoreML execution provider: boom"
        );
        for provider in [ExecutionProviderChoice::Cpu, ExecutionProviderChoice::Auto] {
            assert_eq!(
                provider_init_error(&runtime(provider), anyhow!("boom")).to_string(),
                "boom"
            );
        }
    }
}
