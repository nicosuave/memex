use anyhow::{Result, anyhow};
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use model2vec_rs::model::StaticModel;
use std::path::PathBuf;

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
    "cublasLt64_12.dll",
    "cublas64_12.dll",
    "cufft64_11.dll",
    "cudart64_12.dll",
];

#[cfg(all(feature = "cuda", not(windows)))]
const CUDA_DYLIBS: &[&str] = &[
    "libcublasLt.so.12",
    "libcublas.so.12",
    "libnvrtc.so.12",
    "libcurand.so.10",
    "libcufft.so.11",
    "libcudart.so.12",
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

/// Supported embedding models
#[derive(Debug, Clone, Copy, Default)]
pub enum ModelChoice {
    /// AllMiniLML6V2 - 22M params, 384 dims, very fast
    MiniLM,
    /// BGESmallENV15 - 33M params, 384 dims, good balance
    BGESmall,
    /// NomicEmbedTextV15 - 137M params, 768 dims, good quality
    Nomic,
    /// EmbeddingGemma300M - 300M params, 768 dims, highest quality but slowest
    #[default]
    Gemma,
    /// PotionBase8M - 8M params, model2vec backend, tiny and fast
    Potion,
}

impl ModelChoice {
    fn fastembed_config(self) -> Option<(EmbeddingModel, usize)> {
        match self {
            ModelChoice::MiniLM => Some((EmbeddingModel::AllMiniLML6V2, 384)),
            ModelChoice::BGESmall => Some((EmbeddingModel::BGESmallENV15, 384)),
            ModelChoice::Nomic => Some((EmbeddingModel::NomicEmbedTextV15, 768)),
            ModelChoice::Gemma => Some((EmbeddingModel::EmbeddingGemma300M, 768)),
            ModelChoice::Potion => None,
        }
    }

    /// Parse from string (env var or config)
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "minilm" | "mini" | "fast" => Ok(ModelChoice::MiniLM),
            "bge" | "bge-small" | "bgesmall" => Ok(ModelChoice::BGESmall),
            "nomic" => Ok(ModelChoice::Nomic),
            "gemma" | "embeddinggemma" | "default" => Ok(ModelChoice::Gemma),
            "potion" | "potion8m" | "potion-8m" | "potion-base-8m" | "model2vec" => {
                Ok(ModelChoice::Potion)
            }
            _ => Err(anyhow!(
                "unknown model '{s}', options: minilm, bge, nomic, gemma, potion"
            )),
        }
    }
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
}

impl EmbedRuntimeConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            execution_provider: resolve_execution_provider_from_env()?,
            compute_units: std::env::var(MEMEX_COMPUTE_UNITS_ENV).ok(),
            cuda_device_id: resolve_cuda_device_id_from_env()?,
            cuda_library_paths: resolve_library_paths_from_env(MEMEX_CUDA_LIBRARY_PATHS_ENV),
            cudnn_library_paths: resolve_library_paths_from_env(MEMEX_CUDNN_LIBRARY_PATHS_ENV),
        })
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
    for var in ["CUDA_PATH", "CUDA_HOME", "CUDA_ROOT"] {
        if let Some(root) = std::env::var_os(var).map(PathBuf::from) {
            dirs.push(root.clone());
            dirs.push(root.join("lib64"));
            dirs.push(root.join("lib"));
            dirs.push(root.join("targets/x86_64-linux/lib"));
            dirs.push(root.join("bin"));
        }
    }

    #[cfg(target_os = "linux")]
    dirs.extend([
        PathBuf::from("/usr/local/cuda/lib64"),
        PathBuf::from("/usr/local/cuda/lib"),
        PathBuf::from("/usr/local/cuda/targets/x86_64-linux/lib"),
        PathBuf::from("/usr/lib/x86_64-linux-gnu"),
        PathBuf::from("/usr/lib64"),
        PathBuf::from("/usr/lib"),
        PathBuf::from("/lib/x86_64-linux-gnu"),
        PathBuf::from("/lib64"),
    ]);
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
    let effective_provider = runtime.execution_provider.effective();
    let opts = InitOptions::new(model_type).with_show_download_progress(false);

    match effective_provider {
        ExecutionProviderChoice::Auto => unreachable!("auto should resolve to a concrete provider"),
        ExecutionProviderChoice::Cpu => Ok(opts),
        ExecutionProviderChoice::CoreML => init_options_with_coreml(opts, runtime),
        ExecutionProviderChoice::Cuda => init_options_with_cuda(opts, runtime),
    }
}

#[cfg(target_os = "macos")]
fn init_options_with_coreml(
    opts: InitOptions,
    runtime: &EmbedRuntimeConfig,
) -> Result<InitOptions> {
    use ort::execution_providers::coreml::{CoreMLComputeUnits, CoreMLExecutionProvider};

    let compute_units = runtime
        .compute_units
        .as_deref()
        .map(|v| match v.to_lowercase().as_str() {
            "ane" | "neural" | "neuralengine" => CoreMLComputeUnits::CPUAndNeuralEngine,
            "gpu" => CoreMLComputeUnits::CPUAndGPU,
            "cpu" => CoreMLComputeUnits::CPUOnly,
            _ => CoreMLComputeUnits::All,
        })
        .unwrap_or(CoreMLComputeUnits::All);
    let provider = CoreMLExecutionProvider::default()
        .with_subgraphs(true)
        .with_compute_units(compute_units);
    let dispatch = if matches!(runtime.execution_provider, ExecutionProviderChoice::CoreML) {
        provider.build().error_on_failure()
    } else {
        provider.build()
    };
    Ok(opts.with_execution_providers(vec![dispatch]))
}

#[cfg(not(target_os = "macos"))]
fn init_options_with_coreml(
    _opts: InitOptions,
    _runtime: &EmbedRuntimeConfig,
) -> Result<InitOptions> {
    Err(anyhow!(
        "execution_provider=coreml is only supported on macOS"
    ))
}

#[cfg(feature = "cuda")]
fn init_options_with_cuda(opts: InitOptions, runtime: &EmbedRuntimeConfig) -> Result<InitOptions> {
    use ort::execution_providers::{CUDAExecutionProvider, ExecutionProvider};

    preload_cuda_dependencies(runtime)?;
    let mut provider = CUDAExecutionProvider::default();
    if let Some(device_id) = runtime.cuda_device_id {
        provider = provider.with_device_id(device_id);
    }
    if !provider.is_available()? {
        return Err(anyhow!(
            "CUDA execution provider is not available; ensure this binary was built with \
             `--features cuda` and that the required CUDA 12/cuDNN runtime libraries are installed"
        ));
    }
    Ok(opts.with_execution_providers(vec![provider.build().error_on_failure()]))
}

#[cfg(not(feature = "cuda"))]
fn init_options_with_cuda(
    _opts: InitOptions,
    _runtime: &EmbedRuntimeConfig,
) -> Result<InitOptions> {
    Err(anyhow!(
        "execution_provider=cuda requires a memex binary built with cargo feature `cuda`"
    ))
}

enum EmbedBackend {
    Fastembed(TextEmbedding),
    Model2Vec(StaticModel),
}

pub struct EmbedderHandle {
    backend: EmbedBackend,
    pub dims: usize,
}

impl EmbedderHandle {
    pub fn with_model(choice: ModelChoice) -> Result<Self> {
        let runtime = EmbedRuntimeConfig::from_env()?;
        Self::with_model_and_runtime(choice, &runtime)
    }

    pub fn with_model_and_runtime(
        choice: ModelChoice,
        runtime: &EmbedRuntimeConfig,
    ) -> Result<Self> {
        if let Some((model_type, dims)) = choice.fastembed_config() {
            let requested_provider = runtime.execution_provider;
            let effective_provider = requested_provider.effective();
            let opts = init_options_for_model(model_type, runtime)?;
            let model = TextEmbedding::try_new(opts).map_err(|err| match effective_provider {
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
            })?;
            Ok(Self {
                backend: EmbedBackend::Fastembed(model),
                dims,
            })
        } else {
            let model = StaticModel::from_pretrained("minishlab/potion-base-8M", None, None, None)?;
            let dims = model
                .encode(&[String::from("dimension_check")])
                .first()
                .map(|vec| vec.len())
                .ok_or_else(|| anyhow!("no embedding returned"))?;
            Ok(Self {
                backend: EmbedBackend::Model2Vec(model),
                dims,
            })
        }
    }

    pub fn embed_texts(&mut self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        match &mut self.backend {
            EmbedBackend::Fastembed(model) => Ok(model.embed(texts, None)?),
            EmbedBackend::Model2Vec(model) => {
                let input: Vec<String> = texts.iter().map(|t| t.to_string()).collect();
                Ok(model.encode_with_args(&input, Some(512), 64))
            }
        }
    }
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
        EmbedderHandle::with_model(choice).expect("failed to init embedder")
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
        assert!(matches!(choice, ModelChoice::Potion));
        let choice = ModelChoice::parse("potion-base-8m").expect("parse potion-base-8m");
        assert!(matches!(choice, ModelChoice::Potion));
        let choice = ModelChoice::parse("model2vec").expect("parse model2vec");
        assert!(matches!(choice, ModelChoice::Potion));
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
            EmbedderHandle::with_model(ModelChoice::Potion).expect("init potion embedder");
        let texts = vec!["potion model smoke test", "another short sentence"];
        let embeddings = embedder.embed_texts(&texts).expect("embed with potion");
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].len(), embedder.dims);
    }
}
