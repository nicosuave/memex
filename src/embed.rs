use anyhow::{Result, anyhow};
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use model2vec_rs::model::StaticModel;

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

enum EmbedBackend {
    Fastembed(TextEmbedding),
    Model2Vec(StaticModel),
}

pub struct EmbedderHandle {
    backend: EmbedBackend,
    pub dims: usize,
}

impl EmbedderHandle {
    pub fn new() -> Result<Self> {
        // Check MEMEX_MODEL env var, default to Gemma
        let choice = std::env::var("MEMEX_MODEL")
            .ok()
            .map(|s| ModelChoice::parse(&s))
            .transpose()?
            .unwrap_or_default();

        Self::with_model(choice)
    }

    pub fn with_model(choice: ModelChoice) -> Result<Self> {
        if let Some((model_type, dims)) = choice.fastembed_config() {
            // Set thread count for ONNX Runtime to use all cores
            let num_cpus = std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(8);
            unsafe {
                std::env::set_var("OMP_NUM_THREADS", num_cpus.to_string());
                std::env::set_var("ORT_NUM_THREADS", num_cpus.to_string());
            }

            #[cfg(target_os = "macos")]
            let opts = {
                use ort::execution_providers::coreml::{
                    CoreMLComputeUnits, CoreMLExecutionProvider,
                };
                let compute_units = std::env::var("MEMEX_COMPUTE_UNITS")
                    .ok()
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
                InitOptions::new(model_type)
                    .with_show_download_progress(false)
                    .with_execution_providers(vec![provider.build()])
            };

            #[cfg(not(target_os = "macos"))]
            let opts = InitOptions::new(model_type).with_show_download_progress(false);

            let model = TextEmbedding::try_new(opts)?;
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
    use std::sync::{Mutex, OnceLock};

    fn fastembed_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("lock fastembed")
    }

    #[test]
    fn test_embedder_init() {
        let _guard = fastembed_test_lock();
        let env_model = std::env::var("MEMEX_MODEL").ok().map(|s| s.to_lowercase());
        let embedder = EmbedderHandle::new().expect("failed to init embedder");
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
        let _guard = fastembed_test_lock();
        let mut embedder = EmbedderHandle::new().expect("failed to init embedder");
        let texts = vec!["Hello world"];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");
        assert_eq!(embeddings.len(), 1);
        assert_eq!(embeddings[0].len(), embedder.dims);
    }

    #[test]
    fn test_embed_multiple_texts() {
        let _guard = fastembed_test_lock();
        let mut embedder = EmbedderHandle::new().expect("failed to init embedder");
        let texts = vec!["Hello world", "How are you?", "Rust is great"];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");
        assert_eq!(embeddings.len(), 3);
        for emb in &embeddings {
            assert_eq!(emb.len(), embedder.dims);
        }
    }

    #[test]
    fn test_embed_empty() {
        let _guard = fastembed_test_lock();
        let mut embedder = EmbedderHandle::new().expect("failed to init embedder");
        let texts: Vec<&str> = vec![];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");
        assert!(embeddings.is_empty());
    }

    #[test]
    fn test_embeddings_are_different() {
        let _guard = fastembed_test_lock();
        let mut embedder = EmbedderHandle::new().expect("failed to init embedder");
        let texts = vec!["cats are cute", "dogs are loyal"];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");
        assert_ne!(embeddings[0], embeddings[1]);
    }

    #[test]
    fn test_similar_texts_have_similar_embeddings() {
        let _guard = fastembed_test_lock();
        let mut embedder = EmbedderHandle::new().expect("failed to init embedder");
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
    fn test_potion_embedding() {
        let _guard = fastembed_test_lock();
        let mut embedder =
            EmbedderHandle::with_model(ModelChoice::Potion).expect("init potion embedder");
        let texts = vec!["potion model smoke test", "another short sentence"];
        let embeddings = embedder.embed_texts(&texts).expect("embed with potion");
        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].len(), embedder.dims);
    }
}
