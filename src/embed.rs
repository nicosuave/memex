use anyhow::{Result, anyhow};
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

/// Supported embedding models with their dimensions
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
}

impl ModelChoice {
    fn to_fastembed(self) -> EmbeddingModel {
        match self {
            ModelChoice::MiniLM => EmbeddingModel::AllMiniLML6V2,
            ModelChoice::BGESmall => EmbeddingModel::BGESmallENV15,
            ModelChoice::Nomic => EmbeddingModel::NomicEmbedTextV15,
            ModelChoice::Gemma => EmbeddingModel::EmbeddingGemma300M,
        }
    }

    fn dims(self) -> usize {
        match self {
            ModelChoice::MiniLM => 384,
            ModelChoice::BGESmall => 384,
            ModelChoice::Nomic => 768,
            ModelChoice::Gemma => 768,
        }
    }

    /// Parse from string (env var or config)
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "minilm" | "mini" | "fast" => Ok(ModelChoice::MiniLM),
            "bge" | "bge-small" | "bgesmall" => Ok(ModelChoice::BGESmall),
            "nomic" => Ok(ModelChoice::Nomic),
            "gemma" | "embeddinggemma" | "default" => Ok(ModelChoice::Gemma),
            _ => Err(anyhow!(
                "unknown model '{s}', options: minilm, bge, nomic, gemma"
            )),
        }
    }
}

pub struct EmbedderHandle {
    model: TextEmbedding,
    pub dims: usize,
}

impl EmbedderHandle {
    pub fn new() -> Result<Self> {
        // Check MEMEX_MODEL env var, default to Gemma
        let choice = std::env::var("MEMEX_MODEL")
            .ok()
            .map(|s| ModelChoice::from_str(&s))
            .transpose()?
            .unwrap_or_default();

        Self::with_model(choice)
    }

    pub fn with_model(choice: ModelChoice) -> Result<Self> {
        let model_type = choice.to_fastembed();
        let dims = choice.dims();

        #[cfg(target_os = "macos")]
        let opts = {
            use ort::execution_providers::CoreMLExecutionProvider;
            InitOptions::new(model_type)
                .with_show_download_progress(false)
                .with_execution_providers(vec![CoreMLExecutionProvider::default().build()])
        };

        #[cfg(not(target_os = "macos"))]
        let opts = InitOptions::new(model_type).with_show_download_progress(false);

        let model = TextEmbedding::try_new(opts)?;
        Ok(Self { model, dims })
    }

    pub fn embed_texts(&mut self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let embeddings = self.model.embed(texts, None)?;
        Ok(embeddings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embedder_init() {
        let embedder = EmbedderHandle::new().expect("failed to init embedder");
        // Default is Gemma with 768 dims, but env var could change it
        assert!(embedder.dims == 384 || embedder.dims == 768);
    }

    #[test]
    fn test_embed_single_text() {
        let mut embedder = EmbedderHandle::new().expect("failed to init embedder");
        let texts = vec!["Hello world"];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");
        assert_eq!(embeddings.len(), 1);
        assert_eq!(embeddings[0].len(), embedder.dims);
    }

    #[test]
    fn test_embed_multiple_texts() {
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
        let mut embedder = EmbedderHandle::new().expect("failed to init embedder");
        let texts: Vec<&str> = vec![];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");
        assert!(embeddings.is_empty());
    }

    #[test]
    fn test_embeddings_are_different() {
        let mut embedder = EmbedderHandle::new().expect("failed to init embedder");
        let texts = vec!["cats are cute", "dogs are loyal"];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");
        assert_ne!(embeddings[0], embeddings[1]);
    }

    #[test]
    fn test_similar_texts_have_similar_embeddings() {
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
            "expected high similarity, got {}",
            cosine_sim
        );
    }
}
