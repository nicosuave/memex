use anyhow::Result;
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

pub struct EmbedderHandle {
    model: TextEmbedding,
    pub dims: usize,
}

impl EmbedderHandle {
    pub fn new() -> Result<Self> {
        #[cfg(target_os = "macos")]
        let opts = {
            use ort::execution_providers::CoreMLExecutionProvider;
            InitOptions::new(EmbeddingModel::EmbeddingGemma300M)
                .with_show_download_progress(false)
                .with_execution_providers(vec![CoreMLExecutionProvider::default().build()])
        };

        #[cfg(not(target_os = "macos"))]
        let opts =
            InitOptions::new(EmbeddingModel::EmbeddingGemma300M).with_show_download_progress(false);

        let model = TextEmbedding::try_new(opts)?;
        // EmbeddingGemma outputs 768-dim embeddings
        let dims = 768;
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
        assert_eq!(embedder.dims, 768);
    }

    #[test]
    fn test_embed_single_text() {
        let mut embedder = EmbedderHandle::new().expect("failed to init embedder");
        let texts = vec!["Hello world"];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");
        assert_eq!(embeddings.len(), 1);
        assert_eq!(embeddings[0].len(), 768);
    }

    #[test]
    fn test_embed_multiple_texts() {
        let mut embedder = EmbedderHandle::new().expect("failed to init embedder");
        let texts = vec!["Hello world", "How are you?", "Rust is great"];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");
        assert_eq!(embeddings.len(), 3);
        for emb in &embeddings {
            assert_eq!(emb.len(), 768);
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
        // Embeddings for different texts should be different
        assert_ne!(embeddings[0], embeddings[1]);
    }

    #[test]
    fn test_similar_texts_have_similar_embeddings() {
        let mut embedder = EmbedderHandle::new().expect("failed to init embedder");
        let texts = vec!["the cat sat on the mat", "a cat is sitting on a mat"];
        let embeddings = embedder.embed_texts(&texts).expect("failed to embed");

        // Compute cosine similarity
        let dot: f32 = embeddings[0]
            .iter()
            .zip(embeddings[1].iter())
            .map(|(a, b)| a * b)
            .sum();
        let norm0: f32 = embeddings[0].iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm1: f32 = embeddings[1].iter().map(|x| x * x).sum::<f32>().sqrt();
        let cosine_sim = dot / (norm0 * norm1);

        // Similar sentences should have high cosine similarity (> 0.8)
        assert!(
            cosine_sim > 0.8,
            "expected high similarity, got {}",
            cosine_sim
        );
    }
}
