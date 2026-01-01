use anyhow::Result;
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

fn main() -> Result<()> {
    // Respect MEMEX_MODEL env var
    let (model_type, dims) = match std::env::var("MEMEX_MODEL")
        .ok()
        .as_deref()
        .map(str::to_lowercase)
        .as_deref()
    {
        Some("minilm" | "mini" | "fast") => (EmbeddingModel::AllMiniLML6V2, 384),
        Some("bge" | "bge-small" | "bgesmall") => (EmbeddingModel::BGESmallENV15, 384),
        Some("nomic") => (EmbeddingModel::NomicEmbedTextV15, 768),
        _ => (EmbeddingModel::EmbeddingGemma300M, 768),
    };

    let mut model =
        TextEmbedding::try_new(InitOptions::new(model_type).with_show_download_progress(false))?;
    let input = vec!["hello world", "small embedding smoke test"];
    let embeddings = model.embed(input, None)?;
    if embeddings.is_empty() {
        anyhow::bail!("no embeddings returned");
    }
    println!("embeddings: {} vectors, dims {}", embeddings.len(), dims);
    if let Some(first) = embeddings.first() {
        let preview: Vec<String> = first.iter().take(8).map(|v| format!("{v:.4}")).collect();
        println!("first: [{}]", preview.join(", "));
    }
    Ok(())
}
