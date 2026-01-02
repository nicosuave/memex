use anyhow::Result;
use memex::embed::EmbedderHandle;

fn main() -> Result<()> {
    let input = vec!["hello world", "small embedding smoke test"];
    let mut embedder = EmbedderHandle::new()?;
    let embeddings = embedder.embed_texts(&input)?;
    if embeddings.is_empty() {
        anyhow::bail!("no embeddings returned");
    }
    println!(
        "embeddings: {} vectors, dims {}",
        embeddings.len(),
        embedder.dims
    );
    if let Some(first) = embeddings.first() {
        let preview: Vec<String> = first.iter().take(8).map(|v| format!("{v:.4}")).collect();
        println!("first: [{}]", preview.join(", "));
    }
    Ok(())
}
