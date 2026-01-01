use anyhow::Result;
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use std::time::Instant;

fn generate_texts(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| {
            format!(
                "This is test sentence number {} for embedding benchmarks",
                i
            )
        })
        .collect()
}

fn bench_model(name: &str, model_type: EmbeddingModel, dims: usize) -> Result<()> {
    println!("\n{} ({} dims)", name, dims);
    println!("{}", "-".repeat(60));

    let start = Instant::now();

    #[cfg(target_os = "macos")]
    let opts = {
        use ort::execution_providers::CoreMLExecutionProvider;
        InitOptions::new(model_type)
            .with_show_download_progress(false)
            .with_execution_providers(vec![CoreMLExecutionProvider::default().build()])
    };

    #[cfg(not(target_os = "macos"))]
    let opts = InitOptions::new(model_type).with_show_download_progress(false);

    let mut model = TextEmbedding::try_new(opts)?;
    println!("  Init: {:>6}ms", start.elapsed().as_millis());

    // Warmup
    let _ = model.embed(vec!["warmup"], None)?;

    // Test different batch sizes and internal batch_size param
    let texts_500 = generate_texts(500);
    let text_refs: Vec<&str> = texts_500.iter().map(|s| s.as_str()).collect();

    // Test with different internal batch sizes
    for internal_batch in [None, Some(32), Some(64), Some(128), Some(256)] {
        let start = Instant::now();
        let embeddings = model.embed(&text_refs, internal_batch)?;
        let elapsed = start.elapsed();

        let texts_per_sec = 500.0 / elapsed.as_secs_f64();
        let batch_str = internal_batch
            .map(|b| format!("{}", b))
            .unwrap_or("default".to_string());

        println!(
            "  500 texts (batch_size={:>7}): {:>5}ms | {:>6.0} texts/sec | {} vecs",
            batch_str,
            elapsed.as_millis(),
            texts_per_sec,
            embeddings.len()
        );
    }

    Ok(())
}

fn main() -> Result<()> {
    println!("Embedding Model Benchmark - Internal Batch Size Test");
    println!("=====================================================");
    println!("CPU cores: {}", std::thread::available_parallelism()?.get());
    #[cfg(target_os = "macos")]
    println!("Backend: CoreML + ONNX Runtime");
    #[cfg(not(target_os = "macos"))]
    println!("Backend: ONNX Runtime (CPU)");

    // Just test with MiniLM and Gemma for speed
    let models = [
        ("MiniLM", EmbeddingModel::AllMiniLML6V2, 384),
        ("Gemma", EmbeddingModel::EmbeddingGemma300M, 768),
    ];

    for (name, model_type, dims) in models {
        if let Err(e) = bench_model(name, model_type, dims) {
            println!("{}: error - {}", name, e);
        }
    }

    Ok(())
}
