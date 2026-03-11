use anyhow::Result;
use memex::embed::{EmbedderHandle, ModelChoice};
use std::time::Instant;

fn generate_texts(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| {
            format!(
                "This is test sentence number {i} for embedding benchmarks. \
                 We add some extra text here to make the sentences more realistic \
                 and representative of actual embedding workloads."
            )
        })
        .collect()
}

fn main() -> Result<()> {
    println!("Embedding Benchmark - Testing Performance Options");
    println!("==================================================");
    println!("CPU cores: {}", std::thread::available_parallelism()?.get());

    let texts = generate_texts(500);
    let text_refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();

    // Test 1: Baseline with MiniLM
    println!("\n--- Test 1: MiniLM (default settings) ---");
    unsafe {
        std::env::set_var("MEMEX_EXECUTION_PROVIDER", "cpu");
        std::env::remove_var("MEMEX_COMPUTE_UNITS");
        std::env::remove_var("MEMEX_CUDA_DEVICE_ID");
    }
    {
        let start = Instant::now();
        let mut embedder = EmbedderHandle::with_model(ModelChoice::MiniLM)?;
        println!("  Model init: {}ms", start.elapsed().as_millis());

        // Warmup
        let _ = embedder.embed_texts(&["warmup"])?;

        let start = Instant::now();
        let results = embedder.embed_texts(&text_refs)?;
        let elapsed = start.elapsed();
        println!(
            "  500 texts: {}ms ({:.0} texts/sec) - {} embeddings",
            elapsed.as_millis(),
            500.0 / elapsed.as_secs_f64(),
            results.len()
        );
    }

    #[cfg(target_os = "macos")]
    for unit in ["all", "ane", "gpu", "cpu"] {
        println!("\n--- Test 2: MEMEX_EXECUTION_PROVIDER=coreml MEMEX_COMPUTE_UNITS={unit} ---");
        unsafe {
            std::env::set_var("MEMEX_EXECUTION_PROVIDER", "coreml");
            std::env::set_var("MEMEX_COMPUTE_UNITS", unit);
            std::env::remove_var("MEMEX_CUDA_DEVICE_ID");
        }

        let start = Instant::now();
        let mut embedder = match EmbedderHandle::with_model(ModelChoice::MiniLM) {
            Ok(e) => e,
            Err(e) => {
                println!("  Failed to init: {e}");
                continue;
            }
        };
        println!("  Model init: {}ms", start.elapsed().as_millis());

        let _ = embedder.embed_texts(&["warmup"])?;

        let start = Instant::now();
        let results = embedder.embed_texts(&text_refs)?;
        let elapsed = start.elapsed();
        println!(
            "  500 texts: {}ms ({:.0} texts/sec)",
            elapsed.as_millis(),
            500.0 / elapsed.as_secs_f64()
        );
        let _ = results;
    }

    #[cfg(all(not(target_os = "macos"), feature = "cuda"))]
    {
        println!("\n--- Test 2: MEMEX_EXECUTION_PROVIDER=cuda MEMEX_CUDA_DEVICE_ID=0 ---");
        unsafe {
            std::env::set_var("MEMEX_EXECUTION_PROVIDER", "cuda");
            std::env::set_var("MEMEX_CUDA_DEVICE_ID", "0");
            std::env::remove_var("MEMEX_COMPUTE_UNITS");
        }

        let start = Instant::now();
        let mut embedder = match EmbedderHandle::with_model(ModelChoice::MiniLM) {
            Ok(e) => e,
            Err(e) => {
                println!("  Failed to init: {e}");
                return Ok(());
            }
        };
        println!("  Model init: {}ms", start.elapsed().as_millis());

        let _ = embedder.embed_texts(&["warmup"])?;

        let start = Instant::now();
        let results = embedder.embed_texts(&text_refs)?;
        let elapsed = start.elapsed();
        println!(
            "  500 texts: {}ms ({:.0} texts/sec)",
            elapsed.as_millis(),
            500.0 / elapsed.as_secs_f64()
        );
        let _ = results;
    }

    // Test 3: Gemma model (larger, higher quality)
    println!("\n--- Test 3: Gemma model (larger, higher quality) on CPU ---");
    unsafe {
        std::env::set_var("MEMEX_EXECUTION_PROVIDER", "cpu");
        std::env::remove_var("MEMEX_COMPUTE_UNITS");
        std::env::remove_var("MEMEX_CUDA_DEVICE_ID");
    }
    {
        let start = Instant::now();
        let mut embedder = EmbedderHandle::with_model(ModelChoice::Gemma)?;
        println!("  Model init: {}ms", start.elapsed().as_millis());

        let _ = embedder.embed_texts(&["warmup"])?;

        let start = Instant::now();
        let results = embedder.embed_texts(&text_refs)?;
        let elapsed = start.elapsed();
        println!(
            "  500 texts: {}ms ({:.0} texts/sec)",
            elapsed.as_millis(),
            500.0 / elapsed.as_secs_f64()
        );
        let _ = results;
    }

    println!("\nDone!");
    Ok(())
}
