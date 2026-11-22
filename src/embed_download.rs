//! Time-bounded downloads of local fastembed models into the model cache.
//!
//! hf-hub 0.5 builds its own ureq agent without timeouts and exposes no setting for
//! them, so a stalled connection blocks a download forever. The files are fetched here
//! on a helper thread that reports progress, and the caller gives up when the transfer
//! stalls or runs too long. Model loading then finds every file in the cache.

use anyhow::{Context, Result, anyhow};
use fastembed::{EmbeddingModel, TextEmbedding};
use hf_hub::Cache;
use hf_hub::api::Progress;
use hf_hub::api::sync::{ApiBuilder, ApiRepo};
use std::path::Path;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Tokenizer files fastembed reads from every text model's repo.
const TOKENIZER_FILES: &[&str] = &[
    "tokenizer.json",
    "config.json",
    "special_tokens_map.json",
    "tokenizer_config.json",
];

/// How often the waiting caller checks the download's progress.
const PROGRESS_CHECK_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy)]
pub(crate) struct DownloadLimits {
    /// Longest wait for a response or for more body bytes.
    pub(crate) stall: Duration,
    /// Longest a whole model download may take.
    pub(crate) total: Duration,
}

/// A stall of a minute is a dead connection rather than a slow one. An hour covers a
/// multi-gigabyte model on a slow link; an interrupted download resumes from its
/// partial file on the next attempt.
pub(crate) const DEFAULT_DOWNLOAD_LIMITS: DownloadLimits = DownloadLimits {
    stall: Duration::from_secs(60),
    total: Duration::from_secs(60 * 60),
};

/// Open the model repo in `cache_dir`, laid out as fastembed lays out its cache.
pub(crate) fn model_repo(model_code: &str, cache_dir: &Path) -> Result<ApiRepo> {
    let endpoint =
        std::env::var("HF_ENDPOINT").unwrap_or_else(|_| "https://huggingface.co".to_string());
    model_repo_at(model_code, cache_dir, endpoint)
}

fn model_repo_at(model_code: &str, cache_dir: &Path, endpoint: String) -> Result<ApiRepo> {
    let api = ApiBuilder::new()
        .with_cache_dir(cache_dir.to_path_buf())
        .with_endpoint(endpoint)
        .with_progress(false)
        .build()?;
    Ok(api.model(model_code.to_string()))
}

/// Download every file `model` loads into `cache_dir` unless it is already cached.
pub(crate) fn ensure_model_files(model: &EmbeddingModel, cache_dir: &Path) -> Result<()> {
    let info = TextEmbedding::get_model_info(model)?;
    let files = std::iter::once(info.model_file.as_str())
        .chain(info.additional_files.iter().map(String::as_str))
        .chain(TOKENIZER_FILES.iter().copied())
        .map(str::to_string)
        .collect();
    fetch_files(
        model_repo(&info.model_code, cache_dir)?,
        &info.model_code,
        files,
        cache_dir,
        DEFAULT_DOWNLOAD_LIMITS,
    )
}

/// Fetch `files` from `repo` on a helper thread and wait within `limits`.
///
/// A blocked read cannot be cancelled, so on timeout the helper thread is left behind
/// still holding hf-hub's lock on the partial file. An embedding worker exits on the
/// error, which releases both; a long-lived process keeps them until the read fails.
fn fetch_files(
    repo: ApiRepo,
    model_code: &str,
    files: Vec<String>,
    cache_dir: &Path,
    limits: DownloadLimits,
) -> Result<()> {
    let cached = Cache::new(cache_dir.to_path_buf()).model(model_code.to_string());
    let last_progress = Arc::new(Mutex::new(Instant::now()));
    let (sender, receiver) = mpsc::channel();
    std::thread::Builder::new()
        .name("model-download".to_string())
        .spawn({
            let last_progress = Arc::clone(&last_progress);
            move || {
                let result = files.iter().try_for_each(|file| -> Result<()> {
                    if cached.get(file).is_none() {
                        // Starting a file counts as progress, so the stall clock covers
                        // the metadata request that precedes the body.
                        let clock = ProgressClock(Arc::clone(&last_progress));
                        clock.tick();
                        repo.download_with_progress(file, clock)
                            .with_context(|| format!("failed to download {file}"))?;
                    }
                    Ok(())
                });
                let _ = sender.send(result);
            }
        })
        .context("failed to start the model download thread")?;

    let started = Instant::now();
    let failure = |reason: String| {
        anyhow!(
            "downloading embedding model `{model_code}` into {} {reason}; the next attempt \
             resumes any partial download",
            cache_dir.display()
        )
    };
    loop {
        match receiver.recv_timeout(PROGRESS_CHECK_INTERVAL) {
            Ok(result) => {
                return result.with_context(|| {
                    format!(
                        "failed to download embedding model `{model_code}` into {}",
                        cache_dir.display()
                    )
                });
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(failure("stopped unexpectedly".to_string()));
            }
            Err(RecvTimeoutError::Timeout) => {
                let idle = last_progress
                    .lock()
                    .expect("download progress lock")
                    .elapsed();
                if idle >= limits.stall {
                    return Err(failure(format!(
                        "stalled: no data arrived for {}s",
                        limits.stall.as_secs_f64()
                    )));
                }
                if started.elapsed() >= limits.total {
                    return Err(failure(format!(
                        "did not finish within {}s",
                        limits.total.as_secs_f64()
                    )));
                }
            }
        }
    }
}

/// Records when the download last made progress.
struct ProgressClock(Arc<Mutex<Instant>>);

impl ProgressClock {
    fn tick(&self) {
        *self.0.lock().expect("download progress lock") = Instant::now();
    }
}

impl Progress for ProgressClock {
    fn init(&mut self, _size: usize, _filename: &str) {
        self.tick();
    }

    fn update(&mut self, _size: usize) {
        self.tick();
    }

    fn finish(&mut self) {
        self.tick();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc::Receiver;

    const MODEL: &str = "Xenova/bge-small-en-v1.5";

    /// An endpoint that accepts connections and never answers, like a stalled server.
    /// Each accepted connection is reported and then held open.
    fn silent_endpoint() -> (String, Receiver<TcpStream>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                if sender.send(stream).is_err() {
                    break;
                }
            }
        });
        (endpoint, receiver)
    }

    fn fetch_from_silent_endpoint(limits: DownloadLimits) -> (String, std::path::PathBuf) {
        let (endpoint, connections) = silent_endpoint();
        let cache = tempfile::tempdir().unwrap();
        let repo = model_repo_at(MODEL, cache.path(), endpoint).unwrap();
        let started = Instant::now();
        let error = fetch_files(
            repo,
            MODEL,
            vec!["config.json".to_string()],
            cache.path(),
            limits,
        )
        .expect_err("a silent endpoint cannot complete a download");
        assert!(started.elapsed() < Duration::from_secs(5));
        connections
            .recv_timeout(Duration::from_secs(1))
            .expect("the download reached the endpoint");
        (format!("{error:#}"), cache.path().to_path_buf())
    }

    #[test]
    fn a_silent_connection_fails_after_the_stall_limit() {
        let (error, cache) = fetch_from_silent_endpoint(DownloadLimits {
            stall: Duration::from_millis(500),
            total: Duration::from_secs(60),
        });
        assert!(
            error.contains("stalled: no data arrived for 0.5s"),
            "{error}"
        );
        assert!(error.contains(MODEL), "{error}");
        assert!(error.contains(&cache.display().to_string()), "{error}");
    }

    #[test]
    fn a_download_fails_after_the_total_limit() {
        let (error, cache) = fetch_from_silent_endpoint(DownloadLimits {
            stall: Duration::from_secs(60),
            total: Duration::from_millis(500),
        });
        assert!(error.contains("did not finish within 0.5s"), "{error}");
        assert!(error.contains(MODEL), "{error}");
        assert!(error.contains(&cache.display().to_string()), "{error}");
    }
}
