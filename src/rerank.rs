//! Cross-encoder reranking of the leading search results.
//!
//! Local reranking loads a fastembed cross-encoder on first use and keeps it for the
//! life of the process. The load takes several seconds, so searches run it only when
//! the process opted in with [`enable_background_loading`], which also moves the load
//! to a background thread, or the caller asked for it explicitly.
//!
//! Remote reranking loads nothing and runs in every search; after a failed request,
//! searches skip it for [`REMOTE_FAILURE_BACKOFF`].

use crate::config::RerankMode;
use crate::embed::{EmbedRuntimeConfig, execution_providers, provider_init_error};
use crate::remote_rerank::{RemoteRerankConfig, RemoteReranker, RerankDialect, RerankExtraBody};
use crate::types::Record;
use anyhow::{Result, anyhow};
use fastembed::{RerankInitOptions, RerankerModel, TextRerank};
use std::collections::HashMap;
use std::io::Write;
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, TryLockError};
use std::time::{Duration, Instant};

pub const DEFAULT_RERANK_CANDIDATES: usize = 30;
pub const RERANK_CANDIDATES: RangeInclusive<usize> = 5..=100;
pub const DEFAULT_RERANK_DOC_CHARS: usize = 1500;
pub const RERANK_DOC_CHARS: RangeInclusive<usize> = 200..=8000;
pub const SUPPORTED_MODEL_NAMES: &str = "jina-turbo, bge-v2-m3, bge-base, jina-v2";

/// Query terms that center document windows.
const MAX_WINDOW_TERMS: usize = 16;
/// Characters kept of each window term.
const MAX_WINDOW_TERM_CHARS: usize = 64;
/// Documents scored per ONNX Runtime call; bounds peak memory for long documents.
const RERANK_BATCH_SIZE: usize = 16;
/// Most distinct reranking failures tracked within one failure window.
const MAX_FAILURE_WARNINGS: usize = 64;
const ONE_SHOT_NOTICE: &str =
    "reranking is configured but skipped in one-shot searches; pass --rerank to load the model";

/// Resolved reranking settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RerankSettings {
    pub engine: RerankEngine,
    /// Leading results rescored per search.
    pub candidates: usize,
    /// Characters of each result sent to the model.
    pub doc_chars: usize,
}

/// What scores the documents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RerankEngine {
    Local {
        model: RerankerModel,
        /// ONNX Runtime settings, resolved like those of local embeddings.
        runtime: EmbedRuntimeConfig,
    },
    Remote(RemoteRerankConfig),
}

/// Scores documents against a query.
pub trait RerankBackend: Send {
    /// One relevance score per document, in input order; higher is more relevant.
    fn rerank(&mut self, query: &str, documents: &[String]) -> Result<Vec<f32>>;
}

/// Resolve a `rerank_model` name, ignoring ASCII case.
///
/// Accepts the short names in [`SUPPORTED_MODEL_NAMES`] and fastembed's variant names
/// such as `JINARerankerV1TurboEn`.
pub fn parse_model(name: &str) -> Result<RerankerModel> {
    let name = name.trim();
    [
        RerankerModel::JINARerankerV1TurboEn,
        RerankerModel::BGERerankerV2M3,
        RerankerModel::BGERerankerBase,
        RerankerModel::JINARerankerV2BaseMultiligual,
    ]
    .into_iter()
    .find(|model| {
        name.eq_ignore_ascii_case(model_name(model))
            || name.eq_ignore_ascii_case(&format!("{model:?}"))
    })
    .ok_or_else(|| anyhow!("unknown reranker model '{name}', options: {SUPPORTED_MODEL_NAMES}"))
}

/// The short configuration name of `model`.
pub fn model_name(model: &RerankerModel) -> &'static str {
    match model {
        RerankerModel::JINARerankerV1TurboEn => "jina-turbo",
        RerankerModel::BGERerankerV2M3 => "bge-v2-m3",
        RerankerModel::BGERerankerBase => "bge-base",
        RerankerModel::JINARerankerV2BaseMultiligual => "jina-v2",
    }
}

/// A fastembed cross-encoder running on ONNX Runtime.
pub struct LocalReranker {
    model: TextRerank,
}

impl LocalReranker {
    /// Download the model if needed and load it; takes seconds.
    pub fn load(model: &RerankerModel, runtime: &EmbedRuntimeConfig) -> Result<Self> {
        crate::profiling::span!("search.rerank.model_init");
        let options = RerankInitOptions::new(model.clone())
            .with_execution_providers(execution_providers(runtime)?)
            .with_show_download_progress(false);
        let model =
            TextRerank::try_new(options).map_err(|err| provider_init_error(runtime, err))?;
        Ok(Self { model })
    }
}

impl RerankBackend for LocalReranker {
    /// Relevance probabilities in `[0, 1]`: the model's logits through [`sigmoid`].
    fn rerank(&mut self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        let documents: Vec<&str> = documents.iter().map(String::as_str).collect();
        let results = self
            .model
            .rerank(query, &documents, false, Some(RERANK_BATCH_SIZE))?;
        let mut scores = vec![None; documents.len()];
        for result in results {
            let slot = scores
                .get_mut(result.index)
                .ok_or_else(|| anyhow!("reranker returned unknown document {}", result.index))?;
            if slot.replace(result.score).is_some() {
                return Err(anyhow!("reranker scored document {} twice", result.index));
            }
        }
        scores
            .into_iter()
            .enumerate()
            .map(|(index, score)| {
                score
                    .ok_or_else(|| anyhow!("reranker did not score document {index}"))
                    .and_then(sigmoid)
            })
            .collect()
    }
}

/// Map a cross-encoder logit to a relevance probability in `[0, 1]`.
///
/// Logits beyond the `f32` resolution saturate at 0 or 1. Fails on a NaN or infinite
/// logit.
pub fn sigmoid(logit: f32) -> Result<f32> {
    if !logit.is_finite() {
        return Err(anyhow!("reranker returned a non-finite score"));
    }
    let logit = f64::from(logit);
    // Each branch exponentiates a non-positive number, so neither overflows.
    let probability = if logit >= 0.0 {
        1.0 / (1.0 + (-logit).exp())
    } else {
        let exp = logit.exp();
        exp / (1.0 + exp)
    };
    Ok(probability as f32)
}

/// Wait after a failed model load before a search tries again.
const LOAD_RETRY_DELAY: Duration = Duration::from_secs(5 * 60);
/// Longest a model load may run before searches treat it as failed.
const LOAD_DEADLINE: Duration = Duration::from_secs(30 * 60);
/// Longest a search waits for another search's inference before skipping reranking.
const BUSY_WAIT: Duration = Duration::from_secs(2);
/// Interval between attempts to take a busy model.
const BUSY_POLL: Duration = Duration::from_millis(20);
/// Longest an explicit request waits for another search's model load.
const LOADING_WAIT: Duration = Duration::from_secs(60);
/// Interval between checks on another search's model load.
const LOADING_POLL: Duration = Duration::from_millis(100);
const LOADING_NOTICE: &str =
    "reranker model is loading in the background; results are not reranked until it is ready";
const STILL_LOADING_WARNING: &str = "warning: reranker model is still loading; search not reranked";
const BUSY_WARNING: &str = "warning: reranker is busy; search not reranked";
const SUPERSEDED_WARNING: &str =
    "warning: reranker model load was replaced by another search; search not reranked";
const RECENT_FAILURE_WARNING: &str =
    "warning: reranker model failed to load recently; search not reranked";

/// How a search that needs an unloaded model gets it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadMode {
    /// Load on a background thread and skip reranking until the model is ready.
    Background,
    /// An explicit request: load on a worker thread and wait for it up to
    /// [`LOAD_DEADLINE`], or wait up to [`LOADING_WAIT`] for another search's load, and
    /// warn every time the search is not reranked.
    Blocking,
}

/// Why a search skipped reranking, or a background load outcome, for the user to see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SlotEvent {
    /// A background-mode search skipped while the model loads.
    Loading,
    /// An explicit request waited [`LOADING_WAIT`] for another load and skipped.
    StillLoading,
    /// The model stayed busy for [`BUSY_WAIT`].
    Busy { explicit: bool },
    /// The model failed to load within the last [`LOAD_RETRY_DELAY`].
    RecentFailure { explicit: bool },
    /// A load ran past [`LOAD_DEADLINE`].
    TimedOut { explicit: bool },
    /// Another search replaced an explicit request's load before it finished.
    Superseded,
    /// A background load failed.
    LoadFailed(String),
}

/// Show `event` on stderr: explicit requests warn every time, other searches at most once
/// per process or failure window.
fn report_event(event: SlotEvent) {
    let out = &mut std::io::stderr().lock();
    match event {
        SlotEvent::Loading => {
            notify_once(&LOADING_NOTICE_PRINTED, LOADING_NOTICE, out);
        }
        SlotEvent::StillLoading => {
            let _ = writeln!(out, "{STILL_LOADING_WARNING}");
        }
        SlotEvent::Busy { explicit: true } => {
            let _ = writeln!(out, "{BUSY_WARNING}");
        }
        SlotEvent::Busy { explicit: false } => {
            notify_once(&BUSY_WARNING_PRINTED, BUSY_WARNING, out);
        }
        SlotEvent::RecentFailure { explicit: true } => {
            let _ = writeln!(out, "{RECENT_FAILURE_WARNING}");
        }
        SlotEvent::RecentFailure { explicit: false } => {}
        SlotEvent::Superseded => {
            let _ = writeln!(out, "{SUPERSEDED_WARNING}");
        }
        SlotEvent::TimedOut { explicit } => {
            let message = format!(
                "reranker model load did not finish within {} minutes",
                LOAD_DEADLINE.as_secs() / 60
            );
            if explicit {
                let _ = writeln!(out, "{}", failure_warning(&message));
            } else {
                warn_failure_once(
                    &WARNED_FAILURES,
                    &message,
                    Instant::now(),
                    LOAD_RETRY_DELAY,
                    out,
                );
            }
        }
        SlotEvent::LoadFailed(message) => {
            warn_failure_once(
                &WARNED_FAILURES,
                &message,
                Instant::now(),
                LOAD_RETRY_DELAY,
                out,
            );
        }
    }
}

/// Time source for retry deadlines and busy waits.
pub(crate) trait Clock: Send + Sync {
    fn now(&self) -> Instant;
    fn sleep(&self, duration: Duration);
}

struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

/// Identity of a loaded model; another key replaces it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelKey {
    model: RerankerModel,
    runtime: EmbedRuntimeConfig,
}

/// One load attempt; a later attempt for the same key gets another `id`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LoadTicket {
    key: ModelKey,
    id: u64,
}

enum SlotState<B> {
    Idle,
    Loading {
        load: LoadTicket,
        started: Instant,
    },
    Ready(ModelKey, Arc<Mutex<B>>),
    /// The load failed; no search loads this key again before `retry_at`.
    Failed {
        key: ModelKey,
        retry_at: Instant,
    },
}

type Loader<B> = dyn Fn(&RerankerModel, &EmbedRuntimeConfig) -> Result<B> + Send + Sync;

/// One process-wide reranker model and its load lifecycle.
///
/// The state lock is held only for transitions, never during a load or inference, so a
/// slow load or a busy model never blocks another search for long.
pub(crate) struct ModelSlot<B> {
    state: Mutex<SlotState<B>>,
    next_load_id: AtomicU64,
    loader: Box<Loader<B>>,
    clock: Box<dyn Clock>,
    report: Box<dyn Fn(SlotEvent) + Send + Sync>,
}

/// What one search does next with the slot.
enum Step<B> {
    Score(Arc<Mutex<B>>),
    Load(LoadTicket),
    Loading,
    TimedOut,
    RecentFailure,
}

impl<B: RerankBackend + 'static> ModelSlot<B> {
    pub(crate) fn new(
        loader: Box<Loader<B>>,
        clock: Box<dyn Clock>,
        report: Box<dyn Fn(SlotEvent) + Send + Sync>,
    ) -> Self {
        Self {
            state: Mutex::new(SlotState::Idle),
            next_load_id: AtomicU64::new(0),
            loader,
            clock,
            report,
        }
    }

    /// Score `documents`, or `None` when this search skips reranking because the model is
    /// loading, busy, or failed to load within the last [`LOAD_RETRY_DELAY`].
    ///
    /// A load still running after [`LOAD_DEADLINE`] counts as failed; its eventual result
    /// is discarded.
    ///
    /// Fails when a load in the calling thread fails or the model fails to score.
    pub(crate) fn score(
        self: &Arc<Self>,
        model: &RerankerModel,
        runtime: &EmbedRuntimeConfig,
        mode: LoadMode,
        query: &str,
        documents: &[String],
    ) -> Result<Option<Vec<f32>>> {
        let key = ModelKey {
            model: model.clone(),
            runtime: runtime.clone(),
        };
        let explicit = mode == LoadMode::Blocking;
        let wait_until = self.clock.now() + LOADING_WAIT;
        let backend = loop {
            match self.next_step(&key) {
                Step::Score(backend) => break backend,
                Step::Loading if explicit => {
                    if self.clock.now() >= wait_until {
                        (self.report)(SlotEvent::StillLoading);
                        return Ok(None);
                    }
                    self.clock.sleep(LOADING_POLL);
                }
                Step::Loading => {
                    (self.report)(SlotEvent::Loading);
                    return Ok(None);
                }
                Step::TimedOut => {
                    (self.report)(SlotEvent::TimedOut { explicit });
                    return Ok(None);
                }
                Step::RecentFailure => {
                    (self.report)(SlotEvent::RecentFailure { explicit });
                    return Ok(None);
                }
                Step::Load(load) if explicit => match self.load_in_worker(load)? {
                    Some(backend) => break backend,
                    None => return Ok(None),
                },
                Step::Load(load) => {
                    self.spawn_load(load)?;
                    (self.report)(SlotEvent::Loading);
                    return Ok(None);
                }
            }
        };
        match isolated(|| self.infer(&backend, explicit, query, documents)) {
            Ok(scored) => scored,
            Err(panic) => {
                // The model may be left in an unknown state; the next search reloads it.
                let mut state = self.lock_state();
                if matches!(&*state, SlotState::Ready(_, current) if Arc::ptr_eq(current, &backend))
                {
                    *state = SlotState::Idle;
                }
                Err(anyhow!(panic))
            }
        }
    }

    /// Advance the state for one search that needs `key`.
    fn next_step(&self, key: &ModelKey) -> Step<B> {
        let mut state = self.lock_state();
        let now = self.clock.now();
        match &*state {
            SlotState::Ready(current, backend) if current == key => {
                Step::Score(Arc::clone(backend))
            }
            SlotState::Loading { load, started } if load.key == *key => {
                if now.saturating_duration_since(*started) < LOAD_DEADLINE {
                    Step::Loading
                } else {
                    *state = SlotState::Failed {
                        key: key.clone(),
                        retry_at: now + LOAD_RETRY_DELAY,
                    };
                    Step::TimedOut
                }
            }
            SlotState::Failed {
                key: current,
                retry_at,
            } if current == key && now < *retry_at => Step::RecentFailure,
            // Idle, an expired failure, or another model: this search starts the load.
            // Replacing the state drops the previous model before the next one loads.
            _ => {
                let load = LoadTicket {
                    key: key.clone(),
                    id: self.next_load_id.fetch_add(1, Ordering::Relaxed),
                };
                *state = SlotState::Loading {
                    load: load.clone(),
                    started: now,
                };
                Step::Load(load)
            }
        }
    }

    fn spawn_load(self: &Arc<Self>, load: LoadTicket) -> Result<()> {
        let slot = Arc::clone(self);
        let thread_load = load.clone();
        let spawned = std::thread::Builder::new()
            .name("memex-rerank-load".to_string())
            .spawn(move || match isolated(|| slot.load(thread_load)) {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => (slot.report)(SlotEvent::LoadFailed(format!("{error:#}"))),
                Err(panic) => (slot.report)(SlotEvent::LoadFailed(panic)),
            });
        match spawned {
            Ok(_) => Ok(()),
            Err(error) => {
                let error = anyhow::Error::from(error).context("start the reranker load thread");
                self.finish_load(&load, Err(error)).map(|_| ())
            }
        }
    }

    /// Run the load the caller moved to `Loading` on a worker thread and wait for it.
    ///
    /// Waits at most [`LOAD_DEADLINE`] on the slot clock, then marks the load failed and
    /// returns `None`; a result that arrives later is discarded. Also returns `None` when
    /// another search replaced the load. Both report an explicit skip. A panicking loader
    /// is an error.
    fn load_in_worker(self: &Arc<Self>, load: LoadTicket) -> Result<Option<Arc<Mutex<B>>>> {
        let (sender, receiver) = std::sync::mpsc::channel();
        let slot = Arc::clone(self);
        let worker_load = load.clone();
        let spawned = std::thread::Builder::new()
            .name("memex-rerank-load".to_string())
            .spawn(move || {
                // The receiver is gone only after the caller gave up waiting.
                let _ = sender.send(slot.load(worker_load));
            });
        let worker = match spawned {
            Ok(worker) => worker,
            Err(error) => {
                let error = anyhow::Error::from(error).context("start the reranker load thread");
                return self.finish_load(&load, Err(error));
            }
        };
        let started = self.clock.now();
        loop {
            match receiver.try_recv() {
                Ok(Ok(Some(backend))) => return Ok(Some(backend)),
                Ok(Ok(None)) => {
                    (self.report)(SlotEvent::Superseded);
                    return Ok(None);
                }
                Ok(Err(error)) => return Err(error),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    // The worker ended without sending, so it panicked; the load guard
                    // already marked the load failed.
                    return Err(match worker.join() {
                        Err(payload) => anyhow!(panic_message(payload.as_ref())),
                        Ok(()) => anyhow!("the reranker load thread ended without a result"),
                    });
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    let now = self.clock.now();
                    if now.saturating_duration_since(started) >= LOAD_DEADLINE {
                        let mut state = self.lock_state();
                        if matches!(&*state, SlotState::Loading { load: current, .. } if *current == load)
                        {
                            *state = SlotState::Failed {
                                key: load.key.clone(),
                                retry_at: now + LOAD_RETRY_DELAY,
                            };
                        }
                        drop(state);
                        (self.report)(SlotEvent::TimedOut { explicit: true });
                        return Ok(None);
                    }
                    self.clock.sleep(LOADING_POLL);
                }
            }
        }
    }

    /// Run the load the caller moved to `Loading`.
    ///
    /// Returns `None` when the state moved on during the load, such as past
    /// [`LOAD_DEADLINE`].
    fn load(&self, load: LoadTicket) -> Result<Option<Arc<Mutex<B>>>> {
        let mut guard = LoadGuard {
            slot: self,
            load: &load,
            finished: false,
        };
        let loaded = (self.loader)(&load.key.model, &load.key.runtime);
        guard.finished = true;
        self.finish_load(&load, loaded)
    }

    /// Publish the result of `load`; discarded unless `load` is still the current load.
    fn finish_load(&self, load: &LoadTicket, loaded: Result<B>) -> Result<Option<Arc<Mutex<B>>>> {
        let mut state = self.lock_state();
        if !matches!(&*state, SlotState::Loading { load: current, .. } if current == load) {
            return Ok(None);
        }
        let key = &load.key;
        match loaded {
            Ok(backend) => {
                let backend = Arc::new(Mutex::new(backend));
                *state = SlotState::Ready(key.clone(), Arc::clone(&backend));
                Ok(Some(backend))
            }
            Err(error) => {
                *state = SlotState::Failed {
                    key: key.clone(),
                    retry_at: self.clock.now() + LOAD_RETRY_DELAY,
                };
                Err(error.context("load the reranker model"))
            }
        }
    }

    fn infer(
        &self,
        backend: &Arc<Mutex<B>>,
        explicit: bool,
        query: &str,
        documents: &[String],
    ) -> Result<Option<Vec<f32>>> {
        let deadline = self.clock.now() + BUSY_WAIT;
        loop {
            match backend.try_lock() {
                Ok(mut model) => return model.rerank(query, documents).map(Some),
                Err(TryLockError::WouldBlock) if self.clock.now() < deadline => {
                    self.clock.sleep(BUSY_POLL);
                }
                Err(TryLockError::WouldBlock) => {
                    (self.report)(SlotEvent::Busy { explicit });
                    return Ok(None);
                }
                Err(TryLockError::Poisoned(_)) => {
                    // A panic mid-inference leaves the model in an unknown state.
                    let mut state = self.lock_state();
                    if matches!(&*state, SlotState::Ready(_, current) if Arc::ptr_eq(current, backend))
                    {
                        *state = SlotState::Idle;
                    }
                    return Err(anyhow!(
                        "the reranker model failed during an earlier search and will be reloaded"
                    ));
                }
            }
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, SlotState<B>> {
        self.state.lock().unwrap_or_else(|poisoned| {
            self.state.clear_poison();
            let mut state = poisoned.into_inner();
            *state = SlotState::Idle;
            state
        })
    }
}

/// Marks a load that ended without a result, such as a panicking loader, as failed; the
/// thread that joins the loader reports the panic.
struct LoadGuard<'a, B: RerankBackend + 'static> {
    slot: &'a ModelSlot<B>,
    load: &'a LoadTicket,
    finished: bool,
}

impl<B: RerankBackend + 'static> Drop for LoadGuard<'_, B> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self
                .slot
                .finish_load(self.load, Err(anyhow!("the reranker model load panicked")));
        }
    }
}

/// Run `work` on a scoped worker thread so a panic in the model code fails only this call.
///
/// Returns the panic as `Err` with a "reranker panicked" message.
fn isolated<T: Send>(
    work: impl FnOnce() -> Result<T> + Send,
) -> std::result::Result<Result<T>, String> {
    std::thread::scope(|scope| {
        match std::thread::Builder::new()
            .name("memex-rerank".to_string())
            .spawn_scoped(scope, work)
        {
            Ok(worker) => worker
                .join()
                .map_err(|payload| panic_message(payload.as_ref())),
            Err(error) => Ok(Err(
                anyhow::Error::from(error).context("start a reranker worker thread")
            )),
        }
    })
}

/// "reranker panicked: <message>", with the payload when it is a string.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    let detail = payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string());
    format!("reranker panicked: {detail}")
}

/// The local model shared by every search in this process.
static LOCAL_MODEL: LazyLock<Arc<ModelSlot<LocalReranker>>> = LazyLock::new(|| {
    Arc::new(ModelSlot::new(
        Box::new(LocalReranker::load),
        Box::new(SystemClock),
        Box::new(report_event),
    ))
});

/// Score `documents` with the reranker of `settings`, one score per document in input
/// order, or `None` when this search skips reranking.
///
/// Local reranking uses the process-wide model and skips while the model is loading,
/// busy for longer than two seconds, or failed to load within the last five minutes. An
/// `explicit` request, or any request in a process that did not enable
/// [`enable_background_loading`], waits up to 30 minutes for its own load or a minute
/// for another search's load. Other requests load the model on a background thread.
/// Loads and inference run on worker threads, so a failure or panic in the model code
/// is an error for this search only.
///
/// Remote reranking sends one request and never fails: a failed request is reported on
/// stderr and returns `None`, and searches within [`REMOTE_FAILURE_BACKOFF`] of it skip
/// without a request.
pub fn score(
    settings: &RerankSettings,
    explicit: bool,
    query: &str,
    documents: &[String],
) -> Result<Option<Vec<f32>>> {
    match &settings.engine {
        RerankEngine::Local { model, runtime } => {
            let mode = load_mode(explicit, background_loading_enabled());
            LOCAL_MODEL.score(model, runtime, mode, query, documents)
        }
        RerankEngine::Remote(config) => Ok(REMOTE_BREAKER.score(config, explicit, || {
            RemoteReranker::new(config)?.rerank(query, documents)
        })),
    }
}

/// Wait after a failed remote rerank request before a search sends another.
pub const REMOTE_FAILURE_BACKOFF: Duration = Duration::from_secs(60);
const REMOTE_SKIPPED_WARNING: &str =
    "warning: rerank server failed within the last minute; search not reranked";

/// A remote reranking outcome for the user to see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RemoteEvent {
    /// A search skipped reranking within [`REMOTE_FAILURE_BACKOFF`] of a failure.
    Skipped { explicit: bool },
    /// A request failed.
    Failed { message: String, explicit: bool },
}

/// Show `event` on stderr: explicit requests warn every time; other searches warn once
/// per distinct failure and [`REMOTE_FAILURE_BACKOFF`], and skip quietly.
fn report_remote_event(event: RemoteEvent) {
    let out = &mut std::io::stderr().lock();
    match event {
        RemoteEvent::Skipped { explicit: true } => {
            let _ = writeln!(out, "{REMOTE_SKIPPED_WARNING}");
        }
        RemoteEvent::Skipped { explicit: false } => {}
        RemoteEvent::Failed {
            message,
            explicit: true,
        } => {
            let _ = writeln!(out, "{}", failure_warning(&message));
        }
        RemoteEvent::Failed {
            message,
            explicit: false,
        } => {
            warn_failure_once(
                &WARNED_REMOTE_FAILURES,
                &message,
                Instant::now(),
                REMOTE_FAILURE_BACKOFF,
                out,
            );
        }
    }
}

/// Identity of the remote requests a failure holds back; never holds the API key.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteKey {
    url: String,
    model: Option<String>,
    dialect: RerankDialect,
    extra_body: Option<RerankExtraBody>,
}

impl RemoteKey {
    fn of(config: &RemoteRerankConfig) -> Self {
        Self {
            url: config.endpoint.base_url.clone(),
            model: config.model.clone(),
            dialect: config.dialect,
            extra_body: config.extra_body.clone(),
        }
    }
}

/// Requests held back after a failure.
#[derive(Debug)]
struct Blocked {
    key: RemoteKey,
    /// When the next request may start.
    retry_at: Instant,
    /// A search claimed the next request and has not finished it.
    probing: bool,
}

/// Holds back remote reranking for [`REMOTE_FAILURE_BACKOFF`] after a failed request,
/// so an unreachable server does not add its timeout to every search.
pub(crate) struct RemoteBreaker {
    blocked: Mutex<Option<Blocked>>,
    clock: Box<dyn Clock>,
    report: Box<dyn Fn(RemoteEvent) + Send + Sync>,
}

/// Marks a claimed request that ended without a result, such as by a panic, as failed,
/// so later searches are not held back forever.
struct ProbeGuard<'a> {
    breaker: &'a RemoteBreaker,
    key: &'a RemoteKey,
    armed: bool,
}

impl Drop for ProbeGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.breaker.fail(self.key);
        }
    }
}

impl RemoteBreaker {
    pub(crate) fn new(
        clock: Box<dyn Clock>,
        report: Box<dyn Fn(RemoteEvent) + Send + Sync>,
    ) -> Self {
        Self {
            blocked: Mutex::new(None),
            clock,
            report,
        }
    }

    /// Scores from `request`, or `None` when the request fails or a request for the
    /// same URL, model, dialect, and extra body failed within [`REMOTE_FAILURE_BACKOFF`].
    ///
    /// After a failure, the first search past the backoff sends the only request until
    /// that request finishes, however long it takes; concurrent searches skip meanwhile.
    /// Other settings, such as a reloaded configuration, are not held back.
    pub(crate) fn score(
        &self,
        config: &RemoteRerankConfig,
        explicit: bool,
        request: impl FnOnce() -> Result<Vec<f32>>,
    ) -> Option<Vec<f32>> {
        let key = RemoteKey::of(config);
        let probe = {
            let mut blocked = self.lock_blocked();
            match &mut *blocked {
                Some(state) if state.key == key => {
                    if state.probing || self.clock.now() < state.retry_at {
                        drop(blocked);
                        (self.report)(RemoteEvent::Skipped { explicit });
                        return None;
                    }
                    state.probing = true;
                    true
                }
                _ => false,
            }
        };
        let mut guard = ProbeGuard {
            breaker: self,
            key: &key,
            armed: probe,
        };
        let result = request();
        guard.armed = false;
        match result {
            Ok(scores) => {
                let mut blocked = self.lock_blocked();
                if blocked.as_ref().is_some_and(|state| state.key == key) {
                    *blocked = None;
                }
                Some(scores)
            }
            Err(error) => {
                self.fail(&key);
                (self.report)(RemoteEvent::Failed {
                    message: format!("{error:#}"),
                    explicit,
                });
                None
            }
        }
    }

    /// Hold back requests for `key` for [`REMOTE_FAILURE_BACKOFF`] from now.
    fn fail(&self, key: &RemoteKey) {
        *self.lock_blocked() = Some(Blocked {
            key: key.clone(),
            retry_at: self.clock.now() + REMOTE_FAILURE_BACKOFF,
            probing: false,
        });
    }

    fn lock_blocked(&self) -> MutexGuard<'_, Option<Blocked>> {
        self.blocked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The failure backoff shared by every remote search in this process.
static REMOTE_BREAKER: LazyLock<RemoteBreaker> =
    LazyLock::new(|| RemoteBreaker::new(Box::new(SystemClock), Box::new(report_remote_event)));

/// Explicit requests, and any request in a process without background loading, block.
fn load_mode(explicit: bool, background: bool) -> LoadMode {
    if explicit || !background {
        LoadMode::Blocking
    } else {
        LoadMode::Background
    }
}

static BACKGROUND_LOADING: AtomicBool = AtomicBool::new(false);

/// Let searches in this long-lived process load the local reranker without an explicit
/// request, on a background thread. Called by the daemon and the MCP server.
pub fn enable_background_loading() {
    BACKGROUND_LOADING.store(true, Ordering::Relaxed);
}

pub fn background_loading_enabled() -> bool {
    BACKGROUND_LOADING.load(Ordering::Relaxed)
}

/// Whether one search reranks its results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RerankDecision {
    Run,
    Skip,
    /// Reranking is configured, but this one-shot process did not ask to load the model.
    SkipOneShot,
}

/// Decide whether a search reranks.
///
/// `requested` is the per-search override: `Some(false)` disables reranking and
/// `Some(true)` loads a local model even in a one-shot process. Remote reranking loads
/// nothing, so it runs in every process. Nothing reranks while `mode` is off, and a blank
/// query never reranks.
pub fn decide(
    mode: RerankMode,
    requested: Option<bool>,
    background: bool,
    query: &str,
) -> RerankDecision {
    if mode == RerankMode::Off || requested == Some(false) || query.trim().is_empty() {
        RerankDecision::Skip
    } else if mode == RerankMode::Remote || requested == Some(true) || background {
        RerankDecision::Run
    } else {
        RerankDecision::SkipOneShot
    }
}

/// Text the reranker reads for `record`, at most `max_chars` characters.
///
/// A long text is cut to the window that holds the most specific query terms in
/// `terms`, preferring identifiers and long words over short ones. A record without text
/// is described by its tool name, a window of its tool input of at most half the budget,
/// and a window of its tool output in the rest.
pub fn document(record: &Record, terms: &[String], max_chars: usize) -> String {
    if !record.text.trim().is_empty() {
        return window(&record.text, terms, max_chars);
    }
    let mut document = String::new();
    let mut remaining = max_chars;
    let parts = [
        (record.tool_name.as_deref(), max_chars),
        (record.tool_input.as_deref(), max_chars / 2),
        (record.tool_output.as_deref(), max_chars),
    ];
    for (part, budget) in parts {
        let Some(part) = part.filter(|part| !part.trim().is_empty()) else {
            continue;
        };
        if !document.is_empty() {
            if remaining == 0 {
                break;
            }
            document.push('\n');
            remaining -= 1;
        }
        let part = window(part, terms, budget.min(remaining));
        remaining = remaining.saturating_sub(part.chars().count());
        document.push_str(&part);
    }
    document
}

/// Query terms that center document windows: the first [`MAX_WINDOW_TERMS`] literals of
/// `query`, each cut to [`MAX_WINDOW_TERM_CHARS`] characters, bounding the search cost
/// per candidate.
pub fn window_terms(query: &str) -> Vec<String> {
    crate::cli::query_literals(query)
        .into_iter()
        .take(MAX_WINDOW_TERMS)
        .map(|term| truncate_chars(&term, MAX_WINDOW_TERM_CHARS).to_string())
        .collect()
}

/// `text` cut to at most `max_chars` characters around its most specific query terms.
fn window(text: &str, terms: &[String], max_chars: usize) -> String {
    let total = text.chars().count();
    if total <= max_chars {
        return text.to_string();
    }
    // The window marks each cut with an ellipsis, which counts toward the budget.
    let limit = max_chars.saturating_sub(2);
    let start = window_start(text, total, terms, limit).min(total.saturating_sub(limit));
    let start_byte = text.char_indices().nth(start).map_or(0, |(index, _)| index);
    let rest = text.get(start_byte..).unwrap_or_default();
    let kept = truncate_chars(rest, limit);
    let mut window = String::with_capacity(kept.len() + 2 * '…'.len_utf8());
    if start_byte > 0 {
        window.push('…');
    }
    window.push_str(kept);
    if kept.len() < rest.len() {
        window.push('…');
    }
    truncate_chars(&window, max_chars).to_string()
}

/// Occurrences of one term kept for each match kind, bounding the cost on long tool output.
const MAX_WINDOW_HITS_PER_TERM: usize = 64;

/// One occurrence of a window term, in characters of the original text.
struct WindowHit {
    start: usize,
    end: usize,
    term: usize,
    weight: u32,
    identifier: bool,
}

/// One occurrence of a window term, in bytes of the lowercased text.
struct LoweredHit {
    start: usize,
    end: usize,
    term: usize,
    weight: u32,
    identifier: bool,
}

/// Weight of the identifier terms in a window, then of all its terms; compared in that
/// order.
type WindowScore = (u32, u32);

/// First character of the `limit`-character window with the best [`WindowScore`] of
/// distinct query terms, or 0 when the head of `text` scores as well.
///
/// A term weighs its length in characters, up to 32. A term of at least four characters
/// that holds a letter and a digit, `_`, `::`, or a lower-to-upper case change is an
/// identifier, unless it fills its hit cap in this text, so a window with an identifier
/// beats any window of plain words. An ASCII term shorter than three characters weighs
/// nothing. A hit inside a longer word counts half of a whole-token hit. Each term
/// contributes its best hit in the window, and ties go to the earliest window. The window
/// begins a quarter of `limit` before its first hit; the caller keeps it within the text.
fn window_start(text: &str, total: usize, terms: &[String], limit: usize) -> usize {
    let mut hits = window_hits(text, total, terms);
    if hits.is_empty() {
        return 0;
    }
    hits.sort_by_key(|hit| (hit.start, hit.term));
    let lead = limit / 4;
    let span = limit - lead;
    let mut best_by_term = vec![(0, false); terms.len()];
    // The head shows all of `0..limit`; any other window shows `lead` characters before
    // its first hit, leaving `span` for the hits.
    let head = window_score(&hits, 0, limit, &mut best_by_term);
    let mut best: Option<(WindowScore, usize)> = None;
    for (index, anchor) in hits.iter().enumerate() {
        let candidates = hits.get(index..).unwrap_or_default();
        // The anchor counts even when its term is longer than the window.
        let end = anchor.start.saturating_add(span).max(anchor.end);
        let score = window_score(candidates, anchor.start, end, &mut best_by_term);
        if best.is_none_or(|(best_score, _)| score > best_score) {
            best = Some((score, anchor.start));
        }
    }
    match best {
        Some((score, start)) if score > head => start.saturating_sub(lead),
        _ => 0,
    }
}

/// Score of the distinct terms with a hit inside characters `start..end` of the text,
/// given `hits` sorted by start.
fn window_score(
    hits: &[WindowHit],
    start: usize,
    end: usize,
    best_by_term: &mut [(u32, bool)],
) -> WindowScore {
    best_by_term.fill((0, false));
    for hit in hits {
        if hit.start >= end {
            break;
        }
        if hit.start < start || hit.end > end {
            continue;
        }
        if let Some(best) = best_by_term.get_mut(hit.term)
            && hit.weight > best.0
        {
            *best = (hit.weight, hit.identifier);
        }
    }
    best_by_term
        .iter()
        .fold((0, 0), |(identifiers, all), &(weight, identifier)| {
            let identifiers = if identifier {
                identifiers + weight
            } else {
                identifiers
            };
            (identifiers, all + weight)
        })
}

/// Occurrences of the weighted `terms` in `text` of `total` characters, matched
/// case-insensitively.
fn window_hits(text: &str, total: usize, terms: &[String]) -> Vec<WindowHit> {
    let lower = text.to_lowercase();
    let mut lowered_hits = Vec::new();
    for (index, term) in terms.iter().enumerate() {
        let (weight, identifier) = window_term_weight(term);
        if weight == 0 || terms.get(..index).unwrap_or_default().contains(term) {
            continue;
        }
        let first = lowered_hits.len();
        let (mut whole, mut partial) = (0, 0);
        for byte in memchr::memmem::find_iter(lower.as_bytes(), term.as_bytes()) {
            let end = byte + term.len();
            let bounded = |character: Option<char>| {
                character.is_none_or(|character| !character.is_alphanumeric() && character != '_')
            };
            let is_whole = bounded(lower.get(..byte).and_then(|head| head.chars().next_back()))
                && bounded(lower.get(end..).and_then(|tail| tail.chars().next()));
            let count = if is_whole { &mut whole } else { &mut partial };
            if *count >= MAX_WINDOW_HITS_PER_TERM {
                if whole >= MAX_WINDOW_HITS_PER_TERM && partial >= MAX_WINDOW_HITS_PER_TERM {
                    break;
                }
                continue;
            }
            *count += 1;
            lowered_hits.push(LoweredHit {
                start: byte,
                end,
                term: index,
                weight: if is_whole { 2 * weight } else { weight },
                identifier,
            });
        }
        // A term this common in the document cannot locate a passage in it.
        if whole >= MAX_WINDOW_HITS_PER_TERM || partial >= MAX_WINDOW_HITS_PER_TERM {
            for hit in lowered_hits.get_mut(first..).unwrap_or_default() {
                hit.identifier = false;
            }
        }
    }
    if text.is_ascii() {
        // Lowercasing ASCII keeps every byte in place, and bytes are characters.
        return lowered_hits
            .into_iter()
            .map(|hit| WindowHit {
                start: hit.start,
                end: hit.end,
                term: hit.term,
                weight: hit.weight,
                identifier: hit.identifier,
            })
            .collect();
    }
    let mut offsets = lowered_hits
        .iter()
        .flat_map(|hit| [hit.start, hit.end])
        .collect::<Vec<_>>();
    offsets.sort_unstable();
    offsets.dedup();
    // One pass over `text` maps every offset, since lowercasing can change a character's
    // byte length.
    let mut chars = Vec::with_capacity(offsets.len());
    let mut lowered = 0;
    for (index, character) in text.chars().enumerate() {
        while offsets
            .get(chars.len())
            .is_some_and(|byte| *byte <= lowered)
        {
            chars.push(index);
        }
        if chars.len() == offsets.len() {
            break;
        }
        lowered += character.to_lowercase().map(char::len_utf8).sum::<usize>();
    }
    chars.resize(offsets.len(), total);
    let position = |byte: usize| {
        offsets
            .binary_search(&byte)
            .ok()
            .and_then(|slot| chars.get(slot).copied())
            .unwrap_or(total)
    };
    lowered_hits
        .into_iter()
        .map(|hit| WindowHit {
            start: position(hit.start),
            end: position(hit.end),
            term: hit.term,
            weight: hit.weight,
            identifier: hit.identifier,
        })
        .collect()
}

/// How strongly `term` locates the passage a query is about, and whether it is an
/// identifier; see [`window_start`].
fn window_term_weight(term: &str) -> (u32, bool) {
    let length = term.chars().count();
    if length < 3 && term.is_ascii() {
        return (0, false);
    }
    let weight = u32::try_from(length.min(32)).unwrap_or(32);
    let camel_case = term
        .chars()
        .zip(term.chars().skip(1))
        .any(|(before, after)| before.is_lowercase() && after.is_uppercase());
    let identifier = length >= 4
        && term.chars().any(char::is_alphabetic)
        && (term
            .chars()
            .any(|character| character.is_ascii_digit() || character == '_')
            || term.contains("::")
            || camel_case);
    (weight, identifier)
}

fn truncate_chars(text: &str, max_chars: usize) -> &str {
    match text.char_indices().nth(max_chars) {
        Some((end, _)) => text.get(..end).unwrap_or(text),
        None => text,
    }
}

static ONE_SHOT_NOTICE_PRINTED: AtomicBool = AtomicBool::new(false);
static LOADING_NOTICE_PRINTED: AtomicBool = AtomicBool::new(false);
static BUSY_WARNING_PRINTED: AtomicBool = AtomicBool::new(false);
/// When each distinct failure was last shown.
static WARNED_FAILURES: LazyLock<Mutex<HashMap<String, Instant>>> = LazyLock::new(Default::default);
/// When each distinct remote request failure was last shown.
static WARNED_REMOTE_FAILURES: LazyLock<Mutex<HashMap<String, Instant>>> =
    LazyLock::new(Default::default);

/// Tell the user, once per process, that a one-shot search skipped reranking.
pub fn notify_one_shot_skip() {
    notify_once(
        &ONE_SHOT_NOTICE_PRINTED,
        ONE_SHOT_NOTICE,
        &mut std::io::stderr().lock(),
    );
}

/// Warn about a reranking failure once per distinct error and [`LOAD_RETRY_DELAY`], so a
/// lasting failure is repeated as often as a failed load is retried.
pub fn warn_failure(error: &anyhow::Error) {
    warn_failure_once(
        &WARNED_FAILURES,
        &format!("{error:#}"),
        Instant::now(),
        LOAD_RETRY_DELAY,
        &mut std::io::stderr().lock(),
    );
}

fn failure_warning(message: &str) -> String {
    format!("warning: reranking failed, keeping the original order: {message}")
}

fn notify_once(printed: &AtomicBool, message: &str, out: &mut impl Write) -> bool {
    if printed.swap(true, Ordering::Relaxed) {
        return false;
    }
    let _ = writeln!(out, "{message}");
    true
}

/// Show `message` unless it was shown within `window`; at most [`MAX_FAILURE_WARNINGS`]
/// distinct messages are tracked per window.
fn warn_failure_once(
    warned: &Mutex<HashMap<String, Instant>>,
    message: &str,
    now: Instant,
    window: Duration,
    out: &mut impl Write,
) -> bool {
    let mut warned = warned
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let due = |last: &Instant| now.saturating_duration_since(*last) >= window;
    if warned.get(message).is_some_and(|last| !due(last)) {
        return false;
    }
    if !warned.contains_key(message) && warned.len() >= MAX_FAILURE_WARNINGS {
        warned.retain(|_, last| !due(last));
        if warned.len() >= MAX_FAILURE_WARNINGS {
            return false;
        }
    }
    warned.insert(message.to_string(), now);
    let _ = writeln!(out, "{}", failure_warning(message));
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RecordLinks, SourceKind};

    fn record(text: &str) -> Record {
        Record {
            source: SourceKind::Claude,
            doc_id: 1,
            ts: 1,
            project: "memex".to_string(),
            session_id: "session".to_string(),
            turn_id: 0,
            role: "tool_use".to_string(),
            text: text.to_string(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks::default(),
            source_path: "/tmp/session.jsonl".to_string(),
        }
    }

    #[test]
    fn model_names_resolve_case_insensitively_with_fastembed_variants() {
        for (name, model) in [
            ("jina-turbo", RerankerModel::JINARerankerV1TurboEn),
            ("BGE-V2-M3", RerankerModel::BGERerankerV2M3),
            (" bge-base ", RerankerModel::BGERerankerBase),
            ("jina-v2", RerankerModel::JINARerankerV2BaseMultiligual),
            (
                "JINARerankerV1TurboEn",
                RerankerModel::JINARerankerV1TurboEn,
            ),
            ("bgererankerv2m3", RerankerModel::BGERerankerV2M3),
            (
                "JINARerankerV2BaseMultiligual",
                RerankerModel::JINARerankerV2BaseMultiligual,
            ),
        ] {
            assert_eq!(parse_model(name).expect(name), model, "{name}");
            assert_eq!(parse_model(model_name(&model)).expect("round trip"), model);
        }
        let error = parse_model("ms-marco")
            .expect_err("unknown model")
            .to_string();
        assert!(error.contains("'ms-marco'"), "{error}");
        assert!(error.contains(SUPPORTED_MODEL_NAMES), "{error}");
    }

    #[test]
    fn decision_requires_configuration_a_query_and_an_opt_in() {
        use RerankDecision::{Run, Skip, SkipOneShot};
        use RerankMode::{Local, Off, Remote};
        for (mode, requested, background, query, expected) in [
            (Off, Some(true), true, "q", Skip),
            (Local, Some(false), true, "q", Skip),
            (Local, Some(true), true, " \t\n", Skip),
            (Local, None, true, "", Skip),
            (Local, None, true, "q", Run),
            (Local, Some(true), false, "q", Run),
            (Local, None, false, "q", SkipOneShot),
            // Remote reranking loads nothing, so one-shot processes run it too.
            (Remote, None, false, "q", Run),
            (Remote, None, true, "q", Run),
            (Remote, Some(true), false, "q", Run),
            (Remote, Some(false), false, "q", Skip),
            (Remote, Some(false), true, "q", Skip),
            (Remote, None, false, " ", Skip),
            (Remote, Some(true), false, "", Skip),
        ] {
            assert_eq!(
                decide(mode, requested, background, query),
                expected,
                "{mode:?} {requested:?} {background} {query:?}"
            );
        }
    }

    fn remote_config(url: &str) -> RemoteRerankConfig {
        RemoteRerankConfig {
            endpoint: crate::test_support::remote_server::endpoint(url, None, 0),
            model: Some("rerank-test".to_string()),
            dialect: Default::default(),
            extra_body: None,
        }
    }

    struct TestBreaker {
        breaker: RemoteBreaker,
        now: Arc<Mutex<Instant>>,
        events: Arc<Mutex<Vec<RemoteEvent>>>,
    }

    impl TestBreaker {
        fn new() -> Self {
            let now = Arc::new(Mutex::new(Instant::now()));
            let events = Arc::new(Mutex::new(Vec::new()));
            let reported = Arc::clone(&events);
            let breaker = RemoteBreaker::new(
                Box::new(FakeClock {
                    now: Arc::clone(&now),
                    real_sleep: false,
                    hook: Arc::new(Mutex::new(None)),
                }),
                Box::new(move |event| reported.lock().expect("events").push(event)),
            );
            Self {
                breaker,
                now,
                events,
            }
        }

        fn advance(&self, duration: Duration) {
            *self.now.lock().expect("clock") += duration;
        }

        fn events(&self) -> Vec<RemoteEvent> {
            std::mem::take(&mut *self.events.lock().expect("events"))
        }
    }

    #[test]
    fn remote_failure_holds_back_requests_for_the_backoff() {
        let breaker = TestBreaker::new();
        let config = remote_config("https://rerank.example.test/v1/rerank");
        let requests = std::cell::Cell::new(0);
        let failing = || {
            requests.set(requests.get() + 1);
            Err(anyhow!("rerank server returned HTTP 503: down"))
        };
        assert_eq!(breaker.breaker.score(&config, false, failing), None);
        assert_eq!(
            breaker.events(),
            vec![RemoteEvent::Failed {
                message: "rerank server returned HTTP 503: down".to_string(),
                explicit: false
            }]
        );
        // Within the window no request is sent; explicit requests warn every time.
        for explicit in [false, true, true, false] {
            assert_eq!(breaker.breaker.score(&config, explicit, failing), None);
            breaker.advance(Duration::from_secs(10));
        }
        assert_eq!(requests.get(), 1);
        assert_eq!(
            breaker.events(),
            vec![
                RemoteEvent::Skipped { explicit: false },
                RemoteEvent::Skipped { explicit: true },
                RemoteEvent::Skipped { explicit: true },
                RemoteEvent::Skipped { explicit: false },
            ]
        );
        // Other settings, such as a reloaded configuration, are not held back.
        let other = remote_config("https://other.example.test/rerank");
        assert_eq!(
            breaker.breaker.score(&other, false, || Ok(vec![0.5])),
            Some(vec![0.5])
        );
        let other_body = RemoteRerankConfig {
            extra_body: RerankExtraBody::parse(r#"{"provider": {"zdr": false}}"#).expect("parse"),
            ..config.clone()
        };
        assert_eq!(
            breaker.breaker.score(&other_body, false, || Ok(vec![0.5])),
            Some(vec![0.5])
        );

        breaker.advance(REMOTE_FAILURE_BACKOFF - Duration::from_secs(40));
        assert_eq!(
            breaker.breaker.score(&config, true, || Ok(vec![0.25])),
            Some(vec![0.25])
        );
        assert_eq!(breaker.events(), Vec::new());
        // Success clears the backoff.
        assert_eq!(breaker.breaker.score(&config, false, failing), None);
        assert_eq!(requests.get(), 2);
        breaker.advance(REMOTE_FAILURE_BACKOFF);
        assert_eq!(
            breaker.breaker.score(&config, false, || Ok(vec![0.75])),
            Some(vec![0.75])
        );
        assert_eq!(
            breaker.breaker.score(&config, false, || Ok(vec![0.75])),
            Some(vec![0.75])
        );
    }

    #[test]
    fn remote_probe_after_the_backoff_is_the_only_request_until_it_finishes() {
        let breaker = TestBreaker::new();
        let config = remote_config("https://rerank.example.test/rerank");
        assert_eq!(
            breaker
                .breaker
                .score(&config, false, || Err(anyhow!("timeout"))),
            None
        );
        breaker.advance(REMOTE_FAILURE_BACKOFF);
        let concurrent = breaker.breaker.score(&config, true, || {
            // A search that starts while this probe runs skips without a request, even
            // after the probe has run past another backoff period.
            breaker.advance(REMOTE_FAILURE_BACKOFF * 2);
            assert_eq!(
                breaker
                    .breaker
                    .score(&config, false, || panic!("second request")),
                None
            );
            Err(anyhow!("timeout"))
        });
        assert_eq!(concurrent, None);
        // The probe's failure starts a new backoff from when it finished.
        breaker.advance(REMOTE_FAILURE_BACKOFF - Duration::from_secs(1));
        assert_eq!(
            breaker
                .breaker
                .score(&config, false, || panic!("held back")),
            None
        );
        breaker.advance(Duration::from_secs(1));
        assert_eq!(
            breaker.breaker.score(&config, false, || Ok(vec![0.5])),
            Some(vec![0.5])
        );
        assert_eq!(
            breaker.events(),
            vec![
                RemoteEvent::Failed {
                    message: "timeout".to_string(),
                    explicit: false
                },
                RemoteEvent::Skipped { explicit: false },
                RemoteEvent::Failed {
                    message: "timeout".to_string(),
                    explicit: true
                },
                RemoteEvent::Skipped { explicit: false },
            ]
        );
    }

    #[test]
    fn remote_backoff_is_keyed_on_url_model_and_dialect_only() {
        let breaker = TestBreaker::new();
        let config = remote_config("https://rerank.example.test/rerank");
        assert_eq!(
            breaker
                .breaker
                .score(&config, false, || Err(anyhow!("down"))),
            None
        );
        // Another key or timeout is the same endpoint and stays held back.
        let mut rekeyed = config.clone();
        rekeyed.endpoint.api_key = Some("sk-other".to_string());
        rekeyed.endpoint.timeout = Duration::from_secs(99);
        assert_eq!(
            breaker
                .breaker
                .score(&rekeyed, false, || panic!("held back")),
            None
        );
        assert!(!format!("{:?}", breaker.breaker.lock_blocked()).contains("sk-other"));
        let mut other_model = config.clone();
        other_model.model = Some("other".to_string());
        let mut other_dialect = config.clone();
        other_dialect.dialect = RerankDialect::Texts;
        for other in [other_model, other_dialect] {
            assert_eq!(
                breaker.breaker.score(&other, false, || Ok(vec![0.5])),
                Some(vec![0.5])
            );
        }
    }

    #[test]
    fn remote_texts_dialect_scores_through_the_shared_breaker() -> Result<()> {
        use crate::test_support::remote_server::{join, reply, serve};
        let (base, server) = serve(|_, body| {
            let count = body["texts"].as_array().map_or(0, Vec::len);
            let items: Vec<serde_json::Value> = (0..count)
                .map(|index| serde_json::json!({"index": index, "score": 2.0 - index as f64}))
                .collect();
            reply(200, serde_json::Value::Array(items))
        })?;
        let mut remote = remote_config(&format!("{base}/rerank"));
        remote.dialect = RerankDialect::Texts;
        remote.model = None;
        let settings = RerankSettings {
            engine: RerankEngine::Remote(remote),
            candidates: DEFAULT_RERANK_CANDIDATES,
            doc_chars: DEFAULT_RERANK_DOC_CHARS,
        };
        let documents = vec!["one".to_string(), "two".to_string()];
        let scores = score(&settings, false, "query", &documents)?.expect("scored");
        let captured = join(server)?;
        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0].body,
            serde_json::json!({"query": "query", "texts": ["one", "two"], "truncate": true})
        );
        // Logits outside [0, 1] come back as probabilities.
        assert_eq!(scores, vec![sigmoid(2.0)?, sigmoid(1.0)?]);
        Ok(())
    }

    #[test]
    fn remote_failures_warn_once_per_error_and_backoff_window() {
        let warned = Mutex::new(HashMap::new());
        let start = Instant::now();
        let mut out = Vec::new();
        let window = REMOTE_FAILURE_BACKOFF;
        assert!(warn_failure_once(&warned, "down", start, window, &mut out));
        let almost = start + window - Duration::from_secs(1);
        assert!(!warn_failure_once(
            &warned, "down", almost, window, &mut out
        ));
        assert!(warn_failure_once(
            &warned,
            "down",
            start + window,
            window,
            &mut out
        ));
        assert_eq!(
            String::from_utf8(out).expect("utf8"),
            "warning: reranking failed, keeping the original order: down\n".repeat(2)
        );
    }

    #[test]
    fn documents_prefer_text_and_cap_on_char_boundaries() {
        assert_eq!(document(&record("héllo wörld"), &[], 4), "hé…");
        assert_eq!(document(&record("short"), &[], 200), "short");

        let mut tool = record(" \n");
        tool.tool_name = Some("Bash".to_string());
        tool.tool_input = Some("é".repeat(300));
        tool.tool_output = Some("ö".repeat(300));
        let text = document(&tool, &[], 200);
        assert!(text.chars().count() <= 200, "{text}");
        let input_head = format!("Bash\n{}…\n", "é".repeat(98));
        assert!(text.starts_with(&input_head), "{text}");
        assert!(text.ends_with("ö…"), "{text}");

        let mut output_only = record("");
        output_only.tool_output = Some("exit 0".to_string());
        output_only.tool_input = Some(String::new());
        assert_eq!(document(&output_only, &[], 200), "exit 0");
        assert_eq!(document(&record(""), &[], 200), "");
    }

    type SleepHook = Arc<Mutex<Option<Box<dyn FnMut() + Send>>>>;

    /// A clock whose sleeps advance its time by the full duration. With `real_sleep` each
    /// sleep also takes a millisecond so other threads progress while a search polls; a
    /// hook runs on the next sleep, once.
    struct FakeClock {
        now: Arc<Mutex<Instant>>,
        real_sleep: bool,
        hook: SleepHook,
    }

    impl Clock for FakeClock {
        fn now(&self) -> Instant {
            *self.now.lock().expect("clock")
        }

        fn sleep(&self, duration: Duration) {
            *self.now.lock().expect("clock") += duration;
            if let Some(mut hook) = self.hook.lock().expect("hook").take() {
                hook();
            }
            if self.real_sleep {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }

    /// Scores every document with its value; `NaN` makes inference panic.
    struct Constant(f32);

    impl RerankBackend for Constant {
        fn rerank(&mut self, _query: &str, documents: &[String]) -> Result<Vec<f32>> {
            if self.0.is_nan() {
                panic!("inference panicked");
            }
            Ok(vec![self.0; documents.len()])
        }
    }

    struct TestSlot {
        slot: Arc<ModelSlot<Constant>>,
        loads: Arc<std::sync::atomic::AtomicUsize>,
        now: Arc<Mutex<Instant>>,
        hook: SleepHook,
        events: Arc<Mutex<Vec<SlotEvent>>>,
    }

    /// A slot whose loader counts its calls and then runs `load`.
    fn test_slot(
        load: impl Fn(&RerankerModel) -> Result<Constant> + Send + Sync + 'static,
    ) -> TestSlot {
        test_slot_with(load, true)
    }

    fn test_slot_with(
        load: impl Fn(&RerankerModel) -> Result<Constant> + Send + Sync + 'static,
        real_sleep: bool,
    ) -> TestSlot {
        let loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let now = Arc::new(Mutex::new(Instant::now()));
        let events = Arc::new(Mutex::new(Vec::new()));
        let hook: SleepHook = Arc::new(Mutex::new(None));
        let counter = Arc::clone(&loads);
        let reported = Arc::clone(&events);
        let slot = Arc::new(ModelSlot::new(
            Box::new(move |model: &RerankerModel, _: &EmbedRuntimeConfig| {
                counter.fetch_add(1, Ordering::SeqCst);
                load(model)
            }),
            Box::new(FakeClock {
                now: Arc::clone(&now),
                real_sleep,
                hook: Arc::clone(&hook),
            }),
            Box::new(move |event| reported.lock().expect("events").push(event)),
        ));
        TestSlot {
            slot,
            loads,
            now,
            hook,
            events,
        }
    }

    impl TestSlot {
        fn score(&self, model: RerankerModel, mode: LoadMode) -> Result<Option<Vec<f32>>> {
            self.slot.score(
                &model,
                &EmbedRuntimeConfig::default(),
                mode,
                "query",
                &["one".to_string(), "two".to_string()],
            )
        }

        fn loads(&self) -> usize {
            self.loads.load(Ordering::SeqCst)
        }

        fn advance(&self, duration: Duration) {
            *self.now.lock().expect("clock") += duration;
        }

        /// The events reported since the last call.
        fn events(&self) -> Vec<SlotEvent> {
            std::mem::take(&mut *self.events.lock().expect("events"))
        }

        /// Wait for a background load to leave the `Loading` state.
        fn settle(&self) {
            for _ in 0..500 {
                if !matches!(*self.slot.lock_state(), SlotState::Loading { .. }) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            panic!("background load did not finish");
        }
    }

    const TURBO: RerankerModel = RerankerModel::JINARerankerV1TurboEn;

    #[test]
    fn loaded_model_is_reused_until_the_key_changes() {
        let slot = test_slot(|_| Ok(Constant(0.5)));
        for _ in 0..3 {
            assert_eq!(
                slot.score(TURBO, LoadMode::Blocking).expect("score"),
                Some(vec![0.5, 0.5])
            );
        }
        assert_eq!(slot.loads(), 1);
        slot.score(RerankerModel::BGERerankerBase, LoadMode::Blocking)
            .expect("other model");
        assert_eq!(slot.loads(), 2);
        let other_runtime = EmbedRuntimeConfig {
            execution_provider: crate::embed::ExecutionProviderChoice::Cpu,
            ..EmbedRuntimeConfig::default()
        };
        slot.slot
            .score(
                &RerankerModel::BGERerankerBase,
                &other_runtime,
                LoadMode::Blocking,
                "query",
                &[],
            )
            .expect("other runtime");
        assert_eq!(slot.loads(), 3);
    }

    #[test]
    fn failed_load_is_retried_only_after_the_retry_delay() {
        let slot = test_slot(|_| Err(anyhow!("download failed")));
        let error = format!(
            "{:#}",
            slot.score(TURBO, LoadMode::Blocking).expect_err("load")
        );
        assert!(error.contains("download failed"), "{error}");
        for mode in [LoadMode::Blocking, LoadMode::Blocking, LoadMode::Background] {
            assert_eq!(slot.score(TURBO, mode).expect("skip"), None);
        }
        slot.advance(LOAD_RETRY_DELAY - Duration::from_secs(1));
        assert_eq!(slot.score(TURBO, LoadMode::Blocking).expect("skip"), None);
        assert_eq!(slot.loads(), 1);
        // Explicit requests are told every time; background searches skip quietly.
        assert_eq!(
            slot.events(),
            vec![
                SlotEvent::RecentFailure { explicit: true },
                SlotEvent::RecentFailure { explicit: true },
                SlotEvent::RecentFailure { explicit: false },
                SlotEvent::RecentFailure { explicit: true },
            ]
        );
        slot.advance(Duration::from_secs(1));
        assert!(slot.score(TURBO, LoadMode::Blocking).is_err());
        assert_eq!(slot.loads(), 2);
        // Another model is not held back by this model's failure.
        assert!(
            slot.score(RerankerModel::BGERerankerBase, LoadMode::Blocking)
                .is_err()
        );
        assert_eq!(slot.loads(), 3);
    }

    #[test]
    fn background_load_never_blocks_searches() {
        let (release, released) = std::sync::mpsc::channel::<()>();
        let released = Mutex::new(released);
        let slot = test_slot(move |_| {
            released
                .lock()
                .expect("receiver")
                .recv_timeout(Duration::from_secs(10))?;
            Ok(Constant(0.25))
        });
        let started = Instant::now();
        for _ in 0..2 {
            assert_eq!(
                slot.score(TURBO, LoadMode::Background).expect("loading"),
                None
            );
        }
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(slot.events(), vec![SlotEvent::Loading, SlotEvent::Loading]);
        release.send(()).expect("release the load");
        slot.settle();
        assert_eq!(
            slot.score(TURBO, LoadMode::Background).expect("ready"),
            Some(vec![0.25, 0.25])
        );
        assert_eq!(slot.loads(), 1);
    }

    #[test]
    fn explicit_requests_block_even_with_background_loading() {
        assert_eq!(load_mode(true, true), LoadMode::Blocking);
        assert_eq!(load_mode(true, false), LoadMode::Blocking);
        assert_eq!(load_mode(false, false), LoadMode::Blocking);
        assert_eq!(load_mode(false, true), LoadMode::Background);
    }

    #[test]
    fn explicit_request_waits_for_another_searchs_load() {
        let (release, released) = std::sync::mpsc::channel::<()>();
        let released = Mutex::new(released);
        let slot = test_slot(move |_| {
            released
                .lock()
                .expect("receiver")
                .recv_timeout(Duration::from_secs(10))?;
            Ok(Constant(0.25))
        });
        assert_eq!(
            slot.score(TURBO, LoadMode::Background).expect("loading"),
            None
        );
        // The explicit request's first poll proves it is waiting; only then does the
        // background load finish, and the poll returns once the model is ready.
        let watched = Arc::downgrade(&slot.slot);
        *slot.hook.lock().expect("hook") = Some(Box::new(move || {
            release.send(()).expect("release the load");
            let slot = watched.upgrade().expect("slot");
            for _ in 0..1000 {
                if matches!(*slot.lock_state(), SlotState::Ready(..)) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            panic!("background load did not finish");
        }));
        let before = *slot.now.lock().expect("clock");
        assert_eq!(
            slot.score(TURBO, LoadMode::Blocking).expect("waited"),
            Some(vec![0.25, 0.25])
        );
        assert_eq!(*slot.now.lock().expect("clock") - before, LOADING_POLL);
        assert_eq!(slot.loads(), 1);
        assert_eq!(slot.events(), vec![SlotEvent::Loading]);
    }

    #[test]
    fn explicit_load_panic_fails_the_search_and_backs_off() {
        let slot = test_slot(|_| panic!("loader panicked"));
        let error = slot
            .score(TURBO, LoadMode::Blocking)
            .expect_err("panicked load");
        assert_eq!(error.to_string(), "reranker panicked: loader panicked");
        assert!(matches!(*slot.slot.lock_state(), SlotState::Failed { .. }));
        assert_eq!(
            slot.score(TURBO, LoadMode::Blocking).expect("backoff"),
            None
        );
        assert_eq!(slot.loads(), 1);
        assert_eq!(
            slot.events(),
            vec![SlotEvent::RecentFailure { explicit: true }]
        );
    }

    #[test]
    fn inference_panic_fails_the_search_and_reloads_the_model() {
        let slot = test_slot(|_| Ok(Constant(f32::NAN)));
        let error = slot
            .score(TURBO, LoadMode::Blocking)
            .expect_err("panicked inference");
        assert_eq!(error.to_string(), "reranker panicked: inference panicked");
        assert!(matches!(*slot.slot.lock_state(), SlotState::Idle));
        assert!(slot.score(TURBO, LoadMode::Blocking).is_err());
        assert_eq!(slot.loads(), 2);
        let unknown: Box<dyn std::any::Any + Send> = Box::new(7_u8);
        assert_eq!(
            panic_message(unknown.as_ref()),
            "reranker panicked: unknown panic"
        );
        let owned: Box<dyn std::any::Any + Send> = Box::new(String::from("owned"));
        assert_eq!(panic_message(owned.as_ref()), "reranker panicked: owned");
    }

    #[test]
    fn explicit_load_that_never_returns_fails_open_at_the_deadline() {
        let (_release, released) = std::sync::mpsc::channel::<()>();
        let released = Mutex::new(released);
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let slot = test_slot_with(
            move |_| {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    // Never released; the receiver errors once the test drops the sender.
                    let _ = released.lock().expect("receiver").recv();
                    return Ok(Constant(0.1));
                }
                Ok(Constant(0.9))
            },
            false,
        );
        let before = *slot.now.lock().expect("clock");
        assert_eq!(
            slot.score(TURBO, LoadMode::Blocking).expect("timed out"),
            None
        );
        let waited = *slot.now.lock().expect("clock") - before;
        assert!(
            waited >= LOAD_DEADLINE && waited < LOAD_DEADLINE + LOADING_POLL * 2,
            "{waited:?}"
        );
        assert!(matches!(*slot.slot.lock_state(), SlotState::Failed { .. }));
        assert_eq!(slot.events(), vec![SlotEvent::TimedOut { explicit: true }]);
        slot.advance(LOAD_RETRY_DELAY);
        assert_eq!(
            slot.score(TURBO, LoadMode::Blocking).expect("reloaded"),
            Some(vec![0.9, 0.9])
        );
        assert_eq!(slot.loads(), 2);
    }

    #[test]
    fn explicit_load_replaced_by_another_search_warns() {
        let (release, released) = std::sync::mpsc::channel::<()>();
        let released = Mutex::new(released);
        let (started, load_started) = std::sync::mpsc::channel::<()>();
        let started = Mutex::new(started);
        let slot = Arc::new(test_slot(move |model| {
            if *model == TURBO {
                let _ = started.lock().expect("sender").send(());
                released
                    .lock()
                    .expect("receiver")
                    .recv_timeout(Duration::from_secs(10))?;
            }
            Ok(Constant(0.5))
        }));
        let waiting = Arc::clone(&slot);
        let explicit =
            std::thread::spawn(move || waiting.score(TURBO, LoadMode::Blocking).expect("skip"));
        load_started
            .recv_timeout(Duration::from_secs(10))
            .expect("explicit load started");
        assert_eq!(
            slot.score(RerankerModel::BGERerankerBase, LoadMode::Blocking)
                .expect("other model"),
            Some(vec![0.5, 0.5])
        );
        release.send(()).expect("release the first load");
        assert_eq!(explicit.join().expect("explicit search"), None);
        assert_eq!(slot.events(), vec![SlotEvent::Superseded]);
        assert!(matches!(*slot.slot.lock_state(), SlotState::Ready(..)));
    }

    #[test]
    fn explicit_request_stops_waiting_after_a_minute() {
        let (_release, released) = std::sync::mpsc::channel::<()>();
        let released = Mutex::new(released);
        let slot = test_slot(move |_| {
            // Never released; the receiver errors once the test drops the sender.
            let _ = released.lock().expect("receiver").recv();
            Ok(Constant(0.25))
        });
        assert_eq!(
            slot.score(TURBO, LoadMode::Background).expect("loading"),
            None
        );
        for _ in 0..2 {
            let before = *slot.now.lock().expect("clock");
            assert_eq!(
                slot.score(TURBO, LoadMode::Blocking)
                    .expect("still loading"),
                None
            );
            let waited = *slot.now.lock().expect("clock") - before;
            assert!(
                waited >= LOADING_WAIT && waited < LOADING_WAIT + LOADING_POLL * 2,
                "{waited:?}"
            );
        }
        assert_eq!(slot.loads(), 1);
        assert_eq!(
            slot.events(),
            vec![
                SlotEvent::Loading,
                SlotEvent::StillLoading,
                SlotEvent::StillLoading
            ]
        );
    }

    #[test]
    fn failed_or_panicking_background_load_waits_for_the_retry_delay() {
        let slot = test_slot(|model| match model {
            RerankerModel::BGERerankerBase => panic!("loader panicked"),
            _ => Err(anyhow!("background download failed")),
        });
        for model in [TURBO, RerankerModel::BGERerankerBase] {
            assert_eq!(
                slot.score(model.clone(), LoadMode::Background)
                    .expect("loading"),
                None
            );
            slot.settle();
            assert!(matches!(*slot.slot.lock_state(), SlotState::Failed { .. }));
            assert_eq!(slot.score(model, LoadMode::Background).expect("skip"), None);
        }
        assert_eq!(slot.loads(), 2);
        let events = slot.events();
        assert!(
            events.contains(&SlotEvent::LoadFailed(
                "load the reranker model: background download failed".to_string()
            )),
            "{events:?}"
        );
        assert!(
            events.contains(&SlotEvent::LoadFailed(
                "reranker panicked: loader panicked".to_string()
            )),
            "{events:?}"
        );
    }

    #[test]
    fn hung_load_fails_after_the_deadline_and_its_late_result_is_discarded() {
        let (_release, released) = std::sync::mpsc::channel::<()>();
        let released = Mutex::new(released);
        let (started, load_started) = std::sync::mpsc::channel::<()>();
        let started = Mutex::new(started);
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let slot = test_slot(move |_| {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let _ = started.lock().expect("sender").send(());
                // Never released; the receiver errors once the test drops the sender.
                let _ = released.lock().expect("receiver").recv();
                return Ok(Constant(0.1));
            }
            Ok(Constant(0.9))
        });
        assert_eq!(
            slot.score(TURBO, LoadMode::Background).expect("loading"),
            None
        );
        load_started
            .recv_timeout(Duration::from_secs(10))
            .expect("background load started");
        let stale = match &*slot.slot.lock_state() {
            SlotState::Loading { load, .. } => load.clone(),
            _ => panic!("model is loading"),
        };
        slot.advance(LOAD_DEADLINE - Duration::from_secs(1));
        assert_eq!(
            slot.score(TURBO, LoadMode::Background).expect("loading"),
            None
        );
        assert!(matches!(*slot.slot.lock_state(), SlotState::Loading { .. }));
        slot.advance(Duration::from_secs(1));
        assert_eq!(
            slot.score(TURBO, LoadMode::Background).expect("timed out"),
            None
        );
        assert!(matches!(*slot.slot.lock_state(), SlotState::Failed { .. }));
        assert_eq!(
            slot.events(),
            vec![
                SlotEvent::Loading,
                SlotEvent::Loading,
                SlotEvent::TimedOut { explicit: false }
            ]
        );
        // The normal backoff applies before another load starts.
        slot.advance(LOAD_RETRY_DELAY - Duration::from_secs(1));
        assert_eq!(
            slot.score(TURBO, LoadMode::Blocking).expect("backoff"),
            None
        );
        assert_eq!(slot.loads(), 1);
        slot.advance(Duration::from_secs(1));
        assert_eq!(
            slot.score(TURBO, LoadMode::Blocking).expect("reloaded"),
            Some(vec![0.9, 0.9])
        );
        assert_eq!(slot.loads(), 2);
        // A late result from the hung load never replaces the current model, even though
        // it was for the same key.
        assert!(
            slot.slot
                .finish_load(&stale, Ok(Constant(0.1)))
                .expect("stale")
                .is_none()
        );
        assert_eq!(
            slot.score(TURBO, LoadMode::Blocking)
                .expect("current model"),
            Some(vec![0.9, 0.9])
        );
    }

    #[test]
    fn window_terms_are_capped_in_number_and_length() {
        let query = (0..40)
            .map(|index| format!("term{index:02}"))
            .collect::<Vec<_>>()
            .join(" ");
        let terms = window_terms(&query);
        assert_eq!(terms.len(), MAX_WINDOW_TERMS);
        assert_eq!(terms.first().map(String::as_str), Some("term00"));
        assert_eq!(terms.last().map(String::as_str), Some("term15"));

        let long = "é".repeat(200);
        let terms = window_terms(&format!("{long} short"));
        assert_eq!(terms[0], "é".repeat(MAX_WINDOW_TERM_CHARS));
        assert_eq!(terms[1], "short");
        // A cut term still centers the window on its occurrence.
        let mut record = record(&format!("{} {long} tail", "lead ".repeat(1000)));
        record.doc_id = 2;
        let document = document(&record, &terms, 200);
        assert!(document.contains(&long[..20]), "{document}");
    }

    fn windowed(text: &str, query: &str, max_chars: usize) -> String {
        let document = document(&record(text), &window_terms(query), max_chars);
        assert!(document.chars().count() <= max_chars, "{document}");
        document
    }

    #[test]
    fn window_prefers_a_late_identifier_over_early_context_words() {
        let text = format!(
            "fix the build {} ERR_ABC123 evidence {}",
            "lead ".repeat(2000),
            "tail ".repeat(2000)
        );
        let document = windowed(&text, "fix the build ERR_ABC123", 1500);
        assert!(document.contains("ERR_ABC123 evidence"), "{document}");
        assert!(document.starts_with('…') && document.ends_with('…'));
    }

    #[test]
    fn window_keeps_the_head_when_it_holds_the_best_terms() {
        let text = format!(
            "ERR_ABC123 first {} build {}",
            "lead ".repeat(2000),
            "tail ".repeat(2000)
        );
        let document = windowed(&text, "build ERR_ABC123", 1500);
        assert!(document.starts_with("ERR_ABC123 first"), "{document}");

        let document = windowed(&text, "absent", 1500);
        assert!(document.starts_with("ERR_ABC123 first"), "{document}");
        assert!(document.ends_with('…'));

        let short = "fix the build ERR_ABC123";
        assert_eq!(windowed(short, "ERR_ABC123", 1500), short);
    }

    #[test]
    fn window_on_a_repeated_identifier_is_the_earliest() {
        let text = format!(
            "{} ERR_ABC123 one {} ERR_ABC123 two {} ERR_ABC123 three",
            "lead ".repeat(1000),
            "mid ".repeat(1000),
            "late ".repeat(1000)
        );
        let document = windowed(&text, "ERR_ABC123", 300);
        assert!(document.contains("ERR_ABC123 one"), "{document}");
        assert_eq!(document, windowed(&text, "ERR_ABC123", 300));
    }

    #[test]
    fn window_covers_several_terms_that_occur_together() {
        let text = format!(
            "{} alpha_one alone {} alpha_one beta_two together {}",
            "lead ".repeat(1000),
            "mid ".repeat(1000),
            "tail ".repeat(1000)
        );
        let document = windowed(&text, "alpha_one beta_two", 300);
        assert!(
            document.contains("alpha_one beta_two together"),
            "{document}"
        );
    }

    #[test]
    fn window_ignores_short_terms_and_prefers_whole_tokens() {
        let text = format!("idea {} id {}", "lead ".repeat(2000), "tail ".repeat(2000));
        let document = windowed(&text, "id", 300);
        assert!(document.starts_with("idea lead"), "{document}");

        let text = format!(
            "dialog {} log entry {}",
            "lead ".repeat(2000),
            "tail ".repeat(2000)
        );
        let document = windowed(&text, "log", 300);
        assert!(document.contains(" log entry"), "{document}");
        assert!(!document.contains("dialog"), "{document}");
    }

    #[test]
    fn window_maps_multibyte_and_case_changing_text() {
        // `İ` lowercases to two characters, so offsets into the lowercased text drift.
        let text = format!("{} ERR_ABC123 ö {}", "İ".repeat(3000), "ö".repeat(3000));
        let document = windowed(&text, "err_abc123", 300);
        assert!(document.contains("ERR_ABC123 ö"), "{document}");
        let lead = document
            .find("ERR_ABC123")
            .map(|byte| document[..byte].chars().count());
        // The hit sits a quarter of the window from its start, as when nothing drifts.
        assert_eq!(lead, Some(298 / 4 + 1));
    }

    #[test]
    fn window_starts_at_a_term_longer_than_the_window() {
        let long = "abc".repeat(100);
        let text = format!("{} {long} {}", "lead ".repeat(2000), "tail ".repeat(2000));
        let document = document(&record(&text), std::slice::from_ref(&long), 200);
        assert!(document.starts_with('…'), "{document}");
        assert!(document.contains(&long[..100]), "{document}");
        assert!(document.chars().count() <= 200);
    }

    #[test]
    fn tool_record_windows_center_each_part_on_the_identifier() {
        let mut record = record("");
        record.tool_name = Some("Bash".to_string());
        record.tool_input = Some(format!("cargo build {}", "x ".repeat(1000)));
        record.tool_output = Some(format!(
            "build started {} ERR_ABC123 failed {}",
            "lead ".repeat(2000),
            "tail ".repeat(500)
        ));
        let document = document(&record, &window_terms("build ERR_ABC123"), 600);
        assert!(document.chars().count() <= 600, "{document}");
        let parts = document.split('\n').collect::<Vec<_>>();
        assert_eq!(parts.len(), 3, "{document}");
        assert_eq!(parts[0], "Bash");
        assert!(parts[1].starts_with("cargo build"), "{document}");
        assert!(parts[1].chars().count() <= 300);
        assert!(parts[2].contains("ERR_ABC123 failed"), "{document}");
    }

    #[test]
    fn window_ignores_negated_terms() {
        let text = format!(
            "needle {} late_id_42 {}",
            "lead ".repeat(2000),
            "tail ".repeat(2000)
        );
        let document = windowed(&text, "needle -late_id_42", 300);
        assert!(document.starts_with("needle lead"), "{document}");
    }

    #[test]
    fn window_identifiers_outweigh_plain_words() {
        let context = "authentication middleware configuration";
        let query = format!("{context} ERR_ABC123");
        let text = format!(
            "ERR_ABC123 first {} {context} {}",
            "lead ".repeat(2000),
            "tail ".repeat(2000)
        );
        let document = windowed(&text, &query, 300);
        assert!(document.starts_with("ERR_ABC123 first"), "{document}");

        let text = format!(
            "{context} {} ERR_ABC123 late {}",
            "lead ".repeat(2000),
            "tail ".repeat(2000)
        );
        let document = windowed(&text, &query, 300);
        assert!(document.contains("ERR_ABC123 late"), "{document}");

        // Numbers and short versions are plain words.
        let text = format!(
            "deployment notes {} 2024 v10 {}",
            "lead ".repeat(2000),
            "tail ".repeat(2000)
        );
        let document = windowed(&text, "deployment 2024 v10", 300);
        assert!(document.starts_with("deployment notes"), "{document}");
        assert_eq!(window_term_weight("2024"), (4, false));
        assert_eq!(window_term_weight("v10"), (3, false));
        for word in ["well-known", "e-mail", "re-run", "and/or", "src/main.rs"] {
            assert!(!window_term_weight(word).1, "{word}");
        }
        for identifier in ["err_abc123", "v1.2", "ERR_X", "Foo::bar", "abbreviateField"] {
            assert!(window_term_weight(identifier).1, "{identifier}");
        }
    }

    #[test]
    fn window_terms_that_fill_their_hit_cap_are_not_identifiers() {
        for common in ["src/main.rs", "err_common"] {
            let text = format!(
                "{} {} E0432 late {}",
                format!("{common} ").repeat(150),
                "lead ".repeat(2000),
                "tail ".repeat(2000)
            );
            let document = windowed(&text, &format!("E0432 {common}"), 300);
            assert!(document.contains("E0432 late"), "{document}");
        }
    }

    #[test]
    fn window_centers_on_short_non_ascii_terms() {
        let text = format!(
            "开始 {} 数据库错误 发生 {}",
            "lead ".repeat(2000),
            "tail ".repeat(2000)
        );
        let document = windowed(&text, "错误", 300);
        assert!(document.contains("数据库错误 发生"), "{document}");
    }

    #[test]
    fn window_finds_a_whole_token_after_many_partial_hits() {
        let text = format!(
            "{} {} log entry {}",
            "catalog ".repeat(150),
            "lead ".repeat(2000),
            "tail ".repeat(2000)
        );
        let document = windowed(&text, "log", 300);
        assert!(document.contains(" log entry"), "{document}");
        let hits = window_hits(&text, text.chars().count(), &["log".to_string()]);
        assert_eq!(hits.len(), MAX_WINDOW_HITS_PER_TERM + 1);
    }

    #[test]
    fn window_hits_are_capped_per_term_and_kind() {
        let text = "alpha alphabet beta betamax ".repeat(10_000);
        let terms = ["alpha".to_string(), "beta".to_string()];
        let hits = window_hits(&text, text.chars().count(), &terms);
        assert_eq!(hits.len(), 2 * MAX_WINDOW_HITS_PER_TERM * terms.len());
    }

    #[test]
    fn window_smoke_test_on_megabytes_of_text() {
        let text = format!("{} ERR_ABC123 here", "the error was ".repeat(300_000));
        let document = windowed(&text, "the error ERR_ABC123", 1500);
        assert!(document.contains("ERR_ABC123 here"), "{document}");
    }

    #[test]
    fn window_maps_a_non_ascii_hit_that_ends_the_text() {
        let text = format!("{} ERR_ABC123", "ö".repeat(3000));
        let document = windowed(&text, "ERR_ABC123", 300);
        assert!(
            document.starts_with('…') && document.ends_with("ö ERR_ABC123"),
            "{document}"
        );
        assert_eq!(document.chars().count(), 299);
    }

    #[test]
    fn window_near_the_end_keeps_a_full_window_without_a_trailing_ellipsis() {
        let text = format!("{}ERR_ABC123", "lead ".repeat(2000));
        let document = windowed(&text, "ERR_ABC123", 300);
        assert!(
            document.starts_with('…') && document.ends_with("lead ERR_ABC123"),
            "{document}"
        );
        assert_eq!(document.chars().count(), 299);

        let text = format!("{} ERR_ABC123 tail", "lead ".repeat(2000));
        let document = windowed(&text, "ERR_ABC123", 300);
        assert!(document.ends_with("ERR_ABC123 tail"), "{document}");
        assert_eq!(document.chars().count(), 299);
    }

    #[test]
    fn busy_model_is_skipped_after_a_bounded_wait() {
        let slot = test_slot(|_| Ok(Constant(0.75)));
        slot.score(TURBO, LoadMode::Blocking).expect("load");
        let backend = match &*slot.slot.lock_state() {
            SlotState::Ready(_, backend) => Arc::clone(backend),
            _ => panic!("model is ready"),
        };
        let held = backend.lock().expect("hold the model");
        let before = *slot.now.lock().expect("clock");
        assert_eq!(slot.score(TURBO, LoadMode::Blocking).expect("busy"), None);
        let waited = *slot.now.lock().expect("clock") - before;
        assert!(
            waited >= BUSY_WAIT && waited < BUSY_WAIT + BUSY_POLL * 2,
            "{waited:?}"
        );
        assert_eq!(slot.score(TURBO, LoadMode::Blocking).expect("busy"), None);
        assert_eq!(slot.score(TURBO, LoadMode::Background).expect("busy"), None);
        assert_eq!(
            slot.events(),
            vec![
                SlotEvent::Busy { explicit: true },
                SlotEvent::Busy { explicit: true },
                SlotEvent::Busy { explicit: false },
            ]
        );
        drop(held);
        assert_eq!(
            slot.score(TURBO, LoadMode::Blocking).expect("free"),
            Some(vec![0.75, 0.75])
        );
        assert_eq!(slot.loads(), 1);
    }

    #[test]
    fn poisoned_model_is_dropped_and_reloaded() {
        let slot = test_slot(|_| Ok(Constant(0.5)));
        slot.score(TURBO, LoadMode::Blocking).expect("load");
        let backend = match &*slot.slot.lock_state() {
            SlotState::Ready(_, backend) => Arc::clone(backend),
            _ => panic!("model is ready"),
        };
        let poisoner = Arc::clone(&backend);
        let _ = std::thread::spawn(move || {
            let _model = poisoner.lock();
            panic!("inference panicked");
        })
        .join();
        assert!(backend.is_poisoned());
        let error = slot.score(TURBO, LoadMode::Blocking).expect_err("poisoned");
        assert!(error.to_string().contains("will be reloaded"), "{error}");
        assert_eq!(
            slot.score(TURBO, LoadMode::Blocking).expect("reloaded"),
            Some(vec![0.5, 0.5])
        );
        assert_eq!(slot.loads(), 2);

        // A poisoned state lock resets to idle instead of panicking.
        let state = Arc::clone(&slot.slot);
        let _ = std::thread::spawn(move || {
            let _state = state.state.lock();
            panic!("transition panicked");
        })
        .join();
        assert_eq!(
            slot.score(TURBO, LoadMode::Blocking).expect("recovered"),
            Some(vec![0.5, 0.5])
        );
        assert_eq!(slot.loads(), 3);
    }

    #[test]
    fn one_shot_notice_prints_once() {
        let printed = AtomicBool::new(false);
        let mut first = Vec::new();
        assert!(notify_once(&printed, ONE_SHOT_NOTICE, &mut first));
        assert_eq!(
            String::from_utf8(first).expect("utf8"),
            format!("{ONE_SHOT_NOTICE}\n")
        );
        let mut second = Vec::new();
        assert!(!notify_once(&printed, ONE_SHOT_NOTICE, &mut second));
        assert!(second.is_empty());
    }

    #[test]
    fn failures_warn_once_per_distinct_error_and_failure_window() {
        let warned = Mutex::new(HashMap::new());
        let start = Instant::now();
        let mut out = Vec::new();
        assert!(warn_failure_once(
            &warned,
            "model download failed",
            start,
            LOAD_RETRY_DELAY,
            &mut out
        ));
        assert!(!warn_failure_once(
            &warned,
            "model download failed",
            start,
            LOAD_RETRY_DELAY,
            &mut out
        ));
        assert!(warn_failure_once(
            &warned,
            "tokenizer failed",
            start,
            LOAD_RETRY_DELAY,
            &mut out
        ));
        let almost = start + LOAD_RETRY_DELAY - Duration::from_secs(1);
        assert!(!warn_failure_once(
            &warned,
            "model download failed",
            almost,
            LOAD_RETRY_DELAY,
            &mut out
        ));
        // A lasting failure warns again once per retry window.
        let next_window = start + LOAD_RETRY_DELAY;
        assert!(warn_failure_once(
            &warned,
            "model download failed",
            next_window,
            LOAD_RETRY_DELAY,
            &mut out
        ));
        assert!(!warn_failure_once(
            &warned,
            "model download failed",
            next_window,
            LOAD_RETRY_DELAY,
            &mut out
        ));
        assert_eq!(
            String::from_utf8(out).expect("utf8"),
            "warning: reranking failed, keeping the original order: model download failed\n\
             warning: reranking failed, keeping the original order: tokenizer failed\n\
             warning: reranking failed, keeping the original order: model download failed\n"
        );

        // A full table makes room by dropping entries whose window has passed.
        let warned = Mutex::new(HashMap::new());
        let mut out = Vec::new();
        for index in 0..MAX_FAILURE_WARNINGS {
            assert!(warn_failure_once(
                &warned,
                &format!("e{index}"),
                start,
                LOAD_RETRY_DELAY,
                &mut out
            ));
        }
        assert!(!warn_failure_once(
            &warned,
            "new",
            almost,
            LOAD_RETRY_DELAY,
            &mut out
        ));
        assert!(warn_failure_once(
            &warned,
            "new",
            next_window,
            LOAD_RETRY_DELAY,
            &mut out
        ));
    }

    #[test]
    fn sigmoid_is_stable_bounded_and_rejects_non_finite_logits() {
        assert_eq!(sigmoid(0.0).expect("zero"), 0.5);
        let low = sigmoid(-5.0).expect("negative");
        let high = sigmoid(5.0).expect("positive");
        assert!((low - 0.006_692_851).abs() < 1e-7, "{low}");
        assert!((low + high - 1.0).abs() < 1e-6, "{low} {high}");
        assert!(sigmoid(-3.0).expect("order") < sigmoid(-2.0).expect("order"));
        for (logit, limit) in [(-1e4, 0.0), (f32::MIN, 0.0), (1e4, 1.0), (f32::MAX, 1.0)] {
            assert_eq!(sigmoid(logit).expect("saturates"), limit, "{logit}");
        }
        for logit in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let error = sigmoid(logit).expect_err("non-finite").to_string();
            assert!(error.contains("non-finite"), "{error}");
        }
    }

    #[test]
    #[ignore = "downloads the jina-turbo reranker from Hugging Face"]
    fn local_model_scores_documents_in_input_order() {
        let mut reranker = LocalReranker::load(
            &RerankerModel::JINARerankerV1TurboEn,
            &EmbedRuntimeConfig::default(),
        )
        .expect("load reranker");
        let documents = vec![
            "The weather is sunny today.".to_string(),
            "Rust ownership rules prevent data races.".to_string(),
        ];
        let scores = reranker
            .rerank("how does rust prevent data races", &documents)
            .expect("rerank");
        assert_eq!(scores.len(), 2);
        assert!(
            scores.iter().all(|score| (0.0..=1.0).contains(score)),
            "{scores:?}"
        );
        assert!(scores[1] > scores[0], "{scores:?}");
    }
}
