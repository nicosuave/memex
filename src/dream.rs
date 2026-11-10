//! Dreaming: an offline pass in which a model reads recent conversations across agents and
//! machines, together with the saved Claude and Codex memories, and proposes durable
//! dreams. A dream is one stored statement with the records it rests on; a dream run is one
//! execution of the pass. Memex owns the result:
//!
//! - every dream cites the records it rests on, with an inline excerpt and a text hash, and
//!   a dream memex cannot tie to an offered record is not written;
//! - each run publishes a new generation next to the old ones and a diff against its
//!   parent; nothing is rewritten in place and no dream disappears by omission;
//! - native Claude and Codex memory files are read, never written.
//!
//! See `docs/dreaming.md`.

pub mod apply;
pub mod executor;
pub mod gather;
pub mod model;
pub mod render;
pub mod resolve;
pub mod schedule;
pub mod search;
pub mod store;

use crate::config::{Paths, UserConfig};
use crate::retrieval::CANONICAL_RECORD_ID_VERSION;
use crate::types::SourceFilter;
use anyhow::{Result, anyhow};
use apply::{ApplyContext, Change, apply_ops, parse_response};
use executor::ExecutorSpec;
use gather::{GatherOptions, gather, render_prompt};
use model::{
    DREAM_SCHEMA_VERSION, DreamStatus, EvidenceHealth, Generation, GenerationKind, RejectedOp,
    RunCounts, RunMeta,
};
use render::{render_diff, render_markdown};
use resolve::{LintReport, LiveEvidence, lint_dreams};
use serde::Serialize;
use store::{DreamState, DreamStore, GenerationFiles, new_generation_id};

#[derive(Clone, Debug, Default)]
pub struct DreamOptions {
    /// Run the executor and report what it would change, without publishing.
    pub dry_run: bool,
    /// Build the prompt and return it without running the executor.
    pub print_prompt: bool,
    pub executor: Option<String>,
    pub model: Option<String>,
    /// Read records newer than this instead of continuing from the last dream run.
    pub since_ms: Option<u64>,
    pub max_sessions: Option<usize>,
    pub project: Option<String>,
    pub source: Option<SourceFilter>,
    pub machines: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct DreamReport {
    /// The generation this run published, if it published one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub dry_run: bool,
    /// Why no executor ran, when none did.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,
    pub since_ms: u64,
    pub machines: Vec<String>,
    pub sessions: usize,
    pub sessions_skipped: usize,
    pub records: usize,
    pub memory_sections: usize,
    pub prompt_chars: usize,
    pub counts: RunCounts,
    pub rejected: Vec<RejectedOp>,
    pub failures: Vec<String>,
    pub lint: LintReport,
    pub active_dreams: usize,
    pub diff: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
}

pub fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

/// Dreaming is opt-in: a run sends transcript excerpts to a model, and dreams change what
/// search returns. Both wait for the user to say so in config.
pub fn ensure_enabled(config: &UserConfig) -> Result<()> {
    if config.dream.enabled() {
        return Ok(());
    }
    Err(anyhow!(
        "dreaming is off; to opt in, add this to config.toml:\n\n[dream]\nenabled = true"
    ))
}

pub fn run_dream(
    paths: &Paths,
    config: &UserConfig,
    options: &DreamOptions,
) -> Result<DreamReport> {
    // Printing the prompt sends nothing anywhere, so it is allowed before opting in: it is
    // how a user sees what a run would send.
    if !options.print_prompt {
        ensure_enabled(config)?;
    }
    let store = DreamStore::new(paths);
    let persist = !options.dry_run && !options.print_prompt;
    let lock = if persist { Some(store.lock()?) } else { None };
    let started_at_ms = now_ms();
    let previous = store.current()?;
    let mut dreams = previous
        .as_ref()
        .map(|generation| generation.dreams.clone())
        .unwrap_or_default();
    let redactor = config.egress_redactor()?;
    let redactor = redactor.as_deref();

    // Re-resolve existing citations first, so the executor is told which dreams have lost
    // their evidence and cannot treat them as standing support.
    let (lint, mut changes) = lint_dreams(
        &mut dreams,
        &LiveEvidence::new(paths, config),
        redactor,
        started_at_ms,
    );

    let since_ms = options
        .since_ms
        .or(previous.as_ref().map(|generation| generation.watermark_ms))
        .unwrap_or_else(|| {
            started_at_ms.saturating_sub(config.dream.lookback().as_millis() as u64)
        });
    let machines = if options.machines.is_empty() {
        config.dream.machines.clone()
    } else {
        options.machines.clone()
    };
    let input = gather(
        paths,
        config,
        &GatherOptions {
            machines,
            since_ms,
            max_sessions: options
                .max_sessions
                .unwrap_or_else(|| config.dream.max_sessions())
                .max(1),
            max_prompt_chars: config.dream.max_prompt_chars(),
            project: options.project.clone(),
            source: options.source,
            work_dir: store.work_dir(),
        },
        &dreams,
        redactor,
    )?;

    let mut report = DreamReport {
        parent: previous.as_ref().map(|generation| generation.id.clone()),
        dry_run: options.dry_run,
        since_ms,
        machines: input.machines.clone(),
        sessions: input.sessions.len(),
        sessions_skipped: input.sessions_skipped,
        records: input.record_count(),
        memory_sections: input.memories.len(),
        failures: input.failures.clone(),
        lint: lint.clone(),
        ..DreamReport::default()
    };
    let run_meta = |finished_at_ms: u64| RunMeta {
        started_at_ms,
        finished_at_ms,
        memex_version: env!("CARGO_PKG_VERSION").to_string(),
        rid_version: CANONICAL_RECORD_ID_VERSION.to_string(),
        since_ms: Some(since_ms),
        machines: input.machines.clone(),
        sessions: input.sessions.len(),
        sessions_skipped: input.sessions_skipped,
        records: input.record_count(),
        memory_sections: input.memories.len(),
        failures: input.failures.clone(),
        ..RunMeta::default()
    };

    if input.sessions.is_empty() {
        report.skipped = Some(format!(
            "no new sessions since {}",
            gather::format_day(since_ms)
        ));
        report.active_dreams = active_count(&dreams);
        // Nothing to dream over, but a citation that moved or was lost is still worth
        // recording; the watermark stays where it was.
        if let (Some(lock), Some(previous), true) = (&lock, &previous, lint.changed > 0) {
            let mut run = run_meta(now_ms());
            run.counts.citations_changed = lint.changed;
            let generation = Generation {
                schema_version: DREAM_SCHEMA_VERSION,
                id: new_generation_id(started_at_ms),
                parent: Some(previous.id.clone()),
                kind: GenerationKind::Lint,
                created_at_ms: started_at_ms,
                watermark_ms: previous.watermark_ms,
                run,
                dreams,
            };
            report.diff = publish(&store, lock, &generation, &changes, None)?;
            report.generation = Some(generation.id);
        }
        return Ok(report);
    }

    let prompt = render_prompt(&input, &dreams, started_at_ms);
    report.prompt_chars = prompt.chars().count();
    if options.print_prompt {
        report.prompt = Some(prompt);
        report.active_dreams = active_count(&dreams);
        return Ok(report);
    }

    let executor = ExecutorSpec::resolve(
        &config.dream,
        options.executor.as_deref(),
        options.model.as_deref(),
    )?;
    report.executor = Some(executor.label());
    if persist {
        store.write_state(&DreamState {
            last_attempt_ms: started_at_ms,
            last_error: None,
        })?;
    }
    let answer = executor
        .run(&prompt, &store.work_dir(), config.dream.timeout())
        .and_then(|answer| Ok((parse_response(&answer)?, answer)));
    let (ops, answer) = match answer {
        Ok(parsed) => parsed,
        Err(error) => {
            if persist {
                // The scheduler reads this to back off instead of retrying every minute.
                let _ = store.write_state(&DreamState {
                    last_attempt_ms: started_at_ms,
                    last_error: Some(format!("{error:#}")),
                });
            }
            return Err(error);
        }
    };

    let generation_id = new_generation_id(started_at_ms);
    let outcome = apply_ops(
        &dreams,
        ops,
        &ApplyContext {
            input: &input,
            redactor,
            generation_id: &generation_id,
            now_ms: started_at_ms,
        },
    );
    changes.extend(outcome.changes);
    let mut run = run_meta(now_ms());
    run.executor = Some(executor.label());
    run.model = executor.model().map(str::to_string);
    run.counts = RunCounts {
        citations_changed: lint.changed,
        ..outcome.counts.clone()
    };
    run.rejected = outcome.rejected.clone();
    let generation = Generation {
        schema_version: DREAM_SCHEMA_VERSION,
        id: generation_id,
        parent: previous.as_ref().map(|generation| generation.id.clone()),
        kind: GenerationKind::Run,
        created_at_ms: started_at_ms,
        // Later activity in a session read here is picked up by the next dream run.
        watermark_ms: started_at_ms,
        run,
        dreams: outcome.dreams,
    };
    report.counts = generation.run.counts.clone();
    report.rejected = outcome.rejected;
    report.active_dreams = active_count(&generation.dreams);
    match &lock {
        Some(lock) => {
            report.diff = publish(&store, lock, &generation, &changes, Some(&answer))?;
            report.generation = Some(generation.id);
        }
        None => report.diff = render_diff(&generation, &changes),
    }
    Ok(report)
}

fn active_count(dreams: &[model::Dream]) -> usize {
    dreams
        .iter()
        .filter(|dream| dream.status == DreamStatus::Active)
        .count()
}

fn publish(
    store: &DreamStore,
    lock: &store::DreamLock,
    generation: &Generation,
    changes: &[Change],
    response: Option<&str>,
) -> Result<String> {
    let diff = render_diff(generation, changes);
    store.publish(
        lock,
        generation,
        &GenerationFiles {
            markdown: &render_markdown(generation),
            diff: &diff,
            response,
        },
    )?;
    Ok(diff)
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LintOutcome {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<String>,
    pub dry_run: bool,
    #[serde(flatten)]
    pub report: LintReport,
    pub diff: String,
}

/// Re-resolve every citation of the current generation. A citation that moved is rewritten
/// to its new identity and one whose source is gone is marked lost, in a new generation.
pub fn run_lint(paths: &Paths, config: &UserConfig, dry_run: bool) -> Result<LintOutcome> {
    let store = DreamStore::new(paths);
    let lock = if dry_run { None } else { Some(store.lock()?) };
    let Some(previous) = store.current()? else {
        return Ok(LintOutcome {
            dry_run,
            ..LintOutcome::default()
        });
    };
    let started_at_ms = now_ms();
    let mut dreams = previous.dreams.clone();
    let redactor = config.egress_redactor()?;
    let (report, changes) = lint_dreams(
        &mut dreams,
        &LiveEvidence::new(paths, config),
        redactor.as_deref(),
        started_at_ms,
    );
    let generation = Generation {
        schema_version: DREAM_SCHEMA_VERSION,
        id: new_generation_id(started_at_ms),
        parent: Some(previous.id.clone()),
        kind: GenerationKind::Lint,
        created_at_ms: started_at_ms,
        watermark_ms: previous.watermark_ms,
        run: RunMeta {
            started_at_ms,
            finished_at_ms: now_ms(),
            memex_version: env!("CARGO_PKG_VERSION").to_string(),
            rid_version: CANONICAL_RECORD_ID_VERSION.to_string(),
            counts: RunCounts {
                citations_changed: report.changed,
                ..RunCounts::default()
            },
            ..RunMeta::default()
        },
        dreams,
    };
    let mut outcome = LintOutcome {
        generation: None,
        dry_run,
        diff: render_diff(&generation, &changes),
        report,
    };
    if let (Some(lock), true) = (&lock, outcome.report.changed > 0) {
        publish(&store, lock, &generation, &changes, None)?;
        outcome.generation = Some(generation.id);
    }
    Ok(outcome)
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct DreamTotals {
    pub active: usize,
    pub superseded: usize,
    pub retracted: usize,
    /// Active dreams by how much of their evidence still resolves.
    pub supported: usize,
    pub partial: usize,
    pub unverified: usize,
    pub lost: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct GenerationSummary {
    pub id: String,
    pub kind: GenerationKind,
    pub created_at_ms: u64,
    pub counts: RunCounts,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StoreStatus {
    pub store: String,
    /// `[dream] enabled`: off, nothing below runs or is returned by search.
    pub enabled: bool,
    pub auto: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_dream_ms: Option<u64>,
    pub dreams: DreamTotals,
    pub generations: usize,
    /// Most recent generations, newest first.
    pub recent: Vec<GenerationSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempt_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub running: bool,
    /// Whether the daemon would start a dream run now, and why or why not.
    pub gate: schedule::GateDecision,
}

const STATUS_RECENT_GENERATIONS: usize = 5;

pub fn dream_status(paths: &Paths, config: &UserConfig) -> Result<StoreStatus> {
    let store = DreamStore::new(paths);
    let current = store.current()?;
    let mut totals = DreamTotals::default();
    for dream in current.iter().flat_map(|generation| &generation.dreams) {
        match dream.status {
            DreamStatus::Superseded => totals.superseded += 1,
            DreamStatus::Retracted => totals.retracted += 1,
            DreamStatus::Active => {
                totals.active += 1;
                match dream.evidence_health() {
                    EvidenceHealth::Supported => totals.supported += 1,
                    EvidenceHealth::Partial => totals.partial += 1,
                    EvidenceHealth::Unverified => totals.unverified += 1,
                    EvidenceHealth::Lost => totals.lost += 1,
                }
            }
        }
    }
    let ids = store.generation_ids()?;
    let recent = ids
        .iter()
        .rev()
        .take(STATUS_RECENT_GENERATIONS)
        .filter_map(|id| store.load(id).ok())
        .map(|generation| GenerationSummary {
            id: generation.id,
            kind: generation.kind,
            created_at_ms: generation.created_at_ms,
            counts: generation.run.counts,
            executor: generation.run.executor,
        })
        .collect();
    let state = store.state();
    Ok(StoreStatus {
        store: store.dir().to_string_lossy().to_string(),
        enabled: config.dream.enabled(),
        auto: config.dream.auto(),
        last_dream_ms: current.as_ref().map(|generation| generation.watermark_ms),
        current: current.map(|generation| generation.id),
        dreams: totals,
        generations: ids.len(),
        recent,
        last_attempt_ms: (state.last_attempt_ms > 0).then_some(state.last_attempt_ms),
        last_error: state.last_error,
        running: store.is_locked(),
        gate: schedule::evaluate_gate(paths, config, now_ms()),
    })
}
