//! Decides when the daemon should dream. The gate is activity plus idleness rather than a
//! wall-clock time: enough new sessions since the last dream run, and nobody working right now.
//! The run itself happens in a child `memex dream run`, so a slow executor never stalls
//! indexing.

use super::store::DreamStore;
use crate::analytics::{AnalyticsStore, SessionKindFilter, analytics_path};
use crate::config::{Paths, UserConfig};
use serde::Serialize;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// How often the daemon re-evaluates the gate.
const CHECK_INTERVAL: Duration = Duration::from_secs(60);
/// Wait after a run that published nothing before trying again.
const RETRY_BACKOFF_MS: u64 = 60 * 60 * 1000;

#[derive(Clone, Debug, Default, Serialize)]
pub struct GateDecision {
    pub due: bool,
    pub reason: String,
    /// Primary sessions active since the last dream run.
    pub new_sessions: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_activity_ms: Option<u64>,
}

fn closed(
    reason: impl Into<String>,
    new_sessions: usize,
    last_activity_ms: Option<u64>,
) -> GateDecision {
    GateDecision {
        due: false,
        reason: reason.into(),
        new_sessions,
        last_activity_ms,
    }
}

/// Whether a daemon-started run should begin at `now_ms`. Ignores `[dream] auto`, so
/// `memex dream status` can show what the gate would do if it were enabled.
pub fn evaluate_gate(paths: &Paths, config: &UserConfig, now_ms: u64) -> GateDecision {
    let dream = &config.dream;
    if !dream.enabled() {
        return closed("dreaming is off ([dream] enabled is not set)", 0, None);
    }
    let store = DreamStore::new(paths);
    if store.is_locked() {
        return closed("a dream run is already in progress", 0, None);
    }
    let current = match store.current() {
        Ok(current) => current,
        Err(error) => return closed(format!("dream store unreadable: {error:#}"), 0, None),
    };
    let last_dream_ms = current.as_ref().map(|generation| generation.watermark_ms);
    if let Some(last) = last_dream_ms {
        let wait = dream.min_interval().as_millis() as u64;
        if now_ms.saturating_sub(last) < wait {
            return closed(
                format!(
                    "last dream run was {} ago; minimum interval is {}h",
                    format_age(now_ms.saturating_sub(last)),
                    wait / 3_600_000
                ),
                0,
                None,
            );
        }
    }
    let state = store.state();
    if state.last_attempt_ms > last_dream_ms.unwrap_or(0)
        && now_ms.saturating_sub(state.last_attempt_ms) < RETRY_BACKOFF_MS
    {
        return closed(
            format!(
                "last attempt {} ago did not publish{}; retrying after 1h",
                format_age(now_ms.saturating_sub(state.last_attempt_ms)),
                state
                    .last_error
                    .as_deref()
                    .map(|error| format!(" ({error})"))
                    .unwrap_or_default()
            ),
            0,
            None,
        );
    }

    let analytics = analytics_path(&paths.state);
    if !analytics.exists() {
        return closed("nothing indexed yet", 0, None);
    }
    let since_ms =
        last_dream_ms.unwrap_or_else(|| now_ms.saturating_sub(dream.lookback().as_millis() as u64));
    let needed = dream.min_new_sessions();
    let sessions = AnalyticsStore::open_read_only(&analytics).and_then(|store| {
        let latest = store.query_sessions_detailed(None, None, None, None, Some(1))?;
        let new = store.query_sessions_detailed_filtered(
            None,
            None,
            None,
            Some(since_ms.saturating_add(1)),
            Some(SessionKindFilter::Primary),
            // The gate only needs to know the threshold was reached, plus a margin for
            // the executor's own sessions that are filtered out below.
            Some(needed.saturating_mul(4).max(50)),
        )?;
        Ok((latest, new))
    });
    let (latest, new) = match sessions {
        Ok(sessions) => sessions,
        Err(error) => return closed(format!("session catalog unreadable: {error:#}"), 0, None),
    };
    let work_dir = store.work_dir().to_string_lossy().to_string();
    let new_sessions = new
        .iter()
        .filter(|row| {
            !row.cwd
                .as_deref()
                .is_some_and(|cwd| cwd.starts_with(&work_dir))
        })
        .count();
    let last_activity_ms = latest.first().map(|row| row.last_at);
    if new_sessions < needed {
        return closed(
            format!("{new_sessions} new sessions since the last dream run; {needed} needed"),
            new_sessions,
            last_activity_ms,
        );
    }
    let idle_ms = dream.idle().as_millis() as u64;
    if let Some(last_activity) = last_activity_ms
        && now_ms.saturating_sub(last_activity) < idle_ms
    {
        return closed(
            format!(
                "sessions were active {} ago; waiting for {}m of quiet",
                format_age(now_ms.saturating_sub(last_activity)),
                idle_ms / 60_000
            ),
            new_sessions,
            last_activity_ms,
        );
    }
    GateDecision {
        due: true,
        reason: format!("{new_sessions} new sessions and no recent activity"),
        new_sessions,
        last_activity_ms,
    }
}

fn format_age(ms: u64) -> String {
    let minutes = ms / 60_000;
    if minutes < 120 {
        format!("{minutes}m")
    } else if minutes < 48 * 60 {
        format!("{}h", minutes / 60)
    } else {
        format!("{}d", minutes / (24 * 60))
    }
}

/// Owned by the daemon loop: checks the gate once a minute and supervises the child run.
pub struct DreamScheduler {
    root: Option<PathBuf>,
    child: Option<Child>,
    next_check: Instant,
}

impl DreamScheduler {
    pub fn new(root: Option<PathBuf>) -> Self {
        Self {
            root,
            child: None,
            next_check: Instant::now() + CHECK_INTERVAL,
        }
    }

    /// One cheap step of the daemon loop. Dreaming is optional work: its failures are
    /// logged and never fail indexing.
    pub fn service(&mut self, paths: &Paths) {
        if let Some(child) = self.child.as_mut() {
            match child.try_wait() {
                Ok(None) => return,
                Ok(Some(status)) if status.success() => eprintln!("dream: scheduled run finished"),
                Ok(Some(status)) => eprintln!("dream: scheduled run failed ({status})"),
                Err(error) => eprintln!("dream: cannot poll scheduled run: {error}"),
            }
            self.child = None;
        }
        if Instant::now() < self.next_check {
            return;
        }
        self.next_check = Instant::now() + CHECK_INTERVAL;
        let config = match UserConfig::load(paths) {
            Ok(config) => config,
            Err(_) => return,
        };
        if !config.dream.enabled() || !config.dream.auto() {
            return;
        }
        let decision = evaluate_gate(paths, &config, super::now_ms());
        if !decision.due {
            return;
        }
        let mut command = match std::env::current_exe() {
            Ok(executable) => Command::new(executable),
            Err(error) => {
                eprintln!("dream: cannot locate memex executable: {error}");
                return;
            }
        };
        // The run logs to the daemon log, which nobody reads for release notices.
        command.args(["--no-update-check", "dream", "run", "--format", "text"]);
        if let Some(root) = &self.root {
            command.arg("--root").arg(root);
        }
        match command.stdin(Stdio::null()).spawn() {
            Ok(child) => {
                eprintln!("dream: starting scheduled run ({})", decision.reason);
                self.child = Some(child);
            }
            Err(error) => eprintln!("dream: cannot start scheduled run: {error}"),
        }
    }

    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for DreamScheduler {
    fn drop(&mut self) {
        self.stop();
    }
}
