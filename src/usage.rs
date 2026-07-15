//! Reconstructed local token usage.
//!
//! This module intentionally does not model provider quota percentages. Local logs are useful for
//! request-level accounting, but they are not authoritative subscription-limit telemetry.

use crate::types::SourceFilter;
use anyhow::{Context, Result};
use chrono::DateTime;
use clap::ValueEnum;
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Clone, Debug, Default)]
pub struct UsageQuery {
    pub source: Option<SourceFilter>,
    pub since_ms: Option<u64>,
    pub until_ms: Option<u64>,
    pub cost_mode: CostMode,
    pub include_events: bool,
}

#[derive(Clone, Copy, Debug, Default, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
#[value(rename_all = "kebab-case")]
pub enum CostMode {
    Source,
    #[default]
    Auto,
    Reprice,
}

#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq, Hash)]
pub struct TokenBuckets {
    /// Provider-reported input. For OpenAI-shaped records this includes the cached subset.
    pub raw_input: u64,
    pub uncached_input: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    /// One-hour cache writes, a subset of `cache_write`.
    pub cache_write_1h: u64,
    /// Billable output, including reasoning when a provider reports it separately.
    pub output: u64,
    /// Reasoning output, retained as a subset of `output` for reporting.
    pub reasoning: u64,
}

impl TokenBuckets {
    fn additive_total(&self) -> u64 {
        self.uncached_input
            .saturating_add(self.cache_read)
            .saturating_add(self.cache_write)
            .saturating_add(self.output)
    }

    pub fn total(&self) -> u64 {
        self.additive_total()
    }

    fn codex(input: u64, cached: u64, output: u64, reasoning: u64) -> Self {
        let cache_read = cached.min(input);
        Self {
            raw_input: input,
            uncached_input: input.saturating_sub(cache_read),
            cache_read,
            cache_write: 0,
            cache_write_1h: 0,
            output,
            reasoning,
        }
    }

    fn disjoint(input: u64, cache_read: u64, cache_write: u64, output: u64) -> Self {
        Self {
            raw_input: input,
            uncached_input: input,
            cache_read,
            cache_write,
            cache_write_1h: 0,
            output,
            reasoning: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct UsageEvent {
    pub source: String,
    pub source_path: String,
    pub source_record_id: Option<String>,
    pub session_id: Option<String>,
    pub request_id: Option<String>,
    pub message_id: Option<String>,
    pub timestamp_ms: u64,
    pub project: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub tokens: TokenBuckets,
    pub source_cost_usd: Option<f64>,
    pub dedupe_confidence: &'static str,
    pub conservative_undercount: bool,
    #[serde(skip)]
    sidechain: bool,
    #[serde(skip)]
    source_order: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct UsageSummary {
    pub source: String,
    pub events: u64,
    pub uncached_input: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub output: u64,
    pub reasoning: u64,
    pub total_tokens: u64,
    pub known_cost_usd: f64,
    pub priced_events: u64,
    pub unpriced_events: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct UsageReport {
    pub authority: &'static str,
    pub events: u64,
    pub total_tokens: u64,
    pub unknown_model_events: u64,
    pub conservative_events: u64,
    pub cost_mode: CostMode,
    pub price_catalog: &'static str,
    pub known_cost_usd: f64,
    pub priced_events: u64,
    pub unpriced_events: u64,
    pub by_source: Vec<UsageSummary>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<UsageEvent>,
    pub warnings: Vec<String>,
}

pub fn scan_usage(query: &UsageQuery) -> Result<UsageReport> {
    let mut events = Vec::new();
    let mut warnings = Vec::new();
    let mut scan =
        |filter: SourceFilter, f: fn(&mut Vec<UsageEvent>, &mut Vec<String>) -> Result<()>| {
            if query.source.is_none_or(|source| source == filter)
                && let Err(error) = f(&mut events, &mut warnings)
            {
                warnings.push(format!("{} scanner: {error:#}", filter.as_str()));
            }
        };
    scan(SourceFilter::Claude, scan_claude);
    scan(SourceFilter::Codex, scan_codex);
    scan(SourceFilter::Opencode, scan_opencode);
    scan(SourceFilter::Pi, scan_pi);
    scan(SourceFilter::Cursor, scan_cursor);
    scan(SourceFilter::Copilot, scan_copilot);

    reconcile_claude(&mut events);
    reconcile_codex_copies(&mut events);
    events.retain(|event| {
        query
            .since_ms
            .is_none_or(|since| event.timestamp_ms >= since)
            && query
                .until_ms
                .is_none_or(|until| event.timestamp_ms < until)
    });
    events.sort_by(|a, b| {
        (a.timestamp_ms, &a.source_path, a.source_order).cmp(&(
            b.timestamp_ms,
            &b.source_path,
            b.source_order,
        ))
    });

    let mut by_source: HashMap<String, UsageSummary> = HashMap::new();
    let mut report = UsageReport {
        authority: "local_log",
        cost_mode: query.cost_mode,
        price_catalog: PRICE_CATALOG_ID,
        warnings,
        ..UsageReport::default()
    };
    for event in &events {
        let total = event.tokens.additive_total();
        report.events += 1;
        report.total_tokens = report.total_tokens.saturating_add(total);
        report.unknown_model_events += u64::from(event.model.is_none());
        report.conservative_events += u64::from(event.conservative_undercount);
        let cost = event_cost_nanos(event, query.cost_mode);
        if let Some(cost) = cost {
            report.priced_events += 1;
            report.known_cost_usd += cost as f64 / 1_000_000_000.0;
        } else {
            report.unpriced_events += 1;
        }
        let row = by_source
            .entry(event.source.clone())
            .or_insert_with(|| UsageSummary {
                source: event.source.clone(),
                ..UsageSummary::default()
            });
        row.events += 1;
        row.uncached_input = row
            .uncached_input
            .saturating_add(event.tokens.uncached_input);
        row.cache_read = row.cache_read.saturating_add(event.tokens.cache_read);
        row.cache_write = row.cache_write.saturating_add(event.tokens.cache_write);
        row.output = row.output.saturating_add(event.tokens.output);
        row.reasoning = row.reasoning.saturating_add(event.tokens.reasoning);
        row.total_tokens = row.total_tokens.saturating_add(total);
        if let Some(cost) = cost {
            row.priced_events += 1;
            row.known_cost_usd += cost as f64 / 1_000_000_000.0;
        } else {
            row.unpriced_events += 1;
        }
    }
    report.by_source = by_source.into_values().collect();
    report.by_source.sort_by(|a, b| a.source.cmp(&b.source));
    if query.include_events {
        report.details = events;
    }
    Ok(report)
}

fn home() -> PathBuf {
    directories::BaseDirs::new()
        .map(|dirs| dirs.home_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("/"))
}

fn jsonl_files(roots: impl IntoIterator<Item = PathBuf>) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for root in roots {
        if !root.exists() {
            continue;
        }
        for entry in WalkDir::new(root).follow_links(false).into_iter().flatten() {
            let path = entry.path();
            if entry.file_type().is_file()
                && path.extension().and_then(|v| v.to_str()) == Some("jsonl")
            {
                files.push(path.to_path_buf());
            }
        }
    }
    files.sort();
    files.dedup();
    files
}

fn lines(path: &Path) -> Result<impl Iterator<Item = (u64, Value)>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    Ok(BufReader::new(file)
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let line = line.ok()?;
            serde_json::from_str(&line)
                .ok()
                .map(|value| (index as u64, value))
        }))
}

fn timestamp_ms(value: &Value) -> u64 {
    value
        .as_u64()
        .map(|n| if n < 10_000_000_000 { n * 1000 } else { n })
        .or_else(|| value.as_i64().filter(|n| *n >= 0).map(|n| n as u64))
        .or_else(|| {
            value
                .as_str()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|d| d.timestamp_millis().max(0) as u64)
        })
        .unwrap_or(0)
}

fn u64_at(value: &Value, aliases: &[&str]) -> u64 {
    aliases
        .iter()
        .find_map(|key| value.get(*key).and_then(Value::as_u64))
        .unwrap_or(0)
}

fn str_at(value: &Value, aliases: &[&str]) -> Option<String> {
    aliases
        .iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn scan_claude(out: &mut Vec<UsageEvent>, _warnings: &mut Vec<String>) -> Result<()> {
    let roots = if let Some(config) = std::env::var_os("CLAUDE_CONFIG_DIR") {
        config
            .to_string_lossy()
            .split(',')
            .map(|part| {
                let path = PathBuf::from(part.trim());
                if path.file_name().and_then(|n| n.to_str()) == Some("projects") {
                    path
                } else {
                    path.join("projects")
                }
            })
            .collect()
    } else {
        vec![
            home().join(".claude/projects"),
            home().join(".config/claude/projects"),
        ]
    };
    for path in jsonl_files(roots) {
        let source_path = path.to_string_lossy().to_string();
        let fallback_session = path
            .file_stem()
            .and_then(|n| n.to_str())
            .map(str::to_string);
        for (index, value) in lines(&path)? {
            if value.get("type").and_then(Value::as_str) != Some("assistant") {
                continue;
            }
            let Some(message) = value.get("message") else {
                continue;
            };
            let Some(usage) = message.get("usage") else {
                continue;
            };
            let cache_write = u64_at(
                usage,
                &["cache_creation_input_tokens", "cacheCreationInputTokens"],
            );
            let mut tokens = TokenBuckets::disjoint(
                u64_at(usage, &["input_tokens", "inputTokens"]),
                u64_at(usage, &["cache_read_input_tokens", "cacheReadInputTokens"]),
                cache_write,
                u64_at(usage, &["output_tokens", "outputTokens"]),
            );
            tokens.cache_write_1h = usage
                .pointer("/cache_creation/ephemeral_1h_input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .min(tokens.cache_write);
            if tokens.additive_total() == 0 {
                continue;
            }
            out.push(UsageEvent {
                source: "claude".into(),
                source_path: source_path.clone(),
                source_record_id: Some(format!("line:{index}")),
                session_id: str_at(&value, &["sessionId", "session_id"])
                    .or_else(|| fallback_session.clone()),
                request_id: str_at(&value, &["requestId", "request_id"]),
                message_id: str_at(message, &["id"]),
                timestamp_ms: value.get("timestamp").map(timestamp_ms).unwrap_or(0),
                project: str_at(&value, &["cwd"]),
                provider: Some("anthropic".into()),
                model: str_at(message, &["model"]),
                tokens,
                source_cost_usd: value.get("costUSD").and_then(Value::as_f64),
                dedupe_confidence: if message.get("id").is_some()
                    && value.get("requestId").is_some()
                {
                    "exact"
                } else {
                    "heuristic"
                },
                conservative_undercount: false,
                sidechain: value
                    .get("isSidechain")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                source_order: index,
            });
        }
    }
    Ok(())
}

fn reconcile_claude(events: &mut Vec<UsageEvent>) {
    let mut best_exact: HashMap<(String, String), usize> = HashMap::new();
    let mut keep = vec![true; events.len()];
    for index in 0..events.len() {
        if events[index].source != "claude" {
            continue;
        }
        let (Some(message), Some(request)) = (
            events[index].message_id.clone(),
            events[index].request_id.clone(),
        ) else {
            continue;
        };
        let key = (message, request);
        if let Some(previous) = best_exact.get(&key).copied() {
            let winner = choose_claude(&events[previous], &events[index]);
            if winner {
                keep[index] = false;
            } else {
                keep[previous] = false;
                best_exact.insert(key, index);
            }
        } else {
            best_exact.insert(key, index);
        }
    }
    let mut by_message: HashMap<String, Vec<usize>> = HashMap::new();
    for (index, event) in events.iter().enumerate() {
        if keep[index]
            && event.source == "claude"
            && let Some(message) = &event.message_id
        {
            by_message.entry(message.clone()).or_default().push(index);
        }
    }
    for indices in by_message.into_values() {
        if indices.len() < 2 || !indices.iter().any(|index| !events[*index].sidechain) {
            continue;
        }
        // Message-only matching is a sidechain replay fallback. Keep every distinct parent
        // request; suppress only sidechain copies of a message present in a parent transcript.
        for index in indices {
            if events[index].sidechain {
                keep[index] = false;
            }
        }
    }
    let mut index = 0usize;
    events.retain(|_| {
        let retain = keep[index];
        index += 1;
        retain
    });
}

/// True means `a` wins. Prefer parent/non-sidechain, then completeness, then source order.
fn choose_claude(a: &UsageEvent, b: &UsageEvent) -> bool {
    if a.sidechain != b.sidechain {
        return !a.sidechain;
    }
    let a_parent = !a.source_path.contains("/subagents/");
    let b_parent = !b.source_path.contains("/subagents/");
    if a_parent != b_parent {
        return a_parent;
    }
    let a_total = a.tokens.additive_total();
    let b_total = b.tokens.additive_total();
    if a_total != b_total {
        return a_total > b_total;
    }
    a.source_order >= b.source_order
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
struct CodexTokens {
    input: u64,
    cached: u64,
    output: u64,
    reasoning: u64,
}

impl CodexTokens {
    fn from(value: &Value) -> Self {
        Self {
            input: u64_at(value, &["input_tokens", "inputTokens", "prompt_tokens"]),
            cached: u64_at(
                value,
                &[
                    "cached_input_tokens",
                    "cachedInputTokens",
                    "cache_read_input_tokens",
                ],
            ),
            output: u64_at(
                value,
                &["output_tokens", "outputTokens", "completion_tokens"],
            ),
            reasoning: u64_at(
                value,
                &[
                    "reasoning_output_tokens",
                    "reasoningTokens",
                    "reasoning_tokens",
                ],
            ),
        }
    }
    fn zero(self) -> bool {
        self.input == 0 && self.cached == 0 && self.output == 0 && self.reasoning == 0
    }
    fn add(self, rhs: Self) -> Self {
        Self {
            input: self.input.saturating_add(rhs.input),
            cached: self.cached.saturating_add(rhs.cached),
            output: self.output.saturating_add(rhs.output),
            reasoning: self.reasoning.saturating_add(rhs.reasoning),
        }
    }
    fn sub(self, rhs: Self) -> Self {
        Self {
            input: self.input.saturating_sub(rhs.input),
            cached: self.cached.saturating_sub(rhs.cached),
            output: self.output.saturating_sub(rhs.output),
            reasoning: self.reasoning.saturating_sub(rhs.reasoning),
        }
    }
    fn min(self, rhs: Self) -> Self {
        Self {
            input: self.input.min(rhs.input),
            cached: self.cached.min(rhs.cached),
            output: self.output.min(rhs.output),
            reasoning: self.reasoning.min(rhs.reasoning),
        }
    }
    fn max(self, rhs: Self) -> Self {
        Self {
            input: self.input.max(rhs.input),
            cached: self.cached.max(rhs.cached),
            output: self.output.max(rhs.output),
            reasoning: self.reasoning.max(rhs.reasoning),
        }
    }
    fn at_least(self, rhs: Self) -> bool {
        self.input >= rhs.input
            && self.cached >= rhs.cached
            && self.output >= rhs.output
            && self.reasoning >= rhs.reasoning
    }
    fn at_most(self, rhs: Self) -> bool {
        self.input <= rhs.input
            && self.cached <= rhs.cached
            && self.output <= rhs.output
            && self.reasoning <= rhs.reasoning
    }
}

#[derive(Default)]
struct CodexCounter {
    counted: CodexTokens,
    raw_baseline: CodexTokens,
    watermark: CodexTokens,
    seen: Vec<CodexTokens>,
    divergent: bool,
    interleaved: bool,
}

impl CodexCounter {
    fn establish_unresolved_fork_baseline(&mut self, total: CodexTokens) {
        self.raw_baseline = total;
        self.watermark = self.watermark.max(total);
        if self.seen.last() != Some(&total) {
            self.seen.push(total);
            if self.seen.len() > 64 {
                self.seen.remove(0);
            }
        }
    }

    fn account(&mut self, last: Option<CodexTokens>, total: Option<CodexTokens>) -> CodexTokens {
        if let Some(total) = total {
            if self.seen.contains(&total) {
                return CodexTokens::default();
            }
            if !total.at_least(self.watermark) {
                self.interleaved = true;
            }
        }
        let baseline = self.watermark.max(self.raw_baseline);
        let delta = match (last, total) {
            (Some(last), Some(total)) if self.interleaved => {
                last.min(contained(total, baseline, self.counted))
            }
            (None, Some(total)) if self.interleaved => contained(total, baseline, self.counted),
            (Some(last), Some(total)) => {
                let total_delta = total.sub(baseline);
                if !self.divergent && total.at_least(baseline) && total_delta.at_most(last) {
                    total_delta
                } else {
                    last
                }
            }
            (None, Some(total)) if self.divergent => contained(total, baseline, self.counted),
            (None, Some(total)) => total.sub(baseline),
            (Some(last), None) => last,
            (None, None) => return CodexTokens::default(),
        };
        self.counted = self.counted.add(delta);
        if let Some(total) = total {
            self.raw_baseline = total;
            self.divergent |= total != self.counted;
            self.watermark = self.watermark.max(total);
            if self.seen.last() != Some(&total) {
                self.seen.push(total);
                if self.seen.len() > 64 {
                    self.seen.remove(0);
                }
            }
        } else {
            self.raw_baseline = self.counted;
            self.watermark = self.watermark.max(self.counted);
        }
        delta
    }
}

fn contained(current: CodexTokens, watermark: CodexTokens, counted: CodexTokens) -> CodexTokens {
    fn one(current: u64, watermark: u64, counted: u64) -> u64 {
        if current >= watermark {
            current.saturating_sub(watermark.max(counted))
        } else {
            current.saturating_sub(counted)
        }
    }
    CodexTokens {
        input: one(current.input, watermark.input, counted.input),
        cached: one(current.cached, watermark.cached, counted.cached),
        output: one(current.output, watermark.output, counted.output),
        reasoning: one(current.reasoning, watermark.reasoning, counted.reasoning),
    }
}

fn scan_codex(out: &mut Vec<UsageEvent>, _warnings: &mut Vec<String>) -> Result<()> {
    let homes: Vec<PathBuf> = std::env::var_os("CODEX_HOME")
        .map(|v| {
            v.to_string_lossy()
                .split(',')
                .map(|s| PathBuf::from(s.trim()))
                .collect()
        })
        .unwrap_or_else(|| vec![home().join(".codex")]);
    let mut files = Vec::new();
    for root in homes {
        let active = root.join("sessions");
        let archived = root.join("archived_sessions");
        if active.exists() || archived.exists() {
            files.extend(jsonl_files([active, archived]));
        } else {
            files.extend(jsonl_files([root]));
        }
    }
    for path in files {
        scan_codex_file(&path, out)?;
    }
    Ok(())
}

fn scan_codex_file(path: &Path, out: &mut Vec<UsageEvent>) -> Result<()> {
    let source_path = path.to_string_lossy().to_string();
    let mut session = path
        .file_stem()
        .and_then(|n| n.to_str())
        .map(str::to_string);
    let mut parent = None;
    let mut fork_timestamp_ms = None;
    let mut project = None;
    let mut model = None;
    let mut turn = None;
    let mut counter = CodexCounter::default();
    let mut event_index = 0u64;
    let mut unresolved_fork_baseline_seen = false;
    for (line_index, value) in lines(path)? {
        let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
        let payload = value.get("payload").unwrap_or(&Value::Null);
        match kind {
            "session_meta" => {
                session = str_at(payload, &["id", "session_id"]).or(session);
                parent = str_at(
                    payload,
                    &["forked_from_id", "parent_session_id", "parentSessionId"],
                );
                if parent.is_some() {
                    fork_timestamp_ms = value.get("timestamp").map(timestamp_ms);
                }
                project = str_at(payload, &["cwd"]);
            }
            "turn_context" => model = str_at(payload, &["model", "model_name"]),
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("task_started") => {
                turn = str_at(payload, &["turn_id", "turnId"])
            }
            "event_msg" if payload.get("type").and_then(Value::as_str) == Some("token_count") => {
                let info = payload.get("info").unwrap_or(payload);
                let last = info
                    .get("last_token_usage")
                    .map(CodexTokens::from)
                    .filter(|v| !v.zero());
                let total = info
                    .get("total_token_usage")
                    .map(CodexTokens::from)
                    .filter(|v| !v.zero());
                let event_timestamp_ms = value.get("timestamp").map(timestamp_ms).unwrap_or(0);
                if parent.is_some()
                    && fork_timestamp_ms.is_some_and(|fork| event_timestamp_ms <= fork)
                {
                    if let Some(total) = total {
                        counter.establish_unresolved_fork_baseline(total);
                        unresolved_fork_baseline_seen = true;
                    }
                    continue;
                }
                if parent.is_some()
                    && !unresolved_fork_baseline_seen
                    && let Some(total) = total
                {
                    counter.establish_unresolved_fork_baseline(total);
                    unresolved_fork_baseline_seen = true;
                    continue;
                }
                let delta = counter.account(last, total);
                if delta.zero() {
                    continue;
                }
                out.push(UsageEvent {
                    source: "codex".into(),
                    source_path: source_path.clone(),
                    source_record_id: Some(format!("event:{event_index}")),
                    session_id: session.clone(),
                    request_id: turn.clone(),
                    message_id: None,
                    timestamp_ms: event_timestamp_ms,
                    project: project.clone(),
                    provider: Some("openai".into()),
                    model: model
                        .clone()
                        .or_else(|| str_at(info, &["model", "model_name"])),
                    tokens: TokenBuckets::codex(
                        delta.input,
                        delta.cached,
                        delta.output,
                        delta.reasoning,
                    ),
                    source_cost_usd: None,
                    dedupe_confidence: "strong",
                    conservative_undercount: counter.interleaved || parent.is_some(),
                    sidechain: false,
                    source_order: line_index,
                });
                event_index += 1;
            }
            _ => {
                if let Some(usage) = value
                    .get("usage")
                    .or_else(|| value.pointer("/data/usage"))
                    .or_else(|| value.pointer("/result/usage"))
                    .or_else(|| value.pointer("/response/usage"))
                {
                    let tokens = CodexTokens::from(usage);
                    if tokens.zero() {
                        continue;
                    }
                    out.push(UsageEvent {
                        source: "codex".into(),
                        source_path: source_path.clone(),
                        source_record_id: Some(format!("line:{line_index}")),
                        session_id: session.clone(),
                        request_id: None,
                        message_id: None,
                        timestamp_ms: value
                            .get("timestamp")
                            .or_else(|| value.get("created_at"))
                            .map(timestamp_ms)
                            .unwrap_or(0),
                        project: project.clone(),
                        provider: Some("openai".into()),
                        model: model
                            .clone()
                            .or_else(|| str_at(&value, &["model", "model_name"])),
                        tokens: TokenBuckets::codex(
                            tokens.input,
                            tokens.cached,
                            tokens.output,
                            tokens.reasoning,
                        ),
                        source_cost_usd: None,
                        dedupe_confidence: "strong",
                        conservative_undercount: false,
                        sidechain: false,
                        source_order: line_index,
                    });
                }
            }
        }
    }
    Ok(())
}

fn reconcile_codex_copies(events: &mut Vec<UsageEvent>) {
    let mut seen = HashSet::new();
    events.retain(|event| {
        if event.source != "codex" {
            return true;
        }
        let Some(session) = &event.session_id else {
            return true;
        };
        let Some(record) = &event.source_record_id else {
            return true;
        };
        seen.insert((session.clone(), record.clone(), event.tokens.clone()))
    });
}

fn scan_pi(out: &mut Vec<UsageEvent>, _warnings: &mut Vec<String>) -> Result<()> {
    let roots = if let Some(root) = std::env::var_os("PI_CODING_AGENT_SESSION_DIR") {
        vec![PathBuf::from(root)]
    } else if let Some(root) = std::env::var_os("PI_CODING_AGENT_DIR") {
        vec![PathBuf::from(root).join("sessions")]
    } else {
        vec![home().join(".pi/agent/sessions")]
    };
    for path in jsonl_files(roots) {
        let source_path = path.to_string_lossy().to_string();
        let session = path
            .file_stem()
            .and_then(|n| n.to_str())
            .map(str::to_string);
        let project = path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .map(str::to_string);
        let mut current_model = None;
        let mut current_provider = None;
        for (index, value) in lines(&path)? {
            if value.get("type").and_then(Value::as_str) == Some("model_change") {
                current_model = str_at(&value, &["modelId", "model", "model_id"]);
                current_provider = str_at(&value, &["provider", "providerId", "provider_id"]);
                continue;
            }
            if value.get("type").and_then(Value::as_str) != Some("message") {
                continue;
            }
            let Some(message) = value.get("message") else {
                continue;
            };
            if message.get("role").and_then(Value::as_str) != Some("assistant") {
                continue;
            }
            let Some(usage) = message.get("usage") else {
                continue;
            };
            let tokens = TokenBuckets::disjoint(
                u64_at(
                    usage,
                    &[
                        "input",
                        "inputTokens",
                        "input_tokens",
                        "promptTokens",
                        "prompt_tokens",
                    ],
                ),
                u64_at(
                    usage,
                    &[
                        "cacheRead",
                        "cacheReadTokens",
                        "cache_read",
                        "cache_read_tokens",
                        "cacheReadInputTokens",
                        "cache_read_input_tokens",
                    ],
                ),
                u64_at(
                    usage,
                    &[
                        "cacheWrite",
                        "cacheWriteTokens",
                        "cache_write",
                        "cache_write_tokens",
                        "cacheCreationTokens",
                        "cache_creation_tokens",
                        "cacheCreationInputTokens",
                        "cache_creation_input_tokens",
                    ],
                ),
                u64_at(
                    usage,
                    &[
                        "output",
                        "outputTokens",
                        "output_tokens",
                        "completionTokens",
                        "completion_tokens",
                    ],
                ),
            );
            if tokens.additive_total() == 0 {
                continue;
            }
            out.push(UsageEvent {
                source: "pi".into(),
                source_path: source_path.clone(),
                source_record_id: Some(format!("line:{index}")),
                session_id: session.clone(),
                request_id: None,
                message_id: str_at(&value, &["id"]),
                timestamp_ms: value.get("timestamp").map(timestamp_ms).unwrap_or(0),
                project: project.clone(),
                provider: str_at(message, &["provider"])
                    .or_else(|| str_at(&value, &["provider"]))
                    .or_else(|| current_provider.clone()),
                model: str_at(message, &["model", "modelId"])
                    .or_else(|| str_at(&value, &["model", "modelId"]))
                    .or_else(|| current_model.clone()),
                tokens,
                source_cost_usd: usage.pointer("/cost/total").and_then(Value::as_f64),
                dedupe_confidence: "exact",
                conservative_undercount: false,
                sidechain: false,
                source_order: index,
            });
        }
    }
    Ok(())
}

fn scan_opencode(out: &mut Vec<UsageEvent>, warnings: &mut Vec<String>) -> Result<()> {
    let roots: Vec<PathBuf> = std::env::var_os("OPENCODE_DATA_DIR")
        .map(|v| {
            v.to_string_lossy()
                .split(',')
                .map(|s| PathBuf::from(s.trim()))
                .collect()
        })
        .unwrap_or_else(|| vec![home().join(".local/share/opencode")]);
    for root in roots {
        let mut database_ids = HashSet::new();
        let mut databases = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&root) {
            databases.extend(entries.flatten().map(|entry| entry.path()).filter(|path| {
                path.extension().and_then(|v| v.to_str()) == Some("db")
                    && path
                        .file_name()
                        .and_then(|v| v.to_str())
                        .is_some_and(|n| n.starts_with("opencode"))
            }));
        }
        for database in databases {
            if let Err(error) = scan_opencode_db(&database, out, &mut database_ids) {
                warnings.push(format!("{}: {error:#}", database.display()));
            }
        }
        let message_root = root.join("storage/message");
        if message_root.exists() {
            for entry in WalkDir::new(message_root)
                .into_iter()
                .flatten()
                .filter(|e| {
                    e.file_type().is_file()
                        && e.path().extension().and_then(|v| v.to_str()) == Some("json")
                })
            {
                let value: Value = match File::open(entry.path())
                    .ok()
                    .and_then(|file| serde_json::from_reader(file).ok())
                {
                    Some(value) => value,
                    None => continue,
                };
                let id = str_at(&value, &["id"]).or_else(|| {
                    entry
                        .path()
                        .file_stem()
                        .and_then(|n| n.to_str())
                        .map(str::to_string)
                });
                if id.as_ref().is_some_and(|id| database_ids.contains(id)) {
                    continue;
                }
                push_opencode_event(&value, entry.path(), id, out);
            }
        }
    }
    Ok(())
}

fn scan_opencode_db(
    path: &Path,
    out: &mut Vec<UsageEvent>,
    ids: &mut HashSet<String>,
) -> Result<()> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(std::time::Duration::from_secs(1))?;
    let mut stmt = conn.prepare("SELECT id, session_id, data FROM message")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    for row in rows {
        let (id, session, data) = row?;
        let Ok(mut value) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        if value.get("sessionID").is_none()
            && let Some(object) = value.as_object_mut()
        {
            object.insert(
                "sessionID".into(),
                session.map(Value::String).unwrap_or(Value::Null),
            );
        }
        let before = out.len();
        if !ids.contains(&id) {
            push_opencode_event(&value, path, Some(id.clone()), out);
        }
        if out.len() > before {
            ids.insert(id);
        }
    }
    Ok(())
}

fn push_opencode_event(value: &Value, path: &Path, id: Option<String>, out: &mut Vec<UsageEvent>) {
    let Some(usage) = value.get("tokens") else {
        return;
    };
    let reasoning = u64_at(usage, &["reasoning"]);
    let mut tokens = TokenBuckets::disjoint(
        u64_at(usage, &["input"]),
        usage
            .pointer("/cache/read")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        usage
            .pointer("/cache/write")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        u64_at(usage, &["output"]).saturating_add(reasoning),
    );
    // OpenCode persists reasoning separately from output. Fold it into the
    // additive output bucket while retaining the detail field for reporting.
    tokens.reasoning = reasoning;
    if tokens.additive_total() == 0 {
        return;
    }
    out.push(UsageEvent {
        source: "opencode".into(),
        source_path: path.to_string_lossy().to_string(),
        source_record_id: id.clone(),
        session_id: str_at(value, &["sessionID", "session_id"]),
        request_id: None,
        message_id: id,
        timestamp_ms: value
            .pointer("/time/created")
            .map(timestamp_ms)
            .unwrap_or(0),
        project: str_at(value, &["directory", "cwd"]),
        provider: str_at(value, &["providerID", "provider"]),
        model: str_at(value, &["modelID", "model"]),
        tokens,
        source_cost_usd: value.get("cost").and_then(Value::as_f64),
        dedupe_confidence: "exact",
        conservative_undercount: false,
        sidechain: false,
        source_order: 0,
    });
}

fn scan_cursor(out: &mut Vec<UsageEvent>, warnings: &mut Vec<String>) -> Result<()> {
    let user = if cfg!(target_os = "macos") {
        home().join("Library/Application Support/Cursor/User")
    } else {
        home().join(".config/Cursor/User")
    };
    let mut databases = vec![user.join("globalStorage/state.vscdb")];
    databases.extend(
        WalkDir::new(user.join("workspaceStorage"))
            .max_depth(3)
            .into_iter()
            .flatten()
            .filter(|e| e.file_type().is_file() && e.file_name() == "state.vscdb")
            .map(|e| e.path().to_path_buf()),
    );
    let mut seen = HashSet::new();
    for path in databases.into_iter().filter(|path| path.exists()) {
        if let Err(error) = scan_cursor_db(&path, out, &mut seen) {
            warnings.push(format!("{}: {error:#}", path.display()));
        }
    }
    Ok(())
}

fn scan_cursor_db(
    path: &Path,
    out: &mut Vec<UsageEvent>,
    seen: &mut HashSet<String>,
) -> Result<()> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(std::time::Duration::from_secs(1))?;
    for (table, query) in [
        (
            "cursorDiskKV",
            "SELECT key, value FROM cursorDiskKV WHERE key LIKE 'composerData:%' OR key LIKE 'bubbleId:%'",
        ),
        (
            "ItemTable",
            "SELECT key, value FROM ItemTable WHERE key IN ('aiService.generations', 'workbench.panel.aichat.view.aichat.chatdata')",
        ),
    ] {
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
            [table],
            |row| row.get(0),
        )?;
        if !exists {
            continue;
        }
        let mut stmt = conn.prepare(query)?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            let (key, raw) = row?;
            let Ok(value) = serde_json::from_str::<Value>(&raw) else {
                continue;
            };
            extract_cursor_objects(&value, &key, table, path, out, seen);
        }
    }
    Ok(())
}

fn extract_cursor_objects(
    value: &Value,
    fallback_id: &str,
    table: &str,
    path: &Path,
    out: &mut Vec<UsageEvent>,
    seen: &mut HashSet<String>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                extract_cursor_objects(value, fallback_id, table, path, out, seen);
            }
        }
        Value::Object(object) => {
            let input = nested_u64(object, &["inputTokens", "input_tokens"]);
            let output = nested_u64(object, &["outputTokens", "output_tokens"]);
            if input > 0 || output > 0 {
                let id = object_string(
                    object,
                    &["generationUUID", "generationId", "bubbleId", "id"],
                )
                .unwrap_or_else(|| {
                    format!(
                        "{fallback_id}:{}:{}",
                        object.get("createdAt").unwrap_or(&Value::Null),
                        input + output
                    )
                });
                let dedupe = format!("{id}:{input}:{output}");
                if seen.insert(dedupe) {
                    out.push(UsageEvent {
                        source: "cursor".into(),
                        source_path: path.to_string_lossy().to_string(),
                        source_record_id: Some(id),
                        session_id: object_string(object, &["sessionId", "composerId"]),
                        request_id: None,
                        message_id: None,
                        timestamp_ms: object
                            .get("createdAt")
                            .or_else(|| object.get("timestamp"))
                            .map(timestamp_ms)
                            .unwrap_or(0),
                        project: None,
                        provider: None,
                        model: object_string(object, &["model", "modelName"])
                            .or_else(|| {
                                object
                                    .get("modelInfo")
                                    .and_then(|v| str_at(v, &["modelName"]))
                            })
                            .or_else(|| {
                                object
                                    .get("modelConfig")
                                    .and_then(|v| str_at(v, &["modelName"]))
                            }),
                        tokens: TokenBuckets::disjoint(input, 0, 0, output),
                        source_cost_usd: None,
                        dedupe_confidence: if table == "cursorDiskKV" {
                            "exact"
                        } else {
                            "strong"
                        },
                        conservative_undercount: false,
                        sidechain: false,
                        source_order: 0,
                    });
                }
            }
            for child in object.values() {
                if child.is_array() || child.is_object() {
                    extract_cursor_objects(child, fallback_id, table, path, out, seen);
                }
            }
        }
        _ => {}
    }
}

fn nested_u64(object: &Map<String, Value>, aliases: &[&str]) -> u64 {
    aliases
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_u64))
        .or_else(|| {
            object.get("tokenCount").and_then(|v| {
                aliases
                    .iter()
                    .find_map(|key| v.get(*key).and_then(Value::as_u64))
            })
        })
        .or_else(|| {
            object.get("usage").and_then(|v| {
                aliases
                    .iter()
                    .find_map(|key| v.get(*key).and_then(Value::as_u64))
            })
        })
        .unwrap_or(0)
}

fn object_string(object: &Map<String, Value>, aliases: &[&str]) -> Option<String> {
    aliases
        .iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
        .map(str::to_string)
}

fn scan_copilot(out: &mut Vec<UsageEvent>, _warnings: &mut Vec<String>) -> Result<()> {
    let mut roots = vec![home().join(".copilot/otel")];
    if let Some(path) = std::env::var_os("COPILOT_OTEL_FILE_EXPORTER_PATH") {
        roots.push(PathBuf::from(path));
    }
    let files = roots
        .into_iter()
        .flat_map(|root| {
            if root.is_file() {
                vec![root]
            } else {
                jsonl_files([root])
            }
        })
        .collect::<Vec<_>>();
    let mut seen = HashSet::new();
    for path in files {
        for (index, value) in lines(&path)? {
            extract_otel(&value, &path, index, out, &mut seen);
        }
    }
    Ok(())
}

fn extract_otel(
    value: &Value,
    path: &Path,
    index: u64,
    out: &mut Vec<UsageEvent>,
    seen: &mut HashSet<String>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                extract_otel(value, path, index, out, seen);
            }
        }
        Value::Object(object) => {
            let attrs = otel_attributes(object.get("attributes").unwrap_or(value));
            let operation = attrs
                .get("gen_ai.operation.name")
                .and_then(Value::as_str)
                .or_else(|| object.get("name").and_then(Value::as_str));
            let input = attrs
                .get("gen_ai.usage.input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let output = attrs
                .get("gen_ai.usage.output_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if operation == Some("chat") && (input > 0 || output > 0) {
                let trace = object
                    .get("traceId")
                    .or_else(|| object.get("trace_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let span = object
                    .get("spanId")
                    .or_else(|| object.get("span_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let response = attrs
                    .get("gen_ai.response.id")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let id = if !trace.is_empty() || !span.is_empty() {
                    format!("{trace}:{span}")
                } else if !response.is_empty() {
                    format!("response:{response}")
                } else {
                    format!("{}:{index}:{input}:{output}", path.display())
                };
                if seen.insert(id.clone()) {
                    let cache = attrs
                        .get("gen_ai.usage.cache_read.input_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        .min(input);
                    out.push(UsageEvent {
                        source: "copilot".into(),
                        source_path: path.to_string_lossy().to_string(),
                        source_record_id: Some(id),
                        session_id: attrs
                            .get("gen_ai.conversation.id")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        request_id: attrs
                            .get("gen_ai.response.id")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        message_id: None,
                        timestamp_ms: object
                            .get("startTimeUnixNano")
                            .and_then(Value::as_str)
                            .and_then(|v| v.parse::<u64>().ok())
                            .map(|v| v / 1_000_000)
                            .or_else(|| object.get("timestamp").map(timestamp_ms))
                            .unwrap_or(0),
                        project: attrs
                            .get("copilot_chat.repo.remote_url")
                            .or_else(|| attrs.get("github.copilot.git.repository"))
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        provider: Some("github-copilot".into()),
                        model: attrs
                            .get("gen_ai.response.model")
                            .or_else(|| attrs.get("gen_ai.request.model"))
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        tokens: TokenBuckets::codex(
                            input,
                            cache,
                            output,
                            attrs
                                .get("gen_ai.usage.reasoning.output_tokens")
                                .or_else(|| attrs.get("gen_ai.usage.reasoning_tokens"))
                                .and_then(Value::as_u64)
                                .unwrap_or(0),
                        ),
                        source_cost_usd: None,
                        dedupe_confidence: "exact",
                        conservative_undercount: false,
                        sidechain: false,
                        source_order: index,
                    });
                }
            }
            for child in object.values() {
                if child.is_array() || child.is_object() {
                    extract_otel(child, path, index, out, seen);
                }
            }
        }
        _ => {}
    }
}

fn otel_attributes(value: &Value) -> HashMap<String, Value> {
    if let Some(object) = value.as_object() {
        return object
            .iter()
            .map(|(k, v)| (k.clone(), unwrap_otel_value(v)))
            .collect();
    }
    let mut out = HashMap::new();
    if let Some(array) = value.as_array() {
        for item in array {
            if let (Some(key), Some(value)) =
                (item.get("key").and_then(Value::as_str), item.get("value"))
            {
                out.insert(key.to_string(), unwrap_otel_value(value));
            }
        }
    }
    out
}

fn unwrap_otel_value(value: &Value) -> Value {
    for key in ["stringValue", "intValue", "doubleValue", "boolValue"] {
        if let Some(inner) = value.get(key) {
            if key == "intValue"
                && let Some(text) = inner.as_str()
            {
                return text
                    .parse::<u64>()
                    .map(Value::from)
                    .unwrap_or_else(|_| inner.clone());
            }
            return inner.clone();
        }
    }
    value.clone()
}

// Rates are nano-USD per million tokens. The catalog is deliberately small and versioned:
// unknown models remain unpriced instead of silently inheriting a guessed family rate.
const PRICE_CATALOG_ID: &str = "official-api-prices-2026-07-15";

#[derive(Clone, Copy)]
struct Rates {
    input: u64,
    cache_read: u64,
    cache_write_5m: u64,
    cache_write_1h: u64,
    output: u64,
}

const fn usd_per_million(value_milli_usd: u64) -> u64 {
    value_milli_usd * 1_000_000
}

fn event_cost_nanos(event: &UsageEvent, mode: CostMode) -> Option<u64> {
    let source = event
        .source_cost_usd
        .filter(|value| value.is_finite() && *value >= 0.0)
        .and_then(|value| {
            let nanos = value * 1_000_000_000.0;
            (nanos <= u64::MAX as f64).then_some(nanos.round() as u64)
        });
    match mode {
        CostMode::Source => source,
        CostMode::Auto => source.or_else(|| calculated_cost_nanos(event)),
        CostMode::Reprice => calculated_cost_nanos(event),
    }
}

fn calculated_cost_nanos(event: &UsageEvent) -> Option<u64> {
    let rates = rates_for(event.provider.as_deref(), event.model.as_deref()?)?;
    let cache_write_1h = event.tokens.cache_write_1h.min(event.tokens.cache_write);
    let cache_write_5m = event.tokens.cache_write.saturating_sub(cache_write_1h);
    let total = (event.tokens.uncached_input as u128) * (rates.input as u128)
        + (event.tokens.cache_read as u128) * (rates.cache_read as u128)
        + (cache_write_5m as u128) * (rates.cache_write_5m as u128)
        + (cache_write_1h as u128) * (rates.cache_write_1h as u128)
        + (event.tokens.output as u128) * (rates.output as u128);
    // Rates are per million tokens. Reasoning is retained as an output subset and is not charged
    // a second time.
    u64::try_from(total / 1_000_000).ok()
}

fn rates_for(provider: Option<&str>, model: &str) -> Option<Rates> {
    let model = model.trim().to_ascii_lowercase();
    let provider = provider.unwrap_or("").trim().to_ascii_lowercase();
    let exact_or_snapshot = |base: &str| {
        model == base
            || model.strip_prefix(base).is_some_and(|suffix| {
                suffix.starts_with("-20")
                    && suffix[1..].chars().all(|c| c.is_ascii_digit() || c == '-')
            })
    };

    let openai = provider.is_empty()
        || provider.contains("openai")
        || provider.contains("codex")
        || provider.contains("github-copilot");
    if openai {
        if exact_or_snapshot("gpt-5.5") {
            return Some(openai_rates(5_000, 500, 30_000));
        }
        if exact_or_snapshot("gpt-5.4") {
            return Some(openai_rates(2_500, 250, 15_000));
        }
        if exact_or_snapshot("gpt-5.4-mini") {
            return Some(openai_rates(750, 75, 4_500));
        }
        if exact_or_snapshot("gpt-5.3-codex") || exact_or_snapshot("gpt-5.2-codex") {
            return Some(openai_rates(1_750, 175, 14_000));
        }
        if exact_or_snapshot("gpt-5-codex") || exact_or_snapshot("gpt-5") {
            return Some(openai_rates(1_250, 125, 10_000));
        }
        if exact_or_snapshot("gpt-4o") {
            return Some(openai_rates(2_500, 1_250, 10_000));
        }
        if exact_or_snapshot("gpt-4o-mini") {
            return Some(openai_rates(150, 75, 600));
        }
    }

    let anthropic = provider.is_empty() || provider.contains("anthropic");
    if anthropic {
        if [
            "claude-opus-4-8",
            "claude-opus-4-7",
            "claude-opus-4-6",
            "claude-opus-4-5",
        ]
        .iter()
        .any(|base| exact_or_snapshot(base))
        {
            return Some(claude_rates(5_000, 6_250, 10_000, 500, 25_000));
        }
        if exact_or_snapshot("claude-opus-4-1") || exact_or_snapshot("claude-opus-4") {
            return Some(claude_rates(15_000, 18_750, 30_000, 1_500, 75_000));
        }
        if exact_or_snapshot("claude-sonnet-5") {
            // Promotional rate valid on the catalog's 2026-07-15 effective date.
            return Some(claude_rates(2_000, 2_500, 4_000, 200, 10_000));
        }
        if ["claude-sonnet-4-6", "claude-sonnet-4-5", "claude-sonnet-4"]
            .iter()
            .any(|base| exact_or_snapshot(base))
        {
            return Some(claude_rates(3_000, 3_750, 6_000, 300, 15_000));
        }
        if exact_or_snapshot("claude-haiku-4-5") {
            return Some(claude_rates(1_000, 1_250, 2_000, 100, 5_000));
        }
    }
    None
}

fn openai_rates(input: u64, cached: u64, output: u64) -> Rates {
    Rates {
        input: usd_per_million(input),
        cache_read: usd_per_million(cached),
        cache_write_5m: usd_per_million(input),
        cache_write_1h: usd_per_million(input),
        output: usd_per_million(output),
    }
}

fn claude_rates(input: u64, write_5m: u64, write_1h: u64, read: u64, output: u64) -> Rates {
    Rates {
        input: usd_per_million(input),
        cache_read: usd_per_million(read),
        cache_write_5m: usd_per_million(write_5m),
        cache_write_1h: usd_per_million(write_1h),
        output: usd_per_million(output),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(input: u64, cached: u64, output: u64) -> CodexTokens {
        CodexTokens {
            input,
            cached,
            output,
            reasoning: 0,
        }
    }

    #[test]
    fn codex_repeated_total_does_not_repeat_last() {
        let mut counter = CodexCounter::default();
        assert_eq!(
            counter.account(Some(c(100, 20, 10)), Some(c(100, 20, 10))),
            c(100, 20, 10)
        );
        assert_eq!(
            counter.account(Some(c(100, 20, 10)), Some(c(100, 20, 10))),
            c(0, 0, 0)
        );
    }

    #[test]
    fn codex_interleaved_stream_never_recounts_high_water_gap() {
        let mut counter = CodexCounter::default();
        assert_eq!(counter.account(None, Some(c(1000, 0, 0))), c(1000, 0, 0));
        assert_eq!(
            counter.account(Some(c(200, 0, 0)), Some(c(200, 0, 0))),
            c(0, 0, 0)
        );
        assert_eq!(
            counter.account(Some(c(900, 0, 0)), Some(c(1100, 0, 0))),
            c(100, 0, 0)
        );
        assert_eq!(counter.counted.input, 1100);
    }

    #[test]
    fn openai_cached_input_is_a_subset() {
        let tokens = TokenBuckets::codex(100, 80, 10, 4);
        assert_eq!(tokens.uncached_input, 20);
        assert_eq!(tokens.cache_read, 80);
        assert_eq!(tokens.additive_total(), 110);
    }

    #[test]
    fn opencode_reasoning_is_included_in_output_and_total() {
        let value = serde_json::json!({
            "tokens": {
                "input": 100,
                "output": 20,
                "reasoning": 30,
                "cache": { "read": 40, "write": 10 }
            }
        });
        let mut events = Vec::new();

        push_opencode_event(
            &value,
            Path::new("message.json"),
            Some("message".into()),
            &mut events,
        );

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tokens.reasoning, 30);
        assert_eq!(events[0].tokens.output, 50);
        assert_eq!(events[0].tokens.total(), 200);
    }

    #[test]
    fn claude_cache_write_durations_get_distinct_rates() {
        let mut tokens = TokenBuckets::disjoint(100, 40, 30, 20);
        tokens.cache_write_1h = 10;
        let event = UsageEvent {
            source: "claude".into(),
            source_path: "x".into(),
            source_record_id: None,
            session_id: None,
            request_id: None,
            message_id: None,
            timestamp_ms: 0,
            project: None,
            provider: Some("anthropic".into()),
            model: Some("claude-sonnet-4-6".into()),
            tokens,
            source_cost_usd: None,
            dedupe_confidence: "exact",
            conservative_undercount: false,
            sidechain: false,
            source_order: 0,
        };
        // 100*3 + 40*.3 + 20*3.75 + 10*6 + 20*15 = $0.000747
        assert_eq!(calculated_cost_nanos(&event), Some(747_000));
    }

    #[test]
    fn auto_cost_honors_explicit_zero_source_cost() {
        let event = UsageEvent {
            source: "claude".into(),
            source_path: "x".into(),
            source_record_id: None,
            session_id: None,
            request_id: None,
            message_id: None,
            timestamp_ms: 0,
            project: None,
            provider: Some("anthropic".into()),
            model: Some("claude-sonnet-4-6".into()),
            tokens: TokenBuckets::disjoint(100, 0, 0, 0),
            source_cost_usd: Some(0.0),
            dedupe_confidence: "exact",
            conservative_undercount: false,
            sidechain: false,
            source_order: 0,
        };
        assert_eq!(event_cost_nanos(&event, CostMode::Auto), Some(0));
        assert_eq!(event_cost_nanos(&event, CostMode::Reprice), Some(300_000));
    }
}
