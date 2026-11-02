//! Search-result fusion, opt-in retrieval traces, and offline evaluation helpers.
//!
//! This module deliberately has no dependency on the CLI search execution path.  The
//! CLI can feed its per-query ranked results to [`fuse_ranked_queries`], persist a
//! [`RetrievalTrace`] with [`TraceWriter`], and evaluate the same result shape with
//! the pure metric functions below.

use crate::config::Paths;
use crate::machine::LocatedRecord;
use crate::retrieval::canonical_record_id;
use crate::types::{SourceFilter, SourceKind};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

/// Default reciprocal-rank-fusion constant used by the search skill.
pub const DEFAULT_RRF_K: f32 = 60.0;

/// Stable identity for a search result.  `doc_id` alone is not sufficient for
/// federated indexes because each machine may allocate IDs independently.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ResultKey {
    machine: String,
    source: SourceKind,
    session_id: String,
    source_path: String,
    record_id: String,
}

impl ResultKey {
    fn from_record(record: &LocatedRecord) -> Self {
        Self {
            machine: record.machine.clone(),
            source: record.record.source,
            session_id: record.record.session_id.clone(),
            source_path: record.record.source_path.clone(),
            record_id: canonical_record_id(&record.record),
        }
    }
}

fn compare_result_keys(left: &ResultKey, right: &ResultKey) -> std::cmp::Ordering {
    left.machine
        .cmp(&right.machine)
        .then_with(|| left.source.label().cmp(right.source.label()))
        .then_with(|| left.session_id.cmp(&right.session_id))
        .then_with(|| left.source_path.cmp(&right.source_path))
        .then_with(|| left.record_id.cmp(&right.record_id))
}

#[derive(Debug)]
struct FusedCandidate {
    result: LocatedRecord,
    fused_score: f32,
    key: ResultKey,
}

/// Fuse already-ranked result lists with reciprocal-rank fusion.
///
/// Each inner vector is treated as one independently ranked query view.  A
/// record appearing in multiple views is emitted once and receives the sum of
/// `1 / (rrf_k + rank)` contributions.  The input score is used only to choose
/// the representative payload when duplicate identities have different
/// payloads; it never affects the RRF score.
///
/// Output ordering is deterministic: fused score descending, then timestamp,
/// machine, source, session, source path, and canonical record ID. Invalid
/// `rrf_k` values fall back to [`DEFAULT_RRF_K`]. Non-finite input scores are
/// never preferred over finite scores when choosing a representative payload;
/// ties are resolved by the first ranked query, whose order is part of the API.
pub fn fuse_ranked_queries(
    ranked_queries: Vec<Vec<LocatedRecord>>,
    rrf_k: f32,
) -> Vec<LocatedRecord> {
    let rrf_k = if rrf_k.is_finite() && rrf_k >= 0.0 {
        rrf_k
    } else {
        DEFAULT_RRF_K
    };
    let mut fused = HashMap::<ResultKey, FusedCandidate>::new();

    for ranked in ranked_queries {
        for (rank, result) in ranked.into_iter().enumerate() {
            let contribution = 1.0 / (rrf_k + rank as f32 + 1.0);
            let key = ResultKey::from_record(&result);
            match fused.entry(key.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(FusedCandidate {
                        result,
                        fused_score: contribution,
                        key,
                    });
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let candidate = entry.get_mut();
                    candidate.fused_score += contribution;
                    // Keep the highest-scoring representative. Ties retain the first query's
                    // payload, making the result independent of HashMap iteration order. Treat
                    // NaN and infinities consistently rather than relying on `>` (which makes
                    // NaN silently win or lose based on input order).
                    if compare_input_scores(result.score, candidate.result.score)
                        == std::cmp::Ordering::Greater
                    {
                        candidate.result = result;
                    }
                }
            }
        }
    }

    let mut candidates: Vec<_> = fused.into_values().collect();
    candidates.sort_by(|left, right| {
        right
            .fused_score
            .total_cmp(&left.fused_score)
            .then_with(|| right.result.record.ts.cmp(&left.result.record.ts))
            .then_with(|| compare_result_keys(&left.key, &right.key))
    });

    candidates
        .into_iter()
        .map(|mut candidate| {
            candidate.result.score = candidate.fused_score;
            candidate.result
        })
        .collect()
}

fn compare_input_scores(left: f32, right: f32) -> std::cmp::Ordering {
    left.is_finite()
        .cmp(&right.is_finite())
        .then_with(|| left.total_cmp(&right))
}

/// A query view recorded in a retrieval trace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TraceQuery {
    pub query_index: usize,
    pub query: String,
    pub candidate_count: usize,
}

/// A ranked result in a retrieval trace.  It intentionally contains no
/// transcript text, snippets, tool inputs, or tool outputs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TraceHit {
    pub rank: usize,
    pub machine: String,
    pub score: f32,
    pub source: SourceKind,
    pub session_id: String,
    pub source_path: String,
    pub doc_id: u64,
    pub record_id: String,
}

impl TraceHit {
    pub fn from_result(rank: usize, result: &LocatedRecord) -> Self {
        Self {
            rank,
            machine: result.machine.clone(),
            score: result.score,
            source: result.record.source,
            session_id: result.record.session_id.clone(),
            source_path: result.record.source_path.clone(),
            doc_id: result.record.doc_id,
            record_id: canonical_record_id(&result.record),
        }
    }
}

/// Privacy-conscious, opt-in metadata about one retrieval operation.
///
/// Traces intentionally omit transcript text, snippets, tool inputs, and tool outputs, but they
/// still contain raw query strings, working directories, source paths, session IDs, and machine
/// IDs. Callers should only enable tracing where that metadata is acceptable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetrievalTrace {
    pub trace_id: String,
    pub started_at_ms: u64,
    pub elapsed_ms: Option<u64>,
    pub mode: Option<String>,
    pub queries: Vec<TraceQuery>,
    pub cwd: Option<String>,
    pub machines: Vec<String>,
    pub candidate_count: usize,
    pub result_count: usize,
    pub failures: Vec<String>,
    pub hits: Vec<TraceHit>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RetrievalTraceMetadata {
    pub trace_id: String,
    pub started_at_ms: u64,
    pub elapsed_ms: Option<u64>,
    pub mode: Option<String>,
    pub queries: Vec<TraceQuery>,
    pub cwd: Option<String>,
    pub machines: Vec<String>,
    pub candidate_count: usize,
    pub failures: Vec<String>,
}

impl RetrievalTrace {
    /// Construct a trace from final results while keeping transcript contents
    /// out of the persisted record.
    pub fn from_results(metadata: RetrievalTraceMetadata, results: &[LocatedRecord]) -> Self {
        let hits = results
            .iter()
            .enumerate()
            .map(|(index, result)| TraceHit::from_result(index + 1, result))
            .collect();
        Self {
            trace_id: metadata.trace_id,
            started_at_ms: metadata.started_at_ms,
            elapsed_ms: metadata.elapsed_ms,
            mode: metadata.mode,
            queries: metadata.queries,
            cwd: metadata.cwd,
            machines: metadata.machines,
            candidate_count: metadata.candidate_count,
            result_count: results.len(),
            failures: metadata.failures,
            hits,
        }
    }
}

/// The default append-only trace file under a Memex state directory.
pub fn trace_path(paths: &Paths) -> PathBuf {
    paths.state.join("retrieval-traces.jsonl")
}

/// Append one trace as exactly one JSONL record.
///
/// The target file is opened with `O_APPEND` and held under an advisory exclusive lock while the
/// complete line is written. This protects against interleaving between cooperating memex
/// processes even if `write_all` internally performs more than one OS write.
pub fn append_trace(paths: &Paths, trace: &RetrievalTrace) -> Result<()> {
    append_trace_at_path(&trace_path(paths), trace)
}

fn append_trace_at_path(path: &Path, trace: &RetrievalTrace) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create trace state directory {}", parent.display()))?;
    }
    let mut line = serde_json::to_vec(trace).context("serialize retrieval trace")?;
    line.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open retrieval trace file {}", path.display()))?;
    let lock_result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if lock_result != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("lock retrieval trace file {}", path.display()));
    }
    file.write_all(&line)
        .with_context(|| format!("append retrieval trace {}", path.display()))?;
    file.flush()
        .with_context(|| format!("flush retrieval trace {}", path.display()))?;
    let unlock_result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
    if unlock_result != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("unlock retrieval trace file {}", path.display()));
    }
    Ok(())
}

/// Reusable opt-in writer for callers that emit multiple traces.
#[derive(Debug, Clone)]
pub struct TraceWriter {
    path: PathBuf,
}

impl TraceWriter {
    pub fn new(paths: &Paths) -> Self {
        Self {
            path: trace_path(paths),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, trace: &RetrievalTrace) -> Result<()> {
        append_trace_at_path(&self.path, trace)
    }
}

/// One known relevant result in an evaluation qrels file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvaluationRelevance {
    pub machine: String,
    pub source: SourceKind,
    pub session_id: String,
    pub source_path: String,
    pub doc_id: u64,
    #[serde(default)]
    pub record_id: Option<String>,
    #[serde(default = "default_relevance")]
    pub relevance: f32,
    /// Verbatim spans expected in the rendered snippet for this record.
    #[serde(default)]
    pub evidence: Vec<String>,
}

fn default_relevance() -> f32 {
    1.0
}

/// Search filters applied to an evaluation case.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EvaluationFilters {
    pub project: Option<String>,
    pub source: Option<SourceFilter>,
    pub role: Option<String>,
    pub session: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
}

/// One JSONL retrieval evaluation case. Use either `query` or `queries`, but
/// not both; multi-query cases use the latter. An empty `relevant` list denotes
/// a deliberately judged no-answer case, for which no results should be returned.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EvaluationCase {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub queries: Vec<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub filters: EvaluationFilters,
    pub relevant: Vec<EvaluationRelevance>,
}

impl EvaluationCase {
    pub fn query_views(&self) -> Result<Vec<String>> {
        if self.query.is_some() && !self.queries.is_empty() {
            bail!("evaluation case cannot specify both query and queries");
        }
        let views = self
            .query
            .as_ref()
            .map(|query| vec![query.clone()])
            .unwrap_or_else(|| self.queries.clone());
        if views.is_empty() || views.iter().any(|query| query.trim().is_empty()) {
            bail!("evaluation case must contain a non-empty query or queries list");
        }
        validate_relevance(&self.relevant)?;
        Ok(views)
    }
}

/// Validate qrels before using them in an evaluation run.
pub fn validate_relevance(relevant: &[EvaluationRelevance]) -> Result<()> {
    for relevance in relevant {
        if relevance.machine.trim().is_empty()
            || relevance.session_id.trim().is_empty()
            || relevance.source_path.trim().is_empty()
        {
            bail!("relevance entries require machine, session_id, and source_path");
        }
        if !relevance.relevance.is_finite() || relevance.relevance < 0.0 {
            bail!("relevance values must be finite and non-negative");
        }
        if relevance.evidence.iter().any(|span| span.trim().is_empty()) {
            bail!("evidence spans must be nonempty");
        }
    }
    Ok(())
}

/// A JSONL evaluation dataset.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvaluationDataset {
    pub cases: Vec<EvaluationCase>,
}

impl EvaluationDataset {
    pub fn from_jsonl(input: &str) -> Result<Self> {
        let mut cases = Vec::new();
        for (line_number, line) in input.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let case: EvaluationCase = serde_json::from_str(line)
                .with_context(|| format!("parse evaluation JSONL line {}", line_number + 1))?;
            case.query_views()
                .with_context(|| format!("validate evaluation JSONL line {}", line_number + 1))?;
            cases.push(case);
        }
        if cases.is_empty() {
            bail!("evaluation dataset is empty");
        }
        let mut ids = HashSet::new();
        for case in &cases {
            if let Some(id) = &case.id
                && !ids.insert(id)
            {
                bail!("duplicate evaluation case id '{id}'");
            }
        }
        Ok(Self { cases })
    }

    pub fn read_jsonl(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let input = fs::read_to_string(path)
            .with_context(|| format!("read evaluation dataset {}", path.display()))?;
        Self::from_jsonl(&input)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RelevanceMap {
    values: HashMap<ResultKey, f32>,
    legacy_values: HashMap<LegacyKey, f32>,
    evidence: HashMap<ResultKey, HashSet<String>>,
    legacy_evidence: HashMap<LegacyKey, HashSet<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LegacyKey {
    machine: String,
    source: SourceKind,
    session_id: String,
    source_path: String,
    doc_id: u64,
}

impl LegacyKey {
    fn from_entry(entry: &EvaluationRelevance) -> Self {
        Self {
            machine: entry.machine.clone(),
            source: entry.source,
            session_id: entry.session_id.clone(),
            source_path: entry.source_path.clone(),
            doc_id: entry.doc_id,
        }
    }

    fn from_result(result: &LocatedRecord) -> Self {
        Self {
            machine: result.machine.clone(),
            source: result.record.source,
            session_id: result.record.session_id.clone(),
            source_path: result.record.source_path.clone(),
            doc_id: result.record.doc_id,
        }
    }
}

impl RelevanceMap {
    fn new(relevant: &[EvaluationRelevance]) -> Self {
        let mut values: HashMap<ResultKey, f32> = HashMap::new();
        let mut legacy_values: HashMap<LegacyKey, f32> = HashMap::new();
        let mut stable_aliases: HashMap<LegacyKey, ResultKey> = HashMap::new();
        for entry in relevant {
            let key = ResultKey {
                machine: entry.machine.clone(),
                source: entry.source,
                session_id: entry.session_id.clone(),
                source_path: entry.source_path.clone(),
                record_id: entry
                    .record_id
                    .clone()
                    .unwrap_or_else(|| format!("doc:{}", entry.doc_id)),
            };
            if !entry.relevance.is_finite() || entry.relevance < 0.0 {
                continue;
            }
            if entry.record_id.is_some() {
                stable_aliases.insert(LegacyKey::from_entry(entry), key.clone());
                values
                    .entry(key)
                    .and_modify(|value| *value = value.max(entry.relevance))
                    .or_insert(entry.relevance);
            } else {
                legacy_values
                    .entry(LegacyKey::from_entry(entry))
                    .and_modify(|value| *value = value.max(entry.relevance))
                    .or_insert(entry.relevance);
            }
        }
        // A mixed qrels file may contain the same result once with its stable ID and once in
        // legacy doc-id form. Fold the legacy alias into the stable key when all identifying
        // metadata, including doc_id, agrees.
        let legacy = std::mem::take(&mut legacy_values);
        for (legacy_key, relevance) in legacy {
            if let Some(stable_key) = stable_aliases.get(&legacy_key) {
                values
                    .entry(stable_key.clone())
                    .and_modify(|value| *value = value.max(relevance))
                    .or_insert(relevance);
            } else {
                legacy_values.insert(legacy_key, relevance);
            }
        }
        let mut evidence = HashMap::<ResultKey, HashSet<String>>::new();
        let mut legacy_evidence = HashMap::<LegacyKey, HashSet<String>>::new();
        for entry in relevant {
            if !entry.relevance.is_finite() || entry.relevance <= 0.0 {
                continue;
            }
            let legacy_key = LegacyKey::from_entry(entry);
            let stable_key = entry.record_id.as_ref().map(|record_id| ResultKey {
                machine: entry.machine.clone(),
                source: entry.source,
                session_id: entry.session_id.clone(),
                source_path: entry.source_path.clone(),
                record_id: record_id.clone(),
            });
            let spans = if let Some(key) = stable_key.as_ref().or(stable_aliases.get(&legacy_key)) {
                evidence.entry(key.clone()).or_default()
            } else {
                legacy_evidence.entry(legacy_key).or_default()
            };
            spans.extend(entry.evidence.iter().cloned());
        }
        Self {
            values,
            legacy_values,
            evidence,
            legacy_evidence,
        }
    }

    fn relevance_for(&self, result: &LocatedRecord) -> Option<f32> {
        let stable_key = key_for_result(result);
        self.values.get(&stable_key).copied().or_else(|| {
            self.legacy_values
                .get(&LegacyKey::from_result(result))
                .copied()
        })
    }

    fn positive_count(&self) -> usize {
        self.values
            .values()
            .chain(self.legacy_values.values())
            .filter(|value| **value > 0.0)
            .count()
    }
}

fn key_for_result(result: &LocatedRecord) -> ResultKey {
    ResultKey::from_record(result)
}

/// Ranked hits and the snippets actually presented by a search surface.
#[derive(Debug)]
pub struct EvaluationResults {
    pub hits: Vec<LocatedRecord>,
    pub snippets: Vec<String>,
}

/// Metrics for one case. Positive retrieval metrics are undefined for no-answer cases.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CaseMetrics {
    pub mrr_at_k: Option<f64>,
    pub recall_at_k: Option<f64>,
    pub ndcg_at_k: Option<f64>,
    pub conversation_success_at_5: Option<f64>,
    pub snippet_evidence_coverage: Option<f64>,
    pub no_answer_correct: Option<bool>,
    pub returned_results: usize,
}

/// Evaluate ranked results and their aligned rendered snippets.
///
/// Conversation success considers the first five distinct conversations in the
/// supplied results, independent of the record cutoff `k`. Evidence coverage
/// counts unique positive record/span judgments found verbatim in that record's
/// snippet within the first `k` hits. Missing evidence judgments yield `None`.
pub fn evaluate_case(
    case: &EvaluationCase,
    results: &[LocatedRecord],
    snippets: &[String],
    k: usize,
) -> Result<CaseMetrics> {
    if k == 0 {
        bail!("evaluation cutoff k must be greater than zero");
    }
    if results.len() != snippets.len() {
        bail!("evaluation snippets must align with results");
    }
    case.query_views()?;
    let relevance = RelevanceMap::new(&case.relevant);
    if relevance.positive_count() == 0 {
        return Ok(CaseMetrics {
            mrr_at_k: None,
            recall_at_k: None,
            ndcg_at_k: None,
            conversation_success_at_5: None,
            snippet_evidence_coverage: None,
            no_answer_correct: Some(results.is_empty()),
            returned_results: results.len(),
        });
    }
    let relevant_conversations = case
        .relevant
        .iter()
        .filter(|entry| entry.relevance > 0.0)
        .map(|entry| {
            (
                entry.machine.as_str(),
                entry.source,
                entry.session_id.as_str(),
                entry.source_path.as_str(),
            )
        })
        .collect::<HashSet<_>>();
    let mut seen_conversations = HashSet::new();
    let conversation_success = results
        .iter()
        .map(|result| {
            (
                result.machine.as_str(),
                result.record.source,
                result.record.session_id.as_str(),
                result.record.source_path.as_str(),
            )
        })
        .filter(|key| seen_conversations.insert(*key))
        .take(5)
        .any(|key| relevant_conversations.contains(&key));

    let mut total_evidence = 0;
    let mut found_evidence = 0;
    for (key, spans) in &relevance.evidence {
        for span in spans {
            total_evidence += 1;
            if results
                .iter()
                .zip(snippets)
                .take(k)
                .any(|(result, snippet)| key_for_result(result) == *key && snippet.contains(span))
            {
                found_evidence += 1;
            }
        }
    }
    for (key, spans) in &relevance.legacy_evidence {
        for span in spans {
            total_evidence += 1;
            if results
                .iter()
                .zip(snippets)
                .take(k)
                .any(|(result, snippet)| {
                    !relevance.values.contains_key(&key_for_result(result))
                        && LegacyKey::from_result(result) == *key
                        && snippet.contains(span)
                })
            {
                found_evidence += 1;
            }
        }
    }
    Ok(CaseMetrics {
        mrr_at_k: Some(reciprocal_rank(
            &results[..results.len().min(k)],
            &case.relevant,
        )),
        recall_at_k: Some(recall_at_k(results, &case.relevant, k)),
        ndcg_at_k: Some(ndcg_at_k(results, &case.relevant, k)),
        conversation_success_at_5: Some(if conversation_success { 1.0 } else { 0.0 }),
        snippet_evidence_coverage: (total_evidence > 0)
            .then(|| found_evidence as f64 / total_evidence as f64),
        no_answer_correct: None,
        returned_results: results.len(),
    })
}

/// Binary recall of the known relevant set in the first `k` results.
///
/// Invalid direct API qrels are ignored; callers loading a dataset should use
/// [`validate_relevance`] (which [`EvaluationDataset`] does automatically).
pub fn recall_at_k(results: &[LocatedRecord], relevant: &[EvaluationRelevance], k: usize) -> f64 {
    if k == 0 {
        return 0.0;
    }
    let relevance = RelevanceMap::new(relevant);
    let total = relevance.positive_count();
    if total == 0 {
        return 0.0;
    }
    let found = results
        .iter()
        .take(k)
        .filter(|result| {
            relevance
                .relevance_for(result)
                .is_some_and(|value| value > 0.0)
        })
        .map(key_for_result)
        .collect::<HashSet<_>>()
        .len();
    found as f64 / total as f64
}

/// Reciprocal rank of the first relevant result in one ranked list. Invalid direct API qrels are
/// ignored; callers loading a dataset should use [`validate_relevance`].
pub fn reciprocal_rank(results: &[LocatedRecord], relevant: &[EvaluationRelevance]) -> f64 {
    let relevance = RelevanceMap::new(relevant);
    let mut seen = HashSet::new();
    for (index, result) in results.iter().enumerate() {
        let key = key_for_result(result);
        if !seen.insert(key.clone()) {
            continue;
        }
        if relevance
            .relevance_for(result)
            .is_some_and(|value| value > 0.0)
        {
            return 1.0 / (index as f64 + 1.0);
        }
    }
    0.0
}

/// Mean reciprocal rank across evaluation cases.
pub fn mean_reciprocal_rank(
    result_lists: &[Vec<LocatedRecord>],
    cases: &[EvaluationCase],
) -> Result<f64> {
    if result_lists.len() != cases.len() {
        return Err(anyhow!(
            "result list count ({}) does not match evaluation case count ({})",
            result_lists.len(),
            cases.len()
        ));
    }
    if cases.is_empty() {
        return Ok(0.0);
    }
    let mut total = 0.0;
    for (results, case) in result_lists.iter().zip(cases) {
        case.query_views()?;
        total += reciprocal_rank(results, &case.relevant);
    }
    Ok(total / cases.len() as f64)
}

/// Graded nDCG of the first `k` results. Duplicate identities only count at their first rank.
/// Invalid direct API qrels are ignored; callers loading a dataset should use
/// [`validate_relevance`].
pub fn ndcg_at_k(results: &[LocatedRecord], relevant: &[EvaluationRelevance], k: usize) -> f64 {
    if k == 0 {
        return 0.0;
    }
    let relevance = RelevanceMap::new(relevant);
    let mut seen = HashSet::new();
    let dcg = results
        .iter()
        .take(k)
        .enumerate()
        .filter_map(|(index, result)| {
            let key = key_for_result(result);
            if !seen.insert(key) {
                return None;
            }
            relevance
                .relevance_for(result)
                .map(|gain| (2.0_f64.powf(gain as f64) - 1.0) / (index as f64 + 2.0).log2())
        })
        .sum::<f64>();
    let mut ideal = relevance
        .values
        .values()
        .chain(relevance.legacy_values.values())
        .filter(|value| **value > 0.0)
        .map(|value| *value as f64)
        .collect::<Vec<_>>();
    ideal.sort_by(|left, right| right.total_cmp(left));
    let idcg = ideal
        .into_iter()
        .take(k)
        .enumerate()
        .map(|(index, gain)| (2.0_f64.powf(gain) - 1.0) / (index as f64 + 2.0).log2())
        .sum::<f64>();
    if idcg == 0.0 { 0.0 } else { dcg / idcg }
}

/// Number of distinct federated sessions represented in the first `k` hits.
pub fn unique_sessions_at_k(results: &[LocatedRecord], k: usize) -> usize {
    results
        .iter()
        .take(k)
        .map(|result| {
            (
                result.machine.clone(),
                result.record.source,
                result.record.session_id.clone(),
                result.record.source_path.clone(),
            )
        })
        .collect::<HashSet<_>>()
        .len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Record, RecordLinks};
    use tempfile::TempDir;

    fn located(
        machine: &str,
        source: SourceKind,
        session_id: &str,
        source_path: &str,
        doc_id: u64,
        score: f32,
        ts: u64,
    ) -> LocatedRecord {
        LocatedRecord {
            machine: machine.to_string(),
            score,
            record: Record {
                source,
                doc_id,
                ts,
                project: "project".to_string(),
                session_id: session_id.to_string(),
                turn_id: doc_id as u32,
                role: "user".to_string(),
                text: format!("private transcript {doc_id}"),
                tool_name: None,
                tool_input: None,
                tool_output: None,
                links: RecordLinks::default(),
                source_path: source_path.to_string(),
            },
            rollup: None,
        }
    }

    fn relevance(result: &LocatedRecord, value: f32) -> EvaluationRelevance {
        EvaluationRelevance {
            machine: result.machine.clone(),
            source: result.record.source,
            session_id: result.record.session_id.clone(),
            source_path: result.record.source_path.clone(),
            doc_id: result.record.doc_id,
            record_id: None,
            relevance: value,
            evidence: Vec::new(),
        }
    }

    fn evaluation_case(relevant: Vec<EvaluationRelevance>) -> EvaluationCase {
        EvaluationCase {
            id: None,
            query: Some("foo".to_string()),
            queries: Vec::new(),
            cwd: None,
            filters: EvaluationFilters::default(),
            relevant,
        }
    }

    #[test]
    fn no_answer_cases_have_only_abstention_metrics() {
        let case = evaluation_case(Vec::new());
        let empty = evaluate_case(&case, &[], &[], 5).unwrap();
        assert_eq!(empty.no_answer_correct, Some(true));
        assert_eq!(empty.mrr_at_k, None);
        assert_eq!(empty.recall_at_k, None);
        assert_eq!(empty.ndcg_at_k, None);
        assert_eq!(empty.conversation_success_at_5, None);
        assert_eq!(empty.snippet_evidence_coverage, None);
        assert_eq!(empty.returned_results, 0);
        let hit = located("local", SourceKind::Codex, "s", "s.jsonl", 1, 1.0, 1);
        let nonempty = evaluate_case(&case, &[hit], &["unrelated".into()], 5).unwrap();
        assert_eq!(nonempty.no_answer_correct, Some(false));
        assert_eq!(nonempty.returned_results, 1);
        assert!(evaluate_case(&case, &[], &[], 0).is_err());
        assert!(evaluate_case(&case, &[], &["extra".into()], 5).is_err());
    }

    #[test]
    fn case_mrr_obeys_cutoff() {
        let miss = located("local", SourceKind::Codex, "miss", "m.jsonl", 1, 1.0, 1);
        let hit = located("local", SourceKind::Codex, "hit", "h.jsonl", 2, 1.0, 2);
        let case = evaluation_case(vec![relevance(&hit, 1.0)]);
        let results = vec![miss, hit];
        let snippets = vec![String::new(); 2];
        let metrics = evaluate_case(&case, &results, &snippets, 1).unwrap();
        assert_eq!(metrics.mrr_at_k, Some(0.0));
        assert_eq!(metrics.recall_at_k, Some(0.0));
        assert_eq!(metrics.ndcg_at_k, Some(0.0));
        assert_eq!(metrics.snippet_evidence_coverage, None);
        assert_eq!(metrics.no_answer_correct, None);
        let metrics = evaluate_case(&case, &results, &snippets, 2).unwrap();
        assert_eq!(metrics.mrr_at_k, Some(0.5));
        assert_eq!(metrics.recall_at_k, Some(1.0));
    }

    #[test]
    fn conversation_success_deduplicates_before_cutoff_and_accepts_other_records() {
        let relevant = located("local", SourceKind::Codex, "hit", "h.jsonl", 9, 1.0, 9);
        let case = evaluation_case(vec![relevance(&relevant, 1.0)]);
        let mut results = (1..=6)
            .map(|id| located("local", SourceKind::Codex, "miss", "m.jsonl", id, 1.0, id))
            .collect::<Vec<_>>();
        results.push(located(
            "local",
            SourceKind::Codex,
            "hit",
            "h.jsonl",
            8,
            1.0,
            8,
        ));
        let metrics = evaluate_case(&case, &results, &vec![String::new(); 7], 7).unwrap();
        assert_eq!(metrics.conversation_success_at_5, Some(1.0));
        assert_eq!(metrics.mrr_at_k, Some(0.0));

        let mut distinct = (1..=5)
            .map(|id| {
                located(
                    "local",
                    SourceKind::Codex,
                    &format!("s{id}"),
                    "m.jsonl",
                    id,
                    1.0,
                    id,
                )
            })
            .collect::<Vec<_>>();
        distinct.push(relevant);
        let metrics = evaluate_case(&case, &distinct, &vec![String::new(); 6], 6).unwrap();
        assert_eq!(metrics.conversation_success_at_5, Some(0.0));
    }

    #[test]
    fn evidence_coverage_uses_rendered_snippet_and_matching_record() {
        let mut hit = located("local", SourceKind::Codex, "s", "s.jsonl", 1, 1.0, 1);
        hit.record.text = format!("{} late evidence", "unrelated prefix ".repeat(100));
        let other = located("local", SourceKind::Codex, "s", "s.jsonl", 2, 1.0, 2);
        let mut judgment = relevance(&hit, 1.0);
        judgment.evidence = vec!["late evidence".into(), "missing evidence".into()];
        let case = evaluation_case(vec![judgment]);
        let results = vec![other, hit];
        let snippets = vec!["missing evidence".into(), "… late evidence".into()];
        assert_eq!(
            evaluate_case(&case, &results, &snippets, 1)
                .unwrap()
                .snippet_evidence_coverage,
            Some(0.0)
        );
        assert_eq!(
            evaluate_case(&case, &results, &snippets, 2)
                .unwrap()
                .snippet_evidence_coverage,
            Some(0.5)
        );
        let prefixes = vec!["missing evidence".into(), "unrelated prefix".into()];
        assert_eq!(
            evaluate_case(&case, &results, &prefixes, 2)
                .unwrap()
                .snippet_evidence_coverage,
            Some(0.0)
        );
    }

    #[test]
    fn evidence_deduplicates_stable_legacy_aliases_and_survives_doc_id_changes() {
        let mut hit = located("local", SourceKind::Codex, "s", "s.jsonl", 1, 1.0, 1);
        hit.record.links.event_id = Some("stable-id".into());
        let mut legacy = relevance(&hit, 1.0);
        legacy.evidence = vec!["found".into(), "found".into()];
        let mut stable = legacy.clone();
        stable.record_id = Some(canonical_record_id(&hit.record));
        stable.evidence.push("missing".into());
        let case = evaluation_case(vec![legacy, stable]);
        hit.record.doc_id = 99;
        let metrics = evaluate_case(&case, &[hit], &["found".into()], 1).unwrap();
        assert_eq!(metrics.recall_at_k, Some(1.0));
        assert_eq!(metrics.snippet_evidence_coverage, Some(0.5));
    }

    #[test]
    fn fusion_deduplicates_and_uses_rrf_with_deterministic_ties() {
        let first = located("local", SourceKind::Codex, "a", "a.jsonl", 1, 0.1, 10);
        let second = located("local", SourceKind::Codex, "b", "b.jsonl", 2, 0.9, 20);
        let duplicate = located("local", SourceKind::Codex, "a", "a.jsonl", 1, 0.8, 30);
        let fused = fuse_ranked_queries(
            vec![vec![first.clone(), second.clone()], vec![duplicate, first]],
            1.0,
        );
        assert_eq!(fused.len(), 2);
        assert_eq!(fused[0].record.doc_id, 1);
        assert!((fused[0].score - (1.0 / 2.0 + 1.0 / 2.0 + 1.0 / 3.0)).abs() < 1e-6);
        assert_eq!(fused[1].record.doc_id, 2);
        assert_eq!(fused[0].record.text, "private transcript 1");
    }

    #[test]
    fn fusion_identity_includes_machine_source_and_path() {
        let local = located("local", SourceKind::Codex, "same", "a.jsonl", 1, 1.0, 1);
        let remote = located("mini", SourceKind::Codex, "same", "a.jsonl", 1, 1.0, 1);
        let other_source = located("local", SourceKind::Claude, "same", "a.jsonl", 1, 1.0, 1);
        let other_path = located("local", SourceKind::Codex, "same", "b.jsonl", 1, 1.0, 1);
        assert_eq!(
            fuse_ranked_queries(vec![vec![local, remote, other_source, other_path]], 60.0).len(),
            4
        );
    }

    #[test]
    fn fusion_prefers_finite_representative_scores() {
        let mut non_finite = located("local", SourceKind::Codex, "s", "s.jsonl", 1, f32::NAN, 1);
        non_finite.record.text = "nan".to_string();
        let mut finite = non_finite.clone();
        finite.score = 0.5;
        finite.record.text = "finite".to_string();
        let fused = fuse_ranked_queries(vec![vec![non_finite], vec![finite]], 60.0);
        assert_eq!(fused.len(), 1);
        assert_eq!(fused[0].record.text, "finite");
    }

    #[test]
    fn trace_writer_is_jsonl_and_excludes_transcript_text() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
        let result = located("local", SourceKind::Codex, "s", "session.jsonl", 7, 0.5, 9);
        let trace = RetrievalTrace::from_results(
            RetrievalTraceMetadata {
                trace_id: "trace-1".to_string(),
                started_at_ms: 10,
                elapsed_ms: Some(3),
                mode: Some("hybrid".to_string()),
                queries: vec![TraceQuery {
                    query_index: 0,
                    query: "query".to_string(),
                    candidate_count: 1,
                }],
                cwd: None,
                machines: vec!["local".to_string()],
                candidate_count: 1,
                failures: Vec::new(),
            },
            &[result],
        );
        append_trace(&paths, &trace).unwrap();
        let contents = fs::read_to_string(trace_path(&paths)).unwrap();
        assert_eq!(contents.lines().count(), 1);
        assert!(!contents.contains("private transcript"));
        let round_trip: RetrievalTrace = serde_json::from_str(contents.trim()).unwrap();
        assert_eq!(round_trip, trace);
    }

    #[test]
    fn evaluation_dataset_validates_queries_and_duplicate_ids() {
        let valid = r#"{"id":"one","query":"foo","relevant":[{"machine":"local","source":"codex","session_id":"s","source_path":"s.jsonl","doc_id":1}]}"#;
        let dataset = EvaluationDataset::from_jsonl(valid).unwrap();
        assert_eq!(dataset.cases[0].query_views().unwrap(), vec!["foo"]);
        assert_eq!(dataset.cases[0].filters, EvaluationFilters::default());
        assert!(dataset.cases[0].relevant[0].evidence.is_empty());
        let no_answer =
            r#"{"query":"unknown","relevant":[],"filters":{"source":"codex","role":"user"}}"#;
        let dataset = EvaluationDataset::from_jsonl(no_answer).unwrap();
        assert!(dataset.cases[0].relevant.is_empty());
        assert_eq!(dataset.cases[0].filters.source, Some(SourceFilter::Codex));
        assert_eq!(dataset.cases[0].filters.role.as_deref(), Some("user"));
        let duplicate = format!("{valid}\n{valid}");
        assert!(EvaluationDataset::from_jsonl(&duplicate).is_err());
        let empty_query = r#"{"query":" ","relevant":[{"machine":"local","source":"codex","session_id":"s","source_path":"s.jsonl","doc_id":1}]}"#;
        assert!(EvaluationDataset::from_jsonl(empty_query).is_err());
    }

    #[test]
    fn metrics_are_deterministic_and_deduplicate_hits() {
        let first = located("local", SourceKind::Codex, "s", "s.jsonl", 1, 1.0, 1);
        let second = located("local", SourceKind::Codex, "s", "s.jsonl", 2, 1.0, 2);
        let other_session = located("local", SourceKind::Codex, "other", "o.jsonl", 3, 1.0, 3);
        let relevant = vec![relevance(&first, 1.0), relevance(&second, 2.0)];
        let results = vec![other_session, first.clone(), first.clone(), second.clone()];
        assert!((recall_at_k(&results, &relevant, 3) - 0.5).abs() < 1e-9);
        assert!((reciprocal_rank(&results, &relevant) - 0.5).abs() < 1e-9);
        assert!(ndcg_at_k(&results, &relevant, 4) > 0.0);
        assert_eq!(unique_sessions_at_k(&results, 3), 2);
    }

    #[test]
    fn mixed_stable_and_legacy_qrels_are_one_relevant_item() {
        let first = located("local", SourceKind::Codex, "s", "s.jsonl", 1, 1.0, 1);
        let stable = EvaluationRelevance {
            machine: first.machine.clone(),
            source: first.record.source,
            session_id: first.record.session_id.clone(),
            source_path: first.record.source_path.clone(),
            doc_id: first.record.doc_id,
            record_id: Some(canonical_record_id(&first.record)),
            relevance: 1.0,
            evidence: Vec::new(),
        };
        let legacy = relevance(&first, 2.0);
        assert_eq!(recall_at_k(&[first], &[stable, legacy], 1), 1.0);
    }

    #[test]
    fn invalid_direct_metric_grades_do_not_produce_nan() {
        let first = located("local", SourceKind::Codex, "s", "s.jsonl", 1, 1.0, 1);
        let invalid = relevance(&first, f32::INFINITY);
        assert_eq!(
            recall_at_k(
                std::slice::from_ref(&first),
                std::slice::from_ref(&invalid),
                1
            ),
            0.0
        );
        assert_eq!(ndcg_at_k(&[first], &[invalid], 1), 0.0);
    }

    #[test]
    fn mean_reciprocal_rank_rejects_mismatched_inputs() {
        let case = EvaluationCase {
            id: None,
            query: Some("foo".to_string()),
            queries: Vec::new(),
            cwd: None,
            filters: EvaluationFilters::default(),
            relevant: vec![EvaluationRelevance {
                machine: "local".to_string(),
                source: SourceKind::Codex,
                session_id: "s".to_string(),
                source_path: "s.jsonl".to_string(),
                doc_id: 1,
                record_id: None,
                relevance: 1.0,
                evidence: Vec::new(),
            }],
        };
        assert!(mean_reciprocal_rank(&[], &[case]).is_err());
    }
}
