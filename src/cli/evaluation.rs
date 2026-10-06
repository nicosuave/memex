//! Offline evaluation of the same ranked lists and snippets exposed by search.

use super::surface::{CliSearchMode, EvaluationArgs, EvaluationSurface};
use super::{
    SearchCollectRequest, SearchFormat, SortBy, collect_search_with_auto_index,
    project_located_results,
};
use crate::analytics::{SnapshotRecord, analytics_path, rebuild_from_snapshot};
use crate::config::{Paths, UserConfig};
use crate::index::SearchIndex;
use crate::machine::SearchMode;
use crate::retrieval::canonical_record_id;
use crate::retrieval_eval::{
    EvaluationCase, EvaluationDataset, EvaluationResults, TraceHit, evaluate_case,
};
use crate::vector::VectorIndex;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs;
use std::time::Instant;

pub(super) fn run(args: EvaluationArgs) -> Result<()> {
    validate_args(&args)?;
    let dataset_bytes = fs::read(&args.dataset).context("read evaluation dataset")?;
    let dataset = EvaluationDataset::from_jsonl(std::str::from_utf8(&dataset_bytes)?)?;
    for case in &dataset.cases {
        validate_surface_case(&args, case)?;
        if args.records.is_some() && case.cwd.is_some() {
            bail!(
                "--cwd judgments require an existing root's analytics scope; --records cannot replay checkout metadata"
            );
        }
    }
    let temporary = args
        .records
        .as_ref()
        .map(|_| tempfile::tempdir())
        .transpose()?;
    let paths = Paths::new(
        temporary
            .as_ref()
            .map(|dir| dir.path().to_path_buf())
            .or_else(|| args.root.clone()),
    )?;
    let records_hash = if let Some(records) = &args.records {
        let bytes = fs::read(records).context("read evaluation records")?;
        seed_index(&paths, &bytes, &dataset)?;
        Some(digest(&bytes))
    } else {
        None
    };
    if !SearchIndex::exists(&paths.index) {
        bail!("evaluation requires an existing index or --records; it never auto-indexes");
    }
    let machines = if args.surface == EvaluationSurface::Tui {
        crate::machine::selected_machine_ids(&UserConfig::load(&paths)?, &[])?
    } else {
        vec!["local".to_string()]
    };
    if args.baseline.is_some() && machines != ["local"] {
        bail!(
            "federated TUI baseline comparisons require remote snapshot identities and are unsupported"
        );
    }
    // Production search falls back to lexical when vectors are absent. Such a run must not
    // be labelled a semantic/hybrid comparison.
    let vector_revision = if args.mode != CliSearchMode::Lexical {
        Some(
            VectorIndex::snapshot_revision(&paths.vectors)?
                .context("semantic/hybrid evaluation requires a prepared vector index")?,
        )
    } else {
        None
    };
    let vector_model = if vector_revision.is_some() {
        let vectors = VectorIndex::open(&paths.vectors)
            .context("semantic/hybrid evaluation requires a prepared vector index")?;
        if vectors.doc_id_count() == 0 {
            bail!("semantic/hybrid evaluation requires nonempty vectors");
        }
        let config = UserConfig::load(&paths)?;
        let model =
            crate::machine::resolve_vector_query_model(&vectors, || config.resolve_model(None))?
                .ok_or_else(|| anyhow::anyhow!("vector query model unavailable"))?;
        Some(model.identity().to_string())
    } else {
        None
    };

    let live_revision = if args.records.is_none() {
        Some(index_revision(&paths)?)
    } else {
        None
    };
    let mut cases = Vec::with_capacity(dataset.cases.len());
    for (number, case) in dataset.cases.iter().enumerate() {
        verify_vector_revision(&paths, vector_revision.as_deref())?;
        let started = Instant::now();
        let results = search(&paths, &args, case).with_context(|| {
            format!("evaluate case {}", case.id.as_deref().unwrap_or("unnamed"))
        })?;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        verify_vector_revision(&paths, vector_revision.as_deref())?;
        let metrics = evaluate_case(case, &results.hits, &results.snippets, args.k)?;
        let recall_at_20 = evaluate_case(case, &results.hits, &results.snippets, 20)?.recall_at_k;
        let hits = results
            .hits
            .iter()
            .zip(&results.snippets)
            .enumerate()
            .map(|(rank, (hit, snippet))| {
                let mut value = serde_json::to_value(TraceHit::from_result(rank + 1, hit))?;
                value["snippet"] = json!(snippet);
                Ok(value)
            })
            .collect::<Result<Vec<Value>>>()?;
        cases.push(json!({
            "id": case.id.clone().unwrap_or_else(|| format!("case-{}", number + 1)),
            "queries": case.query_views()?, "cwd": case.cwd, "filters": case.filters,
            "metrics": metrics, "recall_at_20": recall_at_20,
            "elapsed_ms": elapsed_ms, "hits": hits,
        }));
    }
    let index_changed_during_run = if let Some(revision) = &live_revision {
        *revision != index_revision(&paths)?
    } else {
        false
    };
    verify_vector_revision(&paths, vector_revision.as_deref())?;
    let mut configuration = json!({
        "surface": args.surface, "mode": format!("{:?}", args.mode).to_lowercase(),
        "k": args.k, "limit": args.limit, "origin": args.origin,
        "unique_session": args.unique_session, "recency_weight": args.recency_weight,
        "recency_half_life_days": args.recency_half_life_days, "vector_model": vector_model,
        "dataset_sha256": digest(&dataset_bytes), "records_sha256": records_hash,
        "live_index_revision": live_revision,
        "machines": machines,
    });
    // Keep lexical baseline configuration compatible: it does not search vectors.
    if let Some(revision) = vector_revision {
        configuration["vector_index_revision"] = json!(revision);
    }
    let mut report = json!({
        "schema_version": 1, "configuration": configuration, "cases": cases.len(), "k": args.k,
        "summary": summarize(&cases), "per_case": cases,
        "index_changed_during_run": index_changed_during_run,
    });
    let regressions = if let Some(path) = &args.baseline {
        let baseline: Value =
            serde_json::from_slice(&fs::read(path)?).context("parse evaluation baseline")?;
        compare_baseline(&report, &baseline)?
    } else {
        Vec::new()
    };
    report["regressions"] = json!(regressions);
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !regressions.is_empty() {
        bail!(
            "{} per-query quality metrics regressed against baseline",
            regressions.len()
        );
    }
    Ok(())
}

fn validate_args(args: &EvaluationArgs) -> Result<()> {
    if args.k == 0 || args.limit < args.k.max(20) {
        bail!("--k must be positive and --limit must be at least max(k, 20)");
    }
    if !args.recency_weight.is_finite()
        || args.recency_weight < 0.0
        || !args.recency_half_life_days.is_finite()
        || args.recency_half_life_days <= 0.0
    {
        bail!("recency weight must be finite/non-negative and half-life finite/positive");
    }
    if args.surface != EvaluationSurface::Cli
        && (args.mode != CliSearchMode::Lexical
            || args.unique_session
            || args.recency_weight != 0.0)
    {
        bail!(
            "TUI/web evaluation uses its own lexical ranking/grouping; CLI tuning is unsupported"
        );
    }
    if args.surface == EvaluationSurface::Web && args.limit > 100 {
        bail!("web evaluation limit cannot exceed the API's 100-result page");
    }
    Ok(())
}

fn validate_surface_case(args: &EvaluationArgs, case: &EvaluationCase) -> Result<()> {
    let filters = &case.filters;
    if args.surface != EvaluationSurface::Cli
        && (case.query_views()?.len() != 1
            || case.cwd.is_some()
            || filters.role.is_some()
            || filters.session.is_some()
            || filters.since.is_some()
            || filters.until.is_some())
    {
        bail!("TUI/web cases support a single query and project/source filters only");
    }
    Ok(())
}

fn search(
    paths: &Paths,
    args: &EvaluationArgs,
    case: &EvaluationCase,
) -> Result<EvaluationResults> {
    let queries = case.query_views()?;
    let filters = &case.filters;
    if args.surface == EvaluationSurface::Tui {
        return crate::tui::evaluate_search(
            paths,
            &queries[0],
            filters.project.as_deref(),
            filters.source,
            args.origin.into(),
            args.limit,
        );
    }
    if args.surface == EvaluationSurface::Web {
        return crate::web::evaluate_search(
            paths,
            &queries[0],
            filters.project.as_deref(),
            filters.source,
            args.origin.into(),
            args.limit,
        );
    }
    let collection = collect_search_with_auto_index(
        SearchCollectRequest {
            query: queries[0].clone(),
            additional_queries: queries[1..].to_vec(),
            cwd: case.cwd.as_ref().map(Into::into),
            project: filters.project.clone(),
            role: filters.role.clone(),
            tool: None,
            session: filters.session.clone(),
            source: filters.source,
            origin: args.origin,
            mode: match args.mode {
                CliSearchMode::Lexical => SearchMode::Lexical,
                CliSearchMode::Semantic => SearchMode::Semantic,
                CliSearchMode::Hybrid => SearchMode::Hybrid,
            },
            min_score: None,
            recency_weight: args.recency_weight,
            recency_half_life_days: args.recency_half_life_days,
            since: filters.since.clone(),
            until: filters.until.clone(),
            limit: args.limit,
            top_n_per_session: None,
            unique_session: args.unique_session,
            // Match compact CLI output, including its remote text budget before snippet selection.
            fields: Some(HashSet::from(["snippet".into()])),
            sort: SortBy::Score,
            verbose: false,
            format: SearchFormat::Json,
            root: Some(paths.root.clone()),
            machines: vec!["local".into()],
            rerank: Some(false),
        },
        false,
    )?;
    if !collection.failures.is_empty() {
        bail!(
            "evaluation search failed: {}",
            collection.failures.join("; ")
        );
    }
    let snippets = project_located_results(collection.results.clone(), &collection.render)?
        .into_iter()
        .map(|value| value["snippet"].as_str().unwrap_or_default().to_string())
        .collect();
    Ok(EvaluationResults {
        hits: collection.results,
        snippets,
    })
}

fn seed_index(paths: &Paths, bytes: &[u8], dataset: &EvaluationDataset) -> Result<()> {
    let mut records = Vec::new();
    let mut ids = HashSet::new();
    let mut canonical_ids = HashSet::new();
    for (line, text) in std::str::from_utf8(bytes)?.lines().enumerate() {
        if text.trim().is_empty() {
            continue;
        }
        let snapshot: SnapshotRecord = serde_json::from_str(text)
            .with_context(|| format!("parse corpus record line {}", line + 1))?;
        let record = &snapshot.record;
        if !ids.insert(record.doc_id) || !canonical_ids.insert(canonical_record_id(record)) {
            bail!("corpus contains duplicate document/canonical record identities");
        }
        records.push(snapshot);
    }
    if records.is_empty() {
        bail!("record corpus is empty");
    }
    // Missing judged documents would make a broken fixture look like poor retrieval.
    for case in &dataset.cases {
        for judgment in &case.relevant {
            let record = records
                .iter()
                .find(|record| {
                    let record = &record.record;
                    judgment.machine == "local"
                        && judgment.source == record.source
                        && judgment.session_id == record.session_id
                        && judgment.source_path == record.source_path
                        && judgment
                            .record_id
                            .as_ref()
                            .map_or(record.doc_id == judgment.doc_id, |id| {
                                *id == canonical_record_id(record)
                            })
                })
                .with_context(|| format!("judged record missing from corpus for {:?}", case.id))?;
            if judgment.relevance > 0.0
                && judgment
                    .evidence
                    .iter()
                    .any(|span| !record.record.text.contains(span))
            {
                bail!(
                    "judged evidence is absent from corpus record for {:?}",
                    case.id
                );
            }
        }
    }
    paths.ensure_dirs()?;
    fs::write(
        paths.root.join("config.toml"),
        "auto_index_on_search = false\n",
    )?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    let mut writer = index.writer()?;
    for record in &records {
        index.add_record(&mut writer, &record.record)?;
    }
    writer.commit()?;
    writer.wait_merging_threads()?;
    rebuild_from_snapshot(analytics_path(&paths.state), records)?;
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn index_revision(paths: &Paths) -> Result<Value> {
    let revision = SearchIndex::open_or_create(&paths.index)?.revision()?;
    Ok(json!({"opstamp": revision.opstamp, "segments": revision.segments}))
}

fn verify_vector_revision(paths: &Paths, expected: Option<&str>) -> Result<()> {
    if let Some(expected) = expected {
        let current = VectorIndex::snapshot_revision(&paths.vectors)
            .context("verify evaluation vector snapshot")?;
        if current.as_deref() != Some(expected) {
            bail!("vector index changed during evaluation; semantic/hybrid results are invalid");
        }
    }
    Ok(())
}

fn mean(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    let values = values.flatten().collect::<Vec<_>>();
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn summarize(cases: &[Value]) -> Value {
    let mut summary = json!({});
    for metric in QUALITY_METRICS {
        summary[metric] = json!(mean(
            cases.iter().map(|case| case["metrics"][metric].as_f64())
        ));
    }
    summary["recall_at_20"] = json!(mean(cases.iter().map(|case| case["recall_at_20"].as_f64())));
    let no_answer = cases
        .iter()
        .filter_map(|case| case["metrics"]["no_answer_correct"].as_bool())
        .collect::<Vec<_>>();
    summary["no_answer_cases"] = json!(no_answer.len());
    summary["no_answer_accuracy"] = json!(
        (!no_answer.is_empty()).then(
            || no_answer.iter().filter(|value| **value).count() as f64 / no_answer.len() as f64
        )
    );
    summary["mean_elapsed_ms"] = json!(mean(cases.iter().map(|case| case["elapsed_ms"].as_f64())));
    summary
}

const QUALITY_METRICS: [&str; 5] = [
    "mrr_at_k",
    "recall_at_k",
    "ndcg_at_k",
    "conversation_success_at_5",
    "snippet_evidence_coverage",
];

fn compare_baseline(report: &Value, baseline: &Value) -> Result<Vec<Value>> {
    if report["index_changed_during_run"] != false || baseline["index_changed_during_run"] != false
    {
        bail!("baseline comparisons require a stable index throughout both runs");
    }
    if report["schema_version"] != baseline["schema_version"]
        || report["configuration"] != baseline["configuration"]
    {
        bail!("baseline schema, dataset/corpus hashes and search configuration must match");
    }
    let current = report["per_case"]
        .as_array()
        .context("missing per-case report")?;
    let previous = baseline["per_case"]
        .as_array()
        .context("missing per-case baseline")?;
    if current.len() != previous.len() {
        bail!("baseline case count must match");
    }
    let mut regressions = Vec::new();
    for (case, old) in current.iter().zip(previous) {
        if case["id"] != old["id"] {
            bail!("baseline case identities must match");
        }
        for metric in QUALITY_METRICS {
            if let Some(expected) = old["metrics"][metric].as_f64() {
                let actual = case["metrics"][metric]
                    .as_f64()
                    .context("missing baseline metric")?;
                if actual + 1e-9 < expected {
                    regressions.push(json!({"id":case["id"], "metric":metric, "baseline":expected, "actual":actual}));
                }
            }
        }
        if let Some(expected) = old["recall_at_20"].as_f64()
            && case["recall_at_20"].as_f64().context("missing recall@20")? + 1e-9 < expected
        {
            regressions.push(json!({"id":case["id"], "metric":"recall_at_20", "baseline":expected, "actual":case["recall_at_20"]}));
        }
        if old["metrics"]["no_answer_correct"] == true
            && case["metrics"]["no_answer_correct"] != true
        {
            regressions.push(json!({"id":case["id"], "metric":"no_answer_correct", "baseline":true, "actual":false}));
        }
    }
    Ok(regressions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_evaluation_rejects_same_model_replacement_and_deletion() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temporary.path().to_path_buf())).unwrap();
        let mut vectors = VectorIndex::open_or_create(&paths.vectors, 2, Some("fixture")).unwrap();
        vectors.add(1, &[1.0, 0.0]).unwrap();
        vectors.save().unwrap();
        let initial = VectorIndex::snapshot_revision(&paths.vectors)
            .unwrap()
            .unwrap();
        verify_vector_revision(&paths, Some(&initial)).unwrap();

        // Same model and unchanged lexical corpus, but a different searched vector snapshot.
        vectors.add(2, &[0.0, 1.0]).unwrap();
        vectors.save().unwrap();
        assert!(verify_vector_revision(&paths, Some(&initial)).is_err());
        let replacement = VectorIndex::snapshot_revision(&paths.vectors)
            .unwrap()
            .unwrap();
        verify_vector_revision(&paths, Some(&replacement)).unwrap();
        VectorIndex::reset(&paths.vectors).unwrap();
        assert!(verify_vector_revision(&paths, Some(&replacement)).is_err());
        // Lexical evaluations remain independent of vector publication/deletion.
        verify_vector_revision(&paths, None).unwrap();
    }

    #[test]
    fn vector_baselines_require_the_same_snapshot() {
        for mode in ["semantic", "hybrid"] {
            let baseline = json!({
                "schema_version": 1,
                "index_changed_during_run": false,
                "configuration": {
                    "mode": mode, "vector_model": "fixture",
                    "vector_index_revision": "generation-original",
                    "live_index_revision": {"opstamp": 1, "segments": []},
                },
                "per_case": [],
            });
            assert!(compare_baseline(&baseline, &baseline).unwrap().is_empty());
            let mut replacement = baseline.clone();
            replacement["configuration"]["vector_index_revision"] = json!("generation-rebuilt");
            assert!(compare_baseline(&replacement, &baseline).is_err());
            let mut old_report = baseline.clone();
            old_report["configuration"]
                .as_object_mut()
                .unwrap()
                .remove("vector_index_revision");
            assert!(compare_baseline(&baseline, &old_report).is_err());
        }
    }
}
