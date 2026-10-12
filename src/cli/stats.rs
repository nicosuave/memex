use super::surface::{IndexSource, OutputFormat, OutputOptions};
use crate::config::{Paths, UserConfig};
use crate::index::SearchIndex;
use crate::memory::{MemoryFreshness, MemoryStore};
use crate::rerank::RerankEngine;
use crate::state::ScanCache;
use crate::vector::VectorIndex;
use crate::vector_backfill::{self, BackfillStatus};
use anyhow::Result;
use clap::ValueEnum;
use serde::Serialize;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize)]
struct StatsReport {
    index: String,
    documents: usize,
    vectors: VectorStats,
    vector_backfill: Option<BackfillStatus>,
    memory: MemoryStats,
    auto_index_on_search: bool,
    scan_cache_ttl_seconds: u64,
    scan_cache: ScanCacheStats,
    embeddings: bool,
    embeddings_mode: &'static str,
    model: String,
    embedding_dimensions: Option<usize>,
    embedding_batch_size: usize,
    execution_provider: String,
    compute_units: Option<String>,
    rerank: &'static str,
    rerank_model: Option<String>,
    /// Host of the remote rerank URL; never the path, query, or key.
    rerank_host: Option<String>,
    /// Whether remote requests carry `rerank_extra_body`; never its content.
    rerank_extra_body: Option<bool>,
    rerank_candidates: Option<usize>,
    rerank_doc_chars: Option<usize>,
    /// Why the reranking settings are invalid; the rest of the report still renders.
    rerank_error: Option<String>,
    sources: Vec<String>,
}

#[derive(Default, Serialize)]
struct VectorStats {
    exists: bool,
    count: usize,
    dimensions: Option<usize>,
    model: Option<String>,
    doc_ids: Option<usize>,
    index_bytes: Option<u64>,
    ids_bytes: Option<u64>,
}

#[derive(Serialize)]
struct MemoryStats {
    documents: usize,
    sections: usize,
    stale_documents: usize,
}

#[derive(Serialize)]
struct ScanCacheStats {
    last_scan_ts: Option<u64>,
    last_scan: Option<String>,
    age_seconds: Option<u64>,
    fresh: bool,
    file_count: usize,
    total_bytes: u64,
}

pub(super) fn run(root: Option<PathBuf>, output: OutputOptions) -> Result<()> {
    let paths = Paths::new(root)?;
    let report = build_report(&paths)?;
    if output.format == OutputFormat::Text {
        print_report(&report, &mut io::stdout().lock())?;
    } else {
        output.print_value(&serde_json::to_value(&report)?)?;
    }
    Ok(())
}

fn build_report(paths: &Paths) -> Result<StatsReport> {
    let config = UserConfig::load(paths)?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    let memory = MemoryStore::new(paths.root.join("memory/documents.json")).load()?;
    let runtime = config.resolve_embed_runtime()?;
    let scan_cache_ttl_seconds = config.scan_cache_ttl();
    let (rerank, rerank_error) = match config.resolve_rerank() {
        Ok(rerank) => (rerank, None),
        Err(error) => (None, Some(format!("{error:#}"))),
    };
    Ok(StatsReport {
        index: paths.index.display().to_string(),
        documents: index.doc_count()?,
        vectors: read_vector_stats(&paths.vectors)?,
        vector_backfill: vector_backfill::status(paths)?,
        memory: MemoryStats {
            documents: memory.documents.len(),
            sections: memory
                .documents
                .iter()
                .map(|document| document.sections.len())
                .sum(),
            stale_documents: memory
                .documents
                .iter()
                .filter(|document| matches!(document.freshness, MemoryFreshness::Stale { .. }))
                .count(),
        },
        auto_index_on_search: config.auto_index_on_search_default(),
        scan_cache_ttl_seconds,
        scan_cache: read_scan_cache_stats(paths, scan_cache_ttl_seconds)?,
        embeddings: config.embeddings_default(),
        embeddings_mode: config.embeddings_mode().as_str(),
        model: config.resolve_model(None)?.identity().into_owned(),
        embedding_dimensions: runtime.remote.as_ref().and_then(|remote| remote.dimensions),
        embedding_batch_size: runtime.batch_size(),
        execution_provider: runtime.execution_provider.as_str().to_string(),
        compute_units: runtime.compute_units,
        rerank: config.rerank_mode().as_str(),
        rerank_model: rerank.as_ref().and_then(|rerank| match &rerank.engine {
            RerankEngine::Local { model, .. } => Some(crate::rerank::model_name(model).to_string()),
            RerankEngine::Remote(remote) => remote.model.clone(),
        }),
        rerank_host: rerank.as_ref().and_then(|rerank| match &rerank.engine {
            RerankEngine::Local { .. } => None,
            RerankEngine::Remote(remote) => url::Url::parse(&remote.endpoint.base_url)
                .ok()
                .and_then(|url| url.host_str().map(str::to_string)),
        }),
        rerank_extra_body: rerank.as_ref().and_then(|rerank| match &rerank.engine {
            RerankEngine::Local { .. } => None,
            RerankEngine::Remote(remote) => Some(remote.extra_body.is_some()),
        }),
        rerank_candidates: rerank.as_ref().map(|rerank| rerank.candidates),
        rerank_doc_chars: rerank.as_ref().map(|rerank| rerank.doc_chars),
        rerank_error,
        // IndexSource owns the providers with local transcript discovery; usage-only
        // providers must not be presented as supported transcript sources.
        sources: IndexSource::value_variants()
            .iter()
            .map(|source| {
                source
                    .to_possible_value()
                    .expect("index source has a CLI name")
                    .get_name()
                    .to_string()
            })
            .collect(),
    })
}

fn print_report(report: &StatsReport, out: &mut impl Write) -> Result<()> {
    writeln!(out, "index: {}", report.index)?;
    writeln!(out, "documents: {}", report.documents)?;
    if let Some(status) = &report.vector_backfill {
        writeln!(out, "{}", status.line())?;
    }
    writeln!(out, "memory documents: {}", report.memory.documents)?;
    writeln!(out, "memory sections: {}", report.memory.sections)?;
    writeln!(
        out,
        "stale memory documents: {}",
        report.memory.stale_documents
    )?;
    writeln!(out, "{}", vector_stats_line(&report.vectors))?;
    writeln!(out, "\nindexing:")?;
    writeln!(
        out,
        "  auto-index-on-search: {}",
        enabled_label(report.auto_index_on_search)
    )?;
    writeln!(out, "  scan-cache-ttl: {}s", report.scan_cache_ttl_seconds)?;
    let last_scan = report.scan_cache.last_scan.as_deref().unwrap_or("never");
    let freshness = if report.scan_cache.fresh {
        "fresh"
    } else {
        "stale"
    };
    writeln!(out, "  last-scan: {last_scan} ({freshness})")?;
    writeln!(out, "  scanned-files: {}", report.scan_cache.file_count)?;
    writeln!(out, "  scanned-bytes: {}", report.scan_cache.total_bytes)?;
    writeln!(out, "\nembeddings:")?;
    writeln!(out, "  enabled: {}", enabled_label(report.embeddings))?;
    writeln!(out, "  mode: {}", report.embeddings_mode)?;
    writeln!(out, "  model: {}", report.model)?;
    if let Some(dimensions) = report.embedding_dimensions {
        writeln!(out, "  dimensions: {dimensions}")?;
    }
    writeln!(out, "  batch-size: {}", report.embedding_batch_size)?;
    if report.embeddings_mode != "remote" {
        writeln!(out, "  execution-provider: {}", report.execution_provider)?;
        if let Some(compute_units) = &report.compute_units {
            writeln!(out, "  compute-units: {compute_units}")?;
        }
    }
    writeln!(out, "\nreranking:")?;
    writeln!(out, "  mode: {}", report.rerank)?;
    if let Some(model) = &report.rerank_model {
        writeln!(out, "  model: {model}")?;
    }
    if let Some(host) = &report.rerank_host {
        writeln!(out, "  host: {host}")?;
    }
    if let Some(set) = report.rerank_extra_body {
        writeln!(out, "  extra-body: {}", if set { "set" } else { "unset" })?;
    }
    if let Some(candidates) = report.rerank_candidates {
        writeln!(out, "  candidates: {candidates}")?;
    }
    if let Some(doc_chars) = report.rerank_doc_chars {
        writeln!(out, "  doc-chars: {doc_chars}")?;
    }
    if let Some(error) = &report.rerank_error {
        writeln!(out, "  error: {error}")?;
    }
    writeln!(out, "\nsources:")?;
    for source in &report.sources {
        writeln!(out, "  - {source}")?;
    }
    Ok(())
}

fn read_vector_stats(vectors_dir: &Path) -> Result<VectorStats> {
    let Some(inventory) = VectorIndex::inventory(vectors_dir)? else {
        return Ok(VectorStats::default());
    };
    Ok(VectorStats {
        exists: true,
        count: inventory.vector_count,
        dimensions: Some(inventory.dimensions),
        model: inventory.model,
        doc_ids: Some(inventory.doc_ids.len()),
        index_bytes: Some(inventory.index_bytes),
        ids_bytes: Some(inventory.ids_bytes),
    })
}

fn vector_stats_line(stats: &VectorStats) -> String {
    if !stats.exists {
        return "vectors: none".to_string();
    }
    format!(
        "vectors: {} (dims {}, model {}, ids {}, usearch.index {}, doc_ids.bin {})",
        stats.count,
        stats.dimensions.unwrap_or(0),
        stats.model.as_deref().unwrap_or("unknown"),
        stats.doc_ids.unwrap_or(0),
        stats.index_bytes.unwrap_or(0),
        stats.ids_bytes.unwrap_or(0)
    )
}

fn read_scan_cache_stats(paths: &Paths, ttl_seconds: u64) -> Result<ScanCacheStats> {
    // ScanCache::load resolves both legacy JSON and current SQLite checkpoints.
    let cache = ScanCache::load(&paths.state.join("scan_cache.json"))?;
    let last_scan_ts = (cache.last_scan_ts != 0).then_some(cache.last_scan_ts);
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    Ok(ScanCacheStats {
        last_scan_ts,
        last_scan: last_scan_ts.map(|ts| super::format_ts(ts.saturating_mul(1000))),
        age_seconds: last_scan_ts.map(|ts| now.saturating_sub(ts)),
        fresh: last_scan_ts.is_some() && cache.is_fresh(ttl_seconds),
        file_count: cache.file_count,
        total_bytes: cache.total_bytes,
    })
}

fn enabled_label(value: bool) -> &'static str {
    if value { "enabled" } else { "disabled" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::{INGEST_LEASE_TIMEOUT, IngestLease};
    use crate::state::checkpoint::{CheckpointDelta, CheckpointWriter};
    use tempfile::TempDir;

    #[test]
    fn vector_stats_report_current_generation_in_text_and_json() {
        let tmp = TempDir::new().unwrap();
        let mut index = VectorIndex::open_or_create(tmp.path(), 64, Some("bge")).unwrap();
        index
            .add(42, &(0..64).map(|i| (i as f32).sin()).collect::<Vec<_>>())
            .unwrap();
        index.save().unwrap();

        let stats = read_vector_stats(tmp.path()).unwrap();
        let line = vector_stats_line(&stats);
        assert!(line.starts_with("vectors: 1 (dims 64, model bge, ids 1,"));
        assert!(line.contains("usearch.index"));
        assert!(line.contains("doc_ids.bin"));
        assert!(!line.contains("vectors.f32"));
        assert!(!line.contains("doc_ids.u64"));
        let json = serde_json::to_value(stats).unwrap();
        assert_eq!(json["exists"], true);
        assert_eq!(json["count"], 1);
        assert_eq!(json["dimensions"], 64);
        assert_eq!(json["doc_ids"], 1);
        assert!(json["index_bytes"].as_u64().unwrap() > 0);
        assert!(json["ids_bytes"].as_u64().unwrap() > 0);
    }

    #[test]
    fn reranking_settings_are_reported_in_text_and_json() {
        let _guard = crate::test_support::env_lock();
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let report = build_report(&paths).unwrap();
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["rerank"], "off");
        assert!(json["rerank_model"].is_null());
        let mut text = Vec::new();
        print_report(&report, &mut text).unwrap();
        assert!(
            String::from_utf8(text)
                .unwrap()
                .contains("\nreranking:\n  mode: off\n\nsources:")
        );

        std::fs::write(
            paths.root.join("config.toml"),
            "rerank = true\nrerank_model = \"JINARerankerV2BaseMultiligual\"\nrerank_candidates = 12\n",
        )
        .unwrap();
        let report = build_report(&paths).unwrap();
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["rerank"], "local");
        assert_eq!(json["rerank_model"], "jina-v2");
        assert_eq!(json["rerank_candidates"], 12);
        assert_eq!(json["rerank_doc_chars"], 1500);
        let mut text = Vec::new();
        print_report(&report, &mut text).unwrap();
        assert!(String::from_utf8(text).unwrap().contains(
            "\nreranking:\n  mode: local\n  model: jina-v2\n  candidates: 12\n  doc-chars: 1500\n"
        ));
        assert!(json["rerank_error"].is_null());
        assert!(json["rerank_host"].is_null());
        assert!(json["rerank_extra_body"].is_null());

        std::fs::write(
            paths.root.join("config.toml"),
            "rerank = \"remote\"\nrerank_url = \"https://rerank.example.test:8443/v1/rerank\"\n\
             rerank_model = \"cohere/rerank-v3.5\"\nrerank_api_key = \"sk-stats-secret\"\n",
        )
        .unwrap();
        let report = build_report(&paths).unwrap();
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["rerank"], "remote");
        assert_eq!(json["rerank_model"], "cohere/rerank-v3.5");
        assert_eq!(json["rerank_host"], "rerank.example.test");
        let mut text = Vec::new();
        print_report(&report, &mut text).unwrap();
        let text = String::from_utf8(text).unwrap();
        assert!(
            text.contains(
                "\nreranking:\n  mode: remote\n  model: cohere/rerank-v3.5\n  \
                 host: rerank.example.test\n  extra-body: unset\n  candidates: 30\n  \
                 doc-chars: 1500\n"
            ),
            "{text}"
        );
        for output in [text, json.to_string()] {
            assert!(!output.contains("sk-stats-secret"), "{output}");
            assert!(!output.contains("/v1/rerank"), "{output}");
        }
        assert_eq!(json["rerank_extra_body"], false);

        std::fs::write(
            paths.root.join("config.toml"),
            "rerank = \"remote\"\nrerank_url = \"https://rerank.example.test/v1/rerank\"\n\
             rerank_model = \"cohere/rerank-v3.5\"\n\
             rerank_extra_body = '{\"provider\": {\"data_collection\": \"deny\"}}'\n",
        )
        .unwrap();
        let report = build_report(&paths).unwrap();
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["rerank_extra_body"], true);
        let mut text = Vec::new();
        print_report(&report, &mut text).unwrap();
        let text = String::from_utf8(text).unwrap();
        assert!(text.contains("\n  extra-body: set\n"), "{text}");
        for output in [text, json.to_string()] {
            assert!(!output.contains("data_collection"), "{output}");
        }

        std::fs::write(
            paths.root.join("config.toml"),
            "rerank = true\nrerank_model = \"jina-turbo\"\nrerank_candidates = 500\n",
        )
        .unwrap();
        let report = build_report(&paths).expect("misconfigured reranking still reports");
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["rerank"], "local");
        assert!(json["rerank_model"].is_null());
        let error = json["rerank_error"].as_str().unwrap();
        assert!(
            error.starts_with("rerank_candidates must be between 5 and 100"),
            "{error}"
        );
        let mut text = Vec::new();
        print_report(&report, &mut text).unwrap();
        let text = String::from_utf8(text).unwrap();
        assert!(
            text.contains("\nreranking:\n  mode: local\n  error: rerank_candidates must be"),
            "{text}"
        );
        assert!(text.contains("\nsources:"), "{text}");

        std::fs::write(
            paths.root.join("config.toml"),
            "rerank = \"remote\"\nrerank_model = \"cohere/rerank-v3.5\"\n",
        )
        .unwrap();
        let report = build_report(&paths).expect("misconfigured remote reranking still reports");
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["rerank"], "remote");
        assert!(json["rerank_host"].is_null());
        let error = json["rerank_error"].as_str().unwrap();
        assert!(error.contains("requires rerank_url"), "{error}");
    }

    #[test]
    fn vector_stats_report_none_without_vector_store() {
        let tmp = TempDir::new().unwrap();
        let stats = read_vector_stats(tmp.path()).unwrap();
        assert_eq!(vector_stats_line(&stats), "vectors: none");
        assert!(!stats.exists);
        assert_eq!(stats.count, 0);
        assert_eq!(stats.dimensions, None);
    }

    #[test]
    fn scan_cache_stats_read_sqlite_checkpoint_and_handle_missing_cache() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().to_path_buf())).unwrap();
        let missing = read_scan_cache_stats(&paths, 3600).unwrap();
        assert_eq!(missing.last_scan_ts, None);
        assert_eq!(missing.age_seconds, None);
        assert!(!missing.fresh);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let cache = ScanCache {
            last_scan_ts: now - 60,
            file_count: 4,
            total_bytes: 4096,
        };
        let lease = IngestLease::acquire(&paths, "stats test", INGEST_LEASE_TIMEOUT).unwrap();
        // This fixture starts with an empty root and no checkpoint authority.
        let mut writer =
            CheckpointWriter::open(&paths.state.join("ingest.json"), &lease, true).unwrap();
        writer
            .commit_delta(&CheckpointDelta {
                scan_cache: Some(cache),
                ..Default::default()
            })
            .unwrap();
        drop(writer);
        drop(lease);
        let stats = read_scan_cache_stats(&paths, 3600).unwrap();
        assert_eq!(stats.last_scan_ts, Some(now - 60));
        assert!(stats.age_seconds.unwrap() >= 60);
        assert!(stats.last_scan.unwrap().ends_with('Z'));
        assert!(stats.fresh);
        assert_eq!(stats.file_count, 4);
        assert_eq!(stats.total_bytes, 4096);
        assert!(!read_scan_cache_stats(&paths, 0).unwrap().fresh);
    }
}
