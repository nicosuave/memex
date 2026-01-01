use crate::config::{Paths, UserConfig};
use crate::embed::EmbedderHandle;
use crate::index::{QueryOptions, SearchIndex};
use crate::ingest::{IngestOptions, ingest_all};
use crate::types::SourceFilter;
use crate::vector::VectorIndex;
use anyhow::{Result, anyhow};
use chrono::SecondsFormat;
use clap::{Parser, Subcommand, ValueEnum};
use regex::RegexBuilder;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "automem",
    version,
    about = "Fast local history search for Claude and Codex"
)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Commands {
    Index {
        #[arg(long)]
        source: Option<PathBuf>,
        #[arg(long)]
        include_agents: bool,
        #[arg(long, default_value_t = true)]
        codex: bool,
        #[arg(long)]
        embeddings: bool,
        #[arg(long)]
        no_embeddings: bool,
        #[arg(long)]
        root: Option<PathBuf>,
    },
    Reindex {
        #[arg(long)]
        source: Option<PathBuf>,
        #[arg(long)]
        include_agents: bool,
        #[arg(long, default_value_t = true)]
        codex: bool,
        #[arg(long)]
        embeddings: bool,
        #[arg(long)]
        no_embeddings: bool,
        #[arg(long)]
        root: Option<PathBuf>,
    },
    Embed {
        #[arg(long)]
        root: Option<PathBuf>,
    },
    Search {
        query: String,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        role: Option<String>,
        #[arg(long)]
        tool: Option<String>,
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        source: Option<SourceFilter>,
        #[arg(long)]
        semantic: bool,
        #[arg(long)]
        hybrid: bool,
        #[arg(long)]
        min_score: Option<f32>,
        #[arg(long, default_value_t = 1.0)]
        recency_weight: f32,
        #[arg(long, default_value_t = 30.0)]
        recency_half_life_days: f32,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        until: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long = "top-n-per-session")]
        top_n_per_session: Option<usize>,
        #[arg(long)]
        unique_session: bool,
        #[arg(long)]
        json_array: bool,
        #[arg(long)]
        fields: Option<String>,
        #[arg(long, value_enum, default_value = "score")]
        sort: SortBy,
        #[arg(short, long)]
        verbose: bool,
        #[arg(long)]
        root: Option<PathBuf>,
    },
    Session {
        session_id: String,
        #[arg(short, long)]
        verbose: bool,
        #[arg(long)]
        root: Option<PathBuf>,
    },
    Show {
        doc_id: u64,
        #[arg(short, long)]
        verbose: bool,
        #[arg(long)]
        root: Option<PathBuf>,
    },
    Stats {
        #[arg(long)]
        root: Option<PathBuf>,
    },
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Index {
            source,
            include_agents,
            codex,
            embeddings,
            no_embeddings,
            root,
        } => {
            run_index(
                source,
                include_agents,
                codex,
                embeddings,
                no_embeddings,
                root,
                false,
            )?;
        }
        Commands::Reindex {
            source,
            include_agents,
            codex,
            embeddings,
            no_embeddings,
            root,
        } => {
            run_index(
                source,
                include_agents,
                codex,
                embeddings,
                no_embeddings,
                root,
                true,
            )?;
        }
        Commands::Embed { root } => {
            run_embed(root)?;
        }
        Commands::Search {
            query,
            project,
            role,
            tool,
            session,
            source,
            semantic,
            hybrid,
            min_score,
            recency_weight,
            recency_half_life_days,
            since,
            until,
            limit,
            top_n_per_session,
            unique_session,
            json_array,
            fields,
            sort,
            verbose,
            root,
        } => {
            run_search(
                query,
                project,
                role,
                tool,
                session,
                source,
                semantic,
                hybrid,
                min_score,
                recency_weight,
                recency_half_life_days,
                since,
                until,
                limit,
                top_n_per_session,
                unique_session,
                json_array,
                fields,
                sort,
                verbose,
                root,
            )?;
        }
        Commands::Session {
            session_id,
            verbose,
            root,
        } => {
            run_session(session_id, verbose, root)?;
        }
        Commands::Show {
            doc_id,
            verbose,
            root,
        } => {
            run_show(doc_id, verbose, root)?;
        }
        Commands::Stats { root } => {
            run_stats(root)?;
        }
    }
    Ok(())
}

fn run_index(
    source: Option<PathBuf>,
    include_agents: bool,
    codex: bool,
    embeddings_flag: bool,
    no_embeddings: bool,
    root: Option<PathBuf>,
    reindex: bool,
) -> Result<()> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let embeddings = resolve_flag(
        config.embeddings_default(),
        embeddings_flag,
        no_embeddings,
        "embeddings",
    )?;
    if reindex && paths.root.exists() {
        std::fs::remove_dir_all(&paths.root)?;
    }
    paths.ensure_dirs()?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    let vector_exists = paths.vectors.join("meta.json").exists()
        && paths.vectors.join("vectors.f32").exists()
        && paths.vectors.join("doc_ids.u64").exists();
    let backfill_embeddings = embeddings && !vector_exists && index.doc_count()? > 0;

    let opts = IngestOptions {
        claude_source: source.unwrap_or_else(default_claude_source),
        include_agents,
        include_codex: codex,
        embeddings,
        backfill_embeddings,
    };

    let report = ingest_all(&paths, &index, &opts)?;
    if report.records_embedded > 0 {
        println!(
            "indexed {} records, embedded {} across {} files (skipped {})",
            report.records_added,
            report.records_embedded,
            report.files_scanned,
            report.files_skipped
        );
    } else {
        println!(
            "indexed {} records across {} files (skipped {})",
            report.records_added, report.files_scanned, report.files_skipped
        );
    }
    Ok(())
}

fn run_embed(root: Option<PathBuf>) -> Result<()> {
    const BATCH_SIZE: usize = 256;

    let paths = Paths::new(root)?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    let mut embedder = EmbedderHandle::new()?;
    let mut vector = VectorIndex::open_or_create(&paths.vectors, embedder.dims)?;

    let progress = std::sync::Arc::new(crate::progress::Progress::new([0; 3], [0; 3], true));
    progress.set_embed_ready();

    let mut embedded_counts = [0u64; 3];
    let mut embedded_total = 0u64;
    let mut batch: Vec<(u64, String, crate::types::SourceKind)> = Vec::with_capacity(BATCH_SIZE);

    let flush_batch = |batch: &mut Vec<(u64, String, crate::types::SourceKind)>,
                       embedder: &mut EmbedderHandle,
                       vector: &mut VectorIndex,
                       progress: &crate::progress::Progress,
                       embedded_counts: &mut [u64; 3],
                       embedded_total: &mut u64| {
        if batch.is_empty() {
            return Ok(());
        }
        let texts: Vec<&str> = batch.iter().map(|(_, text, _)| text.as_str()).collect();
        let embeddings = embedder.embed_texts(&texts)?;

        for ((doc_id, _, source), vec) in batch.iter().zip(embeddings.iter()) {
            vector.add(*doc_id, vec)?;
            progress.add_embedded(*source, 1);
            embedded_counts[source.idx()] += 1;
            *embedded_total += 1;
        }
        batch.clear();
        Ok::<_, anyhow::Error>(())
    };

    index.for_each_record(|record| {
        if record.text.is_empty() || !is_embedding_role(&record.role) {
            return Ok(());
        }
        if vector.contains(record.doc_id) {
            return Ok(());
        }
        let text = truncate_for_embedding(record.text);
        if !text.is_empty() {
            progress.add_embed_total(record.source, 1);
            batch.push((record.doc_id, text, record.source));

            if batch.len() >= BATCH_SIZE {
                flush_batch(
                    &mut batch,
                    &mut embedder,
                    &mut vector,
                    &progress,
                    &mut embedded_counts,
                    &mut embedded_total,
                )?;
            }
        }
        Ok(())
    })?;

    // Flush remaining
    flush_batch(
        &mut batch,
        &mut embedder,
        &mut vector,
        &progress,
        &mut embedded_counts,
        &mut embedded_total,
    )?;

    vector.save()?;
    progress.finish();
    println!(
        "embedded {} vectors (claude {}, codex {}, history {})",
        embedded_total,
        embedded_counts[crate::types::SourceKind::Claude.idx()],
        embedded_counts[crate::types::SourceKind::CodexSession.idx()],
        embedded_counts[crate::types::SourceKind::CodexHistory.idx()],
    );

    std::io::stdout().flush().ok();
    std::process::exit(0);
}

#[allow(clippy::too_many_arguments)]
fn run_search(
    query: String,
    project: Option<String>,
    role: Option<String>,
    tool: Option<String>,
    session: Option<String>,
    source: Option<SourceFilter>,
    semantic: bool,
    hybrid: bool,
    min_score: Option<f32>,
    recency_weight: f32,
    recency_half_life_days: f32,
    since: Option<String>,
    until: Option<String>,
    limit: usize,
    top_n_per_session: Option<usize>,
    unique_session: bool,
    json_array: bool,
    fields: Option<String>,
    sort: SortBy,
    verbose: bool,
    root: Option<PathBuf>,
) -> Result<()> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let auto_index_on_search = config.auto_index_on_search_default();
    let embeddings_default = config.embeddings_default();
    if auto_index_on_search {
        paths.ensure_dirs()?;
        let index = SearchIndex::open_or_create(&paths.index)?;
        let vector_exists = paths.vectors.join("meta.json").exists()
            && paths.vectors.join("vectors.f32").exists()
            && paths.vectors.join("doc_ids.u64").exists();
        let backfill_embeddings = embeddings_default && !vector_exists && index.doc_count()? > 0;
        let opts = IngestOptions {
            claude_source: default_claude_source(),
            include_agents: false,
            include_codex: true,
            embeddings: embeddings_default,
            backfill_embeddings,
        };
        let _ = ingest_all(&paths, &index, &opts)?;
    }
    let index = SearchIndex::open_or_create(&paths.index)?;

    let options = QueryOptions {
        query,
        project,
        role,
        tool,
        session_id: session,
        source,
        since: parse_ts_millis(since)?,
        until: parse_ts_millis(until)?,
        limit,
    };
    let matchers = build_matchers(&options.query)?;
    let fields = parse_fields(fields)?;
    let top_n_per_session = if unique_session && top_n_per_session.is_none() {
        Some(1)
    } else {
        top_n_per_session
    };
    let render = RenderOptions {
        verbose,
        matchers,
        json_array: json_array && !verbose,
        fields,
        sort,
        min_score,
        top_n_per_session,
        limit,
    };

    let candidate_limit = if top_n_per_session.is_some() || options.source.is_some() {
        (limit * 5).max(limit + 10)
    } else {
        limit
    };

    if hybrid {
        return run_hybrid_search(
            &index,
            &options,
            candidate_limit,
            &render,
            &paths,
            recency_weight,
            recency_half_life_days,
        );
    }
    if semantic {
        return run_semantic_search(
            &index,
            &options,
            candidate_limit,
            &render,
            &paths,
            recency_weight,
            recency_half_life_days,
        );
    }
    let results = index.search(&options)?;
    let now_ms = chrono::Utc::now().timestamp_millis() as u64;
    let mut reranked =
        apply_recency_to_results(results, now_ms, recency_weight, recency_half_life_days);
    reranked.retain(|(_, record)| matches_filters(record, &options));
    let reranked = apply_post_processing(reranked, &render);
    render_results(reranked, &render)?;
    Ok(())
}

fn run_semantic_search(
    index: &SearchIndex,
    options: &QueryOptions,
    limit: usize,
    render: &RenderOptions,
    paths: &Paths,
    recency_weight: f32,
    recency_half_life_days: f32,
) -> Result<()> {
    let vector = VectorIndex::open(&paths.vectors)?;
    let mut embedder = EmbedderHandle::new()?;
    let embeddings = embedder.embed_texts(&[options.query.as_str()])?;
    let embedding = embeddings
        .first()
        .ok_or_else(|| anyhow!("embedding missing"))?;
    let mut results = Vec::new();
    let now_ms = chrono::Utc::now().timestamp_millis() as u64;
    for (doc_id, distance) in vector.search(embedding, limit)? {
        if let Some(record) = index.get_by_doc_id(doc_id)?
            && matches_filters(&record, options)
        {
            let base = score_from_distance(distance);
            let score = apply_recency(
                base,
                record.ts,
                now_ms,
                recency_weight,
                recency_half_life_days,
            );
            results.push((score, record));
        }
    }
    let results = apply_post_processing(results, render);
    render_results(results, render)?;
    Ok(())
}

fn run_hybrid_search(
    index: &SearchIndex,
    options: &QueryOptions,
    limit: usize,
    render: &RenderOptions,
    paths: &Paths,
    recency_weight: f32,
    recency_half_life_days: f32,
) -> Result<()> {
    let vector = VectorIndex::open(&paths.vectors)?;
    let mut embedder = EmbedderHandle::new()?;

    let bm25_k = (limit * 5).clamp(50, 500);
    let vector_k = (limit * 5).clamp(50, 500);

    let bm25_results = index.search(&QueryOptions {
        limit: bm25_k,
        ..options.clone()
    })?;

    let embeddings = embedder.embed_texts(&[options.query.as_str()])?;
    let embedding = embeddings
        .first()
        .ok_or_else(|| anyhow!("embedding missing"))?;
    let vector_results = vector.search(embedding, vector_k)?;

    let mut records: HashMap<u64, crate::types::Record> = HashMap::new();
    let mut scores: HashMap<u64, f32> = HashMap::new();
    let rrf_k = 60.0;

    for (rank, (_, record)) in bm25_results.into_iter().enumerate() {
        if !matches_filters(&record, options) {
            continue;
        }
        let r = rank as f32 + 1.0;
        scores
            .entry(record.doc_id)
            .and_modify(|v| *v += 1.0 / (rrf_k + r))
            .or_insert(1.0 / (rrf_k + r));
        records.insert(record.doc_id, record);
    }

    for (rank, (doc_id, _distance)) in vector_results.into_iter().enumerate() {
        if let Some(record) = index.get_by_doc_id(doc_id)? {
            if !matches_filters(&record, options) {
                continue;
            }
            let r = rank as f32 + 1.0;
            scores
                .entry(doc_id)
                .and_modify(|v| *v += 1.0 / (rrf_k + r))
                .or_insert(1.0 / (rrf_k + r));
            records.entry(doc_id).or_insert(record);
        }
    }

    let now_ms = chrono::Utc::now().timestamp_millis() as u64;
    let merged: Vec<(f32, crate::types::Record)> = scores
        .into_iter()
        .filter_map(|(doc_id, score)| {
            records.remove(&doc_id).map(|r| {
                (
                    apply_recency(score, r.ts, now_ms, recency_weight, recency_half_life_days),
                    r,
                )
            })
        })
        .collect();
    let merged = apply_post_processing(merged, render);
    render_results(merged, render)?;
    Ok(())
}

fn score_from_distance(distance: f32) -> f32 {
    1.0 / (1.0 + distance)
}

fn apply_recency(score: f32, ts: u64, now_ms: u64, weight: f32, half_life_days: f32) -> f32 {
    if score <= 0.0 || weight <= 0.0 || half_life_days <= 0.0 || ts == 0 {
        return score;
    }
    let age_ms = now_ms.saturating_sub(ts);
    let age_days = age_ms as f32 / (1000.0 * 60.0 * 60.0 * 24.0);
    let decay = (-std::f32::consts::LN_2 * age_days / half_life_days).exp();
    score * (1.0 + weight * decay)
}

fn matches_filters(record: &crate::types::Record, options: &QueryOptions) -> bool {
    if let Some(project) = &options.project
        && &record.project != project
    {
        return false;
    }
    if let Some(role) = &options.role
        && &record.role != role
    {
        return false;
    }
    if let Some(tool) = &options.tool
        && record.tool_name.as_deref() != Some(tool.as_str())
    {
        return false;
    }
    if let Some(source) = options.source
        && !source.matches(record.source)
    {
        return false;
    }
    if let Some(session_id) = &options.session_id
        && &record.session_id != session_id
    {
        return false;
    }
    if let Some(since) = options.since
        && record.ts < since
    {
        return false;
    }
    if let Some(until) = options.until
        && record.ts > until
    {
        return false;
    }
    true
}

#[derive(Clone)]
struct RenderOptions {
    verbose: bool,
    matchers: Vec<regex::Regex>,
    json_array: bool,
    fields: Option<HashSet<String>>,
    sort: SortBy,
    min_score: Option<f32>,
    top_n_per_session: Option<usize>,
    limit: usize,
}

#[derive(Serialize)]
struct MatchSpan {
    start: usize,
    end: usize,
    text: String,
    before: String,
    after: String,
}

#[derive(Serialize)]
struct SearchHit {
    score: f32,
    ts: String,
    doc_id: u64,
    project: String,
    role: String,
    session_id: String,
    source_path: String,
    text: String,
    snippet: String,
    matches: Vec<MatchSpan>,
}

fn render_results(results: Vec<(f32, crate::types::Record)>, render: &RenderOptions) -> Result<()> {
    if render.verbose {
        for (score, record) in results {
            let ts = format_ts(record.ts);
            let text = summarize(&record.text, 200);
            println!(
                "[{score:.3}] {} {} {} {} {} {}",
                ts, record.doc_id, record.project, record.role, record.session_id, text
            );
        }
        return Ok(());
    }

    let mut output = Vec::new();
    for (score, record) in results {
        let ts = format_ts(record.ts);
        let text_ref = record.text.as_str();
        let wants_snippet = wants_field(&render.fields, "snippet");
        let wants_matches = wants_field(&render.fields, "matches");
        let wants_text = wants_field(&render.fields, "text");
        let snippet = if wants_snippet {
            summarize(text_ref, 400)
        } else {
            String::new()
        };
        let matches = if wants_matches {
            collect_matches(text_ref, &render.matchers, 8)
        } else {
            Vec::new()
        };
        let text = if wants_text {
            record.text
        } else {
            String::new()
        };

        let value = if let Some(fields) = &render.fields {
            let mut map = serde_json::Map::new();
            if fields.contains("score") {
                map.insert("score".to_string(), Value::from(score));
            }
            if fields.contains("ts") {
                map.insert("ts".to_string(), Value::from(ts));
            }
            if fields.contains("doc_id") {
                map.insert("doc_id".to_string(), Value::from(record.doc_id));
            }
            if fields.contains("project") {
                map.insert("project".to_string(), Value::from(record.project));
            }
            if fields.contains("role") {
                map.insert("role".to_string(), Value::from(record.role));
            }
            if fields.contains("session_id") {
                map.insert("session_id".to_string(), Value::from(record.session_id));
            }
            if fields.contains("source_path") {
                map.insert("source_path".to_string(), Value::from(record.source_path));
            }
            if fields.contains("text") {
                map.insert("text".to_string(), Value::from(text));
            }
            if fields.contains("snippet") {
                map.insert("snippet".to_string(), Value::from(snippet));
            }
            if fields.contains("matches") {
                map.insert("matches".to_string(), serde_json::to_value(matches)?);
            }
            Value::Object(map)
        } else {
            serde_json::to_value(SearchHit {
                score,
                ts,
                doc_id: record.doc_id,
                project: record.project,
                role: record.role,
                session_id: record.session_id,
                source_path: record.source_path,
                text,
                snippet,
                matches,
            })?
        };
        if render.json_array {
            output.push(value);
        } else {
            println!("{}", serde_json::to_string(&value)?);
        }
    }

    if render.json_array {
        println!("{}", serde_json::to_string(&output)?);
    }
    Ok(())
}

fn run_session(session_id: String, verbose: bool, root: Option<PathBuf>) -> Result<()> {
    let paths = Paths::new(root)?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    let mut records = index.records_by_session_id(&session_id)?;
    records.sort_by(|a, b| {
        a.turn_id
            .cmp(&b.turn_id)
            .then_with(|| a.ts.cmp(&b.ts))
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    if verbose {
        for record in records {
            let ts = format_ts(record.ts);
            println!("{ts} {}", record.role);
            if record.text.is_empty() {
                println!("  <empty>");
                continue;
            }
            for line in record.text.lines() {
                println!("  {line}");
            }
        }
        return Ok(());
    }
    for record in records {
        println!("{}", serde_json::to_string(&record)?);
    }
    Ok(())
}

fn run_show(doc_id: u64, verbose: bool, root: Option<PathBuf>) -> Result<()> {
    let paths = Paths::new(root)?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    let record = index
        .get_by_doc_id(doc_id)?
        .ok_or_else(|| anyhow!("doc_id not found"))?;
    if verbose {
        println!("{}", serde_json::to_string_pretty(&record)?);
        return Ok(());
    }
    println!("{}", serde_json::to_string(&record)?);
    Ok(())
}

fn run_stats(root: Option<PathBuf>) -> Result<()> {
    let paths = Paths::new(root)?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    println!("index: {}", paths.index.display());
    println!("documents: {}", index.doc_count()?);
    print_vector_stats(&paths.vectors)?;
    Ok(())
}

fn print_vector_stats(vectors_dir: &std::path::Path) -> Result<()> {
    let meta_path = vectors_dir.join("meta.json");
    let vectors_path = vectors_dir.join("vectors.f32");
    let ids_path = vectors_dir.join("doc_ids.u64");
    if !meta_path.exists() || !vectors_path.exists() || !ids_path.exists() {
        println!("vectors: none");
        return Ok(());
    }
    #[derive(serde::Deserialize)]
    struct Meta {
        dimensions: usize,
    }
    let meta_str = std::fs::read_to_string(&meta_path)?;
    let meta: Meta = serde_json::from_str(&meta_str)?;
    let ids_bytes = std::fs::metadata(&ids_path)?.len();
    let vec_bytes = std::fs::metadata(&vectors_path)?.len();
    let ids = ids_bytes / 8;
    let vecs = if meta.dimensions == 0 {
        0
    } else {
        (vec_bytes / 4) / meta.dimensions as u64
    };
    println!(
        "vectors: {} (dims {}, ids {}, vectors.f32 {}, doc_ids.u64 {})",
        vecs, meta.dimensions, ids, vec_bytes, ids_bytes
    );
    Ok(())
}

fn default_claude_source() -> PathBuf {
    let home = directories::BaseDirs::new()
        .map(|b| b.home_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("/"));
    home.join(".claude").join("projects")
}

fn parse_ts_millis(value: Option<String>) -> Result<Option<u64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.chars().all(|c| c.is_ascii_digit()) {
        let num: u64 = value.parse()?;
        if num > 10_000_000_000 {
            return Ok(Some(num));
        }
        return Ok(Some(num * 1000));
    }
    let dt = chrono::DateTime::parse_from_rfc3339(&value)
        .map_err(|_| anyhow!("invalid timestamp: {value}"))?;
    Ok(Some(dt.timestamp_millis() as u64))
}

fn summarize(text: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    let mut out = String::new();
    let mut count = 0usize;
    let mut last_space = false;
    let mut truncated = false;
    for ch in text.chars() {
        if count >= max {
            truncated = true;
            break;
        }
        if ch.is_whitespace() {
            if out.is_empty() || last_space {
                continue;
            }
            out.push(' ');
            last_space = true;
            count += 1;
            continue;
        }
        out.push(ch);
        last_space = false;
        count += 1;
    }
    if truncated && max >= 3 {
        let keep = max.saturating_sub(3);
        let mut short = String::new();
        for (i, ch) in out.chars().enumerate() {
            if i >= keep {
                break;
            }
            short.push(ch);
        }
        short.push_str("...");
        return short.trim().to_string();
    }
    out.trim().to_string()
}

#[derive(Clone, Copy, ValueEnum)]
enum SortBy {
    Score,
    Ts,
}

fn parse_fields(value: Option<String>) -> Result<Option<HashSet<String>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let mut out = HashSet::new();
    for part in value.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        out.insert(trimmed.to_string());
    }
    if out.is_empty() {
        return Ok(None);
    }
    Ok(Some(out))
}

fn wants_field(fields: &Option<HashSet<String>>, name: &str) -> bool {
    fields
        .as_ref()
        .map(|set| set.contains(name))
        .unwrap_or(true)
}

fn apply_recency_to_results(
    results: Vec<(f32, crate::types::Record)>,
    now_ms: u64,
    recency_weight: f32,
    recency_half_life_days: f32,
) -> Vec<(f32, crate::types::Record)> {
    results
        .into_iter()
        .map(|(score, record)| {
            (
                apply_recency(
                    score,
                    record.ts,
                    now_ms,
                    recency_weight,
                    recency_half_life_days,
                ),
                record,
            )
        })
        .collect()
}

fn apply_post_processing(
    mut results: Vec<(f32, crate::types::Record)>,
    render: &RenderOptions,
) -> Vec<(f32, crate::types::Record)> {
    if let Some(min_score) = render.min_score {
        results.retain(|(score, _)| *score >= min_score);
    }

    match render.sort {
        SortBy::Score => {
            results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        }
        SortBy::Ts => {
            results.sort_by(|a, b| b.1.ts.cmp(&a.1.ts));
        }
    }

    if let Some(k) = render.top_n_per_session {
        let mut per_session: HashMap<String, usize> = HashMap::new();
        let mut grouped = Vec::with_capacity(results.len());
        for (score, record) in results {
            let count = per_session.entry(record.session_id.clone()).or_insert(0);
            if *count < k {
                grouped.push((score, record));
                *count += 1;
            }
        }
        results = grouped;
    }

    if results.len() > render.limit {
        results.truncate(render.limit);
    }
    results
}

fn format_ts(ts: u64) -> String {
    if ts == 0 {
        return "-".to_string();
    }
    let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts as i64) else {
        return "-".to_string();
    };
    dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn build_matchers(query: &str) -> Result<Vec<regex::Regex>> {
    let mut terms = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for part in query.split_whitespace() {
        let cleaned = part.trim_matches(|c: char| !c.is_alphanumeric());
        if cleaned.len() < 2 {
            continue;
        }
        let key = cleaned.to_lowercase();
        if seen.insert(key.clone()) {
            terms.push(key);
        }
    }
    let mut out = Vec::new();
    for term in terms {
        let re = RegexBuilder::new(&regex::escape(&term))
            .case_insensitive(true)
            .build()?;
        out.push(re);
    }
    Ok(out)
}

fn collect_matches(text: &str, matchers: &[regex::Regex], max: usize) -> Vec<MatchSpan> {
    if text.is_empty() || matchers.is_empty() || max == 0 {
        return Vec::new();
    }
    let mut out = Vec::new();
    for re in matchers {
        for m in re.find_iter(text) {
            if out.len() >= max {
                return out;
            }
            let start = m.start();
            let end = m.end();
            let before = take_last_chars(&text[..start], 40);
            let after = take_first_chars(&text[end..], 40);
            out.push(MatchSpan {
                start,
                end,
                text: m.as_str().to_string(),
                before,
                after,
            });
        }
    }
    out
}

fn take_last_chars(text: &str, max: usize) -> String {
    let mut out = Vec::new();
    for ch in text.chars().rev().take(max) {
        out.push(ch);
    }
    out.into_iter().rev().collect()
}

fn take_first_chars(text: &str, max: usize) -> String {
    text.chars().take(max).collect()
}

fn is_embedding_role(role: &str) -> bool {
    role == "user" || role == "assistant"
}

fn truncate_for_embedding(mut text: String) -> String {
    const EMBED_MAX_CHARS: usize = 8192;
    if text.len() <= EMBED_MAX_CHARS {
        return text;
    }
    let mut end = EMBED_MAX_CHARS.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text
}

fn resolve_flag(default: bool, enable: bool, disable: bool, name: &str) -> Result<bool> {
    if enable && disable {
        return Err(anyhow!("--{name} and --no-{name} cannot be used together"));
    }
    if enable {
        return Ok(true);
    }
    if disable {
        return Ok(false);
    }
    Ok(default)
}
