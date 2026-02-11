use crate::config::{Paths, UserConfig, default_claude_source};
use crate::embed::{EmbedderHandle, ModelChoice};
use crate::index::{QueryOptions, SearchIndex};
use crate::ingest::{IngestOptions, ingest_all, ingest_if_stale};
use crate::tui;
use crate::types::SourceFilter;
use crate::vector::VectorIndex;
use anyhow::{Result, anyhow};
use chrono::SecondsFormat;
use clap::{Args, Parser, Subcommand, ValueEnum};
use regex::RegexBuilder;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "memex",
    version,
    about = "Fast local history search for Claude and Codex",
    after_help = "\
QUICK START:
    memex index                     # Index your Claude/Codex history
    memex search \"error handling\"   # Search for keywords
    memex tui                       # Browse sessions interactively

LEARN MORE:
    memex <command> --help          # Detailed help for each command"
)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Args, Clone)]
struct IndexArgs {
    /// Path to Claude projects directory [default: ~/.claude/projects]
    #[arg(long)]
    source: Option<PathBuf>,
    /// Include agent subprocess conversations (Claude Code subagents)
    #[arg(long)]
    include_agents: bool,
    /// Index Codex sessions from ~/.codex [default: true]
    #[arg(long, default_value_t = true)]
    codex: bool,
    /// Index Opencode sessions from ~/.local/share/opencode [default: true]
    #[arg(long, default_value_t = true)]
    opencode: bool,
    /// Generate embeddings for semantic search during indexing
    #[arg(long)]
    embeddings: bool,
    /// Skip embedding generation (overrides config default)
    #[arg(long)]
    no_embeddings: bool,
    /// Embedding model: minilm (fast), bge, nomic, gemma (default, best quality), potion (tiny)
    #[arg(long)]
    model: Option<String>,
    /// Path to memex data directory [default: ~/.memex]
    #[arg(long)]
    root: Option<PathBuf>,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Commands {
    /// Index Claude and Codex conversation history
    #[command(after_help = "\
EXAMPLES:
    memex index                         # Index all Claude and Codex history
    memex index --embeddings            # Also generate embeddings for semantic search
    memex index --include-agents        # Include Claude Code subagent conversations
    memex index --source ~/custom/path  # Use custom Claude projects directory")]
    Index {
        #[command(flatten)]
        index: IndexArgs,
        #[arg(long, hide = true)]
        watch: bool,
        #[arg(
            long = "watch-interval",
            default_value_t = 30,
            value_parser = clap::value_parser!(u64).range(1..),
            hide = true
        )]
        watch_interval: u64,
    },
    /// Delete existing index and rebuild from scratch
    Reindex {
        #[command(flatten)]
        index: IndexArgs,
    },
    /// Generate embeddings for semantic search (requires existing index)
    Embed {
        /// Embedding model: minilm (fast), bge, nomic, gemma (default, best quality), potion (tiny)
        #[arg(long)]
        model: Option<String>,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Search indexed conversation history
    #[command(after_help = "\
EXAMPLES:
    memex search \"error handling\"
    memex search \"API design\" --source claude --limit 50
    memex search \"auth\" --since 2024-01-01T00:00:00Z --semantic
    memex search \"bug\" --fields score,session_id,snippet --json-array

TIMESTAMP FORMAT:
    RFC3339: 2024-01-15T10:30:00Z or 2024-01-15T10:30:00-05:00
    Unix seconds: 1705315800
    Unix milliseconds: 1705315800000

OUTPUT FIELDS (--fields):
    score, ts, doc_id, project, role, session_id, source_path, text, snippet, matches")]
    Search {
        /// Search query (keywords or natural language for semantic search)
        query: String,
        /// Filter by project name
        #[arg(long)]
        project: Option<String>,
        /// Filter by role (user, assistant, tool_use, tool_result)
        #[arg(long)]
        role: Option<String>,
        /// Filter by tool name (e.g., Read, Edit, Bash)
        #[arg(long)]
        tool: Option<String>,
        /// Filter by session ID
        #[arg(long)]
        session: Option<String>,
        /// Filter by source: claude or codex
        #[arg(long)]
        source: Option<SourceFilter>,
        /// Use semantic (embedding-based) search instead of keyword search
        #[arg(long)]
        semantic: bool,
        /// Use hybrid search combining BM25 keyword and semantic scores
        #[arg(long)]
        hybrid: bool,
        /// Minimum score threshold to include in results
        #[arg(long)]
        min_score: Option<f32>,
        /// Weight for recency boost (0 = no boost, higher = more recent preferred)
        #[arg(long, default_value_t = 1.0)]
        recency_weight: f32,
        /// Half-life in days for recency decay (lower = faster decay)
        #[arg(long, default_value_t = 30.0)]
        recency_half_life_days: f32,
        /// Only include results after this timestamp (RFC3339 or unix seconds/ms)
        #[arg(long, value_name = "TIMESTAMP")]
        since: Option<String>,
        /// Only include results before this timestamp (RFC3339 or unix seconds/ms)
        #[arg(long, value_name = "TIMESTAMP")]
        until: Option<String>,
        /// Maximum number of results to return
        #[arg(long, default_value_t = 20)]
        limit: usize,
        /// Limit results per session (useful for getting variety)
        #[arg(long = "top-n-per-session", value_name = "N")]
        top_n_per_session: Option<usize>,
        /// Return at most one result per session (shorthand for --top-n-per-session 1)
        #[arg(long)]
        unique_session: bool,
        /// Output results as a single JSON array instead of newline-delimited JSON
        #[arg(long)]
        json_array: bool,
        /// Comma-separated list of fields to include in output
        #[arg(long, value_name = "FIELDS")]
        fields: Option<String>,
        /// Sort results by score or timestamp
        #[arg(long, value_enum, default_value = "score")]
        sort: SortBy,
        /// Show verbose output with inline text preview
        #[arg(short, long)]
        verbose: bool,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Interactive terminal UI for browsing sessions
    Tui {
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Run indexing as a background service via launchd (macOS only)
    IndexService {
        #[command(subcommand)]
        action: IndexServiceCommand,
    },
    /// Display all messages from a specific session
    Session {
        /// Session ID (from search results or TUI)
        session_id: String,
        /// Show human-readable output with timestamps and role labels
        #[arg(short, long)]
        verbose: bool,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Display a single document by its internal ID
    Show {
        /// Document ID (from search results)
        doc_id: u64,
        /// Pretty-print JSON output
        #[arg(short, long)]
        verbose: bool,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Show index statistics (document count, vector count, storage paths)
    Stats {
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Install the memex-search skill for Claude, Codex, and/or Opencode
    Setup {
        /// Overwrite existing skills/prompts (useful after memex update)
        #[arg(short, long)]
        force: bool,
    },
    /// Update memex to the latest version
    Update {
        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
    },
    /// Share a session via agentexport
    #[command(after_help = "\
EXAMPLES:
    memex share abc123              # Share session abc123
    memex share abc123 --title \"Bug fix session\"  # Share with custom title

REQUIREMENTS:
    Requires agentexport to be installed: brew install nicosuave/tap/agentexport")]
    Share {
        /// Session ID (from search results or TUI)
        session_id: String,
        /// Title for the share (optional)
        #[arg(long)]
        title: Option<String>,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum IndexServiceCommand {
    /// Enable automatic background indexing (launchd on macOS, systemd on Linux)
    Enable {
        #[command(flatten)]
        index: IndexArgs,
        /// Service label/name [default: com.memex.index (macOS) or memex-index (Linux)]
        #[arg(long)]
        label: Option<String>,
        /// Run as a long-lived process instead of periodic execution
        #[arg(long)]
        continuous: bool,
        /// Seconds between index checks in continuous mode [default: 30]
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..), value_name = "SECONDS")]
        poll_interval: Option<u64>,
        /// Seconds between invocations in interval mode [default: 3600]
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..), value_name = "SECONDS")]
        interval: Option<u64>,
        /// Path for stdout log file [default: ~/.memex/index-service.log] (macOS only)
        #[arg(long)]
        stdout: Option<PathBuf>,
        /// Path for stderr log file [default: ~/.memex/index-service.err.log] (macOS only)
        #[arg(long)]
        stderr: Option<PathBuf>,
        /// Path to write launchd plist (macOS only) [default: ~/.memex/index-service.plist]
        #[arg(long)]
        plist: Option<PathBuf>,
        /// Path to systemd user directory (Linux only) [default: ~/.config/systemd/user]
        #[arg(long)]
        systemd_dir: Option<PathBuf>,
    },
    /// Disable and remove the background indexing service
    Disable {
        /// Service label/name [default: com.memex.index (macOS) or memex-index (Linux)]
        #[arg(long)]
        label: Option<String>,
        /// Path to launchd plist (macOS only) [default: ~/.memex/index-service.plist]
        #[arg(long)]
        plist: Option<PathBuf>,
        /// Path to systemd user directory (Linux only) [default: ~/.config/systemd/user]
        #[arg(long)]
        systemd_dir: Option<PathBuf>,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    let should_check = !matches!(cli.command, Commands::Tui { .. } | Commands::Update { .. });
    if should_check {
        check_for_update_async(None);
    }
    match cli.command {
        Commands::Index {
            index,
            watch,
            watch_interval,
        } => {
            if watch {
                run_index_loop(&index, watch_interval)?;
            } else {
                run_index_args(&index, false)?;
            }
        }
        Commands::Reindex { index } => {
            run_index_args(&index, true)?;
        }
        Commands::Embed { model, root } => {
            run_embed(model, root)?;
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
        Commands::Tui { root } => {
            let (update_tx, update_rx) = std::sync::mpsc::channel();
            check_for_update_async(Some(update_tx));
            tui::run(root, Some(update_rx))?;
        }
        Commands::IndexService { action } => match action {
            IndexServiceCommand::Enable {
                index,
                label,
                continuous,
                poll_interval,
                interval,
                stdout,
                stderr,
                plist,
                systemd_dir,
            } => {
                run_index_service_enable(
                    &index,
                    label,
                    continuous,
                    poll_interval,
                    interval,
                    stdout,
                    stderr,
                    plist,
                    systemd_dir,
                )?;
            }
            IndexServiceCommand::Disable {
                label,
                plist,
                systemd_dir,
                root,
            } => {
                run_index_service_disable(label, plist, systemd_dir, root)?;
            }
        },
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
        Commands::Setup { force } => {
            run_setup(force)?;
        }
        Commands::Update { yes } => {
            run_update(yes)?;
        }
        Commands::Share {
            session_id,
            title,
            root,
        } => {
            run_share(session_id, title, root)?;
        }
    }
    Ok(())
}

fn run_index_loop(index: &IndexArgs, interval_secs: u64) -> Result<()> {
    loop {
        run_index_args(index, false)?;
        std::io::stdout().flush().ok();
        std::thread::sleep(Duration::from_secs(interval_secs));
    }
}

fn run_index_args(index: &IndexArgs, reindex: bool) -> Result<()> {
    run_index(
        index.source.clone(),
        index.include_agents,
        index.codex,
        index.opencode,
        index.embeddings,
        index.no_embeddings,
        index.model.clone(),
        index.root.clone(),
        reindex,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_index(
    source: Option<PathBuf>,
    include_agents: bool,
    codex: bool,
    opencode: bool,
    embeddings_flag: bool,
    no_embeddings: bool,
    model: Option<String>,
    root: Option<PathBuf>,
    reindex: bool,
) -> Result<()> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;

    // Model priority: CLI flag > config file > env var > default
    let model_choice = config.resolve_model(model)?;
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
        include_opencode: opencode,
        embeddings,
        backfill_embeddings,
        model: model_choice,
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

fn run_embed(model: Option<String>, root: Option<PathBuf>) -> Result<()> {
    const BATCH_SIZE: usize = 256;

    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;

    // Model priority: CLI flag > config file > env var > default
    let model_choice = config.resolve_model(model)?;

    let index = SearchIndex::open_or_create(&paths.index)?;
    let mut embedder = EmbedderHandle::with_model(model_choice)?;
    let mut vector = VectorIndex::open_or_create(&paths.vectors, embedder.dims)?;

    let progress = std::sync::Arc::new(crate::progress::Progress::new([0; 4], [0; 4], true));
    progress.set_embed_ready();

    let mut embedded_counts = [0u64; 4];
    let mut embedded_total = 0u64;
    let mut batch: Vec<(u64, String, crate::types::SourceKind)> = Vec::with_capacity(BATCH_SIZE);

    let flush_batch = |batch: &mut Vec<(u64, String, crate::types::SourceKind)>,
                       embedder: &mut EmbedderHandle,
                       vector: &mut VectorIndex,
                       progress: &crate::progress::Progress,
                       embedded_counts: &mut [u64; 4],
                       embedded_total: &mut u64| {
        if batch.is_empty() {
            return Ok(());
        }
        let texts: Vec<&str> = batch.iter().map(|(_, text, _)| text.as_str()).collect();
        let embeddings = embedder.embed_texts(&texts)?;

        for ((doc_id, _, source), vec) in batch.iter().zip(embeddings.iter()) {
            vector.add(*doc_id, vec)?;
            progress.sub_embed_pending(*source, 1);
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
            progress.add_embed_pending(record.source, 1);
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
        "embedded {} vectors (claude {}, codex {}, history {}, opencode {})",
        embedded_total,
        embedded_counts[crate::types::SourceKind::Claude.idx()],
        embedded_counts[crate::types::SourceKind::CodexSession.idx()],
        embedded_counts[crate::types::SourceKind::CodexHistory.idx()],
        embedded_counts[crate::types::SourceKind::Opencode.idx()],
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
    let model_choice = config.resolve_model(None)?;
    let auto_index_on_search = config.auto_index_on_search_default();
    let embeddings_default = config.embeddings_default();
    let scan_cache_ttl = config.scan_cache_ttl();
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
            include_opencode: true,
            embeddings: embeddings_default,
            backfill_embeddings,
            model: model_choice,
        };
        // Skip indexing if we recently scanned (within TTL)
        let _ = ingest_if_stale(&paths, &index, &opts, scan_cache_ttl)?;
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
            &SearchContext {
                render: &render,
                paths: &paths,
                model_choice,
                recency_weight,
                recency_half_life_days,
            },
        );
    }
    if semantic {
        return run_semantic_search(
            &index,
            &options,
            candidate_limit,
            &SearchContext {
                render: &render,
                paths: &paths,
                model_choice,
                recency_weight,
                recency_half_life_days,
            },
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

struct SearchContext<'a> {
    render: &'a RenderOptions,
    paths: &'a Paths,
    model_choice: ModelChoice,
    recency_weight: f32,
    recency_half_life_days: f32,
}

fn run_semantic_search(
    index: &SearchIndex,
    options: &QueryOptions,
    limit: usize,
    ctx: &SearchContext,
) -> Result<()> {
    let vector = VectorIndex::open(&ctx.paths.vectors)?;
    let mut embedder = EmbedderHandle::with_model(ctx.model_choice)?;
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
                ctx.recency_weight,
                ctx.recency_half_life_days,
            );
            results.push((score, record));
        }
    }
    let results = apply_post_processing(results, ctx.render);
    render_results(results, ctx.render)?;
    Ok(())
}

fn run_hybrid_search(
    index: &SearchIndex,
    options: &QueryOptions,
    limit: usize,
    ctx: &SearchContext,
) -> Result<()> {
    let vector = VectorIndex::open(&ctx.paths.vectors)?;
    let mut embedder = EmbedderHandle::with_model(ctx.model_choice)?;

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
                    apply_recency(
                        score,
                        r.ts,
                        now_ms,
                        ctx.recency_weight,
                        ctx.recency_half_life_days,
                    ),
                    r,
                )
            })
        })
        .collect();
    let merged = apply_post_processing(merged, ctx.render);
    render_results(merged, ctx.render)?;
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

fn run_setup(force: bool) -> Result<()> {
    use dialoguer::{MultiSelect, theme::ColorfulTheme};

    let theme = ColorfulTheme::default();

    // Detect installed tools
    let claude_path = find_in_path("claude");
    let codex_path = find_in_path("codex");
    let opencode_path = find_in_path("opencode");

    if claude_path.is_none() && codex_path.is_none() && opencode_path.is_none() {
        return Err(anyhow!("Neither claude, codex, nor opencode found in PATH"));
    }

    // Show what will be installed
    let action = if force { "install/update" } else { "install" };
    println!("This will {action}:");
    if claude_path.is_some() {
        println!("  Claude Code: memex-search skill, instruction-improver skill");
    }
    if codex_path.is_some() {
        println!("  Codex: memex-search skill");
    }
    if opencode_path.is_some() {
        println!("  Opencode: memex-search skill");
    }
    if force {
        println!();
        println!("(--force: existing files will be overwritten)");
    }
    println!();

    // Build selection list (only installed tools)
    let mut items: Vec<(&str, String)> = Vec::new();
    let mut defaults = Vec::new();

    if let Some(path) = &claude_path {
        items.push(("claude", format!("Claude Code ({})", path.display())));
        defaults.push(true);
    }
    if let Some(path) = &codex_path {
        items.push(("codex", format!("Codex ({})", path.display())));
        defaults.push(true);
    }
    if let Some(path) = &opencode_path {
        items.push(("opencode", format!("Opencode ({})", path.display())));
        defaults.push(true);
    }

    let labels: Vec<&str> = items.iter().map(|(_, label)| label.as_str()).collect();

    let selected = MultiSelect::with_theme(&theme)
        .with_prompt("Select tools to configure")
        .items(&labels)
        .defaults(&defaults)
        .interact()?;

    if selected.is_empty() {
        println!("Nothing selected.");
        return Ok(());
    }

    println!();

    let home = directories::BaseDirs::new()
        .ok_or_else(|| anyhow!("cannot determine home directory"))?
        .home_dir()
        .to_path_buf();

    // Clean up stale skill/prompt files from previous versions
    let stale_paths: Vec<PathBuf> = vec![
        // Gen 1: automem-era paths
        home.join(".claude/skills/automem-search"),
        home.join(".codex/prompts/automem-search.md"),
        home.join(".local/share/opencode/prompts/automem-search.md"),
        // Gen 2: flat-file skill paths (now directory-based)
        home.join(".codex/skills/memex-search.md"),
        home.join(".local/share/opencode/skills/memex-search.md"),
    ];
    for path in &stale_paths {
        if path.is_dir() {
            if let Err(e) = std::fs::remove_dir_all(path) {
                eprintln!("Warning: failed to remove stale {}: {e}", path.display());
            } else {
                println!("Removed stale {}.", path.display());
            }
        } else if path.is_file() {
            if let Err(e) = std::fs::remove_file(path) {
                eprintln!("Warning: failed to remove stale {}: {e}", path.display());
            } else {
                println!("Removed stale {}.", path.display());
            }
        }
    }

    let claude_skill = include_str!("../skills/memex-search/SKILL.md");
    let instruction_improver_skill = include_str!("../skills/instruction-improver/SKILL.md");
    let codex_skill = include_str!("../skills/codex/memex-search/SKILL.md");
    let opencode_skill = include_str!("../skills/opencode/memex-search/SKILL.md");

    for index in selected {
        let (tool, _) = &items[index];
        match *tool {
            "claude" => {
                // Install memex-search skill
                let dest_dir = home.join(".claude").join("skills").join("memex-search");
                let dest = dest_dir.join("SKILL.md");
                if dest.exists() && !force {
                    println!(
                        "Skipping Claude skill (already installed at {}). Use --force to overwrite.",
                        dest.display()
                    );
                } else {
                    std::fs::create_dir_all(&dest_dir)?;
                    std::fs::write(&dest, claude_skill)?;
                    let verb = if dest.exists() {
                        "Updated"
                    } else {
                        "Installed"
                    };
                    println!("{verb} Claude skill at {}.", dest.display());
                }

                // Install instruction-improver skill
                let improver_dir = home
                    .join(".claude")
                    .join("skills")
                    .join("instruction-improver");
                let improver_dest = improver_dir.join("SKILL.md");
                if improver_dest.exists() && !force {
                    println!(
                        "Skipping instruction-improver skill (already installed at {}). Use --force to overwrite.",
                        improver_dest.display()
                    );
                } else {
                    std::fs::create_dir_all(&improver_dir)?;
                    std::fs::write(&improver_dest, instruction_improver_skill)?;
                    let verb = if improver_dest.exists() {
                        "Updated"
                    } else {
                        "Installed"
                    };
                    println!(
                        "{verb} instruction-improver skill at {}.",
                        improver_dest.display()
                    );
                }
            }
            "codex" => {
                let dest_dir = home.join(".codex").join("skills").join("memex-search");
                let dest = dest_dir.join("SKILL.md");
                if dest.exists() && !force {
                    println!(
                        "Skipping Codex skill (already installed at {}). Use --force to overwrite.",
                        dest.display()
                    );
                } else {
                    std::fs::create_dir_all(&dest_dir)?;
                    std::fs::write(&dest, codex_skill)?;
                    let verb = if dest.exists() {
                        "Updated"
                    } else {
                        "Installed"
                    };
                    println!("{verb} Codex skill at {}.", dest.display());
                }
            }
            "opencode" => {
                let dest_dir = home
                    .join(".local")
                    .join("share")
                    .join("opencode")
                    .join("skills")
                    .join("memex-search");
                let dest = dest_dir.join("SKILL.md");
                if dest.exists() && !force {
                    println!(
                        "Skipping Opencode skill (already installed at {}). Use --force to overwrite.",
                        dest.display()
                    );
                } else {
                    std::fs::create_dir_all(&dest_dir)?;
                    std::fs::write(&dest, opencode_skill)?;
                    let verb = if dest.exists() {
                        "Updated"
                    } else {
                        "Installed"
                    };
                    println!("{verb} Opencode skill at {}.", dest.display());
                }
            }
            _ => {}
        }
    }

    println!();
    println!("Done! Restart Claude Code, Codex, or Opencode to pick up changes.");

    Ok(())
}

fn run_share(session_id: String, title: Option<String>, root: Option<PathBuf>) -> Result<()> {
    // Check if agentexport is installed
    let agentexport_path = find_in_path("agentexport");
    if agentexport_path.is_none() {
        return Err(anyhow!(
            "agentexport not found in PATH. Install it with: brew install nicosuave/tap/agentexport"
        ));
    }

    // Open index and find session
    let paths = Paths::new(root)?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    let records = index.records_by_session_id(&session_id)?;

    if records.is_empty() {
        return Err(anyhow!("session not found: {session_id}"));
    }

    // Get source info from first record
    let record = &records[0];
    let tool = match record.source {
        crate::types::SourceKind::Claude => "claude",
        crate::types::SourceKind::CodexSession | crate::types::SourceKind::CodexHistory => "codex",
        crate::types::SourceKind::Opencode => "opencode",
    };
    let source_path = &record.source_path;

    // Build agentexport command
    let mut cmd = std::process::Command::new("agentexport");
    cmd.args(["publish", "--tool", tool, "--transcript", source_path]);
    if let Some(t) = &title {
        cmd.args(["--title", t]);
    }

    // Run command and capture output
    let output = cmd.output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("agentexport failed: {stderr}"));
    }

    // Print the share URL (agentexport prints URL to stdout)
    let url = String::from_utf8_lossy(&output.stdout);
    let url = url.trim();
    if url.is_empty() {
        return Err(anyhow!("agentexport returned no URL"));
    }

    println!("{url}");
    Ok(())
}

fn find_in_path(binary: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(binary);
        if candidate.is_file() && is_executable(&candidate) {
            return Some(candidate);
        }
    }
    None
}

#[cfg(unix)]
fn is_executable(path: &std::path::Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|meta| meta.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(path: &std::path::Path) -> bool {
    path.is_file()
}

#[allow(clippy::too_many_arguments)]
fn run_index_service_enable(
    index: &IndexArgs,
    label: Option<String>,
    continuous: bool,
    poll_interval: Option<u64>,
    interval: Option<u64>,
    stdout: Option<PathBuf>,
    stderr: Option<PathBuf>,
    plist: Option<PathBuf>,
    systemd_dir: Option<PathBuf>,
) -> Result<()> {
    if index.embeddings && index.no_embeddings {
        return Err(anyhow!(
            "--embeddings and --no-embeddings cannot be used together"
        ));
    }
    if continuous && interval.is_some() {
        return Err(anyhow!(
            "--continuous and --interval cannot be used together"
        ));
    }

    let paths = Paths::new(index.root.clone())?;
    let config = UserConfig::load(&paths)?;
    let cli_continuous = continuous || poll_interval.is_some();
    let config_continuous = match config.index_service_mode() {
        Some("interval") => false,
        Some("continuous") => true,
        Some(other) => {
            return Err(anyhow!(
                "invalid index_service_mode: {other} (expected \"interval\" or \"continuous\")"
            ));
        }
        None => config.index_service_continuous_default(),
    };
    let continuous = if cli_continuous {
        true
    } else if interval.is_some() {
        false
    } else {
        config_continuous
    };
    let poll_interval = poll_interval.unwrap_or(config.index_service_poll_interval());
    let interval = interval.unwrap_or(config.index_service_interval());

    let exe = std::env::current_exe()?;
    let program_args = build_index_command_args(index, continuous, poll_interval);

    std::fs::create_dir_all(&paths.root)?;

    if cfg!(target_os = "macos") {
        run_index_service_enable_launchd(
            &config,
            &paths,
            label,
            continuous,
            interval,
            stdout,
            stderr,
            plist,
            &exe,
            &program_args,
        )
    } else if cfg!(target_os = "linux") {
        run_index_service_enable_systemd(
            &config,
            label,
            continuous,
            interval,
            poll_interval,
            systemd_dir,
            &exe,
            &program_args,
        )
    } else {
        Err(anyhow!(
            "background service scheduling is only supported on macOS and Linux"
        ))
    }
}

#[allow(clippy::too_many_arguments)]
fn run_index_service_enable_launchd(
    config: &UserConfig,
    paths: &Paths,
    label: Option<String>,
    continuous: bool,
    interval: u64,
    stdout: Option<PathBuf>,
    stderr: Option<PathBuf>,
    plist: Option<PathBuf>,
    exe: &std::path::Path,
    program_args: &[String],
) -> Result<()> {
    let default_label = default_index_service_label();
    let default_plist = default_index_service_plist(&paths.root);
    let label = label
        .or_else(|| config.index_service_label.clone())
        .unwrap_or(default_label);
    let stdout = stdout
        .or_else(|| config.index_service_stdout.clone())
        .unwrap_or(default_index_service_stdout(&paths.root));
    let stderr = stderr
        .or_else(|| config.index_service_stderr.clone())
        .unwrap_or(default_index_service_stderr(&paths.root));
    let plist_path = plist
        .or_else(|| config.index_service_plist.clone())
        .unwrap_or(default_plist);
    validate_service_label(&label)?;

    if let Some(parent) = plist_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut full_args = vec![exe.to_string_lossy().to_string()];
    full_args.extend(program_args.iter().cloned());

    let (interval, keep_alive) = if continuous {
        (None, true)
    } else {
        (Some(interval), false)
    };

    let contents = build_launchd_plist(
        &label,
        &full_args,
        interval,
        keep_alive,
        Some(&stdout),
        Some(&stderr),
    );
    std::fs::write(&plist_path, contents)?;

    println!("wrote launchd plist: {}", plist_path.display());
    let status = std::process::Command::new("launchctl")
        .arg("load")
        .arg("-w")
        .arg(&plist_path)
        .status()?;
    if !status.success() {
        return Err(anyhow!("launchctl load failed"));
    }
    println!("enabled launchd job: {label}");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_index_service_enable_systemd(
    config: &UserConfig,
    label: Option<String>,
    continuous: bool,
    interval: u64,
    _poll_interval: u64,
    systemd_dir: Option<PathBuf>,
    exe: &std::path::Path,
    program_args: &[String],
) -> Result<()> {
    let systemd_dir = systemd_dir
        .or_else(|| config.index_service_systemd_dir.clone())
        .unwrap_or_else(default_systemd_user_dir);
    let label = label
        .or_else(|| config.index_service_label.clone())
        .unwrap_or_else(|| "memex-index".to_string());
    validate_service_label(&label)?;

    std::fs::create_dir_all(&systemd_dir)?;

    let service_path = systemd_dir.join(format!("{}.service", label));
    let timer_path = systemd_dir.join(format!("{}.timer", label));

    let service_contents = build_systemd_service(&exe.to_string_lossy(), program_args, continuous);
    std::fs::write(&service_path, service_contents)?;
    println!("wrote systemd service: {}", service_path.display());

    // For interval mode, create a timer unit
    if !continuous {
        let timer_contents = build_systemd_timer(interval);
        std::fs::write(&timer_path, timer_contents)?;
        println!("wrote systemd timer: {}", timer_path.display());
    }

    // Reload systemd user daemon
    let status = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status()?;
    if !status.success() {
        return Err(anyhow!("systemctl daemon-reload failed"));
    }

    // Enable and start the appropriate unit
    if continuous {
        let status = std::process::Command::new("systemctl")
            .args(["--user", "enable", "--now", &format!("{}.service", label)])
            .status()?;
        if !status.success() {
            return Err(anyhow!("systemctl enable failed"));
        }
        println!("enabled systemd service: {}", label);
    } else {
        let status = std::process::Command::new("systemctl")
            .args(["--user", "enable", "--now", &format!("{}.timer", label)])
            .status()?;
        if !status.success() {
            return Err(anyhow!("systemctl enable failed"));
        }
        println!("enabled systemd timer: {}", label);
    }

    Ok(())
}

fn run_index_service_disable(
    label: Option<String>,
    plist: Option<PathBuf>,
    systemd_dir: Option<PathBuf>,
    root: Option<PathBuf>,
) -> Result<()> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;

    if cfg!(target_os = "macos") {
        run_index_service_disable_launchd(&config, &paths, label, plist)
    } else if cfg!(target_os = "linux") {
        run_index_service_disable_systemd(&config, label, systemd_dir)
    } else {
        Err(anyhow!(
            "background service scheduling is only supported on macOS and Linux"
        ))
    }
}

fn run_index_service_disable_launchd(
    config: &UserConfig,
    paths: &Paths,
    label: Option<String>,
    plist: Option<PathBuf>,
) -> Result<()> {
    let default_label = default_index_service_label();
    let default_plist = default_index_service_plist(&paths.root);
    let label = label
        .or_else(|| config.index_service_label.clone())
        .unwrap_or(default_label);
    let plist_path = plist
        .or_else(|| config.index_service_plist.clone())
        .unwrap_or(default_plist);
    validate_service_label(&label)?;
    if !plist_path.exists() {
        println!("no launchd plist found: {}", plist_path.display());
        return Ok(());
    }
    let status = std::process::Command::new("launchctl")
        .arg("unload")
        .arg("-w")
        .arg(&plist_path)
        .status()?;
    if !status.success() {
        return Err(anyhow!("launchctl unload failed"));
    }
    std::fs::remove_file(&plist_path)?;
    println!("disabled launchd job: {label}");
    Ok(())
}

fn run_index_service_disable_systemd(
    config: &UserConfig,
    label: Option<String>,
    systemd_dir: Option<PathBuf>,
) -> Result<()> {
    let systemd_dir = systemd_dir
        .or_else(|| config.index_service_systemd_dir.clone())
        .unwrap_or_else(default_systemd_user_dir);
    let label = label
        .or_else(|| config.index_service_label.clone())
        .unwrap_or_else(|| "memex-index".to_string());
    validate_service_label(&label)?;

    let service_path = systemd_dir.join(format!("{}.service", label));
    let timer_path = systemd_dir.join(format!("{}.timer", label));

    // Stop and disable timer if it exists
    if timer_path.exists() {
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "disable", "--now", &format!("{}.timer", label)])
            .status();
        std::fs::remove_file(&timer_path)?;
        println!("removed systemd timer: {}", timer_path.display());
    }

    // Stop and disable service if it exists
    if service_path.exists() {
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "disable", "--now", &format!("{}.service", label)])
            .status();
        std::fs::remove_file(&service_path)?;
        println!("removed systemd service: {}", service_path.display());
    }

    if !timer_path.exists() && !service_path.exists() {
        println!("no systemd units found for: {}", label);
        return Ok(());
    }

    // Reload daemon
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status();

    println!("disabled systemd service: {}", label);
    Ok(())
}

fn validate_service_label(label: &str) -> Result<()> {
    if label.trim().is_empty() {
        return Err(anyhow!("service label cannot be empty"));
    }
    if label.contains('/') || label.contains('\\') {
        return Err(anyhow!("service label cannot contain path separators"));
    }
    Ok(())
}

fn build_index_command_args(
    index: &IndexArgs,
    continuous: bool,
    poll_interval: u64,
) -> Vec<String> {
    let mut args = Vec::new();
    args.push("index".to_string());

    if let Some(source) = &index.source {
        args.push("--source".to_string());
        args.push(source.to_string_lossy().to_string());
    }
    if index.include_agents {
        args.push("--include-agents".to_string());
    }
    if !index.codex {
        args.push("--no-codex".to_string());
    }
    if !index.opencode {
        args.push("--no-opencode".to_string());
    }
    if index.embeddings {
        args.push("--embeddings".to_string());
    }
    if index.no_embeddings {
        args.push("--no-embeddings".to_string());
    }
    if continuous {
        args.push("--watch".to_string());
        args.push("--watch-interval".to_string());
        args.push(format!("{poll_interval}"));
    }
    if let Some(model) = &index.model {
        args.push("--model".to_string());
        args.push(model.clone());
    }
    if let Some(root) = &index.root {
        args.push("--root".to_string());
        args.push(root.to_string_lossy().to_string());
    }
    args
}

fn build_launchd_plist(
    label: &str,
    program_args: &[String],
    interval: Option<u64>,
    keep_alive: bool,
    stdout: Option<&PathBuf>,
    stderr: Option<&PathBuf>,
) -> String {
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str(
        "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n",
    );
    out.push_str("<plist version=\"1.0\">\n");
    out.push_str("<dict>\n");
    out.push_str("  <key>Label</key>\n");
    out.push_str(&format!("  <string>{}</string>\n", xml_escape(label)));
    out.push_str("  <key>ProgramArguments</key>\n");
    out.push_str("  <array>\n");
    for arg in program_args {
        out.push_str(&format!("    <string>{}</string>\n", xml_escape(arg)));
    }
    out.push_str("  </array>\n");
    out.push_str("  <key>RunAtLoad</key>\n");
    out.push_str("  <true/>\n");
    if let Some(interval) = interval {
        out.push_str("  <key>StartInterval</key>\n");
        out.push_str(&format!("  <integer>{interval}</integer>\n"));
    }
    if keep_alive {
        out.push_str("  <key>KeepAlive</key>\n");
        out.push_str("  <true/>\n");
    }

    if let Some(stdout) = stdout {
        out.push_str("  <key>StandardOutPath</key>\n");
        out.push_str(&format!(
            "  <string>{}</string>\n",
            xml_escape(&stdout.to_string_lossy())
        ));
    }
    if let Some(stderr) = stderr {
        out.push_str("  <key>StandardErrorPath</key>\n");
        out.push_str(&format!(
            "  <string>{}</string>\n",
            xml_escape(&stderr.to_string_lossy())
        ));
    }

    out.push_str("</dict>\n");
    out.push_str("</plist>\n");
    out
}

fn xml_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

fn default_index_service_label() -> String {
    "com.memex.index".to_string()
}

fn default_index_service_stdout(root: &std::path::Path) -> PathBuf {
    root.join("index-service.log")
}

fn default_index_service_stderr(root: &std::path::Path) -> PathBuf {
    root.join("index-service.err.log")
}

fn default_index_service_plist(root: &std::path::Path) -> PathBuf {
    root.join("index-service.plist")
}

fn default_systemd_user_dir() -> PathBuf {
    if let Some(base) = directories::BaseDirs::new() {
        base.config_dir().join("systemd/user")
    } else {
        PathBuf::from("/tmp/systemd/user")
    }
}

fn build_systemd_service(exe_path: &str, program_args: &[String], continuous: bool) -> String {
    let exec_start = if program_args.is_empty() {
        exe_path.to_string()
    } else {
        format!("{} {}", exe_path, program_args.join(" "))
    };

    let mut out = String::new();
    out.push_str("[Unit]\n");
    out.push_str("Description=Memex Index Service\n");
    out.push('\n');
    out.push_str("[Service]\n");
    out.push_str("Type=");
    if continuous {
        out.push_str("simple\n");
        out.push_str("Restart=always\n");
        out.push_str("RestartSec=10\n");
    } else {
        out.push_str("oneshot\n");
    }
    out.push_str(&format!("ExecStart={}\n", exec_start));
    out.push('\n');
    out.push_str("[Install]\n");
    if continuous {
        out.push_str("WantedBy=default.target\n");
    }
    out
}

fn build_systemd_timer(interval: u64) -> String {
    let mut out = String::new();
    out.push_str("[Unit]\n");
    out.push_str("Description=Memex Index Timer\n");
    out.push('\n');
    out.push_str("[Timer]\n");
    out.push_str("OnBootSec=5min\n");
    out.push_str(&format!("OnUnitActiveSec={}s\n", interval));
    out.push('\n');
    out.push_str("[Install]\n");
    out.push_str("WantedBy=timers.target\n");
    out
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

const REPO: &str = "nicosuave/memex";

fn is_homebrew_install() -> bool {
    std::env::current_exe()
        .ok()
        .and_then(|p| {
            p.to_str()
                .map(|s| s.contains("/Cellar/") || s.contains("/homebrew/"))
        })
        .unwrap_or(false)
}

fn run_update(skip_confirm: bool) -> Result<()> {
    if is_homebrew_install() {
        println!("memex was installed via Homebrew.");
        println!("Run 'brew upgrade memex' to update.");
        return Ok(());
    }

    let current = env!("CARGO_PKG_VERSION");
    let latest = fetch_latest_version()?;

    if !is_newer_version(current, &latest) {
        println!("memex is already up to date (v{current})");
        return Ok(());
    }

    println!("Current version: v{current}");
    println!("Latest version:  v{latest}");
    println!();

    if !skip_confirm {
        use dialoguer::{Confirm, theme::ColorfulTheme};
        let confirm = Confirm::with_theme(&ColorfulTheme::default())
            .with_prompt(format!("Update to v{latest}?"))
            .default(true)
            .interact()?;
        if !confirm {
            println!("Update cancelled.");
            return Ok(());
        }
    }

    let (os, arch) = detect_platform()?;
    let url = format!(
        "https://github.com/{REPO}/releases/download/v{latest}/memex-{latest}-{os}-{arch}.tar.gz"
    );

    println!("Downloading {url}...");

    let tmp_dir = tempfile::tempdir()?;
    let archive_path = tmp_dir.path().join("memex.tar.gz");

    // Download using curl
    let status = std::process::Command::new("curl")
        .args(["-fsSL", "-o"])
        .arg(&archive_path)
        .arg(&url)
        .status()?;
    if !status.success() {
        return Err(anyhow!("Failed to download release"));
    }

    // Extract
    let status = std::process::Command::new("tar")
        .args(["-xzf"])
        .arg(&archive_path)
        .arg("-C")
        .arg(tmp_dir.path())
        .status()?;
    if !status.success() {
        return Err(anyhow!("Failed to extract release"));
    }

    let new_binary = tmp_dir.path().join("memex");
    if !new_binary.exists() {
        return Err(anyhow!("Binary not found in release archive"));
    }

    // Replace current binary
    let current_exe = std::env::current_exe()?;
    let backup = current_exe.with_extension("old");

    // Move current to backup, move new to current
    if backup.exists() {
        std::fs::remove_file(&backup)?;
    }
    std::fs::rename(&current_exe, &backup)?;
    std::fs::copy(&new_binary, &current_exe)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&current_exe, std::fs::Permissions::from_mode(0o755))?;
    }

    // Remove backup
    let _ = std::fs::remove_file(&backup);

    println!("Updated memex to v{latest}");
    println!();
    println!("Run 'memex setup --force' to update installed skills/prompts.");
    Ok(())
}

fn fetch_latest_version() -> Result<String> {
    let output = std::process::Command::new("curl")
        .args([
            "-fsSL",
            &format!("https://api.github.com/repos/{REPO}/releases/latest"),
        ])
        .output()?;

    if !output.status.success() {
        return Err(anyhow!("Failed to fetch latest version"));
    }

    let json: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let tag = json["tag_name"]
        .as_str()
        .ok_or_else(|| anyhow!("No tag_name in release"))?;

    Ok(tag.trim_start_matches('v').to_string())
}

fn detect_platform() -> Result<(&'static str, &'static str)> {
    let os = if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "linux") {
        "linux"
    } else {
        return Err(anyhow!("Unsupported OS"));
    };

    let arch = if cfg!(target_arch = "x86_64") {
        "x86_64"
    } else if cfg!(target_arch = "aarch64") {
        "arm64"
    } else {
        return Err(anyhow!("Unsupported architecture"));
    };

    Ok((os, arch))
}

/// Check for updates in the background and print a warning if outdated.
/// This is non-blocking and fails silently.
pub fn check_for_update_async(sender: Option<std::sync::mpsc::Sender<String>>) {
    let is_brew = is_homebrew_install();
    std::thread::spawn(move || {
        if let Ok(latest) = fetch_latest_version() {
            let current = env!("CARGO_PKG_VERSION");
            if is_newer_version(current, &latest) {
                let upgrade_cmd = if is_brew {
                    "brew upgrade memex"
                } else {
                    "memex update"
                };
                if let Some(sender) = sender {
                    let message = format!("update: v{latest} ({upgrade_cmd})");
                    let _ = sender.send(message);
                } else {
                    eprintln!(
                        "\x1b[33mA new version of memex is available: v{latest} (current: v{current})\x1b[0m"
                    );
                    eprintln!("\x1b[33mRun '{upgrade_cmd}' to upgrade.\x1b[0m");
                }
            }
        }
    });
}

fn is_newer_version(current: &str, latest: &str) -> bool {
    let Some(current) = parse_version_parts(current) else {
        return false;
    };
    let Some(latest) = parse_version_parts(latest) else {
        return false;
    };
    latest > current
}

fn parse_version_parts(value: &str) -> Option<(u64, u64, u64)> {
    let mut parts: Vec<u64> = Vec::with_capacity(3);
    let mut buf = String::new();
    for ch in value.chars() {
        if ch.is_ascii_digit() {
            buf.push(ch);
        } else if !buf.is_empty() {
            parts.push(buf.parse().ok()?);
            buf.clear();
            if parts.len() == 3 {
                break;
            }
        }
    }
    if !buf.is_empty() && parts.len() < 3 {
        parts.push(buf.parse().ok()?);
    }
    if parts.len() < 3 {
        return None;
    }
    Some((parts[0], parts[1], parts[2]))
}
