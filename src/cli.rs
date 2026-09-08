use crate::analytics::{AnalyticsStore, analytics_path, backfill_from_index};
use crate::config::{Paths, UserConfig, default_claude_sources};
use crate::embed::EmbedderHandle;
use crate::index::{QueryOptions, SearchIndex, SessionScopeKey};
use crate::ingest::{IngestOptions, ingest_all};
use crate::lease::{INGEST_LEASE_TIMEOUT, IngestLease};
use crate::machine::{
    BoundedRecord, LocatedMemoryHit, LocatedRecord, MAX_HYDRATE_INPUT_BYTES,
    MAX_HYDRATE_LINE_BYTES, MAX_SESSION_BATCH_SIZE, MAX_SESSION_PAGE_SIZE, SearchMode, SearchSpec,
    SessionPageRequest, UsageSpec, federated_memory_search, federated_search, federated_usage,
    read_context, read_memory, read_record, read_session_pages, session_page_context,
};
use crate::memory::{MemoryFreshness, MemoryStore};
use crate::memory_search::{
    MAX_MEMORY_READ_CHARS, MemoryReadRequest, MemoryReadValue, MemorySearchMode,
    MemorySearchOptions, embed_memory, gc_memory_vectors,
};
use crate::read_budget::{ContentPage, DEFAULT_MAX_CHARS, ReadBudget, ReadField};
use crate::retrieval::canonical_record_id;
use crate::retrieval::{ContextOptions, ContextSelector};
use crate::retrieval_eval::{
    EvaluationDataset, RetrievalTrace, RetrievalTraceMetadata, TraceQuery, append_trace,
    fuse_ranked_queries, mean_reciprocal_rank, ndcg_at_k, recall_at_k, unique_sessions_at_k,
};
use crate::transfer::{
    TransferMode as CoreTransferMode, TransferOptions, TransferTarget as CoreTransferTarget,
    transfer_session,
};
use crate::tui;
use crate::types::{RecordLinks, SourceFilter};
use crate::usage::{CostMode, UsageQuery, scan_usage};
use crate::vector::VectorIndex;
use anyhow::{Context, Result, anyhow};
use chrono::SecondsFormat;
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use regex::RegexBuilder;
use rmcp::schemars::{self, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::{IsTerminal, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Duration;
use std::time::Instant;
use toml_edit::{DocumentMut, Item as TomlItem, value};

mod surface;
use surface::{
    CliSearchMode, DaemonMcpArgs, DebugCommand, IndexCommand, IndexSource, OutputArgs,
    OutputFormat, OutputOptions, SessionCommand, WebCommand,
};

static TRACE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Parser)]
#[command(
    name = "memex",
    version,
    help_template = "{about-with-newline}\nUsage: {usage}\n\nFind and read:\n  search       Search history and memories\n  sessions     List sessions\n  projects     List project counts and activity\n  machines     List configured machines\n  session      Read a session or batch of pages\n  show         Read a record or memory\n  context      Read surrounding records\n\nBrowse and reuse:\n  tui          Browse interactively (also the default)\n  web          Serve or open the browser\n  share        Share a session\n  transfer     Transfer a session to another agent\n\nIndex and operate:\n  index        Index history and memories; rebuild, gc, embed, stats\n  daemon       Run indexing, web, and MCP together\n  usage        Report token usage and cost\n\nIntegrate and maintain:\n  mcp          Run the MCP server\n  skill        Manage the bundled search skill\n  update       Update Memex and installed skills\n  debug        Retrieval evaluation\n  help         Show command help\n\nOptions:\n{options}\n{after-help}",
    about = "Search, browse, and reuse local agent history and memory",
    after_help = "\
QUICK START:
    memex                           # Browse sessions interactively
    memex index                     # Index your agent history
    memex search \"error handling\"   # Search for keywords

LEARN MORE:
    memex <command> --help          # Detailed help for each command"
)]
pub struct Cli {
    /// Never prompt or open the TUI (agents may use this even with a PTY)
    #[arg(long, global = true)]
    non_interactive: bool,
    /// Skip release availability checks; does not disable stale-skill warnings
    #[arg(long, global = true)]
    no_update_check: bool,
    /// Defaults to the interactive TUI when no command is given
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Args, Clone)]
struct IndexArgs {
    /// Path to Claude projects directory [default: CLAUDE_CONFIG_DIR or ~/.claude/projects]
    #[arg(
        long = "claude-path",
        alias = "source",
        value_name = "PATH",
        help_heading = "Sources"
    )]
    source: Option<PathBuf>,
    /// Index only these providers (repeatable); exclusions take precedence
    #[arg(long, value_enum, value_name = "SOURCE", help_heading = "Sources")]
    only_source: Vec<IndexSource>,
    /// Skip these providers (repeatable)
    #[arg(long, value_enum, value_name = "SOURCE", help_heading = "Sources")]
    exclude_source: Vec<IndexSource>,
    /// Deprecated no-op (kept for compatibility): agent subprocess
    /// conversations are always indexed now; filter them at query time
    #[arg(long, hide = true)]
    include_agents: bool,
    /// Index plaintext reasoning as BM25-only records (encrypted/redacted reasoning is always dropped)
    #[arg(long, help_heading = "Sources")]
    include_reasoning: bool,
    /// Index Codex sessions from ~/.codex [default: true]
    #[arg(long, default_value_t = true, hide = true)]
    codex: bool,
    /// Skip indexing Codex sessions
    #[arg(long = "no-codex", default_value_t = false, hide = true)]
    no_codex: bool,
    /// Index Opencode sessions from ~/.local/share/opencode [default: true]
    #[arg(long, default_value_t = true, hide = true)]
    opencode: bool,
    /// Skip indexing Opencode sessions
    #[arg(long = "no-opencode", default_value_t = false, hide = true)]
    no_opencode: bool,
    /// Index Cursor agent transcripts from ~/.cursor/projects [default: true]
    #[arg(long = "no-cursor", action = clap::ArgAction::SetFalse, default_value_t = true, hide = true)]
    cursor: bool,
    /// Index Pi sessions from ~/.pi/agent/sessions or $PI_CODING_AGENT_DIR/sessions [default: true]
    #[arg(long, default_value_t = true, hide = true)]
    pi: bool,
    /// Skip indexing Pi sessions
    #[arg(long = "no-pi", default_value_t = false, hide = true)]
    no_pi: bool,
    /// Index Oh My Pi sessions from ~/.omp/agent/sessions [default: true]
    #[arg(long, default_value_t = true, hide = true)]
    omp: bool,
    /// Skip indexing Oh My Pi sessions
    #[arg(long = "no-omp", default_value_t = false, hide = true)]
    no_omp: bool,
    /// Index OpenClaw sessions from ~/.openclaw or ~/.clawdbot [default: true]
    #[arg(long, default_value_t = true, hide = true)]
    openclaw: bool,
    /// Skip indexing OpenClaw sessions
    #[arg(long = "no-openclaw", default_value_t = false, hide = true)]
    no_openclaw: bool,
    /// Index GitHub Copilot CLI sessions from ~/.copilot [default: true]
    #[arg(long, default_value_t = true, hide = true)]
    copilot: bool,
    /// Skip indexing GitHub Copilot CLI sessions
    #[arg(long = "no-copilot", default_value_t = false, hide = true)]
    no_copilot: bool,
    /// Index Grok sessions from ~/.grok/sessions [default: true]
    #[arg(long, default_value_t = true, hide = true)]
    grok: bool,
    /// Skip indexing Grok sessions
    #[arg(long = "no-grok", default_value_t = false, hide = true)]
    no_grok: bool,
    /// Index Jcode sessions from ~/.jcode/sessions [default: true]
    #[arg(long, default_value_t = true, hide = true)]
    jcode: bool,
    /// Skip indexing Jcode sessions
    #[arg(long = "no-jcode", default_value_t = false, hide = true)]
    no_jcode: bool,
    /// Index Muse sessions from ~/.local/share/muse/sessions [default: true]
    #[arg(long, default_value_t = true, hide = true)]
    muse: bool,
    /// Skip indexing Muse sessions
    #[arg(long = "no-muse", default_value_t = false, hide = true)]
    no_muse: bool,
    /// Generate embeddings for semantic search during indexing
    #[arg(long, help_heading = "Embeddings")]
    embeddings: bool,
    /// Skip embedding generation (overrides config default)
    #[arg(long, help_heading = "Embeddings")]
    no_embeddings: bool,
    /// Embedding model: minilm (fast), bge, nomic, gemma (default, best quality), potion (tiny)
    #[arg(long, help_heading = "Embeddings")]
    model: Option<String>,
    /// Path to memex data directory [default: ~/.memex]
    #[arg(long, help_heading = "Storage")]
    root: Option<PathBuf>,
    /// Print aggregate parser diagnostics without transcript content
    #[arg(long, help_heading = "Diagnostics")]
    diagnostics: bool,
    /// Exclude transcripts whose source path matches this glob (repeatable).
    /// Matched transcripts are never indexed. Also configurable via
    /// `exclude_paths` in ~/.memex/config.toml.
    #[arg(long = "exclude", value_name = "GLOB", help_heading = "Sources")]
    exclude: Vec<String>,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Commands {
    /// Index local agent history, or maintain the index
    #[command(
        args_conflicts_with_subcommands = true,
        after_help = "\
EXAMPLES:
    memex index                         # Index all supported local history
    memex index --embeddings            # Also generate embeddings for semantic search
    memex index --exclude '<glob>'        # Skip paths matching a glob
    memex index --claude-path ~/custom/path  # Use custom Claude projects directory"
    )]
    Index {
        #[command(subcommand)]
        action: Option<IndexCommand>,
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
        #[arg(long, hide = true)]
        web_ui: bool,
        #[arg(long, hide = true, value_name = "ADDRESS")]
        web_listen: Option<String>,
        #[arg(long, hide = true, conflicts_with = "no_mcp")]
        mcp: bool,
        #[arg(long, hide = true, conflicts_with = "mcp_listen")]
        no_mcp: bool,
        #[arg(long, hide = true)]
        mcp_listen: Option<std::net::SocketAddr>,
    },
    /// Delete existing index and rebuild from scratch
    #[command(hide = true)]
    Reindex {
        #[command(flatten)]
        index: IndexArgs,
    },
    /// Reclaim unreachable immutable index generations without rebuilding
    #[command(hide = true)]
    IndexGc {
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
        /// Report what would be removed without changing the index
        #[arg(long)]
        dry_run: bool,
        /// Confirm the daemon and all Memex readers are stopped
        #[arg(long)]
        offline: bool,
    },
    /// Generate embeddings for semantic search (requires existing index)
    #[command(hide = true)]
    Embed {
        /// Embedding model: minilm (fast), bge, nomic, gemma (default, best quality), potion (tiny)
        #[arg(long)]
        model: Option<String>,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Search indexed conversation history and memory
    #[command(after_help = "\
EXAMPLES:
    memex search \"error handling\"
    memex search \"release checklist\" --content memories
    memex search \"authentication decision\" --content all
    memex search \"API design\" --source claude --limit 50
    memex search \"auth\" --since 2024-01-01T00:00:00Z --mode semantic
    memex search \"bug\" --fields score,session_id,snippet --format json

TIMESTAMP FORMAT:
    RFC3339: 2024-01-15T10:30:00Z or 2024-01-15T10:30:00-05:00
    Unix seconds: 1705315800
    Unix milliseconds: 1705315800000

OUTPUT FIELDS (--fields):
    kind, machine, score, ts, doc_id, record_id, project, role, session_id, source, source_path, text, snippet, matches
    memory_id, content_version, section_ref, title, heading, cwd, mtime_ms, event_dates, start_line, end_line, document_kind, refs, freshness, changed_since_search
    event_id, parent_event_id, logical_parent_event_id, parent_session_id, thread_source, conversation_kind
    parent_tool_use_id, source_tool_use_id, source_tool_assistant_uuid")]
    Search {
        /// Search query (keywords or natural language for semantic search)
        query: String,
        /// Content corpus to search (conversation history by default)
        #[arg(long, value_enum, default_value_t = SearchContent::Conversations, help_heading = "Search")]
        content: SearchContent,
        /// Additional independent query view to fuse with reciprocal-rank fusion (repeatable)
        #[arg(long = "query", value_name = "QUERY", help_heading = "Tuning")]
        additional_queries: Vec<String>,
        /// Restrict results to sessions from this working directory/repository
        #[arg(long, value_name = "PATH", help_heading = "Filters")]
        cwd: Option<PathBuf>,
        /// Filter by project name
        #[arg(long, help_heading = "Filters")]
        project: Option<String>,
        /// Filter by role (user, assistant, tool_use, tool_result)
        #[arg(long, help_heading = "Filters")]
        role: Option<String>,
        /// Filter by tool name (e.g., Read, Edit, Bash)
        #[arg(long, help_heading = "Filters")]
        tool: Option<String>,
        /// Filter by session ID
        #[arg(long, help_heading = "Filters")]
        session: Option<String>,
        /// Filter by source: claude, codex, cursor, opencode, pi, omp (Oh My Pi), openclaw, copilot, grok, hermes, jcode, or muse
        #[arg(long, help_heading = "Filters")]
        source: Option<SourceFilter>,
        /// Filter by origin: regular (default), interactive, subagent, or all (includes permission reviews)
        #[arg(long, value_enum, default_value_t = SessionOrigin::Regular, help_heading = "Filters")]
        origin: SessionOrigin,
        /// Retrieval mode (default: lexical)
        #[arg(long, value_enum, conflicts_with_all = ["semantic", "hybrid"], help_heading = "Search")]
        mode: Option<CliSearchMode>,
        /// Use semantic (embedding-based) search instead of keyword search
        #[arg(long, hide = true)]
        semantic: bool,
        /// Use hybrid search combining BM25 keyword and semantic scores
        #[arg(long, hide = true)]
        hybrid: bool,
        /// Minimum score threshold to include in results
        #[arg(long, help_heading = "Tuning")]
        min_score: Option<f32>,
        /// Weight for recency boost (0 = no boost, higher = more recent preferred)
        #[arg(long, default_value_t = 1.0, help_heading = "Tuning")]
        recency_weight: f32,
        /// Half-life in days for recency decay (lower = faster decay)
        #[arg(long, default_value_t = 30.0, help_heading = "Tuning")]
        recency_half_life_days: f32,
        /// Only include results after this timestamp (RFC3339 or unix seconds/ms)
        #[arg(long, value_name = "TIMESTAMP", help_heading = "Filters")]
        since: Option<String>,
        /// Only include results before this timestamp (RFC3339 or unix seconds/ms)
        #[arg(long, value_name = "TIMESTAMP", help_heading = "Filters")]
        until: Option<String>,
        /// Maximum number of results to return
        #[arg(long, default_value_t = 20, help_heading = "Results")]
        limit: usize,
        /// Limit results per session (useful for getting variety)
        #[arg(long = "top-n-per-session", value_name = "N", help_heading = "Results")]
        top_n_per_session: Option<usize>,
        /// Return at most one result per session (shorthand for --top-n-per-session 1)
        #[arg(long, help_heading = "Results")]
        unique_session: bool,
        /// Output results as a single JSON array instead of newline-delimited JSON
        #[arg(long, hide = true)]
        json_array: bool,
        /// Search output encoding (JSONL by default)
        #[arg(long, value_enum, default_value = "jsonl", conflicts_with_all = ["json_array", "verbose"], help_heading = "Output")]
        format: SearchFormat,
        /// Pretty-print JSON (requires --format json)
        #[arg(long, help_heading = "Output")]
        pretty: bool,
        /// Comma-separated list of fields to include in output
        #[arg(long, value_name = "FIELDS", help_heading = "Output")]
        fields: Option<String>,
        /// Include full record text and all metadata (legacy search output)
        #[arg(long, conflicts_with = "fields", help_heading = "Output")]
        full: bool,
        /// Sort results by score or timestamp
        #[arg(long, value_enum, default_value = "score", help_heading = "Results")]
        sort: SortBy,
        /// Show verbose output with inline text preview
        #[arg(short, long, hide = true)]
        verbose: bool,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long, help_heading = "Scope")]
        root: Option<PathBuf>,
        /// Machine to search (repeatable). Defaults to multi_machine.default or all configured machines.
        #[arg(long, value_name = "ID", help_heading = "Scope")]
        machine: Vec<String>,
        /// Persist a metadata-only retrieval trace and print its ID to stderr
        #[arg(long, help_heading = "Tuning")]
        trace: bool,
    },
    /// Interactive terminal UI for browsing sessions
    Tui {
        /// Start with this search query
        #[arg(long)]
        query: Option<String>,
        /// Start with this project filter
        #[arg(long)]
        project: Option<String>,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Serve the local conversation browser
    #[command(
        args_conflicts_with_subcommands = true,
        after_help = "\
EXAMPLES:
    memex web
    memex web --listen 127.0.0.1:8080"
    )]
    Web {
        #[command(subcommand)]
        action: Option<WebCommand>,
        /// Address and port to bind
        #[arg(long, default_value = crate::web::DEFAULT_LISTEN)]
        listen: String,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Manage the Memex daemon: indexing, web UI, and MCP
    #[command(name = "daemon", aliases = ["service", "index-service"])]
    IndexService {
        #[command(subcommand)]
        action: IndexServiceCommand,
    },
    /// Read a session, or batch-read session pages
    #[command(subcommand_negates_reqs = true, args_conflicts_with_subcommands = true)]
    Session {
        #[command(subcommand)]
        action: Option<SessionCommand>,
        /// Session ID (use -- before an ID named batch or help)
        #[arg(required = true)]
        session_id: Option<String>,
        /// Originating machine for federated search results
        #[arg(long, default_value = crate::machine::LOCAL_MACHINE_ID)]
        machine: String,
        /// Read only this source transcript path
        #[arg(long)]
        source_path: Option<String>,
        /// Number of records to skip before the page
        #[arg(long, default_value_t = 0)]
        offset: usize,
        /// Return at most this many records (default 50, maximum 500; --full defaults to all)
        #[arg(long)]
        limit: Option<usize>,
        /// Show human-readable output with timestamps and role labels
        #[arg(short, long, hide = true)]
        verbose: bool,
        #[command(flatten)]
        read: ReadArgs,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Read a record or memory document by stable ID
    Show {
        /// Document ID (from search results)
        #[arg(required_unless_present_any = ["record_id", "memory_id"], conflicts_with_all = ["record_id", "memory_id"])]
        doc_id: Option<u64>,
        /// Stable canonical record ID from search output
        #[arg(long, conflicts_with = "memory_id")]
        record_id: Option<String>,
        /// Stable memory document ID from memory search output
        #[arg(long)]
        memory_id: Option<String>,
        /// Read one section of a memory document
        #[arg(long, requires = "memory_id")]
        section: Option<String>,
        /// Version returned by search; reports if the document changed before this read
        #[arg(long, requires = "memory_id")]
        content_version: Option<String>,
        /// Select a content field to continue reading
        #[arg(long, value_enum, conflicts_with = "memory_id")]
        field: Option<ReadField>,
        /// Unicode character offset within the selected record field or memory document/section
        #[arg(long, default_value_t = 0)]
        offset_chars: usize,
        /// Originating machine for federated search results
        #[arg(long, default_value = crate::machine::LOCAL_MACHINE_ID)]
        machine: String,
        /// Pretty-print JSON output
        #[arg(short, long, hide = true)]
        verbose: bool,
        #[command(flatten)]
        read: ReadArgs,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Hydrate bounded session pages from JSONL requests (stdin when omitted)
    #[command(
        name = "hydrate",
        alias = "hydrate-batch",
        hide = true,
        after_help = "\
REQUEST FORMAT (one JSON object per line):
    {\"machine\":\"mini\",\"session_id\":\"abc\",\"source_path\":\"/tmp/session.jsonl\",\"offset\":0,\"limit\":100}

EXAMPLES:
    memex session batch requests.jsonl
    cat requests.jsonl | memex session batch

The input contains at most 32 requests; each page is limited to 500 records."
    )]
    Hydrate {
        /// JSONL request file; omit or use '-' to read stdin
        input: Option<PathBuf>,
        #[command(flatten)]
        read: ReadArgs,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Read surrounding records and linked interactions
    Context {
        /// Originating machine for the anchor
        #[arg(long, default_value = crate::machine::LOCAL_MACHINE_ID)]
        machine: String,
        /// Skip this many records in the selected neighborhood
        #[arg(long, default_value_t = 0)]
        offset: usize,
        /// Stable canonical record ID
        #[arg(long, conflicts_with_all = ["doc_id", "event_id"])]
        record_id: Option<String>,
        /// Legacy local Tantivy document ID
        #[arg(long, conflicts_with_all = ["record_id", "event_id"])]
        doc_id: Option<u64>,
        /// Source-native event ID
        #[arg(long, conflicts_with_all = ["record_id", "doc_id"])]
        event_id: Option<String>,
        /// Optional session scope for native event IDs
        #[arg(long)]
        session: Option<String>,
        /// Optional source scope for native event IDs
        #[arg(long)]
        source: Option<SourceFilter>,
        /// Number of records before the anchor
        #[arg(long, default_value_t = 5)]
        before: usize,
        /// Number of records after the anchor
        #[arg(long, default_value_t = 5)]
        after: usize,
        /// Include linked tool calls/results outside the linear window
        #[arg(long)]
        expand_interactions: bool,
        /// Pretty-print the JSON result
        #[arg(short, long, hide = true)]
        verbose: bool,
        #[command(flatten)]
        read: ReadArgs,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Developer diagnostics and retrieval evaluation
    Debug {
        #[command(subcommand)]
        action: DebugCommand,
    },
    /// Run retrieval queries from a JSONL evaluation dataset
    #[command(hide = true)]
    EvalRetrieval {
        /// JSONL evaluation dataset path
        dataset: PathBuf,
        /// Cutoff used for recall and nDCG metrics
        #[arg(long, default_value_t = 20)]
        k: usize,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// List this machine and enabled configured peers (without connecting)
    Machines {
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// List every indexed project with session count and latest activity
    Projects {
        /// Filter by source
        #[arg(long)]
        source: Option<SourceFilter>,
        /// Read one machine (defaults to local; ignores configured search defaults)
        #[arg(long)]
        machine: Option<String>,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// List indexed sessions with cwd and git metadata (newest first)
    #[command(after_help = "\
EXAMPLES:
    memex sessions                        # 20 most recent sessions as JSONL
    memex sessions --cwd .                # sessions from the current repo
    memex sessions --source claude --limit 5
    memex sessions --format json")]
    Sessions {
        /// Return one exact count object instead of session rows (unavailable totals are null)
        #[arg(long)]
        count: bool,
        /// Count conversations matching this lexical query; requires --count
        #[arg(long, requires = "count", conflicts_with = "cwd")]
        query: Option<String>,
        /// Read one machine (defaults to local; ignores configured search defaults)
        #[arg(long)]
        machine: Option<String>,
        /// Match this exact session ID
        #[arg(long)]
        session_id: Option<String>,
        /// Match this exact indexed source path
        #[arg(long)]
        source_path: Option<String>,
        /// Only sessions whose cwd is this path, lives under it, or whose git root is it
        #[arg(long)]
        cwd: Option<PathBuf>,
        /// Filter by project (repository grouping)
        #[arg(long)]
        project: Option<String>,
        /// Filter by source: claude, codex, cursor, opencode, pi, omp (Oh My Pi), openclaw, copilot, grok, hermes, jcode, or muse
        #[arg(long)]
        source: Option<SourceFilter>,
        /// Only include sessions active on or after this date/timestamp
        #[arg(long, value_name = "DATE_OR_TIMESTAMP")]
        since: Option<String>,
        /// Maximum number of sessions
        #[arg(long, default_value_t = 20)]
        limit: usize,
        /// Filter by origin: regular (default), interactive, subagent, or all (includes permission reviews)
        #[arg(long, value_enum, default_value_t = SessionOrigin::Regular)]
        origin: SessionOrigin,
        /// Only show interactive sessions (alias for --origin interactive)
        #[arg(long, conflicts_with = "origin", hide = true)]
        interactive_only: bool,
        /// Emit one JSON array instead of JSON Lines
        #[arg(long, hide = true)]
        json_array: bool,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Herdr plugin helpers (used by herdr/plugin.sh)
    #[command(hide = true)]
    Herdr {
        #[command(subcommand)]
        action: HerdrCommand,
    },
    /// Show index statistics (document count, vector count, storage paths)
    #[command(hide = true)]
    Stats {
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Reconstruct local token usage from agent logs
    #[command(after_help = "\
EXAMPLES:
    memex usage
    memex usage --source codex --since 2026-07-01
    memex usage --format json")]
    Usage {
        /// Filter by source: claude, codex, cursor, opencode, pi, omp (Oh My Pi), openclaw, copilot, grok, hermes, jcode, or muse
        #[arg(long)]
        source: Option<SourceFilter>,
        /// Session origin; all includes permission-review usage
        #[arg(long, value_enum, default_value_t = SessionOrigin::Regular)]
        origin: SessionOrigin,
        /// Only include events on or after this date/timestamp
        #[arg(long, value_name = "DATE_OR_TIMESTAMP")]
        since: Option<String>,
        /// Only include events before this date/timestamp
        #[arg(long, value_name = "DATE_OR_TIMESTAMP")]
        until: Option<String>,
        /// Emit the report as JSON
        #[arg(long, hide = true)]
        json: bool,
        /// Include normalized request-level events in JSON output
        #[arg(long)]
        events: bool,
        /// Cost source: stored source cost, automatic fallback, or API-rate repricing
        #[arg(long, value_enum, default_value = "auto")]
        cost: CostMode,
        /// Machine to include (repeatable). Defaults to multi_machine.default or all configured machines.
        #[arg(long, value_name = "ID")]
        machine: Vec<String>,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Rebuild the SQLite analytics cache from the existing Tantivy index
    #[command(hide = true)]
    AnalyticsBackfill {
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Report privacy-safe transcript structure and producer-version counts
    #[command(hide = true)]
    SourceAudit {
        /// Limit the audit to one source
        #[arg(long)]
        source: Option<SourceFilter>,
    },
    /// Manage the bundled memex-search skill
    Skill {
        #[command(subcommand)]
        command: SkillCommand,
    },
    /// Deprecated alias for interactive `memex skill install`
    #[command(hide = true)]
    Setup {
        /// Overwrite existing skill copies
        #[arg(short, long)]
        force: bool,
    },
    /// Update memex and refresh existing memex-search skill copies
    #[command(
        after_help = "Use --yes for agents and scripts, including shells with a PTY.\nWithout --yes, update requires a terminal and asks for confirmation.\nExisting skill copies are replaced; missing copies are not installed."
    )]
    Update {
        /// Update without prompting (for agents and scripts)
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
    /// Transfer an indexed session into another agent backend
    #[command(after_help = "\
EXAMPLES:
    memex transfer abc123
    memex transfer abc123 --to pi
    memex transfer abc123 --to opencode
    memex transfer abc123 --mode strict --turns 80
    memex transfer abc123 --source pi --to codex
    memex transfer abc123 --dry-run")]
    Transfer {
        /// Session ID (from search results or TUI)
        session_id: String,
        /// Filter by source when a session id appears in multiple backends
        #[arg(long)]
        source: Option<SourceFilter>,
        /// Target backend to import into
        #[arg(long, value_enum, default_value = "codex")]
        to: TransferTarget,
        /// Compact imports text turns; strict includes tool activity as text notes
        #[arg(long, value_enum, default_value = "compact")]
        mode: TransferMode,
        /// Limit imported turns (Pi defaults to 60 and caps at 400)
        #[arg(long)]
        turns: Option<usize>,
        /// Generate the intermediate transcript without importing into the target
        #[arg(long)]
        dry_run: bool,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Internal versioned RPC endpoint used by remote memex clients
    #[command(hide = true)]
    Rpc {
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Run the Model Context Protocol server (Streamable HTTP by default)
    Mcp {
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long, value_enum, default_value = "http")]
        transport: McpTransport,
        /// HTTP socket address (default: [mcp].listen or 127.0.0.1:5363)
        #[arg(long)]
        listen: Option<std::net::SocketAddr>,
        /// Additional accepted HTTP Host authority; repeat for multiple hosts
        #[arg(long)]
        allowed_host: Vec<String>,
        /// Accepted browser Origin; repeat for multiple origins
        #[arg(long)]
        allowed_origin: Vec<String>,
        /// Public HTTPS origin enabling built-in OAuth (e.g. https://memex.example.com)
        #[arg(long)]
        public_url: Option<String>,
        /// Revoke all OAuth grants in this data directory and exit
        #[arg(long)]
        revoke_all: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum McpTransport {
    Http,
    Stdio,
}

#[derive(Debug, Subcommand)]
enum SkillCommand {
    /// Show whether installed skill copies match this memex binary
    Status {
        /// Installation destination to inspect
        #[arg(long, value_enum, default_value = "all")]
        target: SkillTarget,
    },
    /// Install missing skill copies without overwriting differing files
    Install {
        /// Installation destination; omit for an interactive selection
        #[arg(long, value_enum)]
        target: Option<SkillTarget>,
    },
    /// Update existing skill copies without installing new ones
    Update {
        /// Installation destination to update
        #[arg(long, value_enum, default_value = "all")]
        target: SkillTarget,
    },
    /// Remove obsolete Memex skill and prompt paths from older releases
    Cleanup {
        /// Print obsolete paths without removing them
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum SkillTarget {
    /// Shared agentskills.io location used by Codex, OpenCode, Pi, and Oh My Pi
    Shared,
    /// Claude Code skill location
    Claude,
    /// Both shared and Claude Code locations
    All,
}

#[derive(Debug, Subcommand)]
enum HerdrCommand {
    /// Resume the most recent resumable session, opening a new herdr tab
    ResumeLast {
        /// Prefer sessions from this directory (falls back to the global latest unless strict)
        #[arg(long)]
        cwd: Option<PathBuf>,
        /// Refuse when no resumable session exists in --cwd instead of using another project
        #[arg(long)]
        strict_cwd: bool,
        /// Filter by source: claude, codex, cursor, opencode, pi, omp (Oh My Pi), openclaw, copilot, grok, hermes, jcode, or muse
        #[arg(long)]
        source: Option<SourceFilter>,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Resume a specific session by id, opening a new herdr tab
    Resume {
        /// Session ID (from `memex sessions` or search results)
        session_id: String,
        /// Filter by source when a session id appears in multiple backends
        #[arg(long)]
        source: Option<SourceFilter>,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum TransferTarget {
    Codex,
    Claude,
    Copilot,
    Cursor,
    Opencode,
    Pi,
}

impl From<TransferTarget> for CoreTransferTarget {
    fn from(value: TransferTarget) -> Self {
        match value {
            TransferTarget::Codex => CoreTransferTarget::Codex,
            TransferTarget::Claude => CoreTransferTarget::Claude,
            TransferTarget::Copilot => CoreTransferTarget::Copilot,
            TransferTarget::Cursor => CoreTransferTarget::Cursor,
            TransferTarget::Opencode => CoreTransferTarget::Opencode,
            TransferTarget::Pi => CoreTransferTarget::Pi,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum TransferMode {
    Compact,
    Strict,
}

impl From<TransferMode> for CoreTransferMode {
    fn from(value: TransferMode) -> Self {
        match value {
            TransferMode::Compact => CoreTransferMode::Compact,
            TransferMode::Strict => CoreTransferMode::Strict,
        }
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum, Serialize, Deserialize, JsonSchema,
)]
#[value(rename_all = "kebab-case")]
#[serde(rename_all = "lowercase")]
pub(crate) enum SessionOrigin {
    /// Ordinary sessions, including subagents, without permission reviews.
    #[default]
    Regular,
    Interactive,
    Subagent,
    /// Every session, including permission reviews.
    All,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum McpSearchMode {
    #[default]
    Lexical,
    Semantic,
    Hybrid,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum, Deserialize, JsonSchema)]
#[value(rename_all = "kebab-case")]
#[serde(rename_all = "lowercase")]
pub(crate) enum SearchContent {
    #[default]
    Conversations,
    Memories,
    All,
}

impl From<McpSearchMode> for SearchMode {
    fn from(value: McpSearchMode) -> Self {
        match value {
            McpSearchMode::Lexical => SearchMode::Lexical,
            McpSearchMode::Semantic => SearchMode::Semantic,
            McpSearchMode::Hybrid => SearchMode::Hybrid,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub(crate) enum McpSearchSort {
    #[default]
    Score,
    Ts,
}

impl From<McpSearchSort> for SortBy {
    fn from(value: McpSearchSort) -> Self {
        match value {
            McpSearchSort::Score => SortBy::Score,
            McpSearchSort::Ts => SortBy::Ts,
        }
    }
}

fn default_mcp_limit() -> usize {
    20
}

fn default_true() -> bool {
    true
}

fn default_recency_weight() -> f32 {
    1.0
}

fn default_recency_half_life_days() -> f32 {
    30.0
}

/// Parameters for the MCP search tool. Results always use Memex's compact,
/// bounded search projection; complete transcript text is available through
/// the bounded read tools.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchRequest {
    /// Keywords or natural-language query.
    pub(crate) query: String,
    /// Search conversations, memories, or both. Defaults to conversations.
    #[serde(default)]
    pub(crate) content: SearchContent,
    /// Additional query views fused with reciprocal-rank fusion.
    #[serde(default)]
    pub(crate) additional_queries: Vec<String>,
    /// Restrict results to sessions from this directory or repository.
    pub(crate) cwd: Option<String>,
    /// Repository/project grouping to search.
    pub(crate) project: Option<String>,
    /// Record role such as user, assistant, tool_use, or tool_result.
    pub(crate) role: Option<String>,
    /// Tool name such as Read, Edit, or Bash.
    pub(crate) tool: Option<String>,
    /// Exact session ID.
    pub(crate) session: Option<String>,
    /// Agent source such as claude, codex, cursor, opencode, pi, or omp.
    pub(crate) source: Option<String>,
    /// Session origin: regular (default) excludes permission reviews; all includes them. Interactive and subagent select ordinary session subsets.
    #[serde(default)]
    pub(crate) origin: SessionOrigin,
    /// Lexical, semantic, or hybrid retrieval.
    #[serde(default)]
    pub(crate) mode: McpSearchMode,
    /// Earliest timestamp (RFC3339, date, unix seconds, or unix milliseconds).
    pub(crate) since: Option<String>,
    /// Latest timestamp (RFC3339, unix seconds, or unix milliseconds).
    pub(crate) until: Option<String>,
    /// Maximum returned results (1-500).
    #[serde(default = "default_mcp_limit")]
    pub(crate) limit: usize,
    /// Maximum results from any one session.
    pub(crate) top_n_per_session: Option<usize>,
    /// Return at most one result per session unless top_n_per_session is set.
    #[serde(default = "default_true")]
    pub(crate) unique_session: bool,
    /// Sort by retrieval score or record timestamp.
    #[serde(default)]
    pub(crate) sort: McpSearchSort,
    /// Drop results below this retrieval score.
    pub(crate) min_score: Option<f32>,
    /// Strength of the recency boost (0 disables it).
    #[serde(default = "default_recency_weight")]
    pub(crate) recency_weight: f32,
    /// Half-life in days for recency decay.
    #[serde(default = "default_recency_half_life_days")]
    pub(crate) recency_half_life_days: f32,
    /// Machine IDs to search. Empty uses the configured defaults.
    #[serde(default)]
    pub(crate) machines: Vec<String>,
}

/// Parameters for the MCP session-listing tool.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct SessionsRequest {
    /// Match this exact session ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) session_id: Option<String>,
    /// Match this exact indexed source path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_path: Option<String>,
    /// Restrict sessions to this directory or repository.
    pub(crate) cwd: Option<String>,
    /// Repository/project grouping to list.
    pub(crate) project: Option<String>,
    /// Agent source such as claude, codex, cursor, opencode, pi, or omp.
    pub(crate) source: Option<String>,
    /// Earliest activity timestamp (RFC3339, date, unix seconds, or unix milliseconds).
    pub(crate) since: Option<String>,
    /// Session origin: regular (default) excludes permission reviews; all includes them. Interactive and subagent select ordinary session subsets.
    #[serde(default)]
    pub(crate) origin: SessionOrigin,
    /// Maximum returned sessions (1-500).
    #[serde(default = "default_mcp_limit")]
    pub(crate) limit: usize,
}

impl From<SessionOrigin> for crate::analytics::SessionKindFilter {
    fn from(value: SessionOrigin) -> Self {
        match value {
            SessionOrigin::Interactive => crate::analytics::SessionKindFilter::Primary,
            SessionOrigin::Subagent => crate::analytics::SessionKindFilter::Subagent,
            SessionOrigin::Regular => crate::analytics::SessionKindFilter::Regular,
            SessionOrigin::All => crate::analytics::SessionKindFilter::All,
        }
    }
}

#[derive(Subcommand)]
enum IndexServiceCommand {
    /// Run the configured daemon in the foreground
    Run {
        #[command(flatten)]
        index: IndexArgs,
        /// Serve the browser (also configurable with index_service_web_ui)
        #[arg(long)]
        web_ui: bool,
        /// Browser listener (implies --web-ui)
        #[arg(long)]
        web_listen: Option<String>,
        /// Seconds between index refreshes (default: config or 30)
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        poll_interval: Option<u64>,
        #[command(flatten)]
        mcp: DaemonMcpArgs,
    },
    /// Enable the background daemon (launchd on macOS, systemd on Linux)
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
        /// Serve the local Web UI (implies continuous mode)
        #[arg(long)]
        web_ui: bool,
        /// Web UI address and port [default: 127.0.0.1:6363] (implies --web-ui)
        #[arg(long, value_name = "ADDRESS")]
        web_listen: Option<String>,
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
        #[command(flatten)]
        mcp: DaemonMcpArgs,
    },
    /// Regenerate and restart the daemon using current config
    Restart {
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
        /// Serve the local Web UI (implies continuous mode)
        #[arg(long)]
        web_ui: bool,
        /// Web UI address and port [default: 127.0.0.1:6363] (implies --web-ui)
        #[arg(long, value_name = "ADDRESS")]
        web_listen: Option<String>,
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
        #[command(flatten)]
        mcp: DaemonMcpArgs,
    },
    /// Open the authenticated Web UI in the default browser
    #[command(hide = true)]
    Open {
        #[arg(long, hide = true)]
        print_url: bool,
        /// Web UI address and port [default: config or 127.0.0.1:6363]
        #[arg(long, value_name = "ADDRESS")]
        listen: Option<String>,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Show daemon, web UI, and MCP status
    Status {
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
    /// Disable and remove the background daemon
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
    let interactive = interaction_allowed(
        cli.non_interactive,
        std::io::stdin().is_terminal(),
        std::io::stdout().is_terminal(),
        std::io::stderr().is_terminal(),
        agent_or_ci_environment(),
    );
    let check_updates = !cli.no_update_check;
    if cli.command.is_none() && !interactive {
        if check_updates {
            print_update_notice(available_update().as_deref());
        }
        warn_if_skill_outdated();
        Cli::command().print_help()?;
        println!();
        return Ok(());
    }
    // Bare `memex` opens the TUI home screen.
    let command = cli
        .command
        .unwrap_or(Commands::Tui {
            query: None,
            project: None,
            root: None,
        })
        .canonicalize();
    if !interactive
        && matches!(
            command,
            Commands::Setup { .. }
                | Commands::Skill {
                    command: SkillCommand::Install { target: None }
                }
        )
    {
        return Err(anyhow!(
            "skill installation requires a destination without interactive selection; use `memex skill install --target shared`, `--target claude`, or `--target all`"
        ));
    }
    let should_check = !matches!(
        command,
        Commands::Tui { .. }
            | Commands::Update { .. }
            | Commands::Skill { .. }
            | Commands::Rpc { .. }
            | Commands::Mcp { .. }
            | Commands::Herdr { .. }
    );
    if should_check && check_updates {
        print_update_notice(available_update().as_deref());
    }
    if matches!(command, Commands::Search { .. }) {
        warn_if_skill_outdated();
    }
    match command {
        Commands::Index {
            action: _,
            index,
            watch,
            watch_interval,
            web_ui,
            web_listen,
            mcp,
            no_mcp,
            mcp_listen,
        } => {
            if watch {
                let listen = (web_ui || web_listen.is_some())
                    .then(|| web_listen.unwrap_or_else(|| crate::web::DEFAULT_LISTEN.to_string()));
                let config = UserConfig::load(&Paths::new(index.root.clone())?)?;
                let mcp = DaemonMcpArgs {
                    mcp,
                    no_mcp,
                    mcp_listen,
                }
                .resolve(&config);
                run_index_loop(&index, watch_interval, listen, mcp)?;
            } else if web_ui || web_listen.is_some() || mcp || no_mcp || mcp_listen.is_some() {
                return Err(anyhow!("server options require `memex daemon run`"));
            } else {
                run_index_args(&index, false, false)?;
            }
        }
        Commands::Reindex { index } => {
            run_index_args(&index, true, false)?;
        }
        Commands::IndexGc {
            root,
            dry_run,
            offline,
        } => run_index_gc(root, dry_run, offline)?,
        Commands::Embed { model, root } => {
            run_embed(model, root)?;
        }
        Commands::Search {
            query,
            content,
            additional_queries,
            cwd,
            project,
            role,
            tool,
            session,
            source,
            origin,
            semantic,
            hybrid,
            mode,
            min_score,
            recency_weight,
            recency_half_life_days,
            since,
            until,
            limit,
            top_n_per_session,
            unique_session,
            json_array,
            format,
            pretty,
            fields,
            full,
            sort,
            verbose,
            root,
            machine,
            trace,
        } => {
            run_search(
                query,
                content,
                additional_queries,
                cwd,
                project,
                role,
                tool,
                session,
                source,
                origin,
                mode.map_or(semantic, |mode| mode == CliSearchMode::Semantic),
                mode.map_or(hybrid, |mode| mode == CliSearchMode::Hybrid),
                min_score,
                recency_weight,
                recency_half_life_days,
                since,
                until,
                limit,
                top_n_per_session,
                unique_session,
                json_array,
                format,
                pretty,
                fields,
                full,
                sort,
                verbose,
                root,
                machine,
                trace,
            )?;
        }
        Commands::Tui {
            query,
            project,
            root,
        } => {
            if !interactive {
                return Err(anyhow!(
                    "TUI requires an interactive human terminal; use `memex search` or `memex sessions` for agent/script output"
                ));
            }
            let latest = check_updates.then(available_update).flatten();
            if let Some(latest) = latest.as_deref() {
                print_update_notice(Some(latest));
                if confirm_update()? {
                    perform_update(Some(latest))?;
                    println!("Update finished. Run `memex` again to start the installed version.");
                    return Ok(());
                }
            }
            let update_rx = latest.map(|latest| {
                let (tx, rx) = std::sync::mpsc::channel();
                let _ = tx.send(format!("update: v{latest} (memex update)"));
                rx
            });
            tui::run(root, update_rx, query, project)?;
        }
        Commands::Web { listen, root, .. } => {
            crate::web::serve(root, &listen)?;
        }
        Commands::IndexService { action } => match action {
            IndexServiceCommand::Run {
                index,
                web_ui,
                web_listen,
                poll_interval,
                mcp,
            } => {
                let config = UserConfig::load(&Paths::new(index.root.clone())?)?;
                let web = (web_ui || web_listen.is_some() || config.index_service_web_ui_default())
                    .then(|| {
                        web_listen
                            .or_else(|| config.index_service_web_listen.clone())
                            .unwrap_or_else(|| crate::web::DEFAULT_LISTEN.to_string())
                    });
                let interval = poll_interval.unwrap_or(config.index_service_poll_interval());
                anyhow::ensure!(interval > 0, "poll interval must be positive");
                run_index_loop(&index, interval, web, mcp.resolve(&config))?;
            }
            IndexServiceCommand::Enable {
                index,
                mcp,
                label,
                continuous,
                poll_interval,
                interval,
                web_ui,
                web_listen,
                stdout,
                stderr,
                plist,
                systemd_dir,
            } => {
                run_index_service_enable(
                    &index,
                    mcp,
                    label,
                    continuous,
                    poll_interval,
                    interval,
                    web_ui,
                    web_listen,
                    stdout,
                    stderr,
                    plist,
                    systemd_dir,
                )?;
            }
            IndexServiceCommand::Restart {
                index,
                mcp,
                label,
                continuous,
                poll_interval,
                interval,
                web_ui,
                web_listen,
                stdout,
                stderr,
                plist,
                systemd_dir,
            } => {
                run_index_service_enable(
                    &index,
                    mcp,
                    label,
                    continuous,
                    poll_interval,
                    interval,
                    web_ui,
                    web_listen,
                    stdout,
                    stderr,
                    plist,
                    systemd_dir,
                )?;
            }
            IndexServiceCommand::Status {
                label,
                plist,
                systemd_dir,
                root,
            } => {
                run_index_service_status(label, plist, systemd_dir, root)?;
            }
            IndexServiceCommand::Open {
                listen,
                root,
                print_url,
            } => {
                run_index_service_open(listen, root, print_url)?;
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
            action: _,
            session_id,
            machine,
            source_path,
            offset,
            limit,
            verbose,
            output,
            read,
            root,
        } => {
            run_session(SessionRunArgs {
                session_id: session_id.context("provide a session ID")?,
                machine,
                source_path,
                offset,
                limit,
                output: output.resolve(
                    OutputFormat::Jsonl,
                    verbose.then_some(OutputFormat::Text),
                    false,
                )?,
                read,
                root,
            })?;
        }
        Commands::Show {
            doc_id,
            record_id,
            memory_id,
            section,
            content_version,
            field,
            offset_chars,
            machine,
            verbose,
            output,
            read,
            root,
        } => {
            let output = output.resolve(OutputFormat::Json, None, verbose)?;
            if let Some(memory_id) = memory_id {
                run_show_memory(MemoryShowRunArgs {
                    memory_id,
                    section,
                    content_version,
                    offset_chars,
                    machine,
                    output,
                    read,
                    root,
                })?;
            } else {
                if offset_chars != 0 && field.is_none() {
                    return Err(anyhow!("--offset-chars requires --field for record reads"));
                }
                let selector = match (doc_id, record_id) {
                    (Some(id), None) => ContextSelector::doc_id(id),
                    (None, Some(id)) => ContextSelector::record_id(id),
                    _ => {
                        return Err(anyhow!(
                            "provide a document ID, --record-id, or --memory-id"
                        ));
                    }
                };
                run_show(ShowRunArgs {
                    selector,
                    field,
                    offset_chars,
                    machine,
                    output,
                    read,
                    root,
                })?;
            }
        }
        Commands::Hydrate {
            input,
            read,
            root,
            output,
        } => {
            run_hydrate(
                input,
                read,
                root,
                output.resolve(OutputFormat::Jsonl, None, false)?,
            )?;
        }
        Commands::Context {
            machine,
            offset,
            record_id,
            doc_id,
            event_id,
            session,
            source,
            before,
            after,
            expand_interactions,
            verbose,
            output,
            read,
            root,
        } => {
            run_context(ContextRunArgs {
                machine,
                offset,
                record_id,
                doc_id,
                event_id,
                session,
                source,
                before,
                after,
                expand_interactions,
                output: output.resolve(OutputFormat::Json, None, verbose)?,
                read,
                root,
            })?;
        }
        Commands::Debug {
            action: DebugCommand::EvalRetrieval { dataset, k, root },
        }
        | Commands::EvalRetrieval { dataset, k, root } => {
            run_eval_retrieval(dataset, k, root)?;
        }
        Commands::Machines { root, output } => {
            let paths = Paths::new(root)?;
            let config = UserConfig::load(&paths)?;
            output
                .resolve(OutputFormat::Jsonl, None, false)?
                .print_values(crate::machine::configured_machine_summaries(&config)?)?;
        }
        Commands::Projects {
            source,
            machine,
            root,
            output,
        } => {
            let paths = Paths::new(root)?;
            let mut items = if let Some(id) = machine
                .as_deref()
                .filter(|id| *id != crate::machine::LOCAL_MACHINE_ID)
            {
                let config = UserConfig::load(&paths)?;
                crate::machine::remote_project_summaries(&config, id, source)?
            } else {
                collect_projects(&paths, source)?
            };
            if let Some(machine) = machine {
                for item in &mut items {
                    item["machine"] = Value::String(machine.clone());
                }
            }
            output
                .resolve(OutputFormat::Jsonl, None, false)?
                .print_values(items)?;
        }
        Commands::Sessions {
            count,
            query,
            machine,
            session_id,
            source_path,
            cwd,
            project,
            source,
            since,
            limit,
            origin,
            interactive_only,
            json_array,
            output,
            root,
        } => {
            let origin = if interactive_only {
                SessionOrigin::Interactive
            } else {
                origin
            };
            if count {
                let paths = Paths::new(root)?;
                let request = SessionsRequest {
                    session_id,
                    source_path,
                    cwd: cwd.map(|path| path.to_string_lossy().into_owned()),
                    project,
                    source: source.map(|value| value.as_str().to_string()),
                    since,
                    limit,
                    origin,
                };
                let result = if let Some(id) = machine
                    .as_deref()
                    .filter(|id| *id != crate::machine::LOCAL_MACHINE_ID)
                {
                    crate::machine::remote_session_count(
                        &UserConfig::load(&paths)?,
                        id,
                        request,
                        query,
                    )?
                } else {
                    collect_session_count(&paths, request, query)?
                };
                println!("{}", serde_json::to_string(&result)?);
                return Ok(());
            }
            run_sessions(
                session_id,
                source_path,
                cwd,
                project,
                source,
                since,
                limit,
                origin,
                output.resolve(
                    OutputFormat::Jsonl,
                    json_array.then_some(OutputFormat::Json),
                    false,
                )?,
                root,
                machine,
            )?;
        }
        Commands::Herdr { action } => match action {
            HerdrCommand::ResumeLast {
                cwd,
                strict_cwd,
                source,
                root,
            } => {
                run_herdr_resume(None, cwd, strict_cwd, source, root)?;
            }
            HerdrCommand::Resume {
                session_id,
                source,
                root,
            } => {
                run_herdr_resume(Some(session_id), None, false, source, root)?;
            }
        },
        Commands::Stats { root } => {
            run_stats(root)?;
        }
        Commands::Usage {
            source,
            origin,
            since,
            until,
            json,
            output,
            events,
            cost,
            root,
            machine,
        } => {
            run_usage(UsageCommandOptions {
                source,
                origin,
                since,
                until,
                output: output.resolve(
                    OutputFormat::Text,
                    json.then_some(OutputFormat::Json),
                    json,
                )?,
                include_events: events,
                cost_mode: cost,
                root,
                machines: machine,
            })?;
        }
        Commands::AnalyticsBackfill { root } => {
            run_analytics_backfill(root)?;
        }
        Commands::SourceAudit { source } => {
            let audits = crate::sources::audit::audit_installed_sources(source)?;
            println!("{}", serde_json::to_string_pretty(&audits)?);
        }
        Commands::Skill { command } => {
            run_skill_command(command)?;
        }
        Commands::Setup { force } => {
            eprintln!("warning: `memex setup` is deprecated; use `memex skill install`");
            run_skill_install(None, force.then_some(SkillWriteMode::Replace))?;
        }
        Commands::Update { yes } => {
            if !yes {
                if !interactive {
                    return Err(anyhow!(
                        "update requires confirmation; use `memex update --yes` for a noninteractive update of memex and installed skills"
                    ));
                }
                if !confirm_update()? {
                    println!("Update cancelled.");
                    return Ok(());
                }
            }
            perform_update(None)?;
        }
        Commands::Share {
            session_id,
            title,
            root,
        } => {
            run_share(session_id, title, root)?;
        }
        Commands::Transfer {
            session_id,
            source,
            to,
            mode,
            turns,
            dry_run,
            root,
        } => {
            run_transfer(session_id, source, to, mode, turns, dry_run, root)?;
        }
        Commands::Rpc { root } => {
            crate::machine::run_rpc_stdio(root)?;
        }
        Commands::Mcp {
            root,
            transport,
            listen,
            allowed_host,
            allowed_origin,
            public_url,
            revoke_all,
        } => {
            if revoke_all {
                let count = crate::mcp::revoke_all(root)?;
                println!("Revoked {count} OAuth grants");
                return Ok(());
            }
            anyhow::ensure!(
                transport == McpTransport::Http || public_url.is_none(),
                "--public-url requires the HTTP transport"
            );
            let http = if transport == McpTransport::Http {
                let config = UserConfig::load(&Paths::new(root.clone())?)?;
                Some(surface::mcp_http_options(
                    &config,
                    listen,
                    allowed_host,
                    allowed_origin,
                    public_url,
                ))
            } else {
                None
            };
            crate::mcp::run(root, http)?;
        }
    }
    Ok(())
}

fn run_index_loop(
    index: &IndexArgs,
    interval_secs: u64,
    web_listen: Option<String>,
    mcp: Option<crate::mcp::HttpOptions>,
) -> Result<()> {
    let mcp_server = mcp
        .map(|options| crate::mcp::spawn_http(index.root.clone(), options))
        .transpose()?;
    let _web_thread = initialize_index_loop(
        || run_index_args(index, false, true),
        || {
            web_listen
                .as_deref()
                .map(|listen| crate::web::spawn(index.root.clone(), listen))
                .transpose()
        },
    )?;
    loop {
        if let Some(server) = &mcp_server {
            if !server.wait_timeout(Duration::from_secs(interval_secs))? {
                return Ok(());
            }
        } else {
            std::thread::sleep(Duration::from_secs(interval_secs));
        }
        run_index_args(index, false, true)?;
        std::io::stdout().flush().ok();
    }
}

fn initialize_index_loop<T>(
    index_once: impl FnOnce() -> Result<()>,
    start_web: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let web = start_web()?;
    index_once()?;
    Ok(web)
}

fn run_index_args(index: &IndexArgs, reindex: bool, continuous: bool) -> Result<()> {
    run_index(
        index.source.clone(),
        index.include_agents,
        index.include_reasoning,
        index.source_enabled(IndexSource::Claude),
        index.source_enabled(IndexSource::Codex),
        index.source_enabled(IndexSource::Opencode),
        index.source_enabled(IndexSource::Cursor),
        index.source_enabled(IndexSource::Pi),
        index.source_enabled(IndexSource::Omp),
        index.source_enabled(IndexSource::Openclaw),
        index.source_enabled(IndexSource::Copilot),
        index.source_enabled(IndexSource::Grok),
        index.source_enabled(IndexSource::Jcode),
        index.source_enabled(IndexSource::Muse),
        index.embeddings,
        index.no_embeddings,
        index.model.clone(),
        index.root.clone(),
        index.exclude.clone(),
        reindex,
        continuous,
        index.diagnostics,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_index(
    source: Option<PathBuf>,
    include_agents: bool,
    include_reasoning: bool,
    claude: bool,
    codex: bool,
    opencode: bool,
    cursor: bool,
    pi: bool,
    omp: bool,
    openclaw: bool,
    copilot: bool,
    grok: bool,
    jcode: bool,
    muse: bool,
    embeddings_flag: bool,
    no_embeddings: bool,
    model: Option<String>,
    root: Option<PathBuf>,
    mut excludes: Vec<String>,
    reindex: bool,
    continuous: bool,
    print_diagnostics: bool,
) -> Result<()> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;

    // Config exclusions apply to every index run; CLI --exclude adds one-off patterns.
    excludes.extend(config.exclude_path_patterns());

    // Model priority: CLI flag > config file > env var > default
    let model_choice = config.resolve_model(model)?;
    let embed_runtime = config.resolve_embed_runtime()?;
    let tool_content_limits = config.indexed_tool_content_limits()?;
    let include_reasoning = include_reasoning || config.include_reasoning_default();
    let embeddings = resolve_flag(
        config.embeddings_default(),
        embeddings_flag,
        no_embeddings,
        "embeddings",
    )?;
    let operation = if reindex { "reindex" } else { "index" };
    let lease = IngestLease::acquire(&paths, operation, INGEST_LEASE_TIMEOUT)?;
    if reindex {
        reset_reindex_artifacts(&paths)?;
    }
    paths.ensure_dirs()?;
    let index = if continuous {
        SearchIndex::open_or_create_for_continuous_ingest(&paths.index)?
    } else {
        SearchIndex::open_or_create_for_ingest(&paths.index)?
    };

    let opts = IngestOptions {
        claude_sources: if claude {
            source
                .map(|source| vec![source])
                .unwrap_or_else(default_claude_sources)
        } else {
            Vec::new()
        },
        include_agents,
        include_reasoning,
        include_codex: codex,
        include_opencode: opencode,
        include_cursor: cursor,
        include_pi: pi,
        include_omp: omp,
        include_openclaw: openclaw,
        include_copilot: copilot,
        include_grok: grok,
        include_jcode: jcode,
        include_muse: muse,
        exclude_patterns: excludes,
        embeddings,
        backfill_embeddings: false,
        model: model_choice,
        embed_runtime,
        tool_content_limits,
    };

    let report = ingest_all(&paths, &index, &opts, &lease)?;
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
    if print_diagnostics && !report.diagnostics.is_empty() {
        println!(
            "parser diagnostics:\n{}",
            serde_json::to_string_pretty(&report.diagnostics)?
        );
    }
    Ok(())
}

fn reset_reindex_artifacts(paths: &Paths) -> Result<()> {
    remove_generated_path(&paths.index)?;
    remove_generated_path(&paths.vectors)?;
    remove_generated_path(&paths.root.join("memory"))?;

    for name in [
        "ingest.json",
        "ingest.pending.json",
        "scan_cache.json",
        "analytics.sqlite",
        "analytics.sqlite-wal",
        "analytics.sqlite-shm",
        "analytics.sqlite-journal",
        "usage-cache.sqlite3",
        "usage-cache.sqlite3-wal",
        "usage-cache.sqlite3-shm",
        "usage-cache.sqlite3-journal",
    ] {
        let path = paths.state.join(name);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("remove reindex artifact {}", path.display()));
            }
        }
    }
    Ok(())
}

fn remove_generated_path(path: &Path) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let result = if metadata.is_dir() && !metadata.file_type().is_symlink() {
        std::fs::remove_dir_all(path)
    } else {
        std::fs::remove_file(path)
    };
    result.with_context(|| format!("remove reindex artifact {}", path.display()))
}

fn run_index_gc(root: Option<PathBuf>, dry_run: bool, offline: bool) -> Result<()> {
    if !dry_run && !offline {
        return Err(anyhow!(
            "index GC requires offline confirmation; stop the Memex daemon and all Memex \
             readers, then rerun with `--offline` (or use `--dry-run`)"
        ));
    }
    let paths = Paths::new(root)?;
    let _lease = IngestLease::acquire(&paths, "index-gc", INGEST_LEASE_TIMEOUT)?;
    let report = SearchIndex::garbage_collect_generations_offline(&paths.index, dry_run)?;
    let memory_generations = gc_memory_vectors(&paths, dry_run)?;
    if report.dry_run {
        println!(
            "would remove {} unreachable generations, {} abandoned generation work directories, {} legacy index files, and {} obsolete memory vector generations; no rebuild required",
            report.generations_removed,
            report.abandoned_workdirs_removed,
            report.legacy_files_removed,
            memory_generations
        );
    } else {
        println!(
            "removed {} unreachable generations, {} abandoned generation work directories, {} legacy index files, and {} obsolete memory vector generations; retained the committed indexes without rebuilding",
            report.generations_removed,
            report.abandoned_workdirs_removed,
            report.legacy_files_removed,
            memory_generations
        );
    }
    Ok(())
}

fn run_embed(model: Option<String>, root: Option<PathBuf>) -> Result<()> {
    const BATCH_SIZE: usize = 256;

    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let _lease = IngestLease::acquire(&paths, "embed", INGEST_LEASE_TIMEOUT)?;

    // Model priority: CLI flag > config file > env var > default
    let model_choice = config.resolve_model(model)?;
    let embed_runtime = config.resolve_embed_runtime()?;

    let index = SearchIndex::open_or_create(&paths.index)?;
    let mut embedder = EmbedderHandle::with_model_and_runtime(model_choice, &embed_runtime)?;
    let mut vector =
        VectorIndex::open_or_create(&paths.vectors, embedder.dims, Some(model_choice.as_str()))?;

    let progress = std::sync::Arc::new(crate::progress::Progress::new(
        [0; crate::progress::SOURCE_COUNT],
        [0; crate::progress::SOURCE_COUNT],
        true,
    ));
    progress.set_embed_ready();

    let mut embedded_counts = [0u64; crate::progress::SOURCE_COUNT];
    let mut embedded_total = 0u64;
    let mut batch: Vec<(u64, String, crate::types::SourceKind)> = Vec::with_capacity(BATCH_SIZE);

    let flush_batch = |batch: &mut Vec<(u64, String, crate::types::SourceKind)>,
                       embedder: &mut EmbedderHandle,
                       vector: &mut VectorIndex,
                       progress: &crate::progress::Progress,
                       embedded_counts: &mut [u64; crate::progress::SOURCE_COUNT],
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
    let memory_embedded = embed_memory(&paths, model_choice, &embed_runtime)?;
    progress.finish();
    println!(
        "embedded {} conversation vectors and {} memory section vectors (claude {}, codex {}, opencode {}, cursor {}, pi {}, openclaw {}, copilot {}, jcode {}, muse {}, grok {})",
        embedded_total,
        memory_embedded,
        embedded_counts[crate::types::SourceKind::Claude.idx()],
        embedded_counts[crate::types::SourceKind::Codex.idx()],
        embedded_counts[crate::types::SourceKind::Opencode.idx()],
        embedded_counts[crate::types::SourceKind::Cursor.idx()],
        embedded_counts[crate::types::SourceKind::Pi.idx()],
        embedded_counts[crate::types::SourceKind::OpenClaw.idx()],
        embedded_counts[crate::types::SourceKind::Copilot.idx()],
        embedded_counts[crate::types::SourceKind::Jcode.idx()],
        embedded_counts[crate::types::SourceKind::Muse.idx()],
        embedded_counts[crate::types::SourceKind::Grok.idx()],
    );

    std::io::stdout().flush().ok();
    std::process::exit(0);
}

#[allow(clippy::too_many_arguments)]
fn run_search(
    query: String,
    content: SearchContent,
    additional_queries: Vec<String>,
    cwd: Option<PathBuf>,
    project: Option<String>,
    role: Option<String>,
    tool: Option<String>,
    session: Option<String>,
    source: Option<SourceFilter>,
    origin: SessionOrigin,
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
    format: SearchFormat,
    pretty: bool,
    fields: Option<String>,
    full: bool,
    sort: SortBy,
    verbose: bool,
    root: Option<PathBuf>,
    machines: Vec<String>,
    trace: bool,
) -> Result<()> {
    let format = if json_array && !verbose {
        SearchFormat::Json
    } else {
        format
    };
    if pretty && (verbose || format != SearchFormat::Json) {
        return Err(anyhow!("--pretty requires --format json"));
    }
    let trace_started = Instant::now();
    let trace_started_at_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let mode = if hybrid {
        SearchMode::Hybrid
    } else if semantic {
        SearchMode::Semantic
    } else {
        SearchMode::Lexical
    };
    if content != SearchContent::Conversations {
        return run_search_with_memories(MemorySurfaceSearchArgs {
            query,
            additional_queries,
            cwd,
            project,
            role,
            tool,
            session,
            source,
            origin,
            mode,
            min_score,
            recency_weight,
            recency_half_life_days,
            since,
            until,
            limit,
            top_n_per_session,
            unique_session,
            fields: search_fields(fields, full)?,
            sort,
            format,
            pretty,
            root,
            machines,
            content,
            trace,
        });
    }
    let mut collected = collect_search(SearchCollectRequest {
        query,
        additional_queries,
        cwd,
        project,
        role,
        tool,
        session,
        source,
        origin,
        mode,
        min_score,
        recency_weight,
        recency_half_life_days,
        since,
        until,
        limit,
        top_n_per_session,
        unique_session,
        fields: search_fields(fields, full)?,
        sort,
        verbose,
        format: if json_array && !verbose {
            SearchFormat::Json
        } else {
            format
        },
        root,
        machines,
    })?;
    collected.render.pretty = pretty;
    for failure in &collected.failures {
        if let Some((machine, error)) = failure.split_once(": ") {
            eprintln!("Warning: machine '{machine}' unavailable: {error}");
        } else {
            eprintln!("Warning: machine unavailable: {failure}");
        }
    }
    if trace {
        let mode_label = match (mode, collected.queries.len() > 1) {
            (SearchMode::Lexical, false) => "lexical",
            (SearchMode::Semantic, false) => "semantic",
            (SearchMode::Hybrid, false) => "hybrid",
            (SearchMode::Lexical, true) => "lexical-rrf",
            (SearchMode::Semantic, true) => "semantic-rrf",
            (SearchMode::Hybrid, true) => "hybrid-rrf",
        };
        write_retrieval_trace(TraceWriteArgs {
            paths: &collected.paths,
            queries: &collected.queries,
            query_candidate_counts: &collected.query_candidate_counts,
            cwd: collected.cwd.clone(),
            results: &collected.results,
            mode: mode_label,
            machines: &collected.selected_machines,
            failures: &collected.failures,
            started: trace_started,
            started_at_ms: trace_started_at_ms,
        })?;
    }
    render_located_results(collected.results, &collected.render)
}

struct MemorySurfaceSearchArgs {
    query: String,
    additional_queries: Vec<String>,
    cwd: Option<PathBuf>,
    project: Option<String>,
    role: Option<String>,
    tool: Option<String>,
    session: Option<String>,
    source: Option<SourceFilter>,
    origin: SessionOrigin,
    mode: SearchMode,
    min_score: Option<f32>,
    recency_weight: f32,
    recency_half_life_days: f32,
    since: Option<String>,
    until: Option<String>,
    limit: usize,
    top_n_per_session: Option<usize>,
    unique_session: bool,
    fields: Option<HashSet<String>>,
    sort: SortBy,
    format: SearchFormat,
    pretty: bool,
    root: Option<PathBuf>,
    machines: Vec<String>,
    content: SearchContent,
    trace: bool,
}

enum UnifiedSearchResult {
    Conversation(LocatedRecord),
    Memory(LocatedMemoryHit),
}

impl UnifiedSearchResult {
    fn score(&self) -> f32 {
        match self {
            Self::Conversation(result) => result.score,
            Self::Memory(result) => result.hit.score,
        }
    }

    fn set_score(&mut self, score: f32) {
        match self {
            Self::Conversation(result) => result.score = score,
            Self::Memory(result) => result.hit.score = score,
        }
    }

    fn timestamp(&self) -> u64 {
        match self {
            Self::Conversation(result) => result.record.ts,
            Self::Memory(result) => result.hit.mtime_ms,
        }
    }

    fn tie_key(&self) -> (&str, &str) {
        match self {
            Self::Conversation(result) => (&result.machine, &result.record.session_id),
            Self::Memory(result) => (&result.machine, &result.hit.section_ref),
        }
    }
}

fn run_search_with_memories(args: MemorySurfaceSearchArgs) -> Result<()> {
    let format = args.format;
    let pretty = args.pretty;
    let (values, failures, _) = collect_search_with_memories(args)?;
    for failure in &failures {
        eprintln!("Warning: machine unavailable: {failure}");
    }
    render_unified_search(values, format, pretty)
}

fn collect_search_with_memories(
    args: MemorySurfaceSearchArgs,
) -> Result<(Vec<Value>, Vec<String>, Vec<String>)> {
    let MemorySurfaceSearchArgs {
        query,
        additional_queries,
        cwd,
        project,
        role,
        tool,
        session,
        source,
        origin,
        mode,
        min_score,
        recency_weight,
        recency_half_life_days,
        since,
        until,
        limit,
        top_n_per_session,
        unique_session,
        fields,
        sort,
        format,
        pretty,
        root,
        machines,
        content,
        trace,
    } = args;
    if limit == 0 || limit > 500 {
        return Err(anyhow!("limit must be between 1 and 500"));
    }
    let conversation_only_filters = role.is_some()
        || tool.is_some()
        || session.is_some()
        || !matches!(origin, SessionOrigin::Regular | SessionOrigin::All);
    if content == SearchContent::Memories && conversation_only_filters {
        return Err(anyhow!(
            "--role, --tool, --session, and --origin filter conversations; use --content conversations or all"
        ));
    }
    if trace {
        return Err(anyhow!(
            "retrieval traces currently record conversation searches only"
        ));
    }

    let paths = Paths::new(root.clone())?;
    let config = UserConfig::load(&paths)?;
    let memory_cwd = canonical_cwd_filter(cwd.clone()).map(PathBuf::from);
    let mut failures = Vec::new();
    let include_conversations = content == SearchContent::All;
    let include_memories = !conversation_only_filters;
    let mut conversation_results = Vec::new();
    if include_conversations {
        let collected = collect_search(SearchCollectRequest {
            query: query.clone(),
            additional_queries: additional_queries.clone(),
            cwd: cwd.clone(),
            project: project.clone(),
            role,
            tool,
            session,
            source,
            origin,
            mode,
            min_score,
            recency_weight,
            recency_half_life_days,
            since: since.clone(),
            until: until.clone(),
            limit,
            top_n_per_session,
            unique_session,
            fields: fields.clone(),
            sort,
            verbose: false,
            format: SearchFormat::Json,
            root: root.clone(),
            machines: machines.clone(),
        })?;
        failures.extend(collected.failures);
        conversation_results = collected.results;
    }

    let mut memory_results = Vec::new();
    if include_memories {
        let memory_mode = match mode {
            SearchMode::Lexical => MemorySearchMode::Lexical,
            SearchMode::Semantic => MemorySearchMode::Semantic,
            SearchMode::Hybrid => MemorySearchMode::Hybrid,
        };
        let max_per_document = if unique_session && top_n_per_session.is_none() {
            1
        } else {
            top_n_per_session.unwrap_or(2)
        };
        let options = MemorySearchOptions {
            query: query.clone(),
            additional_queries,
            mode: memory_mode,
            project,
            cwd: memory_cwd,
            source: source.and_then(|value| crate::types::SourceKind::from_label(value.as_str())),
            since: parse_ts_millis(since)?,
            until: parse_ts_millis(until)?,
            min_score,
            recency_weight,
            recency_half_life_days,
            limit,
            max_per_document: max_per_document.min(limit),
            sort_by_timestamp: sort == SortBy::Ts,
            include_text: fields.as_ref().is_none_or(|fields| fields.contains("text")),
        };
        options.validate()?;
        let federated = federated_memory_search(
            &paths,
            &config,
            &machines,
            &options,
            content == SearchContent::Memories,
        )?;
        for (machine, error) in federated.failures {
            failures.push(format!("{machine}: {error}"));
        }
        memory_results = federated.items;
        if content == SearchContent::Memories && memory_results.is_empty() && !failures.is_empty() {
            return Err(anyhow!("memory search failed: {}", failures.join("; ")));
        }
    }

    let mut results = if content == SearchContent::Memories {
        memory_results
            .into_iter()
            .map(UnifiedSearchResult::Memory)
            .collect::<Vec<_>>()
    } else {
        // Scores from independent conversation and memory indexes are not comparable. Rank each
        // corpus with reciprocal-rank fusion before merging them.
        let mut merged = Vec::with_capacity(conversation_results.len() + memory_results.len());
        for (rank, result) in conversation_results.into_iter().enumerate() {
            let mut result = UnifiedSearchResult::Conversation(result);
            result.set_score(1.0 / (60.0 + rank as f32 + 1.0));
            merged.push(result);
        }
        for (rank, result) in memory_results.into_iter().enumerate() {
            let mut result = UnifiedSearchResult::Memory(result);
            result.set_score(1.0 / (60.0 + rank as f32 + 1.0));
            merged.push(result);
        }
        merged.sort_by(|left, right| {
            let order = if sort == SortBy::Ts {
                right.timestamp().cmp(&left.timestamp())
            } else {
                right.score().total_cmp(&left.score())
            };
            order.then_with(|| left.tie_key().cmp(&right.tie_key()))
        });
        merged.truncate(limit);
        merged
    };
    results.truncate(limit);
    let mut values = Vec::with_capacity(results.len());
    for result in results {
        match result {
            UnifiedSearchResult::Conversation(result) => {
                let mut projected = project_located_results(
                    vec![result],
                    &RenderOptions {
                        verbose: false,
                        pretty,
                        matchers: build_matchers(&query)?,
                        format,
                        fields: fields.clone(),
                        sort,
                        min_score: None,
                        top_n_per_session: None,
                        limit: 1,
                        kind_filter: crate::analytics::SessionKindFilter::All,
                    },
                )?;
                if content == SearchContent::All
                    && fields.as_ref().is_none_or(|set| set.contains("kind"))
                {
                    projected[0]["kind"] = Value::from("conversation");
                }
                values.extend(projected);
            }
            UnifiedSearchResult::Memory(result) => {
                let mut projected = project_memory_result(result, &fields)?;
                if content == SearchContent::All
                    && fields.as_ref().is_none_or(|set| set.contains("kind"))
                {
                    projected["kind"] = Value::from("memory");
                }
                values.push(projected);
            }
        }
    }
    let selected_machines = crate::machine::selected_machine_ids(&config, &machines)?;
    Ok((values, failures, selected_machines))
}

fn project_memory_result(
    result: LocatedMemoryHit,
    fields: &Option<HashSet<String>>,
) -> Result<Value> {
    let mut value = serde_json::to_value(result)?;
    if let Some(fields) = fields {
        let object = value
            .as_object_mut()
            .ok_or_else(|| anyhow!("memory search result was not an object"))?;
        object.retain(|key, _| fields.contains(key));
    }
    Ok(value)
}

fn render_unified_search(values: Vec<Value>, format: SearchFormat, pretty: bool) -> Result<()> {
    match format {
        SearchFormat::Jsonl => {
            for value in values {
                println!("{}", serde_json::to_string(&value)?);
            }
        }
        SearchFormat::Json => print_json(&Value::Array(values), pretty)?,
        SearchFormat::Toon => println!(
            "{}",
            toon_format::encode_default(&serde_json::json!({"results": values}))?
        ),
        SearchFormat::Text => {
            for value in values {
                let score = value
                    .get("score")
                    .and_then(Value::as_f64)
                    .unwrap_or_default();
                let machine = value
                    .get("machine")
                    .and_then(Value::as_str)
                    .unwrap_or("local");
                let kind = value
                    .get("kind")
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| {
                        if value.get("memory_id").is_some() {
                            "memory"
                        } else {
                            "conversation"
                        }
                    });
                let identity = value
                    .get("memory_id")
                    .or_else(|| value.get("record_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("-");
                let snippet = value.get("snippet").and_then(Value::as_str).unwrap_or("");
                println!("[{score:.3}] {machine} {kind} {identity} {snippet}");
            }
        }
    }
    Ok(())
}

struct SearchCollectRequest {
    query: String,
    additional_queries: Vec<String>,
    cwd: Option<PathBuf>,
    project: Option<String>,
    role: Option<String>,
    tool: Option<String>,
    session: Option<String>,
    source: Option<SourceFilter>,
    origin: SessionOrigin,
    mode: SearchMode,
    min_score: Option<f32>,
    recency_weight: f32,
    recency_half_life_days: f32,
    since: Option<String>,
    until: Option<String>,
    limit: usize,
    top_n_per_session: Option<usize>,
    unique_session: bool,
    fields: Option<HashSet<String>>,
    sort: SortBy,
    verbose: bool,
    format: SearchFormat,
    root: Option<PathBuf>,
    machines: Vec<String>,
}

struct SearchCollection {
    paths: Paths,
    queries: Vec<String>,
    query_candidate_counts: Vec<usize>,
    cwd: Option<String>,
    results: Vec<LocatedRecord>,
    render: RenderOptions,
    selected_machines: Vec<String>,
    failures: Vec<String>,
}

fn collect_search(request: SearchCollectRequest) -> Result<SearchCollection> {
    let SearchCollectRequest {
        query,
        additional_queries,
        cwd,
        project,
        role,
        tool,
        session,
        source,
        origin,
        mode,
        min_score,
        recency_weight,
        recency_half_life_days,
        since,
        until,
        limit,
        top_n_per_session,
        unique_session,
        fields,
        sort,
        verbose,
        format,
        root,
        machines,
    } = request;
    let mut queries = vec![query];
    queries.extend(additional_queries);
    let mut seen_queries = HashSet::new();
    queries.retain(|query| {
        let query = query.trim();
        !query.is_empty() && seen_queries.insert(query.to_string())
    });
    if queries.is_empty() {
        return Err(anyhow!("at least one non-empty search query is required"));
    }
    let query = queries[0].clone();
    let cwd = canonical_cwd_filter(cwd);
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let options = QueryOptions {
        query,
        project,
        role,
        tool,
        session_id: session,
        session_scope: None,
        source,
        since: parse_ts_millis(since)?,
        until: parse_ts_millis(until)?,
        limit,
    };
    let matchers = queries
        .iter()
        .map(|query| build_matchers(query))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    let top_n_per_session = if unique_session && top_n_per_session.is_none() {
        Some(1)
    } else {
        top_n_per_session
    };
    let kind_filter: crate::analytics::SessionKindFilter = origin.into();
    let render = RenderOptions {
        verbose,
        pretty: false,
        matchers,
        format,
        fields,
        sort,
        min_score,
        top_n_per_session,
        limit,
        kind_filter,
    };

    // Origin filtering applies after retrieval, so a fixed overfetch can
    // still starve when wanted-kind matches rank below the cap. Re-run with
    // a wider cap (bounded) until the filtered list is full or retrieval
    // stops offering more candidates.
    let origin_filtered = kind_filter != crate::analytics::SessionKindFilter::All;
    let mut candidate_limit = if queries.len() > 1
        || top_n_per_session.is_some()
        || options.source.is_some()
        || cwd.is_some()
        || origin_filtered
    {
        (limit * 5).max(limit + 10)
    } else {
        limit
    };
    let selected_machines = crate::machine::selected_machine_ids(&config, &machines)?;
    let mut failures = Vec::new();
    let mut seen_failures = HashSet::new();
    let mut ranked_queries;
    let mut query_candidate_counts;
    let mut results;
    let mut round = 0;
    loop {
        ranked_queries = Vec::with_capacity(queries.len());
        query_candidate_counts = Vec::with_capacity(queries.len());
        let mut round_capped = false;
        for (query_index, query) in queries.iter().enumerate() {
            let spec = SearchSpec {
                query: query.clone(),
                project: options.project.clone(),
                role: options.role.clone(),
                tool: options.tool.clone(),
                session_id: options.session_id.clone(),
                session_scope: None,
                cwd: cwd.clone(),
                source: options.source,
                since: options.since,
                until: options.until,
                limit: candidate_limit,
                mode,
                recency_weight,
                recency_half_life_days,
                min_score,
                project_grouping: None,
            };
            let federated =
                federated_search(&paths, &config, &selected_machines, &spec, query_index == 0)?;
            round_capped = round_capped || federated.candidate_count > federated.items.len();
            query_candidate_counts.push(federated.candidate_count);
            for (machine, error) in federated.failures {
                let message = format!("{machine}: {error}");
                if seen_failures.insert(message.clone()) {
                    failures.push(message);
                }
            }
            ranked_queries.push(federated.items);
        }
        let fused = if ranked_queries.len() == 1 {
            ranked_queries.pop().unwrap_or_default()
        } else {
            fuse_ranked_queries(ranked_queries, crate::retrieval_eval::DEFAULT_RRF_K)
        };
        let stored_kinds = stored_session_kinds(&paths, &fused, origin_filtered);
        let mut merged_render = render.clone();
        merged_render.min_score = None;
        results = apply_post_processing_located(fused, &merged_render, &stored_kinds);
        round += 1;
        if !origin_filtered || results.len() >= render.limit || !round_capped || round >= 3 {
            break;
        }
        candidate_limit = candidate_limit.saturating_mul(5);
    }
    Ok(SearchCollection {
        paths,
        queries,
        query_candidate_counts,
        cwd,
        results,
        render,
        selected_machines,
        failures,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SearchFormat {
    Jsonl,
    Json,
    Toon,
    Text,
}

#[derive(Clone)]
struct RenderOptions {
    verbose: bool,
    pretty: bool,
    matchers: Vec<regex::Regex>,
    format: SearchFormat,
    fields: Option<HashSet<String>>,
    sort: SortBy,
    min_score: Option<f32>,
    top_n_per_session: Option<usize>,
    limit: usize,
    kind_filter: crate::analytics::SessionKindFilter,
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
    machine: String,
    score: f32,
    ts: String,
    doc_id: u64,
    record_id: String,
    project: String,
    role: String,
    session_id: String,
    source: String,
    source_path: String,
    text: String,
    snippet: String,
    matches: Vec<MatchSpan>,
    #[serde(flatten)]
    links: RecordLinks,
}

fn render_located_results(results: Vec<LocatedRecord>, render: &RenderOptions) -> Result<()> {
    if render.verbose || render.format == SearchFormat::Text {
        for LocatedRecord {
            machine,
            score,
            record,
        } in results
        {
            let ts = format_ts(record.ts);
            let text = match_preview(&record.text, &render.matchers, 200);
            println!(
                "[{score:.3}] {} {} {} {} {} {} {}",
                machine, ts, record.doc_id, record.project, record.role, record.session_id, text
            );
        }
        return Ok(());
    }

    let output = project_located_results(results, render)?;
    match render.format {
        SearchFormat::Jsonl => {
            for value in output {
                println!("{}", serde_json::to_string(&value)?);
            }
        }
        SearchFormat::Json => print_json(&Value::Array(output), render.pretty)?,
        SearchFormat::Text => unreachable!("text rendered above"),
        SearchFormat::Toon => println!(
            "{}",
            toon_format::encode_default(&serde_json::json!({"results": output}))?
        ),
    }
    Ok(())
}

fn project_located_results(
    results: Vec<LocatedRecord>,
    render: &RenderOptions,
) -> Result<Vec<Value>> {
    let mut output = Vec::new();
    for LocatedRecord {
        machine,
        score,
        record,
    } in results
    {
        let ts = format_ts(record.ts);
        let record_id = canonical_record_id(&record);
        let text_ref = record.text.as_str();
        let wants_snippet = wants_field(&render.fields, "snippet");
        let wants_matches = wants_field(&render.fields, "matches");
        let wants_text = wants_field(&render.fields, "text");
        let snippet = if wants_snippet {
            match_preview(text_ref, &render.matchers, 400)
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
            if fields.contains("machine") {
                map.insert("machine".to_string(), Value::from(machine.clone()));
            }
            if fields.contains("ts") {
                map.insert("ts".to_string(), Value::from(ts));
            }
            if fields.contains("doc_id") {
                map.insert("doc_id".to_string(), Value::from(record.doc_id));
            }
            if fields.contains("record_id") {
                map.insert("record_id".to_string(), Value::from(record_id.clone()));
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
            if fields.contains("source") {
                map.insert("source".to_string(), Value::from(record.source.label()));
            }
            insert_optional_field(&mut map, fields, "event_id", &record.links.event_id);
            insert_optional_field(
                &mut map,
                fields,
                "parent_event_id",
                &record.links.parent_event_id,
            );
            insert_optional_field(
                &mut map,
                fields,
                "logical_parent_event_id",
                &record.links.logical_parent_event_id,
            );
            insert_optional_field(
                &mut map,
                fields,
                "parent_session_id",
                &record.links.parent_session_id,
            );
            insert_optional_field(
                &mut map,
                fields,
                "thread_source",
                &record.links.thread_source,
            );
            insert_optional_field(
                &mut map,
                fields,
                "conversation_kind",
                &record.links.conversation_kind,
            );
            insert_optional_field(
                &mut map,
                fields,
                "parent_tool_use_id",
                &record.links.parent_tool_use_id,
            );
            insert_optional_field(
                &mut map,
                fields,
                "source_tool_use_id",
                &record.links.source_tool_use_id,
            );
            insert_optional_field(
                &mut map,
                fields,
                "source_tool_assistant_uuid",
                &record.links.source_tool_assistant_uuid,
            );
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
                machine,
                score,
                ts,
                doc_id: record.doc_id,
                record_id,
                project: record.project,
                role: record.role,
                session_id: record.session_id,
                source: record.source.label().to_string(),
                source_path: record.source_path,
                text,
                snippet,
                matches,
                links: record.links,
            })?
        };
        output.push(value);
    }
    Ok(output)
}

pub(crate) fn mcp_search(root: Option<PathBuf>, request: SearchRequest) -> Result<Value> {
    validate_mcp_search_request(&request)?;
    let source = parse_source_filter(request.source)?;
    if request.content != SearchContent::Conversations {
        let (results, failures, selected_machines) =
            collect_search_with_memories(MemorySurfaceSearchArgs {
                query: request.query,
                additional_queries: request.additional_queries,
                cwd: request.cwd.map(PathBuf::from),
                project: request.project,
                role: request.role,
                tool: request.tool,
                session: request.session,
                source,
                origin: request.origin,
                mode: request.mode.into(),
                min_score: request.min_score,
                recency_weight: request.recency_weight,
                recency_half_life_days: request.recency_half_life_days,
                since: request.since,
                until: request.until,
                limit: request.limit,
                top_n_per_session: request.top_n_per_session,
                unique_session: request.unique_session,
                fields: search_fields(None, false)?,
                sort: request.sort.into(),
                format: SearchFormat::Json,
                pretty: false,
                root,
                machines: request.machines,
                content: request.content,
                trace: false,
            })?;
        return Ok(serde_json::json!({
            "results": results,
            "failures": failures,
            "selected_machines": selected_machines,
        }));
    }
    let collected = collect_search(SearchCollectRequest {
        query: request.query,
        additional_queries: request.additional_queries,
        cwd: request.cwd.map(PathBuf::from),
        project: request.project,
        role: request.role,
        tool: request.tool,
        session: request.session,
        source,
        origin: request.origin,
        mode: request.mode.into(),
        min_score: request.min_score,
        recency_weight: request.recency_weight,
        recency_half_life_days: request.recency_half_life_days,
        since: request.since,
        until: request.until,
        limit: request.limit,
        top_n_per_session: request.top_n_per_session,
        unique_session: request.unique_session,
        fields: search_fields(None, false)?,
        sort: request.sort.into(),
        verbose: false,
        format: SearchFormat::Json,
        root,
        machines: request.machines,
    })?;
    let SearchCollection {
        results,
        render,
        selected_machines,
        failures,
        ..
    } = collected;
    let results = project_located_results(results, &render)?;
    Ok(serde_json::json!({
        "results": results,
        "failures": failures,
        "selected_machines": selected_machines,
    }))
}

fn validate_mcp_search_request(request: &SearchRequest) -> Result<()> {
    validate_mcp_limit(request.limit)?;
    let query_count = 1usize.saturating_add(request.additional_queries.len());
    if query_count > 8 {
        return Err(anyhow!("search accepts at most 8 queries"));
    }
    let query_chars = std::iter::once(&request.query)
        .chain(request.additional_queries.iter())
        .map(|query| query.chars().count())
        .sum::<usize>();
    if query_chars > 16_000 {
        return Err(anyhow!(
            "search query text exceeds the 16000 character limit"
        ));
    }
    if request
        .top_n_per_session
        .is_some_and(|value| !(1..=500).contains(&value))
    {
        return Err(anyhow!("top_n_per_session must be between 1 and 500"));
    }
    if request.min_score.is_some_and(|value| !value.is_finite()) {
        return Err(anyhow!("min_score must be finite"));
    }
    if !request.recency_weight.is_finite() || request.recency_weight < 0.0 {
        return Err(anyhow!("recency_weight must be finite and non-negative"));
    }
    if !request.recency_half_life_days.is_finite() || request.recency_half_life_days <= 0.0 {
        return Err(anyhow!(
            "recency_half_life_days must be finite and greater than zero"
        ));
    }
    Ok(())
}

fn validate_mcp_limit(limit: usize) -> Result<()> {
    if !(1..=500).contains(&limit) {
        return Err(anyhow!("limit must be between 1 and 500"));
    }
    Ok(())
}

pub(crate) fn parse_source_filter(source: Option<String>) -> Result<Option<SourceFilter>> {
    source
        .map(|source| {
            SourceFilter::from_str(&source, true).map_err(|_| anyhow!("unknown source '{source}'"))
        })
        .transpose()
}

fn insert_optional_field(
    map: &mut serde_json::Map<String, Value>,
    fields: &HashSet<String>,
    name: &str,
    value: &Option<String>,
) {
    if fields.contains(name)
        && let Some(value) = value
    {
        map.insert(name.to_string(), Value::from(value.clone()));
    }
}

#[derive(Debug, Clone, Args)]
struct ReadArgs {
    /// Maximum Unicode characters across text/tool input/tool output (default 16000; metadata excluded)
    #[arg(long, conflicts_with = "full")]
    max_chars: Option<usize>,
    /// Return complete content without a character budget
    #[arg(long)]
    full: bool,
}

impl ReadArgs {
    fn budget(&self) -> Result<ReadBudget> {
        ReadBudget::new(if self.full {
            None
        } else {
            Some(self.max_chars.unwrap_or(DEFAULT_MAX_CHARS))
        })
    }
}

struct ContextRunArgs {
    machine: String,
    offset: usize,
    record_id: Option<String>,
    doc_id: Option<u64>,
    event_id: Option<String>,
    session: Option<String>,
    source: Option<SourceFilter>,
    before: usize,
    after: usize,
    expand_interactions: bool,
    output: OutputOptions,
    read: ReadArgs,
    root: Option<PathBuf>,
}

fn run_context(args: ContextRunArgs) -> Result<()> {
    let ContextRunArgs {
        machine,
        offset,
        record_id,
        doc_id,
        event_id,
        session,
        source,
        before,
        after,
        expand_interactions,
        output,
        read,
        root,
    } = args;
    let budget = read.budget()?;
    let selector = match (record_id, doc_id, event_id) {
        (Some(id), None, None) => ContextSelector::record_id(id),
        (None, Some(id), None) => ContextSelector::doc_id(id),
        (None, None, Some(id)) => ContextSelector::event_id(id),
        _ => {
            return Err(anyhow!(
                "exactly one of --record-id, --doc-id, or --event-id is required"
            ));
        }
    };
    let source = source.and_then(|value| crate::types::SourceKind::from_label(value.as_str()));
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let result = read_context(
        &paths,
        &config,
        &machine,
        &selector.with_scope(session, source),
        ContextOptions {
            before,
            after,
            expand_interactions,
        },
        offset,
        budget.remaining(),
    )?;
    let mut value = serde_json::to_value(result)?;
    value["machine"] = Value::from(machine);
    output.print_value(&value)
}

fn print_json(value: &Value, pretty: bool) -> Result<()> {
    println!(
        "{}",
        if pretty {
            serde_json::to_string_pretty(value)?
        } else {
            serde_json::to_string(value)?
        }
    );
    Ok(())
}

#[derive(Debug, Deserialize)]
struct HydrateRequest {
    machine: Option<String>,
    session_id: String,
    #[serde(default)]
    source_path: String,
    #[serde(default)]
    offset: usize,
    limit: usize,
}

#[derive(Debug, Serialize)]
struct HydrateRecordOutput {
    #[serde(flatten)]
    record: crate::types::Record,
    record_id: String,
    content: ContentPage,
}

#[derive(Debug, Serialize)]
struct HydrateOutput {
    machine: String,
    session_id: String,
    source_path: String,
    cwd: Option<String>,
    offset: usize,
    total: usize,
    next_offset: Option<usize>,
    records: Vec<HydrateRecordOutput>,
}

#[derive(Debug, Serialize)]
struct HydrateErrorOutput {
    machine: String,
    session_id: String,
    source_path: String,
    offset: usize,
    error: String,
}

fn run_hydrate(
    input: Option<PathBuf>,
    read: ReadArgs,
    root: Option<PathBuf>,
    output: OutputOptions,
) -> Result<()> {
    let budget = read.budget()?;
    let mut contents = String::new();
    let mut reader: Box<dyn Read> = match input {
        Some(path) if path.to_string_lossy() != "-" => Box::new(std::fs::File::open(path)?),
        _ => Box::new(std::io::stdin()),
    };
    reader
        .by_ref()
        .take(MAX_HYDRATE_INPUT_BYTES as u64 + 1)
        .read_to_string(&mut contents)?;
    if contents.len() > MAX_HYDRATE_INPUT_BYTES {
        return Err(anyhow!(
            "session batch input exceeds maximum size of {MAX_HYDRATE_INPUT_BYTES} bytes"
        ));
    }
    let mut requests = Vec::new();
    for (line, raw) in contents.lines().enumerate() {
        if raw.trim().is_empty() {
            continue;
        }
        if raw.len() > MAX_HYDRATE_LINE_BYTES {
            return Err(anyhow!(
                "session batch request line {} exceeds maximum size of {} bytes",
                line + 1,
                MAX_HYDRATE_LINE_BYTES
            ));
        }
        let request = serde_json::from_str::<HydrateRequest>(raw)
            .with_context(|| format!("parse session batch request line {}", line + 1))?;
        if request.session_id.is_empty() {
            return Err(anyhow!(
                "session batch request line {} has an empty session_id",
                line + 1
            ));
        }
        if request.limit == 0 || request.limit > MAX_SESSION_PAGE_SIZE {
            return Err(anyhow!(
                "session batch request line {} limit must be between 1 and {}",
                line + 1,
                MAX_SESSION_PAGE_SIZE
            ));
        }
        requests.push(request);
    }
    if requests.is_empty() {
        return Err(anyhow!("session batch input is empty"));
    }
    if requests.len() > MAX_SESSION_BATCH_SIZE {
        return Err(anyhow!(
            "session batch accepts at most {MAX_SESSION_BATCH_SIZE} requests"
        ));
    }
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    // Contiguous batches preserve input order while allowing the peer to enforce
    // the remaining command budget before sending record bodies over SSH.
    let requests: Vec<_> = requests
        .into_iter()
        .map(|request| {
            (
                request
                    .machine
                    .unwrap_or_else(|| crate::machine::LOCAL_MACHINE_ID.to_string()),
                SessionPageRequest {
                    session_id: request.session_id,
                    source_path: request.source_path,
                    offset: request.offset,
                    limit: request.limit,
                },
            )
        })
        .collect();
    let mut writer = output.writer();
    let mut remaining = budget.remaining();
    let mut position = 0;
    while position < requests.len() {
        let machine = &requests[position].0;
        let end = position
            + requests[position..]
                .iter()
                .take_while(|(id, _)| id == machine)
                .count();
        let batch: Vec<_> = requests[position..end]
            .iter()
            .map(|(_, request)| request.clone())
            .collect();
        match read_session_pages(&paths, &config, machine, &batch, remaining) {
            Ok(pages) => {
                for page in pages {
                    writer.write(hydrate_page_value(machine, page, &mut remaining)?)?;
                }
            }
            Err(error) => {
                eprintln!("Warning: session batch machine '{machine}' failed: {error}");
                for request in &batch {
                    if machine == crate::machine::LOCAL_MACHINE_ID {
                        match read_session_pages(
                            &paths,
                            &config,
                            machine,
                            std::slice::from_ref(request),
                            remaining,
                        ) {
                            Ok(pages) => {
                                for page in pages {
                                    writer.write(hydrate_page_value(
                                        machine,
                                        page,
                                        &mut remaining,
                                    )?)?;
                                }
                                continue;
                            }
                            Err(error) => {
                                writer.write(hydrate_error_value(
                                    machine,
                                    request,
                                    &error.to_string(),
                                )?)?;
                                continue;
                            }
                        }
                    }
                    writer.write(hydrate_error_value(machine, request, &error.to_string())?)?;
                }
            }
        }
        position = end;
    }
    writer.finish()
}

fn hydrate_page_value(
    machine: &str,
    page: crate::machine::BoundedSessionPage,
    remaining: &mut Option<usize>,
) -> Result<Value> {
    let returned: usize = page
        .records
        .iter()
        .map(|record| record.content.returned_chars)
        .sum();
    if let Some(remaining) = remaining {
        *remaining = remaining
            .checked_sub(returned)
            .ok_or_else(|| anyhow!("hydrate response exceeds remaining character budget"))?;
    }
    Ok(serde_json::to_value(HydrateOutput {
        machine: machine.to_string(),
        session_id: page.session_id,
        source_path: page.source_path,
        cwd: page.cwd,
        offset: page.offset,
        total: page.total,
        next_offset: page.next_offset,
        records: page
            .records
            .into_iter()
            .map(|item| HydrateRecordOutput {
                record: item.record,
                record_id: item.record_id,
                content: item.content,
            })
            .collect(),
    })?)
}

fn hydrate_error_value(machine: &str, request: &SessionPageRequest, error: &str) -> Result<Value> {
    Ok(serde_json::to_value(HydrateErrorOutput {
        machine: machine.to_string(),
        session_id: request.session_id.clone(),
        source_path: request.source_path.clone(),
        offset: request.offset,
        error: error.to_string(),
    })?)
}

fn run_eval_retrieval(dataset_path: PathBuf, k: usize, root: Option<PathBuf>) -> Result<()> {
    let dataset = EvaluationDataset::read_jsonl(&dataset_path)?;
    let paths = Paths::new(root)?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    let mut result_lists = Vec::with_capacity(dataset.cases.len());
    for case in &dataset.cases {
        let scope = case
            .cwd
            .as_deref()
            .map(|cwd| session_scope_for_cwd(&paths, cwd))
            .transpose()?
            .flatten();
        let mut ranked = Vec::new();
        for query in case.query_views()? {
            let options = QueryOptions {
                query,
                project: None,
                role: None,
                tool: None,
                session_id: None,
                session_scope: scope.clone(),
                source: None,
                since: None,
                until: None,
                limit: k.max(20),
            };
            ranked.push(
                index
                    .search(&options)?
                    .into_iter()
                    .map(|(score, record)| LocatedRecord {
                        machine: crate::machine::LOCAL_MACHINE_ID.to_string(),
                        score,
                        record,
                    })
                    .collect(),
            );
        }
        result_lists.push(fuse_ranked_queries(
            ranked,
            crate::retrieval_eval::DEFAULT_RRF_K,
        ));
    }
    let mrr = mean_reciprocal_rank(&result_lists, &dataset.cases)?;
    let ndcg = dataset
        .cases
        .iter()
        .zip(&result_lists)
        .map(|(case, results)| ndcg_at_k(results, &case.relevant, k))
        .sum::<f64>()
        / dataset.cases.len() as f64;
    let recall = dataset
        .cases
        .iter()
        .zip(&result_lists)
        .map(|(case, results)| recall_at_k(results, &case.relevant, k))
        .sum::<f64>()
        / dataset.cases.len() as f64;
    let unique_sessions = result_lists
        .iter()
        .map(|results| unique_sessions_at_k(results, k))
        .sum::<usize>() as f64
        / dataset.cases.len() as f64;
    println!(
        "{}",
        serde_json::json!({
            "cases": dataset.cases.len(),
            "k": k,
            "mrr": mrr,
            "recall_at_k": recall,
            "ndcg_at_k": ndcg,
            "mean_unique_sessions_at_k": unique_sessions,
        })
    );
    Ok(())
}

struct SessionRunArgs {
    session_id: String,
    machine: String,
    source_path: Option<String>,
    offset: usize,
    limit: Option<usize>,
    output: OutputOptions,
    read: ReadArgs,
    root: Option<PathBuf>,
}

fn run_session(args: SessionRunArgs) -> Result<()> {
    let SessionRunArgs {
        session_id,
        machine,
        source_path,
        offset,
        limit,
        output,
        read,
        root,
    } = args;
    let mut budget = read.budget()?;
    if session_id.is_empty() {
        return Err(anyhow!("session_id must not be empty"));
    }
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let source_path = source_path.unwrap_or_default();
    let (records, page) = if read.full {
        let records = hydrate_session_records(
            &paths,
            &config,
            &machine,
            &session_id,
            &source_path,
            offset,
            limit,
        )?
        .into_iter()
        .map(|mut record| {
            let record_id = canonical_record_id(&record);
            let content = budget.apply(&mut record, None, 0)?;
            Ok(BoundedRecord {
                record_id,
                record,
                content,
            })
        })
        .collect::<Result<Vec<_>>>()?;
        (records, None)
    } else {
        let request = SessionPageRequest {
            session_id: session_id.clone(),
            source_path: source_path.clone(),
            offset,
            limit: limit.unwrap_or(50),
        };
        let mut pages =
            read_session_pages(&paths, &config, &machine, &[request], budget.remaining())?;
        let page = pages
            .pop()
            .ok_or_else(|| anyhow!("session response missing page"))?;
        (page.records, Some((page.total, page.next_offset)))
    };
    let text = output.format == OutputFormat::Text;
    let mut writer = output.writer();
    for item in records {
        if text {
            println!("{} {}", format_ts(item.record.ts), item.record.role);
            for line in item.record.text.lines() {
                println!("  {line}");
            }
            for (name, value) in [
                ("tool_input", &item.record.tool_input),
                ("tool_output", &item.record.tool_output),
            ] {
                if let Some(value) = value {
                    println!("  {name}: {value}");
                }
            }
            if item.content.truncated {
                println!(
                    "  continuation: {}",
                    serde_json::to_string(
                        &serde_json::json!({"machine": machine, "record_id": item.record_id, "content": item.content})
                    )?
                );
            }
        } else {
            let mut value = serde_json::to_value(item)?;
            value["machine"] = Value::from(machine.clone());
            writer.write(value)?;
        }
    }
    if let Some((total, next_offset)) = page {
        let value = serde_json::json!({ "type": "page", "machine": machine, "session_id": session_id, "source_path": source_path, "offset": offset, "total": total, "next_offset": next_offset });
        if text {
            println!("page: {value}");
        } else {
            writer.write(value)?;
        }
    }
    if !text {
        writer.finish()?;
    }
    Ok(())
}

struct ShowRunArgs {
    selector: ContextSelector,
    field: Option<ReadField>,
    offset_chars: usize,
    machine: String,
    output: OutputOptions,
    read: ReadArgs,
    root: Option<PathBuf>,
}

struct MemoryShowRunArgs {
    memory_id: String,
    section: Option<String>,
    content_version: Option<String>,
    offset_chars: usize,
    machine: String,
    output: OutputOptions,
    read: ReadArgs,
    root: Option<PathBuf>,
}

fn run_show_memory(args: MemoryShowRunArgs) -> Result<()> {
    let paths = Paths::new(args.root)?;
    let config = UserConfig::load(&paths)?;
    if args
        .read
        .max_chars
        .is_some_and(|max_chars| max_chars > MAX_MEMORY_READ_CHARS)
    {
        return Err(anyhow!(
            "memory --max-chars cannot exceed {MAX_MEMORY_READ_CHARS}"
        ));
    }
    let request = MemoryReadRequest {
        memory_id: args.memory_id,
        section_ref: args.section,
        content_version: args.content_version,
        offset_chars: args.offset_chars,
        max_chars: args.read.max_chars.unwrap_or(DEFAULT_MAX_CHARS),
    };
    let result = if args.read.full {
        read_full_memory(&paths, &config, &args.machine, &request)?
    } else {
        read_memory(&paths, &config, &args.machine, &request)?
    };
    let mut value = serde_json::to_value(result)?;
    value["machine"] = Value::from(args.machine);
    args.output.print_value(&value)
}

fn read_full_memory(
    paths: &Paths,
    config: &UserConfig,
    machine: &str,
    original: &MemoryReadRequest,
) -> Result<MemoryReadValue> {
    const MAX_VERSION_RESTARTS: usize = 2;
    for attempt in 0..=MAX_VERSION_RESTARTS {
        let mut request = original.clone();
        request.max_chars = MAX_MEMORY_READ_CHARS;
        let mut first = read_memory(paths, config, machine, &request)?;
        let version = first.content_version.clone();
        let mut text = first.text.clone();
        let mut next = first.next_offset_chars;
        let mut changed_mid_read = false;
        while let Some(offset_chars) = next {
            request.section_ref = first.section_ref.clone();
            request.content_version = Some(version.clone());
            request.offset_chars = offset_chars;
            let page = read_memory(paths, config, machine, &request)?;
            if page.changed_since_search || page.content_version != version {
                changed_mid_read = true;
                break;
            }
            text.push_str(&page.text);
            next = page.next_offset_chars;
        }
        if changed_mid_read {
            if attempt == MAX_VERSION_RESTARTS {
                return Err(anyhow!(
                    "memory changed repeatedly while reading; search again and retry with the new content_version"
                ));
            }
            continue;
        }
        first.text = text;
        first.content.returned_chars = first.text.chars().count();
        first.content.truncated = false;
        first.content.continuations.clear();
        first.next_offset_chars = None;
        return Ok(first);
    }
    unreachable!("bounded memory read retry loop always returns")
}

fn run_show(args: ShowRunArgs) -> Result<()> {
    let ShowRunArgs {
        selector,
        field,
        offset_chars,
        machine,
        output,
        read,
        root,
    } = args;
    let budget = read.budget()?;
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let record = read_record(
        &paths,
        &config,
        &machine,
        &selector,
        field,
        offset_chars,
        budget.remaining(),
    )?;
    let mut value = serde_json::to_value(record)?;
    value["machine"] = Value::from(machine);
    output.print_value(&value)
}

fn hydrate_session_records(
    paths: &Paths,
    config: &UserConfig,
    machine: &str,
    session_id: &str,
    source_path: &str,
    offset: usize,
    limit: Option<usize>,
) -> Result<Vec<crate::types::Record>> {
    if limit.is_some_and(|limit| limit == 0 || limit > MAX_SESSION_PAGE_SIZE) {
        return Err(anyhow!(
            "session limit must be between 1 and {MAX_SESSION_PAGE_SIZE}"
        ));
    }
    let page_limit = limit.unwrap_or(MAX_SESSION_PAGE_SIZE);
    let mut next_offset = offset;
    let mut records = Vec::new();
    loop {
        let context = session_page_context(
            paths,
            config,
            machine,
            &SessionPageRequest {
                session_id: session_id.to_string(),
                source_path: source_path.to_string(),
                offset: next_offset,
                limit: page_limit,
            },
        )?;
        records.extend(context.records);
        if limit.is_some() || context.next_offset.is_none() {
            break;
        }
        next_offset = context.next_offset.expect("checked above");
    }
    Ok(records)
}

fn run_stats(root: Option<PathBuf>) -> Result<()> {
    let paths = Paths::new(root)?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    let memory = MemoryStore::new(paths.root.join("memory/documents.json")).load()?;
    let memory_sections = memory
        .documents
        .iter()
        .map(|document| document.sections.len())
        .sum::<usize>();
    let stale_memories = memory
        .documents
        .iter()
        .filter(|document| matches!(document.freshness, MemoryFreshness::Stale { .. }))
        .count();
    println!("index: {}", paths.index.display());
    println!("documents: {}", index.doc_count()?);
    println!("memory documents: {}", memory.documents.len());
    println!("memory sections: {memory_sections}");
    println!("stale memory documents: {stale_memories}");
    print_vector_stats(&paths.vectors)?;
    Ok(())
}

struct UsageCommandOptions {
    source: Option<SourceFilter>,
    origin: SessionOrigin,
    since: Option<String>,
    until: Option<String>,
    output: OutputOptions,
    include_events: bool,
    cost_mode: CostMode,
    root: Option<PathBuf>,
    machines: Vec<String>,
}

fn run_usage(options: UsageCommandOptions) -> Result<()> {
    let UsageCommandOptions {
        source,
        origin,
        since,
        until,
        output,
        include_events,
        cost_mode,
        root,
        machines,
    } = options;
    if include_events && output.format == OutputFormat::Text {
        return Err(anyhow!("--events requires --format json or --format jsonl"));
    }
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let since_ms = parse_ts_millis(since)?;
    let until_ms = parse_ts_millis(until)?;
    if !machines.is_empty() || !config.machines.is_empty() {
        let report = federated_usage(
            &paths,
            &config,
            &machines,
            &UsageSpec {
                source,
                project: None,
                project_grouping: crate::analytics::ProjectGrouping::Flat,
                session_keys: None,
                machine_session_keys: None,
                since_ms,
                until_ms,
                cost_mode,
                include_events,
                memo_ttl_ms: 0,
                kind: Some(origin.into()),
            },
        )?;
        if output.format != OutputFormat::Text {
            output.print_value(&serde_json::to_value(&report)?)?;
        } else {
            println!("{}", report.authority);
            print_usage_rows(
                report.events,
                report.total_tokens,
                report.known_cost_usd,
                report.priced_events,
                report.unpriced_events,
                &report.cache_waste,
                &report.by_source,
            );
            println!(
                "cost: API-equivalent at {} pricing, catalog {} ({} priced, {} unpriced events)",
                format!("{:?}", report.cost_mode).to_lowercase(),
                report.price_catalog,
                format_count(report.priced_events),
                format_count(report.unpriced_events),
            );
            for warning in &report.warnings {
                eprintln!("warning: {warning}");
            }
        }
        return Ok(());
    }
    if !config.token_usage_enabled() {
        return Err(anyhow!(
            "token usage tracking is disabled; set `token_usage = true` in {}",
            paths.root.join("config.toml").display()
        ));
    }
    let query = UsageQuery {
        source,
        project: None,
        project_grouping: crate::analytics::ProjectGrouping::Flat,
        session_keys: match origin {
            SessionOrigin::All | SessionOrigin::Regular => None,
            other => Some(crate::machine::usage_session_keys_for_kind(
                &paths,
                other.into(),
            )?),
        },
        since_ms,
        until_ms,
        cost_mode,
        include_events,
        include_reviews: origin == SessionOrigin::All,
        cache_path: Some(paths.state.join("usage-cache.sqlite3")),
        memo_ttl_ms: 0,
    };
    // A cold usage cache re-parses whole log corpora, which can take minutes; narrate the
    // parse phase on stderr so the scan doesn't read as a hang. Rendered with the same
    // spinner grammar as `memex index`: one persistent line per source as it completes.
    let scan_finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reporter = std::io::IsTerminal::is_terminal(&std::io::stderr()).then(|| {
        let scan_finished = scan_finished.clone();
        std::thread::spawn(move || {
            let style = ProgressStyle::with_template("  {spinner:.cyan} {msg}")
                .expect("static template")
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏");
            let multi = MultiProgress::new();
            let mut active: Option<(&'static str, usize, ProgressBar)> = None;
            loop {
                if let Some(progress) = crate::usage::usage_scan_progress() {
                    if active
                        .as_ref()
                        .is_none_or(|(source, ..)| *source != progress.source)
                    {
                        if let Some((source, total, bar)) = active.take() {
                            finish_scan_bar(&bar, source, total);
                        }
                        let bar = multi.add(ProgressBar::new_spinner());
                        bar.set_style(style.clone());
                        bar.enable_steady_tick(Duration::from_millis(80));
                        active = Some((progress.source, progress.total, bar));
                    }
                    if let Some((source, total, bar)) = &mut active {
                        *total = progress.total;
                        bar.set_message(format!(
                            "{} parsed {}/{} files",
                            source,
                            crate::progress::format_count(progress.done as u64),
                            crate::progress::format_count(progress.total as u64),
                        ));
                    }
                }
                if scan_finished.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            if let Some((source, total, bar)) = active {
                finish_scan_bar(&bar, source, total);
                // Leave the draw region on a fresh line so the report doesn't append to
                // the frozen spinner line.
                eprintln!();
            }
        })
    });
    let report = scan_usage(&query);
    scan_finished.store(true, std::sync::atomic::Ordering::Relaxed);
    if let Some(reporter) = reporter {
        let _ = reporter.join();
    }
    let report = report?;
    if output.format != OutputFormat::Text {
        output.print_value(&serde_json::to_value(&report)?)?;
    } else {
        println!("local reconstructed usage (not subscription quota)");
        print_usage_table(&report);
        println!(
            "cost: API-equivalent at {} pricing, catalog {} ({} priced, {} unpriced events)",
            format!("{:?}", report.cost_mode).to_lowercase(),
            report.price_catalog,
            format_count(report.priced_events),
            format_count(report.unpriced_events),
        );
        if report.cache_waste.miss_count > 0 {
            println!(
                "cache: re-billed estimates prompt tokens lost to cache misses, at catalog rates ({} misses: {} idle, {} model-switch)",
                format_count(report.cache_waste.miss_count),
                format_count(report.cache_waste.idle_misses),
                format_count(report.cache_waste.model_switch_misses),
            );
        }
        if report.unknown_model_events > 0 || report.conservative_events > 0 {
            println!(
                "quality: {} unknown-model events, {} conservatively undercounted events",
                format_count(report.unknown_model_events),
                format_count(report.conservative_events),
            );
        }
        for warning in &report.warnings {
            eprintln!("warning: {warning}");
        }
    }
    Ok(())
}

fn print_usage_table(report: &crate::usage::UsageReport) {
    print_usage_rows(
        report.events,
        report.total_tokens,
        report.known_cost_usd,
        report.priced_events,
        report.unpriced_events,
        &report.cache_waste,
        &report.by_source,
    );
}

#[allow(clippy::too_many_arguments)]
fn print_usage_rows(
    events: u64,
    total_tokens: u64,
    known_cost_usd: f64,
    priced_events: u64,
    unpriced_events: u64,
    cache_waste: &crate::usage::CacheWaste,
    by_source: &[crate::usage::UsageSummary],
) {
    const HEADERS: [&str; 10] = [
        "source",
        "events",
        "input",
        "cache read",
        "cache write",
        "output",
        "total",
        "cost",
        "hit",
        "re-billed",
    ];
    let mut totals = crate::usage::UsageSummary {
        source: "total".to_string(),
        events,
        total_tokens,
        known_cost_usd,
        priced_events,
        unpriced_events,
        cache_waste: cache_waste.clone(),
        ..Default::default()
    };
    for row in by_source {
        totals.uncached_input += row.uncached_input;
        totals.cache_read += row.cache_read;
        totals.cache_write += row.cache_write;
        totals.output += row.output;
    }
    let cells = |row: &crate::usage::UsageSummary| -> [String; 10] {
        let prompt_tokens = row.uncached_input + row.cache_read + row.cache_write;
        let cache_active = row.cache_read > 0 || row.cache_write > 0;
        [
            row.source.clone(),
            format_count(row.events),
            format_count(row.uncached_input),
            format_count(row.cache_read),
            format_count(row.cache_write),
            format_count(row.output),
            format_count(row.total_tokens),
            if row.priced_events > 0 {
                format_usd(row.known_cost_usd)
            } else {
                "-".to_string()
            },
            if cache_active && prompt_tokens > 0 {
                format!(
                    "{:.1}%",
                    row.cache_read as f64 / prompt_tokens as f64 * 100.0
                )
            } else {
                "-".to_string()
            },
            if row.cache_waste.miss_count > 0 {
                format_usd(row.cache_waste.missed_cost_usd)
            } else if cache_active {
                "$0.00".to_string()
            } else {
                "-".to_string()
            },
        ]
    };
    let mut table: Vec<[String; 10]> = vec![HEADERS.map(str::to_string)];
    table.extend(by_source.iter().map(cells));
    table.push(cells(&totals));
    let mut widths = [0usize; 10];
    for row in &table {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.len());
        }
    }
    for row in &table {
        let mut line = String::new();
        for (index, (cell, width)) in row.iter().zip(&widths).enumerate() {
            if index > 0 {
                line.push_str("  ");
            }
            if index == 0 {
                line.push_str(&format!("{cell:<width$}"));
            } else {
                line.push_str(&format!("{cell:>width$}"));
            }
        }
        println!("{}", line.trim_end());
    }
}

/// Freeze a scan spinner line in place, mirroring the `memex index` finish style. A source
/// only leaves the active slot once its scan completed, so the frozen line reports the
/// file total rather than the last polled position.
fn finish_scan_bar(bar: &ProgressBar, source: &str, total: usize) {
    bar.finish_with_message(format!(
        "{source} parsed {} files done",
        crate::progress::format_count(total as u64)
    ));
}

/// Humanized count with three significant digits; small values stay exact.
fn format_count(value: u64) -> String {
    const UNITS: [(f64, &str); 4] = [(1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "k")];
    if value < 10_000 {
        return value.to_string();
    }
    let value = value as f64;
    for (scale, suffix) in UNITS {
        if value >= scale {
            let scaled = value / scale;
            return if scaled >= 100.0 {
                format!("{scaled:.0}{suffix}")
            } else if scaled >= 10.0 {
                format!("{scaled:.1}{suffix}")
            } else {
                format!("{scaled:.2}{suffix}")
            };
        }
    }
    unreachable!("values below 10k return early")
}

fn format_usd(value: f64) -> String {
    if value > 0.0 && value < 0.01 {
        format!("${value:.4}")
    } else {
        format!("${value:.2}")
    }
}

fn open_analytics_read_only(paths: &Paths) -> Result<AnalyticsStore> {
    let db = analytics_path(&paths.state);
    if !db.exists() {
        return Err(anyhow!(
            "no analytics cache at {} (run `memex index` first)",
            db.display()
        ));
    }
    AnalyticsStore::open_read_only(&db)
}

fn canonical_cwd_filter(cwd: Option<PathBuf>) -> Option<String> {
    let cwd = cwd?;
    let resolved = std::fs::canonicalize(&cwd).unwrap_or(cwd);
    Some(resolved.to_string_lossy().to_string())
}

fn session_scope_for_cwd(paths: &Paths, cwd: &str) -> Result<Option<Vec<SessionScopeKey>>> {
    let db = analytics_path(&paths.state);
    if !db.exists() {
        return Ok(Some(Vec::new()));
    }
    let store = AnalyticsStore::open_read_only(db)?;
    let rows = store.query_sessions_detailed(None, None, Some(cwd), None, None)?;
    Ok(Some(
        rows.into_iter()
            .map(|row| SessionScopeKey {
                source: row.source,
                session_id: row.session_id,
                source_path: row.source_path,
            })
            .collect(),
    ))
}

struct TraceWriteArgs<'a> {
    paths: &'a Paths,
    queries: &'a [String],
    query_candidate_counts: &'a [usize],
    cwd: Option<String>,
    results: &'a [LocatedRecord],
    mode: &'a str,
    machines: &'a [String],
    failures: &'a [String],
    started: Instant,
    started_at_ms: u64,
}

fn write_retrieval_trace(args: TraceWriteArgs<'_>) -> Result<()> {
    let TraceWriteArgs {
        paths,
        queries,
        query_candidate_counts,
        cwd,
        results,
        mode,
        machines,
        failures,
        started,
        started_at_ms,
    } = args;
    let trace_id = format!(
        "{}-{}-{}",
        started_at_ms,
        std::process::id(),
        TRACE_COUNTER.fetch_add(1, AtomicOrdering::Relaxed)
    );
    let queries = queries
        .iter()
        .enumerate()
        .map(|(query_index, query)| TraceQuery {
            query_index,
            query: query.clone(),
            candidate_count: query_candidate_counts
                .get(query_index)
                .copied()
                .unwrap_or_default(),
        })
        .collect();
    let candidate_count = query_candidate_counts.iter().sum();
    let trace = RetrievalTrace::from_results(
        RetrievalTraceMetadata {
            trace_id: trace_id.clone(),
            started_at_ms,
            elapsed_ms: Some(started.elapsed().as_millis().min(u64::MAX as u128) as u64),
            mode: Some(mode.to_string()),
            queries,
            cwd,
            machines: machines.to_vec(),
            candidate_count,
            failures: failures.to_vec(),
        },
        results,
    );
    append_trace(paths, &trace)?;
    eprintln!("retrieval trace: {trace_id}");
    Ok(())
}

fn source_dir_of(source_path: &str) -> String {
    std::path::Path::new(source_path)
        .parent()
        .map(|dir| dir.to_string_lossy().to_string())
        .unwrap_or_default()
}

fn session_resume_command(
    config: &UserConfig,
    row: &crate::analytics::SessionDetailRow,
) -> Option<(String, String)> {
    let template = crate::resume::resume_template(config, row.source, false)?;
    let source_dir = source_dir_of(&row.source_path);
    let cwd = row.cwd.clone().unwrap_or_else(|| source_dir.clone());
    let command = crate::resume::expand_resume_template(
        &template,
        &crate::resume::ResumeSession {
            source: row.source,
            session_id: &row.session_id,
            project: &row.project,
            source_path: &row.source_path,
            source_dir: &source_dir,
        },
        &cwd,
    );
    Some((command, cwd))
}

#[allow(clippy::too_many_arguments)]
fn run_sessions(
    session_id: Option<String>,
    source_path: Option<String>,
    cwd: Option<PathBuf>,
    project: Option<String>,
    source: Option<SourceFilter>,
    since: Option<String>,
    limit: usize,
    origin: SessionOrigin,
    output: OutputOptions,
    root: Option<PathBuf>,
    machine: Option<String>,
) -> Result<()> {
    let mut items = if let Some(id) = machine
        .as_deref()
        .filter(|id| *id != crate::machine::LOCAL_MACHINE_ID)
    {
        let paths = Paths::new(root)?;
        let config = UserConfig::load(&paths)?;
        crate::machine::remote_sessions(
            &config,
            id,
            SessionsRequest {
                session_id,
                source_path,
                cwd: cwd.map(|value| value.to_string_lossy().to_string()),
                project,
                source: source.map(|value| value.as_str().to_string()),
                since,
                limit,
                origin,
            },
        )?
    } else {
        collect_sessions(
            session_id,
            source_path,
            cwd,
            project,
            source,
            since,
            limit,
            origin,
            root,
        )?
    };
    if let Some(machine) = machine {
        for item in &mut items {
            item["machine"] = Value::String(machine.clone());
        }
    }
    output.print_values(items)
}

pub(crate) fn collect_projects(paths: &Paths, source: Option<SourceFilter>) -> Result<Vec<Value>> {
    let store = open_analytics_read_only(paths)?;
    Ok(store.query_project_summaries(source)?.into_iter().map(|row| {
        let last_at = row.last_at.and_then(|timestamp| {
            let formatted = format_ts(timestamp);
            (formatted != "-").then_some(formatted)
        });
        serde_json::json!({"project": row.project, "session_count": row.session_count, "last_at": last_at})
    }).collect())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn collect_sessions(
    session_id: Option<String>,
    source_path: Option<String>,
    cwd: Option<PathBuf>,
    project: Option<String>,
    source: Option<SourceFilter>,
    since: Option<String>,
    limit: usize,
    origin: SessionOrigin,
    root: Option<PathBuf>,
) -> Result<Vec<Value>> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let store = open_analytics_read_only(&paths)?;
    let since_ms = parse_ts_millis(since)?;
    let cwd_filter = canonical_cwd_filter(cwd);
    let kind_filter = match origin {
        SessionOrigin::All => None,
        other => Some(other.into()),
    };
    let rows = store.query_sessions_detailed_selected(
        source,
        project.as_deref(),
        cwd_filter.as_deref(),
        since_ms,
        kind_filter,
        session_id.as_deref(),
        source_path.as_deref(),
        Some(limit),
    )?;

    let mut items = Vec::new();
    for row in &rows {
        let resume_cmd = session_resume_command(&config, row).map(|(command, _)| command);
        let mut value = serde_json::to_value(row)?;
        let object = value
            .as_object_mut()
            .expect("session row serializes to object");
        object.insert(
            "started_at".into(),
            Value::String(format_ts(row.started_at)),
        );
        object.insert("last_at".into(), Value::String(format_ts(row.last_at)));
        if let Some(resume_cmd) = resume_cmd {
            object.insert("resume_cmd".into(), Value::String(resume_cmd));
        }
        items.push(value);
    }
    Ok(items)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SessionCount {
    pub(crate) total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

pub(crate) fn collect_session_count(
    paths: &Paths,
    request: SessionsRequest,
    query: Option<String>,
) -> Result<SessionCount> {
    let source = parse_source_filter(request.source)?;
    let since = parse_ts_millis(request.since)?;
    let kind = request.origin.into();
    let query = query.filter(|query| !query.trim().is_empty());
    if let Some(query) = query {
        if request.cwd.is_some() {
            return Err(anyhow!("--query count does not support --cwd"));
        }
        if !SearchIndex::exists(&paths.index) {
            return Ok(SessionCount {
                total: None,
                reason: Some("search index unavailable".into()),
            });
        }
        let index = SearchIndex::open_or_create(&paths.index)?;
        let scopes = index.fast_session_scopes_matching_query(&QueryOptions {
            query,
            project: request.project,
            role: None,
            tool: None,
            session_id: request.session_id,
            session_scope: None,
            source,
            since,
            until: None,
            limit: 1,
        })?;
        let Some(mut scopes) = scopes else {
            return Ok(SessionCount {
                total: None,
                reason: Some("index lacks fast session identity fields".into()),
            });
        };
        if let Some(path) = request.source_path {
            scopes.retain(|scope| scope.2 == path);
        }
        if scopes.is_empty() || kind == crate::analytics::SessionKindFilter::All {
            return Ok(SessionCount {
                total: Some(scopes.len() as u64),
                reason: None,
            });
        }
        let store = match open_analytics_read_only(paths) {
            Ok(store) => store,
            Err(_) => {
                return Ok(SessionCount {
                    total: None,
                    reason: Some("canonical session metadata unavailable".into()),
                });
            }
        };
        let total = store.count_session_scopes(&scopes, kind)?;
        Ok(SessionCount {
            total,
            reason: total
                .is_none()
                .then(|| "canonical session metadata incomplete".into()),
        })
    } else {
        let store = open_analytics_read_only(paths)?;
        let cwd = canonical_cwd_filter(request.cwd.map(PathBuf::from));
        let total = store.count_sessions_selected(
            source,
            request.project.as_deref(),
            cwd.as_deref(),
            since,
            Some(kind),
            request.session_id.as_deref(),
            request.source_path.as_deref(),
        )?;
        Ok(SessionCount {
            total: Some(total),
            reason: None,
        })
    }
}

pub(crate) fn mcp_sessions(root: Option<PathBuf>, request: SessionsRequest) -> Result<Value> {
    validate_mcp_limit(request.limit)?;
    let source = parse_source_filter(request.source)?;
    let mut results = collect_sessions(
        request.session_id,
        request.source_path,
        request.cwd.map(PathBuf::from),
        request.project,
        source,
        request.since,
        request.limit,
        request.origin,
        root,
    )?;
    for value in &mut results {
        value["machine"] = Value::String(crate::machine::LOCAL_MACHINE_ID.to_string());
    }
    Ok(serde_json::json!({
        "results": results,
        "failures": [],
        "selected_machines": [crate::machine::LOCAL_MACHINE_ID],
    }))
}

fn run_herdr_resume(
    session_id: Option<String>,
    cwd: Option<PathBuf>,
    strict_cwd: bool,
    source: Option<SourceFilter>,
    root: Option<PathBuf>,
) -> Result<()> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let store = open_analytics_read_only(&paths)?;
    let cwd_filter = canonical_cwd_filter(cwd);

    let mut rows =
        store.query_sessions_detailed(source, None, cwd_filter.as_deref(), None, None)?;
    // Resume-last targets the user's own work, never a background worker
    // transcript. An explicit session id still resumes anything.
    let interactive = |row: &crate::analytics::SessionDetailRow| {
        row.conversation_kind
            .as_deref()
            .is_none_or(|kind| kind == "main")
    };
    if let Some(session_id) = &session_id {
        rows.retain(|row| &row.session_id == session_id);
        if rows.is_empty() {
            return Err(anyhow!("session '{session_id}' not found"));
        }
    } else {
        // Implicit resume never selects a background worker: discard
        // non-interactive rows even when nothing interactive matches, so
        // selection falls through to the global fallback or reports none.
        rows.retain(&interactive);
        if rows.is_empty() && cwd_filter.is_some() && !strict_cwd {
            // The public CLI keeps its historical global fallback unless the Herdr plugin
            // explicitly requires the focused directory to match.
            rows = store.query_sessions_detailed(source, None, None, None, Some(50))?;
            rows.retain(&interactive);
        }
    }

    let Some((row, command, cwd)) = rows
        .iter()
        .find_map(|row| session_resume_command(&config, row).map(|(cmd, cwd)| (row, cmd, cwd)))
    else {
        return Err(anyhow!("no resumable session found"));
    };

    if crate::herdr::inside_herdr() {
        let placement = crate::herdr::resume_placement(&config);
        if placement == crate::herdr::ResumePlacement::Off {
            return Err(anyhow!("herdr resume is disabled (herdr_resume = \"off\")"));
        }
        let label = row
            .repo_project
            .clone()
            .unwrap_or_else(|| row.project.clone());
        let pane_id =
            crate::herdr::open_resume_pane(placement, Some(cwd.as_str()), &label, &command)?;
        println!(
            "resumed {} ({}) in herdr pane {pane_id}",
            row.session_id,
            row.source.label()
        );
        return Ok(());
    }

    // Outside herdr, run the resume command directly.
    let status = std::process::Command::new("sh")
        .args(["-lc", &command])
        .status()?;
    if !status.success() {
        return Err(anyhow!("resume command exited with {status}"));
    }
    println!("resumed {} ({})", row.session_id, row.source.label());
    Ok(())
}

fn run_analytics_backfill(root: Option<PathBuf>) -> Result<()> {
    let paths = Paths::new(root)?;
    let _lease = IngestLease::acquire(&paths, "analytics backfill", INGEST_LEASE_TIMEOUT)?;
    paths.ensure_dirs()?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    let db = analytics_path(&paths.state);
    backfill_from_index(&db, &index)?;
    let store = AnalyticsStore::open(&db)?;
    println!("analytics: {}", db.display());
    println!("documents: {}", index.doc_count()?);
    println!("sessions: {}", store.session_count()?);
    Ok(())
}

fn print_vector_stats(vectors_dir: &std::path::Path) -> Result<()> {
    println!("{}", vector_stats_line(vectors_dir)?);
    Ok(())
}

fn vector_stats_line(vectors_dir: &std::path::Path) -> Result<String> {
    let Some(inventory) = VectorIndex::inventory(vectors_dir)? else {
        return Ok("vectors: none".to_string());
    };
    let model = inventory.model.as_deref().unwrap_or("unknown");
    Ok(format!(
        "vectors: {} (dims {}, model {}, ids {}, usearch.index {}, doc_ids.bin {})",
        inventory.vector_count,
        inventory.dimensions,
        model,
        inventory.doc_ids.len(),
        inventory.index_bytes,
        inventory.ids_bytes
    ))
}

const MEMEX_SEARCH_SKILL: &str = include_str!("../skills/memex-search/SKILL.md");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkillWriteMode {
    Install,
    Update,
    Replace,
}

fn run_skill_command(command: SkillCommand) -> Result<()> {
    match command {
        SkillCommand::Status { target } => run_skill_status(target),
        SkillCommand::Install { target } => run_skill_install(target, None),
        SkillCommand::Update { target } => run_skill_write(target, SkillWriteMode::Update),
        SkillCommand::Cleanup { dry_run } => run_skill_cleanup(dry_run),
    }
}

fn home_dir() -> Result<PathBuf> {
    Ok(directories::BaseDirs::new()
        .ok_or_else(|| anyhow!("cannot determine home directory"))?
        .home_dir()
        .to_path_buf())
}

fn skill_destinations(home: &Path, target: SkillTarget) -> Vec<(&'static str, PathBuf)> {
    let shared = ("shared", home.join(".agents/skills/memex-search/SKILL.md"));
    let claude = ("claude", home.join(".claude/skills/memex-search/SKILL.md"));
    match target {
        SkillTarget::Shared => vec![shared],
        SkillTarget::Claude => vec![claude],
        SkillTarget::All => vec![shared, claude],
    }
}

fn run_skill_status(target: SkillTarget) -> Result<()> {
    let home = home_dir()?;
    for (label, path) in skill_destinations(&home, target) {
        let state = match std::fs::read(&path) {
            Ok(contents) if contents == MEMEX_SEARCH_SKILL.as_bytes() => "current",
            Ok(_) => "outdated or locally modified",
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => "not installed",
            Err(err) => return Err(err).with_context(|| format!("read {} skill", path.display())),
        };
        println!("{label}: {state} ({})", path.display());
    }
    Ok(())
}

fn skill_warnings(home: &Path) -> Vec<String> {
    let mut warnings = Vec::new();
    let mut differing = Vec::new();
    for (label, path) in skill_destinations(home, SkillTarget::All) {
        match std::fs::read(&path) {
            Ok(contents) if contents != MEMEX_SEARCH_SKILL.as_bytes() => differing.push(label),
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => warnings.push(format!(
                "cannot check {label} memex-search skill ({}): {err}",
                path.display()
            )),
        }
    }
    if !differing.is_empty() {
        warnings.push(format!(
            "memex-search skill is outdated or locally modified ({}). Run `memex skill status` to inspect, or `memex skill update` to replace installed copies with this version. Use `memex update --yes` to update both memex and its skills.",
            differing.join(", ")
        ));
    }
    warnings
}

fn warn_if_skill_outdated() {
    if let Ok(home) = home_dir() {
        for warning in skill_warnings(&home) {
            eprintln!("warning: {warning}");
        }
    }
}

fn run_skill_install(
    target: Option<SkillTarget>,
    mode_override: Option<SkillWriteMode>,
) -> Result<()> {
    let targets = match target {
        Some(target) => vec![target],
        None => select_skill_targets()?,
    };
    if targets.is_empty() {
        println!("Nothing selected.");
        return Ok(());
    }
    let home = home_dir()?;
    let mode = mode_override.unwrap_or(SkillWriteMode::Install);
    write_skill_targets(&home, &targets, mode)
}

fn run_skill_write(target: SkillTarget, mode: SkillWriteMode) -> Result<()> {
    let home = home_dir()?;
    write_skill_targets(&home, &[target], mode)
}

fn select_skill_targets() -> Result<Vec<SkillTarget>> {
    use dialoguer::{MultiSelect, theme::ColorfulTheme};

    let claude_path = find_in_path("claude");
    let codex_path = find_in_path("codex");
    let opencode_path = find_in_path("opencode");
    let pi_path = find_in_path("pi");
    let omp_path = find_in_path("omp");

    if claude_path.is_none()
        && codex_path.is_none()
        && opencode_path.is_none()
        && pi_path.is_none()
        && omp_path.is_none()
    {
        return Err(anyhow!(
            "Neither claude, codex, opencode, pi, nor omp found in PATH"
        ));
    }

    let shared_agents: Vec<&str> = [
        ("Codex", codex_path.as_ref()),
        ("Opencode", opencode_path.as_ref()),
        ("Pi", pi_path.as_ref()),
        ("Oh My Pi", omp_path.as_ref()),
    ]
    .into_iter()
    .filter_map(|(name, path)| path.map(|_| name))
    .collect::<Vec<_>>();

    let mut items: Vec<(SkillTarget, String)> = Vec::new();
    let mut defaults = Vec::new();

    if let Some(path) = &claude_path {
        items.push((
            SkillTarget::Claude,
            format!("Claude Code ({})", path.display()),
        ));
        defaults.push(true);
    }
    if !shared_agents.is_empty() {
        items.push((
            SkillTarget::Shared,
            format!("Shared agents ({})", shared_agents.join(", ")),
        ));
        defaults.push(true);
    }

    let labels: Vec<&str> = items.iter().map(|(_, label)| label.as_str()).collect();

    let selected = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Select tools to configure")
        .items(&labels)
        .defaults(&defaults)
        .interact()?;

    Ok(selected.into_iter().map(|index| items[index].0).collect())
}

fn write_skill_targets(home: &Path, targets: &[SkillTarget], mode: SkillWriteMode) -> Result<()> {
    let mut destinations = Vec::new();
    for target in targets {
        for destination in skill_destinations(home, *target) {
            if !destinations
                .iter()
                .any(|(_, path): &(&str, PathBuf)| path == &destination.1)
            {
                destinations.push(destination);
            }
        }
    }

    if mode == SkillWriteMode::Install {
        let conflicts = destinations
            .iter()
            .filter_map(|(_, path)| match std::fs::read(path) {
                Ok(contents) if contents != MEMEX_SEARCH_SKILL.as_bytes() => {
                    Some(path.display().to_string())
                }
                Ok(_) => None,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
                Err(err) => Some(format!("{} ({err})", path.display())),
            })
            .collect::<Vec<_>>();
        if !conflicts.is_empty() {
            return Err(anyhow!(
                "refusing to overwrite existing skill file(s): {}. Use `memex skill update` to replace installed copies",
                conflicts.join(", ")
            ));
        }
    }

    let mut changed = false;
    for (label, path) in destinations {
        let existing = match std::fs::read(&path) {
            Ok(contents) => Some(contents),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(err).with_context(|| format!("read {} skill", path.display())),
        };
        if existing.as_deref() == Some(MEMEX_SEARCH_SKILL.as_bytes()) {
            println!("{label}: already current ({})", path.display());
            continue;
        }
        if existing.is_none() && mode == SkillWriteMode::Update {
            println!("{label}: not installed; skipped ({})", path.display());
            continue;
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create skill directory {}", parent.display()))?;
        }
        std::fs::write(&path, MEMEX_SEARCH_SKILL)
            .with_context(|| format!("write {} skill", path.display()))?;
        changed = true;
        let verb = if existing.is_some() {
            "updated"
        } else {
            "installed"
        };
        println!("{label}: {verb} ({})", path.display());
    }
    if changed {
        println!("Restart your agent to pick up skill changes.");
    }
    Ok(())
}

fn legacy_skill_paths(home: &Path) -> Vec<PathBuf> {
    vec![
        // Gen 1: automem-era paths
        home.join(".claude/skills/automem-search"),
        home.join(".codex/prompts/automem-search.md"),
        home.join(".local/share/opencode/prompts/automem-search.md"),
        // Gen 2: flat-file skill paths (now directory-based)
        home.join(".codex/skills/memex-search.md"),
        home.join(".local/share/opencode/skills/memex-search.md"),
        pi_agent_root().join("skills/memex-search.md"),
        omp_agent_root().join("skills/memex-search.md"),
        // Gen 3: agent-specific copies superseded by the shared agentskills.io root
        home.join(".codex/skills/memex-search"),
        home.join(".local/share/opencode/skills/memex-search"),
        pi_agent_root().join("skills/memex-search"),
        omp_agent_root().join("skills/memex-search"),
    ]
}

fn run_skill_cleanup(dry_run: bool) -> Result<()> {
    let home = home_dir()?;
    cleanup_legacy_skill_paths(&legacy_skill_paths(&home), dry_run)
}

fn cleanup_legacy_skill_paths(paths: &[PathBuf], dry_run: bool) -> Result<()> {
    let mut found = false;
    for path in paths {
        if path.is_dir() {
            found = true;
            if dry_run {
                println!("would remove {}", path.display());
            } else {
                std::fs::remove_dir_all(path)
                    .with_context(|| format!("remove legacy skill directory {}", path.display()))?;
                println!("removed {}", path.display());
            }
        } else if path.is_file() {
            found = true;
            if dry_run {
                println!("would remove {}", path.display());
            } else {
                std::fs::remove_file(path)
                    .with_context(|| format!("remove legacy skill file {}", path.display()))?;
                println!("removed {}", path.display());
            }
        }
    }
    if !found {
        println!("No legacy Memex skill paths found.");
    } else if dry_run {
        println!("Dry run only; nothing was removed.");
    }
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
        crate::types::SourceKind::Codex => "codex",
        crate::types::SourceKind::Opencode => "opencode",
        crate::types::SourceKind::Cursor => "cursor",
        crate::types::SourceKind::Pi => "pi",
        crate::types::SourceKind::OpenClaw => "openclaw",
        crate::types::SourceKind::Copilot => "copilot",
        crate::types::SourceKind::Grok => "grok",
        crate::types::SourceKind::Omp => "omp",
        crate::types::SourceKind::Hermes => "hermes",
        crate::types::SourceKind::Jcode => "jcode",
        crate::types::SourceKind::Muse => "muse",
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

fn run_transfer(
    session_id: String,
    source: Option<SourceFilter>,
    to: TransferTarget,
    mode: TransferMode,
    turns: Option<usize>,
    dry_run: bool,
    root: Option<PathBuf>,
) -> Result<()> {
    let paths = Paths::new(root)?;
    let index = SearchIndex::open_or_create(&paths.index)?;
    let result = transfer_session(
        &index,
        TransferOptions {
            session_id,
            source,
            target: to.into(),
            mode: mode.into(),
            turns,
            dry_run,
        },
    )?;

    println!("generated: {}", result.generated_path.display());
    println!("source: {}", result.source.label());
    println!("session: {}", result.session_id);
    println!("messages: {}", result.message_count);
    println!("source_path: {}", result.source_path);
    if let Some(thread_id) = result.thread_id {
        println!("codex_thread: {thread_id}");
    }
    if let Some(resume) = result.resume_command {
        println!("resume: {resume}");
    }
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

fn pi_agent_root() -> PathBuf {
    if let Some(root) = std::env::var_os("PI_CODING_AGENT_DIR") {
        return PathBuf::from(root);
    }
    let home = directories::BaseDirs::new()
        .map(|b| b.home_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("/"));
    home.join(".pi").join("agent")
}

fn omp_agent_root() -> PathBuf {
    crate::sources::omp::agent_root()
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
    mcp_args: DaemonMcpArgs,
    label: Option<String>,
    continuous: bool,
    poll_interval: Option<u64>,
    interval: Option<u64>,
    web_ui: bool,
    web_listen: Option<String>,
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

    let config_updates = IndexServiceConfigUpdates {
        label: label.clone(),
        stdout: config_path_setting(stdout.as_deref(), "--stdout")?,
        stderr: config_path_setting(stderr.as_deref(), "--stderr")?,
        plist: config_path_setting(plist.as_deref(), "--plist")?,
        systemd_dir: config_path_setting(systemd_dir.as_deref(), "--systemd-dir")?,
        ..IndexServiceConfigUpdates::from_cli(
            continuous,
            poll_interval,
            interval,
            web_ui,
            web_listen.as_deref(),
            &mcp_args,
        )?
    };

    let paths = Paths::new(index.root.clone())?;
    let config = UserConfig::load(&paths)?;
    let mcp = mcp_args.resolve(&config);
    let mcp_listen = mcp.as_ref().map(|options| options.listen);
    let cli_web_ui = web_ui || web_listen.is_some();
    let web_ui = cli_web_ui || config.index_service_web_ui_default();
    let web_listen = web_listen
        .or_else(|| config.index_service_web_listen.clone())
        .unwrap_or_else(|| crate::web::DEFAULT_LISTEN.to_string());
    let cli_continuous = continuous || poll_interval.is_some() || cli_web_ui;
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
    let continuous = if cli_continuous || web_ui || mcp.is_some() {
        true
    } else if interval.is_some() {
        false
    } else {
        config_continuous
    };
    let poll_interval = poll_interval.unwrap_or(config.index_service_poll_interval());
    let interval = interval.unwrap_or(config.index_service_interval());
    if web_ui {
        crate::web::validate_listener(&web_listen)?;
    }

    let exe = std::env::current_exe()?;
    let program_args = build_index_command_args(
        index,
        continuous,
        poll_interval,
        web_ui,
        &web_listen,
        mcp_listen,
    );

    std::fs::create_dir_all(&paths.root)?;

    let result = if cfg!(target_os = "macos") {
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
    };

    result?;
    persist_index_service_config(&paths, &config_updates)?;
    disable_auto_index_on_search_by_default(&paths, &config)?;
    if web_ui {
        wait_for_web_ui(&web_listen, Duration::from_secs(5))?;
        println!("web UI: running on {web_listen}");
        println!("open: memex web open --listen {web_listen}");
    }
    if let Some(listen) = mcp_listen {
        let address = mcp_health_address(listen);
        wait_for_http_health(&address, "memex-mcp", Duration::from_secs(5))?;
        println!("MCP: http://{address}/mcp");
        if let Some(public_url) = config.mcp.public_url.as_deref() {
            println!("MCP OAuth: {public_url}/mcp");
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
struct IndexServiceConfigUpdates {
    mode: Option<&'static str>,
    poll_interval: Option<i64>,
    interval: Option<i64>,
    web_ui: Option<bool>,
    web_listen: Option<String>,
    mcp: Option<bool>,
    mcp_listen: Option<std::net::SocketAddr>,
    label: Option<String>,
    stdout: Option<String>,
    stderr: Option<String>,
    plist: Option<String>,
    systemd_dir: Option<String>,
}

fn config_path_setting(path: Option<&Path>, option: &str) -> Result<Option<String>> {
    path.map(|path| {
        path.to_str()
            .map(str::to_owned)
            .ok_or_else(|| anyhow!("{option} must be valid UTF-8 to store in config.toml"))
    })
    .transpose()
}

impl IndexServiceConfigUpdates {
    fn from_cli(
        continuous: bool,
        poll_interval: Option<u64>,
        interval: Option<u64>,
        web_ui: bool,
        web_listen: Option<&str>,
        mcp_args: &DaemonMcpArgs,
    ) -> Result<Self> {
        let mode = if continuous
            || poll_interval.is_some()
            || web_ui
            || web_listen.is_some()
            || mcp_args.mcp
            || mcp_args.mcp_listen.is_some()
        {
            Some("continuous")
        } else if interval.is_some() {
            Some("interval")
        } else {
            None
        };
        Ok(Self {
            mode,
            poll_interval: poll_interval
                .map(i64::try_from)
                .transpose()
                .context("--poll-interval is too large to store in config.toml")?,
            interval: interval
                .map(i64::try_from)
                .transpose()
                .context("--interval is too large to store in config.toml")?,
            web_ui: (web_ui || web_listen.is_some()).then_some(true),
            web_listen: web_listen.map(str::to_owned),
            mcp: if mcp_args.no_mcp {
                Some(false)
            } else if mcp_args.mcp || mcp_args.mcp_listen.is_some() {
                Some(true)
            } else {
                None
            },
            mcp_listen: mcp_args.mcp_listen,
            ..Default::default()
        })
    }
}

fn persist_index_service_config(paths: &Paths, updates: &IndexServiceConfigUpdates) -> Result<()> {
    if updates.mode.is_none()
        && updates.poll_interval.is_none()
        && updates.interval.is_none()
        && updates.web_ui.is_none()
        && updates.web_listen.is_none()
        && updates.mcp.is_none()
        && updates.mcp_listen.is_none()
        && updates.label.is_none()
        && updates.stdout.is_none()
        && updates.stderr.is_none()
        && updates.plist.is_none()
        && updates.systemd_dir.is_none()
    {
        return Ok(());
    }

    std::fs::create_dir_all(&paths.root)?;
    let path = paths.root.join("config.toml");
    let contents = if path.exists() {
        std::fs::read_to_string(&path)?
    } else {
        String::new()
    };
    let mut document = contents
        .parse::<DocumentMut>()
        .with_context(|| format!("parse {} before updating daemon settings", path.display()))?;

    if let Some(mode) = updates.mode {
        replace_toml_value(&mut document["index_service_mode"], value(mode));
    }
    if let Some(interval) = updates.poll_interval {
        // Both names deserialize into one field; keep the existing spelling
        // rather than introducing a duplicate field into a legacy config.
        let key = if document.contains_key("index_service_watch_interval") {
            "index_service_watch_interval"
        } else {
            "index_service_poll_interval"
        };
        replace_toml_value(&mut document[key], value(interval));
    }
    if let Some(interval) = updates.interval {
        replace_toml_value(&mut document["index_service_interval"], value(interval));
    }
    if let Some(enabled) = updates.web_ui {
        replace_toml_value(&mut document["index_service_web_ui"], value(enabled));
    }
    if let Some(listen) = &updates.web_listen {
        replace_toml_value(
            &mut document["index_service_web_listen"],
            value(listen.as_str()),
        );
    }
    if let Some(enabled) = updates.mcp {
        replace_toml_value(&mut document["index_service_mcp"], value(enabled));
    }
    if let Some(listen) = updates.mcp_listen {
        replace_toml_value(&mut document["mcp"]["listen"], value(listen.to_string()));
    }
    for (key, setting) in [
        ("index_service_label", updates.label.as_deref()),
        ("index_service_stdout", updates.stdout.as_deref()),
        ("index_service_stderr", updates.stderr.as_deref()),
        ("index_service_plist", updates.plist.as_deref()),
        ("index_service_systemd_dir", updates.systemd_dir.as_deref()),
    ] {
        if let Some(setting) = setting {
            replace_toml_value(&mut document[key], value(setting));
        }
    }

    std::fs::write(&path, document.to_string())?;
    println!("updated daemon settings: {}", path.display());
    Ok(())
}

fn replace_toml_value(item: &mut TomlItem, replacement: TomlItem) {
    let decor = item.as_value().map(|value| value.decor().clone());
    *item = replacement;
    if let Some(decor) = decor
        && let Some(value) = item.as_value_mut()
    {
        *value.decor_mut() = decor;
    }
}

fn disable_auto_index_on_search_by_default(paths: &Paths, config: &UserConfig) -> Result<()> {
    if config.auto_index_on_search.is_some() {
        return Ok(());
    }

    std::fs::create_dir_all(&paths.root)?;
    let path = paths.root.join("config.toml");
    let mut contents = if path.exists() {
        std::fs::read_to_string(&path)?
    } else {
        String::new()
    };

    contents = format!(
        "# Background indexing handles freshness; avoid duplicate scan work during search.\nauto_index_on_search = false\n\n{contents}"
    );

    std::fs::write(&path, contents)?;
    println!(
        "updated config: {} (auto_index_on_search = false)",
        path.display()
    );
    Ok(())
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
    let env_vars = service_environment_variables(Some(paths))?;

    let contents = build_launchd_plist(
        &label,
        &full_args,
        interval,
        keep_alive,
        Some(&stdout),
        Some(&stderr),
        &env_vars,
    );
    std::fs::write(&plist_path, contents)?;

    println!("wrote launchd plist: {}", plist_path.display());
    let (domain_target, service_target) = launchctl_targets(&label)?;

    // Replace any existing job with the same label to avoid stale launchd state.
    let _ = launchctl_bootout_service(&service_target)?;

    let bootstrap = std::process::Command::new("launchctl")
        .arg("bootstrap")
        .arg(&domain_target)
        .arg(&plist_path)
        .output()?;
    if !bootstrap.status.success() {
        return Err(anyhow!(
            "launchctl bootstrap failed: {}",
            format_command_output(&bootstrap)
        ));
    }

    let enable = std::process::Command::new("launchctl")
        .arg("enable")
        .arg(&service_target)
        .output()?;
    if !enable.status.success() {
        return Err(anyhow!(
            "launchctl enable failed: {}",
            format_command_output(&enable)
        ));
    }

    let kickstart = std::process::Command::new("launchctl")
        .arg("kickstart")
        .arg("-k")
        .arg(&service_target)
        .output()?;
    if !kickstart.status.success() {
        return Err(anyhow!(
            "launchctl kickstart failed: {}",
            format_command_output(&kickstart)
        ));
    }

    verify_launchd_job_loaded(&service_target, &plist_path)?;
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
    let existing_mode = registered_systemd_mode(&service_path, &timer_path)?;
    if let Some(counterpart) =
        systemd_counterpart_unit(&label, continuous, existing_mode, timer_path.exists())
    {
        run_systemctl(
            &["--user", "disable", "--now", &counterpart],
            "systemctl disable counterpart",
        )?;
    }

    let env_vars = service_environment_variables(None)?;
    let service_contents =
        build_systemd_service(&exe.to_string_lossy(), program_args, continuous, &env_vars);
    std::fs::write(&service_path, service_contents)?;
    println!("wrote systemd service: {}", service_path.display());

    // For interval mode, create a timer unit
    if !continuous {
        let timer_contents = build_systemd_timer(interval);
        std::fs::write(&timer_path, timer_contents)?;
        println!("wrote systemd timer: {}", timer_path.display());
    } else if timer_path.exists() {
        std::fs::remove_file(&timer_path)?;
        println!("removed obsolete systemd timer: {}", timer_path.display());
    }

    // Reload systemd user daemon
    run_systemctl(&["--user", "daemon-reload"], "systemctl daemon-reload")?;

    // Enable and restart the appropriate unit. Restarting is necessary when an
    // existing service was regenerated with different arguments from config.
    let unit = if continuous {
        format!("{}.service", label)
    } else {
        format!("{}.timer", label)
    };
    run_systemctl(&["--user", "enable", &unit], "systemctl enable service")?;
    run_systemctl(&["--user", "restart", &unit], "systemctl restart service")?;
    if continuous {
        println!("enabled systemd service: {}", label);
    } else {
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

fn run_index_service_status(
    label: Option<String>,
    plist: Option<PathBuf>,
    systemd_dir: Option<PathBuf>,
    root: Option<PathBuf>,
) -> Result<()> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;

    if cfg!(target_os = "macos") {
        run_index_service_status_launchd(&config, &paths, label, plist)
    } else if cfg!(target_os = "linux") {
        run_index_service_status_systemd(&config, label, systemd_dir)
    } else {
        Err(anyhow!(
            "background service scheduling is only supported on macOS and Linux"
        ))
    }
}

fn run_index_service_open(
    listen: Option<String>,
    root: Option<PathBuf>,
    print_url: bool,
) -> Result<()> {
    let paths = Paths::new(root.clone())?;
    let config = UserConfig::load(&paths)?;
    let listen = listen
        .or(config.index_service_web_listen)
        .unwrap_or_else(|| crate::web::DEFAULT_LISTEN.to_string());
    let url = crate::web::bootstrap_url(root, &listen)?;
    if print_url {
        println!("{url}");
        return Ok(());
    }
    let status = if cfg!(target_os = "macos") {
        std::process::Command::new("open").arg(&url).status()?
    } else if cfg!(target_os = "linux") {
        std::process::Command::new("xdg-open").arg(&url).status()?
    } else {
        return Err(anyhow!(
            "opening a browser is only supported on macOS and Linux"
        ));
    };
    if !status.success() {
        return Err(anyhow!("failed to open the authenticated Web UI"));
    }
    println!("opened authenticated web UI");
    Ok(())
}

fn run_index_service_status_launchd(
    config: &UserConfig,
    paths: &Paths,
    label: Option<String>,
    plist: Option<PathBuf>,
) -> Result<()> {
    let label = label
        .or_else(|| config.index_service_label.clone())
        .unwrap_or_else(default_index_service_label);
    let plist_path = plist
        .or_else(|| config.index_service_plist.clone())
        .unwrap_or_else(|| default_index_service_plist(&paths.root));
    validate_service_label(&label)?;
    let (_domain_target, service_target) = launchctl_targets(&label)?;
    let output = std::process::Command::new("launchctl")
        .arg("print")
        .arg(&service_target)
        .output()?;

    if !output.status.success() {
        if launchctl_not_found(&output) {
            println!("Memex daemon: stopped");
            println!("label: {label}");
            println!("definition: {}", plist_path.display());
            return Ok(());
        }
        return Err(anyhow!(
            "launchctl print failed: {}",
            format_command_output(&output)
        ));
    }

    let state = String::from_utf8_lossy(&output.stdout);
    let service_state = service_output_value(&state, "state").unwrap_or("loaded");
    println!("Memex daemon: {service_state}");
    println!("label: {label}");
    println!(
        "mode: {}",
        if service_output_has_arg(&state, "--watch") {
            "continuous"
        } else {
            "interval"
        }
    );
    print_service_web_ui_status(&state);
    print_service_mcp_status(&state);
    println!("definition: {}", plist_path.display());
    Ok(())
}

fn run_index_service_status_systemd(
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
    if !service_path.exists() && !timer_path.exists() {
        println!("Memex daemon: stopped");
        println!("label: {label}");
        println!("definition: {}", service_path.display());
        return Ok(());
    }

    let mode = registered_systemd_mode(&service_path, &timer_path)?
        .ok_or_else(|| anyhow!("unable to determine registered systemd service mode"))?;
    let unit = match mode {
        SystemdServiceMode::Continuous => format!("{}.service", label),
        SystemdServiceMode::Interval => format!("{}.timer", label),
    };
    let state = systemd_unit_state(&unit)?;
    println!("Memex daemon: {state}");
    println!("label: {label}");
    println!(
        "mode: {}",
        match mode {
            SystemdServiceMode::Continuous => "continuous",
            SystemdServiceMode::Interval => "interval",
        }
    );
    let definition = if service_path.exists() {
        std::fs::read_to_string(&service_path)
            .with_context(|| format!("failed to read {}", service_path.display()))?
    } else {
        String::new()
    };
    print_service_web_ui_status(&definition);
    print_service_mcp_status(&definition);
    println!("definition: {}", service_path.display());
    if mode == SystemdServiceMode::Interval {
        println!("timer: {}", timer_path.display());
    }
    Ok(())
}

fn systemd_unit_state(unit: &str) -> Result<String> {
    let output = std::process::Command::new("systemctl")
        .args(["--user", "is-active", unit])
        .output()?;
    parse_systemd_unit_state(unit, &output)
}

fn parse_systemd_unit_state(unit: &str, output: &std::process::Output) -> Result<String> {
    let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr);
    if output.status.success() || (stderr.trim().is_empty() && is_known_systemd_unit_state(&state))
    {
        return Ok(if state.is_empty() {
            "inactive".to_string()
        } else {
            state
        });
    }
    Err(anyhow!(
        "systemctl is-active {unit} failed: {}",
        format_command_output(output)
    ))
}

fn is_known_systemd_unit_state(state: &str) -> bool {
    matches!(
        state,
        "active"
            | "reloading"
            | "inactive"
            | "failed"
            | "activating"
            | "deactivating"
            | "maintenance"
            | "refreshing"
            | "unknown"
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SystemdServiceMode {
    Continuous,
    Interval,
}

fn registered_systemd_mode(
    service_path: &std::path::Path,
    timer_path: &std::path::Path,
) -> Result<Option<SystemdServiceMode>> {
    if service_path.exists() {
        let service = std::fs::read_to_string(service_path)
            .with_context(|| format!("failed to read {}", service_path.display()))?;
        if service.lines().any(|line| line.trim() == "Type=oneshot") {
            return Ok(Some(SystemdServiceMode::Interval));
        }
        if service.lines().any(|line| line.trim() == "Type=simple") {
            return Ok(Some(SystemdServiceMode::Continuous));
        }
    }
    if timer_path.exists() {
        return Ok(Some(SystemdServiceMode::Interval));
    }
    Ok(None)
}

fn systemd_counterpart_unit(
    label: &str,
    continuous: bool,
    existing_mode: Option<SystemdServiceMode>,
    timer_exists: bool,
) -> Option<String> {
    if continuous && timer_exists {
        Some(format!("{label}.timer"))
    } else if !continuous && existing_mode == Some(SystemdServiceMode::Continuous) {
        Some(format!("{label}.service"))
    } else {
        None
    }
}

fn run_systemctl(args: &[&str], operation: &str) -> Result<()> {
    let output = std::process::Command::new("systemctl")
        .args(args)
        .output()?;
    if !output.status.success() {
        return Err(anyhow!(
            "{operation} failed: {}",
            format_command_output(&output)
        ));
    }
    Ok(())
}

fn service_output_value<'a>(output: &'a str, key: &str) -> Option<&'a str> {
    output.lines().find_map(|line| {
        let (candidate, value) = line.trim().split_once(" = ")?;
        (candidate == key).then_some(value.trim())
    })
}

fn service_output_has_arg(output: &str, arg: &str) -> bool {
    output.lines().any(|line| line.trim() == arg)
}

fn service_output_arg_value<'a>(output: &'a str, arg: &str) -> Option<&'a str> {
    let mut lines = output.lines().map(str::trim);
    while let Some(line) = lines.next() {
        if line == arg {
            return lines.next().filter(|value| !value.is_empty());
        }
        if let Some((_, tail)) = line.split_once(arg) {
            return tail.split_whitespace().next();
        }
    }
    None
}

fn print_service_web_ui_status(output: &str) {
    if service_output_has_arg(output, "--web-ui") || output.contains(" --web-ui") {
        let listen =
            service_output_arg_value(output, "--web-listen").unwrap_or(crate::web::DEFAULT_LISTEN);
        if web_ui_is_healthy(listen) {
            println!("web UI: running on {listen}");
            println!("open: memex web open --listen {listen}");
        } else {
            println!("web UI: unavailable (configured at http://{listen})");
        }
    } else {
        println!("web UI: disabled");
    }
}

fn web_ui_addresses(listen: &str) -> Result<Vec<std::net::SocketAddr>> {
    let addresses = listen
        .to_socket_addrs()
        .with_context(|| format!("resolve Web UI listener {listen}"))?
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err(anyhow!("Web UI listener {listen} resolved to no addresses"));
    }
    Ok(addresses)
}

fn web_ui_is_healthy(listen: &str) -> bool {
    http_is_healthy(listen, "ok")
}

fn http_is_healthy(listen: &str, expected: &str) -> bool {
    web_ui_addresses(listen).is_ok_and(|addresses| {
        addresses.iter().any(|address| {
            let Ok(mut stream) = TcpStream::connect_timeout(address, Duration::from_millis(100))
            else {
                return false;
            };
            stream
                .set_read_timeout(Some(Duration::from_millis(500)))
                .ok();
            stream
                .set_write_timeout(Some(Duration::from_millis(500)))
                .ok();
            if write!(
                stream,
                "GET /healthz HTTP/1.1\r\nHost: {listen}\r\nConnection: close\r\n\r\n"
            )
            .is_err()
            {
                return false;
            }

            let mut response = Vec::new();
            let mut chunk = [0_u8; 1024];
            while response.len() < 4096 {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(read) => {
                        response.extend_from_slice(&chunk[..read]);
                        if is_http_health_response(&response, expected) {
                            return true;
                        }
                    }
                    Err(_) => return false,
                }
            }
            is_http_health_response(&response, expected)
        })
    })
}

fn is_http_health_response(response: &[u8], expected: &str) -> bool {
    let Ok(response) = std::str::from_utf8(response) else {
        return false;
    };
    let Some((headers, body)) = response.split_once("\r\n\r\n") else {
        return false;
    };
    headers
        .lines()
        .next()
        .is_some_and(|status| status.ends_with(" 200 OK"))
        && body == expected
}

fn mcp_health_address(mut listen: std::net::SocketAddr) -> String {
    if listen.ip().is_unspecified() {
        listen.set_ip(if listen.is_ipv4() {
            std::net::Ipv4Addr::LOCALHOST.into()
        } else {
            std::net::Ipv6Addr::LOCALHOST.into()
        });
    }
    listen.to_string()
}

fn wait_for_http_health(listen: &str, expected: &str, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while !http_is_healthy(listen, expected) {
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "{expected} did not become ready at {listen} within {} seconds",
                timeout.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

fn print_service_mcp_status(output: &str) {
    if service_output_has_arg(output, "--mcp") || output.contains(" --mcp ") {
        let listen = service_output_arg_value(output, "--mcp-listen").unwrap_or("127.0.0.1:5363");
        let address = listen
            .parse()
            .map(mcp_health_address)
            .unwrap_or_else(|_| listen.to_string());
        if http_is_healthy(&address, "memex-mcp") {
            println!("MCP: http://{address}/mcp");
        } else {
            println!("MCP: unavailable (configured at {address})");
        }
    } else {
        println!("MCP: disabled");
    }
}

fn wait_for_web_ui(listen: &str, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    while !web_ui_is_healthy(listen) {
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "Web UI did not start listening at http://{listen} within {} seconds",
                timeout.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
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
    let (_domain_target, service_target) = launchctl_targets(&label)?;
    let _ = launchctl_bootout_service(&service_target)?;

    if plist_path.exists() {
        std::fs::remove_file(&plist_path)?;
    } else {
        println!("no launchd plist found: {}", plist_path.display());
    }

    println!("disabled launchd job: {label}");
    Ok(())
}

fn current_uid() -> Result<u32> {
    if let Ok(uid) = std::env::var("UID")
        && let Ok(parsed) = uid.trim().parse::<u32>()
    {
        return Ok(parsed);
    }
    let output = std::process::Command::new("id").arg("-u").output()?;
    if !output.status.success() {
        return Err(anyhow!(
            "failed to determine uid: {}",
            format_command_output(&output)
        ));
    }
    let uid = String::from_utf8_lossy(&output.stdout).trim().to_string();
    uid.parse::<u32>()
        .map_err(|_| anyhow!("invalid uid from id -u: {uid}"))
}

fn launchctl_targets(label: &str) -> Result<(String, String)> {
    let uid = current_uid()?;
    let domain = format!("gui/{uid}");
    let service = format!("{domain}/{label}");
    Ok((domain, service))
}

fn launchctl_bootout_service(service_target: &str) -> Result<bool> {
    let output = std::process::Command::new("launchctl")
        .arg("bootout")
        .arg(service_target)
        .output()?;
    if output.status.success() {
        return Ok(true);
    }
    if launchctl_not_found(&output) {
        return Ok(false);
    }
    if !launchctl_service_exists(service_target)? {
        return Ok(false);
    }
    Err(anyhow!(
        "launchctl bootout failed: {}",
        format_command_output(&output)
    ))
}

fn launchctl_service_exists(service_target: &str) -> Result<bool> {
    let output = std::process::Command::new("launchctl")
        .arg("print")
        .arg(service_target)
        .output()?;
    if output.status.success() {
        return Ok(true);
    }
    if launchctl_not_found(&output) {
        return Ok(false);
    }
    Err(anyhow!(
        "launchctl print failed: {}",
        format_command_output(&output)
    ))
}

fn verify_launchd_job_loaded(service_target: &str, plist_path: &std::path::Path) -> Result<()> {
    let output = std::process::Command::new("launchctl")
        .arg("print")
        .arg(service_target)
        .output()?;
    if !output.status.success() {
        return Err(anyhow!(
            "launchctl print failed: {}",
            format_command_output(&output)
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let expected_path = plist_path.to_string_lossy();
    if !stdout.contains(&format!("path = {expected_path}")) {
        return Err(anyhow!(
            "launchd job state mismatch; expected path {}, launchctl output did not match",
            plist_path.display()
        ));
    }
    Ok(())
}

fn launchctl_not_found(output: &std::process::Output) -> bool {
    let message = format_command_output(output).to_lowercase();
    message.contains("could not find service")
        || message.contains("no such process")
        || message.contains("not found")
        || message.contains("service is disabled")
}

fn format_command_output(output: &std::process::Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => format!("status {}", output.status),
        (false, true) => stdout,
        (true, false) => stderr,
        (false, false) => format!("{stderr}; {stdout}"),
    }
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
    web_ui: bool,
    web_listen: &str,
    mcp_listen: Option<std::net::SocketAddr>,
) -> Vec<String> {
    let mut args = Vec::new();
    args.push("index".to_string());

    if let Some(source) = &index.source {
        args.push("--claude-path".to_string());
        args.push(source.to_string_lossy().to_string());
    }
    for (flag, sources) in [
        ("--only-source", &index.only_source),
        ("--exclude-source", &index.exclude_source),
    ] {
        for source in sources {
            args.push(flag.to_string());
            args.push(
                source
                    .to_possible_value()
                    .expect("index source")
                    .get_name()
                    .to_string(),
            );
        }
    }
    if index.include_agents {
        args.push("--include-agents".to_string());
    }
    if index.include_reasoning {
        args.push("--include-reasoning".to_string());
    }
    for pattern in &index.exclude {
        args.push("--exclude".to_string());
        args.push(pattern.clone());
    }
    if !index.codex || index.no_codex {
        args.push("--no-codex".to_string());
    }
    if !index.opencode || index.no_opencode {
        args.push("--no-opencode".to_string());
    }
    if !index.cursor {
        args.push("--no-cursor".to_string());
    }
    if !index.pi || index.no_pi {
        args.push("--no-pi".to_string());
    }
    if !index.omp || index.no_omp {
        args.push("--no-omp".to_string());
    }
    if !index.openclaw || index.no_openclaw {
        args.push("--no-openclaw".to_string());
    }
    if !index.copilot || index.no_copilot {
        args.push("--no-copilot".to_string());
    }
    if !index.grok || index.no_grok {
        args.push("--no-grok".to_string());
    }
    if !index.jcode || index.no_jcode {
        args.push("--no-jcode".to_string());
    }
    if !index.muse || index.no_muse {
        args.push("--no-muse".to_string());
    }
    if let Some(listen) = mcp_listen {
        args.push("--mcp".to_string());
        args.push("--mcp-listen".to_string());
        args.push(listen.to_string());
    } else if continuous {
        args.push("--no-mcp".to_string());
    }
    if index.embeddings {
        args.push("--embeddings".to_string());
    }
    if index.no_embeddings {
        args.push("--no-embeddings".to_string());
    }
    if index.diagnostics {
        args.push("--diagnostics".to_string());
    }
    if continuous {
        args.push("--watch".to_string());
        args.push("--watch-interval".to_string());
        args.push(format!("{poll_interval}"));
    }
    if web_ui {
        args.push("--web-ui".to_string());
        args.push("--web-listen".to_string());
        args.push(web_listen.to_string());
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
    env_vars: &[(String, String)],
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
    if !env_vars.is_empty() {
        out.push_str("  <key>EnvironmentVariables</key>\n");
        out.push_str("  <dict>\n");
        for (key, value) in env_vars {
            out.push_str(&format!("    <key>{}</key>\n", xml_escape(key)));
            out.push_str(&format!("    <string>{}</string>\n", xml_escape(value)));
        }
        out.push_str("  </dict>\n");
    }

    out.push_str("</dict>\n");
    out.push_str("</plist>\n");
    out
}

fn service_environment_variables(paths: Option<&Paths>) -> Result<Vec<(String, String)>> {
    let mut vars = Vec::new();
    if let Some(base) = directories::BaseDirs::new() {
        vars.push((
            "HOME".to_string(),
            base.home_dir().to_string_lossy().to_string(),
        ));
    }
    let path = std::env::var("PATH")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "/usr/bin:/bin:/usr/sbin:/sbin".to_string());
    vars.push(("PATH".to_string(), path));

    if let Some(paths) = paths {
        let embed_cache = paths.root.join("embed-cache");
        std::fs::create_dir_all(&embed_cache)?;
        let embed_cache = embed_cache.to_string_lossy().to_string();
        vars.push(("FASTEMBED_CACHE_DIR".to_string(), embed_cache.clone()));
        vars.push(("HF_HOME".to_string(), embed_cache));
    }

    for key in ["PI_CODING_AGENT_DIR", "PI_CODING_AGENT_SESSION_DIR"] {
        if let Some(value) = std::env::var_os(key)
            && !value.is_empty()
        {
            vars.push((key.to_string(), value.to_string_lossy().to_string()));
        }
    }

    Ok(vars)
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

fn build_systemd_service(
    exe_path: &str,
    program_args: &[String],
    continuous: bool,
    env_vars: &[(String, String)],
) -> String {
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
    for (key, value) in env_vars {
        out.push_str(&format!(
            "Environment=\"{}={}\"\n",
            systemd_escape_env_value(key),
            systemd_escape_env_value(value)
        ));
    }
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

fn systemd_escape_env_value(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%")
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
    if let Ok(date) = chrono::NaiveDate::parse_from_str(&value, "%Y-%m-%d") {
        let midnight = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow!("invalid date: {value}"))?
            .and_utc();
        return Ok(Some(midnight.timestamp_millis() as u64));
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

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SortBy {
    Score,
    Ts,
}

const DEFAULT_SEARCH_FIELDS: &str = "kind,machine,score,ts,doc_id,record_id,project,role,session_id,source,source_path,snippet,matches,memory_id,content_version,section_ref,title,heading,cwd,mtime_ms,event_dates,start_line,end_line,document_kind,refs,freshness,changed_since_search";

fn search_fields(fields: Option<String>, full: bool) -> Result<Option<HashSet<String>>> {
    if full {
        return Ok(None);
    }
    Ok(parse_fields(fields)?.or_else(|| {
        Some(
            DEFAULT_SEARCH_FIELDS
                .split(',')
                .map(str::to_string)
                .collect(),
        )
    }))
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

/// Stored session kinds for the candidate groups, keyed by
/// (machine, source label, session id) with prefer-main dominance. Remote
/// machines and sessions missing from the analytics cache simply have no
/// entry, and grouping falls back to the matched records. Empty unless an
/// origin filter is active.
fn stored_session_kinds(
    paths: &Paths,
    results: &[LocatedRecord],
    origin_filtered: bool,
) -> HashMap<(String, String, String), Option<String>> {
    let mut out = HashMap::new();
    if !origin_filtered {
        return out;
    }
    let Ok(store) = open_analytics_read_only(paths) else {
        return out;
    };
    for result in results {
        let key = (
            result.machine.clone(),
            result.record.source.storage_label().to_string(),
            result.record.session_id.clone(),
        );
        let stored = store.session_conversation_kind(
            result.record.source.storage_label(),
            &result.record.session_id,
            &result.record.source_path,
        );
        let dominated = stored.as_deref() == Some("main");
        out.entry(key)
            .and_modify(|kind: &mut Option<String>| {
                if dominated {
                    *kind = Some("main".to_string());
                }
            })
            .or_insert(stored);
    }
    out
}

fn apply_post_processing_located(
    mut results: Vec<LocatedRecord>,
    render: &RenderOptions,
    stored_kinds: &HashMap<(String, String, String), Option<String>>,
) -> Vec<LocatedRecord> {
    if let Some(min_score) = render.min_score {
        results.retain(|result| result.score >= min_score);
    }

    // Session-grouped origin filter with prefer-main: a sidechain hit inside
    // a primary session must not hide the session (mirrors the TUI and the
    // analytics accumulator). Groups resolve against the stored session kind
    // first — complete-session truth — and only fall back to the matched
    // records when the analytics cache has no row (remote machines, stale
    // caches).
    if render.kind_filter != crate::analytics::SessionKindFilter::All {
        let mut group_kind: HashMap<(String, String, String), Option<String>> = HashMap::new();
        for result in &results {
            let key = (
                result.machine.clone(),
                result.record.source.storage_label().to_string(),
                result.record.session_id.clone(),
            );
            let dominated = result.record.links.conversation_kind.as_deref() == Some("main");
            group_kind
                .entry(key)
                .and_modify(|kind| {
                    if dominated {
                        *kind = Some("main".to_string());
                    }
                })
                .or_insert_with(|| result.record.links.conversation_kind.clone());
        }
        results.retain(|result| {
            let key = (
                result.machine.clone(),
                result.record.source.storage_label().to_string(),
                result.record.session_id.clone(),
            );
            let kind = stored_kinds
                .get(&key)
                .and_then(|stored| stored.as_deref())
                .or_else(|| group_kind.get(&key).and_then(|kind| kind.as_deref()));
            render.kind_filter.matches_kind(kind)
        });
    }

    match render.sort {
        SortBy::Score => {
            results.sort_by(|left, right| {
                right
                    .score
                    .partial_cmp(&left.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        SortBy::Ts => {
            results.sort_by_key(|result| std::cmp::Reverse(result.record.ts));
        }
    }

    if let Some(k) = render.top_n_per_session {
        let mut per_session: HashMap<(String, String, String), usize> = HashMap::new();
        results.retain(|result| {
            let count = per_session
                .entry((
                    result.machine.clone(),
                    result.record.source.storage_label().to_string(),
                    result.record.session_id.clone(),
                ))
                .or_default();
            if *count >= k {
                return false;
            }
            *count += 1;
            true
        });
    }

    results.truncate(render.limit);
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

pub(crate) fn build_matchers(query: &str) -> Result<Vec<regex::Regex>> {
    use tantivy::query_grammar::{Occur, UserInputAst, UserInputLeaf};
    fn literals(ast: &UserInputAst, terms: &mut Vec<String>) {
        match ast {
            UserInputAst::Clause(children) => {
                for (occur, child) in children {
                    if *occur != Some(Occur::MustNot) {
                        literals(child, terms);
                    }
                }
            }
            UserInputAst::Boost(child, _) => literals(child, terms),
            UserInputAst::Leaf(leaf) => {
                if let UserInputLeaf::Literal(literal) = leaf.as_ref()
                    && literal
                        .field_name
                        .as_deref()
                        .is_none_or(|field| field == "text")
                {
                    terms.extend(literal.phrase.split_whitespace().map(str::to_string));
                }
            }
        }
    }
    let mut terms = Vec::new();
    match tantivy::query_grammar::parse_query(query) {
        Ok(ast) => literals(&ast, &mut terms),
        // Semantic requests need not be valid lexical syntax.
        Err(_) => terms.extend(query.split_whitespace().map(str::to_string)),
    }
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for term in terms {
        let term = term
            .trim_matches(|c: char| !c.is_alphanumeric())
            .to_lowercase();
        if term.is_empty() || !seen.insert(term.clone()) {
            continue;
        }
        out.push(
            RegexBuilder::new(&regex::escape(&term))
                .case_insensitive(true)
                .build()?,
        );
    }
    Ok(out)
}

// Preview the earliest literal hit; semantic-only hits fall back to a compact prefix.
pub(crate) fn match_preview(text: &str, matchers: &[regex::Regex], max_chars: usize) -> String {
    let first = matchers
        .iter()
        .filter_map(|matcher| matcher.find(text))
        .min_by_key(|m| m.start());
    let Some(hit) = first else {
        return summarize(text, max_chars);
    };
    let prefix = take_last_chars(&text[..hit.start()], 80);
    let start = hit.start() - prefix.len();
    let preview = summarize(&text[start..], max_chars.saturating_sub(1));
    if start > 0 {
        format!("…{preview}")
    } else {
        preview
    }
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

const HOMEBREW_FORMULA: &str = "nicosuave/tap/memex";

fn interaction_allowed(
    explicitly_disabled: bool,
    stdin_tty: bool,
    stdout_tty: bool,
    stderr_tty: bool,
    agent_or_ci: bool,
) -> bool {
    !explicitly_disabled && stdin_tty && stdout_tty && stderr_tty && !agent_or_ci
}

fn agent_or_ci_environment() -> bool {
    [
        "CI",
        "CODEX_CI",
        "CODEX_THREAD_ID",
        "CLAUDECODE",
        "CLAUDE_CODE_ENTRYPOINT",
    ]
    .iter()
    .any(|name| {
        std::env::var_os(name).is_some_and(|value| {
            let value = value.to_string_lossy();
            !value.is_empty() && value != "0" && !value.eq_ignore_ascii_case("false")
        })
    })
}

fn homebrew_executable(path: &Path) -> bool {
    let components: Vec<_> = path.components().collect();
    components
        .windows(2)
        .any(|pair| pair[0].as_os_str() == "Cellar" && pair[1].as_os_str() == "memex")
}

fn is_homebrew_install() -> bool {
    std::env::current_exe()
        .ok()
        .and_then(|path| path.canonicalize().ok())
        .is_some_and(|path| homebrew_executable(&path))
}

fn confirm_update() -> Result<bool> {
    use dialoguer::{Confirm, theme::ColorfulTheme};
    if is_homebrew_install() {
        eprintln!("brew update && brew upgrade {HOMEBREW_FORMULA}");
    }
    eprintln!(
        "Existing memex-search skill copies will also be replaced with the installed version; missing copies stay uninstalled."
    );
    Ok(Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt("Update memex and its installed skills now?")
        .default(true)
        .interact()?)
}

fn refresh_installed_skills(binary: &Path) -> Result<()> {
    let status = std::process::Command::new(binary)
        .args(["skill", "update", "--target", "all"])
        .stdin(std::process::Stdio::null())
        .status()
        .with_context(|| format!("run installed skill updater {}", binary.display()))?;
    if !status.success() {
        return Err(anyhow!(
            "memex is installed, but refreshing its skills failed ({status}); run `memex skill update` to retry"
        ));
    }
    Ok(())
}

fn update_homebrew(brew: &Path, expected_version: Option<&str>) -> Result<PathBuf> {
    for args in [vec!["update"], vec!["upgrade", HOMEBREW_FORMULA]] {
        let status = std::process::Command::new(brew)
            .args(&args)
            .env("HOMEBREW_NO_AUTO_UPDATE", "1")
            .stdin(std::process::Stdio::null())
            .status()
            .with_context(|| format!("run brew {}", args.join(" ")))?;
        if !status.success() {
            return Err(anyhow!(
                "brew {} failed ({status}); update stopped",
                args.join(" ")
            ));
        }
    }
    // The running executable can live in an old Cellar version that brew just removed.
    let output = std::process::Command::new(brew)
        .args(["--prefix", HOMEBREW_FORMULA])
        .stdin(std::process::Stdio::null())
        .output()
        .context("locate the installed Homebrew memex")?;
    if !output.status.success() {
        return Err(anyhow!(
            "Homebrew upgrade finished, but `brew --prefix {HOMEBREW_FORMULA}` failed; run `memex skill update` after resolving the installation"
        ));
    }
    let prefix = std::str::from_utf8(&output.stdout)
        .context("Homebrew returned a non-UTF-8 memex prefix")?
        .trim();
    if prefix.is_empty() || prefix.lines().count() != 1 || !Path::new(prefix).is_absolute() {
        return Err(anyhow!(
            "Homebrew returned an invalid memex installation prefix"
        ));
    }
    let binary = Path::new(prefix).join("bin/memex");
    let version = std::process::Command::new(&binary)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .output()
        .with_context(|| format!("check installed memex {}", binary.display()))?;
    if !version.status.success() {
        return Err(anyhow!(
            "Homebrew upgrade finished, but the installed memex version check failed"
        ));
    }
    let version = std::str::from_utf8(&version.stdout)?.trim();
    let installed_version = version
        .strip_prefix("memex ")
        .filter(|value| parse_version_parts(value).is_some())
        .ok_or_else(|| anyhow!("unexpected installed memex version: {version}"))?;
    println!("Installed {version}");
    refresh_installed_skills(&binary)?;
    if expected_version.is_some_and(|latest| is_newer_version(installed_version, latest)) {
        return Err(anyhow!(
            "Homebrew still provides memex v{installed_version}; release v{} is newer. The tap may not have caught up or the formula may be pinned. Installed skills were refreshed; retry `memex update` later",
            expected_version.unwrap()
        ));
    }
    Ok(binary)
}

fn replace_binary(new_binary: &Path, current_exe: &Path) -> Result<()> {
    let parent = current_exe
        .parent()
        .ok_or_else(|| anyhow!("binary has no parent directory"))?;
    // Stage in the destination filesystem so a failed copy leaves the running binary intact.
    let mut staged = tempfile::NamedTempFile::new_in(parent)?;
    let mut source = std::fs::File::open(new_binary)?;
    std::io::copy(&mut source, &mut staged)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        staged
            .as_file()
            .set_permissions(std::fs::Permissions::from_mode(0o755))?;
    }
    staged.as_file().sync_all()?;
    staged.persist(current_exe).map_err(|error| error.error)?;
    Ok(())
}

fn update_release(current_exe: &Path, latest: &str) -> Result<()> {
    let current = env!("CARGO_PKG_VERSION");
    if !is_newer_version(current, latest) {
        println!("memex is already up to date (v{current}); refreshing installed skills");
        return refresh_installed_skills(current_exe);
    }
    let (os, arch) = detect_platform()?;
    let url = format!(
        "https://github.com/{REPO}/releases/download/v{latest}/memex-{latest}-{os}-{arch}.tar.gz"
    );
    println!("Downloading memex v{latest}...");
    let tmp_dir = tempfile::tempdir()?;
    let archive_path = tmp_dir.path().join("memex.tar.gz");
    let status = std::process::Command::new("curl")
        .args([
            "-fsSL",
            "--connect-timeout",
            "10",
            "--max-time",
            "300",
            "-o",
        ])
        .arg(&archive_path)
        .arg(&url)
        .stdin(std::process::Stdio::null())
        .status()?;
    if !status.success() {
        return Err(anyhow!("Failed to download release"));
    }
    let status = std::process::Command::new("tar")
        .args(["-xzf"])
        .arg(&archive_path)
        .arg("-C")
        .arg(tmp_dir.path())
        .stdin(std::process::Stdio::null())
        .status()?;
    if !status.success() {
        return Err(anyhow!("Failed to extract release"));
    }
    replace_binary(&tmp_dir.path().join("memex"), current_exe)?;
    println!("Updated memex to v{latest}");
    refresh_installed_skills(current_exe)
}

fn perform_update(known_latest: Option<&str>) -> Result<()> {
    let current_exe = std::env::current_exe()?.canonicalize()?;
    if homebrew_executable(&current_exe) {
        let brew = find_in_path("brew").ok_or_else(|| {
            anyhow!("Homebrew manages this installation, but brew is not on PATH")
        })?;
        update_homebrew(&brew, known_latest)?;
    } else {
        let fetched;
        let latest = match known_latest {
            Some(latest) => latest,
            None => {
                fetched = fetch_latest_version()?;
                &fetched
            }
        };
        update_release(&current_exe, latest)?;
    }
    Ok(())
}

fn fetch_latest_version() -> Result<String> {
    let output = std::process::Command::new("curl")
        .args([
            "-fsSL",
            "--connect-timeout",
            "1",
            "--max-time",
            "2",
            &format!("https://api.github.com/repos/{REPO}/releases/latest"),
        ])
        .stdin(std::process::Stdio::null())
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

#[derive(Serialize, Deserialize)]
struct UpdateCheck {
    checked_at: u64,
    latest: Option<String>,
}

fn latest_version_cached(
    cache_path: &Path,
    now: u64,
    fetch: impl FnOnce() -> Result<String>,
) -> Option<String> {
    if let Ok(contents) = std::fs::read(cache_path)
        && let Ok(cached) = serde_json::from_slice::<UpdateCheck>(&contents)
    {
        // Retry a failed check sooner, without delaying every command while offline.
        let ttl = if cached.latest.is_some() {
            6 * 60 * 60
        } else {
            5 * 60
        };
        if cached.checked_at <= now && now - cached.checked_at < ttl {
            return cached.latest;
        }
    }
    let latest = fetch().ok();
    let check = UpdateCheck {
        checked_at: now,
        latest: latest.clone(),
    };
    // Cache writes are best effort and atomic; concurrent launches never read a partial file.
    let _ = (|| -> Result<()> {
        let parent = cache_path
            .parent()
            .ok_or_else(|| anyhow!("cache has no parent"))?;
        std::fs::create_dir_all(parent)?;
        let mut file = tempfile::NamedTempFile::new_in(parent)?;
        serde_json::to_writer(file.as_file_mut(), &check)?;
        file.persist(cache_path).map_err(|error| error.error)?;
        Ok(())
    })();
    latest
}

fn available_update() -> Option<String> {
    let home = home_dir().ok()?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    latest_version_cached(
        &home.join(".memex/update-check.json"),
        now,
        fetch_latest_version,
    )
    .filter(|latest| is_newer_version(env!("CARGO_PKG_VERSION"), latest))
}

fn print_update_notice(latest: Option<&str>) {
    if let Some(latest) = latest {
        eprintln!(
            "update: memex v{latest} is available (current v{}). Run `memex update` or `memex update --yes` for a noninteractive update of memex and installed skills.",
            env!("CARGO_PKG_VERSION")
        );
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{EnvVarGuard, env_lock};
    use crate::vector::VectorIndex;
    use tempfile::TempDir;

    #[test]
    fn session_count_query_requires_count_and_rejects_cwd() {
        assert!(Cli::try_parse_from(["memex", "sessions", "--query", "needle"]).is_err());
        assert!(
            Cli::try_parse_from([
                "memex", "sessions", "--count", "--query", "needle", "--cwd", "."
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "memex",
                "sessions",
                "--count",
                "--query=--needle",
                "--format",
                "json"
            ])
            .is_ok()
        );
    }

    #[test]
    fn mcp_search_request_uses_bounded_compact_defaults() {
        let request: SearchRequest = serde_json::from_value(serde_json::json!({
            "query": "needle"
        }))
        .unwrap();

        assert_eq!(request.limit, 20);
        assert_eq!(request.content, SearchContent::Conversations);
        assert!(request.unique_session);
        assert_eq!(request.mode, McpSearchMode::Lexical);
        assert_eq!(request.sort, McpSearchSort::Score);
        assert_eq!(request.recency_weight, 1.0);
        assert_eq!(request.recency_half_life_days, 30.0);
        assert!(request.additional_queries.is_empty());
        assert!(request.machines.is_empty());
        assert_eq!(request.origin, SessionOrigin::Regular);
    }

    #[test]
    fn mcp_request_limits_and_source_names_are_validated() {
        assert!(validate_mcp_limit(1).is_ok());
        assert!(validate_mcp_limit(500).is_ok());
        assert!(validate_mcp_limit(0).is_err());
        assert!(validate_mcp_limit(501).is_err());
        assert_eq!(
            parse_source_filter(Some("open-claw".to_string())).unwrap(),
            Some(SourceFilter::OpenClaw)
        );
        assert!(parse_source_filter(Some("unknown".to_string())).is_err());

        let mut request: SearchRequest =
            serde_json::from_value(serde_json::json!({"query": "needle"})).unwrap();
        request.additional_queries = (0..8).map(|index| format!("query {index}")).collect();
        assert!(validate_mcp_search_request(&request).is_err());
        request.additional_queries.clear();
        request.top_n_per_session = Some(501);
        assert!(validate_mcp_search_request(&request).is_err());
        request.top_n_per_session = None;
        request.recency_half_life_days = 0.0;
        assert!(validate_mcp_search_request(&request).is_err());

        assert!(
            serde_json::from_value::<SessionsRequest>(serde_json::json!({
                "limit": 20,
                "unexpected": true
            }))
            .is_err()
        );
    }

    #[test]
    fn mcp_command_accepts_a_custom_root() {
        let cli = Cli::try_parse_from(["memex", "mcp", "--root", "/tmp/custom-memex"])
            .expect("parse MCP root");
        let Some(Commands::Mcp {
            root,
            transport,
            listen,
            ..
        }) = cli.command
        else {
            panic!("expected MCP command");
        };
        assert_eq!(root, Some(PathBuf::from("/tmp/custom-memex")));
        assert_eq!(transport, McpTransport::Http);
        assert_eq!(listen, None);
    }

    #[test]
    fn update_interaction_requires_a_human_terminal_and_explicit_opt_out_wins() {
        assert!(interaction_allowed(false, true, true, true, false));
        for (disabled, stdin, stdout, stderr, agent) in [
            (true, true, true, true, false),
            (false, false, true, true, false),
            (false, true, false, true, false),
            (false, true, true, false, false),
            (false, true, true, true, true),
        ] {
            assert!(!interaction_allowed(disabled, stdin, stdout, stderr, agent));
        }
        for args in [
            vec!["memex", "--non-interactive", "update", "--yes"],
            vec!["memex", "update", "--yes", "--non-interactive"],
        ] {
            let cli = Cli::try_parse_from(args).unwrap();
            assert!(cli.non_interactive);
            assert!(matches!(cli.command, Some(Commands::Update { yes: true })));
        }
        let cli = Cli::try_parse_from(["memex", "update", "--non-interactive"]).unwrap();
        assert!(matches!(cli.command, Some(Commands::Update { yes: false })));
    }

    #[test]
    fn update_install_detection_requires_the_memex_cellar_entry() {
        for path in [
            "/opt/homebrew/Cellar/memex/0.14.0/bin/memex",
            "/usr/local/Cellar/memex/0.14.0/bin/memex",
            "/home/linuxbrew/.linuxbrew/Cellar/memex/0.14.0/bin/memex",
        ] {
            assert!(homebrew_executable(Path::new(path)));
        }
        for path in [
            "/home/me/homebrew/project/target/debug/memex",
            "/opt/homebrew/Cellar/other/0.14.0/bin/memex",
            "/home/me/.local/bin/memex",
        ] {
            assert!(!homebrew_executable(Path::new(path)));
        }
    }

    #[test]
    fn update_cache_reuses_notices_and_retries_expired_or_failed_checks() {
        let temp = TempDir::new().unwrap();
        let cache = temp.path().join("cache.json");
        assert_eq!(
            latest_version_cached(&cache, 100, || Ok("99.0.0".into())),
            Some("99.0.0".into())
        );
        assert_eq!(
            latest_version_cached(&cache, 200, || panic!("fresh cache must avoid network")),
            Some("99.0.0".into())
        );
        assert_eq!(
            latest_version_cached(&cache, 100 + 6 * 60 * 60, || Ok("100.0.0".into())),
            Some("100.0.0".into())
        );
        std::fs::write(&cache, "partial invalid cache").unwrap();
        assert_eq!(
            latest_version_cached(&cache, 30_000, || Err(anyhow!("offline"))),
            None
        );
        assert_eq!(
            latest_version_cached(&cache, 30_001, || panic!(
                "failure backoff must avoid network"
            )),
            None
        );
        assert_eq!(
            latest_version_cached(&cache, 30_300, || Ok("101.0.0".into())),
            Some("101.0.0".into())
        );
        // A clock correction must not make a future-dated cache permanent.
        assert_eq!(
            latest_version_cached(&cache, 20_000, || Ok("102.0.0".into())),
            Some("102.0.0".into())
        );
        assert_eq!(
            latest_version_cached(&temp.path().join("missing/other.json"), 1, || Ok(
                "99.0.0".into()
            )),
            Some("99.0.0".into())
        );
    }

    #[test]
    fn skill_warnings_are_quiet_for_missing_or_matching_copies_and_clear_after_update() {
        let home = TempDir::new().unwrap();
        assert!(skill_warnings(home.path()).is_empty());
        write_skill_targets(home.path(), &[SkillTarget::Shared], SkillWriteMode::Install).unwrap();
        assert!(skill_warnings(home.path()).is_empty());
        let shared = home.path().join(".agents/skills/memex-search/SKILL.md");
        std::fs::write(&shared, "older or edited skill").unwrap();
        let warning = skill_warnings(home.path());
        assert_eq!(warning.len(), 1);
        assert!(warning[0].contains("outdated or locally modified (shared)"));
        assert_eq!(
            std::fs::read_to_string(&shared).unwrap(),
            "older or edited skill"
        );
        write_skill_targets(home.path(), &[SkillTarget::Claude], SkillWriteMode::Install).unwrap();
        let claude = home.path().join(".claude/skills/memex-search/SKILL.md");
        std::fs::write(&claude, "edited Claude copy").unwrap();
        assert!(skill_warnings(home.path())[0].contains("shared, claude"));
        write_skill_targets(home.path(), &[SkillTarget::All], SkillWriteMode::Update).unwrap();
        assert!(skill_warnings(home.path()).is_empty());
    }

    #[test]
    fn skill_warnings_do_not_fail_on_unreadable_paths() {
        let home = TempDir::new().unwrap();
        std::fs::create_dir_all(home.path().join(".agents/skills/memex-search/SKILL.md")).unwrap();
        let warnings = skill_warnings(home.path());
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("cannot check shared"));
        assert!(!warnings[0].contains("outdated"));
    }

    #[test]
    fn update_binary_replacement_leaves_old_binary_intact_on_copy_failure() {
        let temp = TempDir::new().unwrap();
        let installed = temp.path().join("memex");
        let release = temp.path().join("release-memex");
        std::fs::write(&installed, "old binary").unwrap();
        assert!(replace_binary(&release, &installed).is_err());
        assert_eq!(std::fs::read_to_string(&installed).unwrap(), "old binary");
        std::fs::write(&release, "new binary").unwrap();
        replace_binary(&release, &installed).unwrap();
        assert_eq!(std::fs::read_to_string(&installed).unwrap(), "new binary");
        assert!(!temp.path().join("memex.old").exists());
    }

    #[test]
    fn previews_use_positive_text_literals_and_bound_unicode() {
        let text = format!("padding {} needlé evidence", "界".repeat(1000));
        let matchers = build_matchers("text:needlé AND NOT text:padding").unwrap();
        let preview = match_preview(&text, &matchers, 400);
        assert!(preview.contains("needlé"));
        assert!(!preview.contains("padding"));
        assert!(preview.chars().count() <= 400);
        assert_eq!(
            match_preview("semantic fallback", &[], 400),
            "semantic fallback"
        );
        assert!(build_matchers("project:memex").unwrap().is_empty());
    }

    #[test]
    fn empty_search_projection_keeps_the_compact_default() {
        for fields in [None, Some("".to_string()), Some(" , ".to_string())] {
            let fields = search_fields(fields, false).unwrap().unwrap();
            assert!(fields.contains("record_id"));
            assert!(!fields.contains("text"));
        }
        assert!(search_fields(None, true).unwrap().is_none());
    }

    #[test]
    fn sessions_cli_accepts_exact_identity_selectors() {
        let cli = Cli::try_parse_from([
            "memex",
            "sessions",
            "--source",
            "codex",
            "--session-id",
            "shared",
            "--source-path",
            "/old session.jsonl",
            "--origin",
            "all",
            "--limit",
            "1",
        ])
        .unwrap();
        let Commands::Sessions {
            session_id,
            source_path,
            source,
            origin,
            limit,
            ..
        } = cli.command.unwrap()
        else {
            panic!("expected sessions command");
        };
        assert_eq!(session_id.as_deref(), Some("shared"));
        assert_eq!(source_path.as_deref(), Some("/old session.jsonl"));
        assert_eq!(source, Some(SourceFilter::Codex));
        assert_eq!(origin, SessionOrigin::All);
        assert_eq!(limit, 1);
        let defaults: SessionsRequest = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(defaults.session_id.is_none());
        assert!(defaults.source_path.is_none());
        let serialized = serde_json::to_value(defaults).unwrap();
        assert!(serialized.get("session_id").is_none());
        assert!(serialized.get("source_path").is_none());
    }

    #[test]
    fn discovery_defaults_exclude_reviews_with_explicit_all_opt_in() {
        for command in ["search", "sessions", "usage"] {
            for explicit_all in [false, true] {
                let mut args = vec!["memex", command];
                if command == "search" {
                    args.push("needle");
                }
                if explicit_all {
                    args.extend(["--origin", "all"]);
                }
                let cli = Cli::try_parse_from(args).unwrap();
                let origin = match cli.command.unwrap() {
                    Commands::Search { origin, .. }
                    | Commands::Sessions { origin, .. }
                    | Commands::Usage { origin, .. } => origin,
                    _ => unreachable!(),
                };
                assert_eq!(
                    origin,
                    if explicit_all {
                        SessionOrigin::All
                    } else {
                        SessionOrigin::Regular
                    }
                );
            }
        }
        let request: SessionsRequest = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(request.origin, SessionOrigin::Regular);
    }

    #[test]
    fn sessions_interactive_only_conflicts_with_explicit_origin() {
        assert!(
            Cli::try_parse_from([
                "memex",
                "sessions",
                "--origin",
                "subagent",
                "--interactive-only"
            ])
            .is_err()
        );
        assert!(Cli::try_parse_from(["memex", "sessions", "--interactive-only"]).is_ok());
        assert!(Cli::try_parse_from(["memex", "sessions", "--origin", "subagent"]).is_ok());
        assert!(Cli::try_parse_from(["memex", "sessions"]).is_ok());
    }

    #[test]
    fn build_index_command_args_preserves_disabled_sources() {
        let index = IndexArgs {
            only_source: Vec::new(),
            exclude_source: Vec::new(),
            source: None,
            include_agents: false,
            include_reasoning: false,
            exclude: Vec::new(),
            codex: false,
            opencode: false,
            cursor: false,
            pi: false,
            omp: false,
            openclaw: false,
            copilot: false,
            grok: false,
            jcode: false,
            muse: false,
            no_codex: false,
            no_opencode: false,
            no_pi: false,
            no_omp: false,
            no_openclaw: false,
            no_copilot: false,
            no_grok: false,
            no_jcode: false,
            no_muse: false,
            embeddings: false,
            no_embeddings: false,
            model: None,
            root: None,
            diagnostics: false,
        };

        let args =
            build_index_command_args(&index, false, 30, false, crate::web::DEFAULT_LISTEN, None);

        assert!(args.contains(&"--no-codex".to_string()));
        assert!(args.contains(&"--no-opencode".to_string()));
        assert!(args.contains(&"--no-cursor".to_string()));
        assert!(args.contains(&"--no-pi".to_string()));
        assert!(args.contains(&"--no-openclaw".to_string()));
        assert!(args.contains(&"--no-omp".to_string()));
        assert!(args.contains(&"--no-copilot".to_string()));
        assert!(args.contains(&"--no-grok".to_string()));
        assert!(args.contains(&"--no-jcode".to_string()));
        assert!(args.contains(&"--no-muse".to_string()));
    }

    #[test]
    fn build_index_command_args_forwards_exclude_patterns() {
        let index = IndexArgs {
            only_source: Vec::new(),
            exclude_source: Vec::new(),
            source: None,
            include_agents: false,
            include_reasoning: false,
            exclude: vec!["~/work/**".to_string(), "/tmp/secret/*.jsonl".to_string()],
            codex: true,
            opencode: true,
            cursor: true,
            pi: true,
            omp: true,
            openclaw: true,
            copilot: true,
            grok: true,
            jcode: true,
            muse: true,
            no_codex: false,
            no_opencode: false,
            no_pi: false,
            no_omp: false,
            no_openclaw: false,
            no_copilot: false,
            no_grok: false,
            no_jcode: false,
            no_muse: false,
            embeddings: false,
            no_embeddings: false,
            model: None,
            root: None,
            diagnostics: false,
        };

        let args = build_index_command_args(&index, false, 30, false, "127.0.0.1:7777", None);

        let mut pairs = args.windows(2);
        assert!(pairs.any(|w| w == ["--exclude", "~/work/**"]));
        let mut pairs = args.windows(2);
        assert!(pairs.any(|w| w == ["--exclude", "/tmp/secret/*.jsonl"]));
    }

    #[test]
    fn build_index_command_args_includes_web_ui_options() {
        let index = IndexArgs {
            only_source: Vec::new(),
            exclude_source: Vec::new(),
            source: None,
            include_agents: false,
            include_reasoning: false,
            exclude: Vec::new(),
            codex: true,
            opencode: true,
            cursor: true,
            pi: true,
            omp: true,
            openclaw: true,
            copilot: true,
            grok: true,
            jcode: true,
            muse: true,
            no_codex: false,
            no_opencode: false,
            no_pi: false,
            no_omp: false,
            no_openclaw: false,
            no_copilot: false,
            no_grok: false,
            no_jcode: false,
            no_muse: false,
            embeddings: false,
            no_embeddings: false,
            model: None,
            root: None,
            diagnostics: false,
        };

        let args = build_index_command_args(&index, true, 30, true, "127.0.0.1:6363", None);

        assert!(
            args.windows(2)
                .any(|pair| pair == ["--web-listen", "127.0.0.1:6363"])
        );
        assert!(args.contains(&"--web-ui".to_string()));
        assert!(args.contains(&"--watch".to_string()));
    }

    #[test]
    fn index_loop_starts_web_before_initial_indexing() {
        let events = std::cell::RefCell::new(Vec::new());

        initialize_index_loop(
            || {
                events.borrow_mut().push("index");
                Ok(())
            },
            || {
                events.borrow_mut().push("web");
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(*events.borrow(), ["web", "index"]);
    }

    #[test]
    fn web_ui_readiness_requires_memex_health_response() {
        let server = serve_test_responses(
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
        );

        wait_for_web_ui(&server.listen, Duration::from_secs(1)).unwrap();
        server.shutdown();
    }

    #[test]
    fn web_ui_readiness_rejects_unrelated_tcp_listener() {
        let server = serve_test_responses(
            "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnope",
        );

        assert!(!web_ui_is_healthy(&server.listen));
        server.shutdown();
    }

    struct TestResponseServer {
        listen: String,
        shutdown: std::sync::mpsc::Sender<()>,
        server: std::thread::JoinHandle<()>,
    }

    impl TestResponseServer {
        fn shutdown(self) {
            self.shutdown.send(()).unwrap();
            TcpStream::connect(&self.listen).unwrap();
            self.server.join().unwrap();
        }
    }

    fn serve_test_responses(response: &'static str) -> TestResponseServer {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let listen = listener.local_addr().unwrap().to_string();
        let (shutdown, shutdown_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            loop {
                let (mut stream, _) = listener.accept().unwrap();
                if shutdown_rx.try_recv().is_ok() {
                    break;
                }
                let mut request = [0_u8; 1024];
                if stream.read(&mut request).is_ok() {
                    let _ = stream.write_all(response.as_bytes());
                }
            }
        });
        TestResponseServer {
            listen,
            shutdown,
            server,
        }
    }

    #[test]
    fn service_web_listen_flag_is_accepted() {
        let cli = Cli::try_parse_from([
            "memex",
            "index-service",
            "enable",
            "--web-listen",
            "127.0.0.1:6363",
        ])
        .unwrap();

        let Some(Commands::IndexService {
            action:
                IndexServiceCommand::Enable {
                    web_ui, web_listen, ..
                },
        }) = cli.command
        else {
            panic!("expected index service enable command");
        };
        assert!(!web_ui);
        assert_eq!(web_listen.as_deref(), Some("127.0.0.1:6363"));
    }

    #[test]
    fn index_service_restart_accepts_service_options() {
        let cli = Cli::try_parse_from([
            "memex",
            "index-service",
            "restart",
            "--web-listen",
            "127.0.0.1:8080",
        ])
        .unwrap();

        let Some(Commands::IndexService {
            action:
                IndexServiceCommand::Restart {
                    web_ui, web_listen, ..
                },
        }) = cli.command
        else {
            panic!("expected index service restart command");
        };
        assert!(!web_ui);
        assert_eq!(web_listen.as_deref(), Some("127.0.0.1:8080"));
    }

    #[test]
    fn index_service_status_is_accepted() {
        let cli = Cli::try_parse_from(["memex", "index-service", "status"]).unwrap();

        let Some(Commands::IndexService {
            action: IndexServiceCommand::Status { .. },
        }) = cli.command
        else {
            panic!("expected index service status command");
        };
    }

    #[test]
    fn index_service_open_accepts_local_listener() {
        let cli = Cli::try_parse_from([
            "memex",
            "index-service",
            "open",
            "--listen",
            "127.0.0.1:8080",
        ])
        .unwrap();

        let Some(Commands::IndexService {
            action: IndexServiceCommand::Open { listen, .. },
        }) = cli.command
        else {
            panic!("expected index service open command");
        };
        assert_eq!(listen.as_deref(), Some("127.0.0.1:8080"));
    }

    #[test]
    fn service_status_reads_registered_web_ui_arguments() {
        let output = "\
state = running
arguments = {
    /opt/homebrew/bin/memex
    index
    --watch
    --web-ui
    --web-listen
    127.0.0.1:6363
}";

        assert_eq!(service_output_value(output, "state"), Some("running"));
        assert!(service_output_has_arg(output, "--web-ui"));
        assert_eq!(
            service_output_arg_value(output, "--web-listen"),
            Some("127.0.0.1:6363")
        );
    }

    #[test]
    fn disabling_auto_index_on_search_creates_config_when_unset() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().to_path_buf())).unwrap();

        disable_auto_index_on_search_by_default(&paths, &UserConfig::default()).unwrap();

        let config_path = tmp.path().join("config.toml");
        let contents = std::fs::read_to_string(config_path).unwrap();
        assert!(contents.contains("auto_index_on_search = false"));
    }

    #[test]
    fn disabling_auto_index_preserves_nested_mcp_config() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().to_path_buf())).unwrap();
        std::fs::write(
            tmp.path().join("config.toml"),
            "[mcp]\nlisten = \"127.0.0.1:4567\"\n",
        )
        .unwrap();
        let config = UserConfig::load(&paths).unwrap();
        disable_auto_index_on_search_by_default(&paths, &config).unwrap();
        let updated = UserConfig::load(&paths).unwrap();
        assert_eq!(updated.auto_index_on_search, Some(false));
        assert_eq!(updated.mcp.listen, Some("127.0.0.1:4567".parse().unwrap()));
    }

    #[test]
    fn disabling_auto_index_on_search_preserves_explicit_config() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().to_path_buf())).unwrap();
        std::fs::create_dir_all(tmp.path()).unwrap();
        std::fs::write(
            tmp.path().join("config.toml"),
            "auto_index_on_search = true\n",
        )
        .unwrap();
        let config = UserConfig::load(&paths).unwrap();

        disable_auto_index_on_search_by_default(&paths, &config).unwrap();

        let contents = std::fs::read_to_string(tmp.path().join("config.toml")).unwrap();
        assert_eq!(contents, "auto_index_on_search = true\n");
    }

    #[test]
    fn reindex_resets_only_derived_artifacts() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        std::fs::create_dir_all(paths.root.join("embed-cache")).unwrap();

        let preserved = [
            (paths.root.join("config.toml"), "index_service_mcp = true"),
            (paths.root.join("web-auth-token"), "owner-secret"),
            (paths.root.join("mcp-oauth.sqlite3"), "oauth registrations"),
            (paths.root.join("index-service.plist"), "launch config"),
            (paths.root.join("unrelated-user-file"), "keep me"),
            (
                paths.state.join("retrieval-traces.jsonl"),
                "recorded evaluation",
            ),
            (paths.state.join("unrelated-state"), "keep this too"),
            (paths.root.join("embed-cache/model.bin"), "cached model"),
        ];
        for (path, contents) in &preserved {
            std::fs::write(path, contents).unwrap();
        }

        std::fs::write(paths.index.join("derived-index"), "index").unwrap();
        std::fs::write(paths.vectors.join("derived-vectors"), "vectors").unwrap();
        std::fs::create_dir_all(paths.root.join("memory")).unwrap();
        std::fs::write(paths.root.join("memory/documents.json"), "memory snapshot").unwrap();
        for name in [
            "ingest.json",
            "ingest.pending.json",
            "scan_cache.json",
            "analytics.sqlite",
            "analytics.sqlite-wal",
            "analytics.sqlite-shm",
            "analytics.sqlite-journal",
            "usage-cache.sqlite3",
            "usage-cache.sqlite3-wal",
            "usage-cache.sqlite3-shm",
            "usage-cache.sqlite3-journal",
        ] {
            std::fs::write(paths.state.join(name), "derived").unwrap();
        }

        reset_reindex_artifacts(&paths).unwrap();

        assert!(!paths.index.exists());
        assert!(!paths.vectors.exists());
        assert!(!paths.root.join("memory").exists());
        for name in [
            "ingest.json",
            "ingest.pending.json",
            "scan_cache.json",
            "analytics.sqlite",
            "analytics.sqlite-wal",
            "analytics.sqlite-shm",
            "analytics.sqlite-journal",
            "usage-cache.sqlite3",
            "usage-cache.sqlite3-wal",
            "usage-cache.sqlite3-shm",
            "usage-cache.sqlite3-journal",
        ] {
            assert!(!paths.state.join(name).exists(), "preserved {name}");
        }
        for (path, expected) in preserved {
            assert_eq!(
                std::fs::read_to_string(&path).unwrap(),
                expected,
                "changed preserved file {}",
                path.display()
            );
        }
    }

    #[test]
    fn daemon_cli_settings_override_config_without_discarding_other_content() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().to_path_buf())).unwrap();
        std::fs::write(
            tmp.path().join("config.toml"),
            "# user preferences\nindex_service_mode = \"interval\" # choose scheduling\nindex_service_web_ui = false\nindex_service_mcp = false\ninclude_reasoning = true\n\n[mcp]\n# keep this policy\nlisten = \"127.0.0.1:5363\"\nallowed_hosts = [\"localhost\"]\n",
        )
        .unwrap();
        let web_listen = "127.0.0.1:6464".to_string();
        let mcp_listen = "127.0.0.1:5464".parse().unwrap();

        persist_index_service_config(
            &paths,
            &IndexServiceConfigUpdates {
                mode: Some("continuous"),
                poll_interval: Some(12),
                web_ui: Some(true),
                web_listen: Some(web_listen.clone()),
                mcp: Some(true),
                mcp_listen: Some(mcp_listen),
                plist: Some("/tmp/com.memex.index.plist".into()),
                ..Default::default()
            },
        )
        .unwrap();

        let updated = UserConfig::load(&paths).unwrap();
        assert_eq!(updated.index_service_mode(), Some("continuous"));
        assert_eq!(updated.index_service_poll_interval(), 12);
        assert_eq!(updated.index_service_web_ui, Some(true));
        assert_eq!(
            updated.index_service_web_listen.as_deref(),
            Some(web_listen.as_str())
        );
        assert_eq!(updated.index_service_mcp, Some(true));
        assert_eq!(updated.mcp.listen, Some(mcp_listen));
        assert_eq!(updated.include_reasoning, Some(true));
        assert_eq!(
            updated.index_service_plist.as_deref(),
            Some(Path::new("/tmp/com.memex.index.plist"))
        );
        let contents = std::fs::read_to_string(tmp.path().join("config.toml")).unwrap();
        assert!(contents.contains("# user preferences"));
        assert!(contents.contains("# choose scheduling"));
        assert!(contents.contains("# keep this policy"));
        assert!(contents.contains("allowed_hosts = [\"localhost\"]"));
    }

    #[test]
    fn daemon_poll_interval_updates_preserve_supported_key_spelling() {
        for key in [
            "index_service_watch_interval",
            "index_service_poll_interval",
        ] {
            let tmp = TempDir::new().unwrap();
            let paths = Paths::new(Some(tmp.path().to_path_buf())).unwrap();
            let path = paths.root.join("config.toml");
            std::fs::write(
                &path,
                format!("# polling preference\n{key} = 30 # seconds\nindex_service_watch = true\n"),
            )
            .unwrap();
            assert_eq!(
                UserConfig::load(&paths)
                    .unwrap()
                    .index_service_poll_interval(),
                30
            );

            let updates = IndexServiceConfigUpdates::from_cli(
                false,
                Some(12),
                None,
                false,
                None,
                &DaemonMcpArgs::default(),
            )
            .unwrap();
            persist_index_service_config(&paths, &updates).unwrap();
            let updated = UserConfig::load(&paths).unwrap();
            assert_eq!(updated.index_service_poll_interval(), 12);
            assert_eq!(updated.index_service_continuous, Some(true));
            let contents = std::fs::read_to_string(&path).unwrap();
            assert!(contents.contains(&format!("{key} = 12 # seconds")));
            assert!(contents.contains("# polling preference"));

            // A plain restart must leave the valid saved configuration intact.
            persist_index_service_config(&paths, &IndexServiceConfigUpdates::default()).unwrap();
            assert_eq!(std::fs::read_to_string(&path).unwrap(), contents);
            assert_eq!(
                UserConfig::load(&paths)
                    .unwrap()
                    .index_service_poll_interval(),
                12
            );
        }
    }

    #[test]
    fn explicit_no_mcp_persists_over_configured_enablement() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().to_path_buf())).unwrap();
        std::fs::write(
            tmp.path().join("config.toml"),
            "index_service_mode = \"continuous\"\nindex_service_mcp = true\n",
        )
        .unwrap();

        let cli = Cli::try_parse_from(["memex", "index-service", "restart", "--no-mcp"]).unwrap();
        let Some(Commands::IndexService {
            action:
                IndexServiceCommand::Restart {
                    continuous,
                    poll_interval,
                    interval,
                    web_ui,
                    web_listen,
                    mcp,
                    ..
                },
        }) = cli.command
        else {
            panic!("expected index service restart command");
        };
        let updates = IndexServiceConfigUpdates::from_cli(
            continuous,
            poll_interval,
            interval,
            web_ui,
            web_listen.as_deref(),
            &mcp,
        )
        .unwrap();
        persist_index_service_config(&paths, &updates).unwrap();

        let updated = UserConfig::load(&paths).unwrap();
        assert_eq!(updated.index_service_mcp, Some(false));
        assert!(
            DaemonMcpArgs {
                mcp: false,
                no_mcp: false,
                mcp_listen: None,
            }
            .resolve(&updated)
            .is_none()
        );
    }

    #[test]
    fn plain_restart_reuses_previously_persisted_daemon_settings() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().to_path_buf())).unwrap();
        let mcp_listen = "127.0.0.1:5565".parse().unwrap();
        let cli = Cli::try_parse_from([
            "memex",
            "index-service",
            "enable",
            "--continuous",
            "--poll-interval",
            "9",
            "--web-listen",
            "127.0.0.1:6565",
            "--mcp-listen",
            "127.0.0.1:5565",
        ])
        .unwrap();
        let Some(Commands::IndexService {
            action:
                IndexServiceCommand::Enable {
                    continuous,
                    poll_interval,
                    interval,
                    web_ui,
                    web_listen,
                    mcp,
                    ..
                },
        }) = cli.command
        else {
            panic!("expected index service enable command");
        };
        let updates = IndexServiceConfigUpdates::from_cli(
            continuous,
            poll_interval,
            interval,
            web_ui,
            web_listen.as_deref(),
            &mcp,
        )
        .unwrap();
        persist_index_service_config(&paths, &updates).unwrap();
        let before_restart = std::fs::read_to_string(tmp.path().join("config.toml")).unwrap();

        let restart = Cli::try_parse_from(["memex", "index-service", "restart"]).unwrap();
        let Some(Commands::IndexService {
            action:
                IndexServiceCommand::Restart {
                    continuous,
                    poll_interval,
                    interval,
                    web_ui,
                    web_listen,
                    mcp,
                    ..
                },
        }) = restart.command
        else {
            panic!("expected plain index service restart command");
        };
        let restart_updates = IndexServiceConfigUpdates::from_cli(
            continuous,
            poll_interval,
            interval,
            web_ui,
            web_listen.as_deref(),
            &mcp,
        )
        .unwrap();
        persist_index_service_config(&paths, &restart_updates).unwrap();

        assert_eq!(
            std::fs::read_to_string(tmp.path().join("config.toml")).unwrap(),
            before_restart
        );
        let restarted = UserConfig::load(&paths).unwrap();
        assert_eq!(restarted.index_service_mode(), Some("continuous"));
        assert_eq!(restarted.index_service_poll_interval(), 9);
        assert!(restarted.index_service_web_ui_default());
        assert_eq!(
            restarted.index_service_web_listen.as_deref(),
            Some("127.0.0.1:6565")
        );
        let mcp = DaemonMcpArgs::default().resolve(&restarted).unwrap();
        assert_eq!(mcp.listen, mcp_listen);
    }

    fn make_vector(dims: usize) -> Vec<f32> {
        (0..dims).map(|i| (i as f32).sin()).collect()
    }

    #[test]
    fn vector_stats_line_reports_current_usearch_store() {
        let tmp = TempDir::new().unwrap();
        let mut index = VectorIndex::open_or_create(tmp.path(), 64, Some("bge")).unwrap();
        index.add(42, &make_vector(64)).unwrap();
        index.save().unwrap();

        let line = vector_stats_line(tmp.path()).unwrap();

        assert!(line.starts_with("vectors: 1 (dims 64, model bge, ids 1,"));
        assert!(line.contains("usearch.index"));
        assert!(line.contains("doc_ids.bin"));
        assert!(!line.contains("vectors.f32"));
        assert!(!line.contains("doc_ids.u64"));
    }

    #[test]
    fn vector_stats_line_reports_none_without_vector_store() {
        let tmp = TempDir::new().unwrap();

        assert_eq!(vector_stats_line(tmp.path()).unwrap(), "vectors: none");
    }

    #[test]
    fn index_args_accept_negative_source_flags() {
        let cli = Cli::try_parse_from([
            "memex",
            "index",
            "--no-codex",
            "--no-opencode",
            "--no-pi",
            "--no-copilot",
            "--no-grok",
            "--no-jcode",
            "--no-muse",
        ])
        .unwrap();

        let Some(Commands::Index { index, .. }) = cli.command else {
            panic!("expected index command");
        };
        assert!(index.no_codex);
        assert!(index.no_opencode);
        assert!(index.no_pi);
        assert!(index.no_copilot);
        assert!(index.no_grok);
        assert!(index.no_jcode);
        assert!(index.no_muse);
    }

    #[test]
    fn usage_accepts_custom_root() {
        let cli = Cli::try_parse_from(["memex", "usage", "--root", "/tmp/custom-memex"])
            .expect("parse usage root");

        let Some(Commands::Usage { root, .. }) = cli.command else {
            panic!("expected usage command");
        };
        assert_eq!(root, Some(PathBuf::from("/tmp/custom-memex")));
    }

    #[test]
    fn search_and_usage_accept_repeated_machine_filters() {
        let search = Cli::try_parse_from([
            "memex",
            "search",
            "needle",
            "--machine",
            "local",
            "--machine",
            "mini",
        ])
        .expect("parse search machines");
        let Some(Commands::Search { machine, .. }) = search.command else {
            panic!("expected search command");
        };
        assert_eq!(machine, ["local", "mini"]);

        let usage =
            Cli::try_parse_from(["memex", "usage", "--machine", "mini"]).expect("parse usage");
        let Some(Commands::Usage { machine, .. }) = usage.command else {
            panic!("expected usage command");
        };
        assert_eq!(machine, ["mini"]);
    }

    #[test]
    fn search_content_defaults_to_conversations_and_accepts_memory_corpora() {
        let default = Cli::try_parse_from(["memex", "search", "needle"]).unwrap();
        assert!(matches!(
            default.command,
            Some(Commands::Search {
                content: SearchContent::Conversations,
                ..
            })
        ));

        for (value, expected) in [
            ("memories", SearchContent::Memories),
            ("all", SearchContent::All),
        ] {
            let cli =
                Cli::try_parse_from(["memex", "search", "needle", "--content", value]).unwrap();
            assert!(matches!(
                cli.command,
                Some(Commands::Search { content, .. }) if content == expected
            ));
        }

        let request: SearchRequest = serde_json::from_value(serde_json::json!({
            "query": "needle",
            "content": "memories"
        }))
        .unwrap();
        assert_eq!(request.content, SearchContent::Memories);
    }

    #[test]
    fn show_session_and_hydrate_accept_machine_scoped_requests() {
        let show = Cli::try_parse_from(["memex", "show", "42", "--machine", "mini"])
            .expect("parse machine-scoped show");
        let Some(Commands::Show { machine, .. }) = show.command else {
            panic!("expected show command");
        };
        assert_eq!(machine, "mini");

        let memory = Cli::try_parse_from([
            "memex",
            "show",
            "--memory-id",
            "memory_sha256",
            "--section",
            "heading-2",
            "--content-version",
            "version_sha256",
            "--offset-chars",
            "120",
            "--max-chars",
            "400",
            "--machine",
            "mini",
        ])
        .expect("parse memory read");
        let Some(Commands::Show {
            memory_id,
            section,
            content_version,
            offset_chars,
            machine,
            ..
        }) = memory.command
        else {
            panic!("expected show command");
        };
        assert_eq!(memory_id.as_deref(), Some("memory_sha256"));
        assert_eq!(section.as_deref(), Some("heading-2"));
        assert_eq!(content_version.as_deref(), Some("version_sha256"));
        assert_eq!(offset_chars, 120);
        assert_eq!(machine, "mini");

        assert!(
            Cli::try_parse_from([
                "memex",
                "show",
                "--memory-id",
                "memory",
                "--record-id",
                "record"
            ])
            .is_err()
        );

        let session = Cli::try_parse_from([
            "memex",
            "session",
            "session-id",
            "--machine",
            "mini",
            "--source-path",
            "/tmp/session.jsonl",
            "--offset",
            "500",
            "--limit",
            "100",
        ])
        .expect("parse paginated session");
        let Some(Commands::Session {
            machine,
            source_path,
            offset,
            limit,
            ..
        }) = session.command
        else {
            panic!("expected session command");
        };
        assert_eq!(machine, "mini");
        assert_eq!(source_path.as_deref(), Some("/tmp/session.jsonl"));
        assert_eq!(offset, 500);
        assert_eq!(limit, Some(100));

        let hydrate = Cli::try_parse_from(["memex", "hydrate", "requests.jsonl"])
            .expect("parse hydrate command");
        let Some(Commands::Hydrate { input, .. }) = hydrate.command else {
            panic!("expected hydrate command");
        };
        assert_eq!(input, Some(PathBuf::from("requests.jsonl")));
    }

    #[test]
    fn retrieval_commands_accept_multi_query_scope_trace_context_and_eval() {
        let search = Cli::try_parse_from([
            "memex",
            "search",
            "primary",
            "--query",
            "alternate one",
            "--query",
            "alternate two",
            "--cwd",
            "/tmp/project",
            "--trace",
        ])
        .expect("parse retrieval search options");
        let Some(Commands::Search {
            query,
            additional_queries,
            cwd,
            trace,
            ..
        }) = search.command
        else {
            panic!("expected search command");
        };
        assert_eq!(query, "primary");
        assert_eq!(additional_queries, ["alternate one", "alternate two"]);
        assert_eq!(cwd, Some(PathBuf::from("/tmp/project")));
        assert!(trace);

        let context = Cli::try_parse_from([
            "memex",
            "context",
            "--record-id",
            "rid1_example",
            "--before",
            "3",
            "--after",
            "7",
            "--expand-interactions",
        ])
        .expect("parse context command");
        let Some(Commands::Context {
            record_id,
            before,
            after,
            expand_interactions,
            ..
        }) = context.command
        else {
            panic!("expected context command");
        };
        assert_eq!(record_id.as_deref(), Some("rid1_example"));
        assert_eq!(before, 3);
        assert_eq!(after, 7);
        assert!(expand_interactions);

        let eval = Cli::try_parse_from(["memex", "eval-retrieval", "dataset.jsonl", "--k", "50"])
            .expect("parse retrieval evaluation command");
        let Some(Commands::EvalRetrieval { dataset, k, .. }) = eval.command else {
            panic!("expected eval-retrieval command");
        };
        assert_eq!(dataset, PathBuf::from("dataset.jsonl"));
        assert_eq!(k, 50);
    }

    #[test]
    fn skill_management_subcommands_parse_targets_and_cleanup_mode() {
        let install = Cli::try_parse_from(["memex", "skill", "install", "--target", "shared"])
            .expect("parse skill install");
        let Some(Commands::Skill {
            command: SkillCommand::Install {
                target: Some(target),
            },
        }) = install.command
        else {
            panic!("expected skill install command");
        };
        assert_eq!(target, SkillTarget::Shared);

        let update = Cli::try_parse_from(["memex", "skill", "update"]).expect("parse skill update");
        let Some(Commands::Skill {
            command: SkillCommand::Update { target },
        }) = update.command
        else {
            panic!("expected skill update command");
        };
        assert_eq!(target, SkillTarget::All);

        let cleanup = Cli::try_parse_from(["memex", "skill", "cleanup", "--dry-run"])
            .expect("parse skill cleanup");
        assert!(matches!(
            cleanup.command,
            Some(Commands::Skill {
                command: SkillCommand::Cleanup { dry_run: true }
            })
        ));
    }

    #[test]
    fn skill_install_and_update_have_narrow_overwrite_semantics() {
        let home = TempDir::new().unwrap();
        let shared = home.path().join(".agents/skills/memex-search/SKILL.md");
        let claude = home.path().join(".claude/skills/memex-search/SKILL.md");

        write_skill_targets(home.path(), &[SkillTarget::Shared], SkillWriteMode::Install).unwrap();
        assert_eq!(
            std::fs::read_to_string(&shared).unwrap(),
            MEMEX_SEARCH_SKILL
        );
        assert!(!claude.exists());

        std::fs::write(&shared, "locally modified").unwrap();
        let error =
            write_skill_targets(home.path(), &[SkillTarget::Shared], SkillWriteMode::Install)
                .unwrap_err();
        assert!(error.to_string().contains("refusing to overwrite"));
        assert_eq!(
            std::fs::read_to_string(&shared).unwrap(),
            "locally modified"
        );

        write_skill_targets(home.path(), &[SkillTarget::All], SkillWriteMode::Update).unwrap();
        assert_eq!(
            std::fs::read_to_string(&shared).unwrap(),
            MEMEX_SEARCH_SKILL
        );
        assert!(!claude.exists());
    }

    #[test]
    fn skill_cleanup_is_explicit_and_supports_dry_run() {
        let home = TempDir::new().unwrap();
        let legacy_file = home.path().join("legacy.md");
        let legacy_dir = home.path().join("legacy-skill");
        std::fs::write(&legacy_file, "legacy").unwrap();
        std::fs::create_dir_all(&legacy_dir).unwrap();
        std::fs::write(legacy_dir.join("SKILL.md"), "legacy").unwrap();
        let paths = vec![legacy_file.clone(), legacy_dir.clone()];

        cleanup_legacy_skill_paths(&paths, true).unwrap();
        assert!(legacy_file.exists());
        assert!(legacy_dir.exists());

        cleanup_legacy_skill_paths(&paths, false).unwrap();
        assert!(!legacy_file.exists());
        assert!(!legacy_dir.exists());
    }

    #[test]
    fn service_environment_variables_include_pi_overrides() {
        let _guard = env_lock();
        let _env = EnvVarGuard::set_os(&[
            ("PI_CODING_AGENT_DIR", Some("/tmp/pi agent".as_ref())),
            (
                "PI_CODING_AGENT_SESSION_DIR",
                Some("/tmp/pi sessions".as_ref()),
            ),
        ]);

        let vars = service_environment_variables(None).unwrap();

        assert!(
            vars.iter()
                .any(|(key, value)| { key == "PI_CODING_AGENT_DIR" && value == "/tmp/pi agent" })
        );
        assert!(vars.iter().any(|(key, value)| {
            key == "PI_CODING_AGENT_SESSION_DIR" && value == "/tmp/pi sessions"
        }));
    }

    #[test]
    fn systemd_service_includes_environment_variables() {
        let service = build_systemd_service(
            "/usr/bin/memex",
            &["index".to_string(), "--no-pi".to_string()],
            false,
            &[(
                "PI_CODING_AGENT_SESSION_DIR".to_string(),
                "/tmp/pi \"sessions\" 100%".to_string(),
            )],
        );

        assert!(service.contains(
            "Environment=\"PI_CODING_AGENT_SESSION_DIR=/tmp/pi \\\"sessions\\\" 100%%\"\n"
        ));
        assert!(service.contains("ExecStart=/usr/bin/memex index --no-pi\n"));
    }

    #[test]
    fn registered_systemd_mode_comes_from_definition_not_activity_or_stale_timer() {
        let temp = TempDir::new().unwrap();
        let service_path = temp.path().join("memex-index.service");
        let timer_path = temp.path().join("memex-index.timer");
        std::fs::write(
            &service_path,
            build_systemd_service("/usr/bin/memex", &[], true, &[]),
        )
        .unwrap();
        std::fs::write(&timer_path, build_systemd_timer(60)).unwrap();

        assert_eq!(
            registered_systemd_mode(&service_path, &timer_path).unwrap(),
            Some(SystemdServiceMode::Continuous)
        );

        std::fs::write(
            &service_path,
            build_systemd_service("/usr/bin/memex", &[], false, &[]),
        )
        .unwrap();
        std::fs::remove_file(&timer_path).unwrap();
        assert_eq!(
            registered_systemd_mode(&service_path, &timer_path).unwrap(),
            Some(SystemdServiceMode::Interval)
        );
    }

    #[test]
    fn systemd_mode_changes_disable_the_previous_unit() {
        assert_eq!(
            systemd_counterpart_unit(
                "memex-index",
                false,
                Some(SystemdServiceMode::Continuous),
                false,
            )
            .as_deref(),
            Some("memex-index.service")
        );
        assert_eq!(
            systemd_counterpart_unit(
                "memex-index",
                true,
                Some(SystemdServiceMode::Interval),
                true,
            )
            .as_deref(),
            Some("memex-index.timer")
        );
        assert_eq!(
            systemd_counterpart_unit(
                "memex-index",
                true,
                Some(SystemdServiceMode::Continuous),
                true,
            )
            .as_deref(),
            Some("memex-index.timer")
        );
        assert_eq!(
            systemd_counterpart_unit(
                "memex-index",
                false,
                Some(SystemdServiceMode::Interval),
                true,
            ),
            None
        );
    }

    #[test]
    #[cfg(unix)]
    fn systemd_regeneration_stops_counterpart_and_removes_obsolete_timer() {
        use std::os::unix::fs::PermissionsExt;

        let _guard = env_lock();
        let temp = TempDir::new().unwrap();
        let bin_dir = temp.path().join("bin");
        let systemd_dir = temp.path().join("systemd");
        let log_path = temp.path().join("systemctl.log");
        std::fs::create_dir_all(&bin_dir).unwrap();
        std::fs::create_dir_all(&systemd_dir).unwrap();
        let systemctl = bin_dir.join("systemctl");
        std::fs::write(
            &systemctl,
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$MEMEX_TEST_SYSTEMCTL_LOG\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&systemctl, std::fs::Permissions::from_mode(0o755)).unwrap();

        let bin_path = bin_dir.as_os_str();
        let log_path_os = log_path.as_os_str();
        let _env = EnvVarGuard::set_os(&[
            ("PATH", Some(bin_path)),
            ("MEMEX_TEST_SYSTEMCTL_LOG", Some(log_path_os)),
        ]);
        let service_path = systemd_dir.join("memex-index.service");
        let timer_path = systemd_dir.join("memex-index.timer");
        std::fs::write(
            &service_path,
            build_systemd_service("/usr/bin/memex", &[], true, &[]),
        )
        .unwrap();

        run_index_service_enable_systemd(
            &UserConfig::default(),
            Some("memex-index".to_string()),
            false,
            60,
            30,
            Some(systemd_dir.clone()),
            std::path::Path::new("/usr/bin/memex"),
            &["index".to_string()],
        )
        .unwrap();
        assert!(timer_path.exists());

        run_index_service_enable_systemd(
            &UserConfig::default(),
            Some("memex-index".to_string()),
            true,
            60,
            30,
            Some(systemd_dir),
            std::path::Path::new("/usr/bin/memex"),
            &["index".to_string(), "--watch".to_string()],
        )
        .unwrap();
        assert!(!timer_path.exists());

        let commands = std::fs::read_to_string(log_path).unwrap();
        assert!(commands.contains("--user disable --now memex-index.service"));
        assert!(commands.contains("--user restart memex-index.timer"));
        assert!(commands.contains("--user disable --now memex-index.timer"));
        assert!(commands.contains("--user restart memex-index.service"));
    }

    #[test]
    #[cfg(unix)]
    fn systemd_unit_state_accepts_inactive_and_propagates_manager_failures() {
        use std::os::unix::process::ExitStatusExt;

        let inactive = std::process::Output {
            status: std::process::ExitStatus::from_raw(3 << 8),
            stdout: b"inactive\n".to_vec(),
            stderr: Vec::new(),
        };
        assert_eq!(
            parse_systemd_unit_state("memex-index.service", &inactive).unwrap(),
            "inactive"
        );

        let unavailable = std::process::Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: Vec::new(),
            stderr: b"Failed to connect to bus".to_vec(),
        };
        let error = parse_systemd_unit_state("memex-index.service", &unavailable).unwrap_err();
        assert!(error.to_string().contains("Failed to connect to bus"));
    }
}
