use crate::analytics::{AnalyticsStore, analytics_path, backfill_from_index};
use crate::config::{Paths, UserConfig, default_claude_sources};
use crate::embed::EmbedderHandle;
use crate::index::{QueryOptions, SearchIndex, SessionScopeKey};
use crate::ingest::{IngestOptions, ingest_all};
use crate::lease::{INGEST_LEASE_TIMEOUT, IngestLease};
use crate::machine::{
    LocatedRecord, MAX_HYDRATE_INPUT_BYTES, MAX_HYDRATE_LINE_BYTES, MAX_SESSION_BATCH_SIZE,
    MAX_SESSION_PAGE_SIZE, SearchMode, SearchSpec, SessionPageRequest, UsageSpec,
    batch_session_contexts, federated_search, federated_usage, record_by_doc_id,
    session_page_context,
};
use crate::retrieval::canonical_record_id;
use crate::retrieval::{ContextOptions, ContextSelector, context_records};
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
use clap::{Args, Parser, Subcommand, ValueEnum};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use regex::RegexBuilder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Duration;
use std::time::Instant;

static TRACE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Parser)]
#[command(
    name = "memex",
    version,
    about = "Fast local history search for Claude, Codex, Cursor, OpenCode, Pi, Oh My Pi, OpenClaw, Copilot, Grok, and Hermes",
    after_help = "\
QUICK START:
    memex                           # Browse sessions interactively
    memex index                     # Index your agent history
    memex search \"error handling\"   # Search for keywords

LEARN MORE:
    memex <command> --help          # Detailed help for each command"
)]
pub struct Cli {
    /// Defaults to the interactive TUI when no command is given
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Args, Clone)]
struct IndexArgs {
    /// Path to Claude projects directory [default: CLAUDE_CONFIG_DIR or ~/.claude/projects]
    #[arg(long)]
    source: Option<PathBuf>,
    /// Include agent subprocess conversations (Claude Code subagents)
    #[arg(long)]
    include_agents: bool,
    /// Index plaintext reasoning as BM25-only records (encrypted/redacted reasoning is always dropped)
    #[arg(long)]
    include_reasoning: bool,
    /// Index Codex sessions from ~/.codex [default: true]
    #[arg(long, default_value_t = true)]
    codex: bool,
    /// Skip indexing Codex sessions
    #[arg(long = "no-codex", default_value_t = false)]
    no_codex: bool,
    /// Index Opencode sessions from ~/.local/share/opencode [default: true]
    #[arg(long, default_value_t = true)]
    opencode: bool,
    /// Skip indexing Opencode sessions
    #[arg(long = "no-opencode", default_value_t = false)]
    no_opencode: bool,
    /// Index Cursor agent transcripts from ~/.cursor/projects [default: true]
    #[arg(long = "no-cursor", action = clap::ArgAction::SetFalse, default_value_t = true)]
    cursor: bool,
    /// Index Pi sessions from ~/.pi/agent/sessions or $PI_CODING_AGENT_DIR/sessions [default: true]
    #[arg(long, default_value_t = true)]
    pi: bool,
    /// Skip indexing Pi sessions
    #[arg(long = "no-pi", default_value_t = false)]
    no_pi: bool,
    /// Index Oh My Pi sessions from ~/.omp/agent/sessions [default: true]
    #[arg(long, default_value_t = true)]
    omp: bool,
    /// Skip indexing Oh My Pi sessions
    #[arg(long = "no-omp", default_value_t = false)]
    no_omp: bool,
    /// Index OpenClaw sessions from ~/.openclaw or ~/.clawdbot [default: true]
    #[arg(long, default_value_t = true)]
    openclaw: bool,
    /// Skip indexing OpenClaw sessions
    #[arg(long = "no-openclaw", default_value_t = false)]
    no_openclaw: bool,
    /// Index GitHub Copilot CLI sessions from ~/.copilot [default: true]
    #[arg(long, default_value_t = true)]
    copilot: bool,
    /// Skip indexing GitHub Copilot CLI sessions
    #[arg(long = "no-copilot", default_value_t = false)]
    no_copilot: bool,
    /// Index Grok sessions from ~/.grok/sessions [default: true]
    #[arg(long, default_value_t = true)]
    grok: bool,
    /// Skip indexing Grok sessions
    #[arg(long = "no-grok", default_value_t = false)]
    no_grok: bool,
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
    /// Print aggregate parser diagnostics without transcript content
    #[arg(long)]
    diagnostics: bool,
    /// Exclude transcripts whose source path matches this glob (repeatable).
    /// Matched transcripts are never indexed. Also configurable via
    /// `exclude_paths` in ~/.memex/config.toml.
    #[arg(long = "exclude", value_name = "GLOB")]
    exclude: Vec<String>,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Commands {
    /// Index Claude, Codex, Cursor, OpenCode, Pi, Oh My Pi, OpenClaw, Copilot, and Grok conversation history
    #[command(after_help = "\
EXAMPLES:
    memex index                         # Index all supported local history
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
        #[arg(long, hide = true)]
        web_ui: bool,
        #[arg(long, hide = true, value_name = "ADDRESS")]
        web_listen: Option<String>,
    },
    /// Delete existing index and rebuild from scratch
    Reindex {
        #[command(flatten)]
        index: IndexArgs,
    },
    /// Reclaim unreachable immutable index generations without rebuilding
    IndexGc {
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
        /// Report what would be removed without changing the index
        #[arg(long)]
        dry_run: bool,
        /// Confirm the index service and all Memex readers are stopped
        #[arg(long)]
        offline: bool,
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
    machine, score, ts, doc_id, record_id, project, role, session_id, source, source_path, text, snippet, matches
    event_id, parent_event_id, logical_parent_event_id, parent_session_id, thread_source, conversation_kind
    parent_tool_use_id, source_tool_use_id, source_tool_assistant_uuid")]
    Search {
        /// Search query (keywords or natural language for semantic search)
        query: String,
        /// Additional independent query view to fuse with reciprocal-rank fusion (repeatable)
        #[arg(long = "query", value_name = "QUERY")]
        additional_queries: Vec<String>,
        /// Restrict results to sessions from this working directory/repository
        #[arg(long, value_name = "PATH")]
        cwd: Option<PathBuf>,
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
        /// Filter by source: claude, codex, cursor, opencode, pi, omp (Oh My Pi), openclaw, copilot, grok, or hermes
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
        /// Machine to search (repeatable). Defaults to multi_machine.default or all configured machines.
        #[arg(long, value_name = "ID")]
        machine: Vec<String>,
        /// Persist a metadata-only retrieval trace and print its ID to stderr
        #[arg(long)]
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
    #[command(after_help = "\
EXAMPLES:
    memex web
    memex web --listen 127.0.0.1:8080")]
    Web {
        /// Address and port to bind
        #[arg(long, default_value = crate::web::DEFAULT_LISTEN)]
        listen: String,
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
        /// Originating machine for federated search results
        #[arg(long, default_value = crate::machine::LOCAL_MACHINE_ID)]
        machine: String,
        /// Restrict hydration to this source transcript path
        #[arg(long)]
        source_path: Option<String>,
        /// Number of records to skip before the page
        #[arg(long, default_value_t = 0)]
        offset: usize,
        /// Return at most this many records (maximum 500)
        #[arg(long)]
        limit: Option<usize>,
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
        /// Originating machine for federated search results
        #[arg(long, default_value = crate::machine::LOCAL_MACHINE_ID)]
        machine: String,
        /// Pretty-print JSON output
        #[arg(short, long)]
        verbose: bool,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Hydrate bounded session pages from JSONL requests (stdin when omitted)
    #[command(
        name = "hydrate",
        visible_alias = "hydrate-batch",
        after_help = "\
REQUEST FORMAT (one JSON object per line):
    {\"machine\":\"mini\",\"session_id\":\"abc\",\"source_path\":\"/tmp/session.jsonl\",\"offset\":0,\"limit\":100}

EXAMPLES:
    memex hydrate requests.jsonl
    cat requests.jsonl | memex hydrate

The input contains at most 32 requests; each page is limited to 500 records."
    )]
    Hydrate {
        /// JSONL request file; omit or use '-' to read stdin
        input: Option<PathBuf>,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Return a bounded context neighborhood around a record, document, or native event ID
    Context {
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
        #[arg(short, long)]
        verbose: bool,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Run retrieval queries from a JSONL evaluation dataset
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
    /// List indexed sessions with cwd and git metadata (newest first)
    #[command(after_help = "\
EXAMPLES:
    memex sessions                        # 20 most recent sessions as JSONL
    memex sessions --cwd .                # sessions from the current repo
    memex sessions --source claude --limit 5
    memex sessions --json-array")]
    Sessions {
        /// Only sessions whose cwd is this path, lives under it, or whose git root is it
        #[arg(long)]
        cwd: Option<PathBuf>,
        /// Filter by project (repository grouping)
        #[arg(long)]
        project: Option<String>,
        /// Filter by source: claude, codex, cursor, opencode, pi, omp (Oh My Pi), openclaw, copilot, grok, or hermes
        #[arg(long)]
        source: Option<SourceFilter>,
        /// Only include sessions active on or after this date/timestamp
        #[arg(long, value_name = "DATE_OR_TIMESTAMP")]
        since: Option<String>,
        /// Maximum number of sessions
        #[arg(long, default_value_t = 20)]
        limit: usize,
        /// Emit one JSON array instead of JSON Lines
        #[arg(long)]
        json_array: bool,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Herdr plugin helpers (used by herdr/plugin.sh)
    #[command(hide = true)]
    Herdr {
        #[command(subcommand)]
        action: HerdrCommand,
    },
    /// Show index statistics (document count, vector count, storage paths)
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
    memex usage --json")]
    Usage {
        /// Filter by source: claude, codex, cursor, opencode, pi, omp (Oh My Pi), openclaw, copilot, grok, or hermes
        #[arg(long)]
        source: Option<SourceFilter>,
        /// Only include events on or after this date/timestamp
        #[arg(long, value_name = "DATE_OR_TIMESTAMP")]
        since: Option<String>,
        /// Only include events before this date/timestamp
        #[arg(long, value_name = "DATE_OR_TIMESTAMP")]
        until: Option<String>,
        /// Emit the report as JSON
        #[arg(long)]
        json: bool,
        /// Include normalized request-level events in JSON output
        #[arg(long, requires = "json")]
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
        /// Filter by source: claude, codex, cursor, opencode, pi, omp (Oh My Pi), openclaw, copilot, grok, or hermes
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
    },
    /// Regenerate and restart the background indexing service using current config
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
    },
    /// Open the authenticated Web UI in the default browser
    Open {
        /// Web UI address and port [default: config or 127.0.0.1:6363]
        #[arg(long, value_name = "ADDRESS")]
        listen: Option<String>,
        /// Path to memex data directory [default: ~/.memex]
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Show the registered service state and Web UI status
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
    // Bare `memex` opens the TUI home screen.
    let command = cli.command.unwrap_or(Commands::Tui {
        query: None,
        project: None,
        root: None,
    });
    let should_check = !matches!(
        command,
        Commands::Tui { .. }
            | Commands::Update { .. }
            | Commands::Rpc { .. }
            | Commands::Herdr { .. }
    );
    if should_check {
        check_for_update_async(None);
    }
    match command {
        Commands::Index {
            index,
            watch,
            watch_interval,
            web_ui,
            web_listen,
        } => {
            if watch {
                let listen = (web_ui || web_listen.is_some())
                    .then(|| web_listen.unwrap_or_else(|| crate::web::DEFAULT_LISTEN.to_string()));
                run_index_loop(&index, watch_interval, listen)?;
            } else if web_ui || web_listen.is_some() {
                return Err(anyhow!("--web-ui requires --watch"));
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
            additional_queries,
            cwd,
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
            machine,
            trace,
        } => {
            run_search(
                query,
                additional_queries,
                cwd,
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
                machine,
                trace,
            )?;
        }
        Commands::Tui {
            query,
            project,
            root,
        } => {
            let (update_tx, update_rx) = std::sync::mpsc::channel();
            check_for_update_async(Some(update_tx));
            tui::run(root, Some(update_rx), query, project)?;
        }
        Commands::Web { listen, root } => {
            crate::web::serve(root, &listen)?;
        }
        Commands::IndexService { action } => match action {
            IndexServiceCommand::Enable {
                index,
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
            IndexServiceCommand::Open { listen, root } => {
                run_index_service_open(listen, root)?;
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
            machine,
            source_path,
            offset,
            limit,
            verbose,
            root,
        } => {
            run_session(
                session_id,
                machine,
                source_path,
                offset,
                limit,
                verbose,
                root,
            )?;
        }
        Commands::Show {
            doc_id,
            machine,
            verbose,
            root,
        } => {
            run_show(doc_id, machine, verbose, root)?;
        }
        Commands::Hydrate { input, root } => {
            run_hydrate(input, root)?;
        }
        Commands::Context {
            record_id,
            doc_id,
            event_id,
            session,
            source,
            before,
            after,
            expand_interactions,
            verbose,
            root,
        } => {
            run_context(ContextRunArgs {
                record_id,
                doc_id,
                event_id,
                session,
                source,
                before,
                after,
                expand_interactions,
                verbose,
                root,
            })?;
        }
        Commands::EvalRetrieval { dataset, k, root } => {
            run_eval_retrieval(dataset, k, root)?;
        }
        Commands::Sessions {
            cwd,
            project,
            source,
            since,
            limit,
            json_array,
            root,
        } => {
            run_sessions(cwd, project, source, since, limit, json_array, root)?;
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
            since,
            until,
            json,
            events,
            cost,
            root,
            machine,
        } => {
            run_usage(UsageCommandOptions {
                source,
                since,
                until,
                json,
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
            run_update(yes)?;
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
    }
    Ok(())
}

fn run_index_loop(index: &IndexArgs, interval_secs: u64, web_listen: Option<String>) -> Result<()> {
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
        std::thread::sleep(Duration::from_secs(interval_secs));
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
        index.codex && !index.no_codex,
        index.opencode && !index.no_opencode,
        index.cursor,
        index.pi && !index.no_pi,
        index.omp && !index.no_omp,
        index.openclaw && !index.no_openclaw,
        index.copilot && !index.no_copilot,
        index.grok && !index.no_grok,
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
    codex: bool,
    opencode: bool,
    cursor: bool,
    pi: bool,
    omp: bool,
    openclaw: bool,
    copilot: bool,
    grok: bool,
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
    if reindex && paths.root.exists() {
        std::fs::remove_dir_all(&paths.root)?;
    }
    paths.ensure_dirs()?;
    let index = if continuous {
        SearchIndex::open_or_create_for_continuous_ingest(&paths.index)?
    } else {
        SearchIndex::open_or_create_for_ingest(&paths.index)?
    };

    let opts = IngestOptions {
        claude_sources: source
            .map(|source| vec![source])
            .unwrap_or_else(default_claude_sources),
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

fn run_index_gc(root: Option<PathBuf>, dry_run: bool, offline: bool) -> Result<()> {
    if !dry_run && !offline {
        return Err(anyhow!(
            "index GC requires offline confirmation; stop the Memex index service and all Memex \
             readers, then rerun with `--offline` (or use `--dry-run`)"
        ));
    }
    let paths = Paths::new(root)?;
    let _lease = IngestLease::acquire(&paths, "index-gc", INGEST_LEASE_TIMEOUT)?;
    let report = SearchIndex::garbage_collect_generations_offline(&paths.index, dry_run)?;
    if report.dry_run {
        println!(
            "would remove {} unreachable generations, {} abandoned generation work directories, and {} legacy index files; no rebuild required",
            report.generations_removed,
            report.abandoned_workdirs_removed,
            report.legacy_files_removed
        );
    } else {
        println!(
            "removed {} unreachable generations, {} abandoned generation work directories, and {} legacy index files; retained the committed index without rebuilding",
            report.generations_removed,
            report.abandoned_workdirs_removed,
            report.legacy_files_removed
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
    progress.finish();
    println!(
        "embedded {} vectors (claude {}, codex {}, opencode {}, cursor {}, pi {}, openclaw {}, copilot {})",
        embedded_total,
        embedded_counts[crate::types::SourceKind::Claude.idx()],
        embedded_counts[crate::types::SourceKind::Codex.idx()],
        embedded_counts[crate::types::SourceKind::Opencode.idx()],
        embedded_counts[crate::types::SourceKind::Cursor.idx()],
        embedded_counts[crate::types::SourceKind::Pi.idx()],
        embedded_counts[crate::types::SourceKind::OpenClaw.idx()],
        embedded_counts[crate::types::SourceKind::Copilot.idx()],
    );

    std::io::stdout().flush().ok();
    std::process::exit(0);
}

#[allow(clippy::too_many_arguments)]
fn run_search(
    query: String,
    additional_queries: Vec<String>,
    cwd: Option<PathBuf>,
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
    machines: Vec<String>,
    trace: bool,
) -> Result<()> {
    let trace_started = Instant::now();
    let trace_started_at_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let mut queries = vec![query];
    queries.extend(additional_queries.clone());
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

    let candidate_limit = if queries.len() > 1
        || top_n_per_session.is_some()
        || options.source.is_some()
        || cwd.is_some()
    {
        (limit * 5).max(limit + 10)
    } else {
        limit
    };
    let mode = if hybrid {
        SearchMode::Hybrid
    } else if semantic {
        SearchMode::Semantic
    } else {
        SearchMode::Lexical
    };
    let selected_machines = crate::machine::selected_machine_ids(&config, &machines)?;
    let mut ranked_queries = Vec::with_capacity(queries.len());
    let mut query_candidate_counts = Vec::with_capacity(queries.len());
    let mut failures = Vec::new();
    let mut seen_failures = HashSet::new();
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
        query_candidate_counts.push(federated.candidate_count);
        for (machine, error) in federated.failures {
            let message = format!("{machine}: {error}");
            if seen_failures.insert(message.clone()) {
                eprintln!("Warning: machine '{machine}' unavailable: {error}");
                failures.push(message);
            }
        }
        ranked_queries.push(federated.items);
    }
    let mut results = if ranked_queries.len() == 1 {
        ranked_queries.pop().unwrap_or_default()
    } else {
        fuse_ranked_queries(ranked_queries, crate::retrieval_eval::DEFAULT_RRF_K)
    };
    let mut merged_render = render.clone();
    merged_render.min_score = None;
    results = apply_post_processing_located(results, &merged_render);
    if trace {
        let mode_label = match (mode, queries.len() > 1) {
            (SearchMode::Lexical, false) => "lexical",
            (SearchMode::Semantic, false) => "semantic",
            (SearchMode::Hybrid, false) => "hybrid",
            (SearchMode::Lexical, true) => "lexical-rrf",
            (SearchMode::Semantic, true) => "semantic-rrf",
            (SearchMode::Hybrid, true) => "hybrid-rrf",
        };
        write_retrieval_trace(TraceWriteArgs {
            paths: &paths,
            queries: &queries,
            query_candidate_counts: &query_candidate_counts,
            cwd,
            results: &results,
            mode: mode_label,
            machines: &selected_machines,
            failures: &failures,
            started: trace_started,
            started_at_ms: trace_started_at_ms,
        })?;
    }
    render_located_results(results, &render)
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
    if render.verbose {
        for LocatedRecord {
            machine,
            score,
            record,
        } in results
        {
            let ts = format_ts(record.ts);
            let text = summarize(&record.text, 200);
            println!(
                "[{score:.3}] {} {} {} {} {} {} {}",
                machine, ts, record.doc_id, record.project, record.role, record.session_id, text
            );
        }
        return Ok(());
    }

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

struct ContextRunArgs {
    record_id: Option<String>,
    doc_id: Option<u64>,
    event_id: Option<String>,
    session: Option<String>,
    source: Option<SourceFilter>,
    before: usize,
    after: usize,
    expand_interactions: bool,
    verbose: bool,
    root: Option<PathBuf>,
}

fn run_context(args: ContextRunArgs) -> Result<()> {
    let ContextRunArgs {
        record_id,
        doc_id,
        event_id,
        session,
        source,
        before,
        after,
        expand_interactions,
        verbose,
        root,
    } = args;
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
    let index = SearchIndex::open_or_create(&paths.index)?;
    let result = context_records(
        &index,
        &selector.with_scope(session, source),
        ContextOptions {
            before,
            after,
            expand_interactions,
        },
    )?;
    if verbose {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("{}", serde_json::to_string(&result)?);
    }
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

fn run_hydrate(input: Option<PathBuf>, root: Option<PathBuf>) -> Result<()> {
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
            "hydrate input exceeds maximum size of {MAX_HYDRATE_INPUT_BYTES} bytes"
        ));
    }
    let mut requests = Vec::new();
    for (line, raw) in contents.lines().enumerate() {
        if raw.trim().is_empty() {
            continue;
        }
        if raw.len() > MAX_HYDRATE_LINE_BYTES {
            return Err(anyhow!(
                "hydrate request line {} exceeds maximum size of {} bytes",
                line + 1,
                MAX_HYDRATE_LINE_BYTES
            ));
        }
        let request = serde_json::from_str::<HydrateRequest>(raw)
            .with_context(|| format!("parse hydrate request line {}", line + 1))?;
        if request.session_id.is_empty() {
            return Err(anyhow!(
                "hydrate request line {} has an empty session_id",
                line + 1
            ));
        }
        if request.limit == 0 || request.limit > MAX_SESSION_PAGE_SIZE {
            return Err(anyhow!(
                "hydrate request line {} limit must be between 1 and {}",
                line + 1,
                MAX_SESSION_PAGE_SIZE
            ));
        }
        requests.push(request);
    }
    if requests.is_empty() {
        return Err(anyhow!("hydrate input is empty"));
    }
    if requests.len() > MAX_SESSION_BATCH_SIZE {
        return Err(anyhow!(
            "hydrate accepts at most {MAX_SESSION_BATCH_SIZE} requests"
        ));
    }
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let mut grouped: HashMap<String, Vec<(usize, SessionPageRequest)>> = HashMap::new();
    for (index, request) in requests.into_iter().enumerate() {
        let machine = request
            .machine
            .unwrap_or_else(|| crate::machine::LOCAL_MACHINE_ID.to_string());
        grouped.entry(machine).or_default().push((
            index,
            SessionPageRequest {
                session_id: request.session_id,
                source_path: request.source_path,
                offset: request.offset,
                limit: request.limit,
            },
        ));
    }
    let total_requests = grouped.values().map(Vec::len).sum();
    let mut output: Vec<Option<Value>> = vec![None; total_requests];
    for (machine, batch) in grouped {
        let page_requests: Vec<_> = batch.iter().map(|(_, request)| request.clone()).collect();
        match batch_session_contexts(&paths, &config, &machine, &page_requests) {
            Ok(contexts) => {
                for ((original_index, _), context) in batch.into_iter().zip(contexts) {
                    output[original_index] = Some(hydrate_success_value(&machine, context)?);
                }
            }
            Err(batch_error) => {
                eprintln!("Warning: hydrate machine '{machine}' failed: {batch_error}");
                for (original_index, request) in batch {
                    let value = if machine == crate::machine::LOCAL_MACHINE_ID {
                        match session_page_context(&paths, &config, &machine, &request) {
                            Ok(context) => hydrate_success_value(&machine, context)?,
                            Err(error) => {
                                hydrate_error_value(&machine, &request, &error.to_string())?
                            }
                        }
                    } else {
                        hydrate_error_value(&machine, &request, &batch_error.to_string())?
                    };
                    output[original_index] = Some(value);
                }
            }
        }
    }
    for value in output {
        let value = value.ok_or_else(|| anyhow!("hydrate response missing for request"))?;
        println!("{}", value);
    }
    Ok(())
}

fn hydrate_success_value(
    machine: &str,
    context: crate::machine::SessionPageContext,
) -> Result<Value> {
    Ok(serde_json::to_value(HydrateOutput {
        machine: machine.to_string(),
        session_id: context.session_id,
        source_path: context.source_path,
        cwd: context.cwd,
        offset: context.offset,
        total: context.total,
        next_offset: context.next_offset,
        records: context
            .records
            .into_iter()
            .map(|record| HydrateRecordOutput {
                record_id: canonical_record_id(&record),
                record,
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

fn run_session(
    session_id: String,
    machine: String,
    source_path: Option<String>,
    offset: usize,
    limit: Option<usize>,
    verbose: bool,
    root: Option<PathBuf>,
) -> Result<()> {
    if session_id.is_empty() {
        return Err(anyhow!("session_id must not be empty"));
    }
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let records = hydrate_session_records(
        &paths,
        &config,
        &machine,
        &session_id,
        source_path.as_deref().unwrap_or_default(),
        offset,
        limit,
    )?;
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
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "machine": &machine,
                "record_id": canonical_record_id(&record),
                "record": record,
            }))?
        );
    }
    Ok(())
}

fn run_show(doc_id: u64, machine: String, verbose: bool, root: Option<PathBuf>) -> Result<()> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let record = record_by_doc_id(&paths, &config, &machine, doc_id)?;
    if verbose {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "machine": &machine,
                "record_id": canonical_record_id(&record),
                "record": record,
            }))?
        );
        return Ok(());
    }
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "machine": &machine,
            "record_id": canonical_record_id(&record),
            "record": record,
        }))?
    );
    Ok(())
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
    println!("index: {}", paths.index.display());
    println!("documents: {}", index.doc_count()?);
    print_vector_stats(&paths.vectors)?;
    Ok(())
}

struct UsageCommandOptions {
    source: Option<SourceFilter>,
    since: Option<String>,
    until: Option<String>,
    json: bool,
    include_events: bool,
    cost_mode: CostMode,
    root: Option<PathBuf>,
    machines: Vec<String>,
}

fn run_usage(options: UsageCommandOptions) -> Result<()> {
    let UsageCommandOptions {
        source,
        since,
        until,
        json,
        include_events,
        cost_mode,
        root,
        machines,
    } = options;
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
            },
        )?;
        if json {
            println!("{}", serde_json::to_string_pretty(&report)?);
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
        session_keys: None,
        since_ms,
        until_ms,
        cost_mode,
        include_events,
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
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
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

fn run_sessions(
    cwd: Option<PathBuf>,
    project: Option<String>,
    source: Option<SourceFilter>,
    since: Option<String>,
    limit: usize,
    json_array: bool,
    root: Option<PathBuf>,
) -> Result<()> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let store = open_analytics_read_only(&paths)?;
    let since_ms = parse_ts_millis(since)?;
    let cwd_filter = canonical_cwd_filter(cwd);
    let rows = store.query_sessions_detailed(
        source,
        project.as_deref(),
        cwd_filter.as_deref(),
        since_ms,
        Some(limit),
    )?;

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
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
        if json_array {
            items.push(value);
        } else {
            writeln!(out, "{value}")?;
        }
    }
    if json_array {
        writeln!(out, "{}", Value::Array(items))?;
    }
    Ok(())
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
    if let Some(session_id) = &session_id {
        rows.retain(|row| &row.session_id == session_id);
        if rows.is_empty() {
            return Err(anyhow!("session '{session_id}' not found"));
        }
    } else if rows.is_empty() && cwd_filter.is_some() && !strict_cwd {
        // The public CLI keeps its historical global fallback unless the Herdr plugin
        // explicitly requires the focused directory to match.
        rows = store.query_sessions_detailed(source, None, None, None, Some(50))?;
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

    let paths = Paths::new(index.root.clone())?;
    let config = UserConfig::load(&paths)?;
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
    let continuous = if cli_continuous || web_ui {
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
    let program_args =
        build_index_command_args(index, continuous, poll_interval, web_ui, &web_listen);

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
    disable_auto_index_on_search_by_default(&paths, &config)?;
    if web_ui {
        wait_for_web_ui(&web_listen, Duration::from_secs(5))?;
        println!("web UI: http://{web_listen}");
    }
    Ok(())
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

    if !contents.trim().is_empty() {
        if !contents.ends_with('\n') {
            contents.push('\n');
        }
        contents.push('\n');
    }
    contents.push_str(
        "# Background indexing handles freshness; avoid duplicate scan work during search.\n",
    );
    contents.push_str("auto_index_on_search = false\n");

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

fn run_index_service_open(listen: Option<String>, root: Option<PathBuf>) -> Result<()> {
    let paths = Paths::new(root.clone())?;
    let config = UserConfig::load(&paths)?;
    let listen = listen
        .or(config.index_service_web_listen)
        .unwrap_or_else(|| crate::web::DEFAULT_LISTEN.to_string());
    let url = crate::web::bootstrap_url(root, &listen)?;
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
            println!("index service: stopped");
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
    println!("index service: {service_state}");
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
        println!("index service: stopped");
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
    println!("index service: {state}");
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
            println!("web UI: http://{listen}");
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
                        if is_memex_health_response(&response) {
                            return true;
                        }
                    }
                    Err(_) => return false,
                }
            }
            is_memex_health_response(&response)
        })
    })
}

fn is_memex_health_response(response: &[u8]) -> bool {
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
        && body == "ok"
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

fn apply_post_processing_located(
    mut results: Vec<LocatedRecord>,
    render: &RenderOptions,
) -> Vec<LocatedRecord> {
    if let Some(min_score) = render.min_score {
        results.retain(|result| result.score >= min_score);
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
    println!("Run 'memex skill update' to update installed skill copies.");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{EnvVarGuard, env_lock};
    use crate::vector::VectorIndex;
    use tempfile::TempDir;

    #[test]
    fn build_index_command_args_preserves_disabled_sources() {
        let index = IndexArgs {
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
            no_codex: false,
            no_opencode: false,
            no_pi: false,
            no_omp: false,
            no_openclaw: false,
            no_copilot: false,
            no_grok: false,
            embeddings: false,
            no_embeddings: false,
            model: None,
            root: None,
            diagnostics: false,
        };

        let args = build_index_command_args(&index, false, 30, false, crate::web::DEFAULT_LISTEN);

        assert!(args.contains(&"--no-codex".to_string()));
        assert!(args.contains(&"--no-opencode".to_string()));
        assert!(args.contains(&"--no-cursor".to_string()));
        assert!(args.contains(&"--no-pi".to_string()));
        assert!(args.contains(&"--no-openclaw".to_string()));
        assert!(args.contains(&"--no-omp".to_string()));
        assert!(args.contains(&"--no-copilot".to_string()));
        assert!(args.contains(&"--no-grok".to_string()));
    }

    #[test]
    fn build_index_command_args_forwards_exclude_patterns() {
        let index = IndexArgs {
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
            no_codex: false,
            no_opencode: false,
            no_pi: false,
            no_omp: false,
            no_openclaw: false,
            no_copilot: false,
            no_grok: false,
            embeddings: false,
            no_embeddings: false,
            model: None,
            root: None,
            diagnostics: false,
        };

        let args = build_index_command_args(&index, false, 30, false, "127.0.0.1:7777");

        let mut pairs = args.windows(2);
        assert!(pairs.any(|w| w == ["--exclude", "~/work/**"]));
        let mut pairs = args.windows(2);
        assert!(pairs.any(|w| w == ["--exclude", "/tmp/secret/*.jsonl"]));
    }

    #[test]
    fn build_index_command_args_includes_web_ui_options() {
        let index = IndexArgs {
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
            no_codex: false,
            no_opencode: false,
            no_pi: false,
            no_omp: false,
            no_openclaw: false,
            no_copilot: false,
            no_grok: false,
            embeddings: false,
            no_embeddings: false,
            model: None,
            root: None,
            diagnostics: false,
        };

        let args = build_index_command_args(&index, true, 30, true, "127.0.0.1:6363");

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
        let (listen, server) = serve_one_test_response(
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
        );

        wait_for_web_ui(&listen, Duration::from_secs(1)).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn web_ui_readiness_rejects_unrelated_tcp_listener() {
        let (listen, server) = serve_one_test_response(
            "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnope",
        );

        assert!(!web_ui_is_healthy(&listen));
        server.join().unwrap();
    }

    fn serve_one_test_response(response: &'static str) -> (String, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let listen = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).unwrap();
            stream.write_all(response.as_bytes()).unwrap();
        });
        (listen, server)
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
    fn show_session_and_hydrate_accept_machine_scoped_requests() {
        let show = Cli::try_parse_from(["memex", "show", "42", "--machine", "mini"])
            .expect("parse machine-scoped show");
        let Some(Commands::Show { machine, .. }) = show.command else {
            panic!("expected show command");
        };
        assert_eq!(machine, "mini");

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
