pub mod analytics;
pub mod cli;
pub mod config;
pub mod daemon_runtime;
pub mod embed;
mod embed_cpu;
pub mod herdr;
pub mod index;
pub mod ingest;
pub mod lease;
pub mod machine;
pub mod mcp;
pub mod memory;
pub mod memory_search;
#[cfg(unix)]
mod native;
#[doc(hidden)]
pub mod profiling;
pub mod progress;
pub mod read_budget;
pub mod redact;
pub mod remote_embed;
mod repository;
pub mod resume;
pub mod retrieval;
pub mod retrieval_eval;
pub mod rex;
mod search;
pub mod session_meta;
pub mod sources;
pub mod state;
pub mod transfer;
pub mod tui;
pub mod types;
pub mod usage;
pub mod vector;
pub mod vector_backfill;
pub mod watch;
pub mod web;
pub mod web_auth;

pub mod vector_transfer;

#[cfg(test)]
pub mod test_support;
