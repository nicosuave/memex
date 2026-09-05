pub mod analytics;
pub mod cli;
pub mod config;
pub mod embed;
pub mod herdr;
pub mod index;
pub mod ingest;
pub mod lease;
pub mod machine;
pub mod mcp;
pub mod progress;
pub mod read_budget;
pub mod resume;
pub mod retrieval;
pub mod retrieval_eval;
pub mod sources;
pub mod state;
pub mod transfer;
pub mod tui;
pub mod types;
pub mod usage;
pub mod vector;
pub mod web;
pub mod web_auth;

#[cfg(test)]
pub mod test_support;
