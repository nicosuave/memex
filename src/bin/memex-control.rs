//! Optional execution-control MCP. It does not start providers or widen listeners.
use anyhow::{Result, bail};
use std::path::PathBuf;

fn main() -> Result<()> {
    let mut arguments = std::env::args().skip(1);
    let mut root = None;
    let mut scope = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--root" => {
                root = Some(PathBuf::from(
                    arguments
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--root requires a path"))?,
                ))
            }
            "--conversation" => {
                scope = Some(arguments.next().ok_or_else(|| {
                    anyhow::anyhow!("--conversation requires a hosted conversation id")
                })?)
            }
            "--help" => {
                println!(
                    "memex-control [--root PATH] [--conversation ID]\nSeparate opt-in stdio MCP for a running MemexExecutionHost. Grants same-user execution access. Normal memex mcp remains retrieval-only.\n--conversation (or {}) confines operations to that hosted conversation and the children it creates.",
                    memex::control_mcp::SCOPE_ENV
                );
                return Ok(());
            }
            _ => bail!("unknown argument: {argument}"),
        }
    }
    let environment = std::env::var(memex::control_mcp::SCOPE_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    if let (Some(argument), Some(environment)) = (&scope, &environment)
        && argument != environment
    {
        bail!(
            "--conversation differs from {}; a launcher-set scope cannot be widened",
            memex::control_mcp::SCOPE_ENV
        );
    }
    memex::control_mcp::run(root, scope.or(environment))
}
