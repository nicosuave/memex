//! Optional execution-control MCP. It does not start providers or widen listeners.
use anyhow::{Result, bail};
use std::path::PathBuf;

fn main() -> Result<()> {
    let mut arguments = std::env::args().skip(1);
    let mut root = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--root" => {
                root = Some(PathBuf::from(
                    arguments
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--root requires a path"))?,
                ))
            }
            "--help" => {
                println!(
                    "memex-control [--root PATH]\nSeparate opt-in stdio MCP for a running MemexExecutionHost. Grants same-user execution access. Normal memex mcp remains retrieval-only."
                );
                return Ok(());
            }
            _ => bail!("unknown argument: {argument}"),
        }
    }
    memex::control_mcp::run(root)
}
