//! Run with writers stopped. Build this example against each storage version separately.
use anyhow::Result;
use clap::{Parser, ValueEnum};
use memex::vector_transfer;
use std::path::PathBuf;

#[derive(Clone, ValueEnum)]
enum Action {
    Export,
    Import,
    Verify,
}

#[derive(Parser)]
struct Args {
    #[arg(value_enum)]
    action: Action,
    #[arg(long)]
    root: PathBuf,
    /// SQLite cache outside both database roots; keep until migration is verified.
    #[arg(long)]
    cache: PathBuf,
    #[arg(long, default_value = "bge")]
    model: String,
    #[arg(long, default_value_t = 384)]
    dimensions: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let report = match args.action {
        Action::Export => vector_transfer::export(&args.root, &args.cache)?,
        Action::Import => {
            vector_transfer::import(&args.root, &args.cache, &args.model, args.dimensions)?
        }
        Action::Verify => {
            vector_transfer::verify(&args.root, &args.cache, &args.model, args.dimensions)?
        }
    };
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
