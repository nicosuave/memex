//! `memex dream` and the `--content dreams` search corpus.
use super::{OutputArgs, OutputFormat, SearchMode, parse_ts_millis, print_json};
use crate::config::{Paths, UserConfig};
use crate::dream::gather::format_day;
use crate::dream::model::{Dream, Generation};
use crate::dream::search::{DreamSearchHit, DreamSearchOptions, search_dreams};
use crate::dream::store::{DIFF_FILE, DreamStore, MARKDOWN_FILE};
use crate::dream::{DreamOptions, DreamReport, LintOutcome, dream_status, run_dream, run_lint};
use crate::machine::LOCAL_MACHINE_ID;
use crate::types::{SourceFilter, SourceKind};
use anyhow::{Result, anyhow, bail};
use clap::{Args, Subcommand};
use serde_json::Value;
use std::collections::HashSet;
use std::path::PathBuf;

#[derive(Debug, Clone, Default, Args)]
pub(super) struct DreamRunArgs {
    /// Run the executor and show what would change without publishing a generation
    #[arg(long)]
    dry_run: bool,
    /// Print the prompt the executor would receive and exit
    #[arg(long, conflicts_with = "dry_run")]
    print_prompt: bool,
    /// Agent CLI that judges the run: claude or codex [default: config, then PATH]
    #[arg(long, value_name = "NAME")]
    executor: Option<String>,
    /// Model passed to the executor
    #[arg(long)]
    model: Option<String>,
    /// Read records newer than this instead of continuing from the last dream run
    #[arg(long, value_name = "TIMESTAMP")]
    since: Option<String>,
    /// Most sessions to read, newest first
    #[arg(long, value_name = "N")]
    max_sessions: Option<usize>,
    /// Only sessions and memories of this project
    #[arg(long)]
    project: Option<String>,
    /// Only sessions and memories from this agent
    #[arg(long)]
    source: Option<SourceFilter>,
    /// Machine whose sessions to read (repeatable) [default: config, then this machine]
    #[arg(long, value_name = "ID")]
    machine: Vec<String>,
    /// Path to memex data directory [default: ~/.memex]
    #[arg(long)]
    root: Option<PathBuf>,
    #[command(flatten)]
    output: OutputArgs,
}

#[derive(Subcommand)]
pub(super) enum DreamCommand {
    /// Read new sessions, judge them with the executor, and publish a generation
    Run {
        #[command(flatten)]
        run: DreamRunArgs,
    },
    /// Print the current dreams, or one dream with every citation
    Show {
        /// Dream ID (or a unique prefix) from `dream show` or search
        dream_id: Option<String>,
        /// Read this generation instead of the current one
        #[arg(long, value_name = "ID")]
        generation: Option<String>,
        #[arg(long)]
        root: Option<PathBuf>,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Show what a generation changed relative to its parent
    Diff {
        /// Generation ID [default: current]
        generation: Option<String>,
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Re-resolve every citation; rewrite moved ones and mark lost evidence
    Lint {
        /// Report without publishing a generation
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        root: Option<PathBuf>,
        #[command(flatten)]
        output: OutputArgs,
    },
    /// Show the store, recent generations, and whether the daemon would dream now
    Status {
        #[arg(long)]
        root: Option<PathBuf>,
        #[command(flatten)]
        output: OutputArgs,
    },
}

pub(super) fn run_dream_command(action: Option<DreamCommand>, run: DreamRunArgs) -> Result<()> {
    match action {
        None => dream_run(run),
        Some(DreamCommand::Run { run }) => dream_run(run),
        Some(DreamCommand::Show {
            dream_id,
            generation,
            root,
            output,
        }) => dream_show(dream_id, generation, root, output),
        Some(DreamCommand::Diff { generation, root }) => {
            let store = DreamStore::new(&Paths::new(root)?);
            let id = selected_generation(&store, generation)?;
            let diff = store
                .read_generation_file(&id, DIFF_FILE)?
                .ok_or_else(|| anyhow!("dream generation {id} has no diff"))?;
            print!("{diff}");
            Ok(())
        }
        Some(DreamCommand::Lint {
            dry_run,
            root,
            output,
        }) => {
            let paths = Paths::new(root)?;
            let config = UserConfig::load(&paths)?;
            let outcome = run_lint(&paths, &config, dry_run)?;
            let output = output.resolve(OutputFormat::Text, None, false)?;
            if output.format == OutputFormat::Text {
                print_lint(&outcome);
                Ok(())
            } else {
                print_json(&serde_json::to_value(&outcome)?, output.pretty)
            }
        }
        Some(DreamCommand::Status { root, output }) => {
            let paths = Paths::new(root)?;
            let config = UserConfig::load(&paths)?;
            let status = dream_status(&paths, &config)?;
            output
                .resolve(OutputFormat::Text, None, false)?
                .print_value(&serde_json::to_value(&status)?)
        }
    }
}

fn dream_run(args: DreamRunArgs) -> Result<()> {
    let paths = Paths::new(args.root)?;
    let config = UserConfig::load(&paths)?;
    let output = args.output.resolve(OutputFormat::Text, None, false)?;
    let report = run_dream(
        &paths,
        &config,
        &DreamOptions {
            dry_run: args.dry_run,
            print_prompt: args.print_prompt,
            executor: args.executor,
            model: args.model,
            since_ms: parse_ts_millis(args.since)?,
            max_sessions: args.max_sessions,
            project: args.project,
            source: args.source,
            machines: args.machine,
        },
    )?;
    if output.format != OutputFormat::Text {
        return print_json(&serde_json::to_value(&report)?, output.pretty);
    }
    if let Some(prompt) = &report.prompt {
        print!("{prompt}");
        return Ok(());
    }
    print_report(&report, &paths);
    Ok(())
}

fn print_report(report: &DreamReport, paths: &Paths) {
    for failure in &report.failures {
        eprintln!("Warning: {failure}");
    }
    println!(
        "dream: read {} sessions ({} skipped), {} records, {} memory sections since {}",
        report.sessions,
        report.sessions_skipped,
        report.records,
        report.memory_sections,
        format_day(report.since_ms),
    );
    if report.lint.citations > 0 {
        println!(
            "dream: existing citations: {} resolved, {} remapped, {} unverified, {} lost, {} source revised",
            report.lint.resolved,
            report.lint.remapped,
            report.lint.unverified,
            report.lint.evidence_lost,
            report.lint.source_revised,
        );
    }
    if let Some(reason) = &report.skipped {
        println!("dream: {reason}; nothing to judge");
    } else {
        println!(
            "dream: {} judged {} prompt chars: {} added, {} reinforced, {} revised, {} retracted, {} rejected",
            report.executor.as_deref().unwrap_or("executor"),
            report.prompt_chars,
            report.counts.added,
            report.counts.reinforced,
            report.counts.revised,
            report.counts.retracted,
            report.counts.rejected,
        );
    }
    match &report.generation {
        Some(generation) => println!(
            "dream: published generation {generation} ({} active dreams) in {}",
            report.active_dreams,
            DreamStore::new(paths).dir().display(),
        ),
        None if report.dry_run => println!("dream: dry run, nothing published"),
        None => println!("dream: nothing published"),
    }
    if !report.diff.is_empty() {
        println!("\n{}", report.diff.trim_end());
    }
}

fn print_lint(outcome: &LintOutcome) {
    let report = &outcome.report;
    println!(
        "dream lint: {} citations in {} dreams: {} resolved, {} remapped, {} unverified, {} lost, {} source revised",
        report.citations,
        report.dreams,
        report.resolved,
        report.remapped,
        report.unverified,
        report.evidence_lost,
        report.source_revised,
    );
    if report.dreams_without_evidence > 0 {
        println!(
            "dream lint: {} dreams have no readable evidence left",
            report.dreams_without_evidence
        );
    }
    match &outcome.generation {
        Some(generation) => println!(
            "dream lint: {} citations changed; published generation {generation}",
            report.changed
        ),
        None if report.changed > 0 => println!(
            "dream lint: {} citations would change (dry run)",
            report.changed
        ),
        None => println!("dream lint: nothing changed"),
    }
    if report.changed > 0 {
        println!("\n{}", outcome.diff.trim_end());
    }
}

fn selected_generation(store: &DreamStore, requested: Option<String>) -> Result<String> {
    match requested {
        Some(id) => Ok(id),
        None => store
            .current_id()?
            .ok_or_else(|| anyhow!("no dreams yet; run `memex dream`")),
    }
}

fn find_dream<'a>(generation: &'a Generation, wanted: &str) -> Result<&'a Dream> {
    if let Some(dream) = generation.dream(wanted) {
        return Ok(dream);
    }
    let mut matches = generation
        .dreams
        .iter()
        .filter(|dream| dream.id.starts_with(wanted));
    match (matches.next(), matches.next()) {
        (Some(dream), None) => Ok(dream),
        (Some(_), Some(_)) => bail!("dream prefix {wanted:?} is ambiguous"),
        _ => bail!("dream {wanted:?} not found in generation {}", generation.id),
    }
}

fn dream_show(
    dream_id: Option<String>,
    generation: Option<String>,
    root: Option<PathBuf>,
    output: OutputArgs,
) -> Result<()> {
    let store = DreamStore::new(&Paths::new(root)?);
    let id = selected_generation(&store, generation)?;
    let Some(dream_id) = dream_id else {
        let output = output.resolve(OutputFormat::Text, None, false)?;
        if output.format == OutputFormat::Text {
            let markdown = store
                .read_generation_file(&id, MARKDOWN_FILE)?
                .ok_or_else(|| anyhow!("dream generation {id} has no rendered dreams"))?;
            print!("{markdown}");
            return Ok(());
        }
        return print_json(&serde_json::to_value(store.load(&id)?)?, output.pretty);
    };
    let generation = store.load(&id)?;
    let dream = find_dream(&generation, &dream_id)?;
    let mut value = serde_json::to_value(dream)?;
    value["generation"] = Value::from(generation.id.clone());
    value["evidence"] = Value::from(dream.evidence_health().label());
    output
        .resolve(OutputFormat::Json, None, false)?
        .print_value(&value)
}

pub(super) struct DreamSearchArgs {
    pub(super) query: String,
    pub(super) project: Option<String>,
    pub(super) source: Option<SourceFilter>,
    pub(super) since: Option<String>,
    pub(super) until: Option<String>,
    pub(super) limit: usize,
    pub(super) sort_by_timestamp: bool,
    pub(super) mode: SearchMode,
    /// A filter that only conversation records can satisfy was given.
    pub(super) conversation_filters: bool,
    pub(super) machines: Vec<String>,
    pub(super) fields: Option<HashSet<String>>,
    pub(super) root: Option<PathBuf>,
}

/// Search the dreams of the current generation. Dreams live on the machine whose run wrote
/// them and are matched by keyword; asking for another machine or a vector mode is an
/// error rather than a silently different search.
pub(super) fn collect_dream_search(args: DreamSearchArgs) -> Result<Vec<Value>> {
    if args.mode != SearchMode::Lexical {
        bail!("--content dreams supports lexical search only");
    }
    if args.conversation_filters {
        bail!(
            "--cwd, --role, --tool, --session, --tag, and --origin filter conversations; --content dreams accepts --project, --source, --since, and --until"
        );
    }
    if args
        .machines
        .iter()
        .any(|machine| machine != LOCAL_MACHINE_ID)
    {
        bail!("--content dreams searches this machine's dream store only; use --machine local");
    }
    let paths = Paths::new(args.root)?;
    crate::dream::ensure_enabled(&UserConfig::load(&paths)?)?;
    let hits = search_dreams(
        &paths,
        &DreamSearchOptions {
            query: args.query,
            project: args.project,
            source: args
                .source
                .and_then(|source| SourceKind::from_label(source.as_str())),
            since: parse_ts_millis(args.since)?,
            until: parse_ts_millis(args.until)?,
            limit: args.limit,
            sort_by_timestamp: args.sort_by_timestamp,
            include_inactive: false,
        },
    )?;
    hits.into_iter()
        .map(|hit| project_dream_hit(hit, &args.fields))
        .collect()
}

/// A dream as search prints it, narrowed to the requested fields.
pub(super) fn project_dream_hit(
    hit: DreamSearchHit,
    fields: &Option<HashSet<String>>,
) -> Result<Value> {
    let mut value = serde_json::to_value(hit)?;
    if let (Some(fields), Some(object)) = (fields, value.as_object_mut()) {
        object.retain(|key, _| fields.contains(key));
    }
    Ok(value)
}
