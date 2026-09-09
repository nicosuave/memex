//! Public command organization and output conventions. Legacy entrypoints normalize
//! to the same execution paths so scripts and installed services keep working.
use super::{Commands, IndexArgs, ReadArgs};
use anyhow::{Result, anyhow};
use clap::{Args, Subcommand, ValueEnum};
use serde_json::Value;
use std::path::PathBuf;

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
pub(super) enum IndexCommand {
    /// Rebuild the index from scratch
    Rebuild {
        #[command(flatten)]
        index: IndexArgs,
    },
    /// Reclaim unreachable generations (requires stopped readers)
    Gc {
        #[arg(long)]
        root: Option<PathBuf>,
        /// Report what would be removed
        #[arg(long)]
        dry_run: bool,
        /// Confirm the service and all readers are stopped
        #[arg(long)]
        offline: bool,
    },
    /// Generate embeddings for an existing index
    Embed {
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Show index statistics and storage paths
    Stats {
        #[arg(long)]
        root: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
pub(super) enum WebCommand {
    /// Serve the local conversation browser
    Serve {
        #[arg(long, default_value = crate::web::DEFAULT_LISTEN)]
        listen: String,
        #[arg(long)]
        root: Option<PathBuf>,
    },
    /// Open the authenticated browser (uses config when --listen is omitted)
    Open {
        /// Print a login link without opening a browser
        #[arg(long)]
        print_url: bool,
        #[arg(long)]
        listen: Option<String>,
        #[arg(long)]
        root: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
pub(super) enum SessionCommand {
    /// Read bounded session pages from JSONL requests
    #[command(
        after_help = "REQUEST FORMAT (one JSON object per line):\n    {\"machine\":\"local\",\"session_id\":\"abc\",\"offset\":0,\"limit\":100}\n\nOmit the file or use '-' for stdin. At most 32 requests, 500 records per page.\nThe character budget is shared across all requests, in input order."
    )]
    Batch {
        /// JSONL request file (defaults to stdin)
        input: Option<PathBuf>,
        #[command(flatten)]
        read: ReadArgs,
        #[command(flatten)]
        output: OutputArgs,
        #[arg(long)]
        root: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
pub(super) enum DebugCommand {
    /// Evaluate retrieval with a JSONL dataset
    EvalRetrieval {
        dataset: PathBuf,
        #[arg(long, default_value_t = 20)]
        k: usize,
        #[arg(long)]
        root: Option<PathBuf>,
    },
}

impl Commands {
    pub(super) fn canonicalize(self) -> Self {
        match self {
            Self::Index {
                action: Some(action),
                ..
            } => match action {
                IndexCommand::Rebuild { index } => Self::Reindex { index },
                IndexCommand::Gc {
                    root,
                    dry_run,
                    offline,
                } => Self::IndexGc {
                    root,
                    dry_run,
                    offline,
                },
                IndexCommand::Embed { model, root } => Self::Embed { model, root },
                IndexCommand::Stats { root } => Self::Stats { root },
            },
            Self::Web {
                action: Some(action),
                ..
            } => match action {
                WebCommand::Serve { listen, root } => Self::Web {
                    action: None,
                    listen,
                    root,
                },
                WebCommand::Open {
                    listen,
                    root,
                    print_url,
                } => Self::IndexService {
                    action: super::IndexServiceCommand::Open {
                        listen,
                        root,
                        print_url,
                    },
                },
            },
            Self::Session {
                action:
                    Some(SessionCommand::Batch {
                        input,
                        read,
                        output,
                        root,
                    }),
                ..
            } => Self::Hydrate {
                input,
                read,
                output,
                root,
            },
            command => command,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(super) enum CliSearchMode {
    Lexical,
    Semantic,
    Hybrid,
}

/// Only providers with local discovery in IngestOptions belong here. SourceFilter
/// additionally includes providers whose records can be read from an existing index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(super) enum IndexSource {
    Claude,
    Codex,
    Opencode,
    Cursor,
    Pi,
    Omp,
    #[value(alias = "open-claw")]
    Openclaw,
    Copilot,
    Grok,
    Jcode,
    Muse,
    Antigravity,
}

impl IndexArgs {
    pub(super) fn source_enabled(&self, source: IndexSource) -> bool {
        let legacy_enabled = match source {
            IndexSource::Claude => true,
            IndexSource::Codex => self.codex && !self.no_codex,
            IndexSource::Opencode => self.opencode && !self.no_opencode,
            IndexSource::Cursor => self.cursor,
            IndexSource::Pi => self.pi && !self.no_pi,
            IndexSource::Omp => self.omp && !self.no_omp,
            IndexSource::Openclaw => self.openclaw && !self.no_openclaw,
            IndexSource::Copilot => self.copilot && !self.no_copilot,
            IndexSource::Grok => self.grok && !self.no_grok,
            IndexSource::Jcode => self.jcode && !self.no_jcode,
            IndexSource::Muse => self.muse && !self.no_muse,
            IndexSource::Antigravity => self.antigravity && !self.no_antigravity,
        };
        legacy_enabled
            && (self.only_source.is_empty() || self.only_source.contains(&source))
            && !self.exclude_source.contains(&source)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(super) enum OutputFormat {
    Jsonl,
    Json,
    Text,
}

#[derive(Debug, Clone, Default, Args)]
pub(super) struct OutputArgs {
    /// Output encoding (default: JSONL for lists/pages, JSON for records/context, text for usage)
    #[arg(long, value_enum, help_heading = "Output")]
    format: Option<OutputFormat>,
    /// Pretty-print JSON; streams require --format json
    #[arg(long, help_heading = "Output")]
    pretty: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OutputOptions {
    pub(super) format: OutputFormat,
    pub(super) pretty: bool,
}

impl OutputArgs {
    pub(super) fn resolve(
        self,
        default: OutputFormat,
        legacy: Option<OutputFormat>,
        legacy_pretty: bool,
    ) -> Result<OutputOptions> {
        if self.format.is_some() && (legacy.is_some() || legacy_pretty) {
            return Err(anyhow!(
                "--format cannot be combined with legacy output switches"
            ));
        }
        let output = OutputOptions {
            format: self.format.or(legacy).unwrap_or(default),
            pretty: self.pretty || legacy_pretty,
        };
        if output.pretty && output.format != OutputFormat::Json {
            return Err(anyhow!(
                "--pretty requires JSON output; use --format json --pretty"
            ));
        }
        Ok(output)
    }
}

impl OutputOptions {
    pub(super) fn print_value(self, value: &Value) -> Result<()> {
        match self.format {
            OutputFormat::Text => print_text(value, 0),
            _ => super::print_json(value, self.pretty),
        }
    }

    pub(super) fn print_values(self, values: Vec<Value>) -> Result<()> {
        let mut writer = self.writer();
        for value in values {
            writer.write(value)?;
        }
        writer.finish()
    }

    pub(super) fn writer(self) -> OutputWriter {
        OutputWriter {
            options: self,
            values: Vec::new(),
        }
    }
}

/// Buffer only JSON arrays; JSONL and text keep incremental output behavior.
pub(super) struct OutputWriter {
    options: OutputOptions,
    values: Vec<Value>,
}

impl OutputWriter {
    pub(super) fn write(&mut self, value: Value) -> Result<()> {
        if self.options.format == OutputFormat::Json {
            self.values.push(value);
            Ok(())
        } else {
            self.options.print_value(&value)
        }
    }

    pub(super) fn finish(self) -> Result<()> {
        if self.options.format == OutputFormat::Json {
            self.options.print_value(&Value::Array(self.values))
        } else {
            Ok(())
        }
    }
}

// New text views keep all fields (including pagination/continuation metadata)
// visible, while existing search/session/usage text layouts remain unchanged.
fn print_text(value: &Value, depth: usize) -> Result<()> {
    let indent = "  ".repeat(depth);
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                if value.is_object() || value.is_array() {
                    println!("{indent}{key}:");
                    print_text(value, depth + 1)?;
                } else {
                    println!("{indent}{key}: {}", scalar_text(value));
                }
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                println!("{indent}[{}]", index + 1);
                print_text(item, depth + 1)?;
            }
        }
        _ => println!("{indent}{}", scalar_text(value)),
    }
    Ok(())
}

fn scalar_text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Cli;
    use clap::Parser;

    fn index_args(args: &[&str]) -> IndexArgs {
        let cli = Cli::try_parse_from(args).unwrap();
        match cli.command.unwrap().canonicalize() {
            Commands::Index { index, .. } | Commands::Reindex { index } => index,
            Commands::IndexService {
                action:
                    super::super::IndexServiceCommand::Enable { index, .. }
                    | super::super::IndexServiceCommand::Restart { index, .. },
            } => index,
            _ => panic!("expected indexing command"),
        }
    }

    #[test]
    fn each_discovered_source_can_be_selected_and_excluded() {
        let defaults = index_args(&["memex", "index"]);
        for &source in IndexSource::value_variants() {
            assert!(defaults.source_enabled(source));
            let name = source.to_possible_value().unwrap();
            for command in [
                vec!["memex", "index"],
                vec!["memex", "index", "rebuild"],
                vec!["memex", "service", "enable"],
                vec!["memex", "service", "restart"],
            ] {
                let mut args = command;
                args.extend(["--only-source", name.get_name()]);
                let selected = index_args(&args);
                for &candidate in IndexSource::value_variants() {
                    assert_eq!(selected.source_enabled(candidate), candidate == source);
                }
                args.extend(["--exclude-source", name.get_name()]);
                let excluded = index_args(&args);
                assert!(
                    IndexSource::value_variants()
                        .iter()
                        .all(|&s| !excluded.source_enabled(s))
                );
            }
        }
    }

    #[test]
    fn repeated_sources_and_legacy_disables_survive_service_round_trip() {
        let selected = index_args(&[
            "memex",
            "service",
            "enable",
            "--only-source",
            "claude",
            "--only-source",
            "codex",
            "--only-source",
            "pi",
            "--exclude-source",
            "claude",
            "--no-pi",
            "--claude-path",
            "/tmp/custom projects",
        ]);
        let args = super::super::build_index_command_args(
            &selected,
            true,
            17,
            crate::watch::WatchMode::Events,
            true,
            "127.0.0.1:4567",
            None,
        );
        let mut command = vec!["memex"];
        command.extend(args.iter().map(String::as_str));
        let reparsed = index_args(&command);
        assert_eq!(
            reparsed.source.as_deref(),
            Some(std::path::Path::new("/tmp/custom projects"))
        );
        for &source in IndexSource::value_variants() {
            assert_eq!(
                reparsed.source_enabled(source),
                source == IndexSource::Codex
            );
        }
    }

    #[test]
    fn old_source_path_and_all_old_disable_flags_still_parse() {
        let selected = index_args(&[
            "memex",
            "index",
            "--source",
            "/tmp/custom",
            "--include-agents",
            "--no-codex",
            "--no-opencode",
            "--no-cursor",
            "--no-pi",
            "--no-omp",
            "--no-openclaw",
            "--no-copilot",
            "--no-grok",
            "--no-jcode",
            "--no-muse",
            "--no-antigravity",
        ]);
        assert_eq!(
            selected.source.as_deref(),
            Some(std::path::Path::new("/tmp/custom"))
        );
        for &source in IndexSource::value_variants() {
            assert_eq!(
                selected.source_enabled(source),
                source == IndexSource::Claude
            );
        }
    }

    #[test]
    fn session_id_named_batch_has_an_explicit_escape() {
        let cli = Cli::try_parse_from(["memex", "session", "--", "batch"]).unwrap();
        assert!(
            matches!(cli.command.unwrap().canonicalize(), Commands::Session {
            session_id: Some(id), action: None, ..
        } if id == "batch")
        );
    }

    #[test]
    fn nested_commands_do_not_silently_discard_parent_options() {
        for args in [
            vec!["memex", "index", "--exclude-source", "codex", "rebuild"],
            vec!["memex", "web", "--listen", "127.0.0.1:4567", "serve"],
        ] {
            assert!(Cli::try_parse_from(&args).is_err(), "accepted {args:?}");
        }
    }
}

#[derive(Debug, Clone, Default, Args)]
pub(super) struct DaemonMcpArgs {
    /// Serve MCP in the daemon (implies continuous mode)
    #[arg(long, conflicts_with = "no_mcp", help_heading = "MCP")]
    pub(super) mcp: bool,
    /// Disable configured MCP serving for this daemon invocation
    #[arg(long, conflicts_with = "mcp_listen", help_heading = "MCP")]
    pub(super) no_mcp: bool,
    /// MCP socket (implies --mcp; default: [mcp].listen or 127.0.0.1:5363)
    #[arg(long, help_heading = "MCP")]
    pub(super) mcp_listen: Option<std::net::SocketAddr>,
}

impl DaemonMcpArgs {
    pub(super) fn resolve(
        &self,
        config: &crate::config::UserConfig,
    ) -> Option<crate::mcp::HttpOptions> {
        (!self.no_mcp
            && (self.mcp || self.mcp_listen.is_some() || config.index_service_mcp.unwrap_or(false)))
        .then(|| mcp_http_options(config, self.mcp_listen, Vec::new(), Vec::new(), None))
    }
}

pub(super) fn mcp_http_options(
    config: &crate::config::UserConfig,
    listen: Option<std::net::SocketAddr>,
    hosts: Vec<String>,
    origins: Vec<String>,
    public_url: Option<String>,
) -> crate::mcp::HttpOptions {
    crate::mcp::HttpOptions {
        listen: listen
            .or(config.mcp.listen)
            .unwrap_or_else(|| "127.0.0.1:5363".parse().expect("default MCP socket")),
        allowed_hosts: if hosts.is_empty() {
            config.mcp.allowed_hosts.clone()
        } else {
            hosts
        },
        allowed_origins: if origins.is_empty() {
            config.mcp.allowed_origins.clone()
        } else {
            origins
        },
        public_url: public_url.or_else(|| config.mcp.public_url.clone()),
    }
}

#[cfg(test)]
mod mcp_options_tests {
    use super::*;
    use crate::config::{McpConfig, UserConfig};

    #[test]
    fn daemon_mcp_enablement_and_cli_overrides() {
        let mut config = UserConfig::default();
        assert!(DaemonMcpArgs::default().resolve(&config).is_none());
        assert_eq!(
            mcp_http_options(&config, None, vec![], vec![], None).listen,
            "127.0.0.1:5363".parse().unwrap()
        );
        config.index_service_mcp = Some(true);
        config.mcp = McpConfig {
            listen: Some("127.0.0.1:4567".parse().unwrap()),
            allowed_hosts: vec!["configured.example".into()],
            allowed_origins: vec!["https://configured.example".into()],
            public_url: Some("https://configured.example/mcp".into()),
        };
        let configured = DaemonMcpArgs::default().resolve(&config).unwrap();
        assert_eq!(configured.listen, config.mcp.listen.unwrap());
        assert_eq!(configured.allowed_hosts, config.mcp.allowed_hosts);
        assert_eq!(configured.allowed_origins, config.mcp.allowed_origins);
        assert_eq!(configured.public_url, config.mcp.public_url);
        assert!(
            DaemonMcpArgs {
                no_mcp: true,
                ..Default::default()
            }
            .resolve(&config)
            .is_none()
        );
        config.index_service_mcp = Some(false);
        assert!(
            DaemonMcpArgs {
                mcp: true,
                ..Default::default()
            }
            .resolve(&config)
            .is_some()
        );
        let socket = "127.0.0.1:4568".parse().unwrap();
        assert_eq!(
            DaemonMcpArgs {
                mcp_listen: Some(socket),
                ..Default::default()
            }
            .resolve(&config)
            .unwrap()
            .listen,
            socket
        );
        let overridden = mcp_http_options(
            &config,
            Some(socket),
            vec!["override.example".into()],
            vec!["https://override.example".into()],
            Some("https://override.example/mcp".into()),
        );
        assert_eq!(overridden.listen, socket);
        assert_eq!(overridden.allowed_hosts, ["override.example"]);
        assert_eq!(overridden.allowed_origins, ["https://override.example"]);
        assert_eq!(
            overridden.public_url.as_deref(),
            Some("https://override.example/mcp")
        );
    }
}
