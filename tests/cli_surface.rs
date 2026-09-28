use std::collections::BTreeSet;
use std::path::Path;
use std::process::{Command, Output};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_memex"))
        .args(args)
        .output()
        .unwrap()
}

fn successful_stdout(args: &[&str]) -> String {
    let output = run(args);
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn command_rows(help: &str) -> BTreeSet<String> {
    let mut in_commands = false;
    help.lines()
        .filter_map(|line| {
            if [
                "Commands:",
                "Find and read:",
                "Browse and reuse:",
                "Index and operate:",
                "Integrate and maintain:",
            ]
            .contains(&line)
            {
                in_commands = true;
                return None;
            }
            if !in_commands {
                return None;
            }
            if line.is_empty() {
                in_commands = false;
                return None;
            }
            let row = line.strip_prefix("  ")?;
            row.split_whitespace().next().map(str::to_owned)
        })
        .collect()
}

fn assert_help_has_options(help: &str, visible: &[&str], hidden: &[&str]) {
    let option_rows: Vec<_> = help
        .lines()
        .map(str::trim_start)
        .filter(|line| line.starts_with('-'))
        .collect();
    for option in visible {
        assert!(
            help.contains(option),
            "missing option {option:?} from rows {option_rows:?}"
        );
    }
    for option in hidden {
        assert!(
            !option_rows
                .iter()
                .any(|row| row.split_whitespace().any(|word| {
                    word.trim_end_matches(',') == *option
                        || word
                            .trim_end_matches(',')
                            .starts_with(&format!("{option}="))
                })),
            "found hidden option {option:?} in rows {option_rows:?}"
        );
    }
}

fn assert_directory_empty(path: &Path) {
    assert_eq!(std::fs::read_dir(path).unwrap().count(), 0);
}

#[test]
fn top_level_help_shows_only_the_canonical_command_surface() {
    let rows = command_rows(&successful_stdout(&["--help"]));
    for command in [
        "index", "daemon", "web", "session", "projects", "machines", "debug",
    ] {
        assert!(
            rows.contains(command),
            "missing command row {command:?}: {rows:?}"
        );
    }
    for deprecated in [
        "reindex",
        "index-gc",
        "embed",
        "stats",
        "index-service",
        "service",
        "hydrate",
        "hydrate-batch",
        "eval-retrieval",
        "setup",
    ] {
        assert!(
            !rows.contains(deprecated),
            "deprecated command row {deprecated:?} is visible: {rows:?}"
        );
    }
}

#[test]
fn nested_help_exposes_the_approved_command_groups_without_side_effects() {
    let root = tempfile::tempdir().unwrap();
    let root_arg = root.path().to_str().unwrap();
    let cases: &[&[&str]] = &[
        &["index", "--help"],
        &["index", "rebuild", "--root", root_arg, "--help"],
        &["index", "gc", "--root", root_arg, "--help"],
        &["index", "embed", "--root", root_arg, "--help"],
        &["index", "stats", "--root", root_arg, "--help"],
        &["daemon", "--help"],
        &["daemon", "enable", "--root", root_arg, "--help"],
        &["daemon", "restart", "--root", root_arg, "--help"],
        &["daemon", "status", "--root", root_arg, "--help"],
        &["daemon", "disable", "--root", root_arg, "--help"],
        &["web", "--help"],
        &["web", "serve", "--root", root_arg, "--help"],
        &["web", "open", "--root", root_arg, "--help"],
        &["session", "--help"],
        &["session", "batch", "--root", root_arg, "--help"],
        &["debug", "--help"],
        &["debug", "eval-retrieval", "--root", root_arg, "--help"],
    ];
    for args in cases {
        let help = successful_stdout(args);
        assert!(help.contains("Usage:"), "{args:?}: {help}");
        assert_directory_empty(root.path());
    }
}

#[test]
fn nested_command_rows_match_the_canonical_layout() {
    let cases = [
        (
            vec!["index", "--help"],
            &["rebuild", "gc", "embed", "stats"][..],
        ),
        (
            vec!["daemon", "--help"],
            &["run", "enable", "restart", "status", "disable"][..],
        ),
        (vec!["web", "--help"], &["serve", "open"][..]),
        (vec!["session", "--help"], &["batch"][..]),
        (vec!["debug", "--help"], &["eval-retrieval"][..]),
    ];
    for (args, expected) in cases {
        let rows = command_rows(&successful_stdout(&args));
        for command in expected {
            assert!(
                rows.contains(*command),
                "{args:?} omitted {command:?}: {rows:?}"
            );
        }
        if args[0] == "daemon" {
            assert!(
                !rows.contains("open"),
                "legacy service open is visible: {rows:?}"
            );
        }
    }
}

#[test]
fn search_help_prefers_mode_format_and_pretty() {
    let help = successful_stdout(&["search", "--help"]);
    assert_help_has_options(
        &help,
        &[
            "--mode <MODE>",
            "lexical",
            "semantic",
            "hybrid",
            "--format <FORMAT>",
            "jsonl",
            "json",
            "text",
            "toon",
            "--pretty",
        ],
        &["--semantic", "--hybrid", "--json-array", "--verbose", "-v"],
    );
}

#[test]
fn format_help_is_consistent_and_deprecated_output_flags_are_hidden() {
    for args in [
        vec!["session", "--help"],
        vec!["show", "--help"],
        vec!["context", "--help"],
        vec!["sessions", "--help"],
        vec!["projects", "--help"],
        vec!["machines", "--help"],
        vec!["session", "batch", "--help"],
        vec!["usage", "--help"],
        vec!["index", "stats", "--help"],
    ] {
        let help = successful_stdout(&args);
        assert_help_has_options(
            &help,
            &["--format <FORMAT>", "jsonl", "json", "text", "--pretty"],
            &["--json-array", "--verbose", "-v", "--json"],
        );
    }
}

#[test]
fn stats_text_and_json_preserve_configuration_memory_and_backfill_details() {
    use memex::config::Paths;
    use memex::memory::{MemoryDiscoveryOptions, MemoryStore};
    use memex::state::ScanCache;
    use memex::types::SourceKind;
    use std::collections::HashSet;

    let temp = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(temp.path().join("data"))).unwrap();
    paths.ensure_dirs().unwrap();
    std::fs::write(
        paths.root.join("config.toml"),
        "auto_index_on_search = false\nembeddings = true\nmodel = \"minilm\"\nexecution_provider = \"cpu\"\ncompute_units = \"cpu\"\nscan_cache_ttl = 0\n",
    ).unwrap();
    ScanCache {
        last_scan_ts: 1,
        file_count: 3,
        total_bytes: 1234,
    }
    .save(&paths.state.join("scan_cache.json"))
    .unwrap();

    let projects = temp.path().join("claude/projects");
    let memory_path = projects.join("-work-memex/memory/MEMORY.md");
    std::fs::create_dir_all(memory_path.parent().unwrap()).unwrap();
    std::fs::write(&memory_path, "# Notes\n\nPersisted memory for stats.\n").unwrap();
    let memory_store = MemoryStore::new(paths.root.join("memory/documents.json"));
    memory_store
        .refresh(&MemoryDiscoveryOptions {
            claude_project_roots: vec![projects],
            codex_homes: vec![],
            enabled_sources: HashSet::from([SourceKind::Claude]),
            exclude_patterns: vec![],
        })
        .unwrap();
    let memory = memory_store.load().unwrap();
    let sections: usize = memory
        .documents
        .iter()
        .map(|document| document.sections.len())
        .sum();
    assert_eq!(memory.documents.len(), 1);
    assert!(sections > 0);

    rusqlite::Connection::open(paths.state.join("embed-backfill.sqlite3")).unwrap()
        .execute_batch(
            "create table backfill_meta (
                id integer primary key, model text, dimensions integer, total integer,
                base_completed integer, started_at_ms integer, active_ms integer,
                updated_at_ms integer, phase text, pid integer
            );
            insert into backfill_meta values (1, 'minilm', 384, 5, 2, 1000, 0, 1000, 'embedding', 0);",
        ).unwrap();

    let root = paths.root.to_str().unwrap();
    for command in [vec!["index", "stats"], vec!["stats"]] {
        for format in [
            vec!["--format", "json"],
            vec!["--format", "json", "--pretty"],
            vec!["--json"],
        ] {
            let mut args = command.clone();
            args.extend(format);
            args.extend(["--root", root, "--no-update-check"]);
            let report: serde_json::Value =
                serde_json::from_str(&successful_stdout(&args)).unwrap();
            assert_eq!(report["documents"], 0);
            assert_eq!(report["auto_index_on_search"], false);
            assert_eq!(report["embeddings"], true);
            assert_eq!(report["model"], "minilm");
            assert_eq!(report["execution_provider"], "cpu");
            assert_eq!(report["compute_units"], "cpu");
            assert_eq!(report["scan_cache_ttl_seconds"], 0);
            assert_eq!(report["scan_cache"]["last_scan_ts"], 1);
            assert_eq!(report["scan_cache"]["fresh"], false);
            assert_eq!(report["scan_cache"]["file_count"], 3);
            assert_eq!(report["scan_cache"]["total_bytes"], 1234);
            assert_eq!(report["memory"]["documents"], 1);
            assert_eq!(report["memory"]["sections"], sections);
            assert_eq!(report["memory"]["stale_documents"], 0);
            assert_eq!(report["vectors"]["exists"], false);
            assert_eq!(report["vectors"]["count"], 0);
            assert!(report["vectors"]["dimensions"].is_null());
            assert_eq!(report["vector_backfill"]["completed"], 2);
            assert_eq!(report["vector_backfill"]["total"], 5);
            assert_eq!(report["vector_backfill"]["phase"], "embedding");
            assert_eq!(report["vector_backfill"]["running"], false);
            let sources = report["sources"].as_array().unwrap();
            assert!(sources.iter().any(|source| source == "kiro"));
            assert!(sources.iter().any(|source| source == "zcode"));
            assert!(!sources.iter().any(|source| source == "hermes"));
        }
        let mut args = command;
        args.extend(["--root", root, "--no-update-check"]);
        let text = successful_stdout(&args);
        for expected in [
            "documents: 0",
            "vector backfill: 2/5",
            "memory documents: 1",
            "stale memory documents: 0",
            "vectors: none",
            "auto-index-on-search: disabled",
            "scan-cache-ttl: 0s",
            "scanned-files: 3",
            "scanned-bytes: 1234",
            "  model: minilm",
            "  execution-provider: cpu",
            "  - kiro",
        ] {
            assert!(text.contains(expected), "missing {expected:?}: {text}");
        }
    }
}

#[test]
fn stats_without_cache_or_vectors_reports_never_scanned() {
    let root = tempfile::tempdir().unwrap();
    let root_arg = root.path().to_str().unwrap();
    let json = successful_stdout(&[
        "index",
        "stats",
        "--format",
        "jsonl",
        "--root",
        root_arg,
        "--no-update-check",
    ]);
    assert_eq!(json.lines().count(), 1);
    let report: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(report["auto_index_on_search"], true);
    assert_eq!(report["embeddings"], false);
    assert_eq!(report["scan_cache_ttl_seconds"], 3600);
    assert!(report["scan_cache"]["last_scan_ts"].is_null());
    assert!(report["scan_cache"]["age_seconds"].is_null());
    assert_eq!(report["scan_cache"]["fresh"], false);
    assert!(report["vector_backfill"].is_null());
    let text = successful_stdout(&["index", "stats", "--root", root_arg, "--no-update-check"]);
    assert!(text.contains("last-scan: never (stale)"));
    let invalid = run(&[
        "index",
        "stats",
        "--pretty",
        "--root",
        root_arg,
        "--no-update-check",
    ]);
    assert!(!invalid.status.success());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("--pretty requires JSON output"));
}

#[test]
fn index_source_help_uses_positive_repeatable_filters() {
    for args in [
        vec!["index", "--help"],
        vec!["index", "rebuild", "--help"],
        vec!["daemon", "enable", "--help"],
        vec!["daemon", "restart", "--help"],
    ] {
        let help = successful_stdout(&args);
        assert_help_has_options(
            &help,
            &[
                "--claude-path",
                "--only-source",
                "--exclude-source",
                "hermes",
            ],
            &["--source"],
        );
    }
}

#[test]
fn web_open_can_print_a_login_link_without_launching_a_browser() {
    let root = tempfile::tempdir().unwrap();
    let url = successful_stdout(&[
        "--no-update-check",
        "web",
        "open",
        "--print-url",
        "--listen",
        "127.0.0.1:4567",
        "--root",
        root.path().to_str().unwrap(),
    ]);
    assert!(url.trim().starts_with("http://127.0.0.1:4567/#bootstrap="));
    assert_eq!(url.lines().count(), 1);
    assert!(!url.contains("opened"));
}
