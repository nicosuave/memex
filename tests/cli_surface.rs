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
            &["--claude-path", "--only-source", "--exclude-source"],
            &["--source"],
        );
        assert!(
            !help.contains("hermes"),
            "unsupported ingest source shown in {args:?}"
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
