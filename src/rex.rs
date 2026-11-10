//! Rex integration.
//!
//! When memex runs in a Rex terminal (the Rex module ships in this repo as
//! `rex/memex.lua`; `memex rex install` puts it beside Rex's `init.lua`),
//! resuming a session opens a new Rex tab or split at the session's cwd
//! instead of suspending the TUI. Rex sets `REX_SESSION` and `REX_BLOCK` in
//! every terminal it opens; everything here goes through the `rex` CLI so
//! memex needs no knowledge of Rex's socket protocol.

use crate::config::UserConfig;
use crate::resume::find_in_path;
use anyhow::{Context, Result, anyhow};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;

/// The Rex Lua module, written beside `init.lua` by `memex rex install`.
pub const MODULE_SOURCE: &str = include_str!("../rex/memex.lua");

/// The first line of a module that `memex rex install` wrote, so a reinstall
/// replaces its own file and leaves a hand-written `memex.lua` alone.
pub const MODULE_MARKER: &str = "-- Installed by `memex rex install`";

const TERMINAL_CREATOR: &str = "com.superlogical.terminal";
const SHELL_FLAVOR: &str = "com.superlogical.terminal.shell";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResumePlacement {
    Tab,
    Split,
    Off,
}

/// The Rex session this process runs in, when it runs in a Rex terminal.
pub fn current_session() -> Option<String> {
    std::env::var("REX_SESSION")
        .ok()
        .filter(|value| !value.is_empty())
}

/// The Rex block (terminal pane) this process runs in.
pub fn current_block() -> Option<String> {
    std::env::var("REX_BLOCK")
        .ok()
        .filter(|value| !value.is_empty())
}

/// The rex binary: the one beside the server running this terminal
/// (`REX_BIN_DIR`, which Rex sets) when present, otherwise `rex` on PATH.
pub fn rex_bin() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("REX_BIN_DIR") {
        let path = PathBuf::from(dir).join("rex");
        if path.exists() {
            return Some(path);
        }
    }
    find_in_path("rex")
}

/// Where resume opens sessions inside Rex: `rex_resume` config key, one of
/// "tab" (default), "split", or "off" (suspend the TUI as usual).
pub fn resume_placement(config: &UserConfig) -> ResumePlacement {
    match config.rex_resume.as_deref() {
        Some("split") => ResumePlacement::Split,
        Some("off") => ResumePlacement::Off,
        _ => ResumePlacement::Tab,
    }
}

/// The block in `rex_session` whose processes are running `session_id`, if
/// any. Rex reports each terminal's process tree root, so this matches the
/// command lines of every process under it. The calling process and its
/// ancestors never match, so `memex rex resume <id>` does not find itself.
pub fn block_running_session(rex_session: &str, session_id: &str) -> Option<String> {
    let bin = rex_bin()?;
    let mut cmd = Command::new(&bin);
    cmd.args(["block", "ls", "-s", rex_session, "--json"]);
    let blocks = placed_terminal_blocks(&run_json(cmd).ok()?);
    if blocks.is_empty() {
        return None;
    }
    let table = process_table().ok()?;
    let excluded = ancestors(&table, std::process::id());
    blocks.into_iter().find(|block_id| {
        let mut cmd = Command::new(&bin);
        cmd.args([
            "block",
            "call",
            "-s",
            rex_session,
            "-b",
            block_id,
            "process",
        ]);
        run_json(cmd).ok().is_some_and(|process| {
            block_root_pid(&process)
                .is_some_and(|root| subtree_runs(&table, root, session_id, &excluded))
        })
    })
}

/// Focus a block, making its tab the active one.
pub fn focus_block(rex_session: &str, block_id: &str) -> Result<()> {
    let bin = rex_bin().ok_or_else(|| anyhow!("rex binary not found"))?;
    let output = Command::new(bin)
        .args(["focus", "-s", rex_session, block_id])
        .output()
        .context("rex focus failed")?;
    if !output.status.success() {
        return Err(anyhow!(
            "rex focus failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(())
}

/// Open a new Rex tab, or a split beside `anchor_block`, in `rex_session` and
/// type `command` into its shell, so the shell stays open when the agent
/// exits. `label` names a new tab. Returns the new block id. `cwd` should be
/// `None` for remote sessions (the directory only exists on the remote
/// machine).
pub fn open_resume_block(
    placement: ResumePlacement,
    rex_session: &str,
    anchor_block: Option<&str>,
    cwd: Option<&str>,
    label: &str,
    command: &str,
) -> Result<String> {
    let (method, args) = resume_request(placement, anchor_block, cwd, label, command)?;
    let bin = rex_bin().ok_or_else(|| anyhow!("rex binary not found"))?;
    let mut cmd = Command::new(bin);
    cmd.args(["api", "call", "-s", rex_session, method, &args.to_string()]);
    let value = run_json(cmd).with_context(|| format!("rex {method} failed"))?;
    value
        .pointer("/block_ids/0")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| anyhow!("rex {method} returned no block id"))
}

/// The server method and arguments that open a resumed session's block.
fn resume_request(
    placement: ResumePlacement,
    anchor_block: Option<&str>,
    cwd: Option<&str>,
    label: &str,
    command: &str,
) -> Result<(&'static str, serde_json::Value)> {
    let mut options = serde_json::json!({ "initial_input": format!("{command}\r") });
    if let Some(cwd) = cwd {
        options["cwd"] = cwd.into();
    }
    // Only the tab is labeled: the Rex module finds its own panes by the label "memex", which
    // must not match an agent resumed from a project of that name.
    let layout = serde_json::json!({
        "block": { "flavor": SHELL_FLAVOR, "options": options }
    });
    match placement {
        ResumePlacement::Off => Err(anyhow!("rex resume is disabled")),
        ResumePlacement::Tab => Ok((
            "session.new_window",
            serde_json::json!({ "window_label": label, "focus": true, "layout": layout }),
        )),
        ResumePlacement::Split => {
            let mut args = serde_json::json!({
                "direction": "horizontal",
                "side": "after",
                "focus": true,
                "layout": layout,
            });
            if let Some(anchor) = anchor_block {
                args["anchor_id"] = anchor.into();
            }
            Ok(("session.new_split", args))
        }
    }
}

/// The directory `memex rex install` writes the module to: beside the
/// `init.lua` Rex reads (`REX_CONFIG`, else `$XDG_CONFIG_HOME/rex`, else
/// `~/.config/rex`).
pub fn config_dir() -> Option<PathBuf> {
    if let Some(config) = std::env::var_os("REX_CONFIG").filter(|value| !value.is_empty()) {
        return PathBuf::from(config).parent().map(Path::to_path_buf);
    }
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME").filter(|value| !value.is_empty()) {
        return Some(PathBuf::from(xdg).join("rex"));
    }
    directories::BaseDirs::new().map(|base| base.home_dir().join(".config").join("rex"))
}

/// The module as installed: the repo copy with the install marker and the
/// memex version that wrote it.
pub fn module_contents() -> String {
    format!(
        "{MODULE_MARKER} (memex {}); rerun it after upgrading memex.\n{MODULE_SOURCE}",
        env!("CARGO_PKG_VERSION")
    )
}

fn placed_terminal_blocks(value: &serde_json::Value) -> Vec<String> {
    value
        .get("blocks")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|block| {
            block
                .get("creator_name")
                .and_then(serde_json::Value::as_str)
                == Some(TERMINAL_CREATOR)
                && block.get("placement").and_then(serde_json::Value::as_str) == Some("placed")
        })
        .filter_map(|block| block.get("block_id")?.as_str().map(str::to_string))
        .collect()
}

/// The pid at the root of a terminal's process tree: its child, else the
/// foreground process.
fn block_root_pid(process: &serde_json::Value) -> Option<u32> {
    ["/child/pid", "/foreground/pid"]
        .iter()
        .find_map(|pointer| process.pointer(pointer)?.as_u64())
        .and_then(|pid| u32::try_from(pid).ok())
}

struct ProcessEntry {
    parent: u32,
    command: String,
}

type ProcessTable = HashMap<u32, ProcessEntry>;

fn process_table() -> Result<ProcessTable> {
    let output = Command::new("ps")
        .args(["-axww", "-o", "pid=,ppid=,command="])
        .output()
        .context("ps failed")?;
    if !output.status.success() {
        return Err(anyhow!("ps exited with {}", output.status));
    }
    Ok(parse_process_table(&String::from_utf8_lossy(
        &output.stdout,
    )))
}

fn parse_process_table(text: &str) -> ProcessTable {
    text.lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.parse().ok()?;
            let parent = fields.next()?.parse().ok()?;
            let command = fields.collect::<Vec<_>>().join(" ");
            Some((pid, ProcessEntry { parent, command }))
        })
        .collect()
}

fn ancestors(table: &ProcessTable, pid: u32) -> HashSet<u32> {
    let mut seen = HashSet::new();
    let mut current = pid;
    while current != 0 && seen.insert(current) {
        match table.get(&current) {
            Some(entry) => current = entry.parent,
            None => break,
        }
    }
    seen
}

/// True when `root` or any process under it, other than `excluded` ones, has
/// `session_id` in its command line.
fn subtree_runs(
    table: &ProcessTable,
    root: u32,
    session_id: &str,
    excluded: &HashSet<u32>,
) -> bool {
    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    for (pid, entry) in table {
        children.entry(entry.parent).or_default().push(*pid);
    }
    let mut stack = vec![root];
    let mut visited = HashSet::new();
    while let Some(pid) = stack.pop() {
        if !visited.insert(pid) {
            continue;
        }
        if !excluded.contains(&pid)
            && table
                .get(&pid)
                .is_some_and(|entry| entry.command.contains(session_id))
        {
            return true;
        }
        if let Some(kids) = children.get(&pid) {
            stack.extend(kids);
        }
    }
    false
}

fn run_json(mut cmd: Command) -> Result<serde_json::Value> {
    let output = cmd.output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("{}", stderr.trim().trim_start_matches("Error: ")));
    }
    serde_json::from_slice(&output.stdout).context("unparseable rex response")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_parses_config_values() {
        let mut config = UserConfig::default();
        assert_eq!(resume_placement(&config), ResumePlacement::Tab);
        config.rex_resume = Some("split".to_string());
        assert_eq!(resume_placement(&config), ResumePlacement::Split);
        config.rex_resume = Some("off".to_string());
        assert_eq!(resume_placement(&config), ResumePlacement::Off);
        config.rex_resume = Some("bogus".to_string());
        assert_eq!(resume_placement(&config), ResumePlacement::Tab);
    }

    #[test]
    fn only_placed_terminal_blocks_are_candidates() {
        let value = serde_json::json!({"blocks": [
            {"block_id": "block:a", "creator_name": "com.superlogical.terminal", "placement": "placed"},
            {"block_id": "block:b", "creator_name": "com.superlogical.terminal", "placement": "detached"},
            {"block_id": "block:c", "creator_name": "com.superlogical.onboarding", "placement": "placed"},
        ]});
        assert_eq!(placed_terminal_blocks(&value), vec!["block:a".to_string()]);
        assert!(placed_terminal_blocks(&serde_json::json!({})).is_empty());
    }

    #[test]
    fn block_root_prefers_the_terminal_child() {
        let process = serde_json::json!({
            "child": {"pid": 10, "name": "login"},
            "foreground": {"pid": 12, "name": "claude"},
        });
        assert_eq!(block_root_pid(&process), Some(10));
        let foreground_only = serde_json::json!({"foreground": {"pid": 12}});
        assert_eq!(block_root_pid(&foreground_only), Some(12));
        assert_eq!(block_root_pid(&serde_json::json!({"last_exit": 0})), None);
    }

    #[test]
    fn finds_a_session_anywhere_under_the_block() {
        let table = parse_process_table(
            "    1     0 /sbin/launchd\n\
               10     1 login -pf nico\n\
               11    10 -zsh\n\
               12    11 node /usr/local/bin/claude-wrapper\n\
               13    12 claude --resume abc-123\n\
               20     1 login -pf nico\n\
               21    20 -zsh\n",
        );
        let none = HashSet::new();
        assert!(subtree_runs(&table, 10, "abc-123", &none));
        assert!(!subtree_runs(&table, 20, "abc-123", &none));
        assert!(!subtree_runs(&table, 10, "zzz-999", &none));
        assert!(!subtree_runs(&table, 99, "abc-123", &none));
    }

    #[test]
    fn the_caller_and_its_ancestors_never_match() {
        let table = parse_process_table(
            "10 1 login -pf nico\n\
             11 10 -zsh\n\
             12 11 memex rex resume abc-123\n",
        );
        let excluded = ancestors(&table, 12);
        assert_eq!(excluded, HashSet::from([12, 11, 10, 1]));
        assert!(!subtree_runs(&table, 10, "abc-123", &excluded));
    }

    #[test]
    fn resume_opens_a_tab_or_split_typing_the_command() {
        let (method, args) = resume_request(
            ResumePlacement::Tab,
            Some("block:a"),
            Some("/repo"),
            "memex",
            "claude --resume abc",
        )
        .unwrap();
        assert_eq!(method, "session.new_window");
        assert_eq!(args["window_label"], "memex");
        assert_eq!(args["focus"], true);
        assert!(args.get("anchor_id").is_none());
        assert!(args["layout"]["block"].get("label").is_none());
        let options = &args["layout"]["block"]["options"];
        assert_eq!(options["cwd"], "/repo");
        assert_eq!(options["initial_input"], "claude --resume abc\r");

        let (method, args) = resume_request(
            ResumePlacement::Split,
            Some("block:a"),
            None,
            "memex",
            "ssh box 'codex resume x'",
        )
        .unwrap();
        assert_eq!(method, "session.new_split");
        assert_eq!(args["anchor_id"], "block:a");
        assert_eq!(args["side"], "after");
        assert!(args["layout"]["block"]["options"].get("cwd").is_none());

        assert!(resume_request(ResumePlacement::Off, None, None, "memex", "x").is_err());
    }

    #[test]
    fn installed_module_carries_the_marker_and_version() {
        let contents = module_contents();
        assert!(contents.starts_with(MODULE_MARKER));
        assert!(contents.contains(env!("CARGO_PKG_VERSION")));
        assert!(contents.ends_with(MODULE_SOURCE));
    }
}
