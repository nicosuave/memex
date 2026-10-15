//! Herdr integration.
//!
//! When memex runs inside a herdr-managed pane (the herdr plugin ships in
//! this repo as `herdr-plugin.toml` + `herdr/`), resuming a session opens a
//! new herdr tab or split at the session's cwd instead of suspending the TUI.
//! Herdr injects `HERDR_ENV=1`, `HERDR_BIN_PATH`, and workspace/pane ids into
//! managed panes; everything here goes through the `herdr` CLI so memex needs
//! no socket protocol knowledge.

use crate::config::UserConfig;
use crate::resume::find_in_path;
use anyhow::{Context, Result, anyhow};
use std::path::PathBuf;
use std::process::Command;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResumePlacement {
    Tab,
    Split,
    Off,
}

/// True when running inside a herdr-managed pane.
pub fn inside_herdr() -> bool {
    std::env::var("HERDR_ENV").is_ok_and(|value| value == "1")
}

/// The herdr binary: `HERDR_BIN_PATH` when set (herdr injects it into
/// managed panes), otherwise `herdr` on PATH.
pub fn herdr_bin() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("HERDR_BIN_PATH") {
        let path = PathBuf::from(path);
        if path.exists() {
            return Some(path);
        }
    }
    find_in_path("herdr")
}

/// Where resume opens sessions inside herdr: `herdr_resume` config key,
/// one of "tab" (default), "split", or "off" (suspend the TUI as usual).
pub fn resume_placement(config: &UserConfig) -> ResumePlacement {
    match config.herdr_resume.as_deref() {
        Some("split") => ResumePlacement::Split,
        Some("off") => ResumePlacement::Off,
        _ => ResumePlacement::Tab,
    }
}

/// The herdr pane already running `session_id`, if any. Herdr reports each
/// agent pane's session identity in `herdr pane list`.
pub fn pane_running_session(session_id: &str) -> Option<String> {
    let bin = herdr_bin()?;
    let mut cmd = Command::new(bin);
    cmd.args(["pane", "list"]);
    pane_for_session(&run_json(cmd).ok()?, session_id)
}

fn pane_for_session(value: &serde_json::Value, session_id: &str) -> Option<String> {
    value
        .pointer("/result/panes")?
        .as_array()?
        .iter()
        .find(|pane| {
            pane.pointer("/agent_session/value")
                .and_then(serde_json::Value::as_str)
                == Some(session_id)
        })
        .and_then(|pane| pane.get("pane_id")?.as_str().map(str::to_string))
}

/// Focus an existing herdr pane, switching workspace and tab as needed.
pub fn focus_pane(pane_id: &str) -> Result<()> {
    let bin = herdr_bin().ok_or_else(|| anyhow!("herdr binary not found"))?;
    let mut cmd = Command::new(bin);
    cmd.args(["agent", "focus", pane_id]);
    run_json(cmd).context("herdr agent focus failed")?;
    Ok(())
}

/// Open a new herdr tab or split and run `command` in it. Returns the new
/// pane id. `cwd` should be `None` for remote sessions (the directory only
/// exists on the remote machine).
pub fn open_resume_pane(
    placement: ResumePlacement,
    cwd: Option<&str>,
    label: &str,
    command: &str,
) -> Result<String> {
    let bin = herdr_bin().ok_or_else(|| anyhow!("herdr binary not found"))?;
    let pane_id = match placement {
        ResumePlacement::Off => return Err(anyhow!("herdr resume is disabled")),
        ResumePlacement::Tab => {
            let mut cmd = Command::new(&bin);
            cmd.args(["tab", "create"]);
            if let Ok(workspace) = std::env::var("HERDR_WORKSPACE_ID") {
                cmd.args(["--workspace", &workspace]);
            }
            if let Some(cwd) = cwd {
                cmd.args(["--cwd", cwd]);
            }
            cmd.args(["--label", label, "--focus"]);
            let value = run_json(cmd).context("herdr tab create failed")?;
            pane_id_at(&value, &["result", "root_pane", "pane_id"])
                .ok_or_else(|| anyhow!("herdr tab create returned no root pane id"))?
        }
        ResumePlacement::Split => {
            let mut cmd = Command::new(&bin);
            cmd.args(["pane", "split"]);
            if let Ok(pane) = std::env::var("HERDR_PANE_ID") {
                cmd.args(["--pane", &pane]);
            }
            cmd.args(["--direction", "right"]);
            if let Some(cwd) = cwd {
                cmd.args(["--cwd", cwd]);
            }
            cmd.arg("--focus");
            let value = run_json(cmd).context("herdr pane split failed")?;
            pane_id_at(&value, &["result", "pane", "pane_id"])
                .ok_or_else(|| anyhow!("herdr pane split returned no pane id"))?
        }
    };
    let status = Command::new(&bin)
        .args(["pane", "run", &pane_id, command])
        .output()
        .context("herdr pane run failed")?;
    if !status.status.success() {
        return Err(anyhow!(
            "herdr pane run failed: {}",
            String::from_utf8_lossy(&status.stderr).trim()
        ));
    }
    Ok(pane_id)
}

fn run_json(mut cmd: Command) -> Result<serde_json::Value> {
    let output = cmd.output()?;
    if !output.status.success() {
        return Err(anyhow!(
            "{}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    serde_json::from_slice(&output.stdout).context("unparseable herdr response")
}

fn pane_id_at(value: &serde_json::Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for key in path {
        current = current.get(key)?;
    }
    current.as_str().map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pane_for_session_matches_agent_session_id() {
        let value = serde_json::json!({"result": {"panes": [
            {"pane_id": "w1:p1", "agent_session": {"kind": "id", "value": "aaa"}},
            {"pane_id": "w2:p3", "agent_session": {"kind": "id", "value": "bbb"}},
            {"pane_id": "w2:p4"}
        ]}});
        assert_eq!(pane_for_session(&value, "bbb").as_deref(), Some("w2:p3"));
        assert_eq!(pane_for_session(&value, "ccc"), None);
        assert_eq!(pane_for_session(&serde_json::json!({}), "aaa"), None);
    }

    #[test]
    fn placement_parses_config_values() {
        let mut config = UserConfig::default();
        assert_eq!(resume_placement(&config), ResumePlacement::Tab);
        config.herdr_resume = Some("split".to_string());
        assert_eq!(resume_placement(&config), ResumePlacement::Split);
        config.herdr_resume = Some("off".to_string());
        assert_eq!(resume_placement(&config), ResumePlacement::Off);
        config.herdr_resume = Some("bogus".to_string());
        assert_eq!(resume_placement(&config), ResumePlacement::Tab);
    }

    #[test]
    fn pane_id_extraction() {
        let value: serde_json::Value =
            serde_json::from_str(r#"{"result":{"root_pane":{"pane_id":"w1:p9"}}}"#).unwrap();
        assert_eq!(
            pane_id_at(&value, &["result", "root_pane", "pane_id"]).as_deref(),
            Some("w1:p9")
        );
        assert!(pane_id_at(&value, &["result", "pane", "pane_id"]).is_none());
    }
}
