//! Resume-command templates shared by the TUI and the CLI.
//!
//! A resume command is a shell template configured per source (for example
//! `claude_resume_cmd`) or derived from built-in defaults when the agent
//! binary is on PATH. Templates expand `{session_id}`, `{project}`,
//! `{source}`, `{source_path}`, `{source_dir}`, `{cwd}`, the shell-quoted
//! `*_shell` variants, and `{claude_env}`.

use crate::config::UserConfig;
use crate::types::SourceKind;
use std::path::{Path, PathBuf};

/// Everything a template can reference about a session.
pub struct ResumeSession<'a> {
    pub source: SourceKind,
    pub session_id: &'a str,
    pub project: &'a str,
    pub source_path: &'a str,
    pub source_dir: &'a str,
    /// For `{claude_env}`: the session's Claude config directory when Claude does not use it
    /// by default (see `claude_config_dir`).
    pub claude_config_dir: Option<PathBuf>,
}

/// True for directories inside a supported source's transcript store. A resume
/// command must never `cd` into one: CLIs such as antigravity would ask the
/// user to trust an agent's own state store as a project workspace.
pub fn is_state_store_dir(dir: &str, claude_sources: &[PathBuf]) -> bool {
    crate::sources::is_state_store_dir(Path::new(dir), claude_sources)
}

/// Last-resort cwd for a resume command: the transcript's own directory when
/// it could plausibly be a workspace, otherwise the user's home directory.
pub fn fallback_resume_cwd(source_dir: &str, claude_sources: &[PathBuf]) -> String {
    if !source_dir.is_empty() && !is_state_store_dir(source_dir, claude_sources) {
        source_dir.to_string()
    } else {
        crate::sources::common::home()
            .to_string_lossy()
            .into_owned()
    }
}

/// Select a resume directory using the environment of the machine that owns
/// the session. Remote callers must use its result without applying local roots.
pub fn resume_cwd(cwd: Option<String>, source_dir: &str, claude_sources: &[PathBuf]) -> String {
    cwd.filter(|dir| !dir.is_empty() && !is_state_store_dir(dir, claude_sources))
        .unwrap_or_else(|| fallback_resume_cwd(source_dir, claude_sources))
}

/// The configured template for a source, falling back to built-in defaults.
/// `remote` skips the local PATH probe for sessions resumed over SSH.
pub fn resume_template(config: &UserConfig, source: SourceKind, remote: bool) -> Option<String> {
    let configured = match source {
        SourceKind::Claude => config.claude_resume_cmd.clone(),
        SourceKind::Codex => config.codex_resume_cmd.clone(),
        SourceKind::Opencode => config.opencode_resume_cmd.clone(),
        SourceKind::Cursor => config.cursor_resume_cmd.clone(),
        SourceKind::Pi => config.pi_resume_cmd.clone(),
        SourceKind::Omp => config.omp_resume_cmd.clone(),
        SourceKind::OpenClaw => return None,
        SourceKind::Copilot => config.copilot_resume_cmd.clone(),
        SourceKind::Grok => config.grok_resume_cmd.clone(),
        SourceKind::Hermes => None,
        SourceKind::Jcode => config.jcode_resume_cmd.clone(),
        SourceKind::Muse => config.muse_resume_cmd.clone(),
        SourceKind::Antigravity => config.antigravity_resume_cmd.clone(),
        SourceKind::Bob => config.bob_resume_cmd.clone(),
        // ZCode sessions resume in the desktop app, not a CLI.
        SourceKind::Zcode => None,
        SourceKind::Kiro => None,
        // KiloCode sessions resume with `kilo --session <id>` in a project directory.
        SourceKind::Kilocode => config.kilocode_resume_cmd.clone(),
    };
    configured.or_else(|| default_resume_template(source.label(), remote))
}

pub fn default_resume_template(cmd: &str, remote: bool) -> Option<String> {
    match cmd {
        "claude" if remote || find_in_path("claude").is_some() => {
            Some("cd {cwd_shell} && {claude_env}claude --resume {session_id}".to_string())
        }
        "codex" if remote || find_in_path("codex").is_some() => {
            Some("codex resume {session_id}".to_string())
        }
        "opencode" if remote || find_in_path("opencode").is_some() => {
            Some("opencode --session {session_id}".to_string())
        }
        "cursor" => (remote || find_in_path("cursor-agent").is_some())
            .then(|| "cursor-agent --resume {session_id}".to_string()),
        "pi" if remote || find_in_path("pi").is_some() => {
            Some("pi --session {source_path_shell}".to_string())
        }
        "omp" if remote || find_in_path("omp").is_some() => {
            Some("omp --resume {source_path_shell}".to_string())
        }
        "copilot" if remote || find_in_path("copilot").is_some() => {
            Some("copilot --resume {session_id}".to_string())
        }
        "jcode" if remote || find_in_path("jcode").is_some() => {
            Some("cd {cwd_shell} && jcode --resume {session_id}".to_string())
        }
        "muse" if remote || find_in_path("muse").is_some() => {
            Some("cd {cwd_shell} && muse resume {session_id}".to_string())
        }
        "grok" if remote || find_in_path("grok").is_some() => {
            Some("cd {cwd_shell} && grok --resume {session_id}".to_string())
        }
        "antigravity" if remote || find_in_path("agy").is_some() => {
            Some("cd {cwd_shell} && agy --conversation {session_id}".to_string())
        }
        "antigravity" if find_in_path("antigravity").is_some() => {
            Some("cd {cwd_shell} && antigravity --conversation {session_id}".to_string())
        }
        "bob" if remote || find_in_path("bob").is_some() => {
            Some("cd {cwd_shell} && bob --resume {session_id}".to_string())
        }
        "kilocode" if remote || find_in_path("kilo").is_some() => {
            Some("cd {cwd_shell} && kilo --session {session_id}".to_string())
        }
        _ => None,
    }
}

pub fn expand_resume_template(template: &str, session: &ResumeSession, cwd: &str) -> String {
    template
        .replace("{claude_env}", &claude_env(session))
        .replace("{session_id}", session.session_id)
        .replace("{project}", session.project)
        .replace("{source}", session.source.label())
        .replace("{source_path_shell}", &shell_quote(session.source_path))
        .replace("{source_path}", session.source_path)
        .replace("{source_dir_shell}", &shell_quote(session.source_dir))
        .replace("{source_dir}", session.source_dir)
        .replace("{cwd_shell}", &shell_quote(cwd))
        .replace("{cwd}", cwd)
}

/// The config directory a local Claude session resumes in: the parent of the configured
/// `projects` root that holds its transcript. `None` for Claude's default directories, which
/// need no `CLAUDE_CONFIG_DIR` (setting it also moves `.claude.json`), and for roots that are
/// not a config directory's `projects`.
pub fn claude_config_dir(
    source: SourceKind,
    source_path: &str,
    claude_sources: &[PathBuf],
) -> Option<PathBuf> {
    if source != SourceKind::Claude {
        return None;
    }
    let path = Path::new(source_path);
    let config_dir = claude_sources
        .iter()
        .filter(|root| root.file_name().is_some_and(|name| name == "projects"))
        .find(|root| path.starts_with(root))?
        .parent()?;
    let home = crate::sources::common::home();
    (config_dir != home.join(".claude") && config_dir != home.join(".config").join("claude"))
        .then(|| config_dir.to_path_buf())
}

/// `CLAUDE_CONFIG_DIR='<dir>' ` when the session names a config directory, so
/// `claude --resume` looks there; empty otherwise.
fn claude_env(session: &ResumeSession) -> String {
    session
        .claude_config_dir
        .as_ref()
        .map(|dir| format!("CLAUDE_CONFIG_DIR={} ", shell_quote(&dir.to_string_lossy())))
        .unwrap_or_default()
}

pub fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

pub fn find_in_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> ResumeSession<'static> {
        ResumeSession {
            source: SourceKind::Claude,
            session_id: "abc-123",
            project: "memex",
            source_path: "/logs/it's.jsonl",
            source_dir: "/logs",
            claude_config_dir: None,
        }
    }

    #[test]
    fn expands_all_placeholders() {
        let out = expand_resume_template(
            "{source} {session_id} {project} {source_path} {source_dir} {cwd}",
            &session(),
            "/work",
        );
        assert_eq!(out, "claude abc-123 memex /logs/it's.jsonl /logs /work");
    }

    #[test]
    fn claude_sessions_from_a_configured_profile_resume_there() {
        let _guard = crate::test_support::env_lock();
        let _env = crate::test_support::EnvVarGuard::set(&[
            ("HOME", Some("/home/u")),
            ("CLAUDE_CONFIG_DIR", Some("/home/u/.claude-profiles/other")),
        ]);
        let roots = [
            PathBuf::from("/home/u/.claude/projects"),
            PathBuf::from("/home/u/.claude-profiles/work/projects"),
            PathBuf::from("/data/claude-archive"),
        ];
        let config_dir = |source, path| claude_config_dir(source, path, &roots);
        assert_eq!(
            config_dir(
                SourceKind::Claude,
                "/home/u/.claude-profiles/work/projects/-repo/abc.jsonl"
            ),
            Some(PathBuf::from("/home/u/.claude-profiles/work"))
        );
        // Claude's default directory needs no variable, whatever the environment selects.
        assert_eq!(
            config_dir(
                SourceKind::Claude,
                "/home/u/.claude/projects/-repo/abc.jsonl"
            ),
            None
        );
        // A root that is not a config directory's `projects` names no config directory.
        assert_eq!(
            config_dir(SourceKind::Claude, "/data/claude-archive/-repo/abc.jsonl"),
            None
        );
        assert_eq!(
            config_dir(
                SourceKind::Codex,
                "/home/u/.claude-profiles/work/projects/-repo/abc.jsonl"
            ),
            None
        );

        let template = default_resume_template("claude", true).unwrap();
        let resume = |claude_config_dir| {
            expand_resume_template(
                &template,
                &ResumeSession {
                    source: SourceKind::Claude,
                    session_id: "abc",
                    project: "memex",
                    source_path: "/unused",
                    source_dir: "/unused",
                    claude_config_dir,
                },
                "/repo",
            )
        };
        assert_eq!(
            resume(Some(PathBuf::from("/home/u/.claude-profiles/work"))),
            "cd '/repo' && CLAUDE_CONFIG_DIR='/home/u/.claude-profiles/work' claude --resume abc"
        );
        assert_eq!(resume(None), "cd '/repo' && claude --resume abc");
    }

    #[test]
    fn shell_variants_are_quoted() {
        let out = expand_resume_template("{cwd_shell} {source_path_shell}", &session(), "/wo rk");
        assert_eq!(out, "'/wo rk' '/logs/it'\\''s.jsonl'");
    }

    #[test]
    fn shell_quote_handles_empty_and_quotes() {
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn fallback_resume_cwd_never_enters_state_stores() {
        let temp = tempfile::tempdir().unwrap();
        let _guard = crate::test_support::env_lock();
        let _env = crate::test_support::pin_source_roots(temp.path());
        let home = crate::sources::common::home()
            .to_string_lossy()
            .into_owned();
        let brain_logs = temp
            .path()
            .join("ANTIGRAVITY_HOME/antigravity-cli/brain/id/.system_generated/logs")
            .to_string_lossy()
            .into_owned();
        let workspace = temp.path().join("my-repo").to_string_lossy().into_owned();
        assert!(is_state_store_dir(&brain_logs, &[]));
        assert!(!is_state_store_dir(&workspace, &[]));
        // A plausible transcript directory is kept as the last resort; agent
        // state stores (and empty paths) fall back to the user's home.
        assert_eq!(fallback_resume_cwd(&workspace, &[]), workspace);
        assert_eq!(fallback_resume_cwd(&brain_logs, &[]), home);
        assert_eq!(fallback_resume_cwd("", &[]), home);
        // Configured Claude roots are state stores too.
        assert_eq!(
            fallback_resume_cwd(&workspace, &[temp.path().join("my-repo")]),
            home
        );
    }

    #[test]
    fn remote_omp_default_resumes_the_session_file() {
        assert_eq!(
            default_resume_template("omp", true).as_deref(),
            Some("omp --resume {source_path_shell}")
        );
    }

    #[test]
    fn remote_opencode_default_uses_session_flag() {
        assert_eq!(
            default_resume_template("opencode", true).as_deref(),
            Some("opencode --session {session_id}")
        );
    }

    #[test]
    fn remote_antigravity_default_prefers_agy() {
        assert_eq!(
            default_resume_template("antigravity", true).as_deref(),
            Some("cd {cwd_shell} && agy --conversation {session_id}")
        );
    }
}
