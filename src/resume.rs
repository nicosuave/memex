//! Resume-command templates shared by the TUI and the CLI.
//!
//! A resume command is a shell template configured per source (for example
//! `claude_resume_cmd`) or derived from built-in defaults when the agent
//! binary is on PATH. Templates expand `{session_id}`, `{project}`,
//! `{source}`, `{source_path}`, `{source_dir}`, `{cwd}`, and the
//! shell-quoted `*_shell` variants.

use crate::config::UserConfig;
use crate::types::SourceKind;
use std::path::PathBuf;

/// Everything a template can reference about a session.
pub struct ResumeSession<'a> {
    pub source: SourceKind,
    pub session_id: &'a str,
    pub project: &'a str,
    pub source_path: &'a str,
    pub source_dir: &'a str,
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
    };
    configured.or_else(|| default_resume_template(source.label(), remote))
}

pub fn default_resume_template(cmd: &str, remote: bool) -> Option<String> {
    match cmd {
        "claude" if remote || find_in_path("claude").is_some() => {
            Some("cd {cwd_shell} && claude --resume {session_id}".to_string())
        }
        "codex" if remote || find_in_path("codex").is_some() => {
            Some("codex resume {session_id}".to_string())
        }
        "opencode" if remote || find_in_path("opencode").is_some() => {
            Some("opencode resume {session_id}".to_string())
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
        _ => None,
    }
}

pub fn expand_resume_template(template: &str, session: &ResumeSession, cwd: &str) -> String {
    template
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
    fn remote_omp_default_resumes_the_session_file() {
        assert_eq!(
            default_resume_template("omp", true).as_deref(),
            Some("omp --resume {source_path_shell}")
        );
    }
}
