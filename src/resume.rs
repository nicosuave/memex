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
        SourceKind::Grok => config.grok_resume_cmd.clone(),
        SourceKind::Hermes => None,
        SourceKind::Jcode => config.jcode_resume_cmd.clone(),
        SourceKind::Muse => config.muse_resume_cmd.clone(),
        SourceKind::Antigravity => config.antigravity_resume_cmd.clone(),
        SourceKind::Bob => config.bob_resume_cmd.clone(),
        // ZCode sessions resume in the desktop app, not a CLI.
        SourceKind::Zcode => None,
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
        _ => None,
    }
}

/// Returns true if `path` points into an agent's internal transcript storage
/// directory (such as `~/.local/share/muse/sessions`, `~/.claude/projects`, etc.)
/// rather than a user project repository/working directory.
pub fn is_internal_storage_dir(path: &str) -> bool {
    let p = path.trim();
    if p.is_empty() {
        return false;
    }
    let normalized = p.replace('\\', "/");
    let trimmed = normalized.trim_end_matches('/');

    // Explicitly allow worktrees such as `~/.codex/worktrees/...`
    if trimmed.contains("/.codex/worktrees/")
        || trimmed.contains(".codex/worktrees")
        || trimmed.ends_with("/.codex/worktrees")
    {
        return false;
    }

    let lower = trimmed.to_ascii_lowercase();

    let matches_token = |token: &str| -> bool {
        lower == token
            || lower.starts_with(&format!("{token}/"))
            || lower.contains(&format!("/{token}/"))
            || lower.ends_with(&format!("/{token}"))
    };

    if matches_token(".local/share/muse/sessions")
        || matches_token("muse/sessions")
        || matches_token(".grok/sessions")
        || matches_token(".claude/projects")
        || matches_token(".config/claude")
        || matches_token(".codex/sessions")
        || matches_token(".codex/archived_sessions")
        || matches_token(".pi/agent/sessions")
        || matches_token(".pi/sessions")
        || matches_token(".jcode/sessions")
        || matches_token(".local/share/opencode")
        || matches_token(".config/opencode")
        || matches_token(".config/github-copilot")
        || matches_token(".openclaw/sessions")
        || matches_token(".omp/sessions")
        || matches_token(".hermes/sessions")
        || matches_token(".cursor/sessions")
        || matches_token("antigravity-cli/brain")
        || matches_token("antigravity/brain")
        || matches_token("antigravity-ide/brain")
    {
        return true;
    }

    // Also check canonicalized path if path exists on disk (resolves symlinks and `.` / `..`)
    if let Ok(canon) = std::fs::canonicalize(p) {
        let canon_str = canon.to_string_lossy().replace('\\', "/");
        let canon_trimmed = canon_str.trim_end_matches('/');
        if canon_trimmed.contains("/.codex/worktrees/")
            || canon_trimmed.contains(".codex/worktrees")
        {
            return false;
        }
        let canon_lower = canon_trimmed.to_ascii_lowercase();
        let canon_matches = |token: &str| -> bool {
            canon_lower == token
                || canon_lower.starts_with(&format!("{token}/"))
                || canon_lower.contains(&format!("/{token}/"))
                || canon_lower.ends_with(&format!("/{token}"))
        };
        if canon_matches(".local/share/muse/sessions")
            || canon_matches("muse/sessions")
            || canon_matches(".grok/sessions")
            || canon_matches(".claude/projects")
            || canon_matches(".config/claude")
            || canon_matches(".codex/sessions")
            || canon_matches(".codex/archived_sessions")
            || canon_matches(".pi/agent/sessions")
            || canon_matches(".pi/sessions")
            || canon_matches(".jcode/sessions")
            || canon_matches(".local/share/opencode")
            || canon_matches(".config/opencode")
            || canon_matches(".config/github-copilot")
            || canon_matches(".openclaw/sessions")
            || canon_matches(".omp/sessions")
            || canon_matches(".hermes/sessions")
            || canon_matches(".cursor/sessions")
            || canon_matches("antigravity-cli/brain")
            || canon_matches("antigravity/brain")
            || canon_matches("antigravity-ide/brain")
        {
            return true;
        }
    }

    false
}

pub fn expand_resume_template(template: &str, session: &ResumeSession, cwd: &str) -> String {
    let effective_cwd = if is_internal_storage_dir(cwd) {
        ""
    } else {
        cwd
    };
    template
        .replace("{session_id}", session.session_id)
        .replace("{project}", session.project)
        .replace("{source}", session.source.label())
        .replace("{source_path_shell}", &shell_quote(session.source_path))
        .replace("{source_path}", session.source_path)
        .replace("{source_dir_shell}", &shell_quote(session.source_dir))
        .replace("{source_dir}", session.source_dir)
        .replace("{cwd_shell}", &shell_quote(effective_cwd))
        .replace("{cwd}", effective_cwd)
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

    #[test]
    fn detects_internal_storage_directories() {
        assert!(is_internal_storage_dir(
            "/Users/joe/.local/share/muse/sessions/2026/08/08/e89a358e-084e-43c7-a68b-6d088a689f0b"
        ));
        assert!(is_internal_storage_dir(
            "~/.claude/projects/-Users-joe-Developer-memex"
        ));
        assert!(is_internal_storage_dir(
            "/home/user/.codex/sessions/2026/01/01"
        ));
        assert!(is_internal_storage_dir(
            "/home/user/.codex/archived_sessions/2026/01/01"
        ));
        assert!(is_internal_storage_dir("/Users/joe/.grok/sessions"));
        assert!(is_internal_storage_dir("/Users/joe/.jcode/sessions"));
        assert!(is_internal_storage_dir(
            "/Users/joe/.gemini/antigravity-cli/brain/487a8c64/.system_generated/logs"
        ));
        assert!(is_internal_storage_dir("/Users/joe/.pi/agent/sessions"));

        // User workspaces and projects must NOT be identified as internal storage
        assert!(!is_internal_storage_dir("/Users/joe/Developer/memex"));
        assert!(!is_internal_storage_dir("/Users/joe/Developer/obento"));
        assert!(!is_internal_storage_dir("/home/user/workspace"));

        // Codex worktrees must NOT be identified as internal storage
        assert!(!is_internal_storage_dir(
            "/Users/joe/.codex/worktrees/24fe/omnigent"
        ));
        assert!(!is_internal_storage_dir(
            "/Users/joe/.codex/worktrees/3cb4/BenchBox"
        ));
    }

    #[test]
    fn template_expansion_preserves_cd_cwd_for_real_workspace() {
        let tmpl = "cd {cwd_shell} && muse resume {session_id}";
        let out = expand_resume_template(tmpl, &session(), "/Users/joe/Developer/obento");
        assert_eq!(
            out,
            "cd '/Users/joe/Developer/obento' && muse resume abc-123"
        );
    }

    #[test]
    fn template_expansion_blanks_cwd_for_internal_storage() {
        let tmpl = "cd {cwd_shell} && agy --conversation {session_id}";
        let out = expand_resume_template(
            tmpl,
            &session(),
            "/Users/joe/.gemini/antigravity-cli/brain/487a8c64/.system_generated/logs",
        );
        assert_eq!(out, "cd '' && agy --conversation abc-123");
    }
}
