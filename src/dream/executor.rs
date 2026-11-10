//! Runs the model that judges a dream run. Memex holds no API keys: it shells out to an agent
//! CLI the user already has, with the prompt on stdin and the answer on stdout. The agent
//! gets no tools and an empty working directory, so the only thing it can do with the
//! untrusted transcripts it reads is answer.

use crate::config::DreamConfig;
use anyhow::{Context, Result, anyhow, bail};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Set in the executor's environment so wrappers and hooks can recognize a dream run.
pub const DREAM_ENV: &str = "MEMEX_DREAM";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutorSpec {
    Claude {
        model: Option<String>,
    },
    Codex {
        model: Option<String>,
    },
    /// User-supplied argv: prompt on stdin, answer on stdout.
    Custom {
        argv: Vec<String>,
    },
}

impl ExecutorSpec {
    pub fn label(&self) -> String {
        match self {
            ExecutorSpec::Claude { .. } => "claude".to_string(),
            ExecutorSpec::Codex { .. } => "codex".to_string(),
            ExecutorSpec::Custom { argv } => format!(
                "custom:{}",
                Path::new(argv.first().map(String::as_str).unwrap_or_default())
                    .file_name()
                    .map(|name| name.to_string_lossy().to_string())
                    .unwrap_or_default()
            ),
        }
    }

    pub fn model(&self) -> Option<&str> {
        match self {
            ExecutorSpec::Claude { model } | ExecutorSpec::Codex { model } => model.as_deref(),
            ExecutorSpec::Custom { .. } => None,
        }
    }

    /// Choose the executor: an explicit name wins, then a configured custom command, then
    /// the configured name, then whichever of `claude` and `codex` is on `PATH`.
    pub fn resolve(
        config: &DreamConfig,
        executor: Option<&str>,
        model: Option<&str>,
    ) -> Result<Self> {
        let model = model
            .map(str::to_string)
            .or_else(|| config.model.clone())
            .filter(|model| !model.trim().is_empty());
        if executor.is_none() && !config.command.is_empty() {
            return Ok(ExecutorSpec::Custom {
                argv: config.command.clone(),
            });
        }
        match executor.or(config.executor.as_deref()) {
            Some("claude") => Ok(ExecutorSpec::Claude { model }),
            Some("codex") => Ok(ExecutorSpec::Codex { model }),
            Some(other) => Err(anyhow!(
                "unknown dream executor {other:?}; use \"claude\", \"codex\", or set [dream] command"
            )),
            None if crate::cli::find_in_path("claude").is_some() => {
                Ok(ExecutorSpec::Claude { model })
            }
            None if crate::cli::find_in_path("codex").is_some() => {
                Ok(ExecutorSpec::Codex { model })
            }
            None => Err(anyhow!(
                "no dream executor found: install the claude or codex CLI, or set [dream] command in config.toml"
            )),
        }
    }

    /// Program, arguments, and the file the answer is read from when it is not stdout.
    fn command(&self, work_dir: &Path) -> Result<(String, Vec<String>, Option<PathBuf>)> {
        match self {
            ExecutorSpec::Claude { model } => {
                // No tools, no user customizations, and no saved session: the run must not
                // act on what it reads, follow the user's own agent instructions, or leave
                // a transcript for the next dream run to read back.
                let mut args = vec![
                    "--print".to_string(),
                    "--safe-mode".to_string(),
                    "--no-session-persistence".to_string(),
                    "--output-format".to_string(),
                    "text".to_string(),
                    "--tools".to_string(),
                    String::new(),
                ];
                if let Some(model) = model {
                    args.push("--model".to_string());
                    args.push(model.clone());
                }
                Ok(("claude".to_string(), args, None))
            }
            ExecutorSpec::Codex { model } => {
                let answer = work_dir.join("codex-last-message.txt");
                let mut args = vec![
                    "exec".to_string(),
                    "--sandbox".to_string(),
                    "read-only".to_string(),
                    "--ephemeral".to_string(),
                    "--skip-git-repo-check".to_string(),
                    "--cd".to_string(),
                    work_dir.to_string_lossy().to_string(),
                    "--output-last-message".to_string(),
                    answer.to_string_lossy().to_string(),
                ];
                if let Some(model) = model {
                    args.push("--model".to_string());
                    args.push(model.clone());
                }
                // A lone dash reads the prompt from stdin.
                args.push("-".to_string());
                Ok(("codex".to_string(), args, Some(answer)))
            }
            ExecutorSpec::Custom { argv } => {
                let (program, args) = argv
                    .split_first()
                    .ok_or_else(|| anyhow!("[dream] command is empty"))?;
                Ok((program.clone(), args.to_vec(), None))
            }
        }
    }

    /// Run the executor to completion and return its answer text.
    pub fn run(&self, prompt: &str, work_dir: &Path, timeout: Duration) -> Result<String> {
        std::fs::create_dir_all(work_dir)?;
        let (program, args, answer_file) = self.command(work_dir)?;
        if let Some(path) = &answer_file {
            let _ = std::fs::remove_file(path);
        }
        let mut child = Command::new(&program)
            .args(&args)
            .current_dir(work_dir)
            .env(DREAM_ENV, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("start dream executor `{program}`"))?;

        // Feed and drain on threads: a large prompt and a chatty child would otherwise
        // block each other on full pipes.
        let mut stdin = child.stdin.take().expect("piped stdin");
        let prompt_bytes = prompt.as_bytes().to_vec();
        let writer = std::thread::spawn(move || {
            let _ = stdin.write_all(&prompt_bytes);
        });
        let mut stdout = child.stdout.take().expect("piped stdout");
        let stdout_reader = std::thread::spawn(move || {
            let mut buffer = Vec::new();
            let _ = stdout.read_to_end(&mut buffer);
            buffer
        });
        let mut stderr = child.stderr.take().expect("piped stderr");
        let stderr_reader = std::thread::spawn(move || {
            let mut buffer = Vec::new();
            let _ = stderr.read_to_end(&mut buffer);
            buffer
        });

        let started = Instant::now();
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if started.elapsed() >= timeout {
                let _ = child.kill();
                let _ = child.wait();
                bail!(
                    "dream executor `{program}` exceeded its {}s timeout",
                    timeout.as_secs()
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        let _ = writer.join();
        let stdout = String::from_utf8_lossy(&stdout_reader.join().unwrap_or_default()).to_string();
        let stderr = String::from_utf8_lossy(&stderr_reader.join().unwrap_or_default()).to_string();
        if !status.success() {
            let detail = if stderr.trim().is_empty() {
                stdout.trim()
            } else {
                stderr.trim()
            };
            bail!(
                "dream executor `{program}` exited with {status}: {}",
                tail_chars(detail, 600)
            );
        }
        let answer = match answer_file {
            Some(path) => {
                let answer = std::fs::read_to_string(&path)
                    .with_context(|| format!("read executor answer {}", path.display()))?;
                let _ = std::fs::remove_file(&path);
                answer
            }
            None => stdout,
        };
        if answer.trim().is_empty() {
            bail!("dream executor `{program}` returned an empty answer");
        }
        Ok(answer)
    }
}

fn tail_chars(text: &str, max_chars: usize) -> String {
    let count = text.chars().count();
    text.chars().skip(count.saturating_sub(max_chars)).collect()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn custom(script: &str) -> ExecutorSpec {
        ExecutorSpec::Custom {
            argv: vec!["sh".into(), "-c".into(), script.into()],
        }
    }

    #[test]
    fn a_custom_executor_reads_the_prompt_and_answers_on_stdout() -> Result<()> {
        let work = tempfile::tempdir()?;
        let answer = custom("cat; printf ' <- %s' \"$MEMEX_DREAM\"").run(
            "the prompt",
            work.path(),
            Duration::from_secs(10),
        )?;
        assert_eq!(answer, "the prompt <- 1");
        Ok(())
    }

    #[test]
    fn failures_empty_answers_and_timeouts_are_errors() -> Result<()> {
        let work = tempfile::tempdir()?;
        let run = |script: &str, secs: u64| {
            custom(script)
                .run("p", work.path(), Duration::from_millis(secs))
                .unwrap_err()
                .to_string()
        };
        assert!(run("cat >/dev/null; echo boom >&2; exit 3", 10_000).contains("boom"));
        assert!(run("cat >/dev/null", 10_000).contains("empty answer"));
        assert!(run("sleep 30", 300).contains("timeout"));
        Ok(())
    }

    #[test]
    fn a_configured_command_is_used_unless_an_executor_is_named() -> Result<()> {
        let config = DreamConfig {
            command: vec!["my-llm".into(), "--json".into()],
            model: Some("m1".into()),
            ..DreamConfig::default()
        };
        assert_eq!(
            ExecutorSpec::resolve(&config, None, None)?,
            ExecutorSpec::Custom {
                argv: vec!["my-llm".into(), "--json".into()]
            }
        );
        assert_eq!(
            ExecutorSpec::resolve(&config, Some("codex"), Some("m2"))?,
            ExecutorSpec::Codex {
                model: Some("m2".into())
            }
        );
        assert!(ExecutorSpec::resolve(&config, Some("gemini"), None).is_err());
        Ok(())
    }

    #[test]
    fn built_in_executors_run_without_tools_or_saved_sessions() -> Result<()> {
        let work = Path::new("/tmp/dream-work");
        let (program, args, _) = ExecutorSpec::Claude { model: None }.command(work)?;
        assert_eq!(program, "claude");
        let tools = args.iter().position(|arg| arg == "--tools").unwrap();
        assert_eq!(args[tools + 1], "");
        assert!(args.contains(&"--no-session-persistence".to_string()));

        let (program, args, answer) = ExecutorSpec::Codex {
            model: Some("gpt".into()),
        }
        .command(work)?;
        assert_eq!(program, "codex");
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--sandbox", "read-only"])
        );
        assert!(args.contains(&"--ephemeral".to_string()));
        assert_eq!(args.last().map(String::as_str), Some("-"));
        assert_eq!(answer, Some(work.join("codex-last-message.txt")));
        Ok(())
    }
}
