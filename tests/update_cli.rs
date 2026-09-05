#![cfg(unix)]

use memex::config::Paths;
use memex::index::SearchIndex;
use memex::types::{Record, RecordLinks, SourceKind};
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::FromRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Output, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const AGENT_ENV: &[&str] = &[
    "CI",
    "CODEX_CI",
    "CODEX_THREAD_ID",
    "CLAUDECODE",
    "CLAUDE_CODE_ENTRYPOINT",
];

struct Fixture {
    _temp: tempfile::TempDir,
    home: PathBuf,
    root: PathBuf,
    bin: PathBuf,
    cellar_memex: PathBuf,
    prefix: PathBuf,
    log: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let root = temp.path().join("root");
        let bin = temp.path().join("bin");
        let cellar_memex = temp.path().join("Cellar/memex/0.14.0/bin/memex");
        let prefix = temp.path().join("Cellar/memex/99.0.0");
        let log = temp.path().join("commands.log");
        for path in [
            &home,
            &root,
            &bin,
            cellar_memex.parent().unwrap(),
            &prefix.join("bin"),
        ] {
            std::fs::create_dir_all(path).unwrap();
        }
        std::fs::write(root.join("config.toml"), "auto_index_on_search = false\n").unwrap();
        std::fs::create_dir_all(home.join(".memex")).unwrap();
        std::fs::write(
            home.join(".memex/config.toml"),
            "auto_index_on_search = false\n",
        )
        .unwrap();
        std::fs::copy(env!("CARGO_BIN_EXE_memex"), &cellar_memex).unwrap();
        make_executable(&cellar_memex);

        write_script(
            &bin.join("brew"),
            r#"#!/bin/sh
printf 'brew %s\n' "$*" >> "$MEMEX_TEST_LOG"
if [ "$MEMEX_TEST_FAIL" = "$1" ]; then exit 17; fi
if [ "$1" = "--prefix" ]; then printf '%s\n' "$MEMEX_TEST_PREFIX"; fi
"#,
        );
        write_script(
            &bin.join("curl"),
            r#"#!/bin/sh
printf 'curl %s\n' "$*" >> "$MEMEX_TEST_LOG"
exit 97
"#,
        );
        write_script(
            &prefix.join("bin/memex"),
            r#"#!/bin/sh
printf 'installed %s\n' "$*" >> "$MEMEX_TEST_LOG"
if [ "$1" = "--version" ]; then
  [ "$MEMEX_TEST_FAIL" = "version" ] && exit 18
  printf 'memex 99.0.0\n'
  exit 0
fi
if [ "$MEMEX_TEST_FAIL" = "skill" ]; then exit 19; fi
mkdir -p "$HOME/.agents/skills/memex-search"
printf 'updated by installed binary\n' > "$HOME/.agents/skills/memex-search/SKILL.md"
"#,
        );

        Self {
            _temp: temp,
            home,
            root,
            bin,
            cellar_memex,
            prefix,
            log,
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.cellar_memex);
        let mut path = vec![self.bin.clone()];
        path.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        command
            .env("HOME", &self.home)
            .env("PATH", std::env::join_paths(path).unwrap())
            .env("TERM", "xterm-256color")
            .env("MEMEX_TEST_LOG", &self.log)
            .env("MEMEX_TEST_PREFIX", &self.prefix)
            .env("MEMEX_TEST_FAIL", "");
        for name in AGENT_ENV {
            command.env_remove(name);
        }
        command
    }

    fn cache_update(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        std::fs::write(
            self.home.join(".memex/update-check.json"),
            format!(r#"{{"checked_at":{now},"latest":"99.0.0"}}"#),
        )
        .unwrap();
    }

    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

fn write_script(path: &Path, contents: &str) {
    std::fs::write(path, contents).unwrap();
    make_executable(path);
}

fn make_executable(path: &Path) {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn run(command: &mut Command) -> Output {
    command.output().unwrap()
}

struct PtyOutput {
    status: ExitStatus,
    output: String,
}

fn run_pty(command: &mut Command, input: &[u8]) -> PtyOutput {
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let size = libc::winsize {
        ws_row: 30,
        ws_col: 120,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let result = unsafe {
        libc::openpty(
            &mut master_fd,
            &mut slave_fd,
            std::ptr::null_mut(),
            std::ptr::null(),
            &size,
        )
    };
    assert_eq!(
        result,
        0,
        "openpty failed: {}",
        std::io::Error::last_os_error()
    );
    let mut master = unsafe { File::from_raw_fd(master_fd) };
    let slave = unsafe { File::from_raw_fd(slave_fd) };
    command
        .stdin(Stdio::from(slave.try_clone().unwrap()))
        .stdout(Stdio::from(slave.try_clone().unwrap()))
        .stderr(Stdio::from(slave.try_clone().unwrap()));
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::ioctl(libc::STDIN_FILENO, libc::TIOCSCTTY, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().unwrap();
    drop(slave);
    let mut reader = master.try_clone().unwrap();
    let output = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = reader.read_to_end(&mut bytes);
        String::from_utf8_lossy(&bytes).into_owned()
    });
    master.write_all(input).unwrap();
    master.flush().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("PTY child did not exit within 10 seconds");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    drop(master);
    PtyOutput {
        status,
        output: output.join().unwrap(),
    }
}

#[test]
fn bare_human_startup_accepts_cached_update_before_tui() {
    let fixture = Fixture::new();
    fixture.cache_update();
    let output = run_pty(&mut fixture.command(), b"\r");
    assert!(output.status.success(), "{}", output.output);
    assert!(
        output
            .output
            .contains("Update memex and its installed skills now?")
    );
    assert!(output.output.contains("Update finished. Run `memex` again"));
    assert!(!output.output.contains("\u{1b}[?1049h"));
    assert_eq!(
        fixture.log(),
        "brew update\nbrew upgrade nicosuave/tap/memex\nbrew --prefix nicosuave/tap/memex\ninstalled --version\ninstalled skill update --target all\n"
    );
    assert_eq!(
        std::fs::read_to_string(fixture.home.join(".agents/skills/memex-search/SKILL.md")).unwrap(),
        "updated by installed binary\n"
    );
}

#[test]
fn declining_startup_update_enters_tui_without_mutation() {
    let fixture = Fixture::new();
    fixture.cache_update();
    let output = run_pty(&mut fixture.command(), b"n\rq");
    assert!(output.status.success(), "{}", output.output);
    assert!(
        output
            .output
            .contains("Update memex and its installed skills now?")
    );
    assert!(output.output.contains("\u{1b}[?1049h"));
    assert!(fixture.log().is_empty());
    assert!(
        !fixture
            .home
            .join(".agents/skills/memex-search/SKILL.md")
            .exists()
    );
}

#[test]
fn agent_and_non_interactive_ptys_show_help_without_prompting() {
    for agent in [true, false] {
        let fixture = Fixture::new();
        fixture.cache_update();
        let mut command = fixture.command();
        if agent {
            command.env("CODEX_THREAD_ID", "fixture-thread");
        } else {
            command.arg("--non-interactive");
        }
        let output = run_pty(&mut command, b"");
        assert!(output.status.success(), "{}", output.output);
        assert!(output.output.contains("Fast local history search"));
        assert!(output.output.contains("update: memex v99.0.0 is available"));
        assert!(
            !output
                .output
                .contains("Update memex and its installed skills now?")
        );
        assert!(fixture.log().is_empty());
    }

    let fixture = Fixture::new();
    fixture.cache_update();
    let output = run_pty(fixture.command().args(["tui", "--non-interactive"]), b"");
    assert!(!output.status.success());
    assert!(
        output
            .output
            .contains("TUI requires an interactive human terminal")
    );
    assert!(
        !output
            .output
            .contains("Update memex and its installed skills now?")
    );
    assert!(fixture.log().is_empty());
}

#[test]
fn update_requires_yes_without_a_tty_and_yes_runs_installed_binary_chain() {
    let fixture = Fixture::new();
    let refused = run(fixture.command().arg("update"));
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr)
            .contains("update requires confirmation; use `memex update --yes`")
    );
    assert!(fixture.log().is_empty());

    let accepted = run(fixture.command().args(["update", "--yes"]));
    assert!(
        accepted.status.success(),
        "{}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    assert_eq!(
        fixture.log(),
        "brew update\nbrew upgrade nicosuave/tap/memex\nbrew --prefix nicosuave/tap/memex\ninstalled --version\ninstalled skill update --target all\n"
    );
}

#[test]
fn homebrew_failures_stop_the_update_at_the_failing_step() {
    let cases = [
        ("update", "brew update\n", "brew update failed"),
        (
            "upgrade",
            "brew update\nbrew upgrade nicosuave/tap/memex\n",
            "brew upgrade nicosuave/tap/memex failed",
        ),
        (
            "--prefix",
            "brew update\nbrew upgrade nicosuave/tap/memex\nbrew --prefix nicosuave/tap/memex\n",
            "Homebrew upgrade finished, but `brew --prefix nicosuave/tap/memex` failed",
        ),
        (
            "version",
            "brew update\nbrew upgrade nicosuave/tap/memex\nbrew --prefix nicosuave/tap/memex\ninstalled --version\n",
            "installed memex version check failed",
        ),
        (
            "skill",
            "brew update\nbrew upgrade nicosuave/tap/memex\nbrew --prefix nicosuave/tap/memex\ninstalled --version\ninstalled skill update --target all\n",
            "refreshing its skills failed",
        ),
    ];
    for (failure, expected_log, expected_error) in cases {
        let fixture = Fixture::new();
        let output = run(fixture
            .command()
            .env("MEMEX_TEST_FAIL", failure)
            .args(["update", "--yes"]));
        assert!(!output.status.success(), "{failure} unexpectedly succeeded");
        assert_eq!(fixture.log(), expected_log, "failure at {failure}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(expected_error),
            "failure at {failure}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        if failure == "skill" {
            assert_eq!(
                String::from_utf8_lossy(&output.stdout),
                "Installed memex 99.0.0\n"
            );
        }
    }
}

#[test]
fn no_update_check_keeps_stale_skill_warning_off_search_stdout() {
    let fixture = Fixture::new();
    fixture.cache_update();
    let skill = fixture.home.join(".agents/skills/memex-search/SKILL.md");
    std::fs::create_dir_all(skill.parent().unwrap()).unwrap();
    std::fs::write(&skill, "stale skill\n").unwrap();
    let paths = Paths::new(Some(fixture.root.clone())).unwrap();
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    let mut writer = index.writer().unwrap();
    index
        .add_record(
            &mut writer,
            &Record {
                source: SourceKind::Codex,
                doc_id: 1,
                ts: 1,
                project: "fixture".into(),
                session_id: "session".into(),
                turn_id: 1,
                role: "assistant".into(),
                text: "needle".into(),
                tool_name: None,
                tool_input: None,
                tool_output: None,
                links: RecordLinks::default(),
                source_path: "/tmp/fixture.jsonl".into(),
            },
        )
        .unwrap();
    writer.commit().unwrap();
    drop(writer);

    let search_args = [
        "--no-update-check",
        "search",
        "needle",
        "--machine",
        "local",
        "--root",
        fixture.root.to_str().unwrap(),
    ];
    let output = run(fixture.command().args(search_args));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        serde_json::from_str::<serde_json::Value>(line).unwrap();
    }
    assert_eq!(String::from_utf8_lossy(&output.stdout).lines().count(), 1);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("memex-search skill is outdated or locally modified (shared)"));
    assert!(!stderr.contains("update: memex v99.0.0 is available"));

    let toon = run(fixture
        .command()
        .args(search_args)
        .args(["--format", "toon"]));
    assert!(toon.status.success());
    toon_format::decode_default(std::str::from_utf8(&toon.stdout).unwrap()).unwrap();
    let stderr = String::from_utf8_lossy(&toon.stderr);
    assert!(stderr.contains("memex-search skill is outdated or locally modified (shared)"));
    assert!(!stderr.contains("update: memex v99.0.0 is available"));
}
