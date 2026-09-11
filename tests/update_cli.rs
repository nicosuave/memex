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
        let executable = Path::new(env!("CARGO_BIN_EXE_memex"));
        // Keep fixtures on the executable's filesystem so they can share its inode.
        // Copying a fresh Mach-O for every case triggers repeated macOS verification.
        let temp = tempfile::tempdir_in(executable.parent().unwrap()).unwrap();
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
        // A hard link preserves the simulated Cellar path (unlike a symlink)
        // without creating another executable or changing the build's permissions.
        std::fs::hard_link(executable, &cellar_memex).unwrap();

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
if [ "$1" = "--no-update-check" ]; then
  [ "$MEMEX_TEST_FAIL" = "daemon" ] && exit 20
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

    fn standalone() -> Self {
        let mut fixture = Self::new();
        let standalone = fixture.bin.join("memex");
        std::fs::hard_link(env!("CARGO_BIN_EXE_memex"), &standalone).unwrap();
        fixture.cellar_memex = standalone;
        // The already-current case runs the real reconciler. Keep both platform
        // registration locations inside this fixture, regardless of host config.
        std::fs::write(
            fixture.home.join(".memex/config.toml"),
            format!(
                "auto_index_on_search = false\nindex_service_plist = {:?}\nindex_service_systemd_dir = {:?}\n",
                fixture.home.join("daemon.plist"),
                fixture.home.join("systemd"),
            ),
        )
        .unwrap();

        let release = fixture._temp.path().join("release");
        std::fs::create_dir_all(&release).unwrap();
        write_script(
            &release.join("memex"),
            r#"#!/bin/sh
printf 'release-binary %s\n' "$*" >> "$MEMEX_TEST_LOG"
case "$*" in
  --version)
    [ "$MEMEX_TEST_FAIL" = version ] && exit 18
    if [ "$MEMEX_TEST_FAIL" = wrong-version ]; then
      printf 'memex 98.0.0\n'
    else
      printf 'memex 99.0.0\n'
    fi
    ;;
  '--no-update-check daemon reconcile') exit 0 ;;
  'skill update --target all')
    mkdir -p "$HOME/.agents/skills/memex-search"
    printf 'updated by release binary\n' > "$HOME/.agents/skills/memex-search/SKILL.md"
    ;;
  *) exit 96 ;;
esac
"#,
        );
        let archive = fixture.bin.join("release.tar.gz");
        let archived = Command::new("tar")
            .arg("-czf")
            .arg(&archive)
            .arg("-C")
            .arg(&release)
            .arg("memex")
            .output()
            .unwrap();
        assert!(
            archived.status.success(),
            "{}",
            String::from_utf8_lossy(&archived.stderr)
        );
        write_script(
            &fixture.bin.join("curl"),
            r#"#!/bin/sh
destination=
url=
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) shift; destination=$1 ;;
    https://*) url=$1 ;;
  esac
  shift
done
case "$url" in
  https://api.github.com/repos/nicosuave/memex/releases/latest)
    printf 'release latest\n' >> "$MEMEX_TEST_LOG"
    printf '{"tag_name":"v%s"}\n' "${MEMEX_TEST_LATEST:-99.0.0}"
    ;;
  https://github.com/nicosuave/memex/releases/download/v99.0.0/memex-99.0.0-*.tar.gz)
    printf 'release download\n' >> "$MEMEX_TEST_LOG"
    [ "$MEMEX_TEST_FAIL" = download ] && exit 17
    [ -n "$destination" ] || exit 95
    cp "$(dirname "$0")/release.tar.gz" "$destination"
    ;;
  *) exit 97 ;;
esac
"#,
        );
        fixture
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
    run_pty_with_tui_input(command, input, b"")
}

fn run_pty_with_tui_input(command: &mut Command, input: &[u8], tui_input: &[u8]) -> PtyOutput {
    let mut master_fd = -1;
    let mut slave_fd = -1;
    let mut size = libc::winsize {
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
            std::ptr::null_mut(),
            &mut size,
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
            if libc::ioctl(libc::STDIN_FILENO, libc::TIOCSCTTY as _, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().unwrap();
    // Command retains its configured Stdio handles after spawn. Close the parent's
    // slave copies so the master reader can finish when the child exits on Linux.
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    drop(slave);
    let mut reader = master.try_clone().unwrap();
    let (output_tx, output_rx) = std::sync::mpsc::channel();
    let tui_input = tui_input.to_vec();
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut buffer = [0; 4096];
        let mut cursor_replied = false;
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(len) => {
                    bytes.extend_from_slice(&buffer[..len]);
                    // Ratatui queries the cursor when entering the TUI. Emulate
                    // that terminal response before sending interactive keys.
                    if !cursor_replied && bytes.windows(4).any(|part| part == b"\x1b[6n") {
                        cursor_replied = true;
                        let _ = reader.write_all(b"\x1b[1;1R");
                        let _ = reader.write_all(&tui_input);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                // Linux reports EIO when the PTY slave closes.
                Err(_) => break,
            }
        }
        let _ = output_tx.send(String::from_utf8_lossy(&bytes).into_owned());
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
        output: output_rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("PTY output did not close within 10 seconds"),
    }
}

#[test]
fn pty_output_closes_after_child_exit_while_command_is_alive() {
    let mut command = Command::new("sh");
    command.args(["-c", "printf pty-finished"]);
    let output = run_pty(&mut command, b"");
    assert!(output.status.success());
    assert_eq!(output.output, "pty-finished");
    // Keep Command alive through collection, as the interactive fixtures do.
    assert_eq!(command.get_program(), "sh");
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
        "brew update\nbrew upgrade nicosuave/tap/memex\nbrew --prefix nicosuave/tap/memex\ninstalled --version\ninstalled --no-update-check daemon reconcile\ninstalled skill update --target all\n"
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
    // Ctrl-C is the global quit binding; plain 'q' is home-screen search input.
    let output = run_pty_with_tui_input(&mut fixture.command(), b"n\r", b"\x03");
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
        // Assert help text independently of the host's terminal color settings.
        command.env("NO_COLOR", "1");
        if agent {
            command.env("CODEX_THREAD_ID", "fixture-thread");
        } else {
            command.arg("--non-interactive");
        }
        let output = run_pty(&mut command, b"");
        assert!(output.status.success(), "{}", output.output);
        assert!(output.output.contains("Usage: memex"), "{}", output.output);
        assert!(
            output.output.contains("Find and read:"),
            "{}",
            output.output
        );
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
        "brew update\nbrew upgrade nicosuave/tap/memex\nbrew --prefix nicosuave/tap/memex\ninstalled --version\ninstalled --no-update-check daemon reconcile\ninstalled skill update --target all\n"
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
            "daemon",
            "brew update\nbrew upgrade nicosuave/tap/memex\nbrew --prefix nicosuave/tap/memex\ninstalled --version\ninstalled --no-update-check daemon reconcile\n",
            "daemon activation failed",
        ),
        (
            "skill",
            "brew update\nbrew upgrade nicosuave/tap/memex\nbrew --prefix nicosuave/tap/memex\ninstalled --version\ninstalled --no-update-check daemon reconcile\ninstalled skill update --target all\n",
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
fn standalone_update_verifies_release_then_activates_and_refreshes_skills() {
    let fixture = Fixture::standalone();
    let output = run(fixture
        .command()
        .args(["--no-update-check", "update", "--yes"]));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fixture.log(),
        "release latest\nrelease download\nrelease-binary --version\nrelease-binary --no-update-check daemon reconcile\nrelease-binary skill update --target all\n"
    );
    assert_eq!(
        std::fs::read_to_string(fixture.home.join(".agents/skills/memex-search/SKILL.md")).unwrap(),
        "updated by release binary\n"
    );
    let installed = run(fixture.command().arg("--version"));
    assert!(installed.status.success());
    assert_eq!(installed.stdout, b"memex 99.0.0\n");
}

#[test]
fn standalone_download_and_verification_failures_preserve_original_binary() {
    use std::os::unix::fs::MetadataExt;

    for failure in ["download", "version", "wrong-version"] {
        let fixture = Fixture::standalone();
        let original = std::fs::metadata(&fixture.cellar_memex).unwrap();
        let output = run(fixture.command().env("MEMEX_TEST_FAIL", failure).args([
            "--no-update-check",
            "update",
            "--yes",
        ]));
        assert!(!output.status.success(), "{failure} unexpectedly succeeded");
        let expected = if failure == "download" {
            "release latest\nrelease download\n"
        } else {
            "release latest\nrelease download\nrelease-binary --version\n"
        };
        assert_eq!(fixture.log(), expected, "failure at {failure}");
        let unchanged = std::fs::metadata(&fixture.cellar_memex).unwrap();
        assert_eq!(original.ino(), unchanged.ino(), "failure at {failure}");
        assert_eq!(original.len(), unchanged.len(), "failure at {failure}");
        let installed = run(fixture.command().arg("--version"));
        assert!(installed.status.success());
        assert_eq!(
            String::from_utf8_lossy(&installed.stdout),
            format!("memex {}\n", env!("CARGO_PKG_VERSION"))
        );
    }
}

#[test]
fn standalone_already_current_still_reconciles_and_refreshes_skills() {
    let fixture = Fixture::standalone();
    let skill = fixture.home.join(".agents/skills/memex-search/SKILL.md");
    std::fs::create_dir_all(skill.parent().unwrap()).unwrap();
    std::fs::write(&skill, "outdated skill\n").unwrap();
    let output = run(fixture
        .command()
        .env("MEMEX_TEST_LATEST", env!("CARGO_PKG_VERSION"))
        .args(["--no-update-check", "update", "--yes"]));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fixture.log(), "release latest\n");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("already up to date"), "{stdout}");
    assert!(
        stdout.contains("daemon: no Memex-owned registration; unchanged"),
        "{stdout}"
    );
    let updated = std::fs::read_to_string(&skill).unwrap();
    assert_ne!(updated, "outdated skill\n");
    assert!(updated.contains("memex-search"));
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
    toon_format::decode_default::<serde_json::Value>(std::str::from_utf8(&toon.stdout).unwrap())
        .unwrap();
    let stderr = String::from_utf8_lossy(&toon.stderr);
    assert!(stderr.contains("memex-search skill is outdated or locally modified (shared)"));
    assert!(!stderr.contains("update: memex v99.0.0 is available"));
}
