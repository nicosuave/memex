#![cfg(any(target_os = "macos", target_os = "linux"))]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const LABEL: &str = "test.memex.upgrade";
const OLD_EXE: &str = "/old/installation/bin/memex";

struct Fixture {
    _temp: tempfile::TempDir,
    home: PathBuf,
    root: PathBuf,
    bin: PathBuf,
    definition: PathBuf,
    log: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let root = temp.path().join("custom root");
        let bin = temp.path().join("bin");
        let units = temp.path().join("units");
        for directory in [&home, &root, &bin, &units] {
            std::fs::create_dir_all(directory).unwrap();
        }
        let definition = if cfg!(target_os = "macos") {
            root.join("index-service.plist")
        } else {
            units.join(format!("{LABEL}.service"))
        };
        let log = temp.path().join("manager.log");
        let config = format!(
            "auto_index_on_search = false\nindex_service_label = '{LABEL}'\nindex_service_plist = '{}'\nindex_service_systemd_dir = '{}'\nindex_service_mode = 'interval'\nindex_service_interval = 731\n",
            definition.display(),
            units.display(),
        );
        std::fs::write(root.join("config.toml"), config).unwrap();
        write_script(&bin.join("launchctl"), LAUNCHCTL);
        write_script(&bin.join("systemctl"), SYSTEMCTL);
        Self {
            _temp: temp,
            home,
            root,
            bin,
            definition,
            log,
        }
    }

    fn interval_definition(&self, executable: &str) -> String {
        if cfg!(target_os = "macos") {
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>Label</key><string>{LABEL}</string>
<key>ProgramArguments</key><array>
<string>{executable}</string><string>index</string>
<string>--root</string><string>{}</string>
<string>--no-embeddings</string><string>--no-codex</string>
</array>
<key>StartInterval</key><integer>731</integer>
<key>RunAtLoad</key><true/>
<key>EnvironmentVariables</key><dict><key>CUSTOM_SETTING</key><string>/nix/store/helper/bin &amp; preserve</string><key>USER_NOTE</key><string>nix</string></dict>
<key>StandardOutPath</key><string>/tmp/custom-output.log</string>
</dict></plist>
"#,
                self.root.display()
            )
        } else {
            format!(
                "[Unit]\nDescription=Memex Index Service\n\n[Service]\nEnvironment=\"CUSTOM_SETTING=keep & preserve\"\nType=oneshot\nExecStart={executable} index --root '{}' --no-embeddings --no-codex\n\n[Install]\n",
                self.root.display()
            )
        }
    }

    fn register(&self, executable: &str) -> String {
        let definition = self.interval_definition(executable);
        std::fs::write(&self.definition, &definition).unwrap();
        if cfg!(target_os = "linux") {
            std::fs::write(self.definition.with_extension("timer"),
                "[Unit]\nDescription=Memex Index Timer\n[Timer]\nOnUnitActiveSec=731\n[Install]\nWantedBy=timers.target\n").unwrap();
        }
        definition
    }

    fn reconcile(&self, state: &str) -> Output {
        Command::new(env!("CARGO_BIN_EXE_memex"))
            .args(["--no-update-check", "daemon", "reconcile", "--root"])
            .arg(&self.root)
            .env("HOME", &self.home)
            .env("PATH", &self.bin)
            .env("UID", "12345")
            .env("MEMEX_TEST_LOG", &self.log)
            .env("MEMEX_TEST_STATE", state)
            .env("MEMEX_TEST_DEFINITION", &self.definition)
            .env_remove("MEMEX_SERVICE_MANAGER")
            .output()
            .unwrap()
    }

    fn calls(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

fn write_script(path: &Path, contents: &str) {
    std::fs::write(path, contents).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

const LAUNCHCTL: &str = r#"#!/bin/sh
printf 'launchctl %s\n' "$*" >> "$MEMEX_TEST_LOG"
case "$1" in
  print)
    if [ "$MEMEX_TEST_STATE" = stopped ]; then
      printf 'Could not find service\n' >&2
      exit 113
    fi
    if [ "$MEMEX_TEST_STATE" = foreign ]; then
      printf 'path = /external/manager/memex.plist\n'
      exit 0
    fi
    printf 'path = %s\n' "$MEMEX_TEST_DEFINITION"
    ;;
  print-disabled)
    if [ "$MEMEX_TEST_STATE" = disabled ]; then
      printf '"test.memex.upgrade" => true\n'
    else
      printf '"test.memex.upgrade" => false\n'
    fi
    ;;
  bootout|bootstrap)
    [ "$MEMEX_TEST_STATE" = active ] || exit 91
    ;;
  *) printf 'Unexpected launchctl command\n' >&2; exit 92 ;;
esac
"#;

const SYSTEMCTL: &str = r#"#!/bin/sh
printf 'systemctl %s\n' "$*" >> "$MEMEX_TEST_LOG"
[ "$1" = --user ] || exit 90
case "$2" in
  is-enabled)
    if [ "$MEMEX_TEST_STATE" = unavailable ]; then
      printf 'Failed to connect to user bus\n' >&2
      exit 1
    fi
    if [ "$MEMEX_TEST_STATE" = disabled ]; then printf 'disabled\n'; exit 1; fi
    printf 'enabled\n'
    ;;
  is-active)
    if [ "$MEMEX_TEST_STATE" = stopped ]; then printf 'inactive\n'; exit 3; fi
    printf 'active\n'
    ;;
  show)
    [ "$3" = --property=FragmentPath ] && [ "$4" = --value ] || exit 93
    case "$5" in
      test.memex.upgrade.service)
        if [ "$MEMEX_TEST_STATE" = foreign ]; then
          printf '/external/manager/memex.service\n'
        else
          printf '%s\n' "$MEMEX_TEST_DEFINITION"
        fi
        ;;
      test.memex.upgrade.timer)
        if [ "$MEMEX_TEST_STATE" = foreign-timer ]; then
          printf '/external/manager/memex.timer\n'
        else
          printf '%s.timer\n' "${MEMEX_TEST_DEFINITION%.service}"
        fi
        ;;
      *) exit 94 ;;
    esac
    ;;
  daemon-reload|try-restart|restart)
    [ "$MEMEX_TEST_STATE" = active ] || exit 91
    ;;
  *) printf 'Unexpected systemctl command\n' >&2; exit 92 ;;
esac
"#;

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_no_mutations(calls: &str) {
    for mutation in [
        "bootout",
        "bootstrap",
        "daemon-reload",
        "try-restart",
        "restart",
        "enable",
        "disable",
    ] {
        assert!(
            !calls
                .lines()
                .any(|line| line.split_whitespace().any(|word| word == mutation)),
            "unexpected mutation {mutation}: {calls}"
        );
    }
}

#[test]
fn absent_registration_does_not_call_a_service_manager() {
    let fixture = Fixture::new();
    let output = fixture.reconcile("active");
    assert_success(&output);
    assert!(!fixture.definition.exists());
    assert!(fixture.calls().is_empty());
}

#[test]
fn disabled_and_stopped_registrations_are_preserved() {
    for state in ["disabled", "stopped"] {
        let fixture = Fixture::new();
        let original = fixture.register(OLD_EXE);
        let output = fixture.reconcile(state);
        assert_success(&output);
        assert_eq!(
            std::fs::read_to_string(&fixture.definition).unwrap(),
            original
        );
        assert_no_mutations(&fixture.calls());
    }
}

#[test]
fn interval_activation_changes_only_executable_and_preserves_configuration() {
    let fixture = Fixture::new();
    let original = fixture.register(OLD_EXE);
    let config = std::fs::read(fixture.root.join("config.toml")).unwrap();
    let timer = cfg!(target_os = "linux")
        .then(|| std::fs::read(fixture.definition.with_extension("timer")).unwrap());
    let output = fixture.reconcile("active");
    assert_success(&output);
    // Keep the invoked stable path even when the build cache resolves it to
    // another directory, just as a package profile link must remain stable.
    let executable = Path::new(env!("CARGO_BIN_EXE_memex"));
    assert_eq!(
        std::fs::read_to_string(&fixture.definition).unwrap(),
        original.replacen(OLD_EXE, executable.to_str().unwrap(), 1)
    );
    assert_eq!(
        std::fs::read(fixture.root.join("config.toml")).unwrap(),
        config
    );
    if let Some(timer) = timer {
        assert_eq!(
            std::fs::read(fixture.definition.with_extension("timer")).unwrap(),
            timer
        );
    }
    let calls = fixture.calls();
    if cfg!(target_os = "macos") {
        assert!(
            calls.contains("launchctl bootout gui/12345/test.memex.upgrade\n"),
            "{calls}"
        );
        assert!(
            calls.contains(&format!(
                "launchctl bootstrap gui/12345 {}\n",
                fixture.definition.display()
            )),
            "{calls}"
        );
    } else {
        assert!(
            calls.contains("systemctl --user daemon-reload\n"),
            "{calls}"
        );
        assert!(
            calls.contains("systemctl --user try-restart test.memex.upgrade.service\n"),
            "{calls}"
        );
        assert!(
            calls.contains("systemctl --user restart test.memex.upgrade.timer\n"),
            "{calls}"
        );
    }
}

#[test]
fn nix_store_and_foreign_executables_are_not_rewritten_or_restarted() {
    for executable in [
        "/nix/store/package-memex/bin/memex",
        "/other/package/bin/not-memex",
    ] {
        let fixture = Fixture::new();
        let original = fixture.register(executable);
        let output = fixture.reconcile("active");
        if executable.starts_with("/nix/store/") {
            assert_success(&output);
            assert!(fixture.calls().is_empty());
        } else {
            assert!(!output.status.success());
        }
        assert_eq!(
            std::fs::read_to_string(&fixture.definition).unwrap(),
            original
        );
        assert_no_mutations(&fixture.calls());
    }
}

#[test]
fn nix_manager_marker_preserves_a_mutable_service_definition() {
    let fixture = Fixture::new();
    let original = fixture.register(OLD_EXE);
    let marked = if cfg!(target_os = "macos") {
        original.replace(
            "<key>CUSTOM_SETTING</key>",
            "<key>MEMEX_SERVICE_MANAGER</key><string>nix</string><key>CUSTOM_SETTING</key>",
        )
    } else {
        original.replace(
            "[Service]\n",
            "[Service]\nEnvironment=\"MEMEX_SERVICE_MANAGER=nix\"\n",
        )
    };
    std::fs::write(&fixture.definition, &marked).unwrap();
    let output = fixture.reconcile("active");
    assert_success(&output);
    assert_eq!(
        std::fs::read_to_string(&fixture.definition).unwrap(),
        marked
    );
    assert!(fixture.calls().is_empty());
}

#[test]
fn different_loaded_definition_is_not_replaced() {
    let fixture = Fixture::new();
    let original = fixture.register(OLD_EXE);
    let output = fixture.reconcile("foreign");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("state mismatch") || stderr.contains("owned by"),
        "{stderr}"
    );
    assert_eq!(
        std::fs::read_to_string(&fixture.definition).unwrap(),
        original
    );
    assert_no_mutations(&fixture.calls());
}

#[cfg(target_os = "linux")]
#[test]
fn different_loaded_timer_is_not_replaced_or_restarted() {
    let fixture = Fixture::new();
    let original = fixture.register(OLD_EXE);
    let timer_path = fixture.definition.with_extension("timer");
    let timer = std::fs::read(&timer_path).unwrap();
    let output = fixture.reconcile("foreign-timer");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("owned by /external/manager/memex.timer"),
        "{stderr}"
    );
    assert_eq!(
        std::fs::read_to_string(&fixture.definition).unwrap(),
        original
    );
    assert_eq!(std::fs::read(timer_path).unwrap(), timer);
    let calls = fixture.calls();
    assert!(
        calls.contains("show --property=FragmentPath --value test.memex.upgrade.service\n"),
        "{calls}"
    );
    assert!(
        calls.contains("show --property=FragmentPath --value test.memex.upgrade.timer\n"),
        "{calls}"
    );
    assert_no_mutations(&calls);
}

#[cfg(target_os = "linux")]
#[test]
fn unavailable_systemd_is_an_error_not_a_disabled_daemon() {
    let fixture = Fixture::new();
    let original = fixture.register(OLD_EXE);
    let output = fixture.reconcile("unavailable");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot determine systemd enabled state"),
        "{stderr}"
    );
    assert!(stderr.contains("Failed to connect to user bus"), "{stderr}");
    assert_eq!(
        std::fs::read_to_string(&fixture.definition).unwrap(),
        original
    );
    assert_eq!(
        fixture.calls(),
        "systemctl --user is-enabled test.memex.upgrade.timer\n"
    );
}
