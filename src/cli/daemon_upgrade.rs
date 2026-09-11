//! Installation paths and activation of an existing daemon, independent of installer.
use super::*;
use crate::daemon_runtime::{current_executable_identity, executable_identity};

pub(super) fn nix_store(path: &Path) -> bool {
    path.starts_with("/nix/store")
}

fn homebrew_opt(path: &Path) -> Option<PathBuf> {
    let mut prefix = PathBuf::new();
    let mut components = path.components();
    while let Some(component) = components.next() {
        if component.as_os_str() == "Cellar" {
            return (components.next()?.as_os_str() == "memex")
                .then(|| prefix.join("opt/memex/bin/memex"));
        }
        prefix.push(component);
    }
    None
}

pub(super) fn service_executable() -> Result<PathBuf> {
    let current = std::env::current_exe()?.canonicalize()?;
    if let Some(opt) = homebrew_opt(&current) {
        anyhow::ensure!(
            opt.canonicalize().ok().as_ref() == Some(&current),
            "Homebrew's active memex differs from this executable; run the installed memex daemon restart"
        );
        return Ok(opt);
    }
    // Preserve invocation through a profile or manually maintained symlink.
    let invoked = std::env::args_os().next().map(PathBuf::from);
    let candidate = invoked
        .filter(|path| path.is_absolute())
        .or_else(|| find_in_path("memex"));
    if let Some(path) = candidate
        && !nix_store(&path)
        && path.canonicalize().ok().as_ref() == Some(&current)
    {
        return Ok(path);
    }
    if nix_store(&current) {
        return Err(anyhow!(
            "Use a Nix profile's memex to register a daemon, or configure the NixOS/Home Manager daemon module"
        ));
    }
    Ok(current)
}

/// Check the installed file identity without interrupting an indexing pass.
pub(super) struct Replacement {
    path: Option<PathBuf>,
    identity: String,
    checked: Instant,
}

fn observed_executable(current: &Path, invoked: Option<&Path>) -> Option<PathBuf> {
    homebrew_opt(current)
        .or_else(|| {
            invoked
                .filter(|path| path.is_absolute() && !nix_store(path))
                .map(Path::to_path_buf)
        })
        .or_else(|| (!nix_store(current)).then(|| current.to_path_buf()))
}

impl Replacement {
    pub(super) fn new() -> Result<Self> {
        let path = if std::env::var("MEMEX_SERVICE_MANAGER").as_deref() == Ok("nix") {
            None
        } else {
            // Unlike registration, observing an update must allow the stable
            // link to have advanced while this process was starting.
            let current = std::env::current_exe()?;
            let invoked = std::env::args_os()
                .next()
                .map(PathBuf::from)
                .filter(|path| path.is_absolute())
                .or_else(|| find_in_path("memex"));
            observed_executable(&current, invoked.as_deref())
        };
        Ok(Self {
            path,
            identity: current_executable_identity()?,
            checked: Instant::now(),
        })
    }

    pub(super) fn check(&mut self) {
        if self.checked.elapsed() < Duration::from_secs(2) {
            return;
        }
        self.checked = Instant::now();
        let Some(path) = &self.path else {
            return;
        };
        let result = (|| -> Result<()> {
            let identity = executable_identity(path)?;
            if identity == self.identity {
                return Ok(());
            }
            verify_binary(path)?;
            // Retry failures later; a partially written replacement must not strand
            // the old daemon or be mistaken for an activated build.
            eprintln!("daemon: activating updated executable {}", path.display());
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                let error = std::process::Command::new(path)
                    .args(std::env::args_os().skip(1))
                    .exec();
                Err(error).context("activate updated daemon")
            }
            #[cfg(not(unix))]
            {
                Err(anyhow!("daemon activation requires Unix"))
            }
        })();
        if let Err(error) = result {
            eprintln!("daemon: update not activated (will retry): {error:#}");
        }
    }
}

pub(super) fn verify_binary(path: &Path) -> Result<String> {
    let mut child = std::process::Command::new(path)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if child.try_wait()?.is_some() {
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(anyhow!("replacement version check timed out"));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let output = child.wait_with_output()?;
    let version = std::str::from_utf8(&output.stdout)?.trim();
    anyhow::ensure!(
        output.status.success()
            && version
                .strip_prefix("memex ")
                .is_some_and(|v| parse_version_parts(v).is_some()),
        "replacement is not a working memex binary"
    );
    Ok(version
        .strip_prefix("memex ")
        .expect("validated version")
        .to_string())
}

pub(super) fn ensure_mutable(path: &Path) -> Result<()> {
    if path.canonicalize().is_ok_and(|target| nix_store(&target))
        || std::fs::read_to_string(path).is_ok_and(|contents| {
            contents.contains("MEMEX_SERVICE_MANAGER=nix")
                || contents
                    .split_once("<key>MEMEX_SERVICE_MANAGER</key>")
                    .is_some_and(|(_, value)| {
                        value.trim_start().starts_with("<string>nix</string>")
                    })
        })
    {
        return Err(anyhow!(
            "{} is managed by Nix; update and activate your Nix configuration instead",
            path.display()
        ));
    }
    Ok(())
}

pub(super) fn ensure_systemd_owner(label: &str, definition: &Path) -> Result<()> {
    let unit = definition
        .file_name()
        .context("systemd definition has no unit name")?
        .to_string_lossy();
    let output = std::process::Command::new("systemctl")
        .args([
            "--user",
            "show",
            "--property=FragmentPath",
            "--value",
            &unit,
        ])
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "cannot determine systemd service ownership: {}",
        format_command_output(&output)
    );
    let fragment = String::from_utf8_lossy(&output.stdout);
    let fragment = Path::new(fragment.trim());
    if fragment.as_os_str().is_empty() {
        return Ok(());
    }
    anyhow::ensure!(
        fragment == definition
            || (fragment.canonicalize().ok().is_some()
                && fragment.canonicalize().ok() == definition.canonicalize().ok()),
        "service {label} is owned by {}; use its package manager to activate it",
        fragment.display()
    );
    ensure_mutable(fragment)
}

pub(super) fn wait_ready(paths: &Paths, timeout: Duration) -> Result<()> {
    let expected = current_executable_identity()?;
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(info) = crate::daemon_runtime::read(paths)?
            && info.ready
            && info.executable_id == expected
        {
            println!("daemon: ready (memex {}, pid {})", info.version, info.pid);
            return Ok(());
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "daemon activation did not become ready with the installed build within {} seconds; inspect `memex daemon status` and daemon logs",
            timeout.as_secs()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

pub(super) fn print_runtime(paths: &Paths) -> Result<()> {
    println!("installed version: {}", env!("CARGO_PKG_VERSION"));
    match crate::daemon_runtime::read(paths)? {
        Some(info) => {
            println!("running version: {}", info.version);
            println!("pid: {}", info.pid);
            println!("executable: {}", info.executable.display());
            println!(
                "readiness: {}",
                if info.ready { "ready" } else { "starting" }
            );
            println!(
                "build: {}",
                if info.executable_id == current_executable_identity()? {
                    "matches installed binary"
                } else {
                    "update pending; running build differs"
                }
            );
        }
        None => println!(
            "running build: unavailable (stopped, interval mode, or daemon predates runtime reporting)"
        ),
    }
    Ok(())
}

fn program_range(contents: &str, macos: bool) -> Result<(usize, usize)> {
    Ok(if macos {
        let args = contents
            .find("<key>ProgramArguments</key>")
            .context("missing ProgramArguments")?;
        let start = args
            + contents[args..]
                .find("<string>")
                .context("missing daemon program")?
            + "<string>".len();
        let end = start
            + contents[start..]
                .find("</string>")
                .context("unterminated daemon program")?;
        (start, end)
    } else {
        let start = contents.find("ExecStart=").context("missing ExecStart")? + "ExecStart=".len();
        let end = start
            + contents[start..]
                .find(' ')
                .context("missing daemon arguments")?;
        (start, end)
    })
}

fn different_nix_installation(contents: &str, macos: bool, current: &Path) -> Result<bool> {
    let (start, end) = program_range(contents, macos)?;
    Ok(contents[start..end].starts_with("/nix/store/") && !nix_store(current))
}

fn replace_program(contents: &str, exe: &Path, macos: bool) -> Result<String> {
    let (start, end) = program_range(contents, macos)?;
    let old = &contents[start..end];
    anyhow::ensure!(
        old.ends_with("/memex"),
        "service executable is not a mutable Memex installation"
    );
    let new = if macos {
        xml_escape(&exe.to_string_lossy())
    } else {
        anyhow::ensure!(
            !exe.to_string_lossy().contains(char::is_whitespace),
            "systemd executable path contains whitespace"
        );
        exe.to_string_lossy().into_owned()
    };
    Ok(format!("{}{}{}", &contents[..start], new, &contents[end..]))
}

pub(super) fn reconcile(root: Option<PathBuf>) -> Result<()> {
    let paths = Paths::new(root)?;
    let config = UserConfig::load(&paths)?;
    let macos = cfg!(target_os = "macos");
    let label = config.index_service_label.clone().unwrap_or_else(|| {
        if macos {
            default_index_service_label()
        } else {
            "memex-index".into()
        }
    });
    validate_service_label(&label)?;
    let definition = if macos {
        config
            .index_service_plist
            .clone()
            .unwrap_or_else(|| default_index_service_plist(&paths.root))
    } else {
        config
            .index_service_systemd_dir
            .clone()
            .unwrap_or_else(default_systemd_user_dir)
            .join(format!("{label}.service"))
    };
    if !definition.exists() {
        println!("daemon: no Memex-owned registration; unchanged");
        return Ok(());
    }
    if ensure_mutable(&definition).is_err() {
        println!("daemon: Nix-managed registration; activate your Nix configuration");
        return Ok(());
    }
    let contents = std::fs::read_to_string(&definition)?;
    if different_nix_installation(&contents, macos, &std::env::current_exe()?)? {
        println!("daemon: uses a Nix installation; reconcile through that profile");
        return Ok(());
    }
    let continuous = if macos {
        contents.contains("<key>KeepAlive</key>")
    } else {
        contents.contains("Type=simple")
    };
    let unit = format!("{label}.{}", if continuous { "service" } else { "timer" });
    if macos {
        let (domain, target) = launchctl_targets(&label)?;
        if !launchctl_service_exists(&target)? {
            println!("daemon: stopped; unchanged");
            return Ok(());
        }
        let disabled = std::process::Command::new("launchctl")
            .args(["print-disabled", &domain])
            .output()?;
        anyhow::ensure!(
            disabled.status.success(),
            "cannot determine launchd enabled state"
        );
        if String::from_utf8_lossy(&disabled.stdout).contains(&format!("\"{label}\" => true")) {
            println!("daemon: disabled; unchanged");
            return Ok(());
        }
        verify_launchd_job_loaded(&target, &definition)?;
    } else {
        let enabled = std::process::Command::new("systemctl")
            .args(["--user", "is-enabled", &unit])
            .output()?;
        let state = String::from_utf8_lossy(&enabled.stdout);
        if !matches!(state.trim(), "enabled" | "enabled-runtime") {
            anyhow::ensure!(
                matches!(
                    state.trim(),
                    "disabled" | "masked" | "masked-runtime" | "static" | "indirect" | "not-found"
                ),
                "cannot determine systemd enabled state: {}",
                format_command_output(&enabled)
            );
            println!("daemon: disabled; unchanged");
            return Ok(());
        }
        if systemd_unit_state(&unit)? != "active" {
            println!("daemon: stopped; unchanged");
            return Ok(());
        }
        ensure_systemd_owner(&label, &definition)?;
        if !continuous {
            ensure_systemd_owner(&label, &definition.with_extension("timer"))?;
        }
    }
    let exe = service_executable()?;
    let updated = replace_program(&contents, &exe, macos)?;
    if updated == contents
        && continuous
        && let Some(info) = crate::daemon_runtime::read(&paths)?
        && info.executable_id == current_executable_identity()?
    {
        if info.ready {
            println!("daemon: already running installed build");
        } else {
            // A retry must not restart a healthy process still doing its first
            // index pass. Wait for that same installed process to finish startup.
            wait_ready(&paths, Duration::from_secs(30))?;
        }
        return Ok(());
    }
    // Only the executable changes: preserve every existing argument, root,
    // environment variable, listener and scheduling setting exactly.
    let mut staged = tempfile::NamedTempFile::new_in(
        definition
            .parent()
            .context("service definition has no parent")?,
    )?;
    use std::io::Write;
    staged.write_all(updated.as_bytes())?;
    staged.persist(&definition).map_err(|error| error.error)?;
    if macos {
        let (domain, target) = launchctl_targets(&label)?;
        launchctl_bootout_service(&target)?;
        let output = std::process::Command::new("launchctl")
            .args(["bootstrap", &domain])
            .arg(&definition)
            .output()?;
        anyhow::ensure!(
            output.status.success(),
            "daemon bootstrap failed: {}",
            format_command_output(&output)
        );
    } else {
        run_systemctl(&["--user", "daemon-reload"], "daemon reload")?;
        // An already running interval invocation must also stop using the old
        // executable; try-restart leaves an idle oneshot idle.
        if !continuous {
            run_systemctl(
                &["--user", "try-restart", &format!("{label}.service")],
                "restart active indexer",
            )?;
        }
        run_systemctl(&["--user", "restart", &unit], "daemon restart")?;
    }
    if continuous {
        wait_ready(&paths, Duration::from_secs(30))?;
    } else {
        println!("daemon: interval registration updated; next invocation uses installed binary");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn observation_keeps_profile_and_manual_links_that_advanced_during_startup() {
        let profile = Path::new("/home/me/.nix-profile/bin/memex");
        assert_eq!(
            observed_executable(Path::new("/nix/store/old/bin/memex"), Some(profile)),
            Some(profile.to_path_buf())
        );
        let manual = Path::new("/home/me/bin/memex");
        assert_eq!(
            observed_executable(Path::new("/releases/old/memex"), Some(manual)),
            Some(manual.to_path_buf())
        );
        assert_eq!(
            observed_executable(
                Path::new("/nix/store/old/bin/memex"),
                Some(Path::new("/nix/store/old/bin/memex"))
            ),
            None
        );
    }
    #[test]
    fn cellar_paths_use_stable_formula_link() {
        assert_eq!(
            homebrew_opt(Path::new("/opt/homebrew/Cellar/memex/1.0/bin/memex")),
            Some(PathBuf::from("/opt/homebrew/opt/memex/bin/memex"))
        );
        assert_eq!(
            homebrew_opt(Path::new(
                "/home/linuxbrew/.linuxbrew/Cellar/memex/1/bin/memex"
            )),
            Some(PathBuf::from(
                "/home/linuxbrew/.linuxbrew/opt/memex/bin/memex"
            ))
        );
        assert_eq!(homebrew_opt(Path::new("/tmp/memex")), None);
        assert!(!nix_store(Path::new("/tmp/nix/store/memex")));
        assert!(nix_store(Path::new("/nix/store/hash-memex/bin/memex")));
    }
    #[test]
    fn activation_preserves_service_arguments_and_environment() {
        let args = vec![
            "/old/bin/memex".into(),
            "index".into(),
            "--root".into(),
            "/data/custom root".into(),
            "--no-codex".into(),
        ];
        let env = vec![("CUSTOM".into(), "value".into())];
        let plist = build_launchd_plist("custom", &args, Some(600), false, None, None, &env);
        let updated = replace_program(&plist, Path::new("/new/bin/memex"), true).unwrap();
        assert_eq!(
            updated,
            plist.replacen("/old/bin/memex", "/new/bin/memex", 1)
        );
        let unit = build_systemd_service("/old/bin/memex", &args[1..], true, &env);
        assert_eq!(
            replace_program(&unit, Path::new("/new/bin/memex"), false).unwrap(),
            unit.replacen("/old/bin/memex", "/new/bin/memex", 1)
        );
        let old_profile = "ExecStart=/nix/store/hash/bin/memex index\n";
        assert!(different_nix_installation(old_profile, false, Path::new("/new/memex")).unwrap());
        assert!(
            !different_nix_installation(old_profile, false, Path::new("/nix/store/new/bin/memex"))
                .unwrap()
        );
        assert_eq!(
            replace_program(
                old_profile,
                Path::new("/home/me/.nix-profile/bin/memex"),
                false
            )
            .unwrap(),
            "ExecStart=/home/me/.nix-profile/bin/memex index\n"
        );
        assert!(
            !different_nix_installation(
                "Environment=PATH=/nix/store/helper/bin\nExecStart=/usr/bin/memex index\n",
                false,
                Path::new("/usr/bin/memex")
            )
            .unwrap()
        );
    }
}
