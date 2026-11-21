//! Explicit desktop actions. Transcript references are data until the user opens them.
use serde_json::{Value, json};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const PREVIEW_LIMIT: u64 = 5_000_000;

fn field<'a>(session: &'a Value, names: &[&str]) -> Option<&'a str> {
    names.iter().find_map(|name| session.get(name)?.as_str())
}

fn require_local(session: &Value) -> Result<(), String> {
    let machine = field(session, &["machine", "machine_id", "machineID"]).unwrap_or("local");
    if machine != "local" {
        return Err(format!(
            "This action is available on the conversation's machine ({machine})."
        ));
    }
    Ok(())
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn resume_payload(session: &Value) -> Result<String, String> {
    require_local(session)?;
    let command = field(session, &["resume_cmd", "resume_command", "resumeCommand"])
        .filter(|value| !value.trim().is_empty())
        .ok_or("No resume command is available for this conversation.")?;
    let cwd = [session["resume_cwd"].as_str(), session["cwd"].as_str()]
        .into_iter()
        .flatten()
        .find(|value| !value.trim().is_empty());
    if command.contains('\0') || cwd.is_some_and(|value| value.contains('\0')) {
        return Err("The resume command or working directory is invalid.".into());
    }
    // The CLI owns this shell command; never reconstruct it from a session ID.
    Ok(format!(
        "{}{command}",
        cwd.map(|path| format!("cd -- {} || exit\n", shell_quote(path)))
            .unwrap_or_default()
    ))
}

#[cfg(not(target_os = "macos"))]
fn executable(name: &str) -> Option<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|path| path.join(name))
        .find(|path| {
            path.metadata()
                .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        })
}

#[cfg(target_os = "macos")]
fn app_path(id: &str) -> Option<PathBuf> {
    let bundle = match id {
        "ghostree" => "dev.sidequery.Ghostree",
        "ghostty" => "com.mitchellh.ghostty",
        "terminal" => "com.apple.Terminal",
        "alacritty" => "org.alacritty",
        "kitty" => "net.kovidgoyal.kitty",
        "wezterm" => "com.github.wez.wezterm",
        "cmux" => "com.cmuxterm.app",
        "chatgpt" => "com.openai.codex",
        _ => return None,
    };
    // Resolve the registered bundle, including renamed or nonstandard installs.
    if let Ok(output) = Command::new("/usr/bin/osascript")
        .args([
            "-e",
            &format!("POSIX path of (path to application id \"{bundle}\")"),
        ])
        .output()
        && output.status.success()
    {
        let path = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
        if path.is_dir() {
            return Some(path);
        }
    }
    let name = match id {
        "ghostree" => "Ghostree",
        "ghostty" => "Ghostty",
        "terminal" => "Utilities/Terminal",
        "alacritty" => "Alacritty",
        "kitty" => "kitty",
        "wezterm" => "WezTerm",
        "cmux" => "cmux",
        "chatgpt" => "ChatGPT",
        _ => return None,
    };
    let mut roots = vec![
        PathBuf::from("/Applications"),
        PathBuf::from("/System/Applications"),
    ];
    if let Some(home) = std::env::var_os("HOME") {
        roots.push(PathBuf::from(home).join("Applications"));
    }
    roots
        .into_iter()
        .map(|root| root.join(format!("{name}.app")))
        .find(|path| path.is_dir())
}

fn available(id: &str) -> bool {
    #[cfg(target_os = "macos")]
    {
        let Some(path) = app_path(id) else {
            return false;
        };
        if matches!(id, "ghostty" | "ghostree" | "cmux") {
            let name = if id == "cmux" { "cmux" } else { "Ghostty" };
            let Ok(dictionary) =
                std::fs::read_to_string(path.join(format!("Contents/Resources/{name}.sdef")))
            else {
                return false;
            };
            return dictionary.contains("name=\"new window\"")
                && if id == "cmux" {
                    dictionary.contains("name=\"id\"")
                } else {
                    dictionary.contains("name=\"with configuration\"")
                        && dictionary.contains("name=\"command\"")
                };
        }
        true
    }
    #[cfg(not(target_os = "macos"))]
    {
        matches!(
            id,
            "ghostty" | "alacritty" | "kitty" | "wezterm" | "gnome-terminal" | "konsole" | "xterm"
        ) && executable(id).is_some()
    }
}

pub fn destinations() -> Value {
    let choices = [
        ("ghostree", "Ghostree"),
        ("ghostty", "Ghostty"),
        ("terminal", "Terminal"),
        ("alacritty", "Alacritty"),
        ("kitty", "kitty"),
        ("wezterm", "WezTerm"),
        ("cmux", "cmux"),
        ("gnome-terminal", "GNOME Terminal"),
        ("konsole", "Konsole"),
        ("xterm", "xterm"),
        ("chatgpt", "ChatGPT"),
    ];
    json!(
        choices
            .into_iter()
            .filter(|(id, _)| available(id))
            .map(|(id, title)| json!({"id": id, "title": title}))
            .collect::<Vec<_>>()
    )
}

fn terminal_arguments(id: &str, shell: &str, payload: String) -> Result<Vec<String>, String> {
    let prefix: &[&str] = match id {
        "alacritty" => &["--hold", "--command"],
        "kitty" => &["--hold"],
        "wezterm" => &[
            "start",
            "--no-auto-connect",
            "--always-new-process",
            "--domain",
            "local",
            "--",
        ],
        "ghostty" => &["--gtk-single-instance=false", "-e"],
        "gnome-terminal" => &["--window", "--"],
        "konsole" => &["--separate", "--hold", "-e"],
        "xterm" => &["-hold", "-e"],
        _ => return Err("Unsupported terminal destination.".into()),
    };
    let mut args: Vec<String> = prefix.iter().map(|value| (*value).into()).collect();
    args.extend([shell.into(), "-lic".into(), payload]);
    Ok(args)
}

fn checked_command(program: &str, args: &[String]) -> Result<(), String> {
    let output = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr)
            .chars()
            .take(4000)
            .collect::<String>())
    }
}

fn chatgpt_url(session: &Value) -> Result<String, String> {
    require_local(session)?;
    let id = field(session, &["session_id", "sessionID"]).unwrap_or("");
    let valid_id = id.len() == 36
        && id.bytes().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        });
    if field(session, &["source"]) != Some("codex") || !valid_id {
        return Err("ChatGPT can open local Codex conversations with a valid session ID.".into());
    }
    Ok(format!("codex://threads/{id}"))
}

#[cfg(target_os = "macos")]
fn applescript_string(value: &str) -> String {
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\r', "\\r")
            .replace('\n', "\\n")
    )
}

pub fn resume(session: Value, destination: String) -> Result<(), String> {
    require_local(&session)?;
    if !available(&destination) {
        return Err(
            "The selected application is not installed or lacks the required launch API.".into(),
        );
    }
    if destination == "chatgpt" {
        let url = chatgpt_url(&session)?;
        return checked_command(
            "/usr/bin/open",
            &["-b".into(), "com.openai.codex".into(), url],
        );
    }
    let payload = resume_payload(&session)?;
    #[cfg(target_os = "macos")]
    {
        let app = app_path(&destination).ok_or("Application is unavailable.")?;
        let launch = format!("/bin/zsh -lic {}", shell_quote(&payload));
        let script = match destination.as_str() {
            "terminal" => Some(format!(
                "tell application id \"com.apple.Terminal\"\n do script {}\n activate\nend tell",
                applescript_string(&launch)
            )),
            "ghostty" | "ghostree" => {
                let bundle = if destination == "ghostty" {
                    "com.mitchellh.ghostty"
                } else {
                    "dev.sidequery.Ghostree"
                };
                Some(format!(
                    "tell application id \"{bundle}\"\n new window with configuration {{command:{}, wait after command:true}}\n activate\nend tell",
                    applescript_string(&launch)
                ))
            }
            "cmux" => {
                let prefix = format!(
                    "{} new-workspace --window ",
                    shell_quote(&app.join("Contents/Resources/bin/cmux").to_string_lossy())
                );
                let suffix = format!(" --command {} --focus true", shell_quote(&launch));
                Some(format!(
                    "tell application id \"com.cmuxterm.app\"\n set resumeWindow to new window\n set resumeWindowID to id of resumeWindow\n activate\nend tell\ndo shell script {} & quoted form of resumeWindowID & {}",
                    applescript_string(&prefix),
                    applescript_string(&suffix)
                ))
            }
            _ => None,
        };
        if let Some(script) = script {
            return checked_command("/usr/bin/osascript", &["-e".into(), script]);
        }
        let mut args = vec![
            "-n".into(),
            "-a".into(),
            app.to_string_lossy().into_owned(),
            "--args".into(),
        ];
        args.extend(terminal_arguments(&destination, "/bin/zsh", payload)?);
        checked_command("/usr/bin/open", &args)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let shell = std::env::var("SHELL")
            .ok()
            .filter(|value| Path::new(value).is_absolute() && Path::new(value).is_file())
            .unwrap_or_else(|| "/bin/sh".into());
        let args = terminal_arguments(&destination, &shell, payload)?;
        let mut child = Command::new(executable(&destination).ok_or("Terminal is unavailable.")?)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| error.to_string())?;
        // Reap the launcher without holding the UI request until its window closes.
        std::thread::spawn(move || {
            let _ = child.wait();
        });
        Ok(())
    }
}

fn decode_percent(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let pair = bytes
                .get(index + 1..index + 3)
                .ok_or("Invalid file URL encoding.")?;
            let digits = std::str::from_utf8(pair).map_err(|_| "Invalid file URL encoding.")?;
            decoded.push(u8::from_str_radix(digits, 16).map_err(|_| "Invalid file URL encoding.")?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| "File references must be UTF-8.".into())
}

fn resolve_reference(session: &Value, reference: &str) -> Result<(PathBuf, usize), String> {
    require_local(session)?;
    let (reference, fragment_line) = if let Some((path, fragment)) = reference.rsplit_once("#L") {
        (
            path,
            Some(
                fragment
                    .split('-')
                    .next()
                    .unwrap_or("")
                    .parse::<usize>()
                    .map_err(|_| "Invalid source line.")?,
            ),
        )
    } else {
        (reference, None)
    };
    let (reference, line) = if fragment_line.is_none() {
        match reference.rsplit_once(':') {
            Some((path, line))
                if !line.is_empty() && line.bytes().all(|byte| byte.is_ascii_digit()) =>
            {
                (
                    path,
                    line.parse::<usize>().map_err(|_| "Invalid source line.")?,
                )
            }
            _ => (reference, 1),
        }
    } else {
        (reference, fragment_line.unwrap_or(1))
    };
    if line == 0 {
        return Err("Source line numbers start at one.".into());
    }
    let reference = if let Some(url) = reference.strip_prefix("file://") {
        let path = url
            .strip_prefix("localhost/")
            .map(|path| format!("/{path}"))
            .unwrap_or_else(|| url.into());
        if !path.starts_with('/') {
            return Err("Only local file URLs can be previewed.".into());
        }
        decode_percent(&path)?
    } else {
        if reference.contains("://") {
            return Err("Only local files can be previewed.".into());
        }
        reference.into()
    };
    if reference.is_empty() || reference.contains('\0') {
        return Err("Invalid source path.".into());
    }
    let path = PathBuf::from(reference);
    let path = if path.is_absolute() {
        path
    } else {
        let cwd = field(session, &["cwd"])
            .filter(|value| !value.contains('\0') && Path::new(value).is_absolute())
            .ok_or(
                "This conversation has no absolute working directory for relative file links.",
            )?;
        Path::new(cwd).join(path)
    };
    Ok((path, line))
}

pub fn source_preview(session: Value, url: String) -> Result<Value, String> {
    let (path, line) = resolve_reference(&session, &url)?;
    // O_NONBLOCK prevents a transcript link to a FIFO from blocking before metadata validation.
    use std::os::unix::fs::OpenOptionsExt;
    let file = File::options()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&path)
        .map_err(|error| error.to_string())?;
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file() || metadata.len() > PREVIEW_LIMIT {
        return Err("Preview requires a regular UTF-8 text file no larger than 5 MB.".into());
    }
    let mut bytes = Vec::new();
    file.take(PREVIEW_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > PREVIEW_LIMIT {
        return Err("The file is larger than 5 MB.".into());
    }
    let text = String::from_utf8(bytes).map_err(|_| "The file is not UTF-8 text.")?;
    let mut start = 0;
    let mut range = None;
    for (index, part) in text.split_inclusive('\n').enumerate() {
        let end = start + part.encode_utf16().count();
        if index + 1 == line {
            range = Some((start, end));
            break;
        }
        start = end;
    }
    if range.is_none()
        && (text.is_empty() && line == 1
            || text.ends_with('\n')
                && line == text.bytes().filter(|byte| *byte == b'\n').count() + 1)
    {
        range = Some((start, start));
    }
    Ok(
        json!({"path": path, "line": line, "text": text, "line_start": range.map(|value| value.0), "line_end": range.map(|value| value.1), "line_exists": range.is_some()}),
    )
}

pub fn reveal(session: Value) -> Result<(), String> {
    require_local(&session)?;
    let reference =
        field(&session, &["source_path", "sourcePath"]).ok_or("No source path is available.")?;
    let (path, _) = resolve_reference(&session, reference)?;
    if !path.exists() {
        return Err("The source file is unavailable.".into());
    }
    #[cfg(target_os = "macos")]
    {
        checked_command(
            "/usr/bin/open",
            &["-R".into(), path.to_string_lossy().into_owned()],
        )
    }
    #[cfg(not(target_os = "macos"))]
    {
        checked_command(
            "xdg-open",
            &[path
                .parent()
                .ok_or("No source directory is available.")?
                .to_string_lossy()
                .into_owned()],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_identity_rejects_every_local_action_before_path_access() {
        let remote = json!({"machine": "nicbook-atm", "cwd": "/tmp", "source_path": "/missing", "resume_command": "true"});
        assert!(
            source_preview(remote.clone(), "/missing".into())
                .unwrap_err()
                .contains("nicbook-atm")
        );
        assert!(reveal(remote.clone()).unwrap_err().contains("nicbook-atm"));
        assert!(
            resume(remote, "missing".into())
                .unwrap_err()
                .contains("nicbook-atm")
        );
    }

    #[test]
    fn resume_uses_resolved_cwd_when_the_recorded_workspace_is_gone() {
        let session = json!({
            "resume_cmd": "claude --resume abc",
            "cwd": "/deleted",
            "resume_cwd": "/fallback"
        });
        assert_eq!(
            resume_payload(&session).unwrap(),
            "cd -- '/fallback' || exit\nclaude --resume abc"
        );
    }

    #[test]
    fn canonical_command_is_preserved_and_cwd_fails_closed() {
        let session = json!({"resume_cmd": "custom --resume 'id'", "cwd": "/tmp/a'b $(echo bad)"});
        let payload = resume_payload(&session).unwrap();
        assert_eq!(
            payload,
            "cd -- '/tmp/a'\\''b $(echo bad)' || exit\ncustom --resume 'id'"
        );
        for id in [
            "alacritty",
            "kitty",
            "wezterm",
            "ghostty",
            "gnome-terminal",
            "konsole",
            "xterm",
        ] {
            let args = terminal_arguments(id, "/bin/sh", payload.clone()).unwrap();
            assert_eq!(args.last(), Some(&payload));
            assert!(args.iter().any(|arg| arg == "/bin/sh"));
        }
        assert!(resume_payload(&json!({"resume_command": "x\u{0}"})).is_err());
        assert!(resume_payload(&json!({"session_id": "abc"})).is_err());
    }

    #[test]
    fn file_references_preserve_host_and_line_boundaries() {
        let session = json!({"cwd": "/project"});
        assert_eq!(
            resolve_reference(&session, "src/file.rs:12").unwrap(),
            (PathBuf::from("/project/src/file.rs"), 12)
        );
        assert_eq!(
            resolve_reference(&session, "file:///tmp/a%20b#L3-L5").unwrap(),
            (PathBuf::from("/tmp/a b"), 3)
        );
        assert!(resolve_reference(&session, "file://peer/tmp/a").is_err());
        assert!(resolve_reference(&session, "https://example.org/a").is_err());
        assert!(resolve_reference(&session, "/tmp/a:0").is_err());
        assert!(resolve_reference(&json!({}), "relative").is_err());
    }

    #[test]
    fn previews_are_bounded_read_only_and_use_qt_utf16_offsets() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("source.txt");
        std::fs::write(&path, "😀\nsecond\n").unwrap();
        let preview = source_preview(json!({}), format!("{}:2", path.display())).unwrap();
        assert_eq!(preview["line_start"], 3);
        assert_eq!(preview["line_end"], 10);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "😀\nsecond\n");
        assert_eq!(
            source_preview(json!({}), format!("{}:4", path.display())).unwrap()["line_exists"],
            false
        );
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(PREVIEW_LIMIT + 1)
            .unwrap();
        assert!(source_preview(json!({}), path.to_string_lossy().into_owned()).is_err());
    }

    #[test]
    fn deep_links_accept_only_local_codex_uuids() {
        let session =
            json!({"source": "codex", "session_id": "12345678-1234-1234-1234-123456789abc"});
        assert_eq!(
            chatgpt_url(&session).unwrap(),
            "codex://threads/12345678-1234-1234-1234-123456789abc"
        );
        assert!(chatgpt_url(&json!({"source": "codex", "session_id": "../other"})).is_err());
    }
}
