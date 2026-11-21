//! Native app transport. Call from a worker: neither sockets nor child processes
//! execute on the QML thread. CLI overrides deliberately bypass the daemon.
use serde_json::{Value, json};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_RESPONSE: u64 = 64 * 1024 * 1024;

pub fn request(request: Value) -> Result<Value, String> {
    request_cancellable(request, Arc::new(AtomicBool::new(false)))
}

pub fn request_cancellable(
    mut request: Value,
    cancelled: Arc<AtomicBool>,
) -> Result<Value, String> {
    check_cancelled(&cancelled)?;
    normalize(&mut request)?;
    let arguments = arguments(&request)?;
    let timeout = if request["op"] == "count" { 10 } else { 60 };
    let deadline = Instant::now() + Duration::from_secs(timeout);
    let root = std::env::var_os("MEMEX_ROOT")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".memex")));
    let explicit_cli = std::env::var_os("MEMEX_CLI");
    // Token scans report advancing progress on stderr and can take longer on a
    // cold cache. Like Swift, run these through the CLI progress watchdog.
    if explicit_cli.is_none() && !(request["op"] == "activity" && request["metric"] == "tokens") {
        #[cfg(unix)]
        if let Some(root) = &root {
            match daemon::request_cancellable(root, &request, deadline, &cancelled) {
                Ok(value) => {
                    check_cancelled(&cancelled)?;
                    return validate_response(&request, value);
                }
                Err(daemon::Failure::Request(message)) => return Err(message),
                Err(daemon::Failure::Unavailable) => {}
            }
        }
    }
    let executable = explicit_cli.map(PathBuf::from).unwrap_or_else(find_cli);
    let mut args = vec!["--no-update-check".into(), "--non-interactive".into()];
    if let Some(root) = root {
        args.push("--root".into());
        args.push(root.to_string_lossy().into_owned());
    }
    args.extend(arguments);
    let value = cli_response(&executable, &args, &request, deadline, &cancelled)?;
    check_cancelled(&cancelled)?;
    validate_response(&request, value)
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<(), String> {
    if cancelled.load(Ordering::Relaxed) {
        Err("Memex request cancelled".into())
    } else {
        Ok(())
    }
}

fn cli_response(
    executable: &Path,
    args: &[String],
    request: &Value,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<Value, String> {
    fn run(
        executable: &Path,
        args: &[String],
        deadline: Instant,
        progress: bool,
        cancelled: &AtomicBool,
    ) -> Result<Value, String> {
        let output = execute_cancellable(executable, args, deadline, progress, cancelled)?;
        serde_json::from_slice(&output).map_err(|e| format!("Invalid Memex response: {e}"))
    }
    let response = run(
        executable,
        args,
        deadline,
        request["op"] == "activity",
        cancelled,
    );
    if request["op"] != "session_page" {
        return response;
    }
    let mut value = match response {
        Err(error)
            if error.contains("--page-info")
                && (error.contains("unexpected argument")
                    || error.contains("unrecognized option")) =>
        {
            let legacy: Vec<_> = args
                .iter()
                .filter(|arg| arg.as_str() != "--page-info")
                .cloned()
                .collect();
            run(executable, &legacy, deadline, false, cancelled)?
        }
        result => result?,
    };
    let rows = value
        .as_array_mut()
        .ok_or("Memex returned an unexpected transcript response")?;
    if !rows.iter().any(|row| row["type"] == "page") {
        // Older CLI overrides may accept but not emit --page-info. Probe the
        // same source/machine scope with a one-character metadata budget.
        let mut metadata: Vec<_> = args
            .iter()
            .filter(|arg| {
                arg.as_str() != "--page-info"
                    && arg.as_str() != "--full"
                    && !arg.starts_with("--limit=")
            })
            .cloned()
            .collect();
        let position = metadata
            .iter()
            .position(|arg| arg == "--")
            .unwrap_or(metadata.len());
        metadata.splice(
            position..position,
            ["--limit=1".into(), "--max-chars=1".into()],
        );
        let page = run(executable, &metadata, deadline, false, cancelled)?;
        let page = page
            .as_array()
            .and_then(|rows| rows.iter().rfind(|row| row["type"] == "page"))
            .ok_or("Memex did not return transcript pagination metadata")?;
        if page["total"].as_u64().is_none() {
            return Err("Memex returned an invalid transcript total".into());
        }
        rows.push(page.clone());
    }
    if rows
        .iter()
        .rfind(|row| row["type"] == "page")
        .is_none_or(|page| page["total"].as_u64().is_none())
    {
        return Err("Memex returned an invalid transcript total".into());
    }
    Ok(value)
}

fn find_cli() -> PathBuf {
    let mut candidates = Vec::new();
    if let Ok(app) = std::env::current_exe()
        && let Some(parent) = app.parent()
    {
        candidates.push(parent.join("memex"));
        candidates.push(parent.join("../Helpers/memex"));
    }
    if let Some(path) = std::env::var_os("PATH") {
        candidates.extend(std::env::split_paths(&path).map(|p| p.join("memex")));
    }
    candidates.extend([
        PathBuf::from("/opt/homebrew/bin/memex"),
        PathBuf::from("/usr/local/bin/memex"),
    ]);
    candidates
        .into_iter()
        .find(|p| {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                p.metadata()
                    .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            }
            #[cfg(not(unix))]
            {
                p.is_file()
            }
        })
        .unwrap_or_else(|| PathBuf::from("memex"))
}

fn normalize(request: &mut Value) -> Result<(), String> {
    let object = request.as_object_mut().ok_or("Request must be an object")?;
    let op = object
        .get("op")
        .and_then(Value::as_str)
        .ok_or("Request has no operation")?
        .to_owned();
    if op == "resume_details" {
        let mut filters = json!({"limit": 1, "origin": "all"});
        for key in ["session_id", "source_path", "source"] {
            filters[key] = object.remove(key).ok_or_else(|| format!("Missing {key}"))?;
        }
        object.insert("op".into(), json!("sessions"));
        object.insert("filters".into(), filters);
    }
    if op != "machines" {
        object.entry("machine").or_insert(json!("local"));
    }
    match object["op"].as_str().unwrap_or_default() {
        "sessions" | "count" => {
            let filters = object.entry("filters").or_insert(json!({}));
            let filters = filters.as_object_mut().ok_or("Filters must be an object")?;
            filters.entry("origin").or_insert(json!("all"));
            filters.entry("limit").or_insert(json!(200));
        }
        "search" => {
            object.entry("origin").or_insert(json!("all"));
            object.entry("limit").or_insert(json!(200));
        }
        "session" | "session_page" => {
            object.entry("offset").or_insert(json!(0));
            object.entry("limit").or_insert(json!(60));
        }
        "activity" => {
            object.entry("origin").or_insert(json!("all"));
            object.entry("now_ms").or_insert(json!(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64
            ));
        }
        _ => {}
    }
    Ok(())
}

fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    value[key]
        .as_str()
        .ok_or_else(|| format!("Missing or invalid {key}"))
}

fn option(args: &mut Vec<String>, value: &Value, key: &str) -> Result<(), String> {
    if !value[key].is_null() {
        let value = value[key]
            .as_str()
            .map(str::to_owned)
            .or_else(|| value[key].as_u64().map(|v| v.to_string()))
            .ok_or_else(|| format!("Invalid {key}"))?;
        // Equals syntax keeps leading dashes and arbitrary user content literal.
        args.push(format!("--{}={value}", key.replace('_', "-")));
    }
    Ok(())
}

fn arguments(request: &Value) -> Result<Vec<String>, String> {
    let op = text(request, "op")?;
    let command = match op {
        "count" => "sessions",
        "session_page" => "session",
        "machines" | "projects" | "sessions" | "search" | "session" | "activity" => op,
        _ => return Err(format!("Unsupported Memex operation: {op}")),
    };
    let mut args = vec![command.into(), "--format=json".into()];
    if op != "machines" {
        option(&mut args, request, "machine")?;
    }
    match op {
        "sessions" | "count" => {
            for key in [
                "project",
                "source",
                "since",
                "origin",
                "session_id",
                "source_path",
                "cwd",
            ] {
                option(&mut args, &request["filters"], key)?;
            }
            if op == "count" {
                args.push("--count".into());
                option(&mut args, request, "query")?;
            } else {
                option(&mut args, &request["filters"], "limit")?;
            }
        }
        "search" => {
            args.extend(["--mode=lexical".into(), "--unique-session".into(), "--fields=source,session_id,source_path,project,snippet,ts,machine,record_id,conversation_kind".into()]);
            for key in ["project", "source", "since", "origin", "limit"] {
                option(&mut args, request, key)?;
            }
            args.extend(["--".into(), text(request, "query")?.into()]);
        }
        "session" | "session_page" => {
            text(request, "source_path")?;
            for key in ["source_path", "offset", "limit"] {
                option(&mut args, request, key)?;
            }
            if op == "session_page" {
                args.push("--page-info".into());
            }
            if !request["max_chars"].is_null() && op == "session" {
                option(&mut args, request, "max_chars")?;
            } else {
                args.push("--full".into());
            }
            args.extend(["--".into(), text(request, "session_id")?.into()]);
        }
        "activity" => {
            text(request, "metric")?;
            text(request, "range")?;
            args.extend(["--raw".into(), "--progress".into()]);
            for key in [
                "metric", "range", "origin", "now_ms", "query", "project", "source",
            ] {
                option(&mut args, request, key)?;
            }
        }
        _ => {}
    }
    Ok(args)
}

fn validate_response(request: &Value, value: Value) -> Result<Value, String> {
    let op = text(request, "op")?;
    // Keep the same required and optional fields as Swift's decoded models.
    // Validate before handing JSON to the store, which enriches object rows.
    let validate = || -> Result<(), String> {
        match op {
            "count" => {
                string_fields(&value, &[], &[])?;
                field(&value, "total", false, nonnegative_integer)?;
            }
            "activity" => {
                string_fields(&value, &[], &[])?;
                field(&value, "token_usage_enabled", true, Value::is_boolean)?;
                field(&value, "partial", true, Value::is_boolean)?;
                field(&value, "warnings", false, |v| {
                    v.as_array()
                        .is_some_and(|rows| rows.iter().all(Value::is_string))
                })?;
                let points = value["points"]
                    .as_array()
                    .ok_or("points must be an array")?;
                for point in points {
                    string_fields(point, &["source"], &[])?;
                    field(point, "timestamp_ms", true, |v| v.as_u64().is_some())?;
                    field(point, "value", true, |v| {
                        v.as_f64().is_some_and(|n| n.is_finite() && n >= 0.0)
                    })?;
                }
            }
            "machines" | "projects" | "sessions" | "search" | "session" | "session_page" => {
                let rows = value.as_array().ok_or("expected an array")?;
                let mut pages = 0;
                for row in rows {
                    match op {
                        "machines" => string_fields(row, &["id", "label"], &[])?,
                        "projects" => {
                            string_fields(row, &["project"], &["last_at"])?;
                            field(row, "session_count", true, nonnegative_integer)?;
                        }
                        "sessions" | "search" => {
                            string_fields(
                                row,
                                &["source", "session_id", "source_path", "project"],
                                &[
                                    "label",
                                    "last_at",
                                    "resume_cmd",
                                    "resume_cwd",
                                    "cwd",
                                    "snippet",
                                    "repo_project",
                                    "machine",
                                    "search_record_id",
                                    "record_id",
                                    "ts",
                                    "conversation_kind",
                                ],
                            )?;
                            field(row, "message_count", false, nonnegative_integer)?;
                        }
                        "session" | "session_page" => {
                            string_fields(row, &[], &["type", "machine"])?;
                            if row["type"] == "page" {
                                pages += 1;
                                field(row, "total", true, nonnegative_integer)?;
                                for key in ["offset", "next_offset"] {
                                    field(row, key, false, nonnegative_integer)?;
                                }
                                string_fields(row, &[], &["session_id", "source_path"])?;
                            } else {
                                string_fields(row, &["record_id"], &[])?;
                                string_fields(
                                    &row["record"],
                                    &["role", "text"],
                                    &[
                                        "tool_name",
                                        "tool_input",
                                        "tool_output",
                                        "event_id",
                                        "parent_tool_use_id",
                                        "source_turn_id",
                                        "assistant_phase",
                                        "lifecycle_event",
                                        "source_record_type",
                                        "source_content",
                                    ],
                                )?;
                                field(
                                    &row["record"],
                                    "tool_result_is_error",
                                    false,
                                    Value::is_boolean,
                                )?;
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                if op == "session_page" && pages == 0 {
                    return Err("missing transcript pagination metadata".into());
                }
            }
            _ => return Err(format!("unsupported operation {op}")),
        }
        Ok(())
    };
    if let Err(error) = validate() {
        return Err(format!("Invalid Memex {op} response: {error}"));
    }
    Ok(value)
}

fn nonnegative_integer(value: &Value) -> bool {
    value.as_i64().is_some_and(|number| number >= 0)
}

fn field(
    value: &Value,
    key: &str,
    required: bool,
    valid: impl FnOnce(&Value) -> bool,
) -> Result<(), String> {
    let member = &value[key];
    if (!required && member.is_null()) || valid(member) {
        Ok(())
    } else {
        Err(format!("missing or invalid {key}"))
    }
}

fn string_fields(value: &Value, required: &[&str], optional: &[&str]) -> Result<(), String> {
    if !value.is_object() {
        return Err("expected an object".into());
    }
    for key in required {
        field(value, key, true, Value::is_string)?;
    }
    for key in optional {
        field(value, key, false, Value::is_string)?;
    }
    Ok(())
}

fn execute_cancellable(
    executable: &Path,
    args: &[String],
    mut deadline: Instant,
    progress: bool,
    cancelled: &AtomicBool,
) -> Result<Vec<u8>, String> {
    check_cancelled(cancelled)?;
    if Instant::now() >= deadline {
        return Err("Memex took too long to respond. Try again.".into());
    }
    let watchdog = deadline.saturating_duration_since(Instant::now());
    let mut stdout = tempfile::tempfile().map_err(|e| e.to_string())?;
    let mut stderr = tempfile::tempfile().map_err(|e| e.to_string())?;
    let mut command = Command::new(executable);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(stdout.try_clone().map_err(|e| e.to_string())?)
        .stderr(stderr.try_clone().map_err(|e| e.to_string())?);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command.spawn().map_err(|e| {
        format!(
            "Could not start Memex CLI ({}): {e}. Set MEMEX_CLI to a built memex executable.",
            executable.display()
        )
    })?;
    let mut progress_offset = 0;
    let mut progress_buffer = String::new();
    let mut counters = std::collections::HashMap::new();
    let status = loop {
        if let Err(error) = check_cancelled(cancelled) {
            stop_child(&mut child);
            return Err(error);
        }
        if progress {
            // Cloned file descriptors share offsets, so use positional reads.
            let mut bytes = [0; 65536];
            #[cfg(unix)]
            let read = {
                use std::os::unix::fs::FileExt;
                stderr.read_at(&mut bytes, progress_offset)
            };
            #[cfg(not(unix))]
            let read: std::io::Result<usize> = Ok(0);
            if let Ok(count) = read {
                progress_offset += count as u64;
                progress_buffer.push_str(&String::from_utf8_lossy(&bytes[..count]));
                while let Some(end) = progress_buffer.find('\n') {
                    let line: String = progress_buffer.drain(..=end).collect();
                    if let Some(raw) = line.strip_prefix("MEMEX_PROGRESS ")
                        && let Ok(update) = serde_json::from_str::<Value>(raw)
                        && let (Some(source), Some(done), Some(total)) = (
                            update["source"].as_str(),
                            update["done"].as_u64(),
                            update["total"].as_u64(),
                        )
                        && !source.is_empty()
                        && source.len() <= 100
                        && total > 0
                        && done <= total
                        && done > *counters.get(source).unwrap_or(&0)
                    {
                        counters.insert(source.to_owned(), done);
                        deadline = Instant::now() + watchdog;
                    }
                }
                if progress_buffer.len() > 65536 {
                    progress_buffer.clear();
                }
            }
        }
        let oversized = [&stdout, &stderr]
            .iter()
            .any(|file| file.metadata().map_or(true, |m| m.len() > MAX_RESPONSE));
        let failure = if oversized {
            Some("Memex response exceeds 64 MiB")
        } else if Instant::now() >= deadline {
            Some("Memex took too long to respond. Try again.")
        } else {
            None
        };
        if let Some(message) = failure {
            stop_child(&mut child);
            return Err(message.into());
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(error) => {
                stop_child(&mut child);
                return Err(error.to_string());
            }
        }
    };
    fn read(file: &mut File) -> Result<Vec<u8>, String> {
        file.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
        let mut bytes = Vec::new();
        file.take(MAX_RESPONSE + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        if bytes.len() as u64 > MAX_RESPONSE {
            return Err("Memex response exceeds 64 MiB".into());
        }
        Ok(bytes)
    }
    if !status.success() {
        let bytes = read(&mut stderr)?;
        let message = String::from_utf8_lossy(&bytes)
            .lines()
            .filter(|s| !s.starts_with("MEMEX_PROGRESS "))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(if message.trim().is_empty() {
            format!("Memex exited with {status}")
        } else {
            message.chars().take(4000).collect()
        });
    }
    read(&mut stdout)
}

fn stop_child(child: &mut std::process::Child) {
    #[cfg(unix)]
    // SAFETY: process_group(0) above created a new child-owned group. Killing
    // that group also terminates SSH descendants which inherited spool files.
    unsafe {
        libc::kill(-(child.id() as i32), libc::SIGKILL);
    }
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(unix)]
mod daemon {
    use super::*;
    use std::io::{self, Write};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    use std::os::unix::net::UnixStream;

    #[derive(Debug)]
    pub(super) enum Failure {
        Unavailable,
        Request(String),
    }
    impl From<io::Error> for Failure {
        fn from(_: io::Error) -> Self {
            Self::Unavailable
        }
    }
    impl From<serde_json::Error> for Failure {
        fn from(_: serde_json::Error) -> Self {
            Self::Unavailable
        }
    }

    #[cfg(test)]
    pub(super) fn request(
        root: &Path,
        request: &Value,
        deadline: Instant,
    ) -> Result<Value, Failure> {
        request_cancellable(root, request, deadline, &AtomicBool::new(false))
    }

    pub(super) fn request_cancellable(
        root: &Path,
        request: &Value,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Value, Failure> {
        let root = fs::canonicalize(root)?;
        let path = root.join("state/native/app.sock");
        for (entry, socket) in [(path.as_path(), true), (path.parent().unwrap(), false)] {
            let metadata = fs::symlink_metadata(entry)?;
            // SAFETY: geteuid has no memory preconditions.
            if metadata.uid() != unsafe { libc::geteuid() }
                || metadata.mode() & 0o077 != 0
                || (socket && !metadata.file_type().is_socket())
                || (!socket && !metadata.is_dir())
            {
                return Err(Failure::Unavailable);
            }
        }
        let handshake_deadline = deadline.min(Instant::now() + Duration::from_millis(350));
        let mut stream = connect(&path, handshake_deadline, cancelled)?;
        let hello = exchange(
            &mut stream,
            &json!({"op":"hello"}),
            handshake_deadline,
            cancelled,
        )?;
        if hello["root"].as_str() != root.to_str()
            || !hello["capabilities"]
                .as_array()
                .is_some_and(|ops| ops.contains(&request["op"]))
        {
            return Err(Failure::Unavailable);
        }
        exchange(&mut stream, request, deadline, cancelled)
    }

    fn connect(
        path: &Path,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<UnixStream, Failure> {
        check_cancelled(cancelled).map_err(Failure::Request)?;
        // SAFETY: zero is a valid initialization for sockaddr_un; all fields
        // used by connect are set below. The owned stream closes on every exit.
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        let bytes = path.as_os_str().as_bytes();
        if bytes.len() >= address.sun_path.len() {
            return Err(Failure::Unavailable);
        }
        address.sun_family = libc::AF_UNIX as _;
        for (target, byte) in address.sun_path.iter_mut().zip(bytes) {
            *target = *byte as _;
        }
        let length = std::mem::size_of_val(&address) as libc::socklen_t;
        #[cfg(target_os = "macos")]
        {
            address.sun_len = length as u8;
        }
        // SAFETY: socket returns a fresh descriptor or -1.
        let descriptor = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        if descriptor < 0 {
            return Err(Failure::Unavailable);
        }
        // SAFETY: descriptor is valid and exclusively owned here.
        let stream = unsafe { UnixStream::from_raw_fd(descriptor) };
        stream.set_nonblocking(true)?;
        // SAFETY: descriptor is live; F_SETFD does not access memory.
        if unsafe { libc::fcntl(descriptor, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(Failure::Unavailable);
        }
        #[cfg(target_os = "macos")]
        {
            let enabled: libc::c_int = 1;
            // SAFETY: enabled is a valid integer option for the owned socket.
            if unsafe {
                libc::setsockopt(
                    descriptor,
                    libc::SOL_SOCKET,
                    libc::SO_NOSIGPIPE,
                    (&enabled as *const libc::c_int).cast(),
                    std::mem::size_of_val(&enabled) as libc::socklen_t,
                )
            } != 0
            {
                return Err(Failure::Unavailable);
            }
        }
        // SAFETY: address is initialized and length matches its allocation.
        if unsafe {
            libc::connect(
                descriptor,
                (&address as *const libc::sockaddr_un).cast(),
                length,
            )
        } != 0
        {
            let error = io::Error::last_os_error();
            if !matches!(error.raw_os_error(), Some(libc::EINPROGRESS | libc::EAGAIN)) {
                return Err(Failure::Unavailable);
            }
            wait(&stream, libc::POLLOUT, deadline, cancelled)?;
            if stream.take_error()?.is_some() {
                return Err(Failure::Unavailable);
            }
        }
        #[cfg(target_os = "linux")]
        {
            // SAFETY: credentials and length point to valid writable storage.
            let mut credentials: libc::ucred = unsafe { std::mem::zeroed() };
            let mut length = std::mem::size_of_val(&credentials) as libc::socklen_t;
            if unsafe {
                libc::getsockopt(
                    descriptor,
                    libc::SOL_SOCKET,
                    libc::SO_PEERCRED,
                    (&mut credentials as *mut libc::ucred).cast(),
                    &mut length,
                )
            } != 0
                || credentials.uid != unsafe { libc::geteuid() }
            {
                return Err(Failure::Unavailable);
            }
        }
        #[cfg(target_os = "macos")]
        {
            let (mut uid, mut gid) = (0, 0);
            // SAFETY: both output pointers are valid, descriptor is connected.
            if unsafe { libc::getpeereid(descriptor, &mut uid, &mut gid) } != 0
                || uid != unsafe { libc::geteuid() }
            {
                return Err(Failure::Unavailable);
            }
        }
        Ok(stream)
    }

    fn wait(
        stream: &UnixStream,
        events: i16,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<(), Failure> {
        loop {
            check_cancelled(cancelled).map_err(Failure::Request)?;
            if Instant::now() >= deadline {
                return Err(Failure::Unavailable);
            }
            let mut item = libc::pollfd {
                fd: stream.as_raw_fd(),
                events,
                revents: 0,
            };
            // SAFETY: item is a valid pollfd for the live stream.
            let result = unsafe { libc::poll(&mut item, 1, 20) };
            if result > 0 {
                return if item.revents & events != 0 {
                    Ok(())
                } else {
                    Err(Failure::Unavailable)
                };
            }
            if result < 0 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                return Err(Failure::Unavailable);
            }
        }
    }

    fn exchange(
        stream: &mut UnixStream,
        request: &Value,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Value, Failure> {
        let body = serde_json::to_vec(&json!({"protocol":1,"request":request}))?;
        if body.len() > 1024 * 1024 {
            return Err(Failure::Unavailable);
        }
        let mut frame = (body.len() as u32).to_be_bytes().to_vec();
        frame.extend(body);
        let mut offset = 0;
        while offset < frame.len() {
            wait(stream, libc::POLLOUT, deadline, cancelled)?;
            match stream.write(&frame[offset..]) {
                Ok(0) => return Err(Failure::Unavailable),
                Ok(count) => offset += count,
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) => {}
                Err(e) => return Err(e.into()),
            }
        }
        fn read(
            stream: &mut UnixStream,
            bytes: &mut [u8],
            deadline: Instant,
            cancelled: &AtomicBool,
        ) -> Result<(), Failure> {
            let mut offset = 0;
            while offset < bytes.len() {
                wait(stream, libc::POLLIN, deadline, cancelled)?;
                match stream.read(&mut bytes[offset..]) {
                    Ok(0) => return Err(Failure::Unavailable),
                    Ok(count) => offset += count,
                    Err(e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                        ) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            Ok(())
        }
        let mut header = [0; 4];
        read(stream, &mut header, deadline, cancelled)?;
        let size = u32::from_be_bytes(header) as usize;
        if size == 0 || size as u64 > MAX_RESPONSE {
            return Err(Failure::Unavailable);
        }
        let mut body = vec![0; size];
        read(stream, &mut body, deadline, cancelled)?;
        let envelope: Value = serde_json::from_slice(&body)?;
        if envelope["protocol"] != 1 {
            return Err(Failure::Unavailable);
        }
        if envelope["error"]["code"] == "request_failed" {
            return Err(Failure::Request(
                envelope["error"]["message"]
                    .as_str()
                    .unwrap_or("Memex request failed")
                    .into(),
            ));
        }
        envelope.get("result").cloned().ok_or(Failure::Unavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn execute(
        executable: &Path,
        args: &[String],
        deadline: Instant,
        progress: bool,
    ) -> Result<Vec<u8>, String> {
        execute_cancellable(
            executable,
            args,
            deadline,
            progress,
            &AtomicBool::new(false),
        )
    }

    #[test]
    fn arbitrary_queries_and_session_ids_are_literal_arguments() {
        let mut query =
            json!({"op":"search", "query":"--help; $(touch /tmp/never)", "project":"-project"});
        normalize(&mut query).unwrap();
        let args = arguments(&query).unwrap();
        assert_eq!(
            &args[args.len() - 2..],
            &["--", "--help; $(touch /tmp/never)"]
        );
        assert!(args.contains(&"--project=-project".into()));
        let mut page = json!({"op":"session_page","session_id":"-session","source_path":"-path"});
        normalize(&mut page).unwrap();
        let args = arguments(&page).unwrap();
        assert!(args.contains(&"--page-info".into()));
        assert!(args.contains(&"--full".into()));
        assert_eq!(&args[args.len() - 2..], &["--", "-session"]);
    }

    #[test]
    fn every_native_operation_has_cli_fallback() {
        for mut request in [
            json!({"op":"machines"}),
            json!({"op":"projects"}),
            json!({"op":"sessions"}),
            json!({"op":"count","query":"hi"}),
            json!({"op":"activity","metric":"tokens","range":"week"}),
            json!({"op":"session","session_id":"x","source_path":"/x","max_chars":1}),
            json!({"op":"resume_details","session_id":"x","source_path":"/x","source":"codex"}),
        ] {
            normalize(&mut request).unwrap();
            assert!(arguments(&request).is_ok(), "{request}");
        }
        assert!(arguments(&json!({"op":"delete"})).is_err());
    }

    #[test]
    fn response_validation_preserves_optional_fields_and_unknown_metadata() {
        let session = json!({"source":"codex","session_id":"s","source_path":"/s","project":"","machine":null,"message_count":null,"label":null,"future_field":{"x":1}});
        let record = json!({"record_id":"r","record":{"role":"assistant","text":"","tool_input":null,"tool_result_is_error":null,"source_content":null},"content":{"truncated":false}});
        for (op, response) in [
            ("machines", json!([{"id":"local","label":"This computer"}])),
            (
                "projects",
                json!([{"project":"","session_count":0,"last_at":null}]),
            ),
            ("sessions", json!([session.clone()])),
            ("search", json!([session])),
            ("session", json!([record.clone()])),
            (
                "session_page",
                json!([record,{"type":"page","total":1,"next_offset":null}]),
            ),
            ("count", json!({"total":0})),
            ("count", json!({"total":null})),
            ("count", json!({})),
            (
                "activity",
                json!({"token_usage_enabled":false,"partial":true,"warnings":null,"points":[{"timestamp_ms":0,"source":"codex","value":0.5}]}),
            ),
        ] {
            assert_eq!(
                validate_response(&json!({"op":op}), response.clone()).unwrap(),
                response
            );
        }
    }

    #[test]
    fn malformed_response_rows_and_fields_fail_before_store_mutation() {
        let session = json!({"source":"codex","session_id":"s","source_path":"/s","project":"p"});
        let record = json!({"record_id":"r","record":{"role":"user","text":"hi"}});
        for op in [
            "machines",
            "projects",
            "sessions",
            "search",
            "session",
            "session_page",
        ] {
            for response in [
                json!([1]),
                json!([[]]),
                json!([null]),
                json!([{}]),
                json!({}),
            ] {
                assert!(
                    validate_response(&json!({"op":op}), response.clone()).is_err(),
                    "{op}: {response}"
                );
            }
        }
        let mut invalid_session = session.clone();
        invalid_session["machine"] = json!([]);
        let mut invalid_count = session;
        invalid_count["message_count"] = json!(-1);
        let mut invalid_record = record.clone();
        invalid_record["record"]["tool_input"] = json!({"command":"ls"});
        let mut invalid_flag = record.clone();
        invalid_flag["record"]["tool_result_is_error"] = json!("false");
        for (op, response) in [
            ("machines", json!([{"id":"local","label":null}])),
            ("projects", json!([{"project":"p","session_count":-1}])),
            ("sessions", json!([invalid_session])),
            ("sessions", json!([invalid_count])),
            (
                "search",
                json!([{"source":"codex","session_id":"s","source_path":"/s"}]),
            ),
            ("session", json!([invalid_record])),
            ("session", json!([invalid_flag])),
            ("session", json!([{"record_id":"r","record":[]}])),
            ("session_page", json!([record])),
            ("session_page", json!([{"type":"page","total":-1}])),
            (
                "session_page",
                json!([{"type":"page","total":2,"next_offset":-1}]),
            ),
            ("count", json!({"total":-1})),
            ("count", json!({"total":"2"})),
            ("count", json!({"total":1.5})),
            ("activity", json!({"points":[]})),
            (
                "activity",
                json!({"token_usage_enabled":true,"partial":false,"points":[2]}),
            ),
            (
                "activity",
                json!({"token_usage_enabled":true,"partial":false,"points":[{"timestamp_ms":-1,"source":"codex","value":1}]}),
            ),
            (
                "activity",
                json!({"token_usage_enabled":true,"partial":false,"points":[{"timestamp_ms":1,"source":"codex","value":-1}]}),
            ),
            (
                "activity",
                json!({"token_usage_enabled":true,"partial":false,"points":[{"timestamp_ms":1,"source":[],"value":1}]}),
            ),
            (
                "activity",
                json!({"token_usage_enabled":true,"partial":false,"points":[],"warnings":[1]}),
            ),
        ] {
            assert!(
                validate_response(&json!({"op":op}), response.clone()).is_err(),
                "{op}: {response}"
            );
        }
    }

    #[cfg(unix)]
    fn fixture(body: &str) -> (tempfile::TempDir, PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fixture");
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        (directory, path)
    }

    #[cfg(unix)]
    #[test]
    fn subprocess_drains_large_output_and_reports_stderr() {
        let (_directory, path) = fixture(
            "printf '[\\\"'; dd if=/dev/zero bs=1024 count=256 2>/dev/null | tr '\\000' x; printf '\\\"]'",
        );
        let output = execute(&path, &[], Instant::now() + Duration::from_secs(5), false).unwrap();
        assert!(output.len() > 256 * 1024);
        let (_directory, path) =
            fixture("printf 'MEMEX_PROGRESS {}\\nactual failure\\n' >&2; exit 3");
        assert_eq!(
            execute(&path, &[], Instant::now() + Duration::from_secs(5), false).unwrap_err(),
            "actual failure"
        );
    }

    #[cfg(unix)]
    #[test]
    fn timeout_kills_child_group_and_reaps_process() {
        let (directory, path) = fixture("echo $$ > \"$1\"; trap '' TERM; sleep 30 & wait");
        let pid_file = directory.path().join("pid");
        let started = Instant::now();
        let error = execute(
            &path,
            &[pid_file.to_string_lossy().into_owned()],
            started + Duration::from_secs(3),
            false,
        )
        .unwrap_err();
        assert!(error.contains("too long"));
        assert!(started.elapsed() < Duration::from_secs(6));
        let pid: i32 = fs::read_to_string(pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        // SAFETY: signal zero only checks whether this fixture process exists.
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }

    #[cfg(unix)]
    #[test]
    fn cancellation_stops_advancing_activity() {
        let (directory, path) = fixture(
            r#"echo $$ > "$1"
i=1
while :; do
    printf 'MEMEX_PROGRESS {"source":"codex","done":%s,"total":10000}\n' "$i" >&2
    i=$((i + 1))
    sleep 0.05
done"#,
        );
        let cancelled = Arc::new(AtomicBool::new(false));
        let signal = cancelled.clone();
        let pid_file = directory.path().join("pid");
        let observed_pid = pid_file.clone();
        let cancel = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !observed_pid.exists() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(20));
            }
            assert!(observed_pid.exists(), "activity fixture did not start");
            thread::sleep(Duration::from_millis(200));
            signal.store(true, Ordering::Relaxed);
        });
        let started = Instant::now();
        let error = execute_cancellable(
            &path,
            &[pid_file.to_string_lossy().into_owned()],
            started + Duration::from_secs(10),
            true,
            &cancelled,
        )
        .unwrap_err();
        assert_eq!(error, "Memex request cancelled");
        assert!(started.elapsed() < Duration::from_secs(8));
        cancel.join().unwrap();
        assert_eq!(
            request_cancellable(json!({"op":"machines"}), cancelled).unwrap_err(),
            "Memex request cancelled"
        );
    }

    #[cfg(unix)]
    #[test]
    fn legacy_transcript_page_recovers_missing_option_and_missing_marker() {
        for reject_option in [true, false] {
            let script = format!(
                r#"metadata=false
for arg in "$@"; do
    case "$arg" in
        --max-chars=1) metadata=true ;;
        --page-info) if {reject_option}; then printf 'unexpected argument --page-info\n' >&2; exit 2; fi ;;
    esac
done
if "$metadata"; then
    printf '[{{"type":"page","total":42,"next_offset":11}}]'
else
    printf '[{{"record_id":"r","record":{{"role":"user","text":"full transcript content"}}}}]'
fi"#
            );
            let (_directory, path) = fixture(&script);
            let mut request =
                json!({"op":"session_page","session_id":"s","source_path":"/s","offset":10});
            normalize(&mut request).unwrap();
            let args = arguments(&request).unwrap();
            let result = cli_response(
                &path,
                &args,
                &request,
                Instant::now() + Duration::from_secs(5),
                &AtomicBool::new(false),
            )
            .unwrap();
            assert_eq!(result[0]["record"]["text"], "full transcript content");
            assert_eq!(result[1]["type"], "page");
            assert_eq!(result[1]["total"], 42);
            validate_response(&request, result).unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn daemon_rejects_public_socket_and_bounds_unresponsive_handshake() {
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::net::UnixListener;
        let root = tempfile::Builder::new()
            .prefix("mq-")
            .tempdir_in("/tmp")
            .unwrap();
        let directory = root.path().join("state/native");
        fs::create_dir_all(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.join("app.sock");
        let listener = UnixListener::bind(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
        assert!(matches!(
            daemon::request(
                root.path(),
                &json!({"op":"machines"}),
                Instant::now() + Duration::from_secs(2)
            ),
            Err(daemon::Failure::Unavailable)
        ));
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let (connected, accepted) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                connected.send(()).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut bytes = Vec::new();
                stream.read_to_end(&mut bytes).unwrap();
            }
        });
        let started = Instant::now();
        assert!(matches!(
            daemon::request(
                root.path(),
                &json!({"op":"machines"}),
                started + Duration::from_secs(2)
            ),
            Err(daemon::Failure::Unavailable)
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        accepted.recv_timeout(Duration::from_secs(2)).unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let signal = cancelled.clone();
        let cancel = thread::spawn(move || {
            accepted.recv_timeout(Duration::from_secs(2)).unwrap();
            signal.store(true, Ordering::Relaxed);
        });
        let started = Instant::now();
        assert!(matches!(
            daemon::request_cancellable(root.path(), &json!({"op":"machines"}), started + Duration::from_secs(5), &cancelled),
            Err(daemon::Failure::Request(message)) if message == "Memex request cancelled"
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        cancel.join().unwrap();
        server.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn daemon_verifies_handshake_before_sending_private_request() {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::net::UnixListener;
        for correct_root in [true, false] {
            let root = tempfile::Builder::new()
                .prefix("mq-")
                .tempdir_in("/tmp")
                .unwrap();
            let directory = root.path().join("state/native");
            fs::create_dir_all(&directory).unwrap();
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
            let path = directory.join("app.sock");
            let listener = UnixListener::bind(&path).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            let expected_root = fs::canonicalize(root.path()).unwrap();
            let server = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                fn receive(stream: &mut impl Read) -> Value {
                    let mut header = [0; 4];
                    stream.read_exact(&mut header).unwrap();
                    let mut body = vec![0; u32::from_be_bytes(header) as usize];
                    stream.read_exact(&mut body).unwrap();
                    serde_json::from_slice(&body).unwrap()
                }
                fn reply(stream: &mut impl Write, result: Value) {
                    let bytes = serde_json::to_vec(&json!({"protocol":1,"result":result})).unwrap();
                    stream
                        .write_all(&(bytes.len() as u32).to_be_bytes())
                        .unwrap();
                    stream.write_all(&bytes).unwrap();
                }
                assert_eq!(receive(&mut stream)["request"]["op"], "hello");
                reply(
                    &mut stream,
                    json!({"root":if correct_root {expected_root} else {PathBuf::from("/wrong")},"capabilities":["machines"]}),
                );
                if correct_root {
                    assert_eq!(receive(&mut stream)["request"]["op"], "machines");
                    reply(&mut stream, json!([{"id":"local"}]));
                } else {
                    let mut byte = [0];
                    assert_eq!(stream.read(&mut byte).unwrap(), 0);
                }
            });
            let response = daemon::request(
                root.path(),
                &json!({"op":"machines"}),
                Instant::now() + Duration::from_secs(2),
            );
            if correct_root {
                assert_eq!(response.unwrap(), json!([{"id":"local"}]));
            } else {
                assert!(matches!(response, Err(daemon::Failure::Unavailable)));
            }
            server.join().unwrap();
        }
    }
}
