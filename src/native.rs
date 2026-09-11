//! Private, root-scoped IPC for the native app. This listener never owns indexing.
use crate::cli::{SessionOrigin, SessionsRequest};
use crate::config::Paths;
use anyhow::{Context, Result, anyhow, ensure};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{Read, Write};
use std::net::Shutdown;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const PROTOCOL: u32 = 1;
const MAX_REQUEST: usize = 1024 * 1024;
const MAX_RESPONSE: usize = 64 * 1024 * 1024;
const MAX_CONNECTIONS: usize = 16;
const IO_TIMEOUT: Duration = Duration::from_secs(120);
const CAPABILITIES: &[&str] = &[
    "machines",
    "activity",
    "projects",
    "sessions",
    "count",
    "search",
    "session",
    "session_page",
];

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Operation {
    Hello {},
    Machines {},
    Activity {
        machine: String,
        metric: String,
        range: String,
        query: Option<String>,
        project: Option<String>,
        source: Option<String>,
        origin: SessionOrigin,
        now_ms: u64,
    },
    Projects {
        machine: String,
    },
    Sessions {
        machine: String,
        filters: SessionsRequest,
    },
    Count {
        machine: String,
        filters: SessionsRequest,
        query: Option<String>,
    },
    Search {
        machine: String,
        query: String,
        project: Option<String>,
        source: Option<String>,
        since: Option<String>,
        origin: SessionOrigin,
        limit: usize,
    },
    SessionPage {
        machine: String,
        session_id: String,
        source_path: String,
        offset: usize,
        limit: usize,
    },
    Session {
        machine: String,
        session_id: String,
        source_path: String,
        offset: usize,
        limit: usize,
        max_chars: Option<usize>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    protocol: u32,
    request: Value,
}

#[derive(Debug)]
pub(crate) struct Unavailable(pub &'static str);
impl std::fmt::Display for Unavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Unavailable {}

type Connections = Arc<Mutex<HashMap<u64, UnixStream>>>;

pub(crate) struct Server {
    stop: Arc<AtomicBool>,
    connections: Connections,
    listener_thread: Option<JoinHandle<()>>,
    socket: PathBuf,
    identity: (u64, u64),
    _lock: File,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.listener_thread.take() {
            let _ = handle.join();
        }
        if let Ok(connections) = self.connections.lock() {
            for stream in connections.values() {
                let _ = stream.shutdown(Shutdown::Both);
            }
        }
        remove_owned_socket(&self.socket, self.identity);
    }
}

fn remove_owned_socket(path: &Path, identity: (u64, u64)) {
    if fs::symlink_metadata(path)
        .is_ok_and(|m| m.file_type().is_socket() && (m.dev(), m.ino()) == identity)
    {
        let _ = fs::remove_file(path);
    }
}

fn owned_private(metadata: &fs::Metadata) -> bool {
    // SAFETY: geteuid has no arguments or memory preconditions.
    metadata.uid() == unsafe { libc::geteuid() } && metadata.mode() & 0o077 == 0
}

pub(crate) fn spawn(root: Option<PathBuf>) -> Result<Server> {
    let paths = Paths::new(root)?;
    fs::create_dir_all(&paths.state).context("create native socket state directory")?;
    let paths = Paths::new(Some(fs::canonicalize(&paths.root)?))?;
    let directory = paths.state.join("native");
    match DirBuilder::new().mode(0o700).create(&directory) {
        Ok(()) => (),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(error) => return Err(error.into()),
    }
    let metadata = fs::symlink_metadata(&directory)?;
    ensure!(
        metadata.is_dir() && owned_private(&metadata),
        "native socket directory must be an owned private directory"
    );
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(directory.join("app.lock"))?;
    let metadata = lock.metadata()?;
    ensure!(
        metadata.is_file() && owned_private(&metadata),
        "native socket lock must be an owned private file"
    );
    lock.try_lock()
        .context("native socket already owned by another daemon")?;
    let socket = directory.join("app.sock");
    match fs::symlink_metadata(&socket) {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_socket() && owned_private(&metadata),
                "refusing to replace unexpected native socket path"
            );
            match UnixStream::connect(&socket) {
                Ok(_) => return Err(anyhow!("native socket already accepting connections")),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                    ) => {}
                Err(error) => return Err(error).context("check existing native socket"),
            }
            remove_owned_socket(&socket, (metadata.dev(), metadata.ino()));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    let listener = UnixListener::bind(&socket).context("bind native app socket")?;
    let metadata = fs::symlink_metadata(&socket)?;
    let identity = (metadata.dev(), metadata.ino());
    let stop = Arc::new(AtomicBool::new(false));
    let connections: Connections = Arc::new(Mutex::new(HashMap::new()));
    let mut server = Server {
        stop: stop.clone(),
        connections: connections.clone(),
        listener_thread: None,
        socket,
        identity,
        _lock: lock,
    };
    fs::set_permissions(&server.socket, fs::Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    server.listener_thread = Some(thread::Builder::new().name("memex-native".into()).spawn(
        move || {
            let mut next_id = 0u64;
            while !stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let Ok(mut active) = connections.lock() else {
                            break;
                        };
                        if active.len() >= MAX_CONNECTIONS {
                            continue;
                        }
                        let Ok(tracked) = stream.try_clone() else {
                            continue;
                        };
                        next_id = next_id.wrapping_add(1);
                        let id = next_id;
                        active.insert(id, tracked);
                        drop(active);
                        let connections = connections.clone();
                        let paths = paths.clone();
                        let cleanup = ConnectionGuard { connections, id };
                        // A failed thread spawn also drops cleanup, releasing the connection slot.
                        let _ = thread::Builder::new()
                            .name("memex-native-client".into())
                            .spawn(move || {
                                let _cleanup = cleanup;
                                let _ = serve_connection(stream, &paths);
                            });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(20))
                    }
                    Err(_) => break,
                }
            }
        },
    )?);
    Ok(server)
}

struct ConnectionGuard {
    connections: Connections,
    id: u64,
}
impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if let Ok(mut active) = self.connections.lock() {
            active.remove(&self.id);
        }
    }
}

fn error(code: &str, message: impl std::fmt::Display) -> Value {
    json!({"protocol": PROTOCOL, "error": {"code": code, "message": message.to_string()}})
}

fn dispatch(paths: &Paths, bytes: &[u8]) -> Value {
    let request: Request = match serde_json::from_slice(bytes) {
        Ok(request) => request,
        Err(e) => return error("request_failed", e),
    };
    if request.protocol != PROTOCOL {
        return error("unsupported", "unsupported native protocol");
    }
    let op = request
        .request
        .get("op")
        .and_then(Value::as_str)
        .unwrap_or("");
    if op != "hello" && !CAPABILITIES.contains(&op) {
        return error("unsupported", "unsupported native operation");
    }
    let operation: Operation = match serde_json::from_value(request.request) {
        Ok(operation) => operation,
        Err(e) => return error("request_failed", e),
    };
    if matches!(operation, Operation::Hello {}) {
        return json!({"protocol": PROTOCOL, "result": {"root": paths.root, "capabilities": CAPABILITIES}});
    }
    match crate::cli::native_request(paths, operation) {
        Ok(result) => json!({"protocol": PROTOCOL, "result": result}),
        Err(e) => error(
            if e.downcast_ref::<Unavailable>().is_some() {
                "unavailable"
            } else {
                "request_failed"
            },
            e,
        ),
    }
}

fn serve_connection(mut stream: UnixStream, paths: &Paths) -> Result<()> {
    // Darwin accepts inherit O_NONBLOCK from the listening socket. Each worker
    // uses blocking I/O with deadlines for the lifetime of its connection.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    loop {
        let mut length = [0u8; 4];
        // A closed connection has no further request; partial headers are discarded too.
        match stream.read_exact(&mut length) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        let length = u32::from_be_bytes(length) as usize;
        if length == 0 || length > MAX_REQUEST {
            write_response(
                &mut stream,
                &error("request_failed", "invalid request frame length"),
            )?;
            return Ok(());
        }
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes)?;
        write_response(&mut stream, &dispatch(paths, &bytes))?;
    }
}

fn write_response(stream: &mut UnixStream, value: &Value) -> Result<()> {
    let mut writer = BoundedResponse(Vec::new());
    if serde_json::to_writer(&mut writer, value).is_err() {
        writer.0 = serde_json::to_vec(&error(
            "request_failed",
            "native response exceeds maximum size",
        ))?;
    }
    stream.write_all(&(writer.0.len() as u32).to_be_bytes())?;
    stream.write_all(&writer.0)?;
    Ok(())
}

struct BoundedResponse(Vec<u8>);
impl Write for BoundedResponse {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_RESPONSE.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other("response exceeds maximum size"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::{AnalyticsWriter, analytics_path};
    use crate::index::SearchIndex;
    use crate::types::{Record, RecordLinks, SourceKind};

    fn root() -> tempfile::TempDir {
        // Darwin's sockaddr_un path limit is smaller than some test TMPDIR paths.
        tempfile::Builder::new()
            .prefix("memex-native-")
            .tempdir_in("/tmp")
            .unwrap()
    }

    fn connect(server: &Server) -> UnixStream {
        let stream = UnixStream::connect(&server.socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
    }

    fn receive(stream: &mut UnixStream) -> Value {
        let mut length = [0; 4];
        stream.read_exact(&mut length).unwrap();
        let mut bytes = vec![0; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut bytes).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn request(stream: &mut UnixStream, operation: Value) -> Value {
        raw_request(
            stream,
            &serde_json::to_vec(&json!({"protocol": 1, "request": operation})).unwrap(),
        )
    }

    fn raw_request(stream: &mut UnixStream, bytes: &[u8]) -> Value {
        stream
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .unwrap();
        stream.write_all(bytes).unwrap();
        receive(stream)
    }

    #[test]
    fn persistent_connection_negotiates_root_and_rejects_writes_without_losing_framing() {
        let root = root();
        let server = spawn(Some(root.path().to_path_buf())).unwrap();
        let mut stream = connect(&server);
        let hello = request(&mut stream, json!({"op":"hello"}));
        assert_eq!(
            hello["result"]["root"],
            fs::canonicalize(root.path()).unwrap().to_str().unwrap()
        );
        assert_eq!(hello["result"]["capabilities"], json!(CAPABILITIES));
        for op in ["index", "run", "exec", "unknown"] {
            assert_eq!(
                request(&mut stream, json!({"op":op}))["error"]["code"],
                "unsupported"
            );
        }
        assert_eq!(
            raw_request(&mut stream, br#"{"protocol":2,"request":{"op":"hello"}}"#)["error"]["code"],
            "unsupported"
        );
        assert_eq!(
            raw_request(&mut stream, b"not json")["error"]["code"],
            "request_failed"
        );
        assert_eq!(
            request(&mut stream, json!({"op":"machines","root":"/other"}))["error"]["code"],
            "request_failed"
        );
        assert_eq!(
            request(&mut stream, json!({"op":"machines"}))["result"][0],
            json!({"id":"local","label":"This Mac"})
        );
        assert!(!root.path().join("index").exists());
    }

    #[test]
    fn activity_returns_one_machine_raw_buckets_at_a_shared_clock() {
        let root = root();
        let paths = Paths::new(Some(root.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let day = 86_400_000;
        let mut analytics = AnalyticsWriter::open(analytics_path(&paths.state)).unwrap();
        for (id, timestamp) in [(1, day), (2, 2 * day + 1)] {
            analytics
                .record(&Record {
                    source: SourceKind::Codex,
                    doc_id: id,
                    ts: timestamp,
                    project: "memex".into(),
                    session_id: format!("session-{id}"),
                    turn_id: 1,
                    role: "user".into(),
                    text: "hello".into(),
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: RecordLinks::default(),
                    source_path: format!("/{id}.jsonl"),
                })
                .unwrap();
        }
        analytics.flush().unwrap();
        drop(analytics);
        let server = spawn(Some(root.path().to_path_buf())).unwrap();
        let mut stream = connect(&server);
        let operation = json!({"op":"activity", "machine":"local", "metric":"sessions", "range":"24h",
            "source":"codex", "project":"memex", "origin":"regular", "now_ms":3 * day});
        let response = request(&mut stream, operation.clone());
        assert_eq!(
            response["result"]["points"],
            json!([{"timestamp_ms":2 * day,"source":"codex","value":1}])
        );
        assert_eq!(response["result"]["partial"], false);
        let mut invalid = operation;
        invalid["metric"] = json!("wrong");
        assert_eq!(
            request(&mut stream, invalid)["error"]["code"],
            "request_failed"
        );
    }

    #[test]
    fn frame_limit_closes_only_offending_connection() {
        let root = root();
        let server = spawn(Some(root.path().to_path_buf())).unwrap();
        let mut other = connect(&server);
        for size in [0, MAX_REQUEST + 1] {
            let mut stream = connect(&server);
            stream.write_all(&(size as u32).to_be_bytes()).unwrap();
            assert_eq!(receive(&mut stream)["error"]["code"], "request_failed");
            assert_eq!(stream.read(&mut [0u8; 1]).unwrap(), 0);
        }
        assert!(request(&mut other, json!({"op":"hello"}))["result"].is_object());
    }

    #[test]
    fn connection_limit_partial_frames_and_shutdown_are_bounded() {
        let root = root();
        let server = spawn(Some(root.path().to_path_buf())).unwrap();
        let mut partial = connect(&server);
        partial.write_all(&[0, 0]).unwrap();
        partial.shutdown(Shutdown::Write).unwrap();
        assert_eq!(partial.read(&mut [0u8; 1]).unwrap(), 0);
        let mut streams = Vec::new();
        for _ in 0..MAX_CONNECTIONS {
            let mut stream = connect(&server);
            assert!(request(&mut stream, json!({"op":"hello"}))["result"].is_object());
            streams.push(stream);
        }
        let mut excess = connect(&server);
        assert_eq!(excess.read(&mut [0u8; 1]).unwrap(), 0);
        drop(server);
        for mut stream in streams {
            assert_eq!(stream.read(&mut [0u8; 1]).unwrap(), 0);
        }
        let restarted = spawn(Some(root.path().to_path_buf())).unwrap();
        assert!(request(&mut connect(&restarted), json!({"op":"hello"}))["result"].is_object());
    }

    #[test]
    fn missing_index_is_unavailable_and_does_not_create_index_or_analytics() {
        let root = root();
        let server = spawn(Some(root.path().to_path_buf())).unwrap();
        let mut stream = connect(&server);
        for operation in [
            json!({"op":"search","machine":"local","query":"hello","origin":"all","limit":10}),
            json!({"op":"session","machine":"local","session_id":"s","source_path":"/missing","offset":0,"limit":10}),
            json!({"op":"projects","machine":"local"}),
            json!({"op":"sessions","machine":"local","filters":{"limit":10,"origin":"all"}}),
        ] {
            let response = request(&mut stream, operation);
            assert_eq!(response["error"]["code"], "unavailable", "{response}");
        }
        assert!(!root.path().join("index").exists());
        assert!(!analytics_path(&root.path().join("state")).exists());
    }

    #[test]
    fn socket_permissions_live_owner_stale_recovery_and_identity_safe_cleanup() {
        let root = root();
        let server = spawn(Some(root.path().to_path_buf())).unwrap();
        assert_eq!(
            fs::metadata(server.socket.parent().unwrap())
                .unwrap()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(fs::metadata(&server.socket).unwrap().mode() & 0o777, 0o600);
        assert!(spawn(Some(root.path().to_path_buf())).is_err());
        let socket = server.socket.clone();
        drop(server);
        assert!(!socket.exists());
        let stale = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
        drop(stale);
        let server = spawn(Some(root.path().to_path_buf())).unwrap();
        let old = server.identity;
        assert!(request(&mut connect(&server), json!({"op":"hello"}))["result"].is_object());
        // Another owner replacing the path must survive this guard's cleanup.
        fs::remove_file(&socket).unwrap();
        fs::write(&socket, "replacement").unwrap();
        remove_owned_socket(&socket, old);
        drop(server);
        assert_eq!(fs::read_to_string(&socket).unwrap(), "replacement");
        assert!(spawn(Some(root.path().to_path_buf())).is_err());
    }

    #[test]
    fn rejects_symlink_or_shared_private_directory_and_live_unlocked_socket() {
        let root = root();
        let state = root.path().join("state");
        fs::create_dir(&state).unwrap();
        let outside = root.path().join("outside");
        fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, state.join("native")).unwrap();
        assert!(spawn(Some(root.path().to_path_buf())).is_err());
        fs::remove_file(state.join("native")).unwrap();
        fs::create_dir(state.join("native")).unwrap();
        fs::set_permissions(state.join("native"), fs::Permissions::from_mode(0o755)).unwrap();
        assert!(spawn(Some(root.path().to_path_buf())).is_err());
        fs::set_permissions(state.join("native"), fs::Permissions::from_mode(0o700)).unwrap();
        let socket = state.join("native/app.sock");
        let _listener = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(spawn(Some(root.path().to_path_buf())).is_err());
        assert!(socket.exists());
    }

    fn publish(paths: &Paths, records: &[Record]) {
        let index = SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
        let mut writer = index.writer().unwrap();
        let mut analytics = AnalyticsWriter::open(analytics_path(&paths.state)).unwrap();
        for record in records {
            index.add_record(&mut writer, record).unwrap();
            analytics.record(record).unwrap();
        }
        writer.commit().unwrap();
        writer.wait_merging_threads().unwrap();
        index.publish_generation().unwrap();
        analytics.flush().unwrap();
    }

    #[test]
    fn session_page_returns_full_content_and_metadata_from_the_same_scope() {
        let root = root();
        let paths = Paths::new(Some(root.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        let records = (1..=4)
            .map(|id| Record {
                source: SourceKind::Codex,
                doc_id: id,
                ts: id,
                project: "fixture".into(),
                session_id: "session-page".into(),
                turn_id: id as u32,
                role: "user".into(),
                text: format!("{id}:{}", "λ".repeat(6000)),
                tool_name: None,
                tool_input: Some("argument".repeat(1000)),
                tool_output: Some("output".repeat(2000)),
                links: RecordLinks::default(),
                source_path: if id == 4 {
                    "/other-source.jsonl"
                } else {
                    "/page-source.jsonl"
                }
                .into(),
            })
            .collect::<Vec<_>>();
        publish(&paths, &records);
        let server = spawn(Some(root.path().to_path_buf())).unwrap();
        let mut stream = connect(&server);
        let hello = request(&mut stream, json!({"op":"hello"}));
        assert!(
            hello["result"]["capabilities"]
                .as_array()
                .unwrap()
                .contains(&json!("session_page"))
        );
        let operation = json!({"op":"session_page", "machine":"local", "session_id":"session-page", "source_path":"/page-source.jsonl", "offset":0, "limit":2});
        for (offset, expected, next) in [(0, 2, Some(2)), (2, 1, None), (5, 0, None)] {
            let mut page_operation = operation.clone();
            page_operation["offset"] = json!(offset);
            let response = request(&mut stream, page_operation);
            let items = response["result"].as_array().unwrap();
            assert_eq!(items.len(), expected + 1);
            for (item, original) in items[..expected].iter().zip(records.iter().skip(offset)) {
                assert_eq!(item["record"]["text"], original.text);
                assert_eq!(
                    item["record"]["tool_input"],
                    original.tool_input.as_deref().unwrap()
                );
                assert_eq!(
                    item["record"]["tool_output"],
                    original.tool_output.as_deref().unwrap()
                );
                assert_eq!(item["content"]["truncated"], false);
                assert!(item["record_id"].is_string());
            }
            assert_eq!(
                items.last().unwrap(),
                &json!({"type":"page", "machine":"local", "session_id":"session-page", "source_path":"/page-source.jsonl", "offset":offset, "total":3, "next_offset":next})
            );
        }
        let mut legacy = operation.clone();
        legacy["op"] = json!("session");
        let response = request(&mut stream, legacy);
        assert_eq!(response["result"].as_array().unwrap().len(), 2);
        assert_eq!(response["result"][0]["record"]["text"], records[0].text);
        let mut invalid = operation;
        invalid["limit"] = json!(0);
        assert_eq!(
            request(&mut stream, invalid)["error"]["code"],
            "request_failed"
        );
    }

    #[test]
    fn real_read_contracts_share_metadata_search_and_full_or_bounded_transcripts() {
        let root = root();
        let paths = Paths::new(Some(root.path().to_path_buf())).unwrap();
        paths.ensure_dirs().unwrap();
        // Explicitly enable the policy to prove native requests override it.
        fs::write(
            paths.root.join("config.toml"),
            "auto_index_on_search = true\n",
        )
        .unwrap();
        let record = Record {
            source: SourceKind::Codex,
            doc_id: 1,
            ts: 1_700_000_000_000,
            project: "fixture".into(),
            session_id: "session-a".into(),
            source_path: "/nonexistent/native-fixture.jsonl".into(),
            turn_id: 1,
            role: "user".into(),
            text: "hello persistent socket".into(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks::default(),
        };
        publish(&paths, std::slice::from_ref(&record));
        // Holding the ingest lease makes accidental ingestion fail before opening
        // any real source, and confirms reads do not wait for the daemon writer.
        let _lease =
            crate::lease::IngestLease::acquire(&paths, "native read test", Duration::from_secs(1))
                .unwrap();
        let current = fs::read(paths.index.join("CURRENT")).unwrap();
        let server = spawn(Some(root.path().to_path_buf())).unwrap();
        let mut stream = connect(&server);
        let filters = json!({"source":"codex","origin":"all","limit":10});
        let projects = request(&mut stream, json!({"op":"projects","machine":"local"}));
        assert_eq!(projects["result"][0]["project"], "Unfiled");
        assert_eq!(projects["result"][0]["machine"], "local");
        let sessions = request(
            &mut stream,
            json!({"op":"sessions","machine":"local","filters":filters}),
        );
        assert_eq!(sessions["result"][0]["session_id"], "session-a");
        for query in [Value::Null, json!("hello")] {
            assert_eq!(
                request(
                    &mut stream,
                    json!({"op":"count","machine":"local","filters":filters,"query":query})
                )["result"]["total"],
                1
            );
        }
        let search = request(
            &mut stream,
            json!({"op":"search","machine":"local","query":"hello","project":"fixture","source":"codex","origin":"all","limit":10}),
        );
        assert_eq!(search["result"][0]["session_id"], "session-a", "{search}");
        assert_eq!(search["result"][0]["machine"], "local");
        assert!(search["result"][0]["record_id"].is_string());
        assert!(search["result"][0].get("text").is_none());
        assert_eq!(fs::read(paths.index.join("CURRENT")).unwrap(), current);
        let mut operation = json!({"op":"session","machine":"local","session_id":"session-a","source_path":record.source_path,"offset":0,"limit":10});
        let full = request(&mut stream, operation.clone());
        assert_eq!(full["result"].as_array().unwrap().len(), 1);
        assert_eq!(full["result"][0]["record"]["text"], record.text);
        assert_eq!(full["result"][0]["content"]["truncated"], false);
        operation["max_chars"] = json!(5);
        let bounded = request(&mut stream, operation);
        assert_eq!(bounded["result"][0]["record"]["text"], "hello");
        assert_eq!(bounded["result"][0]["content"]["truncated"], true);
        assert_eq!(bounded["result"][1]["type"], "page");
        assert_eq!(bounded["result"][1]["total"], 1);
        // An already connected client observes new generations without reconnecting.
        let mut next = record.clone();
        next.doc_id = 2;
        next.text = "new generation".into();
        publish(&paths, &[record, next]);
        let count = request(
            &mut stream,
            json!({"op":"count","machine":"local","filters":filters,"query":"generation"}),
        );
        assert_eq!(count["result"]["total"], 1);
    }
}
