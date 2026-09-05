use memex::{
    analytics::{AnalyticsStore, analytics_path, backfill_from_index},
    config::Paths,
    index::SearchIndex,
    retrieval::canonical_record_id,
    types::{Record, RecordLinks, SourceKind},
};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    path::Path,
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver},
    time::{Duration, Instant},
};

struct Client {
    child: Child,
    stdin: Option<ChildStdin>,
    responses: Receiver<Value>,
    id: u64,
}
impl Client {
    fn start(root: &Path) -> Self {
        Self::command(root, |_| {})
    }

    fn command(root: &Path, configure: impl FnOnce(&mut Command)) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_memex"));
        command
            .args(["mcp", "--transport", "stdio", "--root"])
            .arg(root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        configure(&mut command);
        let mut child = command.spawn().unwrap();
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().unwrap();
        let (tx, responses) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let value = serde_json::from_str(&line.unwrap())
                    .expect("stdout must contain only MCP JSON");
                if tx.send(value).is_err() {
                    break;
                }
            }
        });
        let mut client = Self {
            child,
            stdin,
            responses,
            id: 0,
        };
        let init = client.request("initialize", json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"integration-test","version":"1"}}));
        assert_eq!(init["result"]["serverInfo"]["name"], "memex");
        assert!(init["result"]["capabilities"]["tools"].is_object());
        client.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
        client
    }
    fn send(&mut self, value: Value) {
        writeln!(self.stdin.as_mut().unwrap(), "{value}").unwrap();
        self.stdin.as_mut().unwrap().flush().unwrap();
    }
    fn request(&mut self, method: &str, params: Value) -> Value {
        self.id += 1;
        self.send(json!({"jsonrpc":"2.0","id":self.id,"method":method,"params":params}));
        loop {
            let value = self
                .responses
                .recv_timeout(Duration::from_secs(30))
                .expect("MCP response within 30s");
            if value["id"] == self.id {
                return value;
            }
        }
    }
    fn call_result(&mut self, tool: &str, args: Value) -> Value {
        let response = self.request("tools/call", json!({"name":tool,"arguments":args}));
        assert!(response.get("error").is_none(), "{response}");
        response["result"].clone()
    }
    fn call(&mut self, tool: &str, args: Value) -> Value {
        let result = self.call_result(tool, args);
        assert_eq!(result["isError"], false, "{result}");
        result["structuredContent"].clone()
    }
    fn stop(mut self) {
        drop(self.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(Instant::now() < deadline, "MCP should exit on stdin EOF");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn record(id: u64, text: &str) -> Record {
    Record {
        source: SourceKind::Codex,
        doc_id: id,
        ts: 1_700_000_000_000 + id * 1000,
        project: "mcp-test".into(),
        session_id: "session".into(),
        turn_id: id as u32,
        role: "assistant".into(),
        text: text.into(),
        tool_name: None,
        tool_input: None,
        tool_output: None,
        links: RecordLinks {
            event_id: Some(format!("event-{id}")),
            ..Default::default()
        },
        source_path: "/tmp/memex-mcp-fixture.jsonl".into(),
    }
}
fn seed_index(root: &Path, records: &[Record]) {
    let paths = Paths::new(Some(root.to_path_buf())).unwrap();
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    let mut writer = index.writer().unwrap();
    for record in records {
        index.add_record(&mut writer, record).unwrap();
    }
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
}
fn fixture() -> (tempfile::TempDir, Vec<Record>) {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("config.toml"),
        "auto_index_on_search = false\n",
    )
    .unwrap();
    let records = vec![
        record(1, "αβγδε"),
        record(2, "needle implementation"),
        record(3, "needle verified outcome"),
    ];
    seed_index(root.path(), &records);
    (root, records)
}

#[test]
fn handshake_catalog_errors_and_eof() {
    let root = tempfile::tempdir().unwrap();
    let mut client = Client::start(root.path());
    let tools = client.request("tools/list", json!({}));
    let mut names: Vec<_> = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| {
            assert_eq!(tool["inputSchema"]["type"], "object");
            tool["name"].as_str().unwrap()
        })
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            "context", "hydrate", "search", "session", "sessions", "show"
        ]
    );
    assert!(
        !root.path().join("index").exists(),
        "handshake must not create an index"
    );
    let invalid = client.request(
        "tools/call",
        json!({"name":"show","arguments":{"record_id":42}}),
    );
    assert!(invalid.get("error").is_some() || invalid["result"]["isError"] == true);
    let missing = client.request("tools/call", json!({"name":"unknown","arguments":{}}));
    assert!(missing.get("error").is_some() || missing["result"]["isError"] == true);
    let typo = client.request(
        "tools/call",
        json!({"name":"hydrate","arguments":{
            "requests":[{"session_id":"session","machien":"remote"}]
        }}),
    );
    assert!(typo.get("error").is_some() || typo["result"]["isError"] == true);
    for (tool, args) in [
        ("show", json!({})),
        ("show", json!({"record_id":"missing","max_chars":0})),
        ("show", json!({"record_id":"missing","max_chars":64001})),
        ("search", json!({"query":"needle","limit":501})),
        ("session", json!({"session_id":"session","limit":501})),
        ("hydrate", json!({"requests":[]})),
        ("context", json!({"record_id":"missing","before":1001})),
    ] {
        assert_eq!(client.call_result(tool, args)["isError"], true);
    }
    client.stop();
}

#[test]
fn progressive_reads_share_budgets_and_preserve_continuations() {
    let (root, records) = fixture();
    let mut client = Client::start(root.path());
    let id = canonical_record_id(&records[0]);
    let first = client.call("show", json!({"record_id":id,"max_chars":2}));
    assert_eq!(first["record"]["text"], "αβ");
    assert_eq!(first["content"]["continuations"][0]["offset_chars"], 2);
    let next = client.call(
        "show",
        json!({"record_id":id,"field":"text","offset_chars":2,"max_chars":3}),
    );
    assert_eq!(next["record"]["text"], "γδε");
    assert_eq!(next["content"]["truncated"], false);
    let context = client.call(
        "context",
        json!({"record_id":canonical_record_id(&records[1]),"max_chars":7}),
    );
    assert_eq!(
        context["anchor_record_id"],
        canonical_record_id(&records[1])
    );
    assert_eq!(
        context["records"][0]["record_id"],
        canonical_record_id(&records[1])
    );
    assert_eq!(context["order"], "anchor_first");
    let session = client.call(
        "session",
        json!({"session_id":"session","limit":1,"max_chars":2}),
    );
    assert_eq!(session["records"][0]["record"]["text"], "αβ");
    assert_eq!(session["next_offset"], 1);
    assert_eq!(session["records"][0]["content"]["truncated"], true);
    let next_page = client.call(
        "session",
        json!({"session_id":"session","offset":1,"limit":1}),
    );
    assert_eq!(
        next_page["records"][0]["record_id"],
        canonical_record_id(&records[1])
    );
    let hydrated = client.call("hydrate", json!({"requests":[
        {"session_id":"session","limit":1}, {"session_id":"session","offset":1,"limit":1},
        {"session_id":"session","machine":"unconfigured"}, {"session_id":"session","offset":2,"limit":1}
    ],"max_chars":7}));
    assert_eq!(hydrated["pages"].as_array().unwrap().len(), 3);
    assert_eq!(hydrated["remaining_chars"], 0);
    assert_eq!(
        hydrated["pages"][0]["records"][0]["record"]["text"],
        "αβγδε"
    );
    assert_eq!(hydrated["pages"][1]["records"][0]["record"]["text"], "ne");
    assert_eq!(hydrated["failures"][0]["request_index"], 2);
    assert_eq!(hydrated["failures"][0]["machine"], "unconfigured");
    client.stop();
}

#[test]
fn search_reuses_cli_fusion_projection_and_observes_new_generations() {
    let (root, records) = fixture();
    let mut client = Client::start(root.path());
    let args = json!({"query":"needle","additional_queries":["outcome"],"machines":["local"],"sort":"ts","top_n_per_session":2});
    let result = client.call("search", args);
    let cli = Command::new(env!("CARGO_BIN_EXE_memex"))
        .args([
            "--no-update-check",
            "search",
            "needle",
            "--query",
            "outcome",
            "--machine",
            "local",
            "--sort",
            "ts",
            "--top-n-per-session",
            "2",
            "--format",
            "json",
            "--root",
        ])
        .arg(root.path())
        .output()
        .unwrap();
    assert!(
        cli.status.success(),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let cli: Value = serde_json::from_slice(&cli.stdout).unwrap();
    assert_eq!(result["results"], cli);
    assert_eq!(result["results"].as_array().unwrap().len(), 2);
    assert_eq!(
        result["results"][0]["record_id"],
        canonical_record_id(&records[2])
    );
    assert!(result["results"][0].get("text").is_none());
    let unique = client.call("search", json!({"query":"needle","machines":["local"]}));
    assert_eq!(unique["results"].as_array().unwrap().len(), 1);
    // Without vectors, both configured semantic modes retain the CLI's lexical
    // fallback instead of downloading a model or failing an otherwise useful lookup.
    for mode in ["semantic", "hybrid"] {
        let fallback = client.call(
            "search",
            json!({"query":"needle","machines":["local"],"mode":mode}),
        );
        assert_eq!(fallback["results"], unique["results"]);
    }
    // Publish through the production indexing command while this MCP process
    // remains alive, rather than granting tests access to private publication.
    let source = root.path().join("source").join("project");
    std::fs::create_dir_all(&source).unwrap();
    let transcript = json!({"type":"user","uuid":"new-event","sessionId":"new-session",
        "timestamp":"2026-09-05T12:00:00Z","message":{"content":"fresh_generation_marker"}});
    std::fs::write(source.join("new-session.jsonl"), format!("{transcript}\n")).unwrap();
    let indexed = Command::new(env!("CARGO_BIN_EXE_memex"))
        .args(["--no-update-check", "index", "--source"])
        .arg(root.path().join("source"))
        .args([
            "--no-codex",
            "--no-opencode",
            "--no-cursor",
            "--no-pi",
            "--no-omp",
            "--no-openclaw",
            "--no-copilot",
            "--no-grok",
            "--no-jcode",
            "--no-muse",
            "--no-embeddings",
            "--root",
        ])
        .arg(root.path())
        .output()
        .unwrap();
    assert!(
        indexed.status.success(),
        "{}",
        String::from_utf8_lossy(&indexed.stderr)
    );
    assert!(root.path().join("index/CURRENT").exists());
    let fresh = client.call(
        "search",
        json!({"query":"fresh_generation_marker","machines":["local"]}),
    );
    assert_eq!(fresh["results"][0]["session_id"], "new-session");
    client.stop();
}

#[test]
fn sessions_match_cli_and_do_not_auto_index() {
    let (root, _) = fixture();
    let paths = Paths::new(Some(root.path().to_path_buf())).unwrap();
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    backfill_from_index(analytics_path(&paths.state), &index).unwrap();
    let before = std::fs::read(paths.index.join("meta.json")).unwrap();
    std::fs::write(
        root.path().join("config.toml"),
        "auto_index_on_search = true\ncodex_resume_cmd = \"codex resume {session_id}\"\n",
    )
    .unwrap();
    let mut client = Client::command(root.path(), |command| {
        command.env("HOME", root.path());
    });
    let result = client.call("sessions", json!({"project":"mcp-test","source":"codex"}));
    assert_eq!(result["results"].as_array().unwrap().len(), 1);
    assert_eq!(result["results"][0]["machine"], "local");
    assert_eq!(result["results"][0]["session_id"], "session");
    assert!(result["results"][0]["resume_cmd"].is_string());
    assert_eq!(
        std::fs::read(paths.index.join("meta.json")).unwrap(),
        before
    );
    assert!(!paths.index.join("CURRENT").exists());
    let cli = Command::new(env!("CARGO_BIN_EXE_memex"))
        .args([
            "--no-update-check",
            "sessions",
            "--project",
            "mcp-test",
            "--source",
            "codex",
            "--json-array",
            "--root",
        ])
        .arg(root.path())
        .output()
        .unwrap();
    assert!(cli.status.success());
    let mut expected: Value = serde_json::from_slice(&cli.stdout).unwrap();
    for row in expected.as_array_mut().unwrap() {
        row["machine"] = json!("local");
    }
    assert_eq!(result["results"], expected);
    let store = AnalyticsStore::open_read_only(analytics_path(&paths.state)).unwrap();
    assert_eq!(
        store
            .query_sessions_detailed(None, None, None, None, None)
            .unwrap()
            .len(),
        1
    );
    client.stop();
}

#[cfg(unix)]
#[test]
fn federated_search_and_reads_keep_machine_identity_and_partial_failures() {
    use std::os::unix::fs::PermissionsExt;
    let (root, records) = fixture();
    let coordinator = tempfile::tempdir().unwrap();
    let config = format!(
        r#"auto_index_on_search = false
[multi_machine]
default = ["remote", "offline"]
timeout_seconds = 10
[[machines]]
id = "remote"
command = "{}"
[machines.control]
type = "ssh"
host = "mcp-integration-remote"
[machines.index]
type = "remote"
[[machines]]
id = "offline"
command = "memex"
[machines.control]
type = "ssh"
host = "mcp-integration-offline"
[machines.index]
type = "remote"
"#,
        env!("CARGO_BIN_EXE_memex")
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
    );
    std::fs::write(coordinator.path().join("config.toml"), config).unwrap();
    let bin = coordinator.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let ssh = bin.join("ssh");
    let rpc_calls = coordinator.path().join("rpc-calls");
    std::fs::write(
        &ssh,
        r#"#!/bin/sh
for argument in "$@"; do
  if [ "$argument" = "mcp-integration-remote" ] || [ "$argument" = "mcp-integration-offline" ]; then
    target="$argument"
  fi
  remote_command="$argument"
done
printf '%s\n' "$target" >> "$MEMEX_TEST_RPC_CALLS"
if [ "$target" = "mcp-integration-offline" ]; then
  echo "fixture host unavailable" >&2
  exit 7
fi
request=$(cat)
case "$request" in
  *invalid-remote-page*)
    printf '%s' '{"protocol":1,"response":{"kind":"error","message":"invalid remote page"}}'
    exit 0
    ;;
esac
printf '%s' "$request" | sh -c "$remote_command --root \"$MEMEX_TEST_REMOTE_ROOT\""
"#,
    )
    .unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut path = vec![bin];
    path.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let mut client = Client::command(coordinator.path(), |command| {
        command
            .env("PATH", std::env::join_paths(path).unwrap())
            .env("MEMEX_TEST_REMOTE_ROOT", root.path())
            .env("MEMEX_TEST_RPC_CALLS", &rpc_calls);
    });
    let result = client.call("search", json!({"query":"needle"}));
    assert_eq!(result["results"][0]["machine"], "remote");
    assert_eq!(result["failures"].as_array().unwrap().len(), 1);
    assert!(result["failures"][0].as_str().unwrap().contains("offline"));
    let read = client.call(
        "show",
        json!({"record_id":canonical_record_id(&records[0]),"machine":"remote","max_chars":2}),
    );
    assert_eq!(read["machine"], "remote");
    assert_eq!(read["record"]["text"], "αβ");
    let batch = client.call("hydrate", json!({"requests":[{"session_id":"session","machine":"remote","limit":1},
        {"session_id":"session","machine":"offline"},{"session_id":"session","machine":"remote","offset":1,"limit":1}],"max_chars":7}));
    assert_eq!(batch["failures"].as_array().unwrap().len(), 1);
    assert_eq!(batch["pages"][1]["records"][0]["record"]["text"], "ne");

    let count_calls = |target: &str| {
        std::fs::read_to_string(&rpc_calls)
            .unwrap()
            .lines()
            .filter(|line| *line == target)
            .count()
    };
    let remote_calls = count_calls("mcp-integration-remote");
    let offline_calls = count_calls("mcp-integration-offline");
    let partial = client.call(
        "hydrate",
        json!({"requests":[
            {"session_id":"session","machine":"remote","limit":1},
            {"session_id":"invalid-remote-page","machine":"remote","limit":1},
            {"session_id":"session","machine":"remote","offset":1,"limit":1},
            {"session_id":"session","machine":"offline","limit":1},
            {"session_id":"session","machine":"offline","offset":1,"limit":1}
        ],"max_chars":7}),
    );
    assert_eq!(partial["pages"].as_array().unwrap().len(), 2);
    assert_eq!(partial["pages"][0]["machine"], "remote");
    assert_eq!(partial["pages"][0]["offset"], 0);
    assert_eq!(partial["pages"][0]["records"][0]["record"]["text"], "αβγδε");
    assert_eq!(partial["pages"][1]["machine"], "remote");
    assert_eq!(partial["pages"][1]["offset"], 1);
    assert_eq!(partial["pages"][1]["records"][0]["record"]["text"], "ne");
    assert_eq!(partial["remaining_chars"], 0);
    assert_eq!(partial["failures"].as_array().unwrap().len(), 3);
    assert_eq!(partial["failures"][0]["request_index"], 1);
    assert_eq!(partial["failures"][0]["machine"], "remote");
    assert_eq!(
        partial["failures"][0]["error"],
        "session-page read failed: invalid remote page"
    );
    assert_eq!(partial["failures"][1]["request_index"], 3);
    assert_eq!(partial["failures"][1]["machine"], "offline");
    assert_eq!(partial["failures"][2]["request_index"], 4);
    assert_eq!(partial["failures"][2]["machine"], "offline");
    assert_eq!(count_calls("mcp-integration-remote") - remote_calls, 4);
    assert_eq!(count_calls("mcp-integration-offline") - offline_calls, 1);
    client.stop();
}

#[cfg(unix)]
#[test]
fn cancellation_drops_queued_work_and_transport_stays_responsive() {
    use std::os::unix::fs::PermissionsExt;
    let (root, _) = fixture();
    std::fs::write(
        root.path().join("config.toml"),
        r#"auto_index_on_search = false
[multi_machine]
default = ["slow"]
timeout_seconds = 10
[[machines]]
id = "slow"
command = "memex"
[machines.control]
type = "ssh"
host = "mcp-integration-slow"
[machines.index]
type = "remote"
"#,
    )
    .unwrap();
    let bin = root.path().join("bin");
    let ready = root.path().join("ready");
    std::fs::create_dir(&bin).unwrap();
    std::fs::create_dir(&ready).unwrap();
    let ssh = bin.join("ssh");
    // Bound the fake process lifetime even if an assertion fails before release.
    std::fs::write(
        &ssh,
        r#"#!/bin/sh
: > "$MEMEX_TEST_READY/$$"
n=0
while [ ! -f "$MEMEX_TEST_RELEASE" ] && [ "$n" -lt 250 ]; do
  sleep 0.02
  n=$((n+1))
done
exit 7
"#,
    )
    .unwrap();
    std::fs::set_permissions(&ssh, std::fs::Permissions::from_mode(0o755)).unwrap();
    let release = root.path().join("release");
    let mut path = vec![bin];
    path.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let mut client = Client::command(root.path(), |command| {
        command
            .env("PATH", std::env::join_paths(path).unwrap())
            .env("MEMEX_TEST_READY", &ready)
            .env("MEMEX_TEST_RELEASE", &release);
    });
    let mut active_ids = Vec::new();
    for _ in 0..4 {
        client.id += 1;
        active_ids.push(client.id);
        client.send(json!({"jsonrpc":"2.0","id":client.id,"method":"tools/call",
            "params":{"name":"search","arguments":{"query":"needle"}}}));
    }
    let deadline = Instant::now() + Duration::from_secs(4);
    while std::fs::read_dir(&ready).unwrap().count() < 4 {
        assert!(
            Instant::now() < deadline,
            "four blocking workers should start"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    client.id += 1;
    let cancelled_id = client.id;
    client.send(
        json!({"jsonrpc":"2.0","id":cancelled_id,"method":"tools/call",
        "params":{"name":"search","arguments":{"query":"needle"}}}),
    );
    client.send(json!({"jsonrpc":"2.0","method":"notifications/cancelled",
        "params":{"requestId":cancelled_id,"reason":"test cancellation"}}));
    // Ping must work while all four retrieval workers remain blocked.
    let ping_started = Instant::now();
    let ping = client.request("ping", json!({}));
    assert!(ping.get("result").is_some(), "{ping}");
    assert!(ping_started.elapsed() < Duration::from_secs(2));
    assert_eq!(std::fs::read_dir(&ready).unwrap().count(), 4);
    std::fs::write(&release, "release").unwrap();
    while !active_ids.is_empty() {
        let response = client
            .responses
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        assert_ne!(response["id"], cancelled_id);
        if let Some(id) = response["id"].as_u64() {
            active_ids.retain(|active| *active != id);
        }
    }
    let local = client.call("search", json!({"query":"needle","machines":["local"]}));
    assert_eq!(local["results"].as_array().unwrap().len(), 1);
    // The cancelled fifth request must never start another SSH process.
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(std::fs::read_dir(&ready).unwrap().count(), 4);
    client.stop();
}
