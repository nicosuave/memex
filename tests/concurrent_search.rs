use serde_json::{Value, json};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::{Arc, Barrier};

const CONCURRENT_SEARCHES: usize = 8;
const APPENDED_RECORDS: usize = 1_500;

#[test]
fn concurrent_searches_coalesce_stale_auto_indexing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let home = temp.path().join("home");
    let root = temp.path().join("memex");
    let claude_root = home.join(".claude").join("projects");
    let project = claude_root.join("-tmp-concurrent-search");
    let transcript = project.join("session.jsonl");
    fs::create_dir_all(&project).expect("create Claude project");
    fs::create_dir_all(&root).expect("create memex root");
    fs::write(
        root.join("config.toml"),
        "auto_index_on_search = true\nembeddings = false\nscan_cache_ttl = 3600\n",
    )
    .expect("write config");

    let mut transcript_file = File::create(&transcript).expect("create transcript");
    write_claude_record(&mut transcript_file, 0, "seed");
    transcript_file.flush().expect("flush seed transcript");

    let initial = isolated_memex(&home)
        .args([
            "index",
            "--source",
            claude_root.to_str().expect("Claude root"),
            "--no-codex",
            "--no-opencode",
            "--no-cursor",
            "--no-pi",
            "--no-copilot",
            "--no-grok",
            "--no-embeddings",
        ])
        .arg("--root")
        .arg(&root)
        .output()
        .expect("run initial index");
    assert_success(&initial, "initial index");

    let mut transcript_file = OpenOptions::new()
        .append(true)
        .open(&transcript)
        .expect("open transcript for append");
    for record_id in 1..=APPENDED_RECORDS {
        write_claude_record(
            &mut transcript_file,
            record_id,
            &format!("concurrent-marker record {record_id}"),
        );
    }
    transcript_file.flush().expect("flush appended transcript");
    fs::remove_file(root.join("state").join("scan_cache.json")).expect("expire scan cache");

    let barrier = Arc::new(Barrier::new(CONCURRENT_SEARCHES));
    let handles = (0..CONCURRENT_SEARCHES)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let home = home.clone();
            let root = root.clone();
            std::thread::spawn(move || {
                let mut command = isolated_memex(&home);
                command.args(["search", "seed", "--json-array", "--fields", "doc_id"]);
                command.arg("--root").arg(&root);
                barrier.wait();
                command.output().expect("run concurrent search")
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        let output = handle.join().expect("join concurrent search");
        assert_success(&output, "concurrent search");
        let value: Value = serde_json::from_slice(&output.stdout).expect("valid search JSON");
        assert!(value.is_array());
    }

    let final_search = isolated_memex(&home)
        .args([
            "search",
            "concurrent-marker",
            "--limit",
            "2000",
            "--json-array",
            "--fields",
            "doc_id",
        ])
        .arg("--root")
        .arg(&root)
        .output()
        .expect("run final search");
    assert_success(&final_search, "final search");
    let rows: Vec<Value> =
        serde_json::from_slice(&final_search.stdout).expect("valid final search JSON");
    assert_eq!(rows.len(), APPENDED_RECORDS);
    let doc_ids = rows
        .iter()
        .map(|row| row["doc_id"].as_u64().expect("numeric doc id"))
        .collect::<HashSet<_>>();
    assert_eq!(doc_ids.len(), APPENDED_RECORDS);

    let stats = isolated_memex(&home)
        .arg("stats")
        .arg("--root")
        .arg(&root)
        .output()
        .expect("run stats");
    assert_success(&stats, "stats");
    let stats = String::from_utf8(stats.stdout).expect("UTF-8 stats");
    assert!(
        stats.contains(&format!("documents: {}", APPENDED_RECORDS + 1)),
        "unexpected stats output: {stats}"
    );
}

fn isolated_memex(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_memex"));
    command
        .env("HOME", home)
        .env("CODEX_HOME", home.join(".codex"))
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("XDG_DATA_HOME", home.join(".local").join("share"))
        .env("PI_CODING_AGENT_DIR", home.join(".pi").join("agent"));
    command
}

fn write_claude_record(file: &mut File, record_id: usize, text: &str) {
    let record = json!({
        "type": "user",
        "uuid": format!("user-{record_id}"),
        "parentUuid": Value::Null,
        "sessionId": "concurrent-search-session",
        "isSidechain": false,
        "timestamp": "2026-07-26T17:00:00Z",
        "message": { "content": text }
    });
    writeln!(file, "{record}").expect("write Claude record");
}

fn assert_success(output: &Output, operation: &str) {
    assert!(
        output.status.success(),
        "{operation} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
