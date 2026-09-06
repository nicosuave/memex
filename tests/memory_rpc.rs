use memex::{
    config::Paths,
    memory::{MemoryDiscoveryOptions, MemoryStore},
    memory_search::MemorySearchOptions,
    types::SourceKind,
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

fn rpc(root: &Path, request: Value) -> Value {
    let mut child = Command::new(env!("CARGO_BIN_EXE_memex"))
        .args(["--non-interactive", "--no-update-check", "rpc", "--root"])
        .arg(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(
            serde_json::to_string(&json!({"protocol":1,"request":request}))
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice::<Value>(&out.stdout).unwrap()["response"].clone()
}

#[test]
fn memory_rpc_reads_versioned_documents_with_unicode_continuations() {
    let tmp = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(tmp.path().join("data"))).unwrap();
    paths.ensure_dirs().unwrap();
    std::fs::write(
        paths.root.join("config.toml"),
        "auto_index_on_search = false\n",
    )
    .unwrap();
    let projects = tmp.path().join("claude/projects");
    let source = projects.join("-work-memex/memory/MEMORY.md");
    std::fs::create_dir_all(source.parent().unwrap()).unwrap();
    let original = "# Architecture\n\nUnicode retrieval café 🦀 evidence.\n";
    std::fs::write(&source, original).unwrap();
    MemoryStore::new(paths.root.join("memory/documents.json"))
        .refresh(&MemoryDiscoveryOptions {
            claude_project_roots: vec![projects],
            codex_homes: vec![],
            enabled_sources: HashSet::from([SourceKind::Claude]),
            exclude_patterns: vec![],
        })
        .unwrap();
    let search = rpc(
        &paths.root,
        json!({"op":"memory_search","options":MemorySearchOptions {
            query:"retrieval".into(), ..Default::default()
        }}),
    );
    assert_eq!(search["kind"], "memory_hits", "{search}");
    let hit = &search["hits"][0];
    assert_eq!(hit["source"], "claude");
    assert!(hit.get("session_id").is_none());
    let mut offset = 0;
    let mut reconstructed = String::new();
    loop {
        let read = rpc(
            &paths.root,
            json!({"op":"memory_read","request":{
                "memory_id":hit["memory_id"], "section_ref":null,
                "content_version":hit["content_version"], "offset_chars":offset,"max_chars":7
            }}),
        );
        assert_eq!(read["kind"], "memory_document", "{read}");
        let doc = &read["document"];
        let text = doc["text"].as_str().unwrap();
        assert!(text.chars().count() <= 7);
        reconstructed.push_str(text);
        let Some(next) = doc["next_offset_chars"].as_u64() else {
            break;
        };
        assert!(next > offset);
        offset = next;
    }
    assert_eq!(reconstructed, original);
    std::fs::write(&source, "# Replacement\nNew content after search.\n").unwrap();
    let changed = rpc(
        &paths.root,
        json!({"op":"memory_read","request":{
            "memory_id":hit["memory_id"], "section_ref":hit["section_ref"],
            "content_version":hit["content_version"],"offset_chars":999,"max_chars":64
        }}),
    );
    assert_eq!(changed["kind"], "memory_document", "{changed}");
    assert_eq!(changed["document"]["changed_since_search"], true);
    assert_eq!(changed["document"]["offset_chars"], 0);
    assert_eq!(changed["document"]["section_ref"], Value::Null);
    assert!(
        changed["document"]["text"]
            .as_str()
            .unwrap()
            .contains("New content")
    );

    let rejected = rpc(
        &paths.root,
        json!({"op":"memory_read","request":{
            "memory_id":hit["memory_id"], "section_ref":null,"content_version":null,
            "offset_chars":0,"max_chars":64001
        }}),
    );
    assert_eq!(rejected["kind"], "error");
}
