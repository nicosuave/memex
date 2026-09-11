use super::discovery::{
    FILE_IDENTITY_PREFIX_BYTES, Narrowing, changed_ns, discovered_metadata, file_identity,
    modified_ns, prepare_refresh, unchanged_file_metadata,
};
use super::execution::{
    RecordSender, build_parser_thread_pool, execute_refresh, finish_file_task, parse_claude_file,
    parse_codex_session, parse_copilot_session, parse_pi_file, record_channel,
};
use super::publication::{
    open_vector_index_for_ingest, prepare_pending_ingest_recovery, recover_checkpoint,
};
use crate::analytics::backfill_from_index;

fn run_writer_fixture(
    index: SearchIndex,
    writer: tantivy::IndexWriter,
    rx: Receiver<Record>,
    decision: Receiver<WriterDecision>,
    deletes: Vec<String>,
    context: WriterContext,
) -> Result<WriterOutcome> {
    drop(writer);
    writer_loop(index, rx, decision, deletes, context)
}

use super::*;
use crate::config::{IndexedToolContentLimits, Paths};
use crate::embed::{EmbedRuntimeConfig, ModelChoice};
use crate::index::SearchIndex;
use crate::state::IngestState;
use crate::test_support::{EnvVarGuard, env_lock};
use crate::vector::VectorIndex;
use std::fs;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn ingest_options(embeddings: bool, model: ModelChoice) -> IngestOptions {
    IngestOptions {
        claude_sources: vec![PathBuf::from("/does/not/exist")],
        exclude_patterns: Vec::new(),
        include_agents: false,
        include_reasoning: false,
        include_codex: false,
        include_opencode: false,
        include_cursor: false,
        include_pi: false,
        include_omp: false,
        include_openclaw: false,
        include_copilot: false,
        include_grok: false,
        include_jcode: false,
        include_muse: false,
        include_antigravity: false,
        embeddings,
        backfill_embeddings: false,
        model,
        embed_runtime: EmbedRuntimeConfig::default(),
        tool_content_limits: IndexedToolContentLimits::default(),
        defer_merges: false,
    }
}

#[test]
fn torn_jsonl_checkpoint_survives_completion_truncation_and_append() {
    use std::io::Write;

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("claude-projects");
    let project = root.join("project");
    fs::create_dir_all(&project).unwrap();
    let path = project.join("session.jsonl");
    let prefix = "{\"type\":\"user\",\"message\":{\"content\":\"prefix\"}}\n";
    let tail = r#"{"type":"user","message":{"content":"completed"}}"#;
    let mut writer = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap();
    writer.write_all(prefix.as_bytes()).unwrap();
    writer
        .write_all(&tail.as_bytes()[..tail.len() - 2])
        .unwrap();
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let lease = ingest_lease(&paths);
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![root];
    let ingest = || ingest_all(&paths, &open_search_index(&paths), &options, &lease).unwrap();
    let saved_state = || {
        IngestState::load(&paths.state.join("ingest.json"))
            .unwrap()
            .files[&path.to_string_lossy().into_owned()]
            .clone()
    };
    let texts = || {
        let index = open_search_index(&paths);
        let mut texts = Vec::new();
        index
            .for_each_record(|record| {
                texts.push(record.text.clone());
                Ok(())
            })
            .unwrap();
        texts.sort();
        texts
    };
    assert_eq!(ingest().records_added, 1);
    assert_eq!(saved_state().offset, prefix.len() as u64);
    assert_eq!(texts(), ["prefix"]);
    assert_eq!(ingest().records_added, 0);
    writer
        .write_all(&tail.as_bytes()[tail.len() - 2..])
        .unwrap();
    assert_eq!(ingest().records_added, 1);
    assert_eq!(texts(), ["completed", "prefix"]);
    assert_eq!(saved_state().offset, path.metadata().unwrap().len());

    let replacement = r#"{"type":"user","message":{"content":"replacement"}}"#;
    writer.set_len(0).unwrap();
    writer
        .write_all(&replacement.as_bytes()[..replacement.len() - 2])
        .unwrap();
    assert_eq!(ingest().records_added, 0);
    assert!(texts().is_empty());
    assert_eq!(saved_state().offset, 0);
    assert_eq!(saved_state().turn_id, 0);
    writer
        .write_all(&replacement.as_bytes()[replacement.len() - 2..])
        .unwrap();
    assert_eq!(ingest().records_added, 1);
    writer.write_all(format!("\n{tail}\n").as_bytes()).unwrap();
    assert_eq!(ingest().records_added, 1);
    assert_eq!(texts(), ["completed", "replacement"]);
    assert_eq!(saved_state().turn_id, 2);
    assert_eq!(ingest().records_added, 0);
}

#[test]
fn exclusion_filters_new_and_previously_indexed_transcripts() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let claude_root = tmp.path().join("claude-projects");
    let keep_dir = claude_root.join("-Users-nico-Code-personal");
    let drop_dir = claude_root.join("-Users-nico-Code-client-x");
    fs::create_dir_all(&keep_dir).expect("create keep dir");
    fs::create_dir_all(&drop_dir).expect("create drop dir");
    let keep_file = keep_dir.join("keep.jsonl");
    let drop_file = drop_dir.join("drop.jsonl");
    let line = br#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hello"}]},"uuid":"u1","timestamp":"2024-01-01T00:00:00Z"}
"#;
    fs::write(&keep_file, line).expect("write keep");
    fs::write(&drop_file, line).expect("write drop");

    let paths = Paths::new(Some(tmp.path().join("memex-root"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    let index = open_search_index(&paths);

    // First run with no exclusions indexes both transcripts.
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![claude_root.clone()];
    let lease = ingest_lease(&paths);
    let report = ingest_all(&paths, &index, &options, &lease).expect("first ingest");
    assert_eq!(report.records_added, 2);
    assert!(index.doc_count().expect("doc count") >= 2);

    // Adding an exclusion removes the previously indexed transcript and
    // never indexes newly discovered files under matched paths.
    let drop_pattern = format!("{}/*-client-*/*.jsonl", claude_root.to_string_lossy());
    options.exclude_patterns = vec![drop_pattern];
    let new_drop = drop_dir.join("new-drop.jsonl");
    fs::write(&new_drop, line).expect("write new drop");
    let report = ingest_all(&paths, &index, &options, &lease).expect("second ingest");
    assert_eq!(
        report.records_added, 0,
        "excluded files must not be indexed"
    );

    let mut remaining = Vec::new();
    index
        .for_each_record(|record| {
            remaining.push(record.source_path.clone());
            Ok(())
        })
        .expect("collect remaining records");
    assert!(
        remaining.iter().all(|p| !p.contains("-client-")),
        "excluded transcripts must be purged from the index, got: {remaining:?}"
    );
    assert!(
        remaining.iter().any(|p| p.contains("keep.jsonl")),
        "non-excluded transcripts must remain indexed, got: {remaining:?}"
    );

    // Ingest state must not retain entries for excluded paths.
    let state = IngestState::load(&paths.state.join("ingest.json")).expect("load state");
    assert!(
        state.files.keys().all(|k| !k.contains("-client-")),
        "excluded paths must be pruned from ingest state"
    );
}

#[test]
fn memory_edits_refresh_inside_transcript_scan_ttl_without_creating_sessions() {
    let tmp = tempfile::tempdir().unwrap();
    let claude = tmp.path().join("claude/projects");
    let project = claude.join("-work-project");
    let memory = project.join("memory/MEMORY.md");
    fs::create_dir_all(memory.parent().unwrap()).unwrap();
    fs::write(project.join("session.jsonl"),
            "{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"a conversation\"},\"uuid\":\"u1\"}\n"
        ).unwrap();
    fs::write(
        &memory,
        "# Decisions\n\nOriginal middle paragraph.\n\n## Retired\nOld decision.\n",
    )
    .unwrap();
    let paths = Paths::new(Some(tmp.path().join("data"))).unwrap();
    paths.ensure_dirs().unwrap();
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![claude];
    let lease = ingest_lease(&paths);
    ingest_all(&paths, &open_search_index(&paths), &options, &lease).unwrap();
    let store = crate::memory::MemoryStore::new(paths.root.join("memory/documents.json"));
    let original = store.load().unwrap();
    assert_eq!(original.documents.len(), 1);
    let original_id = original.documents[0].stable_id.clone();
    let original_version = original.documents[0].version_sha256.clone();
    let published = SearchIndex::open_or_create(&paths.index).unwrap();
    assert_eq!(
        published.doc_count().unwrap(),
        1,
        "memories must not become transcript records"
    );

    fs::write(&memory, "# Decisions\n\nRevised middle paragraph.\n").unwrap();
    assert!(can_skip_fresh_scan(&paths, &published, &options, 3600).unwrap());
    assert!(
        ingest_if_stale(&paths, &published, &options, 3600, &lease, None)
            .unwrap()
            .is_none()
    );
    let updated = store.load().unwrap();
    assert_eq!(updated.documents[0].stable_id, original_id);
    assert_ne!(updated.documents[0].version_sha256, original_version);
    assert!(!updated.documents[0].content.contains("Retired"));
    assert!(updated.documents[0].content.contains("Revised middle"));
    assert_eq!(published.doc_count().unwrap(), 1);

    fs::remove_file(memory).unwrap();
    ingest_if_stale(&paths, &published, &options, 3600, &lease, None).unwrap();
    assert!(store.load().unwrap().documents.is_empty());
}

#[test]
fn ingest_discovers_claude_transcripts_across_multiple_roots() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let first_root = tmp.path().join("claude-one").join("projects");
    let second_root = tmp.path().join("claude-two").join("projects");
    let first_project = first_root.join("-Users-nico-Code-first");
    let second_project = second_root.join("-Users-nico-Code-second");
    fs::create_dir_all(&first_project).expect("create first project");
    fs::create_dir_all(&second_project).expect("create second project");
    fs::write(
        first_project.join("first.jsonl"),
        r#"{"type":"user","message":{"role":"user","content":"first root"},"uuid":"u1"}
"#,
    )
    .expect("write first transcript");
    fs::write(
        second_project.join("second.jsonl"),
        r#"{"type":"user","message":{"role":"user","content":"second root"},"uuid":"u2"}
"#,
    )
    .expect("write second transcript");

    let paths = Paths::new(Some(tmp.path().join("memex-root"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    let index = open_search_index(&paths);
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![first_root, second_root];

    let lease = ingest_lease(&paths);
    let report = ingest_all(&paths, &index, &options, &lease).expect("ingest");

    assert_eq!(report.files_scanned, 2);
    assert_eq!(report.records_added, 2);
}

#[test]
fn exclusion_glob_star_matches_path_separators() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let nested = tmp.path().join("work/deep/project/session.jsonl");
    let patterns = vec![format!("{}/work/**", tmp.path().to_string_lossy())];
    let excluder = PathExcluder::build(&patterns).expect("build excluder");
    assert!(excluder.is_excluded(&nested));
    assert!(!excluder.is_excluded(&tmp.path().join("other/session.jsonl")));
}

#[test]
fn exclusion_invalid_pattern_is_rejected() {
    let result = PathExcluder::build(&["[unclosed".to_string()]);
    assert!(result.is_err());
}

#[test]
fn exclusion_empty_patterns_match_nothing() {
    let excluder = PathExcluder::build(&[]).expect("build excluder");
    assert!(!excluder.is_excluded(Path::new("/anything/at/all.jsonl")));
}

fn save_vector_store(paths: &Paths, model: &str, dimensions: usize) {
    let mut vector = VectorIndex::open_or_create(&paths.vectors, dimensions, Some(model))
        .expect("open vector store");
    vector.add(1, &vec![0.0; dimensions]).expect("add vector");
    vector.save().unwrap();
}

fn open_search_index(paths: &Paths) -> SearchIndex {
    fs::create_dir_all(&paths.index).expect("create index dir");
    SearchIndex::open_or_create(&paths.index).expect("open search index")
}

fn ingest_lease(paths: &Paths) -> IngestLease {
    IngestLease::acquire(paths, "test ingest", Duration::from_secs(1))
        .expect("acquire ingest lease")
}

fn save_search_records(paths: &Paths, records: &[Record]) -> SearchIndex {
    let index = open_search_index(paths);
    let mut writer = index.writer().expect("open index writer");
    for record in records {
        index.add_record(&mut writer, record).expect("add record");
    }
    writer.commit().expect("commit records");
    index
}

fn mark_analytics_complete(paths: &Paths) {
    AnalyticsStore::open(analytics_path(&paths.state))
        .expect("open analytics")
        .mark_complete()
        .expect("mark analytics complete");
}

fn assert_recovers_cross_store_crash(lexical_advanced: bool) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let claude_root = tmp.path().join("claude-projects");
    let project = claude_root.join("-Users-nico-Code-memex");
    fs::create_dir_all(&project).expect("create project");
    let transcript = project.join("session.jsonl");
    fs::write(
            &transcript,
            r#"{"type":"user","uuid":"original","sessionId":"original-session","timestamp":"2026-08-08T10:00:00Z","message":{"content":"original"}}
"#,
        )
        .expect("write original transcript");

    let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    let mut options = ingest_options(false, ModelChoice::default());
    options.claude_sources = vec![claude_root];
    {
        let index =
            SearchIndex::open_or_create_for_ingest(&paths.index).expect("initial generation");
        let lease = ingest_lease(&paths);
        ingest_all(&paths, &index, &options, &lease).expect("initial ingest");
    }
    save_vector_store(&paths, "unavailable-test-model", 4);
    let untouched_vector_pointer =
        fs::read(paths.vectors.join("current.json")).expect("vector pointer");

    fs::write(
            &transcript,
            r#"{"type":"user","uuid":"recovered","sessionId":"recovered-session","timestamp":"2026-08-08T11:00:00Z","message":{"content":"recovered"}}
"#,
        )
        .expect("write recovered transcript");
    let source_path = transcript.to_string_lossy().to_string();
    PendingIngest {
        next_doc_id: 100,
        source_paths: vec![source_path.clone()],
        session_scopes: Vec::new(),
        vector_delete_paths: Vec::new(),
        vector_publication: false,
        embedding_publication: Some(false),
    }
    .save_with_lease(&pending_ingest_path(&paths), &ingest_lease(&paths))
    .expect("save interrupted ingest marker");

    let mut interrupted = record(99, "user", "interrupted");
    interrupted.session_id = "interrupted-session".to_string();
    interrupted.source_path = source_path.clone();
    if lexical_advanced {
        let index = SearchIndex::open_or_create_for_ingest(&paths.index)
            .expect("interrupted lexical generation");
        let mut writer = index.writer().expect("lexical writer");
        index.delete_by_source_path(&mut writer, &source_path);
        index
            .add_record(&mut writer, &interrupted)
            .expect("stage interrupted record");
        writer.commit().expect("commit interrupted lexical state");
        writer.wait_merging_threads().expect("finish lexical write");
        index
            .publish_generation()
            .expect("publish interrupted lexical state");
    } else {
        let mut analytics = AnalyticsWriter::open(analytics_path(&paths.state))
            .expect("interrupted analytics writer");
        analytics
            .delete_source_path(&source_path)
            .expect("delete old analytics row");
        analytics
            .record(&interrupted)
            .expect("stage interrupted analytics row");
        analytics
            .flush()
            .expect("commit interrupted analytics state");
    }

    {
        let index =
            SearchIndex::open_or_create_for_ingest(&paths.index).expect("recovery generation");
        let lease = ingest_lease(&paths);
        ingest_all(&paths, &index, &options, &lease).expect("recover interrupted ingest");
    }

    let index = SearchIndex::open_or_create(&paths.index).expect("published recovery index");
    let mut records = Vec::new();
    index
        .for_each_record(|record| {
            if record.source_path == source_path {
                records.push(record);
            }
            Ok(())
        })
        .expect("collect recovered records");
    assert_eq!(records.len(), 1, "source path must not be duplicated");
    assert_eq!(records[0].session_id, "session");
    assert_eq!(records[0].text, "recovered");
    assert!(records[0].doc_id >= 100);

    let analytics = AnalyticsStore::open(analytics_path(&paths.state)).expect("analytics");
    let sessions = analytics
        .query_sessions_detailed(None, None, None, None, None)
        .expect("query recovered analytics");
    assert_eq!(sessions.len(), 1, "analytics must not retain stale rows");
    assert_eq!(sessions[0].session_id, "session");
    assert_eq!(sessions[0].source_path, source_path);
    assert_eq!(sessions[0].message_count, 1);

    let state = IngestState::load(&paths.state.join("ingest.json")).expect("ingest state");
    assert!(state.next_doc_id > records[0].doc_id);
    assert!(state.files.contains_key(&source_path));
    assert_eq!(
        PendingIngest::load(&pending_ingest_path(&paths)).expect("pending marker"),
        None
    );
    assert_eq!(
        fs::read(paths.vectors.join("current.json")).expect("untouched vector pointer"),
        untouched_vector_pointer,
        "lexical-only recovery must not initialize or publish vectors"
    );
}

#[test]
fn recovery_reconciles_analytics_commit_before_lexical_publish() {
    assert_recovers_cross_store_crash(false);
}

#[test]
fn recovery_reconciles_lexical_publish_before_analytics_commit() {
    assert_recovers_cross_store_crash(true);
}

fn assert_recovers_vector_crash(publish_interrupted_vectors: bool, embedding_publication: bool) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let claude_root = tmp.path().join("claude-projects");
    let project = claude_root.join("-Users-nico-Code-memex");
    fs::create_dir_all(&project).expect("create project");
    let transcript = project.join("session.jsonl");
    fs::write(
            &transcript,
            r#"{"type":"user","uuid":"original","sessionId":"session","timestamp":"2026-08-08T10:00:00Z","message":{"content":"original"}}
"#,
        )
        .expect("write original transcript");

    let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    let mut options = ingest_options(true, ModelChoice::Potion);
    options.claude_sources = vec![claude_root];
    {
        let index =
            SearchIndex::open_or_create_for_ingest(&paths.index).expect("initial generation");
        let lease = ingest_lease(&paths);
        ingest_all(&paths, &index, &options, &lease).expect("initial ingest");
    }
    let original_vector_pointer =
        fs::read(paths.vectors.join("current.json")).expect("original vector pointer");
    let original_vectors = VectorIndex::inventory(&paths.vectors)
        .expect("original vector inventory")
        .expect("original vectors");
    assert_eq!(original_vectors.doc_ids.len(), 1);

    fs::write(
            &transcript,
            r#"{"type":"user","uuid":"recovered","sessionId":"session","timestamp":"2026-08-08T11:00:00Z","message":{"content":"recovered"}}
"#,
        )
        .expect("write recovered transcript");
    let source_path = transcript.to_string_lossy().to_string();
    PendingIngest {
        next_doc_id: 100,
        source_paths: vec![source_path.clone()],
        vector_delete_paths: Vec::new(),
        session_scopes: Vec::new(),
        vector_publication: true,
        embedding_publication: Some(embedding_publication),
    }
    .save_with_lease(&pending_ingest_path(&paths), &ingest_lease(&paths))
    .expect("save interrupted ingest marker");

    let mut interrupted = record(99, "user", "interrupted");
    interrupted.session_id = "interrupted-session".to_string();
    interrupted.source_path = source_path.clone();

    // The writer stages vector files before publishing either pointer.
    let mut interrupted_vectors = VectorIndex::open(&paths.vectors).expect("active vectors");
    interrupted_vectors
        .add(99, &vec![0.0; original_vectors.dimensions])
        .expect("add interrupted vector");
    let staged_vectors = interrupted_vectors
        .stage()
        .expect("stage interrupted vectors");
    assert_eq!(
        fs::read_dir(paths.vectors.join("generations"))
            .expect("vector generations")
            .count(),
        2,
        "a process crash can leave one unpublished generation"
    );

    let interrupted_index = SearchIndex::open_or_create_for_ingest(&paths.index)
        .expect("interrupted lexical generation");
    let mut writer = interrupted_index.writer().expect("lexical writer");
    interrupted_index.delete_by_source_path(&mut writer, &source_path);
    interrupted_index
        .add_record(&mut writer, &interrupted)
        .expect("stage interrupted record");
    writer.commit().expect("commit interrupted lexical state");
    writer.wait_merging_threads().expect("finish lexical write");
    interrupted_index
        .publish_generation()
        .expect("publish interrupted lexical state");

    if publish_interrupted_vectors {
        staged_vectors
            .publish()
            .expect("publish interrupted vectors");
        let active = VectorIndex::inventory(&paths.vectors)
            .expect("interrupted vector inventory")
            .expect("interrupted vectors");
        assert!(active.doc_ids.contains(&99));
    } else {
        assert_eq!(
            fs::read(paths.vectors.join("current.json")).expect("active vector pointer"),
            original_vector_pointer,
            "staging vector files must not expose them before vector publication"
        );
        assert_eq!(
            VectorIndex::inventory(&paths.vectors)
                .expect("active vector inventory")
                .expect("active vectors")
                .doc_ids,
            original_vectors.doc_ids
        );
        std::mem::forget(staged_vectors);
    }

    options.embeddings = false;
    {
        let index =
            SearchIndex::open_or_create_for_ingest(&paths.index).expect("recovery generation");
        let lease = ingest_lease(&paths);
        ingest_all(&paths, &index, &options, &lease).expect("recover interrupted ingest");
    }

    let index = SearchIndex::open_or_create(&paths.index).expect("published recovery index");
    let mut records = Vec::new();
    index
        .for_each_record(|record| {
            if record.source_path == source_path {
                records.push(record);
            }
            Ok(())
        })
        .expect("collect recovered records");
    assert_eq!(records.len(), 1, "source path must not be duplicated");
    assert_eq!(records[0].text, "recovered");
    assert!(records[0].doc_id >= 100);

    let vectors = VectorIndex::inventory(&paths.vectors)
        .expect("recovered vector inventory")
        .expect("recovered vectors");
    assert_eq!(vectors.doc_ids, HashSet::from([records[0].doc_id]));
    assert!(!vectors.doc_ids.contains(&99));
    assert_eq!(
        fs::read_dir(paths.vectors.join("generations"))
            .expect("recovered vector generations")
            .count(),
        1,
        "successful recovery must collect the unpublished generation"
    );

    let analytics = AnalyticsStore::open(analytics_path(&paths.state)).expect("analytics");
    let sessions = analytics
        .query_sessions_detailed(None, None, None, None, None)
        .expect("query recovered analytics");
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].source_path, source_path);
    assert_eq!(sessions[0].message_count, 1);
    assert_eq!(
        PendingIngest::load(&pending_ingest_path(&paths)).expect("pending marker"),
        None
    );
}

#[test]
fn recovery_reconciles_lexical_publish_before_vector_publish() {
    assert_recovers_vector_crash(false, true);
}

#[test]
fn recovery_removes_vectors_published_before_marker_clear() {
    assert_recovers_vector_crash(true, true);
}

#[test]
fn vector_only_pending_ingest_is_completed_when_embeddings_are_disabled() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    let index = save_search_records(
        &paths,
        &[record(1, "user", "first"), record(2, "assistant", "second")],
    );
    index
        .publish_generation_if_uninitialized()
        .expect("publish initial lexical generation");
    save_vector_store(&paths, "potion", 256);
    PendingIngest {
        next_doc_id: 3,
        source_paths: Vec::new(),
        session_scopes: Vec::new(),
        vector_delete_paths: Vec::new(),
        vector_publication: true,
        embedding_publication: Some(true),
    }
    .save(&pending_ingest_path(&paths))
    .expect("save vector-only pending marker");

    let index = SearchIndex::open_or_create_for_ingest(&paths.index).expect("recovery generation");
    let lease = ingest_lease(&paths);
    IngestState {
        next_doc_id: 3,
        ..IngestState::default()
    }
    .save_with_lease(&paths.state.join("ingest.json"), &lease)
    .unwrap();
    let options = ingest_options(false, ModelChoice::Potion);
    ingest_all(&paths, &index, &options, &lease).expect("finish vector recovery");

    let vectors = VectorIndex::inventory(&paths.vectors)
        .expect("vector inventory")
        .expect("vectors");
    assert_eq!(vectors.doc_ids, HashSet::from([1, 2]));
    let state = IngestState::load(&paths.state.join("ingest.json")).unwrap();
    assert!(
        state.files.is_empty(),
        "vector-only recovery needs no file upserts"
    );
    assert_eq!(state.next_doc_id, 3);
    assert_eq!(
        PendingIngest::load(&pending_ingest_path(&paths)).expect("pending marker"),
        None
    );
}

#[test]
fn vector_recovery_runs_when_pending_session_scopes_are_present() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    let index = save_search_records(
        &paths,
        &[record(1, "user", "first"), record(2, "assistant", "second")],
    );
    index
        .publish_generation_if_uninitialized()
        .expect("publish initial lexical generation");
    let mut vectors = VectorIndex::open_or_create(&paths.vectors, 256, Some("potion"))
        .expect("create interrupted vectors");
    vectors.add(1, &vec![0.0; 256]).expect("add live vector");
    vectors.add(99, &vec![0.0; 256]).expect("add stale vector");
    vectors.save().expect("save interrupted vectors");
    PendingIngest {
        next_doc_id: 3,
        source_paths: Vec::new(),
        session_scopes: vec![SessionScope {
            source_path: "/unavailable/opencode.db".to_string(),
            session_id: "deferred-session".to_string(),
        }],
        vector_delete_paths: Vec::new(),
        vector_publication: true,
        embedding_publication: Some(true),
    }
    .save(&pending_ingest_path(&paths))
    .expect("save pending vector publication with deferred scope");

    let index = SearchIndex::open_or_create_for_ingest(&paths.index).expect("recovery generation");
    let lease = ingest_lease(&paths);
    IngestState {
        next_doc_id: 3,
        ..IngestState::default()
    }
    .save_with_lease(&paths.state.join("ingest.json"), &lease)
    .unwrap();
    let options = ingest_options(false, ModelChoice::Potion);
    ingest_all(&paths, &index, &options, &lease).expect("finish vector recovery");

    let vectors = VectorIndex::inventory(&paths.vectors)
        .expect("vector inventory")
        .expect("vectors");
    assert_eq!(vectors.doc_ids, HashSet::from([1, 2]));
    let pending = PendingIngest::load(&pending_ingest_path(&paths))
        .unwrap()
        .unwrap();
    assert_eq!(pending.next_doc_id, 3);
    assert!(pending.source_paths.is_empty());
    assert!(!pending.vector_publication);
    assert_eq!(
        pending.session_scopes,
        vec![SessionScope {
            source_path: "/unavailable/opencode.db".to_string(),
            session_id: "deferred-session".to_string(),
        }]
    );
    assert!(!pending_ingest_path(&paths).exists());
}

#[test]
fn parser_pool_leaves_global_rayon_available_under_backpressure() {
    let parser_pool = build_parser_thread_pool(2).expect("build parser pool");
    let (tx, rx) = bounded::<usize>(1);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let consumer = std::thread::spawn(move || {
        let first = rx.recv().expect("receive first parser result");
        let sum: usize = (0..1_000usize).into_par_iter().sum();
        let count = 1 + rx.iter().count();
        done_tx.send((first, sum, count)).expect("report result");
    });

    parser_pool.install(|| {
        (0..4usize)
            .into_par_iter()
            .for_each(|value| tx.send(value).expect("send parser result"));
    });
    drop(tx);

    let (_first, sum, count) = done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("global Rayon work should not deadlock behind parser backpressure");
    consumer.join().expect("join consumer");
    assert_eq!(sum, (0..1_000usize).sum::<usize>());
    assert_eq!(count, 4);
}

fn incremental_task(
    path: &Path,
    source: SourceKind,
    offset: u64,
    turn_id: u32,
    pending_tool_calls: HashMap<String, PendingToolCall>,
) -> FileTask {
    let metadata = path.metadata().expect("transcript metadata");
    FileTask {
        codex_metadata_offsets: None,
        path: path.to_path_buf(),
        source,
        offset,
        turn_id,
        legacy_turn_id: Some(turn_id),
        claude_background: None,
        size: metadata.len(),
        mtime: metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or(0),
        change: FileChange::Append,
        pending_tool_calls,
        identity: file_identity(
            path,
            &metadata,
            metadata.len().min(FILE_IDENTITY_PREFIX_BYTES as u64) as usize,
        ),
        parser_version: crate::sources::index_state_version(source),
    }
}

fn parser_channels() -> (
    RecordSender,
    Receiver<Record>,
    Sender<FileUpdate>,
    Receiver<FileUpdate>,
) {
    let (raw_tx_record, rx_record) = unbounded();
    let (tx_update, rx_update) = unbounded();
    (
        RecordSender::new(raw_tx_record, IndexedToolContentLimits::default()),
        rx_record,
        tx_update,
        rx_update,
    )
}

fn record(doc_id: u64, role: &str, text: &str) -> Record {
    Record {
        source: SourceKind::Claude,
        doc_id,
        ts: doc_id,
        project: "project".to_string(),
        session_id: "session".to_string(),
        turn_id: doc_id as u32,
        role: role.to_string(),
        text: text.to_string(),
        tool_name: None,
        tool_input: None,
        tool_output: None,
        links: RecordLinks::default(),
        source_path: format!("source-{doc_id}.jsonl"),
    }
}

#[test]
fn record_channel_applies_backpressure_at_capacity() {
    let (tx_record, _rx_record) = record_channel();
    for doc_id in 0..RECORD_CHANNEL_CAPACITY {
        tx_record
            .try_send(record(doc_id as u64, "assistant", "text"))
            .expect("record within channel capacity");
    }

    let result = tx_record.try_send(record(RECORD_CHANNEL_CAPACITY as u64, "assistant", "text"));
    assert!(matches!(
        result,
        Err(crossbeam_channel::TrySendError::Full(_))
    ));
}

#[test]
fn transcript_removed_after_discovery_is_skipped() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("removed.jsonl");
    fs::write(&path, "{}\n").expect("seed transcript");
    let metadata = path.metadata().expect("transcript metadata");
    let (task, skip) =
        discovery::prepare_file_task(path.clone(), SourceKind::Claude, false, &metadata, None);
    assert!(!skip);
    fs::remove_file(&path).expect("remove transcript after discovery");
    assert!(
        discovered_metadata(&path)
            .expect("missing metadata should not fail")
            .is_none()
    );

    let (raw_tx_record, _rx_record) = unbounded();
    let tx_record = RecordSender::new(raw_tx_record, IndexedToolContentLimits::default());
    let (tx_update, _rx_update) = unbounded();
    let next_doc_id = AtomicU64::new(1);
    let progress = Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], false));
    let skipped = AtomicUsize::new(0);

    let parse_result = parse_claude_file(
        &task,
        false,
        &tx_record,
        &tx_update,
        &next_doc_id,
        &progress,
    );
    finish_file_task(&task, &progress, &skipped, parse_result)
        .expect("removed transcript should be skipped");

    assert_eq!(skipped.load(Ordering::Relaxed), 1);
}

#[test]
fn writer_initialization_error_is_not_masked_by_disconnected_channel() {
    let temp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(temp.path().join("memex"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    let claude_root = temp.path().join("claude");
    let project = claude_root.join("-tmp-project");
    fs::create_dir_all(&project).expect("create project");
    fs::write(
            project.join("session.jsonl"),
            r#"{"type":"user","uuid":"u1","sessionId":"session","timestamp":"2026-07-26T17:00:00Z","message":{"content":"hello"}}"#,
        )
        .expect("write transcript");
    let index = SearchIndex::open_or_create(&paths.index).expect("index");
    let _existing_writer = index.writer().expect("existing writer");
    let lease = ingest_lease(&paths);
    let mut options = ingest_options(false, ModelChoice::default());
    options.claude_sources = vec![claude_root];

    let error = ingest_all(&paths, &index, &options, &lease).expect_err("writer collision");
    let message = format!("{error:#}");

    assert!(message.contains("failed to initialize the Tantivy index writer"));
    assert!(!message.contains("disconnected channel"));
}

#[test]
fn cancelled_writer_does_not_publish_staged_records() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    let index = save_search_records(&paths, &[record(1, "user", "existing")]);
    backfill_from_index(analytics_path(&paths.state), &index).expect("seed analytics");
    let (tx_record, rx_record) = unbounded();
    tx_record
        .send(record(2, "user", "staged"))
        .expect("send staged record");
    drop(tx_record);
    let (decision_tx, decision_rx) = bounded(1);
    decision_tx.send(WriterDecision::Cancel).expect("cancel");
    let ctx = WriterContext {
        index_root: PathBuf::new(),
        input_bytes: None,
        defer_merges: false,
        embeddings: false,
        do_backfill_embeddings: false,
        reset_vector_store: false,
        vector_dir: paths.vectors.clone(),
        analytics_path: analytics_path(&paths.state),
        progress: Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], false)),
        model: ModelChoice::default(),
        embed_runtime: EmbedRuntimeConfig::default(),
        tool_content_limits: IndexedToolContentLimits::default(),
        reconcile_vector_ids: false,
        scope_targets: Vec::new(),
        opencode_session_cwds: HashMap::new(),
        repositories: Arc::new(crate::repository::RepositoryResolver::default()),
        codex_metadata_checkpoints: HashMap::new(),
        vector_delete_paths: HashSet::new(),
    };
    let writer = index.writer().expect("writer");

    let outcome = run_writer_fixture(
        index.clone(),
        writer,
        rx_record,
        decision_rx,
        vec!["source-1.jsonl".to_string()],
        ctx,
    )
    .expect("cancel writer");

    assert_eq!(outcome, WriterOutcome::Cancelled);
    assert_eq!(index.doc_count().expect("document count"), 1);
    let existing = index
        .get_by_doc_id(1)
        .expect("existing lexical lookup")
        .expect("existing lexical record");
    assert_eq!(existing.source_path, "source-1.jsonl");
    let analytics = AnalyticsStore::open(analytics_path(&paths.state)).expect("analytics");
    let sessions = analytics
        .query_sessions_detailed(None, None, None, None, None)
        .expect("analytics sessions");
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].source_path, "source-1.jsonl");
}

#[test]
fn checkpoint_only_writer_skips_embedding_initialization() {
    let tmp = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let mut writer = index.writer().unwrap();
    index
        .add_record(&mut writer, &record(1, "user", "existing"))
        .unwrap();
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
    index.publish_generation().unwrap();
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    let generation = fs::read(paths.index.join("CURRENT")).unwrap();
    let (records, rx) = unbounded();
    drop(records);
    let (decision, decisions) = bounded(1);
    decision.send(WriterDecision::Commit).unwrap();
    let outcome = writer_loop(
        index,
        rx,
        decisions,
        Vec::new(),
        WriterContext {
            index_root: paths.index.clone(),
            defer_merges: false,
            input_bytes: Some(0),
            embeddings: true,
            do_backfill_embeddings: false,
            reset_vector_store: false,
            vector_dir: paths.vectors.clone(),
            analytics_path: analytics_path(&paths.state),
            progress: Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], true)),
            model: ModelChoice::Gemma,
            embed_runtime: EmbedRuntimeConfig::default(),
            tool_content_limits: IndexedToolContentLimits::default(),
            reconcile_vector_ids: false,
            scope_targets: Vec::new(),
            opencode_session_cwds: HashMap::new(),
            repositories: Arc::new(crate::repository::RepositoryResolver::default()),
            codex_metadata_checkpoints: HashMap::new(),
            vector_delete_paths: HashSet::new(),
        },
    )
    .unwrap();
    assert_eq!(outcome, WriterOutcome::CheckpointsOnly);
    assert_eq!(fs::read(paths.index.join("CURRENT")).unwrap(), generation);
    assert!(!VectorIndex::exists(&paths.vectors));
}

#[test]
fn replacing_transcript_removes_its_old_vectors() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("claude");
    fs::create_dir_all(&source).unwrap();
    let transcript = source.join("session.jsonl");
    append_claude_message(&transcript, "original");
    let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![source];
    let lease = ingest_lease(&paths);
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    ingest_all(&paths, &index, &options, &lease).unwrap();
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    let mut ids = Vec::new();
    index
        .for_each_record(|record| {
            ids.push(record.doc_id);
            Ok(())
        })
        .unwrap();
    let mut vectors = VectorIndex::open_or_create(&paths.vectors, 256, Some("potion")).unwrap();
    for id in &ids {
        vectors.add(*id, &vec![0.1; 256]).unwrap();
    }
    vectors.save().unwrap();
    drop(vectors);
    fs::remove_file(&transcript).unwrap();
    append_claude_message(&transcript, "replacement");
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    ingest_all(&paths, &index, &options, &lease).unwrap();
    let vectors = VectorIndex::open(&paths.vectors).unwrap();
    assert!(ids.iter().all(|id| !vectors.contains(*id)));
    assert_eq!(indexed_texts(&paths), ["replacement"]);
}

#[test]
fn enabling_embeddings_backfills_an_unchanged_lexical_index() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("claude");
    fs::create_dir_all(&source).unwrap();
    append_claude_message(&source.join("session.jsonl"), "existing searchable message");
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let lease = ingest_lease(&paths);
    let mut options = ingest_options(false, ModelChoice::Potion);
    options.claude_sources = vec![source];
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    ingest_all(&paths, &index, &options, &lease).unwrap();
    assert!(!VectorIndex::exists(&paths.vectors));
    options.embeddings = true;
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    let report = ingest_all(&paths, &index, &options, &lease).unwrap();
    assert_eq!(report.records_added, 0);
    assert_eq!(report.records_embedded, 1);
    assert_eq!(VectorIndex::open(&paths.vectors).unwrap().len(), 1);
}

#[test]
fn parser_cancellation_preserves_active_vectors() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    save_vector_store(&paths, "potion", 256);
    let original_pointer =
        fs::read(paths.vectors.join("current.json")).expect("original vector pointer");
    let original_inventory = VectorIndex::inventory(&paths.vectors)
        .expect("original inventory")
        .expect("original vectors");

    let index = open_search_index(&paths);
    let (tx_record, rx_record) = unbounded();
    for offset in 0..EMBED_BATCH_SIZE {
        tx_record
            .send(record(100 + offset as u64, "user", "staged replacement"))
            .expect("send staged embedding record");
    }
    drop(tx_record);
    let (decision_tx, decision_rx) = bounded(1);
    // This is the decision ingest_all sends when any parser fails.
    decision_tx
        .send(WriterDecision::Cancel)
        .expect("cancel after parser failure");
    let ctx = WriterContext {
        index_root: PathBuf::new(),
        input_bytes: None,
        defer_merges: false,
        embeddings: true,
        do_backfill_embeddings: false,
        reset_vector_store: true,
        vector_dir: paths.vectors.clone(),
        analytics_path: analytics_path(&paths.state),
        progress: Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], true)),
        model: ModelChoice::Potion,
        embed_runtime: EmbedRuntimeConfig::default(),
        tool_content_limits: IndexedToolContentLimits::default(),
        reconcile_vector_ids: false,
        scope_targets: Vec::new(),
        opencode_session_cwds: HashMap::new(),
        repositories: Arc::new(crate::repository::RepositoryResolver::default()),
        codex_metadata_checkpoints: HashMap::new(),
        vector_delete_paths: HashSet::new(),
    };
    let writer = index.writer().expect("writer");

    let outcome = run_writer_fixture(index, writer, rx_record, decision_rx, Vec::new(), ctx)
        .expect("cancel writer");

    assert_eq!(outcome, WriterOutcome::Cancelled);
    assert_eq!(
        fs::read(paths.vectors.join("current.json")).expect("active vector pointer"),
        original_pointer
    );
    let published = VectorIndex::inventory(&paths.vectors)
        .expect("published inventory")
        .expect("published vectors");
    assert_eq!(published.doc_ids, original_inventory.doc_ids);
    assert_eq!(published.vector_count, original_inventory.vector_count);
}

#[test]
fn record_sender_caps_tool_payloads_but_keeps_plain_text() {
    let limits = IndexedToolContentLimits {
        input_bytes: 1024,
        output_bytes: 2048,
    };
    let plain_text = format!("plain-begin{}plain-end", "w".repeat(4096));
    let plain = record(1, "assistant", &plain_text);

    let mut tool_use = record(
        2,
        "tool_use",
        &format!("input-begin{}input-end", "🦀".repeat(2048)),
    );
    tool_use.tool_input = Some(tool_use.text.clone());
    let mut tool_result = record(
        3,
        "tool_result",
        &format!("output-begin{}output-end", "y".repeat(4096)),
    );
    tool_result.tool_output = Some(tool_result.text.clone());
    let role_only_tool_result = record(
        4,
        "tool_result",
        &format!("role-output-begin{}role-output-end", "z".repeat(4096)),
    );

    let (raw_tx, rx) = unbounded();
    let tx = RecordSender::new(raw_tx, limits);
    tx.send(plain).expect("queue plain record");
    tx.send(tool_use).expect("queue tool-use record");
    tx.send(tool_result).expect("queue tool-result record");
    tx.send(role_only_tool_result)
        .expect("queue role-only tool-result record");
    drop(tx);
    let records = rx.iter().collect::<Vec<_>>();

    assert_eq!(records[0].text, plain_text);
    assert_truncated_content(
        &records[1].text,
        limits.input_bytes,
        "input-begin",
        "input-end",
    );
    assert_truncated_content(
        records[1].tool_input.as_deref().expect("tool input"),
        limits.input_bytes,
        "input-begin",
        "input-end",
    );
    assert_truncated_content(
        &records[2].text,
        limits.output_bytes,
        "output-begin",
        "output-end",
    );
    assert_truncated_content(
        records[2].tool_output.as_deref().expect("tool output"),
        limits.output_bytes,
        "output-begin",
        "output-end",
    );
    assert_truncated_content(
        &records[3].text,
        limits.output_bytes,
        "role-output-begin",
        "role-output-end",
    );
}

fn assert_truncated_content(content: &str, max_bytes: usize, prefix: &str, suffix: &str) {
    assert!(content.len() <= max_bytes);
    assert!(content.starts_with(prefix));
    assert!(content.contains("bytes truncated"));
    assert!(content.ends_with(suffix));
}

fn fresh_scan_cache() -> ScanCache {
    let last_scan_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_secs();
    ScanCache {
        last_scan_ts,
        file_count: 0,
        total_bytes: 0,
    }
}

#[test]
fn database_races_are_failed_but_successful_inventory_absence_is_confirmed() {
    assert_eq!(
        classify_opencode_database_outcome(None, true, true),
        OpencodeDatabaseOutcome::Failed
    );
    assert_eq!(
        classify_opencode_database_outcome(None, false, true),
        OpencodeDatabaseOutcome::ConfirmedAbsent
    );
    assert_eq!(
        classify_opencode_database_outcome(None, false, false),
        OpencodeDatabaseOutcome::Failed
    );
}

#[test]
fn failed_database_owner_has_priority_over_ready_duplicate() {
    let mut owners = HashMap::new();
    claim_opencode_session_owner(&mut owners, "session".to_string(), "/failed.db");
    claim_opencode_session_owner(&mut owners, "session".to_string(), "/ready.db");
    assert_eq!(
        owners.get("session").map(String::as_str),
        Some("/failed.db")
    );
}

#[test]
fn prepublication_pending_intent_keeps_active_and_deferred_scopes() {
    let active = SessionScope {
        source_path: "/ready.db".to_string(),
        session_id: "active".to_string(),
    };
    let deferred = SessionScope {
        source_path: "/failed.db".to_string(),
        session_id: "deferred".to_string(),
    };
    let scopes = pending_scope_union(
        std::slice::from_ref(&active),
        &[deferred.clone(), active.clone()],
    );
    assert_eq!(scopes, vec![deferred, active]);
}

#[test]
fn no_publish_refresh_retains_deferred_scopes_and_updates_cache_atomically() {
    let temp = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let index = save_search_records(&paths, &[record(1, "user", "original")]);
    index.publish_generation_if_uninitialized().unwrap();
    let lease = ingest_lease(&paths);
    IngestState {
        next_doc_id: 2,
        ..Default::default()
    }
    .save_with_lease(&paths.state.join("ingest.json"), &lease)
    .unwrap();
    let first = SessionScope {
        source_path: "/deferred-a.db".to_string(),
        session_id: "a".to_string(),
    };
    let second = SessionScope {
        source_path: "/deferred-b.db".to_string(),
        session_id: "b".to_string(),
    };
    PendingIngest {
        next_doc_id: 50,
        source_paths: Vec::new(),
        session_scopes: vec![second.clone(), first.clone(), second.clone()],
        vector_delete_paths: Vec::new(),
        vector_publication: false,
        embedding_publication: Some(false),
    }
    .save_with_lease(&pending_ingest_path(&paths), &lease)
    .unwrap();
    let options = ingest_options(false, ModelChoice::Gemma);
    let index = open_search_index(&paths);
    let report = ingest_all(&paths, &index, &options, &lease).unwrap();
    assert_eq!(report.records_added, 0);
    assert_eq!(indexed_texts(&paths), ["original"]);
    let header = CheckpointReader::open(&paths.state.join("ingest.json"))
        .unwrap()
        .header()
        .unwrap();
    assert_eq!(header.next_doc_id, 50);
    assert!(header.scan_cache.is_fresh(3600));
    assert_eq!(header.scan_cache.file_count, 0);
    assert_eq!(
        header.pending,
        Some(PendingIngest {
            next_doc_id: 50,
            source_paths: Vec::new(),
            session_scopes: vec![first, second],
            vector_delete_paths: Vec::new(),
            vector_publication: false,
            embedding_publication: Some(false),
        })
    );
    assert!(!pending_ingest_path(&paths).exists());
}

#[test]
fn pending_session_scopes_retain_database_state_for_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let database_path = temp
        .path()
        .join("opencode.db")
        .to_string_lossy()
        .to_string();
    let scope = SessionScope {
        source_path: database_path.clone(),
        session_id: "session".to_string(),
    };
    let mut state = IngestState::default();
    state.opencode_databases.insert(
        database_path.clone(),
        crate::state::OpencodeDatabaseState {
            owned_session_ids: ["session".to_string()].into_iter().collect(),
            ..Default::default()
        },
    );
    let lease = ingest_lease(&paths);
    PendingIngest {
        next_doc_id: 1,
        source_paths: Vec::new(),
        session_scopes: vec![scope],
        vector_delete_paths: Vec::new(),
        vector_publication: false,
        embedding_publication: Some(false),
    }
    .save_with_lease(&pending_ingest_path(&paths), &lease)
    .unwrap();
    let state_path = paths.state.join("ingest.json");
    state.save_with_lease(&state_path, &lease).unwrap();
    let mut state = CheckpointSession::open(&state_path, &lease, false, None).unwrap();
    prepare_pending_ingest_recovery(&mut state).expect("pending recovery");
    assert!(state.opencode_databases.contains_key(&database_path));

    PendingIngest::clear_with_lease(&pending_ingest_path(&paths), &lease).unwrap();
    PendingIngest {
        next_doc_id: 1,
        source_paths: vec![database_path.clone()],
        session_scopes: Vec::new(),
        vector_delete_paths: Vec::new(),
        vector_publication: false,
        embedding_publication: Some(false),
    }
    .save_with_lease(&pending_ingest_path(&paths), &lease)
    .unwrap();
    prepare_pending_ingest_recovery(&mut state).expect("full-path pending recovery");
    state
        .commit_intent(&PendingIngest {
            next_doc_id: 1,
            source_paths: vec![database_path.clone()],
            session_scopes: Vec::new(),
            vector_delete_paths: Vec::new(),
            vector_publication: false,
            embedding_publication: Some(false),
        })
        .unwrap();
    prepare_pending_ingest_recovery(&mut state).expect("full-path pending recovery");
    assert!(!state.opencode_databases.contains_key(&database_path));
}

#[test]
fn stale_opencode_spools_are_cleaned_without_touching_other_state() {
    let temp = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let stale = paths.state.join(format!("{OPENCODE_SPOOL_PREFIX}stale"));
    let unrelated = paths.state.join("unrelated");
    fs::write(&stale, b"stale").unwrap();
    fs::write(&unrelated, b"keep").unwrap();
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    ingest_all(
        &paths,
        &index,
        &ingest_options(false, ModelChoice::default()),
        &ingest_lease(&paths),
    )
    .unwrap();
    assert!(!stale.exists());
    assert_eq!(fs::read(unrelated).unwrap(), b"keep");
}

#[test]
fn empty_index_rebuild_persists_cleared_database_state() {
    let temp = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let database_path = temp
        .path()
        .join("opencode.db")
        .to_string_lossy()
        .to_string();
    let mut state = IngestState {
        next_doc_id: 57,
        ..IngestState::default()
    };
    state.opencode_databases.insert(
        database_path,
        crate::state::OpencodeDatabaseState::default(),
    );
    state.save(&paths.state.join("ingest.json")).unwrap();
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    ingest_all(
        &paths,
        &index,
        &ingest_options(false, ModelChoice::default()),
        &ingest_lease(&paths),
    )
    .unwrap();
    let state = IngestState::load(&paths.state.join("ingest.json")).unwrap();
    assert!(state.opencode_databases.is_empty());
    assert_eq!(state.next_doc_id, 57);
}

#[test]
fn modern_opencode_database_ingests_once_and_skips_noop_hydration() {
    use tantivy::Directory;

    let _guard = env_lock();
    let tmp = tempfile::tempdir().expect("tempdir");
    let db_path = tmp.path().join("opencode.db");
    let db = rusqlite::Connection::open(&db_path).expect("open OpenCode fixture");
    db.execute_batch(
            "CREATE TABLE session (
                id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT,
                time_created INTEGER, time_updated INTEGER
             );
             CREATE TABLE message (
                id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT
             );
             CREATE TABLE part (
                id TEXT PRIMARY KEY, message_id TEXT, data TEXT
             );
             CREATE TABLE event (id TEXT NOT NULL, aggregate_id TEXT NOT NULL);
             INSERT INTO session VALUES ('ses_db-session', NULL, '/tmp', 1, 2);
             INSERT INTO message VALUES ('db-message', 'ses_db-session', 3, '{\"role\":\"assistant\"}');
             INSERT INTO part VALUES ('db-part', 'db-message', '{\"type\":\"text\",\"text\":\"before\"}');
             INSERT INTO event VALUES ('db-event-1', 'ses_db-session');",
        )
        .expect("write OpenCode fixture");
    drop(db);
    let _env = EnvVarGuard::set_os(&[("OPENCODE_DATA_DIR", Some(tmp.path().as_os_str()))]);
    let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
    paths.ensure_dirs().expect("ensure paths");
    let index = open_search_index(&paths);
    let legacy_path = tmp.path().join("storage/message/ses_db-session");
    fs::create_dir_all(&legacy_path).expect("create legacy session");
    let mut legacy_record = record(900, "user", "legacy copy");
    legacy_record.source = SourceKind::Opencode;
    legacy_record.session_id = "ses_db-session".to_string();
    legacy_record.source_path = legacy_path.to_string_lossy().to_string();
    let mut seed_writer = index.writer().expect("open seed writer");
    index
        .add_record(&mut seed_writer, &legacy_record)
        .expect("seed legacy record");
    seed_writer.commit().expect("commit legacy record");
    drop(seed_writer);
    let mut vectors = VectorIndex::open_or_create(&paths.vectors, 4, Some("fixture"))
        .expect("create legacy vector");
    vectors
        .add(legacy_record.doc_id, &[1.0, 0.0, 0.0, 0.0])
        .expect("seed legacy vector");
    vectors.save().expect("save legacy vector");
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.include_opencode = true;

    IngestState {
        next_doc_id: 901,
        ..IngestState::default()
    }
    .save_with_lease(&paths.state.join("ingest.json"), &ingest_lease(&paths))
    .unwrap();
    let first = ingest_all(&paths, &index, &options, &ingest_lease(&paths));
    assert_eq!(first.expect("initial database ingest").records_added, 1);
    let source_path = db_path.to_string_lossy().to_string();
    let records = index
        .records_by_session_id("ses_db-session")
        .expect("database records");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].source_path, source_path);
    assert_eq!(records[0].text, "before");
    assert!(
        !VectorIndex::inventory(&paths.vectors)
            .expect("inspect legacy vector removal")
            .expect("legacy vectors")
            .doc_ids
            .contains(&legacy_record.doc_id)
    );
    let analytics = crate::analytics::AnalyticsStore::open_read_only(analytics_path(&paths.state))
        .expect("open analytics");
    let sessions = analytics
        .query_sessions(
            Some(crate::types::SourceFilter::Opencode),
            None,
            None,
            crate::analytics::ProjectGrouping::Flat,
            None,
        )
        .expect("query analytics");
    assert_eq!(sessions[0].cwd.as_deref(), Some("/tmp"));

    let metadata_before = index
        .index
        .directory()
        .atomic_read(Path::new("meta.json"))
        .unwrap();
    let second = ingest_all(&paths, &index, &options, &ingest_lease(&paths));
    assert_eq!(second.expect("no-op database ingest").records_added, 0);
    assert_eq!(
        metadata_before,
        index
            .index
            .directory()
            .atomic_read(Path::new("meta.json"))
            .unwrap()
    );

    let mut analytics_writer = AnalyticsWriter::open(analytics_path(&paths.state)).unwrap();
    analytics_writer.record(&legacy_record).unwrap();
    analytics_writer.flush().unwrap();
    assert!(
        analytics
            .source_paths(&HashSet::from([legacy_record.source_path.clone()]))
            .unwrap()
            .contains(legacy_path.to_str().unwrap())
    );
    assert!(
        index
            .doc_ids_by_source_path(legacy_path.to_str().unwrap())
            .unwrap()
            .is_empty()
    );
    ingest_all(&paths, &index, &options, &ingest_lease(&paths)).unwrap();
    assert!(
        !analytics
            .source_paths(&HashSet::from([legacy_record.source_path.clone()]))
            .unwrap()
            .contains(legacy_path.to_str().unwrap())
    );
    let cleaned = index
        .index
        .directory()
        .atomic_read(Path::new("meta.json"))
        .unwrap();
    ingest_all(&paths, &index, &options, &ingest_lease(&paths)).unwrap();
    assert_eq!(
        cleaned,
        index
            .index
            .directory()
            .atomic_read(Path::new("meta.json"))
            .unwrap()
    );

    let db = rusqlite::Connection::open(&db_path).expect("reopen OpenCode fixture");
    db.execute(
        "UPDATE part SET data = '{\"type\":\"text\",\"text\":\"without event\"}'",
        [],
    )
    .expect("update part without event");
    drop(db);
    let third = ingest_all(&paths, &index, &options, &ingest_lease(&paths));
    assert_eq!(
        third.expect("unchanged event cursor ingest").records_added,
        0
    );
    assert_eq!(
        index.records_by_session_id("ses_db-session").unwrap()[0].text,
        "before"
    );
    let doc_id = index.records_by_session_id("ses_db-session").unwrap()[0].doc_id;
    let mut vectors = VectorIndex::open_or_create(&paths.vectors, 4, Some("fixture"))
        .expect("create fixture vectors");
    vectors
        .add(doc_id, &[1.0, 0.0, 0.0, 0.0])
        .expect("seed fixture vector");
    vectors.save().expect("save fixture vector");

    let db = rusqlite::Connection::open(&db_path).expect("reopen OpenCode fixture");
    db.execute_batch(
        "UPDATE part SET data = '{\"type\":\"text\",\"text\":\"after event\"}';
             INSERT INTO event VALUES ('db-event-2', 'ses_db-session');",
    )
    .expect("update OpenCode fixture");
    drop(db);
    let fourth = ingest_all(&paths, &index, &options, &ingest_lease(&paths));
    assert_eq!(fourth.expect("event database ingest").records_added, 1);
    let records = index
        .records_by_session_id("ses_db-session")
        .expect("updated records");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].text, "after event");
    assert!(
        VectorIndex::inventory(&paths.vectors)
            .expect("inspect updated vectors")
            .expect("fixture vectors")
            .doc_ids
            .is_empty()
    );

    fs::remove_dir_all(&legacy_path).expect("remove legacy fallback");
    let unrelated_source = tmp.path().join("claude");
    fs::create_dir_all(&unrelated_source).expect("create unrelated source");
    fs::write(
            unrelated_source.join("unrelated.jsonl"),
            r#"{"type":"user","uuid":"unrelated","sessionId":"unrelated-session","timestamp":"2026-08-08T11:00:00Z","message":{"content":"unrelated source"}}
"#,
        )
        .expect("write unrelated source");
    options.claude_sources = vec![unrelated_source];
    PendingIngest {
        next_doc_id: 1,
        source_paths: Vec::new(),
        session_scopes: vec![SessionScope {
            source_path: source_path.clone(),
            session_id: "ses_db-session".to_string(),
        }],
        vector_delete_paths: Vec::new(),
        vector_publication: false,
        embedding_publication: Some(false),
    }
    .save_with_lease(&pending_ingest_path(&paths), &ingest_lease(&paths))
    .expect("save pending database scope");
    fs::write(&db_path, b"corrupt OpenCode database").expect("corrupt OpenCode database");
    ingest_all(&paths, &index, &options, &ingest_lease(&paths))
        .expect("failed database falls back safely");
    assert_eq!(index.doc_count().expect("remaining document count"), 2);
    assert_eq!(
        index.records_by_session_id("ses_db-session").unwrap()[0].text,
        "after event"
    );
    let state = IngestState::load(&paths.state.join("ingest.json")).expect("load ingest state");
    assert!(
        state.opencode_databases.contains_key(&source_path),
        "failed database state must be preserved"
    );
    let pending = PendingIngest::load(&pending_ingest_path(&paths))
        .expect("load deferred scope")
        .expect("deferred scope marker");
    assert_eq!(pending.source_paths, Vec::<String>::new());
    assert_eq!(pending.session_scopes.len(), 1);

    fs::remove_file(&db_path).expect("remove corrupt database");
    let db = rusqlite::Connection::open(&db_path).expect("recreate OpenCode fixture");
    db.execute_batch(
            "CREATE TABLE session (
                id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT,
                time_created INTEGER, time_updated INTEGER
             );
             CREATE TABLE message (
                id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT
             );
             CREATE TABLE part (
                id TEXT PRIMARY KEY, message_id TEXT, data TEXT
             );
             CREATE TABLE event (id TEXT NOT NULL, aggregate_id TEXT NOT NULL);
             INSERT INTO session VALUES ('ses_db-session', NULL, '/recovered', 2, 3);
             INSERT INTO message VALUES ('recovered-message', 'ses_db-session', 4, '{\"role\":\"assistant\"}');
             INSERT INTO part VALUES ('recovered-part', 'recovered-message', '{\"type\":\"text\",\"text\":\"recovered\"}');
             INSERT INTO session VALUES ('ses_reappeared', NULL, '/reappeared', 4, 5);
             INSERT INTO message VALUES ('reappeared-message', 'ses_reappeared', 6, '{\"role\":\"assistant\"}');
             INSERT INTO part VALUES ('reappeared-part', 'reappeared-message', '{\"type\":\"text\",\"text\":\"reappeared\"}');
             INSERT INTO event VALUES ('reappeared-event', 'ses_reappeared');",
        )
        .expect("write reappeared OpenCode fixture");
    drop(db);
    ingest_all(&paths, &index, &options, &ingest_lease(&paths))
        .expect("reappeared database cold hydration");
    let records = index
        .records_by_session_id("ses_reappeared")
        .expect("reappeared database records");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].text, "reappeared");
    assert_eq!(
        index.records_by_session_id("ses_db-session").unwrap()[0].text,
        "recovered"
    );
    let state = IngestState::load(&paths.state.join("ingest.json")).expect("load ingest state");
    assert!(state.opencode_databases.contains_key(&source_path));
    assert!(
        PendingIngest::load(&pending_ingest_path(&paths))
            .expect("load resolved scope")
            .is_none()
    );

    fs::remove_file(&db_path).expect("remove unavailable database");
    ingest_all(&paths, &index, &options, &ingest_lease(&paths))
        .expect("confirmed-absent database cleanup");
    assert_eq!(index.doc_count().expect("remaining document count"), 1);
    let state = IngestState::load(&paths.state.join("ingest.json")).expect("load ingest state");
    assert!(!state.opencode_databases.contains_key(&source_path));
}

#[test]
fn cursor_session_id_uses_agent_transcripts_session_directory() {
    let path = Path::new(
        "/Users/nico/.cursor/projects/-Users-nico-Code-memex/agent-transcripts/\
             11111111-1111-1111-1111-111111111111/\
             11111111-1111-1111-1111-111111111111.jsonl",
    );

    assert_eq!(
        crate::sources::cursor::session_id_from_path(path),
        "11111111-1111-1111-1111-111111111111"
    );
}

#[test]
fn cursor_session_id_strips_direct_transcript_extension() {
    let path = Path::new(
        "/Users/nico/.cursor/projects/-Users-nico-Code-memex/agent-transcripts/\
             11111111-1111-1111-1111-111111111111.jsonl",
    );

    assert_eq!(
        crate::sources::cursor::session_id_from_path(path),
        "11111111-1111-1111-1111-111111111111"
    );
}

#[test]
fn cursor_session_id_uses_parent_session_for_subagent_transcripts() {
    let path = Path::new(
        "/Users/nico/.cursor/projects/-Users-nico-Code-memex/agent-transcripts/\
             11111111-1111-1111-1111-111111111111/subagents/\
             22222222-2222-2222-2222-222222222222.jsonl",
    );

    assert_eq!(
        crate::sources::cursor::session_id_from_path(path),
        "11111111-1111-1111-1111-111111111111"
    );
}

#[test]
fn cursor_parent_transcripts_start_at_cached_turn_id() {
    let path = Path::new(
        "/Users/nico/.cursor/projects/-Users-nico-Code-memex/agent-transcripts/\
             11111111-1111-1111-1111-111111111111/\
             11111111-1111-1111-1111-111111111111.jsonl",
    );

    assert_eq!(crate::sources::cursor::initial_turn_id(path, 0), 0);
    assert_eq!(crate::sources::cursor::initial_turn_id(path, 42), 42);
}

#[test]
fn cursor_subagent_transcripts_use_reserved_turn_range() {
    let path = Path::new(
        "/Users/nico/.cursor/projects/-Users-nico-Code-memex/agent-transcripts/\
             11111111-1111-1111-1111-111111111111/subagents/\
             22222222-2222-2222-2222-222222222222.jsonl",
    );

    let initial = crate::sources::cursor::initial_turn_id(path, 0);
    assert!(initial >= 1_000_000_000);
    assert_eq!(
        crate::sources::cursor::initial_turn_id(path, initial + 3),
        initial + 3
    );
}

#[test]
fn cursor_record_links_mark_subagent_parent_session() {
    let path = Path::new(
        "/Users/nico/.cursor/projects/-Users-nico-Code-memex/agent-transcripts/\
             11111111-1111-1111-1111-111111111111/subagents/\
             22222222-2222-2222-2222-222222222222.jsonl",
    );

    let links =
        crate::sources::cursor::record_links(path, "11111111-1111-1111-1111-111111111111", 42);

    assert_eq!(
        links.event_id.as_deref(),
        Some("22222222-2222-2222-2222-222222222222:42")
    );
    assert_eq!(
        links.parent_session_id.as_deref(),
        Some("11111111-1111-1111-1111-111111111111")
    );
    assert_eq!(links.thread_source.as_deref(), Some("subagent"));
    assert_eq!(links.conversation_kind.as_deref(), Some("subagent"));
}

#[test]
fn opencode_session_links_preserve_parent_id() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let project = tmp.path().join("project");
    fs::create_dir_all(&project).expect("create opencode project");
    fs::write(
        project.join("ses_child.json"),
        r#"{"id":"ses_child","parentID":"ses_parent","projectID":"global"}"#,
    )
    .expect("write opencode session");

    let links = crate::sources::opencode::session_links_by_id_from_root(tmp.path())
        .remove("ses_child")
        .expect("child links");

    assert_eq!(links.parent_session_id.as_deref(), Some("ses_parent"));
    assert_eq!(links.thread_source.as_deref(), Some("fork"));
    assert_eq!(links.conversation_kind.as_deref(), Some("fork"));
}

#[test]
fn opencode_session_links_by_id_caches_metadata_tree() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let project = tmp.path().join("project");
    fs::create_dir_all(&project).expect("create opencode project");
    fs::write(
        project.join("ses_child.json"),
        r#"{"id":"ses_child","parentID":"ses_parent","projectID":"global"}"#,
    )
    .expect("write child session");
    fs::write(
        project.join("ses_main.json"),
        r#"{"id":"ses_main","projectID":"global"}"#,
    )
    .expect("write main session");

    let links_by_id = crate::sources::opencode::session_links_by_id_from_root(tmp.path());
    let child_links = links_by_id.get("ses_child").expect("child links");
    let main_links = links_by_id.get("ses_main").expect("main links");

    assert_eq!(links_by_id.len(), 2);
    assert_eq!(child_links.parent_session_id.as_deref(), Some("ses_parent"));
    assert_eq!(child_links.thread_source.as_deref(), Some("fork"));
    assert_eq!(child_links.conversation_kind.as_deref(), Some("fork"));
    assert_eq!(main_links.parent_session_id, None);
    assert_eq!(main_links.thread_source, None);
    assert_eq!(main_links.conversation_kind.as_deref(), Some("main"));
}

#[test]
fn codex_session_meta_preserves_fork_and_subagent_links() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp
        .path()
        .join("rollout-2026-05-22T13-17-11-019e5155-b507-7d83-8c3d-9ecee5f93f12.jsonl");
    fs::write(
            &path,
            r#"{"timestamp":"2026-05-22T20:17:12.595Z","type":"session_meta","payload":{"id":"019e5155-b507-7d83-8c3d-9ecee5f93f12","forked_from_id":"019e5117-c673-7660-b218-af0489416e0f","cwd":"/tmp/project","source":{"subagent":{"thread_spawn":{"parent_thread_id":"019e5117-c673-7660-b218-af0489416e0f","depth":1}}},"thread_source":"subagent"}}"#
                .to_string()
                + "\n",
        )
        .expect("write codex session");

    let meta = crate::sources::codex::probe(&path).expect("read codex meta");

    assert_eq!(
        meta.session.session_id,
        "019e5155-b507-7d83-8c3d-9ecee5f93f12"
    );
    assert_eq!(meta.project.as_deref(), Some("project"));
    assert_eq!(
        meta.session.parent_session_id.as_deref(),
        Some("019e5117-c673-7660-b218-af0489416e0f")
    );
    assert_eq!(
        meta.session.conversation_kind,
        crate::sources::ConversationKind::Subagent
    );
}

#[test]
fn claude_incremental_results_use_persisted_calls_out_of_order() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let project = tmp.path().join("-Users-nico-Code-memex");
    fs::create_dir_all(&project).expect("project dir");
    let path = project.join("claude-incremental.jsonl");
    let calls = concat!(
        "{\"type\":\"assistant\",\"uuid\":\"assistant-1\",\"sessionId\":\"claude-incremental\",\"timestamp\":\"2026-07-20T10:00:00Z\",\"message\":{\"content\":[",
        "{\"type\":\"tool_use\",\"id\":\"call-a\",\"name\":\"Read\",\"input\":{\"path\":\"a\"}},",
        "{\"type\":\"tool_use\",\"id\":\"call-b\",\"name\":\"Grep\",\"input\":{\"pattern\":\"b\"}}]}}\n"
    );
    let results = concat!(
        "{\"type\":\"user\",\"uuid\":\"result-b\",\"parentUuid\":\"assistant-1\",\"sessionId\":\"claude-incremental\",\"timestamp\":\"2026-07-20T10:00:01Z\",\"message\":{\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"call-b\",\"content\":\"B\"}]}}\n",
        "{\"type\":\"user\",\"uuid\":\"result-a\",\"parentUuid\":\"result-b\",\"sessionId\":\"claude-incremental\",\"timestamp\":\"2026-07-20T10:00:02Z\",\"message\":{\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"call-a\",\"content\":\"A\"}]}}\n"
    );
    fs::write(&path, calls).expect("write calls");

    let progress = Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], false));
    let next_doc_id = AtomicU64::new(1);
    let (tx_record, rx_record, tx_update, rx_update) = parser_channels();
    let first = incremental_task(&path, SourceKind::Claude, 0, 0, HashMap::new());
    parse_claude_file(
        &first,
        false,
        &tx_record,
        &tx_update,
        &next_doc_id,
        &progress,
    )
    .expect("parse calls");
    let first_records: Vec<_> = rx_record.try_iter().collect();
    let first_state = rx_update.try_recv().expect("first state").state;
    assert_eq!(first_records.len(), 2);
    assert_eq!(first_state.pending_tool_calls.len(), 2);
    let pending_a = first_state
        .pending_tool_calls
        .get("call-a")
        .expect("pending call a");
    assert_eq!(pending_a.tool_name.as_deref(), Some("Read"));
    assert_eq!(pending_a.tool_use_event_id.as_deref(), Some("call-a"));
    assert_eq!(pending_a.tool_use_doc_id, Some(first_records[0].doc_id));
    assert!(pending_a.argument_sha256.is_some());
    assert!(pending_a.argument_bytes.is_some_and(|bytes| bytes > 0));

    use std::io::Write;
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open append")
        .write_all(results.as_bytes())
        .expect("append results");
    let second = incremental_task(
        &path,
        SourceKind::Claude,
        first_state.offset,
        first_state.turn_id,
        first_state.pending_tool_calls,
    );
    parse_claude_file(
        &second,
        false,
        &tx_record,
        &tx_update,
        &next_doc_id,
        &progress,
    )
    .expect("parse results");
    let second_records: Vec<_> = rx_record.try_iter().collect();
    let second_state = rx_update.try_recv().expect("second state").state;

    assert_eq!(second_records.len(), 2);
    assert_eq!(second_records[0].tool_name.as_deref(), Some("Grep"));
    assert_eq!(
        second_records[0].links.parent_tool_use_id.as_deref(),
        Some("call-b")
    );
    assert_eq!(second_records[1].tool_name.as_deref(), Some("Read"));
    assert_eq!(
        second_records[1].links.parent_event_id.as_deref(),
        Some("call-a")
    );
    assert!(
        second_records
            .iter()
            .all(|record| record.session_id == "claude-incremental"
                && record.source == SourceKind::Claude
                && record.source_path == path.to_string_lossy())
    );
    assert!(second_state.pending_tool_calls.is_empty());
}

#[test]
fn codex_incremental_result_uses_persisted_call_metadata() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp
        .path()
        .join("rollout-2026-07-20T10-00-00-11111111-1111-4111-8111-111111111111.jsonl");
    let call = concat!(
        "{\"timestamp\":\"2026-07-20T10:00:00Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"11111111-1111-4111-8111-111111111111\",\"cwd\":\"/Users/nico/Code/memex\"}}\n",
        "{\"timestamp\":\"2026-07-20T10:00:01Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"id\":\"fc-item\",\"call_id\":\"call-1\",\"name\":\"shell\",\"arguments\":\"{\\\"cmd\\\":\\\"pwd\\\"}\"}}\n"
    );
    let result = "{\"timestamp\":\"2026-07-20T10:00:02Z\",\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"call-1\",\"output\":\"/Users/nico/Code/memex\"}}\n";
    fs::write(&path, call).expect("write call");

    let progress = Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], false));
    let next_doc_id = AtomicU64::new(10);
    let (tx_record, rx_record, tx_update, rx_update) = parser_channels();
    let first = incremental_task(&path, SourceKind::Codex, 0, 0, HashMap::new());
    parse_codex_session(
        &first,
        false,
        &tx_record,
        &tx_update,
        &next_doc_id,
        &progress,
    )
    .expect("parse call");
    let call_record = rx_record.try_recv().expect("call record");
    let first_state = rx_update.try_recv().expect("first state").state;
    assert_eq!(call_record.tool_name.as_deref(), Some("shell"));
    assert_eq!(
        first_state
            .pending_tool_calls
            .get("call-1")
            .and_then(|call| call.tool_use_doc_id),
        Some(call_record.doc_id)
    );

    use std::io::Write;
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open append")
        .write_all(result.as_bytes())
        .expect("append result");
    let second = incremental_task(
        &path,
        SourceKind::Codex,
        first_state.offset,
        first_state.turn_id,
        first_state.pending_tool_calls,
    );
    parse_codex_session(
        &second,
        false,
        &tx_record,
        &tx_update,
        &next_doc_id,
        &progress,
    )
    .expect("parse result");
    let result_record = rx_record.try_recv().expect("result record");
    let second_state = rx_update.try_recv().expect("second state").state;

    assert_eq!(result_record.tool_name.as_deref(), Some("shell"));
    assert_eq!(
        result_record.links.parent_tool_use_id.as_deref(),
        Some("call-1")
    );
    assert_eq!(
        result_record.links.parent_event_id.as_deref(),
        Some("call-1")
    );
    assert_eq!(
        result_record.session_id,
        "11111111-1111-4111-8111-111111111111"
    );
    assert_eq!(result_record.project, "memex");
    assert_eq!(result_record.source, SourceKind::Codex);
    assert_eq!(result_record.source_path, path.to_string_lossy());
    assert!(second_state.pending_tool_calls.is_empty());
}

#[test]
fn truncation_and_replacement_clear_stale_pending_calls() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("session.jsonl");
    fs::write(&path, "original transcript with a pending call\n").expect("write original");
    let metadata = path.metadata().expect("original metadata");
    let (mut original, _) =
        discovery::prepare_file_task(path.clone(), SourceKind::Claude, false, &metadata, None);
    original.pending_tool_calls.insert(
        "stale".to_string(),
        PendingToolCall {
            tool_name: Some("StaleTool".to_string()),
            ..PendingToolCall::default()
        },
    );
    let prior = execution::completed_file_state(
        &original,
        metadata.len(),
        1,
        Some(1),
        original.pending_tool_calls.clone(),
    );

    fs::write(&path, "short\n").expect("truncate");
    let truncated_meta = path.metadata().expect("truncated metadata");
    let (truncated, skip) = discovery::prepare_file_task(
        path.clone(),
        SourceKind::Claude,
        false,
        &truncated_meta,
        Some(&prior),
    );
    assert!(!skip);
    assert!(truncated.delete_first());
    assert_eq!(truncated.offset, 0);
    assert!(truncated.pending_tool_calls.is_empty());

    let replacement = tmp.path().join("replacement.jsonl");
    fs::write(
        &replacement,
        "replacement transcript that is longer than the original\n",
    )
    .expect("write replacement");
    fs::rename(&replacement, &path).expect("replace path");
    let replacement_meta = path.metadata().expect("replacement metadata");
    let (replaced, skip) = discovery::prepare_file_task(
        path,
        SourceKind::Claude,
        false,
        &replacement_meta,
        Some(&prior),
    );
    assert!(!skip);
    assert!(replaced.delete_first());
    assert_eq!(replaced.offset, 0);
    assert!(replaced.pending_tool_calls.is_empty());
}

#[test]
fn device_renumbering_preserves_append_continuity() {
    let previous = FileIdentity {
        sqlite_wal: None,
        device: Some(1),
        inode: Some(2),
        prefix_sha256: Some("same".to_string()),
        prefix_bytes: 4,
        modified_ns: Some(3),
        changed_ns: None,
    };
    let current = FileIdentity {
        sqlite_wal: None,
        device: Some(4),
        ..previous.clone()
    };

    assert!(!file_was_replaced(&previous, &current));
}

#[test]
fn device_renumbering_does_not_hide_file_replacement() {
    let previous = FileIdentity {
        sqlite_wal: None,
        device: Some(1),
        inode: Some(2),
        prefix_sha256: Some("original".to_string()),
        prefix_bytes: 8,
        modified_ns: Some(3),
        changed_ns: None,
    };
    let different_inode = FileIdentity {
        sqlite_wal: None,
        device: Some(4),
        inode: Some(5),
        ..previous.clone()
    };
    let different_prefix = FileIdentity {
        sqlite_wal: None,
        device: Some(4),
        prefix_sha256: Some("replaced".to_string()),
        ..previous.clone()
    };

    assert!(file_was_replaced(&previous, &different_inode));
    assert!(file_was_replaced(&previous, &different_prefix));
}

#[test]
fn short_file_append_preserves_pending_calls_and_offset() {
    use std::io::Write;

    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("session.jsonl");
    fs::write(&path, "tool call\n").expect("write call");
    let metadata = path.metadata().expect("call metadata");
    let (mut first, _) =
        discovery::prepare_file_task(path.clone(), SourceKind::Claude, false, &metadata, None);
    first.pending_tool_calls.insert(
        "call-1".to_string(),
        PendingToolCall {
            tool_name: Some("Read".to_string()),
            ..PendingToolCall::default()
        },
    );
    let previous = execution::completed_file_state(
        &first,
        metadata.len(),
        1,
        Some(1),
        first.pending_tool_calls.clone(),
    );

    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open append")
        .write_all(b"tool result\n")
        .expect("append result");
    let appended_metadata = path.metadata().expect("appended metadata");
    let (appended, skip) = discovery::prepare_file_task(
        path,
        SourceKind::Claude,
        false,
        &appended_metadata,
        Some(&previous),
    );

    assert!(!skip);
    assert!(!appended.delete_first());
    assert_eq!(appended.offset, metadata.len());
    assert_eq!(
        appended
            .pending_tool_calls
            .get("call-1")
            .and_then(|call| call.tool_name.as_deref()),
        Some("Read")
    );
}

#[test]
fn jcode_append_forces_whole_file_replacement() {
    use std::io::Write;

    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("session_test.json");
    fs::write(&path, r#"{"id":"s1","messages":[]}"#).expect("write session");
    let metadata = path.metadata().expect("session metadata");
    let (first, _) =
        discovery::prepare_file_task(path.clone(), SourceKind::Jcode, false, &metadata, None);
    let previous = execution::completed_file_state(
        &first,
        metadata.len(),
        1,
        None,
        first.pending_tool_calls.clone(),
    );

    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open append")
        .write_all(br#", "appended": true}"#)
        .expect("append");
    let appended_metadata = path.metadata().expect("appended metadata");
    let (appended, skip) = discovery::prepare_file_task(
        path,
        SourceKind::Jcode,
        false,
        &appended_metadata,
        Some(&previous),
    );

    assert!(!skip);
    assert!(appended.delete_first());
    assert_eq!(appended.offset, 0);
    assert!(appended.pending_tool_calls.is_empty());
}

#[test]
fn index_parser_version_change_rebuilds_and_clears_pending_state() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("session.jsonl");
    fs::write(&path, "{}\n").expect("transcript");
    let metadata = path.metadata().expect("metadata");
    let identity = file_identity(
        &path,
        &metadata,
        metadata.len().min(FILE_IDENTITY_PREFIX_BYTES as u64) as usize,
    );
    let previous = FileState {
        codex_metadata_offsets: None,
        size: metadata.len(),
        mtime: metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or(0),
        offset: metadata.len(),
        turn_id: 1,
        legacy_turn_id: None,
        claude_background: None,
        parser_version: crate::sources::index_state_version(SourceKind::Claude).saturating_sub(1),
        pending_tool_calls: HashMap::from([(
            "stale".to_string(),
            PendingToolCall {
                tool_name: Some("Old".to_string()),
                ..PendingToolCall::default()
            },
        )]),
        identity,
    };
    let (task, skip) =
        discovery::prepare_file_task(path, SourceKind::Claude, false, &metadata, Some(&previous));
    assert!(!skip);
    assert!(task.delete_first());
    assert!(task.parser_version_invalidated());
    assert_eq!(task.offset, 0);
    assert!(task.pending_tool_calls.is_empty());
}

#[test]
fn grown_jcode_file_reparses_atomically_instead_of_resuming() {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("session_jcode.json");
    // Fabricate prior state for a smaller file with identical identity
    // so only the jcode whole-object arm (not replacement detection)
    // can explain atomic reparse.
    fs::write(&path, "A".repeat(5100)).expect("transcript");
    let metadata = path.metadata().expect("metadata");
    let identity = file_identity(
        &path,
        &metadata,
        metadata.len().min(FILE_IDENTITY_PREFIX_BYTES as u64) as usize,
    );
    let version = crate::sources::index_state_version(SourceKind::Jcode);
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0);
    let previous = FileState {
        codex_metadata_offsets: None,
        size: metadata.len() - 100,
        mtime,
        offset: metadata.len() - 100,
        turn_id: 3,
        legacy_turn_id: None,
        claude_background: None,
        parser_version: version,
        pending_tool_calls: HashMap::new(),
        identity,
    };
    let (task, skip) =
        discovery::prepare_file_task(path, SourceKind::Jcode, false, &metadata, Some(&previous));
    // Byte offsets cannot resume a single-JSON-object file: the parser
    // re-emits from message zero, so the stale rows must go first.
    assert!(!skip);
    assert!(task.delete_first());
    assert_eq!(task.offset, 0);
    assert_eq!(task.turn_id, 0);
}

#[test]
fn parser_version_migration_rebuilds_vectors_with_the_existing_model() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    save_vector_store(&paths, "bge", 384);
    let transcript = tmp.path().join("session.jsonl");
    fs::write(&transcript, "{}\n").expect("transcript");
    let mut task = incremental_task(&transcript, SourceKind::Claude, 0, 0, HashMap::new());
    task.change = FileChange::ParserChanged;

    let migration = vector_migration(&paths.vectors, &[task], ModelChoice::Gemma);

    assert!(migration.rebuild);
    assert_eq!(migration.model, ModelChoice::BGESmall);
}

#[test]
fn ordinary_file_replacement_does_not_rebuild_the_vector_store() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    save_vector_store(&paths, "bge", 384);
    let transcript = tmp.path().join("session.jsonl");
    fs::write(&transcript, "{}\n").expect("transcript");
    let task = incremental_task(&transcript, SourceKind::Claude, 0, 0, HashMap::new());

    let migration = vector_migration(&paths.vectors, &[task], ModelChoice::Gemma);

    assert!(!migration.rebuild);
    assert_eq!(migration.model, ModelChoice::Gemma);
}

#[test]
fn parser_rebuild_keeps_published_vectors_until_replacement_is_saved() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    save_vector_store(&paths, "bge", 384);

    let replacement =
        open_vector_index_for_ingest(&paths.vectors, 384, ModelChoice::BGESmall, true)
            .expect("start replacement");

    assert!(replacement.is_empty());
    assert_eq!(
        crate::vector::VectorIndex::open(&paths.vectors)
            .expect("published vectors")
            .len(),
        1
    );
}

#[test]
fn ingest_claude_records_preserve_sidechain_and_tool_links() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let claude_root = tmp.path().join("claude-projects");
    let project_root = claude_root.join("-Users-nico-Code-memex");
    fs::create_dir_all(&project_root).expect("create claude project");
    let session_file = project_root.join("sess-claude.jsonl");
    fs::write(
            &session_file,
            r#"{"type":"user","uuid":"u1","parentUuid":null,"sessionId":"sess-claude","isSidechain":false,"timestamp":"2026-03-11T01:23:43.844Z","message":{"content":"question"}}
{"type":"assistant","uuid":"a1","parentUuid":"u1","logicalParentUuid":"u0","sessionId":"sess-claude","isSidechain":true,"sourceToolUseID":"source-tool","sourceToolAssistantUUID":"source-assistant","timestamp":"2026-03-11T01:23:44.844Z","message":{"content":[{"type":"text","text":"answer"},{"type":"tool_use","id":"tool-claude","name":"Read","input":{"file_path":"Cargo.toml"}}]}}
{"type":"user","uuid":"r1","parentUuid":"a1","sessionId":"sess-claude","isSidechain":true,"timestamp":"2026-03-11T01:23:45.844Z","message":{"content":[{"type":"tool_result","tool_use_id":"tool-claude","content":"ok"}]}}
"#,
        )
        .expect("write claude fixture");

    let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    let index = SearchIndex::open_or_create(&paths.index).expect("index");
    let options = IngestOptions {
        claude_sources: vec![claude_root],
        exclude_patterns: Vec::new(),
        include_agents: false,
        include_reasoning: false,
        include_codex: false,
        include_opencode: false,
        include_cursor: false,
        include_pi: false,
        include_omp: false,
        include_openclaw: false,
        include_copilot: false,
        include_grok: false,
        include_jcode: false,
        include_muse: false,
        include_antigravity: false,
        embeddings: false,
        backfill_embeddings: false,
        model: ModelChoice::default(),
        embed_runtime: EmbedRuntimeConfig::default(),
        tool_content_limits: IndexedToolContentLimits::default(),
        defer_merges: false,
    };

    let lease = ingest_lease(&paths);
    let report = ingest_all(&paths, &index, &options, &lease).expect("ingest");
    assert_eq!(report.records_added, 4);

    let mut records = index
        .records_by_session_id("sess-claude")
        .expect("records by session");
    records.sort_by_key(|record| record.turn_id);

    assert_eq!(records.len(), 4);
    assert_eq!(records[0].role, "user");
    assert_eq!(records[0].links.event_id.as_deref(), Some("u1"));
    assert_eq!(records[0].links.conversation_kind.as_deref(), Some("main"));
    assert_eq!(records[1].role, "tool_use");
    assert_eq!(records[1].links.event_id.as_deref(), Some("tool-claude"));
    assert_eq!(records[1].links.parent_event_id.as_deref(), Some("a1"));
    assert_eq!(
        records[1].links.logical_parent_event_id.as_deref(),
        Some("u0")
    );
    assert_eq!(
        records[1].links.source_tool_use_id.as_deref(),
        Some("source-tool")
    );
    assert_eq!(
        records[1].links.source_tool_assistant_uuid.as_deref(),
        Some("source-assistant")
    );
    assert_eq!(records[1].links.thread_source.as_deref(), Some("sidechain"));
    assert_eq!(
        records[1].links.conversation_kind.as_deref(),
        Some("sidechain")
    );
    assert_eq!(records[2].role, "assistant");
    assert_eq!(records[2].links.event_id.as_deref(), Some("a1"));
    assert_eq!(records[2].links.parent_event_id.as_deref(), Some("u1"));
    assert_eq!(records[2].links.thread_source.as_deref(), Some("sidechain"));
    assert_eq!(
        records[2].links.conversation_kind.as_deref(),
        Some("sidechain")
    );
    assert_eq!(records[3].role, "tool_result");
    assert_eq!(
        records[3].links.event_id.as_deref(),
        Some("r1:tool_result:tool-claude")
    );
    assert_eq!(
        records[3].links.parent_event_id.as_deref(),
        Some("tool-claude")
    );
    assert_eq!(
        records[3].links.parent_tool_use_id.as_deref(),
        Some("tool-claude")
    );
    assert_eq!(records[3].tool_name.as_deref(), Some("Read"));
}

#[test]
fn collect_codex_session_files_includes_archived_sessions() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let sessions_root = tmp.path().join("sessions");
    let archived_root = tmp.path().join("archived_sessions");

    let sessions_day = sessions_root.join("2026/02/11");
    fs::create_dir_all(&sessions_day).expect("create sessions day");
    fs::create_dir_all(archived_root.join("state")).expect("create archived state");

    let live = sessions_day.join("session-live.jsonl");
    let archived = archived_root.join("rollout-archive.jsonl");
    let ignored = archived_root.join("state/ingest.json");

    fs::write(&live, "{}\n").expect("write live");
    fs::write(&archived, "{}\n").expect("write archived");
    fs::write(&ignored, "{}\n").expect("write ignored");

    let files = crate::sources::common::jsonl_files([sessions_root, archived_root]);

    assert_eq!(files, vec![archived, live]);
}

#[cfg(unix)]
#[test]
fn metadata_fast_path_detects_rewrites_with_restored_mtime() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("session.jsonl");
    let mut content = vec![b'a'; 8192];
    fs::write(&path, &content).unwrap();
    let metadata = path.metadata().unwrap();
    let (task, _) =
        discovery::prepare_file_task(path.clone(), SourceKind::Claude, false, &metadata, None);
    let state = execution::completed_file_state(&task, metadata.len(), 1, Some(1), HashMap::new());
    let version = crate::sources::index_state_version_for(SourceKind::Claude, false);
    if changed_ns(&metadata).is_none() {
        assert!(!unchanged_file_metadata(&state, &metadata, version));
        return;
    }
    assert!(unchanged_file_metadata(&state, &metadata, version));
    let mut legacy = state.clone();
    legacy.identity.changed_ns = None;
    assert!(!unchanged_file_metadata(&legacy, &metadata, version));
    let (upgraded, skip) = discovery::prepare_file_task(
        path.clone(),
        SourceKind::Claude,
        false,
        &metadata,
        Some(&legacy),
    );
    assert!(skip);
    assert_eq!(upgraded.identity.changed_ns, state.identity.changed_ns);
    assert!(!unchanged_file_metadata(&state, &metadata, version + 1));

    std::thread::sleep(std::time::Duration::from_millis(2));
    content[6000] = b'b';
    fs::write(&path, content).unwrap();
    File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(metadata.modified().unwrap()))
        .unwrap();
    let changed = path.metadata().unwrap();
    assert_eq!(modified_ns(&changed), state.identity.modified_ns);
    assert!(!unchanged_file_metadata(&state, &changed, version));
    let (replacement, skip) =
        discovery::prepare_file_task(path, SourceKind::Claude, false, &changed, Some(&state));
    assert!(!skip);
    assert!(replacement.delete_first());
    assert_eq!(replacement.offset, 0);
}

#[test]
fn can_skip_noop_index_when_embeddings_are_disabled() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    let index = open_search_index(&paths);
    let options = ingest_options(false, ModelChoice::BGESmall);

    assert!(can_skip_noop_index(&paths, &index, &options).unwrap());
}

#[test]
fn can_skip_fresh_scan_when_embeddings_are_disabled() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    let index = save_search_records(&paths, &[record(1, "user", "hello")]);
    let options = ingest_options(false, ModelChoice::BGESmall);
    let cache = fresh_scan_cache();
    mark_analytics_complete(&paths);

    cache.save(&paths.state.join("scan_cache.json")).unwrap();
    assert!(can_skip_fresh_scan(&paths, &index, &options, 60).unwrap());
}

#[test]
fn can_skip_fresh_scan_with_compatible_vectors() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    save_vector_store(&paths, "bge", 384);
    let index = save_search_records(&paths, &[record(1, "user", "hello")]);
    let options = ingest_options(true, ModelChoice::BGESmall);
    let cache = fresh_scan_cache();
    mark_analytics_complete(&paths);

    cache.save(&paths.state.join("scan_cache.json")).unwrap();
    assert!(can_skip_fresh_scan(&paths, &index, &options, 60).unwrap());
}

#[test]
fn cannot_skip_fresh_scan_when_vectors_are_missing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    let index = open_search_index(&paths);
    let options = ingest_options(true, ModelChoice::BGESmall);
    let cache = fresh_scan_cache();

    cache.save(&paths.state.join("scan_cache.json")).unwrap();
    assert!(!can_skip_fresh_scan(&paths, &index, &options, 60).unwrap());
}

#[test]
fn cannot_skip_fresh_scan_with_pending_ingest() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    let index = save_search_records(&paths, &[record(1, "user", "hello")]);
    let options = ingest_options(false, ModelChoice::BGESmall);
    let cache = fresh_scan_cache();
    mark_analytics_complete(&paths);
    PendingIngest {
        next_doc_id: 2,
        source_paths: vec!["source-1.jsonl".to_string()],
        session_scopes: Vec::new(),
        vector_delete_paths: Vec::new(),
        vector_publication: false,
        embedding_publication: Some(false),
    }
    .save(&pending_ingest_path(&paths))
    .expect("save pending ingest");

    cache.save(&paths.state.join("scan_cache.json")).unwrap();
    assert!(!can_skip_fresh_scan(&paths, &index, &options, 60).unwrap());
}

#[test]
fn freshness_uses_sqlite_pending_and_cache_without_sidecars() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("claude");
    fs::create_dir_all(&source).unwrap();
    let transcript = source.join("session.jsonl");
    append_claude_message(&transcript, "original");
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let lease = ingest_lease(&paths);
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![source];
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    ingest_all(&paths, &index, &options, &lease).unwrap();
    let index = open_search_index(&paths);
    let cache_path = paths.state.join("scan_cache.json");
    assert!(!cache_path.exists());
    assert!(can_skip_fresh_scan(&paths, &index, &options, 3600).unwrap());
    let pending_path = pending_ingest_path(&paths);
    PendingIngest {
        next_doc_id: 2,
        source_paths: vec![transcript.to_string_lossy().into_owned()],
        session_scopes: Vec::new(),
        vector_delete_paths: Vec::new(),
        vector_publication: false,
        embedding_publication: Some(false),
    }
    .save_with_lease(&pending_path, &lease)
    .unwrap();
    assert!(!pending_path.exists());
    assert!(!can_skip_fresh_scan(&paths, &index, &options, 3600).unwrap());
    let header = CheckpointReader::open(&paths.state.join("ingest.json"))
        .unwrap()
        .header()
        .unwrap();
    assert!(header.pending.is_some());
    assert!(header.scan_cache.is_fresh(3600));
    PendingIngest::clear_with_lease(&pending_path, &lease).unwrap();
    assert!(can_skip_fresh_scan(&paths, &index, &options, 3600).unwrap());
    let database = rusqlite::Connection::open(paths.state.join("checkpoints.sqlite")).unwrap();
    database
        .execute("UPDATE metadata SET scancache_json='{}'", [])
        .unwrap();
    assert!(!can_skip_fresh_scan(&paths, &index, &options, 3600).unwrap());
    fresh_scan_cache()
        .save_with_lease(&cache_path, &lease)
        .unwrap();
    database
        .execute("UPDATE metadata SET pending_json='{}'", [])
        .unwrap();
    assert!(can_skip_fresh_scan(&paths, &index, &options, 3600).is_err());
}

#[test]
fn freshness_reads_legacy_and_sql_v1_sidecars_without_upgrade() {
    for sqlite_v1 in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let state_path = paths.state.join("ingest.json");
        let initial = IngestState {
            next_doc_id: 2,
            ..Default::default()
        };
        if sqlite_v1 {
            initial
                .save_with_lease(&state_path, &ingest_lease(&paths))
                .unwrap();
            let database =
                rusqlite::Connection::open(paths.state.join("checkpoints.sqlite")).unwrap();
            database
                .execute_batch(
                    "BEGIN;
                ALTER TABLE metadata DROP COLUMN pending_json;
                ALTER TABLE metadata DROP COLUMN scancache_json;
                UPDATE metadata SET format_version=1;
                COMMIT;",
                )
                .unwrap();
            let marker: String = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
            assert!(marker.starts_with("memex-checkpoints:2:"));
            fs::write(
                &state_path,
                serde_json::to_vec(&marker.replacen(
                    "memex-checkpoints:2:",
                    "memex-checkpoints:1:",
                    1,
                ))
                .unwrap(),
            )
            .unwrap();
        } else {
            initial.save(&state_path).unwrap();
        }
        let marker = fs::read(&state_path).unwrap();
        let index = save_search_records(&paths, &[record(1, "user", "original")]);
        mark_analytics_complete(&paths);
        let options = ingest_options(false, ModelChoice::Gemma);
        fresh_scan_cache()
            .save(&paths.state.join("scan_cache.json"))
            .unwrap();
        assert!(can_skip_fresh_scan(&paths, &index, &options, 3600).unwrap());
        let pending_path = pending_ingest_path(&paths);
        PendingIngest {
            next_doc_id: 2,
            source_paths: vec!["source-1.jsonl".to_string()],
            session_scopes: Vec::new(),
            vector_delete_paths: Vec::new(),
            vector_publication: false,
            embedding_publication: Some(false),
        }
        .save(&pending_path)
        .unwrap();
        assert!(!can_skip_fresh_scan(&paths, &index, &options, 3600).unwrap());
        assert_eq!(fs::read(&state_path).unwrap(), marker);
        fs::write(&pending_path, b"{").unwrap();
        assert!(can_skip_fresh_scan(&paths, &index, &options, 3600).is_err());
        assert_eq!(fs::read(&state_path).unwrap(), marker);
    }
}

#[test]
fn database_discovery_error_never_allows_fresh_scan_skip() {
    let _guard = env_lock();
    let tmp = tempfile::tempdir().expect("tempdir");
    let bad_root = tmp.path().join("not-a-directory");
    fs::write(&bad_root, "fixture").expect("write invalid data root");
    let _env = EnvVarGuard::set_os(&[("OPENCODE_DATA_DIR", Some(bad_root.as_os_str()))]);
    let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
    let index = save_search_records(&paths, &[record(1, "user", "hello")]);
    let options = ingest_options(false, ModelChoice::BGESmall);
    let mut options = options;
    options.include_opencode = true;
    mark_analytics_complete(&paths);

    fresh_scan_cache()
        .save(&paths.state.join("scan_cache.json"))
        .unwrap();
    assert!(!can_skip_fresh_scan(&paths, &index, &options, 60).unwrap());
    assert!(ingest_all(&paths, &index, &options, &ingest_lease(&paths)).is_err());
    assert_eq!(index.doc_count().expect("preserved documents"), 1);
}

#[test]
fn cannot_skip_fresh_scan_with_incompatible_vectors() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    save_vector_store(&paths, "minilm", 384);
    let index = open_search_index(&paths);
    let options = ingest_options(true, ModelChoice::BGESmall);
    let cache = fresh_scan_cache();

    cache.save(&paths.state.join("scan_cache.json")).unwrap();
    assert!(!can_skip_fresh_scan(&paths, &index, &options, 60).unwrap());
}

#[test]
fn cannot_skip_fresh_scan_when_cache_is_stale() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    let index = open_search_index(&paths);
    let options = ingest_options(false, ModelChoice::BGESmall);
    let cache = ScanCache {
        last_scan_ts: 0,
        file_count: 0,
        total_bytes: 0,
    };

    cache.save(&paths.state.join("scan_cache.json")).unwrap();
    assert!(!can_skip_fresh_scan(&paths, &index, &options, 60).unwrap());
}

#[test]
fn updating_scan_cache_replaces_malformed_cache() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    paths.ensure_dirs().expect("dirs");
    let cache_path = paths.state.join("scan_cache.json");
    fs::write(&cache_path, "{\"last_scan_ts\":").expect("seed malformed cache");

    let lease = ingest_lease(&paths);
    let mut state =
        CheckpointSession::open(&paths.state.join("ingest.json"), &lease, true, None).unwrap();
    let cache = updated_scan_cache(Some(std::mem::take(&mut state.scan_cache)), 7, 42, true);
    state
        .commit_final(cache, PendingChange::Keep)
        .expect("update scan cache");

    let cache = ScanCache::load(&cache_path).expect("load replaced cache");
    assert_eq!(cache.file_count, 7);
    assert_eq!(cache.total_bytes, 42);
}

#[test]
fn can_skip_noop_index_with_compatible_vectors() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    save_vector_store(&paths, "bge", 384);
    let index = save_search_records(&paths, &[record(1, "user", "hello")]);
    let options = ingest_options(true, ModelChoice::BGESmall);

    assert!(can_skip_noop_index(&paths, &index, &options).unwrap());
}

#[test]
fn cannot_skip_noop_index_with_partial_compatible_vectors() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    save_vector_store(&paths, "bge", 384);
    let index = save_search_records(
        &paths,
        &[
            record(1, "user", "embedded"),
            record(2, "assistant", "missing vector"),
        ],
    );
    let options = ingest_options(true, ModelChoice::BGESmall);

    assert!(!can_skip_noop_index(&paths, &index, &options).unwrap());
}

#[test]
fn can_skip_noop_index_ignores_records_that_do_not_need_embeddings() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    save_vector_store(&paths, "bge", 384);
    let index = save_search_records(
        &paths,
        &[
            record(1, "user", "embedded"),
            record(2, "tool_result", "not embedded"),
            record(3, "assistant", ""),
        ],
    );
    let options = ingest_options(true, ModelChoice::BGESmall);

    assert!(can_skip_noop_index(&paths, &index, &options).unwrap());
}

#[test]
fn cannot_skip_noop_index_when_vectors_are_missing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    let index = open_search_index(&paths);
    let options = ingest_options(true, ModelChoice::BGESmall);

    assert!(!can_skip_noop_index(&paths, &index, &options).unwrap());
}

#[test]
fn cannot_skip_noop_index_with_incompatible_vectors() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    save_vector_store(&paths, "minilm", 384);
    let index = open_search_index(&paths);
    let options = ingest_options(true, ModelChoice::BGESmall);

    assert!(!can_skip_noop_index(&paths, &index, &options).unwrap());
}

#[test]
fn cannot_skip_noop_index_with_wrong_vector_dimensions() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    save_vector_store(&paths, "bge", 768);
    let index = open_search_index(&paths);
    let options = ingest_options(true, ModelChoice::BGESmall);

    assert!(!can_skip_noop_index(&paths, &index, &options).unwrap());
}

#[test]
fn cannot_skip_noop_index_when_model_dimensions_are_dynamic() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().to_path_buf())).expect("paths");
    save_vector_store(&paths, "potion", 256);
    let index = open_search_index(&paths);
    let options = ingest_options(true, ModelChoice::Potion);

    assert!(!can_skip_noop_index(&paths, &index, &options).unwrap());
}
#[test]
fn collect_pi_files_recurses_under_sessions_root() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let sessions_root = tmp.path().join("sessions");
    let project_root = sessions_root.join("--Users-nico-Code-memex--");
    fs::create_dir_all(&project_root).expect("create pi session dir");

    let session = project_root.join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");
    let ignored = project_root.join("notes.json");
    fs::write(&session, "{}\n").expect("write pi session");
    fs::write(&ignored, "{}\n").expect("write ignored");

    let files = crate::sources::common::jsonl_files([sessions_root]);

    assert_eq!(files, vec![session]);
}

#[test]
fn pi_sessions_root_honors_session_dir_override() {
    let _guard = env_lock();
    let tmp = tempfile::tempdir().expect("tempdir");
    let custom_sessions = tmp.path().join("custom-sessions");
    let _env = EnvVarGuard::set_os(&[
        (
            "PI_CODING_AGENT_SESSION_DIR",
            Some(custom_sessions.as_os_str()),
        ),
        ("PI_CODING_AGENT_DIR", None),
    ]);

    assert_eq!(crate::sources::pi::sessions_root(), custom_sessions);
}

#[test]
fn pi_sessions_root_honors_settings_session_dir() {
    let _guard = env_lock();
    let tmp = tempfile::tempdir().expect("tempdir");
    let pi_root = tmp.path().join("pi-agent");
    fs::create_dir_all(&pi_root).expect("create pi root");
    fs::write(
        pi_root.join("settings.json"),
        r#"{ "sessionDir": ".pi/sessions" }"#,
    )
    .expect("write settings");
    let _env = EnvVarGuard::set_os(&[
        ("PI_CODING_AGENT_SESSION_DIR", None),
        ("PI_CODING_AGENT_DIR", Some(pi_root.as_os_str())),
    ]);

    assert_eq!(
        crate::sources::pi::sessions_root(),
        pi_root.join(".pi/sessions")
    );
}

#[test]
fn pi_session_path_fallback_preserves_project_name() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let home_path = tmp
        .path()
        .join("sessions")
        .join("--home-alice-code-memex--")
        .join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");
    let users_path = tmp
        .path()
        .join("sessions")
        .join("--Users-nico-Code-memex--")
        .join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");
    let windows_path = tmp
        .path()
        .join("sessions")
        .join("--C--Users-alice-Code-memex--")
        .join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");
    let nested_path = tmp
        .path()
        .join("sessions")
        .join("--home-alice-code-acme-memex--")
        .join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");

    assert_eq!(crate::sources::pi::project_from_path(&home_path), "memex");
    assert_eq!(crate::sources::pi::project_from_path(&users_path), "memex");
    assert_eq!(
        crate::sources::pi::project_from_path(&windows_path),
        "memex"
    );
    assert_eq!(crate::sources::pi::project_from_path(&nested_path), "memex");
}

#[test]
fn ingest_pi_session_records_supported_message_shapes() {
    let _guard = env_lock();
    let tmp = tempfile::tempdir().expect("tempdir");
    let pi_root = tmp.path().join("pi-agent");
    let omp_root = tmp.path().join("omp");
    let sessions_root = pi_root.join("sessions").join("--Users-nico-Code-memex--");
    fs::create_dir_all(&sessions_root).expect("create pi sessions");
    let session_file =
        sessions_root.join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");
    fs::write(
            &session_file,
            r#"{"type":"session","version":3,"id":"11111111-1111-1111-1111-111111111111","timestamp":"2026-07-03T01:02:03Z","cwd":"/Users/nico/Code/memex"}
{"type":"message","id":"u1","timestamp":"2026-07-03T01:02:04Z","message":{"role":"user","content":[{"type":"text","text":"hello pi"}]}}
{"type":"message","id":"a1","parentId":"u1","timestamp":"2026-07-03T01:02:05Z","message":{"role":"assistant","content":[{"type":"thinking","thinking":"considering options"},{"type":"text","text":"I will run a command"},{"type":"toolCall","id":"tc1","name":"Read","arguments":{"file_path":"README.md"}}]}}
{"type":"message","id":"tr1","parentId":"a1","timestamp":"2026-07-03T01:02:06Z","message":{"role":"toolResult","toolCallId":"tc1","toolName":"Read","content":[{"type":"text","text":"README contents"}],"isError":false}}
{"type":"message","id":"b1","parentId":"tr1","timestamp":"2026-07-03T01:02:07Z","message":{"role":"bashExecution","command":"cargo test","output":"ok","exitCode":0,"cancelled":false,"truncated":false}}
{"type":"message","id":"bh1","parentId":"b1","timestamp":"2026-07-03T01:02:07Z","message":{"role":"bashExecution","command":"echo secret","output":"secret output","exitCode":0,"excludeFromContext":true}}
{"type":"compaction","id":"c1","parentId":"b1","timestamp":"2026-07-03T01:02:08Z","summary":"compacted top-level summary","firstKeptEntryId":"tr1","tokensBefore":50000}
{"type":"branch_summary","id":"br1","parentId":"u1","timestamp":"2026-07-03T01:02:09Z","fromId":"c1","summary":"branch top-level summary"}
{"type":"custom_message","id":"cm1","parentId":"br1","timestamp":"2026-07-03T01:02:10Z","customType":"memex","content":[{"type":"text","text":"extension context"}],"display":true}
{"type":"message","id":"mcs1","parentId":"cm1","timestamp":"2026-07-03T01:02:11Z","message":{"role":"compactionSummary","content":"summary text"}}
{"type":"message","id":"mbs1","parentId":"mcs1","timestamp":"2026-07-03T01:02:12Z","message":{"role":"branchSummary","summary":"message summary text"}}
"#,
        )
        .expect("write pi fixture");
    let _env = EnvVarGuard::set_os(&[
        ("PI_CODING_AGENT_DIR", Some(pi_root.as_os_str())),
        ("PI_CODING_AGENT_SESSION_DIR", None),
        ("PI_CONFIG_DIR", Some(omp_root.as_os_str())),
        ("XDG_DATA_HOME", None),
    ]);

    let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    let index = SearchIndex::open_or_create(&paths.index).expect("index");
    let options = IngestOptions {
        claude_sources: vec![tmp.path().join("missing-claude")],
        exclude_patterns: Vec::new(),
        include_agents: false,
        include_reasoning: false,
        include_codex: false,
        include_opencode: false,
        include_cursor: false,
        include_pi: true,
        include_omp: false,
        include_openclaw: false,
        include_copilot: false,
        include_grok: false,
        include_jcode: false,
        include_muse: false,
        include_antigravity: false,
        embeddings: false,
        backfill_embeddings: false,
        model: ModelChoice::default(),
        embed_runtime: EmbedRuntimeConfig::default(),
        tool_content_limits: IndexedToolContentLimits::default(),
        defer_merges: false,
    };

    let lease = ingest_lease(&paths);
    let report = ingest_all(&paths, &index, &options, &lease).expect("ingest");
    assert_eq!(report.records_added, 10);

    let mut records = index
        .records_by_session_id("11111111-1111-1111-1111-111111111111")
        .expect("records by session");
    records.sort_by_key(|record| record.turn_id);

    assert_eq!(records.len(), 10);
    assert!(records.iter().all(|record| record.source == SourceKind::Pi));
    assert!(records.iter().all(|record| record.project == "memex"));
    let source_path = session_file.to_string_lossy().to_string();
    assert!(
        records
            .iter()
            .all(|record| record.source_path == source_path)
    );
    assert_eq!(records[0].role, "user");
    assert_eq!(records[0].text, "hello pi");
    assert_eq!(records[0].links.event_id.as_deref(), Some("u1"));
    assert_eq!(records[0].links.conversation_kind.as_deref(), Some("main"));
    assert_eq!(records[1].role, "tool_use");
    assert_eq!(records[1].tool_name.as_deref(), Some("Read"));
    assert!(records[1].text.contains("README.md"));
    assert_eq!(records[1].links.event_id.as_deref(), Some("tc1"));
    assert_eq!(records[1].links.parent_event_id.as_deref(), Some("a1"));
    assert_eq!(records[2].role, "assistant");
    assert!(records[2].text.contains("I will run a command"));
    assert!(!records[2].text.contains("considering options"));
    assert_eq!(records[2].links.event_id.as_deref(), Some("a1"));
    assert_eq!(records[2].links.parent_event_id.as_deref(), Some("u1"));
    assert_eq!(records[3].role, "tool_result");
    assert_eq!(records[3].tool_name.as_deref(), Some("Read"));
    assert_eq!(records[3].text, "README contents");
    assert_eq!(records[3].links.event_id.as_deref(), Some("tr1"));
    assert_eq!(records[3].links.parent_event_id.as_deref(), Some("a1"));
    assert_eq!(records[3].links.parent_tool_use_id.as_deref(), Some("tc1"));
    assert_eq!(records[4].role, "tool_result");
    assert_eq!(records[4].tool_name.as_deref(), Some("Bash"));
    assert!(records[4].text.contains("$ cargo test"));
    assert!(records[4].text.contains("exit code: 0"));
    assert_eq!(records[5].role, "assistant");
    assert_eq!(records[5].links.event_id.as_deref(), Some("c1"));
    assert_eq!(
        records[5].links.thread_source.as_deref(),
        Some("compaction")
    );
    assert_eq!(
        records[5].links.conversation_kind.as_deref(),
        Some("compaction")
    );
    assert_eq!(records[5].text, "compaction: compacted top-level summary");
    assert_eq!(records[6].role, "assistant");
    assert_eq!(records[6].text, "branch_summary: branch top-level summary");
    assert_eq!(records[6].links.event_id.as_deref(), Some("br1"));
    assert_eq!(records[6].links.parent_event_id.as_deref(), Some("u1"));
    assert_eq!(
        records[6].links.logical_parent_event_id.as_deref(),
        Some("c1")
    );
    assert_eq!(records[6].links.thread_source.as_deref(), Some("branch"));
    assert_eq!(
        records[6].links.conversation_kind.as_deref(),
        Some("branch")
    );
    assert_eq!(records[7].role, "assistant");
    assert_eq!(records[7].text, "custom_message(memex): extension context");
    assert_eq!(records[8].role, "assistant");
    assert_eq!(records[8].text, "compactionSummary: summary text");
    assert_eq!(
        records[8].links.conversation_kind.as_deref(),
        Some("compaction")
    );
    assert_eq!(records[9].role, "assistant");
    assert_eq!(records[9].text, "branchSummary: message summary text");
    assert_eq!(
        records[9].links.conversation_kind.as_deref(),
        Some("branch")
    );
    assert!(!records.iter().any(|record| record.text.contains("secret")));
}

#[test]
fn ingest_omp_session_from_agent_root_override() {
    let _guard = env_lock();
    let tmp = tempfile::tempdir().expect("tempdir");
    let omp_agent_root = tmp.path().join("omp-agent");
    let pi_sessions = tmp.path().join("pi-sessions");
    let session_dir = omp_agent_root
        .join("sessions")
        .join("--Users-nico-Code-omp--");
    fs::create_dir_all(&session_dir).expect("create omp session dir");
    let session_file = session_dir.join("omp-session.jsonl");
    fs::write(
        &session_file,
        include_str!("../../fixtures/trajectory_parity/omp.jsonl"),
    )
    .expect("write omp fixture");
    let _env = EnvVarGuard::set_os(&[
        ("PI_CONFIG_DIR", None),
        ("PI_CODING_AGENT_SESSION_DIR", Some(pi_sessions.as_os_str())),
        ("PI_CODING_AGENT_DIR", Some(omp_agent_root.as_os_str())),
        ("XDG_DATA_HOME", None),
    ]);

    let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    let index = SearchIndex::open_or_create(&paths.index).expect("index");
    let mut options = ingest_options(false, ModelChoice::default());
    options.include_omp = true;
    let lease = ingest_lease(&paths);
    let report = ingest_all(&paths, &index, &options, &lease).expect("ingest");
    assert_eq!(report.files_scanned, 1);
    assert_eq!(report.records_added, 4);

    let records = index
        .records_by_session_id("omp-session")
        .expect("records by session");
    assert_eq!(records.len(), 4);
    assert!(
        records
            .iter()
            .all(|record| record.source == SourceKind::Omp)
    );
    assert!(records.iter().all(|record| record.project == "omp-project"));
    assert!(
        records
            .iter()
            .any(|record| record.text == "Inspect the project")
    );
    assert!(
        records
            .iter()
            .any(|record| record.text == "project contents")
    );
    assert!(
        records
            .iter()
            .all(|record| record.source_path == session_file.to_string_lossy())
    );
}

#[test]
fn ingest_grok_session_from_grok_home_override() {
    let _guard = env_lock();
    let tmp = tempfile::tempdir().expect("tempdir");
    let grok_home = tmp.path().join("grok-home");
    let session_dir = grok_home
        .join("sessions")
        .join("%2Fworkspace%2Fgrok-project")
        .join("grok-session");
    fs::create_dir_all(&session_dir).expect("create grok session dir");
    fs::write(
            session_dir.join("summary.json"),
            r#"{"info":{"id":"grok-session","cwd":"/workspace/grok-project"},"git_root_dir":"/workspace/grok-project/"}"#,
        )
        .expect("write grok summary");
    let session_file = session_dir.join("updates.jsonl");
    fs::write(
        &session_file,
        include_str!("../../fixtures/trajectory_parity/grok.jsonl"),
    )
    .expect("write grok fixture");
    let _env = EnvVarGuard::set_os(&[("GROK_HOME", Some(grok_home.as_os_str()))]);

    let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    let index = SearchIndex::open_or_create(&paths.index).expect("index");
    let mut options = ingest_options(false, ModelChoice::default());
    options.include_grok = true;
    let lease = ingest_lease(&paths);
    let report = ingest_all(&paths, &index, &options, &lease).expect("ingest");
    assert_eq!(report.files_scanned, 1);
    // Reasoning is off, the pending tool update and the unknown event are skipped.
    assert_eq!(report.records_added, 5);

    let records = index
        .records_by_session_id("grok-session")
        .expect("records by session");
    assert_eq!(records.len(), 5);
    assert!(
        records
            .iter()
            .all(|record| record.source == SourceKind::Grok)
    );
    assert!(
        records
            .iter()
            .all(|record| record.project == "grok-project")
    );
    assert!(!records.iter().any(|record| record.role == "reasoning"));
    assert!(
        records
            .iter()
            .any(|record| record.text == "Inspect the project")
    );
    assert!(
        records
            .iter()
            .any(|record| record.text == "project contents")
    );
    assert!(records.iter().any(
        |record| record.tool_name.as_deref() == Some("read_file") && record.role == "tool_use"
    ));
    assert!(records.iter().any(|record| {
        record.links.conversation_kind.as_deref() == Some("compaction")
            && record.text.starts_with("session_recap: Read the README")
    }));
    assert!(
        records
            .iter()
            .all(|record| record.source_path == session_file.to_string_lossy())
    );
}

#[test]
fn ingest_pi_incremental_records_keep_header_project() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let sessions_root = tmp
        .path()
        .join("sessions")
        .join("--home-alice-code-my-project--");
    fs::create_dir_all(&sessions_root).expect("create pi sessions");
    let session_file =
        sessions_root.join("20260703T010203Z_11111111-1111-1111-1111-111111111111.jsonl");
    let existing = r#"{"type":"session","version":3,"id":"22222222-2222-2222-2222-222222222222","timestamp":"2026-07-03T01:02:03Z","cwd":"/home/alice/code/my-project"}
{"type":"message","id":"u1","timestamp":"2026-07-03T01:02:04Z","message":{"role":"user","content":"first"}}
"#;
    let appended = r#"{"type":"message","id":"a1","timestamp":"2026-07-03T01:02:05Z","message":{"role":"assistant","content":"second"}}
"#;
    fs::write(&session_file, format!("{existing}{appended}")).expect("write pi fixture");

    let (raw_tx_record, rx_record) = unbounded();
    let tx_record = RecordSender::new(raw_tx_record, IndexedToolContentLimits::default());
    let (tx_update, _rx_update) = unbounded();
    let task = FileTask {
        codex_metadata_offsets: None,
        path: session_file,
        source: SourceKind::Pi,
        offset: existing.len() as u64,
        turn_id: 1,
        legacy_turn_id: None,
        claude_background: None,
        size: (existing.len() + appended.len()) as u64,
        mtime: 0,
        change: FileChange::Append,
        pending_tool_calls: HashMap::new(),
        identity: FileIdentity::default(),
        parser_version: crate::sources::index_state_version(SourceKind::Pi),
    };
    let progress = Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], false));
    let next_doc_id = AtomicU64::new(1);

    parse_pi_file(
        &task,
        false,
        &tx_record,
        &tx_update,
        &next_doc_id,
        &progress,
    )
    .expect("parse pi");
    drop(tx_record);
    let records: Vec<_> = rx_record.try_iter().collect();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].project, "my-project");
    assert_eq!(records[0].text, "second");
}
#[test]
fn collect_copilot_files_finds_session_events() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let session_dir = tmp
        .path()
        .join("session-state")
        .join("11111111-1111-4111-8111-111111111111");
    fs::create_dir_all(&session_dir).expect("create session dir");

    let events = session_dir.join("events.jsonl");
    let ignored = session_dir.join("workspace.yaml");
    fs::write(&events, "{}\n").expect("write events");
    fs::write(&ignored, "cwd: /tmp/project\n").expect("write workspace");

    let files =
        crate::sources::copilot::discover_sessions_from_root(&tmp.path().join("session-state"))
            .into_iter()
            .map(|file| file.path)
            .collect::<Vec<_>>();

    assert_eq!(files, vec![events]);
}

#[test]
fn parse_copilot_session_extracts_messages_tools_and_workspace() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let session_id = "11111111-1111-4111-8111-111111111111";
    let session_dir = tmp.path().join("session-state").join(session_id);
    fs::create_dir_all(&session_dir).expect("create session dir");
    fs::write(
            session_dir.join("workspace.yaml"),
            "cwd: /Users/nico/Code/memex\ngitRoot: /Users/nico/Code/memex\nrepository: nicosuave/memex\nbranch: main\n",
        )
        .expect("write workspace");
    let events = session_dir.join("events.jsonl");
    fs::write(
            &events,
            concat!(
                "{\"type\":\"session.start\",\"timestamp\":\"2026-06-01T12:00:00Z\",\"data\":{\"sessionId\":\"11111111-1111-4111-8111-111111111111\",\"context\":{\"cwd\":\"/Users/nico/Code/memex\",\"repository\":\"nicosuave/memex\"}}}\n",
                "{\"type\":\"user.message\",\"timestamp\":\"2026-06-01T12:00:01Z\",\"data\":{\"content\":\"Find the parser\"}}\n",
                "{\"type\":\"assistant.message\",\"timestamp\":\"2026-06-01T12:00:02Z\",\"data\":{\"content\":\"I will inspect ingestion.\"}}\n",
                "{\"type\":\"tool.execution_start\",\"timestamp\":\"2026-06-01T12:00:03Z\",\"data\":{\"toolCallId\":\"call-1\",\"toolName\":\"grep\",\"arguments\":{\"pattern\":\"parse_copilot\"}}}\n",
                "{\"type\":\"tool.execution_complete\",\"timestamp\":\"2026-06-01T12:00:04Z\",\"data\":{\"toolCallId\":\"call-1\",\"success\":true,\"result\":{\"content\":\"src/ingest.rs\"}}}\n"
            ),
        )
        .expect("write events");
    let meta = events.metadata().expect("metadata");
    let task = FileTask {
        codex_metadata_offsets: None,
        path: events.clone(),
        source: SourceKind::Copilot,
        offset: 0,
        turn_id: 0,
        legacy_turn_id: None,
        claude_background: None,
        size: meta.len(),
        mtime: 0,
        change: FileChange::Append,
        pending_tool_calls: HashMap::new(),
        identity: FileIdentity::default(),
        parser_version: crate::sources::index_state_version(SourceKind::Copilot),
    };
    let (raw_tx_record, rx_record) = unbounded();
    let tx_record = RecordSender::new(raw_tx_record, IndexedToolContentLimits::default());
    let (tx_update, rx_update) = unbounded();
    let next_doc_id = AtomicU64::new(1);
    let mut total_bytes = [0; SOURCE_COUNT];
    total_bytes[SourceKind::Copilot.idx()] = meta.len();
    let mut files_total = [0; SOURCE_COUNT];
    files_total[SourceKind::Copilot.idx()] = 1;
    let progress = Arc::new(Progress::new(total_bytes, files_total, false));

    parse_copilot_session(&task, &tx_record, &tx_update, &next_doc_id, &progress)
        .expect("parse copilot session");
    drop(tx_record);
    drop(tx_update);

    let records: Vec<Record> = rx_record.try_iter().collect();
    assert_eq!(records.len(), 4);
    assert!(records.iter().all(|r| r.source == SourceKind::Copilot));
    assert!(records.iter().all(|r| r.project == "memex"));
    assert!(records.iter().all(|r| r.session_id == session_id));
    assert_eq!(records[0].role, "user");
    assert_eq!(
        records[0].links.event_id.as_deref(),
        Some("11111111-1111-4111-8111-111111111111:0")
    );
    assert_eq!(records[0].links.conversation_kind.as_deref(), Some("main"));
    assert_eq!(records[1].role, "assistant");
    assert_eq!(
        records[1].links.event_id.as_deref(),
        Some("11111111-1111-4111-8111-111111111111:1")
    );
    assert_eq!(records[2].role, "tool_use");
    assert_eq!(records[2].tool_name.as_deref(), Some("grep"));
    assert!(records[2].text.contains("parse_copilot"));
    assert_eq!(records[2].links.event_id.as_deref(), Some("call-1"));
    assert_eq!(records[3].role, "tool_result");
    assert_eq!(records[3].tool_name.as_deref(), Some("grep"));
    assert_eq!(records[3].tool_output.as_deref(), Some("src/ingest.rs"));
    assert_eq!(records[3].links.parent_event_id.as_deref(), Some("call-1"));
    assert_eq!(
        records[3].links.parent_tool_use_id.as_deref(),
        Some("call-1")
    );

    let update = rx_update.try_recv().expect("file update");
    assert_eq!(update.state.offset, meta.len());
    assert_eq!(update.state.turn_id, 4);
}

#[test]
fn writer_loop_accepts_copilot_source_progress() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let index_dir = tmp.path().join("index");
    let vector_dir = tmp.path().join("vectors");
    fs::create_dir_all(&index_dir).expect("create index dir");
    fs::create_dir_all(&vector_dir).expect("create vector dir");
    let index = SearchIndex::open_or_create(&index_dir).expect("open index");
    let (tx_record, rx_record) = unbounded();
    tx_record
        .send(Record {
            source: SourceKind::Copilot,
            doc_id: 1,
            ts: 1_780_291_200_000,
            project: "memex".to_string(),
            session_id: "11111111-1111-4111-8111-111111111111".to_string(),
            turn_id: 0,
            role: "user".to_string(),
            text: "Find the parser".to_string(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks::default(),
            source_path: tmp
                .path()
                .join(".copilot/session-state/11111111-1111-4111-8111-111111111111/events.jsonl")
                .to_string_lossy()
                .to_string(),
        })
        .expect("send record");
    drop(tx_record);

    let progress = Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], false));
    let ctx = WriterContext {
        index_root: PathBuf::new(),
        input_bytes: None,
        defer_merges: false,
        embeddings: false,
        do_backfill_embeddings: false,
        reset_vector_store: false,
        vector_dir,
        analytics_path: tmp.path().join("state").join("analytics.sqlite"),
        progress,
        model: ModelChoice::default(),
        embed_runtime: EmbedRuntimeConfig::default(),
        tool_content_limits: IndexedToolContentLimits::default(),
        reconcile_vector_ids: false,
        scope_targets: Vec::new(),
        opencode_session_cwds: HashMap::new(),
        repositories: Arc::new(crate::repository::RepositoryResolver::default()),
        codex_metadata_checkpoints: HashMap::new(),
        vector_delete_paths: HashSet::new(),
    };

    let writer = index.writer().expect("open writer");
    let (decision_tx, decision_rx) = bounded(1);
    decision_tx.send(WriterDecision::Commit).expect("commit");
    let outcome = run_writer_fixture(index, writer, rx_record, decision_rx, Vec::new(), ctx)
        .expect("write copilot record");
    let WriterOutcome::Published {
        records_added,
        records_embedded,
    } = outcome
    else {
        panic!("writer cancelled")
    };

    assert_eq!(records_added, 1);
    assert_eq!(records_embedded, 0);
}

fn append_claude_message(path: &Path, text: &str) {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(
        file,
        "{}",
        serde_json::json!({
            "type": "user", "uuid": text,
            "message": {"role": "user", "content": text},
        })
    )
    .unwrap();
}

fn indexed_texts(paths: &Paths) -> Vec<String> {
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    let mut texts = Vec::new();
    index
        .for_each_record(|record| {
            texts.push(record.text);
            Ok(())
        })
        .unwrap();
    texts.sort();
    texts
}

#[test]
fn targeted_ingest_only_updates_selected_files_until_reconciliation() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("claude");
    fs::create_dir_all(&source).unwrap();
    let first = source.join("first.jsonl");
    let second = source.join("second.jsonl");
    append_claude_message(&first, "first original");
    append_claude_message(&second, "second original");
    let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![source];
    let lease = ingest_lease(&paths);
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    assert_eq!(
        ingest_all(&paths, &index, &options, &lease)
            .unwrap()
            .files_scanned,
        2
    );
    let cache_path = paths.state.join("scan_cache.json");
    let original_cache = serde_json::to_value(ScanCache::load(&cache_path).unwrap()).unwrap();
    let state_before = IngestState::load(&paths.state.join("ingest.json")).unwrap();
    append_claude_message(&first, "first appended");
    append_claude_message(&second, "second missed event");
    // The watcher emits canonical paths; ingestion must retain lexical
    // discovery keys rather than create duplicate state/index entries.
    let dirty = HashSet::from([fs::canonicalize(&first).unwrap()]);
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let result = ingest_dirty(&paths, &index, &options, &lease, &dirty).unwrap();
    assert!(!result.full_scan);
    assert_eq!(result.report.files_scanned, 1);
    assert_eq!(result.report.records_added, 1);
    assert_eq!(
        indexed_texts(&paths),
        ["first appended", "first original", "second original"]
    );
    let state_after = IngestState::load(&paths.state.join("ingest.json")).unwrap();
    let key = second.to_string_lossy();
    assert_eq!(
        state_after.files[key.as_ref()].offset,
        state_before.files[key.as_ref()].offset
    );
    assert_eq!(state_after.files.len(), 2);
    assert_eq!(
        serde_json::to_value(ScanCache::load(&cache_path).unwrap()).unwrap(),
        original_cache
    );

    // A no-op batch must not mark a complete scan fresh either.
    let database = rusqlite::Connection::open(paths.state.join("checkpoints.sqlite")).unwrap();
    let version: i64 = database
        .pragma_query_value(None, "data_version", |row| row.get(0))
        .unwrap();
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let result = ingest_dirty(&paths, &index, &options, &lease, &dirty).unwrap();
    assert_eq!(
        database
            .pragma_query_value(None, "data_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        version
    );
    assert!(!result.full_scan);
    assert_eq!(result.report.records_added, 0);
    assert_eq!(
        serde_json::to_value(ScanCache::load(&cache_path).unwrap()).unwrap(),
        original_cache
    );

    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let report = ingest_all(&paths, &index, &options, &lease).unwrap();
    assert_eq!(report.files_scanned, 2);
    assert_eq!(report.records_added, 1);
    assert_eq!(
        indexed_texts(&paths),
        [
            "first appended",
            "first original",
            "second missed event",
            "second original"
        ]
    );
}

#[test]
fn targeted_ingest_creates_and_replaces_files_without_unrelated_discovery() {
    let _guard = env_lock();
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("claude");
    fs::create_dir_all(&source).unwrap();
    let first = source.join("first.jsonl");
    append_claude_message(&first, "existing");
    let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![source.clone()];
    let lease = ingest_lease(&paths);
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    ingest_all(&paths, &index, &options, &lease).unwrap();
    // A full discovery would fail on this unrelated configured source.
    let bad_root = tmp.path().join("not-a-directory");
    fs::write(&bad_root, "not a directory").unwrap();
    let _env = EnvVarGuard::set_os(&[("OPENCODE_DATA_DIR", Some(bad_root.as_os_str()))]);
    options.include_opencode = true;
    let new_file = source.join("new.jsonl");
    append_claude_message(&new_file, "created");
    for expected in ["created", "rewritten"] {
        if expected == "rewritten" {
            fs::write(&new_file, []).unwrap();
            append_claude_message(&new_file, expected);
        }
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        let result = ingest_dirty(
            &paths,
            &index,
            &options,
            &lease,
            &HashSet::from([new_file.clone()]),
        )
        .unwrap();
        assert!(!result.full_scan);
        assert_eq!(result.report.files_scanned, 1);
        assert_eq!(result.report.records_added, 1);
        let mut expected_texts = vec!["existing".to_string(), expected.to_string()];
        expected_texts.sort();
        assert_eq!(indexed_texts(&paths), expected_texts);
    }
}

#[test]
fn targeted_ingest_escalates_pending_publication_to_full_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("claude");
    fs::create_dir_all(&source).unwrap();
    let first = source.join("first.jsonl");
    let second = source.join("second.jsonl");
    append_claude_message(&first, "first original");
    append_claude_message(&second, "second original");
    let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![source];
    let lease = ingest_lease(&paths);
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    ingest_all(&paths, &index, &options, &lease).unwrap();
    let db = analytics_path(&paths.state);
    let mut analytics = AnalyticsWriter::open(&db).unwrap();
    analytics
        .record(&record(999, "user", "unpublished orphan"))
        .unwrap();
    analytics.flush().unwrap();
    AnalyticsStore::open(&db).unwrap().mark_complete().unwrap();
    append_claude_message(&first, "first appended");
    append_claude_message(&second, "second missed event");
    let state = IngestState::load(&paths.state.join("ingest.json")).unwrap();
    PendingIngest {
        next_doc_id: state.next_doc_id,
        source_paths: vec![second.to_string_lossy().into_owned()],
        session_scopes: Vec::new(),
        vector_delete_paths: Vec::new(),
        vector_publication: false,
        embedding_publication: Some(false),
    }
    .save_with_lease(&pending_ingest_path(&paths), &lease)
    .unwrap();
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let result = ingest_dirty(&paths, &index, &options, &lease, &HashSet::from([first])).unwrap();
    assert!(result.full_scan);
    assert_eq!(result.report.files_scanned, 2);
    assert_eq!(
        indexed_texts(&paths),
        [
            "first appended",
            "first original",
            "second missed event",
            "second original"
        ]
    );
    assert!(
        PendingIngest::load(&pending_ingest_path(&paths))
            .unwrap()
            .is_none()
    );
    let rows = AnalyticsStore::open_read_only(&db)
        .unwrap()
        .query_sessions_detailed(None, None, None, None, None)
        .unwrap();
    assert!(rows.iter().all(|row| row.source_path != "source-999.jsonl"));
}

#[test]
fn targeted_ingest_preserves_unselected_database_ownership_and_wal_updates() {
    let _guard = env_lock();
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("opencode");
    fs::create_dir_all(&source).unwrap();
    let first = source.join("opencode.db");
    let second = source.join("opencode-work.db");
    let create = |path: &Path, id: &str| {
        let writer = rusqlite::Connection::open(path).unwrap();
        writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
            CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT, time_created INTEGER, time_updated INTEGER);
            CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
            CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, data TEXT);
            CREATE TABLE event (id TEXT NOT NULL, aggregate_id TEXT NOT NULL);").unwrap();
        writer
            .execute("INSERT INTO session VALUES (?1, NULL, '/tmp', 1, 2)", [id])
            .unwrap();
        writer
            .execute(
                "INSERT INTO message VALUES ('message', ?1, 3, '{\"role\":\"assistant\"}')",
                [id],
            )
            .unwrap();
        writer
            .execute(
                "INSERT INTO part VALUES ('part', 'message', ?1)",
                [
                    serde_json::json!({"type": "text", "text": format!("{id} original")})
                        .to_string(),
                ],
            )
            .unwrap();
        writer
            .execute("INSERT INTO event VALUES ('event-1', ?1)", [id])
            .unwrap();
        writer
    };
    let first_writer = create(&first, "first");
    let second_writer = create(&second, "second");
    let _env = EnvVarGuard::set_os(&[("OPENCODE_DATA_DIR", Some(source.as_os_str()))]);
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.include_opencode = true;
    let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let lease = ingest_lease(&paths);
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    assert_eq!(
        ingest_all(&paths, &index, &options, &lease)
            .unwrap()
            .records_added,
        2
    );
    let second_key = second.to_string_lossy().into_owned();
    let prior = IngestState::load(&paths.state.join("ingest.json")).unwrap();
    for (writer, id) in [(&first_writer, "first"), (&second_writer, "second")] {
        writer
            .execute(
                "UPDATE part SET data = ?1",
                [
                    serde_json::json!({"type": "text", "text": format!("{id} updated")})
                        .to_string(),
                ],
            )
            .unwrap();
        writer
            .execute("INSERT INTO event VALUES ('event-2', ?1)", [id])
            .unwrap();
    }
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let dirty = HashSet::from([source.join("opencode.db-wal")]);
    let result = ingest_dirty(&paths, &index, &options, &lease, &dirty).unwrap();
    assert!(!result.full_scan);
    assert_eq!(result.report.files_scanned, 1);
    assert_eq!(indexed_texts(&paths), ["first updated", "second original"]);
    let updated = IngestState::load(&paths.state.join("ingest.json")).unwrap();
    assert_eq!(
        updated.opencode_databases[&second_key],
        prior.opencode_databases[&second_key]
    );
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    ingest_all(&paths, &index, &options, &lease).unwrap();
    assert_eq!(indexed_texts(&paths), ["first updated", "second updated"]);

    // Ownership changes require a complete inventory, including the
    // unaffected database and any compatible legacy session store.
    first_writer
        .execute("DELETE FROM session WHERE id = 'first'", [])
        .unwrap();
    first_writer
        .execute("INSERT INTO event VALUES ('event-3', 'first')", [])
        .unwrap();
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let result = ingest_dirty(&paths, &index, &options, &lease, &dirty).unwrap();
    assert!(result.full_scan);
    assert_eq!(indexed_texts(&paths), ["second updated"]);
}

#[test]
fn targeted_ingest_codex_history_uses_known_rollouts_without_discovery() {
    let _guard = env_lock();
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("custom-codex");
    let sessions = home.join("sessions");
    fs::create_dir_all(&sessions).unwrap();
    let id = "11111111-1111-1111-1111-111111111111";
    let rollout = sessions.join(format!("rollout-{id}.jsonl"));
    fs::write(&rollout, format!("{}\n{}\n",
        serde_json::json!({"type": "session_meta", "payload": {"id": id, "cwd": "/tmp"}}),
        serde_json::json!({"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "rollout original"}]}}),
    )).unwrap();
    let history = home.join("history.jsonl");
    fs::write(
        &history,
        format!(
            "{}\n",
            serde_json::json!({"session_id": id, "text": "duplicate history", "ts": 1})
        ),
    )
    .unwrap();
    let _env = EnvVarGuard::set_os(&[("CODEX_HOME", Some(home.as_os_str()))]);
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.include_codex = true;
    let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let lease = ingest_lease(&paths);
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    ingest_all(&paths, &index, &options, &lease).unwrap();
    assert_eq!(indexed_texts(&paths), ["rollout original"]);
    let mut file = fs::OpenOptions::new().append(true).open(&history).unwrap();
    for (session_id, text) in [(id, "duplicate append"), ("history-only", "history only")] {
        writeln!(
            file,
            "{}",
            serde_json::json!({"session_id": session_id, "text": text, "ts": 2})
        )
        .unwrap();
    }
    drop(file);
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let result = ingest_dirty(&paths, &index, &options, &lease, &HashSet::from([history])).unwrap();
    assert!(!result.full_scan);
    assert_eq!(result.report.files_scanned, 1);
    assert_eq!(indexed_texts(&paths), ["history only", "rollout original"]);
}

#[test]
fn full_reconciliation_removes_confirmed_missing_transcript_everywhere() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("claude");
    fs::create_dir_all(&source).unwrap();
    let removed = source.join("removed.jsonl");
    let surviving = source.join("surviving.jsonl");
    append_claude_message(&removed, "remove this transcript");
    append_claude_message(&surviving, "keep this transcript");
    let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![source];
    let lease = ingest_lease(&paths);
    let index = SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
    ingest_all(&paths, &index, &options, &lease).unwrap();
    drop(index);

    let removed_path = removed.to_string_lossy().into_owned();
    let surviving_path = surviving.to_string_lossy().into_owned();
    let mut removed_doc_ids = HashSet::new();
    let mut surviving_doc_ids = HashSet::new();
    SearchIndex::open_or_create(&paths.index)
        .unwrap()
        .for_each_record(|record| {
            if record.source_path == removed_path {
                removed_doc_ids.insert(record.doc_id);
            } else if record.source_path == surviving_path {
                surviving_doc_ids.insert(record.doc_id);
            }
            Ok(())
        })
        .unwrap();
    let mut vectors = VectorIndex::open_or_create(&paths.vectors, 4, Some("test-model")).unwrap();
    for doc_id in removed_doc_ids.iter().chain(surviving_doc_ids.iter()) {
        vectors.add(*doc_id, &[0.0; 4]).unwrap();
    }
    vectors.save().unwrap();

    fs::remove_file(&removed).unwrap();
    // A targeted event for the surviving file must not infer global
    // absence from its partial inventory.
    let index = SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
    let dirty = ingest_dirty(
        &paths,
        &index,
        &options,
        &lease,
        &HashSet::from([surviving.clone()]),
    )
    .unwrap();
    assert!(!dirty.full_scan);
    assert!(
        IngestState::load(&paths.state.join("ingest.json"))
            .unwrap()
            .files
            .contains_key(&removed_path)
    );

    let index = SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
    let report = ingest_all(&paths, &index, &options, &lease).unwrap();
    assert_eq!(report.records_added, 0);
    assert_eq!(indexed_texts(&paths), ["keep this transcript"]);

    let analytics = AnalyticsStore::open(analytics_path(&paths.state)).unwrap();
    let sessions = analytics
        .query_sessions_detailed(None, None, None, None, None)
        .unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].source_path, surviving_path);
    let state = IngestState::load(&paths.state.join("ingest.json")).unwrap();
    assert!(!state.files.contains_key(&removed_path));
    assert!(state.files.contains_key(&surviving_path));
    let vector_inventory = VectorIndex::inventory(&paths.vectors).unwrap().unwrap();
    assert_eq!(vector_inventory.doc_ids, surviving_doc_ids);

    let index = SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
    assert_eq!(
        ingest_all(&paths, &index, &options, &lease)
            .unwrap()
            .records_added,
        0,
        "a repeated full reconciliation is idempotent"
    );

    append_claude_message(&removed, "recreated transcript");
    let index = SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
    assert_eq!(
        ingest_all(&paths, &index, &options, &lease)
            .unwrap()
            .records_added,
        1
    );
    assert_eq!(
        indexed_texts(&paths),
        ["keep this transcript", "recreated transcript"]
    );
    assert!(
        IngestState::load(&paths.state.join("ingest.json"))
            .unwrap()
            .files
            .contains_key(&removed_path)
    );
}

#[test]
fn full_reconciliation_keeps_history_when_a_transcript_parent_is_unavailable() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("claude");
    let project = root.join("project");
    let transcript = project.join("session.jsonl");
    fs::create_dir_all(&project).unwrap();
    append_claude_message(&transcript, "must survive unavailable parent");
    let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![root];
    let lease = ingest_lease(&paths);
    ingest_all(&paths, &open_search_index(&paths), &options, &lease).unwrap();

    let source_path = transcript.to_string_lossy().into_owned();
    fs::remove_dir_all(&project).unwrap();
    let report = ingest_all(&paths, &open_search_index(&paths), &options, &lease).unwrap();

    assert_eq!(report.records_added, 0);
    assert_eq!(indexed_texts(&paths), ["must survive unavailable parent"]);
    assert!(
        IngestState::load(&paths.state.join("ingest.json"))
            .unwrap()
            .files
            .contains_key(&source_path)
    );
}

#[test]
fn background_marker_completed_after_partial_ingest_reclassifies_session() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("claude");
    fs::create_dir_all(&source).unwrap();
    let transcript = source.join("session.jsonl");
    // Keep appends beyond the identity prefix so only incremental parsing
    // can discover the marker, rather than a replacement-triggered parse.
    append_claude_message(&transcript, &"initial text ".repeat(500));
    let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![source];
    let lease = ingest_lease(&paths);
    ingest_all(&paths, &open_search_index(&paths), &options, &lease).unwrap();
    let marker = r#"{"type":"assistant","sessionKind":"bg","message":{"content":"background"}}"#;
    let split = marker.len() / 2;
    let marker_offset = transcript.metadata().unwrap().len();
    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(&marker.as_bytes()[..split]).unwrap();
    }
    ingest_all(&paths, &open_search_index(&paths), &options, &lease).unwrap();
    let state = IngestState::load(&paths.state.join("ingest.json")).unwrap();
    assert_eq!(
        state.files[transcript.to_str().unwrap()].offset,
        marker_offset
    );
    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(&marker.as_bytes()[split..]).unwrap();
        file.write_all(b"\n").unwrap();
    }
    append_claude_message(&transcript, "after background marker");
    ingest_all(&paths, &open_search_index(&paths), &options, &lease).unwrap();
    let state = IngestState::load(&paths.state.join("ingest.json")).unwrap();
    assert_eq!(
        state.files[transcript.to_str().unwrap()].claude_background,
        Some(true)
    );
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    let mut count = 0;
    index
        .for_each_record(|record| {
            count += 1;
            assert_eq!(record.links.conversation_kind.as_deref(), Some("subagent"));
            Ok(())
        })
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn incremental_claude_background_marker_reclassifies_prior_records() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("claude");
    fs::create_dir_all(&source).unwrap();
    let transcript = source.join("session.jsonl");
    append_claude_message(&transcript, "before background marker");
    let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![source];
    let lease = ingest_lease(&paths);
    ingest_all(&paths, &open_search_index(&paths), &options, &lease).unwrap();

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&transcript)
        .unwrap();
    writeln!(
        file,
        "{}",
        serde_json::json!({
            "type": "assistant", "sessionKind": "bg", "uuid": "background-marker",
            "message": {"role": "assistant", "content": "background marker"},
        })
    )
    .unwrap();
    ingest_dirty(
        &paths,
        &open_search_index(&paths),
        &options,
        &lease,
        &HashSet::from([transcript.clone()]),
    )
    .unwrap();

    let source_path = transcript.to_string_lossy().into_owned();
    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    let mut records = Vec::new();
    index
        .for_each_record(|record| {
            if record.source_path == source_path {
                records.push(record);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(records.len(), 2);
    assert!(
        records
            .iter()
            .all(|record| { record.links.conversation_kind.as_deref() == Some("subagent") })
    );
    let sessions = AnalyticsStore::open(analytics_path(&paths.state))
        .unwrap()
        .query_sessions_detailed(None, None, None, None, None)
        .unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].conversation_kind.as_deref(), Some("subagent"));
    let state = IngestState::load(&paths.state.join("ingest.json")).unwrap();
    assert_eq!(state.files[&source_path].claude_background, Some(true));
}

#[test]
fn claude_parser_defers_background_marker_appended_after_task_boundary() {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("claude");
    fs::create_dir_all(&source).unwrap();
    let transcript = source.join("session.jsonl");
    append_claude_message(&transcript, "initial record");
    let paths = Paths::new(Some(tmp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![source];
    let lease = ingest_lease(&paths);
    ingest_all(&paths, &open_search_index(&paths), &options, &lease).unwrap();

    let source_path = transcript.to_string_lossy().into_owned();
    let mut state = IngestState::load(&paths.state.join("ingest.json")).unwrap();
    append_claude_message(&transcript, "within task boundary");
    let background_marker = serde_json::to_string(&serde_json::json!({
        "type": "assistant", "sessionKind": "bg", "uuid": "late-background-marker",
        "message": {"role": "assistant", "content": "late background marker"},
    }))
    .unwrap();
    let split_at = background_marker.len() / 2;
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&transcript)
        .unwrap();
    file.write_all(&background_marker.as_bytes()[..split_at])
        .unwrap();
    drop(file);
    let metadata = transcript.metadata().unwrap();
    let (task, skip) = discovery::prepare_file_task(
        transcript.clone(),
        SourceKind::Claude,
        false,
        &metadata,
        state.files.get(&source_path),
    );
    assert!(!skip);
    assert_eq!(task.claude_background, Some(false));

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&transcript)
        .unwrap();
    file.write_all(&background_marker.as_bytes()[split_at..])
        .unwrap();
    file.write_all(b"\n").unwrap();
    drop(file);

    let mut parsed_records = Vec::new();
    let (parsed, background_session) = crate::sources::claude::parse_index_records_with_background(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            legacy_turn_id: task.legacy_turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        false,
        task.claude_background,
        task.size,
        &AtomicU64::new(state.next_doc_id),
        |record| {
            parsed_records.push(record);
            Ok(())
        },
    )
    .unwrap();
    assert!(parsed.offset < task.size);
    assert!(!background_session);
    assert_eq!(parsed_records.len(), 1);

    let mut bounded_state = execution::completed_file_state(
        &task,
        parsed.offset,
        parsed.turn_id,
        parsed.legacy_turn_id,
        parsed.pending_tool_calls,
    );
    bounded_state.claude_background = Some(background_session);
    state.files.insert(source_path.clone(), bounded_state);
    state
        .save_with_lease(&paths.state.join("ingest.json"), &lease)
        .unwrap();

    ingest_dirty(
        &paths,
        &open_search_index(&paths),
        &options,
        &lease,
        &HashSet::from([transcript]),
    )
    .unwrap();

    let index = SearchIndex::open_or_create(&paths.index).unwrap();
    let mut records = Vec::new();
    index
        .for_each_record(|record| {
            if record.source_path == source_path {
                records.push(record);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(records.len(), 3);
    assert!(
        records
            .iter()
            .all(|record| { record.links.conversation_kind.as_deref() == Some("subagent") })
    );
    let state = IngestState::load(&paths.state.join("ingest.json")).unwrap();
    assert_eq!(state.files[&source_path].claude_background, Some(true));
}

#[test]
fn antigravity_ingest_tracks_wal_updates_and_checkpoint_without_duplicates() {
    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("gemini");
    let database = source.join("antigravity-ide/conversations/session.db");
    fs::create_dir_all(database.parent().unwrap()).unwrap();
    let _env = EnvVarGuard::set_os(&[("ANTIGRAVITY_HOME", Some(source.as_os_str()))]);
    let writer = rusqlite::Connection::open(&database).unwrap();
    writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
        CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_type INTEGER, status INTEGER, step_payload BLOB);").unwrap();
    // Protobuf user step: field 19 { field 2: text }.
    let put = |text: &str| {
        let mut payload = vec![0x9a, 0x01, (text.len() + 2) as u8, 0x12, text.len() as u8];
        payload.extend_from_slice(text.as_bytes());
        writer
            .execute(
                "INSERT OR REPLACE INTO steps VALUES (0, 14, 3, ?1)",
                [payload],
            )
            .unwrap();
    };
    put("original");
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.include_antigravity = true;
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let lease = ingest_lease(&paths);
    let full = || {
        let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
        ingest_all(&paths, &index, &options, &lease).unwrap()
    };
    assert_eq!(full().records_added, 1);
    assert_eq!(indexed_texts(&paths), ["original"]);
    assert_eq!(full().records_added, 0);
    let before = database.metadata().unwrap();
    put("updated");
    assert_eq!(
        database.metadata().unwrap().modified().unwrap(),
        before.modified().unwrap()
    );
    assert_eq!(database.metadata().unwrap().len(), before.len());
    assert!(crate::watch::dirty_needs_ingest(&paths, &HashSet::from([database.clone()])).unwrap());
    let wal = database.with_file_name("session.db-wal");
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let result = ingest_dirty(&paths, &index, &options, &lease, &HashSet::from([wal])).unwrap();
    assert!(!result.full_scan);
    assert_eq!(result.report.records_added, 1);
    assert_eq!(indexed_texts(&paths), ["updated"]);
    assert!(!crate::watch::dirty_needs_ingest(&paths, &HashSet::from([database.clone()])).unwrap());
    put("full scan update");
    assert_eq!(full().records_added, 1);
    assert_eq!(indexed_texts(&paths), ["full scan update"]);
    writer
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    full();
    assert_eq!(indexed_texts(&paths), ["full scan update"]);
    drop(writer);
    full();
    assert_eq!(indexed_texts(&paths), ["full scan update"]);
    assert_eq!(full().records_added, 0);
}

#[test]
fn cleanup_recovery_restores_reparsed_survivor_vectors() {
    assert_recovers_vector_crash(false, false);
    assert_recovers_vector_crash(true, false);
}

#[test]
fn cleanup_only_vector_recovery_reconciles_orphans_without_deletion_targets() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = Paths::new(Some(tmp.path().join("memex"))).expect("paths");
    paths.ensure_dirs().expect("ensure dirs");
    let index = save_search_records(
        &paths,
        &[record(1, "user", "first"), record(2, "assistant", "second")],
    );
    index.publish_generation_if_uninitialized().unwrap();
    let mut vectors =
        VectorIndex::open_or_create(&paths.vectors, 4, Some("test-model")).expect("create vectors");
    vectors.add(1, &[0.0; 4]).unwrap();
    vectors.add(2, &[0.0; 4]).unwrap();
    // This is an orphan from the interrupted vector generation. The marker
    // has no deletion path or session scope, so only live-ID reconciliation
    // can remove it after lexical publication has already completed.
    vectors.add(99, &[0.0; 4]).unwrap();
    vectors.save().unwrap();

    let recovery_lease = ingest_lease(&paths);
    IngestState {
        next_doc_id: 3,
        ..IngestState::default()
    }
    .save_with_lease(&paths.state.join("ingest.json"), &recovery_lease)
    .unwrap();
    PendingIngest {
        next_doc_id: 3,
        source_paths: Vec::new(),
        vector_delete_paths: Vec::new(),
        session_scopes: Vec::new(),
        vector_publication: true,
        embedding_publication: Some(false),
    }
    .save_with_lease(&pending_ingest_path(&paths), &recovery_lease)
    .unwrap();
    drop(recovery_lease);

    // A cleanup-only recovery must be able to use the existing vector
    // store even when the configured embedding model is unavailable.
    let options = ingest_options(false, ModelChoice::Gemma);
    ingest_all(
        &paths,
        &open_search_index(&paths),
        &options,
        &ingest_lease(&paths),
    )
    .unwrap();

    assert_eq!(
        VectorIndex::inventory(&paths.vectors)
            .unwrap()
            .unwrap()
            .doc_ids,
        HashSet::from([1, 2])
    );
    assert_eq!(
        PendingIngest::load(&pending_ingest_path(&paths)).unwrap(),
        None
    );
}

#[test]
fn rebuilding_empty_index_replaces_stale_catalog_rows() {
    for has_transcript in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let source_dir = temp.path().join("transcripts");
        fs::create_dir_all(&source_dir).unwrap();
        let transcript = source_dir.join("archived.jsonl");
        if has_transcript {
            fs::write(&transcript, br#"{"type":"user","sessionId":"session","message":{"role":"user","content":[{"type":"text","text":"Actual request"}]},"uuid":"u1","timestamp":"2024-01-01T00:00:00Z"}
"#).unwrap();
        }
        let db = analytics_path(&paths.state);
        let mut old = AnalyticsWriter::open(&db).unwrap();
        for path in [temp.path().join("old-active.jsonl"), transcript.clone()] {
            let mut stale = record(1, "user", "Previous request");
            stale.source = SourceKind::Claude;
            stale.session_id = "session".into();
            stale.source_path = path.to_string_lossy().into_owned();
            old.record(&stale).unwrap();
        }
        old.flush().unwrap();
        AnalyticsStore::open(&db).unwrap().mark_complete().unwrap();
        // The old catalog remains even if ingest state was lost as well.
        let index = open_search_index(&paths);
        let mut options = ingest_options(false, ModelChoice::default());
        options.claude_sources = vec![source_dir];
        ingest_all(&paths, &index, &options, &ingest_lease(&paths)).unwrap();
        let store = AnalyticsStore::open_read_only(&db).unwrap();
        let rows = store
            .query_sessions_detailed(None, None, None, None, None)
            .unwrap();
        assert_eq!(rows.len(), usize::from(has_transcript));
        if has_transcript {
            assert_eq!(rows[0].source_path, transcript.to_string_lossy());
            assert_eq!(rows[0].message_count, 1);
            assert_eq!(rows[0].label.as_deref(), Some("Actual request"));
        }
        assert!(store.complete().unwrap());
    }
}

#[test]
fn reader_identity_counter_survives_ingest_state_and_missing_counter_reparses() {
    use crate::retrieval::canonical_record_id;
    use serde_json::json;
    use std::io::Write;
    for source in [SourceKind::Codex, SourceKind::Claude] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let (added, question, answer) = if source == SourceKind::Codex {
            (
                json!({"type":"event_msg", "payload":{"type":"task_started", "turn_id":"turn"}}),
                json!({"type":"response_item", "payload":{"type":"message", "role":"user", "content":"Question"}}),
                json!({"type":"response_item", "payload":{"type":"message", "role":"assistant", "content":"Answer"}}),
            )
        } else {
            (
                json!({"type":"user", "message":{"content":[{"type":"image", "source":{"type":"url", "url":"https://example.com/image.png"}}]}}),
                json!({"type":"user", "message":{"content":"Question"}}),
                json!({"type":"assistant", "message":{"content":"Answer"}}),
            )
        };
        fs::write(&path, format!("{added}\n{question}\n")).unwrap();
        let progress = Arc::new(Progress::new([0; SOURCE_COUNT], [0; SOURCE_COUNT], false));
        let next_doc_id = AtomicU64::new(1);
        let (tx_record, rx_record, tx_update, rx_update) = parser_channels();
        let parse = |task: &FileTask| match source {
            SourceKind::Codex => {
                parse_codex_session(task, false, &tx_record, &tx_update, &next_doc_id, &progress)
            }
            SourceKind::Claude => {
                parse_claude_file(task, false, &tx_record, &tx_update, &next_doc_id, &progress)
            }
            _ => unreachable!(),
        };
        let metadata = path.metadata().unwrap();
        let (first, _) = discovery::prepare_file_task(path.clone(), source, false, &metadata, None);
        parse(&first).unwrap();
        let mut incremental = rx_record.try_iter().collect::<Vec<_>>();
        let state = rx_update.try_recv().unwrap().state;
        assert_eq!(state.turn_id, 2);
        assert_eq!(state.legacy_turn_id, Some(1));
        let serialized = serde_json::to_value(state).unwrap();
        let persisted: FileState = serde_json::from_value(serialized.clone()).unwrap();
        let mut missing_counter = serialized;
        missing_counter
            .as_object_mut()
            .unwrap()
            .remove("legacy_turn_id");
        let missing_counter: FileState = serde_json::from_value(missing_counter).unwrap();
        let (rebuild, skip) = discovery::prepare_file_task(
            path.clone(),
            source,
            false,
            &metadata,
            Some(&missing_counter),
        );
        assert!(!skip);
        assert!(rebuild.delete_first() && rebuild.parser_version_invalidated());
        assert_eq!(rebuild.offset, 0);
        assert_eq!(rebuild.legacy_turn_id, Some(0));

        writeln!(
            fs::OpenOptions::new().append(true).open(&path).unwrap(),
            "{answer}"
        )
        .unwrap();
        let (resumed, _) = discovery::prepare_file_task(
            path.clone(),
            source,
            false,
            &path.metadata().unwrap(),
            Some(&persisted),
        );
        assert!(!resumed.delete_first());
        assert_eq!(resumed.legacy_turn_id, Some(1));
        parse(&resumed).unwrap();
        incremental.extend(rx_record.try_iter());
        let state = rx_update.try_recv().unwrap().state;
        assert_eq!(state.legacy_turn_id, Some(2));
        assert_eq!(incremental.last().unwrap().links.legacy_turn_id, Some(1));
        let (full, _) = discovery::prepare_file_task(
            path.clone(),
            source,
            false,
            &path.metadata().unwrap(),
            None,
        );
        parse(&full).unwrap();
        let full = rx_record.try_iter().collect::<Vec<_>>();
        assert_eq!(
            incremental
                .iter()
                .map(canonical_record_id)
                .collect::<Vec<_>>(),
            full.iter().map(canonical_record_id).collect::<Vec<_>>()
        );
    }
}

#[test]
fn checkpoint_session_keeps_unloaded_absent_and_deleted_paths_distinct() {
    let temp = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let lease = ingest_lease(&paths);
    let state_path = paths.state.join("ingest.json");
    let file: FileState = serde_json::from_value(serde_json::json!({
        "size": 12, "mtime": -1, "offset": 12, "turn_id": 2
    }))
    .unwrap();
    let initial = IngestState {
        next_doc_id: 20,
        files: HashMap::from([
            ("known".to_string(), file.clone()),
            ("unloaded".to_string(), file.clone()),
        ]),
        ..IngestState::default()
    };
    initial.save_with_lease(&state_path, &lease).unwrap();
    let mut state = CheckpointSession::open(&state_path, &lease, false, None).unwrap();
    state
        .preload(&["known".to_string(), "absent".to_string()])
        .unwrap();
    assert_eq!(state.loaded.len(), 2);
    assert!(state.file("known").is_some());
    assert!(state.file("absent").is_none());
    assert!(!state.loaded.contains_key("unloaded"));
    assert!(state.contains_file("unloaded").unwrap());
    assert!(!state.loaded.contains_key("unloaded"));
    state.upsert_file("known".to_string(), file);
    assert!(!state.commit_final(None, PendingChange::Keep).unwrap());
    state.delete_file("unloaded");
    state.preload(&["unloaded".to_string()]).unwrap();
    assert!(state.file("unloaded").is_none());
    let pending = PendingIngest {
        next_doc_id: 99,
        source_paths: vec!["known".to_string()],
        session_scopes: Vec::new(),
        vector_delete_paths: Vec::new(),
        vector_publication: false,
        embedding_publication: Some(false),
    };
    state.next_doc_id = 99;
    state.commit_intent(&pending).unwrap();
    assert_eq!(
        PendingIngest::load(&pending_ingest_path(&paths)).unwrap(),
        Some(pending)
    );
    assert_eq!(IngestState::load(&state_path).unwrap().next_doc_id, 20);
    assert!(
        IngestState::load(&state_path)
            .unwrap()
            .files
            .contains_key("unloaded")
    );
    assert!(state.commit_final(None, PendingChange::Keep).unwrap());
    assert!(!state.commit_final(None, PendingChange::Keep).unwrap());
    let saved = IngestState::load(&state_path).unwrap();
    assert_eq!(saved.files.len(), 1);
    assert_eq!(saved.files["known"], initial.files["known"]);
}

#[test]
fn full_single_source_reads_and_writes_only_discovered_checkpoint() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("claude");
    fs::create_dir_all(&source).unwrap();
    let transcript = source.join("session.jsonl");
    append_claude_message(&transcript, "original");
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let lease = ingest_lease(&paths);
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![source];
    ingest_all(&paths, &index, &options, &lease).unwrap();
    let state_path = paths.state.join("ingest.json");
    let mut original = IngestState::load(&state_path).unwrap();
    let key = transcript.to_string_lossy().into_owned();
    let file = original.files[&key].clone();
    for number in 0..2000 {
        original
            .files
            .insert(format!("/unrelated/{number}"), file.clone());
    }
    original.save_with_lease(&state_path, &lease).unwrap();
    let database = rusqlite::Connection::open(paths.state.join("checkpoints.sqlite")).unwrap();
    database.execute_batch("CREATE TABLE changed_paths (path TEXT);
        CREATE TRIGGER audit_file_update AFTER UPDATE ON files BEGIN INSERT INTO changed_paths VALUES (NEW.path); END;").unwrap();
    append_claude_message(&transcript, "appended");
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let recovered = recover_checkpoint(&paths, &index, &lease, None).unwrap();
    assert!(recovered.state.loaded.is_empty());
    let pool = parser_thread_pool().unwrap();
    let prepared = prepare_refresh(
        &paths,
        &index,
        &options,
        &pool,
        recovered,
        Narrowing::None,
        None,
    )
    .unwrap();
    assert_eq!(prepared.state.loaded.len(), 1);
    assert!(prepared.state.loaded.contains_key(&key));
    let report = execute_refresh(
        prepared,
        &paths,
        &index,
        &options,
        Arc::new(crate::repository::RepositoryResolver::default()),
        &pool,
    )
    .unwrap();
    assert_eq!(report.records_added, 1);
    let changed: Vec<String> = database
        .prepare("SELECT path FROM changed_paths")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(changed, vec![key.clone()]);
    let saved = IngestState::load(&state_path).unwrap();
    assert_eq!(saved.files.len(), 2001);
    for (path, file) in original.files.iter().filter(|(path, _)| *path != &key) {
        assert_eq!(&saved.files[path], file);
    }
    let cache_path = paths.state.join("scan_cache.json");
    let mut expired_cache = ScanCache::load(&cache_path).unwrap();
    expired_cache.last_scan_ts = 1;
    expired_cache.save_with_lease(&cache_path, &lease).unwrap();
    let version: i64 = database
        .query_row("PRAGMA data_version", [], |row| row.get(0))
        .unwrap();
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    assert_eq!(
        ingest_all(&paths, &index, &options, &lease)
            .unwrap()
            .records_added,
        0
    );
    let after: i64 = database
        .query_row("PRAGMA data_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(after, version + 1, "full no-op commits only the scan cache");
    let refreshed_cache = ScanCache::load(&cache_path).unwrap();
    assert!(refreshed_cache.is_fresh(3600));
    refreshed_cache
        .save_with_lease(&cache_path, &lease)
        .unwrap();
    assert_eq!(
        database
            .query_row("PRAGMA data_version", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        after,
        "saving the same cache must not change checkpoint rows"
    );
    let changed_rows: usize = database
        .query_row("SELECT count(*) FROM changed_paths", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        changed_rows, 1,
        "full no-op must not rewrite file checkpoints"
    );
}

#[test]
fn checkpoint_failure_after_publication_keeps_pending_recoverable() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("claude");
    fs::create_dir_all(&source).unwrap();
    let transcript = source.join("session.jsonl");
    append_claude_message(&transcript, "original");
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let lease = ingest_lease(&paths);
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![source];
    ingest_all(&paths, &index, &options, &lease).unwrap();
    let state_path = paths.state.join("ingest.json");
    let before = IngestState::load(&state_path).unwrap();
    let cache_path = paths.state.join("scan_cache.json");
    ScanCache {
        last_scan_ts: 1,
        file_count: 7,
        total_bytes: 42,
    }
    .save_with_lease(&cache_path, &lease)
    .unwrap();
    let cache_before = serde_json::to_value(ScanCache::load(&cache_path).unwrap()).unwrap();
    let database = rusqlite::Connection::open(paths.state.join("checkpoints.sqlite")).unwrap();
    database.execute_batch("CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF scancache_json ON metadata BEGIN SELECT RAISE(FAIL, 'checkpoint failure'); END;").unwrap();
    append_claude_message(&transcript, "appended");
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let error = ingest_all(&paths, &index, &options, &lease).unwrap_err();
    assert!(format!("{error:#}").contains("checkpoint failure"));
    assert!(
        PendingIngest::load(&pending_ingest_path(&paths))
            .unwrap()
            .is_some()
    );
    let saved = IngestState::load(&state_path).unwrap();
    assert_eq!(saved.files, before.files);
    assert_eq!(saved.next_doc_id, before.next_doc_id);
    assert_eq!(saved.opencode_databases, before.opencode_databases);
    assert_eq!(
        serde_json::to_value(ScanCache::load(&cache_path).unwrap()).unwrap(),
        cache_before
    );
    assert!(
        !pending_ingest_path(&paths).exists(),
        "intent must be durable in SQLite"
    );
    assert_eq!(indexed_texts(&paths), ["appended", "original"]);
    database
        .execute_batch("DROP TRIGGER fail_checkpoint;")
        .unwrap();
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    ingest_all(&paths, &index, &options, &lease).unwrap();
    assert_eq!(indexed_texts(&paths), ["appended", "original"]);
    assert!(
        PendingIngest::load(&pending_ingest_path(&paths))
            .unwrap()
            .is_none()
    );
}

#[test]
fn early_intent_failure_cancels_publication_without_flushing_recovery_changes() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("claude");
    fs::create_dir_all(&source).unwrap();
    let transcript = source.join("session.jsonl");
    append_claude_message(&transcript, "original");
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let lease = ingest_lease(&paths);
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![source];
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    ingest_all(&paths, &index, &options, &lease).unwrap();
    let state_path = paths.state.join("ingest.json");
    let before = IngestState::load(&state_path).unwrap();
    let key = transcript.to_string_lossy().into_owned();
    let pending = PendingIngest {
        next_doc_id: 100,
        source_paths: vec![key.clone()],
        session_scopes: Vec::new(),
        vector_delete_paths: Vec::new(),
        vector_publication: false,
        embedding_publication: Some(false),
    };
    pending
        .save_with_lease(&pending_ingest_path(&paths), &lease)
        .unwrap();
    let cache_before =
        serde_json::to_value(ScanCache::load(&paths.state.join("scan_cache.json")).unwrap())
            .unwrap();
    append_claude_message(&transcript, "appended");
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    let recovered = recover_checkpoint(&paths, &index, &lease, None).unwrap();
    assert_eq!(recovered.state.next_doc_id, 100);
    assert!(!recovered.state.contains_file(&key).unwrap());
    let pool = parser_thread_pool().unwrap();
    let mut prepared = prepare_refresh(
        &paths,
        &index,
        &options,
        &pool,
        recovered,
        Narrowing::None,
        None,
    )
    .unwrap();
    prepared
        .state
        .upsert_file("unpublished".to_string(), before.files[&key].clone());
    prepared
        .state
        .opencode_databases
        .insert("unpublished.db".to_string(), Default::default());
    let database = rusqlite::Connection::open(paths.state.join("checkpoints.sqlite")).unwrap();
    database.execute_batch("CREATE TRIGGER fail_intent BEFORE UPDATE OF pending_json ON metadata BEGIN SELECT RAISE(FAIL, 'intent failure'); END;").unwrap();
    let error = execute_refresh(
        prepared,
        &paths,
        &index,
        &options,
        Arc::new(crate::repository::RepositoryResolver::default()),
        &pool,
    )
    .unwrap_err();
    assert!(format!("{error:#}").contains("intent failure"));
    let saved = IngestState::load(&state_path).unwrap();
    assert_eq!(saved.files, before.files);
    assert_eq!(saved.next_doc_id, before.next_doc_id);
    assert_eq!(saved.opencode_databases, before.opencode_databases);
    assert_eq!(
        PendingIngest::load(&pending_ingest_path(&paths)).unwrap(),
        Some(pending)
    );
    assert_eq!(
        serde_json::to_value(ScanCache::load(&paths.state.join("scan_cache.json")).unwrap())
            .unwrap(),
        cache_before
    );
    assert_eq!(indexed_texts(&paths), ["original"]);
    let analytics = AnalyticsStore::open(analytics_path(&paths.state)).unwrap();
    let sessions = analytics
        .query_sessions_detailed(None, None, None, None, None)
        .unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].message_count, 1);
    drop(
        index
            .writer()
            .expect("cancelled writer must be joined and released"),
    );
    database.execute_batch("DROP TRIGGER fail_intent").unwrap();
    let index = SearchIndex::open_or_create_for_continuous_ingest(&paths.index).unwrap();
    ingest_all(&paths, &index, &options, &lease).unwrap();
    assert_eq!(indexed_texts(&paths), ["appended", "original"]);
}

#[test]
fn missing_checkpoint_requires_complete_pending_record_coverage() {
    for (targets, next_doc_id, succeeds) in [
        (vec!["source-1.jsonl".to_string()], 2, true),
        (vec!["source-1.jsonl".to_string()], 1, false),
        (vec!["unrelated.jsonl".to_string()], 2, false),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
        paths.ensure_dirs().unwrap();
        let index = save_search_records(&paths, &[record(1, "user", "interrupted")]);
        PendingIngest {
            next_doc_id,
            source_paths: targets,
            vector_delete_paths: Vec::new(),
            session_scopes: Vec::new(),
            vector_publication: false,
            embedding_publication: Some(false),
        }
        .save(&pending_ingest_path(&paths))
        .unwrap();
        let lease = ingest_lease(&paths);
        let recovered = recover_checkpoint(&paths, &index, &lease, None);
        assert_eq!(recovered.is_ok(), succeeds);
        assert!(
            PendingIngest::load(&pending_ingest_path(&paths))
                .unwrap()
                .is_some()
        );
        if let Ok(recovered) = recovered {
            assert_eq!(recovered.state.next_doc_id, 2);
            assert_eq!(
                IngestState::load(&paths.state.join("ingest.json"))
                    .unwrap()
                    .next_doc_id,
                1
            );
        } else {
            assert!(!paths.state.join("ingest.json").exists());
        }
    }
}

fn can_skip_fresh_scan(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    ttl_seconds: u64,
) -> Result<bool> {
    let header = CheckpointReader::open(&paths.state.join("ingest.json"))?.header()?;
    super::can_skip_fresh_scan(&header, paths, index, options, ttl_seconds)
}

#[cfg(target_os = "macos")]
#[test]
fn journal_refreshes_narrow_to_changed_paths_and_walk_after_a_directory_rename() {
    use std::io::Write;

    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap().join("claude-projects");
    let project = root.join("project");
    fs::create_dir_all(&project).unwrap();
    let line =
        |text: &str| format!("{{\"type\":\"user\",\"message\":{{\"content\":\"{text}\"}}}}\n");
    fs::write(project.join("first.jsonl"), line("alpha")).unwrap();
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let lease = ingest_lease(&paths);
    let mut options = ingest_options(false, ModelChoice::Gemma);
    options.claude_sources = vec![root.clone()];
    let settle = || std::thread::sleep(Duration::from_millis(150));
    let refresh = || {
        let journal = discovery::start_journal_replay(&paths, &options);
        journal.wait_until_streaming(journal::REPLAY_BUDGET);
        let index = open_search_index(&paths);
        ingest_selected(&paths, &index, &options, &lease, None, None, Some(journal)).unwrap()
    };
    let indexed = || {
        let index = open_search_index(&paths);
        let mut texts = Vec::new();
        index
            .for_each_record(|record| {
                texts.push(record.text.clone());
                Ok(())
            })
            .unwrap();
        texts.sort();
        texts
    };

    let first = refresh();
    assert!(first.full_scan, "the first refresh has no cursor and walks");
    assert_eq!(first.report.records_added, 1);
    settle();

    let mut appended = fs::OpenOptions::new()
        .append(true)
        .open(project.join("first.jsonl"))
        .unwrap();
    appended.write_all(line("beta").as_bytes()).unwrap();
    drop(appended);
    fs::write(project.join("second.jsonl"), line("gamma")).unwrap();
    settle();
    // fseventsd occasionally holds a replay past the budget, which legitimately falls back
    // to a walk; a narrowed refresh must arrive within a few attempts.
    let mut added = 0;
    let mut narrowed = false;
    for _ in 0..5 {
        let refreshed = refresh();
        added += refreshed.report.records_added;
        narrowed = !refreshed.full_scan;
        settle();
        if narrowed {
            break;
        }
    }
    assert!(narrowed, "a journaled interval narrows the refresh");
    assert_eq!(added, 2);
    assert_eq!(indexed(), vec!["alpha", "beta", "gamma"]);

    fs::rename(&project, root.join("renamed")).unwrap();
    settle();
    let fourth = refresh();
    assert!(fourth.full_scan, "a renamed directory forces a walk");
    assert_eq!(fourth.report.files_scanned, 2);
    assert_eq!(fourth.report.records_added, 3);
}

#[test]
fn full_scan_preserves_the_cursor_captured_before_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(temp.path().join("memex"))).unwrap();
    paths.ensure_dirs().unwrap();
    let lease = ingest_lease(&paths);
    let index = open_search_index(&paths);
    let options = ingest_options(false, ModelChoice::Gemma);
    let mut recovered = recover_checkpoint(&paths, &index, &lease, None).unwrap();
    let cursor = journal::JournalCursorUpdate {
        fingerprint: "captured-before-fallback".into(),
        cursor: journal::JournalCursor {
            device_uuid: "test-volume".into(),
            event_id: 42,
        },
    };
    recovered.state.journal_cursor = Some(cursor.clone());
    let prepared = prepare_refresh(
        &paths,
        &index,
        &options,
        &parser_thread_pool().unwrap(),
        recovered,
        Narrowing::None,
        None,
    )
    .unwrap();
    assert_eq!(prepared.state.journal_cursor, Some(cursor));
}
