use super::snapshot::lock_partitions;
use super::{UsageQuery, UsageReport, scan_usage};
use crate::test_support::{EnvVarGuard, env_lock};
use crate::types::SourceFilter;
use rusqlite::Connection;
use std::fs;
use std::time::{Duration, Instant};

fn fact_event(path: &str, record: &str) -> super::UsageEvent {
    super::UsageEvent {
        source: "cursor",
        source_path: path.into(),
        source_record_id: Some(record.into()),
        session_id: None,
        request_id: None,
        message_id: None,
        timestamp_ms: 1000,
        project: None,
        provider: None,
        model: None,
        tokens: super::TokenBuckets::disjoint(10, 0, 0, 5),
        source_cost_usd: None,
        cost_authoritative: false,
        dedupe_confidence: "exact",
        conservative_undercount: false,
        cache_chain_excluded: false,
        sidechain: false,
        permission_review: false,
        source_order: 0,
    }
}

fn stored_fact_events(cache: &super::cache::UsageCache) -> Vec<super::UsageEvent> {
    super::facts::read_ordinal_run(
        &cache.connection,
        super::scan::source_ordinal(SourceFilter::Cursor) as i64,
        None,
        None,
    )
    .unwrap()
    .into_iter()
    .map(super::facts::FactRow::into_event)
    .collect()
}

#[test]
fn canonical_delta_preserves_untouched_files_and_removes_empty_contributions() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = super::cache::UsageCache::open(&temp.path().join("cache.sqlite3")).unwrap();
    let kept = fact_event("kept", "same");
    let removed = fact_event("removed", "old");
    cache
        .replace_partition_facts(SourceFilter::Cursor, &[kept.clone(), removed], &[], &[])
        .unwrap();
    cache
        .connection
        .execute_batch(
            "CREATE TRIGGER protect_kept_delete BEFORE DELETE ON usage_facts
         WHEN OLD.path = 'kept' BEGIN SELECT RAISE(ABORT, 'untouched file deleted'); END;
         CREATE TRIGGER protect_kept_insert BEFORE INSERT ON usage_facts
         WHEN NEW.path = 'kept' BEGIN SELECT RAISE(ABORT, 'untouched file inserted'); END;",
        )
        .unwrap();
    cache
        .replace_partition_facts(SourceFilter::Cursor, &[kept], &[], &[])
        .unwrap();
    let events = stored_fact_events(&cache);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].source_path.as_ref(), "kept");
    cache
        .connection
        .execute_batch("DROP TRIGGER protect_kept_delete; DROP TRIGGER protect_kept_insert;")
        .unwrap();
    cache
        .replace_partition_facts(SourceFilter::Cursor, &[], &[], &[])
        .unwrap();
    assert!(stored_fact_events(&cache).is_empty());
}

#[test]
fn canonical_delta_persists_equal_key_order_and_internal_fields() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = super::cache::UsageCache::open(&temp.path().join("cache.sqlite3")).unwrap();
    let first = fact_event("file", "first");
    let mut second = fact_event("file", "second");
    cache
        .replace_partition_facts(
            SourceFilter::Cursor,
            &[first.clone(), second.clone()],
            &[],
            &[],
        )
        .unwrap();
    cache
        .replace_partition_facts(
            SourceFilter::Cursor,
            &[second.clone(), first.clone()],
            &[],
            &[],
        )
        .unwrap();
    let events = stored_fact_events(&cache);
    assert_eq!(events[0].source_record_id.as_deref(), Some("second"));
    assert_eq!(events[1].source_record_id.as_deref(), Some("first"));
    second.cache_chain_excluded = true;
    second.permission_review = true;
    cache
        .replace_partition_facts(SourceFilter::Cursor, &[second, first], &[], &[])
        .unwrap();
    let events = stored_fact_events(&cache);
    assert!(events[0].cache_chain_excluded);
    assert!(events[0].permission_review);
}

#[test]
fn canonical_delta_recovers_missing_digests_and_incremental_writes() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = super::cache::UsageCache::open(&temp.path().join("cache.sqlite3")).unwrap();
    let original = fact_event("file", "original");
    cache
        .replace_partition_facts(
            SourceFilter::Cursor,
            &[original.clone(), fact_event("orphan", "removed")],
            &[],
            &[],
        )
        .unwrap();
    // Simulate a pre-digest database: existing facts are still authoritative
    // for detecting removed paths, even without any hash metadata.
    cache
        .connection
        .execute("DELETE FROM usage_fact_files", [])
        .unwrap();
    cache
        .replace_partition_facts(
            SourceFilter::Cursor,
            std::slice::from_ref(&original),
            &[],
            &[],
        )
        .unwrap();
    assert_eq!(stored_fact_events(&cache).len(), 1);
    cache
        .upsert_file_facts(
            SourceFilter::Cursor,
            &["file".into()],
            &[fact_event("file", "incremental")],
            &[],
            &[],
        )
        .unwrap();
    assert_eq!(
        stored_fact_events(&cache)[0].source_record_id.as_deref(),
        Some("incremental")
    );
    // Returning to the original contribution must not match a stale digest
    // left over from before the incremental write.
    cache
        .replace_partition_facts(SourceFilter::Cursor, &[original], &[], &[])
        .unwrap();
    assert_eq!(
        stored_fact_events(&cache)[0].source_record_id.as_deref(),
        Some("original")
    );
}

#[test]
fn canonical_delta_rolls_back_facts_digests_and_generation_on_failure() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = super::cache::UsageCache::open(&temp.path().join("cache.sqlite3")).unwrap();
    cache
        .replace_partition_facts(SourceFilter::Cursor, &[fact_event("file", "old")], &[], &[])
        .unwrap();
    let generation = cache.fact_generation("cursor").unwrap();
    let digest = |cache: &super::cache::UsageCache| -> Vec<u8> {
        cache
            .connection
            .query_row(
                "SELECT digest FROM usage_fact_files WHERE source = 'cursor' AND path = 'file'",
                [],
                |row| row.get(0),
            )
            .unwrap()
    };
    let old_digest = digest(&cache);
    cache
        .connection
        .execute_batch(
            "CREATE TRIGGER fail_sync BEFORE UPDATE ON usage_fact_sync
         BEGIN SELECT RAISE(ABORT, 'simulated commit failure'); END;",
        )
        .unwrap();
    let replacement = fact_event("file", "new");
    assert!(
        cache
            .replace_partition_facts(
                SourceFilter::Cursor,
                std::slice::from_ref(&replacement),
                &[],
                &[],
            )
            .is_err()
    );
    assert_eq!(
        stored_fact_events(&cache)[0].source_record_id.as_deref(),
        Some("old")
    );
    assert_eq!(digest(&cache), old_digest);
    assert_eq!(cache.fact_generation("cursor").unwrap(), generation);
    cache
        .connection
        .execute_batch("DROP TRIGGER fail_sync")
        .unwrap();
    cache
        .replace_partition_facts(SourceFilter::Cursor, &[replacement], &[], &[])
        .unwrap();
    assert_eq!(
        stored_fact_events(&cache)[0].source_record_id.as_deref(),
        Some("new")
    );
    assert_ne!(digest(&cache), old_digest);
    assert_ne!(cache.fact_generation("cursor").unwrap(), generation);
}

fn expire(query: &UsageQuery) {
    lock_partitions()
        .get_mut(&(query.source.unwrap(), query.cache_path.clone()))
        .expect("retained partition")
        .checked_at = Instant::now() - Duration::from_secs(120);
}

fn assert_report_matches(actual: &UsageReport, expected: &UsageReport) {
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
}

fn claude_line(message: &str, tokens: u64) -> String {
    serde_json::json!({
        "type": "assistant", "sessionId": "session", "requestId": "request",
        "timestamp": 1000, "cwd": "/repo/memex",
        "message": { "id": message, "model": "claude-sonnet-4-6", "usage": { "inputTokens": tokens } }
    }).to_string() + "\n"
}

fn claude_query(temp: &tempfile::TempDir) -> UsageQuery {
    UsageQuery {
        source: Some(SourceFilter::Claude),
        cache_path: Some(temp.path().join("cache.sqlite3")),
        include_events: true,
        memo_ttl_ms: 60_000,
        ..UsageQuery::default()
    }
}

#[test]
fn failed_cold_validation_rebuilds_without_blessing_a_later_mutation() {
    use super::cache::UsageCache;
    use super::snapshot::{populate_after_failed_validation, validate_partition};

    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let projects = temp.path().join("projects/memex");
    fs::create_dir_all(&projects).unwrap();
    let transcript = projects.join("session.jsonl");
    fs::write(&transcript, claude_line("old", 10)).unwrap();
    let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(temp.path().as_os_str()))]);
    let query = claude_query(&temp);
    let path = query.cache_path.as_deref().unwrap();
    let key = (SourceFilter::Claude, query.cache_path.clone());
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 10);
    lock_partitions().remove(&key);
    fs::write(&transcript, claude_line("changed", 20)).unwrap();
    let observed = validate_partition(SourceFilter::Claude, Some(path), None).unwrap();
    assert!(!observed.valid);
    // The supplied checkpoint predates this second mutation. The rebuilt
    // assembly can answer, but its disk facts must not be marked synchronized.
    fs::write(&transcript, claude_line("later", 300)).unwrap();
    populate_after_failed_validation(SourceFilter::Claude, path, observed.fingerprint);
    assert!(lock_partitions().contains_key(&key));
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 300);
    assert!(
        UsageCache::open(path)
            .unwrap()
            .fact_sync("claude")
            .unwrap()
            .is_none()
    );
    expire(&query);
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 300);
    assert!(
        validate_partition(SourceFilter::Claude, Some(path), None)
            .unwrap()
            .valid
    );

    // Exercise the actual cold query entry point with a stable changed source.
    lock_partitions().remove(&key);
    fs::write(&transcript, claude_line("final", 400)).unwrap();
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 400);
    assert!(
        validate_partition(SourceFilter::Claude, Some(path), None)
            .unwrap()
            .valid
    );
}

#[test]
fn failed_cold_validation_does_not_replace_a_snapshot_or_wait_for_refresh() {
    use super::snapshot::{
        USAGE_SCAN_LOCK, discovery_fingerprint, populate_after_failed_validation,
    };

    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let projects = temp.path().join("projects/memex");
    fs::create_dir_all(&projects).unwrap();
    let transcript = projects.join("session.jsonl");
    fs::write(&transcript, claude_line("old", 10)).unwrap();
    let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(temp.path().as_os_str()))]);
    let query = claude_query(&temp);
    let path = query.cache_path.as_deref().unwrap();
    let key = (SourceFilter::Claude, query.cache_path.clone());
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 10);
    let original = lock_partitions().get(&key).unwrap().assembly.clone();
    fs::write(&transcript, claude_line("new", 200)).unwrap();
    populate_after_failed_validation(
        SourceFilter::Claude,
        path,
        discovery_fingerprint(SourceFilter::Claude),
    );
    assert!(std::sync::Arc::ptr_eq(
        &original,
        &lock_partitions().get(&key).unwrap().assembly
    ));
    lock_partitions().remove(&key);
    let _refresh = USAGE_SCAN_LOCK.lock().unwrap();
    populate_after_failed_validation(
        SourceFilter::Claude,
        path,
        discovery_fingerprint(SourceFilter::Claude),
    );
    assert!(!lock_partitions().contains_key(&key));
}

#[test]
fn rewriting_a_winner_recovers_unchanged_suppressed_occurrences() {
    let _guard = env_lock();
    for (replacement, expected) in [
        (claude_line("shared", 5), 10),
        (claude_line("different", 3), 13),
        (String::new(), 10),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let projects = temp.path().join("projects/memex");
        fs::create_dir_all(&projects).unwrap();
        fs::write(projects.join("loser.jsonl"), claude_line("shared", 10)).unwrap();
        let winner = projects.join("winner.jsonl");
        fs::write(&winner, claude_line("shared", 70)).unwrap();
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(temp.path().as_os_str()))]);
        let query = claude_query(&temp);
        assert_eq!(scan_usage(&query).unwrap().total_tokens, 70);
        fs::write(&winner, replacement).unwrap();
        expire(&query);
        let updated = scan_usage(&query).unwrap();
        assert_eq!(updated.total_tokens, expected);
        let fresh = scan_usage(&UsageQuery {
            cache_path: Some(temp.path().join("fresh.sqlite3")),
            memo_ttl_ms: 0,
            ..query.clone()
        })
        .unwrap();
        assert_report_matches(&updated, &fresh);
        lock_partitions().remove(&(SourceFilter::Claude, query.cache_path.clone()));
        assert_report_matches(&scan_usage(&query).unwrap(), &fresh);
    }
}

#[test]
fn interrupted_blob_only_refresh_cannot_validate_old_facts() {
    use super::cache::UsageCache;
    use super::scan::{run_partition_scanner, scan_claude};

    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let projects = temp.path().join("projects/memex");
    fs::create_dir_all(&projects).unwrap();
    let transcript = projects.join("session.jsonl");
    fs::write(&transcript, claude_line("old", 10)).unwrap();
    let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(temp.path().as_os_str()))]);
    let query = claude_query(&temp);
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 10);
    fs::write(
        &transcript,
        claude_line("old", 10) + &claude_line("new", 70),
    )
    .unwrap();

    // Stop at the exact interruption boundary: blob chunks committed, canonical
    // facts not written. A new connection must see invalid synchronization.
    let mut cache = UsageCache::open(query.cache_path.as_deref().unwrap()).unwrap();
    let mut warnings = Vec::new();
    let events = run_partition_scanner(
        SourceFilter::Claude,
        scan_claude,
        &mut warnings,
        Some(&mut cache),
    );
    assert!(warnings.is_empty());
    assert_eq!(
        events.iter().map(|event| event.tokens.total()).sum::<u64>(),
        80
    );
    drop(cache);
    let cache = UsageCache::open(query.cache_path.as_deref().unwrap()).unwrap();
    assert!(cache.fact_sync("claude").unwrap().is_none());
    drop(cache);
    expire(&query);
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 80);
    lock_partitions().remove(&(SourceFilter::Claude, query.cache_path.clone()));
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 80);
}

#[test]
fn failed_facts_commit_is_retried_from_raw_cached_occurrences() {
    use super::cache::UsageCache;

    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let projects = temp.path().join("projects/memex");
    fs::create_dir_all(&projects).unwrap();
    let transcript = projects.join("session.jsonl");
    fs::write(&transcript, claude_line("old", 10)).unwrap();
    let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(temp.path().as_os_str()))]);
    let query = claude_query(&temp);
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 10);
    let connection = Connection::open(query.cache_path.as_deref().unwrap()).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fail_facts_insert BEFORE INSERT ON usage_facts
         BEGIN SELECT RAISE(ABORT, 'simulated facts write failure'); END;",
        )
        .unwrap();
    fs::write(&transcript, claude_line("new", 80)).unwrap();
    expire(&query);
    let fallback = scan_usage(&query).unwrap();
    assert_eq!(fallback.total_tokens, 80);
    assert!(
        fallback
            .warnings
            .iter()
            .any(|warning| warning.contains("simulated facts write failure"))
    );
    assert!(
        UsageCache::open(query.cache_path.as_deref().unwrap())
            .unwrap()
            .fact_sync("claude")
            .unwrap()
            .is_none()
    );
    connection
        .execute_batch("DROP TRIGGER fail_facts_insert;")
        .unwrap();
    expire(&query);
    let healed = scan_usage(&query).unwrap();
    assert_eq!(healed.total_tokens, 80);
    assert!(healed.warnings.is_empty());
    lock_partitions().remove(&(SourceFilter::Claude, query.cache_path.clone()));
    assert_report_matches(&scan_usage(&query).unwrap(), &healed);
}

#[test]
fn unresolved_codex_facts_reuse_until_a_parent_appears() {
    use super::cache::UsageCache;

    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let sessions = temp.path().join("sessions");
    fs::create_dir_all(&sessions).unwrap();
    let child_id = "019f0000-0000-7000-8000-000000000002";
    let parent_id = "019f0000-0000-7000-8000-000000000001";
    let token_line = |timestamp: &str, last: u64, total: u64| {
        serde_json::json!({"type":"event_msg", "timestamp":timestamp, "payload":{
            "type":"token_count", "info":{
                "last_token_usage":{"input_tokens":last},
                "total_token_usage":{"input_tokens":total}
            }
        }})
        .to_string()
            + "\n"
    };
    let child = serde_json::json!({"type":"session_meta", "timestamp":"2026-07-15T09:00:00Z",
        "payload":{"id":child_id,"forked_from_id":parent_id,"cwd":"/repo/memex"}})
    .to_string()
        + "\n"
        + &token_line("2026-07-15T09:00:01Z", 100, 100)
        + &token_line("2026-07-15T09:00:02Z", 500, 600)
        + &token_line("2026-07-15T09:05:00Z", 150, 750);
    fs::write(sessions.join(format!("rollout-{child_id}.jsonl")), child).unwrap();
    let _env = EnvVarGuard::set_os(&[("CODEX_HOME", Some(temp.path().as_os_str()))]);
    let query = UsageQuery {
        source: Some(SourceFilter::Codex),
        cache_path: Some(temp.path().join("cache.sqlite3")),
        include_events: true,
        memo_ttl_ms: 60_000,
        ..UsageQuery::default()
    };
    let initial = scan_usage(&query).unwrap();
    assert!(initial.total_tokens > 150);
    let cache = UsageCache::open(query.cache_path.as_deref().unwrap()).unwrap();
    let rows: u64 = cache
        .connection
        .query_row(
            "SELECT count(*) FROM usage_file_cache WHERE source = 'codex'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        rows, 0,
        "unresolved fork deliberately has no blob checkpoint"
    );
    let generation = cache.fact_generation("codex").unwrap();
    expire(&query);
    assert_report_matches(&scan_usage(&query).unwrap(), &initial);
    assert_eq!(
        cache.fact_generation("codex").unwrap(),
        generation,
        "unchanged unresolved forks must not rewrite the canonical partition"
    );
    lock_partitions().remove(&(SourceFilter::Codex, query.cache_path.clone()));
    assert_report_matches(&scan_usage(&query).unwrap(), &initial);

    let parent = serde_json::json!({"type":"session_meta", "timestamp":"2026-07-14T10:00:00Z",
        "payload":{"id":parent_id,"cwd":"/repo/memex"}})
    .to_string()
        + "\n"
        + &token_line("2026-07-14T10:01:00Z", 100, 100)
        + &token_line("2026-07-14T10:02:00Z", 500, 600);
    fs::write(sessions.join(format!("rollout-{parent_id}.jsonl")), parent).unwrap();
    expire(&query);
    let resolved = scan_usage(&query).unwrap();
    assert_eq!(resolved.total_tokens, 750);
    assert_eq!(
        resolved
            .details
            .iter()
            .filter(|event| event.session_id.as_deref() == Some(child_id))
            .map(|event| event.tokens.total())
            .sum::<u64>(),
        150
    );
}

#[test]
#[cfg(unix)]
fn unresolved_codex_facts_retry_a_parent_whose_permissions_recover() {
    use std::os::unix::fs::PermissionsExt;

    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let sessions = temp.path().join("sessions");
    fs::create_dir_all(&sessions).unwrap();
    let parent_id = "019f0000-0000-7000-8000-000000000001";
    let child_id = "019f0000-0000-7000-8000-000000000002";
    let parent = sessions.join(format!("rollout-{parent_id}.jsonl"));
    fs::write(&parent, concat!(
        r#"{"type":"session_meta","timestamp":"2026-07-14T10:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000001"}}"#,
        "\n",
        r#"{"type":"event_msg","timestamp":"2026-07-14T10:00:30Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
        "\n",
        r#"{"type":"event_msg","timestamp":"2026-07-14T10:01:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":500},"total_token_usage":{"input_tokens":600}}}}"#,
        "\n"
    )).unwrap();
    let _env = EnvVarGuard::set_os(&[("CODEX_HOME", Some(temp.path().as_os_str()))]);
    let query = UsageQuery {
        source: Some(SourceFilter::Codex),
        cache_path: Some(temp.path().join("cache.sqlite3")),
        include_events: true,
        memo_ttl_ms: 60_000,
        ..UsageQuery::default()
    };
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 600);
    let metadata = super::scan::usage_file_metadata(&parent).unwrap();
    let permissions = fs::metadata(&parent).unwrap().permissions();
    fs::set_permissions(&parent, fs::Permissions::from_mode(0)).unwrap();
    if fs::File::open(&parent).is_ok() {
        // Privileged test users can bypass Unix permissions.
        fs::set_permissions(&parent, permissions).unwrap();
        return;
    }
    fs::write(sessions.join(format!("rollout-{child_id}.jsonl")), concat!(
        r#"{"type":"session_meta","timestamp":"2026-07-15T09:00:00Z","payload":{"id":"019f0000-0000-7000-8000-000000000002","forked_from_id":"019f0000-0000-7000-8000-000000000001"}}"#,
        "\n",
        r#"{"type":"event_msg","timestamp":"2026-07-15T09:00:01Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":100},"total_token_usage":{"input_tokens":100}}}}"#,
        "\n",
        r#"{"type":"event_msg","timestamp":"2026-07-15T09:05:00Z","payload":{"type":"token_count","info":{"last_token_usage":{"input_tokens":650},"total_token_usage":{"input_tokens":750}}}}"#,
        "\n"
    )).unwrap();
    expire(&query);
    let unresolved = scan_usage(&query).unwrap();
    assert!(unresolved.warnings.is_empty());
    assert!(unresolved.total_tokens > 750);
    fs::set_permissions(&parent, permissions).unwrap();
    assert_eq!(super::scan::usage_file_metadata(&parent).unwrap(), metadata);
    expire(&query);
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 750);
}

#[test]
fn hermes_real_wal_refresh_survives_another_writer_advancing_cache() {
    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("hermes");
    fs::create_dir_all(&root).unwrap();
    let database = root.join("state.db");
    let writer = Connection::open(&database).unwrap();
    writer.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA wal_autocheckpoint=0;
         CREATE TABLE sessions (id TEXT, model TEXT, started_at INTEGER,
             input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER,
             cache_write_tokens INTEGER, reasoning_tokens INTEGER, billing_provider TEXT,
             estimated_cost_usd REAL, cwd TEXT, git_repo_root TEXT, profile_name TEXT);
         INSERT INTO sessions VALUES ('s1','model',1000,10,5,0,0,0,NULL,NULL,'/repo/memex',NULL,NULL);
         PRAGMA wal_checkpoint(TRUNCATE);",
    ).unwrap();
    let _env = EnvVarGuard::set_os(&[("HERMES_PROFILE_ROOTS", Some(root.as_os_str()))]);
    let query = UsageQuery {
        source: Some(SourceFilter::Hermes),
        cache_path: Some(temp.path().join("cache.sqlite3")),
        include_events: true,
        memo_ttl_ms: 60_000,
        ..UsageQuery::default()
    };
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 15);
    let metadata = super::scan::usage_file_metadata(&database).unwrap();
    writer
        .execute("UPDATE sessions SET input_tokens=70", [])
        .unwrap();
    assert_eq!(
        super::scan::usage_file_metadata(&database).unwrap(),
        metadata
    );

    // A non-retaining caller advances disk facts and cached WAL dependencies,
    // while the retained assembly still represents the original transaction.
    let oneshot = UsageQuery {
        memo_ttl_ms: 0,
        ..query.clone()
    };
    assert_eq!(scan_usage(&oneshot).unwrap().total_tokens, 75);
    expire(&query);
    let refreshed = scan_usage(&query).unwrap();
    assert_eq!(refreshed.total_tokens, 75);
    let fresh = scan_usage(&UsageQuery {
        cache_path: Some(temp.path().join("fresh.sqlite3")),
        ..oneshot
    })
    .unwrap();
    assert_report_matches(&refreshed, &fresh);
    // Keep the writer open through both reads: closing it may checkpoint WAL.
    drop(writer);
}

#[test]
fn cursor_mapping_only_changes_survive_retained_and_cold_fact_queries() {
    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let _env = EnvVarGuard::set_os(&[("HOME", Some(temp.path().as_os_str()))]);
    let user = if cfg!(target_os = "macos") {
        temp.path().join("Library/Application Support/Cursor/User")
    } else {
        temp.path().join(".config/Cursor/User")
    };
    let database = user.join("globalStorage/state.vscdb");
    fs::create_dir_all(database.parent().unwrap()).unwrap();
    let writer = Connection::open(&database).unwrap();
    writer.execute_batch(
        "CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY, value TEXT);
         INSERT INTO cursorDiskKV VALUES ('composerData:composer-main',
           '{\"generationUUID\":\"gen-1\",\"createdAt\":1000,\"inputTokens\":10,\"outputTokens\":5}');
         INSERT INTO cursorDiskKV VALUES ('composerData:composer-other',
           '{\"generationUUID\":\"gen-2\",\"createdAt\":1000,\"inputTokens\":20,\"outputTokens\":5}');",
    ).unwrap();
    drop(writer);
    let query = UsageQuery {
        source: Some(SourceFilter::Cursor),
        cache_path: Some(temp.path().join("cache.sqlite3")),
        include_events: true,
        memo_ttl_ms: 60_000,
        ..UsageQuery::default()
    };
    let initial = scan_usage(&query).unwrap();
    assert_eq!(initial.events, 2);
    assert_eq!(initial.total_tokens, 40);
    assert!(initial.details.iter().all(|event| event.project.is_none()));
    let metadata = super::scan::usage_file_metadata(&database).unwrap();
    let transcript = temp
        .path()
        .join(".cursor/projects/memex/agent-transcripts/composer-main.jsonl");
    fs::create_dir_all(transcript.parent().unwrap()).unwrap();
    fs::write(&transcript, "\n").unwrap();
    expire(&query);
    let mapped = scan_usage(&query).unwrap();
    assert_eq!(
        super::scan::usage_file_metadata(&database).unwrap(),
        metadata
    );
    assert_eq!(
        mapped
            .details
            .iter()
            .filter(|event| event.project.as_deref() == Some("memex"))
            .count(),
        1
    );
    let fresh = scan_usage(&UsageQuery {
        cache_path: Some(temp.path().join("fresh.sqlite3")),
        memo_ttl_ms: 0,
        ..query.clone()
    })
    .unwrap();
    assert_report_matches(&mapped, &fresh);
    lock_partitions().remove(&(SourceFilter::Cursor, query.cache_path.clone()));
    assert_report_matches(&scan_usage(&query).unwrap(), &fresh);

    fs::remove_file(&transcript).unwrap();
    // Cold serving must also notice that attribution disappeared.
    lock_partitions().remove(&(SourceFilter::Cursor, query.cache_path.clone()));
    let unmapped = scan_usage(&query).unwrap();
    assert_report_matches(&unmapped, &initial);
}

#[test]
fn bob_usage_matches_through_uncached_and_canonical_fact_scans() {
    use crate::sources::bob::fixtures;
    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let database = temp.path().join("bob.db");
    let writer = fixtures::create(&database);
    fixtures::insert_task(&writer, "task", None, "normal", "file:/work/repo", "T", 1);
    fixtures::insert_message(
        &writer,
        "message",
        "task",
        "assistant",
        r#"{"role":"assistant","content":"done","_meta":{"timestamp":1700000000002,"spend":{"input":100,"output":20,"cost":0.0125}}}"#,
        1,
    );
    let _env = EnvVarGuard::set_os(&[("MEMEX_BOB_DB", Some(database.as_os_str()))]);
    let query = UsageQuery {
        source: Some(SourceFilter::Bob),
        include_events: true,
        ..Default::default()
    };
    let uncached = scan_usage(&query).unwrap();
    assert_eq!(uncached.events, 1);
    assert_eq!(uncached.total_tokens, 120);
    let cached_query = UsageQuery {
        cache_path: Some(temp.path().join("usage.sqlite3")),
        ..query
    };
    assert_report_matches(&scan_usage(&cached_query).unwrap(), &uncached);
    assert_report_matches(&scan_usage(&cached_query).unwrap(), &uncached);
}
