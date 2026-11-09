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
        credits: None,
        token_usage_available: true,
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

#[test]
fn retaining_queries_distinguish_claude_roots_in_partitions_and_merged_views() {
    use super::snapshot::{USAGE_SCAN_LOCK, lock_merged};

    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let _env = crate::test_support::pin_source_roots(temp.path());
    let work = temp.path().join("work/projects");
    let personal = temp.path().join("personal/projects");
    for (root, message, tokens) in [(&work, "work", 10), (&personal, "personal", 20)] {
        fs::create_dir_all(root.join("-repo")).unwrap();
        fs::write(
            root.join("-repo/session.jsonl"),
            claude_line(message, tokens),
        )
        .unwrap();
    }
    for source in [Some(SourceFilter::Claude), None] {
        for disk_cache in [false, true] {
            lock_partitions().clear();
            lock_merged().clear();
            let mut query = UsageQuery {
                source,
                memo_ttl_ms: 60_000,
                cache_path: disk_cache.then(|| temp.path().join(format!("{source:?}.sqlite3"))),
                claude_sources: Some(vec![work.clone()]),
                ..UsageQuery::default()
            };
            // Populate facts without retention first, then exercise cold snapshot publication.
            assert_eq!(
                scan_usage(&UsageQuery {
                    memo_ttl_ms: 0,
                    ..query.clone()
                })
                .unwrap()
                .total_tokens,
                10
            );
            assert_eq!(scan_usage(&query).unwrap().total_tokens, 10);
            query.claude_sources = Some(vec![personal.clone()]);
            assert_eq!(scan_usage(&query).unwrap().total_tokens, 20);
            query.claude_sources = Some(vec![work.clone()]);
            assert_eq!(scan_usage(&query).unwrap().total_tokens, 10);

            // A refresh in progress may serve stale data only from the requested corpus.
            let refresh = USAGE_SCAN_LOCK.lock().unwrap();
            query.claude_sources = Some(vec![personal.clone()]);
            let (tx, rx) = std::sync::mpsc::channel();
            let join = std::thread::spawn(move || {
                tx.send(scan_usage(&query).unwrap().total_tokens).unwrap()
            });
            let premature = rx.recv_timeout(Duration::from_millis(50)).ok();
            drop(refresh);
            join.join().unwrap();
            assert_eq!(premature, None);
            assert_eq!(rx.recv().unwrap(), 20);
        }
    }
    lock_partitions().clear();
    lock_merged().clear();
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
    let observed = validate_partition(
        SourceFilter::Claude,
        Some(path),
        None,
        &crate::config::default_claude_sources(),
    )
    .unwrap();
    assert!(!observed.valid);
    // The supplied checkpoint predates this second mutation. The rebuilt
    // assembly can answer, but its disk facts must not be marked synchronized.
    fs::write(&transcript, claude_line("later", 300)).unwrap();
    populate_after_failed_validation(
        SourceFilter::Claude,
        path,
        observed.fingerprint,
        &crate::config::default_claude_sources(),
    );
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
        validate_partition(
            SourceFilter::Claude,
            Some(path),
            None,
            &crate::config::default_claude_sources()
        )
        .unwrap()
        .valid
    );

    // Exercise the actual cold query entry point with a stable changed source.
    lock_partitions().remove(&key);
    fs::write(&transcript, claude_line("final", 400)).unwrap();
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 400);
    assert!(
        validate_partition(
            SourceFilter::Claude,
            Some(path),
            None,
            &crate::config::default_claude_sources()
        )
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
        discovery_fingerprint(
            SourceFilter::Claude,
            &crate::config::default_claude_sources(),
        ),
        &crate::config::default_claude_sources(),
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
        discovery_fingerprint(
            SourceFilter::Claude,
            &crate::config::default_claude_sources(),
        ),
        &crate::config::default_claude_sources(),
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
        &crate::config::default_claude_sources(),
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
    fs::set_permissions(&parent, fs::Permissions::from_mode(0o0)).unwrap();
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

#[test]
fn facts_refresh_replaces_warnings_from_an_earlier_pass() {
    use super::cache::write_fact_sync;
    use super::snapshot::lock_merged;

    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let _env = crate::test_support::pin_source_roots(temp.path());
    let root = temp.path().join("JCODE_SESSIONS_DIR");
    fs::create_dir_all(&root).unwrap();
    let session = root.join("session_one.json");
    let write_session = |input: u64| {
        let doc = serde_json::json!({
            "id": "session_one",
            "messages": [{"id": "m1", "role": "assistant", "timestamp": 1788338819456u64,
                "token_usage": {"input_tokens": input, "output_tokens": 5}}],
        });
        fs::write(&session, doc.to_string()).unwrap();
    };
    write_session(10);
    let query = UsageQuery {
        source: Some(SourceFilter::Jcode),
        cache_path: Some(temp.path().join("cache.sqlite3")),
        memo_ttl_ms: 60_000,
        ..UsageQuery::default()
    };
    let key = (SourceFilter::Jcode, query.cache_path.clone());
    assert_eq!(scan_usage(&query).unwrap().total_tokens, 15);
    // Record warnings an earlier pass could have reported (a transiently
    // locked file, a failed cache write) against the current facts.
    let stamp_transient_warnings = || {
        let fingerprint = lock_partitions()
            .get(&key)
            .map(|entry| entry.fingerprint.clone())
            .expect("snapshot published");
        let stored = Connection::open(query.cache_path.as_ref().unwrap()).unwrap();
        write_fact_sync(
            &stored,
            SourceFilter::Jcode,
            &fingerprint,
            &[
                format!(
                    "jcode usage file skipped ({}): database is locked",
                    session.display()
                ),
                "jcode usage cache write failed: database is locked".to_string(),
            ],
        )
        .unwrap();
        expire(&query);
    };
    // Cold queries serve the stored checkpoint's warnings directly.
    let cold_warnings = || {
        lock_partitions().clear();
        lock_merged().clear();
        scan_usage(&query).unwrap().warnings
    };

    // The file reparses cleanly, so its warning must not survive the refresh.
    stamp_transient_warnings();
    write_session(70);
    let reparsed = scan_usage(&query).unwrap();
    assert_eq!(reparsed.total_tokens, 75);
    assert!(reparsed.warnings.is_empty(), "{:?}", reparsed.warnings);
    assert!(cold_warnings().is_empty());

    // Nothing changed on disk: the refresh reuses every fact yet still drops
    // the old warnings, from both the report and the stored checkpoint.
    stamp_transient_warnings();
    let unchanged = scan_usage(&query).unwrap();
    assert_eq!(unchanged.total_tokens, 75);
    assert!(unchanged.warnings.is_empty(), "{:?}", unchanged.warnings);
    assert!(cold_warnings().is_empty());
}

fn kiro_session(root: &std::path::Path, name: &str, execution: &str, at: &str, credits: f64) {
    let dir = root.join("workspace").join(name);
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("session.json"), r#"{"id":"shared"}"#).unwrap();
    let line = serde_json::json!({"id": name, "timestamp": at,
        "payload": {"type": "usage_summary", "executionId": execution,
            "promptTurnSummaries": [{"unit": "credit", "usage": credits}]}});
    fs::write(dir.join("messages.jsonl"), format!("{line}\n")).unwrap();
}

#[test]
fn kiro_refresh_reconciles_an_execution_repeated_in_a_new_file() {
    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let _pinned = crate::test_support::pin_source_roots(temp.path());
    let root = temp.path().join("kiro");
    let _env = EnvVarGuard::set_os(&[("KIRO_SESSIONS_DIR", Some(root.as_os_str()))]);
    kiro_session(&root, "a", "exec", "2026-09-04T11:11:05.286Z", 1.0);
    let query = UsageQuery {
        source: Some(SourceFilter::Kiro),
        cache_path: Some(temp.path().join("cache.sqlite3")),
        memo_ttl_ms: 60_000,
        ..UsageQuery::default()
    };
    let initial = scan_usage(&query).unwrap();
    assert_eq!((initial.events, initial.credits), (1, Some(1.0)));

    // A later copy of the same (session, executionId) summary supersedes the
    // first even though only the new file reparses.
    kiro_session(&root, "b", "exec", "2026-09-04T12:00:00.000Z", 2.0);
    expire(&query);
    let refreshed = scan_usage(&query).unwrap();
    assert_eq!((refreshed.events, refreshed.credits), (1, Some(2.0)));
    let fresh = scan_usage(&UsageQuery {
        cache_path: Some(temp.path().join("fresh.sqlite3")),
        memo_ttl_ms: 0,
        ..query.clone()
    })
    .unwrap();
    assert_report_matches(&refreshed, &fresh);
}

#[test]
fn kiro_credit_warning_needs_kiro_usage() {
    use super::snapshot::lock_merged;

    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let _pinned = crate::test_support::pin_source_roots(temp.path());
    let root = temp.path().join("kiro");
    let _env = EnvVarGuard::set_os(&[("KIRO_SESSIONS_DIR", Some(root.as_os_str()))]);
    let kiro_warning = |report: &UsageReport| {
        report
            .warnings
            .iter()
            .any(|warning| warning.starts_with("Kiro reports credits"))
    };
    lock_partitions().clear();
    lock_merged().clear();
    assert!(!kiro_warning(&scan_usage(&UsageQuery::default()).unwrap()));
    kiro_session(&root, "a", "exec", "2026-09-04T11:11:05.286Z", 1.0);
    lock_partitions().clear();
    lock_merged().clear();
    assert!(kiro_warning(&scan_usage(&UsageQuery::default()).unwrap()));
}

#[test]
fn codex_usage_includes_compressed_rollouts_once() {
    let _guard = env_lock();
    let temp = tempfile::tempdir().unwrap();
    let sessions = temp.path().join("sessions/2026/09/30");
    let archived = temp.path().join("archived_sessions");
    fs::create_dir_all(&sessions).unwrap();
    fs::create_dir_all(&archived).unwrap();
    let rollout = |id: &str, tokens: u64| {
        serde_json::json!({"type":"session_meta", "timestamp":"2026-09-30T09:00:00Z",
            "payload":{"id":id,"cwd":"/repo/memex"}})
        .to_string()
            + "\n"
            + &serde_json::json!({"type":"event_msg", "timestamp":"2026-09-30T09:00:01Z",
            "payload":{"type":"token_count", "info":{
                "last_token_usage":{"input_tokens":tokens},
                "total_token_usage":{"input_tokens":tokens}
            }}})
            .to_string()
            + "\n"
    };
    let compress = |path: std::path::PathBuf, contents: &str| {
        fs::write(&path, zstd::encode_all(contents.as_bytes(), 3).unwrap()).unwrap();
        path
    };
    let plain_id = "019a0000-0000-7000-8000-000000000011";
    let archived_id = "019a0000-0000-7000-8000-000000000012";
    let compressing_id = "019a0000-0000-7000-8000-000000000013";
    fs::write(
        sessions.join(format!("rollout-{plain_id}.jsonl")),
        rollout(plain_id, 100),
    )
    .unwrap();
    compress(
        archived.join(format!("rollout-{archived_id}.jsonl.zst")),
        &rollout(archived_id, 20),
    );
    // Codex writes the archive before it removes the original.
    let compressing = sessions.join(format!("rollout-{compressing_id}.jsonl"));
    fs::write(&compressing, rollout(compressing_id, 3)).unwrap();
    let compressed = compress(
        sessions.join(format!("rollout-{compressing_id}.jsonl.zst")),
        &rollout(compressing_id, 3),
    );
    let _env = EnvVarGuard::set_os(&[("CODEX_HOME", Some(temp.path().as_os_str()))]);
    let query = UsageQuery {
        source: Some(SourceFilter::Codex),
        cache_path: Some(temp.path().join("cache.sqlite3")),
        include_events: true,
        memo_ttl_ms: 60_000,
        ..UsageQuery::default()
    };
    let session_paths = |report: &UsageReport| {
        let mut paths = report
            .details
            .iter()
            .map(|event| {
                (
                    event.session_id.clone().unwrap(),
                    event.source_path.to_string(),
                )
            })
            .collect::<Vec<_>>();
        paths.sort();
        paths
    };

    let report = scan_usage(&query).unwrap();
    assert_eq!(report.total_tokens, 123);
    let paths = session_paths(&report);
    assert_eq!(paths.len(), 3, "{paths:?}");
    assert!(
        paths
            .iter()
            .any(|(id, path)| id == archived_id && path.ends_with(".jsonl.zst"))
    );
    assert!(
        paths
            .iter()
            .any(|(id, path)| id == compressing_id && path == &compressing.to_string_lossy())
    );

    fs::remove_file(&compressing).unwrap();
    expire(&query);
    let report = scan_usage(&query).unwrap();
    assert_eq!(report.total_tokens, 123);
    assert!(
        session_paths(&report)
            .iter()
            .any(|(id, path)| id == compressing_id && path == &compressed.to_string_lossy())
    );
}
