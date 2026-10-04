use super::*;
use crate::analytics::{AnalyticsWriter, SessionKindFilter};
use crate::types::Record;

fn request(
    paths: &Paths,
    query: &str,
    origin: SessionOrigin,
    limit: usize,
) -> SearchCollectRequest {
    SearchCollectRequest {
        query: query.into(),
        additional_queries: Vec::new(),
        cwd: None,
        project: Some("museum".into()),
        role: None,
        tool: None,
        session: None,
        source: Some(SourceFilter::Codex),
        origin,
        mode: SearchMode::Lexical,
        min_score: None,
        recency_weight: default_recency_weight(),
        recency_half_life_days: 30.0,
        since: None,
        until: None,
        limit,
        top_n_per_session: None,
        unique_session: true,
        fields: search_fields(None, false).unwrap(),
        sort: SortBy::Score,
        verbose: false,
        format: SearchFormat::Json,
        root: Some(paths.root.clone()),
        machines: vec!["local".into()],
    }
}

fn signatures(hits: &[LocatedRecord]) -> Vec<(String, f32)> {
    hits.iter()
        .map(|hit| (canonical_record_id(&hit.record), hit.score))
        .collect()
}

fn json_signatures(value: &Value) -> Vec<(String, f32)> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| {
            (
                hit["record_id"].as_str().unwrap().to_owned(),
                hit["score"].as_f64().unwrap() as f32,
            )
        })
        .collect()
}

#[test]
fn conversation_ranking_matches_all_five_surfaces() {
    let _lock = crate::test_support::env_lock();
    let root = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(root.path().to_path_buf())).unwrap();
    paths.ensure_dirs().unwrap();
    std::fs::write(
        paths.root.join("config.toml"),
        "auto_index_on_search = false\n",
    )
    .unwrap();
    let index = SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
    let mut writer = index.writer().unwrap();
    let mut analytics = AnalyticsWriter::open(analytics_path(&paths.state)).unwrap();
    let phrase = "prepare the amber gallery display and notify the curator";
    for id in 1..=1220u64 {
        // A large tied prefix from one session starves fixed-overfetch grouping.
        // Reused session IDs must remain distinct when source paths differ.
        let session = if id <= 1200 { 0 } else { id % 3 };
        let path = if id <= 1200 { 0 } else { id };
        let text = if id == 1210 {
            format!(
                "needle {phrase}. {}",
                "Background catalog maintenance. ".repeat(1000)
            )
        } else {
            "needle notify the curator and prepare the display in the amber gallery".into()
        };
        let record: Record = serde_json::from_value(serde_json::json!({
            "source": if id == 1219 { "claude" } else { "codex" },
            "doc_id": id, "ts": 1700000000000u64, "project": if id == 1220 { "other" } else { "museum" },
            "repo_project": "museum", "session_id": format!("session-{session}"),
            "turn_id": id, "role": "user", "text": text,
            "source_path": format!("/fixture/{path}.jsonl"), "event_id": format!("event-{id}"),
            "conversation_kind": if id == 1201 { "subagent" } else { "main" },
        })).unwrap();
        index.add_record(&mut writer, &record).unwrap();
        analytics.record(&record).unwrap();
    }
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
    index.publish_generation().unwrap();
    analytics.flush().unwrap();
    drop(analytics);
    // This fixture has no source files to resolve repository metadata from.
    rusqlite::Connection::open(analytics_path(&paths.state))
        .unwrap()
        .execute("update sessions set repo_project = project", [])
        .unwrap();

    for query in ["needle", phrase] {
        for (origin, kind, origin_name) in [
            (SessionOrigin::All, SessionKindFilter::All, "all"),
            (
                SessionOrigin::Interactive,
                SessionKindFilter::Primary,
                "interactive",
            ),
        ] {
            for limit in [1, 5, 18] {
                let cli =
                    collect_search_with_auto_index(request(&paths, query, origin, limit), false)
                        .unwrap();
                let expected = signatures(&cli.results);
                assert_eq!(expected.len(), limit);
                if query == phrase {
                    assert_eq!(cli.results[0].record.doc_id, 1210);
                }
                if query == "needle" {
                    assert_eq!(cli.results[0].record.doc_id, 1);
                }
                let mcp = mcp_search(Some(paths.root.clone()), serde_json::from_value(serde_json::json!({
                    "query": query, "project": "museum", "source": "codex", "origin": origin_name,
                    "limit": limit, "machines": ["local"],
                })).unwrap()).unwrap();
                assert_eq!(
                    json_signatures(&mcp["results"]),
                    expected,
                    "MCP {query} {limit}"
                );
                #[cfg(unix)]
                {
                    let native = native_request(
                        &paths,
                        crate::native::Operation::Search {
                            machine: "local".into(),
                            query: query.into(),
                            project: Some("museum".into()),
                            source: Some("codex".into()),
                            since: None,
                            origin,
                            limit,
                        },
                    )
                    .unwrap();
                    assert_eq!(
                        native
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|hit| hit["record_id"].as_str().unwrap())
                            .collect::<Vec<_>>(),
                        expected
                            .iter()
                            .map(|(id, _)| id.as_str())
                            .collect::<Vec<_>>(),
                        "native {query} {limit}"
                    );
                }
                let tui = crate::tui::evaluate_search(
                    &paths,
                    query,
                    Some("museum"),
                    Some(SourceFilter::Codex),
                    kind,
                    limit,
                )
                .unwrap();
                let web = crate::web::evaluate_search(
                    &paths,
                    query,
                    Some("museum"),
                    Some(SourceFilter::Codex),
                    kind,
                    limit,
                )
                .unwrap();
                assert_eq!(signatures(&tui.hits), expected, "TUI {query} {limit}");
                assert_eq!(signatures(&web.hits), expected, "web {query} {limit}");
            }
        }
    }
    let mut records = request(&paths, "needle", SessionOrigin::All, 5);
    records.unique_session = false;
    let records = collect_search_with_auto_index(records, false).unwrap();
    assert_eq!(
        records
            .results
            .iter()
            .map(|hit| hit.record.doc_id)
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
}

#[test]
fn explicit_recency_timestamp_and_score_options_remain_effective() {
    let root = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(root.path().to_path_buf())).unwrap();
    let index = SearchIndex::open_or_create_for_ingest(&paths.index).unwrap();
    let mut writer = index.writer().unwrap();
    let now = chrono::Utc::now().timestamp_millis() as u64;
    for (id, ts, text) in [
        (1, 1_000, "needle".to_string()),
        (2, now, format!("needle {}", "background ".repeat(20))),
    ] {
        let record: Record = serde_json::from_value(serde_json::json!({
            "source":"codex", "doc_id":id, "ts":ts, "project":"museum",
            "session_id":format!("session-{id}"), "turn_id":1, "role":"user", "text":text,
            "source_path":format!("/fixture/{id}.jsonl"), "conversation_kind":"main",
        }))
        .unwrap();
        index.add_record(&mut writer, &record).unwrap();
    }
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
    index.publish_generation().unwrap();
    let baseline =
        collect_search_with_auto_index(request(&paths, "needle", SessionOrigin::All, 2), false)
            .unwrap();
    assert_eq!(baseline.results[0].record.doc_id, 1);
    let mut recent = request(&paths, "needle", SessionOrigin::All, 2);
    recent.recency_weight = 100.0;
    let recent = collect_search_with_auto_index(recent, false).unwrap();
    assert_eq!(recent.results[0].record.doc_id, 2);
    let mut timestamp = request(&paths, "needle", SessionOrigin::All, 2);
    timestamp.sort = SortBy::Ts;
    let timestamp = collect_search_with_auto_index(timestamp, false).unwrap();
    assert_eq!(timestamp.results[0].record.doc_id, 2);
    let mut threshold = request(&paths, "needle", SessionOrigin::All, 2);
    threshold.min_score = Some((baseline.results[0].score + baseline.results[1].score) / 2.0);
    let threshold = collect_search_with_auto_index(threshold, false).unwrap();
    assert_eq!(threshold.results.len(), 1);
    assert_eq!(threshold.results[0].record.doc_id, 1);
}
