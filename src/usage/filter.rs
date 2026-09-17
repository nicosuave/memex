//! Query predicates: timestamps, projects, sessions, and permission reviews.
//!
//! Timestamp bounds use binary search over sorted assemblies; the remaining
//! predicates apply per event with per-query precomputations shared by
//! single-assembly and merged filtering.

use super::UsageQuery;
use super::compact::{FilterFields, UsageAssembly};
use crate::analytics::ProjectGrouping;
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Query predicates with per-query precomputations shared by single-assembly and
/// merged filtering: the normalized project query, borrowed session keys, and the
/// repository-project cache.
pub(crate) struct FilterPlan<'a> {
    grouping: ProjectGrouping,
    include_reviews: bool,
    project_raw: Option<&'a str>,
    project_key: Option<String>,
    session_set: Option<HashSet<(&'a str, &'a str)>>,
    project_cache: HashMap<String, String>,
}

impl<'a> FilterPlan<'a> {
    pub(crate) fn new(query: &'a UsageQuery) -> Self {
        // Normalize the project query once instead of per event.
        let project_raw = query.project.as_deref();
        let project_key = project_raw.map(usage_project_key);
        // Borrow session keys once; lookups below avoid per-event String allocation.
        let session_set: Option<HashSet<(&str, &str)>> = query.session_keys.as_ref().map(|keys| {
            keys.iter()
                .map(|(source, session)| (source.as_str(), session.as_str()))
                .collect()
        });
        Self {
            grouping: query.project_grouping,
            include_reviews: query.include_reviews,
            project_raw,
            project_key,
            session_set,
            project_cache: HashMap::new(),
        }
    }

    pub(crate) fn matches(&mut self, event: FilterFields<'_>) -> bool {
        (self.include_reviews || !event.permission_review)
            && self.project_raw.is_none_or(|project| {
                self.project_key.as_deref().is_some_and(|key| {
                    event.project.is_some_and(|candidate| {
                        usage_project_matches_precomputed(
                            candidate,
                            project,
                            key,
                            self.grouping,
                            &mut self.project_cache,
                        )
                    })
                })
            })
            && self.session_set.as_ref().is_none_or(|keys| {
                event
                    .session_id
                    .is_some_and(|session_id| keys.contains(&(event.source, session_id)))
            })
    }
}

/// Assembled events are already sorted; filtering preserves that order.
/// Timestamp bounds use binary search so narrow memo queries avoid walking all history.
pub(crate) fn filtered_events<'a>(
    assembled: &'a UsageAssembly,
    query: &'a UsageQuery,
) -> impl Iterator<Item = usize> + 'a {
    let start = query
        .since_ms
        .map_or(0, |since| assembled.lower_bound(since));
    let end = query
        .until_ms
        .map_or(assembled.len(), |until| assembled.upper_bound(until, start));
    let start = start.min(assembled.len());
    let end = end.clamp(start, assembled.len());
    let mut plan = FilterPlan::new(query);
    (start..end).filter(move |&index| plan.matches(assembled.filter_fields(index)))
}

#[allow(dead_code)]
fn usage_project_matches(
    candidate: &str,
    project: &str,
    grouping: ProjectGrouping,
    cache: &mut HashMap<String, String>,
) -> bool {
    let project_key = usage_project_key(project);
    usage_project_matches_precomputed(candidate, project, &project_key, grouping, cache)
}

fn usage_project_matches_precomputed(
    candidate: &str,
    project_raw: &str,
    project_key: &str,
    grouping: ProjectGrouping,
    cache: &mut HashMap<String, String>,
) -> bool {
    if candidate.eq_ignore_ascii_case(project_raw) {
        return true;
    }
    match grouping {
        ProjectGrouping::Flat => project_tail(candidate).eq_ignore_ascii_case(project_key),
        ProjectGrouping::Repository => {
            if let Some(cached) = cache.get(candidate) {
                return cached.eq_ignore_ascii_case(project_key);
            }
            let computed = if Path::new(candidate).is_absolute() {
                crate::analytics::repository_project_for_cwd(candidate)
                    .unwrap_or_else(|| crate::analytics::UNFILED_PROJECT.to_string())
            } else {
                project_tail(candidate).to_string()
            };
            let matched = computed.eq_ignore_ascii_case(project_key);
            cache.insert(candidate.to_string(), computed);
            matched
        }
    }
}

fn starts_with_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    // `get` refuses a cut inside a multibyte char; byte slicing would panic.
    haystack
        .get(..needle.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(needle))
}

fn project_tail(value: &str) -> &str {
    let trimmed = value.trim().trim_end_matches(['/', '\\']);
    let mut tail = trimmed.rsplit(['/', '\\', ':']).next().unwrap_or(trimmed);
    tail = tail.strip_suffix(".git").unwrap_or(tail);
    let encoded = tail.trim_matches('-');
    if tail.starts_with('-')
        && (starts_with_ignore_ascii_case(encoded, "users-")
            || starts_with_ignore_ascii_case(encoded, "home-"))
    {
        return encoded.rsplit('-').next().unwrap_or(encoded);
    }
    tail
}

fn usage_project_key(value: &str) -> String {
    project_tail(value).to_string()
}

#[cfg(test)]
mod tests {
    use super::super::snapshot::sort_usage_events;
    use super::super::{SourceFilter, UsageQuery, cache_event, scan_usage, scan_usage_activity};
    use super::*;
    use rusqlite::Connection;
    use std::fs;
    use std::sync::Arc;

    #[test]
    fn multibyte_project_prefix_cannot_panic_prefix_match() {
        // "--abcdeéxyz--" puts a two-byte char across the 6-byte "users-"
        // cut; byte slicing would panic instead of returning false.
        let mut cache = HashMap::new();
        assert!(!usage_project_matches(
            "--abcdeéxyz--",
            "nomatch",
            ProjectGrouping::Flat,
            &mut cache,
        ));
        assert!(!starts_with_ignore_ascii_case("abécd", "users-"));
        assert!(starts_with_ignore_ascii_case("Users-memex", "users-"));
    }

    #[test]
    fn usage_project_matching_normalizes_paths_slugs_and_remotes() {
        let mut cache = HashMap::new();

        for candidate in [
            "/Users/nico/Code/memex",
            "--Users-nico-Code-memex--",
            "git@github.com:nicosuave/memex.git",
        ] {
            assert!(usage_project_matches(
                candidate,
                "memex",
                ProjectGrouping::Flat,
                &mut cache,
            ));
        }
        assert!(!usage_project_matches(
            "/Users/nico/Code/other",
            "memex",
            ProjectGrouping::Flat,
            &mut cache,
        ));
    }

    #[test]
    fn repository_usage_groups_absolute_non_git_paths_as_unfiled() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let standalone = tmp.path().join("generated-task-name");
        fs::create_dir(&standalone).expect("standalone dir");
        let mut cache = HashMap::new();

        assert!(usage_project_matches(
            standalone.to_string_lossy().as_ref(),
            crate::analytics::UNFILED_PROJECT,
            ProjectGrouping::Repository,
            &mut cache,
        ));
        assert!(usage_project_matches(
            "/missing/home/.codex/worktrees/8952/memex",
            "memex",
            ProjectGrouping::Repository,
            &mut cache,
        ));
        assert!(usage_project_matches(
            "memex",
            "memex",
            ProjectGrouping::Repository,
            &mut cache,
        ));
    }
    #[test]
    fn index_sort_preserves_stable_order_including_equal_keys() {
        for len in [0, 1, 2, 7, 128, 4097] {
            let mut events: Vec<_> = (0..len)
                .map(|index| {
                    let mut event =
                        cache_event("session", ((index * 17) % 23) as u64, "model", 10, 0, 0);
                    event.source_path = Arc::from(format!("path-{}", (index * 7) % 3));
                    event.source_order = (index % 5) as u64;
                    event.source_record_id = Some(index.to_string());
                    event
                })
                .collect();
            let mut expected = events.clone();
            expected.sort_by(|a, b| {
                (a.timestamp_ms, &a.source_path, a.source_order).cmp(&(
                    b.timestamp_ms,
                    &b.source_path,
                    b.source_order,
                ))
            });
            sort_usage_events(&mut events);
            assert_eq!(
                serde_json::to_value(events).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
    }

    #[test]
    fn claude_warm_cache_reconciles_old_parents_before_since_filter() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects = tmp.path().join("projects/memex");
        let subagents = projects.join("subagents");
        std::fs::create_dir_all(&subagents).expect("create projects");
        std::fs::write(
            projects.join("parent.jsonl"),
            concat!(
                r#"{"type":"assistant","sessionId":"parent","requestId":"parent-request","timestamp":1000,"cwd":"/repo/memex","message":{"id":"shared-message","model":"claude-sonnet-4-6","usage":{"inputTokens":10}}}"#,
                "\n"
            ),
        )
        .expect("write parent transcript");
        std::fs::write(
            subagents.join("agent.jsonl"),
            concat!(
                r#"{"type":"assistant","sessionId":"agent","requestId":"sidechain-request","timestamp":3000,"cwd":"/repo/memex","isSidechain":true,"message":{"id":"shared-message","model":"claude-sonnet-4-6","usage":{"inputTokens":10}}}"#,
                "\n"
            ),
        )
        .expect("write sidechain transcript");
        let _env = EnvVarGuard::set_os(&[("CLAUDE_CONFIG_DIR", Some(tmp.path().as_os_str()))]);
        let query = UsageQuery {
            source: Some(SourceFilter::Claude),
            since_ms: Some(2_000_000),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            ..UsageQuery::default()
        };

        let cold = scan_usage(&query).expect("cold scan");
        let cache = Connection::open(query.cache_path.as_ref().expect("cache path"))
            .expect("open usage cache");
        let cached_files: u64 = cache
            .query_row(
                "SELECT count(*) FROM usage_file_cache WHERE source = 'claude'",
                [],
                |row| row.get(0),
            )
            .expect("count cached files");
        let warm = scan_usage(&query).expect("warm scan");

        assert_eq!(cold.events, 0);
        assert_eq!(cached_files, 2);
        assert_eq!(warm.events, cold.events);
        assert_eq!(warm.total_tokens, 0);
    }

    #[test]
    fn permission_reviews_are_opt_in_for_cold_cached_memoized_usage_and_activity() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let sessions = tmp.path().join("sessions/2026/07/03");
        std::fs::create_dir_all(&sessions).expect("create sessions");
        for (id, origin) in [
            ("primary", serde_json::json!({"source": "cli"})),
            (
                "agent",
                serde_json::json!({"source": {"subagent": "worker"}}),
            ),
            (
                "review",
                serde_json::json!({"thread_source": "guardian_review"}),
            ),
            (
                "legacy-review",
                serde_json::json!({"source": {"subagent": {"other": "guardian"}}}),
            ),
        ] {
            let mut payload = origin;
            payload["id"] = id.into();
            payload["cwd"] = "/repo/memex".into();
            let metadata = serde_json::json!({"type": "session_meta", "payload": payload});
            let usage = serde_json::json!({
                "type": "event_msg", "timestamp": "2026-07-03T01:02:05Z",
                "payload": {"type": "token_count", "info": {
                    "last_token_usage": {"input_tokens": 100, "output_tokens": 25},
                    "total_token_usage": {"input_tokens": 100, "output_tokens": 25}
                }}
            });
            std::fs::write(
                sessions.join(format!("rollout-{id}.jsonl")),
                format!("{metadata}\n{usage}\n"),
            )
            .expect("write session");
        }
        let _env = EnvVarGuard::set_os(&[("CODEX_HOME", Some(tmp.path().as_os_str()))]);
        let mut query = UsageQuery {
            source: Some(SourceFilter::Codex),
            include_events: true,
            cache_path: Some(tmp.path().join("usage-cache.sqlite3")),
            ..UsageQuery::default()
        };
        let cold = scan_usage(&query).expect("cold scan");
        assert_eq!(cold.events, 2);
        assert_eq!(cold.total_tokens, 250);
        assert!(cold.details.iter().all(|event| !event.permission_review));
        let warm = scan_usage(&query).expect("cached scan");
        assert_eq!(warm.events, 2);
        query.memo_ttl_ms = 60_000;
        assert_eq!(scan_usage(&query).unwrap().events, 2);
        query.include_reviews = true;
        assert_eq!(scan_usage(&query).unwrap().events, 4);
        assert_eq!(scan_usage_activity(&query).unwrap().0.len(), 4);
        query.include_reviews = false;
        assert_eq!(scan_usage_activity(&query).unwrap().0.len(), 2);
        query.memo_ttl_ms = 0;
        query.include_reviews = true;
        assert_eq!(scan_usage(&query).unwrap().total_tokens, 500);
    }

    #[test]
    fn opencode_project_filter_matches_indexed_project() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let message_dir = tmp.path().join("storage/message/ses_test");
        std::fs::create_dir_all(&message_dir).expect("create message directory");
        std::fs::write(
            message_dir.join("msg_test.json"),
            serde_json::to_vec(&serde_json::json!({
                "id": "msg_test",
                "sessionID": "ses_test",
                "path": { "cwd": "/repo/memex" },
                "time": { "created": 1_750_000_000_000u64 },
                "tokens": {
                    "input": 10,
                    "output": 5,
                    "reasoning": 0,
                    "cache": { "read": 0, "write": 0 }
                }
            }))
            .expect("serialize message"),
        )
        .expect("write message");
        let _env = EnvVarGuard::set_os(&[("OPENCODE_DATA_DIR", Some(tmp.path().as_os_str()))]);
        let mut query = UsageQuery {
            source: Some(SourceFilter::Opencode),
            project: Some("opencode".into()),
            project_grouping: ProjectGrouping::Flat,
            include_events: true,
            ..UsageQuery::default()
        };

        let matching = scan_usage(&query).expect("scan matching project");
        query.project = Some("memex".into());
        let mismatched = scan_usage(&query).expect("scan mismatched project");
        query.project = Some("opencode".into());
        query.session_keys = Some(HashSet::from([("opencode".into(), "ses_test".into())]));
        let matching_session = scan_usage(&query).expect("scan matching session");
        query.session_keys = Some(HashSet::from([("opencode".into(), "ses_other".into())]));
        let mismatched_session = scan_usage(&query).expect("scan mismatched session");

        assert_eq!(matching.events, 1);
        assert_eq!(matching.details[0].project.as_deref(), Some("opencode"));
        assert_eq!(mismatched.events, 0);
        assert_eq!(matching_session.events, 1);
        assert_eq!(mismatched_session.events, 0);
    }

    #[test]
    fn pi_scanner_uses_configured_session_directory() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let agent_root = tmp.path().join("pi-agent");
        let omp_root = tmp.path().join("omp");
        let session_root = agent_root.join("custom/sessions/--C--Users-alice-Code-memex--");
        std::fs::create_dir_all(&session_root).expect("create session root");
        std::fs::write(
            agent_root.join("settings.json"),
            r#"{ "sessionDir": "custom/sessions" }"#,
        )
        .expect("write settings");
        std::fs::write(
            session_root.join("session.jsonl"),
            concat!(
                r#"{"type":"message","id":"a1","timestamp":"2026-07-03T01:02:05Z","message":{"role":"assistant","provider":"anthropic","model":"claude-sonnet-4-6","usage":{"input":10,"cacheRead":2,"cacheWrite":3,"output":4}}}"#,
                "\n"
            ),
        )
        .expect("write session");
        let _env = EnvVarGuard::set_os(&[
            ("PI_CODING_AGENT_SESSION_DIR", None),
            ("PI_CODING_AGENT_DIR", Some(agent_root.as_os_str())),
            ("PI_CONFIG_DIR", Some(omp_root.as_os_str())),
            ("XDG_DATA_HOME", None),
        ]);
        let report = scan_usage(&UsageQuery {
            source: Some(SourceFilter::Pi),
            project: Some("memex".into()),
            project_grouping: ProjectGrouping::Flat,
            include_events: true,
            ..UsageQuery::default()
        })
        .expect("scan pi");

        assert!(report.warnings.is_empty());
        assert_eq!(report.events, 1);
        assert_eq!(report.details[0].tokens.total(), 19);
        assert_eq!(report.details[0].project.as_deref(), Some("memex"));
        assert!(
            report.details[0]
                .source_path
                .ends_with("custom/sessions/--C--Users-alice-Code-memex--/session.jsonl")
        );
    }

    #[test]
    fn pi_scanner_matches_indexed_header_and_filename_session_ids() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let omp_root = tmp.path().join("omp");
        let session_root = tmp.path().join("--Users-nico-Code-other--");
        std::fs::create_dir_all(&session_root).expect("create session root");

        let filename_id = "11111111-1111-1111-1111-111111111111";
        let header_id = "22222222-2222-2222-2222-222222222222";
        std::fs::write(
            session_root.join(format!("20260703T010203Z_{filename_id}.jsonl")),
            format!(
                concat!(
                    r#"{{"type":"session","id":"{header_id}","cwd":"/Users/nico/Code/memex"}}"#,
                    "\n",
                    r#"{{"type":"message","id":"a1","timestamp":"2026-07-03T01:02:05Z","message":{{"role":"assistant","usage":{{"input":10,"output":4}}}}}}"#,
                    "\n"
                ),
                header_id = header_id,
            ),
        )
        .expect("write header session");

        let fallback_id = "33333333-3333-3333-3333-333333333333";
        let fallback_stem = format!("20260703T010204Z_{fallback_id}");
        std::fs::write(
            session_root.join(format!("{fallback_stem}.jsonl")),
            concat!(
                r#"{"type":"message","id":"a2","timestamp":"2026-07-03T01:02:06Z","message":{"role":"assistant","usage":{"input":20,"output":5}}}"#,
                "\n"
            ),
        )
        .expect("write filename session");

        let _env = EnvVarGuard::set_os(&[
            ("PI_CODING_AGENT_SESSION_DIR", Some(tmp.path().as_os_str())),
            ("PI_CODING_AGENT_DIR", None),
            ("PI_CONFIG_DIR", Some(omp_root.as_os_str())),
            ("XDG_DATA_HOME", None),
        ]);
        let mut query = UsageQuery {
            source: Some(SourceFilter::Pi),
            include_events: true,
            ..UsageQuery::default()
        };

        query.session_keys = Some(HashSet::from([("pi".into(), header_id.into())]));
        let header = scan_usage(&query).expect("scan header session");
        query.session_keys = Some(HashSet::from([("pi".into(), filename_id.into())]));
        let overridden_filename = scan_usage(&query).expect("scan overridden filename session");
        query.session_keys = Some(HashSet::from([("pi".into(), fallback_id.into())]));
        let fallback = scan_usage(&query).expect("scan filename session");
        query.session_keys = Some(HashSet::from([("pi".into(), fallback_stem)]));
        let full_stem = scan_usage(&query).expect("scan full filename stem");

        assert_eq!(header.events, 1);
        assert_eq!(header.details[0].session_id.as_deref(), Some(header_id));
        assert_eq!(header.details[0].project.as_deref(), Some("memex"));
        assert_eq!(overridden_filename.events, 0);
        assert_eq!(fallback.events, 1);
        assert_eq!(fallback.details[0].session_id.as_deref(), Some(fallback_id));
        assert_eq!(full_stem.events, 0);
    }
}
