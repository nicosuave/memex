use super::{ParserVersions, SourceFile};
use crate::types::SourceKind;
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::{Context, Result, anyhow};
use chrono::DateTime;
use rusqlite::{Connection, OpenFlags, types::Value};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 1,
    index: 1,
    usage: 4,
};

pub fn matches_path(path: &str) -> bool {
    path.ends_with("/.hermes/state.db")
        || path.contains("/.hermes/profiles/") && path.ends_with("/state.db")
        || path.ends_with("\\.hermes\\state.db")
        || path.contains("\\.hermes\\profiles\\") && path.ends_with("\\state.db")
}

pub fn discover() -> Vec<SourceFile> {
    discover_from_roots(&profile_roots())
}

pub fn profile_roots() -> Vec<PathBuf> {
    let home = super::common::home();
    let explicit_roots = std::env::var_os("HERMES_PROFILE_ROOTS");
    let hermes_home = std::env::var_os("HERMES_HOME").map(PathBuf::from);
    let state_dir = std::env::var_os("HERMES_STATE_DIR").map(PathBuf::from);
    let xdg_state_home = std::env::var_os("XDG_STATE_HOME").map(PathBuf::from);
    profile_roots_for(
        &home,
        explicit_roots.as_deref(),
        hermes_home.as_deref(),
        state_dir.as_deref(),
        xdg_state_home.as_deref(),
    )
}

fn profile_roots_for(
    home: &Path,
    explicit_roots: Option<&std::ffi::OsStr>,
    hermes_home: Option<&Path>,
    state_dir: Option<&Path>,
    xdg_state_home: Option<&Path>,
) -> Vec<PathBuf> {
    if let Some(roots) = explicit_roots {
        return split_roots(roots);
    }
    let mut roots = vec![home.join(".hermes"), home.join(".local/state/hermes")];
    if let Some(xdg) = xdg_state_home {
        roots.push(xdg.join("hermes"));
    }
    if let Some(hermes_home) = hermes_home {
        roots.push(hermes_home.to_path_buf());
    }
    if let Some(state_dir) = state_dir {
        roots.push(state_dir.to_path_buf());
    }
    roots
}

fn split_roots(roots: &std::ffi::OsStr) -> Vec<PathBuf> {
    roots
        .to_string_lossy()
        .split(',')
        .filter_map(|root| {
            let root = root.trim();
            (!root.is_empty()).then(|| PathBuf::from(root))
        })
        .collect()
}

pub fn discover_from_roots(roots: &[PathBuf]) -> Vec<SourceFile> {
    let mut candidates = Vec::new();
    for root in roots {
        if root.is_file() {
            if is_state_db(root) {
                candidates.push(root.clone());
            }
            continue;
        }
        if !root.is_dir() {
            continue;
        }
        if root.file_name().and_then(|v| v.to_str()) == Some("profiles") {
            add_profile_children(root, &mut candidates);
        } else {
            add_state(root, &mut candidates);
            add_profile_children(&root.join("profiles"), &mut candidates);
        }
    }
    let mut seen = HashSet::new();
    candidates.retain(|path| seen.insert(path.canonicalize().unwrap_or_else(|_| path.clone())));
    candidates.sort();
    candidates
        .into_iter()
        .map(|path| SourceFile {
            source: SourceKind::Hermes,
            path,
        })
        .collect()
}

fn add_state(root: &Path, out: &mut Vec<PathBuf>) {
    let path = root.join("state.db");
    if is_state_db(&path) {
        out.push(path);
    }
}

fn add_profile_children(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            add_state(&entry.path(), out);
        }
    }
}

fn is_state_db(path: &Path) -> bool {
    path.file_name().and_then(|v| v.to_str()) == Some("state.db")
        && !path.components().any(|component| {
            matches!(
                component.as_os_str().to_str(),
                Some(
                    "snapshots"
                        | "snapshot"
                        | "backups"
                        | "backup"
                        | "upgrades"
                        | "upgrade"
                        | "sandbox"
                        | "sandboxes"
                        | "repo"
                        | "repos"
                        | "cron"
                        | "kanban"
                        | "verification"
                )
            )
        })
}

type Row = HashMap<String, Value>;

pub(crate) fn parse_usage_file(path: &Path) -> Result<crate::sources::UsageParseOutput> {
    let wal = PathBuf::from(format!("{}-wal", path.to_string_lossy()));
    let wal_before = crate::sources::UsageDependency::from_path_or_absent(&wal);
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open Hermes SQLite database {}", path.display()))?;
    let tables = table_names(&conn)?;
    if !tables.contains("sessions") {
        return Err(anyhow!("Hermes database has no sessions table"));
    }
    let sessions = rows(
        &conn,
        "sessions",
        &[
            "id",
            "model",
            "started_at",
            "ended_at",
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "reasoning_tokens",
            "billing_provider",
            "billing_mode",
            "estimated_cost_usd",
            "actual_cost_usd",
            "cost_status",
            "cost_source",
            "cwd",
            "git_repo_root",
            "profile_name",
        ],
        &["id", "started_at", "ended_at", "model", "profile_name"],
    )?;
    let model_rows = if tables.contains("session_model_usage") {
        rows(
            &conn,
            "session_model_usage",
            &[
                "session_id",
                "model",
                "billing_provider",
                "billing_base_url",
                "billing_mode",
                "task",
                "api_call_count",
                "input_tokens",
                "output_tokens",
                "cache_read_tokens",
                "cache_write_tokens",
                "reasoning_tokens",
                "estimated_cost_usd",
                "actual_cost_usd",
                "cost_status",
                "cost_source",
                "first_seen",
                "last_seen",
            ],
            &[
                "session_id",
                "model",
                "task",
                "billing_provider",
                "first_seen",
                "last_seen",
            ],
        )?
    } else {
        Vec::new()
    };
    let mut by_session: HashMap<String, Vec<&Row>> = HashMap::new();
    for row in &model_rows {
        if let Some(id) = text(row, "session_id") {
            by_session.entry(id).or_default().push(row);
        }
    }
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let mut events = Vec::new();
    for session in &sessions {
        let Some(id) = text(session, "id") else {
            continue;
        };
        let aggregate = buckets(session);
        let Some(model_rows) = by_session.get(&id) else {
            if !is_zero(&aggregate) {
                events.push(event(
                    &source_path,
                    &id,
                    Some(&id),
                    session,
                    aggregate,
                    cost(session),
                    true,
                    0,
                ));
            }
            continue;
        };
        let mut summed = TokenBuckets::default();
        let mut invalid_model = false;
        let model_event_start = events.len();
        for row in model_rows {
            let raw_current = buckets(row);
            let current = cap_to_remaining(raw_current.clone(), aggregate.clone(), summed.clone());
            let inconsistent = current != raw_current;
            invalid_model |= inconsistent;
            summed = add(summed, &current);
            if !is_zero(&current) {
                let model = text(row, "model").or_else(|| text(session, "model"));
                let task = text(row, "task").unwrap_or_else(|| "default".to_string());
                let identity = serde_json::to_string(&(
                    id.as_str(),
                    model.as_deref().unwrap_or("unknown"),
                    task.as_str(),
                    text(session, "profile_name").unwrap_or_default(),
                ))
                .expect("tuple serialization cannot fail");
                let id = format!("model:{identity}");
                events.push(model_event(
                    &source_path,
                    &id,
                    &text(row, "session_id"),
                    row,
                    session,
                    model,
                    current,
                    (!inconsistent).then(|| cost(row)).flatten(),
                    inconsistent,
                    timestamp(row).max(timestamp(session)),
                ));
            }
        }
        let residual = subtract(aggregate, summed);
        if invalid_model {
            for event in &mut events[model_event_start..] {
                event.source_cost_usd = None;
            }
        }
        let source_cost = if invalid_model {
            cost(session)
        } else {
            residual_cost(session, model_rows)
        };
        if !is_zero(&residual) || invalid_model && source_cost.is_some() {
            events.push(event(
                &source_path,
                &format!("session:{id}:residual"),
                Some(&id),
                session,
                residual,
                source_cost,
                true,
                timestamp(session),
            ));
        }
    }
    let wal_after = crate::sources::UsageDependency::from_path_or_absent(&wal);
    Ok(crate::sources::UsageParseOutput {
        events,
        cacheable: cacheable_after_wal_read(&wal_before, &wal_after),
        // SQLite WAL pages can contain committed data not yet checkpointed into the main
        // database. The SHM file is coordination metadata and must not make scans stale.
        deps: vec![wal_after],
    })
}

fn table_names(conn: &Connection) -> Result<HashSet<String>> {
    let mut statement = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
    Ok(statement
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(Result::ok)
        .collect())
}

fn rows(conn: &Connection, table: &str, wanted: &[&str], order_by: &[&str]) -> Result<Vec<Row>> {
    let columns = table_columns(conn, table)?;
    let selected: Vec<&str> = wanted
        .iter()
        .copied()
        .filter(|column| columns.contains(*column))
        .collect();
    if selected.is_empty() {
        return Ok(Vec::new());
    }
    let order = order_by
        .iter()
        .filter(|column| columns.contains(**column))
        .map(|column| format!("\"{column}\""));
    let order = order.collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT {} FROM {}{}",
        selected
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect::<Vec<_>>()
            .join(","),
        table,
        if order.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {order}")
        }
    );
    let mut statement = conn.prepare(&sql)?;
    let mut result = Vec::new();
    let mut query = statement.query([])?;
    while let Some(row) = query.next()? {
        let mut values = Row::new();
        for (index, column) in selected.iter().enumerate() {
            values.insert((*column).to_string(), row.get(index)?);
        }
        result.push(values);
    }
    Ok(result)
}

fn table_columns(conn: &Connection, table: &str) -> Result<HashSet<String>> {
    let sql = format!("PRAGMA table_info(\"{table}\")");
    let mut statement = conn.prepare(&sql)?;
    Ok(statement
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(Result::ok)
        .collect())
}

fn number(row: &Row, key: &str) -> u64 {
    match row.get(key) {
        Some(Value::Integer(value)) => (*value).max(0) as u64,
        Some(Value::Real(value)) if *value >= 0.0 => *value as u64,
        _ => 0,
    }
}
fn text(row: &Row, key: &str) -> Option<String> {
    match row.get(key) {
        Some(Value::Text(value)) => Some(value.clone()),
        Some(Value::Integer(value)) => Some(value.to_string()),
        _ => None,
    }
}
fn cost(row: &Row) -> Option<f64> {
    let valid = |value: Option<&Value>| match value {
        Some(Value::Real(value)) if value.is_finite() && *value >= 0.0 => Some(*value),
        Some(Value::Integer(value)) if *value >= 0 => Some(*value as f64),
        _ => None,
    };
    valid(row.get("actual_cost_usd")).or_else(|| valid(row.get("estimated_cost_usd")))
}
fn timestamp(row: &Row) -> u64 {
    let numeric_timestamp = |key: &str| match row.get(key) {
        Some(Value::Integer(value)) => timestamp_number(*value as f64),
        Some(Value::Real(value)) => timestamp_number(*value),
        _ => 0,
    };
    let text_timestamp = |key: &str| {
        text(row, key).and_then(|value| {
            DateTime::parse_from_rfc3339(&value)
                .ok()
                .map(|date| date.timestamp_millis().max(0) as u64)
        })
    };
    numeric_timestamp("last_seen")
        .max(numeric_timestamp("first_seen"))
        .max(numeric_timestamp("ended_at"))
        .max(numeric_timestamp("started_at"))
        .max(text_timestamp("last_seen").unwrap_or(0))
        .max(text_timestamp("first_seen").unwrap_or(0))
        .max(text_timestamp("ended_at").unwrap_or(0))
        .max(text_timestamp("started_at").unwrap_or(0))
}
fn timestamp_number(value: f64) -> u64 {
    if !value.is_finite() || value < 0.0 {
        return 0;
    }
    let millis = if value < 10_000_000_000.0 {
        value * 1000.0
    } else {
        value
    };
    millis.round().min(u64::MAX as f64) as u64
}
fn buckets(row: &Row) -> TokenBuckets {
    let input = number(row, "input_tokens");
    let cache_read = number(row, "cache_read_tokens");
    let cache_write = number(row, "cache_write_tokens");
    let output = number(row, "output_tokens");
    let mut buckets = TokenBuckets::disjoint(input, cache_read, cache_write, output);
    buckets.reasoning = number(row, "reasoning_tokens").min(output);
    buckets
}
fn add(a: TokenBuckets, b: &TokenBuckets) -> TokenBuckets {
    TokenBuckets {
        raw_input: a.raw_input.saturating_add(b.raw_input),
        uncached_input: a.uncached_input.saturating_add(b.uncached_input),
        cache_read: a.cache_read.saturating_add(b.cache_read),
        cache_write: a.cache_write.saturating_add(b.cache_write),
        cache_write_1h: 0,
        output: a.output.saturating_add(b.output),
        reasoning: a.reasoning.saturating_add(b.reasoning),
    }
}
fn is_zero(value: &TokenBuckets) -> bool {
    value.raw_input == 0
        && value.uncached_input == 0
        && value.output == 0
        && value.cache_write == 0
        && value.cache_read == 0
        && value.reasoning == 0
}
fn subtract(a: TokenBuckets, b: TokenBuckets) -> TokenBuckets {
    TokenBuckets {
        raw_input: a.raw_input.saturating_sub(b.raw_input),
        uncached_input: a.uncached_input.saturating_sub(b.uncached_input),
        cache_read: a.cache_read.saturating_sub(b.cache_read),
        cache_write: a.cache_write.saturating_sub(b.cache_write),
        cache_write_1h: 0,
        output: a.output.saturating_sub(b.output),
        reasoning: a.reasoning.saturating_sub(b.reasoning),
    }
}
fn cap_to_remaining(
    value: TokenBuckets,
    aggregate: TokenBuckets,
    used: TokenBuckets,
) -> TokenBuckets {
    let cap = |total: u64, already: u64, current: u64| current.min(total.saturating_sub(already));
    TokenBuckets {
        raw_input: cap(aggregate.raw_input, used.raw_input, value.raw_input),
        uncached_input: cap(
            aggregate.uncached_input,
            used.uncached_input,
            value.uncached_input,
        ),
        cache_read: cap(aggregate.cache_read, used.cache_read, value.cache_read),
        cache_write: cap(aggregate.cache_write, used.cache_write, value.cache_write),
        cache_write_1h: 0,
        output: cap(aggregate.output, used.output, value.output),
        reasoning: cap(aggregate.reasoning, used.reasoning, value.reasoning),
    }
}
fn residual_cost(session: &Row, rows: &[&Row]) -> Option<f64> {
    let total = cost(session)?;
    let used: f64 = rows.iter().filter_map(|row| cost(row)).sum();
    (total > used).then_some(total - used)
}

fn cacheable_after_wal_read(
    wal_before: &crate::sources::UsageDependency,
    wal_after: &crate::sources::UsageDependency,
) -> bool {
    wal_before == wal_after
}
#[allow(clippy::too_many_arguments)]
fn event(
    path: &Arc<str>,
    record: &str,
    session_id: Option<&str>,
    row: &Row,
    tokens: TokenBuckets,
    source_cost_usd: Option<f64>,
    conservative: bool,
    order: u64,
) -> UsageEvent {
    UsageEvent {
        source: "hermes",
        source_path: path.clone(),
        source_record_id: Some(record.to_string()),
        session_id: session_id.map(str::to_string),
        request_id: None,
        message_id: None,
        timestamp_ms: timestamp(row),
        project: text(row, "cwd").or_else(|| text(row, "git_repo_root")),
        provider: text(row, "billing_provider"),
        model: text(row, "model"),
        tokens,
        source_cost_usd,
        dedupe_confidence: "strong",
        conservative_undercount: conservative,
        sidechain: false,
        source_order: order,
    }
}
#[allow(clippy::too_many_arguments)]
fn model_event(
    path: &Arc<str>,
    record: &str,
    session_id: &Option<String>,
    row: &Row,
    session: &Row,
    model: Option<String>,
    tokens: TokenBuckets,
    source_cost_usd: Option<f64>,
    conservative: bool,
    order: u64,
) -> UsageEvent {
    let mut event = event(
        path,
        record,
        session_id.as_deref(),
        row,
        tokens,
        source_cost_usd,
        conservative,
        order,
    );
    event.model = model;
    event.provider = text(row, "billing_provider").or_else(|| text(session, "billing_provider"));
    event.project = text(session, "cwd").or_else(|| text(session, "git_repo_root"));
    event
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::fs;

    fn db(path: &Path, model_usage: bool) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch("CREATE TABLE sessions (id TEXT, model TEXT, started_at INTEGER, input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER, billing_provider TEXT, estimated_cost_usd REAL, cwd TEXT, git_repo_root TEXT, profile_name TEXT); CREATE TABLE messages (content TEXT, system_prompt TEXT);").unwrap();
        if model_usage {
            conn.execute_batch("CREATE TABLE session_model_usage (session_id TEXT, model TEXT, billing_provider TEXT, task TEXT, input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER, estimated_cost_usd REAL, first_seen INTEGER);").unwrap();
        }
    }

    #[test]
    fn discovery_only_returns_canonical_root_and_profile_databases() {
        let temp = tempfile::tempdir().unwrap();
        for path in [
            "state.db",
            "profiles/a/state.db",
            "profiles/b/state.db",
            "snapshots/state.db",
            "nested/state.db",
            "sessions/old.jsonl",
        ] {
            let path = temp.path().join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"x").unwrap();
        }
        let files =
            discover_from_roots(&[temp.path().to_path_buf(), temp.path().join("profiles/a")]);
        assert_eq!(
            files
                .iter()
                .map(|f| f
                    .path
                    .strip_prefix(temp.path())
                    .unwrap()
                    .to_string_lossy()
                    .to_string())
                .collect::<Vec<_>>(),
            vec!["profiles/a/state.db", "profiles/b/state.db", "state.db"]
        );
    }

    #[test]
    fn legacy_sessions_are_aggregates_and_ignore_transcript_tables() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        db(&path, false);
        let conn = Connection::open(&path).unwrap();
        conn.execute("INSERT INTO sessions VALUES ('s','m',1000,100,20,30,4,5,'p',1.25,'/work','/repo','prof')", []).unwrap();
        drop(conn);
        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.events.len(), 1);
        assert_eq!(events.events[0].tokens.uncached_input, 100);
        assert_eq!(events.events[0].tokens.cache_read, 30);
        assert_eq!(events.events[0].tokens.reasoning, 5);
        assert_eq!(events.events[0].session_id.as_deref(), Some("s"));
        assert_eq!(events.events[0].source_cost_usd, Some(1.25));
    }

    #[test]
    fn database_paths_with_uri_metacharacters_open_read_only() {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("profile#100%");
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("state?db");
        db(&path, false);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
            .unwrap();
        conn.execute(
            "INSERT INTO sessions VALUES ('s','m',1000,1,0,0,0,0,'p',NULL,'/work','/repo','prof')",
            [],
        )
        .unwrap();

        let parsed = parse_usage_file(&path).unwrap();
        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].tokens.raw_input, 1);
        drop(conn);
    }

    #[test]
    fn model_rows_count_once_and_residuals_are_non_negative() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        db(&path, true);
        let conn = Connection::open(&path).unwrap();
        conn.execute("INSERT INTO sessions VALUES ('s','fallback',1000,100,20,30,4,5,'p',10.0,'/work','/repo','prof')", []).unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','m','p','task',60,10,20,2,3,6.0,1000)",
            [],
        )
        .unwrap();
        drop(conn);
        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.events.len(), 2);
        assert_eq!(
            events.events[0].tokens.raw_input + events.events[1].tokens.raw_input,
            100
        );
        assert!(
            events
                .events
                .iter()
                .any(|event| event.conservative_undercount)
        );
    }

    #[test]
    fn inconsistent_model_costs_use_one_authoritative_session_fallback() {
        for (index, (aggregate_input, first_input, second_input, first_cost, residual)) in [
            (10, 4, 10, 12.0, 0), // zero residual, retained model cost over aggregate
            (10, 4, 10, 6.0, 0),  // zero residual, retained model cost under aggregate
            (10, 4, 1, 12.0, 5),  // positive residual, retained model cost over aggregate
            (10, 4, 1, 6.0, 5),   // positive residual, retained model cost under aggregate
        ]
        .into_iter()
        .enumerate()
        {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(format!("cost-{index}.db"));
            db(&path, true);
            let conn = Connection::open(&path).unwrap();
            conn.execute(
                "INSERT INTO sessions VALUES ('s','fallback',1000,?1,0,0,0,0,'p',10.0,'/work','/repo','prof')",
                [aggregate_input],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO session_model_usage VALUES ('s','m1','p','task-1',?1,0,0,0,0,?2,1000)",
                rusqlite::params![first_input, first_cost],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO session_model_usage VALUES ('s','m2','p','task-2',?1,?2,0,0,0,3.0,1000)",
                rusqlite::params![second_input, 1],
            )
            .unwrap();
            drop(conn);

            let events = parse_usage_file(&path).unwrap().events;
            assert!(events.iter().all(|event| {
                !event
                    .source_record_id
                    .as_deref()
                    .unwrap()
                    .starts_with("model:")
                    || event.source_cost_usd.is_none()
            }));
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.source_record_id.as_deref() == Some("session:s:residual"))
                    .count(),
                1
            );
            assert_eq!(
                events
                    .iter()
                    .find(|event| event.source_record_id.as_deref() == Some("session:s:residual"))
                    .unwrap()
                    .tokens
                    .raw_input,
                residual
            );
            assert_eq!(
                events
                    .iter()
                    .filter_map(|event| event.source_cost_usd)
                    .sum::<f64>(),
                10.0
            );
        }
    }

    #[test]
    fn malformed_database_is_an_error_and_jsonl_is_not_discovered() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        fs::write(&path, b"not sqlite").unwrap();
        assert!(parse_usage_file(&path).is_err());
        fs::write(temp.path().join("usage.jsonl"), b"{}\n").unwrap();
        assert_eq!(discover_from_roots(&[temp.path().to_path_buf()]).len(), 1);
    }

    #[test]
    fn numeric_timestamps_are_epoch_milliseconds_and_sidecar_absence_is_tracked() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        db(&path, false);
        let conn = Connection::open(&path).unwrap();
        conn.execute("INSERT INTO sessions VALUES ('s','m',1720000000.125,1,1,0,0,0,'p',NULL,'/private','/private','profile')", []).unwrap();
        drop(conn);
        let parsed = parse_usage_file(&path).unwrap();
        assert_eq!(parsed.events[0].timestamp_ms, 1_720_000_000_125);
        assert_eq!(parsed.deps.len(), 1);
        assert!(parsed.deps.iter().all(|dependency| !dependency.exists));
        let wal = PathBuf::from(format!("{}-wal", path.to_string_lossy()));
        fs::write(&wal, b"wal").unwrap();
        let current = crate::sources::UsageDependency::from_path_or_absent(&wal);
        assert!(current.exists);
        assert!(
            !parsed
                .deps
                .iter()
                .any(|dependency| dependency.path == current.path && dependency.is_current())
        );
    }

    #[test]
    fn wal_change_during_read_makes_parse_output_non_cacheable() {
        let before = crate::sources::UsageDependency {
            path: "state.db-wal".to_string(),
            size: 10,
            mtime_ns: 1,
            exists: true,
        };
        let after = crate::sources::UsageDependency {
            size: 20,
            ..before.clone()
        };
        assert!(!cacheable_after_wal_read(&before, &after));
        assert!(cacheable_after_wal_read(&before, &before));
    }

    #[test]
    fn model_identity_is_encoded_and_ordered_by_logical_dimensions() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        db(&path, true);
        let conn = Connection::open(&path).unwrap();
        conn.execute("INSERT INTO sessions VALUES ('s','fallback',1720000000,10,2,0,0,0,'p',3.0,'/private','/private','profile')", []).unwrap();
        conn.execute("INSERT INTO session_model_usage VALUES ('s','model:z','p','task:b',5,1,0,0,0,1.0,1720000000)", []).unwrap();
        conn.execute("INSERT INTO session_model_usage VALUES ('s','model:a','p','task:a',5,1,0,0,0,1.0,1720000000)", []).unwrap();
        drop(conn);
        let events = parse_usage_file(&path).unwrap().events;
        assert_eq!(events.len(), 2);
        assert!(
            events[0]
                .source_record_id
                .as_deref()
                .unwrap()
                .contains("model:a")
        );
        assert!(
            events[1]
                .source_record_id
                .as_deref()
                .unwrap()
                .contains("task:b")
        );
        assert_ne!(events[0].source_record_id, events[1].source_record_id);
    }

    #[test]
    fn disjoint_buckets_preserve_cache_beyond_input_and_reconcile_every_bucket() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.db");
        db(&path, true);
        let conn = Connection::open(&path).unwrap();
        conn.execute("INSERT INTO sessions VALUES ('s','fallback',1000,10,3,100,7,2,'p',NULL,'/work','/repo','prof')", []).unwrap();
        conn.execute(
            "INSERT INTO session_model_usage VALUES ('s','m','p','task',10,3,100,7,2,NULL,1000)",
            [],
        )
        .unwrap();
        drop(conn);
        let events = parse_usage_file(&path).unwrap().events;
        assert_eq!(events.len(), 1);
        let tokens = &events[0].tokens;
        assert_eq!(tokens.raw_input, 10);
        assert_eq!(tokens.uncached_input, 10);
        assert_eq!(tokens.cache_read, 100);
        assert_eq!(tokens.cache_write, 7);
        assert_eq!(tokens.output, 3);
        assert_eq!(tokens.reasoning, 2);
        assert_eq!(tokens.total(), 120);
    }

    #[test]
    fn default_discovery_keeps_all_profiles_when_home_points_at_one_profile() {
        let temp = tempfile::tempdir().unwrap();
        for relative in [
            ".hermes/state.db",
            ".hermes/profiles/main/state.db",
            ".hermes/profiles/builder/state.db",
            ".hermes/profiles/researcher/state.db",
        ] {
            let path = temp.path().join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"x").unwrap();
        }
        let roots = profile_roots_for(
            temp.path(),
            None,
            Some(&temp.path().join(".hermes/profiles/main")),
            None,
            None,
        );
        let files = discover_from_roots(&roots);
        assert_eq!(files.len(), 4);
        assert_eq!(
            files
                .iter()
                .map(|file| file.path.strip_prefix(temp.path()).unwrap().to_path_buf())
                .collect::<Vec<_>>(),
            [
                PathBuf::from(".hermes/profiles/builder/state.db"),
                PathBuf::from(".hermes/profiles/main/state.db"),
                PathBuf::from(".hermes/profiles/researcher/state.db"),
                PathBuf::from(".hermes/state.db"),
            ]
        );
    }

    #[test]
    fn aggregate_fixture_matrix_matches_each_database_sessions_exactly_once() {
        let temp = tempfile::tempdir().unwrap();
        let expected = [
            (10, 100, 7, 3, 2),
            (20, 30, 4, 5, 1),
            (6, 8, 2, 9, 4),
            (11, 13, 1, 2, 0),
        ];
        let mut total = TokenBuckets::default();
        for (index, (input, cache_read, cache_write, output, reasoning)) in
            expected.iter().enumerate()
        {
            let path = temp.path().join(format!("db-{index}.db"));
            db(&path, true);
            let conn = Connection::open(&path).unwrap();
            conn.execute("INSERT INTO sessions VALUES ('s','m',1000,?1,?2,?3,?4,?5,'p',NULL,'/work','/repo','prof')", rusqlite::params![input, output, cache_read, cache_write, reasoning]).unwrap();
            conn.execute("INSERT INTO session_model_usage VALUES ('s','m','p','task',?1,?2,?3,?4,?5,NULL,1000)", rusqlite::params![input, output, cache_read, cache_write, reasoning]).unwrap();
            drop(conn);
            let events = parse_usage_file(&path).unwrap().events;
            let mut actual = TokenBuckets::default();
            for event in events {
                actual = add(actual, &event.tokens);
            }
            let session = TokenBuckets::disjoint(*input, *cache_read, *cache_write, *output);
            let mut session = session;
            session.reasoning = *reasoning;
            assert_eq!(actual, session);
            total = add(total, &actual);
        }
        assert_eq!(total.raw_input, 47);
        assert_eq!(total.uncached_input, 47);
        assert_eq!(total.cache_read, 151);
        assert_eq!(total.cache_write, 14);
        assert_eq!(total.output, 19);
        assert_eq!(total.reasoning, 7);
        assert_eq!(total.total(), 231);
    }

    #[test]
    fn reconciliation_has_independent_positive_residual_and_overaggregate_caps() {
        let cases = [
            ((10, 100, 7, 3, 2), (4, 60, 2, 1, 1), (6, 40, 5, 2, 1)),
            ((10, 100, 7, 3, 2), (20, 200, 10, 5, 4), (0, 0, 0, 0, 0)),
        ];
        for (index, (session, model, residual)) in cases.into_iter().enumerate() {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join(format!("case-{index}.db"));
            db(&path, true);
            let conn = Connection::open(&path).unwrap();
            conn.execute("INSERT INTO sessions VALUES ('s','m',1000,?1,?2,?3,?4,?5,'p',NULL,'/work','/repo','prof')", rusqlite::params![session.0, session.3, session.1, session.2, session.4]).unwrap();
            conn.execute("INSERT INTO session_model_usage VALUES ('s','m','p','task',?1,?2,?3,?4,?5,NULL,1000)", rusqlite::params![model.0, model.3, model.1, model.2, model.4]).unwrap();
            drop(conn);
            let events = parse_usage_file(&path).unwrap().events;
            assert_eq!(events.len(), if residual.0 == 0 { 1 } else { 2 });
            let mut actual = TokenBuckets::default();
            for event in events {
                actual = add(actual, &event.tokens);
            }
            let expected = TokenBuckets {
                raw_input: session.0,
                uncached_input: session.0,
                cache_read: session.1,
                cache_write: session.2,
                cache_write_1h: 0,
                output: session.3,
                reasoning: session.4,
            };
            assert_eq!(actual, expected);
            assert_eq!(
                actual.total(),
                session.0 + session.1 + session.2 + session.3
            );
        }
    }

    #[test]
    fn explicit_profile_roots_are_the_only_discovery_scope() {
        let temp = tempfile::tempdir().unwrap();
        let explicit = temp.path().join("explicit");
        let other = temp.path().join("other");
        for path in [explicit.join("state.db"), other.join("state.db")] {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, b"x").unwrap();
        }
        let roots = profile_roots_for(
            temp.path(),
            Some(explicit.as_os_str()),
            Some(&other),
            Some(&other),
            Some(&other),
        );
        assert_eq!(roots, vec![explicit]);
    }
}
