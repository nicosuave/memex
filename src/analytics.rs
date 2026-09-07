use crate::state::SessionScope;
use crate::types::{
    Record, SourceFilter, SourceKind, jcode_text_is_subagent_directive,
    jcode_tmp_cwd_is_worker_sandbox,
};
use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params, params_from_iter};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const SCHEMA_VERSION: i64 = 6;
const GIT_METADATA_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_LABEL_CHARS: usize = 150;
pub const UNFILED_PROJECT: &str = "Unfiled";
const REPOSITORY_PROJECT_SQL: &str = "COALESCE(NULLIF(repo_project, ''), 'Unfiled')";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectGrouping {
    #[default]
    Flat,
    Repository,
}

#[derive(Clone, Debug)]
pub struct SessionRow {
    pub source: SourceKind,
    pub session_id: String,
    pub source_path: String,
    pub project: String,
    pub display_project: String,
    pub cwd: Option<String>,
    pub last_at: u64,
    pub message_count: u64,
    pub label: Option<String>,
    pub conversation_kind: Option<String>,
}

/// Full-index repository totals, keyed exactly like the sessions project filter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectSummary {
    pub project: String,
    pub session_count: u64,
    pub last_at: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionKindFilter {
    Primary,
    Subagent,
    #[default]
    Regular,
    All,
}

impl SessionKindFilter {
    /// Shared origin predicate. Regular includes ordinary sessions of every
    /// kind; permission reviews require the explicit All filter.
    pub fn matches_kind(self, kind: Option<&str>) -> bool {
        match self {
            SessionKindFilter::All => true,
            SessionKindFilter::Regular => kind != Some("guardian_review"),
            SessionKindFilter::Primary => kind.is_none() || kind == Some("main"),
            SessionKindFilter::Subagent => {
                kind.is_some() && kind != Some("main") && kind != Some("guardian_review")
            }
        }
    }
}

/// A session row with every stored column, for `memex sessions`.
#[derive(Clone, Debug, Serialize)]
pub struct SessionDetailRow {
    pub source: SourceKind,
    pub session_id: String,
    pub source_path: String,
    pub project: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo_project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_root: Option<String>,
    pub started_at: u64,
    pub last_at: u64,
    pub message_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_kind: Option<String>,
}

pub struct AnalyticsStore {
    conn: Connection,
}

pub struct AnalyticsWriter {
    store: AnalyticsStore,
    sessions: HashMap<SessionKey, SessionAccumulator>,
    metadata_cache: HashMap<SessionKey, SessionMetadata>,
    git_cache: HashMap<String, GitMetadata>,
    cwd_overrides: HashMap<SessionKey, String>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SessionKey {
    source: SourceKind,
    session_id: String,
    source_path: String,
}

#[derive(Clone, Debug)]
struct SessionAccumulator {
    key: SessionKey,
    project: String,
    started_at: u64,
    last_at: u64,
    message_count: u64,
    first_user_text: Option<String>,
    conversation_kind: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct SessionMetadata {
    pub cwd: Option<String>,
    pub git_root: Option<String>,
    pub git_common_dir: Option<String>,
    pub repo_project: Option<String>,
    pub resolution_status: String,
}

impl AnalyticsStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(2))?;
        let store = Self { conn };
        store.init()?;
        Ok(store)
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(Duration::from_secs(2))?;
        conn.pragma_update(None, "query_only", true)?;
        Ok(Self { conn })
    }

    fn init(&self) -> Result<()> {
        // Ingest state advances only after analytics commits. FULL keeps each WAL commit durable
        // before the cross-store publication can clear its recovery marker.
        self.conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = FULL;
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                source TEXT NOT NULL,
                session_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                project TEXT NOT NULL,
                cwd TEXT,
                git_root TEXT,
                git_common_dir TEXT,
                repo_project TEXT,
                started_at INTEGER NOT NULL,
                last_at INTEGER NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0,
                resolution_status TEXT NOT NULL DEFAULT '',
                label TEXT,
                conversation_kind TEXT,
                PRIMARY KEY (source, session_id, source_path)
            );
            CREATE INDEX IF NOT EXISTS sessions_last_at_idx ON sessions(last_at);
            CREATE INDEX IF NOT EXISTS sessions_project_last_at_idx ON sessions(project, last_at);
            CREATE INDEX IF NOT EXISTS sessions_repo_project_last_at_idx ON sessions(repo_project, last_at);
            DROP INDEX IF EXISTS sessions_display_project_last_at_idx;
            CREATE INDEX IF NOT EXISTS sessions_repository_project_last_at_idx
                ON sessions(COALESCE(NULLIF(repo_project, ''), 'Unfiled'), last_at);
            CREATE INDEX IF NOT EXISTS sessions_source_last_at_idx ON sessions(source, last_at);
            "#,
        )?;
        // Additive migrations for existing databases: ignore duplicate-column errors.
        for sql in [
            "ALTER TABLE sessions ADD COLUMN label TEXT",
            "ALTER TABLE sessions ADD COLUMN conversation_kind TEXT",
        ] {
            let _ = self.conn.execute(sql, []);
        }
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS sessions_conversation_kind_idx ON sessions(conversation_kind);
             CREATE INDEX IF NOT EXISTS sessions_label_idx ON sessions(label);",
        )?;
        let previous_schema_version: Option<i64> = self
            .conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .and_then(|value| value.parse().ok());
        if previous_schema_version != Some(SCHEMA_VERSION) {
            self.conn
                .execute("DELETE FROM meta WHERE key = 'analytics_complete'", [])?;
            // Labels generated before the system-tag stripper was complete contained raw
            // system wrappers and truncated prefixes. Clear them so the next backfill
            // recomputes with comprehensive stripping and suffix-preserving truncation.
            let _ = self.conn.execute(
                "UPDATE sessions SET label = NULL WHERE \
                 label LIKE '%<system-reminder>%' OR \
                 label LIKE '%<command-message>%' OR \
                 label LIKE '%<command-name>%' OR \
                 label LIKE '%<INSTRUCTIONS>%' OR \
                 label LIKE '%<environment_context>%' OR \
                 label LIKE '%<recommended_plugins>%' OR \
                 label LIKE '%<user_instructions>%' OR \
                 label LIKE '%<skill>%' OR \
                 label LIKE '%<%' OR \
                 label LIKE '# AGENTS.md%' OR \
                 label LIKE 'You are a reminder observer%'",
                [],
            );
        }
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES('schema_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![SCHEMA_VERSION.to_string()],
        )?;
        Ok(())
    }

    pub fn session_count(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))?;
        Ok(count.max(0) as u64)
    }

    pub fn is_ready(path: impl AsRef<Path>) -> bool {
        Self::open_read_only(path)
            .and_then(|store| store.session_count())
            .map(|count| count > 0)
            .unwrap_or(false)
    }

    pub fn is_complete(path: impl AsRef<Path>) -> bool {
        Self::open_read_only(path)
            .and_then(|store| store.complete())
            .unwrap_or(false)
    }

    pub fn complete(&self) -> Result<bool> {
        let value: Option<String> = self
            .conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'analytics_complete'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        Ok(value.as_deref() == Some("1"))
    }

    pub fn mark_complete(&self) -> Result<()> {
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES('analytics_complete', '1')
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [],
        )?;
        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        self.conn.execute("DELETE FROM sessions", [])?;
        Ok(())
    }

    pub fn delete_source_path(&self, source_path: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM sessions WHERE source_path = ?1",
            params![source_path],
        )?;
        Ok(())
    }

    pub fn delete_session_scope(&self, scope: &SessionScope) -> Result<()> {
        self.conn.execute(
            "DELETE FROM sessions WHERE source = ?1 AND source_path = ?2 AND session_id = ?3",
            params![
                SourceKind::Opencode.storage_label(),
                scope.source_path,
                scope.session_id
            ],
        )?;
        Ok(())
    }

    pub fn query_sessions(
        &self,
        source: Option<SourceFilter>,
        since_ms: Option<u64>,
        project: Option<&str>,
        grouping: ProjectGrouping,
        limit: Option<usize>,
    ) -> Result<Vec<SessionRow>> {
        self.query_sessions_filtered(source, since_ms, project, grouping, None, limit)
    }

    pub fn query_sessions_filtered(
        &self,
        source: Option<SourceFilter>,
        since_ms: Option<u64>,
        project: Option<&str>,
        grouping: ProjectGrouping,
        kind: Option<SessionKindFilter>,
        limit: Option<usize>,
    ) -> Result<Vec<SessionRow>> {
        let mut sql = format!(
            "SELECT source, session_id, source_path, project,
                    {REPOSITORY_PROJECT_SQL} AS display_project,
                    cwd, last_at, message_count, label, conversation_kind
             FROM sessions"
        );
        let mut clauses = Vec::new();
        let mut values: Vec<rusqlite::types::Value> = Vec::new();

        if let Some(source) = source {
            let labels = source.storage_labels();
            let placeholders = std::iter::repeat_n("?", labels.len())
                .collect::<Vec<_>>()
                .join(", ");
            clauses.push(format!("source IN ({placeholders})"));
            values.extend(
                labels
                    .iter()
                    .map(|label| rusqlite::types::Value::Text((*label).to_string())),
            );
        }
        if let Some(since_ms) = since_ms {
            clauses.push("last_at >= ?".to_string());
            values.push(rusqlite::types::Value::Integer(since_ms as i64));
        }
        if let Some(project) = project {
            match grouping {
                ProjectGrouping::Flat => clauses.push("project = ?".to_string()),
                ProjectGrouping::Repository => {
                    clauses.push(format!("{REPOSITORY_PROJECT_SQL} = ?"))
                }
            }
            values.push(rusqlite::types::Value::Text(project.to_string()));
        }
        if let Some(kind) = kind {
            match kind {
                SessionKindFilter::Primary => {
                    clauses.push(
                        "(conversation_kind IS NULL OR conversation_kind = 'main')".to_string(),
                    );
                }
                SessionKindFilter::Subagent => {
                    clauses.push(
                        "conversation_kind IS NOT NULL AND conversation_kind NOT IN ('main', 'guardian_review')".to_string(),
                    );
                }
                SessionKindFilter::Regular => {
                    clauses.push(
                        "(conversation_kind IS NULL OR conversation_kind != 'guardian_review')"
                            .to_string(),
                    );
                }
                SessionKindFilter::All => {}
            }
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY last_at DESC");
        if let Some(limit) = limit {
            sql.push_str(" LIMIT ?");
            values.push(rusqlite::types::Value::Integer(limit as i64));
        }

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(values), |row| {
            let source_label: String = row.get(0)?;
            let source = SourceKind::from_label(&source_label).unwrap_or(SourceKind::Claude);
            let project: String = row.get(3)?;
            let raw_display_project: String = match grouping {
                ProjectGrouping::Flat => project.clone(),
                ProjectGrouping::Repository => row.get(4)?,
            };
            let display_project = display_project_name(&raw_display_project);
            Ok(SessionRow {
                source,
                session_id: row.get(1)?,
                source_path: row.get(2)?,
                project,
                display_project,
                cwd: row.get::<_, Option<String>>(5)?.filter(|v| !v.is_empty()),
                last_at: row.get::<_, i64>(6)?.max(0) as u64,
                message_count: row.get::<_, i64>(7)?.max(0) as u64,
                label: row.get::<_, Option<String>>(8)?.filter(|v| !v.is_empty()),
                conversation_kind: row.get::<_, Option<String>>(9)?.filter(|v| !v.is_empty()),
            })
        })?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Sessions with full stored metadata, newest first. `cwd` restricts to
    /// sessions whose working directory is the given path, lives under it,
    /// or whose git root is the given path (so a repo path matches sessions
    /// started in any of its subdirectories).
    pub fn query_sessions_detailed(
        &self,
        source: Option<SourceFilter>,
        project: Option<&str>,
        cwd: Option<&str>,
        since_ms: Option<u64>,
        limit: Option<usize>,
    ) -> Result<Vec<SessionDetailRow>> {
        self.query_sessions_detailed_filtered(source, project, cwd, since_ms, None, limit)
    }

    pub fn query_sessions_detailed_filtered(
        &self,
        source: Option<SourceFilter>,
        project: Option<&str>,
        cwd: Option<&str>,
        since_ms: Option<u64>,
        kind: Option<SessionKindFilter>,
        limit: Option<usize>,
    ) -> Result<Vec<SessionDetailRow>> {
        self.query_sessions_detailed_selected(
            source, project, cwd, since_ms, kind, None, None, limit,
        )
    }

    /// Apply exact identity selectors together with all listing filters before limiting rows.
    #[allow(clippy::too_many_arguments)]
    pub fn query_sessions_detailed_selected(
        &self,
        source: Option<SourceFilter>,
        project: Option<&str>,
        cwd: Option<&str>,
        since_ms: Option<u64>,
        kind: Option<SessionKindFilter>,
        session_id: Option<&str>,
        source_path: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<SessionDetailRow>> {
        let mut sql = String::from(
            "SELECT source, session_id, source_path, project, repo_project,
                    cwd, git_root, started_at, last_at, message_count, label, conversation_kind
             FROM sessions",
        );
        let mut clauses = Vec::new();
        let mut values: Vec<rusqlite::types::Value> = Vec::new();

        if let Some(session_id) = session_id {
            clauses.push("session_id = ?".to_string());
            values.push(rusqlite::types::Value::Text(session_id.to_string()));
        }
        if let Some(source_path) = source_path {
            clauses.push("source_path = ?".to_string());
            values.push(rusqlite::types::Value::Text(source_path.to_string()));
        }

        if let Some(source) = source {
            let labels = source.storage_labels();
            let placeholders = std::iter::repeat_n("?", labels.len())
                .collect::<Vec<_>>()
                .join(", ");
            clauses.push(format!("source IN ({placeholders})"));
            values.extend(
                labels
                    .iter()
                    .map(|label| rusqlite::types::Value::Text((*label).to_string())),
            );
        }
        if let Some(project) = project {
            clauses.push(format!("{REPOSITORY_PROJECT_SQL} = ?"));
            values.push(rusqlite::types::Value::Text(project.to_string()));
        }
        if let Some(cwd) = cwd {
            let root = cwd.trim_end_matches('/').to_string();
            // Escape LIKE wildcards so a path like /tmp/foo_bar doesn't also
            // match sessions under /tmp/fooXbar.
            let escaped = root
                .replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_");
            clauses.push("(cwd = ? OR cwd LIKE ? ESCAPE '\\' OR git_root = ?)".to_string());
            values.push(rusqlite::types::Value::Text(root.clone()));
            values.push(rusqlite::types::Value::Text(format!("{escaped}/%")));
            values.push(rusqlite::types::Value::Text(root));
        }
        if let Some(since_ms) = since_ms {
            clauses.push("last_at >= ?".to_string());
            values.push(rusqlite::types::Value::Integer(since_ms as i64));
        }
        if let Some(kind) = kind {
            match kind {
                SessionKindFilter::Primary => {
                    clauses.push(
                        "(conversation_kind IS NULL OR conversation_kind = 'main')".to_string(),
                    );
                }
                SessionKindFilter::Subagent => {
                    clauses.push(
                        "conversation_kind IS NOT NULL AND conversation_kind NOT IN ('main', 'guardian_review')".to_string(),
                    );
                }
                SessionKindFilter::Regular => {
                    clauses.push(
                        "(conversation_kind IS NULL OR conversation_kind != 'guardian_review')"
                            .to_string(),
                    );
                }
                SessionKindFilter::All => {}
            }
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY last_at DESC");
        if let Some(limit) = limit {
            sql.push_str(" LIMIT ?");
            values.push(rusqlite::types::Value::Integer(limit as i64));
        }

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(values), |row| {
            let source_label: String = row.get(0)?;
            let source = SourceKind::from_label(&source_label).unwrap_or(SourceKind::Claude);
            let repo_project: Option<String> = row.get(4)?;
            Ok(SessionDetailRow {
                source,
                session_id: row.get(1)?,
                source_path: row.get(2)?,
                project: row.get(3)?,
                repo_project: repo_project.filter(|value| !value.is_empty()),
                cwd: row.get::<_, Option<String>>(5)?.filter(|v| !v.is_empty()),
                git_root: row.get::<_, Option<String>>(6)?.filter(|v| !v.is_empty()),
                started_at: row.get::<_, i64>(7)?.max(0) as u64,
                last_at: row.get::<_, i64>(8)?.max(0) as u64,
                message_count: row.get::<_, i64>(9)?.max(0) as u64,
                label: row.get::<_, Option<String>>(10)?.filter(|v| !v.is_empty()),
                conversation_kind: row.get::<_, Option<String>>(11)?.filter(|v| !v.is_empty()),
            })
        })?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Stored conversation kind for one exact session identity, if the
    /// analytics cache has a row for it. Search-result grouping uses this
    /// complete-session truth so a session is never classified from only
    /// the matched records; missing rows yield `None` and callers fall
    /// back to hit-derived kinds.
    pub fn session_conversation_kind(
        &self,
        source: &str,
        session_id: &str,
        source_path: &str,
    ) -> Option<String> {
        self.conn
            .query_row(
                "SELECT conversation_kind FROM sessions
                 WHERE source = ?1 AND session_id = ?2 AND source_path = ?3",
                params![source, session_id, source_path],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .ok()
            .flatten()
            .flatten()
            .filter(|kind| !kind.is_empty())
    }

    /// Aggregate in SQLite rather than materializing every session in the client.
    /// One stored row is one (source, session_id, source_path) session identity.
    /// Match repository grouping and the default regular-session filter.
    pub fn query_project_summaries(
        &self,
        source: Option<SourceFilter>,
    ) -> Result<Vec<ProjectSummary>> {
        let mut sql = format!(
            "with project_sessions as (
                select {REPOSITORY_PROJECT_SQL} as project,
                       last_at
                from sessions
                where (conversation_kind is null or conversation_kind != 'guardian_review')",
        );
        let mut values: Vec<rusqlite::types::Value> = Vec::new();
        if let Some(source) = source {
            let labels = source.storage_labels();
            let placeholders = std::iter::repeat_n("?", labels.len())
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" and source in ({placeholders})"));
            values.extend(
                labels
                    .iter()
                    .map(|label| rusqlite::types::Value::Text((*label).to_string())),
            );
        }
        sql.push_str(
            ")
             select project, count(*) as session_count,
                    max(case when last_at > 0 then last_at end) as last_at
             from project_sessions
             group by project
             order by last_at desc, project asc",
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(values), |row| {
            Ok(ProjectSummary {
                project: row.get(0)?,
                session_count: row.get(1)?,
                last_at: row.get(2)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn query_projects(
        &self,
        source: Option<SourceFilter>,
        grouping: ProjectGrouping,
    ) -> Result<Vec<String>> {
        let project_expr = match grouping {
            ProjectGrouping::Flat => "project",
            ProjectGrouping::Repository => REPOSITORY_PROJECT_SQL,
        };
        let mut sql = format!("SELECT DISTINCT {project_expr} FROM sessions");
        let mut values: Vec<rusqlite::types::Value> = Vec::new();
        if let Some(source) = source {
            let labels = source.storage_labels();
            let placeholders = std::iter::repeat_n("?", labels.len())
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" WHERE source IN ({placeholders})"));
            values.extend(
                labels
                    .iter()
                    .map(|label| rusqlite::types::Value::Text((*label).to_string())),
            );
        }
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(values), |row| row.get::<_, String>(0))?;
        let mut projects = Vec::new();
        for row in rows {
            let project = display_project_name(&row?);
            if !project.is_empty() {
                projects.push(project);
            }
        }
        projects.sort_by(|left, right| {
            match (
                left.as_str() == UNFILED_PROJECT,
                right.as_str() == UNFILED_PROJECT,
            ) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => left.cmp(right),
            }
        });
        projects.dedup();
        Ok(projects)
    }

    pub fn query_source_timestamps(&self, since_ms: Option<u64>) -> Result<Vec<(SourceKind, u64)>> {
        self.query_source_timestamps_filtered(
            None,
            since_ms,
            None,
            None,
            ProjectGrouping::Flat,
            None,
        )
    }

    pub fn query_source_timestamps_filtered(
        &self,
        source: Option<SourceFilter>,
        since_ms: Option<u64>,
        until_ms: Option<u64>,
        project: Option<&str>,
        grouping: ProjectGrouping,
        kind: Option<SessionKindFilter>,
    ) -> Result<Vec<(SourceKind, u64)>> {
        let mut sql = String::from("SELECT source, last_at FROM sessions");
        let mut clauses = Vec::new();
        let mut values: Vec<rusqlite::types::Value> = Vec::new();
        if let Some(source) = source {
            let labels = source.storage_labels();
            let placeholders = std::iter::repeat_n("?", labels.len())
                .collect::<Vec<_>>()
                .join(", ");
            clauses.push(format!("source IN ({placeholders})"));
            values.extend(
                labels
                    .iter()
                    .map(|label| rusqlite::types::Value::Text((*label).to_string())),
            );
        }
        if let Some(since_ms) = since_ms {
            clauses.push("last_at >= ?".to_string());
            values.push(rusqlite::types::Value::Integer(since_ms as i64));
        }
        if let Some(until_ms) = until_ms {
            clauses.push("last_at <= ?".to_string());
            values.push(rusqlite::types::Value::Integer(until_ms as i64));
        }
        if let Some(project) = project {
            let project_expr = match grouping {
                ProjectGrouping::Flat => "project",
                ProjectGrouping::Repository => REPOSITORY_PROJECT_SQL,
            };
            clauses.push(format!("{project_expr} = ?"));
            values.push(rusqlite::types::Value::Text(project.to_string()));
        }
        if let Some(kind) = kind {
            match kind {
                SessionKindFilter::Primary => {
                    clauses.push(
                        "(conversation_kind IS NULL OR conversation_kind = 'main')".to_string(),
                    );
                }
                SessionKindFilter::Subagent => {
                    clauses.push(
                        "conversation_kind IS NOT NULL AND conversation_kind NOT IN ('main', 'guardian_review')".to_string(),
                    );
                }
                SessionKindFilter::Regular => {
                    clauses.push(
                        "(conversation_kind IS NULL OR conversation_kind != 'guardian_review')"
                            .to_string(),
                    );
                }
                SessionKindFilter::All => {}
            }
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(values), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?.max(0) as u64,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (label, ts) = row?;
            if let Some(kind) = SourceKind::from_label(&label) {
                out.push((kind, ts));
            }
        }
        Ok(out)
    }

    pub fn query_source_labels(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT DISTINCT source FROM sessions")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        out.sort();
        Ok(out)
    }

    pub fn query_project_timestamps(
        &self,
        source: Option<SourceFilter>,
        since_ms: Option<u64>,
        grouping: ProjectGrouping,
    ) -> Result<Vec<(String, u64)>> {
        let project_expr = match grouping {
            ProjectGrouping::Flat => "project",
            ProjectGrouping::Repository => REPOSITORY_PROJECT_SQL,
        };
        let mut sql = format!("SELECT {project_expr}, last_at FROM sessions");
        let mut clauses = Vec::new();
        let mut values: Vec<rusqlite::types::Value> = Vec::new();
        if let Some(source) = source {
            let labels = source.storage_labels();
            let placeholders = std::iter::repeat_n("?", labels.len())
                .collect::<Vec<_>>()
                .join(", ");
            clauses.push(format!("source IN ({placeholders})"));
            values.extend(
                labels
                    .iter()
                    .map(|label| rusqlite::types::Value::Text((*label).to_string())),
            );
        }
        if let Some(since_ms) = since_ms {
            clauses.push("last_at >= ?".to_string());
            values.push(rusqlite::types::Value::Integer(since_ms as i64));
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(values), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?.max(0) as u64,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (project, last_at) = row?;
            out.push((display_project_name(&project), last_at));
        }
        Ok(out)
    }

    pub fn project_for_session(
        &self,
        source: SourceKind,
        session_id: &str,
        source_path: &str,
        grouping: ProjectGrouping,
    ) -> Result<Option<String>> {
        let display_expr = match grouping {
            ProjectGrouping::Flat => "project",
            ProjectGrouping::Repository => REPOSITORY_PROJECT_SQL,
        };
        let project: Option<String> = self
            .conn
            .query_row(
                &format!(
                    "SELECT {display_expr} FROM sessions
                     WHERE source = ?1 AND session_id = ?2 AND source_path = ?3"
                ),
                params![source.storage_label(), session_id, source_path],
                |row| row.get(0),
            )
            .optional()?;
        Ok(project.map(|project| display_project_name(&project)))
    }

    pub fn query_session_projects(
        &self,
        sessions: &[(SourceKind, String, String)],
        grouping: ProjectGrouping,
    ) -> Result<HashMap<(SourceKind, String, String), String>> {
        if sessions.is_empty() {
            return Ok(HashMap::new());
        }
        let display_expr = match grouping {
            ProjectGrouping::Flat => "project",
            ProjectGrouping::Repository => REPOSITORY_PROJECT_SQL,
        };
        let conditions = std::iter::repeat_n(
            "(source = ? AND session_id = ? AND source_path = ?)",
            sessions.len(),
        )
        .collect::<Vec<_>>()
        .join(" OR ");
        let mut stmt = self.conn.prepare(&format!(
            "SELECT source, session_id, source_path, {display_expr}
             FROM sessions WHERE {conditions}"
        ))?;
        let values = sessions
            .iter()
            .flat_map(|(source, session_id, source_path)| {
                [
                    rusqlite::types::Value::Text(source.storage_label().to_string()),
                    rusqlite::types::Value::Text(session_id.clone()),
                    rusqlite::types::Value::Text(source_path.clone()),
                ]
            });
        let rows = stmt.query_map(params_from_iter(values), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        let mut projects = HashMap::new();
        for row in rows {
            let (source, session_id, source_path, project) = row?;
            let Some(source) = SourceKind::from_label(&source) else {
                continue;
            };
            projects.insert(
                (source, session_id, source_path),
                display_project_name(&project),
            );
        }
        Ok(projects)
    }
}

impl AnalyticsWriter {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            store: AnalyticsStore::open(path)?,
            sessions: HashMap::new(),
            metadata_cache: HashMap::new(),
            git_cache: HashMap::new(),
            cwd_overrides: HashMap::new(),
        })
    }

    pub fn clear(&self) -> Result<()> {
        self.store.clear()
    }

    pub fn delete_source_path(&self, source_path: &str) -> Result<()> {
        self.store.delete_source_path(source_path)
    }

    pub fn delete_session_scope(&self, scope: &SessionScope) -> Result<()> {
        self.store.delete_session_scope(scope)
    }

    pub fn set_session_cwd(
        &mut self,
        source: SourceKind,
        source_path: &str,
        session_id: &str,
        cwd: &str,
    ) {
        self.cwd_overrides.insert(
            SessionKey {
                source,
                session_id: session_id.to_string(),
                source_path: source_path.to_string(),
            },
            cwd.to_string(),
        );
    }

    pub fn record(&mut self, record: &Record) -> Result<()> {
        let key = SessionKey {
            source: record.source,
            session_id: record.session_id.clone(),
            source_path: record.source_path.clone(),
        };
        let entry = self
            .sessions
            .entry(key.clone())
            .or_insert_with(|| SessionAccumulator {
                key,
                project: record.project.clone(),
                started_at: record.ts,
                last_at: record.ts,
                message_count: 0,
                first_user_text: None,
                conversation_kind: None,
            });
        if record.ts < entry.started_at {
            entry.started_at = record.ts;
        }
        if record.ts >= entry.last_at {
            entry.last_at = record.ts;
            if !record.project.is_empty() {
                entry.project = record.project.clone();
            }
        }
        entry.message_count = entry.message_count.saturating_add(1);
        if entry.first_user_text.is_none()
            && record.role == "user"
            && !record.text.trim().is_empty()
            && !sanitize_label(&record.text).is_empty()
        {
            entry.first_user_text = Some(record.text.clone());
        }
        // Prefer an explicit "main" over per-record non-main kinds: Pi and
        // OpenClaw stamp compaction/branch on entries inside otherwise-main
        // sessions, and Claude stamps sidechain lines the same way. A session
        // is non-interactive only when no record claims it as main.
        if let Some(kind) = record.links.conversation_kind.clone()
            && !kind.is_empty()
            && (entry.conversation_kind.is_none() || kind == "main")
        {
            entry.conversation_kind = Some(kind);
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.sessions.is_empty() {
            return Ok(());
        }
        let pending_sessions: Vec<SessionAccumulator> = self.sessions.values().cloned().collect();
        let sessions: Vec<(SessionAccumulator, SessionMetadata)> = pending_sessions
            .into_iter()
            .map(|session| {
                let metadata = self.resolve_metadata(&session.key);
                (session, metadata)
            })
            .collect();
        let tx = self.store.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                r#"
                INSERT INTO sessions(
                    source, session_id, source_path, project, cwd, git_root, git_common_dir,
                    repo_project, started_at, last_at, message_count, resolution_status,
                    label, conversation_kind
                )
                VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                ON CONFLICT(source, session_id, source_path) DO UPDATE SET
                    project = excluded.project,
                    cwd = excluded.cwd,
                    git_root = excluded.git_root,
                    git_common_dir = excluded.git_common_dir,
                    repo_project = excluded.repo_project,
                    started_at = MIN(sessions.started_at, excluded.started_at),
                    last_at = MAX(sessions.last_at, excluded.last_at),
                    message_count = sessions.message_count + excluded.message_count,
                    resolution_status = excluded.resolution_status,
                    -- The stored label/kind describe the session's opening and
                    -- survive incremental deltas, which only see mid-session
                    -- records. Corrections flow through parser-version bumps,
                    -- which delete the row first (delete_first) and recompute.
                    label = COALESCE(sessions.label, excluded.label),
                    conversation_kind = COALESCE(sessions.conversation_kind, excluded.conversation_kind)
                "#,
            )?;
            // OpenCode title/agent lookups hit the source SQLite database. Sessions
            // from one database share the same file, so memoize per flush to
            // avoid reopening it once per session during large index scans.
            let mut opencode_cache = OpencodeLookupCache::default();
            for (session, metadata) in sessions {
                let label = extract_session_label(
                    session.key.source,
                    &session.key.source_path,
                    &session.key.session_id,
                    session.first_user_text.as_deref(),
                    metadata.cwd.as_deref(),
                    &mut opencode_cache,
                );
                let conversation_kind = infer_session_kind(
                    session.key.source,
                    &session.key.source_path,
                    &session.key.session_id,
                    session.conversation_kind.as_deref(),
                    metadata.cwd.as_deref(),
                    session.first_user_text.as_deref(),
                    &mut opencode_cache,
                );
                stmt.execute(params![
                    session.key.source.storage_label(),
                    session.key.session_id,
                    session.key.source_path,
                    session.project,
                    metadata.cwd,
                    metadata.git_root,
                    metadata.git_common_dir,
                    metadata.repo_project,
                    session.started_at as i64,
                    session.last_at as i64,
                    session.message_count as i64,
                    metadata.resolution_status,
                    label,
                    conversation_kind,
                ])?;
            }
        }
        tx.commit()?;
        self.sessions.clear();
        Ok(())
    }

    fn resolve_metadata(&mut self, key: &SessionKey) -> SessionMetadata {
        if let Some(cached) = self.metadata_cache.get(key) {
            return cached.clone();
        }
        let metadata = self.resolve_uncached_metadata(key);
        self.metadata_cache.insert(key.clone(), metadata.clone());
        metadata
    }

    fn resolve_uncached_metadata(&mut self, key: &SessionKey) -> SessionMetadata {
        let cwd = self.cwd_overrides.get(key).cloned().or_else(|| {
            resolve_session_cwd_from_parts(key.source, &key.source_path, &key.session_id)
        });
        let Some(cwd) = cwd else {
            return SessionMetadata {
                resolution_status: "no-cwd".to_string(),
                ..SessionMetadata::default()
            };
        };
        let git = self
            .git_cache
            .entry(cwd.clone())
            .or_insert_with(|| git_metadata_for_cwd(&cwd))
            .clone();
        SessionMetadata {
            cwd: Some(cwd),
            git_root: git.git_root,
            git_common_dir: git.git_common_dir,
            repo_project: git.repo_project,
            resolution_status: git.status,
        }
    }
}

#[derive(Clone, Default)]
struct GitMetadata {
    git_root: Option<String>,
    git_common_dir: Option<String>,
    repo_project: Option<String>,
    status: String,
}

fn git_metadata_for_cwd(cwd: &str) -> GitMetadata {
    let deadline = Instant::now() + GIT_METADATA_TIMEOUT;
    let root = git_rev_parse(cwd, &["rev-parse", "--show-toplevel"], deadline);
    let common_dir = git_rev_parse(
        cwd,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        deadline,
    );
    let path_repo_project =
        claude_worktree_repo_project(cwd).or_else(|| codex_worktree_repo_project(cwd));
    let repo_project = common_dir
        .as_deref()
        .and_then(common_dir_project_name)
        .or_else(|| root.as_deref().and_then(path_file_name))
        .or_else(|| path_repo_project.clone());

    let status = if repo_project.is_some() && root.is_none() && common_dir.is_none() {
        "path-fallback"
    } else if repo_project.is_some() {
        "ok"
    } else if root.is_some() || common_dir.is_some() {
        "git-partial"
    } else {
        "not-git"
    }
    .to_string();

    GitMetadata {
        git_root: root,
        git_common_dir: common_dir,
        repo_project,
        status,
    }
}

pub(crate) fn repository_project_for_cwd(cwd: &str) -> Option<String> {
    git_metadata_for_cwd(cwd).repo_project
}

fn claude_worktree_repo_project(cwd: &str) -> Option<String> {
    for ancestor in Path::new(cwd).ancestors() {
        if ancestor.file_name().and_then(|n| n.to_str()) != Some("worktrees") {
            continue;
        }
        let claude_dir = ancestor.parent()?;
        if claude_dir.file_name().and_then(|n| n.to_str()) != Some(".claude") {
            continue;
        }
        let repo_dir = claude_dir.parent()?;
        return path_file_name(repo_dir.to_string_lossy().as_ref());
    }
    None
}

fn codex_worktree_repo_project(cwd: &str) -> Option<String> {
    let cwd = Path::new(cwd);
    for ancestor in cwd.ancestors() {
        if ancestor.file_name().and_then(|name| name.to_str()) != Some("worktrees") {
            continue;
        }
        let codex_dir = ancestor.parent()?;
        if codex_dir.file_name().and_then(|name| name.to_str()) != Some(".codex") {
            continue;
        }
        let mut relative = cwd.strip_prefix(ancestor).ok()?.components();
        relative.next()?;
        return relative
            .next()
            .and_then(|component| component.as_os_str().to_str())
            .filter(|name| !name.is_empty())
            .map(str::to_string);
    }
    None
}

fn git_rev_parse(cwd: &str, args: &[&str], deadline: Instant) -> Option<String> {
    if Instant::now() >= deadline {
        return None;
    }
    let child = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let output = child_output_before(child, deadline)?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let text = text.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

fn child_output_before(mut child: Child, deadline: Instant) -> Option<Output> {
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return child.wait_with_output().ok(),
            Ok(None) => {}
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(remaining.min(Duration::from_millis(10)));
    }
}

fn common_dir_project_name(path: &str) -> Option<String> {
    let path = Path::new(path);
    if path.file_name().and_then(|n| n.to_str()) == Some(".git") {
        return path
            .parent()
            .and_then(|p| path_file_name(p.to_string_lossy().as_ref()));
    }
    path_file_name(path.to_string_lossy().as_ref())
}

fn display_project_name(project: &str) -> String {
    decode_encoded_project_path(project).unwrap_or_else(|| project.to_string())
}

fn decode_encoded_project_path(project: &str) -> Option<String> {
    let trimmed = project.trim_matches('-');
    let lower = trimmed.to_lowercase();
    if !(lower.starts_with("users-") || lower.starts_with("home-") || lower.contains("-users-")) {
        return None;
    }
    let parts: Vec<&str> = trimmed.split('-').filter(|part| !part.is_empty()).collect();
    if parts.len() < 3 {
        return None;
    }

    if let Some(home) = home_relative_encoded_path(&parts) {
        return Some(home);
    }

    if parts[0].eq_ignore_ascii_case("home") {
        let tail = parts.get(2..)?;
        if tail.is_empty() {
            return None;
        }
        return Some(encoded_tail_display(tail));
    }

    let users_idx = parts
        .iter()
        .position(|part| part.eq_ignore_ascii_case("Users"))?;
    let tail = parts.get(users_idx + 2..)?;
    if tail.is_empty() {
        return None;
    }
    Some(encoded_tail_display(tail))
}

fn home_relative_encoded_path(parts: &[&str]) -> Option<String> {
    let home = std::env::var("HOME").ok()?;
    let mut home_parts = Path::new(&home)
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .filter(|part| !part.is_empty());
    let home_parent = home_parts.next_back()?;
    let users_idx = parts
        .iter()
        .position(|part| part.eq_ignore_ascii_case("Users"))?;
    if parts.get(users_idx + 1)? != &home_parent {
        return None;
    }
    let tail = parts.get(users_idx + 2..)?;
    if tail.is_empty() {
        return None;
    }
    Some(encoded_tail_display(tail))
}

fn encoded_tail_display(tail: &[&str]) -> String {
    if tail.len() == 1 {
        return format!("~/{}", tail[0]);
    }
    let common_dirs = [
        "projects",
        "code",
        "repos",
        "src",
        "dev",
        "work",
        "documents",
    ];
    if common_dirs.contains(&tail[0].to_lowercase().as_str()) && tail.len() > 1 {
        return tail[1..].join("-");
    }
    tail.join("-")
}

fn path_file_name(path: &str) -> Option<String> {
    Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .filter(|name| !name.is_empty())
        .map(|name| name.to_string())
}

fn resolve_session_cwd_from_parts(
    source: SourceKind,
    source_path: &str,
    session_id: &str,
) -> Option<String> {
    if source == SourceKind::Opencode && crate::sources::opencode::is_database_path(source_path) {
        return crate::sources::opencode::enumerate_sessions(Path::new(source_path))
            .ok()?
            .into_iter()
            .find(|session| session.id == session_id)
            .map(|session| session.directory);
    }
    if source == SourceKind::Copilot
        && let Some(cwd) = resolve_copilot_workspace_cwd(source_path)
    {
        return Some(cwd);
    }
    if source == SourceKind::Grok
        && let Some(cwd) = crate::sources::grok::session_cwd(Path::new(source_path))
    {
        return Some(cwd);
    }
    if source == SourceKind::Jcode
        && let Some(cwd) = crate::sources::jcode::cwd_from_jcode_session(Path::new(source_path))
    {
        return Some(cwd.to_string_lossy().to_string());
    }
    if source == SourceKind::Muse
        && let Some(cwd) = crate::sources::muse::cwd_from_muse_session(Path::new(source_path))
    {
        return Some(cwd.to_string_lossy().to_string());
    }
    let file = std::fs::File::open(source_path).ok()?;
    let reader = std::io::BufReader::new(file);
    let mut fallback: Option<String> = None;
    for line in std::io::BufRead::lines(reader).map_while(std::result::Result::ok) {
        let value: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let cwd = value
            .get("cwd")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        if fallback.is_none() {
            fallback = cwd.clone();
        }

        let session_id_match = value
            .get("sessionId")
            .and_then(|v| v.as_str())
            .or_else(|| value.get("session_id").and_then(|v| v.as_str()))
            .map(|s| s == session_id)
            .unwrap_or(false);

        if session_id_match && cwd.is_some() {
            return cwd;
        }

        if source == SourceKind::Codex
            && value.get("type").and_then(|v| v.as_str()) == Some("session_meta")
        {
            let payload_cwd = value
                .get("payload")
                .and_then(|v| v.get("cwd"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if payload_cwd.is_some() {
                return payload_cwd;
            }
        }

        if matches!(source, SourceKind::Pi | SourceKind::OpenClaw)
            && value.get("type").and_then(|v| v.as_str()) == Some("session")
        {
            let cwd = value
                .get("cwd")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if cwd.is_some() {
                return cwd;
            }
        }
    }
    fallback
}

#[derive(Default)]
struct CopilotWorkspaceCwd {
    cwd: Option<String>,
    git_root: Option<String>,
}

fn resolve_copilot_workspace_cwd(source_path: &str) -> Option<String> {
    let workspace_path = Path::new(source_path).parent()?.join("workspace.yaml");
    let contents = std::fs::read_to_string(workspace_path).ok()?;
    let workspace = parse_copilot_workspace_cwd(&contents);
    workspace.cwd.or(workspace.git_root)
}

fn parse_copilot_workspace_cwd(contents: &str) -> CopilotWorkspaceCwd {
    let mut workspace = CopilotWorkspaceCwd::default();
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty()
            || trimmed.starts_with('#')
            || line.chars().next().is_some_and(|c| c.is_whitespace())
        {
            continue;
        }
        let Some((key, value)) = trimmed.split_once(':') else {
            continue;
        };
        let value = value
            .trim()
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();
        if value.is_empty() {
            continue;
        }
        match key.trim() {
            "cwd" => workspace.cwd = Some(value),
            "gitRoot" | "git_root" => workspace.git_root = Some(value),
            _ => {}
        }
    }
    workspace
}

#[allow(clippy::while_let_loop)]
pub fn sanitize_label(raw: &str) -> String {
    // Comprehensive stripping of system wrappers (case-insensitive).
    // Fast path: neither the tag stripper nor the generic unwrap can match
    // without a '<', so ordinary prose skips the owned buffer entirely and
    // goes straight to the single-allocation finish pass. This keeps the
    // per-record emptiness check in `record` cheap during index scans.
    if raw.contains('<') {
        let mut current = raw.to_string();
        const DROP_TAGS: &[&str] = &[
            "system-reminder",
            "command-message",
            "command-name",
            "local-command-stdout",
            "local-command-caveat",
            "local-command-output",
            "instructions",
            "environment_context",
            "cwd",
            "approval_policy",
            "shell",
            "user_instructions",
            "recommended_plugins",
            "skill",
            "user_action",
            "context",
            "task-notification",
            "task-id",
            "tool-use-id",
            "subagent_notification",
            "turn_aborted",
            "current_date",
            "timezone",
            "epoch",
            "collaboration_mode",
            "apps_instructions",
            "permissions",
            "total_tokens",
        ];
        for tag in DROP_TAGS {
            let open = format!("<{tag}");
            let close = format!("</{tag}>");
            loop {
                let Some(start) = find_ascii_ci(&current, &open) else {
                    break;
                };
                let open_end = match current[start..].find('>') {
                    Some(p) => start + p + 1,
                    None => {
                        current.truncate(start);
                        break;
                    }
                };
                if let Some(end_offset) = find_ascii_ci(&current[open_end..], &close) {
                    let abs_end = open_end + end_offset + close.len();
                    current.replace_range(start..abs_end, " ");
                } else {
                    current.truncate(start);
                    break;
                }
            }
        }
        // Generic unwrap: remove any remaining <...> tags but keep inner text.
        let mut search_start = 0;
        loop {
            let Some(rel_start) = current[search_start..].find('<') else {
                break;
            };
            let start = search_start + rel_start;
            let Some(end) = current[start..].find('>') else {
                break;
            };
            let abs_end = start + end + 1;
            let after_lt = current[start + 1..].chars().next().unwrap_or(' ');
            if after_lt.is_ascii_alphabetic() || after_lt == '/' || after_lt == '!' {
                current.replace_range(start..abs_end, " ");
                search_start = start;
            } else {
                search_start = abs_end;
            }
            if current.len() > 10000 {
                break;
            }
        }
        return finish_label(&current);
    }
    finish_label(raw)
}

/// Single-allocation finish pass for `sanitize_label`: ANSI strip (borrowed
/// when there is no ESC), whitespace collapse, control-char removal, and
/// the boilerplate suppressions. Equivalent to the old
/// split-whitespace-join plus control-filter pipeline: after collapsing,
/// the only surviving whitespace is ' ', and the suppression patterns are
/// pure ASCII so `eq_ignore_ascii_case` matches `to_lowercase` + compare
/// on them without a whole-message lowercase copy.
fn finish_label(text: &str) -> String {
    let stripped;
    let no_ansi: &str = if text.contains('\x1b') {
        stripped = strip_ansi(text);
        &stripped
    } else {
        text
    };
    let mut collapsed = String::with_capacity(no_ansi.len().min(1024));
    let mut pending_space = false;
    for c in no_ansi.chars() {
        // Whitespace first: newlines/tabs are also control characters, and
        // dropping them here would glue words together ("hello\nworld" must
        // collapse to "hello world", not "helloworld").
        if c.is_whitespace() {
            pending_space = true;
            continue;
        }
        if c.is_control() {
            continue;
        }
        if pending_space && !collapsed.is_empty() {
            collapsed.push(' ');
        }
        pending_space = false;
        collapsed.push(c);
    }
    if collapsed.is_empty() {
        return String::new();
    }
    if starts_ascii_ci(&collapsed, "# agents.md")
        || find_ascii_ci(&collapsed, "global agent preferences").is_some()
        || starts_ascii_ci(&collapsed, "you are a reminder observer")
    {
        return String::new();
    }
    let printable = collapsed.as_str();
    if printable.chars().count() <= MAX_LABEL_CHARS {
        return printable.to_string();
    }
    // Suffix-preserving truncation: keep head and tail to preserve distinguishing suffix.
    const TAIL_LEN: usize = 40;
    let head_len = MAX_LABEL_CHARS.saturating_sub(TAIL_LEN + 1);
    let head_raw: String = printable.chars().take(head_len).collect();
    let head = if let Some(pos) = head_raw.rfind(' ') {
        if pos > 80 {
            head_raw[..pos].to_string()
        } else {
            head_raw
        }
    } else {
        head_raw
    };
    let rev_tail: String = printable.chars().rev().take(TAIL_LEN).collect();
    let tail_raw: String = rev_tail.chars().rev().collect();
    let tail = if let Some(pos) = tail_raw.find(' ') {
        tail_raw[pos + 1..].trim().to_string()
    } else {
        tail_raw.trim().to_string()
    };
    if tail.is_empty() {
        let mut out = head;
        out.push('…');
        return out;
    }
    let mut tail = tail;
    while head.chars().count() + 1 + tail.chars().count() > MAX_LABEL_CHARS {
        if let Some(pos) = tail.find(' ') {
            tail = tail[pos + 1..].trim().to_string();
        } else if tail.chars().count() > 10 {
            tail = tail.chars().skip(tail.chars().count() - 10).collect();
        } else {
            break;
        }
    }
    format!("{}…{}", head, tail)
}

/// ASCII case-insensitive substring search over the original string's bytes.
/// Tag patterns are pure ASCII, so byte-wise matching keeps every index in the
/// original string's coordinates: Unicode `to_lowercase` can shift byte
/// offsets (e.g. U+0130 folds 2 bytes into 3), which would make `replace_range`
/// or `truncate` panic on a non-char-boundary. Every match starts at a `<`
/// byte, which is always a char boundary in UTF-8.
fn find_ascii_ci(haystack: &str, needle: &str) -> Option<usize> {
    let hay = haystack.as_bytes();
    let ndl = needle.as_bytes();
    if ndl.is_empty() || hay.len() < ndl.len() {
        return None;
    }
    (0..=hay.len() - ndl.len()).find(|&i| hay[i..i + ndl.len()].eq_ignore_ascii_case(ndl))
}

/// ASCII case-insensitive prefix test. Uses `get` so a needle length that
/// lands mid-char safely returns false instead of panicking.
fn starts_ascii_ci(haystack: &str, needle: &str) -> bool {
    haystack
        .get(..needle.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(needle))
}

fn strip_ansi(input: &str) -> String {
    if !input.contains('\x1b') {
        return input.to_string();
    }
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == 0x1b {
            // CSI: ESC [ ... letter
            if i + 1 < bytes.len() && bytes[i + 1] == b'[' {
                i += 2;
                while i < bytes.len() && !bytes[i].is_ascii_alphabetic() {
                    i += 1;
                }
                if i < bytes.len() {
                    i += 1;
                }
                continue;
            }
            // OSC: ESC ] ... BEL or ESC \
            if i + 1 < bytes.len() && bytes[i + 1] == b']' {
                i += 2;
                while i < bytes.len()
                    && bytes[i] != 0x07
                    && !(bytes[i] == 0x1b && i + 1 < bytes.len() && bytes[i + 1] == b'\\')
                {
                    i += 1;
                }
                if i < bytes.len() && bytes[i] == 0x07 {
                    i += 1;
                } else if i + 1 < bytes.len() && bytes[i] == 0x1b {
                    i += 2;
                }
                continue;
            }
            i += 1;
            continue;
        }
        // Copy one full UTF-8 scalar value: pushing a single byte as char
        // would corrupt multi-byte sequences (e.g. emoji) into mojibake and
        // inflate char counts used by truncation. `i` always rests on a char
        // boundary here because every skipped escape sequence is pure ASCII.
        let Some(ch) = input[i..].chars().next() else {
            break;
        };
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn opencode_title_for_session(db_path: &str, session_id: &str) -> Option<String> {
    let path = Path::new(db_path);
    if !path.is_file() {
        return None;
    }
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    conn.busy_timeout(Duration::from_secs(1)).ok()?;
    let mut stmt = conn
        .prepare("SELECT title FROM session WHERE id = ?1")
        .ok()?;
    let title: Option<String> = stmt
        .query_row(params![session_id], |row| row.get(0))
        .optional()
        .ok()
        .flatten()
        .filter(|t: &String| !t.trim().is_empty());
    title
}

fn grok_title_for_session(updates_path: &str) -> Option<String> {
    let path = Path::new(updates_path);
    let parent = path.parent()?;
    let summary_path = parent.join("summary.json");
    let contents = std::fs::read_to_string(&summary_path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&contents).ok()?;
    for key in [
        "generated_title",
        "generatedTitle",
        "title",
        "session_summary",
        "sessionSummary",
        "summary",
    ] {
        if let Some(title) = value
            .get(key)
            .and_then(|v| v.as_str())
            .filter(|s: &&str| !s.trim().is_empty())
        {
            return Some(title.to_string());
        }
        if let Some(title) = value
            .pointer(&format!("/info/{key}"))
            .and_then(|v| v.as_str())
            .filter(|s: &&str| !s.trim().is_empty())
        {
            return Some(title.to_string());
        }
    }
    value
        .get("info")
        .and_then(|info| info.get("generated_title").or_else(|| info.get("title")))
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.to_string())
        .or_else(|| {
            value
                .pointer("/info/cwd")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        })
}

fn jcode_label_from_file(path: &str) -> Option<String> {
    let contents = std::fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&contents).ok()?;
    let messages = value.get("messages")?.as_array()?;
    for msg in messages {
        let role = msg.get("role").and_then(|v| v.as_str()).unwrap_or("");
        if role != "user" {
            continue;
        }
        let content = msg.get("content")?;
        let mut texts = Vec::new();
        if let Some(arr) = content.as_array() {
            for block in arr {
                if let Some(text) = block.get("text").and_then(|v| v.as_str()) {
                    texts.push(text.to_string());
                } else if let Some(text) = block.as_str() {
                    texts.push(text.to_string());
                }
            }
        } else if let Some(text) = content.as_str() {
            texts.push(text.to_string());
        }
        let combined = texts.join("\n");
        if combined.trim().is_empty() {
            continue;
        }
        // Skip pure <system-reminder> messages – look for next user message.
        if sanitize_label(&combined).is_empty() {
            continue;
        }
        return Some(combined);
    }
    None
}

/// Parent linkage is the only subagent signal: the `agent` column records
/// the selected agent (build/plan/custom), so a plan-mode session without
/// a parent is still interactive.
fn opencode_session_has_parent(db_path: &str, session_id: &str) -> bool {
    let path = Path::new(db_path);
    if !path.is_file() {
        return false;
    }
    let conn = match Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(c) => c,
        Err(_) => return false,
    };
    let _ = conn.busy_timeout(Duration::from_secs(1));
    let check = || -> Option<bool> {
        let mut stmt = conn
            .prepare("SELECT parent_id FROM session WHERE id = ?1")
            .ok()?;
        let parent: Option<String> = stmt
            .query_row(params![session_id], |row| row.get(0))
            .optional()
            .ok()
            .flatten();
        Some(parent.is_some_and(|p| !p.trim().is_empty()))
    };
    check().unwrap_or(false)
}

/// Memoized OpenCode source-database lookups for one flush. Titles and
/// parent links are immutable per session, so caching across the sessions
/// of a flush only collapses repeated reads of the same row.
#[derive(Default)]
struct OpencodeLookupCache {
    titles: HashMap<(String, String), Option<String>>,
    parented: HashMap<(String, String), bool>,
}

impl OpencodeLookupCache {
    fn title(&mut self, db_path: &str, session_id: &str) -> Option<String> {
        self.titles
            .entry((db_path.to_string(), session_id.to_string()))
            .or_insert_with(|| opencode_title_for_session(db_path, session_id))
            .clone()
    }

    fn has_parent(&mut self, db_path: &str, session_id: &str) -> bool {
        *self
            .parented
            .entry((db_path.to_string(), session_id.to_string()))
            .or_insert_with(|| opencode_session_has_parent(db_path, session_id))
    }
}

fn extract_session_label(
    source: SourceKind,
    source_path: &str,
    session_id: &str,
    first_user_text: Option<&str>,
    _cwd: Option<&str>,
    opencode: &mut OpencodeLookupCache,
) -> Option<String> {
    let raw = match source {
        SourceKind::Opencode => {
            if let Some(title) = opencode.title(source_path, session_id)
                && !title.trim().is_empty()
            {
                title
            } else {
                first_user_text?.to_string()
            }
        }
        SourceKind::Grok => {
            if let Some(title) = grok_title_for_session(source_path)
                && !title.trim().is_empty()
            {
                title
            } else {
                first_user_text?.to_string()
            }
        }
        SourceKind::Jcode => {
            if let Some(text) = jcode_label_from_file(source_path) {
                text
            } else {
                first_user_text?.to_string()
            }
        }
        _ => first_user_text?.to_string(),
    };
    let label = sanitize_label(&raw);
    if label.is_empty() { None } else { Some(label) }
}

fn infer_session_kind(
    source: SourceKind,
    source_path: &str,
    session_id: &str,
    initial_kind: Option<&str>,
    cwd: Option<&str>,
    first_user_text: Option<&str>,
    opencode: &mut OpencodeLookupCache,
) -> Option<String> {
    if let Some(kind) = initial_kind
        && kind != "main"
        && !kind.is_empty()
    {
        return Some(kind.to_string());
    }
    match source {
        SourceKind::Jcode => {
            // Same worker-sandbox rule as the parser: a bare /tmp cwd is
            // not evidence, only a sandbox leaf name is.
            if let Some(cwd) = cwd
                && jcode_tmp_cwd_is_worker_sandbox(cwd)
            {
                return Some("subagent".to_string());
            }
            if let Some(text) = first_user_text {
                // Shared with the jcode parser directive scan; see
                // `crate::types::jcode_text_is_subagent_directive`.
                if jcode_text_is_subagent_directive(&text.to_lowercase()) {
                    return Some("subagent".to_string());
                }
            }
        }
        SourceKind::Opencode => {
            // Same value the parser stores, so parse-time and backfill
            // classification can never disagree.
            if opencode.has_parent(source_path, session_id) {
                return Some("fork".to_string());
            }
        }
        // Match whole path components (like the Cursor parser's
        // `is_subagent_transcript`): a bare substring would false-positive
        // on projects such as `my-subagents-tool`.
        SourceKind::Muse | SourceKind::Claude | SourceKind::Cursor => {
            let normalized = source_path.replace('\\', "/");
            if normalized
                .split('/')
                .any(|c| c == "subagents" || c == "subagent")
            {
                return Some("subagent".to_string());
            }
            // Same agent-file convention as the Claude parser's
            // `is_agent_transcript`: applies to any of these sources.
            if source == SourceKind::Claude
                && let Some(name) = normalized.rsplit('/').next()
                && name.starts_with("agent-")
                && name.ends_with(".jsonl")
            {
                return Some("subagent".to_string());
            }
        }
        SourceKind::Codex => {}
        _ => {}
    }
    Some("main".to_string())
}

pub fn analytics_path(state_dir: &Path) -> PathBuf {
    state_dir.join("analytics.sqlite")
}

pub fn rebuild_from_records(
    path: impl AsRef<Path>,
    records: impl IntoIterator<Item = Record>,
) -> Result<()> {
    let mut writer = AnalyticsWriter::open(path)?;
    writer.clear()?;
    for record in records {
        writer.record(&record)?;
    }
    writer.flush()?;
    writer.store.mark_complete()
}

pub fn backfill_from_index(
    path: impl AsRef<Path>,
    index: &crate::index::SearchIndex,
) -> Result<()> {
    let mut writer = AnalyticsWriter::open(path)?;
    writer.clear()?;
    index
        .for_each_record(|record| {
            writer.record(&record)?;
            Ok(())
        })
        .context("read records for analytics backfill")?;
    writer.flush()?;
    writer.store.mark_complete()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::env_lock;
    use crate::types::RecordLinks;
    use std::fs;

    #[cfg(unix)]
    #[test]
    fn timed_out_child_is_killed_and_reaped() {
        let _guard = env_lock();
        let child = Command::new("sleep")
            .arg("30")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn child");
        let pid = child.id();

        assert!(child_output_before(child, Instant::now() + Duration::from_millis(20)).is_none());
        assert!(
            !Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stderr(Stdio::null())
                .status()
                .expect("check child")
                .success()
        );
    }

    #[test]
    fn writable_connections_use_durable_wal_commits() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store =
            AnalyticsStore::open(tmp.path().join("analytics.sqlite")).expect("open analytics");

        let journal_mode: String = store
            .conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("journal mode");
        let synchronous: i64 = store
            .conn
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .expect("synchronous mode");

        assert_eq!(journal_mode, "wal");
        assert_eq!(synchronous, 2, "FULL synchronous mode");
    }

    fn record(project: &str, session_id: &str, source_path: &Path, ts: u64) -> Record {
        Record {
            source: SourceKind::Codex,
            doc_id: ts,
            ts,
            project: project.to_string(),
            session_id: session_id.to_string(),
            turn_id: ts as u32,
            role: "user".to_string(),
            text: "hello".to_string(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks::default(),
            source_path: source_path.to_string_lossy().to_string(),
        }
    }

    #[test]
    fn detailed_sessions_exact_selectors_apply_before_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let store = AnalyticsStore::open(tmp.path().join("analytics.sqlite")).unwrap();
        store.conn.execute_batch(
            "insert into sessions (source, session_id, source_path, project, started_at, last_at) values
             ('codex', 'shared', '/old', 'target', 1, 1),
             ('codex', 'shared', '/new', 'target', 2, 2),
             ('codex', 'other', '/old', 'target', 3, 3),
             ('claude', 'shared', '/old', 'target', 4, 4);",
        ).unwrap();
        let query = |session_id, source_path| {
            store
                .query_sessions_detailed_selected(
                    Some(SourceFilter::Codex),
                    None,
                    None,
                    None,
                    None,
                    session_id,
                    source_path,
                    Some(1),
                )
                .unwrap()
        };
        let exact = query(Some("shared"), Some("/old"));
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0].last_at, 1);
        assert_eq!(exact[0].source, SourceKind::Codex);
        assert_eq!(query(Some("shared"), None)[0].source_path, "/new");
        assert_eq!(query(None, Some("/old"))[0].session_id, "other");
        assert!(query(Some("missing"), Some("/old")).is_empty());
        assert!(query(Some("shared"), Some("/old%")).is_empty());
        assert!(
            store
                .query_sessions_detailed_selected(
                    Some(SourceFilter::Codex),
                    Some("different"),
                    None,
                    None,
                    None,
                    Some("shared"),
                    Some("/old"),
                    Some(1),
                )
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn project_summaries_count_the_full_index_and_group_repositories() {
        let tmp = tempfile::tempdir().unwrap();
        let store = AnalyticsStore::open(tmp.path().join("analytics.sqlite")).unwrap();
        store.conn.execute_batch("begin").unwrap();
        for index in 0..250 {
            store.conn.execute(
                "insert into sessions (source, session_id, source_path, project, repo_project, started_at, last_at)
                 values ('codex', ?1, ?2, ?3, 'repo', 0, ?4)",
                params![format!("session-{index}"), format!("/path/{index}"), format!("worktree-{index}"), index + 1],
            ).unwrap();
        }
        store.conn.execute_batch(
            "insert into sessions (source, session_id, source_path, project, repo_project, started_at, last_at) values
             ('claude', 'session-0', '/path/0', 'raw', 'repo', 0, 999),
             ('codex', 'session-0', '/other-path', 'raw', 'repo', 0, 998),
             ('codex', 'older', '/older', 'older', '', 0, 12),
             ('codex', 'blank', '/blank', '   ', null, 0, 5000);
             insert into sessions (source, session_id, source_path, project, repo_project, started_at, last_at, conversation_kind) values
             ('codex', 'review', '/review', 'raw', 'repo', 0, 9000, 'guardian_review'),
             ('codex', 'review-only', '/review-only', 'raw', 'review-only', 0, 9001, 'guardian_review'),
             ('codex', 'unfiled-review', '/unfiled-review', 'raw', null, 0, 9002, 'guardian_review'),
             ('codex', 'agent', '/agent', 'raw', 'repo', 0, 900, 'subagent');
             commit;",
        ).unwrap();
        let summaries = store.query_project_summaries(None).unwrap();
        assert_eq!(
            summaries,
            vec![
                ProjectSummary {
                    project: UNFILED_PROJECT.into(),
                    session_count: 2,
                    last_at: Some(5000)
                },
                ProjectSummary {
                    project: "repo".into(),
                    session_count: 253,
                    last_at: Some(999)
                },
            ]
        );
        for summary in &summaries {
            let matching = store
                .query_sessions_detailed_filtered(
                    None,
                    Some(&summary.project),
                    None,
                    None,
                    Some(SessionKindFilter::Regular),
                    None,
                )
                .unwrap();
            assert_eq!(matching.len() as u64, summary.session_count);
        }
        let codex = store
            .query_project_summaries(Some(SourceFilter::Codex))
            .unwrap();
        assert_eq!(codex[1].session_count, 252);
        assert_eq!(codex[1].last_at, Some(998));
    }

    #[test]
    fn project_summaries_sort_ties_and_preserve_filter_keys_and_unknown_dates() {
        let tmp = tempfile::tempdir().unwrap();
        let store = AnalyticsStore::open(tmp.path().join("analytics.sqlite")).unwrap();
        store.conn.execute_batch(
            "insert into sessions (source, session_id, source_path, project, repo_project, started_at, last_at) values
             ('codex', 'b', '/b', 'raw', 'b', 0, 10),
             ('codex', 'a', '/a', 'raw', 'a', 0, 10),
             ('codex', 'zero', '/zero', 'raw', 'unknown', 0, 0),
             ('codex', 'negative', '/negative', 'raw', 'unknown', 0, -1),
             ('codex', 'encoded', '/encoded', 'raw', '-Users-nico-Code-project', 0, 0);",
        ).unwrap();
        let rows = store.query_project_summaries(None).unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.project.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "-Users-nico-Code-project", "unknown"]
        );
        assert_eq!(rows[2].last_at, None);
        assert_eq!(rows[3].last_at, None);
        assert_eq!(rows[3].session_count, 2);
    }

    #[test]
    fn display_project_decodes_path_shaped_project_slugs() {
        assert_eq!(display_project_name("-Users-nico-Code"), "~/Code");
        assert_eq!(
            display_project_name("-Users-nico-Code-sidequery-backend"),
            "sidequery-backend"
        );
        assert_eq!(display_project_name("model-serving"), "model-serving");
    }

    #[test]
    fn analytics_writer_rolls_records_up_to_sessions() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let transcript = tmp.path().join("session.jsonl");
        fs::write(
            &transcript,
            format!(
                "{{\"type\":\"session_meta\",\"payload\":{{\"cwd\":\"{}\"}}}}\n",
                tmp.path().display()
            ),
        )
        .expect("write transcript");
        let db = tmp.path().join("analytics.sqlite");
        let mut writer = AnalyticsWriter::open(&db).expect("open analytics");
        writer
            .record(&record("memex", "s1", &transcript, 10))
            .expect("record");
        writer
            .record(&record("memex", "s1", &transcript, 20))
            .expect("record");
        writer.flush().expect("flush");

        let store = AnalyticsStore::open(&db).expect("open store");
        let rows = store
            .query_sessions(None, None, None, ProjectGrouping::Flat, None)
            .expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].session_id, "s1");
        assert_eq!(rows[0].message_count, 2);
        assert_eq!(rows[0].last_at, 20);
    }

    #[test]
    fn detailed_sessions_filter_by_cwd_prefix() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repo = tmp.path().join("repo");
        let nested = repo.join("crates/core");
        let other = tmp.path().join("other");
        fs::create_dir_all(&nested).expect("mkdir");
        fs::create_dir_all(&other).expect("mkdir");
        let mut transcripts = Vec::new();
        for (name, cwd) in [("in.jsonl", &nested), ("out.jsonl", &other)] {
            let transcript = tmp.path().join(name);
            fs::write(
                &transcript,
                format!(
                    "{{\"type\":\"session_meta\",\"payload\":{{\"cwd\":\"{}\"}}}}\n",
                    cwd.display()
                ),
            )
            .expect("write transcript");
            transcripts.push(transcript);
        }
        let db = tmp.path().join("analytics.sqlite");
        let mut writer = AnalyticsWriter::open(&db).expect("open analytics");
        writer
            .record(&record("repo", "s-in", &transcripts[0], 10))
            .expect("record");
        writer
            .record(&record("other", "s-out", &transcripts[1], 20))
            .expect("record");
        writer.flush().expect("flush");

        let store = AnalyticsStore::open_read_only(&db).expect("open read only");
        let all = store
            .query_sessions_detailed(None, None, None, None, None)
            .expect("all sessions");
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].session_id, "s-out");

        let scoped = store
            .query_sessions_detailed(
                None,
                None,
                Some(repo.to_string_lossy().as_ref()),
                None,
                None,
            )
            .expect("scoped sessions");
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].session_id, "s-in");
        assert_eq!(scoped[0].cwd.as_deref(), Some(&*nested.to_string_lossy()));
    }

    #[test]
    fn detailed_sessions_cwd_filter_escapes_like_wildcards() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let target = tmp.path().join("foo_bar");
        let sibling = tmp.path().join("fooXbar");
        fs::create_dir_all(target.join("sub")).expect("mkdir");
        fs::create_dir_all(sibling.join("sub")).expect("mkdir");
        let mut transcripts = Vec::new();
        for (name, cwd) in [
            ("target.jsonl", target.join("sub")),
            ("sibling.jsonl", sibling.join("sub")),
        ] {
            let transcript = tmp.path().join(name);
            fs::write(
                &transcript,
                format!(
                    "{{\"type\":\"session_meta\",\"payload\":{{\"cwd\":\"{}\"}}}}\n",
                    cwd.display()
                ),
            )
            .expect("write transcript");
            transcripts.push(transcript);
        }
        let db = tmp.path().join("analytics.sqlite");
        let mut writer = AnalyticsWriter::open(&db).expect("open analytics");
        writer
            .record(&record("foo_bar", "s-target", &transcripts[0], 10))
            .expect("record");
        writer
            .record(&record("fooXbar", "s-sibling", &transcripts[1], 20))
            .expect("record");
        writer.flush().expect("flush");

        let store = AnalyticsStore::open_read_only(&db).expect("open read only");
        let scoped = store
            .query_sessions_detailed(
                None,
                None,
                Some(target.to_string_lossy().as_ref()),
                None,
                None,
            )
            .expect("scoped sessions");
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].session_id, "s-target");
    }

    #[test]
    fn read_only_store_rejects_writes() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = tmp.path().join("analytics.sqlite");
        drop(AnalyticsStore::open(&db).expect("initialize analytics"));

        let store = AnalyticsStore::open_read_only(&db).expect("open read only");

        assert!(store.mark_complete().is_err());
    }

    #[test]
    fn project_queries_are_distinct_and_timeline_projection_is_narrow() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let source_a = tmp.path().join("a.jsonl");
        let source_b = tmp.path().join("b.jsonl");
        fs::write(&source_a, "").expect("source a");
        fs::write(&source_b, "").expect("source b");
        let db = tmp.path().join("analytics.sqlite");
        rebuild_from_records(
            &db,
            [
                record("alpha", "s1", &source_a, 10),
                record("alpha", "s2", &source_b, 20),
            ],
        )
        .expect("rebuild");
        let store = AnalyticsStore::open_read_only(&db).expect("open read only");

        assert_eq!(
            store
                .query_projects(None, ProjectGrouping::Flat)
                .expect("projects"),
            vec!["alpha"]
        );
        assert_eq!(
            store
                .query_project_timestamps(None, Some(15), ProjectGrouping::Flat)
                .expect("timestamps"),
            vec![("alpha".to_string(), 20)]
        );
    }

    #[test]
    fn source_timestamps_apply_activity_filters_without_a_result_limit() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = tmp.path().join("analytics.sqlite");
        let records = [("alpha", "s1", 10), ("alpha", "s2", 20), ("beta", "s3", 30)]
            .into_iter()
            .map(|(project, session, ts)| {
                record(
                    project,
                    session,
                    &tmp.path().join(format!("{session}.jsonl")),
                    ts,
                )
            });
        rebuild_from_records(&db, records).expect("rebuild");
        let store = AnalyticsStore::open_read_only(&db).expect("open read only");

        assert_eq!(
            store
                .query_source_timestamps_filtered(
                    Some(SourceFilter::Codex),
                    Some(15),
                    Some(25),
                    Some("alpha"),
                    ProjectGrouping::Flat,
                    None,
                )
                .expect("filtered activity"),
            vec![(SourceKind::Codex, 20)]
        );
    }

    #[test]
    fn repository_project_filter_uses_expression_index() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = tmp.path().join("analytics.sqlite");
        let store = AnalyticsStore::open(&db).expect("open analytics");
        let plan: String = store
            .conn
            .query_row(
                "EXPLAIN QUERY PLAN SELECT source FROM sessions
                 WHERE COALESCE(NULLIF(repo_project, ''), 'Unfiled') = ?1
                 ORDER BY last_at DESC LIMIT 200",
                params!["memex"],
                |row| row.get(3),
            )
            .expect("query plan");

        assert!(
            plan.contains("sessions_repository_project_last_at_idx"),
            "{plan}"
        );
    }

    #[test]
    fn repository_grouping_buckets_non_git_sessions_as_unfiled() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let standalone = tmp.path().join("generated-task-name");
        fs::create_dir(&standalone).expect("standalone dir");
        let standalone_transcript = tmp.path().join("standalone.jsonl");
        fs::write(
            &standalone_transcript,
            format!(
                "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"standalone\",\"cwd\":\"{}\"}}}}\n",
                standalone.display()
            ),
        )
        .expect("write standalone transcript");
        let repo_transcript = tmp.path().join("repo.jsonl");
        fs::write(&repo_transcript, "").expect("write repo transcript");
        let db = tmp.path().join("analytics.sqlite");
        rebuild_from_records(
            &db,
            [
                record(
                    "generated-task-name",
                    "standalone",
                    &standalone_transcript,
                    10,
                ),
                record("raw-repo-slug", "repo", &repo_transcript, 20),
            ],
        )
        .expect("rebuild");

        let store = AnalyticsStore::open(&db).expect("open store");
        store
            .conn
            .execute(
                "UPDATE sessions SET repo_project = 'alpha' WHERE session_id = 'repo'",
                [],
            )
            .expect("seed repository project");

        let rows = store
            .query_sessions(None, None, None, ProjectGrouping::Repository, None)
            .expect("repository sessions");
        assert_eq!(rows[0].display_project, "alpha");
        assert_eq!(rows[1].project, "generated-task-name");
        assert_eq!(rows[1].display_project, UNFILED_PROJECT);
        assert_eq!(
            store
                .query_projects(None, ProjectGrouping::Repository)
                .expect("repository projects"),
            vec![UNFILED_PROJECT, "alpha"]
        );

        let unfiled = store
            .query_sessions(
                None,
                None,
                Some(UNFILED_PROJECT),
                ProjectGrouping::Repository,
                None,
            )
            .expect("unfiled sessions");
        assert_eq!(unfiled.len(), 1);
        assert_eq!(unfiled[0].session_id, "standalone");
        assert_eq!(
            store
                .query_sessions_detailed(None, Some(UNFILED_PROJECT), None, None, None)
                .expect("detailed unfiled sessions")
                .len(),
            1
        );
        assert_eq!(
            store
                .query_project_timestamps(None, None, ProjectGrouping::Repository)
                .expect("repository timestamps"),
            vec![(UNFILED_PROJECT.to_string(), 10), ("alpha".to_string(), 20)]
        );

        let session_projects = store
            .query_session_projects(
                &[(
                    SourceKind::Codex,
                    "standalone".to_string(),
                    standalone_transcript.to_string_lossy().to_string(),
                )],
                ProjectGrouping::Repository,
            )
            .expect("session projects");
        assert_eq!(
            session_projects.values().next().map(String::as_str),
            Some(UNFILED_PROJECT)
        );

        let flat = store
            .query_sessions(None, None, None, ProjectGrouping::Flat, None)
            .expect("flat sessions");
        assert_eq!(flat[1].display_project, "generated-task-name");
    }

    #[test]
    fn analytics_schema_version_change_marks_incomplete() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = tmp.path().join("analytics.sqlite");
        {
            let conn = Connection::open(&db).expect("open sqlite");
            conn.execute_batch(
                r#"
                CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                INSERT INTO meta(key, value) VALUES('schema_version', '1');
                INSERT INTO meta(key, value) VALUES('analytics_complete', '1');
                "#,
            )
            .expect("seed meta");
        }

        let store = AnalyticsStore::open(&db).expect("open store");

        assert!(!store.complete().expect("complete"));
    }

    #[test]
    fn repository_grouping_uses_git_common_dir_project() {
        let _guard = env_lock();
        let tmp = tempfile::tempdir().expect("tempdir");
        let repo = tmp.path().join("memex");
        fs::create_dir_all(&repo).expect("repo dir");
        assert!(
            Command::new("git")
                .args(["init"])
                .current_dir(&repo)
                .output()
                .expect("git init")
                .status
                .success()
        );
        let transcript = tmp.path().join("session.jsonl");
        fs::write(
            &transcript,
            format!(
                "{{\"cwd\":\"{}\",\"type\":\"session_meta\",\"payload\":{{\"cwd\":\"{}\"}}}}\n",
                repo.display(),
                repo.display()
            ),
        )
        .expect("write transcript");

        let db = tmp.path().join("analytics.sqlite");
        rebuild_from_records(
            &db,
            [record(
                "memex-claude-worktrees-feature",
                "s1",
                &transcript,
                10,
            )],
        )
        .expect("rebuild");

        let store = AnalyticsStore::open(&db).expect("open store");
        let rows = store
            .query_sessions(None, None, None, ProjectGrouping::Repository, None)
            .expect("query");
        assert_eq!(rows[0].project, "memex-claude-worktrees-feature");
        assert_eq!(rows[0].display_project, "memex");
    }

    #[test]
    fn claude_worktree_path_falls_back_to_parent_repo() {
        assert_eq!(
            claude_worktree_repo_project(
                "/Users/nico/Code/atm-backend/.claude/worktrees/exciting-morse-e2914f"
            )
            .as_deref(),
            Some("atm-backend")
        );
        assert_eq!(
            claude_worktree_repo_project("/Users/nico/Code/atm-backend"),
            None
        );
    }

    #[test]
    fn codex_worktree_path_falls_back_to_repo_directory() {
        let cwd = "/missing/home/.codex/worktrees/8952/memex/crates/core";
        assert_eq!(codex_worktree_repo_project(cwd).as_deref(), Some("memex"));
        let metadata = git_metadata_for_cwd(cwd);
        assert_eq!(metadata.repo_project.as_deref(), Some("memex"));
        assert_eq!(metadata.status, "path-fallback");
        assert_eq!(
            codex_worktree_repo_project("/missing/home/.codex/worktrees/8952"),
            None
        );
        assert_eq!(
            codex_worktree_repo_project("/missing/home/Documents/Codex/2026-09-07/hel"),
            None
        );
    }

    #[test]
    fn repository_grouping_uses_claude_worktree_path_without_local_git() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let transcript = tmp.path().join("session.jsonl");
        fs::write(
            &transcript,
            "{\"cwd\":\"/Users/nico/Code/atm-backend/.claude/worktrees/exciting-morse-e2914f\"}\n",
        )
        .expect("write transcript");

        let db = tmp.path().join("analytics.sqlite");
        rebuild_from_records(
            &db,
            [record(
                "ssh-d4309b74-100f-407e-b64d-31c7160044cd",
                "s1",
                &transcript,
                10,
            )],
        )
        .expect("rebuild");

        let store = AnalyticsStore::open(&db).expect("open store");
        let rows = store
            .query_sessions(None, None, None, ProjectGrouping::Repository, None)
            .expect("query");
        assert_eq!(rows[0].project, "ssh-d4309b74-100f-407e-b64d-31c7160044cd");
        assert_eq!(rows[0].display_project, "atm-backend");
    }

    #[test]
    fn sanitize_label_collapses_whitespace_and_truncates() {
        let raw = "  Hello\n   world   \x1b[31mred\x1b[0m  <system-reminder>ignore</system-reminder>  this is a very long prompt that should be truncated at word boundary because it exceeds the one hundred fifty character limit significantly and we want to ensure ellipsis handling works correctly for display";
        let label = sanitize_label(raw);
        assert!(!label.contains('\n'));
        assert!(!label.contains("\x1b"));
        assert!(!label.contains("ignore"));
        assert!(label.chars().count() <= MAX_LABEL_CHARS);
        assert!(label.ends_with('…') || label.chars().count() < MAX_LABEL_CHARS);
        assert!(label.starts_with("Hello world red"));
    }

    #[test]
    fn analytics_stores_label_from_first_user_message() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let transcript = tmp.path().join("session.jsonl");
        fs::write(
            &transcript,
            format!(
                "{{\"type\":\"session_meta\",\"payload\":{{\"cwd\":\"{}\"}}}}\n",
                tmp.path().display()
            ),
        )
        .expect("write");
        let db = tmp.path().join("analytics.sqlite");
        let mut writer = AnalyticsWriter::open(&db).expect("open");
        let mut rec = record("proj", "s-label", &transcript, 10);
        rec.role = "user".to_string();
        rec.text = "Fix the login bug on the dashboard".to_string();
        rec.links.conversation_kind = Some("main".to_string());
        writer.record(&rec).expect("record");
        writer.flush().expect("flush");
        let store = AnalyticsStore::open_read_only(&db).expect("open ro");
        let rows = store
            .query_sessions_detailed(None, None, None, None, None)
            .expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].label.as_deref(),
            Some("Fix the login bug on the dashboard")
        );
        assert_eq!(rows[0].conversation_kind.as_deref(), Some("main"));
    }

    #[test]
    fn incremental_delta_keeps_original_label_and_kind() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let transcript = tmp.path().join("session.jsonl");
        fs::write(&transcript, "").expect("write");
        let db = tmp.path().join("analytics.sqlite");
        let mut first = record("proj", "s-delta", &transcript, 10);
        first.role = "user".to_string();
        first.text = "REAL FIRST PROMPT about the login bug".to_string();
        first.links.conversation_kind = Some("main".to_string());
        let mut writer = AnalyticsWriter::open(&db).expect("open");
        writer.record(&first).expect("record");
        writer.flush().expect("flush");
        drop(writer);
        // A later incremental run sees only a mid-session message, possibly
        // with a different per-record kind. Neither may clobber the stored row.
        let mut delta = record("proj", "s-delta", &transcript, 20);
        delta.role = "user".to_string();
        delta.text = "now also update the changelog".to_string();
        delta.links.conversation_kind = Some("subagent".to_string());
        let mut writer = AnalyticsWriter::open(&db).expect("reopen");
        writer.record(&delta).expect("record");
        writer.flush().expect("flush");
        let store = AnalyticsStore::open_read_only(&db).expect("open ro");
        let rows = store
            .query_sessions_detailed(None, None, None, None, None)
            .expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].label.as_deref(),
            Some("REAL FIRST PROMPT about the login bug")
        );
        assert_eq!(rows[0].conversation_kind.as_deref(), Some("main"));
    }

    #[test]
    fn main_record_wins_over_compaction_entries() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let transcript = tmp.path().join("session.jsonl");
        fs::write(&transcript, "").expect("write");
        let db = tmp.path().join("analytics.sqlite");
        let mut writer = AnalyticsWriter::open(&db).expect("open");
        // Compaction summary arrives before any main message, as in a
        // resumed Pi transcript: the session is still interactive.
        let mut summary = record("proj", "s-compact", &transcript, 5);
        summary.role = "user".to_string();
        summary.text = "summary of prior work".to_string();
        summary.links.conversation_kind = Some("compaction".to_string());
        writer.record(&summary).expect("record");
        let mut prompt = record("proj", "s-compact", &transcript, 10);
        prompt.role = "user".to_string();
        prompt.text = "please fix the parser".to_string();
        prompt.links.conversation_kind = Some("main".to_string());
        writer.record(&prompt).expect("record");
        writer.flush().expect("flush");
        let store = AnalyticsStore::open_read_only(&db).expect("open ro");
        let rows = store
            .query_sessions_detailed(None, None, None, None, None)
            .expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].conversation_kind.as_deref(), Some("main"));
    }

    #[test]
    fn every_stored_kind_has_a_defined_filter_bucket() {
        // Exercise every stored kind through SQL and the in-memory predicate.
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = tmp.path().join("analytics.sqlite");
        let mut writer = AnalyticsWriter::open(&db).expect("open");
        for (id, kind, ts) in [
            ("s-main", "main", 10),
            ("s-sub", "subagent", 20),
            ("s-fork", "fork", 30),
            ("s-side", "sidechain", 40),
            ("s-compact", "compaction", 50),
            ("s-branch", "branch", 60),
            ("s-review", "guardian_review", 70),
        ] {
            let path = tmp.path().join(format!("{id}.jsonl"));
            fs::write(&path, "").expect("write");
            let mut rec = record("proj", id, &path, ts);
            rec.role = "user".to_string();
            rec.text = format!("task {id}");
            rec.links.conversation_kind = Some(kind.to_string());
            writer.record(&rec).expect("record");
        }
        writer.flush().expect("flush");
        let store = AnalyticsStore::open_read_only(&db).expect("open ro");
        let filtered = |kind| {
            store
                .query_sessions_detailed_filtered(None, None, None, None, Some(kind), None)
                .expect("query")
                .into_iter()
                .map(|row| row.session_id)
                .collect::<Vec<_>>()
        };
        assert_eq!(filtered(SessionKindFilter::Primary), vec!["s-main"]);
        let mut sub = filtered(SessionKindFilter::Subagent);
        sub.sort();
        assert_eq!(
            sub,
            vec!["s-branch", "s-compact", "s-fork", "s-side", "s-sub"]
        );
        assert_eq!(filtered(SessionKindFilter::Regular).len(), 6);
        assert_eq!(filtered(SessionKindFilter::All).len(), 7);
        for filter in [
            SessionKindFilter::Primary,
            SessionKindFilter::Subagent,
            SessionKindFilter::Regular,
            SessionKindFilter::All,
        ] {
            let rows = store
                .query_sessions_filtered(
                    None,
                    None,
                    None,
                    ProjectGrouping::Flat,
                    Some(filter),
                    None,
                )
                .unwrap();
            assert_eq!(rows.len(), filtered(filter).len());
            for row in rows {
                assert!(filter.matches_kind(row.conversation_kind.as_deref()));
            }
            assert_eq!(
                filter.matches_kind(Some("guardian_review")),
                filter == SessionKindFilter::All
            );
        }
        assert_eq!(
            store
                .query_sessions_detailed(None, None, None, None, None)
                .expect("all")
                .len(),
            7
        );
    }

    #[test]
    fn session_cwd_resolves_from_jcode_working_dir() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("session_cwd.json");
        fs::write(
            &path,
            r#"{"id":"s-cwd","working_dir":"/repo/example","messages":[]}"#,
        )
        .expect("write");
        let cwd =
            resolve_session_cwd_from_parts(SourceKind::Jcode, &path.to_string_lossy(), "s-cwd");
        assert_eq!(cwd.as_deref(), Some("/repo/example"));
    }

    #[test]
    fn analytics_filters_by_conversation_kind() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let a_path = tmp.path().join("a.jsonl");
        let b_path = tmp.path().join("b.jsonl");
        for p in [&a_path, &b_path] {
            fs::write(p, "").expect("write");
        }
        let db = tmp.path().join("analytics.sqlite");
        let mut writer = AnalyticsWriter::open(&db).expect("open");
        let mut primary = record("proj", "s-primary", &a_path, 10);
        primary.role = "user".to_string();
        primary.text = "primary task".to_string();
        primary.links.conversation_kind = Some("main".to_string());
        let mut sub = record("proj", "s-sub", &b_path, 20);
        sub.role = "user".to_string();
        sub.text = "subagent task".to_string();
        sub.links.conversation_kind = Some("subagent".to_string());
        writer.record(&primary).expect("record");
        writer.record(&sub).expect("record");
        writer.flush().expect("flush");
        let store = AnalyticsStore::open_read_only(&db).expect("open ro");
        let primary_rows = store
            .query_sessions_detailed_filtered(
                None,
                None,
                None,
                None,
                Some(SessionKindFilter::Primary),
                None,
            )
            .expect("primary");
        assert_eq!(primary_rows.len(), 1);
        assert_eq!(primary_rows[0].session_id, "s-primary");
        let sub_rows = store
            .query_sessions_detailed_filtered(
                None,
                None,
                None,
                None,
                Some(SessionKindFilter::Subagent),
                None,
            )
            .expect("sub");
        assert_eq!(sub_rows.len(), 1);
        assert_eq!(sub_rows[0].session_id, "s-sub");
        let all_rows = store
            .query_sessions_detailed(None, None, None, None, None)
            .expect("all");
        assert_eq!(all_rows.len(), 2);

        let primary_ts = store
            .query_source_timestamps_filtered(
                None,
                None,
                None,
                None,
                ProjectGrouping::Flat,
                Some(SessionKindFilter::Primary),
            )
            .expect("primary ts");
        assert_eq!(primary_ts, vec![(SourceKind::Codex, 10)]);

        let sub_ts = store
            .query_source_timestamps_filtered(
                None,
                None,
                None,
                None,
                ProjectGrouping::Flat,
                Some(SessionKindFilter::Subagent),
            )
            .expect("sub ts");
        assert_eq!(sub_ts, vec![(SourceKind::Codex, 20)]);

        let all_ts = store
            .query_source_timestamps_filtered(
                None,
                None,
                None,
                None,
                ProjectGrouping::Flat,
                Some(SessionKindFilter::All),
            )
            .expect("all ts");
        assert_eq!(all_ts.len(), 2);
    }

    #[test]
    fn jcode_tmp_cwd_needs_worker_sandbox_leaf_for_subagent() {
        let _tmp = tempfile::tempdir().expect("tempdir");
        let _transcript = _tmp.path().join("session_session_tmp.json");
        // Need a file that contains cwd; but we also need to test inference via cwd.
        // We'll directly test infer_session_kind helper.
        let infer = |cwd: &str| {
            infer_session_kind(
                SourceKind::Jcode,
                "/tmp/.jcode/sessions/session_tmp.json",
                "session_tmp",
                None,
                Some(cwd),
                Some("hello"),
                &mut OpencodeLookupCache::default(),
            )
        };
        // A bare /tmp cwd is not evidence: users legitimately work in /tmp.
        assert_eq!(infer("/tmp/work").as_deref(), Some("main"));
        assert_eq!(infer("/tmp").as_deref(), Some("main"));
        // A worker-sandbox leaf under /tmp corroborates a spawned worker.
        assert_eq!(
            infer("/private/tmp/bossmode-hygiene-v2-worker-docs").as_deref(),
            Some("subagent")
        );
        let kind2 = infer_session_kind(
            SourceKind::Jcode,
            "/tmp/.jcode/sessions/session_main.json",
            "session_main",
            Some("main"),
            Some("/repo/example"),
            Some("hello"),
            &mut OpencodeLookupCache::default(),
        );
        assert_eq!(kind2.as_deref(), Some("main"));
    }

    #[test]
    fn strip_ansi_preserves_multibyte_unicode() {
        let label = sanitize_label("Fix the 🚀 deploy \x1b[31mred\x1b[0m pipeline ✅ now");
        assert_eq!(label, "Fix the 🚀 deploy red pipeline ✅ now");
        assert!(label.contains('🚀'));
    }

    #[test]
    fn sanitize_label_truncates_unicode_by_chars_not_bytes() {
        let raw = "🚀".repeat(200);
        let label = sanitize_label(&raw);
        assert!(label.chars().count() <= MAX_LABEL_CHARS);
        assert!(label.contains('…'));
        assert!(label.starts_with("🚀"));
    }

    #[test]
    fn sanitize_label_with_unicode_case_fold_before_tag_does_not_panic() {
        // U+0130 folds to 2 chars on lowercase; byte offsets computed on the
        // folded copy would be wrong for the original. Must strip safely.
        let raw = "İstanbul \u{130} <SYSTEM-REMINDER>hidden</SYSTEM-REMINDER> visible task";
        let label = sanitize_label(raw);
        assert!(!label.contains("hidden"));
        assert!(!label.contains("SYSTEM-REMINDER"));
        assert!(label.contains("visible task"));
    }

    #[test]
    fn sanitize_label_preserves_lone_comparison_brackets() {
        let raw = "a < b and c > d comparison";
        let label = sanitize_label(raw);
        assert_eq!(label, "a < b and c > d comparison");
    }

    #[test]
    fn session_conversation_kind_resolves_stored_row() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let transcript = tmp.path().join("session.jsonl");
        fs::write(&transcript, "").expect("write");
        let db = tmp.path().join("analytics.sqlite");
        let mut writer = AnalyticsWriter::open(&db).expect("open");
        let mut rec = record("proj", "s-kind", &transcript, 10);
        rec.role = "user".to_string();
        rec.text = "do the thing".to_string();
        rec.links.conversation_kind = Some("fork".to_string());
        writer.record(&rec).expect("record");
        writer.flush().expect("flush");
        let store = AnalyticsStore::open_read_only(&db).expect("open ro");
        assert_eq!(
            store.session_conversation_kind(
                SourceKind::Codex.storage_label(),
                "s-kind",
                &transcript.to_string_lossy()
            ),
            Some("fork".to_string())
        );
        assert_eq!(
            store.session_conversation_kind(
                SourceKind::Codex.storage_label(),
                "s-missing",
                &transcript.to_string_lossy()
            ),
            None
        );
    }

    #[test]
    fn sanitize_label_keeps_word_boundary_on_control_whitespace() {
        assert_eq!(sanitize_label("hello\nworld"), "hello world");
        assert_eq!(sanitize_label("hello\tworld"), "hello world");
        assert_eq!(sanitize_label("hello\r\nworld"), "hello world");
    }

    #[test]
    fn infer_session_kind_matches_cursor_path_components_only() {
        let mut cache = OpencodeLookupCache::default();
        // Bare substring must not classify: project merely mentions subagents.
        let not_sub = infer_session_kind(
            SourceKind::Cursor,
            "/data/my-subagents-tool/session.json",
            "session",
            Some("main"),
            None,
            None,
            &mut cache,
        );
        assert_eq!(not_sub.as_deref(), Some("main"));
        let is_sub = infer_session_kind(
            SourceKind::Cursor,
            "/data/Cursor/projects/subagents/agent-1/transcript.json",
            "agent-1",
            Some("main"),
            None,
            None,
            &mut cache,
        );
        assert_eq!(is_sub.as_deref(), Some("subagent"));
    }

    #[test]
    fn infer_session_kind_matches_muse_plural_subagents_dir() {
        let mut cache = OpencodeLookupCache::default();
        let kind = infer_session_kind(
            SourceKind::Muse,
            "/data/muse/projects/p/subagents/abc123/stream.jsonl",
            "abc123",
            Some("main"),
            None,
            None,
            &mut cache,
        );
        assert_eq!(kind.as_deref(), Some("subagent"));
    }

    #[test]
    fn extract_session_label_falls_back_without_opencode_db() {
        let mut cache = OpencodeLookupCache::default();
        let label = extract_session_label(
            SourceKind::Opencode,
            "/nonexistent/opencode.db",
            "missing",
            Some("  hello world  "),
            None,
            &mut cache,
        );
        assert_eq!(label.as_deref(), Some("hello world"));
    }
}
