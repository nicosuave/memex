use crate::types::{Record, SourceFilter, SourceKind};
use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params, params_from_iter};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

const SCHEMA_VERSION: i64 = 4;
const CATALOG_BATCH_RECORDS: usize = 4_096;

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
    pub started_at: u64,
    pub last_at: u64,
    pub message_count: u64,
}

pub struct AnalyticsStore {
    conn: Connection,
}

pub struct AnalyticsWriter {
    store: AnalyticsStore,
    sessions: HashMap<SessionKey, SessionAccumulator>,
    metadata_cache: HashMap<SessionKey, SessionMetadata>,
    git_cache: HashMap<String, GitMetadata>,
    catalog_batch_open: bool,
    catalog_batch_records: usize,
    affected_session_keys: HashSet<String>,
    bulk_load: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SessionKey {
    stable_key: String,
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
}

struct LegacySessionRow {
    source: String,
    session_id: String,
    source_path: String,
    project: String,
    cwd: Option<String>,
    git_root: Option<String>,
    git_common_dir: Option<String>,
    repo_project: Option<String>,
    started_at: i64,
    last_at: i64,
    message_count: i64,
    resolution_status: String,
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
        let mut store = Self { conn };
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

    fn init(&mut self) -> Result<()> {
        crate::catalog::init_connection(&self.conn)?;
        self.migrate_legacy_sessions()?;
        self.conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA cache_size = -131072;
            PRAGMA mmap_size = 268435456;
            PRAGMA temp_store = MEMORY;
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                session_key TEXT PRIMARY KEY,
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
                resolution_status TEXT NOT NULL DEFAULT ''
            );
            CREATE UNIQUE INDEX IF NOT EXISTS sessions_source_identity_idx
                ON sessions(source, session_id) WHERE session_id != '';
            CREATE INDEX IF NOT EXISTS sessions_last_at_idx ON sessions(last_at);
            CREATE INDEX IF NOT EXISTS sessions_project_last_at_idx ON sessions(project, last_at);
            CREATE INDEX IF NOT EXISTS sessions_repo_project_last_at_idx ON sessions(repo_project, last_at);
            CREATE INDEX IF NOT EXISTS sessions_display_project_last_at_idx
                ON sessions(COALESCE(NULLIF(repo_project, ''), project), last_at);
            CREATE INDEX IF NOT EXISTS sessions_source_last_at_idx ON sessions(source, last_at);
            CREATE TABLE IF NOT EXISTS source_projections (
                source TEXT NOT NULL,
                source_path TEXT PRIMARY KEY,
                source_identity TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                parser_version INTEGER NOT NULL,
                committed_offset INTEGER NOT NULL DEFAULT 0,
                projection_epoch INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                record_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            );
            CREATE INDEX IF NOT EXISTS source_projections_status_idx
                ON source_projections(status);
            CREATE INDEX IF NOT EXISTS source_projections_source_idx
                ON source_projections(source);
            "#,
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
        }
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES('schema_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![SCHEMA_VERSION.to_string()],
        )?;
        Ok(())
    }

    fn migrate_legacy_sessions(&mut self) -> Result<()> {
        let sessions_exists = self.conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sessions'
            )",
            [],
            |row| row.get::<_, bool>(0),
        )?;
        if !sessions_exists {
            return Ok(());
        }

        let has_session_key = self.conn.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM pragma_table_info('sessions') WHERE name = 'session_key'
            )",
            [],
            |row| row.get::<_, bool>(0),
        )?;
        if has_session_key {
            return Ok(());
        }

        let tx = self.conn.transaction()?;
        let legacy_rows = {
            let mut stmt = tx.prepare(
                "SELECT source, session_id, source_path, project, cwd, git_root,
                        git_common_dir, repo_project, started_at, last_at, message_count,
                        resolution_status
                 FROM sessions",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(LegacySessionRow {
                    source: row.get(0)?,
                    session_id: row.get(1)?,
                    source_path: row.get(2)?,
                    project: row.get(3)?,
                    cwd: row.get(4)?,
                    git_root: row.get(5)?,
                    git_common_dir: row.get(6)?,
                    repo_project: row.get(7)?,
                    started_at: row.get(8)?,
                    last_at: row.get(9)?,
                    message_count: row.get(10)?,
                    resolution_status: row.get(11)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        tx.execute_batch(
            r#"
            CREATE TABLE sessions_v4 (
                session_key TEXT PRIMARY KEY,
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
                resolution_status TEXT NOT NULL DEFAULT ''
            );
            "#,
        )?;
        {
            let mut insert = tx.prepare(
                r#"
                INSERT INTO sessions_v4(
                    session_key, source, session_id, source_path, project, cwd, git_root,
                    git_common_dir, repo_project, started_at, last_at, message_count,
                    resolution_status
                )
                VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                ON CONFLICT(session_key) DO UPDATE SET
                    source_path = CASE
                        WHEN excluded.last_at >= sessions_v4.last_at
                        THEN excluded.source_path ELSE sessions_v4.source_path END,
                    project = CASE
                        WHEN excluded.last_at >= sessions_v4.last_at
                        THEN excluded.project ELSE sessions_v4.project END,
                    cwd = CASE
                        WHEN excluded.last_at >= sessions_v4.last_at
                        THEN excluded.cwd ELSE sessions_v4.cwd END,
                    git_root = CASE
                        WHEN excluded.last_at >= sessions_v4.last_at
                        THEN excluded.git_root ELSE sessions_v4.git_root END,
                    git_common_dir = CASE
                        WHEN excluded.last_at >= sessions_v4.last_at
                        THEN excluded.git_common_dir ELSE sessions_v4.git_common_dir END,
                    repo_project = CASE
                        WHEN excluded.last_at >= sessions_v4.last_at
                        THEN excluded.repo_project ELSE sessions_v4.repo_project END,
                    started_at = MIN(sessions_v4.started_at, excluded.started_at),
                    last_at = MAX(sessions_v4.last_at, excluded.last_at),
                    message_count = MAX(sessions_v4.message_count, excluded.message_count),
                    resolution_status = CASE
                        WHEN excluded.last_at >= sessions_v4.last_at
                        THEN excluded.resolution_status ELSE sessions_v4.resolution_status END
                "#,
            )?;
            for row in legacy_rows {
                let session_key = crate::catalog::session_key_from_label(
                    &row.source,
                    &row.session_id,
                    &row.source_path,
                );
                insert.execute(params![
                    session_key,
                    row.source,
                    row.session_id,
                    row.source_path,
                    row.project,
                    row.cwd,
                    row.git_root,
                    row.git_common_dir,
                    row.repo_project,
                    row.started_at,
                    row.last_at,
                    row.message_count,
                    row.resolution_status,
                ])?;
            }
        }
        tx.execute_batch(
            r#"
            DROP TABLE sessions;
            ALTER TABLE sessions_v4 RENAME TO sessions;
            "#,
        )?;
        tx.commit().context("migrate legacy sessions schema")?;
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

    pub fn clear_projection_manifest(&self) -> Result<()> {
        self.conn.execute("DELETE FROM source_projections", [])?;
        Ok(())
    }

    pub fn delete_source_path(&self, source_path: &str) -> Result<()> {
        crate::catalog::delete_source_path(&self.conn, source_path)?;
        self.conn.execute(
            "DELETE FROM sessions
             WHERE session_key NOT IN (SELECT DISTINCT session_key FROM records)",
            [],
        )?;
        Ok(())
    }

    pub fn incomplete_source_paths(&self) -> Result<HashSet<String>> {
        let mut statement = self
            .conn
            .prepare("SELECT source_path FROM source_projections WHERE status != 'committed'")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<std::result::Result<HashSet<_>, _>>()
            .map_err(Into::into)
    }

    pub fn projection_sources(&self) -> Result<HashMap<String, SourceKind>> {
        let mut statement = self
            .conn
            .prepare("SELECT source_path, source FROM source_projections")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut sources = HashMap::new();
        for row in rows {
            let (path, source) = row?;
            if let Some(source) = SourceKind::from_label(&source) {
                sources.insert(path, source);
            }
        }
        Ok(sources)
    }

    pub fn begin_projection(
        &self,
        source: SourceKind,
        source_path: &str,
        source_identity: &str,
        source_fingerprint: &str,
        parser_version: u32,
        committed_offset: u64,
    ) -> Result<u64> {
        let previous_epoch: Option<i64> = self
            .conn
            .query_row(
                "SELECT projection_epoch FROM source_projections WHERE source_path = ?1",
                params![source_path],
                |row| row.get(0),
            )
            .optional()?;
        let epoch = previous_epoch.unwrap_or(0).saturating_add(1).max(1);
        self.conn.execute(
            r#"
            INSERT INTO source_projections(
                source, source_path, source_identity, source_fingerprint, parser_version,
                committed_offset, projection_epoch, status, record_count, last_error
            )
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, 'projecting', 0, NULL)
            ON CONFLICT(source_path) DO UPDATE SET
                source = excluded.source,
                source_identity = excluded.source_identity,
                source_fingerprint = excluded.source_fingerprint,
                parser_version = excluded.parser_version,
                projection_epoch = excluded.projection_epoch,
                status = 'projecting',
                last_error = NULL
            "#,
            params![
                source.storage_label(),
                source_path,
                source_identity,
                source_fingerprint,
                parser_version as i64,
                committed_offset.min(i64::MAX as u64) as i64,
                epoch,
            ],
        )?;
        Ok(epoch as u64)
    }

    pub fn complete_projection(
        &self,
        source_path: &str,
        source_identity: &str,
        committed_offset: u64,
        projection_epoch: u64,
        record_count: usize,
    ) -> Result<()> {
        let changed = self.conn.execute(
            r#"
            UPDATE source_projections
            SET source_identity = ?2,
                committed_offset = ?3,
                status = 'committed',
                record_count = ?4,
                last_error = NULL
            WHERE source_path = ?1 AND projection_epoch = ?5
            "#,
            params![
                source_path,
                source_identity,
                committed_offset.min(i64::MAX as u64) as i64,
                record_count.min(i64::MAX as usize) as i64,
                projection_epoch.min(i64::MAX as u64) as i64,
            ],
        )?;
        if changed != 1 {
            anyhow::bail!("projection epoch changed before commit for source path {source_path}");
        }
        Ok(())
    }

    pub fn delete_projection(&self, source_path: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM source_projections WHERE source_path = ?1",
            params![source_path],
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
        let mut sql = String::from(
            "SELECT source, session_id, source_path, project,
                    COALESCE(NULLIF(repo_project, ''), project) AS display_project,
                    cwd, started_at, last_at, message_count
             FROM sessions",
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
                    clauses.push("COALESCE(NULLIF(repo_project, ''), project) = ?".to_string())
                }
            }
            values.push(rusqlite::types::Value::Text(project.to_string()));
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
                cwd: row.get(5)?,
                started_at: row.get::<_, i64>(6)?.max(0) as u64,
                last_at: row.get::<_, i64>(7)?.max(0) as u64,
                message_count: row.get::<_, i64>(8)?.max(0) as u64,
            })
        })?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn query_projects(
        &self,
        source: Option<SourceFilter>,
        grouping: ProjectGrouping,
    ) -> Result<Vec<String>> {
        let project_expr = match grouping {
            ProjectGrouping::Flat => "project",
            ProjectGrouping::Repository => "COALESCE(NULLIF(repo_project, ''), project)",
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
        projects.sort();
        projects.dedup();
        Ok(projects)
    }

    pub fn query_source_timestamps(&self, since_ms: Option<u64>) -> Result<Vec<(SourceKind, u64)>> {
        let mut sql = String::from("SELECT source, last_at FROM sessions");
        let mut values: Vec<rusqlite::types::Value> = Vec::new();
        if let Some(since_ms) = since_ms {
            sql.push_str(" WHERE last_at >= ?");
            values.push(rusqlite::types::Value::Integer(since_ms as i64));
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
            ProjectGrouping::Repository => "COALESCE(NULLIF(repo_project, ''), project)",
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
            ProjectGrouping::Repository => "COALESCE(NULLIF(repo_project, ''), project)",
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
            ProjectGrouping::Repository => "COALESCE(NULLIF(repo_project, ''), project)",
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

    pub fn session_by_key(&self, session_key: &str) -> Result<Option<SessionRow>> {
        self.conn
            .query_row(
                r#"
                SELECT source, session_id, source_path, project, cwd,
                       started_at, last_at, message_count
                FROM sessions
                WHERE session_key = ?1
                "#,
                params![session_key],
                |row| {
                    let source_label: String = row.get(0)?;
                    let source = SourceKind::from_label(&source_label).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            format!("unknown source label: {source_label}").into(),
                        )
                    })?;
                    let project: String = row.get(3)?;
                    Ok(SessionRow {
                        source,
                        session_id: row.get(1)?,
                        source_path: row.get(2)?,
                        display_project: display_project_name(&project),
                        project,
                        cwd: row.get(4)?,
                        started_at: row.get::<_, i64>(5)?.max(0) as u64,
                        last_at: row.get::<_, i64>(6)?.max(0) as u64,
                        message_count: row.get::<_, i64>(7)?.max(0) as u64,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }
}

impl AnalyticsWriter {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_mode(path, false)
    }

    pub fn open_bulk(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_mode(path, true)
    }

    fn open_with_mode(path: impl AsRef<Path>, bulk_load: bool) -> Result<Self> {
        let store = AnalyticsStore::open(path)?;
        store.conn.set_prepared_statement_cache_capacity(64);
        store.conn.pragma_update(None, "wal_autocheckpoint", 0)?;
        if bulk_load {
            crate::catalog::begin_bulk_load(&store.conn)?;
        }
        Ok(Self {
            store,
            sessions: HashMap::new(),
            metadata_cache: HashMap::new(),
            git_cache: HashMap::new(),
            catalog_batch_open: false,
            catalog_batch_records: 0,
            affected_session_keys: HashSet::new(),
            bulk_load,
        })
    }

    pub fn clear(&self) -> Result<()> {
        self.store.clear()?;
        crate::catalog::clear(&self.store.conn)
    }

    pub fn affected_session_keys(&self) -> Vec<String> {
        self.affected_session_keys.iter().cloned().collect()
    }

    pub fn delete_source_path(&mut self, source_path: &str) -> Result<()> {
        self.begin_catalog_batch()?;
        {
            let mut statement = self.store.conn.prepare_cached(
                "SELECT DISTINCT session_key FROM records WHERE source_path = ?1",
            )?;
            let rows = statement.query_map(params![source_path], |row| row.get::<_, String>(0))?;
            for row in rows {
                self.affected_session_keys.insert(row?);
            }
        }
        self.store.delete_source_path(source_path)
    }

    pub fn record(&mut self, record: &Record) -> Result<()> {
        let mut canonical_record = record.clone();
        canonical_record.ensure_record_key();
        self.begin_catalog_batch()?;
        if self.bulk_load {
            crate::catalog::bulk_insert_record(&self.store.conn, &canonical_record)?;
        } else {
            crate::catalog::upsert_record(&self.store.conn, &canonical_record)?;
        }
        self.catalog_batch_records += 1;
        if self.catalog_batch_records >= CATALOG_BATCH_RECORDS {
            self.commit_catalog_batch()?;
        }

        let key = SessionKey {
            stable_key: crate::catalog::session_key(
                canonical_record.source,
                &canonical_record.session_id,
                &canonical_record.source_path,
            ),
            source: canonical_record.source,
            session_id: canonical_record.session_id.clone(),
            source_path: canonical_record.source_path.clone(),
        };
        self.affected_session_keys.insert(key.stable_key.clone());
        let entry = self
            .sessions
            .entry(key.clone())
            .or_insert_with(|| SessionAccumulator {
                key,
                project: canonical_record.project.clone(),
                started_at: canonical_record.ts,
                last_at: canonical_record.ts,
                message_count: 0,
            });
        if canonical_record.ts < entry.started_at {
            entry.started_at = canonical_record.ts;
        }
        if canonical_record.ts >= entry.last_at {
            entry.last_at = canonical_record.ts;
            if !canonical_record.project.is_empty() {
                entry.project = canonical_record.project.clone();
            }
        }
        if matches!(canonical_record.role.as_str(), "user" | "assistant") {
            entry.message_count = entry.message_count.saturating_add(1);
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.commit_catalog_batch()?;
        if self.bulk_load {
            // Bulk inserts leave the canonical tables in a large WAL. Checkpoint before
            // index construction and relationship joins so those read-heavy passes do
            // not pay a WAL-frame lookup for nearly every B-tree page.
            self.checkpoint_catalog()?;
            crate::catalog::finish_bulk_load(&self.store.conn)?;
        } else {
            self.link_catalog_relations()?;
        }
        if self.sessions.is_empty() {
            self.checkpoint_catalog()?;
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
                    session_key, source, session_id, source_path, project, cwd, git_root,
                    git_common_dir, repo_project, started_at, last_at, message_count,
                    resolution_status
                )
                VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                ON CONFLICT(session_key) DO UPDATE SET
                    source_path = excluded.source_path,
                    project = excluded.project,
                    cwd = excluded.cwd,
                    git_root = excluded.git_root,
                    git_common_dir = excluded.git_common_dir,
                    repo_project = excluded.repo_project,
                    started_at = MIN(sessions.started_at, excluded.started_at),
                    last_at = MAX(sessions.last_at, excluded.last_at),
                    message_count = sessions.message_count + excluded.message_count,
                    resolution_status = excluded.resolution_status
                "#,
            )?;
            for (session, metadata) in sessions {
                stmt.execute(params![
                    session.key.stable_key,
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
                ])?;
            }
        }
        tx.commit()?;
        self.sessions.clear();
        self.checkpoint_catalog()?;
        Ok(())
    }

    fn begin_catalog_batch(&mut self) -> Result<()> {
        if !self.catalog_batch_open {
            self.store.conn.execute_batch("BEGIN IMMEDIATE")?;
            self.catalog_batch_open = true;
        }
        Ok(())
    }

    fn commit_catalog_batch(&mut self) -> Result<()> {
        if self.catalog_batch_open {
            self.store.conn.execute_batch("COMMIT")?;
            self.catalog_batch_open = false;
            self.catalog_batch_records = 0;
        }
        Ok(())
    }

    fn checkpoint_catalog(&self) -> Result<()> {
        self.store
            .conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        Ok(())
    }

    fn link_catalog_relations(&mut self) -> Result<()> {
        if self.affected_session_keys.is_empty() {
            return Ok(());
        }
        let tx = self.store.conn.transaction()?;
        crate::catalog::link_relations_for_sessions(
            &tx,
            self.affected_session_keys.iter().map(String::as_str),
        )?;
        tx.commit()?;
        self.affected_session_keys.clear();
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
        let cwd = resolve_session_cwd_from_parts(key.source, &key.source_path, &key.session_id);
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
    let root = git_rev_parse(cwd, &["rev-parse", "--show-toplevel"]);
    let common_dir = git_rev_parse(
        cwd,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    );
    let path_repo_project = claude_worktree_repo_project(cwd);
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

fn git_rev_parse(cwd: &str, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .ok()?;
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
    if source == SourceKind::Copilot
        && let Some(cwd) = resolve_copilot_workspace_cwd(source_path)
    {
        return Some(cwd);
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

pub fn analytics_path(state_dir: &Path) -> PathBuf {
    state_dir.join("catalog.sqlite")
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
    let path = path.as_ref();
    let catalog_count = crate::catalog::CatalogStore::open_read_only(path)
        .and_then(|catalog| catalog.record_count())
        .context("canonical catalog is unavailable; run `memex index` to replay source logs")?;
    let index_count = index.doc_count()? as u64;
    if catalog_count != index_count {
        anyhow::bail!(
            "canonical catalog has {catalog_count} records but the search index has \
             {index_count}; run `memex index` to replay source logs"
        );
    }
    let mut records = Vec::with_capacity(index_count as usize);
    index
        .for_each_record(|record| {
            records.push(record);
            Ok(())
        })
        .context("hydrate records from canonical catalog")?;
    rebuild_from_records(path, records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RecordLinks;
    use std::fs;

    fn record(project: &str, session_id: &str, source_path: &Path, ts: u64) -> Record {
        Record {
            source: SourceKind::Codex,
            record_key: String::new(),
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
    fn read_only_store_rejects_writes() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = tmp.path().join("analytics.sqlite");
        drop(AnalyticsStore::open(&db).expect("initialize analytics"));

        let store = AnalyticsStore::open_read_only(&db).expect("open read only");

        assert!(store.mark_complete().is_err());
    }

    #[test]
    fn projection_manifest_exposes_only_incomplete_epochs() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = tmp.path().join("analytics.sqlite");
        let store = AnalyticsStore::open(&db).expect("open store");
        let path = "/logs/session.jsonl";

        let first_epoch = store
            .begin_projection(SourceKind::Codex, path, "fs:1:2", "fingerprint-a", 3, 10)
            .expect("begin projection");
        assert_eq!(first_epoch, 1);
        assert!(
            store
                .incomplete_source_paths()
                .expect("incomplete")
                .contains(path)
        );

        store
            .complete_projection(path, "session-1", 20, first_epoch, 4)
            .expect("complete projection");
        assert!(
            !store
                .incomplete_source_paths()
                .expect("incomplete")
                .contains(path)
        );

        let second_epoch = store
            .begin_projection(SourceKind::Codex, path, "session-1", "fingerprint-b", 3, 20)
            .expect("begin next projection");
        assert_eq!(second_epoch, 2);
        assert!(
            store
                .complete_projection(path, "session-1", 30, first_epoch, 5)
                .is_err()
        );
        assert!(
            store
                .incomplete_source_paths()
                .expect("incomplete")
                .contains(path)
        );
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
    fn repository_project_filter_uses_expression_index() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db = tmp.path().join("analytics.sqlite");
        let store = AnalyticsStore::open(&db).expect("open analytics");
        let plan: String = store
            .conn
            .query_row(
                "EXPLAIN QUERY PLAN SELECT source FROM sessions
                 WHERE COALESCE(NULLIF(repo_project, ''), project) = ?1
                 ORDER BY last_at DESC LIMIT 200",
                params!["memex"],
                |row| row.get(3),
            )
            .expect("query plan");

        assert!(
            plan.contains("sessions_display_project_last_at_idx"),
            "{plan}"
        );
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
    fn legacy_sessions_schema_is_migrated_before_writes() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let transcript = tmp.path().join("session.jsonl");
        fs::write(&transcript, "").expect("write transcript");
        let db = tmp.path().join("analytics.sqlite");
        {
            let conn = Connection::open(&db).expect("open sqlite");
            conn.execute_batch(
                r#"
                CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                INSERT INTO meta(key, value) VALUES('schema_version', '2');
                INSERT INTO meta(key, value) VALUES('analytics_complete', '1');
                CREATE TABLE sessions (
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
                    PRIMARY KEY (source, session_id, source_path)
                );
                "#,
            )
            .expect("seed legacy schema");
            conn.execute(
                "INSERT INTO sessions(
                    source, session_id, source_path, project, started_at, last_at, message_count
                 ) VALUES('codex', 's1', ?1, 'memex', 10, 10, 3)",
                params![transcript.to_string_lossy().as_ref()],
            )
            .expect("seed legacy session");
        }

        let mut writer = AnalyticsWriter::open(&db).expect("open analytics");
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
        assert_eq!(rows[0].last_at, 20);
        assert_eq!(rows[0].message_count, 4);
        assert!(!store.complete().expect("complete"));
    }

    #[test]
    fn repository_grouping_uses_git_common_dir_project() {
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
                "{{\"type\":\"session_meta\",\"payload\":{{\"cwd\":\"{}\"}}}}\n",
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
}
