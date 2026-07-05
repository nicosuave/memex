use crate::types::{Record, SourceFilter, SourceKind};
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

const SCHEMA_VERSION: i64 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectGrouping {
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
}

pub struct AnalyticsStore {
    conn: Connection,
}

pub struct AnalyticsWriter {
    store: AnalyticsStore,
    sessions: HashMap<SessionKey, SessionAccumulator>,
    metadata_cache: HashMap<SessionKey, SessionMetadata>,
    git_cache: HashMap<String, GitMetadata>,
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
        let store = Self { conn };
        store.init()?;
        Ok(store)
    }

    fn init(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
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
                PRIMARY KEY (source, session_id, source_path)
            );
            CREATE INDEX IF NOT EXISTS sessions_last_at_idx ON sessions(last_at);
            CREATE INDEX IF NOT EXISTS sessions_project_last_at_idx ON sessions(project, last_at);
            CREATE INDEX IF NOT EXISTS sessions_repo_project_last_at_idx ON sessions(repo_project, last_at);
            CREATE INDEX IF NOT EXISTS sessions_source_last_at_idx ON sessions(source, last_at);
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

    pub fn session_count(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))?;
        Ok(count.max(0) as u64)
    }

    pub fn is_ready(path: impl AsRef<Path>) -> bool {
        Self::open(path)
            .and_then(|store| store.session_count())
            .map(|count| count > 0)
            .unwrap_or(false)
    }

    pub fn is_complete(path: impl AsRef<Path>) -> bool {
        Self::open(path)
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
                    cwd, last_at, message_count
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
                last_at: row.get::<_, i64>(6)?.max(0) as u64,
                message_count: row.get::<_, i64>(7)?.max(0) as u64,
            })
        })?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
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
        self.conn
            .query_row(
                &format!(
                    "SELECT {display_expr} FROM sessions
                     WHERE source = ?1 AND session_id = ?2 AND source_path = ?3"
                ),
                params![source.storage_label(), session_id, source_path],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }
}

impl AnalyticsWriter {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            store: AnalyticsStore::open(path)?,
            sessions: HashMap::new(),
            metadata_cache: HashMap::new(),
            git_cache: HashMap::new(),
        })
    }

    pub fn clear(&self) -> Result<()> {
        self.store.clear()
    }

    pub fn delete_source_path(&self, source_path: &str) -> Result<()> {
        self.store.delete_source_path(source_path)
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
                    repo_project, started_at, last_at, message_count, resolution_status
                )
                VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                ON CONFLICT(source, session_id, source_path) DO UPDATE SET
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

        if source == SourceKind::CodexSession
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

        if source == SourceKind::Pi && value.get("type").and_then(|v| v.as_str()) == Some("session")
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
    use crate::types::RecordLinks;
    use std::fs;

    fn record(project: &str, session_id: &str, source_path: &Path, ts: u64) -> Record {
        Record {
            source: SourceKind::CodexSession,
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
