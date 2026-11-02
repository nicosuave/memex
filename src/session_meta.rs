//! User-owned session assignments: project overrides and free-form tags.
//!
//! These live in their own SQLite file beside the analytics cache, never in it, so
//! `memex index rebuild`, schema migrations, and analytics backfills cannot drop them.
//! Rows are keyed by machine, provider, and session ID rather than transcript path:
//! continuation files that share a session ID share its assignment, and moved
//! transcripts keep theirs. Each machine stores the assignments for the sessions it
//! indexes, so its own project and tag filters see them; remote writes go to the
//! owning machine over RPC. `machine` is therefore always `local` today, and is part
//! of the key so assignments from different machines can never collide if stores are
//! ever combined.
use crate::machine::LOCAL_MACHINE_ID;
use crate::types::SourceKind;
use anyhow::{Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Schema name the analytics connection attaches this store under.
const ATTACHED_SCHEMA: &str = "user_meta";
const MAX_TAG_CHARS: usize = 64;
const MAX_PROJECT_CHARS: usize = 200;

const TABLES: &str = "
    CREATE TABLE IF NOT EXISTS {schema}.session_projects (
        machine TEXT NOT NULL,
        source TEXT NOT NULL,
        session_id TEXT NOT NULL,
        project TEXT NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (machine, source, session_id)
    );
    CREATE TABLE IF NOT EXISTS {schema}.session_tags (
        machine TEXT NOT NULL,
        source TEXT NOT NULL,
        session_id TEXT NOT NULL,
        tag TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (machine, source, session_id, tag)
    );
    CREATE INDEX IF NOT EXISTS {schema}.session_tags_tag_idx ON session_tags(tag);
";

pub fn session_meta_path(state_dir: &Path) -> PathBuf {
    state_dir.join("session-metadata.sqlite")
}

fn create_tables(conn: &Connection, schema: &str) -> Result<()> {
    conn.execute_batch(&TABLES.replace("{schema}", schema))?;
    Ok(())
}

/// Attach the assignment store to an analytics connection. Queries name its tables
/// unqualified, which SQLite resolves to the attached store. A read-only connection
/// whose store does not exist yet gets empty TEMP tables instead, so queries can
/// always reference them without creating the file.
pub(crate) fn attach(conn: &Connection, state_dir: &Path, read_only: bool) -> Result<()> {
    let path = session_meta_path(state_dir);
    if !read_only {
        conn.execute(
            &format!("ATTACH DATABASE ?1 AS {ATTACHED_SCHEMA}"),
            params![path.to_string_lossy()],
        )?;
        return create_tables(conn, ATTACHED_SCHEMA);
    }
    if path.exists() {
        conn.execute(
            &format!("ATTACH DATABASE ?1 AS {ATTACHED_SCHEMA}"),
            params![format!("file:{}?mode=ro", uri_path(&path))],
        )?;
        let tables: i64 = conn.query_row(
            &format!(
                "SELECT count(*) FROM {ATTACHED_SCHEMA}.sqlite_master
                 WHERE type = 'table' AND name IN ('session_projects', 'session_tags')"
            ),
            [],
            |row| row.get(0),
        )?;
        if tables == 2 {
            return Ok(());
        }
        conn.execute(&format!("DETACH DATABASE {ATTACHED_SCHEMA}"), [])?;
    }
    create_tables(conn, "temp")
}

fn uri_path(path: &Path) -> String {
    let mut out = String::new();
    for byte in path.to_string_lossy().bytes() {
        match byte {
            b'?' | b'#' | b'%' | b' ' => out.push_str(&format!("%{byte:02X}")),
            _ => out.push(byte as char),
        }
    }
    out
}

/// Lowercased, trimmed tag. Tags are case-insensitive labels without commas or
/// surrounding whitespace.
pub fn normalize_tag(tag: &str) -> Result<String> {
    let tag = tag.trim().trim_start_matches('#').trim().to_lowercase();
    if tag.is_empty() {
        bail!("tag cannot be empty");
    }
    if tag.contains(',') || tag.chars().any(char::is_control) {
        bail!("tag {tag:?} cannot contain commas or control characters");
    }
    if tag.chars().count() > MAX_TAG_CHARS {
        bail!("tag {tag:?} is longer than {MAX_TAG_CHARS} characters");
    }
    Ok(tag)
}

pub fn normalize_project(project: &str) -> Result<String> {
    let project = project.trim();
    if project.is_empty() {
        bail!("project cannot be empty (use `memex session clear-project` to remove an override)");
    }
    if project.chars().any(char::is_control) {
        bail!("project cannot contain control characters");
    }
    if project.chars().count() > MAX_PROJECT_CHARS {
        bail!("project is longer than {MAX_PROJECT_CHARS} characters");
    }
    Ok(project.to_string())
}

/// What a session itself carries, before parent inheritance.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnAssignment {
    pub project: Option<String>,
    pub tags: Vec<String>,
}

pub struct SessionMetaStore {
    conn: Connection,
}

impl SessionMetaStore {
    pub fn open(state_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(state_dir)?;
        let conn = Connection::open(session_meta_path(state_dir))?;
        conn.busy_timeout(Duration::from_secs(5))?;
        create_tables(&conn, "main")?;
        Ok(Self { conn })
    }

    pub fn set_project(&self, source: SourceKind, session_id: &str, project: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO session_projects(machine, source, session_id, project, updated_at)
             VALUES(?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(machine, source, session_id) DO UPDATE SET
                 project = excluded.project, updated_at = excluded.updated_at",
            params![
                LOCAL_MACHINE_ID,
                source.storage_label(),
                session_id,
                project,
                now_ms()
            ],
        )?;
        Ok(())
    }

    /// Returns whether an override existed.
    pub fn clear_project(&self, source: SourceKind, session_id: &str) -> Result<bool> {
        Ok(self.conn.execute(
            "DELETE FROM session_projects WHERE machine = ?1 AND source = ?2 AND session_id = ?3",
            params![LOCAL_MACHINE_ID, source.storage_label(), session_id],
        )? > 0)
    }

    /// Returns the tags that were not already present.
    pub fn add_tags(
        &self,
        source: SourceKind,
        session_id: &str,
        tags: &[String],
    ) -> Result<Vec<String>> {
        let now = now_ms();
        let mut added = Vec::new();
        for tag in tags {
            let inserted = self.conn.execute(
                "INSERT OR IGNORE INTO session_tags(machine, source, session_id, tag, created_at)
                 VALUES(?1, ?2, ?3, ?4, ?5)",
                params![
                    LOCAL_MACHINE_ID,
                    source.storage_label(),
                    session_id,
                    tag,
                    now
                ],
            )?;
            if inserted > 0 {
                added.push(tag.clone());
            }
        }
        Ok(added)
    }

    /// Returns the tags that were present and removed.
    pub fn remove_tags(
        &self,
        source: SourceKind,
        session_id: &str,
        tags: &[String],
    ) -> Result<Vec<String>> {
        let mut removed = Vec::new();
        for tag in tags {
            let deleted = self.conn.execute(
                "DELETE FROM session_tags
                 WHERE machine = ?1 AND source = ?2 AND session_id = ?3 AND tag = ?4",
                params![LOCAL_MACHINE_ID, source.storage_label(), session_id, tag],
            )?;
            if deleted > 0 {
                removed.push(tag.clone());
            }
        }
        Ok(removed)
    }

    pub fn own_assignment(&self, source: SourceKind, session_id: &str) -> Result<OwnAssignment> {
        let project = self
            .conn
            .query_row(
                "SELECT project FROM session_projects
                 WHERE machine = ?1 AND source = ?2 AND session_id = ?3",
                params![LOCAL_MACHINE_ID, source.storage_label(), session_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let mut stmt = self.conn.prepare(
            "SELECT tag FROM session_tags
             WHERE machine = ?1 AND source = ?2 AND session_id = ?3 ORDER BY tag",
        )?;
        let tags = stmt
            .query_map(
                params![LOCAL_MACHINE_ID, source.storage_label(), session_id],
                |row| row.get::<_, String>(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(OwnAssignment { project, tags })
    }
}

/// A change to, or a read of, one session's assignments.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum AssignmentAction {
    SetProject { project: String },
    ClearProject,
    AddTags { tags: Vec<String> },
    RemoveTags { tags: Vec<String> },
    Inspect,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssignmentRequest {
    pub session_id: String,
    /// Provider label; required only when several providers share the session ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(flatten)]
    pub action: AssignmentAction,
}

/// One indexed transcript of the session, with its derived (never user-edited) facts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DerivedTranscript {
    pub source_path: String,
    pub project: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_root: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_kind: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InheritedAssignment {
    pub session_id: String,
    #[serde(flatten)]
    pub assignment: OwnAssignment,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AssignmentReport {
    pub machine: String,
    pub source: SourceKind,
    pub session_id: String,
    /// Grouping and filtering use this project.
    pub project: String,
    /// Where `project` comes from: `session`, `parent`, or `derived`.
    pub project_origin: String,
    /// The repository grouping the session would have without any assignment.
    pub derived_project: String,
    /// Own and inherited tags.
    pub tags: Vec<String>,
    /// The session's own assignments.
    pub assigned: OwnAssignment,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<InheritedAssignment>,
    pub transcripts: Vec<DerivedTranscript>,
    /// Tags or project values this request changed; empty for inspect and no-ops.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed: Vec<String>,
}

/// Apply `request` to a session indexed on this machine and report its assignments.
/// Only indexed sessions can be assigned, so typos do not create orphaned rows.
pub fn apply_local(
    paths: &crate::config::Paths,
    request: &AssignmentRequest,
) -> Result<AssignmentReport> {
    let db = crate::analytics::analytics_path(&paths.state);
    if !db.exists() {
        bail!(
            "no analytics cache at {} (run `memex index` first)",
            db.display()
        );
    }
    let analytics = crate::analytics::AnalyticsStore::open_read_only(&db)?;
    let mut sources = analytics.session_sources(&request.session_id)?;
    if let Some(source) = &request.source {
        let wanted = SourceKind::from_label(source)
            .ok_or_else(|| anyhow::anyhow!("unknown source '{source}'"))?;
        sources.retain(|candidate| *candidate == wanted);
    }
    let source = match sources.as_slice() {
        [] => bail!(
            "no indexed session '{}'{}",
            request.session_id,
            request
                .source
                .as_ref()
                .map(|source| format!(" from {source}"))
                .unwrap_or_default()
        ),
        [source] => *source,
        several => bail!(
            "session '{}' exists in several sources ({}); pass --source",
            request.session_id,
            several
                .iter()
                .map(|source| source.label())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };
    drop(analytics);

    let store = SessionMetaStore::open(&paths.state)?;
    let changed = match &request.action {
        AssignmentAction::SetProject { project } => {
            let project = normalize_project(project)?;
            let previous = store.own_assignment(source, &request.session_id)?.project;
            store.set_project(source, &request.session_id, &project)?;
            if previous.as_deref() == Some(project.as_str()) {
                Vec::new()
            } else {
                vec![project]
            }
        }
        AssignmentAction::ClearProject => {
            let previous = store.own_assignment(source, &request.session_id)?.project;
            store.clear_project(source, &request.session_id)?;
            previous.into_iter().collect()
        }
        AssignmentAction::AddTags { tags } => {
            store.add_tags(source, &request.session_id, &normalize_tags(tags)?)?
        }
        AssignmentAction::RemoveTags { tags } => {
            store.remove_tags(source, &request.session_id, &normalize_tags(tags)?)?
        }
        AssignmentAction::Inspect => Vec::new(),
    };
    let mut report = inspect(paths, &store, source, &request.session_id)?;
    report.changed = changed;
    Ok(report)
}

fn normalize_tags(tags: &[String]) -> Result<Vec<String>> {
    if tags.is_empty() {
        bail!("provide at least one tag");
    }
    let mut out = tags
        .iter()
        .map(|tag| normalize_tag(tag))
        .collect::<Result<Vec<_>>>()?;
    out.sort();
    out.dedup();
    Ok(out)
}

fn inspect(
    paths: &crate::config::Paths,
    store: &SessionMetaStore,
    source: SourceKind,
    session_id: &str,
) -> Result<AssignmentReport> {
    let analytics = crate::analytics::AnalyticsStore::open_read_only(
        crate::analytics::analytics_path(&paths.state),
    )?;
    let rows = analytics.query_sessions_detailed_selected(
        None,
        None,
        None,
        None,
        Some(crate::analytics::SessionKindFilter::All),
        Some(session_id),
        None,
        None,
        None,
    )?;
    let rows = rows
        .into_iter()
        .filter(|row| row.source == source)
        .collect::<Vec<_>>();
    let latest = rows
        .first()
        .ok_or_else(|| anyhow::anyhow!("no indexed session '{session_id}'"))?;
    let assigned = store.own_assignment(source, session_id)?;
    let parent = match latest.parent_session_id.as_deref() {
        Some(parent) => Some(InheritedAssignment {
            session_id: parent.to_string(),
            assignment: store.own_assignment(source, parent)?,
        }),
        None => None,
    };
    let derived_project = latest
        .repo_project
        .clone()
        .unwrap_or_else(|| crate::analytics::UNFILED_PROJECT.to_string());
    let (project, project_origin) = if let Some(project) = &assigned.project {
        (project.clone(), "session")
    } else if let Some(project) = parent
        .as_ref()
        .and_then(|parent| parent.assignment.project.clone())
    {
        (project, "parent")
    } else {
        (derived_project.clone(), "derived")
    };
    Ok(AssignmentReport {
        machine: LOCAL_MACHINE_ID.to_string(),
        source,
        session_id: session_id.to_string(),
        project,
        project_origin: project_origin.to_string(),
        derived_project,
        tags: latest.tags.clone(),
        assigned,
        parent,
        transcripts: rows
            .iter()
            .map(|row| DerivedTranscript {
                source_path: row.source_path.clone(),
                project: row.project.clone(),
                repo_project: row.repo_project.clone(),
                cwd: row.cwd.clone(),
                git_root: row.git_root.clone(),
                conversation_kind: row.conversation_kind.clone(),
            })
            .collect(),
        changed: Vec::new(),
    })
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_normalize_case_whitespace_and_hash() {
        assert_eq!(normalize_tag("  #Tooling ").unwrap(), "tooling");
        assert!(normalize_tag(" ").is_err());
        assert!(normalize_tag("a,b").is_err());
        assert!(normalize_project("  ").is_err());
        assert_eq!(normalize_project(" ProjectA ").unwrap(), "ProjectA");
    }

    #[test]
    fn assignments_round_trip_and_report_changes() {
        let dir = tempfile::tempdir().unwrap();
        let store = SessionMetaStore::open(dir.path()).unwrap();
        store
            .set_project(SourceKind::Claude, "s1", "ProjectA")
            .unwrap();
        let tags = vec!["tooling".to_string(), "infra".to_string()];
        assert_eq!(
            store.add_tags(SourceKind::Claude, "s1", &tags).unwrap(),
            tags
        );
        assert!(
            store
                .add_tags(SourceKind::Claude, "s1", &tags)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store.own_assignment(SourceKind::Claude, "s1").unwrap(),
            OwnAssignment {
                project: Some("ProjectA".into()),
                tags: vec!["infra".into(), "tooling".into()],
            }
        );
        // Another provider with the same session ID is a different session.
        assert_eq!(
            store.own_assignment(SourceKind::Codex, "s1").unwrap(),
            OwnAssignment::default()
        );
        assert!(store.clear_project(SourceKind::Claude, "s1").unwrap());
        assert!(!store.clear_project(SourceKind::Claude, "s1").unwrap());
        assert_eq!(
            store
                .remove_tags(SourceKind::Claude, "s1", &["infra".into(), "nope".into()])
                .unwrap(),
            vec!["infra".to_string()]
        );
    }
}
