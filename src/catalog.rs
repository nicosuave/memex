use crate::types::{Record, RecordLinks, SourceKind};
use anyhow::{Context, Result, anyhow};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::io::Cursor;
use std::path::Path;
use std::time::Duration;

const CATALOG_SCHEMA_VERSION: i64 = 10;
const INLINE_BODY_BYTES: usize = 4 * 1024;

pub struct CatalogStore {
    conn: Connection,
}

#[derive(Debug)]
struct StoredBody {
    inline: Option<String>,
    blob_hash: Option<String>,
}

impl CatalogStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(2))?;
        conn.set_prepared_statement_cache_capacity(64);
        init_connection(&conn)?;
        Ok(Self { conn })
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

    pub fn record_by_key(&self, record_key: &str) -> Result<Option<Record>> {
        record_by_key(&self.conn, record_key)
    }

    pub fn record_by_doc_id(&self, doc_id: u64) -> Result<Option<Record>> {
        let record_key: Option<String> = self
            .conn
            .query_row(
                "SELECT record_key FROM records WHERE doc_id = ?1",
                params![to_sql_u64(doc_id)],
                |row| row.get(0),
            )
            .optional()?;
        record_key
            .as_deref()
            .map(|key| self.record_by_key(key))
            .transpose()
            .map(Option::flatten)
    }

    pub fn latest_record_for_session(&self, session_key: &str) -> Result<Option<Record>> {
        let record_key: Option<String> = self
            .conn
            .query_row(
                "SELECT record_key
                 FROM records
                 WHERE session_key = ?1
                 ORDER BY occurred_at DESC, source_order DESC, record_id DESC
                 LIMIT 1",
                params![session_key],
                |row| row.get(0),
            )
            .optional()?;
        record_key
            .as_deref()
            .map(|key| self.record_by_key(key))
            .transpose()
            .map(Option::flatten)
    }

    pub fn record_count(&self) -> Result<u64> {
        let count: i64 = self
            .conn
            .query_row("SELECT count(*) FROM records", [], |row| row.get(0))?;
        Ok(count.max(0) as u64)
    }

    pub fn embedding_anchor_by_vector_id(&self, vector_id: u64) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT anchor_record_key FROM embedding_documents WHERE vector_id = ?1",
                params![to_sql_u64(vector_id)],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn embedding_vector_ids_for_source_paths(
        &self,
        source_paths: &[String],
    ) -> Result<Vec<u64>> {
        if source_paths.is_empty() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        let mut statement = self.conn.prepare(
            "SELECT DISTINCT ed.vector_id
             FROM embedding_documents ed
             JOIN records r ON r.record_key = ed.anchor_record_key
             WHERE r.source_path = ?1",
        )?;
        for source_path in source_paths {
            let rows = statement.query_map(params![source_path], |row| row.get::<_, i64>(0))?;
            for row in rows {
                ids.push(from_sql_u64(row?));
            }
        }
        Ok(ids)
    }

    pub fn embedding_vector_ids(&self) -> Result<Vec<u64>> {
        let mut statement = self
            .conn
            .prepare("SELECT vector_id FROM embedding_documents")?;
        let rows = statement.query_map([], |row| row.get::<_, i64>(0))?;
        rows.map(|row| row.map(from_sql_u64).map_err(Into::into))
            .collect()
    }

    pub fn has_embeddable_records(&self) -> Result<bool> {
        self.conn
            .query_row(
                r#"
                SELECT EXISTS(
                    SELECT 1
                    FROM records AS record
                    JOIN messages AS message ON message.record_id = record.record_id
                    WHERE record.role IN ('user', 'assistant')
                      AND (
                          message.content_blob_hash IS NOT NULL
                          OR length(trim(COALESCE(message.content_inline, ''))) > 0
                      )
                )
                "#,
                [],
                |row| row.get(0),
            )
            .map_err(Into::into)
    }

    pub fn embedding_vector_ids_for_session_keys(
        &self,
        session_keys: &[String],
    ) -> Result<Vec<u64>> {
        let mut ids = Vec::new();
        let mut statement = self
            .conn
            .prepare("SELECT vector_id FROM embedding_documents WHERE session_key = ?1")?;
        for session_key in session_keys {
            let rows = statement.query_map(params![session_key], |row| row.get::<_, i64>(0))?;
            for row in rows {
                ids.push(from_sql_u64(row?));
            }
        }
        Ok(ids)
    }

    pub fn records_for_session_keys(&self, session_keys: &[String]) -> Result<Vec<Record>> {
        let mut record_keys = Vec::new();
        let mut statement = self.conn.prepare(
            "SELECT record_key
             FROM records
             WHERE session_key = ?1
             ORDER BY occurred_at, source_order, record_id",
        )?;
        for session_key in session_keys {
            let rows = statement.query_map(params![session_key], |row| row.get::<_, String>(0))?;
            for row in rows {
                record_keys.push(row?);
            }
        }
        record_keys
            .iter()
            .filter_map(|key| self.record_by_key(key).transpose())
            .collect()
    }

    pub fn replace_embedding_documents(
        &mut self,
        documents: &[crate::embedding_documents::EmbeddingDocument],
    ) -> Result<()> {
        let transaction = self.conn.transaction()?;
        transaction.execute("DELETE FROM embedding_documents", [])?;
        {
            let mut insert = transaction.prepare(
                r#"
                INSERT INTO embedding_documents(
                    embedding_key, vector_id, session_key, role, anchor_record_key,
                    start_record_key, end_record_key, chunk_index, start_offset,
                    end_offset, content_hash
                )
                VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                "#,
            )?;
            for document in documents {
                insert.execute(params![
                    document.embedding_key,
                    to_sql_u64(document.vector_id),
                    document.session_key,
                    document.role,
                    document.anchor_record_key,
                    document.start_record_key,
                    document.end_record_key,
                    document.chunk_index as i64,
                    document.start_offset.min(i64::MAX as usize) as i64,
                    document.end_offset.min(i64::MAX as usize) as i64,
                    document.content_hash,
                ])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn replace_embedding_documents_for_sessions(
        &mut self,
        session_keys: &[String],
        documents: &[crate::embedding_documents::EmbeddingDocument],
    ) -> Result<()> {
        let transaction = self.conn.transaction()?;
        {
            let mut delete =
                transaction.prepare("DELETE FROM embedding_documents WHERE session_key = ?1")?;
            for session_key in session_keys {
                delete.execute(params![session_key])?;
            }
        }
        insert_embedding_documents(&transaction, documents)?;
        transaction.commit()?;
        Ok(())
    }
}

fn insert_embedding_documents(
    conn: &Connection,
    documents: &[crate::embedding_documents::EmbeddingDocument],
) -> Result<()> {
    let mut insert = conn.prepare(
        r#"
        INSERT INTO embedding_documents(
            embedding_key, vector_id, session_key, role, anchor_record_key,
            start_record_key, end_record_key, chunk_index, start_offset,
            end_offset, content_hash
        )
        VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
        "#,
    )?;
    for document in documents {
        insert.execute(params![
            document.embedding_key,
            to_sql_u64(document.vector_id),
            document.session_key,
            document.role,
            document.anchor_record_key,
            document.start_record_key,
            document.end_record_key,
            document.chunk_index as i64,
            document.start_offset.min(i64::MAX as usize) as i64,
            document.end_offset.min(i64::MAX as usize) as i64,
            document.content_hash,
        ])?;
    }
    Ok(())
}

pub(crate) fn init_connection(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "foreign_keys", true)?;
    reset_incompatible_catalog(conn)?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS catalog_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS content_blobs (
            content_hash TEXT PRIMARY KEY,
            codec TEXT NOT NULL,
            original_bytes INTEGER NOT NULL,
            payload BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS interactions (
            interaction_id INTEGER PRIMARY KEY,
            session_key TEXT NOT NULL,
            source_interaction_id TEXT NOT NULL,
            started_at INTEGER NOT NULL,
            ended_at INTEGER NOT NULL,
            UNIQUE(session_key, source_interaction_id)
        );
        CREATE TABLE IF NOT EXISTS records (
            record_id INTEGER PRIMARY KEY,
            record_key TEXT NOT NULL UNIQUE,
            content_hash TEXT NOT NULL,
            doc_id INTEGER NOT NULL UNIQUE,
            entity_type TEXT NOT NULL,
            source TEXT NOT NULL,
            source_path TEXT NOT NULL,
            session_key TEXT NOT NULL,
            session_id TEXT NOT NULL,
            occurred_at INTEGER NOT NULL,
            source_order INTEGER NOT NULL,
            message_ordinal INTEGER,
            role TEXT NOT NULL,
            tool_name TEXT,
            event_id TEXT,
            parent_event_id TEXT,
            logical_parent_event_id TEXT,
            parent_session_id TEXT,
            thread_source TEXT,
            conversation_kind TEXT,
            interaction_id INTEGER REFERENCES interactions(interaction_id) ON DELETE SET NULL,
            parent_tool_use_id TEXT,
            source_tool_use_id TEXT,
            source_tool_assistant_uuid TEXT
        );
        CREATE INDEX IF NOT EXISTS records_session_time_idx
            ON records(session_key, occurred_at, source_order);
        CREATE INDEX IF NOT EXISTS records_source_path_idx ON records(source_path);
        CREATE INDEX IF NOT EXISTS records_event_idx ON records(session_key, event_id);
        CREATE INDEX IF NOT EXISTS records_interaction_idx ON records(interaction_id);

        CREATE TABLE IF NOT EXISTS messages (
            record_id INTEGER PRIMARY KEY REFERENCES records(record_id) ON DELETE CASCADE,
            message_identity TEXT,
            session_key TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            model TEXT,
            input_tokens INTEGER,
            cache_read_tokens INTEGER,
            cache_write_tokens INTEGER,
            output_tokens INTEGER,
            reasoning_tokens INTEGER,
            content_inline TEXT,
            content_blob_hash TEXT REFERENCES content_blobs(content_hash),
            UNIQUE(session_key, ordinal)
        );
        CREATE INDEX IF NOT EXISTS messages_identity_idx
            ON messages(message_identity) WHERE message_identity IS NOT NULL;

        CREATE TABLE IF NOT EXISTS thinking (
            record_id INTEGER PRIMARY KEY REFERENCES records(record_id) ON DELETE CASCADE,
            message_identity TEXT,
            message_record_id INTEGER REFERENCES records(record_id) ON DELETE SET NULL,
            owner_link_status TEXT NOT NULL,
            content_inline TEXT,
            content_blob_hash TEXT REFERENCES content_blobs(content_hash)
        );
        CREATE INDEX IF NOT EXISTS thinking_message_idx ON thinking(message_record_id);
        CREATE INDEX IF NOT EXISTS thinking_unlinked_identity_idx
            ON thinking(message_identity)
            WHERE message_record_id IS NULL AND message_identity IS NOT NULL;

        CREATE TABLE IF NOT EXISTS tool_calls (
            record_id INTEGER PRIMARY KEY REFERENCES records(record_id) ON DELETE CASCADE,
            message_identity TEXT,
            message_record_id INTEGER REFERENCES records(record_id) ON DELETE SET NULL,
            owner_link_status TEXT NOT NULL,
            call_index INTEGER,
            source_tool_use_id TEXT,
            category TEXT,
            status TEXT,
            source_status TEXT,
            skill_name TEXT,
            file_path TEXT,
            subagent_session_id TEXT,
            input_inline TEXT,
            input_blob_hash TEXT REFERENCES content_blobs(content_hash),
            search_text_inline TEXT,
            search_text_blob_hash TEXT REFERENCES content_blobs(content_hash)
        );
        CREATE INDEX IF NOT EXISTS tool_calls_message_idx
            ON tool_calls(message_record_id, call_index);
        CREATE INDEX IF NOT EXISTS tool_calls_unlinked_identity_idx
            ON tool_calls(message_identity)
            WHERE message_record_id IS NULL AND message_identity IS NOT NULL;
        CREATE INDEX IF NOT EXISTS tool_calls_source_id_idx
            ON tool_calls(source_tool_use_id);

        CREATE TABLE IF NOT EXISTS tool_results (
            record_id INTEGER PRIMARY KEY REFERENCES records(record_id) ON DELETE CASCADE,
            tool_call_record_id INTEGER REFERENCES records(record_id) ON DELETE SET NULL,
            source_tool_use_id TEXT,
            call_link_status TEXT NOT NULL,
            event_index INTEGER,
            status TEXT,
            source_status TEXT,
            subagent_session_id TEXT,
            output_inline TEXT,
            output_blob_hash TEXT REFERENCES content_blobs(content_hash),
            search_text_inline TEXT,
            search_text_blob_hash TEXT REFERENCES content_blobs(content_hash)
        );
        CREATE INDEX IF NOT EXISTS tool_results_call_idx
            ON tool_results(tool_call_record_id, event_index);
        CREATE INDEX IF NOT EXISTS tool_results_unlinked_source_id_idx
            ON tool_results(source_tool_use_id) WHERE tool_call_record_id IS NULL;

        CREATE TABLE IF NOT EXISTS embedding_documents (
            embedding_key TEXT PRIMARY KEY,
            vector_id INTEGER NOT NULL UNIQUE,
            session_key TEXT NOT NULL,
            role TEXT NOT NULL,
            anchor_record_key TEXT NOT NULL REFERENCES records(record_key) ON DELETE CASCADE,
            start_record_key TEXT NOT NULL,
            end_record_key TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            start_offset INTEGER NOT NULL,
            end_offset INTEGER NOT NULL,
            content_hash TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS embedding_documents_session_idx
            ON embedding_documents(session_key, role, chunk_index);
        CREATE INDEX IF NOT EXISTS embedding_documents_anchor_idx
            ON embedding_documents(anchor_record_key);
        "#,
    )?;
    ensure_column(conn, "tool_calls", "search_text_inline", "TEXT")?;
    ensure_column(
        conn,
        "tool_calls",
        "search_text_blob_hash",
        "TEXT REFERENCES content_blobs(content_hash)",
    )?;
    ensure_column(conn, "tool_results", "search_text_inline", "TEXT")?;
    ensure_column(
        conn,
        "tool_results",
        "search_text_blob_hash",
        "TEXT REFERENCES content_blobs(content_hash)",
    )?;
    conn.execute(
        "INSERT INTO catalog_meta(key, value) VALUES('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![CATALOG_SCHEMA_VERSION.to_string()],
    )?;
    Ok(())
}

fn reset_incompatible_catalog(conn: &Connection) -> Result<()> {
    let has_catalog_meta: bool = conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'catalog_meta'
        )",
        [],
        |row| row.get(0),
    )?;
    if !has_catalog_meta {
        return Ok(());
    }
    let schema_version = conn
        .query_row(
            "SELECT value FROM catalog_meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .and_then(|value| value.parse::<i64>().ok());
    if schema_version == Some(CATALOG_SCHEMA_VERSION) {
        return Ok(());
    }
    if conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'meta'
        )",
        [],
        |row| row.get::<_, bool>(0),
    )? {
        conn.execute("DELETE FROM meta WHERE key = 'analytics_complete'", [])?;
    }
    conn.execute_batch(
        r#"
        DROP TABLE IF EXISTS embedding_documents;
        DROP TABLE IF EXISTS tool_results;
        DROP TABLE IF EXISTS tool_calls;
        DROP TABLE IF EXISTS thinking;
        DROP TABLE IF EXISTS messages;
        DROP TABLE IF EXISTS records;
        DROP TABLE IF EXISTS content_blobs;
        DROP TABLE IF EXISTS catalog_meta;
        "#,
    )?;
    Ok(())
}

pub(crate) fn begin_bulk_load(conn: &Connection) -> Result<()> {
    let threads = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(64);
    conn.pragma_update(None, "threads", threads)?;
    conn.pragma_update(None, "wal_autocheckpoint", 0)?;
    conn.execute_batch(
        r#"
        DROP INDEX IF EXISTS records_session_time_idx;
        DROP INDEX IF EXISTS records_source_path_idx;
        DROP INDEX IF EXISTS records_event_idx;
        DROP INDEX IF EXISTS records_interaction_idx;
        DROP INDEX IF EXISTS messages_identity_idx;
        DROP INDEX IF EXISTS thinking_message_idx;
        DROP INDEX IF EXISTS thinking_unlinked_identity_idx;
        DROP INDEX IF EXISTS tool_calls_message_idx;
        DROP INDEX IF EXISTS tool_calls_unlinked_identity_idx;
        DROP INDEX IF EXISTS tool_calls_source_id_idx;
        DROP INDEX IF EXISTS tool_results_call_idx;
        DROP INDEX IF EXISTS tool_results_unlinked_source_id_idx;
        DROP INDEX IF EXISTS embedding_documents_session_idx;
        DROP INDEX IF EXISTS embedding_documents_anchor_idx;
        DROP TABLE IF EXISTS temp.pending_record_interactions;
        CREATE TEMP TABLE pending_record_interactions (
            record_id INTEGER PRIMARY KEY,
            session_key TEXT NOT NULL,
            source_interaction_id TEXT NOT NULL,
            occurred_at INTEGER NOT NULL
        );
        "#,
    )?;
    Ok(())
}

pub(crate) fn finish_bulk_load(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE INDEX IF NOT EXISTS records_session_time_idx
            ON records(session_key, occurred_at, source_order);
        CREATE INDEX IF NOT EXISTS records_source_path_idx ON records(source_path);
        CREATE INDEX IF NOT EXISTS records_event_idx ON records(session_key, event_id);
        CREATE INDEX IF NOT EXISTS records_interaction_idx ON records(interaction_id);
        CREATE INDEX IF NOT EXISTS messages_identity_idx
            ON messages(message_identity) WHERE message_identity IS NOT NULL;
        CREATE INDEX IF NOT EXISTS thinking_message_idx ON thinking(message_record_id);
        CREATE INDEX IF NOT EXISTS thinking_unlinked_identity_idx
            ON thinking(message_identity)
            WHERE message_record_id IS NULL AND message_identity IS NOT NULL;
        CREATE INDEX IF NOT EXISTS tool_calls_message_idx
            ON tool_calls(message_record_id, call_index);
        CREATE INDEX IF NOT EXISTS tool_calls_unlinked_identity_idx
            ON tool_calls(message_identity)
            WHERE message_record_id IS NULL AND message_identity IS NOT NULL;
        CREATE INDEX IF NOT EXISTS tool_calls_source_id_idx
            ON tool_calls(source_tool_use_id);
        CREATE INDEX IF NOT EXISTS tool_results_call_idx
            ON tool_results(tool_call_record_id, event_index);
        CREATE INDEX IF NOT EXISTS tool_results_unlinked_source_id_idx
            ON tool_results(source_tool_use_id) WHERE tool_call_record_id IS NULL;
        CREATE INDEX IF NOT EXISTS embedding_documents_session_idx
            ON embedding_documents(session_key, role, chunk_index);
        CREATE INDEX IF NOT EXISTS embedding_documents_anchor_idx
            ON embedding_documents(anchor_record_key);
        "#,
    )?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    link_interactions(conn)?;
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    link_relations(conn)?;
    Ok(())
}

fn ensure_column(conn: &Connection, table: &str, column: &str, definition: &str) -> Result<()> {
    if table_has_column(conn, table, column)? {
        return Ok(());
    }
    conn.execute(
        &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
        [],
    )?;
    Ok(())
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut statement = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    for existing in columns {
        if existing? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = ?1
        )",
        params![table],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

pub(crate) fn upsert_record(conn: &Connection, record: &Record) -> Result<()> {
    upsert_record_with_relations(conn, record, true)
}

pub(crate) fn bulk_insert_record(conn: &Connection, record: &Record) -> Result<()> {
    upsert_record_with_relations(conn, record, false)
}

fn upsert_record_with_relations(
    conn: &Connection,
    record: &Record,
    resolve_relations: bool,
) -> Result<()> {
    if record.record_key.is_empty() {
        return Err(anyhow!(
            "record_key must be assigned before catalog insertion"
        ));
    }
    let session_key = stable_session_key(record);
    let entity_type = entity_type(record);
    let interaction_id = if resolve_relations {
        record
            .links
            .interaction_id
            .as_deref()
            .map(|source_id| ensure_interaction(conn, &session_key, source_id, record.ts))
            .transpose()?
    } else {
        None
    };
    let record_id: i64 = conn
        .prepare_cached(
            r#"
        INSERT INTO records(
            record_key, content_hash, doc_id, entity_type, source, source_path,
            session_key, session_id,
            occurred_at, source_order, message_ordinal, role, tool_name,
            event_id, parent_event_id,
            logical_parent_event_id, parent_session_id, thread_source, conversation_kind,
            interaction_id, parent_tool_use_id, source_tool_use_id,
            source_tool_assistant_uuid
        )
        VALUES(
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
            ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23
        )
        ON CONFLICT(record_key) DO UPDATE SET
            content_hash = excluded.content_hash,
            doc_id = excluded.doc_id,
            entity_type = excluded.entity_type,
            source = excluded.source,
            source_path = excluded.source_path,
            session_key = excluded.session_key,
            session_id = excluded.session_id,
            occurred_at = excluded.occurred_at,
            source_order = excluded.source_order,
            message_ordinal = excluded.message_ordinal,
            role = excluded.role,
            tool_name = excluded.tool_name,
            event_id = excluded.event_id,
            parent_event_id = excluded.parent_event_id,
            logical_parent_event_id = excluded.logical_parent_event_id,
            parent_session_id = excluded.parent_session_id,
            thread_source = excluded.thread_source,
            conversation_kind = excluded.conversation_kind,
            interaction_id = excluded.interaction_id,
            parent_tool_use_id = excluded.parent_tool_use_id,
            source_tool_use_id = excluded.source_tool_use_id,
            source_tool_assistant_uuid = excluded.source_tool_assistant_uuid
        RETURNING record_id
        "#,
        )?
        .query_row(
            params![
                record.record_key,
                record.computed_content_hash(),
                to_sql_u64(record.doc_id),
                entity_type,
                record.source.storage_label(),
                record.source_path,
                session_key,
                record.session_id,
                to_sql_u64(record.ts),
                record.turn_id as i64,
                record.links.message_ordinal.map(i64::from),
                record.role,
                record.tool_name,
                record.links.event_id,
                record.links.parent_event_id,
                record.links.logical_parent_event_id,
                record.links.parent_session_id,
                record.links.thread_source,
                record.links.conversation_kind,
                interaction_id,
                record.links.parent_tool_use_id,
                record.links.source_tool_use_id,
                record.links.source_tool_assistant_uuid,
            ],
            |row| row.get(0),
        )?;

    if !resolve_relations
        && let Some(source_interaction_id) = record.links.interaction_id.as_deref()
    {
        conn.prepare_cached(
            r#"
            INSERT INTO temp.pending_record_interactions(
                record_id, session_key, source_interaction_id, occurred_at
            )
            VALUES(?1, ?2, ?3, ?4)
            ON CONFLICT(record_id) DO UPDATE SET
                session_key = excluded.session_key,
                source_interaction_id = excluded.source_interaction_id,
                occurred_at = excluded.occurred_at
            "#,
        )?
        .execute(params![
            record_id,
            session_key,
            source_interaction_id,
            to_sql_u64(record.ts)
        ])?;
    }

    match entity_type {
        "message" => upsert_message(conn, record_id, record, &session_key)?,
        "thinking" => upsert_thinking(conn, record_id, record, &session_key)?,
        "tool_call" => upsert_tool_call(conn, record_id, record, &session_key, resolve_relations)?,
        "tool_result" => {
            upsert_tool_result(conn, record_id, record, &session_key, resolve_relations)?
        }
        _ => upsert_message(conn, record_id, record, &session_key)?,
    }
    Ok(())
}

pub(crate) fn delete_source_path(conn: &Connection, source_path: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM records WHERE source_path = ?1",
        params![source_path],
    )?;
    conn.execute(
        "DELETE FROM content_blobs
         WHERE content_hash NOT IN (
             SELECT content_blob_hash FROM messages WHERE content_blob_hash IS NOT NULL
             UNION SELECT content_blob_hash FROM thinking WHERE content_blob_hash IS NOT NULL
             UNION SELECT input_blob_hash FROM tool_calls WHERE input_blob_hash IS NOT NULL
             UNION SELECT output_blob_hash FROM tool_results WHERE output_blob_hash IS NOT NULL
             UNION SELECT search_text_blob_hash FROM tool_calls
                 WHERE search_text_blob_hash IS NOT NULL
             UNION SELECT search_text_blob_hash FROM tool_results
                 WHERE search_text_blob_hash IS NOT NULL
         )",
        [],
    )?;
    prune_orphan_interactions(conn)?;
    Ok(())
}

pub(crate) fn prune_orphan_interactions(conn: &Connection) -> Result<()> {
    if table_exists(conn, "usage_events")? {
        conn.execute(
            "DELETE FROM interactions
             WHERE NOT EXISTS (
                 SELECT 1 FROM records
                 WHERE records.interaction_id = interactions.interaction_id
             )
             AND NOT EXISTS (
                 SELECT 1 FROM usage_events
                 WHERE usage_events.interaction_id = interactions.interaction_id
             )",
            [],
        )?;
    } else {
        conn.execute(
            "DELETE FROM interactions
             WHERE NOT EXISTS (
                 SELECT 1 FROM records
                 WHERE records.interaction_id = interactions.interaction_id
             )",
            [],
        )?;
    }
    Ok(())
}

pub(crate) fn clear(conn: &Connection) -> Result<()> {
    conn.execute("DELETE FROM records", [])?;
    conn.execute("DELETE FROM interactions", [])?;
    conn.execute("DELETE FROM content_blobs", [])?;
    Ok(())
}

fn upsert_message(
    conn: &Connection,
    record_id: i64,
    record: &Record,
    session_key: &str,
) -> Result<()> {
    let identity = message_identity(record, session_key);
    let ordinal = record.links.message_ordinal.unwrap_or(record.turn_id) as i64;
    let body = store_body(conn, &record.text)?;
    conn.prepare_cached(
        r#"
        INSERT INTO messages(
            record_id, message_identity, session_key, ordinal, model, input_tokens,
            cache_read_tokens, cache_write_tokens, output_tokens, reasoning_tokens,
            content_inline, content_blob_hash
        )
        VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        ON CONFLICT(record_id) DO UPDATE SET
            message_identity = excluded.message_identity,
            session_key = excluded.session_key,
            ordinal = excluded.ordinal,
            model = excluded.model,
            input_tokens = excluded.input_tokens,
            cache_read_tokens = excluded.cache_read_tokens,
            cache_write_tokens = excluded.cache_write_tokens,
            output_tokens = excluded.output_tokens,
            reasoning_tokens = excluded.reasoning_tokens,
            content_inline = excluded.content_inline,
            content_blob_hash = excluded.content_blob_hash
        "#,
    )?
    .execute(params![
        record_id,
        identity,
        session_key,
        ordinal,
        record.links.model,
        record.links.input_tokens.map(to_sql_u64),
        record.links.cache_read_tokens.map(to_sql_u64),
        record.links.cache_write_tokens.map(to_sql_u64),
        record.links.output_tokens.map(to_sql_u64),
        record.links.reasoning_tokens.map(to_sql_u64),
        body.inline,
        body.blob_hash
    ])?;
    Ok(())
}

fn upsert_thinking(
    conn: &Connection,
    record_id: i64,
    record: &Record,
    session_key: &str,
) -> Result<()> {
    let identity = owner_message_identity(record, session_key);
    let owner_status = pending_owner_status(identity.as_deref(), record.links.message_ordinal);
    let body = store_body(conn, &record.text)?;
    conn.prepare_cached(
        r#"
        INSERT INTO thinking(
            record_id, message_identity, message_record_id, owner_link_status,
            content_inline, content_blob_hash
        )
        VALUES(?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(record_id) DO UPDATE SET
            message_identity = excluded.message_identity,
            message_record_id = excluded.message_record_id,
            owner_link_status = excluded.owner_link_status,
            content_inline = excluded.content_inline,
            content_blob_hash = excluded.content_blob_hash
        "#,
    )?
    .execute(params![
        record_id,
        identity,
        Option::<i64>::None,
        owner_status,
        body.inline,
        body.blob_hash
    ])?;
    Ok(())
}

fn upsert_tool_call(
    conn: &Connection,
    record_id: i64,
    record: &Record,
    session_key: &str,
    resolve_relations: bool,
) -> Result<()> {
    let identity = owner_message_identity(record, session_key);
    let owner_status = pending_owner_status(identity.as_deref(), record.links.message_ordinal);
    let source_tool_use_id = record
        .links
        .source_tool_use_id
        .as_deref()
        .or(record.links.event_id.as_deref());
    let input = record.tool_input.as_deref().unwrap_or(&record.text);
    let category = normalized_tool_category(record.tool_name.as_deref());
    let skill_name = record
        .links
        .skill_name
        .clone()
        .or_else(|| tool_input_string(input, &["skill", "skill_name", "name"]));
    let file_path = record.links.file_path.clone().or_else(|| {
        tool_input_string(
            input,
            &["file_path", "path", "file", "notebook_path", "target_file"],
        )
    });
    let subagent_session_id = record.links.subagent_session_id.clone().or_else(|| {
        tool_input_string(
            input,
            &[
                "subagent_session_id",
                "subagentSessionId",
                "agent_id",
                "agentId",
            ],
        )
    });
    let body = store_body(conn, input)?;
    let search_body = store_distinct_search_body(conn, &record.text, input)?;
    conn.prepare_cached(
        r#"
        INSERT INTO tool_calls(
            record_id, message_identity, message_record_id, owner_link_status,
            call_index, source_tool_use_id, category, status, source_status,
            skill_name, file_path,
            subagent_session_id,
            input_inline, input_blob_hash, search_text_inline, search_text_blob_hash
        )
        VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
        ON CONFLICT(record_id) DO UPDATE SET
            message_identity = excluded.message_identity,
            message_record_id = excluded.message_record_id,
            owner_link_status = excluded.owner_link_status,
            call_index = excluded.call_index,
            source_tool_use_id = excluded.source_tool_use_id,
            category = excluded.category,
            status = excluded.status,
            source_status = excluded.source_status,
            skill_name = excluded.skill_name,
            file_path = excluded.file_path,
            subagent_session_id = excluded.subagent_session_id,
            input_inline = excluded.input_inline,
            input_blob_hash = excluded.input_blob_hash,
            search_text_inline = excluded.search_text_inline,
            search_text_blob_hash = excluded.search_text_blob_hash
        "#,
    )?
    .execute(params![
        record_id,
        identity,
        Option::<i64>::None,
        owner_status,
        record.links.call_index.map(i64::from),
        source_tool_use_id,
        category,
        record.links.status,
        record.links.source_status,
        skill_name,
        file_path,
        subagent_session_id,
        body.inline,
        body.blob_hash,
        search_body.inline,
        search_body.blob_hash
    ])?;
    if resolve_relations && let Some(source_tool_use_id) = source_tool_use_id {
        conn.prepare_cached(
            r#"
            UPDATE tool_results
            SET tool_call_record_id = ?1
            WHERE tool_call_record_id IS NULL
              AND source_tool_use_id = ?2
              AND record_id IN (
                  SELECT record_id FROM records WHERE session_key = ?3
              )
            "#,
        )?
        .execute(params![record_id, source_tool_use_id, session_key])?;
    }
    Ok(())
}

fn upsert_tool_result(
    conn: &Connection,
    record_id: i64,
    record: &Record,
    session_key: &str,
    resolve_relations: bool,
) -> Result<()> {
    let source_tool_use_id = record
        .links
        .parent_tool_use_id
        .as_deref()
        .or(record.links.source_tool_use_id.as_deref())
        .or(record.links.parent_event_id.as_deref());
    let tool_call_record_id = if resolve_relations {
        source_tool_use_id
            .map(|source_id| lookup_tool_call_key(conn, session_key, source_id))
            .transpose()?
            .flatten()
    } else {
        None
    };
    let output = record.tool_output.as_deref().unwrap_or(&record.text);
    let call_link_status = if tool_call_record_id.is_some() {
        "linked"
    } else if source_tool_use_id.is_some() {
        "source_identity_unmatched"
    } else {
        "source_identity_missing"
    };
    let body = store_body(conn, output)?;
    let search_body = store_distinct_search_body(conn, &record.text, output)?;
    conn.prepare_cached(
        r#"
        INSERT INTO tool_results(
            record_id, tool_call_record_id, source_tool_use_id, call_link_status,
            event_index, status, source_status, subagent_session_id,
            output_inline, output_blob_hash, search_text_inline, search_text_blob_hash
        )
        VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        ON CONFLICT(record_id) DO UPDATE SET
            tool_call_record_id = excluded.tool_call_record_id,
            source_tool_use_id = excluded.source_tool_use_id,
            call_link_status = excluded.call_link_status,
            event_index = excluded.event_index,
            status = excluded.status,
            source_status = excluded.source_status,
            subagent_session_id = excluded.subagent_session_id,
            output_inline = excluded.output_inline,
            output_blob_hash = excluded.output_blob_hash,
            search_text_inline = excluded.search_text_inline,
            search_text_blob_hash = excluded.search_text_blob_hash
        "#,
    )?
    .execute(params![
        record_id,
        tool_call_record_id,
        source_tool_use_id,
        call_link_status,
        record.links.event_index.map(i64::from),
        record.links.status,
        record.links.source_status,
        record.links.subagent_session_id,
        body.inline,
        body.blob_hash,
        search_body.inline,
        search_body.blob_hash
    ])?;
    Ok(())
}

fn ensure_interaction(
    conn: &Connection,
    session_key: &str,
    source_interaction_id: &str,
    occurred_at: u64,
) -> Result<i64> {
    conn.prepare_cached(
        r#"
        INSERT INTO interactions(
            session_key, source_interaction_id, started_at, ended_at
        )
        VALUES(?1, ?2, ?3, ?3)
        ON CONFLICT(session_key, source_interaction_id) DO UPDATE SET
            started_at = MIN(interactions.started_at, excluded.started_at),
            ended_at = MAX(interactions.ended_at, excluded.ended_at)
        RETURNING interaction_id
        "#,
    )?
    .query_row(
        params![session_key, source_interaction_id, to_sql_u64(occurred_at)],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn link_interactions(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        INSERT INTO interactions(
            session_key, source_interaction_id, started_at, ended_at
        )
        SELECT
            session_key,
            source_interaction_id,
            MIN(occurred_at),
            MAX(occurred_at)
        FROM temp.pending_record_interactions
        GROUP BY session_key, source_interaction_id
        ON CONFLICT(session_key, source_interaction_id) DO UPDATE SET
            started_at = MIN(interactions.started_at, excluded.started_at),
            ended_at = MAX(interactions.ended_at, excluded.ended_at);

        DROP TABLE IF EXISTS temp.record_interaction_links;
        CREATE TEMP TABLE record_interaction_links (
            record_id INTEGER PRIMARY KEY,
            interaction_id INTEGER NOT NULL
        );
        INSERT INTO record_interaction_links(record_id, interaction_id)
        SELECT pending.record_id, interaction.interaction_id
        FROM temp.pending_record_interactions AS pending
        JOIN interactions AS interaction
          ON interaction.session_key = pending.session_key
         AND interaction.source_interaction_id = pending.source_interaction_id
        JOIN records AS record ON record.record_id = pending.record_id
        WHERE record.interaction_id IS NULL;
        UPDATE records AS record
        SET interaction_id = link.interaction_id
        FROM record_interaction_links AS link
        WHERE record.record_id = link.record_id
          AND record.interaction_id IS NULL;
        DROP TABLE temp.record_interaction_links;
        DROP TABLE temp.pending_record_interactions;
        "#,
    )?;
    Ok(())
}

pub(crate) fn link_relations(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        DROP TABLE IF EXISTS temp.affected_sessions;
        CREATE TEMP TABLE affected_sessions (
            session_key TEXT PRIMARY KEY
        ) WITHOUT ROWID;
        INSERT INTO affected_sessions(session_key)
        SELECT DISTINCT session_key FROM records;
        "#,
    )?;
    link_relations_in_affected_sessions(conn)
}

pub(crate) fn link_relations_for_sessions<'a>(
    conn: &Connection,
    session_keys: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    conn.execute_batch(
        r#"
        DROP TABLE IF EXISTS temp.affected_sessions;
        CREATE TEMP TABLE affected_sessions (
            session_key TEXT PRIMARY KEY
        ) WITHOUT ROWID;
        "#,
    )?;
    {
        let mut insert =
            conn.prepare_cached("INSERT OR IGNORE INTO affected_sessions(session_key) VALUES(?1)")?;
        for session_key in session_keys {
            insert.execute(params![session_key])?;
        }
    }
    link_relations_in_affected_sessions(conn)
}

fn link_relations_in_affected_sessions(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        DROP TABLE IF EXISTS temp.message_entity_links;
        CREATE TEMP TABLE message_entity_links (
            child_id INTEGER PRIMARY KEY,
            message_id INTEGER NOT NULL
        );
        INSERT INTO message_entity_links(child_id, message_id)
        SELECT child.record_id, MIN(parent.record_id)
        FROM thinking AS child
        JOIN records AS child_record ON child_record.record_id = child.record_id
        JOIN affected_sessions AS affected
          ON affected.session_key = child_record.session_key
        JOIN messages AS parent
          ON parent.session_key = child_record.session_key
         AND parent.message_identity = child.message_identity
        WHERE child.message_record_id IS NULL
          AND child.message_identity IS NOT NULL
        GROUP BY child.record_id
        UNION ALL
        SELECT child.record_id, MIN(parent.record_id)
        FROM tool_calls AS child
        JOIN records AS child_record ON child_record.record_id = child.record_id
        JOIN affected_sessions AS affected
          ON affected.session_key = child_record.session_key
        JOIN messages AS parent
          ON parent.session_key = child_record.session_key
         AND parent.message_identity = child.message_identity
        WHERE child.message_record_id IS NULL
          AND child.message_identity IS NOT NULL
        GROUP BY child.record_id;
        INSERT OR IGNORE INTO message_entity_links(child_id, message_id)
        SELECT child.record_id, MIN(parent.record_id)
        FROM thinking AS child
        JOIN records AS child_record ON child_record.record_id = child.record_id
        JOIN affected_sessions AS affected
          ON affected.session_key = child_record.session_key
        JOIN messages AS parent
          ON parent.session_key = child_record.session_key
         AND parent.ordinal = child_record.message_ordinal
        WHERE child.message_record_id IS NULL
          AND child_record.message_ordinal IS NOT NULL
        GROUP BY child.record_id;
        INSERT OR IGNORE INTO message_entity_links(child_id, message_id)
        SELECT child.record_id, MIN(parent.record_id)
        FROM tool_calls AS child
        JOIN records AS child_record ON child_record.record_id = child.record_id
        JOIN affected_sessions AS affected
          ON affected.session_key = child_record.session_key
        JOIN messages AS parent
          ON parent.session_key = child_record.session_key
         AND parent.ordinal = child_record.message_ordinal
        WHERE child.message_record_id IS NULL
          AND child_record.message_ordinal IS NOT NULL
        GROUP BY child.record_id;
        UPDATE thinking AS child
        SET message_record_id = links.message_id
        FROM message_entity_links AS links
        WHERE child.message_record_id IS NULL
          AND child.record_id = links.child_id;
        UPDATE tool_calls AS child
        SET message_record_id = links.message_id
        FROM message_entity_links AS links
        WHERE child.message_record_id IS NULL
          AND child.record_id = links.child_id;
        UPDATE thinking
        SET owner_link_status = CASE
            WHEN message_record_id IS NOT NULL THEN 'linked'
            WHEN message_identity IS NOT NULL THEN 'source_identity_unmatched'
            WHEN EXISTS (
                SELECT 1
                FROM records
                WHERE records.record_id = thinking.record_id
                  AND records.message_ordinal IS NOT NULL
            ) THEN 'source_ordinal_unmatched'
            ELSE 'source_identity_missing'
        END
        WHERE record_id IN (
            SELECT record.record_id
            FROM records AS record
            JOIN affected_sessions AS affected USING(session_key)
        )
          AND owner_link_status <> CASE
            WHEN message_record_id IS NOT NULL THEN 'linked'
            WHEN message_identity IS NOT NULL THEN 'source_identity_unmatched'
            WHEN EXISTS (
                SELECT 1
                FROM records
                WHERE records.record_id = thinking.record_id
                  AND records.message_ordinal IS NOT NULL
            ) THEN 'source_ordinal_unmatched'
            ELSE 'source_identity_missing'
        END;
        UPDATE tool_calls
        SET owner_link_status = CASE
            WHEN message_record_id IS NOT NULL THEN 'linked'
            WHEN message_identity IS NOT NULL THEN 'source_identity_unmatched'
            WHEN EXISTS (
                SELECT 1
                FROM records
                WHERE records.record_id = tool_calls.record_id
                  AND records.message_ordinal IS NOT NULL
            ) THEN 'source_ordinal_unmatched'
            ELSE 'source_identity_missing'
        END
        WHERE record_id IN (
            SELECT record.record_id
            FROM records AS record
            JOIN affected_sessions AS affected USING(session_key)
        )
          AND owner_link_status <> CASE
            WHEN message_record_id IS NOT NULL THEN 'linked'
            WHEN message_identity IS NOT NULL THEN 'source_identity_unmatched'
            WHEN EXISTS (
                SELECT 1
                FROM records
                WHERE records.record_id = tool_calls.record_id
                  AND records.message_ordinal IS NOT NULL
            ) THEN 'source_ordinal_unmatched'
            ELSE 'source_identity_missing'
        END;
        DROP TABLE temp.message_entity_links;

        DROP TABLE IF EXISTS temp.tool_call_identity_links;
        CREATE TEMP TABLE tool_call_identity_links (
            session_key TEXT NOT NULL,
            source_tool_use_id TEXT NOT NULL,
            call_id INTEGER NOT NULL,
            PRIMARY KEY(session_key, source_tool_use_id)
        ) WITHOUT ROWID;
        INSERT INTO tool_call_identity_links(session_key, source_tool_use_id, call_id)
        SELECT record.session_key, call.source_tool_use_id, MIN(call.record_id)
        FROM tool_calls AS call
        JOIN records AS record ON record.record_id = call.record_id
        JOIN affected_sessions AS affected USING(session_key)
        WHERE call.source_tool_use_id IS NOT NULL
        GROUP BY record.session_key, call.source_tool_use_id;

        DROP TABLE IF EXISTS temp.tool_result_links;
        CREATE TEMP TABLE tool_result_links (
            result_id INTEGER PRIMARY KEY,
            call_id INTEGER NOT NULL
        );
        INSERT INTO tool_result_links(result_id, call_id)
        SELECT result.record_id, call.call_id
        FROM tool_results AS result
        JOIN records AS record ON record.record_id = result.record_id
        JOIN affected_sessions AS affected USING(session_key)
        JOIN tool_call_identity_links AS call
          ON call.session_key = record.session_key
         AND call.source_tool_use_id = result.source_tool_use_id
        WHERE result.tool_call_record_id IS NULL
          AND result.source_tool_use_id IS NOT NULL;
        UPDATE tool_results AS result
        SET tool_call_record_id = links.call_id
        FROM tool_result_links AS links
        WHERE result.record_id = links.result_id
          AND result.tool_call_record_id IS NULL;
        UPDATE tool_results
        SET call_link_status = CASE
            WHEN tool_call_record_id IS NOT NULL THEN 'linked'
            WHEN source_tool_use_id IS NOT NULL THEN 'source_identity_unmatched'
            ELSE 'source_identity_missing'
        END
        WHERE record_id IN (
            SELECT record.record_id
            FROM records AS record
            JOIN affected_sessions AS affected USING(session_key)
        )
          AND call_link_status <> CASE
            WHEN tool_call_record_id IS NOT NULL THEN 'linked'
            WHEN source_tool_use_id IS NOT NULL THEN 'source_identity_unmatched'
            ELSE 'source_identity_missing'
        END;
        DROP TABLE temp.tool_result_links;
        DROP TABLE temp.tool_call_identity_links;

        DROP TABLE IF EXISTS temp.tool_call_ordinals;
        CREATE TEMP TABLE tool_call_ordinals (
            record_id INTEGER PRIMARY KEY,
            call_index INTEGER NOT NULL
        );
        INSERT INTO tool_call_ordinals(record_id, call_index)
        SELECT call.record_id,
               ROW_NUMBER() OVER (
                   PARTITION BY call.message_record_id
                   ORDER BY record.source_order, call.record_id
               ) - 1
        FROM tool_calls AS call
        JOIN records AS record ON record.record_id = call.record_id
        JOIN affected_sessions AS affected USING(session_key)
        WHERE call.message_record_id IS NOT NULL;
        UPDATE tool_calls AS call
        SET call_index = ordinal.call_index
        FROM tool_call_ordinals AS ordinal
        WHERE call.record_id = ordinal.record_id
          AND call.call_index IS NOT ordinal.call_index;
        DROP TABLE temp.tool_call_ordinals;

        DROP TABLE IF EXISTS temp.tool_result_ordinals;
        CREATE TEMP TABLE tool_result_ordinals (
            record_id INTEGER PRIMARY KEY,
            event_index INTEGER NOT NULL
        );
        INSERT INTO tool_result_ordinals(record_id, event_index)
        SELECT result.record_id,
               ROW_NUMBER() OVER (
                   PARTITION BY result.tool_call_record_id
                   ORDER BY record.source_order, result.record_id
               ) - 1
        FROM tool_results AS result
        JOIN records AS record ON record.record_id = result.record_id
        JOIN affected_sessions AS affected USING(session_key)
        WHERE result.tool_call_record_id IS NOT NULL;
        UPDATE tool_results AS result
        SET event_index = ordinal.event_index
        FROM tool_result_ordinals AS ordinal
        WHERE result.record_id = ordinal.record_id
          AND result.event_index IS NOT ordinal.event_index;
        DROP TABLE temp.tool_result_ordinals;

        UPDATE records AS child_record
        SET interaction_id = owner_record.interaction_id
        FROM thinking AS child
        JOIN records AS affected_record ON affected_record.record_id = child.record_id
        JOIN affected_sessions AS affected
          ON affected.session_key = affected_record.session_key
        JOIN records AS owner_record
          ON owner_record.record_id = child.message_record_id
        WHERE child_record.record_id = child.record_id
          AND child_record.interaction_id IS NULL
          AND owner_record.interaction_id IS NOT NULL;
        UPDATE records AS child_record
        SET interaction_id = owner_record.interaction_id
        FROM tool_calls AS child
        JOIN records AS affected_record ON affected_record.record_id = child.record_id
        JOIN affected_sessions AS affected
          ON affected.session_key = affected_record.session_key
        JOIN records AS owner_record
          ON owner_record.record_id = child.message_record_id
        WHERE child_record.record_id = child.record_id
          AND child_record.interaction_id IS NULL
          AND owner_record.interaction_id IS NOT NULL;
        UPDATE records AS result_record
        SET interaction_id = call_record.interaction_id
        FROM tool_results AS result
        JOIN records AS affected_record ON affected_record.record_id = result.record_id
        JOIN affected_sessions AS affected
          ON affected.session_key = affected_record.session_key
        JOIN records AS call_record
          ON call_record.record_id = result.tool_call_record_id
        WHERE result_record.record_id = result.record_id
          AND result_record.interaction_id IS NULL
          AND call_record.interaction_id IS NOT NULL;

        DROP TABLE temp.affected_sessions;
        "#,
    )?;
    Ok(())
}

fn record_by_key(conn: &Connection, record_key: &str) -> Result<Option<Record>> {
    let metadata = conn
        .query_row(
            r#"
            SELECT
                   record.record_id, record.doc_id, record.entity_type, record.source,
                   record.source_path, record.session_key, record.session_id,
                   record.occurred_at, record.source_order, record.role, record.tool_name,
                   record.event_id, record.parent_event_id, record.logical_parent_event_id,
                   record.parent_session_id, record.thread_source, record.conversation_kind,
                   interaction.source_interaction_id, record.parent_tool_use_id,
                   record.source_tool_use_id, record.source_tool_assistant_uuid,
                   COALESCE(record.message_ordinal, message.ordinal),
                   call.call_index, result.event_index,
                   COALESCE(result.status, call.status),
                   COALESCE(result.source_status, call.source_status), message.model,
                   message.input_tokens, message.cache_read_tokens, message.cache_write_tokens,
                   message.output_tokens, message.reasoning_tokens,
                   call.skill_name, call.file_path,
                   COALESCE(call.subagent_session_id, result.subagent_session_id)
            FROM records AS record
            LEFT JOIN interactions AS interaction
              ON interaction.interaction_id = record.interaction_id
            LEFT JOIN messages AS message ON message.record_id = record.record_id
            LEFT JOIN tool_calls AS call ON call.record_id = record.record_id
            LEFT JOIN tool_results AS result ON result.record_id = record.record_id
            WHERE record.record_key = ?1
            "#,
            params![record_key],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    RecordLinks {
                        message_ordinal: row.get::<_, Option<i64>>(21)?.map(|value| value as u32),
                        call_index: row.get::<_, Option<i64>>(22)?.map(|value| value as u32),
                        event_index: row.get::<_, Option<i64>>(23)?.map(|value| value as u32),
                        interaction_id: row.get(17)?,
                        event_id: row.get(11)?,
                        parent_event_id: row.get(12)?,
                        logical_parent_event_id: row.get(13)?,
                        parent_session_id: row.get(14)?,
                        thread_source: row.get(15)?,
                        conversation_kind: row.get(16)?,
                        parent_tool_use_id: row.get(18)?,
                        source_tool_use_id: row.get(19)?,
                        source_tool_assistant_uuid: row.get(20)?,
                        status: row.get(24)?,
                        source_status: row.get(25)?,
                        model: row.get(26)?,
                        input_tokens: row.get::<_, Option<i64>>(27)?.map(from_sql_u64),
                        cache_read_tokens: row.get::<_, Option<i64>>(28)?.map(from_sql_u64),
                        cache_write_tokens: row.get::<_, Option<i64>>(29)?.map(from_sql_u64),
                        output_tokens: row.get::<_, Option<i64>>(30)?.map(from_sql_u64),
                        reasoning_tokens: row.get::<_, Option<i64>>(31)?.map(from_sql_u64),
                        skill_name: row.get(32)?,
                        file_path: row.get(33)?,
                        subagent_session_id: row.get(34)?,
                    },
                ))
            },
        )
        .optional()?;
    let Some((
        record_id,
        doc_id,
        entity_type,
        source,
        source_path,
        session_key,
        session_id,
        occurred_at,
        turn_id,
        role,
        tool_name,
        links,
    )) = metadata
    else {
        return Ok(None);
    };
    let (text, tool_input, tool_output) = load_entity_body(conn, record_id, &entity_type)?;
    let project = if table_has_column(conn, "sessions", "session_key")? {
        conn.query_row(
            "SELECT project FROM sessions WHERE session_key = ?1",
            params![session_key],
            |row| row.get::<_, String>(0),
        )
        .optional()?
    } else {
        conn.query_row(
            "SELECT project FROM sessions
             WHERE source = ?1 AND session_id = ?2 AND source_path = ?3",
            params![source, session_id, source_path],
            |row| row.get::<_, String>(0),
        )
        .optional()?
    }
    .unwrap_or_default();
    Ok(Some(Record {
        source: SourceKind::from_label(&source).unwrap_or_default(),
        record_key: record_key.to_string(),
        doc_id: from_sql_u64(doc_id),
        ts: from_sql_u64(occurred_at),
        project,
        session_id,
        turn_id: turn_id.max(0) as u32,
        role,
        text,
        tool_name,
        tool_input,
        tool_output,
        links,
        source_path,
    }))
}

fn load_entity_body(
    conn: &Connection,
    record_id: i64,
    entity_type: &str,
) -> Result<(String, Option<String>, Option<String>)> {
    let (table, inline_column, blob_column) = match entity_type {
        "thinking" => ("thinking", "content_inline", "content_blob_hash"),
        "tool_call" => ("tool_calls", "input_inline", "input_blob_hash"),
        "tool_result" => ("tool_results", "output_inline", "output_blob_hash"),
        _ => ("messages", "content_inline", "content_blob_hash"),
    };
    let sql = format!("SELECT {inline_column}, {blob_column} FROM {table} WHERE record_id = ?1");
    let body: Option<(Option<String>, Option<String>)> = conn
        .query_row(&sql, params![record_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()?;
    let text = match body {
        Some((Some(inline), _)) => inline,
        Some((None, Some(hash))) => load_blob(conn, &hash)?,
        _ => String::new(),
    };
    let search_text = match entity_type {
        "tool_call" | "tool_result" => load_optional_body(
            conn,
            table,
            "search_text_inline",
            "search_text_blob_hash",
            record_id,
        )?,
        _ => None,
    };
    let rendered_text = search_text.unwrap_or_else(|| text.clone());
    Ok(match entity_type {
        "tool_call" => (rendered_text, Some(text), None),
        "tool_result" => (rendered_text, None, Some(text)),
        _ => (rendered_text, None, None),
    })
}

fn load_optional_body(
    conn: &Connection,
    table: &str,
    inline_column: &str,
    blob_column: &str,
    record_id: i64,
) -> Result<Option<String>> {
    let sql = format!("SELECT {inline_column}, {blob_column} FROM {table} WHERE record_id = ?1");
    let body: Option<(Option<String>, Option<String>)> = conn
        .query_row(&sql, params![record_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()?;
    match body {
        Some((Some(inline), _)) => Ok(Some(inline)),
        Some((None, Some(hash))) => load_blob(conn, &hash).map(Some),
        _ => Ok(None),
    }
}

fn store_distinct_search_body(
    conn: &Connection,
    search_text: &str,
    typed_body: &str,
) -> Result<StoredBody> {
    if search_text == typed_body {
        return Ok(StoredBody {
            inline: None,
            blob_hash: None,
        });
    }
    store_body(conn, search_text)
}

fn store_body(conn: &Connection, body: &str) -> Result<StoredBody> {
    if body.len() <= INLINE_BODY_BYTES {
        return Ok(StoredBody {
            inline: Some(body.to_string()),
            blob_hash: None,
        });
    }
    let hash = format!("{:x}", Sha256::digest(body.as_bytes()));
    let payload = zstd::stream::encode_all(Cursor::new(body.as_bytes()), 3)?;
    conn.prepare_cached(
        "INSERT OR IGNORE INTO content_blobs(content_hash, codec, original_bytes, payload)
         VALUES(?1, 'zstd', ?2, ?3)",
    )?
    .execute(params![hash, to_sql_u64(body.len() as u64), payload])?;
    Ok(StoredBody {
        inline: None,
        blob_hash: Some(hash),
    })
}

fn load_blob(conn: &Connection, hash: &str) -> Result<String> {
    let (codec, payload): (String, Vec<u8>) = conn.query_row(
        "SELECT codec, payload FROM content_blobs WHERE content_hash = ?1",
        params![hash],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let bytes = match codec.as_str() {
        "zstd" => zstd::stream::decode_all(Cursor::new(payload))?,
        other => return Err(anyhow!("unsupported catalog blob codec {other}")),
    };
    String::from_utf8(bytes).context("catalog blob is not UTF-8")
}

fn entity_type(record: &Record) -> &'static str {
    match record.role.as_str() {
        "reasoning" => "thinking",
        "tool_use" => "tool_call",
        "tool_result" => "tool_result",
        _ => "message",
    }
}

fn stable_session_key(record: &Record) -> String {
    session_key(record.source, &record.session_id, &record.source_path)
}

pub(crate) fn session_key(source: SourceKind, session_id: &str, source_path: &str) -> String {
    session_key_from_label(source.storage_label(), session_id, source_path)
}

pub(crate) fn session_key_from_label(source: &str, session_id: &str, source_path: &str) -> String {
    stable_key(
        "session",
        &[
            source,
            if session_id.is_empty() {
                source_path
            } else {
                session_id
            },
        ],
    )
}

fn message_identity(record: &Record, session_key: &str) -> Option<String> {
    record
        .links
        .event_id
        .as_deref()
        .map(|event| stable_key("message", &[session_key, event]))
}

fn owner_message_identity(record: &Record, session_key: &str) -> Option<String> {
    record
        .links
        .parent_event_id
        .as_deref()
        .or(record.links.source_tool_assistant_uuid.as_deref())
        .map(|event| stable_key("message", &[session_key, event]))
}

fn pending_owner_status(identity: Option<&str>, message_ordinal: Option<u32>) -> &'static str {
    if identity.is_some() || message_ordinal.is_some() {
        "pending"
    } else {
        "source_identity_missing"
    }
}

fn normalized_tool_category(tool_name: Option<&str>) -> &'static str {
    let Some(name) = tool_name else {
        return "other";
    };
    let normalized = name.to_ascii_lowercase();
    if normalized.contains("skill") {
        "skill"
    } else if [
        "read", "write", "edit", "patch", "glob", "grep", "file", "notebook",
    ]
    .iter()
    .any(|part| normalized.contains(part))
    {
        "filesystem"
    } else if ["bash", "shell", "exec", "terminal", "command"]
        .iter()
        .any(|part| normalized.contains(part))
    {
        "shell"
    } else if normalized.contains("search") || normalized.contains("fetch") {
        "search"
    } else if ["agent", "task", "delegate"]
        .iter()
        .any(|part| normalized.contains(part))
    {
        "agent"
    } else {
        "other"
    }
}

fn tool_input_string(input: &str, keys: &[&str]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(input).ok()?;
    let object = value.as_object()?;
    keys.iter().find_map(|key| {
        object
            .get(*key)
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    })
}

fn stable_key(kind: &str, parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(kind.as_bytes());
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("{kind}_{:x}", hasher.finalize())
}

fn lookup_tool_call_key(
    conn: &Connection,
    session_key: &str,
    source_tool_use_id: &str,
) -> Result<Option<i64>> {
    conn.prepare_cached(
        r#"
        SELECT tc.record_id
        FROM tool_calls tc
        JOIN records r ON r.record_id = tc.record_id
        WHERE r.session_key = ?1 AND tc.source_tool_use_id = ?2
        ORDER BY tc.call_index LIMIT 1
        "#,
    )?
    .query_row(params![session_key, source_tool_use_id], |row| row.get(0))
    .optional()
    .map_err(Into::into)
}

fn to_sql_u64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn from_sql_u64(value: i64) -> u64 {
    value.max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(role: &str, text: String) -> Record {
        Record {
            source: SourceKind::Codex,
            record_key: format!("rk-{role}"),
            doc_id: 1,
            ts: 10,
            project: "memex".to_string(),
            session_id: "session".to_string(),
            turn_id: 1,
            role: role.to_string(),
            text,
            tool_name: (role == "tool_use").then(|| "Read".to_string()),
            tool_input: None,
            tool_output: None,
            links: RecordLinks {
                event_id: Some("event".to_string()),
                parent_event_id: Some("assistant-event".to_string()),
                parent_tool_use_id: Some("call".to_string()),
                ..RecordLinks::default()
            },
            source_path: "/tmp/session.jsonl".to_string(),
        }
    }

    fn seed_session(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS sessions (
                source TEXT NOT NULL,
                session_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                project TEXT NOT NULL,
                PRIMARY KEY(source, session_id, source_path)
             );
             INSERT INTO sessions(source, session_id, source_path, project)
             VALUES('codex', 'session', '/tmp/session.jsonl', 'memex');",
        )
        .expect("seed session");
    }

    #[test]
    fn large_bodies_are_compressed_and_hydrated() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = CatalogStore::open(tmp.path().join("catalog.sqlite")).expect("catalog");
        seed_session(&store.conn);
        let original = record("assistant", "repeated content ".repeat(4_000));

        upsert_record(&store.conn, &original).expect("insert");
        let hydrated = store
            .record_by_key(&original.record_key)
            .expect("hydrate")
            .expect("record");

        assert_eq!(hydrated.text, original.text);
        let blobs: i64 = store
            .conn
            .query_row("SELECT count(*) FROM content_blobs", [], |row| row.get(0))
            .expect("blob count");
        assert_eq!(blobs, 1);
    }

    #[test]
    fn tool_payload_is_stored_once_and_reconstructed_into_record_shape() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = CatalogStore::open(tmp.path().join("catalog.sqlite")).expect("catalog");
        seed_session(&store.conn);
        let mut original = record("tool_use", "{\"path\":\"Cargo.toml\"}".to_string());
        original.links.status = Some("pending".to_string());
        original.links.source_status = Some("in_progress".to_string());

        upsert_record(&store.conn, &original).expect("insert");
        let hydrated = store
            .record_by_key(&original.record_key)
            .expect("hydrate")
            .expect("record");

        assert_eq!(hydrated.text, original.text);
        assert_eq!(hydrated.tool_input.as_deref(), Some(original.text.as_str()));
        assert_eq!(hydrated.links.status.as_deref(), Some("pending"));
        assert_eq!(hydrated.links.source_status.as_deref(), Some("in_progress"));
        let stored_copies: i64 = store
            .conn
            .query_row(
                "SELECT (tc.input_inline IS NOT NULL)
                 FROM tool_calls tc
                 JOIN records r ON r.record_id = tc.record_id
                 WHERE r.record_key = ?1",
                params![original.record_key],
                |row| row.get(0),
            )
            .expect("stored body");
        assert_eq!(stored_copies, 1);
    }

    #[test]
    fn owner_link_updates_use_partial_identity_indexes() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = CatalogStore::open(tmp.path().join("catalog.sqlite")).expect("catalog");

        for (table, index) in [
            ("thinking", "thinking_unlinked_identity_idx"),
            ("tool_calls", "tool_calls_unlinked_identity_idx"),
        ] {
            let sql = format!(
                "EXPLAIN QUERY PLAN UPDATE {table}
                 SET message_record_id = ?1
                 WHERE message_identity = ?2 AND message_record_id IS NULL"
            );
            let details = store
                .conn
                .prepare(&sql)
                .expect("prepare query plan")
                .query_map(params!["message", "identity"], |row| {
                    row.get::<_, String>(3)
                })
                .expect("query plan")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("collect query plan");
            assert!(
                details.iter().any(|detail| detail.contains(index)),
                "{table} plan did not use {index}: {details:?}"
            );
        }
    }

    #[test]
    fn transcript_navigation_uses_ordering_indexes() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = CatalogStore::open(tmp.path().join("catalog.sqlite")).expect("catalog");
        for (sql, expected_index) in [
            (
                "SELECT record_key FROM records
                 WHERE session_key = ?1
                 ORDER BY occurred_at, source_order",
                "records_session_time_idx",
            ),
            (
                "SELECT record_id FROM messages
                 WHERE session_key = ?1
                 ORDER BY ordinal",
                "sqlite_autoindex_messages_1",
            ),
            (
                "SELECT record_id FROM tool_calls
                 WHERE message_record_id = ?1
                 ORDER BY call_index",
                "tool_calls_message_idx",
            ),
            (
                "SELECT record_id FROM tool_results
                 WHERE tool_call_record_id = ?1
                 ORDER BY event_index",
                "tool_results_call_idx",
            ),
        ] {
            let details = store
                .conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .expect("prepare query plan")
                .query_map(params!["owner"], |row| row.get::<_, String>(3))
                .expect("query plan")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("collect query plan");
            assert!(
                details.iter().any(|detail| detail.contains(expected_index)),
                "plan did not use {expected_index}: {details:?}"
            );
        }
    }

    #[test]
    fn relation_linking_reconciles_out_of_order_entities() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = CatalogStore::open(tmp.path().join("catalog.sqlite")).expect("catalog");
        seed_session(&store.conn);

        let mut result = record("tool_result", "result".to_string());
        result.record_key = "rk-result".to_string();
        result.doc_id = 1;
        result.links.event_id = Some("result-event".to_string());
        result.links.parent_tool_use_id = Some("call".to_string());

        let mut call = record("tool_use", "input".to_string());
        call.record_key = "rk-call".to_string();
        call.doc_id = 2;
        call.links.interaction_id = Some("turn-1".to_string());
        call.links.event_id = Some("call".to_string());
        call.links.parent_event_id = Some("assistant-event".to_string());

        let mut message = record("assistant", "answer".to_string());
        message.record_key = "rk-message".to_string();
        message.doc_id = 3;
        message.links.event_id = Some("assistant-event".to_string());

        for entity in [&result, &call, &message] {
            upsert_record(&store.conn, entity).expect("insert entity");
        }
        link_relations(&store.conn).expect("link relations");

        let message_key: Option<String> = store
            .conn
            .query_row(
                "SELECT owner.record_key
                 FROM tool_calls tc
                 JOIN records child ON child.record_id = tc.record_id
                 LEFT JOIN records owner ON owner.record_id = tc.message_record_id
                 WHERE child.record_key = 'rk-call'",
                [],
                |row| row.get(0),
            )
            .expect("tool call owner");
        let tool_call_key: Option<String> = store
            .conn
            .query_row(
                "SELECT owner.record_key
                 FROM tool_results tr
                 JOIN records child ON child.record_id = tr.record_id
                 LEFT JOIN records owner ON owner.record_id = tr.tool_call_record_id
                 WHERE child.record_key = 'rk-result'",
                [],
                |row| row.get(0),
            )
            .expect("tool result owner");
        let result_interaction: Option<String> = store
            .conn
            .query_row(
                "SELECT interaction.source_interaction_id
                 FROM records record
                 LEFT JOIN interactions interaction
                   ON interaction.interaction_id = record.interaction_id
                 WHERE record.record_key = 'rk-result'",
                [],
                |row| row.get(0),
            )
            .expect("tool result interaction");
        assert_eq!(message_key.as_deref(), Some("rk-message"));
        assert_eq!(tool_call_key.as_deref(), Some("rk-call"));
        assert_eq!(result_interaction.as_deref(), Some("turn-1"));
    }

    #[test]
    fn incremental_relation_linking_is_scoped_to_affected_sessions() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = CatalogStore::open(tmp.path().join("catalog.sqlite")).expect("catalog");
        let mut session_keys = Vec::new();
        for (offset, session_id) in ["session-a", "session-b"].into_iter().enumerate() {
            let source_path = format!("/tmp/{session_id}.jsonl");
            let mut call = record("tool_use", "{}".to_string());
            call.record_key = format!("rk-{session_id}-call");
            call.doc_id = (offset * 2 + 1) as u64;
            call.session_id = session_id.to_string();
            call.source_path = source_path.clone();
            call.links.event_id = Some(format!("{session_id}-call"));
            call.links.parent_event_id = Some(format!("{session_id}-message"));

            let mut message = record("assistant", "owner".to_string());
            message.record_key = format!("rk-{session_id}-message");
            message.doc_id = (offset * 2 + 2) as u64;
            message.session_id = session_id.to_string();
            message.source_path = source_path;
            message.links.event_id = Some(format!("{session_id}-message"));

            bulk_insert_record(&store.conn, &call).expect("insert call");
            bulk_insert_record(&store.conn, &message).expect("insert message");
            session_keys.push(stable_session_key(&message));
        }

        link_relations_for_sessions(&store.conn, [session_keys[0].as_str()])
            .expect("link affected session");

        let statuses = store
            .conn
            .prepare(
                "SELECT record.session_id, call.owner_link_status
                 FROM tool_calls AS call
                 JOIN records AS record ON record.record_id = call.record_id
                 ORDER BY record.session_id",
            )
            .expect("prepare statuses")
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .expect("query statuses")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect statuses");
        assert_eq!(
            statuses,
            vec![
                ("session-a".to_string(), "linked".to_string()),
                ("session-b".to_string(), "pending".to_string()),
            ]
        );
    }

    #[test]
    fn relationship_projection_assigns_true_child_ordinals_and_link_states() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = CatalogStore::open(tmp.path().join("catalog.sqlite")).expect("catalog");
        seed_session(&store.conn);

        let mut message = record("assistant", "working".to_string());
        message.record_key = "rk-message".to_string();
        message.doc_id = 1;
        message.turn_id = 10;
        message.links = RecordLinks {
            event_id: Some("message-1".to_string()),
            message_ordinal: Some(4),
            model: Some("gpt-test".to_string()),
            input_tokens: Some(100),
            output_tokens: Some(20),
            ..RecordLinks::default()
        };

        let mut first_call = record("tool_use", r#"{"file_path":"a.rs"}"#.to_string());
        first_call.record_key = "rk-call-a".to_string();
        first_call.doc_id = 2;
        first_call.turn_id = 11;
        first_call.links = RecordLinks {
            event_id: Some("call-a".to_string()),
            message_ordinal: Some(4),
            call_index: Some(99),
            ..RecordLinks::default()
        };

        let mut second_call = record(
            "tool_use",
            r#"{"skill":"review","agent_id":"agent-7"}"#.to_string(),
        );
        second_call.record_key = "rk-call-b".to_string();
        second_call.doc_id = 3;
        second_call.turn_id = 12;
        second_call.tool_name = Some("Skill".to_string());
        second_call.links = RecordLinks {
            event_id: Some("call-b".to_string()),
            message_ordinal: Some(4),
            call_index: Some(99),
            ..RecordLinks::default()
        };

        let mut first_result = record("tool_result", "progress".to_string());
        first_result.record_key = "rk-result-a0".to_string();
        first_result.doc_id = 4;
        first_result.turn_id = 13;
        first_result.links = RecordLinks {
            parent_tool_use_id: Some("call-a".to_string()),
            event_index: Some(99),
            status: Some("pending".to_string()),
            source_status: Some("running".to_string()),
            ..RecordLinks::default()
        };

        let mut final_result = record("tool_result", "done".to_string());
        final_result.record_key = "rk-result-a1".to_string();
        final_result.doc_id = 5;
        final_result.turn_id = 14;
        final_result.links = RecordLinks {
            parent_tool_use_id: Some("call-a".to_string()),
            event_index: Some(99),
            status: Some("success".to_string()),
            source_status: Some("completed".to_string()),
            ..RecordLinks::default()
        };

        let mut orphan_call = record("tool_use", "{}".to_string());
        orphan_call.record_key = "rk-call-orphan".to_string();
        orphan_call.doc_id = 6;
        orphan_call.turn_id = 15;
        orphan_call.links = RecordLinks {
            event_id: Some("call-orphan".to_string()),
            ..RecordLinks::default()
        };
        let mut unmatched_ordinal_call = record("tool_use", "{}".to_string());
        unmatched_ordinal_call.record_key = "rk-call-unmatched-ordinal".to_string();
        unmatched_ordinal_call.doc_id = 7;
        unmatched_ordinal_call.turn_id = 16;
        unmatched_ordinal_call.links = RecordLinks {
            event_id: Some("call-unmatched-ordinal".to_string()),
            message_ordinal: Some(99),
            ..RecordLinks::default()
        };

        for entity in [
            &final_result,
            &second_call,
            &first_result,
            &first_call,
            &message,
            &orphan_call,
            &unmatched_ordinal_call,
        ] {
            upsert_record(&store.conn, entity).expect("insert entity");
        }
        link_relations(&store.conn).expect("link relations");

        let calls = store
            .conn
            .prepare(
                "SELECT record.record_key, call.call_index, call.owner_link_status,
                        call.category, call.skill_name, call.file_path,
                        call.subagent_session_id
                 FROM tool_calls AS call
                 JOIN records AS record ON record.record_id = call.record_id
                 WHERE call.message_record_id IS NOT NULL
                 ORDER BY call.call_index",
            )
            .expect("prepare calls")
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })
            .expect("query calls")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect calls");
        assert_eq!(
            calls,
            vec![
                (
                    "rk-call-a".to_string(),
                    0,
                    "linked".to_string(),
                    "filesystem".to_string(),
                    None,
                    Some("a.rs".to_string()),
                    None,
                ),
                (
                    "rk-call-b".to_string(),
                    1,
                    "linked".to_string(),
                    "skill".to_string(),
                    Some("review".to_string()),
                    None,
                    Some("agent-7".to_string()),
                ),
            ]
        );

        let results = store
            .conn
            .prepare(
                "SELECT result.event_index, result.call_link_status, result.status,
                        result.source_status
                 FROM tool_results AS result
                 JOIN records AS record ON record.record_id = result.record_id
                 WHERE result.tool_call_record_id = (
                     SELECT record_id FROM records WHERE record_key = 'rk-call-a'
                 )
                 ORDER BY result.event_index",
            )
            .expect("prepare results")
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .expect("query results")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect results");
        assert_eq!(
            results,
            vec![
                (
                    0,
                    "linked".to_string(),
                    Some("pending".to_string()),
                    Some("running".to_string()),
                ),
                (
                    1,
                    "linked".to_string(),
                    Some("success".to_string()),
                    Some("completed".to_string()),
                ),
            ]
        );

        let message_facts: (i64, String, i64, i64) = store
            .conn
            .query_row(
                "SELECT ordinal, model, input_tokens, output_tokens FROM messages",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("message facts");
        assert_eq!(message_facts, (4, "gpt-test".to_string(), 100, 20));

        let orphan_status: String = store
            .conn
            .query_row(
                "SELECT owner_link_status FROM tool_calls
                 WHERE record_id = (
                     SELECT record_id FROM records WHERE record_key = 'rk-call-orphan'
                 )",
                [],
                |row| row.get(0),
            )
            .expect("orphan status");
        assert_eq!(orphan_status, "source_identity_missing");
        let unmatched_ordinal_status: String = store
            .conn
            .query_row(
                "SELECT owner_link_status FROM tool_calls
                 WHERE record_id = (
                     SELECT record_id FROM records
                     WHERE record_key = 'rk-call-unmatched-ordinal'
                 )",
                [],
                |row| row.get(0),
            )
            .expect("unmatched ordinal status");
        assert_eq!(unmatched_ordinal_status, "source_ordinal_unmatched");
    }

    #[test]
    fn bulk_load_links_sibling_entities_to_their_source_interaction() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = CatalogStore::open(tmp.path().join("catalog.sqlite")).expect("catalog");
        seed_session(&store.conn);

        let mut message = record("assistant", "progress".to_string());
        message.record_key = "rk-progress".to_string();
        message.doc_id = 1;
        message.links.interaction_id = Some("turn-1".to_string());
        let mut call = record("tool_use", "input".to_string());
        call.record_key = "rk-call".to_string();
        call.doc_id = 2;
        call.links.interaction_id = Some("turn-1".to_string());

        begin_bulk_load(&store.conn).expect("begin bulk load");
        bulk_insert_record(&store.conn, &message).expect("insert message");
        bulk_insert_record(&store.conn, &call).expect("insert call");
        finish_bulk_load(&store.conn).expect("finish bulk load");

        let (interactions, linked_records): (i64, i64) = store
            .conn
            .query_row(
                "SELECT
                    (SELECT count(*) FROM interactions),
                    (SELECT count(*) FROM records WHERE interaction_id IS NOT NULL)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("interaction counts");
        assert_eq!(interactions, 1);
        assert_eq!(linked_records, 2);
    }

    #[test]
    fn catalog_schema_change_marks_the_projection_for_rebuild() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("catalog.sqlite");
        {
            let store = CatalogStore::open(&path).expect("catalog");
            store
                .conn
                .execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                    INSERT INTO meta(key, value)
                    VALUES('analytics_complete', '1');
                    INSERT INTO interactions(
                        session_key, source_interaction_id, started_at, ended_at
                    ) VALUES('session-1', 'turn-1', 1, 1);
                    CREATE TABLE usage_events (
                        interaction_id INTEGER
                            REFERENCES interactions(interaction_id) ON DELETE SET NULL
                    );
                    INSERT INTO usage_events(interaction_id)
                    SELECT interaction_id FROM interactions;
                    UPDATE catalog_meta SET value = '8' WHERE key = 'schema_version';
                    "#,
                )
                .expect("seed stale schema");
        }

        let store = CatalogStore::open(&path).expect("migrated catalog");
        let complete: Option<String> = store
            .conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'analytics_complete'",
                [],
                |row| row.get(0),
            )
            .optional()
            .expect("read completeness");
        let version: String = store
            .conn
            .query_row(
                "SELECT value FROM catalog_meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .expect("catalog version");
        let preserved_usage_link: Option<i64> = store
            .conn
            .query_row("SELECT interaction_id FROM usage_events", [], |row| {
                row.get(0)
            })
            .expect("preserved usage link");
        assert!(complete.is_none());
        assert_eq!(version, CATALOG_SCHEMA_VERSION.to_string());
        assert!(preserved_usage_link.is_some());
    }
}
