use super::{IndexParseOutput, IndexParseState, ParserVersions, SourceFile};
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::Result;
use rusqlite::{Connection, OpenFlags};
use sha2::{Digest, Sha256};
use simd_json::BorrowedValue;
use simd_json::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use walkdir::WalkDir;

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 2,
    index: 4,
    usage: 3,
};

pub fn matches_path(path: &str) -> bool {
    path.contains("opencode/storage/message") || path.contains("opencode\\storage\\message")
}

pub fn data_roots() -> Vec<PathBuf> {
    std::env::var_os("OPENCODE_DATA_DIR")
        .map(|roots| {
            roots
                .to_string_lossy()
                .split(',')
                .map(|root| PathBuf::from(root.trim()))
                .collect()
        })
        .unwrap_or_else(|| vec![super::common::home().join(".local/share/opencode")])
}

pub fn storage_root() -> PathBuf {
    data_roots()
        .into_iter()
        .next()
        .unwrap_or_else(|| super::common::home().join(".local/share/opencode"))
        .join("storage")
}

pub fn message_root() -> PathBuf {
    storage_root().join("message")
}

pub fn parts_root() -> PathBuf {
    storage_root().join("part")
}

fn parts_root_for_session(session_dir: &Path) -> PathBuf {
    session_dir
        .parent()
        .and_then(Path::parent)
        .map(|storage| storage.join("part"))
        .unwrap_or_else(parts_root)
}

/// Fingerprint every file that contributes to one OpenCode session projection. OpenCode stores
/// messages and their mutable tool state in separate trees, so watching only the message
/// directory misses status/output-only updates.
pub(crate) fn session_dependency_fingerprint(
    session_dir: &Path,
) -> Result<(u64, i64, i64, String)> {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;

    let mut dependencies = Vec::new();
    let parts_root = parts_root_for_session(session_dir);
    for entry in std::fs::read_dir(session_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let mut message_id = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(str::to_string);
        if let Ok(mut bytes) = std::fs::read(&path)
            && let Ok(message) = simd_json::to_borrowed_value(&mut bytes)
        {
            message_id = message
                .get("id")
                .and_then(|value| value.as_str())
                .filter(|id| !id.is_empty())
                .map(str::to_string)
                .or(message_id);
        }
        dependencies.push(path);
        if let Some(message_id) = message_id {
            let part_dir = parts_root.join(message_id);
            if let Ok(entries) = std::fs::read_dir(part_dir) {
                dependencies.extend(
                    entries.flatten().map(|entry| entry.path()).filter(|path| {
                        path.extension().and_then(|ext| ext.to_str()) == Some("json")
                    }),
                );
            }
        }
    }
    dependencies.sort();

    let mut total_size = 0u64;
    let mut newest_seconds = 0i64;
    let mut newest_ns = 0i64;
    let mut hasher = Sha256::new();
    for path in dependencies {
        let metadata = match path.metadata() {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let modified_ns = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
            .unwrap_or(0);
        total_size = total_size.saturating_add(metadata.len());
        newest_ns = newest_ns.max(modified_ns);
        newest_seconds = newest_seconds.max(modified_ns / 1_000_000_000);
        let path = path.to_string_lossy();
        hasher.update((path.len() as u64).to_le_bytes());
        hasher.update(path.as_bytes());
        hasher.update(metadata.len().to_le_bytes());
        hasher.update(modified_ns.to_le_bytes());
        #[cfg(unix)]
        {
            hasher.update(metadata.dev().to_le_bytes());
            hasher.update(metadata.ino().to_le_bytes());
        }
    }
    Ok((
        total_size,
        newest_seconds,
        newest_ns,
        format!("{:x}", hasher.finalize()),
    ))
}

pub fn discover_sessions() -> anyhow::Result<Vec<SourceFile>> {
    discover_sessions_from_root(&message_root())
}

pub fn discover_sessions_from_root(root: &Path) -> anyhow::Result<Vec<SourceFile>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir()
            && entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("ses_"))
        {
            files.push(SourceFile {
                source: SourceKind::Opencode,
                path: entry.path(),
            });
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

#[derive(Clone, Default)]
pub(crate) struct SessionLinks {
    pub parent_session_id: Option<String>,
    pub thread_source: Option<String>,
    pub conversation_kind: Option<String>,
}

impl SessionLinks {
    fn record_links(&self) -> RecordLinks {
        RecordLinks {
            parent_session_id: self.parent_session_id.clone(),
            thread_source: self.thread_source.clone(),
            conversation_kind: self.conversation_kind.clone(),
            ..RecordLinks::default()
        }
    }
}

fn default_session_links() -> SessionLinks {
    SessionLinks {
        conversation_kind: Some("main".to_string()),
        ..SessionLinks::default()
    }
}

pub(crate) fn session_links_by_id() -> HashMap<String, SessionLinks> {
    session_links_by_id_from_root(&storage_root().join("session"))
}

pub(crate) fn session_links_by_id_from_root(root: &Path) -> HashMap<String, SessionLinks> {
    let mut links_by_id = HashMap::new();
    if !root.exists() {
        return links_by_id;
    }
    for entry in WalkDir::new(root).into_iter().flatten().filter(|entry| {
        entry.file_type().is_file()
            && entry.path().extension().and_then(|ext| ext.to_str()) == Some("json")
    }) {
        let Some(session_id) = entry
            .path()
            .file_stem()
            .and_then(|stem| stem.to_str())
            .filter(|id| !id.is_empty())
            .map(str::to_string)
        else {
            continue;
        };
        let Ok(mut bytes) = std::fs::read(entry.path()) else {
            continue;
        };
        let Ok(value) = simd_json::to_borrowed_value(&mut bytes) else {
            continue;
        };
        links_by_id.insert(session_id, session_links_from_value(&value));
    }
    links_by_id
}

fn session_links_from_value(value: &BorrowedValue<'_>) -> SessionLinks {
    let parent_session_id = value
        .get("parentID")
        .and_then(|value| value.as_str())
        .filter(|id| !id.is_empty())
        .map(str::to_string);
    SessionLinks {
        conversation_kind: Some(if parent_session_id.is_some() {
            "fork".to_string()
        } else {
            "main".to_string()
        }),
        thread_source: parent_session_id.as_ref().map(|_| "fork".to_string()),
        parent_session_id,
    }
}

pub(crate) fn parse_index_records(
    session_dir: &Path,
    state: IndexParseState,
    session_links: &HashMap<String, SessionLinks>,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    let session_id = session_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown")
        .to_string();
    let links = session_links
        .get(&session_id)
        .cloned()
        .unwrap_or_else(default_session_links);
    let mut messages = Vec::new();
    for entry in std::fs::read_dir(session_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let Ok(mut bytes) = std::fs::read(path) else {
            continue;
        };
        let Ok(message) = simd_json::to_borrowed_value(&mut bytes) else {
            continue;
        };
        let Some(message_id) = message
            .get("id")
            .and_then(|value| value.as_str())
            .filter(|id| !id.is_empty())
        else {
            continue;
        };
        let mut message_facts = RecordLinks {
            model: message
                .get("modelID")
                .or_else(|| message.get("model"))
                .and_then(|value| value.as_str())
                .map(str::to_string),
            ..RecordLinks::default()
        };
        if let Some(tokens) = message.get("tokens").and_then(|value| value.as_object()) {
            message_facts.input_tokens = super::common::borrowed_u64(tokens, "input");
            message_facts.output_tokens = super::common::borrowed_u64(tokens, "output");
            message_facts.reasoning_tokens = super::common::borrowed_u64(tokens, "reasoning");
            if let Some(cache) = tokens.get("cache").and_then(|value| value.as_object()) {
                message_facts.cache_read_tokens = super::common::borrowed_u64(cache, "read");
                message_facts.cache_write_tokens = super::common::borrowed_u64(cache, "write");
            }
        }
        messages.push((
            message_id.to_string(),
            message
                .get("time")
                .and_then(|value| value.get("created"))
                .and_then(|value| value.as_u64())
                .unwrap_or(0),
            message
                .get("role")
                .and_then(|value| value.as_str())
                .unwrap_or("user")
                .to_string(),
            message_facts,
        ));
    }
    messages.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
    let source_path = session_dir.to_string_lossy().to_string();
    let project = SourceKind::Opencode.label().to_string();
    let mut turn_id = state.turn_id;
    for (message_id, timestamp, role, message_facts) in messages {
        let message_ordinal = turn_id;
        let part_dir = parts_root_for_session(session_dir).join(&message_id);
        if !part_dir.exists() {
            continue;
        }
        let Ok(part_entries) = std::fs::read_dir(part_dir) else {
            continue;
        };
        let mut part_files = part_entries
            .flatten()
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        part_files.sort();
        let mut text_parts = Vec::new();
        let mut tool_call_index = 0u32;
        for path in part_files {
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let Ok(mut bytes) = std::fs::read(path) else {
                continue;
            };
            let Ok(part) = simd_json::to_borrowed_value(&mut bytes) else {
                continue;
            };
            if let Some(text) = part.get("text").and_then(|value| value.as_str()) {
                text_parts.push(text.to_string());
            }
            if part.get("type").and_then(|value| value.as_str()) != Some("tool") {
                continue;
            }
            let call_id = part
                .get("callID")
                .or_else(|| part.get("callId"))
                .or_else(|| part.get("id"))
                .and_then(|value| value.as_str())
                .map(str::to_string);
            let tool_name = part
                .get("tool")
                .or_else(|| part.get("toolName"))
                .and_then(|value| value.as_str())
                .map(str::to_string);
            let state = part.get("state");
            let tool_input = state.and_then(|value| value.get("input")).map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| value.to_string())
            });
            let source_status = state
                .and_then(|value| value.get("status"))
                .and_then(|value| value.as_str())
                .map(str::to_string);
            let tool_output = state.and_then(|value| value.get("output")).map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| value.to_string())
            });

            let mut call_links = links.record_links();
            call_links.message_ordinal = Some(message_ordinal);
            call_links.parent_event_id = Some(message_id.clone());
            call_links.event_id = call_id.clone();
            call_links.call_index = Some(tool_call_index);
            call_links.model = message_facts.model.clone();
            call_links.status =
                super::common::normalized_tool_status(source_status.as_deref(), None);
            call_links.source_status = source_status.clone();
            tool_call_index += 1;
            emit(Record {
                source: SourceKind::Opencode,
                record_key: String::new(),
                doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                ts: timestamp,
                project: project.clone(),
                session_id: session_id.clone(),
                turn_id,
                role: "tool_use".to_string(),
                text: tool_input.clone().unwrap_or_default(),
                tool_name: tool_name.clone(),
                tool_input,
                tool_output: None,
                links: call_links,
                source_path: source_path.clone(),
            })?;
            turn_id += 1;

            if tool_output.is_some() || source_status.is_some() {
                let tool_output = tool_output.unwrap_or_default();
                let mut result_links = links.record_links();
                result_links.message_ordinal = Some(message_ordinal);
                result_links.parent_tool_use_id = call_id.clone();
                result_links.parent_event_id = call_id;
                result_links.event_index = Some(0);
                result_links.status =
                    super::common::normalized_tool_status(source_status.as_deref(), None);
                result_links.source_status = source_status;
                result_links.model = message_facts.model.clone();
                emit(Record {
                    source: SourceKind::Opencode,
                    record_key: String::new(),
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts: timestamp,
                    project: project.clone(),
                    session_id: session_id.clone(),
                    turn_id,
                    role: "tool_result".to_string(),
                    text: tool_output.clone(),
                    tool_name,
                    tool_input: None,
                    tool_output: Some(tool_output),
                    links: result_links,
                    source_path: source_path.clone(),
                })?;
                turn_id += 1;
            }
        }
        if text_parts.is_empty() && tool_call_index == 0 {
            continue;
        }
        let mut record_links = links.record_links();
        record_links.event_id = Some(message_id);
        record_links.message_ordinal = Some(message_ordinal);
        record_links.model = message_facts.model;
        record_links.input_tokens = message_facts.input_tokens;
        record_links.cache_read_tokens = message_facts.cache_read_tokens;
        record_links.cache_write_tokens = message_facts.cache_write_tokens;
        record_links.output_tokens = message_facts.output_tokens;
        record_links.reasoning_tokens = message_facts.reasoning_tokens;
        emit(Record {
            source: SourceKind::Opencode,
            record_key: String::new(),
            doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
            ts: timestamp,
            project: project.clone(),
            session_id: session_id.clone(),
            turn_id,
            role,
            text: text_parts.join("\n"),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: record_links,
            source_path: source_path.clone(),
        })?;
        turn_id += 1;
    }
    Ok(IndexParseOutput {
        offset: 0,
        turn_id,
        pending_tool_calls: state.pending_tool_calls,
        parser_stream: state.parser_stream,
        session_id: Some(session_id),
        diagnostics: Default::default(),
    })
}

/// Databases precede message files so duplicate reconciliation retains the database copy,
/// matching OpenCode's pre-cache scan order.
pub fn usage_files() -> Vec<PathBuf> {
    let roots = data_roots();
    let mut files = Vec::new();
    for root in &roots {
        let mut databases = std::fs::read_dir(root)
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.extension().and_then(|value| value.to_str()) == Some("db")
                    && path
                        .file_name()
                        .and_then(|value| value.to_str())
                        .is_some_and(|name| name.starts_with("opencode"))
            })
            .collect::<Vec<_>>();
        databases.sort();
        files.extend(databases);
    }
    for root in &roots {
        let message_root = root.join("storage/message");
        if message_root.exists() {
            files.extend(
                WalkDir::new(message_root)
                    .into_iter()
                    .flatten()
                    .filter(|entry| {
                        entry.file_type().is_file()
                            && entry.path().extension().and_then(|value| value.to_str())
                                == Some("json")
                    })
                    .map(|entry| entry.path().to_path_buf()),
            );
        }
    }
    files
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<Vec<UsageEvent>> {
    if path.extension().and_then(|value| value.to_str()) == Some("db") {
        parse_usage_database(path)
    } else {
        parse_usage_message(path)
    }
}

fn parse_usage_message(path: &Path) -> Result<Vec<UsageEvent>> {
    let mut bytes = std::fs::read(path)?;
    let Ok(value) = simd_json::to_borrowed_value(&mut bytes) else {
        return Ok(Vec::new());
    };
    let id = borrowed_string(&value, &["id"]).or_else(|| {
        path.file_stem()
            .and_then(|name| name.to_str())
            .map(str::to_string)
    });
    Ok(usage_event(&value, path, id, None).into_iter().collect())
}

fn parse_usage_database(path: &Path) -> Result<Vec<UsageEvent>> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(Duration::from_secs(1))?;
    let mut statement = connection.prepare("SELECT id, session_id, data FROM message")?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let mut ids = HashSet::new();
    let mut events = Vec::new();
    for row in rows {
        let (id, session, data) = row?;
        if ids.contains(&id) {
            continue;
        }
        let mut bytes = data.into_bytes();
        let Ok(value) = simd_json::to_borrowed_value(&mut bytes) else {
            continue;
        };
        if let Some(mut event) = usage_event(&value, path, Some(id.clone()), session.as_deref()) {
            event.source_path = source_path.clone();
            ids.insert(id);
            events.push(event);
        }
    }
    Ok(events)
}

fn usage_event(
    value: &BorrowedValue<'_>,
    path: &Path,
    id: Option<String>,
    fallback_session: Option<&str>,
) -> Option<UsageEvent> {
    let usage = value.get("tokens")?;
    let number = |key: &str| usage.get(key).and_then(|value| value.as_u64()).unwrap_or(0);
    let reasoning = number("reasoning");
    let cache = usage.get("cache");
    let mut tokens = TokenBuckets::disjoint(
        number("input"),
        cache
            .and_then(|value| value.get("read"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0),
        cache
            .and_then(|value| value.get("write"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0),
        number("output").saturating_add(reasoning),
    );
    tokens.reasoning = reasoning;
    if tokens.additive_total() == 0 {
        return None;
    }
    Some(UsageEvent {
        source: "opencode",
        source_path: Arc::from(path.to_string_lossy()),
        source_record_id: id.clone(),
        session_id: borrowed_string(value, &["sessionID", "session_id"])
            .or_else(|| fallback_session.map(str::to_string)),
        request_id: None,
        message_id: id,
        timestamp_ms: value
            .get("time")
            .and_then(|value| value.get("created"))
            .map(timestamp_millis)
            .unwrap_or(0),
        project: Some(SourceKind::Opencode.label().to_string()),
        provider: borrowed_string(value, &["providerID", "provider"]),
        model: borrowed_string(value, &["modelID", "model"]),
        tokens,
        source_cost_usd: value.get("cost").and_then(|value| value.as_f64()),
        dedupe_confidence: "exact",
        conservative_undercount: false,
        sidechain: false,
        source_order: 0,
    })
}

fn borrowed_string(value: &BorrowedValue<'_>, aliases: &[&str]) -> Option<String> {
    aliases
        .iter()
        .find_map(|key| value.get(*key).and_then(|value| value.as_str()))
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn timestamp_millis(value: &BorrowedValue<'_>) -> u64 {
    value
        .as_u64()
        .map(|number| {
            if number < 10_000_000_000 {
                number.saturating_mul(1000)
            } else {
                number
            }
        })
        .or_else(|| {
            value
                .as_i64()
                .filter(|value| *value >= 0)
                .map(|value| value as u64)
        })
        .or_else(|| value.as_str().and_then(super::common::parse_iso_millis))
        .unwrap_or(0)
}

pub(crate) fn reconcile_usage(events: &mut Vec<UsageEvent>) {
    let mut seen = HashSet::new();
    events.retain(|event| {
        event.source != "opencode"
            || event
                .source_record_id
                .as_ref()
                .is_none_or(|record| seen.insert(record.clone()))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_projection_preserves_tool_parts_and_message_facts() {
        use crate::test_support::{EnvVarGuard, env_lock};

        let _guard = env_lock();
        let temp = tempfile::tempdir().unwrap();
        let storage = temp.path().join("storage");
        let session_dir = storage.join("message/ses_test");
        let part_dir = storage.join("part/msg_test");
        std::fs::create_dir_all(&session_dir).unwrap();
        std::fs::create_dir_all(&part_dir).unwrap();
        std::fs::write(
            session_dir.join("msg_test.json"),
            r#"{"id":"msg_test","role":"assistant","modelID":"opencode-test","tokens":{"input":15,"output":4,"reasoning":2,"cache":{"read":5,"write":1}},"time":{"created":10}}"#,
        )
        .unwrap();
        std::fs::write(
            part_dir.join("01-tool.json"),
            r#"{"id":"part-tool","type":"tool","callID":"call-1","tool":"Read","state":{"status":"completed","input":{"file_path":"README.md"},"output":"done"}}"#,
        )
        .unwrap();
        let _env = EnvVarGuard::set_os(&[("OPENCODE_DATA_DIR", Some(temp.path().as_os_str()))]);

        let mut records = Vec::new();
        parse_index_records(
            &session_dir,
            IndexParseState::default(),
            &HashMap::new(),
            &AtomicU64::new(1),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].role, "tool_use");
        assert_eq!(records[1].role, "tool_result");
        assert_eq!(records[2].role, "assistant");
        assert_eq!(
            records[0].links.message_ordinal,
            records[2].links.message_ordinal
        );
        assert_eq!(records[0].links.call_index, Some(0));
        assert_eq!(records[0].links.status.as_deref(), Some("success"));
        assert_eq!(records[0].links.source_status.as_deref(), Some("completed"));
        assert_eq!(records[1].links.status.as_deref(), Some("success"));
        assert!(records[2].text.is_empty());
        assert_eq!(records[2].links.input_tokens, Some(15));
        assert_eq!(records[2].links.cache_read_tokens, Some(5));
        assert_eq!(records[2].links.cache_write_tokens, Some(1));
        assert_eq!(records[2].links.output_tokens, Some(4));
        assert_eq!(records[2].links.reasoning_tokens, Some(2));
        assert_eq!(records[1].links.event_index, Some(0));
        assert_eq!(records[2].links.model.as_deref(), Some("opencode-test"));
    }

    #[test]
    fn session_fingerprint_changes_when_only_a_tool_part_changes() {
        let temp = tempfile::tempdir().unwrap();
        let storage = temp.path().join("storage");
        let session_dir = storage.join("message/ses_test");
        let part_dir = storage.join("part/msg_test");
        std::fs::create_dir_all(&session_dir).unwrap();
        std::fs::create_dir_all(&part_dir).unwrap();
        std::fs::write(
            session_dir.join("msg_test.json"),
            r#"{"id":"msg_test","role":"assistant","time":{"created":10}}"#,
        )
        .unwrap();
        let part = part_dir.join("01-tool.json");
        std::fs::write(
            &part,
            r#"{"type":"tool","callID":"call-1","state":{"status":"running"}}"#,
        )
        .unwrap();
        let before = session_dependency_fingerprint(&session_dir).unwrap();

        std::fs::write(
            &part,
            r#"{"type":"tool","callID":"call-1","state":{"status":"completed","output":"done"}}"#,
        )
        .unwrap();
        let after = session_dependency_fingerprint(&session_dir).unwrap();

        assert_ne!(before.3, after.3);
        assert!(after.0 > before.0);
    }

    #[test]
    fn reasoning_is_included_in_output_and_total() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("message.json");
        std::fs::write(
            &path,
            r#"{
                "id": "message",
                "tokens": {
                    "input": 100,
                    "output": 20,
                    "reasoning": 30,
                    "cache": { "read": 40, "write": 10 }
                }
            }"#,
        )
        .unwrap();

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tokens.reasoning, 30);
        assert_eq!(events[0].tokens.output, 50);
        assert_eq!(events[0].tokens.total(), 200);
        assert_eq!(events[0].project.as_deref(), Some("opencode"));
    }
}
