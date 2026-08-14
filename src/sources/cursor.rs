use super::{IndexParseOutput, IndexParseState, ParserVersions, SourceFile};
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::Result;
use memchr::memchr;
use memmap2::Mmap;
use rusqlite::{Connection, OpenFlags};
use simd_json::BorrowedValue;
use simd_json::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use walkdir::WalkDir;

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 2,
    index: 2,
    usage: 3,
};

pub fn matches_path(path: &str) -> bool {
    path.contains(".cursor/projects")
        || path.contains(".cursor\\projects")
        || path.contains("agent-transcripts")
}

pub fn projects_root() -> PathBuf {
    super::common::home().join(".cursor/projects")
}

pub fn discover_transcripts() -> Vec<SourceFile> {
    let mut files = WalkDir::new(projects_root())
        .into_iter()
        .flatten()
        .filter(|entry| {
            entry.file_type().is_file()
                && entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl")
                && entry.path().to_str().is_some_and(|path| {
                    path.contains("/agent-transcripts/") || path.contains("\\agent-transcripts\\")
                })
        })
        .map(|entry| SourceFile {
            source: SourceKind::Cursor,
            path: entry.path().to_path_buf(),
        })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.path.cmp(&right.path));
    files
}

pub fn transcript_id(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

pub fn session_id_from_path(path: &Path) -> String {
    let components = path.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        if component.as_os_str().to_str() == Some("agent-transcripts")
            && let Some(session) = components
                .get(index + 1)
                .and_then(|component| Path::new(component.as_os_str()).file_stem())
                .and_then(|stem| stem.to_str())
                .filter(|stem| !stem.is_empty())
        {
            return session.to_string();
        }
    }
    crate::sources::codex::session_id_from_path(path).unwrap_or_else(|| transcript_id(path))
}

pub fn project_from_path(path: &Path) -> String {
    let Ok(relative) = path.strip_prefix(projects_root()) else {
        return SourceKind::Cursor.label().to_string();
    };
    relative
        .components()
        .next()
        .and_then(|component| component.as_os_str().to_str())
        .map(|folder| {
            if folder == "empty-window" {
                folder.to_string()
            } else {
                folder
                    .trim_matches('-')
                    .rsplit('-')
                    .find(|part| !part.is_empty())
                    .unwrap_or(folder)
                    .to_string()
            }
        })
        .unwrap_or_else(|| SourceKind::Cursor.label().to_string())
}

const SUBAGENT_TURN_BASE: u32 = 1_000_000_000;
const SUBAGENT_TURN_STRIDE: u32 = 50_000;
const SUBAGENT_TURN_BUCKETS: u32 = 65_536;

pub(crate) fn parse_index_records(
    path: &Path,
    mtime: i64,
    state: IndexParseState,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut start = state.offset as usize;
    let mut turn_id = initial_turn_id(path, state.turn_id);
    let mut pending_tool_calls = state.pending_tool_calls;
    let source_path = path.to_string_lossy().to_string();
    let session_id = session_id_from_path(path);
    let project = project_from_path(path);
    let timestamp = mtime.max(0) as u64 * 1000;
    let mut buffer = Vec::new();
    while start < mmap.len() {
        let slice = &mmap[start..];
        let relative = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..relative];
        start += relative + usize::from(relative < slice.len());
        if line.is_empty() {
            continue;
        }
        buffer.clear();
        buffer.extend_from_slice(line);
        let Ok(value) = simd_json::to_borrowed_value(&mut buffer) else {
            continue;
        };
        let Some(object) = value.as_object() else {
            continue;
        };
        let role = object
            .get("role")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if role != "user" && role != "assistant" {
            continue;
        }
        let Some(message) = object.get("message").and_then(|value| value.as_object()) else {
            continue;
        };
        let mut text_parts = Vec::new();
        if let Some(content) = message.get("content") {
            if let Some(text) = content.as_str() {
                text_parts.push(text);
            } else if let Some(array) = content.as_array() {
                for block in array {
                    let Some(block_object) = block.as_object() else {
                        continue;
                    };
                    match block_object
                        .get("type")
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                    {
                        "text" => {
                            if let Some(text) =
                                block_object.get("text").and_then(|value| value.as_str())
                            {
                                text_parts.push(text);
                            }
                        }
                        "tool_use" => {
                            let tool_name = super::common::borrowed_string(block_object, "name");
                            let tool_input = block_object.get("input").map(ToString::to_string);
                            let tool_id = ["id", "tool_use_id", "toolCallId"]
                                .iter()
                                .find_map(|key| {
                                    block_object.get(*key).and_then(|value| value.as_str())
                                })
                                .map(str::to_string);
                            let mut links = record_links(path, &session_id, turn_id);
                            links.event_id = tool_id.clone();
                            let doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
                            if let Some(tool_id) = tool_id {
                                pending_tool_calls.insert(
                                    tool_id.clone(),
                                    super::common::pending_tool_call(
                                        tool_name.clone(),
                                        Some(tool_id),
                                        doc_id,
                                        timestamp,
                                        tool_input.as_deref(),
                                        &links,
                                        &session_id,
                                    ),
                                );
                            }
                            emit(Record {
                                source: SourceKind::Cursor,
                                doc_id,
                                ts: timestamp,
                                project: project.clone(),
                                session_id: session_id.clone(),
                                turn_id,
                                role: "tool_use".to_string(),
                                text: tool_input.clone().unwrap_or_default(),
                                tool_name,
                                tool_input,
                                tool_output: None,
                                links,
                                source_path: source_path.clone(),
                            })?;
                            turn_id += 1;
                        }
                        "tool_result" => {
                            let tool_output = block_object.get("content").map(ToString::to_string);
                            let mut text =
                                super::common::tool_result_text(block).unwrap_or_default();
                            if text.is_empty() {
                                text = tool_output.clone().unwrap_or_default();
                            }
                            if text.is_empty() {
                                continue;
                            }
                            let tool_use_id = ["tool_use_id", "toolCallId"]
                                .iter()
                                .find_map(|key| {
                                    block_object.get(*key).and_then(|value| value.as_str())
                                })
                                .map(str::to_string);
                            let pending = tool_use_id
                                .as_ref()
                                .and_then(|id| pending_tool_calls.remove(id));
                            let tool_name = super::common::borrowed_string(block_object, "name")
                                .or_else(|| pending.and_then(|call| call.tool_name));
                            let mut links = record_links(path, &session_id, turn_id);
                            if let Some(tool_use_id) = tool_use_id {
                                links.parent_event_id = Some(tool_use_id.clone());
                                links.parent_tool_use_id = Some(tool_use_id);
                            }
                            emit(Record {
                                source: SourceKind::Cursor,
                                doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                                ts: timestamp,
                                project: project.clone(),
                                session_id: session_id.clone(),
                                turn_id,
                                role: "tool_result".to_string(),
                                text,
                                tool_name,
                                tool_input: None,
                                tool_output,
                                links,
                                source_path: source_path.clone(),
                            })?;
                            turn_id += 1;
                        }
                        _ => {}
                    }
                }
            }
        }
        let text = text_parts.join("\n").trim().to_string();
        if !text.is_empty() {
            emit(Record {
                source: SourceKind::Cursor,
                doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                ts: timestamp,
                project: project.clone(),
                session_id: session_id.clone(),
                turn_id,
                role: role.to_string(),
                text,
                tool_name: None,
                tool_input: None,
                tool_output: None,
                links: record_links(path, &session_id, turn_id),
                source_path: source_path.clone(),
            })?;
            turn_id += 1;
        }
    }
    Ok(IndexParseOutput {
        offset: mmap.len() as u64,
        turn_id,
        pending_tool_calls,
        session_id: Some(session_id),
        diagnostics: Default::default(),
    })
}

pub(crate) fn initial_turn_id(path: &Path, cached_turn_id: u32) -> u32 {
    if cached_turn_id != 0 {
        return cached_turn_id;
    }
    subagent_turn_base(path).unwrap_or(cached_turn_id)
}

fn subagent_turn_base(path: &Path) -> Option<u32> {
    if !is_subagent_transcript(path) {
        return None;
    }
    let agent_id = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|id| !id.is_empty())?;
    Some(SUBAGENT_TURN_BASE + stable_turn_bucket(agent_id).saturating_mul(SUBAGENT_TURN_STRIDE))
}

fn is_subagent_transcript(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str().to_str() == Some("subagents"))
}

pub(crate) fn record_links(path: &Path, session_id: &str, turn_id: u32) -> RecordLinks {
    let is_subagent = is_subagent_transcript(path);
    RecordLinks {
        event_id: Some(format!("{}:{turn_id}", transcript_id(path))),
        parent_session_id: is_subagent.then(|| session_id.to_string()),
        thread_source: is_subagent.then(|| "subagent".to_string()),
        conversation_kind: Some(if is_subagent { "subagent" } else { "main" }.to_string()),
        ..RecordLinks::default()
    }
}

fn stable_turn_bucket(value: &str) -> u32 {
    let mut hash = 2_166_136_261u32;
    for byte in value.as_bytes() {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(16_777_619);
    }
    hash % SUBAGENT_TURN_BUCKETS
}

pub fn usage_databases() -> Vec<PathBuf> {
    let user = if cfg!(target_os = "macos") {
        super::common::home().join("Library/Application Support/Cursor/User")
    } else {
        super::common::home().join(".config/Cursor/User")
    };
    let mut databases = vec![user.join("globalStorage/state.vscdb")];
    databases.extend(
        WalkDir::new(user.join("workspaceStorage"))
            .max_depth(3)
            .into_iter()
            .flatten()
            .filter(|entry| entry.file_type().is_file() && entry.file_name() == "state.vscdb")
            .map(|entry| entry.path().to_path_buf()),
    );
    databases.retain(|path| path.exists());
    databases
}

pub(crate) fn project_by_session() -> HashMap<String, String> {
    let mut projects = HashMap::new();
    for entry in WalkDir::new(projects_root())
        .into_iter()
        .flatten()
        .filter(|entry| {
            entry.file_type().is_file()
                && entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl")
        })
    {
        let path = entry.path();
        let project = project_from_path(path);
        projects.insert(session_id_from_path(path), project.clone());
        projects.insert(transcript_id(path), project);
    }
    projects
}

pub(crate) fn apply_projects(
    events: &mut [UsageEvent],
    project_by_session: &HashMap<String, String>,
) {
    for event in events {
        event.project = event
            .session_id
            .as_deref()
            .and_then(|session| project_by_session.get(session))
            .cloned();
    }
}

pub(crate) fn parse_usage_database(path: &Path) -> Result<Vec<UsageEvent>> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(Duration::from_secs(1))?;
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let mut events = Vec::new();
    let mut seen = HashSet::new();
    for (table, query) in [
        (
            "cursorDiskKV",
            "SELECT key, value FROM cursorDiskKV WHERE key LIKE 'composerData:%' OR key LIKE 'bubbleId:%'",
        ),
        (
            "ItemTable",
            "SELECT key, value FROM ItemTable WHERE key IN ('aiService.generations', 'workbench.panel.aichat.view.aichat.chatdata')",
        ),
    ] {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
            [table],
            |row| row.get(0),
        )?;
        if !exists {
            continue;
        }
        let mut statement = connection.prepare(query)?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })?;
        for row in rows {
            let (key, Some(raw)) = row? else {
                continue;
            };
            let mut bytes = raw.into_bytes();
            let Ok(value) = simd_json::to_borrowed_value(&mut bytes) else {
                continue;
            };
            extract_usage(&value, &key, table, &source_path, &mut events, &mut seen);
        }
    }
    Ok(events)
}

fn extract_usage(
    value: &BorrowedValue<'_>,
    fallback_id: &str,
    table: &str,
    source_path: &Arc<str>,
    events: &mut Vec<UsageEvent>,
    seen: &mut HashSet<String>,
) {
    if let Some(array) = value.as_array() {
        for child in array {
            extract_usage(child, fallback_id, table, source_path, events, seen);
        }
        return;
    }
    let Some(object) = value.as_object() else {
        return;
    };
    let input = nested_u64(value, &["inputTokens", "input_tokens"]);
    let output = nested_u64(value, &["outputTokens", "output_tokens"]);
    let session_id = borrowed_string(value, &["sessionId", "composerId"]).or_else(|| {
        fallback_id
            .strip_prefix("composerData:")
            .filter(|id| !id.is_empty())
            .map(str::to_string)
    });
    let counted = input > 0 || output > 0;
    if counted {
        let id = borrowed_string(value, &["generationUUID", "generationId", "bubbleId", "id"])
            .unwrap_or_else(|| {
                let created = object
                    .get("createdAt")
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "null".to_string());
                format!("{fallback_id}:{created}:{}", input + output)
            });
        let dedupe = format!("{id}:{input}:{output}");
        if seen.insert(dedupe) {
            events.push(UsageEvent {
                source: "cursor",
                source_path: source_path.clone(),
                source_record_id: Some(id),
                session_id,
                request_id: None,
                message_id: None,
                timestamp_ms: object
                    .get("createdAt")
                    .or_else(|| object.get("timestamp"))
                    .map(timestamp_millis)
                    .unwrap_or(0),
                project: None,
                provider: None,
                model: borrowed_string(value, &["model", "modelName"])
                    .or_else(|| {
                        object
                            .get("modelInfo")
                            .and_then(|value| borrowed_string(value, &["modelName"]))
                    })
                    .or_else(|| {
                        object
                            .get("modelConfig")
                            .and_then(|value| borrowed_string(value, &["modelName"]))
                    }),
                tokens: TokenBuckets::disjoint(input, 0, 0, output),
                source_cost_usd: None,
                cost_authoritative: false,
                dedupe_confidence: if table == "cursorDiskKV" {
                    "exact"
                } else {
                    "strong"
                },
                conservative_undercount: false,
                cache_chain_excluded: false,
                sidechain: false,
                source_order: 0,
            });
        }
    }
    for (key, child) in object {
        if counted && matches!(key.as_ref(), "usage" | "tokenCount") {
            continue;
        }
        if child.is_array() || child.is_object() {
            extract_usage(child, fallback_id, table, source_path, events, seen);
        }
    }
}

fn nested_u64(value: &BorrowedValue<'_>, aliases: &[&str]) -> u64 {
    aliases
        .iter()
        .find_map(|key| value.get(*key).and_then(|value| value.as_u64()))
        .or_else(|| {
            value.get("tokenCount").and_then(|nested| {
                aliases
                    .iter()
                    .find_map(|key| nested.get(*key).and_then(|value| value.as_u64()))
            })
        })
        .or_else(|| {
            value.get("usage").and_then(|nested| {
                aliases
                    .iter()
                    .find_map(|key| nested.get(*key).and_then(|value| value.as_u64()))
            })
        })
        .unwrap_or(0)
}

fn borrowed_string(value: &BorrowedValue<'_>, aliases: &[&str]) -> Option<String> {
    aliases
        .iter()
        .find_map(|key| value.get(*key).and_then(|value| value.as_str()))
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
        event.source != "cursor"
            || event.source_record_id.as_ref().is_none_or(|record| {
                seen.insert((record.clone(), event.tokens.raw_input, event.tokens.output))
            })
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_token_containers_are_not_recounted() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.vscdb");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                r#"CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY, value TEXT);
                   INSERT INTO cursorDiskKV VALUES (
                     'composerData:composer-main',
                     '[{"generationUUID":"usage-parent","composerId":"composer-main","usage":{"inputTokens":10,"outputTokens":5},"children":[{"generationId":"nested-request","inputTokens":7,"outputTokens":3}]},{"generationUUID":"count-parent","tokenCount":{"input_tokens":20,"output_tokens":8}}]'
                   );"#,
            )
            .unwrap();
        drop(connection);

        let mut events = parse_usage_database(&path).unwrap();
        apply_projects(
            &mut events,
            &HashMap::from([("composer-main".to_string(), "memex".to_string())]),
        );

        assert_eq!(events.len(), 3);
        let ids = events
            .iter()
            .filter_map(|event| event.source_record_id.as_deref())
            .collect::<HashSet<_>>();
        assert_eq!(
            ids,
            HashSet::from(["usage-parent", "nested-request", "count-parent"])
        );
        assert_eq!(
            events.iter().map(|event| event.tokens.total()).sum::<u64>(),
            53
        );
        assert_eq!(events[0].project.as_deref(), Some("memex"));
    }
}
