use super::{
    ConversationKind, IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions,
    SessionIdentity, SourceFile, SourceMetadata,
};
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::{Context, Result};
use memchr::memmem;
use once_cell::sync::Lazy;
use serde_json::Value;
use simd_json::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 2,
    // Preserve pre-reader fallback IDs independently of attachment-only messages,
    // and classify background transcripts consistently across every record.
    index: 8,
    // Reconcile content blocks and divergent workflow copies as one logical agent.
    usage: 6,
};

/// Return the human-facing title Claude stores alongside a conversation.
///
/// Title records are metadata rather than transcript messages, so the search
/// index intentionally does not contain them. The shared session catalog reads
/// this metadata so every client sees saved names and subsequent renames.
pub fn session_title(path: &Path, session_id: &str) -> Option<String> {
    use memchr::memchr;
    use memmap2::Mmap;
    let file = File::open(path).ok()?;
    let bytes = unsafe { Mmap::map(&file).ok()? };
    let mut custom_title = None;
    let mut ai_title = None;
    let mut agent_name = None;

    let mut offset = 0;
    while offset < bytes.len() {
        let remaining = &bytes[offset..];
        let length = memchr(b'\n', remaining).unwrap_or(remaining.len());
        let line = &remaining[..length];
        offset += length + usize::from(length < remaining.len());
        // Most lines contain large transcript/tool bodies. Only metadata
        // candidates need JSON decoding; still inspect every line for renames.
        // Escaped JSON strings may encode the type using Unicode escapes.
        if ![
            b"custom-title".as_slice(),
            b"ai-title",
            b"agent-name",
            b"\\u",
        ]
        .iter()
        .any(|needle| memmem::find(line, needle).is_some())
        {
            continue;
        }
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        if value
            .get("sessionId")
            .and_then(Value::as_str)
            .is_some_and(|id| id != session_id)
        {
            continue;
        }
        let Some(entry_type) = value.get("type").and_then(Value::as_str) else {
            continue;
        };
        let title = match entry_type {
            "custom-title" => value
                .get("customTitle")
                .or_else(|| value.get("title"))
                .and_then(Value::as_str),
            "ai-title" => value.get("aiTitle").and_then(Value::as_str),
            "agent-name" => value
                .get("agentName")
                .or_else(|| value.get("name"))
                .and_then(Value::as_str),
            _ => None,
        }
        .map(crate::analytics::sanitize_label)
        .filter(|title| !title.is_empty());

        match entry_type {
            "custom-title" if title.is_some() => custom_title = title,
            "ai-title" if title.is_some() => ai_title = title,
            "agent-name" if title.is_some() => agent_name = title,
            _ => {}
        }
    }

    custom_title.or(ai_title).or(agent_name)
}

pub fn discover(
    root: &Path,
    _include_agents: bool,
    walk: Option<&mut crate::ingest::directories::StampedWalk>,
) -> Result<Vec<SourceFile>> {
    // `_include_agents` is a retired opt-in kept only for CLI compatibility:
    // agent transcripts are always indexed now, matching every other source.
    // Consumers hide them from default views via `conversation_kind`.
    let mut files = Vec::new();
    for path in super::common::files_under(root, walk) {
        if path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
            continue;
        }
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        let is_agent = name.starts_with("agent-");
        let under_subagents = path.ancestors().any(|ancestor| {
            ancestor.file_name().and_then(|name| name.to_str()) == Some("subagents")
        });
        if under_subagents && !is_agent {
            // Workflow journals and other non-transcript files.
            continue;
        }
        // Standard sessions live at most two levels below root
        // (`<project>/<session>.jsonl`); only agent transcripts nest
        // deeper, under a `subagents/` directory (see `is_subagent_path`).
        // Anything deeper outside `subagents/` is not a session file.
        let relative_depth = path
            .strip_prefix(root)
            .map(|path| path.components().count())
            .unwrap_or(0);
        if relative_depth > 2 && !under_subagents {
            continue;
        }
        files.push(SourceFile {
            source: SourceKind::Claude,
            path,
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

pub fn usage_files(roots: &[PathBuf]) -> Vec<PathBuf> {
    // Copies can contain unique history. Read them all and reconcile shared responses.
    super::common::jsonl_files(roots.iter().cloned())
}

/// A discovered agent copy and the byte boundary captured for this refresh.
#[derive(Clone, Debug)]
pub(crate) struct AgentCopy {
    pub path: PathBuf,
    pub size: u64,
    pub metadata_sha256: String,
}

pub fn session_id_from_path(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

pub fn project_from_path(path: &Path) -> String {
    path.parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .map(decode_project_name)
        .unwrap_or_else(|| SourceKind::Claude.label().to_string())
}

fn decode_project_name(folder_name: &str) -> String {
    let prefixes_to_strip = ["-home-", "-mnt-c-Users-", "-mnt-c-users-", "-Users-"];
    let mut name = folder_name;
    if name.len() > 10 {
        let bytes = name.as_bytes();
        if bytes[0] == b'-'
            && bytes[2] == b'-'
            && bytes[3] == b'-'
            && bytes[1].is_ascii_alphabetic()
            && name[4..].to_lowercase().starts_with("users-")
        {
            name = &name[10..];
        }
    }
    for prefix in prefixes_to_strip {
        if name.to_lowercase().starts_with(&prefix.to_lowercase()) {
            name = &name[prefix.len()..];
            break;
        }
    }
    let parts: Vec<&str> = name.split('-').filter(|part| !part.is_empty()).collect();
    let skip_dirs = [
        "projects",
        "code",
        "repos",
        "src",
        "dev",
        "work",
        "documents",
    ];
    let mut meaningful = Vec::new();
    let mut found_project = false;
    for (index, part) in parts.iter().enumerate() {
        if index == 0 && !found_project {
            let remaining: Vec<String> = parts[index + 1..]
                .iter()
                .map(|part| part.to_lowercase())
                .collect();
            if remaining
                .iter()
                .any(|directory| skip_dirs.contains(&directory.as_str()))
            {
                continue;
            }
        }
        if skip_dirs.contains(&part.to_lowercase().as_str()) {
            found_project = true;
            continue;
        }
        meaningful.push(*part);
        found_project = true;
    }
    if meaningful.is_empty() {
        folder_name.to_string()
    } else {
        meaningful.join("-")
    }
}

/// Index of the `projects` component that roots a Claude transcript path. Encoded cwd
/// directories start with `-`, so the last component named `projects` is the root's.
fn projects_component(path: &Path) -> Option<usize> {
    path.components()
        .enumerate()
        .filter(|(_, component)| component.as_os_str() == "projects")
        .last()
        .map(|(index, _)| index)
}

/// The transcript's path below its `projects` directory: `<encoded cwd>/<session>.jsonl`,
/// or deeper for agent transcripts. It names the same conversation under any config root.
pub(crate) fn project_relative_path(path: &Path) -> Option<PathBuf> {
    let projects = projects_component(path)?;
    let relative: PathBuf = path.components().skip(projects + 1).collect();
    (relative.components().count() >= 2).then_some(relative)
}

/// Name of the Claude config directory a transcript was read from, such as `work` for
/// `~/.claude-profiles/work/projects/...` or `.claude` for `~/.claude/projects/...`.
pub fn profile_from_path(path: &Path) -> Option<String> {
    let projects = projects_component(path)?;
    let config_dir = path.components().nth(projects.checked_sub(1)?)?;
    match config_dir {
        std::path::Component::Normal(name) => Some(name.to_string_lossy().into_owned()),
        _ => None,
    }
}

/// The `subagents` directory and file name shared by every copy of an agent transcript.
/// Workflow runs store each agent twice, as `subagents/agent-<id>.jsonl` and as
/// `subagents/workflows/<run>/agent-<id>.jsonl`.
pub(crate) fn agent_copy_key(path: &Path) -> Option<(PathBuf, std::ffi::OsString)> {
    let name = path.file_name()?;
    if !name.to_str()?.starts_with("agent-") {
        return None;
    }
    let parent = path.parent()?;
    let subagents = if parent.file_name()? == "subagents" {
        parent
    } else {
        let workflows = parent.parent()?;
        let subagents = workflows.parent()?;
        (workflows.file_name()? == "workflows" && subagents.file_name()? == "subagents")
            .then_some(subagents)?
    };
    Some((subagents.to_path_buf(), name.to_os_string()))
}

/// The workflow run directories under a `subagents` directory.
pub(crate) fn workflow_runs(subagents: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(subagents.join("workflows"))
        .map(|runs| runs.flatten().map(|run| run.path()).collect())
        .unwrap_or_default()
}

/// Every copy of one agent transcript that exists on disk.
pub(crate) fn agent_copies(
    subagents: &Path,
    runs: &[PathBuf],
    name: &std::ffi::OsStr,
) -> Vec<PathBuf> {
    let mut copies = vec![subagents.join(name)];
    copies.extend(runs.iter().map(|run| run.join(name)));
    copies.retain(|path| path.is_file());
    copies
}

/// Map each duplicate to the path that owns its merged index projection. Usage retains the
/// physical copies so its file cache observes changes to every part of the logical agent.
pub(crate) fn agent_copy_owners(
    paths: impl IntoIterator<Item = PathBuf>,
) -> std::collections::HashMap<PathBuf, PathBuf> {
    let mut groups = std::collections::HashMap::<_, Vec<PathBuf>>::new();
    for path in paths {
        if let Some(key) = agent_copy_key(&path) {
            groups.entry(key).or_default().push(path);
        }
    }
    let mut owners = std::collections::HashMap::new();
    for mut copies in groups.into_values() {
        copies.sort();
        copies.dedup();
        if copies.len() < 2 {
            continue;
        }
        let owner = preferred_agent_copy(&copies)
            .expect("a duplicated agent has copies")
            .clone();
        owners.extend(copies.into_iter().map(|copy| (copy, owner.clone())));
    }
    owners
}

/// The path that owns an agent's merged index projection. The workflow copy is written as
/// the agent runs; the direct copy appears later, may hold only the tail, and gains resumed
/// records. Prefer the newest last record, then file size, then the direct `subagents/` copy.
/// Timestamps count only when every copy's last record is readable. This selects a source
/// path, never a reason to discard unique events from another copy.
pub(crate) fn preferred_agent_copy(copies: &[PathBuf]) -> Option<&PathBuf> {
    let timestamps = copies
        .iter()
        .map(|path| last_record_timestamp(path))
        .collect::<Option<Vec<_>>>();
    copies
        .iter()
        .enumerate()
        .max_by_key(|(index, path)| {
            let size = std::fs::metadata(path).map_or(0, |metadata| metadata.len());
            (
                timestamps
                    .as_ref()
                    .map(|timestamps| timestamps[*index].clone()),
                size,
                std::cmp::Reverse(path.components().count()),
            )
        })
        .map(|(_, path)| path)
}

/// The `timestamp` of the last record in a transcript's final 256 KiB. Most last lines are a
/// few KiB, so a short tail is tried first.
fn last_record_timestamp(path: &Path) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct Stamped {
        timestamp: Option<String>,
    }
    let mut file = File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    for tail_bytes in [16 * 1024, 256 * 1024] {
        let start = len.saturating_sub(tail_bytes);
        file.seek(SeekFrom::Start(start)).ok()?;
        let mut tail = Vec::new();
        file.read_to_end(&mut tail).ok()?;
        // Unless the tail is the whole file, its first line began before it.
        let complete = match tail.iter().position(|byte| *byte == b'\n') {
            Some(newline) if start > 0 => &tail[newline + 1..],
            None if start > 0 => &[],
            _ => &tail[..],
        };
        let timestamp = complete.rsplit(|byte| *byte == b'\n').find_map(|line| {
            serde_json::from_slice::<Stamped>(line)
                .ok()
                .and_then(|record| record.timestamp)
        });
        if timestamp.is_some() || start == 0 {
            return timestamp;
        }
    }
    None
}

pub fn is_subagent_path(path: &Path) -> bool {
    session_id_from_path(path).starts_with("agent-")
        || path
            .components()
            .any(|component| component.as_os_str().to_str() == Some("subagents"))
}

pub fn classify(path: &Path, is_sidechain: bool) -> ConversationKind {
    classify_with_session_kind(path, is_sidechain, false)
}

fn classify_with_session_kind(
    path: &Path,
    is_sidechain: bool,
    is_background: bool,
) -> ConversationKind {
    // Claude Code's `bg` transcripts are background workers rather than an
    // interactive conversation. Reuse the existing subagent origin so all
    // session views consistently keep them out of the interactive default.
    if is_background || is_subagent_path(path) {
        ConversationKind::Subagent
    } else if is_sidechain {
        ConversationKind::Sidechain
    } else {
        ConversationKind::Main
    }
}

fn line_has_background_session_kind(line: &[u8]) -> bool {
    serde_json::from_slice::<Value>(line)
        .ok()
        .is_some_and(|value| value.get("sessionKind").and_then(Value::as_str) == Some("bg"))
}

/// Detect a background marker in the newly appended portion of a transcript.
/// Callers persist the result, so normal incremental ingests never revisit
/// historical lines.
pub(crate) fn has_background_session_kind_since(
    path: &Path,
    offset: u64,
    byte_boundary: u64,
) -> Result<bool> {
    let mut file = File::open(path)?;
    let end = byte_boundary.min(file.metadata()?.len());
    let start = offset.min(end);
    file.seek(SeekFrom::Start(start))?;
    let reader = BufReader::new(file.take(end - start));
    for line in reader.split(b'\n') {
        if line_has_background_session_kind(&line?) {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn probe(path: &Path) -> Result<SourceMetadata> {
    let fallback_id = session_id_from_path(path);
    let mut session_id = fallback_id;
    let mut parent_session_id = None;
    let mut cwd = None;
    let mut project = Some(project_from_path(path));
    let mut is_background = false;
    let mut conversation_kind = classify_with_session_kind(path, false, is_background);

    let reader = BufReader::new(File::open(path)?);
    for line in reader.lines().take(64) {
        let Ok(line) = line else { continue };
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        is_background |= value.get("sessionKind").and_then(Value::as_str) == Some("bg");
        if let Some(id) = value.get("sessionId").and_then(Value::as_str) {
            session_id = id.to_string();
        }
        if let Some(value_cwd) = value.get("cwd").and_then(Value::as_str) {
            cwd = Some(PathBuf::from(value_cwd));
            project = Some(super::common::project_from_path(value_cwd));
        }
        let sidechain = value
            .get("isSidechain")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        conversation_kind = classify_with_session_kind(path, sidechain, is_background);
        if is_subagent_path(path) {
            parent_session_id = value
                .get("sessionId")
                .and_then(Value::as_str)
                .map(str::to_string);
        }
        if cwd.is_some() && value.get("sessionId").is_some() {
            break;
        }
    }

    Ok(SourceMetadata {
        session: SessionIdentity {
            source: SourceKind::Claude,
            session_id,
            parent_session_id,
            conversation_kind,
            source_path: path.to_path_buf(),
        },
        cwd,
        project,
        git_branch: None,
    })
}

#[cfg(test)]
pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    parse_index_records_with_background(
        path,
        state,
        include_reasoning,
        None,
        File::open(path)?.metadata()?.len(),
        next_doc_id,
        emit,
    )
    .map(|(output, _)| output)
}

pub(crate) fn parse_index_records_with_background(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    known_background: Option<bool>,
    byte_boundary: u64,
    next_doc_id: &AtomicU64,
    emit: impl FnMut(Record) -> Result<()>,
) -> Result<(IndexParseOutput, bool)> {
    let file = File::open(path)?;
    let mmap = super::common::map_sequential(&file)?;
    parse_index_bytes(
        path,
        bounded_transcript_bytes(&mmap, byte_boundary),
        state,
        include_reasoning,
        known_background,
        next_doc_id,
        emit,
    )
}

/// A growing file's captured boundary may end inside a line. Leave that line for the next
/// refresh, even if its prefix happens to be valid JSON before the writer finishes it.
fn bounded_transcript_bytes(bytes: &[u8], byte_boundary: u64) -> &[u8] {
    let end = byte_boundary.min(bytes.len() as u64) as usize;
    let captured = &bytes[..end];
    if end < bytes.len() && !captured.ends_with(b"\n") {
        &captured[..captured
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |end| end + 1)]
    } else {
        captured
    }
}

/// Read every copy at its discovered boundary, deduplicate shared events, and parse the
/// chronological union under one source path. The source transcripts are never rewritten.
pub(crate) fn parse_agent_records(
    owner: &Path,
    copies: &[AgentCopy],
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    emit: impl FnMut(Record) -> Result<()>,
) -> Result<(IndexParseOutput, bool)> {
    use std::collections::{HashMap, HashSet};

    let mut lines = Vec::new();
    let mut seen = HashSet::new();
    // Keep the owner's representation when copies differ in metadata for the same event.
    let mut copies = copies.iter().collect::<Vec<_>>();
    copies.sort_by_key(|copy| (copy.path != owner, &copy.path));
    for copy in copies {
        let file = File::open(&copy.path)?;
        let mmap = super::common::map_sequential(&file)?;
        let bytes = bounded_transcript_bytes(&mmap, copy.size);
        let mut occurrences = HashMap::new();
        for line in bytes.split_inclusive(|byte| *byte == b'\n') {
            let value = match serde_json::from_slice::<Value>(line) {
                Ok(value) => Some(value),
                Err(error) if !line.ends_with(b"\n") && error.is_eof() => continue,
                Err(_) => None,
            };
            let key = value.as_ref().map(|value| {
                if let Some(uuid) = value
                    .get("uuid")
                    .and_then(Value::as_str)
                    .filter(|uuid| !uuid.is_empty())
                {
                    // Content blocks can share an event UUID. Match the block as well so a
                    // tail-only copy cannot hide a different block from the same response.
                    serde_json::json!([
                        value.get("type"),
                        uuid,
                        value
                            .get("message")
                            .and_then(|message| message.get("content"))
                    ])
                    .to_string()
                } else if let Some(message) = value.get("message") {
                    serde_json::json!([
                        value.get("type"),
                        value.get("timestamp"),
                        message.get("id"),
                        message.get("role"),
                        message.get("content")
                    ])
                    .to_string()
                } else {
                    value.to_string()
                }
            });
            if let Some(key) = key {
                // Repeated identical events within one file remain distinct. Only matching
                // occurrences in another physical copy are suppressed.
                let occurrence = occurrences.entry(key.clone()).or_insert(0usize);
                let unique = seen.insert((key, *occurrence));
                *occurrence += 1;
                if !unique {
                    continue;
                }
            }
            let timestamp = value
                .as_ref()
                .and_then(|value| value.get("timestamp"))
                .and_then(Value::as_str)
                .and_then(super::common::parse_iso_millis)
                .unwrap_or(0);
            lines.push((timestamp, line.to_vec()));
        }
    }
    lines.sort_by_key(|(timestamp, _)| *timestamp);
    let mut merged = Vec::new();
    for (_, line) in lines {
        merged.extend_from_slice(&line);
        if !line.ends_with(b"\n") {
            merged.push(b'\n');
        }
    }
    parse_index_bytes(
        owner,
        &merged,
        IndexParseState::default(),
        include_reasoning,
        None,
        next_doc_id,
        emit,
    )
}

fn parse_index_bytes(
    path: &Path,
    mmap: &[u8],
    state: IndexParseState,
    include_reasoning: bool,
    known_background: Option<bool>,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<(IndexParseOutput, bool)> {
    let mut legacy_turn_id = state.legacy_ordinal()?;
    let mut emit = |mut record: Record| {
        if record.links.source_record_offset.is_none() {
            record.links.legacy_turn_id = Some(legacy_turn_id);
            legacy_turn_id += 1;
        }
        emit(record)
    };
    use memchr::memchr;
    use simd_json::prelude::*;

    // `sessionKind` is not repeated on every event. Establish this file-level
    // origin before emitting any records so a later `bg` marker cannot leave
    // the transcript split between interactive and background classifications.
    let is_background = known_background.unwrap_or_else(|| {
        mmap.split(|byte| *byte == b'\n')
            .any(line_has_background_session_kind)
    });
    let mut start = state.offset as usize;
    let mut turn_id = state.turn_id;
    let mut pending_tool_calls = state.pending_tool_calls;
    let project = project_from_path(path);
    let session_id = session_id_from_path(path);
    let is_agent_file = is_subagent_path(path);
    let source_path = path.to_string_lossy().to_string();
    let mut buffer = Vec::new();
    let mut diagnostics = ParseDiagnostics::default();
    let mut session_cwd: Option<String> = None;

    while start < mmap.len() {
        let source_record_offset = start as u64;
        let slice = &mmap[start..];
        let relative = memchr(b'\n', slice).unwrap_or(slice.len());
        let has_newline = relative < slice.len();
        // The task metadata can race a writer which has appended only part of
        // a JSONL record. Do not classify that prefix as malformed: retain its
        // start offset so the next ingest reads the complete record. A final
        // non-newline record is still valid when this mmap is exactly the task
        // boundary.
        let line = &slice[..relative];
        start += relative + usize::from(has_newline);
        if line.is_empty() {
            continue;
        }
        buffer.clear();
        buffer.extend_from_slice(line);
        let value = match simd_json::to_borrowed_value(&mut buffer) {
            Ok(value) => value,
            Err(_) => {
                // A writer can pause mid-record without growing the file
                // between discovery and mmap. Retry that incomplete JSON on
                // the next append, while still accepting complete final JSON
                // without a newline and diagnosing malformed complete lines.
                if !has_newline
                    && serde_json::from_slice::<Value>(line).is_err_and(|error| error.is_eof())
                {
                    start = source_record_offset as usize;
                    break;
                }
                diagnostics.malformed_json_lines += 1;
                continue;
            }
        };
        let Some(object) = value.as_object() else {
            diagnostics.non_object_json_lines += 1;
            continue;
        };
        if session_cwd.is_none()
            && let Some(cwd) = object.get("cwd").and_then(|value| value.as_str())
            && !cwd.is_empty()
        {
            session_cwd = Some(cwd.to_string());
        }
        let entry_type = object
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if entry_type != "user" && entry_type != "assistant" {
            if !matches!(
                entry_type,
                "progress"
                    | "summary"
                    | "system"
                    | "file-history-snapshot"
                    | "queue-operation"
                    | "pr-link"
                    | "last-prompt"
                    | "custom-title"
                    | "ai-title"
                    | "agent-name"
                    | "permission-mode"
                    | "attachment"
                    | "mode"
            ) {
                diagnostics.increment_unknown_top_level(entry_type);
            }
            continue;
        }
        let entry_uuid = super::common::borrowed_string(object, "uuid");
        let entry_parent_uuid = super::common::borrowed_string(object, "parentUuid");
        let sidechain = object
            .get("isSidechain")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let kind = classify_with_session_kind(path, sidechain, is_background);
        let thread_source = match kind {
            ConversationKind::Subagent => Some("subagent".to_string()),
            ConversationKind::Sidechain => Some("sidechain".to_string()),
            _ => None,
        };
        let entry_links = RecordLinks {
            event_id: entry_uuid.clone(),
            parent_event_id: entry_parent_uuid,
            logical_parent_event_id: super::common::borrowed_string(object, "logicalParentUuid"),
            parent_session_id: is_agent_file
                .then(|| super::common::borrowed_string(object, "sessionId"))
                .flatten(),
            thread_source,
            conversation_kind: Some(kind.as_str().to_string()),
            parent_tool_use_id: super::common::borrowed_string(object, "parentToolUseID"),
            source_tool_use_id: super::common::borrowed_string(object, "sourceToolUseID"),
            source_tool_assistant_uuid: super::common::borrowed_string(
                object,
                "sourceToolAssistantUUID",
            ),
            ..RecordLinks::default()
        };
        let timestamp = object
            .get("timestamp")
            .and_then(|value| value.as_str())
            .and_then(super::common::parse_iso_millis)
            .unwrap_or(0);
        let Some(message) = object.get("message").and_then(|value| value.as_object()) else {
            continue;
        };
        let content = message.get("content");
        let mut text_parts = Vec::new();
        let mut content_index = 0usize;
        if let Some(content) = content {
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
                            let tool_name = block_object
                                .get("name")
                                .and_then(|value| value.as_str())
                                .map(str::to_string);
                            let tool_input = block_object
                                .get("input")
                                .and_then(super::common::tool_value_text);
                            let text = tool_input.clone().unwrap_or_default();
                            let tool_id = block_object
                                .get("id")
                                .and_then(|value| value.as_str())
                                .map(str::to_string);
                            let mut links = entry_links.clone();
                            if let Some(tool_id) = &tool_id {
                                links.event_id = Some(tool_id.clone());
                                links.parent_event_id = entry_uuid.clone();
                            }
                            let doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
                            if let Some(tool_id) = tool_id {
                                let replaced = pending_tool_calls.insert(
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
                                if replaced.is_some() {
                                    diagnostics.duplicate_tool_calls += 1;
                                }
                            }
                            emit(Record {
                                source: SourceKind::Claude,
                                doc_id,
                                ts: timestamp,
                                project: project.clone(),
                                session_id: session_id.clone(),
                                turn_id,
                                role: "tool_use".to_string(),
                                text,
                                tool_name,
                                tool_input,
                                tool_output: None,
                                links,
                                source_path: source_path.clone(),
                            })?;
                            turn_id += 1;
                        }
                        "thinking" => {
                            let thinking = block_object
                                .get("thinking")
                                .and_then(|value| value.as_str())
                                .map(str::trim)
                                .filter(|text| !text.is_empty());
                            if let Some(thinking) = thinking {
                                if include_reasoning {
                                    let mut links = entry_links.clone();
                                    links.event_id = entry_uuid
                                        .as_ref()
                                        .map(|uuid| format!("{uuid}:reasoning:{content_index}"));
                                    emit(Record {
                                        source: SourceKind::Claude,
                                        doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                                        ts: timestamp,
                                        project: project.clone(),
                                        session_id: session_id.clone(),
                                        turn_id,
                                        role: "reasoning".to_string(),
                                        text: thinking.to_string(),
                                        tool_name: None,
                                        tool_input: None,
                                        tool_output: None,
                                        links,
                                        source_path: source_path.clone(),
                                    })?;
                                    turn_id += 1;
                                }
                            } else if block_object.contains_key("signature")
                                || block_object.contains_key("data")
                            {
                                diagnostics.encrypted_reasoning_dropped += 1;
                            }
                        }
                        "redacted_thinking" => {
                            diagnostics.encrypted_reasoning_dropped += 1;
                        }
                        "tool_result" | "image" | "document" => {}
                        unknown => diagnostics.increment_unknown_semantic(unknown),
                    }
                    content_index += 1;
                }
            }
        }

        if entry_type == "user"
            && let Some(array) = content.and_then(|value| value.as_array())
        {
            for block in array {
                let Some(block_object) = block.as_object() else {
                    continue;
                };
                if block_object.get("type").and_then(|value| value.as_str()) != Some("tool_result")
                {
                    continue;
                }
                let tool_output = block_object
                    .get("content")
                    .and_then(super::common::tool_value_text);
                let mut text = super::common::tool_result_text(block).unwrap_or_default();
                if text.is_empty()
                    && let Some(content) = block_object.get("content")
                {
                    text = super::common::tool_value_text(content).unwrap_or_default();
                }
                let tool_use_id = block_object
                    .get("tool_use_id")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let pending = tool_use_id
                    .as_ref()
                    .and_then(|id| pending_tool_calls.remove(id));
                if tool_use_id.is_some() && pending.is_none() {
                    diagnostics.orphan_tool_results += 1;
                }
                let tool_name = pending.and_then(|call| call.tool_name);
                let mut links = entry_links.clone();
                links.tool_result_is_error = block_object.get("is_error").and_then(|v| v.as_bool());
                if let Some(tool_use_id) = &tool_use_id {
                    links.event_id = entry_uuid
                        .as_ref()
                        .map(|uuid| format!("{uuid}:tool_result:{tool_use_id}"));
                    links.parent_event_id = Some(tool_use_id.clone());
                    links.parent_tool_use_id = Some(tool_use_id.clone());
                }
                emit(Record {
                    source: SourceKind::Claude,
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
        }

        let text = text_parts.join(" ").trim().to_string();
        let mut entry_links = entry_links;
        if let Some(array) = content.and_then(|v| v.as_array())
            && array.iter().any(|block| {
                matches!(
                    block.get("type").and_then(|v| v.as_str()),
                    Some("image" | "document")
                )
            })
        {
            // Tool and reasoning blocks have their own governed projections. Do not
            // duplicate them or expose hidden thinking through attachment metadata.
            let display_blocks = array
                .iter()
                .filter(|block| {
                    matches!(
                        block.get("type").and_then(|v| v.as_str()),
                        Some("text" | "image" | "document")
                    )
                })
                .collect::<Vec<_>>();
            entry_links.source_content = Some(serde_json::to_string(&display_blocks)?);
        }
        if !text.is_empty() || entry_links.source_content.is_some() {
            if text.is_empty() {
                entry_links.source_record_offset = Some(source_record_offset);
            }
            emit(Record {
                source: SourceKind::Claude,
                doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                ts: timestamp,
                project: project.clone(),
                session_id: session_id.clone(),
                turn_id,
                role: entry_type.to_string(),
                text,
                tool_name: None,
                tool_input: None,
                tool_output: None,
                links: entry_links,
                source_path: source_path.clone(),
            })?;
            turn_id += 1;
        }
    }

    Ok((
        IndexParseOutput {
            offset: start as u64,
            turn_id,
            legacy_turn_id: Some(legacy_turn_id),
            pending_tool_calls,
            session_id: Some(session_id),
            diagnostics,
            session_cwd,
        },
        is_background,
    ))
}

pub(crate) fn parse_usage_file(path: &Path) -> Result<Vec<UsageEvent>> {
    static USAGE_NEEDLE: Lazy<memmem::Finder<'static>> =
        Lazy::new(|| memmem::Finder::new(b"\"usage\""));
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let fallback_session = Some(session_id_from_path(path));
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut line = Vec::with_capacity(16 * 1024);
    let mut index = 0u64;
    let mut events = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if USAGE_NEEDLE.find(&line).is_some()
            && let Ok(value) = simd_json::to_borrowed_value(&mut line)
            && value.get("type").and_then(|value| value.as_str()) == Some("assistant")
            && let Some(message) = value.get("message")
            && let Some(usage) = message.get("usage")
        {
            let number = |value: &simd_json::BorrowedValue, names: &[&str]| {
                names
                    .iter()
                    .find_map(|name| value.get(*name).and_then(|value| value.as_u64()))
                    .unwrap_or(0)
            };
            let cache_write = number(
                usage,
                &["cache_creation_input_tokens", "cacheCreationInputTokens"],
            );
            let mut tokens = TokenBuckets::disjoint(
                number(usage, &["input_tokens", "inputTokens"]),
                number(usage, &["cache_read_input_tokens", "cacheReadInputTokens"]),
                cache_write,
                number(usage, &["output_tokens", "outputTokens"]),
            );
            tokens.cache_write_1h = usage
                .get("cache_creation")
                .and_then(|cache| cache.get("ephemeral_1h_input_tokens"))
                .and_then(|value| value.as_u64())
                .unwrap_or_default()
                .min(tokens.cache_write);
            if tokens.additive_total() > 0 {
                let string = |names: &[&str]| {
                    names
                        .iter()
                        .find_map(|name| value.get(*name).and_then(|value| value.as_str()))
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                };
                let session_id = string(&["sessionId", "session_id"]);
                let request_id = string(&["requestId", "request_id"]);
                let message_id = message
                    .get("id")
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.is_empty())
                    .map(str::to_string);
                let exact_dedupe = message_id.is_some() && request_id.is_some();
                let project = value
                    .get("cwd")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let timestamp_ms = value
                    .get("timestamp")
                    .map(usage_timestamp_millis)
                    .unwrap_or(0);
                events.push(UsageEvent {
                    source: "claude",
                    source_path: source_path.clone(),
                    source_record_id: Some(format!("line:{index}")),
                    session_id: session_id.or_else(|| fallback_session.clone()),
                    request_id,
                    message_id,
                    timestamp_ms,
                    project,
                    provider: Some("anthropic".into()),
                    model: message
                        .get("model")
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty())
                        .map(str::to_string),
                    tokens,
                    credits: None,
                    token_usage_available: true,
                    source_cost_usd: value.get("costUSD").and_then(|value| value.as_f64()),
                    cost_authoritative: false,
                    dedupe_confidence: if exact_dedupe { "exact" } else { "heuristic" },
                    conservative_undercount: false,
                    cache_chain_excluded: false,
                    permission_review: false,
                    sidechain: value
                        .get("isSidechain")
                        .and_then(|value| value.as_bool())
                        .unwrap_or(false),
                    source_order: index,
                });
            }
        }
        index += 1;
    }
    Ok(events)
}

fn usage_timestamp_millis(timestamp: &simd_json::BorrowedValue<'_>) -> u64 {
    timestamp
        .as_u64()
        .map(|value| {
            if value < 10_000_000_000 {
                value.saturating_mul(1_000)
            } else {
                value
            }
        })
        .or_else(|| {
            timestamp
                .as_i64()
                .filter(|value| *value >= 0)
                .map(|value| value as u64)
        })
        .or_else(|| timestamp.as_str().and_then(super::common::parse_iso_millis))
        .unwrap_or(0)
}

pub(crate) fn reconcile_usage(events: &mut Vec<UsageEvent>) {
    use std::collections::HashMap;

    let mut best_exact: HashMap<(&str, &str, &str), usize> = HashMap::new();
    let mut best_agent = HashMap::new();
    let mut keep = vec![true; events.len()];
    for (index, event) in events.iter().enumerate() {
        if event.source != "claude" {
            continue;
        }
        if let Some(message) = event.message_id.as_deref()
            && let Some(agent) = agent_copy_key(Path::new(&*event.source_path))
        {
            // The direct copy can omit requestId or hold only the resumed tail. Message IDs
            // still name responses within the same logical agent across all its copies.
            if let Some(previous) = best_agent.get(&(agent.clone(), message)).copied() {
                if choose_usage(&events[previous], event) {
                    keep[index] = false;
                } else {
                    keep[previous] = false;
                    best_agent.insert((agent, message), index);
                }
            } else {
                best_agent.insert((agent, message), index);
            }
        }
    }
    for (index, event) in events.iter().enumerate() {
        if !keep[index] || event.source != "claude" {
            continue;
        }
        // Claude Code writes one line per content block, each repeating the response's usage.
        // Without a `requestId`, the message ID still names the response within its file.
        let key = match (event.message_id.as_deref(), event.request_id.as_deref()) {
            (Some(message), Some(request)) => (message, request, ""),
            (Some(message), None) => (message, "", &*event.source_path),
            (None, _) => continue,
        };
        if let Some(previous) = best_exact.get(&key).copied() {
            if choose_usage(&events[previous], event) {
                keep[index] = false;
            } else {
                keep[previous] = false;
                best_exact.insert(key, index);
            }
        } else {
            best_exact.insert(key, index);
        }
    }
    let mut by_message: HashMap<&str, Vec<usize>> = HashMap::new();
    for (index, event) in events.iter().enumerate() {
        if keep[index]
            && event.source == "claude"
            && let Some(message) = event.message_id.as_deref()
        {
            by_message.entry(message).or_default().push(index);
        }
    }
    for indices in by_message.into_values() {
        let is_agent = |event: &UsageEvent| {
            event.sidechain || is_subagent_path(Path::new(&*event.source_path))
        };
        if indices.len() < 2 || !indices.iter().any(|index| !is_agent(&events[*index])) {
            continue;
        }
        for index in indices {
            if is_agent(&events[index]) {
                keep[index] = false;
            }
        }
    }
    let mut index = 0usize;
    events.retain(|_| {
        let retain = keep[index];
        index += 1;
        retain
    });
}

fn choose_usage(left: &UsageEvent, right: &UsageEvent) -> bool {
    if left.sidechain != right.sidechain {
        return !left.sidechain;
    }
    let left_parent = !left.source_path.contains("/subagents/");
    let right_parent = !right.source_path.contains("/subagents/");
    if left_parent != right_parent {
        return left_parent;
    }
    let left_total = left.tokens.additive_total();
    let right_total = right.tokens.additive_total();
    if left_total != right_total {
        return left_total > right_total;
    }
    left.source_order >= right.source_order
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn discovery_always_includes_agent_transcripts() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("main.jsonl"), "{}\n").unwrap();
        fs::write(temp.path().join("agent-child.jsonl"), "{}\n").unwrap();
        // The retired opt-in flag no longer gates anything.
        assert_eq!(discover(temp.path(), false, None).unwrap().len(), 2);
        assert_eq!(discover(temp.path(), true, None).unwrap().len(), 2);
    }

    #[test]
    fn discovery_excludes_nested_workflow_journals() {
        let temp = tempfile::tempdir().unwrap();
        let subagents = temp.path().join("project/session/subagents/workflows/run");
        fs::create_dir_all(&subagents).unwrap();
        fs::write(subagents.join("agent-child.jsonl"), "{}\n").unwrap();
        fs::write(subagents.join("journal.jsonl"), "{}\n").unwrap();

        let files = discover(temp.path(), false, None).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].path.file_name().and_then(|name| name.to_str()),
            Some("agent-child.jsonl")
        );
    }

    #[test]
    fn probe_shares_sidechain_session_and_project_identity() {
        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("-Users-nico-Code-memex");
        fs::create_dir_all(&project).unwrap();
        let path = project.join("fallback.jsonl");
        fs::write(
            &path,
            "{\"type\":\"assistant\",\"sessionId\":\"session-1\",\"cwd\":\"/Users/nico/Code/memex\",\"isSidechain\":true}\n",
        )
        .unwrap();
        let metadata = probe(&path).unwrap();
        assert_eq!(metadata.session.session_id, "session-1");
        assert_eq!(
            metadata.session.conversation_kind,
            ConversationKind::Sidechain
        );
        assert_eq!(metadata.project.as_deref(), Some("memex"));
    }

    #[test]
    fn background_session_kind_marks_the_complete_transcript_non_interactive() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("background.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"type\":\"user\",\"sessionId\":\"background\",\"message\":{\"content\":\"before marker\"}}\n",
                "{\"type\":\"assistant\",\"sessionKind\":\"bg\",\"message\":{\"content\":\"background work\"}}\n",
                "{\"type\":\"user\",\"message\":{\"content\":\"after marker\"}}\n"
            ),
        )
        .unwrap();

        assert_eq!(
            probe(&path).unwrap().session.conversation_kind,
            ConversationKind::Subagent
        );
        let mut records = Vec::new();
        parse_index_records(
            &path,
            IndexParseState::default(),
            false,
            &AtomicU64::new(1),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(records.len(), 3);
        assert!(records.iter().all(|record| {
            record.links.conversation_kind.as_deref() == Some("subagent")
                && record.links.thread_source.as_deref() == Some("subagent")
        }));
    }

    #[test]
    fn probe_keeps_background_detection_to_its_bounded_prefix() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("late-background.jsonl");
        let mut transcript = String::new();
        for index in 0..64 {
            transcript.push_str(&format!(
                r#"{{"type":"user","sessionId":"session","message":{{"content":"{index}"}}}}"#
            ));
            transcript.push('\n');
        }
        transcript.push_str(
            r#"{"type":"assistant","sessionKind":"bg","message":{"content":"late marker"}}
"#,
        );
        fs::write(&path, transcript).unwrap();

        // Ingest persists and incrementally updates the complete file-level
        // classification. Probe is intentionally a bounded metadata reader.
        assert_eq!(
            probe(&path).unwrap().session.conversation_kind,
            ConversationKind::Main
        );
    }

    #[test]
    fn parser_accepts_a_final_non_newline_record_at_the_task_boundary() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("final-record.jsonl");
        let line = r#"{"type":"user","message":{"content":"final record"}}"#;
        fs::write(&path, line).unwrap();

        let mut records = Vec::new();
        let (parsed, background) = parse_index_records_with_background(
            &path,
            IndexParseState::default(),
            false,
            None,
            line.len() as u64,
            &AtomicU64::new(1),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();

        assert!(!background);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].text, "final record");
        assert_eq!(parsed.offset, line.len() as u64);
    }

    #[test]
    fn session_title_prefers_custom_title_over_ai_title() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"type\":\"ai-title\",\"sessionId\":\"session-1\",\"aiTitle\":\"Generated title\"}\n",
                "{\"type\":\"custom-title\",\"sessionId\":\"session-1\",\"customTitle\":\"My title\"}\n"
            ),
        )
        .unwrap();

        assert_eq!(
            session_title(&path, "session-1").as_deref(),
            Some("My title")
        );
        assert_eq!(session_title(&path, "another-session"), None);
    }

    #[test]
    fn session_title_scans_past_large_messages_and_keeps_last_matching_rename() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let lines = [
            serde_json::json!({"type":"custom-title", "sessionId":"s", "customTitle":"Original"}),
            serde_json::json!({"type":"assistant", "message":{"content":"x".repeat(262144)}}),
            serde_json::json!({"type":"assistant", "customTitle":"False match", "message":{"content":"custom-title"}}),
            serde_json::json!({"type":"custom-title", "sessionId":"other", "customTitle":"Other session"}),
            serde_json::json!({"type":"agent-name", "sessionId":"s", "agentName":"Agent name"}),
            serde_json::json!({"type":"custom-title", "sessionId":"s", "customTitle":"Renamed"}),
            serde_json::json!({"type":"ai-title", "sessionId":"s", "aiTitle":"Later generated title"}),
        ];
        // The final event intentionally has no trailing newline.
        fs::write(
            &path,
            lines
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        assert_eq!(session_title(&path, "s").as_deref(), Some("Renamed"));
        assert_eq!(
            session_title(&path, "other").as_deref(),
            Some("Other session")
        );
    }

    #[test]
    fn session_title_keeps_agent_fallback_and_empty_file_behavior() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        fs::write(&path, "").unwrap();
        assert_eq!(session_title(&path, "s"), None);
        fs::write(&path, "malformed custom-title\n{\"type\":\"agent-name\",\"sessionId\":\"s\",\"name\":\"Task name\"}").unwrap();
        assert_eq!(session_title(&path, "s").as_deref(), Some("Task name"));
        fs::write(
            &path,
            r#"{"type":"ai\u002dtitle","sessionId":"s","aiTitle":"Escaped type"}"#,
        )
        .unwrap();
        assert_eq!(session_title(&path, "s").as_deref(), Some("Escaped type"));
    }

    #[test]
    fn transcript_paths_name_their_profile_and_conversation() {
        let main = Path::new("/home/u/.claude-profiles/work/projects/-home-u-repo/s.jsonl");
        let agent = Path::new(
            "/home/u/.claude/projects/-home-u-projects/s/subagents/workflows/wf_1/agent-a.jsonl",
        );
        assert_eq!(profile_from_path(main).as_deref(), Some("work"));
        assert_eq!(profile_from_path(agent).as_deref(), Some(".claude"));
        assert_eq!(profile_from_path(Path::new("/data/claude/s.jsonl")), None);
        assert_eq!(
            project_relative_path(main),
            Some(PathBuf::from("-home-u-repo/s.jsonl"))
        );
        assert_eq!(
            project_relative_path(agent),
            Some(PathBuf::from(
                "-home-u-projects/s/subagents/workflows/wf_1/agent-a.jsonl"
            ))
        );
        assert_eq!(
            project_relative_path(Path::new("/x/projects/s.jsonl")),
            None
        );
    }

    #[test]
    fn agent_copies_share_a_key_and_equal_sizes_keep_the_direct_copy() {
        let temp = tempfile::tempdir().unwrap();
        let subagents = temp.path().join("projects/-p/s/subagents");
        let direct = subagents.join("agent-a.jsonl");
        let workflow = subagents.join("workflows/wf_1/agent-a.jsonl");
        fs::create_dir_all(workflow.parent().unwrap()).unwrap();
        fs::write(&direct, "{}\n").unwrap();
        fs::write(&workflow, "{}\n").unwrap();
        fs::write(subagents.join("workflows/wf_1/journal.jsonl"), "{}\n").unwrap();

        assert_eq!(agent_copy_key(&direct), agent_copy_key(&workflow));
        assert_eq!(
            agent_copy_key(&subagents.join("workflows/wf_1/journal.jsonl")),
            None
        );
        assert_eq!(
            agent_copy_key(&temp.path().join("projects/-p/agent-a.jsonl")),
            None
        );
        let (directory, name) = agent_copy_key(&workflow).unwrap();
        let mut copies = agent_copies(&directory, &workflow_runs(&directory), &name);
        copies.sort();
        assert_eq!(copies, [direct.clone(), workflow.clone()]);
        assert_eq!(preferred_agent_copy(&copies), Some(&direct));
        fs::write(&workflow, "{}\n{}\n").unwrap();
        assert_eq!(preferred_agent_copy(&copies), Some(&workflow));
    }

    #[test]
    fn the_agent_copy_with_the_newest_record_wins_over_a_larger_one() {
        let temp = tempfile::tempdir().unwrap();
        let subagents = temp.path().join("projects/-p/s/subagents");
        let direct = subagents.join("agent-a.jsonl");
        let workflow = subagents.join("workflows/wf_1/agent-a.jsonl");
        fs::create_dir_all(workflow.parent().unwrap()).unwrap();
        let record = |uuid: &str, minute: u32, extra: &str| {
            format!(
                "{{\"type\":\"user\",\"uuid\":\"{uuid}\",{extra}\"timestamp\":\"2026-10-11T10:{minute:02}:00.000Z\",\"message\":{{\"content\":\"x\"}}}}\n"
            )
        };
        // The workflow copy repeats extra fields on every line.
        let extra = format!("\"version\":\"{}\",", "2".repeat(200));
        fs::write(
            &workflow,
            record("u1", 1, &extra) + &record("u2", 2, &extra),
        )
        .unwrap();
        // A resumed agent adds one short record to the direct copy only.
        fs::write(
            &direct,
            record("u1", 1, "") + &record("u2", 2, "") + &record("u3", 3, ""),
        )
        .unwrap();
        assert!(fs::metadata(&direct).unwrap().len() < fs::metadata(&workflow).unwrap().len());
        let copies = [direct.clone(), workflow.clone()];
        assert_eq!(preferred_agent_copy(&copies), Some(&direct));

        // A direct copy rewritten with only the tail ends on the same record and loses to the
        // complete workflow copy.
        fs::write(&direct, record("u2", 2, "")).unwrap();
        assert_eq!(preferred_agent_copy(&copies), Some(&workflow));

        // A last line longer than the tail hides the workflow copy's newest record; an older
        // direct copy must not win by default, so sizes decide.
        let huge = format!("\"content\":\"{}\",", "x".repeat(300 * 1024));
        fs::write(
            &workflow,
            record("u1", 1, &extra) + &record("u2", 2, &extra) + &record("u3", 3, &huge),
        )
        .unwrap();
        fs::write(&direct, record("u1", 1, "") + &record("u2", 2, "")).unwrap();
        assert_eq!(preferred_agent_copy(&copies), Some(&workflow));
    }

    #[test]
    fn usage_reads_all_copies_and_counts_a_workflow_response_once() {
        let _guard = crate::test_support::env_lock();
        let temp = tempfile::tempdir().unwrap();
        let _env = crate::test_support::EnvVarGuard::set_os(&[(
            "CLAUDE_CONFIG_DIR",
            Some(temp.path().as_os_str()),
        )]);
        let subagents = temp.path().join("projects/-p/s/subagents");
        let direct = subagents.join("agent-a.jsonl");
        let workflow = subagents.join("workflows/wf_1/agent-a.jsonl");
        fs::create_dir_all(workflow.parent().unwrap()).unwrap();
        // Claude Code omits `requestId` from the direct copy only, so reconciliation alone
        // cannot tell that both copies record the same response.
        let line = r#"{"type":"assistant","isSidechain":true,"message":{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":5}}}"#;
        fs::write(&direct, format!("{line}\n")).unwrap();
        fs::write(
            &workflow,
            format!(
                "{}\n",
                line.replacen('{', r#"{"requestId":"req_1","version":"2.1","#, 1)
            ),
        )
        .unwrap();
        let mut both = parse_usage_file(&direct).unwrap();
        both.extend(parse_usage_file(&workflow).unwrap());
        reconcile_usage(&mut both);
        assert_eq!(both.len(), 1);

        assert_eq!(
            usage_files(&crate::config::default_claude_sources()),
            [direct, workflow]
        );
    }

    #[test]
    fn usage_still_prefers_a_parent_response_when_agent_copies_omit_sidechain_flags() {
        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("projects/-p");
        let parent = project.join("s.jsonl");
        let direct = project.join("s/subagents/agent-a.jsonl");
        let workflow = project.join("s/subagents/workflows/wf_1/agent-a.jsonl");
        fs::create_dir_all(workflow.parent().unwrap()).unwrap();
        let line = r#"{"type":"assistant","message":{"id":"msg_1","usage":{"input_tokens":10,"output_tokens":5}}}"#;
        let requested = line.replacen('{', r#"{"requestId":"req_1","#, 1);
        fs::write(&parent, format!("{requested}\n")).unwrap();
        fs::write(&direct, format!("{line}\n")).unwrap();
        fs::write(&workflow, format!("{requested}\n")).unwrap();
        let mut events = parse_usage_file(&direct).unwrap();
        events.extend(parse_usage_file(&workflow).unwrap());
        events.extend(parse_usage_file(&parent).unwrap());

        reconcile_usage(&mut events);

        assert_eq!(events.len(), 1);
        assert_eq!(&*events[0].source_path, parent.to_str().unwrap());
        assert_eq!(events[0].tokens.total(), 15);
    }

    #[test]
    fn merged_agent_blocks_keep_unique_content_and_reconstruct_tool_pairs_in_order() {
        let temp = tempfile::tempdir().unwrap();
        let subagents = temp.path().join("projects/-p/s/subagents");
        let direct = subagents.join("agent-a.jsonl");
        let workflow = subagents.join("workflows/wf_1/agent-a.jsonl");
        fs::create_dir_all(workflow.parent().unwrap()).unwrap();
        let line = |uuid: &str, minute: u32, kind: &str, content: Value| {
            serde_json::json!({
                "type": kind, "uuid": uuid,
                "timestamp": format!("2026-10-11T10:{minute:02}:00.000Z"),
                "message": {"content": content}
            })
            .to_string()
                + "\n"
        };
        let first = line(
            "shared",
            1,
            "assistant",
            serde_json::json!([{"type":"text","text":"first block"}]),
        );
        let second = line(
            "shared",
            2,
            "assistant",
            serde_json::json!([{"type":"text","text":"second block"}]),
        );
        let tool = line(
            "call",
            3,
            "assistant",
            serde_json::json!([{"type":"tool_use","id":"tool-1","name":"Read","input":{"path":"x"}}]),
        );
        let result = line(
            "result",
            4,
            "user",
            serde_json::json!([{"type":"tool_result","tool_use_id":"tool-1","content":"contents"}]),
        );
        fs::write(&workflow, format!("{first}{second}{tool}")).unwrap();
        fs::write(&direct, format!("{second}{result}")).unwrap();
        let copies = [&direct, &workflow]
            .into_iter()
            .map(|path| AgentCopy {
                path: path.clone(),
                size: fs::metadata(path).unwrap().len(),
                metadata_sha256: String::new(),
            })
            .collect::<Vec<_>>();
        let mut records = Vec::new();

        let (parsed, _) =
            parse_agent_records(&direct, &copies, false, &AtomicU64::new(1), |record| {
                records.push(record);
                Ok(())
            })
            .unwrap();

        assert_eq!(records.len(), 4);
        assert_eq!(records[0].text, "first block");
        assert_eq!(records[1].text, "second block");
        assert_eq!(records[2].role, "tool_use");
        assert_eq!(records[3].role, "tool_result");
        assert_eq!(records[3].tool_name.as_deref(), Some("Read"));
        assert!(
            records
                .iter()
                .all(|record| record.source_path == direct.to_string_lossy())
        );
        assert_eq!(parsed.diagnostics.orphan_tool_results, 0);
    }

    #[test]
    fn usage_counts_each_content_block_response_once_without_request_ids() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("agent-a.jsonl");
        let line = |output: u64| {
            format!(
                "{{\"type\":\"assistant\",\"isSidechain\":true,\"message\":{{\"id\":\"msg_1\",\"usage\":{{\"input_tokens\":10,\"output_tokens\":{output}}}}}}}\n"
            )
        };
        // Two content blocks of one response, then a separate response.
        let other = line(7).replace("msg_1", "msg_2");
        fs::write(&path, format!("{}{}{other}", line(4), line(5))).unwrap();
        let mut events = parse_usage_file(&path).unwrap();
        // The same response in another file is not merged without a request ID.
        let elsewhere = temp.path().join("other.jsonl");
        fs::write(&elsewhere, line(5)).unwrap();
        events.extend(parse_usage_file(&elsewhere).unwrap());

        reconcile_usage(&mut events);

        let mut outputs = events
            .iter()
            .map(|event| (event.source_path.to_string(), event.tokens.output))
            .collect::<Vec<_>>();
        outputs.sort();
        let path = path.to_string_lossy().into_owned();
        let elsewhere = elsewhere.to_string_lossy().into_owned();
        assert_eq!(outputs, [(path.clone(), 5), (path, 7), (elsewhere, 5)]);
    }

    #[test]
    fn usage_preserves_the_full_cwd_and_does_not_invent_a_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"type\":\"assistant\",\"cwd\":\"/Users/nico/Code/memex\",",
                "\"message\":{\"id\":\"one\",\"usage\":{\"input_tokens\":1}}}\n",
                "{\"type\":\"assistant\",",
                "\"message\":{\"id\":\"two\",\"usage\":{\"input_tokens\":1}}}\n"
            ),
        )
        .unwrap();

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events[0].project.as_deref(), Some("/Users/nico/Code/memex"));
        assert_eq!(events[1].project, None);
    }

    #[test]
    fn usage_converts_numeric_second_timestamps_to_milliseconds() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"type\":\"assistant\",\"timestamp\":1776386452,",
                "\"message\":{\"id\":\"seconds\",\"usage\":{\"input_tokens\":1}}}\n",
                "{\"type\":\"assistant\",\"timestamp\":1776386452437,",
                "\"message\":{\"id\":\"milliseconds\",\"usage\":{\"input_tokens\":1}}}\n"
            ),
        )
        .unwrap();

        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events[0].timestamp_ms, 1_776_386_452_000);
        assert_eq!(events[1].timestamp_ms, 1_776_386_452_437);
    }

    #[test]
    fn structured_tool_values_preserve_json_and_verbatim_strings() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        for output in [
            serde_json::json!({"ok":true,"rows":[1,2]}),
            serde_json::json!([{"type":"text","text":"hello", "extra":42}]),
            serde_json::json!("plain\noutput"),
        ] {
            fs::write(&path, format!("{}\n", serde_json::json!({"type":"user", "sessionId":"session", "message":{"content":[{"type":"tool_result", "tool_use_id":"call", "content":output}]}}))).unwrap();
            let mut records = Vec::new();
            parse_index_records(
                &path,
                IndexParseState::default(),
                false,
                &AtomicU64::new(1),
                |record| {
                    records.push(record);
                    Ok(())
                },
            )
            .unwrap();
            let actual = records[0].tool_output.as_ref().unwrap();
            if let Some(text) = output.as_str() {
                assert_eq!(actual, text);
            } else {
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(actual).unwrap(),
                    output
                );
            }
        }
    }

    #[test]
    fn attachment_records_preserve_legacy_ids_across_full_and_incremental_parsing() {
        use crate::retrieval::canonical_record_id;
        use serde_json::json;
        use std::io::Write;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let entry = |role, content| json!({"type":role, "sessionId":"session", "message":{"content":content}});
        let legacy = vec![
            entry("user", json!("Question")),
            entry(
                "assistant",
                json!([
                    {"type":"tool_use", "id":"call", "name":"Read", "input":{}},
                    {"type":"thinking", "thinking":"Reasoning"},
                    {"type":"text", "text":"Working"}
                ]),
            ),
            entry(
                "user",
                json!([{"type":"tool_result", "tool_use_id":"call", "content":"read result"}]),
            ),
            entry("assistant", json!("Answer A")),
            entry(
                "assistant",
                json!([{"type":"tool_use", "name":"Other", "input":"other input"}]),
            ),
            json!({"type":"assistant", "uuid":"native-answer", "message":{"content":"Answer B"}}),
        ];
        let image =
            json!({"type":"image", "source":{"type":"url", "url":"https://example.com/image.png"}});
        let document = json!({"type":"document", "source":{"type":"base64", "media_type":"application/pdf", "data":"AAAA"}});
        let mut mixed = legacy.clone();
        // A tool-only source line gains an attachment-only aggregate message;
        // the tool's existing identity and the next legacy ordinal both survive.
        mixed[4]["message"]["content"]
            .as_array_mut()
            .unwrap()
            .push(image.clone());
        mixed.insert(2, entry("user", json!([document])));
        mixed.insert(0, entry("user", json!([image])));
        let encoded = |values: &[serde_json::Value]| {
            values.iter().map(|v| format!("{v}\n")).collect::<String>()
        };
        for include_reasoning in [false, true] {
            let parse = |state| {
                let mut records = Vec::new();
                let output = parse_index_records(
                    &path,
                    state,
                    include_reasoning,
                    &AtomicU64::new(1),
                    |record| {
                        records.push(record);
                        Ok(())
                    },
                )
                .unwrap();
                (records, output)
            };
            fs::write(&path, encoded(&legacy)).unwrap();
            let (baseline, _) = parse(IndexParseState::default());
            assert_eq!(baseline.len(), 7 + usize::from(include_reasoning));
            let baseline_ids = baseline
                .iter()
                .enumerate()
                .map(|(ordinal, record)| {
                    let mut original = record.clone();
                    original.turn_id = ordinal as u32;
                    original.links.legacy_turn_id = None;
                    original.links.source_record_offset = None;
                    canonical_record_id(&original)
                })
                .collect::<Vec<_>>();
            fs::write(&path, encoded(&mixed)).unwrap();
            let (full, output) = parse(IndexParseState::default());
            let old_records = full
                .iter()
                .filter(|record| record.links.source_record_offset.is_none())
                .collect::<Vec<_>>();
            assert_eq!(
                old_records
                    .iter()
                    .map(|record| canonical_record_id(record))
                    .collect::<Vec<_>>(),
                baseline_ids
            );
            assert_eq!(
                old_records
                    .iter()
                    .map(|record| &record.text)
                    .collect::<Vec<_>>(),
                baseline
                    .iter()
                    .map(|record| &record.text)
                    .collect::<Vec<_>>()
            );
            assert_eq!(full.len(), baseline.len() + 3);
            assert_eq!(output.legacy_turn_id, Some(baseline.len() as u32));
            let full_ids = full.iter().map(canonical_record_id).collect::<Vec<_>>();
            assert_eq!(
                full_ids
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
                full.len()
            );

            fs::write(&path, "").unwrap();
            let mut state = IndexParseState::default();
            let mut incremental = Vec::new();
            for value in &mixed {
                writeln!(
                    fs::OpenOptions::new().append(true).open(&path).unwrap(),
                    "{value}"
                )
                .unwrap();
                let (records, output) = parse(state);
                incremental.extend(records);
                state = IndexParseState {
                    offset: output.offset,
                    turn_id: output.turn_id,
                    legacy_turn_id: output.legacy_turn_id,
                    pending_tool_calls: output.pending_tool_calls,
                };
            }
            assert_eq!(
                incremental
                    .iter()
                    .map(canonical_record_id)
                    .collect::<Vec<_>>(),
                full_ids
            );
            assert_eq!(state.legacy_turn_id, Some(baseline.len() as u32));
            assert!(
                parse_index_records(
                    &path,
                    IndexParseState {
                        offset: state.offset,
                        turn_id: state.turn_id,
                        ..IndexParseState::default()
                    },
                    include_reasoning,
                    &AtomicU64::new(1),
                    |_| Ok(())
                )
                .is_err()
            );
        }
    }

    #[test]
    fn tool_result_envelope_failure_survives_plain_text_projection() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        for flag in [
            serde_json::json!(true),
            serde_json::json!(false),
            serde_json::json!(1),
            serde_json::Value::Null,
        ] {
            fs::write(&path, format!("{}\n", serde_json::json!({"type":"user", "sessionId":"session", "message":{"content":[{"type":"tool_result", "tool_use_id":"call", "is_error":flag, "content":"permission denied"}]}}))).unwrap();
            let mut records = Vec::new();
            parse_index_records(
                &path,
                IndexParseState::default(),
                false,
                &AtomicU64::new(1),
                |record| {
                    records.push(record);
                    Ok(())
                },
            )
            .unwrap();
            let result = &records[0];
            assert_eq!(result.tool_output.as_deref(), Some("permission denied"));
            assert_eq!(result.links.tool_result_is_error, flag.as_bool());
            let json = serde_json::to_value(result).unwrap();
            assert_eq!(
                json.get("tool_result_is_error").and_then(|v| v.as_bool()),
                flag.as_bool()
            );
        }
    }

    #[test]
    fn source_content_preserves_images_documents_without_exposing_reasoning() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("session.jsonl");
        let image = serde_json::json!({"type":"image", "source":{"type":"base64", "media_type":"image/png", "data":"AAAA"}});
        let document = serde_json::json!({"type":"document", "source":{"type":"url", "url":"https://example.com/notes.pdf"}, "title":"Notes"});
        let contents = [
            serde_json::json!([image]),
            serde_json::json!([{"type":"text", "text":"<<ImageDisplayed>>"}, document, {"type":"thinking", "thinking":"private reasoning"}]),
            serde_json::json!([{"type":"text", "text":"plain text"}]),
        ];
        fs::write(&path, contents.iter().map(|content| format!("{}\n", serde_json::json!({"type":"user", "sessionId":"session", "message":{"role":"user", "content":content}}))).collect::<String>()).unwrap();
        let mut records = Vec::new();
        let parsed = parse_index_records(
            &path,
            IndexParseState::default(),
            false,
            &AtomicU64::new(1),
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert!(
            !parsed
                .diagnostics
                .unknown_semantic_types
                .contains_key("document")
        );
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].text, "");
        assert_eq!(records[1].text, "<<ImageDisplayed>>");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                records[0].links.source_content.as_ref().unwrap()
            )
            .unwrap(),
            contents[0]
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                records[1].links.source_content.as_ref().unwrap()
            )
            .unwrap(),
            serde_json::json!([contents[1][0], document])
        );
        assert!(
            !serde_json::to_string(&records)
                .unwrap()
                .contains("private reasoning")
        );
        assert!(records[2].links.source_content.is_none());
    }

    #[test]
    fn parity_fixture_makes_plain_reasoning_opt_in_and_drops_redacted_payloads() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("claude.jsonl");
        fs::write(
            &path,
            include_str!("../../fixtures/trajectory_parity/claude.jsonl"),
        )
        .unwrap();

        let mut without_reasoning = Vec::new();
        let parsed = parse_index_records(
            &path,
            IndexParseState::default(),
            false,
            &AtomicU64::new(1),
            |record| {
                without_reasoning.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert!(
            !without_reasoning
                .iter()
                .any(|record| record.role == "reasoning")
        );
        assert_eq!(parsed.diagnostics.encrypted_reasoning_dropped, 1);
        assert_eq!(parsed.diagnostics.malformed_json_lines, 1);

        let mut with_reasoning = Vec::new();
        parse_index_records(
            &path,
            IndexParseState::default(),
            true,
            &AtomicU64::new(1),
            |record| {
                with_reasoning.push(record);
                Ok(())
            },
        )
        .unwrap();
        assert!(with_reasoning.iter().any(|record| {
            record.role == "reasoning" && record.text == "Plain Claude reasoning"
        }));
        assert!(!with_reasoning.iter().any(|record| {
            record.text.contains("ciphertext-must-never-be-indexed")
                || record.text.contains("signature-must-never-be-indexed")
        }));
    }
}
