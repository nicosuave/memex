//! Kimi Code CLI's per-agent `wire.jsonl` journals and `state.json` session metadata.
//!
//! A session lives at `<home>/sessions/<workspace key>/<session id>/`. The main agent
//! and every subagent journal their own `agents/<agent id>/wire.jsonl`; `state.json`
//! beside `agents/` carries the session id, working directory, and generated title.
//! User prompts are journaled as `context.append_message` records, while assistant
//! replies, reasoning, and tool exchanges only exist as `context.append_loop_event`
//! records streamed by the agent loop.
use super::{IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions, SourceFile};
use crate::state::PendingToolCall;
use crate::types::{Record, RecordLinks, SourceKind};
use crate::usage::{TokenBuckets, UsageEvent};
use anyhow::Result;
use memchr::memchr;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufRead;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use walkdir::WalkDir;

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 1,
    index: 1,
    usage: 1,
};

const WIRE_FILE: &str = "wire.jsonl";
const MAIN_AGENT: &str = "main";
/// Session metadata paths under a session directory, in the order Kimi reads them.
pub(crate) const METADATA_FILES: [&str; 2] = ["state.json", "session-meta/state.json"];

pub fn home() -> PathBuf {
    std::env::var_os("KIMI_CODE_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| super::common::home().join(".kimi-code"))
}

pub fn sessions_root() -> PathBuf {
    home().join("sessions")
}

pub fn matches_path(path: &str) -> bool {
    let normalized = path.replace('\\', "/");
    let in_store = normalized.contains("/.kimi-code/sessions/")
        || Path::new(&normalized).starts_with(sessions_root());
    in_store && normalized.contains("/agents/") && normalized.ends_with("/wire.jsonl")
}

pub fn discover() -> Vec<SourceFile> {
    discover_from_root(&sessions_root())
}

fn discover_from_root(root: &Path) -> Vec<SourceFile> {
    let mut files = WalkDir::new(root)
        .min_depth(5)
        .max_depth(5)
        .into_iter()
        .flatten()
        .filter(|entry| {
            entry.file_type().is_file()
                && entry.file_name() == WIRE_FILE
                && is_agent_wire(entry.path())
        })
        .map(|entry| SourceFile {
            source: SourceKind::Kimi,
            path: entry.into_path(),
        })
        .collect::<Vec<_>>();
    files.sort_by(|a, b| a.path.cmp(&b.path));
    files
}

pub fn usage_files() -> Vec<PathBuf> {
    discover().into_iter().map(|file| file.path).collect()
}

fn dir_name(path: &Path) -> Option<&str> {
    path.file_name().and_then(|name| name.to_str())
}

/// `.../<session>/agents/<agent>/wire.jsonl`.
fn is_agent_wire(path: &Path) -> bool {
    path.parent().and_then(Path::parent).and_then(dir_name) == Some("agents")
}

struct Layout {
    session_dir: PathBuf,
    agent_id: String,
}

/// A journal outside the `agents/` tree is treated as the main agent's.
fn layout(path: &Path) -> Layout {
    let parent = path.parent().unwrap_or(Path::new(""));
    if is_agent_wire(path)
        && let Some(session_dir) = parent.parent().and_then(Path::parent)
    {
        return Layout {
            session_dir: session_dir.to_path_buf(),
            agent_id: dir_name(parent).unwrap_or(MAIN_AGENT).to_string(),
        };
    }
    Layout {
        session_dir: parent.to_path_buf(),
        agent_id: MAIN_AGENT.to_string(),
    }
}

/// Stable `state.json` fields; volatile ones (`lastPrompt`, `updatedAt`) stay out so
/// a running session does not re-fingerprint on every turn. The session id is the
/// directory name: Kimi addresses sessions by directory and honors `id` only in
/// current-version metadata.
#[derive(Default, serde::Serialize)]
struct Metadata {
    cwd: Option<String>,
    title: Option<String>,
    forked_from: Option<String>,
}

fn string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_owned)
}

fn metadata(session_dir: &Path) -> Metadata {
    // Older sessions keep it under `session-meta/`, which Kimi still reads when the
    // session root has none.
    let value = METADATA_FILES
        .iter()
        .find_map(|name| std::fs::read(session_dir.join(name)).ok())
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .unwrap_or(Value::Null);
    Metadata {
        // Sessions migrated from the legacy kimi-cli keep `workDir`.
        cwd: string(&value, "cwd")
            .or_else(|| string(&value, "workDir"))
            .or_else(|| string(&value["custom"], "cwd")),
        title: string(&value, "title"),
        forked_from: string(&value, "forkedFrom"),
    }
}

/// Agents disclose their cwd in `profile.bind` and `config.update` records near
/// the top of the journal; only sessions without metadata need this scan.
fn wire_cwd(path: &Path) -> Option<String> {
    let reader = std::io::BufReader::new(File::open(path).ok()?);
    reader.lines().map_while(Result::ok).find_map(|line| {
        serde_json::from_str::<Value>(&line)
            .ok()?
            .pointer("/environmentDisclosure/cwd")?
            .as_str()
            .filter(|cwd| !cwd.is_empty())
            .map(str::to_owned)
    })
}

struct Identity {
    session_id: String,
    parent_session_id: Option<String>,
    kind: &'static str,
    cwd: Option<String>,
    project: String,
}

fn identity(path: &Path) -> Identity {
    let layout = layout(path);
    let meta = metadata(&layout.session_dir);
    let main_id = dir_name(&layout.session_dir)
        .unwrap_or("unknown")
        .to_string();
    let cwd = meta.cwd.or_else(|| wire_cwd(path));
    let project = cwd
        .as_deref()
        .map(super::common::project_from_path)
        .unwrap_or_else(|| SourceKind::Kimi.label().to_string());
    if layout.agent_id == MAIN_AGENT {
        let forked = meta.forked_from.is_some();
        Identity {
            session_id: main_id,
            parent_session_id: meta.forked_from,
            kind: if forked { "fork" } else { "main" },
            cwd,
            project,
        }
    } else {
        Identity {
            session_id: format!("{main_id}:{}", layout.agent_id),
            parent_session_id: Some(main_id),
            kind: "subagent",
            cwd,
            project,
        }
    }
}

pub fn session_cwd(path: &Path) -> Option<String> {
    metadata(&layout(path).session_dir)
        .cwd
        .or_else(|| wire_cwd(path))
}

/// The generated title names the main conversation; subagent journals share its
/// `state.json` but are labeled by their own directive.
pub fn session_title(path: &Path) -> Option<String> {
    let layout = layout(path);
    if layout.agent_id != MAIN_AGENT {
        return None;
    }
    metadata(&layout.session_dir).title
}

/// Kimi resumes whole sessions, so a subagent's `<session>:<agent>` id resumes the
/// session that owns it.
pub fn resume_session_id(session_id: &str) -> &str {
    session_id
        .split_once(':')
        .map_or(session_id, |(session, _)| session)
}

pub(crate) fn metadata_fingerprint(path: &Path) -> String {
    let meta = metadata(&layout(path).session_dir);
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&meta).expect("metadata strings serialize"))
    )
}

/// Byte offset just past the history a journal inherited, which was indexed and counted
/// where it began. Forking a session copies every agent journal verbatim and appends a
/// top-level `forked` record; a forked subagent gets its parent's context replayed as
/// messages, closed by a `fork_context` notice. The last marker wins, since a fork of
/// a fork inherits the earlier one.
fn inherited_prefix_end(bytes: &[u8]) -> usize {
    let forked = last_line_end(bytes, br#""type":"forked""#, |value| {
        value["type"] == "forked"
    });
    let fork_context = last_line_end(bytes, br#""variant":"fork_context""#, |value| {
        let origin = &value["message"]["origin"];
        value["type"] == "context.append_message"
            && origin["kind"] == "injection"
            && origin["variant"] == "fork_context"
    });
    forked.max(fork_context)
}

/// Byte offset just past the last complete line containing `needle` whose record
/// `matches`; the needle only prefilters, since tool arguments can spell it too.
fn last_line_end(bytes: &[u8], needle: &[u8], matches: impl Fn(&Value) -> bool) -> usize {
    let finder = memchr::memmem::FinderRev::new(needle);
    let mut end = bytes.len();
    while let Some(hit) = finder.rfind(&bytes[..end]) {
        let line_start = memchr::memrchr(b'\n', &bytes[..hit]).map_or(0, |i| i + 1);
        // An unterminated line is still being written.
        if let Some(line_len) = memchr(b'\n', &bytes[hit..]) {
            let line_end = hit + line_len;
            if serde_json::from_slice::<Value>(&bytes[line_start..line_end])
                .is_ok_and(|value| matches(&value))
            {
                return line_end + 1;
            }
        }
        end = line_start;
    }
    0
}

/// Every undo journals a `context.undo` record, whether or not a branch switch
/// accompanies it.
const UNDO_NEEDLE: &[u8] = br#""type":"context.undo""#;

fn is_undo(value: &Value) -> bool {
    value["type"] == "context.undo"
}

const RETRY_NEEDLE: &[u8] = br#""type":"turn.step.retrying""#;

fn is_retry(value: &Value) -> bool {
    value["type"] == "turn.step.retrying"
}

/// Whether bytes `[from, to)` of a journal hold an undo or a step retry. Either can
/// retract lines an earlier pass indexed (a retry drops the failed attempt's partial
/// reply), so an append carrying one needs a full reparse.
pub(crate) fn has_retraction_since(path: &Path, from: u64, to: u64) -> Result<bool> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = File::open(path)?;
    let end = to.min(file.metadata()?.len());
    let start = from.min(end);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::with_capacity((end - start) as usize);
    file.take(end - start).read_to_end(&mut bytes)?;
    Ok(last_line_end(&bytes, UNDO_NEEDLE, is_undo) > 0
        || last_line_end(&bytes, RETRY_NEEDLE, is_retry) > 0)
}

fn origin_kind(origin: &Value) -> Option<&str> {
    origin.as_str().or_else(|| origin["kind"].as_str())
}

/// Prompts an undo counts back through: the ones a person started.
fn is_undo_anchor(message: &Value) -> bool {
    let origin = &message["origin"];
    message["role"] == "user"
        && origin["inTurn"] != true
        && match origin_kind(origin) {
            None | Some("user") => true,
            Some("skill_activation" | "plugin_command") => origin["trigger"] == "user-slash",
            _ => false,
        }
}

/// Journal lines an undo removed from the conversation, numbered from 1 over non-empty
/// lines as Kimi numbers them. Kimi keeps undone lines in the journal and rebuilds only
/// the active branch. An undo journals a branch switch whose `base.line` is the last
/// kept line; older sessions journal a bare `context.undo` that drops the last `count`
/// prompts and everything after them. Compaction also switches branches, but it only
/// shrinks the model's context, so its history stays.
fn undone_lines(bytes: &[u8]) -> Vec<Range<u64>> {
    if memchr::memmem::find(bytes, UNDO_NEEDLE).is_none() {
        return Vec::new();
    }
    let mut undone = Vec::new();
    // Prompts and compactions still on the active branch, as (line, is_compaction).
    let mut anchors: Vec<(u64, bool)> = Vec::new();
    let mut paired_undo_lines = HashSet::new();
    let mut number = 0u64;
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        number += 1;
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        match value["type"].as_str().unwrap_or("") {
            "agent.switched" if value["reason"] == "undo" => {
                let Some(base) = value["base"]["line"].as_u64() else {
                    continue;
                };
                if base + 1 < number {
                    undone.push(base + 1..number);
                }
                anchors.retain(|(line, _)| *line <= base);
                if let Some(legacy) = value["legacyUndoLine"].as_u64() {
                    paired_undo_lines.insert(legacy);
                }
            }
            "context.undo" if !paired_undo_lines.contains(&number) => {
                let count = value["count"].as_u64().unwrap_or(0) as usize;
                let mut remaining = count;
                let mut cut = None;
                for (index, (_, compaction)) in anchors.iter().enumerate().rev() {
                    // An undo never reaches behind a compaction.
                    if *compaction || remaining == 0 {
                        break;
                    }
                    remaining -= 1;
                    if remaining == 0 {
                        cut = Some(index);
                    }
                }
                if let Some(index) = cut {
                    undone.push(anchors[index].0..number);
                    anchors.truncate(index);
                }
            }
            "context.clear" => anchors.clear(),
            "context.apply_compaction" => anchors.push((number, true)),
            "context.append_message" if is_undo_anchor(&value["message"]) => {
                anchors.push((number, false));
            }
            _ => {}
        }
    }
    undone
}

/// Steps the agent loop retried. A failed attempt journals whatever it had streamed
/// before the retry repeats the reply, all in one burst.
fn retried_steps(bytes: &[u8]) -> HashSet<String> {
    let finder = memchr::memmem::Finder::new(RETRY_NEEDLE);
    finder
        .find_iter(bytes)
        .filter_map(|hit| {
            let line_start = memchr::memrchr(b'\n', &bytes[..hit]).map_or(0, |i| i + 1);
            let line_end = hit + memchr(b'\n', &bytes[hit..])?;
            let value = serde_json::from_slice::<Value>(&bytes[line_start..line_end]).ok()?;
            is_retry(&value).then(|| string(&value, "stepId")).flatten()
        })
        .collect()
}

/// Records that only drive the agent runtime (turn lifecycle, token accounting,
/// tool registries, permissions, plans, goals) carry no conversation content.
fn is_runtime_record(kind: &str) -> bool {
    matches!(
        kind,
        "metadata" | "forked" | "context.undo" | "context.undone" | "context.clear"
    ) || matches!(
        kind.split('.').next().unwrap_or(""),
        "turn"
            | "prompt"
            | "llm"
            | "usage"
            | "token_counting"
            | "subagent"
            | "task"
            | "tools"
            | "permission"
            | "plan_mode"
            | "plan"
            | "goal"
            | "cron"
            | "file_history"
            | "full_compaction"
            | "swarm_mode"
            | "tower_mode"
            | "interaction"
            | "mcp"
            | "agent"
            | "profile"
            | "config"
            | "runtime"
            | "plugin"
            | "staleGuard"
            | "interruptionReminder"
            // Older journals prefix the engine store's records, which duplicate the
            // `context.*` projection.
            | "human"
    )
}

/// The text of a prompt someone authored: the user (typed, a `!` shell command, or a
/// slash command), a scheduled cron job, or the parent agent's directive to a subagent.
/// Runtime prompts (reminders, task notifications, stop-hook and goal continuations,
/// shell output, skill bodies) are context, not conversation, and yield `None`.
fn authored_prompt_text(message: &Value) -> Option<String> {
    let origin = &message["origin"];
    match origin_kind(origin) {
        None | Some("user" | "cron_job") => Some(typed_text(message)),
        Some("system_trigger") if origin["name"] == "subagent" => Some(typed_text(message)),
        Some("shell_command") if origin["phase"] == "input" => Some(typed_text(message)),
        // The journal holds the expanded command body; show what was typed instead.
        Some("skill_activation") if origin["trigger"] == "user-slash" => Some(slash_command(
            origin["skillName"].as_str()?,
            &origin["skillArgs"],
        )),
        Some("plugin_command") if origin["trigger"] == "user-slash" => Some(slash_command(
            &format!(
                "{}:{}",
                origin["pluginId"].as_str()?,
                origin["commandName"].as_str()?
            ),
            &origin["commandArgs"],
        )),
        _ => None,
    }
}

fn slash_command(name: &str, args: &Value) -> String {
    match args.as_str().map(str::trim).filter(|args| !args.is_empty()) {
        Some(args) => format!("/{name} {args}"),
        None => format!("/{name}"),
    }
}

/// Text parts a person typed. Kimi tags the skill bodies and hook output it bundles
/// into a prompt with `meta.source`; older prompts list bundled skills in
/// `origin.skillActivations` instead, and their bodies are the first untagged parts.
fn typed_text(message: &Value) -> String {
    let content = &message["content"];
    let Some(parts) = content.as_array() else {
        return parts_text(content);
    };
    let is_tagged = |part: &Value| !part["meta"]["source"].is_null();
    let mut bundled = if parts
        .iter()
        .any(|part| part["meta"]["source"] == "skill activation")
    {
        0
    } else {
        message["origin"]["skillActivations"]
            .as_array()
            .map_or(0, Vec::len)
    };
    parts
        .iter()
        .filter(|part| part["type"] == "text" && !is_tagged(part))
        .filter(|_| {
            let skill_body = bundled > 0;
            bundled = bundled.saturating_sub(1);
            !skill_body
        })
        .filter_map(|part| part["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn json_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        _ => serde_json::to_string(value).unwrap_or_default(),
    }
}

/// Text parts of a content list, joined; a bare string is already the text.
fn parts_text(content: &Value) -> String {
    if let Some(text) = content.as_str() {
        return text.to_string();
    }
    content
        .as_array()
        .map(|parts| {
            parts
                .iter()
                .filter(|part| part["type"] == "text")
                .filter_map(|part| part["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn think_text(content: &Value) -> String {
    content
        .as_array()
        .map(|parts| {
            parts
                .iter()
                .filter(|part| part["type"] == "think")
                .filter_map(|part| part["think"].as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn non_empty_lines(bytes: &[u8]) -> u64 {
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .count() as u64
}

/// `record` as a tool call. Without an event id of its own, the call id identifies it.
fn tool_call(
    mut record: Record,
    call_id: Option<String>,
    name: Option<String>,
    args: Option<&Value>,
) -> Record {
    record.role = "tool_use".into();
    record.tool_name = name;
    record.tool_input = args.map(json_text).filter(|args| !args.is_empty());
    record.text = record.tool_input.clone().unwrap_or_default();
    if record
        .links
        .event_id
        .as_deref()
        .is_some_and(|id| id.starts_with("offset:"))
        && let Some(call_id) = &call_id
    {
        record.links.event_id = Some(call_id.clone());
    }
    record.links.source_tool_use_id = call_id;
    record
}

/// `record` as the result of `call_id`, or `None` when it has nothing to show.
fn tool_result(
    mut record: Record,
    call_id: Option<String>,
    output: String,
    is_error: Option<bool>,
    pending_tool_calls: &mut HashMap<String, PendingToolCall>,
    diagnostics: &mut ParseDiagnostics,
) -> Option<Record> {
    let pending = call_id
        .as_ref()
        .and_then(|id| pending_tool_calls.remove(id));
    if pending.is_none() {
        diagnostics.orphan_tool_results += 1;
    }
    let output = if !output.trim().is_empty() {
        output
    } else if is_error == Some(true) {
        "[tool error]".to_string()
    } else {
        return None;
    };
    record.role = "tool_result".into();
    record.tool_name = pending.as_ref().and_then(|p| p.tool_name.clone());
    record.links.parent_event_id = pending.and_then(|p| p.tool_use_event_id);
    record.links.parent_tool_use_id = call_id;
    record.links.tool_result_is_error = is_error;
    record.tool_output = Some(output.clone());
    record.text = output;
    Some(record)
}

/// Media parts as typed display blocks, in the shape the attachment readers expect.
/// Kimi offloads large inline media to `blobref:<mime>;<sha256>` files in the agent's
/// `blobs` directory, which `blob_dir` names.
fn parts_media(content: &Value, blob_dir: &Path) -> Option<String> {
    let blocks = content
        .as_array()?
        .iter()
        .filter_map(|part| {
            let (kind, key, reference) = match part["type"].as_str()? {
                "image_url" => ("image", "imageUrl", "image_url"),
                "audio_url" => ("document", "audioUrl", "file_url"),
                "video_url" => ("document", "videoUrl", "file_url"),
                _ => return None,
            };
            let url = part[key]["url"].as_str().filter(|url| !url.is_empty())?;
            let blob = url
                .strip_prefix("blobref:")
                .and_then(|reference| reference.rsplit_once(';'))
                .map(|(_, hash)| hash)
                .filter(|hash| !hash.is_empty() && hash.bytes().all(|b| b.is_ascii_hexdigit()));
            let url = match blob {
                Some(hash) => blob_dir.join(hash).to_string_lossy().into_owned(),
                None => url.to_string(),
            };
            let mut block = serde_json::json!({"type": kind, reference: url});
            if let Some(name) = part[key]["name"].as_str() {
                block["title"] = name.into();
            }
            Some(block)
        })
        .collect::<Vec<_>>();
    (!blocks.is_empty()).then(|| serde_json::to_string(&blocks).unwrap_or_default())
}

pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    let file = File::open(path)?;
    let mmap = super::common::map_sequential(&file)?;
    let incremental = state.offset > 0;
    let mut start = super::jsonl::resume_offset(&mmap, state.offset, |line| {
        serde_json::from_slice::<Value>(line).is_ok()
    })
    .max(inherited_prefix_end(&mmap));
    let mut turn_id = state.turn_id;
    let mut pending_tool_calls = state.pending_tool_calls;
    let mut diagnostics = ParseDiagnostics::default();
    let identity = identity(path);
    let source_path = path.to_string_lossy().into_owned();
    let blob_dir = path.with_file_name("blobs");
    let base_links = RecordLinks {
        parent_session_id: identity.parent_session_id.clone(),
        conversation_kind: Some(identity.kind.to_string()),
        thread_source: (identity.kind == "subagent").then(|| "subagent".to_string()),
        ..RecordLinks::default()
    };
    // An incremental pass stops at any undo (below), so whatever earlier undos retracted
    // lies before it and only a full pass needs the retracted lines.
    let undone = if incremental {
        Vec::new()
    } else {
        undone_lines(&mmap)
    };
    let mut line_number = if undone.is_empty() {
        0
    } else {
        non_empty_lines(&mmap[..start])
    };
    let retried = retried_steps(&mmap[start..]);
    let mut last_ts = 0;

    while start < mmap.len() {
        let line_start = start;
        let slice = &mmap[start..];
        let rel = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..rel];
        start += rel + usize::from(rel < slice.len());
        if line.is_empty() {
            continue;
        }
        line_number += 1;
        let value: Value = match serde_json::from_slice(line) {
            Ok(value) => value,
            Err(_) => {
                // An unterminated tail is still being written.
                if rel == slice.len() {
                    start = line_start;
                    break;
                }
                diagnostics.malformed_json_lines += 1;
                continue;
            }
        };
        if !value.is_object() {
            diagnostics.non_object_json_lines += 1;
            continue;
        }
        // An undo or retry retracts lines earlier passes indexed. Leave it for the next
        // refresh, which sees it in the appended bytes and reparses the whole journal.
        if incremental && (is_undo(&value) || is_retry(&value)) {
            start = line_start;
            break;
        }
        let record_type = value["type"].as_str().unwrap_or("");
        // Lines migrated from kimi-cli carry no `time`; they take the last one journaled
        // before them, starting from the header's `created_at`.
        if let Some(time) = value
            .get("time")
            .or_else(|| (record_type == "metadata").then(|| &value["created_at"]))
        {
            last_ts = super::common::timestamp_millis(time);
        }
        if is_runtime_record(record_type) || undone.iter().any(|lines| lines.contains(&line_number))
        {
            continue;
        }
        let template = Record {
            source: SourceKind::Kimi,
            doc_id: 0,
            ts: last_ts,
            project: identity.project.clone(),
            session_id: identity.session_id.clone(),
            turn_id: 0,
            role: String::new(),
            text: String::new(),
            tool_name: None,
            tool_input: None,
            tool_output: None,
            links: RecordLinks {
                event_id: Some(format!("offset:{line_start}")),
                ..base_links.clone()
            },
            source_path: source_path.clone(),
        };
        let mut records = Vec::new();
        match record_type {
            "context.append_message" => {
                let message = &value["message"];
                let mut record = template;
                if let Some(id) = string(message, "id") {
                    record.links.event_id = Some(id);
                }
                match message["role"].as_str().unwrap_or("") {
                    "user" => {
                        let Some(text) = authored_prompt_text(message) else {
                            continue;
                        };
                        record.role = "user".into();
                        record.text = text;
                        record.links.source_content = parts_media(&message["content"], &blob_dir);
                        if !record.text.trim().is_empty() || record.links.source_content.is_some() {
                            records.push(record);
                        }
                    }
                    // Live sessions stream replies as loop events; whole assistant and
                    // tool messages are what kimi-cli migrations journal.
                    "assistant" => {
                        let base_id = record.links.event_id.clone().unwrap_or_default();
                        let content = &message["content"];
                        let reasoning = think_text(content);
                        if include_reasoning && !reasoning.trim().is_empty() {
                            let mut part = record.clone();
                            part.role = "reasoning".into();
                            part.text = reasoning;
                            part.links.event_id = Some(format!("{base_id}:reasoning"));
                            records.push(part);
                        }
                        let text = parts_text(content);
                        if !text.trim().is_empty() {
                            let mut part = record.clone();
                            part.role = "assistant".into();
                            part.text = text;
                            records.push(part);
                        }
                        let calls = message["toolCalls"]
                            .as_array()
                            .map_or(&[][..], Vec::as_slice);
                        for (index, call) in calls.iter().enumerate() {
                            // Migrations keep OpenAI's nested `function` shape.
                            let function = if call["function"].is_object() {
                                &call["function"]
                            } else {
                                call
                            };
                            let mut part = record.clone();
                            part.links.event_id = Some(format!("{base_id}:call:{index}"));
                            records.push(tool_call(
                                part,
                                string(call, "id"),
                                string(function, "name"),
                                function.get("arguments"),
                            ));
                        }
                    }
                    "tool" => {
                        records.extend(tool_result(
                            record,
                            string(message, "toolCallId"),
                            parts_text(&message["content"]),
                            None,
                            &mut pending_tool_calls,
                            &mut diagnostics,
                        ));
                    }
                    "system" => continue,
                    other => {
                        diagnostics.increment_unknown_semantic(other);
                        continue;
                    }
                }
            }
            "context.append_loop_event" => {
                let event = &value["event"];
                let event_type = event["type"].as_str().unwrap_or("");
                let mut record = template;
                if let Some(uuid) = string(event, "uuid") {
                    record.links.event_id = Some(uuid);
                }
                match event_type {
                    "content.part" => {
                        // A retried step's partial reply is repeated by the retry.
                        if event["stepUuid"]
                            .as_str()
                            .is_some_and(|step| retried.contains(step))
                        {
                            continue;
                        }
                        let part = &event["part"];
                        let (role, text) = match part["type"].as_str().unwrap_or("") {
                            "text" => ("assistant", part["text"].as_str()),
                            "think" if include_reasoning => ("reasoning", part["think"].as_str()),
                            "think" | "image_url" | "audio_url" | "video_url" => continue,
                            other => {
                                diagnostics.increment_unknown_semantic(other);
                                continue;
                            }
                        };
                        let text = text.unwrap_or_default();
                        if text.trim().is_empty() {
                            continue;
                        }
                        record.role = role.into();
                        record.text = text.to_string();
                        records.push(record);
                    }
                    "tool.call" => records.push(tool_call(
                        record,
                        string(event, "toolCallId"),
                        string(event, "name"),
                        event.get("args"),
                    )),
                    "tool.result" => {
                        let result = &event["result"];
                        let output = match &result["output"] {
                            output @ Value::Array(_) => parts_text(output),
                            Value::Null => String::new(),
                            other => json_text(other),
                        };
                        records.extend(tool_result(
                            record,
                            string(event, "toolCallId"),
                            output,
                            result["isError"].as_bool(),
                            &mut pending_tool_calls,
                            &mut diagnostics,
                        ));
                    }
                    "step.begin" | "step.end" => continue,
                    "" => {
                        diagnostics.increment_unknown_semantic("missing_loop_event_type");
                        continue;
                    }
                    other => {
                        diagnostics.increment_unknown_semantic(other);
                        continue;
                    }
                }
            }
            "context.apply_compaction" => {
                let summary = &value["summary"];
                let text = summary
                    .as_str()
                    .map(str::to_owned)
                    .or_else(|| string(&value, "contextSummary"))
                    .unwrap_or_else(|| parts_text(&summary["content"]));
                if text.trim().is_empty() {
                    continue;
                }
                let mut record = template;
                record.role = "assistant".into();
                record.text = text;
                record.links.conversation_kind = Some("compaction".into());
                record.links.thread_source = Some("compaction".into());
                records.push(record);
            }
            "" => {
                diagnostics.increment_unknown_top_level("missing_type");
                continue;
            }
            unknown => {
                diagnostics.increment_unknown_top_level(unknown);
                continue;
            }
        }
        for mut record in records {
            record.turn_id = turn_id;
            record.doc_id = next_doc_id.fetch_add(1, Ordering::SeqCst);
            if record.role == "tool_use"
                && let Some(call_id) = record.links.source_tool_use_id.clone()
            {
                let pending = super::common::pending_tool_call(
                    record.tool_name.clone(),
                    record.links.event_id.clone(),
                    record.doc_id,
                    record.ts,
                    record.tool_input.as_deref(),
                    &record.links,
                    &identity.session_id,
                );
                if pending_tool_calls.insert(call_id, pending).is_some() {
                    diagnostics.duplicate_tool_calls += 1;
                }
            }
            emit(record)?;
            turn_id += 1;
        }
    }

    Ok(IndexParseOutput {
        legacy_turn_id: None,
        offset: start as u64,
        turn_id,
        pending_tool_calls,
        session_id: Some(identity.session_id),
        diagnostics,
        session_cwd: identity.cwd,
    })
}

/// One `usage.record` per model request, including the compaction requests journaled
/// with a `session` scope. Subagent journals count as sidechains of their session.
pub(crate) fn parse_usage_file(path: &Path) -> Result<Vec<UsageEvent>> {
    let file = File::open(path)?;
    let mmap = super::common::map_sequential(&file)?;
    let identity = identity(path);
    let source_path: Arc<str> = Arc::from(path.to_string_lossy());
    let inherited = inherited_prefix_end(&mmap);
    let mut start = 0usize;
    let mut order = 0u64;
    let mut events = Vec::new();
    while start < mmap.len() {
        let line_start = start;
        let slice = &mmap[start..];
        let rel = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..rel];
        start += rel + usize::from(rel < slice.len());
        order += 1;
        // Requests before a fork marker were the source session's, counted there.
        if line_start < inherited {
            continue;
        }
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        if value["type"] != "usage.record" {
            continue;
        }
        let Some(usage) = value["usage"].as_object() else {
            continue;
        };
        let count = |key: &str| usage.get(key).and_then(Value::as_u64).unwrap_or(0);
        let uncached_input = count("inputOther");
        let cache_read = count("inputCacheRead");
        let cache_write = count("inputCacheCreation");
        let tokens = TokenBuckets {
            raw_input: uncached_input
                .saturating_add(cache_read)
                .saturating_add(cache_write),
            uncached_input,
            cache_read,
            cache_write,
            cache_write_1h: 0,
            output: count("output"),
            reasoning: 0,
        };
        if tokens.additive_total() == 0 {
            continue;
        }
        // Model aliases are `<provider name>/<model>` as configured in config.toml.
        let model = string(&value, "model");
        let provider = model
            .as_deref()
            .and_then(|alias| alias.split_once('/'))
            .map(|(provider, _)| provider.to_string());
        events.push(UsageEvent {
            source: "kimi",
            source_path: source_path.clone(),
            source_record_id: Some(format!("line:{order}")),
            session_id: Some(identity.session_id.clone()),
            request_id: None,
            message_id: None,
            timestamp_ms: super::common::timestamp_millis(&value["time"]),
            project: identity.cwd.clone(),
            provider,
            model,
            tokens,
            credits: None,
            token_usage_available: true,
            source_cost_usd: None,
            cost_authoritative: false,
            dedupe_confidence: "exact",
            conservative_undercount: false,
            cache_chain_excluded: false,
            sidechain: identity.kind == "subagent",
            permission_review: false,
            source_order: order,
        });
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::io::Write;

    fn line(value: Value) -> String {
        value.to_string() + "\n"
    }

    fn loop_event(agent: &str, time: u64, event: Value) -> String {
        line(
            json!({"type": "context.append_loop_event", "agentId": agent, "event": event, "time": time}),
        )
    }

    fn prompt(agent: &str, time: u64, origin: Value, text: &str) -> String {
        line(
            json!({"type": "context.append_message", "agentId": agent, "time": time,
            "message": {"role": "user", "origin": origin, "toolCalls": [],
                "content": [{"type": "text", "text": text}]}}),
        )
    }

    fn session(root: &Path, id: &str) -> PathBuf {
        let dir = root.join("wd_project_0123456789ab").join(id);
        fs::create_dir_all(dir.join("agents/main")).unwrap();
        dir
    }

    fn parse(
        path: &Path,
        state: IndexParseState,
        reasoning: bool,
    ) -> (Vec<Record>, IndexParseOutput) {
        let mut records = Vec::new();
        let output = parse_index_records(path, state, reasoning, &AtomicU64::new(1), |record| {
            records.push(record);
            Ok(())
        })
        .unwrap();
        (records, output)
    }

    fn resume(output: &IndexParseOutput) -> IndexParseState {
        IndexParseState {
            offset: output.offset,
            turn_id: output.turn_id,
            legacy_turn_id: output.legacy_turn_id,
            pending_tool_calls: output.pending_tool_calls.clone(),
        }
    }

    #[test]
    fn discovers_only_agent_journals() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "session_a");
        fs::create_dir_all(dir.join("agents/agent-0")).unwrap();
        fs::create_dir_all(dir.join("agents/main/tasks/bash-1")).unwrap();
        fs::create_dir_all(temp.path().join(".index-cache")).unwrap();
        for relative in [
            "agents/main/wire.jsonl",
            "agents/agent-0/wire.jsonl",
            "wire.jsonl",
            "state.json",
            "agents/main/tasks/bash-1/wire.jsonl",
            "agents/main/output.log",
        ] {
            fs::write(dir.join(relative), "").unwrap();
        }
        fs::write(temp.path().join(".index-cache/scan.json"), "{}").unwrap();
        let paths = discover_from_root(temp.path())
            .into_iter()
            .map(|file| {
                assert_eq!(file.source, SourceKind::Kimi);
                file.path.strip_prefix(&dir).unwrap().to_path_buf()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            [
                PathBuf::from("agents/agent-0/wire.jsonl"),
                PathBuf::from("agents/main/wire.jsonl")
            ]
        );
        assert!(matches_path(
            "/Users/me/.kimi-code/sessions/wd_x_1/session_a/agents/main/wire.jsonl"
        ));
        assert!(!matches_path(
            "/Users/me/.kimi-code/sessions/wd_x_1/session_a/state.json"
        ));
        assert!(!matches_path(
            "/Users/me/.kimi/sessions/md5/session/wire.jsonl"
        ));
    }

    #[test]
    fn parses_prompts_replies_tools_and_compaction_across_appends() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "session_a");
        fs::write(
            dir.join("state.json"),
            json!({"id": "session_a", "version": 2, "cwd": "/work/repo", "title": "Fix the build",
                "lastPrompt": "fix it", "agents": {"main": {"type": "main"}}})
            .to_string(),
        )
        .unwrap();
        let path = dir.join("agents/main/wire.jsonl");
        let header = line(json!({"type": "metadata", "protocol_version": "1.5", "created_at": 1}))
            + &line(
                json!({"type": "runtime.set_binding", "agentId": "main", "workspaceId": "wd", "runtimeId": "local", "time": 2}),
            );
        let prefix = header.clone()
            + &prompt("main", 3, json!({"kind": "user"}), "fix the build")
            + &prompt(
                "main",
                4,
                json!({"kind": "injection", "variant": "todo_list_reminder"}),
                "<system-reminder>todo</system-reminder>",
            )
            + &loop_event(
                "main",
                5,
                json!({"type": "step.begin", "uuid": "step-1", "turnId": "0", "step": 1}),
            )
            + &line(
                json!({"type": "llm.request", "agentId": "main", "kind": "loop", "provider": "openai", "model": "k3", "time": 5}),
            )
            + &line(
                json!({"type": "usage.record", "agentId": "main", "model": "kimi-code/k3", "usage": {"inputOther": 10, "output": 5, "inputCacheRead": 20, "inputCacheCreation": 0}, "usageScope": "turn", "time": 6}),
            )
            + &loop_event(
                "main",
                7,
                json!({"type": "content.part", "uuid": "think-1", "stepUuid": "step-1", "part": {"type": "think", "think": "private plan"}}),
            )
            + &loop_event(
                "main",
                8,
                json!({"type": "content.part", "uuid": "text-1", "stepUuid": "step-1", "part": {"type": "text", "text": "Let me look."}}),
            )
            + &loop_event(
                "main",
                9,
                json!({"type": "tool.call", "uuid": "call-1", "stepUuid": "step-1", "toolCallId": "tool_1", "name": "Bash", "args": {"command": "cargo build"}}),
            );
        fs::write(&path, &prefix).unwrap();
        let (mut records, first) = parse(&path, IndexParseState::default(), false);
        assert_eq!(first.offset, prefix.len() as u64);
        assert_eq!(first.session_id.as_deref(), Some("session_a"));
        assert_eq!(first.session_cwd.as_deref(), Some("/work/repo"));
        assert!(first.diagnostics.is_empty());
        assert_eq!(first.pending_tool_calls.len(), 1);
        assert_eq!(
            records
                .iter()
                .map(|r| (r.role.as_str(), r.turn_id))
                .collect::<Vec<_>>(),
            [("user", 0), ("assistant", 1), ("tool_use", 2)]
        );
        assert_eq!(records[0].text, "fix the build");
        assert_eq!(records[0].project, "repo");
        // Prompts without a message id are identified by their byte offset.
        assert_eq!(
            records[0].links.event_id.as_deref(),
            Some(format!("offset:{}", header.len()).as_str())
        );
        assert_eq!(records[0].links.conversation_kind.as_deref(), Some("main"));
        assert_eq!(records[1].links.event_id.as_deref(), Some("text-1"));
        assert_eq!(records[2].tool_name.as_deref(), Some("Bash"));
        assert_eq!(
            records[2].tool_input.as_deref(),
            Some(r#"{"command":"cargo build"}"#)
        );
        assert_eq!(
            records[2].links.source_tool_use_id.as_deref(),
            Some("tool_1")
        );
        assert_eq!(records[2].ts, 9);

        let suffix = loop_event(
            "main",
            10,
            json!({"type": "tool.result", "parentUuid": "call-1", "toolCallId": "tool_1",
                "result": {"output": "Finished", "note": "<system>exit 0</system>", "durationMs": 3}}),
        ) + &loop_event(
            "main",
            11,
            json!({"type": "tool.result", "toolCallId": "tool_1", "result": {"output": "", "isError": true}}),
        ) + &loop_event(
            "main",
            12,
            json!({"type": "step.end", "uuid": "step-1", "finishReason": "stop", "usage": {"inputOther": 1, "output": 1, "inputCacheRead": 0, "inputCacheCreation": 0}}),
        ) + &line(
            json!({"type": "context.apply_compaction", "agentId": "main", "summary": "Built the project.", "compactedCount": 4, "time": 13}),
        ) + &line(
            json!({"type": "agent.message.appended", "kind": "event", "message": {"message": {"role": "assistant", "content": [{"type": "text", "text": "duplicate projection"}]}}, "time": 14}),
        ) + &line(
            json!({"type": "human.agent.message.appended", "message": {"role": "assistant", "content": "legacy projection"}, "time": 14}),
        ) + &line(
            json!({"type": "context.future_record", "agentId": "main", "time": 15}),
        ) + "not json\n[]\n";
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(suffix.as_bytes())
            .unwrap();
        let (appended, second) = parse(&path, resume(&first), false);
        records.extend(appended);
        assert_eq!(second.offset, (prefix.len() + suffix.len()) as u64);
        assert!(second.pending_tool_calls.is_empty());
        assert_eq!(second.diagnostics.malformed_json_lines, 1);
        assert_eq!(second.diagnostics.non_object_json_lines, 1);
        assert_eq!(second.diagnostics.orphan_tool_results, 1);
        assert_eq!(
            second
                .diagnostics
                .unknown_top_level_types
                .get("context.future_record"),
            Some(&1)
        );
        assert_eq!(second.diagnostics.unknown_top_level_types.len(), 1);
        assert_eq!(
            records
                .iter()
                .map(|r| (r.role.as_str(), r.turn_id))
                .collect::<Vec<_>>(),
            [
                ("user", 0),
                ("assistant", 1),
                ("tool_use", 2),
                ("tool_result", 3),
                ("tool_result", 4),
                ("assistant", 5)
            ]
        );
        assert_eq!(records[3].text, "Finished");
        assert_eq!(records[3].tool_name.as_deref(), Some("Bash"));
        assert_eq!(records[3].links.parent_event_id.as_deref(), Some("call-1"));
        assert_eq!(
            records[3].links.parent_tool_use_id.as_deref(),
            Some("tool_1")
        );
        assert_eq!(records[3].links.tool_result_is_error, None);
        assert_eq!(records[4].text, "[tool error]");
        assert_eq!(records[4].tool_name, None);
        assert_eq!(records[4].links.tool_result_is_error, Some(true));
        assert_eq!(records[5].text, "Built the project.");
        assert_eq!(
            records[5].links.conversation_kind.as_deref(),
            Some("compaction")
        );
        assert_eq!(
            records[5].links.thread_source.as_deref(),
            Some("compaction")
        );
        assert!(
            !records
                .iter()
                .any(|r| r.text.contains("duplicate projection") || r.text.contains("todo"))
        );

        let (full, _) = parse(&path, IndexParseState::default(), false);
        assert_eq!(
            full.iter()
                .map(|r| (&r.text, r.turn_id))
                .collect::<Vec<_>>(),
            records
                .iter()
                .map(|r| (&r.text, r.turn_id))
                .collect::<Vec<_>>()
        );
        for (incremental, complete) in records.iter().zip(&full) {
            assert_eq!(
                crate::retrieval::canonical_record_id(incremental),
                crate::retrieval::canonical_record_id(complete)
            );
        }
        let (reasoning, _) = parse(&path, IndexParseState::default(), true);
        assert_eq!(reasoning.len(), full.len() + 1);
        assert_eq!(reasoning[1].role, "reasoning");
        assert_eq!(reasoning[1].text, "private plan");
        assert_eq!(reasoning[1].links.event_id.as_deref(), Some("think-1"));
        assert_eq!(session_title(&path).as_deref(), Some("Fix the build"));
    }

    #[test]
    fn subagent_journal_is_its_own_session_under_the_parent() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "session_a");
        fs::write(
            dir.join("state.json"),
            json!({"id": "session_a", "cwd": "/work/repo", "title": "Parent title"}).to_string(),
        )
        .unwrap();
        fs::create_dir_all(dir.join("agents/agent-0")).unwrap();
        let path = dir.join("agents/agent-0/wire.jsonl");
        // The session title names the main conversation, not its subagents.
        assert_eq!(
            session_title(&dir.join("agents/main/wire.jsonl")).as_deref(),
            Some("Parent title")
        );
        assert_eq!(session_title(&path), None);
        fs::write(
            &path,
            prompt("agent-0", 1, json!({"kind": "system_trigger", "name": "subagent"}), "Review the tests")
                + &prompt("agent-0", 2, json!({"kind": "task", "taskId": "bash-1"}), "<notification>done</notification>")
                + &loop_event("agent-0", 3, json!({"type": "content.part", "uuid": "t", "stepUuid": "s", "part": {"type": "text", "text": "Reviewed."}}))
                + &line(json!({"type": "usage.record", "agentId": "agent-0", "model": "ollama-cloud/kimi-k3", "usage": {"inputOther": 7, "output": 3, "inputCacheRead": 0, "inputCacheCreation": 2}, "time": 4})),
        )
        .unwrap();
        let (records, output) = parse(&path, IndexParseState::default(), false);
        assert_eq!(output.session_id.as_deref(), Some("session_a:agent-0"));
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].role, "user");
        assert_eq!(records[0].text, "Review the tests");
        for record in &records {
            assert_eq!(record.session_id, "session_a:agent-0");
            assert_eq!(record.links.parent_session_id.as_deref(), Some("session_a"));
            assert_eq!(record.links.conversation_kind.as_deref(), Some("subagent"));
            assert_eq!(record.links.thread_source.as_deref(), Some("subagent"));
            assert_eq!(record.project, "repo");
        }
        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].session_id.as_deref(), Some("session_a:agent-0"));
        assert!(events[0].sidechain);
        assert_eq!(events[0].provider.as_deref(), Some("ollama-cloud"));
        assert_eq!(events[0].model.as_deref(), Some("ollama-cloud/kimi-k3"));
        assert_eq!(events[0].tokens.cache_write, 2);
        assert_eq!(events[0].tokens.raw_input, 9);
    }

    #[test]
    fn metadata_fallbacks_and_fingerprint() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "session_b");
        let path = dir.join("agents/main/wire.jsonl");
        fs::write(
            &path,
            line(json!({"type": "metadata", "protocol_version": "1.4", "created_at": 1}))
                + &line(json!({"type": "profile.bind", "agentId": "main", "thinkingEffort": "high",
                    "systemPrompt": "", "disallowedTools": [], "environmentDisclosure": {"cwd": "/work/disclosed"}, "time": 2}))
                + &prompt("main", 3, json!({"kind": "user"}), "hello"),
        )
        .unwrap();
        // No state.json: the directory names the session and the journal discloses the cwd.
        let missing = metadata_fingerprint(&path);
        assert_eq!(session_cwd(&path).as_deref(), Some("/work/disclosed"));
        let (records, output) = parse(&path, IndexParseState::default(), false);
        assert_eq!(records[0].session_id, "session_b");
        assert_eq!(records[0].project, "disclosed");
        assert_eq!(output.session_cwd.as_deref(), Some("/work/disclosed"));
        assert_eq!(session_title(&path), None);
        // Legacy migrated sessions record `workDir` instead of `cwd`.
        fs::write(
            dir.join("state.json"),
            json!({"workDir": "/work/legacy", "lastPrompt": "a", "updatedAt": 1}).to_string(),
        )
        .unwrap();
        let legacy = metadata_fingerprint(&path);
        assert_ne!(missing, legacy);
        assert_eq!(session_cwd(&path).as_deref(), Some("/work/legacy"));
        fs::write(
            dir.join("state.json"),
            json!({"workDir": "/work/legacy", "lastPrompt": "b", "updatedAt": 2}).to_string(),
        )
        .unwrap();
        assert_eq!(legacy, metadata_fingerprint(&path));
        fs::write(
            dir.join("state.json"),
            json!({"workDir": "/work/legacy", "title": "Named", "forkedFrom": "session_a"})
                .to_string(),
        )
        .unwrap();
        assert_ne!(legacy, metadata_fingerprint(&path));
        assert_eq!(session_title(&path).as_deref(), Some("Named"));
        let (records, _) = parse(&path, IndexParseState::default(), false);
        assert_eq!(records[0].links.conversation_kind.as_deref(), Some("fork"));
        assert_eq!(
            records[0].links.parent_session_id.as_deref(),
            Some("session_a")
        );
        // Older sessions keep their metadata under `session-meta/`, read when the
        // session root has none; the directory names the session either way.
        fs::remove_file(dir.join("state.json")).unwrap();
        fs::create_dir_all(dir.join("session-meta")).unwrap();
        fs::write(
            dir.join("session-meta/state.json"),
            json!({"id": "renamed", "version": 1, "cwd": "/work/meta", "title": "Legacy title"})
                .to_string(),
        )
        .unwrap();
        assert_eq!(session_title(&path).as_deref(), Some("Legacy title"));
        assert_eq!(session_cwd(&path).as_deref(), Some("/work/meta"));
        let (records, _) = parse(&path, IndexParseState::default(), false);
        assert_eq!(records[0].session_id, "session_b");
        assert_eq!(records[0].project, "meta");
    }

    #[test]
    fn prompts_keep_attachments_and_tool_results_flatten_parts() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "session_c");
        let path = dir.join("agents/main/wire.jsonl");
        fs::write(
            &path,
            line(json!({"type": "context.append_message", "agentId": "main", "time": 1, "message": {"role": "user",
                "origin": "user", "id": "msg_1", "content": [
                    {"type": "image_url", "imageUrl": {"url": "file:///tmp/shot.png", "name": "shot.png"}},
                    {"type": "text", "text": "What is this?"}]}}))
                + &loop_event("main", 2, json!({"type": "tool.call", "toolCallId": "t1", "name": "ReadMediaFile", "args": "raw args"}))
                + &loop_event("main", 3, json!({"type": "tool.result", "toolCallId": "t1", "result": {"output": [
                    {"type": "text", "text": "<image>"}, {"type": "image_url", "imageUrl": {"url": "data:image/png;base64,AAAA"}},
                    {"type": "text", "text": "</image>"}]}}))
                + &loop_event("main", 4, json!({"type": "tool.call", "toolCallId": "t2", "name": "Glob", "args": {}}))
                + &loop_event("main", 5, json!({"type": "tool.result", "toolCallId": "t2", "result": {"output": ""}}))
                + &line(json!({"type": "context.apply_compaction", "agentId": "main", "count": 2, "time": 6,
                    "summary": {"role": "user", "content": [{"type": "text", "text": "Earlier context"}]}})),
        )
        .unwrap();
        let (records, output) = parse(&path, IndexParseState::default(), false);
        assert!(output.diagnostics.is_empty());
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].links.event_id.as_deref(), Some("msg_1"));
        assert_eq!(records[0].text, "What is this?");
        assert_eq!(
            serde_json::from_str::<Value>(records[0].links.source_content.as_deref().unwrap())
                .unwrap(),
            json!([{"type": "image", "image_url": "file:///tmp/shot.png", "title": "shot.png"}])
        );
        assert_eq!(records[1].links.event_id.as_deref(), Some("t1"));
        assert_eq!(records[1].tool_input.as_deref(), Some("raw args"));
        assert_eq!(records[2].text, "<image>\n</image>");
        assert_eq!(records[2].tool_name.as_deref(), Some("ReadMediaFile"));
        assert_eq!(records[3].role, "tool_use");
        assert_eq!(records[3].tool_input.as_deref(), Some("{}"));
        assert_eq!(records[4].text, "Earlier context");
        assert_eq!(
            records[4].links.conversation_kind.as_deref(),
            Some("compaction")
        );
    }

    #[test]
    fn forked_session_skips_history_inherited_from_its_source() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "session_f");
        fs::write(
            dir.join("state.json"),
            json!({"id": "session_f", "version": 2, "cwd": "/work/repo", "forkedFrom": "session_a"})
                .to_string(),
        )
        .unwrap();
        let usage = |time: u64, output: u64| {
            line(
                json!({"type": "usage.record", "agentId": "main", "model": "kimi-code/k3",
                "usage": {"inputOther": 10, "output": output, "inputCacheRead": 0, "inputCacheCreation": 0},
                "usageScope": "turn", "time": time}),
            )
        };
        let path = dir.join("agents/main/wire.jsonl");
        let inherited = line(json!({"type": "metadata", "protocol_version": "1.5", "created_at": 1}))
            + &prompt("main", 2, json!({"kind": "user"}), "source prompt")
            // Tool arguments can spell the marker without being one.
            + &loop_event("main", 3, json!({"type": "tool.call", "toolCallId": "t1", "name": "Echo", "args": {"type": "forked"}}))
            + &usage(4, 5)
            + &loop_event("main", 5, json!({"type": "content.part", "uuid": "a", "part": {"type": "text", "text": "source reply"}}));
        let forked = inherited.clone()
            + &line(json!({"type": "forked", "agentId": "main", "time": 6}))
            + &prompt("main", 7, json!({"kind": "user"}), "fork prompt")
            + &usage(8, 7);
        fs::write(&path, &forked).unwrap();

        let (records, output) = parse(&path, IndexParseState::default(), false);
        assert_eq!(output.offset, forked.len() as u64);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].text, "fork prompt");
        assert_eq!(records[0].links.conversation_kind.as_deref(), Some("fork"));
        assert_eq!(
            records[0].links.parent_session_id.as_deref(),
            Some("session_a")
        );
        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tokens.output, 7);
        assert_eq!(events[0].source_record_id.as_deref(), Some("line:8"));

        // A marker still being written does not hide the history before it.
        fs::write(
            &path,
            inherited.clone() + r#"{"type":"forked","agentId":"main","time":6}"#,
        )
        .unwrap();
        assert_eq!(parse_usage_file(&path).unwrap().len(), 1);
        assert_eq!(inherited_prefix_end(&fs::read(&path).unwrap()), 0);
    }

    #[test]
    fn only_authored_prompt_text_is_indexed() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "session_o");
        let path = dir.join("agents/main/wire.jsonl");
        let message = |time: u64, origin: Value, content: Value| {
            line(
                json!({"type": "context.append_message", "agentId": "main", "time": time,
                "message": {"role": "user", "origin": origin, "toolCalls": [], "content": content}}),
            )
        };
        let text = |text: &str| json!([{"type": "text", "text": text}]);
        fs::write(
            &path,
            // Bundled skill bodies and hook output ride along with typed text.
            message(1, json!({"kind": "user"}), json!([
                {"type": "text", "text": "skill body", "meta": {"source": "skill activation", "activationId": "a1"}},
                {"type": "text", "text": "typed prompt"},
                {"type": "text", "text": "<hook/>", "meta": {"contentType": "text/xml", "source": "user prompt submit hook"}}]))
                // Older prompts list bundled skills on the origin instead of tagging parts.
                + &message(2, json!({"kind": "user", "skillActivations": [{"activationId": "a2"}]}),
                    json!([{"type": "text", "text": "legacy skill body"}, {"type": "text", "text": "legacy typed"}]))
                + &message(3, json!({"kind": "system_trigger", "name": "stop_hook"}), text("hook says continue"))
                + &message(4, json!({"kind": "system_trigger", "name": "goal_continuation"}), text("keep working"))
                + &message(5, json!({"kind": "shell_command", "phase": "input"}), text("<bash-input>\nls\n</bash-input>"))
                + &message(6, json!({"kind": "shell_command", "phase": "output"}), text("<bash-stdout>a</bash-stdout>"))
                + &message(7, json!({"kind": "skill_activation", "trigger": "user-slash", "skillName": "review", "skillArgs": " src "}),
                    text("User activated the skill \"review\". <skill-loaded>long body</skill-loaded>"))
                + &message(8, json!({"kind": "skill_activation", "trigger": "model-tool", "skillName": "review"}), text("model skill body"))
                + &message(9, json!({"kind": "plugin_command", "trigger": "user-slash", "pluginId": "deploy", "commandName": "ship"}),
                    text("expanded plugin body"))
                + &message(10, json!({"kind": "cron_job"}), text("nightly report")),
        )
        .unwrap();
        let (records, output) = parse(&path, IndexParseState::default(), false);
        assert!(output.diagnostics.is_empty());
        assert_eq!(
            records
                .iter()
                .map(|r| (r.role.as_str(), r.text.as_str()))
                .collect::<Vec<_>>(),
            [
                ("user", "typed prompt"),
                ("user", "legacy typed"),
                ("user", "<bash-input>\nls\n</bash-input>"),
                ("user", "/review src"),
                ("user", "/deploy:ship"),
                ("user", "nightly report"),
            ]
        );
    }

    #[test]
    fn forked_subagent_skips_the_replayed_parent_context() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "session_s");
        fs::create_dir_all(dir.join("agents/agent-0")).unwrap();
        let path = dir.join("agents/agent-0/wire.jsonl");
        let replayed = |message: Value| {
            line(
                json!({"type": "context.append_message", "agentId": "agent-0", "time": 1, "message": message}),
            )
        };
        fs::write(
            &path,
            line(json!({"type": "metadata", "protocol_version": "1.5", "created_at": 1}))
                + &replayed(json!({"role": "user", "origin": {"kind": "user"}, "toolCalls": [],
                    "content": [{"type": "text", "text": "parent prompt"}]}))
                + &replayed(json!({"role": "assistant", "content": [{"type": "text", "text": "parent reply"}],
                    "toolCalls": [{"type": "function", "id": "p1", "function": {"name": "Read", "arguments": "{}"}}]}))
                + &replayed(json!({"role": "tool", "toolCallId": "p1", "toolCalls": [],
                    "content": [{"type": "text", "text": "parent tool output"}]}))
                + &replayed(json!({"role": "user", "toolCalls": [],
                    "origin": {"kind": "injection", "variant": "fork_context"},
                    "content": [{"type": "text", "text": "The conversation above is not your own history"}]}))
                + &prompt("agent-0", 2, json!({"kind": "system_trigger", "name": "subagent"}), "Check the tests")
                + &loop_event("agent-0", 3, json!({"type": "content.part", "uuid": "c", "part": {"type": "text", "text": "Checked."}})),
        )
        .unwrap();
        let (records, output) = parse(&path, IndexParseState::default(), false);
        assert!(output.pending_tool_calls.is_empty());
        assert_eq!(
            records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            ["Check the tests", "Checked."]
        );
    }

    #[test]
    fn undone_turns_are_dropped_and_appended_undos_wait_for_a_full_parse() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "session_u");
        let path = dir.join("agents/main/wire.jsonl");
        let reply = |time: u64, text: &str| {
            loop_event(
                "main",
                time,
                json!({"type": "content.part", "uuid": format!("r{time}"), "part": {"type": "text", "text": text}}),
            )
        };
        // Lines: 1 metadata, 2-3 first turn, 4-5 second turn, 6 a compaction switch
        // that keeps history, 7-9 the undo triple cutting back to line 3, 10 new turn.
        let kept = line(json!({"type": "metadata", "protocol_version": "1.5", "created_at": 1}))
            + &prompt("main", 2, json!({"kind": "user"}), "first")
            + &reply(3, "first reply")
            + &prompt("main", 4, json!({"kind": "user"}), "second")
            + &reply(5, "second reply")
            + &line(
                json!({"type": "agent.switched", "agentId": "main", "branch": "b1", "reason": "compaction",
                "base": {"branch": "main", "line": 1}, "time": 6}),
            );
        let undo = line(
            json!({"type": "agent.switched", "agentId": "main", "branch": "b2", "reason": "undo",
                "base": {"branch": "main", "line": 3}, "turns": 1, "legacyUndoLine": 8, "time": 7}),
        ) + &line(
            json!({"type": "context.undo", "agentId": "main", "count": 1, "time": 7}),
        ) + &line(
            json!({"type": "context.undone", "agentId": "main", "turns": 1, "fromTurnId": 1, "time": 7}),
        ) + &prompt("main", 8, json!({"kind": "user"}), "third");
        fs::write(&path, &kept).unwrap();
        let (records, first) = parse(&path, IndexParseState::default(), false);
        assert_eq!(records.len(), 4);
        assert!(!has_retraction_since(&path, 0, kept.len() as u64).unwrap());

        // An incremental pass stops at the undo instead of indexing past it ...
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(undo.as_bytes())
            .unwrap();
        let (appended, second) = parse(&path, resume(&first), false);
        assert!(appended.is_empty());
        assert!(second.offset < (kept.len() + undo.len()) as u64);
        // ... so the refresh that follows sees it in the tail and reparses everything.
        assert!(
            has_retraction_since(&path, second.offset, (kept.len() + undo.len()) as u64).unwrap()
        );
        let (full, _) = parse(&path, IndexParseState::default(), false);
        assert_eq!(
            full.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            ["first", "first reply", "third"]
        );

        // Older sessions journal a bare undo that drops the last `count` prompts.
        fs::write(
            &path,
            kept.clone()
                + &line(json!({"type": "context.undo", "agentId": "main", "count": 1, "time": 7}))
                + &prompt("main", 8, json!({"kind": "user"}), "retyped"),
        )
        .unwrap();
        let (legacy, _) = parse(&path, IndexParseState::default(), false);
        assert_eq!(
            legacy.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            ["first", "first reply", "retyped"]
        );
    }

    #[test]
    fn retried_step_drops_the_failed_partial_reply() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "session_r");
        let path = dir.join("agents/main/wire.jsonl");
        let part = |time: u64, step: &str, text: &str| {
            loop_event(
                "main",
                time,
                json!({"type": "content.part", "uuid": format!("p{time}"), "stepUuid": step, "part": {"type": "text", "text": text}}),
            )
        };
        fs::write(
            &path,
            prompt("main", 1, json!({"kind": "user"}), "explain")
                + &loop_event("main", 2, json!({"type": "step.begin", "uuid": "S1", "step": 1}))
                + &part(3, "S1", "Partial expl")
                + &loop_event("main", 4, json!({"type": "step.end", "uuid": "S1", "step": 1, "finishReason": "error"}))
                + &line(json!({"type": "turn.step.retrying", "agentId": "main", "turnId": 0, "step": 1, "stepId": "S1",
                    "failedAttempt": 1, "nextAttempt": 2, "time": 5}))
                + &loop_event("main", 6, json!({"type": "step.begin", "uuid": "S2", "step": 1}))
                + &part(7, "S2", "Full explanation")
                // An interrupted step's partial reply was shown and stays.
                + &part(8, "S3", "Interrupted partial")
                + &loop_event("main", 9, json!({"type": "step.end", "uuid": "S3", "step": 2, "finishReason": "interrupted"})),
        )
        .unwrap();
        let (records, _) = parse(&path, IndexParseState::default(), false);
        assert_eq!(
            records.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            ["explain", "Full explanation", "Interrupted partial"]
        );
    }

    #[test]
    fn appended_retry_of_an_indexed_partial_waits_for_a_full_parse() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "session_ri");
        let path = dir.join("agents/main/wire.jsonl");
        let part = |time: u64, step: &str, text: &str| {
            loop_event(
                "main",
                time,
                json!({"type": "content.part", "uuid": format!("p{time}"), "stepUuid": step, "part": {"type": "text", "text": text}}),
            )
        };
        let streamed = prompt("main", 1, json!({"kind": "user"}), "explain")
            + &loop_event(
                "main",
                2,
                json!({"type": "step.begin", "uuid": "S1", "step": 1}),
            )
            + &part(3, "S1", "Partial expl");
        let retry = loop_event(
            "main",
            4,
            json!({"type": "step.end", "uuid": "S1", "step": 1, "finishReason": "error"}),
        ) + &line(
            json!({"type": "turn.step.retrying", "agentId": "main", "turnId": 0, "step": 1, "stepId": "S1",
                "failedAttempt": 1, "nextAttempt": 2, "time": 5}),
        ) + &loop_event(
            "main",
            6,
            json!({"type": "step.begin", "uuid": "S2", "step": 1}),
        ) + &part(7, "S2", "Full explanation");
        fs::write(&path, &streamed).unwrap();
        // A refresh indexes the partial reply while the step is still streaming.
        let (indexed, first) = parse(&path, IndexParseState::default(), false);
        assert_eq!(
            indexed.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            ["explain", "Partial expl"]
        );
        assert!(!has_retraction_since(&path, first.offset, streamed.len() as u64).unwrap());

        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(retry.as_bytes())
            .unwrap();
        let size = (streamed.len() + retry.len()) as u64;
        // The incremental pass stops at the retry, and the next refresh reparses fully.
        let (appended, second) = parse(&path, resume(&first), false);
        assert!(appended.is_empty());
        assert!(has_retraction_since(&path, second.offset, size).unwrap());
        assert!(has_retraction_since(&path, first.offset, size).unwrap());
        let (full, _) = parse(&path, IndexParseState::default(), false);
        assert_eq!(
            full.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            ["explain", "Full explanation"]
        );
    }

    #[test]
    fn migrated_kimi_cli_sessions_keep_tools_reasoning_and_time() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "ses_migrated");
        let path = dir.join("agents/main/wire.jsonl");
        let message =
            |message: Value| line(json!({"type": "context.append_message", "message": message}));
        fs::write(
            &path,
            line(json!({"type": "metadata", "protocol_version": "1.5", "created_at": 1000}))
                + &message(json!({"role": "user", "toolCalls": [], "content": [{"type": "text", "text": "list files"}]}))
                + &message(json!({"role": "assistant",
                    "content": [{"type": "think", "think": "use the shell"}, {"type": "text", "text": "Listing."}],
                    "toolCalls": [{"type": "function", "id": "call_1", "function": {"name": "Shell", "arguments": "{\"command\":\"ls\"}"}}]}))
                + &message(json!({"role": "tool", "toolCallId": "call_1", "toolCalls": [],
                    "content": [{"type": "text", "text": "README.md"}]}))
                + &line(json!({"type": "turn.ended", "turnId": 0, "time": 2000}))
                + &prompt("main", 3000, json!({"kind": "user"}), "thanks"),
        )
        .unwrap();
        let (records, output) = parse(&path, IndexParseState::default(), true);
        assert!(output.diagnostics.is_empty());
        assert!(output.pending_tool_calls.is_empty());
        assert_eq!(
            records
                .iter()
                .map(|r| (r.role.as_str(), r.text.as_str(), r.ts))
                .collect::<Vec<_>>(),
            [
                ("user", "list files", 1000),
                ("reasoning", "use the shell", 1000),
                ("assistant", "Listing.", 1000),
                ("tool_use", r#"{"command":"ls"}"#, 1000),
                ("tool_result", "README.md", 1000),
                ("user", "thanks", 3000),
            ]
        );
        assert_eq!(records[0].session_id, "ses_migrated");
        assert_eq!(records[3].tool_name.as_deref(), Some("Shell"));
        assert_eq!(records[3].links.event_id.as_deref(), Some("call_1"));
        assert_eq!(records[4].tool_name.as_deref(), Some("Shell"));
        assert_eq!(
            records[4].links.parent_tool_use_id.as_deref(),
            Some("call_1")
        );
        let event_ids = records
            .iter()
            .map(|r| r.links.event_id.clone())
            .collect::<HashSet<_>>();
        assert_eq!(event_ids.len(), records.len());
    }

    #[test]
    fn offloaded_media_points_at_the_agent_blob() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "session_m");
        let path = dir.join("agents/main/wire.jsonl");
        fs::write(
            &path,
            line(json!({"type": "context.append_message", "agentId": "main", "time": 1, "message": {"role": "user",
                "origin": {"kind": "user"}, "toolCalls": [], "content": [
                    {"type": "image_url", "imageUrl": {"url": "blobref:image/png;0a1b2c"}},
                    {"type": "text", "text": "what is this"}]}})),
        )
        .unwrap();
        let (records, _) = parse(&path, IndexParseState::default(), false);
        let blob = dir.join("agents/main/blobs/0a1b2c");
        assert_eq!(
            serde_json::from_str::<Value>(records[0].links.source_content.as_deref().unwrap())
                .unwrap(),
            json!([{"type": "image", "image_url": blob.to_string_lossy()}])
        );
    }

    #[test]
    fn usage_counts_every_request_including_compaction() {
        let temp = tempfile::tempdir().unwrap();
        let dir = session(temp.path(), "session_d");
        fs::write(
            dir.join("state.json"),
            json!({"id": "session_d", "cwd": "/work/repo"}).to_string(),
        )
        .unwrap();
        let path = dir.join("agents/main/wire.jsonl");
        fs::write(
            &path,
            line(json!({"type": "usage.record", "agentId": "main", "model": "kimi-code/k3", "usage": {"inputOther": 100, "output": 20, "inputCacheRead": 300, "inputCacheCreation": 0}, "usageScope": "turn", "time": 1000}))
                + &line(json!({"type": "usage.record", "agentId": "main", "model": "kimi-code/k3", "usage": {"inputOther": 0, "output": 0, "inputCacheRead": 0, "inputCacheCreation": 0}, "usageScope": "turn", "time": 1001}))
                + &line(json!({"type": "usage.record", "agentId": "main", "model": "kimi-code/k3", "usage": {"inputOther": 50, "output": 400, "inputCacheRead": 0, "inputCacheCreation": 0}, "usageScope": "session", "time": 1002}))
                + "{torn",
        )
        .unwrap();
        let events = parse_usage_file(&path).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].source, "kimi");
        assert_eq!(events[0].source_record_id.as_deref(), Some("line:1"));
        assert_eq!(events[0].session_id.as_deref(), Some("session_d"));
        assert_eq!(events[0].project.as_deref(), Some("/work/repo"));
        assert_eq!(events[0].provider.as_deref(), Some("kimi-code"));
        assert_eq!(events[0].model.as_deref(), Some("kimi-code/k3"));
        assert_eq!(events[0].timestamp_ms, 1000);
        assert_eq!(events[0].tokens.uncached_input, 100);
        assert_eq!(events[0].tokens.cache_read, 300);
        assert_eq!(events[0].tokens.raw_input, 400);
        assert_eq!(events[0].tokens.output, 20);
        assert!(!events[0].sidechain);
        assert_eq!(events[1].source_record_id.as_deref(), Some("line:3"));
        assert_eq!(events[1].tokens.output, 400);
    }
}
