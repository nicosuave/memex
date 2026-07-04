use crate::index::SearchIndex;
use crate::types::{Record, SourceFilter, SourceKind};
use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const IMPORT_TIMEOUT: Duration = Duration::from_secs(120);
const PI_DEFAULT_TURNS: usize = 60;
const PI_MAX_TURNS: usize = 400;
const PI_DEFAULT_PROVIDER: &str = "openrouter";
const PI_DEFAULT_MODEL: &str = "openai/gpt-5.2-codex";
const OPENCODE_DEFAULT_PROVIDER: &str = "openai";
const OPENCODE_DEFAULT_MODEL: &str = "gpt-5.2-codex";
const OPENCODE_DEFAULT_AGENT: &str = "build";
static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferTarget {
    Codex,
    Claude,
    Copilot,
    Cursor,
    Opencode,
    Pi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferMode {
    Compact,
    Strict,
}

#[derive(Debug, Clone)]
pub struct TransferOptions {
    pub session_id: String,
    pub source: Option<SourceFilter>,
    pub target: TransferTarget,
    pub mode: TransferMode,
    pub turns: Option<usize>,
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct TransferResult {
    pub source: SourceKind,
    pub session_id: String,
    pub source_path: String,
    pub generated_path: PathBuf,
    pub thread_id: Option<String>,
    pub resume_command: Option<String>,
    pub message_count: usize,
}

#[derive(Debug, Clone)]
struct Conversation {
    source: SourceKind,
    session_id: String,
    source_path: String,
    cwd: PathBuf,
    title: Option<String>,
    tool_calls: usize,
    tool_results: usize,
    messages: Vec<ConversationMessage>,
}

#[derive(Debug, Clone)]
struct ConversationMessage {
    role: ConversationRole,
    text: String,
    timestamp_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConversationRole {
    User,
    Assistant,
}

pub fn transfer_session(index: &SearchIndex, options: TransferOptions) -> Result<TransferResult> {
    match options.target {
        TransferTarget::Codex => transfer_session_to_codex(index, options),
        TransferTarget::Claude => transfer_session_to_claude(index, options),
        TransferTarget::Copilot => transfer_session_to_copilot(index, options),
        TransferTarget::Cursor => transfer_session_to_cursor(index, options),
        TransferTarget::Opencode => transfer_session_to_opencode(index, options),
        TransferTarget::Pi => transfer_session_to_pi(index, options),
    }
}

fn transfer_session_to_codex(
    index: &SearchIndex,
    options: TransferOptions,
) -> Result<TransferResult> {
    let conversation = conversation_from_index(index, &options)?;
    let generated_path = write_claude_import_jsonl(&conversation, options.dry_run)?;
    let message_count = conversation.messages.len();

    if options.dry_run {
        return Ok(TransferResult {
            source: conversation.source,
            session_id: conversation.session_id,
            source_path: conversation.source_path,
            generated_path,
            thread_id: None,
            resume_command: None,
            message_count,
        });
    }

    let thread_id = import_generated_session_to_codex(&generated_path, &conversation.cwd)?;
    let resume_command = format!("codex resume {thread_id}");
    Ok(TransferResult {
        source: conversation.source,
        session_id: conversation.session_id,
        source_path: conversation.source_path,
        generated_path,
        thread_id: Some(thread_id),
        resume_command: Some(resume_command),
        message_count,
    })
}

fn transfer_session_to_claude(
    index: &SearchIndex,
    options: TransferOptions,
) -> Result<TransferResult> {
    let conversation = conversation_from_index(index, &options)?;
    let target_session_id = random_uuid_like();
    let generated_path =
        write_claude_session_jsonl(&conversation, &target_session_id, options.dry_run)?;
    let message_count = conversation.messages.len();
    let resume_command = if options.dry_run {
        None
    } else {
        Some(format!("claude --resume {target_session_id}"))
    };

    Ok(TransferResult {
        source: conversation.source,
        session_id: conversation.session_id,
        source_path: conversation.source_path,
        generated_path,
        thread_id: None,
        resume_command,
        message_count,
    })
}

fn transfer_session_to_copilot(
    index: &SearchIndex,
    options: TransferOptions,
) -> Result<TransferResult> {
    let conversation = conversation_from_index(index, &options)?;
    let target_session_id = random_uuid_like();
    let generated_path =
        write_copilot_session_events(&conversation, &target_session_id, options.dry_run)?;
    let message_count = conversation.messages.len();
    let resume_command = if options.dry_run {
        None
    } else {
        Some(format!("copilot --resume {target_session_id}"))
    };

    Ok(TransferResult {
        source: conversation.source,
        session_id: conversation.session_id,
        source_path: conversation.source_path,
        generated_path,
        thread_id: None,
        resume_command,
        message_count,
    })
}

fn transfer_session_to_cursor(
    index: &SearchIndex,
    options: TransferOptions,
) -> Result<TransferResult> {
    let conversation = conversation_from_index(index, &options)?;
    let target_session_id = random_uuid_like();
    let generated_path =
        write_cursor_session_jsonl(&conversation, &target_session_id, options.dry_run)?;
    let message_count = conversation.messages.len();
    let resume_command = if options.dry_run {
        None
    } else {
        Some(format!("cursor-agent --resume {target_session_id}"))
    };

    Ok(TransferResult {
        source: conversation.source,
        session_id: conversation.session_id,
        source_path: conversation.source_path,
        generated_path,
        thread_id: None,
        resume_command,
        message_count,
    })
}

fn transfer_session_to_opencode(
    index: &SearchIndex,
    options: TransferOptions,
) -> Result<TransferResult> {
    let conversation = conversation_from_index(index, &options)?;
    let target_session_id = random_opencode_id("ses");
    let generated_path = write_opencode_import_json(&conversation, &target_session_id)?;
    let message_count = conversation.messages.len();
    if !options.dry_run {
        import_opencode_session(&generated_path, &conversation.cwd)?;
    }
    let resume_command = if options.dry_run {
        None
    } else {
        Some(format!("opencode --session {target_session_id}"))
    };

    Ok(TransferResult {
        source: conversation.source,
        session_id: conversation.session_id,
        source_path: conversation.source_path,
        generated_path,
        thread_id: None,
        resume_command,
        message_count,
    })
}

fn transfer_session_to_pi(
    index: &SearchIndex,
    mut options: TransferOptions,
) -> Result<TransferResult> {
    // Pi's importer applies its turn limit after parsing, with a default of 60
    // and a hard cap of 400. Keep the full normalized conversation until then.
    let requested_turns = options.turns;
    options.turns = None;
    let conversation = conversation_from_index(index, &options)?;
    let max_turns = requested_turns
        .unwrap_or(PI_DEFAULT_TURNS)
        .min(PI_MAX_TURNS);
    let selected_messages = take_last_messages(conversation.messages.clone(), max_turns);
    if selected_messages.is_empty() {
        return Err(anyhow!(
            "session has no importable messages after Pi turn limit: {}",
            conversation.session_id
        ));
    }

    let generated_path = write_pi_session_jsonl(
        &conversation,
        &selected_messages,
        options.mode,
        options.dry_run,
    )?;
    let resume_command = if options.dry_run {
        None
    } else {
        Some(format!("pi --session {}", generated_path.display()))
    };
    Ok(TransferResult {
        source: conversation.source,
        session_id: conversation.session_id,
        source_path: conversation.source_path,
        generated_path,
        thread_id: None,
        resume_command,
        message_count: selected_messages.len(),
    })
}

fn conversation_from_index(index: &SearchIndex, options: &TransferOptions) -> Result<Conversation> {
    let mut records = index.records_by_session_id(&options.session_id)?;
    if let Some(source) = options.source {
        records.retain(|record| source.matches(record.source));
    }
    if records.is_empty() {
        return Err(anyhow!("session not found: {}", options.session_id));
    }
    records.sort_by(|a, b| {
        a.turn_id
            .cmp(&b.turn_id)
            .then_with(|| a.ts.cmp(&b.ts))
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });

    let first = records
        .first()
        .ok_or_else(|| anyhow!("session not found: {}", options.session_id))?;
    let source_kind = first.source;
    if records.iter().any(|record| record.source != source_kind) {
        let sources = ambiguous_source_labels(&records);
        return Err(anyhow!(
            "session id {} exists in multiple sources ({}); retry with --source",
            options.session_id,
            sources
        ));
    }

    let cwd = resolve_cwd_from_source(&records)
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));
    let title = records
        .iter()
        .find(|record| record.role == "user" && !record.text.trim().is_empty())
        .map(|record| summarize_title(&record.text));
    let tool_calls = records
        .iter()
        .filter(|record| record.role == "tool_use")
        .count();
    let tool_results = records
        .iter()
        .filter(|record| record.role == "tool_result" || record.role == "tool")
        .count();
    let messages = conversation_messages_from_records(&records, options.mode, options.turns);
    if messages.is_empty() {
        return Err(anyhow!(
            "session has no importable messages: {}",
            options.session_id
        ));
    }

    Ok(Conversation {
        source: source_kind,
        session_id: options.session_id.clone(),
        source_path: first.source_path.clone(),
        cwd,
        title,
        tool_calls,
        tool_results,
        messages,
    })
}

fn ambiguous_source_labels(records: &[Record]) -> String {
    let sources = records
        .iter()
        .map(|record| record.source.label())
        .collect::<BTreeSet<_>>();
    sources.into_iter().collect::<Vec<_>>().join(", ")
}

fn conversation_messages_from_records(
    records: &[Record],
    mode: TransferMode,
    turns: Option<usize>,
) -> Vec<ConversationMessage> {
    let mut messages = records
        .iter()
        .filter_map(|record| record_to_conversation_message(record, mode))
        .collect::<Vec<_>>();
    if mode == TransferMode::Compact {
        messages = compact_messages(messages);
    }
    if let Some(turns) = turns {
        messages = take_last_user_turns(messages, turns);
    }
    messages
}

fn record_to_conversation_message(
    record: &Record,
    mode: TransferMode,
) -> Option<ConversationMessage> {
    let text = record.text.trim();
    if text.is_empty() {
        return None;
    }
    let (role, text) = match record.role.as_str() {
        "user" => {
            let text = if mode == TransferMode::Compact {
                map_user_text_for_compact(text)?
            } else {
                text.to_string()
            };
            (ConversationRole::User, text)
        }
        "assistant" => {
            let text = if mode == TransferMode::Compact {
                map_assistant_text_for_compact(text)?
            } else {
                text.to_string()
            };
            (ConversationRole::Assistant, text)
        }
        "tool_use" if mode == TransferMode::Strict => (
            ConversationRole::Assistant,
            tool_note("tool_use", record.tool_name.as_deref(), text),
        ),
        "tool_result" | "tool" if mode == TransferMode::Strict => (
            ConversationRole::Assistant,
            tool_note("tool_result", record.tool_name.as_deref(), text),
        ),
        _ => return None,
    };
    Some(ConversationMessage {
        role,
        text,
        timestamp_ms: record.ts,
    })
}

fn map_user_text_for_compact(text: &str) -> Option<String> {
    let raw = text.trim();
    if raw.is_empty() {
        return None;
    }
    let normalized = normalize_text(raw);
    if normalized == "continue" || normalized == "contiue" {
        return Some("[User requested continuation]".to_string());
    }
    if normalized.starts_with("<task-notification>") {
        let summary = extract_tag_value(raw, "summary");
        return Some(if summary.is_empty() {
            "[Task notification]".to_string()
        } else {
            format!("[Task notification] {summary}")
        });
    }
    if normalized.starts_with("<command-name>") {
        let command_name = extract_tag_value(raw, "command-name");
        let command_message = extract_tag_value(raw, "command-message");
        return Some(
            match (command_name.is_empty(), command_message.is_empty()) {
                (false, false) => format!("[Local command] {command_name}: {command_message}"),
                (false, true) => format!("[Local command] {command_name}"),
                _ => "[Local command]".to_string(),
            },
        );
    }
    if normalized.starts_with("<local-command-stdout>") {
        let stdout = extract_tag_value(raw, "local-command-stdout");
        return Some(if stdout.is_empty() {
            "[Local command stdout]".to_string()
        } else {
            format!("[Local command stdout] {stdout}")
        });
    }
    if normalized.starts_with("<local-command-caveat>") {
        let caveat = extract_tag_value(raw, "local-command-caveat");
        return Some(if caveat.is_empty() {
            "[Local command caveat]".to_string()
        } else {
            format!("[Local command caveat] {caveat}")
        });
    }
    Some(raw.to_string())
}

fn map_assistant_text_for_compact(text: &str) -> Option<String> {
    let raw = text.trim();
    if raw.is_empty() {
        return None;
    }
    let normalized = normalize_text(raw);
    if normalized.contains("you've hit your org's monthly usage limit") {
        return Some("[Assistant error] Usage limit reached".to_string());
    }
    if normalized.contains("operation aborted") {
        return Some("[Assistant error] Operation aborted".to_string());
    }
    Some(raw.to_string())
}

fn compact_messages(messages: Vec<ConversationMessage>) -> Vec<ConversationMessage> {
    let mut deduped = Vec::new();
    for message in messages {
        let current = normalize_text(&message.text);
        let previous: Option<&ConversationMessage> = deduped.last();
        if previous
            .map(|prev| prev.role == message.role && normalize_text(&prev.text) == current)
            .unwrap_or(false)
        {
            continue;
        }
        deduped.push(message);
    }

    let mut compacted = Vec::new();
    let mut index = 0;
    while index < deduped.len() {
        let a = &deduped[index];
        let b = deduped.get(index + 1);
        if let Some(b) = b {
            let a_norm = normalize_text(&a.text);
            let b_norm = normalize_text(&b.text);
            let mut reps = 1;
            let mut cursor = index + 2;
            while cursor + 1 < deduped.len() {
                let next_a = &deduped[cursor];
                let next_b = &deduped[cursor + 1];
                if next_a.role == a.role
                    && next_b.role == b.role
                    && normalize_text(&next_a.text) == a_norm
                    && normalize_text(&next_b.text) == b_norm
                {
                    reps += 1;
                    cursor += 2;
                } else {
                    break;
                }
            }
            if reps >= 3 {
                compacted.push(a.clone());
                compacted.push(b.clone());
                compacted.push(ConversationMessage {
                    role: ConversationRole::Assistant,
                    text: format!("[Importer compacted repeated exchange x{}]", reps - 1),
                    timestamp_ms: b.timestamp_ms,
                });
                index += reps * 2;
                continue;
            }
        }
        compacted.push(a.clone());
        index += 1;
    }
    compacted
}

fn take_last_user_turns(
    messages: Vec<ConversationMessage>,
    max_user_turns: usize,
) -> Vec<ConversationMessage> {
    if max_user_turns == 0 {
        return Vec::new();
    }
    let mut seen = 0usize;
    let mut start = 0usize;
    for (idx, message) in messages.iter().enumerate().rev() {
        if message.role == ConversationRole::User {
            seen += 1;
            start = idx;
            if seen == max_user_turns {
                break;
            }
        }
    }
    messages.into_iter().skip(start).collect()
}

fn take_last_messages(
    messages: Vec<ConversationMessage>,
    max_messages: usize,
) -> Vec<ConversationMessage> {
    if max_messages == 0 {
        return Vec::new();
    }
    let len = messages.len();
    messages
        .into_iter()
        .skip(len.saturating_sub(max_messages))
        .collect()
}

fn tool_note(kind: &str, tool_name: Option<&str>, text: &str) -> String {
    match tool_name {
        Some(name) if !name.is_empty() => format!("[{kind}: {name}]\n{text}\n[/{kind}]"),
        _ => format!("[{kind}]\n{text}\n[/{kind}]"),
    }
}

fn write_pi_session_jsonl(
    conversation: &Conversation,
    selected_messages: &[ConversationMessage],
    mode: TransferMode,
    dry_run: bool,
) -> Result<PathBuf> {
    let now = chrono::Utc::now();
    let now_iso = now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let session_id = random_pi_session_id();
    let session_dir = if dry_run {
        memex_transfer_dir()?
            .join("pi")
            .join(sanitize_pi_cwd(&conversation.cwd))
    } else {
        pi_sessions_root()?.join(sanitize_pi_cwd(&conversation.cwd))
    };
    fs::create_dir_all(&session_dir).with_context(|| {
        format!(
            "failed to create Pi session directory {}",
            session_dir.display()
        )
    })?;
    let file_name = format!("{}_{}.jsonl", iso_for_pi_filename(now), session_id);
    let path = session_dir.join(file_name);
    let records =
        build_pi_session_records(conversation, selected_messages, mode, &now_iso, &session_id);
    let mut file = File::create(&path)
        .with_context(|| format!("failed to create Pi session file {}", path.display()))?;
    for record in records {
        writeln!(file, "{}", serde_json::to_string(&record)?)?;
    }
    Ok(path)
}

fn build_pi_session_records(
    conversation: &Conversation,
    selected_messages: &[ConversationMessage],
    mode: TransferMode,
    now_iso: &str,
    session_id: &str,
) -> Vec<Value> {
    let cwd = conversation.cwd.to_string_lossy();
    let model_change_id = random_hex_id(8);
    let thinking_id = random_hex_id(8);
    let mut parent_id = thinking_id.clone();
    let mut records = vec![
        json!({
            "type": "session",
            "version": 3,
            "id": session_id,
            "timestamp": now_iso,
            "cwd": cwd,
        }),
        json!({
            "type": "model_change",
            "id": model_change_id,
            "parentId": null,
            "timestamp": now_iso,
            "provider": PI_DEFAULT_PROVIDER,
            "modelId": PI_DEFAULT_MODEL,
        }),
        json!({
            "type": "thinking_level_change",
            "id": thinking_id,
            "parentId": model_change_id,
            "timestamp": now_iso,
            "thinkingLevel": "medium",
        }),
    ];

    let bootstrap = build_pi_bootstrap(conversation, selected_messages, mode);
    let bootstrap_record = pi_message_record(
        Some(parent_id.as_str()),
        ConversationRole::Assistant,
        &bootstrap,
        now_iso,
    );
    parent_id = bootstrap_record
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    records.push(bootstrap_record);

    for message in selected_messages {
        let timestamp =
            millis_to_rfc3339(message.timestamp_ms).unwrap_or_else(|| now_iso.to_string());
        let record = pi_message_record(
            Some(parent_id.as_str()),
            message.role,
            &message.text,
            &timestamp,
        );
        parent_id = record
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        records.push(record);
    }

    records
}

fn build_pi_bootstrap(
    conversation: &Conversation,
    selected_messages: &[ConversationMessage],
    mode: TransferMode,
) -> String {
    let objective = selected_messages
        .iter()
        .find(|message| message.role == ConversationRole::User)
        .map(|message| message.text.as_str())
        .unwrap_or("Continue the imported discussion.");
    let last_user = selected_messages
        .iter()
        .rev()
        .find(|message| message.role == ConversationRole::User)
        .map(|message| message.text.as_str())
        .unwrap_or("");
    let mode_label = match mode {
        TransferMode::Compact => "compact",
        TransferMode::Strict => "strict",
    };
    [
        format!(
            "Imported from {} session: {}",
            conversation.source.label(),
            conversation.source_path
        ),
        format!("Import mode: {mode_label}"),
        format!(
            "Imported turns: {} (total parsed: {})",
            selected_messages.len(),
            conversation.messages.len()
        ),
        format!(
            "Observed tool activity in source: {} tool_use, {} tool_result",
            conversation.tool_calls, conversation.tool_results
        ),
        String::new(),
        "Objective:".to_string(),
        truncate_chars(objective, 700),
        String::new(),
        "Most recent user ask:".to_string(),
        if last_user.is_empty() {
            "N/A".to_string()
        } else {
            truncate_chars(last_user, 700)
        },
        String::new(),
        "Continue from this context and ask clarifying questions only if needed.".to_string(),
    ]
    .join("\n")
}

fn pi_message_record(
    parent_id: Option<&str>,
    role: ConversationRole,
    text: &str,
    timestamp: &str,
) -> Value {
    let role_label = match role {
        ConversationRole::User => "user",
        ConversationRole::Assistant => "assistant",
    };
    let timestamp_ms = chrono::DateTime::parse_from_rfc3339(timestamp)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or_else(|_| chrono::Utc::now().timestamp_millis());
    let mut message = json!({
        "role": role_label,
        "content": [
            {
                "type": "text",
                "text": text,
            }
        ],
        "timestamp": timestamp_ms,
    });
    if role == ConversationRole::Assistant {
        message["api"] = Value::String("openai-completions".to_string());
        message["provider"] = Value::String(PI_DEFAULT_PROVIDER.to_string());
        message["model"] = Value::String(PI_DEFAULT_MODEL.to_string());
        message["usage"] = json!({
            "input": 0,
            "output": 0,
            "cacheRead": 0,
            "cacheWrite": 0,
            "totalTokens": 0,
            "cost": {
                "input": 0,
                "output": 0,
                "cacheRead": 0,
                "cacheWrite": 0,
                "total": 0,
            },
        });
        message["stopReason"] = Value::String("done".to_string());
    }
    json!({
        "type": "message",
        "id": random_hex_id(8),
        "parentId": parent_id,
        "timestamp": timestamp,
        "message": message,
    })
}

fn write_claude_session_jsonl(
    conversation: &Conversation,
    target_session_id: &str,
    dry_run: bool,
) -> Result<PathBuf> {
    let dir = if dry_run {
        memex_transfer_dir()?
            .join("claude")
            .join(sanitize_claude_cwd(&conversation.cwd))
    } else {
        claude_projects_dir()?.join(sanitize_claude_cwd(&conversation.cwd))
    };
    fs::create_dir_all(&dir).with_context(|| {
        format!(
            "failed to create Claude session directory {}",
            dir.display()
        )
    })?;
    let path = dir.join(format!("{target_session_id}.jsonl"));
    let records = build_claude_session_records(conversation, target_session_id);
    let mut file = File::create(&path)
        .with_context(|| format!("failed to create Claude session file {}", path.display()))?;
    for record in records {
        writeln!(file, "{}", serde_json::to_string(&record)?)?;
    }
    Ok(path)
}

fn build_claude_session_records(
    conversation: &Conversation,
    target_session_id: &str,
) -> Vec<Value> {
    let now_iso = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let mut records = Vec::new();
    if let Some(title) = &conversation.title {
        records.push(json!({
            "type": "custom-title",
            "customTitle": title,
            "sessionId": target_session_id,
            "timestamp": now_iso,
        }));
    }

    let mut parent_uuid = None::<String>;
    for message in &conversation.messages {
        let ty = match message.role {
            ConversationRole::User => "user",
            ConversationRole::Assistant => "assistant",
        };
        let uuid = random_uuid_like();
        let timestamp =
            millis_to_rfc3339(message.timestamp_ms).unwrap_or_else(|| now_iso.to_string());
        records.push(json!({
            "type": ty,
            "cwd": conversation.cwd,
            "sessionId": target_session_id,
            "uuid": uuid,
            "parentUuid": parent_uuid,
            "timestamp": timestamp,
            "message": {
                "role": ty,
                "content": message.text,
            },
        }));
        parent_uuid = Some(uuid);
    }
    records
}

fn write_copilot_session_events(
    conversation: &Conversation,
    target_session_id: &str,
    dry_run: bool,
) -> Result<PathBuf> {
    let dir = if dry_run {
        memex_transfer_dir()?
            .join("copilot")
            .join("session-state")
            .join(target_session_id)
    } else {
        copilot_session_root()?.join(target_session_id)
    };
    fs::create_dir_all(&dir).with_context(|| {
        format!(
            "failed to create Copilot session directory {}",
            dir.display()
        )
    })?;
    let workspace_path = dir.join("workspace.yaml");
    write_copilot_workspace(&workspace_path, &conversation.cwd)?;
    let events_path = dir.join("events.jsonl");
    let mut file = File::create(&events_path).with_context(|| {
        format!(
            "failed to create Copilot events file {}",
            events_path.display()
        )
    })?;
    for record in build_copilot_session_events(conversation, target_session_id) {
        writeln!(file, "{}", serde_json::to_string(&record)?)?;
    }
    Ok(events_path)
}

fn write_copilot_workspace(path: &Path, cwd: &Path) -> Result<()> {
    let cwd = cwd.to_string_lossy();
    let mut file = File::create(path)
        .with_context(|| format!("failed to create Copilot workspace file {}", path.display()))?;
    writeln!(file, "cwd: {}", yaml_quote(&cwd))?;
    writeln!(file, "gitRoot: {}", yaml_quote(&cwd))?;
    Ok(())
}

fn build_copilot_session_events(
    conversation: &Conversation,
    target_session_id: &str,
) -> Vec<Value> {
    let now_iso = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let start_event_id = random_copilot_event_id();
    let mut parent_id = start_event_id.clone();
    let mut events = vec![json!({
        "id": start_event_id,
        "parentId": null,
        "ephemeral": false,
        "type": "session.start",
        "timestamp": now_iso,
        "data": {
            "sessionId": target_session_id,
            "context": {
                "cwd": conversation.cwd,
                "gitRoot": conversation.cwd,
            },
        },
    })];

    for message in &conversation.messages {
        let event_id = random_copilot_event_id();
        let event_type = match message.role {
            ConversationRole::User => "user.message",
            ConversationRole::Assistant => "assistant.message",
        };
        let timestamp =
            millis_to_rfc3339(message.timestamp_ms).unwrap_or_else(|| now_iso.to_string());
        events.push(json!({
            "id": event_id,
            "parentId": parent_id,
            "ephemeral": false,
            "type": event_type,
            "timestamp": timestamp,
            "data": {
                "sessionId": target_session_id,
                "content": message.text,
            },
        }));
        parent_id = event_id;
    }
    events
}

fn write_cursor_session_jsonl(
    conversation: &Conversation,
    target_session_id: &str,
    dry_run: bool,
) -> Result<PathBuf> {
    let dir = if dry_run {
        memex_transfer_dir()?
            .join("cursor")
            .join(sanitize_cursor_cwd(&conversation.cwd))
            .join("agent-transcripts")
            .join(target_session_id)
    } else {
        cursor_projects_dir()?
            .join(sanitize_cursor_cwd(&conversation.cwd))
            .join("agent-transcripts")
            .join(target_session_id)
    };
    fs::create_dir_all(&dir).with_context(|| {
        format!(
            "failed to create Cursor session directory {}",
            dir.display()
        )
    })?;
    let path = dir.join(format!("{target_session_id}.jsonl"));
    let mut file = File::create(&path)
        .with_context(|| format!("failed to create Cursor session file {}", path.display()))?;
    for record in build_cursor_session_records(conversation) {
        writeln!(file, "{}", serde_json::to_string(&record)?)?;
    }
    Ok(path)
}

fn build_cursor_session_records(conversation: &Conversation) -> Vec<Value> {
    conversation
        .messages
        .iter()
        .map(|message| {
            let role = match message.role {
                ConversationRole::User => "user",
                ConversationRole::Assistant => "assistant",
            };
            json!({
                "role": role,
                "message": {
                    "content": [
                        {
                            "type": "text",
                            "text": message.text,
                        }
                    ],
                },
            })
        })
        .collect()
}

fn write_opencode_import_json(
    conversation: &Conversation,
    target_session_id: &str,
) -> Result<PathBuf> {
    let dir = memex_transfer_dir()?.join("opencode");
    fs::create_dir_all(&dir).with_context(|| {
        format!(
            "failed to create OpenCode transfer directory {}",
            dir.display()
        )
    })?;
    let path = dir.join(format!("{target_session_id}.json"));
    let payload = build_opencode_import_export(conversation, target_session_id);
    let mut file = File::create(&path)
        .with_context(|| format!("failed to create OpenCode import file {}", path.display()))?;
    serde_json::to_writer_pretty(&mut file, &payload)
        .with_context(|| format!("failed to write OpenCode import file {}", path.display()))?;
    writeln!(file)?;
    Ok(path)
}

fn build_opencode_import_export(conversation: &Conversation, target_session_id: &str) -> Value {
    let created_ms = conversation
        .messages
        .first()
        .map(|message| message.timestamp_ms)
        .filter(|timestamp| *timestamp > 0)
        .unwrap_or_else(now_millis);
    let mut previous_ms = created_ms.saturating_sub(1);
    let title = conversation
        .title
        .as_deref()
        .unwrap_or("Imported memex session");
    let mut messages = Vec::new();
    let mut previous_message_id: Option<String> = None;

    if conversation
        .messages
        .first()
        .map(|message| message.role == ConversationRole::Assistant)
        .unwrap_or(false)
    {
        let synthetic_id = opencode_indexed_id("msg", 0);
        let synthetic_part_id = opencode_indexed_id("prt", 0);
        previous_ms = previous_ms.saturating_add(1);
        messages.push(json!({
            "info": opencode_user_message_info(
                target_session_id,
                &synthetic_id,
                previous_ms,
            ),
            "parts": [
                opencode_text_part(
                    target_session_id,
                    &synthetic_id,
                    &synthetic_part_id,
                    "[Imported session begins with assistant context]",
                )
            ],
        }));
        previous_message_id = Some(synthetic_id);
    }

    for (index, message) in conversation.messages.iter().enumerate() {
        previous_ms = previous_ms.max(message.timestamp_ms).saturating_add(1);
        let timestamp_ms = if message.timestamp_ms > 0 {
            message.timestamp_ms.max(previous_ms)
        } else {
            previous_ms
        };
        let message_id = opencode_indexed_id("msg", index + 1);
        let part_id = opencode_indexed_id("prt", index + 1);
        let info = match message.role {
            ConversationRole::User => {
                opencode_user_message_info(target_session_id, &message_id, timestamp_ms)
            }
            ConversationRole::Assistant => opencode_assistant_message_info(
                target_session_id,
                &message_id,
                previous_message_id.as_deref().unwrap_or(&message_id),
                timestamp_ms,
                &conversation.cwd,
            ),
        };
        messages.push(json!({
            "info": info,
            "parts": [
                opencode_text_part(target_session_id, &message_id, &part_id, &message.text)
            ],
        }));
        previous_message_id = Some(message_id);
        previous_ms = timestamp_ms;
    }

    json!({
        "info": {
            "id": target_session_id,
            "slug": target_session_id,
            "projectID": "memex-transfer",
            "directory": conversation.cwd,
            "title": title,
            "version": env!("CARGO_PKG_VERSION"),
            "time": {
                "created": created_ms,
                "updated": previous_ms.max(created_ms),
            },
            "agent": OPENCODE_DEFAULT_AGENT,
            "model": {
                "providerID": OPENCODE_DEFAULT_PROVIDER,
                "modelID": OPENCODE_DEFAULT_MODEL,
            },
            "metadata": {
                "source": conversation.source.label(),
                "sourceSessionId": conversation.session_id,
                "sourcePath": conversation.source_path,
            },
        },
        "messages": messages,
    })
}

fn opencode_user_message_info(session_id: &str, message_id: &str, timestamp_ms: u64) -> Value {
    json!({
        "id": message_id,
        "sessionID": session_id,
        "role": "user",
        "time": {
            "created": timestamp_ms,
        },
        "agent": OPENCODE_DEFAULT_AGENT,
        "model": {
            "providerID": OPENCODE_DEFAULT_PROVIDER,
            "modelID": OPENCODE_DEFAULT_MODEL,
        },
    })
}

fn opencode_assistant_message_info(
    session_id: &str,
    message_id: &str,
    parent_id: &str,
    timestamp_ms: u64,
    cwd: &Path,
) -> Value {
    json!({
        "id": message_id,
        "sessionID": session_id,
        "role": "assistant",
        "time": {
            "created": timestamp_ms,
            "completed": timestamp_ms,
        },
        "parentID": parent_id,
        "modelID": OPENCODE_DEFAULT_MODEL,
        "providerID": OPENCODE_DEFAULT_PROVIDER,
        "mode": OPENCODE_DEFAULT_AGENT,
        "agent": OPENCODE_DEFAULT_AGENT,
        "path": {
            "cwd": cwd,
            "root": cwd,
        },
        "cost": 0,
        "tokens": {
            "input": 0,
            "output": 0,
            "reasoning": 0,
            "cache": {
                "read": 0,
                "write": 0,
            },
        },
        "finish": "stop",
    })
}

fn opencode_text_part(session_id: &str, message_id: &str, part_id: &str, text: &str) -> Value {
    json!({
        "id": part_id,
        "sessionID": session_id,
        "messageID": message_id,
        "type": "text",
        "text": text,
    })
}

fn import_opencode_session(path: &Path, cwd: &Path) -> Result<()> {
    let output = Command::new("opencode")
        .arg("import")
        .arg(path)
        .current_dir(cwd)
        .output()
        .with_context(|| "failed to run opencode import; is opencode installed and on PATH?")?;
    if output.status.success() {
        return Ok(());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(anyhow!(
        "opencode import failed with status {}: {}{}{}",
        output.status,
        stdout.trim(),
        if stdout.trim().is_empty() || stderr.trim().is_empty() {
            ""
        } else {
            "\n"
        },
        stderr.trim()
    ))
}

fn write_claude_import_jsonl(conversation: &Conversation, dry_run: bool) -> Result<PathBuf> {
    let dir = if dry_run {
        memex_transfer_dir()?.join("codex")
    } else {
        claude_projects_dir()?.join("-memex-transfer")
    };
    fs::create_dir_all(&dir)
        .with_context(|| format!("failed to create transfer directory {}", dir.display()))?;
    let file_name = format!(
        "{}-{}.jsonl",
        conversation.source.label(),
        sanitize_file_component(&conversation.session_id)
    );
    let path = dir.join(file_name);
    let mut file = File::create(&path)
        .with_context(|| format!("failed to create transfer transcript {}", path.display()))?;

    if let Some(title) = &conversation.title {
        writeln!(
            file,
            "{}",
            serde_json::to_string(&json!({
                "type": "custom-title",
                "customTitle": title,
            }))?
        )?;
    }

    for message in &conversation.messages {
        let ty = match message.role {
            ConversationRole::User => "user",
            ConversationRole::Assistant => "assistant",
        };
        let mut record = json!({
            "type": ty,
            "cwd": conversation.cwd,
            "message": {
                "role": ty,
                "content": message.text,
            },
        });
        if message.timestamp_ms > 0
            && let Some(timestamp) = millis_to_rfc3339(message.timestamp_ms)
        {
            record["timestamp"] = Value::String(timestamp);
        }
        writeln!(file, "{}", serde_json::to_string(&record)?)?;
    }
    Ok(path)
}

fn import_generated_session_to_codex(source_path: &Path, cwd: &Path) -> Result<String> {
    let canonical_source = fs::canonicalize(source_path)
        .with_context(|| format!("failed to canonicalize {}", source_path.display()))?;
    let mut client = CodexAppServerClient::spawn(cwd)?;
    client.initialize()?;
    client.request(
        "externalAgentConfig/import",
        json!({
            "migrationItems": [
                {
                    "itemType": "SESSIONS",
                    "description": format!("Transfer memex session {}", source_path.display()),
                    "cwd": null,
                    "details": {
                        "plugins": [],
                        "sessions": [
                            {
                                "path": canonical_source,
                                "cwd": cwd,
                                "title": null,
                            }
                        ],
                        "mcpServers": [],
                        "hooks": [],
                        "subagents": [],
                        "commands": [],
                    }
                }
            ],
            "source": "memex",
        }),
    )?;
    if let Some(thread_id) = client.wait_for_import_completed()? {
        return Ok(thread_id);
    }
    thread_id_from_import_ledger(&canonical_source)?
        .ok_or_else(|| anyhow!("Codex import completed but no imported thread id was reported"))
}

struct CodexAppServerClient {
    child: Child,
    stdin: ChildStdin,
    lines: Receiver<Result<String, String>>,
    next_id: u64,
}

impl CodexAppServerClient {
    fn spawn(cwd: &Path) -> Result<Self> {
        let mut child = Command::new("codex")
            .arg("app-server")
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("failed to start `codex app-server`; is Codex installed and in PATH?")?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to open codex app-server stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("failed to open codex app-server stdout"))?;
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                match line {
                    Ok(line) => {
                        if tx.send(Ok(line)).is_err() {
                            return;
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(Err(err.to_string()));
                        return;
                    }
                }
            }
        });
        Ok(Self {
            child,
            stdin,
            lines: rx,
            next_id: 1,
        })
    }

    fn initialize(&mut self) -> Result<()> {
        self.request(
            "initialize",
            json!({
                "clientInfo": {
                    "title": "memex",
                    "name": "memex",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": {
                    "experimentalApi": false,
                    "requestAttestation": false,
                    "optOutNotificationMethods": [
                        "item/agentMessage/delta",
                        "item/reasoning/summaryTextDelta",
                        "item/reasoning/summaryPartAdded",
                        "item/reasoning/textDelta",
                    ],
                },
            }),
        )?;
        self.notify("initialized", json!({}))
    }

    fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        self.write_message(&json!({
            "id": id,
            "method": method,
            "params": params,
        }))?;
        let deadline = Instant::now() + IMPORT_TIMEOUT;
        loop {
            let line = self.read_line_before(deadline)?;
            let value: Value = serde_json::from_str(&line)
                .with_context(|| format!("invalid codex app-server JSON: {line}"))?;
            if value.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            if let Some(error) = value.get("error") {
                return Err(anyhow!("codex app-server {method} failed: {error}"));
            }
            return Ok(value.get("result").cloned().unwrap_or_else(|| json!({})));
        }
    }

    fn notify(&mut self, method: &str, params: Value) -> Result<()> {
        self.write_message(&json!({
            "method": method,
            "params": params,
        }))
    }

    fn wait_for_import_completed(&mut self) -> Result<Option<String>> {
        let deadline = Instant::now() + IMPORT_TIMEOUT;
        loop {
            let line = self.read_line_before(deadline)?;
            let value: Value = serde_json::from_str(&line)
                .with_context(|| format!("invalid codex app-server JSON: {line}"))?;
            if value.get("method").and_then(Value::as_str)
                != Some("externalAgentConfig/import/completed")
            {
                continue;
            }
            return Ok(thread_id_from_completed_notification(&value));
        }
    }

    fn write_message(&mut self, message: &Value) -> Result<()> {
        writeln!(self.stdin, "{}", serde_json::to_string(message)?)?;
        self.stdin.flush()?;
        Ok(())
    }

    fn read_line_before(&mut self, deadline: Instant) -> Result<String> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(anyhow!("timed out waiting for codex app-server"));
            }
            let timeout = deadline.saturating_duration_since(now);
            match self.lines.recv_timeout(timeout) {
                Ok(Ok(line)) if !line.trim().is_empty() => return Ok(line),
                Ok(Ok(_)) => continue,
                Ok(Err(err)) => {
                    return Err(anyhow!("failed to read codex app-server stdout: {err}"));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(anyhow!("timed out waiting for codex app-server"));
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(anyhow!("codex app-server closed stdout"));
                }
            }
        }
    }
}

impl Drop for CodexAppServerClient {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn thread_id_from_completed_notification(value: &Value) -> Option<String> {
    value
        .get("params")?
        .get("itemTypeResults")?
        .as_array()?
        .iter()
        .flat_map(|result| {
            result
                .get("successes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .find_map(|success| {
            success
                .get("target")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

fn thread_id_from_import_ledger(source_path: &Path) -> Result<Option<String>> {
    #[derive(Deserialize)]
    struct Ledger {
        records: Vec<LedgerRecord>,
    }
    #[derive(Deserialize)]
    struct LedgerRecord {
        source_path: PathBuf,
        content_sha256: String,
        imported_thread_id: String,
    }

    let path = codex_home()?.join("external_agent_session_imports.json");
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read Codex import ledger {}", path.display()))?;
    let ledger: Ledger = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse Codex import ledger {}", path.display()))?;
    let hash = file_sha256(source_path)?;
    Ok(ledger
        .records
        .into_iter()
        .rev()
        .find(|record| record.source_path == source_path && record.content_sha256 == hash)
        .map(|record| record.imported_thread_id))
}

fn file_sha256(path: &Path) -> Result<String> {
    let bytes = fs::read(path)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn resolve_cwd_from_source(records: &[Record]) -> Option<PathBuf> {
    let first = records.first()?;
    match first.source {
        SourceKind::Claude => cwd_from_jsonl(Path::new(&first.source_path)),
        SourceKind::CodexSession => cwd_from_codex_session(Path::new(&first.source_path)),
        SourceKind::Copilot => cwd_from_copilot_session(Path::new(&first.source_path)),
        SourceKind::Cursor => cwd_from_cursor_session(Path::new(&first.source_path)),
        SourceKind::Opencode => cwd_from_opencode_session(Path::new(&first.source_path)),
        SourceKind::Pi => cwd_from_pi_session(Path::new(&first.source_path)),
        SourceKind::CodexHistory => None,
    }
    .filter(|path| path.is_dir())
}

fn cwd_from_jsonl(path: &Path) -> Option<PathBuf> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    for line in reader.lines().map_while(std::result::Result::ok) {
        let value: Value = serde_json::from_str(&line).ok()?;
        if let Some(cwd) = value.get("cwd").and_then(Value::as_str) {
            return Some(PathBuf::from(cwd));
        }
    }
    None
}

fn cwd_from_codex_session(path: &Path) -> Option<PathBuf> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    for line in reader.lines().map_while(std::result::Result::ok) {
        let value: Value = serde_json::from_str(&line).ok()?;
        if value.get("type").and_then(Value::as_str) != Some("session_meta") {
            continue;
        }
        let cwd = value
            .get("payload")
            .and_then(|payload| payload.get("cwd"))
            .and_then(Value::as_str)?;
        return Some(PathBuf::from(cwd));
    }
    None
}

fn cwd_from_pi_session(path: &Path) -> Option<PathBuf> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    for line in reader.lines().map_while(std::result::Result::ok) {
        let value: Value = serde_json::from_str(&line).ok()?;
        if value.get("type").and_then(Value::as_str) != Some("session") {
            continue;
        }
        let cwd = value.get("cwd").and_then(Value::as_str)?;
        return Some(PathBuf::from(cwd));
    }
    None
}

fn cwd_from_cursor_session(path: &Path) -> Option<PathBuf> {
    cwd_from_cursor_transcript(path).or_else(|| cwd_from_cursor_project_path(path))
}

fn cwd_from_cursor_transcript(path: &Path) -> Option<PathBuf> {
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    for line in reader.lines().map_while(std::result::Result::ok) {
        let value: Value = serde_json::from_str(&line).ok()?;
        if let Some(path) = cursor_cwd_from_value(&value) {
            return Some(path);
        }
    }
    None
}

fn cursor_cwd_from_value(value: &Value) -> Option<PathBuf> {
    match value {
        Value::Object(obj) => {
            for key in ["cwd", "workspace", "target_directory"] {
                if let Some(path) = obj
                    .get(key)
                    .and_then(Value::as_str)
                    .and_then(existing_dir_from_absolute)
                {
                    return Some(path);
                }
            }
            for key in ["path", "file_path"] {
                if let Some(path) = obj
                    .get(key)
                    .and_then(Value::as_str)
                    .and_then(existing_dir_or_parent_from_absolute)
                {
                    return Some(path);
                }
            }
            obj.values().find_map(cursor_cwd_from_value)
        }
        Value::Array(values) => values.iter().find_map(cursor_cwd_from_value),
        _ => None,
    }
}

fn cwd_from_cursor_project_path(path: &Path) -> Option<PathBuf> {
    let mut saw_projects = false;
    for component in path.components() {
        let value = component.as_os_str().to_string_lossy();
        if saw_projects {
            if value.is_empty() || value == "agent-transcripts" {
                return None;
            }
            return decode_cursor_project_component(&value);
        }
        saw_projects = value == "projects";
    }
    None
}

fn decode_cursor_project_component(value: &str) -> Option<PathBuf> {
    let parts = value
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    decode_cursor_project_parts(Path::new("/").to_path_buf(), &parts)
}

fn decode_cursor_project_parts(base: PathBuf, parts: &[&str]) -> Option<PathBuf> {
    if parts.is_empty() {
        return base.is_dir().then_some(base);
    }

    for len in 1..=parts.len() {
        let candidate = base.join(parts[..len].join("-"));
        if !candidate.is_dir() {
            continue;
        }
        if let Some(path) = decode_cursor_project_parts(candidate, &parts[len..]) {
            return Some(path);
        }
    }
    None
}

fn existing_dir_from_absolute(value: &str) -> Option<PathBuf> {
    let path = PathBuf::from(value);
    (path.is_absolute() && path.is_dir()).then_some(path)
}

fn existing_dir_or_parent_from_absolute(value: &str) -> Option<PathBuf> {
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return None;
    }
    if path.is_dir() {
        return Some(path);
    }
    path.parent()
        .filter(|parent| parent.is_dir())
        .map(Path::to_path_buf)
}

fn cwd_from_opencode_session(message_dir: &Path) -> Option<PathBuf> {
    let session_id = message_dir.file_name()?.to_str()?;
    let storage_root = message_dir.parent()?.parent()?;
    let session_path = storage_root
        .join("session")
        .join(format!("{session_id}.json"));
    let file = File::open(session_path).ok()?;
    let value: Value = serde_json::from_reader(file).ok()?;
    let directory = value.get("directory").and_then(Value::as_str)?;
    Some(PathBuf::from(directory))
}

fn cwd_from_copilot_session(path: &Path) -> Option<PathBuf> {
    let workspace_path = path.parent()?.join("workspace.yaml");
    let contents = fs::read_to_string(workspace_path).ok()?;
    for line in contents.lines() {
        let trimmed = line.trim();
        let Some((key, value)) = trimmed.split_once(':') else {
            continue;
        };
        if key.trim() != "cwd" {
            continue;
        }
        let value = value.trim().trim_matches('"').trim_matches('\'');
        if !value.is_empty() {
            return Some(PathBuf::from(value));
        }
    }
    None
}

fn normalize_text(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_lowercase()
}

fn extract_tag_value(text: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let lower = text.to_lowercase();
    let Some(start) = lower.find(&open) else {
        return String::new();
    };
    let start = start + open.len();
    let Some(end) = lower[start..].find(&close).map(|idx| start + idx) else {
        return String::new();
    };
    text[start..end]
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn millis_to_rfc3339(ms: u64) -> Option<String> {
    let secs = i64::try_from(ms / 1000).ok()?;
    let nanos = u32::try_from((ms % 1000) * 1_000_000).ok()?;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

fn summarize_title(text: &str) -> String {
    let first = text.lines().next().unwrap_or_default().trim();
    let mut title = first.chars().take(120).collect::<String>();
    if first.chars().count() > 120 {
        title.push_str("...");
    }
    title
}

fn sanitize_file_component(value: &str) -> String {
    let mut out = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    if out.is_empty() {
        out.push_str("session");
    }
    out.truncate(160);
    out
}

fn sanitize_pi_cwd(cwd: &Path) -> String {
    let normalized = cwd
        .to_string_lossy()
        .replace('\\', "/")
        .trim_matches('/')
        .to_string();
    format!("--{}--", normalized.replace('/', "-"))
}

fn sanitize_claude_cwd(cwd: &Path) -> String {
    let normalized = cwd
        .to_string_lossy()
        .replace('\\', "/")
        .trim_matches('/')
        .to_string();
    if normalized.is_empty() {
        "-".to_string()
    } else {
        format!("-{}", normalized.replace('/', "-"))
    }
}

fn sanitize_cursor_cwd(cwd: &Path) -> String {
    let normalized = cwd
        .to_string_lossy()
        .replace('\\', "/")
        .trim_matches('/')
        .to_string();
    if normalized.is_empty() {
        "root".to_string()
    } else {
        normalized.replace('/', "-")
    }
}

fn iso_for_pi_filename(now: chrono::DateTime<chrono::Utc>) -> String {
    now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        .replace(':', "-")
}

fn random_pi_session_id() -> String {
    random_uuid_like()
}

fn random_uuid_like() -> String {
    format!(
        "{}-{}-{}-{}-{}",
        random_hex_id(8),
        random_hex_id(4),
        random_hex_id(4),
        random_hex_id(4),
        random_hex_id(12)
    )
}

fn random_opencode_id(prefix: &str) -> String {
    format!("{prefix}_{}", random_hex_id(24))
}

fn opencode_indexed_id(prefix: &str, index: usize) -> String {
    format!("{prefix}_{:08x}{}", index, random_hex_id(16))
}

fn random_copilot_event_id() -> String {
    format!("evt_{}", random_hex_id(24))
}

fn random_hex_id(length: usize) -> String {
    let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let seed = format!("{nanos}:{}:{counter}", std::process::id());
    let mut hex = format!("{:x}", Sha256::digest(seed.as_bytes()));
    hex.truncate(length);
    hex
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn yaml_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn now_millis() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0)
}

fn claude_projects_dir() -> Result<PathBuf> {
    Ok(home_dir()?.join(".claude").join("projects"))
}

fn cursor_projects_dir() -> Result<PathBuf> {
    Ok(home_dir()?.join(".cursor").join("projects"))
}

fn copilot_session_root() -> Result<PathBuf> {
    let root = std::env::var_os("COPILOT_HOME")
        .map(PathBuf::from)
        .unwrap_or(home_dir()?.join(".copilot"));
    Ok(root.join("session-state"))
}

fn pi_sessions_root() -> Result<PathBuf> {
    if let Some(root) = std::env::var_os("PI_CODING_AGENT_SESSION_DIR") {
        return Ok(PathBuf::from(root));
    }
    let agent_root = pi_agent_root()?;
    if let Some(root) = pi_configured_session_root(&agent_root) {
        return Ok(root);
    }
    Ok(agent_root.join("sessions"))
}

fn memex_transfer_dir() -> Result<PathBuf> {
    Ok(home_dir()?.join(".memex").join("transfers"))
}

fn pi_agent_root() -> Result<PathBuf> {
    if let Some(root) = std::env::var_os("PI_CODING_AGENT_DIR") {
        return Ok(PathBuf::from(root));
    }
    Ok(home_dir()?.join(".pi").join("agent"))
}

fn pi_configured_session_root(agent_root: &Path) -> Option<PathBuf> {
    let settings_path = agent_root.join("settings.json");
    let contents = fs::read_to_string(settings_path).ok()?;
    let value: Value = serde_json::from_str(&contents).ok()?;
    let session_dir = value.get("sessionDir")?.as_str()?.trim();
    if session_dir.is_empty() {
        return None;
    }
    Some(resolve_pi_settings_path(session_dir, agent_root))
}

fn resolve_pi_settings_path(raw: &str, base: &Path) -> PathBuf {
    if raw == "~" {
        return directories::BaseDirs::new()
            .map(|dirs| dirs.home_dir().to_path_buf())
            .unwrap_or_else(|| PathBuf::from(raw));
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        return directories::BaseDirs::new()
            .map(|dirs| dirs.home_dir().join(rest))
            .unwrap_or_else(|| PathBuf::from(raw));
    }
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}

fn codex_home() -> Result<PathBuf> {
    if let Some(value) = std::env::var_os("CODEX_HOME") {
        return Ok(PathBuf::from(value));
    }
    Ok(home_dir()?.join(".codex"))
}

fn home_dir() -> Result<PathBuf> {
    directories::BaseDirs::new()
        .map(|dirs| dirs.home_dir().to_path_buf())
        .ok_or_else(|| anyhow!("cannot determine home directory"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{EnvVarGuard, env_lock};

    fn msg(role: ConversationRole, text: &str) -> ConversationMessage {
        ConversationMessage {
            role,
            text: text.to_string(),
            timestamp_ms: 0,
        }
    }

    #[test]
    fn compact_maps_claude_wrappers() {
        assert_eq!(
            map_user_text_for_compact(
                "<command-name>x</command-name><command-message>run tests</command-message>"
            )
            .as_deref(),
            Some("[Local command] x: run tests")
        );
        assert_eq!(
            map_user_text_for_compact("continue").as_deref(),
            Some("[User requested continuation]")
        );
    }

    #[test]
    fn compact_repeated_exchange() {
        let messages = vec![
            msg(ConversationRole::User, "again"),
            msg(ConversationRole::Assistant, "ok"),
            msg(ConversationRole::User, "again"),
            msg(ConversationRole::Assistant, "ok"),
            msg(ConversationRole::User, "again"),
            msg(ConversationRole::Assistant, "ok"),
        ];

        let compacted = compact_messages(messages);

        assert_eq!(compacted.len(), 3);
        assert_eq!(
            compacted[2].text,
            "[Importer compacted repeated exchange x2]"
        );
    }

    #[test]
    fn limits_to_last_user_turns() {
        let messages = vec![
            msg(ConversationRole::User, "one"),
            msg(ConversationRole::Assistant, "a"),
            msg(ConversationRole::User, "two"),
            msg(ConversationRole::Assistant, "b"),
        ];

        let limited = take_last_user_turns(messages, 1);

        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].text, "two");
    }

    #[test]
    fn pi_sanitizes_cwd_like_importer() {
        assert_eq!(
            sanitize_pi_cwd(Path::new("/Users/nico/Code/memex")),
            "--Users-nico-Code-memex--"
        );
        assert_eq!(
            sanitize_pi_cwd(Path::new("C:\\Users\\nico\\Code\\memex")),
            "--C:-Users-nico-Code-memex--"
        );
    }

    #[test]
    fn claude_sanitizes_cwd_like_projects_dir() {
        assert_eq!(
            sanitize_claude_cwd(Path::new("/Users/nico/Code/memex")),
            "-Users-nico-Code-memex"
        );
        assert_eq!(
            sanitize_claude_cwd(Path::new("C:\\Users\\nico\\Code\\memex")),
            "-C:-Users-nico-Code-memex"
        );
    }

    #[test]
    fn cursor_sanitizes_and_resolves_project_paths() {
        assert_eq!(
            sanitize_cursor_cwd(Path::new("/Users/nico/Code/memex")),
            "Users-nico-Code-memex"
        );
        assert_eq!(
            cwd_from_cursor_session(Path::new(
                "/Users/nico/.cursor/projects/Users-nico-Code-memex/agent-transcripts/abc/abc.jsonl"
            ))
            .as_deref(),
            Some(Path::new("/Users/nico/Code/memex"))
        );
    }

    #[test]
    fn cursor_resolves_hyphenated_project_paths() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("my-project");
        fs::create_dir_all(&project).unwrap();
        let encoded = sanitize_cursor_cwd(&project);
        let transcript = dir
            .path()
            .join(".cursor")
            .join("projects")
            .join(encoded)
            .join("agent-transcripts")
            .join("abc")
            .join("abc.jsonl");

        assert_eq!(
            cwd_from_cursor_session(&transcript).as_deref(),
            Some(project.as_path())
        );
    }

    #[test]
    fn opencode_resolves_cwd_from_session_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("my-project");
        fs::create_dir_all(&project).unwrap();
        let storage = dir.path().join("storage");
        let message_dir = storage.join("message").join("ses_abc");
        let session_dir = storage.join("session");
        fs::create_dir_all(&message_dir).unwrap();
        fs::create_dir_all(&session_dir).unwrap();
        fs::write(
            session_dir.join("ses_abc.json"),
            format!(
                r#"{{"id":"ses_abc","directory":{}}}"#,
                serde_json::to_string(project.to_str().unwrap()).unwrap()
            ),
        )
        .unwrap();

        assert_eq!(
            cwd_from_opencode_session(&message_dir).as_deref(),
            Some(project.as_path())
        );
    }

    #[test]
    fn copilot_session_root_honors_copilot_home() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("copilot-home");
        let _env = EnvVarGuard::set_os(&[("COPILOT_HOME", Some(home.as_os_str()))]);

        assert_eq!(copilot_session_root().unwrap(), home.join("session-state"));
    }

    #[test]
    fn pi_sessions_root_honors_settings_session_dir() {
        let _guard = env_lock();
        let dir = tempfile::tempdir().unwrap();
        let pi_root = dir.path().join("pi-agent");
        fs::create_dir_all(&pi_root).unwrap();
        fs::write(
            pi_root.join("settings.json"),
            r#"{ "sessionDir": ".pi/sessions" }"#,
        )
        .unwrap();
        let _env = EnvVarGuard::set_os(&[
            ("PI_CODING_AGENT_SESSION_DIR", None),
            ("PI_CODING_AGENT_DIR", Some(pi_root.as_os_str())),
        ]);

        assert_eq!(pi_sessions_root().unwrap(), pi_root.join(".pi/sessions"));
    }

    #[test]
    fn claude_records_include_resume_chain() {
        let conversation = Conversation {
            source: SourceKind::Pi,
            session_id: "pi-session".to_string(),
            source_path: "/tmp/pi.jsonl".to_string(),
            cwd: PathBuf::from("/tmp/project"),
            title: Some("Transfer me".to_string()),
            tool_calls: 0,
            tool_results: 0,
            messages: vec![
                msg(ConversationRole::User, "first"),
                msg(ConversationRole::Assistant, "second"),
            ],
        };

        let records = build_claude_session_records(&conversation, "target-session");

        assert_eq!(records.len(), 3);
        assert_eq!(records[0]["type"], "custom-title");
        assert_eq!(records[1]["type"], "user");
        assert_eq!(records[1]["sessionId"], "target-session");
        assert_eq!(records[1]["message"]["content"], "first");
        assert_eq!(records[2]["type"], "assistant");
        assert_eq!(records[2]["parentUuid"], records[1]["uuid"]);
    }

    #[test]
    fn cursor_records_match_agent_transcript_shape() {
        let conversation = Conversation {
            source: SourceKind::Claude,
            session_id: "claude-session".to_string(),
            source_path: "/tmp/claude.jsonl".to_string(),
            cwd: PathBuf::from("/tmp/project"),
            title: None,
            tool_calls: 0,
            tool_results: 0,
            messages: vec![
                msg(ConversationRole::User, "first"),
                msg(ConversationRole::Assistant, "second"),
            ],
        };

        let records = build_cursor_session_records(&conversation);

        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["role"], "user");
        assert_eq!(records[0]["message"]["content"][0]["type"], "text");
        assert_eq!(records[0]["message"]["content"][0]["text"], "first");
        assert_eq!(records[1]["role"], "assistant");
    }

    #[test]
    fn copilot_events_match_session_state_shape() {
        let conversation = Conversation {
            source: SourceKind::Claude,
            session_id: "claude-session".to_string(),
            source_path: "/tmp/claude.jsonl".to_string(),
            cwd: PathBuf::from("/tmp/project"),
            title: None,
            tool_calls: 0,
            tool_results: 0,
            messages: vec![
                msg(ConversationRole::User, "first"),
                msg(ConversationRole::Assistant, "second"),
            ],
        };

        let events = build_copilot_session_events(&conversation, "target-session");

        assert_eq!(events.len(), 3);
        assert!(events[0]["id"].as_str().unwrap().starts_with("evt_"));
        assert_eq!(events[0]["parentId"], Value::Null);
        assert_eq!(events[0]["ephemeral"], false);
        assert_eq!(events[0]["type"], "session.start");
        assert_eq!(events[0]["data"]["sessionId"], "target-session");
        assert_eq!(events[1]["parentId"], events[0]["id"]);
        assert_eq!(events[1]["type"], "user.message");
        assert_eq!(events[1]["data"]["content"], "first");
        assert_eq!(events[2]["parentId"], events[1]["id"]);
        assert_eq!(events[2]["type"], "assistant.message");
        assert_eq!(events[2]["data"]["content"], "second");
    }

    #[test]
    fn copilot_workspace_parser_reads_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace.yaml");
        fs::write(
            &workspace,
            "cwd: \"/Users/nico/Code/memex\"\ngitRoot: \"/Users/nico/Code/memex\"\n",
        )
        .unwrap();
        let events = dir.path().join("events.jsonl");

        assert_eq!(
            cwd_from_copilot_session(&events).as_deref(),
            Some(Path::new("/Users/nico/Code/memex"))
        );
    }

    #[test]
    fn opencode_import_export_matches_canonical_shape() {
        let conversation = Conversation {
            source: SourceKind::Claude,
            session_id: "claude-session".to_string(),
            source_path: "/tmp/claude.jsonl".to_string(),
            cwd: PathBuf::from("/tmp/project"),
            title: Some("Build this".to_string()),
            tool_calls: 0,
            tool_results: 0,
            messages: vec![
                msg(ConversationRole::User, "first"),
                msg(ConversationRole::Assistant, "second"),
            ],
        };

        let export = build_opencode_import_export(&conversation, "ses_abc");
        let messages = export["messages"].as_array().unwrap();

        assert_eq!(export["info"]["id"], "ses_abc");
        assert_eq!(export["info"]["title"], "Build this");
        assert_eq!(export["info"]["directory"], "/tmp/project");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["info"]["role"], "user");
        assert_eq!(messages[0]["parts"][0]["type"], "text");
        assert_eq!(messages[0]["parts"][0]["text"], "first");
        assert_eq!(messages[1]["info"]["role"], "assistant");
        assert_eq!(messages[1]["info"]["parentID"], messages[0]["info"]["id"]);
        assert_eq!(messages[1]["info"]["path"]["cwd"], "/tmp/project");
        assert_eq!(messages[1]["info"]["tokens"]["cache"]["read"], 0);
        assert_eq!(
            messages[1]["parts"][0]["messageID"],
            messages[1]["info"]["id"]
        );
        assert_eq!(messages[1]["parts"][0]["text"], "second");
    }

    #[test]
    fn pi_records_match_v3_session_shape() {
        let conversation = Conversation {
            source: SourceKind::Claude,
            session_id: "abc123".to_string(),
            source_path: "/tmp/abc123.jsonl".to_string(),
            cwd: PathBuf::from("/Users/nico/Code/memex"),
            title: None,
            tool_calls: 2,
            tool_results: 3,
            messages: vec![
                msg(ConversationRole::User, "build this"),
                msg(ConversationRole::Assistant, "done"),
            ],
        };
        let records = build_pi_session_records(
            &conversation,
            &conversation.messages,
            TransferMode::Compact,
            "2026-07-03T12:34:56.000Z",
            "session-id",
        );

        assert_eq!(records.len(), 6);
        assert_eq!(records[0]["type"], "session");
        assert_eq!(records[0]["version"], 3);
        assert_eq!(records[0]["id"], "session-id");
        assert_eq!(records[0]["cwd"], "/Users/nico/Code/memex");
        assert_eq!(records[1]["type"], "model_change");
        assert_eq!(records[1]["provider"], PI_DEFAULT_PROVIDER);
        assert_eq!(records[1]["modelId"], PI_DEFAULT_MODEL);
        assert_eq!(records[2]["type"], "thinking_level_change");
        assert_eq!(records[2]["parentId"], records[1]["id"]);
        assert_eq!(records[3]["type"], "message");
        assert_eq!(records[3]["parentId"], records[2]["id"]);
        assert_eq!(records[4]["parentId"], records[3]["id"]);
        assert_eq!(records[5]["parentId"], records[4]["id"]);
        assert_eq!(records[4]["message"]["role"], "user");
        assert_eq!(records[5]["message"]["role"], "assistant");
        assert_eq!(records[5]["message"]["provider"], PI_DEFAULT_PROVIDER);
    }

    #[test]
    fn pi_bootstrap_includes_import_context() {
        let conversation = Conversation {
            source: SourceKind::Pi,
            session_id: "pi-session".to_string(),
            source_path: "/tmp/pi.jsonl".to_string(),
            cwd: PathBuf::from("/tmp/project"),
            title: None,
            tool_calls: 1,
            tool_results: 4,
            messages: vec![
                msg(ConversationRole::User, "first ask"),
                msg(ConversationRole::Assistant, "answer"),
                msg(ConversationRole::User, "latest ask"),
            ],
        };

        let bootstrap = build_pi_bootstrap(
            &conversation,
            &conversation.messages[1..],
            TransferMode::Strict,
        );

        assert!(bootstrap.contains("Imported from pi session: /tmp/pi.jsonl"));
        assert!(bootstrap.contains("Import mode: strict"));
        assert!(bootstrap.contains("Imported turns: 2 (total parsed: 3)"));
        assert!(bootstrap.contains("1 tool_use, 4 tool_result"));
        assert!(bootstrap.contains("Most recent user ask:\nlatest ask"));
    }
}
