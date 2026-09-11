use crate::state::PendingToolCall;
use crate::types::RecordLinks;
use chrono::{DateTime, Utc};
use directories::BaseDirs;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

pub fn home() -> PathBuf {
    BaseDirs::new()
        .map(|dirs| dirs.home_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("/"))
}

pub fn parse_iso_millis(input: &str) -> Option<u64> {
    DateTime::parse_from_rfc3339(input)
        .ok()
        .map(|date| date.with_timezone(&Utc).timestamp_millis().max(0) as u64)
}

pub fn timestamp_millis(value: &Value) -> u64 {
    value
        .as_u64()
        .or_else(|| value.as_i64().map(|value| value.max(0) as u64))
        .or_else(|| value.as_str().and_then(parse_iso_millis))
        .unwrap_or(0)
}

pub fn jsonl_files(roots: impl IntoIterator<Item = PathBuf>) -> Vec<PathBuf> {
    let mut files = roots
        .into_iter()
        .filter(|root| root.exists())
        .flat_map(|root| {
            WalkDir::new(root)
                .into_iter()
                .flatten()
                .filter(|entry| {
                    entry.file_type().is_file()
                        && entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl")
                })
                .map(|entry| entry.path().to_path_buf())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    files.sort();
    files.dedup();
    files
}

pub(crate) fn jsonl_files_with_inventory(
    roots: impl IntoIterator<Item = PathBuf>,
    inventory: &mut crate::directory_inventory::DiscoveryInventory,
) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for root in roots {
        if !root.try_exists()? {
            continue;
        }
        for entry in inventory.walk(&root) {
            let entry = entry?;
            if entry.file_type == crate::directory_inventory::EntryType::File
                && entry.path.extension().and_then(|ext| ext.to_str()) == Some("jsonl")
            {
                files.push(entry.path);
            }
        }
    }
    files.sort();
    files.dedup();
    Ok(files)
}

pub fn project_from_path(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(path)
        .to_string()
}

pub(crate) fn pending_tool_call(
    tool_name: Option<String>,
    event_id: Option<String>,
    doc_id: u64,
    timestamp: u64,
    arguments: Option<&str>,
    links: &RecordLinks,
    session_id: &str,
) -> PendingToolCall {
    PendingToolCall {
        tool_name,
        tool_use_event_id: event_id,
        tool_use_doc_id: Some(doc_id),
        timestamp,
        argument_sha256: arguments.map(|value| format!("{:x}", Sha256::digest(value.as_bytes()))),
        argument_bytes: arguments.map(|value| value.len() as u64),
        parent_event_id: links.parent_event_id.clone(),
        session_id: Some(session_id.to_string()),
        source_tool_use_id: links.source_tool_use_id.clone(),
        source_tool_assistant_uuid: links.source_tool_assistant_uuid.clone(),
    }
}

pub(crate) fn borrowed_string(
    object: &simd_json::borrowed::Object<'_>,
    key: &str,
) -> Option<String> {
    use simd_json::prelude::*;
    object
        .get(key)
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

/// Preserve string tool payloads verbatim and structured payloads as valid JSON.
pub(crate) fn tool_value_text(value: &simd_json::BorrowedValue<'_>) -> Option<String> {
    use simd_json::prelude::*;
    value
        .as_str()
        .map(str::to_string)
        .or_else(|| serde_json::to_string(value).ok())
}

pub(crate) fn tool_result_text(block: &simd_json::BorrowedValue<'_>) -> Option<String> {
    use simd_json::prelude::*;
    let object = block.as_object()?;
    let content = object.get("content")?;
    if let Some(text) = content.as_str() {
        return Some(text.to_string());
    }
    let array = content.as_array()?;
    let parts = array
        .iter()
        .filter_map(|item| {
            item.as_object()
                .and_then(|object| object.get("text"))
                .and_then(|value| value.as_str())
        })
        .collect::<Vec<_>>();
    (!parts.is_empty()).then(|| parts.join("\n"))
}
