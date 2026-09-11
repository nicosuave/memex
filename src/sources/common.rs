use crate::state::PendingToolCall;
use crate::types::RecordLinks;
use chrono::{DateTime, Utc};
use directories::BaseDirs;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

/// Maps a transcript for one sequential pass and tells the kernel so, which widens its
/// read-ahead window beyond what it does around an ordinary fault. A rebuild reads gigabytes
/// of transcripts, so the difference is measurable there.
///
/// Only `MADV_SEQUENTIAL`, which both Linux and Darwin record on the mapping and consult on
/// every fault. `MADV_WILLNEED` is a one-shot request to page the whole mapping in now, which
/// contradicts asking for a sliding window and, on a transcript larger than the free page
/// cache, evicts pages the rest of the refresh still wants.
///
/// The advice is only a hint: `madvise` can refuse it without the mapping becoming any less
/// valid, so a refusal must not fail the parse. It is counted instead, because silently
/// losing it would show up as a slow rebuild and nothing else.
pub(crate) fn map_sequential(file: &std::fs::File) -> std::io::Result<memmap2::Mmap> {
    let mmap = unsafe { memmap2::Mmap::map(file)? };
    if mmap.advise(memmap2::Advice::Sequential).is_ok() {
        crate::profiling::count!("sources.mmap_advice_accepted", 1);
    } else {
        crate::profiling::count!("sources.mmap_advice_refused", 1);
    }
    Ok(mmap)
}

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
    jsonl_files_with(roots, None)
}

/// `.jsonl` files below `roots`, reusing directory stamps from `walk` when one is supplied.
pub(crate) fn jsonl_files_with(
    roots: impl IntoIterator<Item = PathBuf>,
    mut walk: Option<&mut crate::ingest::directories::StampedWalk>,
) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for root in roots {
        if !root.exists() {
            continue;
        }
        files.extend(
            files_under(&root, walk.as_deref_mut())
                .into_iter()
                .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("jsonl")),
        );
    }
    files.sort();
    files.dedup();
    files
}

/// Every regular file below `root`, without following symlinks below it.
pub(crate) fn files_under(
    root: &Path,
    walk: Option<&mut crate::ingest::directories::StampedWalk>,
) -> Vec<PathBuf> {
    match walk {
        Some(walk) => walk.files(root),
        None => WalkDir::new(root)
            .into_iter()
            .flatten()
            .filter(|entry| entry.file_type().is_file())
            .map(|entry| entry.path().to_path_buf())
            .collect(),
    }
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
