use crate::types::{SourceFilter, SourceKind};
use anyhow::Result;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::io::BufRead;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct SourceAudit {
    pub source: String,
    pub files: u64,
    pub valid_json_lines: u64,
    pub malformed_json_lines: u64,
    pub non_object_json_lines: u64,
    pub encrypted_reasoning_rows: u64,
    pub top_level_types: BTreeMap<String, u64>,
    pub semantic_types: BTreeMap<String, u64>,
    pub content_block_types: BTreeMap<String, u64>,
    pub producer_versions: BTreeMap<String, u64>,
}

pub fn audit_installed_sources(source: Option<SourceFilter>) -> Result<Vec<SourceAudit>> {
    let mut groups = Vec::new();
    let mut push = |kind: SourceKind, files: Vec<PathBuf>| {
        if source.is_none_or(|filter| filter.matches(kind)) {
            groups.push((kind, files));
        }
    };

    push(SourceKind::Claude, super::claude::usage_files());
    push(
        SourceKind::Codex,
        super::codex::discover_rollouts()
            .into_iter()
            .map(|file| file.path)
            .collect(),
    );
    push(
        SourceKind::Opencode,
        super::opencode::discover_sessions()?
            .into_iter()
            .map(|file| file.path)
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("jsonl"))
            .collect(),
    );
    push(
        SourceKind::Cursor,
        super::cursor::discover_transcripts()
            .into_iter()
            .map(|file| file.path)
            .collect(),
    );
    push(
        SourceKind::Pi,
        super::pi::discover()
            .into_iter()
            .map(|file| file.path)
            .collect(),
    );
    push(
        SourceKind::OpenClaw,
        super::openclaw::discover()
            .into_iter()
            .map(|file| file.path)
            .collect(),
    );
    push(
        SourceKind::Copilot,
        super::copilot::discover_sessions()
            .into_iter()
            .map(|file| file.path)
            .collect(),
    );
    push(
        SourceKind::Grok,
        super::grok::discover_sessions()
            .into_iter()
            .map(|file| file.path)
            .collect(),
    );
    push(
        SourceKind::Hermes,
        super::hermes::discover()
            .into_iter()
            .map(|file| file.path)
            .collect(),
    );

    push(
        SourceKind::Omp,
        super::omp::discover()
            .into_iter()
            .map(|file| file.path)
            .collect(),
    );
    groups
        .into_iter()
        .map(|(kind, files)| audit_files(kind, &deduplicate(files)))
        .collect()
}

fn deduplicate(files: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut seen = HashSet::new();
    files
        .into_iter()
        .filter(|path| seen.insert(path.clone()))
        .collect()
}

pub fn audit_files(source: SourceKind, files: &[PathBuf]) -> Result<SourceAudit> {
    let mut audit = SourceAudit {
        source: source.storage_label().to_string(),
        files: files.len() as u64,
        ..SourceAudit::default()
    };
    for path in files {
        audit_file(source, path, &mut audit)?;
    }
    Ok(audit)
}

fn audit_file(source: SourceKind, path: &Path, audit: &mut SourceAudit) -> Result<()> {
    if source == SourceKind::Hermes {
        // Hermes usage truth is SQLite aggregate data.  Audit must not reinterpret the
        // database as JSON, and in particular must not read transcript/message columns.
        std::fs::File::open(path)?;
        return Ok(());
    }
    let reader = std::io::BufReader::new(std::fs::File::open(path)?);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => {
                audit.malformed_json_lines += 1;
                continue;
            }
        };
        let Some(object) = value.as_object() else {
            audit.non_object_json_lines += 1;
            continue;
        };
        audit.valid_json_lines += 1;
        let top_level = object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        increment(&mut audit.top_level_types, top_level);

        if let Some(version) = producer_version(source, &value) {
            increment(&mut audit.producer_versions, &version);
        }
        record_semantics(source, &value, top_level, audit);
    }
    Ok(())
}

fn producer_version(source: SourceKind, value: &Value) -> Option<String> {
    let version = match source {
        SourceKind::Codex => value
            .get("payload")
            .and_then(|payload| payload.get("cli_version")),
        _ => value.get("version"),
    }?;
    match version {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn record_semantics(source: SourceKind, value: &Value, top_level: &str, audit: &mut SourceAudit) {
    match source {
        SourceKind::Codex => {
            if let Some(payload) = value.get("payload").and_then(Value::as_object)
                && let Some(payload_type) = payload.get("type").and_then(Value::as_str)
            {
                increment(
                    &mut audit.semantic_types,
                    &format!("{top_level}/{payload_type}"),
                );
                if payload.contains_key("encrypted_content") {
                    audit.encrypted_reasoning_rows += 1;
                }
                record_content_blocks(payload.get("content"), audit);
            }
        }
        SourceKind::Claude => {
            if matches!(top_level, "user" | "assistant") {
                increment(&mut audit.semantic_types, top_level);
                if let Some(content) = value
                    .get("message")
                    .and_then(|message| message.get("content"))
                {
                    record_content_blocks(Some(content), audit);
                }
            }
        }
        SourceKind::Pi | SourceKind::OpenClaw | SourceKind::Omp => {
            if top_level == "message"
                && let Some(message) = value.get("message").and_then(Value::as_object)
            {
                let role = message
                    .get("role")
                    .and_then(Value::as_str)
                    .unwrap_or("<missing>");
                increment(&mut audit.semantic_types, &format!("message/{role}"));
                record_content_blocks(message.get("content"), audit);
            }
        }
        SourceKind::Opencode | SourceKind::Cursor | SourceKind::Copilot => {
            if let Some(role) = value.get("role").and_then(Value::as_str) {
                increment(&mut audit.semantic_types, role);
            }
            record_content_blocks(value.get("content"), audit);
        }
        SourceKind::Grok => {
            if let Some(kind) = value
                .pointer("/params/update/sessionUpdate")
                .and_then(Value::as_str)
            {
                increment(&mut audit.semantic_types, kind);
            }
            record_content_blocks(value.pointer("/params/update/content"), audit);
        }
        SourceKind::Hermes => {
            if value.get("records").and_then(Value::as_array).is_some() {
                increment(&mut audit.semantic_types, "records");
            }
        }
    }
}

fn record_content_blocks(content: Option<&Value>, audit: &mut SourceAudit) {
    let Some(array) = content.and_then(Value::as_array) else {
        return;
    };
    for block in array {
        let Some(object) = block.as_object() else {
            continue;
        };
        let block_type = object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        increment(&mut audit.content_block_types, block_type);
        if matches!(block_type, "redacted_thinking" | "encrypted_reasoning")
            || object.contains_key("encrypted_content")
        {
            audit.encrypted_reasoning_rows += 1;
        }
    }
}

fn increment(map: &mut BTreeMap<String, u64>, key: &str) {
    *map.entry(key.to_string()).or_default() += 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn audit_reports_structure_without_field_values() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("codex.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"type\":\"session_meta\",\"payload\":{\"cli_version\":\"1.2.3\"}}\n",
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"reasoning\",",
                "\"encrypted_content\":\"ciphertext\",\"summary\":[]}}\n",
                "not-json\n"
            ),
        )
        .unwrap();

        let audit = audit_files(SourceKind::Codex, &[path]).unwrap();
        assert_eq!(audit.files, 1);
        assert_eq!(audit.valid_json_lines, 2);
        assert_eq!(audit.malformed_json_lines, 1);
        assert_eq!(audit.encrypted_reasoning_rows, 1);
        assert_eq!(audit.producer_versions.get("1.2.3"), Some(&1));
        let json = serde_json::to_string(&audit).unwrap();
        assert!(!json.contains("ciphertext"));
    }

    #[test]
    fn audit_reports_numeric_producer_versions() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("pi.jsonl");
        fs::write(&path, "{\"type\":\"session\",\"version\":3}\n").unwrap();

        let audit = audit_files(SourceKind::Pi, &[path]).unwrap();
        assert_eq!(audit.producer_versions.get("3"), Some(&1));
    }
}
