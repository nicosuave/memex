use super::*;
use crate::state::{FileIdentity, PendingToolCall};
use serde::de::DeserializeOwned;

#[derive(Deserialize)]
struct Extended<T> {
    #[serde(flatten)]
    _known: T,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

fn extensions<T: DeserializeOwned>(value: Value) -> Result<Map<String, Value>> {
    Ok(serde_json::from_value::<Extended<T>>(value)?.extra)
}

fn preserve<T: DeserializeOwned + Serialize>(
    current: &T,
    previous: Option<&Value>,
) -> Result<Value> {
    let mut value = serde_json::to_value(current)?;
    if let Some(previous) = previous {
        let extra = extensions::<T>(previous.clone())?;
        value
            .as_object_mut()
            .context("checkpoint payload must be an object")?
            .extend(extra);
    }
    Ok(value)
}

pub(super) fn file_payload(file: &FileState, previous: Option<&str>) -> Result<String> {
    let old: Option<Value> = previous.map(serde_json::from_str).transpose()?;
    let mut value = preserve(file, old.as_ref())?;
    value["identity"] = preserve::<FileIdentity>(
        &file.identity,
        old.as_ref().and_then(|value| value.get("identity")),
    )?;
    let calls = value["pending_tool_calls"]
        .as_object_mut()
        .context("invalid pending tool calls")?;
    for (id, call) in &file.pending_tool_calls {
        let previous = old
            .as_ref()
            .and_then(|value| value.get("pending_tool_calls"))
            .and_then(|calls| calls.get(id));
        calls.insert(id.clone(), preserve::<PendingToolCall>(call, previous)?);
    }
    Ok(serde_json::to_string(&value)?)
}

pub(super) fn database_payload(
    databases: &HashMap<String, OpencodeDatabaseState>,
    previous: &str,
) -> Result<String> {
    let old: Value = serde_json::from_str(previous)?;
    let mut value = Map::new();
    for (path, database) in databases {
        value.insert(path.clone(), preserve(database, old.get(path))?);
    }
    Ok(serde_json::to_string(&value)?)
}

pub(super) fn pending_document(raw: Option<&[u8]>) -> Result<Option<Value>> {
    raw.map(|raw| {
        let value: Value =
            serde_json::from_slice(raw).context("invalid pending ingest checkpoint")?;
        ensure!(
            value.is_object(),
            "pending ingest checkpoint must be an object"
        );
        serde_json::from_value::<PendingIngest>(value.clone())
            .context("invalid pending ingest checkpoint")?;
        Ok(value)
    })
    .transpose()
}

pub(super) fn scan_cache_document(raw: Option<&[u8]>) -> Option<Value> {
    raw.filter(|raw| raw.len() as u64 <= ScanCache::MAX_JSON_BYTES)
        .and_then(|raw| serde_json::from_slice::<Value>(raw).ok())
        .filter(Value::is_object)
}

pub(super) fn scan_cache(raw: Option<&str>) -> ScanCache {
    scan_cache_document(raw.map(str::as_bytes))
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default()
}

pub(super) fn pending_payload(pending: &PendingIngest, previous: Option<&str>) -> Result<String> {
    let old = pending_document(previous.map(str::as_bytes))?;
    let mut value = preserve(pending, old.as_ref())?;
    let mut old_scopes = HashMap::new();
    if let Some(scopes) = old
        .as_ref()
        .and_then(|value| value.get("session_scopes"))
        .and_then(Value::as_array)
    {
        for scope in scopes {
            let identity: super::super::SessionScope = serde_json::from_value(scope.clone())?;
            if let Some(previous) = old_scopes.insert(identity, scope) {
                ensure!(
                    previous == scope,
                    "conflicting pending scope extensions for one identity"
                );
            }
        }
    }
    value["session_scopes"] = Value::Array(
        pending
            .session_scopes
            .iter()
            .map(|scope| preserve(scope, old_scopes.get(scope).copied()))
            .collect::<Result<_>>()?,
    );
    Ok(serde_json::to_string(&value)?)
}

pub(super) fn scan_cache_payload(cache: &ScanCache, previous: Option<&str>) -> Result<String> {
    let mut value = serde_json::to_value(cache)?;
    if let Some(Value::Object(mut old)) = scan_cache_document(previous.map(str::as_bytes)) {
        for key in value.as_object().context("invalid scan cache")?.keys() {
            old.remove(key);
        }
        value
            .as_object_mut()
            .context("invalid scan cache")?
            .extend(old);
    }
    Ok(serde_json::to_string(&value)?)
}

pub(super) fn legacy_extras(value: &Value) -> Result<String> {
    Ok(serde_json::to_string(&extensions::<IngestState>(
        value.clone(),
    )?)?)
}
