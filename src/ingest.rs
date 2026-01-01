use crate::config::Paths;
use crate::embed::EmbedderHandle;
use crate::index::SearchIndex;
use crate::progress::Progress;
use crate::state::{FileState, IngestState};
use crate::types::{Record, SourceKind};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use crossbeam_channel::{Receiver, Sender, unbounded};
use memchr::memchr;
use memmap2::Mmap;
use rayon::prelude::*;
use simd_json::BorrowedValue;
use simd_json::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use walkdir::WalkDir;

const EMBED_BATCH_SIZE: usize = 16;
const EMBED_MAX_CHARS: usize = 8192;

#[derive(Debug, Clone)]
pub struct IngestOptions {
    pub claude_source: PathBuf,
    pub include_agents: bool,
    pub include_codex: bool,
    pub embeddings: bool,
    pub backfill_embeddings: bool,
}

#[derive(Debug)]
pub struct IngestReport {
    pub records_added: usize,
    pub records_embedded: usize,
    pub files_scanned: usize,
    pub files_skipped: usize,
}

#[derive(Debug)]
struct FileTask {
    path: PathBuf,
    source: SourceKind,
    offset: u64,
    turn_id: u32,
    size: u64,
    mtime: i64,
    delete_first: bool,
}

#[derive(Debug)]
struct FileUpdate {
    path: String,
    state: FileState,
    session_id: Option<String>,
}

pub fn ingest_all(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
) -> Result<IngestReport> {
    let state_path = paths.state.join("ingest.json");
    let mut state = IngestState::load(&state_path)?;
    let next_doc_id = Arc::new(AtomicU64::new(state.next_doc_id));

    let mut tasks = Vec::new();
    let mut files_scanned = 0usize;
    let mut files_skipped = 0usize;

    if options.claude_source.exists() {
        let claude_files = collect_claude_files(&options.claude_source, options.include_agents)?;
        for path in claude_files {
            let meta = path.metadata()?;
            let size = meta.len();
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            files_scanned += 1;
            let key = path.to_string_lossy().to_string();
            let prev = state.files.get(&key);
            let (offset, turn_id, delete_first, skip) = match prev {
                None => (0, 0, false, false),
                Some(prev) => {
                    if size < prev.size || mtime < prev.mtime {
                        (0, 0, true, false)
                    } else if size == prev.size && mtime == prev.mtime {
                        (prev.offset, prev.turn_id, false, true)
                    } else {
                        (prev.offset, prev.turn_id, false, false)
                    }
                }
            };
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(FileTask {
                path,
                source: SourceKind::Claude,
                offset,
                turn_id,
                size,
                mtime,
                delete_first,
            });
        }
    }

    let mut session_ids = HashSet::new();
    if options.include_codex {
        let codex_files = collect_codex_session_files()?;
        for path in codex_files {
            if let Some(id) = session_id_from_filename(&path) {
                session_ids.insert(id);
            }
            let meta = path.metadata()?;
            let size = meta.len();
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            files_scanned += 1;
            let key = path.to_string_lossy().to_string();
            let prev = state.files.get(&key);
            let (offset, turn_id, delete_first, skip) = match prev {
                None => (0, 0, false, false),
                Some(prev) => {
                    if size < prev.size || mtime < prev.mtime {
                        (0, 0, true, false)
                    } else if size == prev.size && mtime == prev.mtime {
                        (prev.offset, prev.turn_id, false, true)
                    } else {
                        (prev.offset, prev.turn_id, false, false)
                    }
                }
            };
            if skip {
                files_skipped += 1;
                continue;
            }
            tasks.push(FileTask {
                path,
                source: SourceKind::CodexSession,
                offset,
                turn_id,
                size,
                mtime,
                delete_first,
            });
        }
    }

    if options.include_codex {
        let history_path = codex_history_path();
        if history_path.exists() {
            let meta = history_path.metadata()?;
            let size = meta.len();
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            files_scanned += 1;
            let key = history_path.to_string_lossy().to_string();
            let prev = state.files.get(&key);
            let (offset, turn_id, delete_first, skip) = match prev {
                None => (0, 0, false, false),
                Some(prev) => {
                    if size < prev.size || mtime < prev.mtime {
                        (0, 0, true, false)
                    } else if size == prev.size && mtime == prev.mtime {
                        (prev.offset, prev.turn_id, false, true)
                    } else {
                        (prev.offset, prev.turn_id, false, false)
                    }
                }
            };
            if skip {
                files_skipped += 1;
            } else {
                tasks.push(FileTask {
                    path: history_path,
                    source: SourceKind::CodexHistory,
                    offset,
                    turn_id,
                    size,
                    mtime,
                    delete_first,
                });
            }
        }
    }

    let totals = compute_totals(&tasks);
    let file_totals = compute_file_totals(&tasks);
    let progress = Arc::new(Progress::new(totals, file_totals, options.embeddings));

    let (tx_record, rx_record) = unbounded::<Record>();
    let (tx_update, rx_update) = unbounded::<FileUpdate>();

    let delete_paths: Vec<String> = tasks
        .iter()
        .filter(|t| t.delete_first)
        .map(|t| t.path.to_string_lossy().to_string())
        .collect();

    let writer_index = index.clone();
    let embeddings = options.embeddings;
    let backfill_embeddings = options.backfill_embeddings;
    let vector_dir = paths.vectors.clone();
    let progress_for_writer = progress.clone();
    let writer_handle = std::thread::spawn(move || {
        writer_loop(
            writer_index,
            rx_record,
            delete_paths,
            embeddings,
            backfill_embeddings,
            vector_dir,
            progress_for_writer,
        )
    });

    let tasks_arc = Arc::new(tasks);
    tasks_arc.par_iter().try_for_each(|task| -> Result<()> {
        match task.source {
            SourceKind::Claude => {
                parse_claude_file(task, &tx_record, &tx_update, &next_doc_id, &progress)?
            }
            SourceKind::CodexSession => {
                parse_codex_session(task, &tx_record, &tx_update, &next_doc_id, &progress)?
            }
            SourceKind::CodexHistory => parse_codex_history(
                task,
                &tx_record,
                &tx_update,
                &next_doc_id,
                &session_ids,
                &progress,
            )?,
        }
        Ok(())
    })?;

    drop(tx_record);
    drop(tx_update);

    let writer_result = writer_handle
        .join()
        .map_err(|_| anyhow!("writer thread panicked"))?;
    progress.finish();
    let (records_added, records_embedded) = writer_result?;

    let mut updated_files = HashMap::new();
    while let Ok(update) = rx_update.recv() {
        updated_files.insert(update.path.clone(), update.state.clone());
        let _ = update.session_id;
    }

    for (path, update) in updated_files {
        state.files.insert(path, update);
    }
    state.next_doc_id = next_doc_id.load(Ordering::SeqCst);
    state.save(&state_path)?;

    Ok(IngestReport {
        records_added,
        records_embedded,
        files_scanned,
        files_skipped,
    })
}

fn writer_loop(
    index: SearchIndex,
    rx: Receiver<Record>,
    delete_paths: Vec<String>,
    embeddings: bool,
    do_backfill_embeddings: bool,
    vector_dir: PathBuf,
    progress: Arc<Progress>,
) -> Result<(usize, usize)> {
    let mut writer = index.writer()?;
    for path in delete_paths {
        index.delete_by_source_path(&mut writer, &path);
    }

    let mut count = 0usize;
    let mut embedded_count = 0usize;
    let mut vector_index = None;
    let mut embedder: Option<EmbedderHandle> = None;
    let mut embed_buffer: Vec<(u64, String, SourceKind)> = Vec::new();
    if embeddings {
        unsafe {
            std::env::set_var("HF_HUB_DISABLE_PROGRESS_BARS", "1");
        }
        let handle = EmbedderHandle::new()?;
        let dims = handle.dims;
        vector_index = Some(crate::vector::VectorIndex::open_or_create(
            &vector_dir,
            dims,
        )?);
        embedder = Some(handle);
        progress.set_embed_ready();
    }

    for mut record in rx.iter() {
        index.add_record(&mut writer, &record)?;
        progress.add_indexed(record.source, 1);
        if embeddings && is_embedding_role(&record.role) && !record.text.is_empty() {
            let text = truncate_for_embedding(std::mem::take(&mut record.text));
            if let Some(vindex) = vector_index.as_ref()
                && !vindex.contains(record.doc_id)
            {
                progress.add_embed_total(record.source, 1);
                progress.add_embed_pending(record.source, 1);
                embed_buffer.push((record.doc_id, text, record.source));
            }
            if let Some(emb) = embedder.as_mut()
                && embed_buffer.len() >= EMBED_BATCH_SIZE
            {
                embedded_count += flush_embeddings(
                    &mut embed_buffer,
                    emb,
                    vector_index.as_mut().unwrap(),
                    &progress,
                )?;
            }
        }
        count += 1;
    }

    writer.commit()?;
    if embeddings {
        if do_backfill_embeddings {
            embedded_count += backfill_embeddings(
                &index,
                embedder.as_mut().unwrap(),
                vector_index.as_mut().unwrap(),
                &progress,
            )?;
        }

        if !embed_buffer.is_empty() {
            embedded_count += flush_embeddings(
                &mut embed_buffer,
                embedder.as_mut().unwrap(),
                vector_index.as_mut().unwrap(),
                &progress,
            )?;
        }
        if let Some(vindex) = vector_index.as_mut() {
            vindex.save()?;
        }
        if let Some(handle) = embedder.take() {
            std::mem::forget(handle);
        }
    }
    Ok((count, embedded_count))
}

fn backfill_embeddings(
    index: &SearchIndex,
    embedder: &mut EmbedderHandle,
    vector_index: &mut crate::vector::VectorIndex,
    progress: &Arc<Progress>,
) -> Result<usize> {
    use std::cell::Cell;
    let embedded_count = Cell::new(0usize);
    let mut embed_buffer: Vec<(u64, String, SourceKind)> = Vec::new();
    index.for_each_record(|record| {
        if record.text.is_empty()
            || !is_embedding_role(&record.role)
            || vector_index.contains(record.doc_id)
        {
            return Ok(());
        }
        progress.add_embed_total(record.source, 1);
        progress.add_embed_pending(record.source, 1);
        embed_buffer.push((
            record.doc_id,
            truncate_for_embedding(record.text),
            record.source,
        ));
        if embed_buffer.len() >= EMBED_BATCH_SIZE {
            let n = flush_embeddings(&mut embed_buffer, embedder, vector_index, progress)?;
            embedded_count.set(embedded_count.get() + n);
        }
        Ok(())
    })?;
    if !embed_buffer.is_empty() {
        let n = flush_embeddings(&mut embed_buffer, embedder, vector_index, progress)?;
        embedded_count.set(embedded_count.get() + n);
    }
    Ok(embedded_count.get())
}

fn collect_claude_files(source: &Path, include_agents: bool) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in WalkDir::new(source).into_iter().filter_map(Result::ok) {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
            continue;
        }
        if !include_agents
            && let Some(name) = path.file_name().and_then(|n| n.to_str())
            && name.starts_with("agent-")
        {
            continue;
        }
        files.push(path.to_path_buf());
    }
    Ok(files)
}

fn collect_codex_session_files() -> Result<Vec<PathBuf>> {
    let root = codex_sessions_root();
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in WalkDir::new(root).into_iter().filter_map(Result::ok) {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
            continue;
        }
        files.push(path.to_path_buf());
    }
    Ok(files)
}

fn codex_sessions_root() -> PathBuf {
    let home = directories::BaseDirs::new()
        .map(|b| b.home_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("/"));
    home.join(".codex").join("sessions")
}

fn codex_history_path() -> PathBuf {
    let home = directories::BaseDirs::new()
        .map(|b| b.home_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("/"));
    home.join(".codex").join("history.jsonl")
}

fn parse_claude_file(
    task: &FileTask,
    tx_record: &Sender<Record>,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let file = File::open(&task.path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut start = task.offset as usize;
    let mut turn_id = task.turn_id;

    let project = project_from_claude_path(&task.path);
    let session_id = task
        .path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();
    let source_path = task.path.to_string_lossy().to_string();
    let mut tool_id_to_name: HashMap<String, String> = HashMap::new();

    let mut buf = Vec::new();
    let mut parsed_bytes = 0u64;
    while start < mmap.len() {
        let slice = &mmap[start..];
        let rel = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..rel];
        let advanced = rel + 1;
        start += advanced;
        parsed_bytes += advanced as u64;
        if parsed_bytes >= 64 * 1024 {
            progress.add_parsed_bytes(SourceKind::Claude, parsed_bytes);
            parsed_bytes = 0;
        }
        if line.is_empty() {
            continue;
        }
        buf.clear();
        buf.extend_from_slice(line);
        let value: BorrowedValue = match simd_json::to_borrowed_value(&mut buf) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let obj = match value.as_object() {
            Some(o) => o,
            None => continue,
        };
        let entry_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if entry_type != "user" && entry_type != "assistant" {
            continue;
        }
        let timestamp = obj
            .get("timestamp")
            .and_then(|v| v.as_str())
            .and_then(parse_iso_millis)
            .unwrap_or(0);
        let message = match obj.get("message").and_then(|v| v.as_object()) {
            Some(m) => m,
            None => continue,
        };
        let content = message.get("content");
        let mut text_parts = Vec::new();
        if let Some(content) = content {
            if let Some(text) = content.as_str() {
                text_parts.push(text);
            } else if let Some(arr) = content.as_array() {
                for block in arr {
                    let block_obj = match block.as_object() {
                        Some(b) => b,
                        None => continue,
                    };
                    let block_type = block_obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    if block_type == "text" {
                        if let Some(text) = block_obj.get("text").and_then(|v| v.as_str()) {
                            text_parts.push(text);
                        }
                    } else if block_type == "tool_use" {
                        let tool_name = block_obj
                            .get("name")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        if let (Some(id), Some(name)) = (
                            block_obj.get("id").and_then(|v| v.as_str()),
                            tool_name.clone(),
                        ) {
                            tool_id_to_name.insert(id.to_string(), name);
                        }
                        let tool_input = block_obj.get("input").map(|v| v.to_string());
                        let text = tool_input.clone().unwrap_or_default();
                        let record = Record {
                            source: SourceKind::Claude,
                            doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                            ts: timestamp,
                            project: project.clone(),
                            session_id: session_id.clone(),
                            turn_id,
                            role: "tool_use".to_string(),
                            text,
                            tool_name,
                            tool_input,
                            tool_output: None,
                            source_path: source_path.clone(),
                        };
                        progress.add_produced(SourceKind::Claude, 1);
                        tx_record.send(record)?;
                        turn_id += 1;
                    }
                }
            }
        }

        if entry_type == "user"
            && let Some(content) = content
            && let Some(arr) = content.as_array()
        {
            for block in arr {
                let block_obj = match block.as_object() {
                    Some(b) => b,
                    None => continue,
                };
                if block_obj.get("type").and_then(|v| v.as_str()) == Some("tool_result") {
                    let tool_output = block_obj.get("content").map(|v| v.to_string());
                    let mut text = extract_text_from_tool_result(block).unwrap_or_default();
                    if text.is_empty()
                        && let Some(content) = block_obj.get("content")
                    {
                        text = content.to_string();
                    }
                    let tool_name = block_obj
                        .get("tool_use_id")
                        .and_then(|v| v.as_str())
                        .and_then(|id| tool_id_to_name.get(id))
                        .cloned();
                    let record = Record {
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
                        source_path: source_path.clone(),
                    };
                    progress.add_produced(SourceKind::Claude, 1);
                    tx_record.send(record)?;
                    turn_id += 1;
                }
            }
        }

        let text = text_parts.join(" ").trim().to_string();
        if !text.is_empty() {
            let record = Record {
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
                source_path: source_path.clone(),
            };
            progress.add_produced(SourceKind::Claude, 1);
            tx_record.send(record)?;
            turn_id += 1;
        }
    }

    if parsed_bytes > 0 {
        progress.add_parsed_bytes(SourceKind::Claude, parsed_bytes);
    }
    progress.add_files_done(SourceKind::Claude, 1);
    let state = FileState {
        size: task.size,
        mtime: task.mtime,
        offset: mmap.len() as u64,
        turn_id,
    };
    tx_update.send(FileUpdate {
        path: source_path,
        state,
        session_id: Some(session_id),
    })?;
    Ok(())
}

fn parse_codex_session(
    task: &FileTask,
    tx_record: &Sender<Record>,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let file = File::open(&task.path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut start = task.offset as usize;
    let mut turn_id = task.turn_id;

    let source_path = task.path.to_string_lossy().to_string();
    let mut session_id =
        session_id_from_filename(&task.path).unwrap_or_else(|| "unknown".to_string());
    let mut project = "codex".to_string();
    let mut call_id_to_name: HashMap<String, String> = HashMap::new();

    let mut buf = Vec::new();
    let mut parsed_bytes = 0u64;
    while start < mmap.len() {
        let slice = &mmap[start..];
        let rel = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..rel];
        let advanced = rel + 1;
        start += advanced;
        parsed_bytes += advanced as u64;
        if parsed_bytes >= 64 * 1024 {
            progress.add_parsed_bytes(SourceKind::CodexSession, parsed_bytes);
            parsed_bytes = 0;
        }
        if line.is_empty() {
            continue;
        }
        buf.clear();
        buf.extend_from_slice(line);
        let value: BorrowedValue = match simd_json::to_borrowed_value(&mut buf) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let obj = match value.as_object() {
            Some(o) => o,
            None => continue,
        };
        let entry_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let timestamp = obj
            .get("timestamp")
            .and_then(|v| v.as_str())
            .and_then(parse_iso_millis)
            .unwrap_or(0);
        if entry_type == "session_meta" {
            if let Some(payload) = obj.get("payload").and_then(|v| v.as_object()) {
                if let Some(id) = payload.get("id").and_then(|v| v.as_str()) {
                    session_id = id.to_string();
                }
                if let Some(cwd) = payload.get("cwd").and_then(|v| v.as_str()) {
                    project = project_from_path(cwd);
                }
            }
            continue;
        }
        if entry_type != "response_item" {
            continue;
        }
        let payload = match obj.get("payload").and_then(|v| v.as_object()) {
            Some(p) => p,
            None => continue,
        };
        let payload_type = payload.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if payload_type == "message" {
            let role = payload.get("role").and_then(|v| v.as_str()).unwrap_or("");
            let content = payload.get("content");
            let mut text_parts = Vec::new();
            if let Some(content) = content {
                if let Some(text) = content.as_str() {
                    text_parts.push(text);
                } else if let Some(arr) = content.as_array() {
                    for block in arr {
                        if let Some(block_obj) = block.as_object()
                            && let Some(text) = block_obj.get("text").and_then(|v| v.as_str())
                        {
                            text_parts.push(text);
                        }
                    }
                }
            }
            let text = text_parts.join("\n").trim().to_string();
            if text.is_empty() {
                continue;
            }
            if is_system_instruction(&text) {
                continue;
            }
            let record = Record {
                source: SourceKind::CodexSession,
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
                source_path: source_path.clone(),
            };
            progress.add_produced(SourceKind::CodexSession, 1);
            tx_record.send(record)?;
            turn_id += 1;
        } else if payload_type == "function_call" {
            let tool_name = payload
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let tool_input = payload
                .get("arguments")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if let Some(call_id) = payload.get("call_id").and_then(|v| v.as_str())
                && let Some(name) = tool_name.clone()
            {
                call_id_to_name.insert(call_id.to_string(), name);
            }
            let text = tool_input.clone().unwrap_or_default();
            let record = Record {
                source: SourceKind::CodexSession,
                doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                ts: timestamp,
                project: project.clone(),
                session_id: session_id.clone(),
                turn_id,
                role: "tool_use".to_string(),
                text,
                tool_name,
                tool_input,
                tool_output: None,
                source_path: source_path.clone(),
            };
            progress.add_produced(SourceKind::CodexSession, 1);
            tx_record.send(record)?;
            turn_id += 1;
        } else if payload_type == "function_call_output" {
            let call_id = payload
                .get("call_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let tool_name = call_id_to_name.get(call_id).cloned();
            let tool_output = payload
                .get("output")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let text = tool_output.clone().unwrap_or_default();
            if text.is_empty() {
                continue;
            }
            let record = Record {
                source: SourceKind::CodexSession,
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
                source_path: source_path.clone(),
            };
            progress.add_produced(SourceKind::CodexSession, 1);
            tx_record.send(record)?;
            turn_id += 1;
        }
    }

    if parsed_bytes > 0 {
        progress.add_parsed_bytes(SourceKind::CodexSession, parsed_bytes);
    }
    progress.add_files_done(SourceKind::CodexSession, 1);
    let state = FileState {
        size: task.size,
        mtime: task.mtime,
        offset: mmap.len() as u64,
        turn_id,
    };
    tx_update.send(FileUpdate {
        path: source_path,
        state,
        session_id: Some(session_id),
    })?;
    Ok(())
}

fn parse_codex_history(
    task: &FileTask,
    tx_record: &Sender<Record>,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    session_ids: &HashSet<String>,
    progress: &Arc<Progress>,
) -> Result<()> {
    let file = File::open(&task.path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let mut start = task.offset as usize;
    let mut turn_id = task.turn_id;
    let source_path = task.path.to_string_lossy().to_string();

    let mut buf = Vec::new();
    let mut parsed_bytes = 0u64;
    while start < mmap.len() {
        let slice = &mmap[start..];
        let rel = memchr(b'\n', slice).unwrap_or(slice.len());
        let line = &slice[..rel];
        let advanced = rel + 1;
        start += advanced;
        parsed_bytes += advanced as u64;
        if parsed_bytes >= 64 * 1024 {
            progress.add_parsed_bytes(SourceKind::CodexHistory, parsed_bytes);
            parsed_bytes = 0;
        }
        if line.is_empty() {
            continue;
        }
        buf.clear();
        buf.extend_from_slice(line);
        let value: BorrowedValue = match simd_json::to_borrowed_value(&mut buf) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let obj = match value.as_object() {
            Some(o) => o,
            None => continue,
        };
        let session_id = obj.get("session_id").and_then(|v| v.as_str()).unwrap_or("");
        if session_id.is_empty() || session_ids.contains(session_id) {
            continue;
        }
        let ts = obj.get("ts").and_then(|v| v.as_i64()).unwrap_or(0);
        let ts_ms = (ts.max(0) as u64) * 1000;
        let text = obj
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if text.is_empty() {
            continue;
        }
        let record = Record {
            source: SourceKind::CodexHistory,
            doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
            ts: ts_ms,
            project: "codex".to_string(),
            session_id: session_id.to_string(),
            turn_id,
            role: "user".to_string(),
            text,
            tool_name: None,
            tool_input: None,
            tool_output: None,
            source_path: source_path.clone(),
        };
        progress.add_produced(SourceKind::CodexHistory, 1);
        tx_record.send(record)?;
        turn_id += 1;
    }

    if parsed_bytes > 0 {
        progress.add_parsed_bytes(SourceKind::CodexHistory, parsed_bytes);
    }
    progress.add_files_done(SourceKind::CodexHistory, 1);
    let state = FileState {
        size: task.size,
        mtime: task.mtime,
        offset: mmap.len() as u64,
        turn_id,
    };
    tx_update.send(FileUpdate {
        path: source_path,
        state,
        session_id: None,
    })?;
    Ok(())
}

fn parse_iso_millis(input: &str) -> Option<u64> {
    DateTime::parse_from_rfc3339(input)
        .ok()
        .map(|dt| dt.with_timezone(&Utc).timestamp_millis() as u64)
}

fn project_from_claude_path(path: &Path) -> String {
    let Some(parent) = path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
    else {
        return "unknown".to_string();
    };
    decode_project_name(parent)
}

fn project_from_path(path: &str) -> String {
    let p = Path::new(path);
    if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
        return name.to_string();
    }
    "codex".to_string()
}

fn decode_project_name(folder_name: &str) -> String {
    let prefixes_to_strip = ["-home-", "-mnt-c-Users-", "-mnt-c-users-", "-Users-"];
    let mut name = folder_name;
    for prefix in prefixes_to_strip {
        if name.to_lowercase().starts_with(&prefix.to_lowercase()) {
            name = &name[prefix.len()..];
            break;
        }
    }
    let parts: Vec<&str> = name.split('-').filter(|p| !p.is_empty()).collect();
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

    for (i, part) in parts.iter().enumerate() {
        if i == 0 && !found_project {
            let remaining: Vec<String> = parts[i + 1..].iter().map(|p| p.to_lowercase()).collect();
            if remaining.iter().any(|d| skip_dirs.contains(&d.as_str())) {
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
        return folder_name.to_string();
    }
    meaningful.join("-")
}

fn extract_text_from_tool_result(block: &simd_json::BorrowedValue) -> Option<String> {
    let obj = block.as_object()?;
    let content = obj.get("content")?;
    if let Some(text) = content.as_str() {
        return Some(text.to_string());
    }
    if let Some(arr) = content.as_array() {
        let mut parts = Vec::new();
        for item in arr {
            if let Some(obj) = item.as_object()
                && obj.get("type").and_then(|v| v.as_str()) == Some("text")
                && let Some(text) = obj.get("text").and_then(|v| v.as_str())
            {
                parts.push(text);
            }
        }
        if !parts.is_empty() {
            return Some(parts.join(" "));
        }
    }
    None
}

fn session_id_from_filename(path: &Path) -> Option<String> {
    static UUID_RE: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| {
        regex::Regex::new(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})")
            .expect("uuid regex")
    });
    let name = path.file_stem()?.to_string_lossy();
    UUID_RE
        .captures(&name)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
}

fn is_system_instruction(text: &str) -> bool {
    let t = text.trim_start();
    t.starts_with("<system_instruction>") || t.starts_with("<system-instruction>")
}

fn flush_embeddings(
    buffer: &mut Vec<(u64, String, SourceKind)>,
    embedder: &mut EmbedderHandle,
    vindex: &mut crate::vector::VectorIndex,
    progress: &Arc<Progress>,
) -> Result<usize> {
    if buffer.is_empty() {
        return Ok(0);
    }
    let mut count = 0;
    for (doc_id, text, source) in buffer.drain(..) {
        let text = truncate_for_embedding(text);
        if text.is_empty() {
            continue;
        }
        let embeddings = embedder.embed_texts(&[text.as_str()])?;
        if let Some(vec) = embeddings.get(0) {
            vindex.add(doc_id, vec)?;
            progress.add_embedded(source, 1);
            count += 1;
        }
    }
    Ok(count)
}

fn compute_totals(tasks: &[FileTask]) -> [u64; 3] {
    let mut totals = [0u64; 3];
    for task in tasks {
        let remaining = task.size.saturating_sub(task.offset);
        totals[task.source.idx()] += remaining;
    }
    totals
}

fn compute_file_totals(tasks: &[FileTask]) -> [u64; 3] {
    let mut totals = [0u64; 3];
    for task in tasks {
        totals[task.source.idx()] += 1;
    }
    totals
}

fn truncate_for_embedding(mut text: String) -> String {
    if text.len() <= EMBED_MAX_CHARS {
        return text;
    }
    let mut end = EMBED_MAX_CHARS.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text
}

fn is_embedding_role(role: &str) -> bool {
    role == "user" || role == "assistant"
}
