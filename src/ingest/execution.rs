use super::*;

pub(super) fn parse_claude_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let (parsed, background_session) = crate::sources::claude::parse_index_records_with_background(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            legacy_turn_id: task.legacy_turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        task.claude_background,
        task.size,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Claude, 1);
            tx_record.send(record)
        },
    )?;
    finish_parsed_source(
        task,
        tx_update,
        progress,
        SourceKind::Claude,
        source_path,
        parsed,
        None,
        Some(background_session),
    )
}

pub(super) fn parse_antigravity_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::antigravity::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            legacy_turn_id: task.legacy_turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Antigravity, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Antigravity,
        source_path,
        parsed,
    )
}

pub(super) fn parse_codex_session(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let (parsed, metadata_offsets) =
        crate::sources::codex::parse_index_records_with_metadata_offsets(
            &task.path,
            crate::sources::IndexParseState {
                offset: task.offset,
                turn_id: task.turn_id,
                legacy_turn_id: task.legacy_turn_id,
                pending_tool_calls: task.pending_tool_calls.clone(),
            },
            include_reasoning,
            next_doc_id,
            task.codex_metadata_offsets.as_deref(),
            |record| {
                progress.add_produced(SourceKind::Codex, 1);
                tx_record.send(record)
            },
        )?;
    finish_source_parse_with_codex_metadata(
        task,
        tx_update,
        progress,
        SourceKind::Codex,
        source_path,
        parsed,
        Some(metadata_offsets),
    )
}

pub(super) fn parse_codex_history(
    task: &FileTask,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    session_ids: &HashSet<String>,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::codex::parse_history_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            legacy_turn_id: task.legacy_turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        session_ids,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Codex, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Codex,
        source_path,
        parsed,
    )
}

pub(super) fn finish_source_parse(
    task: &FileTask,
    tx_update: &Sender<FileUpdate>,
    progress: &Arc<Progress>,
    source: SourceKind,
    source_path: String,
    parsed: crate::sources::IndexParseOutput,
) -> Result<()> {
    finish_parsed_source(
        task,
        tx_update,
        progress,
        source,
        source_path,
        parsed,
        None,
        None,
    )
}

pub(super) fn finish_source_parse_with_codex_metadata(
    task: &FileTask,
    tx_update: &Sender<FileUpdate>,
    progress: &Arc<Progress>,
    source: SourceKind,
    source_path: String,
    parsed: crate::sources::IndexParseOutput,
    codex_metadata_offsets: Option<Vec<u64>>,
) -> Result<()> {
    finish_parsed_source(
        task,
        tx_update,
        progress,
        source,
        source_path,
        parsed,
        codex_metadata_offsets,
        None,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "one checkpoint assembled from independent parser outputs"
)]
fn finish_parsed_source(
    task: &FileTask,
    tx_update: &Sender<FileUpdate>,
    progress: &Arc<Progress>,
    source: SourceKind,
    source_path: String,
    parsed: crate::sources::IndexParseOutput,
    codex_metadata_offsets: Option<Vec<u64>>,
    background_session: Option<bool>,
) -> Result<()> {
    progress.add_parsed_bytes(source, parsed.offset.saturating_sub(task.offset));
    progress.add_files_done(source, 1);
    let mut state = completed_file_state(
        task,
        parsed.offset,
        parsed.turn_id,
        parsed.legacy_turn_id,
        parsed.pending_tool_calls,
    );
    state.codex_metadata_offsets = codex_metadata_offsets;
    if source == SourceKind::Claude {
        state.claude_background = background_session;
    }
    tx_update.send(FileUpdate {
        path: source_path,
        state,
        session_id: parsed.session_id,
        diagnostics: parsed.diagnostics,
    })?;
    Ok(())
}
pub(super) fn parse_opencode_file(
    task: &FileTask,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
    opencode_session_links: &HashMap<String, crate::sources::opencode::SessionLinks>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::opencode::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            legacy_turn_id: task.legacy_turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        opencode_session_links,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Opencode, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Opencode,
        source_path,
        parsed,
    )
}

pub(super) fn parse_jcode_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::jcode::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            legacy_turn_id: task.legacy_turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Jcode, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Jcode,
        source_path,
        parsed,
    )
}

pub(super) fn parse_muse_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::muse::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            legacy_turn_id: task.legacy_turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Muse, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Muse,
        source_path,
        parsed,
    )
}

pub(super) fn parse_cursor_file(
    task: &FileTask,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::cursor::parse_index_records(
        &task.path,
        task.mtime,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            legacy_turn_id: task.legacy_turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Cursor, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Cursor,
        source_path,
        parsed,
    )
}
pub(super) fn parse_pi_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::pi::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            legacy_turn_id: task.legacy_turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Pi, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Pi,
        source_path,
        parsed,
    )
}
pub(super) fn parse_omp_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::omp::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            legacy_turn_id: task.legacy_turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Omp, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Omp,
        source_path,
        parsed,
    )
}
pub(super) fn parse_openclaw_file(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::openclaw::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            legacy_turn_id: task.legacy_turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::OpenClaw, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::OpenClaw,
        source_path,
        parsed,
    )
}
pub(super) fn parse_copilot_session(
    task: &FileTask,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::copilot::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            legacy_turn_id: task.legacy_turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Copilot, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Copilot,
        source_path,
        parsed,
    )
}

pub(super) fn parse_grok_session(
    task: &FileTask,
    include_reasoning: bool,
    tx_record: &RecordSender,
    tx_update: &Sender<FileUpdate>,
    next_doc_id: &AtomicU64,
    progress: &Arc<Progress>,
) -> Result<()> {
    let source_path = task.path.to_string_lossy().to_string();
    let parsed = crate::sources::grok::parse_index_records(
        &task.path,
        crate::sources::IndexParseState {
            offset: task.offset,
            turn_id: task.turn_id,
            legacy_turn_id: task.legacy_turn_id,
            pending_tool_calls: task.pending_tool_calls.clone(),
        },
        include_reasoning,
        next_doc_id,
        |record| {
            progress.add_produced(SourceKind::Grok, 1);
            tx_record.send(record)
        },
    )?;
    finish_source_parse(
        task,
        tx_update,
        progress,
        SourceKind::Grok,
        source_path,
        parsed,
    )
}
pub(super) fn flush_embeddings(
    buffer: &mut Vec<(u64, String, SourceKind)>,
    embedder: &mut EmbedderHandle,
    vindex: &mut crate::vector::VectorIndex,
    progress: &Arc<Progress>,
) -> Result<usize> {
    if buffer.is_empty() {
        return Ok(0);
    }

    // Prepare texts for batch embedding
    let items: Vec<(u64, String, SourceKind)> = buffer
        .drain(..)
        .map(|(doc_id, text, source)| (doc_id, truncate_for_embedding(text), source))
        .filter(|(_, text, _)| !text.is_empty())
        .collect();

    if items.is_empty() {
        return Ok(0);
    }

    // Batch embed all texts at once (ONNX Runtime handles internal parallelism)
    let texts: Vec<&str> = items.iter().map(|(_, text, _)| text.as_str()).collect();
    let embeddings = embedder.embed_texts(&texts)?;

    // Add embeddings to index
    let mut count = 0;
    for ((doc_id, _, source), vec) in items.iter().zip(embeddings.iter()) {
        vindex.add(*doc_id, vec)?;
        progress.sub_embed_pending(*source, 1);
        progress.add_embedded(*source, 1);
        count += 1;
    }
    Ok(count)
}

pub(super) fn compute_totals(tasks: &[FileTask]) -> [u64; SOURCE_COUNT] {
    let mut totals = [0u64; SOURCE_COUNT];
    for task in tasks {
        let remaining = task.size.saturating_sub(task.offset);
        totals[task.source.idx()] += remaining;
    }
    totals
}

pub(super) fn compute_file_totals(tasks: &[FileTask]) -> [u64; SOURCE_COUNT] {
    let mut totals = [0u64; SOURCE_COUNT];
    for task in tasks {
        totals[task.source.idx()] += 1;
    }
    totals
}

pub(super) fn truncate_for_embedding(mut text: String) -> String {
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

pub(super) fn limit_record_tool_content(
    record: &mut Record,
    limits: IndexedToolContentLimits,
) -> (bool, bool) {
    let original_input_len = record.tool_input.as_ref().map(String::len);
    let original_output_len = record.tool_output.as_ref().map(String::len);
    let text_limit = match record.role.as_str() {
        "tool_use" => Some(limits.input_bytes),
        "tool_result" => Some(limits.output_bytes),
        _ if record.tool_output.is_some() => Some(limits.output_bytes),
        _ if record.tool_input.is_some() => Some(limits.input_bytes),
        _ => None,
    };
    if let Some(max_bytes) = text_limit {
        truncate_for_index(&mut record.text, max_bytes);
    }
    if let Some(tool_input) = record.tool_input.as_mut() {
        truncate_for_index(tool_input, limits.input_bytes);
    }
    if let Some(tool_output) = record.tool_output.as_mut() {
        truncate_for_index(tool_output, limits.output_bytes);
    }
    (
        original_input_len
            .zip(record.tool_input.as_ref().map(String::len))
            .is_some_and(|(before, after)| after < before),
        original_output_len
            .zip(record.tool_output.as_ref().map(String::len))
            .is_some_and(|(before, after)| after < before),
    )
}

pub(super) fn truncate_for_index(text: &mut String, max_bytes: usize) {
    if text.len() <= max_bytes {
        return;
    }

    let original_len = text.len();
    let mut marker = truncation_marker(original_len);
    let (head_end, tail_start) = loop {
        let retained_bytes = max_bytes.saturating_sub(marker.len());
        let head_target = retained_bytes.saturating_mul(RETAINED_HEAD_PERCENT) / 100;
        let head_end = char_boundary_at_or_before(text, head_target);
        let tail_target = retained_bytes.saturating_sub(head_end);
        let tail_start = char_boundary_at_or_after(text, original_len.saturating_sub(tail_target));
        let omitted_bytes = tail_start.saturating_sub(head_end);
        let updated_marker = truncation_marker(omitted_bytes);
        if updated_marker.len() == marker.len() {
            marker = updated_marker;
            break (head_end, tail_start);
        }
        marker = updated_marker;
    };

    let tail = text[tail_start..].to_string();
    text.truncate(head_end);
    text.push_str(&marker);
    text.push_str(&tail);
}

pub(super) fn char_boundary_at_or_before(text: &str, mut position: usize) -> usize {
    position = position.min(text.len());
    while position > 0 && !text.is_char_boundary(position) {
        position -= 1;
    }
    position
}

pub(super) fn char_boundary_at_or_after(text: &str, mut position: usize) -> usize {
    position = position.min(text.len());
    while position < text.len() && !text.is_char_boundary(position) {
        position += 1;
    }
    position
}

pub(super) fn truncation_marker(omitted_bytes: usize) -> String {
    format!("\n\n[... {omitted_bytes} bytes truncated ...]\n\n")
}

pub(super) fn is_embedding_role(role: &str) -> bool {
    role == "user" || role == "assistant"
}

pub(super) struct ParserContext<'a> {
    pub pool: &'a rayon::ThreadPool,
    pub options: &'a IngestOptions,
    pub records: &'a RecordSender,
    pub updates: &'a Sender<FileUpdate>,
    pub next_id: &'a AtomicU64,
    pub progress: &'a Arc<Progress>,
    pub session_ids: &'a HashSet<String>,
    pub opencode_links: &'a HashMap<String, crate::sources::opencode::SessionLinks>,
}

impl ParserContext<'_> {
    pub fn parse(&self, tasks: &[FileTask], skipped: &AtomicUsize) -> Result<()> {
        let parse = |task: &FileTask| {
            crate::profiling::span!("ingest.parse_file");
            let result = match task.source {
                SourceKind::Claude => parse_claude_file(
                    task,
                    self.options.include_reasoning,
                    self.records,
                    self.updates,
                    self.next_id,
                    self.progress,
                ),
                SourceKind::Codex => {
                    if crate::sources::codex::is_history_path(&task.path) {
                        parse_codex_history(
                            task,
                            self.records,
                            self.updates,
                            self.next_id,
                            self.session_ids,
                            self.progress,
                        )
                    } else {
                        parse_codex_session(
                            task,
                            self.options.include_reasoning,
                            self.records,
                            self.updates,
                            self.next_id,
                            self.progress,
                        )
                    }
                }
                SourceKind::Opencode => parse_opencode_file(
                    task,
                    self.records,
                    self.updates,
                    self.next_id,
                    self.progress,
                    self.opencode_links,
                ),
                SourceKind::Omp => parse_omp_file(
                    task,
                    self.options.include_reasoning,
                    self.records,
                    self.updates,
                    self.next_id,
                    self.progress,
                ),
                SourceKind::Cursor => parse_cursor_file(
                    task,
                    self.records,
                    self.updates,
                    self.next_id,
                    self.progress,
                ),
                SourceKind::Pi => parse_pi_file(
                    task,
                    self.options.include_reasoning,
                    self.records,
                    self.updates,
                    self.next_id,
                    self.progress,
                ),
                SourceKind::OpenClaw => parse_openclaw_file(
                    task,
                    self.options.include_reasoning,
                    self.records,
                    self.updates,
                    self.next_id,
                    self.progress,
                ),
                SourceKind::Copilot => parse_copilot_session(
                    task,
                    self.records,
                    self.updates,
                    self.next_id,
                    self.progress,
                ),
                SourceKind::Grok => parse_grok_session(
                    task,
                    self.options.include_reasoning,
                    self.records,
                    self.updates,
                    self.next_id,
                    self.progress,
                ),
                SourceKind::Hermes => Err(anyhow!("Hermes indexing is not supported")),
                SourceKind::Jcode => parse_jcode_file(
                    task,
                    self.options.include_reasoning,
                    self.records,
                    self.updates,
                    self.next_id,
                    self.progress,
                ),
                SourceKind::Muse => parse_muse_file(
                    task,
                    self.options.include_reasoning,
                    self.records,
                    self.updates,
                    self.next_id,
                    self.progress,
                ),
                SourceKind::Antigravity => parse_antigravity_file(
                    task,
                    self.options.include_reasoning,
                    self.records,
                    self.updates,
                    self.next_id,
                    self.progress,
                ),
            };
            finish_file_task(task, self.progress, skipped, result)
        };
        if tasks.len() <= 1 {
            tasks.iter().try_for_each(parse)
        } else {
            self.pool.install(|| tasks.par_iter().try_for_each(parse))
        }
    }
}

pub(super) fn finish_file_task(
    task: &FileTask,
    progress: &Progress,
    skipped: &AtomicUsize,
    result: Result<()>,
) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if is_not_found(&error) => {
            // Active agent clients may rotate or delete a transcript after discovery. Treat
            // that filesystem race as a skipped file instead of discarding the whole ingest.
            progress.add_files_done(task.source, 1);
            skipped.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to parse {} transcript {}",
                task.source.label(),
                task.path.display()
            )
        }),
    }
}

pub(super) fn completed_file_state(
    task: &FileTask,
    offset: u64,
    turn_id: u32,
    legacy_turn_id: Option<u32>,
    pending_tool_calls: HashMap<String, PendingToolCall>,
) -> FileState {
    FileState {
        size: task.size,
        mtime: task.mtime,
        offset,
        turn_id,
        legacy_turn_id,
        parser_version: task.parser_version,
        pending_tool_calls,
        identity: task.identity.clone(),
        codex_metadata_offsets: None,
        claude_background: task.claude_background,
    }
}

#[derive(Clone)]
pub(super) struct RecordSender {
    sender: Sender<Record>,
    limits: IndexedToolContentLimits,
    diagnostics: Arc<Mutex<crate::sources::ParseDiagnostics>>,
}

impl RecordSender {
    #[cfg(test)]
    pub(super) fn new(sender: Sender<Record>, limits: IndexedToolContentLimits) -> Self {
        Self::with_diagnostics(
            sender,
            limits,
            Arc::new(Mutex::new(crate::sources::ParseDiagnostics::default())),
        )
    }

    pub(super) fn with_diagnostics(
        sender: Sender<Record>,
        limits: IndexedToolContentLimits,
        diagnostics: Arc<Mutex<crate::sources::ParseDiagnostics>>,
    ) -> Self {
        Self {
            sender,
            limits,
            diagnostics,
        }
    }

    pub(super) fn send(&self, mut record: Record) -> Result<()> {
        let (input_truncated, output_truncated) =
            limit_record_tool_content(&mut record, self.limits);
        if input_truncated || output_truncated {
            let mut diagnostics = self.diagnostics.lock().unwrap();
            diagnostics.truncated_tool_inputs += u64::from(input_truncated);
            diagnostics.truncated_tool_outputs += u64::from(output_truncated);
        }
        self.sender.send(record)?;
        Ok(())
    }
}

pub(super) fn prehydrate_opencode_database(
    path: &Path,
    scan: &crate::sources::opencode::DatabaseScan,
    session_ids: &[String],
    next_doc_id: &AtomicU64,
    state_dir: &Path,
) -> Result<PreparedOpencodeDatabase> {
    crate::profiling::span!("opencode.hydrate");
    let mut spool = tempfile::Builder::new()
        .prefix(OPENCODE_SPOOL_PREFIX)
        .tempfile_in(state_dir)
        .with_context(|| format!("create OpenCode hydration spool in {}", state_dir.display()))?;
    let mut diagnostics = crate::sources::ParseDiagnostics::default();
    for session_id in session_ids {
        let output = crate::sources::opencode::parse_database_records(
            path,
            session_id,
            crate::sources::IndexParseState::default(),
            next_doc_id,
            |record| {
                serde_json::to_writer(spool.as_file_mut(), &record)?;
                spool.as_file_mut().write_all(b"\n")?;
                Ok(())
            },
        )
        .with_context(|| {
            format!(
                "hydrate OpenCode session `{session_id}` from {}",
                path.display()
            )
        })?;
        diagnostics.merge(output.diagnostics);
    }
    spool.as_file_mut().flush()?;
    Ok(PreparedOpencodeDatabase {
        path: path.to_path_buf(),
        scan: scan.clone(),
        spool,
        diagnostics,
    })
}

pub(super) fn cleanup_opencode_spools(state_dir: &Path) -> Result<()> {
    let entries = match std::fs::read_dir(state_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(OPENCODE_SPOOL_PREFIX)
            && entry.file_type()?.is_file()
        {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

pub(super) fn replay_opencode_spool(
    spool: &mut tempfile::NamedTempFile,
    tx_record: &RecordSender,
    database_path: &Path,
    owned_session_ids: &HashSet<String>,
    progress: &Progress,
) -> Result<()> {
    spool.as_file_mut().seek(SeekFrom::Start(0))?;
    let reader = BufReader::new(spool.as_file_mut());
    for line in reader.lines() {
        let line = line.with_context(|| {
            format!(
                "read OpenCode hydration spool for {}",
                database_path.display()
            )
        })?;
        let record = serde_json::from_str::<Record>(&line).with_context(|| {
            format!(
                "decode OpenCode hydration spool for {}",
                database_path.display()
            )
        })?;
        if owned_session_ids.contains(&record.session_id) {
            progress.add_produced(SourceKind::Opencode, 1);
            tx_record.send(record)?;
        }
    }
    Ok(())
}

pub(super) fn record_channel() -> (Sender<Record>, Receiver<Record>) {
    bounded(RECORD_CHANNEL_CAPACITY)
}

pub(super) fn parser_thread_pool() -> Result<rayon::ThreadPool> {
    build_parser_thread_pool(
        std::thread::available_parallelism().map_or(1, |count| count.get().min(8)),
    )
}

pub(super) fn build_parser_thread_pool(num_threads: usize) -> Result<rayon::ThreadPool> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads.max(1))
        .thread_name(|index| format!("memex-parser-{index}"))
        .build()
        .context("build parser thread pool")
}

pub(super) fn refresh_memories(
    paths: &Paths,
    options: &IngestOptions,
    repositories: &crate::repository::RepositoryResolver,
) -> Result<()> {
    crate::profiling::span!("memory.refresh");
    let mut enabled_sources = HashSet::new();
    if !options.claude_sources.is_empty() {
        enabled_sources.insert(SourceKind::Claude);
    }
    if options.include_codex {
        enabled_sources.insert(SourceKind::Codex);
    }
    let discovery = crate::memory::MemoryDiscoveryOptions {
        claude_project_roots: options.claude_sources.clone(),
        codex_homes: if options.include_codex {
            crate::sources::codex::homes()
        } else {
            Vec::new()
        },
        enabled_sources,
        exclude_patterns: options.exclude_patterns.clone(),
    };
    let store = crate::memory::MemoryStore::new(paths.root.join("memory/documents.json"));
    let report = store.refresh_with_repositories(&discovery, repositories)?;
    if report.changed && (report.document_count > 0 || report.deleted > 0) {
        eprintln!(
            "memory index: {} documents, {} sections ({} updated, {} deleted, {} stale)",
            report.document_count,
            report.section_count,
            report.parsed,
            report.deleted,
            report.stale_count,
        );
    }
    for failure in &report.failures {
        eprintln!(
            "memory index: {}: {}",
            failure.path.display(),
            failure.error
        );
    }
    if options.embeddings {
        let count =
            crate::memory_search::embed_memory(paths, options.model, &options.embed_runtime)?;
        if count > 0 {
            eprintln!("memory index: embedded {count} sections");
        }
    }
    Ok(())
}

pub(super) fn execute_refresh(
    prepared: discovery::PreparedRefresh,
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    repositories: Arc<crate::repository::RepositoryResolver>,
    pool: &rayon::ThreadPool,
) -> Result<IngestReport> {
    let discovery::PreparedRefresh {
        full_scan,
        scan_cache,
        state_path,
        mut state,
        recovering_pending_ingest,
        empty_index_rebuild,
        next_doc_id,
        tasks,
        files_scanned,
        files_skipped,
        total_bytes,
        session_ids,
        deferred_pending_scopes,
        mut opencode_ready_databases,
        opencode_ready_owned_sessions,
        opencode_diagnostics,
        opencode_scope_targets,
        opencode_session_cwds,
        opencode_database_paths_to_delete,
        opencode_session_links,
        recover_embeddings,
        reconcile_pending_vector_ids,
        recover_vector_cleanup,
        delete_paths,
        vector_delete_paths,
        installed_opencode_states,
        opencode_database_state_changed,
        identities_changed,
    } = prepared;
    let totals = compute_totals(&tasks);
    let file_totals = compute_file_totals(&tasks);
    let analytics_db = analytics_path(&paths.state);
    // A replacement or empty index cannot inherit a complete catalog from its
    // predecessor: moved paths would survive as rows with no indexed records, and
    // reparsed messages would be added to the old counts.
    let analytics_needs_backfill = empty_index_rebuild
        || recovering_pending_ingest
        || if index.doc_count()? == 0 {
            AnalyticsStore::is_ready(&analytics_db)
        } else {
            !AnalyticsStore::is_complete(&analytics_db)
        };
    if analytics_needs_backfill {
        // Persist this before publishing any new index record, so recovery retries
        // reconciliation even when interrupted before the backfill starts.
        AnalyticsStore::open(&analytics_db)?.mark_incomplete()?;
    }
    let source_changes = !tasks.is_empty()
        || !delete_paths.is_empty()
        || !opencode_scope_targets.is_empty()
        || opencode_ready_databases
            .iter()
            .any(|database| !database.scan.dirty_session_ids.is_empty());
    let vector_work = recover_embeddings
        || reconcile_pending_vector_ids
        || (full_scan && !source_changes && !can_skip_noop_index(paths, index, options)?);
    let decision = plan::decide_refresh(plan::RefreshFacts {
        source_changes,
        vector_work,
        checkpoint_changes: recovering_pending_ingest
            || empty_index_rebuild
            || identities_changed
            || opencode_database_state_changed
            || analytics_needs_backfill,
    });
    if decision != plan::RefreshDecision::Publish {
        if analytics_needs_backfill {
            backfill_from_index_with_repositories(&analytics_db, index, repositories.clone())?;
        }
        crate::profiling::count!("ingest.noop_returns", 1);
        index.publish_generation_if_uninitialized()?;
        state.opencode_databases = installed_opencode_states;
        if recovering_pending_ingest || empty_index_rebuild || identities_changed {
            state.save(&state_path)?;
        }
        if opencode_database_state_changed {
            state.save(&state_path)?;
        }
        if let Some(scan_cache) = scan_cache {
            update_scan_cache(paths, files_scanned, total_bytes, scan_cache)?;
        }
        if recovering_pending_ingest {
            finalize_pending_ingest(
                &pending_ingest_path(paths),
                &deferred_pending_scopes,
                state.next_doc_id,
            )?;
        }
        return Ok(IngestReport {
            records_added: 0,
            records_embedded: 0,
            files_scanned,
            files_skipped,
            diagnostics: Default::default(),
        });
    }

    let mut vector_migration = vector_migration(&paths.vectors, &tasks, options.model);
    if recover_embeddings
        && !options.embeddings
        && crate::vector::VectorIndex::exists(&paths.vectors)
        && let Some(model) = crate::vector::VectorIndex::open(&paths.vectors)?
            .model()
            .and_then(|model| ModelChoice::parse(model).ok())
    {
        vector_migration.model = model;
    }
    let embeddings = options.embeddings || vector_migration.rebuild || recover_embeddings;
    let vector_publication = embeddings
        || reconcile_pending_vector_ids
        || ((recover_vector_cleanup
            || !opencode_scope_targets.is_empty()
            || !opencode_database_paths_to_delete.is_empty()
            || !vector_delete_paths.is_empty())
            && crate::vector::VectorIndex::exists(&paths.vectors));
    let progress = Arc::new(Progress::new(totals, file_totals, embeddings));

    let (raw_tx_record, rx_record) = record_channel();
    let shared_diagnostics = Arc::new(Mutex::new(crate::sources::ParseDiagnostics::default()));
    shared_diagnostics
        .lock()
        .unwrap()
        .merge(opencode_diagnostics);
    let tx_record = RecordSender::with_diagnostics(
        raw_tx_record,
        options.tool_content_limits,
        shared_diagnostics.clone(),
    );
    let (tx_update, rx_update) = unbounded::<FileUpdate>();

    let mut affected_paths = delete_paths.clone();
    affected_paths.extend(
        tasks
            .iter()
            .map(|task| task.path.to_string_lossy().to_string()),
    );
    affected_paths.sort();
    affected_paths.dedup();
    let pending_scopes = pending_scope_union(&opencode_scope_targets, &deferred_pending_scopes);
    let mut pending_ingest = PendingIngest {
        next_doc_id: state.next_doc_id,
        source_paths: affected_paths,
        vector_delete_paths: vector_delete_paths.iter().cloned().collect(),
        session_scopes: pending_scopes,
        vector_publication,
        embedding_publication: Some(embeddings),
    };
    let pending_path = pending_ingest_path(paths);
    let existing_records_change = !delete_paths.is_empty() || !opencode_scope_targets.is_empty();

    let input_bytes = if tasks.iter().any(|task| task.source == SourceKind::Opencode) {
        None
    } else {
        let mut bytes = totals
            .iter()
            .fold(0u64, |total, bytes| total.saturating_add(*bytes));
        for database in &opencode_ready_databases {
            bytes = bytes.saturating_add(database.spool.as_file().metadata()?.len());
        }
        Some(bytes)
    };
    let writer_index = index.clone();
    let writer_ctx = WriterContext {
        index_root: paths.index.clone(),
        input_bytes,
        embeddings,
        do_backfill_embeddings: options.backfill_embeddings
            || vector_migration.rebuild
            || recover_embeddings
            || (embeddings && vector_work),
        reset_vector_store: vector_migration.rebuild,
        vector_dir: paths.vectors.clone(),
        analytics_path: analytics_db.clone(),
        progress: progress.clone(),
        model: vector_migration.model,
        embed_runtime: options.embed_runtime.clone(),
        tool_content_limits: options.tool_content_limits,
        reconcile_vector_ids: reconcile_pending_vector_ids,
        scope_targets: opencode_scope_targets.clone(),
        opencode_session_cwds: opencode_session_cwds.clone(),
        repositories: repositories.clone(),
        codex_metadata_checkpoints: tasks
            .iter()
            .filter_map(|task| {
                task.codex_metadata_offsets.as_ref().map(|offsets| {
                    (
                        task.path.to_string_lossy().into_owned(),
                        (task.offset, offsets.clone()),
                    )
                })
            })
            .collect(),
        vector_delete_paths,
    };
    let (decision_tx, decision_rx) = bounded(1);
    let writer_handle = std::thread::spawn(move || {
        writer_loop(
            writer_index,
            rx_record,
            decision_rx,
            delete_paths,
            writer_ctx,
        )
    });

    let parse_skipped = AtomicUsize::new(0);
    let parser = execution::ParserContext {
        pool,
        options,
        records: &tx_record,
        updates: &tx_update,
        next_id: &next_doc_id,
        progress: &progress,
        session_ids: &session_ids,
        opencode_links: &opencode_session_links,
    };
    let parser_result = parser.parse(&tasks, &parse_skipped);
    let parser_result = parser_result.and_then(|_| {
        for database in &mut opencode_ready_databases {
            let path = database.path.to_string_lossy().to_string();
            let owned_session_ids = opencode_ready_owned_sessions
                .get(&path)
                .cloned()
                .unwrap_or_default();
            replay_opencode_spool(
                &mut database.spool,
                &tx_record,
                &database.path,
                &owned_session_ids,
                &progress,
            )?;
        }
        Ok(())
    });

    drop(tx_record);
    drop(tx_update);

    // Parsers are done; what follows is commit/merge/publish plus analytics
    // and state writes, none of which is per-source work. Keep one spinner
    // visible so the tail doesn't read as hung.
    let tail = progress.tail_spinner("committing index…");
    pending_ingest.next_doc_id = next_doc_id.load(Ordering::SeqCst);
    let needs_publication = existing_records_change
        || embeddings
        || recover_embeddings
        || reconcile_pending_vector_ids
        || next_doc_id.load(Ordering::SeqCst) != state.next_doc_id;
    let pending_update_error = if needs_publication {
        pending_ingest
            .save(&pending_path)
            .with_context(|| format!("prepare ingest publication at {}", pending_path.display()))
            .err()
    } else {
        None
    };
    let decision = if parser_result.is_ok() && pending_update_error.is_none() {
        WriterDecision::Commit
    } else {
        WriterDecision::Cancel
    };
    // A failed writer may have already closed the channel. Joining below keeps that root cause.
    let _ = decision_tx.send(decision);
    #[cfg(feature = "profiling")]
    let writer_wait_profile = crate::profiling::Scope::enter("ingest.writer_wait");
    let writer_result = writer_handle.join().map_err(|_| {
        tail.finish_and_clear();
        anyhow!("writer thread panicked")
    })?;
    #[cfg(feature = "profiling")]
    drop(writer_wait_profile);
    progress.finish();
    tail.set_message("updating analytics…");
    let outcome = (|| -> Result<IngestReport> {
        let writer_outcome =
            writer_result.context("index writer stopped before ingestion completed")?;
        parser_result?;
        if let Some(error) = pending_update_error {
            return Err(error);
        }
        let (records_added, records_embedded) = match writer_outcome {
            WriterOutcome::Published {
                records_added,
                records_embedded,
            } => (records_added, records_embedded),
            WriterOutcome::CheckpointsOnly => (0, 0),
            WriterOutcome::Cancelled => {
                return Err(anyhow!(
                    "index writer cancelled a successful ingest publication"
                ));
            }
        };
        if analytics_needs_backfill {
            let published_index = SearchIndex::open_or_create(&paths.index)?;
            backfill_from_index_with_repositories(
                &analytics_db,
                &published_index,
                repositories.clone(),
            )?;
        } else {
            AnalyticsStore::open(&analytics_db)?.mark_complete()?;
        }

        let mut diagnostics = shared_diagnostics.lock().unwrap().clone();
        let mut updated_files = HashMap::new();
        while let Ok(update) = rx_update.recv() {
            updated_files.insert(update.path.clone(), update.state.clone());
            diagnostics.merge(update.diagnostics);
            let _ = update.session_id;
        }

        for (path, update) in updated_files {
            state.files.insert(path, update);
        }
        state.opencode_databases = installed_opencode_states;
        state.next_doc_id = next_doc_id.load(Ordering::SeqCst);
        state.save(&state_path)?;

        if let Some(scan_cache) = scan_cache {
            update_scan_cache(paths, files_scanned, total_bytes, scan_cache)?;
        }
        finalize_pending_ingest(&pending_path, &deferred_pending_scopes, state.next_doc_id)?;

        Ok(IngestReport {
            records_added,
            records_embedded,
            files_scanned,
            files_skipped: files_skipped + parse_skipped.load(Ordering::Relaxed),
            diagnostics,
        })
    })();
    tail.finish_and_clear();
    outcome
}
