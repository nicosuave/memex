use super::*;

pub(super) fn open_vector_index_for_ingest(
    vector_dir: &Path,
    dimensions: usize,
    model: ModelChoice,
    replace: bool,
) -> Result<crate::vector::VectorIndex> {
    if replace {
        crate::vector::VectorIndex::empty_replacement(vector_dir, dimensions, Some(model.as_str()))
    } else {
        crate::vector::VectorIndex::open_or_create(vector_dir, dimensions, Some(model.as_str()))
    }
}

pub(super) fn writer_loop(
    index: SearchIndex,
    rx: Receiver<Record>,
    decision_rx: Receiver<WriterDecision>,
    delete_paths: Vec<String>,
    ctx: WriterContext,
) -> Result<WriterOutcome> {
    crate::profiling::span!("ingest.writer");
    let first = rx.recv().ok();
    if first.is_none()
        && delete_paths.is_empty()
        && ctx.scope_targets.is_empty()
        && !ctx.do_backfill_embeddings
        && !ctx.reset_vector_store
        && !ctx.reconcile_vector_ids
    {
        return match decision_rx.recv() {
            Ok(WriterDecision::Commit) => {
                index.publish_generation_if_uninitialized()?;
                crate::profiling::count!("ingest.checkpoint_only_returns", 1);
                Ok(WriterOutcome::CheckpointsOnly)
            }
            _ => Ok(WriterOutcome::Cancelled),
        };
    }
    let index = if index.is_writable() {
        index
    } else if ctx.defer_merges {
        SearchIndex::open_or_create_for_search_refresh(&ctx.index_root)?
    } else {
        SearchIndex::open_or_create_for_continuous_ingest(&ctx.index_root)?
    };
    let mut writer = index
        .writer_for_ingest(ctx.input_bytes)
        .context("failed to initialize the Tantivy index writer")?;
    let WriterContext {
        index_root: _,
        input_bytes: _,
        defer_merges: _,
        embeddings,
        do_backfill_embeddings,
        reset_vector_store,
        vector_dir,
        analytics_path,
        progress,
        model,
        embed_runtime,
        tool_content_limits,
        reconcile_vector_ids,
        scope_targets,
        opencode_session_cwds,
        repositories,
        codex_metadata_checkpoints,
        vector_delete_paths,
    } = ctx;
    let mut analytics = AnalyticsWriter::with_repositories(&analytics_path, repositories)?;
    for (path, (offset, metadata_offsets)) in codex_metadata_checkpoints {
        analytics.set_codex_metadata_checkpoint(path, offset, metadata_offsets);
    }
    let mut scoped_doc_ids = HashSet::new();
    for scope in &scope_targets {
        for doc_id in index.doc_ids_by_source_scope(scope)? {
            scoped_doc_ids.insert(doc_id);
        }
        index.delete_by_source_scope(&mut writer, scope)?;
    }
    for path in &delete_paths {
        if vector_delete_paths.contains(path) {
            for doc_id in index.doc_ids_by_source_path(path)? {
                scoped_doc_ids.insert(doc_id);
            }
        }
        index.delete_by_source_path(&mut writer, path);
    }
    for (scope, cwd) in &opencode_session_cwds {
        analytics.set_session_cwd(
            SourceKind::Opencode,
            &scope.source_path,
            &scope.session_id,
            cwd,
        );
    }

    let mut count = 0usize;
    let mut embedded_count = 0usize;
    let mut vector_index = None;
    let mut embedder: Option<EmbedderHandle> = None;
    let mut embed_buffer: Vec<(u64, String, SourceKind)> = Vec::new();
    let mut index_pending = [0u64; SOURCE_COUNT];
    if embeddings {
        let handle = EmbedderHandle::with_model_and_runtime(model, &embed_runtime)?;
        let dims = handle.dims;
        vector_index = Some(open_vector_index_for_ingest(
            &vector_dir,
            dims,
            model,
            reset_vector_store,
        )?);
        embedder = Some(handle);
        progress.set_embed_ready();
    } else if (reconcile_vector_ids || !scoped_doc_ids.is_empty())
        && crate::vector::VectorIndex::exists(&vector_dir)
    {
        vector_index = Some(crate::vector::VectorIndex::open(&vector_dir)?);
    }
    if let Some(vindex) = vector_index.as_mut() {
        vindex.remove_doc_ids(&scoped_doc_ids)?;
    }

    for mut record in first.into_iter().chain(rx.iter()) {
        // Parsers apply the limit before queueing; enforce it here as a defensive boundary too.
        let _ = limit_record_tool_content(&mut record, tool_content_limits);
        analytics.record(&record)?;
        index.add_record(&mut writer, &record)?;
        let source_idx = record.source.idx();
        index_pending[source_idx] += 1;
        if index_pending[source_idx] >= INDEX_PROGRESS_BATCH {
            progress.add_indexed(record.source, index_pending[source_idx]);
            index_pending[source_idx] = 0;
        }
        if embeddings
            && !reset_vector_store
            && is_embedding_role(&record.role)
            && !record.text.is_empty()
        {
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

    // Flush any remaining index progress
    for (idx, &pending) in index_pending.iter().enumerate() {
        if pending > 0
            && let Some(source) = SourceKind::from_idx(idx)
        {
            progress.add_indexed(source, pending);
        }
    }

    match decision_rx.recv() {
        Ok(WriterDecision::Commit) => {}
        Ok(WriterDecision::Cancel) | Err(_) => {
            writer.rollback()?;
            return Ok(WriterOutcome::Cancelled);
        }
    }

    let prepared_analytics = analytics.prepare();
    prepared_analytics.commit(&delete_paths, &scope_targets)?;
    {
        crate::profiling::span!("lexical.commit");
        writer.commit()?;
    }
    let mut staged_vectors = None;
    if reconcile_vector_ids {
        let mut live_doc_ids = HashSet::new();
        index.for_each_record(|record| {
            if is_embedding_role(&record.role) && !record.text.is_empty() {
                live_doc_ids.insert(record.doc_id);
            }
            Ok(())
        })?;
        if let Some(vindex) = vector_index.as_mut() {
            vindex.retain_doc_ids(&live_doc_ids)?;
        }
    }
    if embeddings {
        if !embed_buffer.is_empty() {
            embedded_count += flush_embeddings(
                &mut embed_buffer,
                embedder.as_mut().unwrap(),
                vector_index.as_mut().unwrap(),
                &progress,
            )?;
        }

        let needs_vector_backfill = match vector_index.as_ref() {
            Some(vindex) => {
                vindex.needs_backfill() || !vector_index_covers_embeddable_records(&index, vindex)?
            }
            None => false,
        };
        if do_backfill_embeddings || needs_vector_backfill {
            embedded_count += backfill_embeddings(
                &index,
                embedder.as_mut().unwrap(),
                vector_index.as_mut().unwrap(),
                &progress,
            )?;
        }
    }
    if let Some(vindex) = vector_index.as_ref() {
        staged_vectors = Some(vindex.stage()?);
    }
    if let Some(handle) = embedder.take() {
        std::mem::forget(handle);
    }
    {
        crate::profiling::span!("lexical.merge_wait");
        writer.wait_merging_threads()?;
    }
    index.check_continuous_segment_limit()?;
    index.publish_generation()?;
    if let Some(staged) = staged_vectors {
        staged.publish()?;
    }
    crate::profiling::count!("ingest.records_added", count);
    crate::profiling::count!("ingest.records_embedded", embedded_count);
    Ok(WriterOutcome::Published {
        records_added: count,
        records_embedded: embedded_count,
    })
}

pub(super) fn backfill_embeddings(
    index: &SearchIndex,
    embedder: &mut EmbedderHandle,
    vector_index: &mut crate::vector::VectorIndex,
    progress: &Arc<Progress>,
) -> Result<usize> {
    crate::profiling::span!("vectors.backfill");
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

pub(super) fn pending_ingest_path(paths: &Paths) -> PathBuf {
    paths.state.join("ingest.pending.json")
}

pub(super) fn finalized_pending_ingest(
    deferred_scopes: &[SessionScope],
    next_doc_id: u64,
) -> PendingChange {
    if deferred_scopes.is_empty() {
        return PendingChange::Clear;
    }
    PendingChange::Replace(PendingIngest {
        next_doc_id,
        source_paths: Vec::new(),
        vector_delete_paths: Vec::new(),
        session_scopes: pending_scope_union(&[], deferred_scopes),
        vector_publication: false,
        embedding_publication: Some(false),
    })
}

pub(super) fn pending_scope_union(
    active_scopes: &[SessionScope],
    deferred_scopes: &[SessionScope],
) -> Vec<SessionScope> {
    let mut scopes = active_scopes
        .iter()
        .chain(deferred_scopes)
        .cloned()
        .collect::<Vec<_>>();
    scopes.sort_by(|left, right| {
        left.source_path
            .cmp(&right.source_path)
            .then_with(|| left.session_id.cmp(&right.session_id))
    });
    scopes.dedup();
    scopes
}

pub(super) fn prepare_pending_ingest_recovery(
    state: &mut CheckpointSession,
) -> Option<PendingIngest> {
    let pending = state.pending.clone()?;

    for source_path in &pending.source_paths {
        state.delete_file(source_path);
        if crate::sources::opencode::is_database_path(source_path) {
            state.opencode_databases.remove(source_path);
        }
    }
    state.next_doc_id = state.next_doc_id.max(pending.next_doc_id);
    Some(pending)
}

pub(super) fn updated_scan_cache(
    cache: Option<ScanCache>,
    files_scanned: usize,
    total_bytes: u64,
    full_scan: bool,
) -> Option<ScanCache> {
    cache.map(|mut cache| {
        if full_scan {
            cache.update(files_scanned, total_bytes);
        } else {
            cache.touch();
        }
        cache
    })
}

pub(super) struct RecoveredCheckpoint {
    pub state: CheckpointSession,
    pub pending_recovery: Option<PendingIngest>,
    pub empty_index_rebuild: bool,
}

pub(super) fn recover_checkpoint(
    paths: &Paths,
    index: &SearchIndex,
    lease: &IngestLease,
    header: Option<CheckpointHeader>,
) -> Result<RecoveredCheckpoint> {
    let state_path = paths.state.join("ingest.json");
    let empty_index = index.doc_count()? == 0;
    let allow_initialize = if crate::state::checkpoint::has_authority(&state_path)? {
        false
    } else if empty_index {
        true
    } else {
        let pending = PendingIngest::load(&pending_ingest_path(paths))?
            .context("missing ingest checkpoint for a populated index")?;
        let source_paths = pending.source_paths.iter().collect::<HashSet<_>>();
        let scopes = pending.session_scopes.iter().collect::<HashSet<_>>();
        index.for_each_record(|record| {
            anyhow::ensure!(
                record.doc_id < pending.next_doc_id
                    && (source_paths.contains(&record.source_path)
                        || scopes.contains(&SessionScope {
                            source_path: record.source_path.clone(),
                            session_id: record.session_id.clone(),
                        })),
                "missing ingest checkpoint: pending intent does not cover indexed records"
            );
            Ok(())
        })?;
        true
    };
    let mut state = CheckpointSession::open(&state_path, lease, allow_initialize, header)?;
    // Apply additive analytics migrations even when the scan finds no changed files.
    drop(AnalyticsStore::open(analytics_path(&paths.state))?);
    let pending_recovery = prepare_pending_ingest_recovery(&mut state);
    cleanup_opencode_spools(&paths.state)?;
    let mut empty_index_rebuild = false;
    if empty_index && (state.has_files()? || !state.opencode_databases.is_empty()) {
        empty_index_rebuild = true;
        state.clear_files();
        state.opencode_databases.clear();
        if paths.vectors.exists() {
            std::fs::remove_dir_all(&paths.vectors)?;
            std::fs::create_dir_all(&paths.vectors)?;
        }
    }

    Ok(RecoveredCheckpoint {
        state,
        pending_recovery,
        empty_index_rebuild,
    })
}
