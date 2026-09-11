use super::*;

pub(super) const FILE_IDENTITY_PREFIX_BYTES: usize = 4096;

pub(super) struct TranscriptDiscovery {
    pub tasks: Vec<FileTask>,
    pub unchanged_identities: Vec<(String, FileIdentity)>,
    pub files_scanned: usize,
    pub files_skipped: usize,
    pub total_bytes: u64,
    pub session_ids: HashSet<String>,
}

pub(super) fn discover_transcripts(
    options: &IngestOptions,
    excluder: &PathExcluder,
    state: &IngestState,
    pool: &rayon::ThreadPool,
    selected: Option<&[crate::sources::SourceFile]>,
    mut inventory: Option<&mut crate::directory_inventory::DiscoveryInventory>,
) -> Result<TranscriptDiscovery> {
    let full_scan = selected.is_none();
    let mut files = selected.unwrap_or_default().to_vec();
    for root in options.claude_sources.iter().filter(|_| full_scan) {
        files.extend(match inventory.as_deref_mut() {
            Some(inventory) => crate::sources::claude::discover_with_inventory(
                root,
                options.include_agents,
                inventory,
            )?,
            None => crate::sources::claude::discover(root, options.include_agents)?,
        });
    }
    if options.include_codex && full_scan {
        files.extend(match inventory.as_deref_mut() {
            Some(inventory) => crate::sources::codex::discover_rollouts_with_inventory(inventory)?,
            None => crate::sources::codex::discover_rollouts(),
        });
        files.extend(
            crate::sources::codex::history_paths()
                .into_iter()
                .map(|path| crate::sources::SourceFile {
                    source: SourceKind::Codex,
                    path,
                }),
        );
    }
    if options.include_cursor && full_scan {
        files.extend(crate::sources::cursor::discover_transcripts());
    }
    if options.include_pi && full_scan {
        files.extend(match inventory.as_deref_mut() {
            Some(inventory) => crate::sources::pi::discover_with_inventory(inventory)?,
            None => crate::sources::pi::discover(),
        });
    }
    if options.include_omp && full_scan {
        files.extend(match inventory {
            Some(inventory) => crate::sources::omp::discover_with_inventory(inventory)?,
            None => crate::sources::omp::discover(),
        });
    }
    if options.include_openclaw && full_scan {
        files.extend(crate::sources::openclaw::discover());
    }
    if options.include_copilot && full_scan {
        files.extend(crate::sources::copilot::discover_sessions());
    }
    if options.include_grok && full_scan {
        files.extend(crate::sources::grok::discover_sessions());
    }
    if options.include_jcode && full_scan {
        files.extend(crate::sources::jcode::discover());
    }
    if options.include_muse && full_scan {
        files.extend(crate::sources::muse::discover());
    }
    if options.include_antigravity && full_scan {
        files.extend(crate::sources::antigravity::discover());
    }

    let session_ids = selected
        .filter(|files| {
            files.iter().any(|file| {
                file.source == SourceKind::Codex
                    && crate::sources::codex::is_history_path(&file.path)
            })
        })
        .map_or_else(HashSet::new, |files| {
            selection::codex_session_ids(options, state, files)
        });

    enum Observed {
        Excluded,
        Missing {
            session_id: Option<String>,
        },
        File {
            task: Box<FileTask>,
            skip: bool,
            session_id: Option<String>,
        },
    }
    let observations = pool.install(|| {
        files
            .into_par_iter()
            .map(|file| -> Result<Observed> {
                let path = file.path;
                if excluder.is_excluded(&path) {
                    return Ok(Observed::Excluded);
                }
                let session_id = (file.source == SourceKind::Codex
                    && !crate::sources::codex::is_history_path(&path))
                .then(|| crate::sources::codex::session_id_from_path(&path))
                .flatten();
                let Some(metadata) = discovered_metadata(&path)? else {
                    return Ok(Observed::Missing { session_id });
                };
                let key = path.to_string_lossy().into_owned();
                let (task, skip) = prepare_file_task(
                    path,
                    file.source,
                    options.include_reasoning,
                    &metadata,
                    state.files.get(&key),
                );
                Ok(Observed::File {
                    task: Box::new(task),
                    skip,
                    session_id,
                })
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let mut result = TranscriptDiscovery {
        tasks: Vec::new(),
        unchanged_identities: Vec::new(),
        files_scanned: 0,
        files_skipped: 0,
        total_bytes: 0,
        session_ids,
    };
    for observation in observations {
        match observation {
            Observed::Excluded => result.files_skipped += 1,
            Observed::Missing { session_id } => {
                result.session_ids.extend(session_id);
                result.files_skipped += 1;
            }
            Observed::File {
                task,
                skip,
                session_id,
            } => {
                result.session_ids.extend(session_id);
                result.files_scanned += 1;
                result.total_bytes += task.size;
                if skip {
                    result.files_skipped += 1;
                    result
                        .unchanged_identities
                        .push((task.path.to_string_lossy().into_owned(), task.identity));
                } else {
                    result.tasks.push(*task);
                }
            }
        }
    }
    Ok(result)
}

pub(super) fn modified_ns(metadata: &std::fs::Metadata) -> Option<i64> {
    metadata
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|time| time.as_nanos().min(i64::MAX as u128) as i64)
}

pub(super) fn changed_ns(metadata: &std::fs::Metadata) -> Option<i64> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (metadata.ctime_nsec() != 0).then(|| {
            metadata
                .ctime()
                .saturating_mul(1_000_000_000)
                .saturating_add(metadata.ctime_nsec())
        })
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        None
    }
}

pub(super) fn unchanged_file_metadata(
    previous: &FileState,
    metadata: &std::fs::Metadata,
    parser_version: u32,
) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        previous.size == metadata.len()
            && previous.parser_version == parser_version
            && previous.identity.device == Some(metadata.dev())
            && previous.identity.inode == Some(metadata.ino())
            && previous.identity.modified_ns.is_some()
            && previous.identity.modified_ns == modified_ns(metadata)
            && previous.identity.changed_ns.is_some()
            && previous.identity.changed_ns == changed_ns(metadata)
    }
    #[cfg(not(unix))]
    {
        let _ = (previous, metadata, parser_version);
        false
    }
}

pub(super) fn file_identity(
    path: &Path,
    metadata: &std::fs::Metadata,
    prefix_bytes: usize,
) -> FileIdentity {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;

    let prefix_sha256 = if metadata.is_file() {
        File::open(path).ok().and_then(|mut file| {
            let mut bytes = vec![0; prefix_bytes];
            let read = file.read(&mut bytes).ok()?;
            crate::profiling::count!("ingest.prefix_reads", 1);
            crate::profiling::count!("ingest.prefix_bytes", read);
            bytes.truncate(read);
            Some(format!("{:x}", Sha256::digest(&bytes)))
        })
    } else {
        None
    };

    FileIdentity {
        sqlite_wal: None,
        #[cfg(unix)]
        device: Some(metadata.dev()),
        #[cfg(not(unix))]
        device: None,
        #[cfg(unix)]
        inode: Some(metadata.ino()),
        #[cfg(not(unix))]
        inode: None,
        prefix_sha256,
        prefix_bytes: prefix_bytes as u64,
        modified_ns: modified_ns(metadata),
        changed_ns: changed_ns(metadata),
    }
}

pub(super) fn prepare_file_task(
    path: PathBuf,
    source: SourceKind,
    include_reasoning: bool,
    metadata: &std::fs::Metadata,
    previous: Option<&FileState>,
) -> (FileTask, bool) {
    crate::profiling::span!("ingest.file_check");
    let size = metadata.len();
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0);
    let prefix_bytes = previous
        .map(|state| {
            if state.identity.prefix_bytes > 0 {
                state.identity.prefix_bytes
            } else {
                state.size.min(FILE_IDENTITY_PREFIX_BYTES as u64)
            }
        })
        .unwrap_or_else(|| size.min(FILE_IDENTITY_PREFIX_BYTES as u64))
        .min(size) as usize;
    let parser_version = crate::sources::index_state_version_for(source, include_reasoning);
    let mut identity = previous
        .filter(|previous| unchanged_file_metadata(previous, metadata, parser_version))
        .map(|previous| previous.identity.clone())
        .unwrap_or_else(|| file_identity(&path, metadata, prefix_bytes));
    if source == SourceKind::Antigravity && crate::sources::antigravity::is_db_path(&path) {
        identity.sqlite_wal = Some(crate::state::SqliteWalIdentity::read(&path));
    }
    let mut change = plan::classify_file(source, size, mtime, &identity, parser_version, previous);
    let (mut offset, mut turn_id, mut pending_tool_calls) = match (change, previous) {
        (FileChange::Append | FileChange::Unchanged, Some(previous)) => (
            previous.offset,
            previous.turn_id,
            previous.pending_tool_calls.clone(),
        ),
        _ => (0, 0, HashMap::new()),
    };
    let claude = resolve_claude_background(&path, source, size, previous, change);
    if claude.reparse {
        change = FileChange::Replaced;
        offset = 0;
        turn_id = 0;
        pending_tool_calls.clear();
    }
    let claude_background = claude.background;

    (
        FileTask {
            path,
            source,
            offset,
            turn_id,
            legacy_turn_id: if matches!(source, SourceKind::Claude | SourceKind::Codex)
                && offset == 0
            {
                Some(0)
            } else {
                previous.and_then(|previous| previous.legacy_turn_id)
            },
            size,
            mtime,
            change,
            pending_tool_calls,
            identity,
            parser_version,
            codex_metadata_offsets: previous
                .filter(|_| source == SourceKind::Codex && offset > 0 && !change.replaces_records())
                .and_then(|state| state.codex_metadata_offsets.clone()),
            claude_background,
        },
        change == FileChange::Unchanged,
    )
}

/// Claude marks a whole transcript as a background session with a file-level flag that can
/// appear long after its first records were indexed. Discovering it late reclassifies every
/// record in the file, so the transcript is reparsed from zero when the marker turns up.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ClaudeBackground {
    background: Option<bool>,
    reparse: bool,
}

fn resolve_claude_background(
    path: &Path,
    source: SourceKind,
    size: u64,
    previous: Option<&FileState>,
    change: FileChange,
) -> ClaudeBackground {
    if source != SourceKind::Claude {
        return ClaudeBackground::default();
    }
    if change.replaces_records() {
        // A replacement must rediscover the marker from the new contents.
        return ClaudeBackground::default();
    }
    let mut resolved = ClaudeBackground {
        background: previous.and_then(|state| state.claude_background),
        reparse: false,
    };
    match resolved.background {
        Some(true) => {}
        Some(false) if size > previous.map_or(0, |state| state.offset) => {
            match crate::sources::claude::has_background_session_kind_since(
                path,
                previous.map_or(0, |state| state.offset),
                size,
            ) {
                // If the tail cannot be inspected, fail safe by reparsing; parsing
                // will surface a persistent read failure.
                Ok(true) | Err(_) => {
                    resolved.background = None;
                    resolved.reparse = true;
                }
                Ok(false) => {}
            }
        }
        None if previous.is_some() => {
            // State written before this was tracked needs a one-time full check;
            // later appends inspect only their own tail.
            resolved.background =
                crate::sources::claude::has_background_session_kind_since(path, 0, size).ok();
            if resolved.background == Some(true) && previous.is_some_and(|state| state.offset > 0) {
                resolved.reparse = true;
            }
        }
        _ => {}
    }
    resolved
}

pub(super) fn discovered_metadata(path: &Path) -> Result<Option<std::fs::Metadata>> {
    match path.metadata() {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read metadata for {}", path.display())),
    }
}

pub(super) fn is_not_found(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    })
}

pub(super) struct OpenCodeDiscovery {
    pub ready_databases: Vec<PreparedOpencodeDatabase>,
    pub ready_owned_sessions: HashMap<String, HashSet<String>>,
    pub diagnostics: crate::sources::ParseDiagnostics,
    pub scope_targets: Vec<SessionScope>,
    pub session_cwds: HashMap<SessionScope, String>,
    pub database_states: HashMap<String, crate::state::OpencodeDatabaseState>,
    pub database_paths_to_delete: Vec<String>,
    pub database_outcomes: HashMap<String, OpencodeDatabaseOutcome>,
    pub legacy_paths_to_delete: Vec<String>,
    pub tasks: Vec<FileTask>,
    pub unchanged_identities: Vec<(String, FileIdentity)>,
    pub files_scanned: usize,
    pub files_skipped: usize,
    pub total_bytes: u64,
    pub deferred_pending_scopes: Vec<SessionScope>,
}

pub(super) fn discover_opencode(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    selected: Option<&[crate::sources::SourceFile]>,
    state: &mut IngestState,
    pending_recovery: &Option<PendingIngest>,
    next_doc_id: &Arc<AtomicU64>,
) -> Result<Option<OpenCodeDiscovery>> {
    let full_scan = selected.is_none();
    let excluder = build_path_excluder(options)?;
    let mut tasks = Vec::new();
    let mut unchanged_identities = Vec::new();
    let mut files_scanned = 0;
    let mut files_skipped = 0;
    let mut total_bytes = 0;
    let mut opencode_discovered_database_paths = HashSet::new();
    let mut deferred_pending_scopes = pending_recovery
        .as_ref()
        .map(|pending| pending.session_scopes.clone())
        .unwrap_or_default();
    let mut opencode_ready_databases: Vec<PreparedOpencodeDatabase> = Vec::new();
    let mut opencode_ready_owned_sessions: HashMap<String, HashSet<String>> = HashMap::new();
    let mut opencode_diagnostics: crate::sources::ParseDiagnostics = Default::default();
    let mut opencode_scope_targets: Vec<SessionScope> = Vec::new();
    let mut opencode_session_cwds: HashMap<SessionScope, String> = HashMap::new();
    let mut opencode_database_states: HashMap<String, crate::state::OpencodeDatabaseState> =
        HashMap::new();
    let mut opencode_database_paths_to_delete: Vec<String> = Vec::new();
    let mut opencode_database_outcomes: HashMap<String, OpencodeDatabaseOutcome> = HashMap::new();
    let mut opencode_legacy_paths_to_delete: Vec<String> = Vec::new();
    if options.include_opencode && selected.is_none_or(|databases| !databases.is_empty()) {
        let database_files = if let Some(databases) = selected {
            databases.to_vec()
        } else {
            crate::sources::opencode::discover_databases()?
        };
        let mut planned_databases = Vec::new();
        for source_file in database_files {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let key = path.to_string_lossy().to_string();
            opencode_discovered_database_paths.insert(key.clone());
            let Some(meta) = (match discovered_metadata(&path) {
                Ok(meta) => meta,
                Err(error) => {
                    if !full_scan {
                        return Err(error)
                            .with_context(|| format!("stat changed database {}", path.display()));
                    }
                    opencode_database_outcomes.insert(key, OpencodeDatabaseOutcome::Failed);
                    files_skipped += 1;
                    continue;
                }
            }) else {
                if !full_scan {
                    return Ok(None);
                }
                opencode_database_outcomes.insert(key, OpencodeDatabaseOutcome::Failed);
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let previous = state.opencode_databases.get(&key);
            match crate::sources::opencode::scan_database(&path, previous) {
                Ok(scan) => {
                    if !full_scan
                        && previous.is_none_or(|previous| {
                            let sessions = scan
                                .sessions
                                .iter()
                                .map(|session| session.id.clone())
                                .collect::<HashSet<_>>();
                            sessions != previous.owned_session_ids
                        })
                    {
                        return Ok(None);
                    }
                    opencode_database_outcomes.insert(key, OpencodeDatabaseOutcome::Planned);
                    planned_databases.push(PlannedOpencodeDatabase { path, scan });
                }
                Err(error) => {
                    if !full_scan {
                        return Err(error)
                            .with_context(|| format!("scan changed database {}", path.display()));
                    }
                    // A bad/locked modern database must not hide the compatible JSON store.
                    opencode_database_outcomes.insert(key, OpencodeDatabaseOutcome::Failed);
                    files_skipped += 1;
                }
            }
        }
        planned_databases.sort_by(|left, right| left.path.cmp(&right.path));

        for database in planned_databases {
            let path = database.path.to_string_lossy().to_string();
            let mut hydration_session_ids = database.scan.dirty_session_ids.clone();
            if let Some(pending) = &pending_recovery {
                hydration_session_ids.extend(
                    pending
                        .session_scopes
                        .iter()
                        .filter(|scope| scope.source_path == path)
                        .map(|scope| scope.session_id.clone()),
                );
                hydration_session_ids.sort();
                hydration_session_ids.dedup();
            }
            match prehydrate_opencode_database(
                &database.path,
                &database.scan,
                &hydration_session_ids,
                next_doc_id,
                &paths.state,
            ) {
                Ok(prepared) => {
                    opencode_database_outcomes.insert(path, OpencodeDatabaseOutcome::Ready);
                    opencode_diagnostics.merge(prepared.diagnostics.clone());
                    opencode_ready_databases.push(prepared);
                }
                Err(error) => {
                    if !full_scan {
                        return Err(error).with_context(|| {
                            format!("parse changed database {}", database.path.display())
                        });
                    }
                    opencode_database_outcomes.insert(path, OpencodeDatabaseOutcome::Failed);
                    files_skipped += 1;
                }
            }
        }
        opencode_ready_databases.sort_by(|left, right| left.path.cmp(&right.path));

        let mut owner_by_session = HashMap::<String, String>::new();
        let mut failed_previous = state
            .opencode_databases
            .iter()
            .filter(|(path, _)| {
                matches!(
                    classify_opencode_database_outcome(
                        opencode_database_outcomes.get(*path).copied(),
                        opencode_discovered_database_paths.contains(*path),
                        full_scan,
                    ),
                    OpencodeDatabaseOutcome::Failed
                )
            })
            .collect::<Vec<_>>();
        failed_previous.sort_by_key(|(path, _)| *path);
        for (path, previous) in failed_previous {
            for session_id in &previous.owned_session_ids {
                claim_opencode_session_owner(&mut owner_by_session, session_id.clone(), path);
            }
        }
        for database in &opencode_ready_databases {
            let path = database.path.to_string_lossy().to_string();
            for session in &database.scan.sessions {
                claim_opencode_session_owner(&mut owner_by_session, session.id.clone(), &path);
            }
        }
        for (path, previous) in &state.opencode_databases {
            let outcome = classify_opencode_database_outcome(
                opencode_database_outcomes.get(path).copied(),
                opencode_discovered_database_paths.contains(path),
                full_scan,
            );
            if outcome != OpencodeDatabaseOutcome::Ready {
                if outcome == OpencodeDatabaseOutcome::ConfirmedAbsent {
                    opencode_database_paths_to_delete.push(path.clone());
                }
                continue;
            }
            for session_id in &previous.owned_session_ids {
                if owner_by_session.get(session_id) != Some(path) {
                    opencode_scope_targets.push(SessionScope {
                        source_path: path.clone(),
                        session_id: session_id.clone(),
                    });
                }
            }
        }

        for database in &opencode_ready_databases {
            let path = database.path.to_string_lossy().to_string();
            let owned_sessions = database
                .scan
                .sessions
                .iter()
                .filter(|session| owner_by_session.get(&session.id) == Some(&path))
                .collect::<Vec<_>>();
            let owned_session_ids = owned_sessions
                .iter()
                .map(|session| session.id.clone())
                .collect::<HashSet<_>>();
            opencode_ready_owned_sessions.insert(path.clone(), owned_session_ids.clone());
            for session in &owned_sessions {
                opencode_session_cwds.insert(
                    SessionScope {
                        source_path: path.clone(),
                        session_id: session.id.clone(),
                    },
                    session.directory.clone(),
                );
            }
            for session_id in &database.scan.dirty_session_ids {
                if owner_by_session.get(session_id) == Some(&path) {
                    opencode_scope_targets.push(SessionScope {
                        source_path: path.clone(),
                        session_id: session_id.clone(),
                    });
                }
            }
            opencode_database_states.insert(
                path,
                crate::state::OpencodeDatabaseState {
                    parser_version: crate::sources::opencode::DATABASE_STATE_VERSION,
                    event_rowid: database.scan.cursor.event_rowid,
                    event_id: database.scan.cursor.event_id.clone(),
                    owned_session_ids,
                },
            );
        }
        if let Some(pending) = &pending_recovery {
            deferred_pending_scopes = pending
                .session_scopes
                .iter()
                .filter(|scope| {
                    classify_opencode_database_outcome(
                        opencode_database_outcomes.get(&scope.source_path).copied(),
                        opencode_discovered_database_paths.contains(&scope.source_path),
                        full_scan,
                    ) == OpencodeDatabaseOutcome::Failed
                })
                .cloned()
                .collect();
            for scope in &pending.session_scopes {
                if matches!(
                    opencode_database_outcomes.get(&scope.source_path),
                    Some(OpencodeDatabaseOutcome::Ready)
                ) {
                    opencode_scope_targets.push(scope.clone());
                }
            }
        }
        opencode_scope_targets.sort_by(|left, right| {
            left.source_path
                .cmp(&right.source_path)
                .then_with(|| left.session_id.cmp(&right.session_id))
        });
        opencode_scope_targets.dedup();
        opencode_database_paths_to_delete.sort();
        opencode_database_paths_to_delete.dedup();

        let opencode_files = if full_scan {
            crate::sources::opencode::discover_sessions()?
        } else {
            Vec::new()
        };
        let legacy_candidates = opencode_files
            .iter()
            .filter(|file| {
                !excluder.is_excluded(&file.path)
                    && file
                        .path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|session| owner_by_session.contains_key(session))
            })
            .map(|file| file.path.to_string_lossy().into_owned())
            .collect::<HashSet<_>>();
        let mut legacy_cleanup = index.source_paths_with_records(&legacy_candidates)?;
        if !legacy_candidates.is_empty() {
            let analytics = AnalyticsStore::open_read_only(analytics_path(&paths.state))?;
            legacy_cleanup.extend(analytics.source_paths(&legacy_candidates)?);
        }
        for source_file in opencode_files {
            let path = source_file.path;
            if excluder.is_excluded(&path) {
                files_skipped += 1;
                continue;
            }
            let session_id = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            if owner_by_session.contains_key(session_id) {
                let path_key = path.to_string_lossy().to_string();
                if state.files.remove(&path_key).is_some() || legacy_cleanup.contains(&path_key) {
                    opencode_legacy_paths_to_delete.push(path_key);
                }
                files_skipped += 1;
                continue;
            }
            let Some(meta) = discovered_metadata(&path)? else {
                files_skipped += 1;
                continue;
            };
            files_scanned += 1;
            total_bytes += meta.len();
            let key = path.to_string_lossy().to_string();
            let (task, skip) = prepare_file_task(
                path,
                SourceKind::Opencode,
                options.include_reasoning,
                &meta,
                state.files.get(&key),
            );
            if skip {
                unchanged_identities.push((key, task.identity));
                files_skipped += 1;
                continue;
            }
            tasks.push(task);
        }
    }

    Ok(Some(OpenCodeDiscovery {
        ready_databases: opencode_ready_databases,
        ready_owned_sessions: opencode_ready_owned_sessions,
        diagnostics: opencode_diagnostics,
        scope_targets: opencode_scope_targets,
        session_cwds: opencode_session_cwds,
        database_states: opencode_database_states,
        database_paths_to_delete: opencode_database_paths_to_delete,
        database_outcomes: opencode_database_outcomes,
        legacy_paths_to_delete: opencode_legacy_paths_to_delete,
        tasks,
        unchanged_identities,
        files_scanned,
        files_skipped,
        total_bytes,
        deferred_pending_scopes,
    }))
}

/// Glob-based path exclusion applied at discovery time so matched
/// transcripts never enter the index. Empty pattern sets disable matching.
#[derive(Debug, Clone)]
pub(crate) struct PathExcluder {
    pub(super) set: Option<globset::GlobSet>,
}

impl PathExcluder {
    pub(crate) fn build(patterns: &[String]) -> Result<Self> {
        if patterns.is_empty() {
            return Ok(Self { set: None });
        }
        let mut builder = globset::GlobSetBuilder::new();
        for pattern in patterns {
            builder.add(
                globset::GlobBuilder::new(pattern)
                    .literal_separator(false)
                    .build()
                    .with_context(|| format!("invalid exclude pattern: {pattern}"))?,
            );
        }
        let set = builder
            .build()
            .context("failed to compile exclude patterns")?;
        Ok(Self { set: Some(set) })
    }

    pub(crate) fn is_excluded(&self, path: &Path) -> bool {
        let Some(set) = &self.set else {
            return false;
        };
        set.is_match(path)
            || path
                .canonicalize()
                .is_ok_and(|canonical| canonical != path && set.is_match(&canonical))
    }
}

pub(crate) fn build_path_excluder(options: &IngestOptions) -> Result<PathExcluder> {
    let expanded = crate::config::expand_exclude_patterns(options.exclude_patterns.clone());
    PathExcluder::build(&expanded)
}

pub(super) fn directory_projection(options: &IngestOptions) -> Vec<u8> {
    let roots = [
        options.claude_sources.clone(),
        if options.include_codex {
            crate::sources::codex::rollout_roots()
        } else {
            Vec::new()
        },
        if options.include_pi {
            vec![crate::sources::pi::sessions_root()]
        } else {
            Vec::new()
        },
        if options.include_omp {
            crate::sources::omp::session_roots()
        } else {
            Vec::new()
        },
    ];
    let mut hash = Sha256::new();
    hash.update(b"memex-directory-discovery-v1");
    for enabled in [
        options.include_agents,
        options.include_reasoning,
        options.include_codex,
        options.include_opencode,
        options.include_cursor,
        options.include_pi,
        options.include_omp,
        options.include_openclaw,
        options.include_copilot,
        options.include_grok,
        options.include_jcode,
        options.include_muse,
    ] {
        hash.update([u8::from(enabled)]);
    }
    for roots in roots {
        hash.update((roots.len() as u64).to_le_bytes());
        for root in roots {
            let bytes = root.as_os_str().as_encoded_bytes();
            hash.update((bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        }
    }
    for pattern in crate::config::expand_exclude_patterns(options.exclude_patterns.clone()) {
        hash.update((pattern.len() as u64).to_le_bytes());
        hash.update(pattern.as_bytes());
    }
    hash.finalize().to_vec()
}

pub(super) struct PreparedRefresh {
    pub full_scan: bool,
    pub scan_cache: Option<ScanCache>,
    pub state_path: PathBuf,
    pub state: IngestState,
    pub recovering_pending_ingest: bool,
    pub empty_index_rebuild: bool,
    pub next_doc_id: Arc<AtomicU64>,
    pub tasks: Vec<FileTask>,
    pub files_scanned: usize,
    pub files_skipped: usize,
    pub total_bytes: u64,
    pub session_ids: HashSet<String>,
    pub deferred_pending_scopes: Vec<SessionScope>,
    pub opencode_ready_databases: Vec<PreparedOpencodeDatabase>,
    pub opencode_ready_owned_sessions: HashMap<String, HashSet<String>>,
    pub opencode_diagnostics: crate::sources::ParseDiagnostics,
    pub opencode_scope_targets: Vec<SessionScope>,
    pub opencode_session_cwds: HashMap<SessionScope, String>,
    pub opencode_database_paths_to_delete: Vec<String>,
    pub opencode_session_links: HashMap<String, crate::sources::opencode::SessionLinks>,
    pub recover_embeddings: bool,
    pub reconcile_pending_vector_ids: bool,
    pub recover_vector_cleanup: bool,
    pub delete_paths: Vec<String>,
    pub vector_delete_paths: HashSet<String>,
    pub installed_opencode_states: HashMap<String, crate::state::OpencodeDatabaseState>,
    pub opencode_database_state_changed: bool,
    pub identities_changed: bool,
}

pub(super) fn prepare_refresh(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    pool: &rayon::ThreadPool,
    recovered: publication::RecoveredCheckpoint,
    dirty: Option<&HashSet<PathBuf>>,
    mut scan_cache: Option<ScanCache>,
) -> Result<PreparedRefresh> {
    let publication::RecoveredCheckpoint {
        mut state,
        pending_recovery,
        empty_index_rebuild,
    } = recovered;
    let recovering_pending_ingest = pending_recovery.is_some();
    let state_path = paths.state.join("ingest.json");
    let selected = if let Some(dirty) = dirty
        && !recovering_pending_ingest
        && !empty_index_rebuild
        && state_path.exists()
    {
        match selection::resolve_dirty(options, dirty, &state)? {
            selection::DirtySelection::Paths { files, databases } => Some((files, databases)),
            selection::DirtySelection::Resync => None,
        }
    } else {
        None
    };
    let full_scan = selected.is_none();

    // Index-time exclusion: matched transcripts never enter the index, and
    // records previously indexed from now-excluded paths are removed.
    #[cfg(feature = "profiling")]
    let discovery_profile = crate::profiling::Scope::enter("ingest.discovery");
    let excluder = build_path_excluder(options)?;
    let mut excluded_state_paths: Vec<String> = Vec::new();
    if full_scan {
        state.files.retain(|key, _| {
            if excluder.is_excluded(Path::new(key)) {
                excluded_state_paths.push(key.clone());
                false
            } else {
                true
            }
        });
    }
    let next_doc_id = Arc::new(AtomicU64::new(state.next_doc_id));

    let mut tasks = Vec::new();
    let mut unchanged_identities = Vec::new();
    let mut files_scanned = 0usize;
    let mut files_skipped = 0usize;
    let mut total_bytes = 0u64;
    if !full_scan {
        scan_cache = None;
    } else if scan_cache.is_none() {
        scan_cache = Some(ScanCache::load(&paths.state.join("scan_cache.json"))?);
    }
    let mut inventory = scan_cache.as_mut().map(|cache| {
        crate::directory_inventory::DiscoveryInventory::new(
            cache.directory_inventory.take(),
            &directory_projection(options),
        )
    });
    let transcripts = discovery::discover_transcripts(
        options,
        &excluder,
        &state,
        pool,
        selected.as_ref().map(|(files, _)| files.as_slice()),
        inventory.as_mut(),
    )?;
    #[cfg(feature = "profiling")]
    if let Some(inventory) = &inventory {
        let counters = inventory.counters();
        crate::profiling::count!(
            "discovery.directories_checked",
            counters.directories_checked
        );
        crate::profiling::count!("discovery.directories_reused", counters.directories_reused);
        crate::profiling::count!(
            "discovery.directories_enumerated",
            counters.directories_enumerated
        );
        crate::profiling::count!("discovery.fallback_walks", counters.fallback_walks);
        crate::profiling::count!("discovery.metadata_checks", counters.metadata_checks);
    }
    if let Some(cache) = scan_cache.as_mut() {
        cache.directory_inventory =
            inventory.and_then(crate::directory_inventory::DiscoveryInventory::finish);
    }
    tasks.extend(transcripts.tasks);
    unchanged_identities.extend(transcripts.unchanged_identities);
    files_scanned += transcripts.files_scanned;
    files_skipped += transcripts.files_skipped;
    total_bytes += transcripts.total_bytes;
    let session_ids = transcripts.session_ids;

    let Some(opencode) = discovery::discover_opencode(
        paths,
        index,
        options,
        selected.as_ref().map(|(_, databases)| databases.as_slice()),
        &mut state,
        &pending_recovery,
        &next_doc_id,
    )?
    else {
        return prepare_refresh(
            paths,
            index,
            options,
            pool,
            publication::RecoveredCheckpoint {
                state,
                pending_recovery,
                empty_index_rebuild,
            },
            None,
            scan_cache,
        );
    };
    tasks.extend(opencode.tasks);
    unchanged_identities.extend(opencode.unchanged_identities);
    files_scanned += opencode.files_scanned;
    files_skipped += opencode.files_skipped;
    total_bytes += opencode.total_bytes;
    let deferred_pending_scopes = opencode.deferred_pending_scopes;
    let opencode_ready_databases = opencode.ready_databases;
    let opencode_ready_owned_sessions = opencode.ready_owned_sessions;
    let opencode_diagnostics = opencode.diagnostics;
    let opencode_scope_targets = opencode.scope_targets;
    let opencode_session_cwds = opencode.session_cwds;
    let opencode_database_states = opencode.database_states;
    let opencode_database_paths_to_delete = opencode.database_paths_to_delete;
    let opencode_database_outcomes = opencode.database_outcomes;
    let opencode_legacy_paths_to_delete = opencode.legacy_paths_to_delete;

    // Discovery inventories can be incomplete when a source is disabled or a root is
    // unavailable, so absence from discovery never removes state on its own. A direct
    // NotFound confirms removal only while the file's immediate container is still
    // readable: otherwise one unavailable ancestor looks like every transcript under it
    // was deleted individually.
    let mut missing_state_paths = Vec::new();
    if full_scan {
        state
            .files
            .retain(|path, _| match Path::new(path).metadata() {
                Err(error)
                    if error.kind() == std::io::ErrorKind::NotFound
                        && Path::new(path).parent().is_some_and(|parent| {
                            parent.metadata().is_ok_and(|meta| meta.is_dir())
                        }) =>
                {
                    missing_state_paths.push(path.clone());
                    false
                }
                _ => true,
            });
    }

    // Previously indexed records under now-excluded paths must be deleted even
    // when there is no ingest state entry for them (e.g. state loss or legacy runs).
    let mut excluded_index_paths: Vec<String> = Vec::new();
    if full_scan && excluder.set.is_some() {
        index.for_each_record(|record| {
            if excluder.is_excluded(Path::new(&record.source_path)) {
                excluded_index_paths.push(record.source_path.clone());
            }
            Ok(())
        })?;
        excluded_index_paths.sort();
        excluded_index_paths.dedup();
    }
    files_skipped += excluded_state_paths.len();

    let opencode_session_links = if tasks.iter().any(|task| task.source == SourceKind::Opencode) {
        crate::sources::opencode::session_links_by_id()
    } else {
        HashMap::new()
    };

    // Markers written before `embedding_publication` existed recorded every vector
    // publication as an embedding publication; preserve that recovery behavior. Pure
    // deletion recovery needs no model, but a mixed batch can also replay surviving files
    // under new document IDs, so restore their vector coverage before live-ID
    // reconciliation discards the previous IDs.
    let recover_embeddings = pending_recovery.as_ref().is_some_and(|pending| {
        pending
            .embedding_publication
            .unwrap_or(pending.vector_publication)
            || (pending.vector_publication
                && crate::vector::VectorIndex::exists(&paths.vectors)
                && tasks.iter().any(|task| {
                    let path = task.path.to_string_lossy();
                    pending
                        .source_paths
                        .iter()
                        .any(|source| source == path.as_ref())
                        && !pending
                            .vector_delete_paths
                            .iter()
                            .any(|source| source == path.as_ref())
                }))
    });
    // Pure deletion recovery needs no embedder, but it still crossed the vector
    // publication boundary and must remove every vector that is no longer live.
    let reconcile_pending_vector_ids = pending_recovery
        .as_ref()
        .is_some_and(|pending| pending.vector_publication);
    let pending_ready_scope_recovery = pending_recovery.as_ref().is_some_and(|pending| {
        pending.session_scopes.iter().any(|scope| {
            matches!(
                opencode_database_outcomes.get(&scope.source_path),
                Some(OpencodeDatabaseOutcome::Ready)
            )
        })
    });
    let recover_vector_cleanup = pending_ready_scope_recovery
        || pending_recovery.as_ref().is_some_and(|pending| {
            pending
                .source_paths
                .iter()
                .any(|path| crate::sources::opencode::is_database_path(path))
        });
    let mut vector_delete_paths: HashSet<String> = pending_recovery
        .as_ref()
        .map(|pending| pending.vector_delete_paths.iter().cloned().collect())
        .unwrap_or_default();
    // Markers written before `vector_delete_paths` existed only recorded OpenCode
    // database deletions; preserve that recovery behavior.
    if let Some(pending) = &pending_recovery {
        vector_delete_paths.extend(
            pending
                .source_paths
                .iter()
                .filter(|path| crate::sources::opencode::is_database_path(path))
                .cloned(),
        );
    }
    vector_delete_paths.extend(opencode_database_paths_to_delete.iter().cloned());
    vector_delete_paths.extend(opencode_legacy_paths_to_delete.iter().cloned());
    vector_delete_paths.extend(missing_state_paths.iter().cloned());
    // A newly excluded transcript loses its indexed records, so its embeddings have to go
    // with them; otherwise they stay live and keep matching semantic searches.
    vector_delete_paths.extend(excluded_state_paths.iter().cloned());
    vector_delete_paths.extend(excluded_index_paths.iter().cloned());
    vector_delete_paths.extend(
        tasks
            .iter()
            .filter(|task| task.delete_first())
            .map(|task| task.path.to_string_lossy().into_owned()),
    );
    let mut delete_paths = pending_recovery
        .as_ref()
        .map(|pending| pending.source_paths.clone())
        .unwrap_or_default();
    delete_paths.extend(opencode_database_paths_to_delete.clone());
    delete_paths.extend(opencode_legacy_paths_to_delete.clone());
    delete_paths.extend(missing_state_paths);
    delete_paths.extend(excluded_state_paths);
    delete_paths.extend(excluded_index_paths);
    delete_paths.extend(
        tasks
            .iter()
            .filter(|task| task.delete_first())
            .map(|task| task.path.to_string_lossy().to_string()),
    );
    delete_paths.sort();
    delete_paths.dedup();
    let mut installed_opencode_states = state.opencode_databases.clone();
    for path in &opencode_database_paths_to_delete {
        installed_opencode_states.remove(path);
    }
    installed_opencode_states.extend(opencode_database_states.clone());
    let opencode_database_state_changed = installed_opencode_states != state.opencode_databases;

    #[cfg(feature = "profiling")]
    drop(discovery_profile);
    crate::profiling::count!("ingest.files_scanned", files_scanned);
    crate::profiling::count!("ingest.files_skipped", files_skipped);
    crate::profiling::count!("ingest.parse_tasks", tasks.len());
    crate::profiling::count!(
        "opencode.legacy_deletes_scheduled",
        opencode_legacy_paths_to_delete.len()
    );
    crate::profiling::count!(
        "opencode.scope_deletes_scheduled",
        opencode_scope_targets.len()
    );
    crate::profiling::count!(
        "opencode.database_deletes_scheduled",
        opencode_database_paths_to_delete.len()
    );
    let mut identities_changed = false;
    for (path, identity) in unchanged_identities {
        if let Some(previous) = state.files.get_mut(&path)
            && previous.identity != identity
        {
            previous.identity = identity;
            identities_changed = true;
        }
    }

    Ok(PreparedRefresh {
        full_scan,
        scan_cache,
        state_path,
        state,
        recovering_pending_ingest,
        empty_index_rebuild,
        next_doc_id,
        tasks,
        files_scanned,
        files_skipped,
        total_bytes,
        session_ids,
        deferred_pending_scopes,
        opencode_ready_databases,
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
    })
}

pub(super) fn can_skip_fresh_scan(
    cache: &ScanCache,
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
    ttl_seconds: u64,
) -> Result<bool> {
    let pending_path = pending_ingest_path(paths);
    if pending_path
        .try_exists()
        .with_context(|| format!("check pending ingest at {}", pending_path.display()))?
    {
        return Ok(false);
    }
    if options.include_opencode {
        let databases = match crate::sources::opencode::discover_databases() {
            Ok(databases) => databases,
            Err(_) => return Ok(false),
        };
        if !databases.is_empty() {
            return Ok(false);
        }
    }
    if index.doc_count()? == 0 {
        return Ok(false);
    }
    if !cache.is_fresh(ttl_seconds) {
        return Ok(false);
    }
    let analytics = AnalyticsStore::open(analytics_path(&paths.state))?;
    if !analytics.complete()? && index.doc_count()? > 0 {
        return Ok(false);
    }
    can_skip_noop_index(paths, index, options)
}

pub(super) fn can_skip_noop_index(
    paths: &Paths,
    index: &SearchIndex,
    options: &IngestOptions,
) -> Result<bool> {
    crate::profiling::span!("vectors.compatibility");
    if !options.embeddings {
        return Ok(true);
    }
    let Some(dimensions) = options.model.known_dimensions() else {
        return Ok(false);
    };
    if !crate::vector::VectorIndex::exists(&paths.vectors) {
        return Ok(false);
    }
    let vector_index = crate::vector::VectorIndex::open(&paths.vectors)?;
    if vector_index.model() != Some(options.model.as_str())
        || vector_index.dimensions() != dimensions
    {
        return Ok(false);
    }
    vector_index_covers_embeddable_records(index, &vector_index)
}

pub(super) fn vector_index_covers_embeddable_records(
    index: &SearchIndex,
    vector_index: &crate::vector::VectorIndex,
) -> Result<bool> {
    crate::profiling::span!("vectors.coverage_check");
    let mut covers_all = true;
    index.for_each_record(|record| {
        if record_needs_embedding(&record) && !vector_index.contains(record.doc_id) {
            covers_all = false;
        }
        Ok(())
    })?;
    Ok(covers_all)
}

pub(super) fn record_needs_embedding(record: &Record) -> bool {
    is_embedding_role(&record.role) && !record.text.is_empty()
}

pub(super) fn vector_migration(
    vector_dir: &Path,
    tasks: &[FileTask],
    configured_model: ModelChoice,
) -> VectorMigration {
    let rebuild = tasks.iter().any(|task| task.parser_version_invalidated())
        && crate::vector::VectorIndex::exists(vector_dir);
    let model = if rebuild {
        crate::vector::VectorIndex::open(vector_dir)
            .ok()
            .and_then(|index| {
                index
                    .model()
                    .and_then(|model| ModelChoice::parse(model).ok())
            })
            .unwrap_or(configured_model)
    } else {
        configured_model
    };
    VectorMigration { rebuild, model }
}
