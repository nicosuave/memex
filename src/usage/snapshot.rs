//! Retained per-source snapshots with generation-based revalidation.
//!
//! Queries clone snapshot `Arc`s under a short store lock and run lock-free.
//! TTL-fresh entries serve outright; stale ones revalidate against file state
//! (reused without decode when unchanged); only changed corpora rebuild.
//! Refreshes single-flight on the refresh lock while concurrent queries keep
//! serving the previous snapshot.

use super::cache::UsageCache;
use super::compact::UsageAssembly;
use super::facts::{FactRow, read_ordinal_run};
use super::merge::{MergedPos, build_merged_order};
use super::progress::{UsageScanProgress, publish_scan_progress};
use super::scan::{
    FileFingerprint, PARSE_SAVE_CHUNK, SCANNERS, deps_observed_current, fingerprint_files,
    parse_missing_usage_files, parse_source_file, run_partition_scanner, source_files,
    source_ordinal, source_spec, stable_triples, usage_file_metadata,
};
use super::{UsageEvent, UsageQuery, usage_timing};
use crate::types::SourceFilter;
use anyhow::Result;
use once_cell::sync::Lazy;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub(crate) type PartitionKey = (SourceFilter, Option<PathBuf>);

/// Retained per-source assembly. Immutable once published: queries clone the `Arc`s
/// under a short store lock and then filter and aggregate lock-free, so a cheap
/// query never waits behind a refresh or another expensive report.
pub(crate) struct PartitionEntry {
    /// Last time this entry was served fresh or revalidated. The query TTL decides
    /// when to re-check freshness — not when to discard usable state.
    pub(crate) checked_at: Instant,
    /// Inputs observed for this assembly, including Cursor project attribution.
    pub(crate) fingerprint: FileFingerprint,
    pub(crate) fact_generation: Option<String>,
    pub(crate) assembly: Arc<UsageAssembly>,
    pub(crate) warnings: Arc<Vec<String>>,
}

/// Bounded per-source snapshot store. Alternating between filters reuses instead of
/// evicting like the previous single slot.
pub(crate) static PARTITIONS: Lazy<Mutex<HashMap<PartitionKey, PartitionEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Maximum retained partitions; the least-recently-checked entry is evicted on
/// insert. Bounds memory across distinct cache paths.
pub(crate) const MAX_PARTITIONS: usize = 32;

/// Precomputed global order over shared partition assemblies, plus the warnings
/// their refreshes reported, in scanner order.
pub(crate) struct MergedSnapshot {
    pub(crate) parts: Vec<Arc<UsageAssembly>>,
    pub(crate) order: Arc<Vec<MergedPos>>,
    pub(crate) warnings: Arc<Vec<String>>,
}

pub(crate) struct MergedEntry {
    checked_at: Instant,
    snapshot: MergedSnapshot,
}

/// Retained combined views, keyed by cache alone: every combined query shares one
/// merged order instead of each rebuilding a full assembly.
pub(crate) static MERGED: Lazy<Mutex<HashMap<Option<PathBuf>, MergedEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Maximum retained merged orders.
pub(crate) const MAX_MERGED: usize = 8;

/// Assembled query input: one partition or the shared merged view.
pub(crate) enum Snapshot {
    Partition(Arc<UsageAssembly>, Arc<Vec<String>>),
    Merged(MergedSnapshot),
}

pub(crate) fn lock_partitions()
-> std::sync::MutexGuard<'static, HashMap<PartitionKey, PartitionEntry>> {
    PARTITIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) fn lock_merged() -> std::sync::MutexGuard<'static, HashMap<Option<PathBuf>, MergedEntry>>
{
    MERGED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Returns the assembled query input: TTL-fresh snapshots are reused outright,
/// stale ones are revalidated against current file state (reused without any decode
/// when nothing changed), and only genuinely changed corpora pay for a rebuild.
/// Refreshes single-flight on `USAGE_SCAN_LOCK`; concurrent queries keep serving
/// the previous snapshot instead of queueing behind a refresh.
pub(crate) fn ensure_snapshot(query: &UsageQuery) -> Result<Snapshot> {
    let ttl = Duration::from_millis(query.memo_ttl_ms);
    if ttl.is_zero() {
        // One-shot queries never publish or evict another caller's snapshot.
        return Ok(oneshot_snapshot(query));
    }
    match query.source {
        Some(filter) => {
            let (assembly, warnings) = ensure_partition(filter, query.cache_path.clone(), ttl);
            Ok(Snapshot::Partition(assembly, warnings))
        }
        None => Ok(Snapshot::Merged(ensure_merged(
            query.cache_path.clone(),
            ttl,
        ))),
    }
}

/// Combined assembly without retention: one partition per source through the
/// same facts-maintained rebuild as retaining refreshes, merged without
/// publishing. Single-source one-shots scan only that source. Unlike the old
/// blob-only assembly, every rebuild keeps facts consistent with the blobs it
/// wrote, so a one-shot can never strand the facts behind (which used to
/// poison later facts-backed refreshes with permanently stale rows).
pub(crate) fn oneshot_snapshot(query: &UsageQuery) -> Snapshot {
    let _refresh = USAGE_SCAN_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let assembly_start = Instant::now();
    let snapshot = match query.source {
        Some(filter) => {
            let (assembly, warnings) = oneshot_partition(filter, query.cache_path.clone());
            Snapshot::Partition(assembly, warnings)
        }
        None => Snapshot::Merged(oneshot_merged(&query.cache_path)),
    };
    let events = match &snapshot {
        Snapshot::Partition(assembly, _) => assembly.len(),
        Snapshot::Merged(merged) => merged.order.len(),
    };
    usage_timing(assembly_start, || {
        format!("assemble total ({events} events)")
    });
    snapshot
}

/// One source partition without retention: a still-valid retained snapshot is
/// reused read-only (no freshness stamp, no eviction), otherwise the partition
/// rebuilds through the facts-maintained path without publishing.
fn oneshot_partition(
    filter: SourceFilter,
    cache_path: Option<PathBuf>,
) -> (Arc<UsageAssembly>, Arc<Vec<String>>) {
    let key = (filter, cache_path.clone());
    let previous = lock_partitions().get(&key).map(|entry| {
        (
            entry.fingerprint.clone(),
            entry.fact_generation.clone(),
            entry.assembly.clone(),
            entry.warnings.clone(),
        )
    });
    if let Some((fingerprint, generation, assembly, warnings)) = previous
        && check_snapshot_valid(
            filter,
            key.1.as_deref(),
            &fingerprint,
            generation.as_deref(),
        )
    {
        return (assembly, warnings);
    }
    let entry = rebuild_partition(filter, &cache_path, None, None);
    (entry.assembly, entry.warnings)
}

/// Fresh merged view without retention: per-source reuse-or-rebuild plus a
/// merged order, never published to the merged store.
fn oneshot_merged(cache_path: &Option<PathBuf>) -> MergedSnapshot {
    let mut parts = Vec::with_capacity(SCANNERS.len());
    let mut warnings = Vec::new();
    for (filter, _) in SCANNERS {
        let (assembly, partition_warnings) = oneshot_partition(filter, cache_path.clone());
        warnings.extend(partition_warnings.iter().cloned());
        parts.push(assembly);
    }
    let order = Arc::new(build_merged_order(&parts));
    MergedSnapshot {
        parts,
        order,
        warnings: Arc::new(warnings),
    }
}

/// Returns one fresh source partition, revalidating or rebuilding as needed.
pub(crate) fn ensure_partition(
    filter: SourceFilter,
    cache_path: Option<PathBuf>,
    ttl: Duration,
) -> (Arc<UsageAssembly>, Arc<Vec<String>>) {
    let key = (filter, cache_path);
    loop {
        let stale = {
            let lock_start = Instant::now();
            let store = lock_partitions();
            usage_timing(lock_start, || "lock wait".to_string());
            match store.get(&key) {
                Some(entry) if entry.checked_at.elapsed() < ttl => {
                    return (entry.assembly.clone(), entry.warnings.clone());
                }
                Some(entry) => Some((entry.assembly.clone(), entry.warnings.clone())),
                None => None,
            }
        };
        match USAGE_SCAN_LOCK.try_lock() {
            Ok(_refresh) => {
                // Another thread may have refreshed between our check and acquiring
                // the refresh lock; re-check before doing any I/O.
                if let Some(entry) = lock_partitions().get(&key)
                    && entry.checked_at.elapsed() < ttl
                {
                    return (entry.assembly.clone(), entry.warnings.clone());
                }
                return refresh_partition(&key);
            }
            Err(_) => {
                // A refresh is already running: serve the previous snapshot instead
                // of queueing behind it. With no snapshot at all, block until the
                // in-flight refresh publishes, then retry.
                if let Some(stale) = stale {
                    return stale;
                }
                let _wait = USAGE_SCAN_LOCK
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
    }
}

/// Revalidates or rebuilds one partition. Callers must hold `USAGE_SCAN_LOCK`.
pub(crate) fn refresh_partition(key: &PartitionKey) -> (Arc<UsageAssembly>, Arc<Vec<String>>) {
    let (filter, cache_path) = (key.0, key.1.clone());
    let previous = {
        let store = lock_partitions();
        store
            .get(key)
            .map(|entry| (entry.fingerprint.clone(), entry.fact_generation.clone()))
    };
    let check_start = Instant::now();
    let reuse = previous.as_ref().is_some_and(|(fingerprint, generation)| {
        check_snapshot_valid(
            filter,
            cache_path.as_deref(),
            fingerprint,
            generation.as_deref(),
        )
    });
    usage_timing(check_start, || {
        format!(
            "{} freshness check ({})",
            filter.as_str(),
            if reuse { "reuse" } else { "refresh" }
        )
    });
    if reuse {
        let mut store = lock_partitions();
        // The entry cannot have been evicted: eviction only happens on publish,
        // which needs the refresh lock we hold.
        if let Some(entry) = store.get_mut(key) {
            entry.checked_at = Instant::now();
            return (entry.assembly.clone(), entry.warnings.clone());
        }
    }
    // Rebuild. The previous snapshot stays published while scanning, so
    // concurrent queries keep serving it; it is taken for buffer reuse only for
    // the compaction window below.
    let entry = rebuild_partition(filter, &cache_path, Some(key), None);
    let result = (entry.assembly.clone(), entry.warnings.clone());
    let mut store = lock_partitions();
    if store.len() >= MAX_PARTITIONS {
        evict_oldest(&mut store, key, |entry| entry.checked_at);
    }
    store.insert(key.clone(), entry);
    result
}

/// Complete a failed cold facts lookup without repeating its input discovery.
/// The observation is only a pre-scan checkpoint, never evidence of validity.
pub(crate) fn populate_after_failed_validation(
    filter: SourceFilter,
    cache_path: &Path,
    fingerprint: FileFingerprint,
) {
    let Ok(_refresh) = USAGE_SCAN_LOCK.try_lock() else {
        return;
    };
    let key = (filter, Some(cache_path.to_path_buf()));
    if lock_partitions().contains_key(&key) {
        return;
    }
    let entry = rebuild_partition(filter, &key.1, None, Some(fingerprint));
    let mut store = lock_partitions();
    if store.len() >= MAX_PARTITIONS {
        evict_oldest(&mut store, &key, |entry| entry.checked_at);
    }
    store.insert(key, entry);
}

/// Rebuild one partition, reusing facts where reconciliation permits it and
/// otherwise scanning raw/blob occurrences. A retained entry stays available
/// while scanning, then donates compact buffers when no reader holds them.
/// The caller publishes the result or discards it after a one-shot query.
fn rebuild_partition(
    filter: SourceFilter,
    cache_path: &Option<PathBuf>,
    previous_key: Option<&PartitionKey>,
    observed_fingerprint: Option<FileFingerprint>,
) -> PartitionEntry {
    let mut warnings = Vec::new();
    let mut cache = match cache_path
        .as_deref()
        .map(UsageCache::open_for_refresh)
        .transpose()
    {
        Ok(cache) => cache,
        Err(error) => {
            warnings.push(format!("usage cache disabled: {error:#}"));
            None
        }
    };
    let stored_warnings = cache.as_ref().and_then(|cache| {
        cache
            .fact_sync(filter.as_str())
            .ok()
            .flatten()
            .map(|(_, _, warnings)| warnings)
    });
    let (events, fingerprint) = match (cache.as_mut(), stored_warnings) {
        (Some(cache), Some(stored)) if !reconciles_cross_file(filter) => {
            match refresh_partition_from_facts(filter, cache, stored, &mut warnings) {
                Ok(done) => done,
                Err(_) => legacy_refresh_partition(
                    filter,
                    Some(cache),
                    &mut warnings,
                    observed_fingerprint,
                ),
            }
        }
        // Canonical facts omit suppressed occurrences. Invalidated cross-file
        // sources need raw reconciliation to recover deleted/weakened winners;
        // skip rediscovery merely to reject their facts path. Cursor also needs
        // current transcript-derived project attribution on unchanged DB rows.
        (cache, _) => legacy_refresh_partition(filter, cache, &mut warnings, observed_fingerprint),
    };
    let fact_generation = cache
        .as_ref()
        .and_then(|cache| cache.fact_generation(filter.as_str()).ok().flatten());
    // Stamp after assembly: an assembly slower than the TTL would otherwise be
    // expired the moment it finishes, and queued follow-up queries would reassemble.
    let compaction_start = Instant::now();
    let event_count = events.len();
    let previous = previous_key
        .and_then(|key| lock_partitions().remove(key))
        .and_then(|entry| Arc::try_unwrap(entry.assembly).ok());
    let assembly = Arc::new(UsageAssembly::new(events, previous));
    usage_timing(compaction_start, || {
        format!("{} compact ({event_count} events)", filter.as_str())
    });
    PartitionEntry {
        checked_at: Instant::now(),
        assembly,
        warnings: Arc::new(warnings),
        fingerprint,
        fact_generation,
    }
}

/// Legacy full partition rebuild: scan every file (decoding all cached blobs),
/// reconcile, sort, and persist changed canonical contributions. Used when no
/// sync row exists (e.g. pre-facts databases) and as the fallback when a
/// facts-backed refresh hits any error. Returns sorted events plus the discovery
/// fingerprint for the snapshot entry.
pub(crate) fn legacy_refresh_partition(
    filter: SourceFilter,
    mut cache: Option<&mut UsageCache>,
    warnings: &mut Vec<String>,
    observed_fingerprint: Option<FileFingerprint>,
) -> (Vec<UsageEvent>, FileFingerprint) {
    let scanner = SCANNERS
        .iter()
        .find_map(|(candidate, scanner)| (*candidate == filter).then_some(*scanner))
        .expect("scanner for every source filter");
    // Stamp the inputs before scanning. A source that changes during the scan
    // must not get a checkpoint describing bytes the assembly never read.
    let fingerprint = observed_fingerprint.unwrap_or_else(|| discovery_fingerprint(filter));
    let mut events = run_partition_scanner(filter, scanner, warnings, cache.as_deref_mut());
    publish_scan_progress(None);
    let sort_start = Instant::now();
    sort_usage_events(&mut events);
    usage_timing(sort_start, || {
        format!("{} sort ({} events)", filter.as_str(), events.len())
    });
    // Canonical facts mirror the partition exactly (a failed write only warns,
    // like blob saves; the next refresh retries the atomic delta).
    if let Some(cache) = cache {
        let facts_start = Instant::now();
        let written = cache.replace_partition_facts(filter, &events, &fingerprint, warnings);
        if let Err(error) = written {
            warnings.push(format!("{} facts write failed: {error:#}", filter.as_str()));
            let _ = cache.invalidate_facts(filter.as_str());
        } else if fingerprint != discovery_fingerprint(filter) {
            let _ = cache.invalidate_facts(filter.as_str());
        }
        usage_timing(facts_start, || {
            format!("{} facts ({} events)", filter.as_str(), events.len())
        });
    }
    (events, fingerprint)
}

/// Whether a source's `reconcile_usage` decides across files (mirrors
/// `reconcile_source_partition`'s arms). Only these sources need the
/// facts-refresh reconcile gates; the rest reconcile to a no-op.
fn reconciles_cross_file(filter: SourceFilter) -> bool {
    matches!(
        filter,
        SourceFilter::Claude
            | SourceFilter::Codex
            | SourceFilter::Cursor
            | SourceFilter::Copilot
            | SourceFilter::Opencode
    )
}

/// Facts-backed refresh for sources without cross-file reconciliation: hits
/// reuse their fact rows with no blob decode, and only changed files parse.
/// Any error returns `Err` so the caller
/// falls back to the legacy path, which reproduces today's exact behavior.
/// Returns sorted events plus the discovery fingerprint for the snapshot entry.
pub(crate) fn refresh_partition_from_facts(
    filter: SourceFilter,
    cache: &mut UsageCache,
    stored_warnings: Vec<String>,
    warnings: &mut Vec<String>,
) -> Result<(Vec<UsageEvent>, FileFingerprint)> {
    let name = filter.as_str();
    let spec = source_spec(filter);
    let files = source_files(filter);
    let parents = (filter == SourceFilter::Codex)
        .then(|| crate::sources::codex::UsageParentIndex::new(&files));
    let now_ms = epoch_ms_now();
    // Stat every file; messages mirror the scan loop exactly.
    let mut examined: Vec<(PathBuf, (u64, i64))> = Vec::with_capacity(files.len());
    for path in &files {
        match usage_file_metadata(path) {
            Ok(metadata) => examined.push((path.clone(), metadata)),
            Err(error) => warnings.push(format!(
                "{name} usage file skipped ({}): {error:#}",
                path.display()
            )),
        }
    }
    let rows = cache
        .load_source_meta(name, spec.parser_version)
        .map_err(|_| anyhow::anyhow!("{name} usage cache read failed"))?;
    let mut dep_observations: HashMap<Vec<u8>, (u64, i64, bool)> = HashMap::new();
    let mut live = HashSet::with_capacity(examined.len());
    let mut missing = Vec::new();
    let mut hit_paths = HashSet::new();
    for (index, (path, metadata)) in examined.iter().enumerate() {
        let key = path.to_string_lossy().to_string();
        let hit = match rows.get(&key) {
            Some(row) => {
                let metadata_current = (spec.volatile_reuse_ms)(path).map_or_else(
                    || (row.size, row.mtime_ns) == *metadata,
                    |window| now_ms.saturating_sub(row.scanned_at_ms) < window,
                );
                metadata_current
                    && deps_observed_current(&row.deps, &mut dep_observations)
                    && (filter != SourceFilter::Codex
                        || parents.as_ref().is_some_and(|parents| {
                            parents.deps_match_current_candidates(&row.deps)
                        }))
            }
            None => false,
        };
        live.insert(key.clone());
        if hit {
            hit_paths.insert(key);
        } else {
            missing.push((index, path.clone(), *metadata));
        }
    }
    let stale: Vec<String> = rows
        .keys()
        .filter(|key| !live.contains(*key))
        .cloned()
        .collect();
    if !stale.is_empty() {
        cache.delete_stale(name, &stale)?;
    }
    // Hits' events come from facts (no blob decode); changed and stale rows are
    // excluded by the hit set. Vanished files simply have no rows to read.
    let ordinal = source_ordinal(filter) as i64;
    let mut events: Vec<UsageEvent> = read_ordinal_run(&cache.connection, ordinal, None, None)
        .map_err(|_| anyhow::anyhow!("{name} facts read failed"))?
        .into_iter()
        .filter(|row| hit_paths.contains(&row.path))
        .map(FactRow::into_event)
        .collect();
    // Parse what changed, with chunked saves mirroring the scan loop.
    let parse = |path: &Path| parse_source_file(filter, path, parents.as_ref());
    let mut parsed_paths: HashSet<String> = HashSet::new();
    let mut parsed_events: Vec<UsageEvent> = Vec::new();
    if !missing.is_empty() {
        publish_scan_progress(Some(UsageScanProgress {
            source: name,
            done: 0,
            total: missing.len(),
        }));
    }
    let mut save_warned = false;
    let parse_start = Instant::now();
    let missing_count = missing.len();
    for chunk in missing.chunks(PARSE_SAVE_CHUNK) {
        let parsed = parse_missing_usage_files(name, chunk, warnings, &parse);
        if parsed.iter().any(|file| file.cacheable)
            && let Err(error) = cache.save_batch(name, spec.parser_version, now_ms, &parsed)
            && !save_warned
        {
            save_warned = true;
            warnings.push(format!("{name} usage cache write failed: {error:#}"));
        }
        for file in parsed {
            parsed_paths.insert(file.path.to_string_lossy().to_string());
            parsed_events.extend(file.events);
        }
    }
    // Files that failed to parse contribute no events — like the scan loop's
    // empty slot — so they join the removed set below instead of lingering as
    // stale fact rows. (The legacy path likewise serves nothing for them while
    // keeping the blob row for a later retry.)
    for (_, path, _) in &missing {
        let key = path.to_string_lossy().to_string();
        if !parsed_paths.contains(&key) {
            parsed_paths.insert(key);
        }
    }
    if missing_count > 0 {
        usage_timing(parse_start, || {
            format!("{name} parse ({missing_count} changed files)")
        });
    }
    publish_scan_progress(None);
    events.extend(parsed_events);
    let sort_start = Instant::now();
    sort_usage_events(&mut events);
    usage_timing(sort_start, || {
        format!("{} sort ({} events)", filter.as_str(), events.len())
    });
    // Rewrite facts for changed, vanished, and skipped (unstatable) files; hits
    // keep their rows. The fingerprint covers exactly the statted files,
    // matching what a future check recomputes.
    let mut fingerprint: FileFingerprint = examined
        .iter()
        .map(|(path, (size, mtime_ns))| (path.to_string_lossy().to_string(), *size, *mtime_ns))
        .collect();
    fingerprint.sort();
    let mut removed: HashSet<String> = parsed_paths.into_iter().collect();
    removed.extend(stale);
    for path in &files {
        let key = path.to_string_lossy().to_string();
        if !live.contains(&key) {
            // Discovered but unstatable: warned above, dropped like stale rows.
            removed.insert(key);
        }
    }
    let mut changed: Vec<UsageEvent> = Vec::new();
    for event in &events {
        let path: &str = event.source_path.as_ref();
        if removed.contains(path) {
            changed.push(event.clone());
        }
    }
    // Stored warnings describe unchanged files; new warnings describe this
    // refresh's parses. Union them deduplicated (see write_fact_sync).
    let mut stored = stored_warnings;
    for warning in warnings.iter() {
        if !stored.contains(warning) {
            stored.push(warning.clone());
        }
    }
    let upsert_start = Instant::now();
    if !removed.is_empty() {
        cache
            .upsert_file_facts(
                filter,
                &removed.into_iter().collect::<Vec<_>>(),
                &changed,
                &fingerprint,
                &stored,
            )
            .map_err(|_| anyhow::anyhow!("{name} facts write failed"))?;
    }
    usage_timing(upsert_start, || {
        format!(
            "{} facts upsert ({} events)",
            filter.as_str(),
            changed.len()
        )
    });
    *warnings = stored;
    Ok((events, fingerprint))
}

/// Returns the fresh merged view over all source partitions, rebuilding the order
/// only when some partition changed. Combined queries reference the same per-source
/// snapshots instead of requiring a separate full assembly.
pub(crate) fn ensure_merged(cache_path: Option<PathBuf>, ttl: Duration) -> MergedSnapshot {
    loop {
        let stale = {
            let store = lock_merged();
            match store.get(&cache_path) {
                Some(entry) if entry.checked_at.elapsed() < ttl => {
                    return clone_merged(&entry.snapshot);
                }
                Some(entry) => Some(clone_merged(&entry.snapshot)),
                None => None,
            }
        };
        match USAGE_SCAN_LOCK.try_lock() {
            Ok(_refresh) => {
                if let Some(entry) = lock_merged().get(&cache_path)
                    && entry.checked_at.elapsed() < ttl
                {
                    return clone_merged(&entry.snapshot);
                }
                return refresh_merged(&cache_path);
            }
            Err(_) => {
                if let Some(stale) = stale {
                    return stale;
                }
                let _wait = USAGE_SCAN_LOCK
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
    }
}

pub(crate) fn clone_merged(snapshot: &MergedSnapshot) -> MergedSnapshot {
    MergedSnapshot {
        parts: snapshot.parts.clone(),
        order: snapshot.order.clone(),
        warnings: snapshot.warnings.clone(),
    }
}

/// Brings every partition up to date, then reuses or rebuilds the merged order.
/// Callers must hold `USAGE_SCAN_LOCK`; partition refreshes reuse the same lock
/// instead of their own try-lock, so this never deadlocks.
pub(crate) fn refresh_merged(cache_path: &Option<PathBuf>) -> MergedSnapshot {
    let mut parts = Vec::with_capacity(SCANNERS.len());
    let mut warnings = Vec::new();
    for (filter, _) in SCANNERS {
        let (assembly, partition_warnings) = refresh_partition(&(filter, cache_path.clone()));
        warnings.extend(partition_warnings.iter().cloned());
        parts.push(assembly);
    }
    let current: Vec<Arc<UsageAssembly>> = parts;
    let mut store = lock_merged();
    if let Some(entry) = store.get_mut(cache_path)
        && entry.snapshot.parts.len() == current.len()
        && entry
            .snapshot
            .parts
            .iter()
            .zip(current.iter())
            .all(|(old, new)| Arc::ptr_eq(old, new))
    {
        entry.checked_at = Instant::now();
        return clone_merged(&entry.snapshot);
    }
    let order = Arc::new(build_merged_order(&current));
    let snapshot = MergedSnapshot {
        parts: current,
        order,
        warnings: Arc::new(warnings),
    };
    if store.len() >= MAX_MERGED {
        evict_oldest(&mut store, cache_path, |entry| entry.checked_at);
    }
    store.insert(
        cache_path.clone(),
        MergedEntry {
            checked_at: Instant::now(),
            snapshot: clone_merged(&snapshot),
        },
    );
    snapshot
}

/// Evicts the least-recently-checked entry, never the key being published.
pub(crate) fn evict_oldest<K, V>(
    store: &mut HashMap<K, V>,
    keep: &K,
    checked_at: impl Fn(&V) -> Instant,
) where
    K: Clone + Eq + std::hash::Hash,
{
    let oldest = store
        .iter()
        .filter(|(key, _)| *key != keep)
        .min_by_key(|(_, entry)| checked_at(entry))
        .map(|(key, _)| key.clone());
    if let Some(key) = oldest {
        store.remove(&key);
    }
}

/// Partition freshness without decoding payloads. With a disk cache, the facts
/// sync fingerprint decides for plain log files (a new/removed/extended file, or
/// a parser bump, changes it), while volatile databases additionally consult
/// their rows' reuse windows exactly like the scan does. New parent copies and
/// vanished files change the discovered set, so fork-set completeness and stale
/// rows fall out of the fingerprint with no dependency bookkeeping here. Without
/// a disk cache, compares the discovery fingerprint against the stored one. Any
/// I/O hiccup (or a missing sync row, e.g. a pre-facts database) returns false
/// so the refresh reproduces today's exact behavior instead of reusing on
/// uncertain ground.
/// `previous_fingerprint` binds a retained snapshot to its own generation:
/// `Some` requires the current discovery set to match what the snapshot was
/// built from (stable-compared, so volatile mtimes don't churn), because
/// another process may have advanced the facts past what the snapshot
/// describes — disk-vs-facts agreement alone would then reuse stale data.
/// `None` (cold checks with no snapshot) skips the binding: there is nothing
/// to bind, only disk-vs-facts agreement matters.
pub(crate) fn check_partition_valid(
    filter: SourceFilter,
    cache_path: Option<&Path>,
    previous_fingerprint: Option<&[(String, u64, i64)]>,
) -> bool {
    valid_partition_fingerprint(filter, cache_path, previous_fingerprint).is_some()
}

pub(crate) fn valid_partition_fingerprint(
    filter: SourceFilter,
    cache_path: Option<&Path>,
    previous_fingerprint: Option<&[(String, u64, i64)]>,
) -> Option<FileFingerprint> {
    let observed = validate_partition(filter, cache_path, previous_fingerprint)?;
    observed.valid.then_some(observed.fingerprint)
}

pub(crate) struct PartitionValidation {
    pub(crate) fingerprint: FileFingerprint,
    pub(crate) valid: bool,
}

/// A complete observation remains useful as a rebuild checkpoint when invalid.
pub(crate) fn validate_partition(
    filter: SourceFilter,
    cache_path: Option<&Path>,
    previous_fingerprint: Option<&[(String, u64, i64)]>,
) -> Option<PartitionValidation> {
    let files = source_files(filter);
    let mut fingerprint = Vec::with_capacity(files.len());
    let mut observed_metadata = Vec::with_capacity(files.len());
    for path in &files {
        let Ok(metadata) = usage_file_metadata(path) else {
            return None;
        };
        fingerprint.push((path.to_string_lossy().to_string(), metadata.0, metadata.1));
        observed_metadata.push(metadata);
    }
    append_source_context(filter, &mut fingerprint);
    fingerprint.sort();
    let valid = (|| {
        let Some(cache_path) = cache_path else {
            // Without recorded database dependencies, re-read after the memo TTL.
            if matches!(
                filter,
                SourceFilter::Hermes | SourceFilter::Cursor | SourceFilter::Opencode
            ) {
                return None;
            }
            return previous_fingerprint
                .is_some_and(|previous| fingerprint == previous)
                .then_some(());
        };
        if let Some(previous) = previous_fingerprint
            && stable_triples(filter, &fingerprint) != stable_triples(filter, previous)
        {
            return None;
        }
        let spec = source_spec(filter);
        let cache = match UsageCache::open(cache_path) {
            Ok(cache) => cache,
            Err(_) => return None,
        };
        let expected = fingerprint_files(&stable_triples(filter, &fingerprint));
        let allow_uncached_codex = match cache.fact_sync(filter.as_str()) {
            Ok(Some((recorded, version, warnings))) => {
                if recorded != expected || version != spec.parser_version {
                    return None;
                }
                // Unresolved forks deliberately have no blob row. They can reuse
                // facts while still unresolved, but must retry parent resolution
                // below: an unreadable parent's permissions may have recovered
                // without changing its size or mtime.
                filter == SourceFilter::Codex && warnings.is_empty()
            }
            _ => return None,
        };
        // Validate the metadata that actually produced the facts. The discovery
        // checkpoint alone misses WAL dependencies and files changed mid-scan.
        // Missing rows otherwise require a retry rather than making a failed parse
        // authoritative indefinitely.
        let rows = match cache.load_source_meta(filter.as_str(), spec.parser_version) {
            Ok(rows) => rows,
            Err(_) => return None,
        };
        let now = epoch_ms_now();
        let mut observations = HashMap::new();
        let parents = (filter == SourceFilter::Codex)
            .then(|| crate::sources::codex::UsageParentIndex::new(&files));
        for (path, metadata) in files.iter().zip(observed_metadata) {
            let key = path.to_string_lossy();
            let Some(row) = rows.get(key.as_ref()) else {
                if allow_uncached_codex
                    && parse_source_file(filter, path, parents.as_ref())
                        .is_ok_and(|parsed| !parsed.cacheable)
                {
                    continue;
                }
                return None;
            };
            let metadata_current = (spec.volatile_reuse_ms)(path).map_or_else(
                || metadata == (row.size, row.mtime_ns),
                |window| now.saturating_sub(row.scanned_at_ms) < window,
            );
            if !metadata_current
                || !deps_observed_current(&row.deps, &mut observations)
                || parents
                    .as_ref()
                    .is_some_and(|parents| !parents.deps_match_current_candidates(&row.deps))
            {
                return None;
            }
        }
        Some(())
    })()
    .is_some();
    Some(PartitionValidation { fingerprint, valid })
}

/// Current disk facts do not validate an older retained assembly. Compare the
/// identity of the facts this snapshot actually loaded as well as its inputs.
fn check_snapshot_valid(
    filter: SourceFilter,
    cache_path: Option<&Path>,
    fingerprint: &FileFingerprint,
    generation: Option<&str>,
) -> bool {
    if !check_partition_valid(filter, cache_path, Some(fingerprint)) {
        return false;
    }
    let Some(path) = cache_path else {
        return true;
    };
    generation.is_some()
        && UsageCache::open(path)
            .and_then(|cache| cache.fact_generation(filter.as_str()))
            .is_ok_and(|current| current.as_deref() == generation)
}

/// Cursor attribution comes from transcripts independently of the usage DB.
/// Checkpoint the mapping so additions/removals invalidate retained and cold
/// facts without forcing an unchanged database to be parsed again.
fn append_source_context(filter: SourceFilter, fingerprint: &mut FileFingerprint) {
    if filter == SourceFilter::Cursor {
        let mut projects: Vec<_> = crate::sources::cursor::project_by_session()
            .into_iter()
            .collect();
        projects.sort();
        for (session, project) in projects {
            let identity =
                serde_json::to_string(&(session, project)).expect("string pair serializes");
            fingerprint.push((format!("\0cursor-project:{identity}"), 0, 0));
        }
    }
}

/// Current discovery fingerprint for one source, sorted by path.
pub(crate) fn discovery_fingerprint(filter: SourceFilter) -> FileFingerprint {
    let start = Instant::now();
    let mut fingerprint = Vec::new();
    for path in source_files(filter) {
        if let Ok(metadata) = usage_file_metadata(&path) {
            fingerprint.push((path.to_string_lossy().to_string(), metadata.0, metadata.1));
        }
    }
    append_source_context(filter, &mut fingerprint);
    fingerprint.sort();
    usage_timing(start, || {
        format!("{} discovery fingerprint", filter.as_str())
    });
    fingerprint
}

/// Preserve stable event ordering without the full event-sized scratch allocation
/// used by a parallel merge sort. Sorting indices also avoids repeatedly moving the
/// large owned records. Original positions break equal-key ties exactly as before.
pub(crate) fn sort_usage_events(events: &mut [UsageEvent]) {
    let keys_start = Instant::now();
    // Most comparisons are decided by timestamp. Keep it beside the index so
    // sorting does not repeatedly fetch large, scattered event records.
    let mut order: Vec<(u64, usize)> = events
        .iter()
        .enumerate()
        .map(|(index, event)| (event.timestamp_ms, index))
        .collect();
    usage_timing(keys_start, || "sort key extraction".to_string());
    let ordering_start = Instant::now();
    order.par_sort_unstable_by(|&(left_time, left), &(right_time, right)| {
        let by_time = left_time.cmp(&right_time);
        if !by_time.is_eq() {
            return by_time;
        }
        let a = &events[left];
        let b = &events[right];
        (&a.source_path, a.source_order)
            .cmp(&(&b.source_path, b.source_order))
            .then_with(|| left.cmp(&right))
    });
    usage_timing(ordering_start, || "sort index ordering".to_string());
    let permutation_start = Instant::now();
    // Each entry maps a destination to its original position. Rotate each cycle
    // through one hole: swapping would copy these large records three times.
    let base = events.as_mut_ptr();
    for start in 0..order.len() {
        if order[start].1 == start {
            continue;
        }
        let mut current = start;
        // SAFETY: sorting preserves the permutation of 0..events.len(), so every
        // index is in bounds and each unvisited cycle returns to its start.
        // Read the start into a temporary, move each successor into the hole,
        // then fill the final hole with the saved record. Distinct cycle indices
        // do not overlap. There are no allocations, callbacks, or panicking
        // operations while the hole exists, and every record is owned once again
        // before leaving this block. Visited cycles become fixed points.
        unsafe {
            let saved = base.add(start).read();
            loop {
                let position = &mut order.get_unchecked_mut(current).1;
                let next = *position;
                *position = current;
                if next == start {
                    base.add(current).write(saved);
                    break;
                }
                std::ptr::copy_nonoverlapping(base.add(next), base.add(current), 1);
                current = next;
            }
        }
    }
    usage_timing(permutation_start, || "sort event permutation".to_string());
}

#[cfg(test)]
mod sort_tests {
    use super::*;

    #[test]
    fn event_rotation_preserves_ownership_for_every_small_permutation() {
        fn check(order: &[usize]) {
            let path: Arc<str> = Arc::from("shared");
            let mut events: Vec<_> = (0..order.len())
                .map(|index| {
                    let mut event = super::super::cache_event(
                        &format!("session-{index}"),
                        0,
                        "model",
                        index as u64,
                        0,
                        0,
                    );
                    event.source_path = path.clone();
                    event.message_id = Some(format!("owned-{index}"));
                    event
                })
                .collect();
            for (rank, &index) in order.iter().enumerate() {
                events[index].timestamp_ms = rank as u64;
            }
            let expected: Vec<_> = order.iter().map(|&index| events[index].clone()).collect();
            sort_usage_events(&mut events);
            assert_eq!(
                serde_json::to_value(&events).unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
            drop(events);
            drop(expected);
            assert_eq!(Arc::strong_count(&path), 1);
        }
        fn permutations(order: &mut [usize], start: usize) {
            if start == order.len() {
                check(order);
                return;
            }
            for index in start..order.len() {
                order.swap(start, index);
                permutations(order, start + 1);
                order.swap(start, index);
            }
        }
        // Includes empty/singleton inputs, fixed points, swaps, long cycles,
        // and multiple disjoint cycles, with owned strings and shared paths.
        for len in 0..=7 {
            permutations(&mut (0..len).collect::<Vec<_>>(), 0);
        }
    }

    #[test]
    fn timestamp_index_sort_matches_stable_full_event_sort() {
        let mut events: Vec<_> = (0..513)
            .map(|index| {
                let mut event =
                    super::super::cache_event("session", index % 7, "model", index, 0, 0);
                event.timestamp_ms = if index % 19 == 0 { u64::MAX } else { index % 7 };
                event.source_path = Arc::from(["/é", "/a", ""][index as usize % 3]);
                event.source_order = index % 2;
                event
            })
            .collect();
        let mut expected = events.clone();
        expected.sort_by(|a, b| {
            (a.timestamp_ms, &a.source_path, a.source_order).cmp(&(
                b.timestamp_ms,
                &b.source_path,
                b.source_order,
            ))
        });
        sort_usage_events(&mut events);
        assert_eq!(
            serde_json::to_value(&events).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        sort_usage_events(&mut events);
        assert_eq!(
            serde_json::to_value(&events).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        sort_usage_events(&mut []);
    }
}

/// Refresh/publication lock. Held only while revalidating or rebuilding a snapshot —
/// never across filtering, aggregation, or visitor callbacks. Concurrent queries
/// clone snapshot `Arc`s under the short store lock and run lock-free.
pub(crate) static USAGE_SCAN_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

pub(crate) fn epoch_ms_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
