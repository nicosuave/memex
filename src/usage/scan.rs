//! Source file discovery, cached scanning, and usage parsers dispatch.
//!
//! Each source scans its files through the per-file cache (unchanged files
//! decode from cached blobs, changed files re-parse), then reconciles its own
//! partition. Freshness specs and file listings live here so scans and
//! snapshot checks agree on what defines a source.

use super::cache::{CachedUsageEvent, UsageCache};
use super::progress::{UsageScanProgress, bump_scan_progress, publish_scan_progress};
use super::snapshot::epoch_ms_now;
use super::{UsageEvent, usage_timing};
use crate::types::SourceFilter;
use anyhow::Result;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

/// Reuse cached Cursor state databases this long even when their metadata changed: a
/// running Cursor rewrites its (potentially multi-GB) databases continuously, and
/// re-reading them on every scan makes live scans unusable.
/// Expiry forces a read even when main-file metadata is unchanged: committed
/// SQLite writes can remain entirely in the WAL until checkpoint.
pub(crate) const VOLATILE_DB_REUSE_MS: i64 = 60_000;
/// Cache rows are persisted after every chunk of parsed files, not once per source, so an
/// interrupted cold scan resumes from the last completed chunk instead of starting over.
pub(crate) const PARSE_SAVE_CHUNK: usize = 128;

pub(crate) type UsageFileDep = crate::sources::UsageDependency;
pub(crate) type FileParse = crate::sources::UsageParseOutput;

/// One parsed file: events plus the cache row that describes them.
pub(crate) struct ParsedUsageFile {
    pub(crate) index: usize,
    pub(crate) path: PathBuf,
    pub(crate) size: u64,
    pub(crate) mtime_ns: i64,
    pub(crate) events: Vec<UsageEvent>,
    pub(crate) cacheable: bool,
    pub(crate) deps: Vec<UsageFileDep>,
}

#[derive(Clone, Copy)]
pub(crate) struct SourceScan {
    pub(crate) source: &'static str,
    pub(crate) parser_version: i64,
    /// Returns how long cached rows for this path may be reused even if the file metadata
    /// changed, or `None` to always re-parse on change. Used for databases that are
    /// continuously rewritten while their application runs; plain log files must return
    /// `None` so appends are picked up immediately.
    pub(crate) volatile_reuse_ms: fn(&Path) -> Option<i64>,
}

/// Per-source inventory shared by scanning and freshness checks, so both agree on
/// which files, parser version, and volatility rule define a source.
pub(crate) struct SourceSpec {
    pub(crate) parser_version: i64,
    pub(crate) volatile_reuse_ms: fn(&Path) -> Option<i64>,
}

pub(crate) fn no_volatile_reuse(_path: &Path) -> Option<i64> {
    None
}

/// Only the databases are volatile; message JSON files are updated in place
/// while a response streams and must re-parse as soon as they change.
fn volatile_reuse_opencode(path: &Path) -> Option<i64> {
    (path.extension().and_then(|value| value.to_str()) == Some("db"))
        .then_some(VOLATILE_DB_REUSE_MS)
}

fn volatile_reuse_cursor(_path: &Path) -> Option<i64> {
    Some(VOLATILE_DB_REUSE_MS)
}

pub(crate) fn source_spec(filter: SourceFilter) -> SourceSpec {
    match filter {
        SourceFilter::Claude => SourceSpec {
            parser_version: crate::sources::claude::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        SourceFilter::Codex => SourceSpec {
            parser_version: crate::sources::codex::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        SourceFilter::Opencode => SourceSpec {
            parser_version: crate::sources::opencode::VERSIONS.usage,
            volatile_reuse_ms: volatile_reuse_opencode,
        },
        SourceFilter::Pi => SourceSpec {
            parser_version: crate::sources::pi::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        SourceFilter::Omp => SourceSpec {
            parser_version: crate::sources::omp::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        SourceFilter::OpenClaw => SourceSpec {
            parser_version: crate::sources::openclaw::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        SourceFilter::Cursor => SourceSpec {
            parser_version: crate::sources::cursor::VERSIONS.usage,
            volatile_reuse_ms: volatile_reuse_cursor,
        },
        SourceFilter::Copilot => SourceSpec {
            parser_version: crate::sources::copilot::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        SourceFilter::Grok => SourceSpec {
            parser_version: crate::sources::grok::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        SourceFilter::Hermes => SourceSpec {
            parser_version: crate::sources::hermes::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        SourceFilter::Jcode => SourceSpec {
            parser_version: crate::sources::jcode::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        SourceFilter::Muse => SourceSpec {
            parser_version: crate::sources::muse::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        SourceFilter::Antigravity => SourceSpec {
            parser_version: crate::sources::antigravity::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
    }
}

/// The file listing each source scans. Freshness checks use the same listing so a
/// new, removed, or replaced file invalidates exactly when a rescan would reparse.
pub(crate) fn source_files(filter: SourceFilter) -> Vec<PathBuf> {
    match filter {
        SourceFilter::Claude => crate::sources::claude::usage_files(),
        SourceFilter::Codex => crate::sources::codex::discover_rollouts(None)
            .into_iter()
            .map(|file| file.path)
            .collect(),
        SourceFilter::Opencode => crate::sources::opencode::usage_files(),
        SourceFilter::Pi => crate::sources::pi::discover(None)
            .into_iter()
            .map(|file| file.path)
            .collect(),
        SourceFilter::Omp => crate::sources::omp::discover(None)
            .into_iter()
            .map(|file| file.path)
            .collect(),
        SourceFilter::OpenClaw => crate::sources::openclaw::discover()
            .into_iter()
            .map(|file| file.path)
            .collect(),
        SourceFilter::Cursor => crate::sources::cursor::usage_databases(),
        SourceFilter::Copilot => crate::sources::copilot::usage_files(),
        SourceFilter::Grok => crate::sources::grok::discover_sessions()
            .into_iter()
            .map(|file| file.path)
            .collect(),
        SourceFilter::Hermes => crate::sources::hermes::discover()
            .into_iter()
            .map(|file| file.path)
            .collect(),
        SourceFilter::Jcode => crate::sources::jcode::usage_files(),
        SourceFilter::Muse => crate::sources::muse::usage_files(),
        SourceFilter::Antigravity => crate::sources::antigravity::usage_files(),
    }
}

/// Ordinal of a source in scanner layout order. Stored per fact row so combined
/// reads reproduce the merged order exactly.
pub(crate) fn source_ordinal(filter: SourceFilter) -> usize {
    SCANNERS
        .iter()
        .position(|(candidate, _)| *candidate == filter)
        .expect("scanner for every source filter")
}

/// Sorted file identity triples for freshness fingerprints.
pub(crate) type FileFingerprint = Vec<(String, u64, i64)>;

/// Stable triples for freshness: full identity for plain log files, paths only
/// for volatile databases (their bytes are judged by reuse windows, not mtime).
/// Input comes sorted from discovery and stays sorted.
pub(crate) fn stable_triples(
    filter: SourceFilter,
    fingerprint: &[(String, u64, i64)],
) -> FileFingerprint {
    let spec = source_spec(filter);
    fingerprint
        .iter()
        .map(|(path, size, mtime_ns)| {
            if (spec.volatile_reuse_ms)(Path::new(path)).is_some() {
                (path.clone(), 0, 0)
            } else {
                (path.clone(), *size, *mtime_ns)
            }
        })
        .collect()
}

/// FNV-1a 64-bit fingerprint over sorted file triples. Deterministic across
/// builds (unlike the default hasher), so fingerprints persist in SQLite.
pub(crate) fn fingerprint_files(triples: &[(String, u64, i64)]) -> String {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for (path, size, mtime_ns) in triples {
        for byte in path.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(PRIME);
        for word in [*size, *mtime_ns as u64] {
            for byte in word.to_le_bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(PRIME);
            }
        }
    }
    format!("{hash:016x}")
}

/// Scan `files` through the per-file cache: unchanged files are served from cached blobs
/// (decoded in parallel), changed or new files are re-parsed in parallel, and cache rows
/// for vanished files are dropped. Events are appended to `out` in `files` order.
pub(crate) fn scan_files_cached(
    scan: SourceScan,
    files: &[PathBuf],
    cache: Option<&mut UsageCache>,
    warnings: &mut Vec<String>,
    out: &mut Vec<UsageEvent>,
    parse: impl Fn(&Path) -> Result<FileParse> + Sync,
) {
    scan_files_cached_with(scan, files, cache, warnings, out, parse, |_| true);
}

/// Dependency metadata check with one stat per distinct path per scan. Each observation
/// is the live fingerprint, so rows recording different fingerprints for the same path
/// still compare correctly while sharing the single stat.
pub(crate) fn deps_observed_current(
    deps: &[UsageFileDep],
    observations: &mut HashMap<Vec<u8>, (u64, i64, bool)>,
) -> bool {
    deps.iter().all(|dep| {
        let observed = match observations.get(dep.native_path.as_slice()) {
            Some(&cached) => cached,
            None => {
                let fresh = dep.observed();
                observations.insert(dep.native_path.clone(), fresh);
                fresh
            }
        };
        (dep.size, dep.mtime_ns, dep.exists) == observed
    })
}

/// Like `scan_files_cached`, but with a source-specific validity predicate over a cached
/// row's recorded dependencies. `deps_current` runs in addition to each dependency's own
/// metadata check; a source uses it to invalidate cache hits on state that per-file metadata
/// cannot see — e.g. codex forks, whose baseline depends on the *set* of parent rollout
/// copies, so a newly appearing parent copy must invalidate the child even though every
/// already-recorded dependency is still unchanged.
#[allow(clippy::too_many_arguments)]
fn scan_files_cached_with(
    scan: SourceScan,
    files: &[PathBuf],
    cache: Option<&mut UsageCache>,
    warnings: &mut Vec<String>,
    out: &mut Vec<UsageEvent>,
    parse: impl Fn(&Path) -> Result<FileParse> + Sync,
    deps_current: impl Fn(&[UsageFileDep]) -> bool,
) {
    let SourceScan {
        source,
        parser_version,
        volatile_reuse_ms,
    } = scan;
    let now_ms = epoch_ms_now();
    let load_start = Instant::now();
    let mut rows = match cache.as_deref() {
        Some(cache) => match cache.load_source(source, parser_version) {
            Ok(rows) => rows,
            Err(error) => {
                warnings.push(format!("{source} usage cache read failed: {error:#}"));
                HashMap::new()
            }
        },
        None => HashMap::new(),
    };
    usage_timing(load_start, || {
        format!("{source} cache load ({} rows)", rows.len())
    });
    let stat_start = Instant::now();
    let mut slots: Vec<Option<Vec<UsageEvent>>> = (0..files.len()).map(|_| None).collect();
    let mut hits: Vec<(usize, String, Vec<u8>)> = Vec::new();
    let mut missing: Vec<(usize, PathBuf, (u64, i64))> = Vec::new();
    // One observation per distinct dependency path per scan: fork children sharing a
    // parent rollout stat it once instead of once per dependent file.
    let mut dep_observations: HashMap<Vec<u8>, (u64, i64, bool)> = HashMap::new();
    for (index, path) in files.iter().enumerate() {
        let metadata = match usage_file_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) => {
                warnings.push(format!(
                    "{source} usage file skipped ({}): {error:#}",
                    path.display()
                ));
                continue;
            }
        };
        let key = path.to_string_lossy().to_string();
        match rows.remove(&key) {
            // A dependency change (e.g. a fork's parent rollout was extended, or a new parent
            // copy appeared) invalidates the cached result even when the file itself is
            // unchanged, so it must re-parse.
            Some(row)
                if volatile_reuse_ms(path).map_or_else(
                    || (row.size, row.mtime_ns) == metadata,
                    |window| now_ms.saturating_sub(row.scanned_at_ms) < window,
                ) && deps_observed_current(&row.deps, &mut dep_observations)
                    && deps_current(&row.deps) =>
            {
                hits.push((index, key, row.events_blob));
            }
            _ => missing.push((index, path.clone(), metadata)),
        }
    }
    usage_timing(stat_start, || {
        format!("{source} stat ({} files)", files.len())
    });
    let decode_start = Instant::now();
    let hit_count = hits.len();
    let decoded = hits
        .into_par_iter()
        .map(|(index, key, blob)| {
            let source_path: Arc<str> = Arc::from(key.as_str());
            let events = postcard::from_bytes::<Vec<CachedUsageEvent>>(&blob).map(|events| {
                events
                    .into_iter()
                    .map(|event| event.into_event(source, source_path.clone()))
                    .collect::<Vec<_>>()
            });
            (index, key, events)
        })
        .collect::<Vec<_>>();
    for (index, key, events) in decoded {
        match events {
            Ok(events) => slots[index] = Some(events),
            // A corrupt cached blob demotes the file to a fresh parse.
            Err(_) => {
                let path = PathBuf::from(&key);
                match usage_file_metadata(&path) {
                    Ok(metadata) => missing.push((index, path, metadata)),
                    Err(error) => warnings.push(format!(
                        "{source} usage file skipped ({}): {error:#}",
                        path.display()
                    )),
                }
            }
        }
    }
    usage_timing(decode_start, || {
        format!("{source} decode ({hit_count} cached files)")
    });
    let mut cache = cache;
    let stale_paths: Vec<String> = rows.into_keys().collect();
    if let Some(cache) = cache.as_deref_mut()
        && !stale_paths.is_empty()
        && let Err(error) = cache.delete_stale(source, &stale_paths)
    {
        warnings.push(format!("{source} usage cache write failed: {error:#}"));
    }
    if !missing.is_empty() {
        publish_scan_progress(Some(UsageScanProgress {
            source,
            done: 0,
            total: missing.len(),
        }));
    }
    // Parse and persist in chunks so an interrupted cold scan keeps the chunks it finished;
    // the next scan resumes from there instead of re-parsing the whole source.
    let parse_start = Instant::now();
    let missing_count = missing.len();
    let mut save_warned = false;
    for chunk in missing.chunks(PARSE_SAVE_CHUNK) {
        let parsed = parse_missing_usage_files(source, chunk, warnings, &parse);
        // Unresolved-fork parses (cacheable == false) are excluded from persistence so a
        // later scan re-runs them once their fork parent is available; they still populate
        // `out`.
        if let Some(cache) = cache.as_deref_mut()
            && parsed.iter().any(|file| file.cacheable)
            && let Err(error) = cache.save_batch(source, parser_version, now_ms, &parsed)
            && !save_warned
        {
            save_warned = true;
            warnings.push(format!("{source} usage cache write failed: {error:#}"));
        }
        for file in parsed {
            slots[file.index] = Some(file.events);
        }
    }
    if missing_count > 0 {
        usage_timing(parse_start, || {
            format!("{source} parse ({missing_count} changed files)")
        });
    }
    // The decoded lengths are known; avoid repeatedly reallocating the large event buffer.
    out.reserve(slots.iter().flatten().map(Vec::len).sum());
    for events in slots.into_iter().flatten() {
        out.extend(events);
    }
}

pub(crate) fn usage_file_metadata(path: &Path) -> Result<(u64, i64)> {
    let metadata = path.metadata()?;
    let mtime_ns = metadata
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64;
    Ok((metadata.len(), mtime_ns))
}

pub(crate) fn scan_claude(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = source_files(SourceFilter::Claude);
    scan_files_cached(
        SourceScan {
            source: "claude",
            parser_version: crate::sources::claude::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::claude::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

/// Parse one file with its source's parser. Mirrors the parse expressions in the
/// `scan_*` functions (which stay on the legacy combined path); codex forks
/// resolve against `parents`, built from the same discovery as the split.
pub(crate) fn parse_source_file(
    filter: SourceFilter,
    path: &Path,
    parents: Option<&crate::sources::codex::UsageParentIndex>,
) -> Result<FileParse> {
    match filter {
        SourceFilter::Claude => {
            crate::sources::claude::parse_usage_file(path).map(FileParse::cacheable)
        }
        SourceFilter::Codex => crate::sources::codex::parse_usage_file(
            path,
            parents.expect("codex parsing needs its parent index"),
        ),
        SourceFilter::Opencode => {
            crate::sources::opencode::parse_usage_file(path).map(FileParse::cacheable)
        }
        SourceFilter::Pi => crate::sources::pi::parse_usage_file(path).map(FileParse::cacheable),
        SourceFilter::Omp => crate::sources::omp::parse_usage_file(path).map(FileParse::cacheable),
        SourceFilter::OpenClaw => {
            crate::sources::openclaw::parse_usage_file(path).map(FileParse::cacheable)
        }
        SourceFilter::Cursor => {
            crate::sources::cursor::parse_usage_database(path).map(FileParse::cacheable)
        }
        SourceFilter::Copilot => {
            crate::sources::copilot::parse_usage_file(path).map(FileParse::cacheable)
        }
        SourceFilter::Grok => {
            crate::sources::grok::parse_usage_file(path).map(FileParse::cacheable)
        }
        SourceFilter::Hermes => crate::sources::hermes::parse_usage_file(path),
        SourceFilter::Jcode => {
            crate::sources::jcode::parse_usage_file(path).map(FileParse::cacheable)
        }
        SourceFilter::Muse => {
            crate::sources::muse::parse_usage_file(path).map(FileParse::cacheable)
        }
        SourceFilter::Antigravity => {
            crate::sources::antigravity::parse_usage_file(path).map(FileParse::cacheable)
        }
    }
}

pub(crate) fn parse_missing_usage_files(
    source: &str,
    missing: &[(usize, PathBuf, (u64, i64))],
    warnings: &mut Vec<String>,
    parse: &(impl Fn(&Path) -> Result<FileParse> + Sync),
) -> Vec<ParsedUsageFile> {
    let outcomes = missing
        .par_iter()
        .map(|(index, path, metadata)| {
            let outcome = parse(path).map(|parsed| ParsedUsageFile {
                index: *index,
                path: path.clone(),
                size: metadata.0,
                mtime_ns: metadata.1,
                events: parsed.events,
                cacheable: parsed.cacheable,
                deps: parsed.deps,
            });
            bump_scan_progress();
            outcome
        })
        .collect::<Vec<_>>();
    let mut parsed = Vec::with_capacity(outcomes.len());
    for ((_, path, _), outcome) in missing.iter().zip(outcomes) {
        match outcome {
            Ok(file) => parsed.push(file),
            Err(error) => warnings.push(format!(
                "{source} usage file skipped ({}): {error:#}",
                path.display()
            )),
        }
    }
    parsed
}

pub(crate) fn scan_codex(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = source_files(SourceFilter::Codex);
    let parents = crate::sources::codex::UsageParentIndex::new(&files);
    scan_files_cached_with(
        SourceScan {
            source: "codex",
            parser_version: crate::sources::codex::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::codex::parse_usage_file(path, &parents),
        |deps| parents.deps_match_current_candidates(deps),
    );
    Ok(())
}

pub(crate) fn scan_pi(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = source_files(SourceFilter::Pi);
    scan_files_cached(
        SourceScan {
            source: "pi",
            parser_version: crate::sources::pi::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::pi::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

pub(crate) fn scan_omp(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = source_files(SourceFilter::Omp);
    scan_files_cached(
        SourceScan {
            source: "omp",
            parser_version: crate::sources::omp::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::omp::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

pub(crate) fn scan_openclaw(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = source_files(SourceFilter::OpenClaw);
    scan_files_cached(
        SourceScan {
            source: "openclaw",
            parser_version: crate::sources::openclaw::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::openclaw::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

pub(crate) fn scan_opencode(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = source_files(SourceFilter::Opencode);
    scan_files_cached(
        SourceScan {
            source: "opencode",
            parser_version: crate::sources::opencode::VERSIONS.usage,
            volatile_reuse_ms: volatile_reuse_opencode,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::opencode::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

pub(crate) fn scan_cursor(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let databases = source_files(SourceFilter::Cursor);
    let start = out.len();
    scan_files_cached(
        SourceScan {
            source: "cursor",
            parser_version: crate::sources::cursor::VERSIONS.usage,
            volatile_reuse_ms: volatile_reuse_cursor,
        },
        &databases,
        cache,
        warnings,
        out,
        |path| crate::sources::cursor::parse_usage_database(path).map(FileParse::cacheable),
    );
    crate::sources::cursor::apply_projects(
        &mut out[start..],
        &crate::sources::cursor::project_by_session(),
    );
    Ok(())
}

pub(crate) fn scan_copilot(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = source_files(SourceFilter::Copilot);
    scan_files_cached(
        SourceScan {
            source: "copilot",
            parser_version: crate::sources::copilot::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::copilot::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

pub(crate) fn scan_grok(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = source_files(SourceFilter::Grok);
    scan_files_cached(
        SourceScan {
            source: "grok",
            parser_version: crate::sources::grok::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::grok::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

pub(crate) fn scan_hermes(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = source_files(SourceFilter::Hermes);
    scan_files_cached(
        SourceScan {
            source: "hermes",
            parser_version: crate::sources::hermes::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        &files,
        cache,
        warnings,
        out,
        crate::sources::hermes::parse_usage_file,
    );
    Ok(())
}

pub(crate) fn scan_jcode(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = source_files(SourceFilter::Jcode);
    scan_files_cached(
        SourceScan {
            source: "jcode",
            parser_version: crate::sources::jcode::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::jcode::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

pub(crate) fn scan_muse(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = source_files(SourceFilter::Muse);
    scan_files_cached(
        SourceScan {
            source: "muse",
            parser_version: crate::sources::muse::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::muse::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

pub(crate) fn scan_antigravity(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = source_files(SourceFilter::Antigravity);
    scan_files_cached(
        SourceScan {
            source: "antigravity",
            parser_version: crate::sources::antigravity::VERSIONS.usage,
            volatile_reuse_ms: no_volatile_reuse,
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::antigravity::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

pub(crate) type SourceScanner =
    fn(&mut Vec<UsageEvent>, &mut Vec<String>, Option<&mut UsageCache>) -> Result<()>;

/// Scanner ordinals double as merge tiebreaks: partitions are laid out and merged in
/// this order, reproducing the combined assembly's stable sort exactly.
pub(crate) const SCANNERS: [(SourceFilter, SourceScanner); 14] = [
    (SourceFilter::Claude, scan_claude),
    (SourceFilter::Codex, scan_codex),
    (SourceFilter::Opencode, scan_opencode),
    (SourceFilter::Pi, scan_pi),
    (SourceFilter::Omp, scan_omp),
    (SourceFilter::OpenClaw, scan_openclaw),
    (SourceFilter::Cursor, scan_cursor),
    (SourceFilter::Copilot, scan_copilot),
    (SourceFilter::Grok, scan_grok),
    (SourceFilter::Hermes, scan_hermes),
    (SourceFilter::Bob, scan_bob),
    (SourceFilter::Jcode, scan_jcode),
    (SourceFilter::Muse, scan_muse),
    (SourceFilter::Antigravity, scan_antigravity),
];

/// Scan and reconcile one source partition. Shared by combined assembly and
/// per-source snapshot refreshes so both observe identical per-source pipelines.
/// The partition is left unsorted: combined assembly sorts globally, while snapshot
/// refreshes sort the partition (see `refresh_partition`).
pub(crate) fn run_partition_scanner(
    filter: SourceFilter,
    scanner: SourceScanner,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Vec<UsageEvent> {
    let scanner_start = Instant::now();
    // Reconcile each source's partition before combining so Claude's keep
    // bitmap and multi-pass scans cover only its own events, not all history.
    let mut partition = Vec::new();
    let result = scanner(&mut partition, warnings, cache);
    if let Err(error) = result {
        warnings.push(format!("{} scanner: {error:#}", filter.as_str()));
        partition.clear();
    } else {
        let reconcile_start = Instant::now();
        reconcile_source_partition(filter, &mut partition);
        usage_timing(reconcile_start, || {
            format!("{} reconcile ({} events)", filter.as_str(), partition.len())
        });
    }
    usage_timing(scanner_start, || format!("{} scanner", filter.as_str()));
    partition
}

pub(crate) fn reconcile_source_partition(filter: SourceFilter, events: &mut Vec<UsageEvent>) {
    match filter {
        SourceFilter::Claude => crate::sources::claude::reconcile_usage(events),
        SourceFilter::Codex => crate::sources::codex::reconcile_usage(events),
        SourceFilter::Cursor => crate::sources::cursor::reconcile_usage(events),
        SourceFilter::Copilot => crate::sources::copilot::reconcile_usage(events),
        SourceFilter::Opencode => crate::sources::opencode::reconcile_usage(events),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn volatile_sqlite_usage_refreshes_held_open_wal_after_reuse_window() {
        for source in ["opencode", "cursor"] {
            let temp = tempfile::tempdir().unwrap();
            let database = temp.path().join(if source == "opencode" {
                "opencode.db"
            } else {
                "state.vscdb"
            });
            let writer = Connection::open(&database).unwrap();
            writer
                .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
                .unwrap();
            if source == "opencode" {
                writer
                    .execute_batch(
                        "CREATE TABLE message (id TEXT, session_id TEXT, data TEXT);
                     INSERT INTO message VALUES ('m', 's', '{\"tokens\":{\"input\":10}}');",
                    )
                    .unwrap();
            } else {
                writer
                    .execute_batch(
                        "CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY, value TEXT);
                     INSERT INTO cursorDiskKV VALUES ('composerData:s',
                     '{\"generationUUID\":\"g\",\"inputTokens\":10}');",
                    )
                    .unwrap();
            }
            writer
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();
            let metadata = usage_file_metadata(&database).unwrap();
            let mut cache = UsageCache::open(&temp.path().join("usage-cache.sqlite3")).unwrap();
            let run = |cache: &mut UsageCache| {
                let mut warnings = Vec::new();
                let mut events = Vec::new();
                scan_files_cached(
                    SourceScan {
                        source,
                        parser_version: 1,
                        volatile_reuse_ms: |_| Some(VOLATILE_DB_REUSE_MS),
                    },
                    std::slice::from_ref(&database),
                    Some(cache),
                    &mut warnings,
                    &mut events,
                    |path| {
                        if source == "opencode" {
                            crate::sources::opencode::parse_usage_file(path)
                        } else {
                            crate::sources::cursor::parse_usage_database(path)
                        }
                        .map(FileParse::cacheable)
                    },
                );
                assert!(warnings.is_empty(), "{source}: {warnings:?}");
                events
                    .iter()
                    .map(|event| event.tokens.additive_total())
                    .sum::<u64>()
            };
            assert_eq!(run(&mut cache), 10, "{source} cold read");
            if source == "opencode" {
                writer
                    .execute(
                        "UPDATE message SET data = '{\"tokens\":{\"input\":20}}'",
                        [],
                    )
                    .unwrap();
            } else {
                writer.execute("UPDATE cursorDiskKV SET value = '{\"generationUUID\":\"g\",\"inputTokens\":20}'", []).unwrap();
            }
            assert_eq!(usage_file_metadata(&database).unwrap(), metadata);
            assert_eq!(run(&mut cache), 10, "{source} preserves reuse window");
            cache
                .connection
                .execute("UPDATE usage_file_cache SET scanned_at_ms = 0", [])
                .unwrap();
            assert_eq!(run(&mut cache), 20, "{source} refreshes WAL after expiry");
            assert_eq!(usage_file_metadata(&database).unwrap(), metadata);
        }
    }

    #[test]
    fn hermes_wal_changes_invalidate_but_shm_changes_do_not() {
        let temp = tempfile::tempdir().expect("tempdir");
        let db_path = temp.path().join("state.db");
        let conn = Connection::open(&db_path).expect("create db");
        conn.execute_batch(
            "CREATE TABLE sessions (id TEXT, model TEXT, started_at INTEGER, input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER, billing_provider TEXT, estimated_cost_usd REAL, cwd TEXT, git_repo_root TEXT, profile_name TEXT);",
        )
        .expect("create sessions");
        drop(conn);
        let wal = PathBuf::from(format!("{}-wal", db_path.to_string_lossy()));
        let shm = PathBuf::from(format!("{}-shm", db_path.to_string_lossy()));
        fs::write(&wal, "wal-1").expect("write wal");
        fs::write(&shm, "shm-1").expect("write shm");
        let cache_path = temp.path().join("usage-cache.sqlite3");
        let mut cache = UsageCache::open(&cache_path).expect("open cache");
        let mut warnings = Vec::new();
        let mut events = Vec::new();
        let parses = AtomicUsize::new(0);
        let scan =
            |cache: &mut UsageCache, warnings: &mut Vec<String>, events: &mut Vec<UsageEvent>| {
                scan_files_cached(
                    SourceScan {
                        source: "hermes",
                        parser_version: crate::sources::hermes::VERSIONS.usage,
                        volatile_reuse_ms: no_volatile_reuse,
                    },
                    std::slice::from_ref(&db_path),
                    Some(cache),
                    warnings,
                    events,
                    |path| {
                        parses.fetch_add(1, Ordering::SeqCst);
                        crate::sources::hermes::parse_usage_file(path)
                    },
                );
            };
        scan(&mut cache, &mut warnings, &mut events);
        fs::write(&shm, "shm-2").expect("change shm");
        scan(&mut cache, &mut warnings, &mut events);
        assert_eq!(parses.load(Ordering::SeqCst), 1);
        fs::write(&wal, "wal-2").expect("change wal");
        scan(&mut cache, &mut warnings, &mut events);
        assert_eq!(parses.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn cursor_project_mapping_is_recomputed_on_cache_hits() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("state.vscdb");
        let conn = Connection::open(&db_path).expect("create cursor db");
        conn.execute_batch(
            "CREATE TABLE cursorDiskKV (key TEXT PRIMARY KEY, value TEXT); \
             INSERT INTO cursorDiskKV VALUES ('composerData:composer-main', \
             '{\"generationUUID\":\"gen-1\",\"inputTokens\":10,\"outputTokens\":5}');",
        )
        .expect("populate cursor db");
        drop(conn);
        let cache_path = tmp.path().join("usage-cache.sqlite3");
        let files = vec![db_path];
        let run = |project_by_session: &HashMap<String, String>| {
            let mut cache = UsageCache::open(&cache_path).expect("open cache");
            let mut warnings = Vec::new();
            let mut events = Vec::new();
            scan_files_cached(
                SourceScan {
                    source: "cursor",
                    parser_version: crate::sources::cursor::VERSIONS.usage,
                    volatile_reuse_ms: |_| Some(VOLATILE_DB_REUSE_MS),
                },
                &files,
                Some(&mut cache),
                &mut warnings,
                &mut events,
                |path| crate::sources::cursor::parse_usage_database(path).map(FileParse::cacheable),
            );
            assert_eq!(warnings, Vec::<String>::new());
            crate::sources::cursor::apply_projects(&mut events, project_by_session);
            events
        };

        // Cold scan before any transcript is indexed: no attribution.
        let cold = run(&HashMap::new());
        // The database is unchanged, so this scan is served from the cache; a transcript
        // mapping discovered afterwards must still take effect.
        let warm = run(&HashMap::from([(
            "composer-main".to_string(),
            "memex".to_string(),
        )]));

        assert_eq!(cold.len(), 1);
        assert_eq!(cold[0].project, None);
        assert_eq!(warm.len(), 1);
        assert_eq!(warm[0].project.as_deref(), Some("memex"));
    }
    #[test]
    fn claude_file_parse_failures_preserve_successful_files() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let valid = tmp.path().join("valid.jsonl");
        let vanished = tmp.path().join("vanished.jsonl");
        std::fs::write(
            &valid,
            concat!(
                r#"{"type":"assistant","timestamp":1000,"message":{"id":"valid","usage":{"inputTokens":10}}}"#,
                "\n"
            ),
        )
        .expect("write valid transcript");
        std::fs::write(&vanished, "").expect("write disappearing transcript");
        let valid_metadata = usage_file_metadata(&valid).expect("valid metadata");
        let vanished_metadata = usage_file_metadata(&vanished).expect("vanished metadata");
        std::fs::remove_file(&vanished).expect("remove transcript");
        let missing = vec![
            (0, valid, valid_metadata),
            (1, vanished.clone(), vanished_metadata),
        ];
        let mut warnings = Vec::new();

        let parsed =
            parse_missing_usage_files("claude", &missing, &mut warnings, &|path: &Path| {
                crate::sources::claude::parse_usage_file(path).map(FileParse::cacheable)
            });

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].index, 0);
        assert_eq!(parsed[0].events.len(), 1);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains(vanished.to_string_lossy().as_ref()));
    }
}

fn scan_bob(
    out: &mut Vec<UsageEvent>,
    warnings: &mut Vec<String>,
    cache: Option<&mut UsageCache>,
) -> Result<()> {
    let files = crate::sources::bob::usage_files();
    scan_files_cached(
        SourceScan {
            source: "bob",
            parser_version: crate::sources::bob::VERSIONS.usage,
            // WAL commits leave the main file's size and mtime untouched until a
            // checkpoint, so metadata alone would serve stale spend indefinitely.
            volatile_reuse_ms: |_| Some(VOLATILE_DB_REUSE_MS),
        },
        &files,
        cache,
        warnings,
        out,
        |path| crate::sources::bob::parse_usage_file(path).map(FileParse::cacheable),
    );
    Ok(())
}

// Rates are nano-USD per million tokens. The catalog is deliberately small and versioned:
// unknown models remain unpriced instead of silently inheriting a guessed family rate.
const PRICE_CATALOG_ID: &str = "official-api-prices-2026-07-15";

#[derive(Clone, Copy)]
struct Rates {
    input: u64,
    cache_read: u64,
    cache_write_5m: u64,
    cache_write_1h: u64,
    output: u64,
}

const fn usd_per_million(value_milli_usd: u64) -> u64 {
    value_milli_usd * 1_000_000
}

pub(crate) fn event_cost_nanos<S: std::ops::Deref<Target = str>, P>(
    event: &UsageEventData<S, P>,
    mode: CostMode,
) -> Option<u64> {
    let source = event
        .source_cost_usd
        .filter(|value| value.is_finite() && *value >= 0.0)
        .and_then(|value| {
            let nanos = value * 1_000_000_000.0;
            (nanos <= u64::MAX as f64).then_some(nanos.round() as u64)
        });
    match mode {
        CostMode::Source => source,
        CostMode::Auto => source.or_else(|| {
            (!event.cost_authoritative)
                .then(|| calculated_cost_nanos(event))
                .flatten()
        }),
        CostMode::Reprice => calculated_cost_nanos(event),
    }
}

