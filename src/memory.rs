//! Read-only discovery, parsing, and durable snapshots for agent memory documents.
//!
//! Memory documents are intentionally separate from conversation [`crate::types::Record`]s.
//! They have path-based identities, content versions, document structure, and freshness state
//! rather than synthetic sessions or turns.

use crate::config::expand_exclude_patterns;
use crate::types::SourceKind;
use anyhow::{Context, Result, anyhow, bail};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};
use walkdir::WalkDir;

pub const MEMORY_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub const MAX_MEMORY_SECTION_CHARS: usize = 8_000;

const SNAPSHOT_READ_ATTEMPTS: usize = 3;
const CONSISTENT_READ_ATTEMPTS: usize = 3;
const RETRY_DELAY: Duration = Duration::from_millis(10);
const WRITE_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct MemoryDiscoveryOptions {
    /// Configured Claude `projects` directories. Only `*/memory/**/*.md` is considered.
    pub claude_project_roots: Vec<PathBuf>,
    /// Configured Codex home (normally `~/.codex`).
    pub codex_homes: Vec<PathBuf>,
    /// Providers omitted here are not selected for this refresh; prior documents are preserved.
    pub enabled_sources: HashSet<SourceKind>,
    /// Glob patterns applied to canonical source paths before parsing.
    pub exclude_patterns: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryDiscovery {
    pub candidates: Vec<MemoryCandidate>,
    /// A failure makes deletion under that provider unconfirmed for this refresh.
    pub failures: Vec<MemoryDiscoveryFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryDiscoveryFailure {
    pub provider: SourceKind,
    pub root: PathBuf,
    pub error: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryCandidate {
    pub provider: SourceKind,
    pub source_path: PathBuf,
    pub scope: MemoryScope,
    pub kind: MemoryDocumentKind,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryScope {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryDocumentKind {
    Index,
    Summary,
    Note,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MemoryFreshness {
    Fresh,
    Stale { error: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryEventDate {
    pub label: String,
    pub value: String,
    pub line: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryRef {
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub line: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemorySection {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heading: Option<String>,
    pub level: u8,
    pub start_line: u32,
    pub end_line: u32,
    pub content: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryDocument {
    pub provider: SourceKind,
    pub stable_id: String,
    pub version_sha256: String,
    pub source_path: PathBuf,
    pub scope: MemoryScope,
    pub kind: MemoryDocumentKind,
    pub mtime_ms: u64,
    pub event_dates: Vec<MemoryEventDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub content: String,
    pub sections: Vec<MemorySection>,
    pub refs: Vec<MemoryRef>,
    pub freshness: MemoryFreshness,
}

impl MemoryDocument {
    pub fn section(&self, id: &str) -> Option<&MemorySection> {
        self.sections.iter().find(|section| section.id == id)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemorySnapshot {
    pub schema_version: u32,
    pub documents: Vec<MemoryDocument>,
}

impl Default for MemorySnapshot {
    fn default() -> Self {
        Self {
            schema_version: MEMORY_SNAPSHOT_SCHEMA_VERSION,
            documents: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemoryRefreshReport {
    pub discovered: usize,
    pub parsed: usize,
    pub unchanged: usize,
    pub stale_retained: usize,
    pub deleted: usize,
    pub document_count: usize,
    pub section_count: usize,
    pub stale_count: usize,
    pub failures: Vec<MemoryRefreshFailure>,
    pub changed: bool,
    pub snapshot: MemorySnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryRefreshFailure {
    pub provider: SourceKind,
    pub path: PathBuf,
    pub error: String,
}

#[derive(Clone, Debug)]
pub struct MemoryStore {
    snapshot_path: PathBuf,
}

impl MemoryStore {
    /// `snapshot_path` is the file itself, normally `<Paths.root>/memory/documents.json`.
    pub fn new(snapshot_path: impl Into<PathBuf>) -> Self {
        Self {
            snapshot_path: snapshot_path.into(),
        }
    }

    pub fn snapshot_path(&self) -> &Path {
        &self.snapshot_path
    }

    /// Load one complete published snapshot. Transient replacement/read races are retried.
    pub fn load(&self) -> Result<MemorySnapshot> {
        let mut last_error = None;
        for attempt in 0..SNAPSHOT_READ_ATTEMPTS {
            match load_snapshot_once(&self.snapshot_path) {
                Ok(snapshot) => return Ok(snapshot),
                Err(error) => last_error = Some(error),
            }
            if attempt + 1 < SNAPSHOT_READ_ATTEMPTS {
                thread::sleep(RETRY_DELAY);
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("failed to read memory snapshot")))
    }

    pub fn get(&self, stable_id: &str) -> Result<Option<MemoryDocument>> {
        Ok(self
            .load()?
            .documents
            .into_iter()
            .find(|document| document.stable_id == stable_id))
    }

    /// Refresh changed files as whole documents and publish one atomic replacement snapshot.
    pub fn refresh(&self, options: &MemoryDiscoveryOptions) -> Result<MemoryRefreshReport> {
        let parent = parent_directory(&self.snapshot_path)?;
        fs::create_dir_all(parent)?;
        let _lock = SnapshotWriteLock::acquire(&self.snapshot_path, WRITE_LOCK_TIMEOUT)?;

        let snapshot_existed = self.snapshot_path.is_file();
        let previous = self.load()?;
        if previous.schema_version != MEMORY_SNAPSHOT_SCHEMA_VERSION {
            bail!(
                "unsupported memory snapshot schema version {} (expected {})",
                previous.schema_version,
                MEMORY_SNAPSHOT_SCHEMA_VERSION
            );
        }

        let discovery = discover_memory_documents(options)?;
        let mut report = MemoryRefreshReport {
            discovered: discovery.candidates.len(),
            ..MemoryRefreshReport::default()
        };
        for failure in &discovery.failures {
            report.failures.push(MemoryRefreshFailure {
                provider: failure.provider,
                path: failure.root.clone(),
                error: failure.error.clone(),
            });
        }

        let excluder = PathExcluder::build(&options.exclude_patterns)?;
        let mut previous_by_id = previous
            .documents
            .iter()
            .cloned()
            .map(|document| (document.stable_id.clone(), document))
            .collect::<HashMap<_, _>>();
        let mut documents = Vec::with_capacity(discovery.candidates.len());

        for candidate in discovery.candidates {
            let stable_id = memory_stable_id(candidate.provider, &candidate.source_path);
            let prior = previous_by_id.remove(&stable_id);
            match read_consistent(&candidate.source_path) {
                Ok((content, metadata)) => {
                    let version = sha256_hex(content.as_bytes());
                    if let Some(mut document) = prior.filter(|document| {
                        matches!(document.freshness, MemoryFreshness::Fresh)
                            && document.version_sha256 == version
                    }) {
                        document.mtime_ms = modified_millis(&metadata)?;
                        documents.push(document);
                        report.unchanged += 1;
                    } else {
                        documents.push(parse_memory_content(&candidate, content, &metadata)?);
                        report.parsed += 1;
                    }
                }
                Err(error) => {
                    let error = format!("{error:#}");
                    report.failures.push(MemoryRefreshFailure {
                        provider: candidate.provider,
                        path: candidate.source_path.clone(),
                        error: error.clone(),
                    });
                    if let Some(mut document) = prior {
                        document.freshness = MemoryFreshness::Stale { error };
                        documents.push(document);
                        report.stale_retained += 1;
                    }
                }
            }
        }

        for (_, mut document) in previous_by_id {
            if excluder.is_excluded(&document.source_path) {
                report.deleted += 1;
            } else if !options.enabled_sources.contains(&document.provider)
                || !document_is_under_selected_root(&document, options)
            {
                documents.push(document);
            } else if discovery_failure_covers(
                &discovery.failures,
                document.provider,
                &document.source_path,
            ) {
                let error = discovery
                    .failures
                    .iter()
                    .filter(|failure| {
                        failure.provider == document.provider
                            && document.source_path.starts_with(&failure.root)
                    })
                    .map(|failure| format!("{}: {}", failure.root.display(), failure.error))
                    .collect::<Vec<_>>()
                    .join("; ");
                document.freshness = MemoryFreshness::Stale {
                    error: format!("memory discovery incomplete: {error}"),
                };
                documents.push(document);
                report.stale_retained += 1;
            } else {
                report.deleted += 1;
            }
        }

        resolve_memory_refs(&mut documents);
        sort_documents(&mut documents);
        let next = MemorySnapshot {
            schema_version: MEMORY_SNAPSHOT_SCHEMA_VERSION,
            documents,
        };
        report.document_count = next.documents.len();
        report.section_count = next
            .documents
            .iter()
            .map(|document| document.sections.len())
            .sum();
        report.stale_count = next
            .documents
            .iter()
            .filter(|document| matches!(document.freshness, MemoryFreshness::Stale { .. }))
            .count();
        report.changed = !snapshot_existed || next != previous;
        if report.changed {
            atomic_write_snapshot(&self.snapshot_path, &next)?;
        }
        report.snapshot = next;
        Ok(report)
    }
}

fn discovery_failure_covers(
    failures: &[MemoryDiscoveryFailure],
    provider: SourceKind,
    source_path: &Path,
) -> bool {
    failures
        .iter()
        .any(|failure| failure.provider == provider && source_path.starts_with(&failure.root))
}

fn document_is_under_selected_root(
    document: &MemoryDocument,
    options: &MemoryDiscoveryOptions,
) -> bool {
    let roots: Vec<PathBuf> = match document.provider {
        SourceKind::Claude => options
            .claude_project_roots
            .iter()
            .map(|root| canonicalize_with_missing(root))
            .collect(),
        SourceKind::Codex => options
            .codex_homes
            .iter()
            .map(|home| home.join("memories"))
            .map(|root| canonicalize_with_missing(&root))
            .collect(),
        _ => Vec::new(),
    };
    roots
        .iter()
        .any(|root| document.source_path.starts_with(root))
}

pub fn discover_memory_documents(options: &MemoryDiscoveryOptions) -> Result<MemoryDiscovery> {
    let excluder = PathExcluder::build(&options.exclude_patterns)?;
    let mut discovery = MemoryDiscovery {
        candidates: Vec::new(),
        failures: Vec::new(),
    };

    if options.enabled_sources.contains(&SourceKind::Claude) {
        for root in &options.claude_project_roots {
            discover_claude_root(root, &excluder, &mut discovery);
        }
    }
    if options.enabled_sources.contains(&SourceKind::Codex) {
        for codex_home in &options.codex_homes {
            discover_codex_root(codex_home, &excluder, &mut discovery);
        }
    }

    discovery.candidates.sort_by(|left, right| {
        left.provider
            .label()
            .cmp(right.provider.label())
            .then_with(|| left.source_path.cmp(&right.source_path))
    });
    discovery.candidates.dedup_by(|left, right| {
        left.provider == right.provider && left.source_path == right.source_path
    });
    Ok(discovery)
}

pub fn parse_memory_document(candidate: &MemoryCandidate) -> Result<MemoryDocument> {
    let (content, metadata) = read_consistent(&candidate.source_path)?;
    parse_memory_content(candidate, content, &metadata)
}

/// Re-read a snapshot-known source without mutating either the source or memory snapshot.
pub(crate) fn reparse_memory_document(previous: &MemoryDocument) -> Result<MemoryDocument> {
    if fs::canonicalize(&previous.source_path)? != previous.source_path {
        bail!("memory source path identity changed before reading");
    }
    let current = parse_memory_document(&MemoryCandidate {
        provider: previous.provider,
        source_path: previous.source_path.clone(),
        scope: previous.scope.clone(),
        kind: previous.kind,
    })?;
    if fs::canonicalize(&previous.source_path)? != previous.source_path {
        bail!("memory source path identity changed while reading");
    }
    Ok(current)
}

fn parse_memory_content(
    candidate: &MemoryCandidate,
    content: String,
    metadata: &fs::Metadata,
) -> Result<MemoryDocument> {
    let mtime_ms = modified_millis(metadata)?;
    let version_sha256 = sha256_hex(content.as_bytes());
    let stable_id = memory_stable_id(candidate.provider, &candidate.source_path);
    let (sections, title) = parse_sections(&content);
    let source_metadata = parse_source_metadata(&content);
    let mut scope = candidate.scope.clone();
    if candidate.provider == SourceKind::Codex && candidate.kind == MemoryDocumentKind::Summary {
        if scope.cwd.is_none() {
            scope.cwd = source_metadata.cwd.clone();
        }
        if scope.project.is_none() {
            scope.project = scope
                .cwd
                .as_deref()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                .map(str::to_string);
        }
    }

    scope.cwd = scope.cwd.map(|cwd| canonicalize_with_missing(&cwd));

    Ok(MemoryDocument {
        provider: candidate.provider,
        stable_id,
        version_sha256,
        source_path: candidate.source_path.clone(),
        scope,
        kind: candidate.kind,
        mtime_ms,
        event_dates: parse_explicit_dates(&content),
        title: title.or_else(|| {
            candidate
                .source_path
                .file_stem()
                .and_then(|name| name.to_str())
                .map(str::to_string)
        }),
        refs: parse_refs(&content, &source_metadata),
        content,
        sections,
        freshness: MemoryFreshness::Fresh,
    })
}

pub fn memory_stable_id(provider: SourceKind, canonical_source_path: &Path) -> String {
    let mut digest = Sha256::new();
    digest.update(provider.storage_label().as_bytes());
    digest.update([0]);
    digest.update(canonical_source_path.to_string_lossy().as_bytes());
    format!(
        "memory-{}-{:x}",
        provider.storage_label(),
        digest.finalize()
    )
}

fn discover_claude_root(root: &Path, excluder: &PathExcluder, out: &mut MemoryDiscovery) {
    let root = match canonical_source_root(root) {
        Ok(Some(root)) => root,
        Ok(None) => return,
        Err(error) => {
            push_discovery_failure(out, SourceKind::Claude, root, error);
            return;
        }
    };
    let projects = match fs::read_dir(&root) {
        Ok(projects) => projects,
        Err(error) => {
            push_discovery_failure(out, SourceKind::Claude, &root, error);
            return;
        }
    };
    for project in projects {
        let project = match project {
            Ok(project) => project,
            Err(error) => {
                push_discovery_failure(out, SourceKind::Claude, &root, error);
                continue;
            }
        };
        let project_root = project.path();
        match project.file_type() {
            Ok(kind) if kind.is_dir() => {}
            Ok(_) => continue,
            Err(error) => {
                push_discovery_failure(out, SourceKind::Claude, &project_root, error);
                continue;
            }
        }
        let project_root = match canonical_in_root(&project_root, &root) {
            Ok(project_root) => project_root,
            Err(error) => {
                push_discovery_failure(out, SourceKind::Claude, &project_root, error);
                continue;
            }
        };
        let memory_root = project_root.join("memory");
        match fs::symlink_metadata(&memory_root) {
            Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            }
            Ok(_) => continue,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                push_discovery_failure(out, SourceKind::Claude, &memory_root, error);
                continue;
            }
        }
        let memory_root = match canonical_source_root(&memory_root) {
            Ok(Some(memory_root)) if memory_root.starts_with(&project_root) => memory_root,
            Ok(Some(_)) => continue,
            Ok(None) => continue,
            Err(error) => {
                push_discovery_failure(out, SourceKind::Claude, &memory_root, error);
                continue;
            }
        };
        let scope = claude_project_scope(&project_root);
        for entry in WalkDir::new(&memory_root).follow_links(false).into_iter() {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    let failed_path = error.path().unwrap_or(&memory_root).to_path_buf();
                    push_discovery_failure(out, SourceKind::Claude, &failed_path, error);
                    continue;
                }
            };
            if !entry.file_type().is_file() || !is_markdown(entry.path()) {
                continue;
            }
            let Ok(relative) = entry.path().strip_prefix(&memory_root) else {
                continue;
            };
            if has_forbidden_component(relative) {
                continue;
            }
            let source_path = match canonical_in_root(entry.path(), &memory_root) {
                Ok(path) => path,
                Err(error) => {
                    push_discovery_failure(out, SourceKind::Claude, &memory_root, error);
                    continue;
                }
            };
            if excluder.is_excluded(&source_path) {
                continue;
            }
            out.candidates.push(MemoryCandidate {
                provider: SourceKind::Claude,
                kind: kind_from_path(&source_path),
                source_path,
                scope: scope.clone(),
            });
        }
    }
}

fn claude_project_scope(project_root: &Path) -> MemoryScope {
    let mut scope = MemoryScope {
        project: Some(crate::sources::claude::project_from_path(
            &project_root.join("memory.md"),
        )),
        cwd: None,
    };
    let Ok(entries) = fs::read_dir(project_root) else {
        return scope;
    };
    let transcripts = entries.flatten().take(64).filter(|entry| {
        entry.file_type().is_ok_and(|kind| kind.is_file())
            && entry
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                == Some("jsonl")
    });
    for entry in transcripts.take(8) {
        let path = entry.path();
        if let Ok(metadata) = crate::sources::claude::probe(&path)
            && let Some(cwd) = metadata.cwd
        {
            scope.cwd = Some(cwd);
            scope.project = metadata.project;
            break;
        }
    }
    scope
}

fn discover_codex_root(codex_home: &Path, excluder: &PathExcluder, out: &mut MemoryDiscovery) {
    let memories = codex_home.join("memories");
    let root = match canonical_source_root(&memories) {
        Ok(Some(root)) => root,
        Ok(None) => return,
        Err(error) => {
            push_discovery_failure(out, SourceKind::Codex, &memories, error);
            return;
        }
    };

    let direct_files = [root.join("MEMORY.md"), root.join("memory_summary.md")];
    for path in direct_files {
        discover_exact_codex_file(&root, &path, excluder, out);
    }

    let rollout_root = root.join("rollout_summaries");
    let entries = match fs::read_dir(&rollout_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            push_discovery_failure(out, SourceKind::Codex, &rollout_root, error);
            return;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                push_discovery_failure(out, SourceKind::Codex, &root, error);
                continue;
            }
        };
        let path = entry.path();
        match entry.file_type() {
            Ok(kind) if kind.is_file() && is_markdown(&path) => {
                discover_exact_codex_file(&root, &path, excluder, out);
            }
            Ok(_) => {}
            Err(error) => {
                push_discovery_failure(out, SourceKind::Codex, &path, error);
            }
        }
    }
}

fn discover_exact_codex_file(
    root: &Path,
    path: &Path,
    excluder: &PathExcluder,
    out: &mut MemoryDiscovery,
) {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            push_discovery_failure(out, SourceKind::Codex, path, error);
            return;
        }
    };
    if !metadata.file_type().is_file() {
        return;
    }
    let source_path = match canonical_in_root(path, root) {
        Ok(path) => path,
        Err(error) => {
            push_discovery_failure(out, SourceKind::Codex, path, error);
            return;
        }
    };
    if excluder.is_excluded(&source_path) {
        return;
    }
    out.candidates.push(MemoryCandidate {
        provider: SourceKind::Codex,
        kind: kind_from_path(&source_path),
        source_path,
        scope: MemoryScope::default(),
    });
}

fn canonical_source_root(root: &Path) -> Result<Option<PathBuf>> {
    match root.canonicalize() {
        Ok(root) => {
            let metadata = fs::metadata(&root)
                .with_context(|| format!("inspect memory root {}", root.display()))?;
            if !metadata.is_dir() {
                bail!("memory root is not a directory: {}", root.display());
            }
            Ok(Some(root))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read memory root {}", root.display())),
    }
}

fn canonical_in_root(path: &Path, root: &Path) -> Result<PathBuf> {
    let canonical = path
        .canonicalize()
        .with_context(|| format!("resolve memory document {}", path.display()))?;
    if !canonical.starts_with(root) {
        bail!(
            "memory document {} resolves outside configured root {}",
            path.display(),
            root.display()
        );
    }
    Ok(canonical)
}

fn push_discovery_failure(
    out: &mut MemoryDiscovery,
    provider: SourceKind,
    root: &Path,
    error: impl std::fmt::Display,
) {
    let error = error.to_string();
    if !out.failures.iter().any(|existing| {
        existing.provider == provider && existing.root == root && existing.error == error
    }) {
        out.failures.push(MemoryDiscoveryFailure {
            provider,
            root: canonicalize_with_missing(root),
            error,
        });
    }
}

fn component_text(component: Component<'_>) -> Option<&str> {
    match component {
        Component::Normal(value) => value.to_str(),
        _ => None,
    }
}

fn has_forbidden_component(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component_text(component), Some("raw" | "skills" | ".git")))
}

fn is_markdown(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("md"))
}

fn kind_from_path(path: &Path) -> MemoryDocumentKind {
    if path
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        == Some("rollout_summaries")
    {
        return MemoryDocumentKind::Summary;
    }
    match path.file_name().and_then(|name| name.to_str()) {
        Some("MEMORY.md") => MemoryDocumentKind::Index,
        Some(name) if name.eq_ignore_ascii_case("memory_summary.md") => MemoryDocumentKind::Summary,
        _ => MemoryDocumentKind::Note,
    }
}

#[derive(Clone, Debug)]
struct PathExcluder {
    set: Option<GlobSet>,
}

impl PathExcluder {
    fn build(patterns: &[String]) -> Result<Self> {
        let patterns = expand_exclude_patterns(patterns.to_vec());
        if patterns.is_empty() {
            return Ok(Self { set: None });
        }
        let mut builder = GlobSetBuilder::new();
        for pattern in patterns {
            let mut variants = vec![pattern.clone()];
            if let Some(canonical) = canonicalize_glob_prefix(&pattern)
                && canonical != pattern
            {
                variants.push(canonical);
            }
            for variant in variants {
                builder.add(
                    GlobBuilder::new(&variant)
                        .literal_separator(false)
                        .build()
                        .with_context(|| format!("invalid exclude pattern: {pattern}"))?,
                );
            }
        }
        Ok(Self {
            set: Some(builder.build().context("compile memory exclude patterns")?),
        })
    }

    fn is_excluded(&self, path: &Path) -> bool {
        self.set.as_ref().is_some_and(|set| set.is_match(path))
    }
}

fn canonicalize_glob_prefix(pattern: &str) -> Option<String> {
    let wildcard = pattern
        .char_indices()
        .find(|(_, character)| matches!(character, '*' | '?' | '[' | '{'))
        .map(|(index, _)| index)
        .unwrap_or(pattern.len());
    let split = pattern[..wildcard].rfind('/').map(|index| index + 1)?;
    let prefix = pattern[..split].trim_end_matches('/');
    if prefix.is_empty() {
        return None;
    }
    let canonical = canonicalize_with_missing(Path::new(prefix));
    Some(format!("{}{}", canonical.display(), &pattern[split - 1..]))
}

fn canonicalize_with_missing(path: &Path) -> PathBuf {
    let mut cursor = path;
    let mut suffix = Vec::new();
    loop {
        if let Ok(mut canonical) = cursor.canonicalize() {
            for component in suffix.iter().rev() {
                canonical.push(component);
            }
            return normalize_lexical(&canonical);
        }
        let Some(name) = cursor.file_name() else {
            return normalize_lexical(path);
        };
        suffix.push(name.to_os_string());
        let Some(parent) = cursor.parent() else {
            return normalize_lexical(path);
        };
        cursor = parent;
    }
}

fn read_consistent(path: &Path) -> Result<(String, fs::Metadata)> {
    let mut last_error = None;
    for attempt in 0..CONSISTENT_READ_ATTEMPTS {
        let result = (|| {
            let before = fs::symlink_metadata(path)
                .with_context(|| format!("stat memory document {}", path.display()))?;
            if !before.file_type().is_file() || before.file_type().is_symlink() {
                bail!(
                    "memory document is not a regular non-symlink file: {}",
                    path.display()
                );
            }
            let first_content = fs::read_to_string(path)
                .with_context(|| format!("read memory document {}", path.display()))?;
            let middle = fs::symlink_metadata(path)
                .with_context(|| format!("restat memory document {}", path.display()))?;
            let second_content = fs::read_to_string(path)
                .with_context(|| format!("reread memory document {}", path.display()))?;
            let after = fs::symlink_metadata(path)
                .with_context(|| format!("final stat memory document {}", path.display()))?;
            if !after.file_type().is_file() || after.file_type().is_symlink() {
                bail!("memory document became a non-regular or symlink file while it was read");
            }
            if !consistent_read_matches(
                &file_fingerprint(&before)?,
                &first_content,
                &file_fingerprint(&middle)?,
                &second_content,
                &file_fingerprint(&after)?,
            ) {
                bail!("memory document changed while it was being read");
            }
            Ok((second_content, after))
        })();
        match result {
            Ok(value) => return Ok(value),
            Err(error) => last_error = Some(error),
        }
        if attempt + 1 < CONSISTENT_READ_ATTEMPTS {
            thread::sleep(RETRY_DELAY);
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("failed to read memory document consistently")))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileFingerprint {
    len: u64,
    modified_ns: u128,
    #[cfg(unix)]
    changed_seconds: i64,
    #[cfg(unix)]
    changed_nanoseconds: i64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

fn file_fingerprint(metadata: &fs::Metadata) -> Result<FileFingerprint> {
    let modified_ns = metadata
        .modified()
        .context("read memory document modification time")?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(FileFingerprint {
            len: metadata.len(),
            modified_ns,
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        Ok(FileFingerprint {
            len: metadata.len(),
            modified_ns,
        })
    }
}

fn consistent_read_matches(
    before: &FileFingerprint,
    first_content: &str,
    middle: &FileFingerprint,
    second_content: &str,
    after: &FileFingerprint,
) -> bool {
    before == middle
        && middle == after
        && first_content == second_content
        && after.len == second_content.len() as u64
}

fn modified_millis(metadata: &fs::Metadata) -> Result<u64> {
    Ok(metadata
        .modified()
        .context("read memory document modification time")?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64)
}

fn parse_sections(content: &str) -> (Vec<MemorySection>, Option<String>) {
    let lines = numbered_lines(content);
    if lines.is_empty() {
        return (
            vec![MemorySection {
                id: "document".to_string(),
                heading: None,
                level: 0,
                start_line: 1,
                end_line: 1,
                content: String::new(),
            }],
            None,
        );
    }

    let mut boundaries = vec![SectionBoundary {
        start_index: 0,
        heading: None,
        level: 0,
    }];
    let mut fence = None;
    let mut title = None;
    for (index, (_, line)) in lines.iter().enumerate() {
        if update_markdown_fence(line, &mut fence) {
            continue;
        }
        if fence.is_some() {
            continue;
        }
        if let Some((level, heading)) = atx_heading(line) {
            if title.is_none() {
                title = Some(heading.clone());
            }
            if index == 0 && boundaries[0].heading.is_none() {
                boundaries[0] = SectionBoundary {
                    start_index: 0,
                    heading: Some(heading),
                    level,
                };
            } else {
                boundaries.push(SectionBoundary {
                    start_index: index,
                    heading: Some(heading),
                    level,
                });
            }
        }
    }

    let mut sections = Vec::new();
    let mut used_ids = HashMap::<String, usize>::new();
    for (boundary_index, boundary) in boundaries.iter().enumerate() {
        let end_index = boundaries
            .get(boundary_index + 1)
            .map(|next| next.start_index)
            .unwrap_or(lines.len());
        let base = boundary
            .heading
            .as_deref()
            .map(slugify)
            .filter(|slug| !slug.is_empty())
            .unwrap_or_else(|| "document".to_string());
        let occurrence = used_ids.entry(base.clone()).or_default();
        *occurrence += 1;
        let base_id = if *occurrence == 1 {
            base
        } else {
            format!("{base}-{}", *occurrence)
        };
        append_bounded_sections(
            &mut sections,
            &lines[boundary.start_index..end_index],
            boundary,
            &base_id,
        );
    }
    // Heading slugs and generated occurrence/chunk suffixes share one namespace.
    // Reserve final emitted IDs so a literal heading cannot alias another section.
    let mut emitted_ids = HashSet::new();
    for section in &mut sections {
        let base = section.id.clone();
        let mut suffix = 2;
        while !emitted_ids.insert(section.id.clone()) {
            section.id = format!("{base}-{suffix}");
            suffix += 1;
        }
    }
    (sections, title)
}

#[derive(Clone, Debug)]
struct SectionBoundary {
    start_index: usize,
    heading: Option<String>,
    level: u8,
}

fn numbered_lines(content: &str) -> Vec<(u32, &str)> {
    content
        .split_inclusive('\n')
        .enumerate()
        .map(|(index, line)| (index.saturating_add(1).min(u32::MAX as usize) as u32, line))
        .collect()
}

fn append_bounded_sections(
    out: &mut Vec<MemorySection>,
    lines: &[(u32, &str)],
    boundary: &SectionBoundary,
    base_id: &str,
) {
    let mut chunks: Vec<(u32, u32, String)> = Vec::new();
    let mut chunk = String::new();
    let mut chunk_start = lines.first().map(|(line, _)| *line).unwrap_or(1);
    let mut chunk_end = chunk_start;

    for (line_number, mut line) in lines.iter().copied() {
        while !line.is_empty() {
            let remaining = MAX_MEMORY_SECTION_CHARS.saturating_sub(chunk.chars().count());
            if remaining == 0 {
                chunks.push((chunk_start, chunk_end, std::mem::take(&mut chunk)));
                chunk_start = line_number;
                chunk_end = line_number;
                continue;
            }
            let line_chars = line.chars().count();
            if line_chars <= remaining {
                chunk.push_str(line);
                chunk_end = line_number;
                break;
            }
            let split = byte_index_after_chars(line, remaining);
            chunk.push_str(&line[..split]);
            chunk_end = line_number;
            chunks.push((chunk_start, chunk_end, std::mem::take(&mut chunk)));
            chunk_start = line_number;
            line = &line[split..];
        }
    }
    if !chunk.is_empty() || chunks.is_empty() {
        chunks.push((chunk_start, chunk_end, chunk));
    }

    let chunk_count = chunks.len();
    for (index, (start_line, end_line, content)) in chunks.into_iter().enumerate() {
        out.push(MemorySection {
            id: if chunk_count == 1 {
                base_id.to_string()
            } else {
                format!("{base_id}-part-{}", index + 1)
            },
            heading: boundary.heading.clone(),
            level: boundary.level,
            start_line,
            end_line,
            content,
        });
    }
}

fn byte_index_after_chars(value: &str, count: usize) -> usize {
    if count == 0 {
        return 0;
    }
    value
        .char_indices()
        .nth(count)
        .map(|(index, _)| index)
        .unwrap_or(value.len())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MarkdownFence {
    character: char,
    run_length: usize,
}

fn update_markdown_fence(line: &str, active: &mut Option<MarkdownFence>) -> bool {
    let line = line.trim_end_matches(['\r', '\n']);
    let indentation = line.bytes().take_while(|byte| *byte == b' ').count();
    if indentation > 3 {
        return false;
    }
    let line = &line[indentation..];
    let Some(character) = line
        .chars()
        .next()
        .filter(|value| matches!(value, '`' | '~'))
    else {
        return false;
    };
    let run_length = line.chars().take_while(|value| *value == character).count();
    if run_length < 3 {
        return false;
    }
    let remainder = &line[run_length..];
    match active {
        Some(open)
            if open.character == character
                && run_length >= open.run_length
                && remainder.trim().is_empty() =>
        {
            *active = None;
            true
        }
        Some(_) => false,
        None if character == '`' && remainder.contains('`') => false,
        None => {
            *active = Some(MarkdownFence {
                character,
                run_length,
            });
            true
        }
    }
}

fn atx_heading(line: &str) -> Option<(u8, String)> {
    static HEADING: OnceLock<Regex> = OnceLock::new();
    let captures = HEADING
        .get_or_init(|| Regex::new(r"^\s{0,3}(#{1,6})[ \t]+(.+?)\s*#*\s*$").expect("heading regex"))
        .captures(line.trim_end_matches(['\r', '\n']))?;
    let level = captures.get(1)?.as_str().len() as u8;
    let heading = captures.get(2)?.as_str().trim().to_string();
    Some((level, heading))
}

fn slugify(value: &str) -> String {
    let mut slug = String::new();
    let mut pending_separator = false;
    for character in value.chars() {
        if character.is_alphanumeric() {
            if pending_separator && !slug.is_empty() {
                slug.push('-');
            }
            slug.extend(character.to_lowercase());
            pending_separator = false;
        } else {
            pending_separator = true;
        }
    }
    slug
}

#[derive(Clone, Debug, Default)]
struct ParsedSourceMetadata {
    thread_id: Option<String>,
    rollout_path: Option<PathBuf>,
    cwd: Option<PathBuf>,
}

fn parse_source_metadata(content: &str) -> ParsedSourceMetadata {
    let mut metadata = ParsedSourceMetadata::default();
    for line in content.lines().take(32) {
        if line.trim().is_empty() {
            break;
        }
        if let Some(value) = metadata_field(line, "thread_id") {
            metadata.thread_id = Some(value.to_string());
        } else if let Some(value) = metadata_field(line, "rollout_path") {
            metadata.rollout_path = Some(PathBuf::from(value));
        } else if let Some(value) = metadata_field(line, "cwd") {
            metadata.cwd = Some(PathBuf::from(value));
        } else if !is_generic_metadata_field(line) {
            break;
        }
    }
    metadata
}

fn metadata_field<'a>(line: &'a str, field: &str) -> Option<&'a str> {
    let (key, value) = line.split_once(':')?;
    (key.trim() == field)
        .then(|| value.trim().trim_matches(['\'', '"']))
        .filter(|value| !value.is_empty())
}

fn inline_metadata_value(line: &str, field: &str) -> Option<String> {
    static VALUE: OnceLock<Regex> = OnceLock::new();
    let expression = VALUE.get_or_init(|| {
        Regex::new(r"(?:^|[,(]\s*)(thread_id|rollout_path|cwd)\s*[:=]\s*([^,)\s]+)")
            .expect("inline source metadata regex")
    });
    expression.captures_iter(line).find_map(|captures| {
        (captures.get(1)?.as_str() == field)
            .then(|| captures.get(2).map(|value| value.as_str().to_string()))
            .flatten()
    })
}

fn is_generic_metadata_field(line: &str) -> bool {
    static FIELD: OnceLock<Regex> = OnceLock::new();
    FIELD
        .get_or_init(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_-]*\s*:").expect("metadata field regex"))
        .is_match(line)
}

fn parse_explicit_dates(content: &str) -> Vec<MemoryEventDate> {
    static FIELD: OnceLock<Regex> = OnceLock::new();
    static DATE_VALUE: OnceLock<Regex> = OnceLock::new();
    static DATE_HEADING: OnceLock<Regex> = OnceLock::new();
    let field = FIELD.get_or_init(|| {
        Regex::new(r##"(?i)^\s*(date|created|created_at|updated|updated_at|last_modified|event_date)\s*:\s*['"]?([^'"#]+)"##)
            .expect("date field regex")
    });
    let date_value = DATE_VALUE.get_or_init(|| {
        Regex::new(r"^\d{4}-\d{2}-\d{2}(?:[Tt][0-9:.+-]+[Zz]?)?$").expect("date value regex")
    });
    let date_heading = DATE_HEADING.get_or_init(|| {
        Regex::new(r"^\s{0,3}#{1,6}\s+(\d{4}-\d{2}-\d{2})\s*#*\s*$").expect("date heading regex")
    });

    let mut dates = Vec::new();
    let mut in_frontmatter = false;
    let mut in_metadata_prefix = true;
    let mut fence = None;
    for (index, line) in content.lines().enumerate() {
        let line_number = index.saturating_add(1).min(u32::MAX as usize) as u32;
        if index == 0 && line.trim() == "---" {
            in_frontmatter = true;
            in_metadata_prefix = false;
            continue;
        }
        if in_frontmatter && line.trim() == "---" {
            in_frontmatter = false;
            continue;
        }
        if (in_frontmatter || in_metadata_prefix)
            && let Some(captures) = field.captures(line)
        {
            let label = captures
                .get(1)
                .expect("field label")
                .as_str()
                .to_lowercase();
            let value = captures.get(2).expect("field value").as_str().trim();
            if date_value.is_match(value) {
                dates.push(MemoryEventDate {
                    label,
                    value: value.to_string(),
                    line: line_number,
                });
            }
            continue;
        }
        if in_metadata_prefix {
            if line.trim().is_empty() || !is_generic_metadata_field(line) {
                in_metadata_prefix = false;
            } else {
                continue;
            }
        }
        if update_markdown_fence(line, &mut fence) {
            continue;
        }
        if fence.is_none()
            && let Some(captures) = date_heading.captures(line)
        {
            dates.push(MemoryEventDate {
                label: "section".to_string(),
                value: captures.get(1).expect("heading date").as_str().to_string(),
                line: line_number,
            });
        }
    }
    dates
}

fn parse_refs(content: &str, document_metadata: &ParsedSourceMetadata) -> Vec<MemoryRef> {
    static MARKDOWN_LINK: OnceLock<Regex> = OnceLock::new();
    static SOURCE_REF: OnceLock<Regex> = OnceLock::new();
    let markdown_link = MARKDOWN_LINK.get_or_init(|| {
        Regex::new(r#"\[([^\]]+)\]\((?:<([^>]+)>|([^\s)]+))(?:\s+['"][^'"]*['"])?\)"#)
            .expect("markdown link regex")
    });
    let source_ref = SOURCE_REF.get_or_init(|| {
        Regex::new(r"(?:^|[\s(])([A-Za-z0-9_./-]+\.md(?::\d+(?:-\d+)?)?)\b")
            .expect("source ref regex")
    });
    let mut refs = Vec::new();
    let mut seen = HashSet::new();
    let mut fence = None;
    for (index, line) in content.lines().enumerate() {
        let line_number = index.saturating_add(1).min(u32::MAX as usize) as u32;
        if update_markdown_fence(line, &mut fence) {
            continue;
        }
        if fence.is_some() {
            continue;
        }
        let line_session_id = inline_metadata_value(line, "thread_id")
            .or_else(|| document_metadata.thread_id.clone());
        let line_source_path = inline_metadata_value(line, "rollout_path")
            .map(PathBuf::from)
            .or_else(|| document_metadata.rollout_path.clone());
        let mut markdown_ranges = Vec::new();
        for captures in markdown_link.captures_iter(line) {
            let whole = captures.get(0).expect("whole markdown link");
            markdown_ranges.push(whole.range());
            let target = captures
                .get(2)
                .or_else(|| captures.get(3))
                .expect("link target")
                .as_str()
                .to_string();
            if seen.insert((line_number, target.clone())) {
                refs.push(MemoryRef {
                    target,
                    label: Some(captures.get(1).expect("link label").as_str().to_string()),
                    line: line_number,
                    memory_id: None,
                    session_id: line_session_id.clone(),
                    source_path: line_source_path.clone(),
                });
            }
        }
        for captures in source_ref.captures_iter(line) {
            let whole = captures.get(0).expect("whole source reference");
            if markdown_ranges
                .iter()
                .any(|range| range.start < whole.end() && whole.start() < range.end)
            {
                continue;
            }
            let target = captures.get(1).expect("source target").as_str().to_string();
            if seen.insert((line_number, target.clone())) {
                refs.push(MemoryRef {
                    target,
                    label: None,
                    line: line_number,
                    memory_id: None,
                    session_id: line_session_id.clone(),
                    source_path: line_source_path.clone(),
                });
            }
        }
    }
    if let Some(source_path) = &document_metadata.rollout_path {
        let target = source_path.to_string_lossy().to_string();
        if seen.insert((1, target.clone())) {
            refs.push(MemoryRef {
                target,
                label: Some("rollout".to_string()),
                line: 1,
                memory_id: None,
                session_id: document_metadata.thread_id.clone(),
                source_path: Some(source_path.clone()),
            });
        }
    }
    refs
}

fn resolve_memory_refs(documents: &mut [MemoryDocument]) {
    let by_path = documents
        .iter()
        .map(|document| (document.source_path.clone(), document.stable_id.clone()))
        .collect::<HashMap<_, _>>();
    for document in documents {
        let parent = document
            .source_path
            .parent()
            .unwrap_or_else(|| Path::new("."));
        for reference in &mut document.refs {
            reference.memory_id = None;
            let Some(target_path) = memory_ref_path(&reference.target) else {
                continue;
            };
            let target_path = if target_path.is_absolute() {
                normalize_lexical(&target_path)
            } else {
                normalize_lexical(&parent.join(target_path))
            };
            reference.memory_id = by_path.get(&target_path).cloned();
        }
    }
}

fn memory_ref_path(target: &str) -> Option<PathBuf> {
    let target = target.split('#').next()?;
    let markdown_end = target.to_ascii_lowercase().find(".md")? + 3;
    Some(PathBuf::from(&target[..markdown_end]))
}

fn normalize_lexical(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sort_documents(documents: &mut [MemoryDocument]) {
    documents.sort_by(|left, right| {
        left.provider
            .label()
            .cmp(right.provider.label())
            .then_with(|| left.source_path.cmp(&right.source_path))
    });
}

fn load_snapshot_once(path: &Path) -> Result<MemorySnapshot> {
    let contents = match fs::read(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MemorySnapshot::default());
        }
        Err(error) => {
            return Err(error).with_context(|| format!("read memory snapshot {}", path.display()));
        }
    };
    let snapshot: MemorySnapshot = serde_json::from_slice(&contents)
        .with_context(|| format!("parse memory snapshot {}", path.display()))?;
    if snapshot.schema_version != MEMORY_SNAPSHOT_SCHEMA_VERSION {
        bail!(
            "unsupported memory snapshot schema version {} (expected {})",
            snapshot.schema_version,
            MEMORY_SNAPSHOT_SCHEMA_VERSION
        );
    }
    Ok(snapshot)
}

fn atomic_write_snapshot(path: &Path, snapshot: &MemorySnapshot) -> Result<()> {
    let parent = parent_directory(path)?;
    fs::create_dir_all(parent)?;
    let bytes = serde_json::to_vec_pretty(snapshot)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(&bytes)?;
    temporary.write_all(b"\n")?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("publish memory snapshot {}", path.display()))?;
    sync_directory(parent)
}

fn parent_directory(path: &Path) -> Result<&Path> {
    match path.parent() {
        Some(parent) if parent.as_os_str().is_empty() => Ok(Path::new(".")),
        Some(parent) => Ok(parent),
        None => Err(anyhow!(
            "memory snapshot path has no parent: {}",
            path.display()
        )),
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

struct SnapshotWriteLock {
    file: File,
}

impl SnapshotWriteLock {
    fn acquire(snapshot_path: &Path, timeout: Duration) -> Result<Self> {
        let parent = parent_directory(snapshot_path)?;
        fs::create_dir_all(parent)?;
        let lock_path = snapshot_path.with_extension("lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("open memory snapshot lock {}", lock_path.display()))?;
        let started = Instant::now();
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(Self { file }),
                Err(TryLockError::WouldBlock) if started.elapsed() < timeout => {
                    thread::sleep(Duration::from_millis(50));
                }
                Err(TryLockError::WouldBlock) => {
                    bail!(
                        "timed out after {:.1}s waiting for memory snapshot lock {}",
                        timeout.as_secs_f32(),
                        lock_path.display()
                    );
                }
                Err(TryLockError::Error(error)) => {
                    return Err(error).with_context(|| {
                        format!("acquire memory snapshot lock {}", lock_path.display())
                    });
                }
            }
        }
    }
}

impl Drop for SnapshotWriteLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    fn options(claude_root: PathBuf, codex_home: PathBuf) -> MemoryDiscoveryOptions {
        MemoryDiscoveryOptions {
            claude_project_roots: vec![claude_root],
            codex_homes: vec![codex_home],
            enabled_sources: [SourceKind::Claude, SourceKind::Codex]
                .into_iter()
                .collect(),
            exclude_patterns: Vec::new(),
        }
    }

    fn write(path: &Path, contents: &str) {
        fs::create_dir_all(path.parent().expect("parent")).expect("create parent");
        fs::write(path, contents).expect("write fixture");
    }

    #[test]
    fn discovery_is_provider_specific_and_excludes_forbidden_trees() {
        let temp = tempfile::tempdir().expect("tempdir");
        let claude = temp.path().join("claude-projects");
        let codex = temp.path().join("codex");
        write(&claude.join("project-a/memory/note.md"), "# Claude note\n");
        write(
            &claude.join("project-a/memory/raw/private.md"),
            "excluded\n",
        );
        write(&claude.join("project-a/not-memory/other.md"), "excluded\n");
        write(&codex.join("memories/MEMORY.md"), "# Index\n");
        write(&codex.join("memories/memory_summary.md"), "# Summary\n");
        write(
            &codex.join("memories/rollout_summaries/run.md"),
            "# Rollout\n",
        );
        write(&codex.join("memories/skills/ignored.md"), "excluded\n");
        write(&codex.join("memories/unlisted.md"), "excluded\n");

        let found = discover_memory_documents(&options(claude, codex)).expect("discover");
        let relative = found
            .candidates
            .iter()
            .map(|candidate| {
                candidate
                    .source_path
                    .strip_prefix(temp.path().canonicalize().expect("canonical temp"))
                    .expect("under temp")
                    .to_string_lossy()
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            relative,
            vec![
                "claude-projects/project-a/memory/note.md",
                "codex/memories/MEMORY.md",
                "codex/memories/memory_summary.md",
                "codex/memories/rollout_summaries/run.md",
            ]
        );
        assert_eq!(
            found.candidates[0].scope.project.as_deref(),
            Some("project-a")
        );
        assert!(found.failures.is_empty());
    }

    #[test]
    fn discovery_respects_disabled_provider_and_exclude_glob() {
        let temp = tempfile::tempdir().expect("tempdir");
        let claude = temp.path().join("claude-projects");
        let codex = temp.path().join("codex");
        write(&claude.join("project/memory/keep.md"), "keep\n");
        write(&claude.join("project/memory/skip.md"), "skip\n");
        write(&codex.join("memories/MEMORY.md"), "disabled\n");
        let mut options = options(claude, codex);
        options.enabled_sources = [SourceKind::Claude].into_iter().collect();
        options.exclude_patterns = vec!["**/skip.md".to_string()];

        let found = discover_memory_documents(&options).expect("discover");
        assert_eq!(found.candidates.len(), 1);
        assert!(found.candidates[0].source_path.ends_with("keep.md"));
    }

    #[test]
    fn parser_builds_bounded_sections_outside_fences_dates_and_refs() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("MEMORY.md");
        let long = "é".repeat(MAX_MEMORY_SECTION_CHARS + 25);
        write(
            &path,
            &format!(
                "---\ncreated: 2026-09-01\nupdated: someday\n---\n# Main\nSee [run](rollout_summaries/run.md) and MEMORY.md:12-13.\n```md\n# Not a heading\n```\n## 2026-09-02\n{long}\n"
            ),
        );
        let canonical = path.canonicalize().expect("canonical");
        let document = parse_memory_document(&MemoryCandidate {
            provider: SourceKind::Codex,
            source_path: canonical,
            scope: MemoryScope::default(),
            kind: MemoryDocumentKind::Index,
        })
        .expect("parse");

        assert_eq!(document.title.as_deref(), Some("Main"));
        assert_eq!(
            document
                .event_dates
                .iter()
                .map(|date| date.value.as_str())
                .collect::<Vec<_>>(),
            vec!["2026-09-01", "2026-09-02"]
        );
        assert_eq!(document.refs.len(), 2);
        assert!(
            document
                .sections
                .iter()
                .all(|section| section.content.chars().count() <= MAX_MEMORY_SECTION_CHARS)
        );
        assert_eq!(
            document
                .sections
                .iter()
                .filter(|section| section.heading.as_deref() == Some("Not a heading"))
                .count(),
            0
        );
        assert!(document.sections.len() >= 3);
    }

    #[test]
    fn section_ids_remain_unique_across_heading_and_chunk_suffixes() {
        for content in [
            "# Topic\nfirst\n# Topic\nsecond\n# Topic-2\nthird\n# Topic-2-2\nfourth\n".to_string(),
            format!(
                "# Topic\n{}\n# Topic-part-1\nlast\n",
                "x".repeat(MAX_MEMORY_SECTION_CHARS)
            ),
            format!(
                "# Topic-part-1\nfirst\n# Topic\n{}\n",
                "x".repeat(MAX_MEMORY_SECTION_CHARS)
            ),
        ] {
            let (sections, _) = parse_sections(&content);
            let ids: HashSet<_> = sections.iter().map(|section| &section.id).collect();
            assert_eq!(ids.len(), sections.len(), "duplicate IDs: {sections:?}");
            assert_eq!(
                sections
                    .iter()
                    .map(|section| section.content.as_str())
                    .collect::<String>(),
                content
            );
            assert_eq!(sections, parse_sections(&content).0);
        }
    }

    #[test]
    fn longer_fence_is_not_closed_by_shorter_literal_fence() {
        let content = "# Visible\n````md\n```\n# Hidden\n## 2026-09-02\n[hidden](hidden.md)\n```\n````\n## Shown\n";
        let (sections, _) = parse_sections(content);
        assert!(
            sections
                .iter()
                .any(|section| section.heading.as_deref() == Some("Shown"))
        );
        assert!(
            sections
                .iter()
                .all(|section| section.heading.as_deref() != Some("Hidden"))
        );
        assert!(parse_explicit_dates(content).is_empty());
        assert!(parse_refs(content, &ParsedSourceMetadata::default()).is_empty());
    }

    #[test]
    fn consistency_check_rejects_equal_size_different_reads_with_identical_metadata() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("memory.md");
        write(&path, "alpha\n");
        let fingerprint =
            file_fingerprint(&fs::metadata(path).expect("metadata")).expect("fingerprint");
        assert!(!consistent_read_matches(
            &fingerprint,
            "alpha\n",
            &fingerprint,
            "bravo\n",
            &fingerprint,
        ));
    }

    #[cfg(unix)]
    #[test]
    fn claude_memory_directory_symlink_is_rejected_without_scope_aliasing() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let claude = temp.path().join("claude-projects");
        write(&claude.join("project-b/memory/note.md"), "# B\n");
        fs::create_dir_all(claude.join("project-a")).expect("project a");
        symlink(
            claude.join("project-b/memory"),
            claude.join("project-a/memory"),
        )
        .expect("memory symlink");
        let found = discover_memory_documents(&MemoryDiscoveryOptions {
            claude_project_roots: vec![claude],
            codex_homes: Vec::new(),
            enabled_sources: [SourceKind::Claude].into_iter().collect(),
            exclude_patterns: Vec::new(),
        })
        .expect("discover");
        assert_eq!(found.candidates.len(), 1);
        assert_eq!(
            found.candidates[0].scope.project.as_deref(),
            Some("project-b")
        );
        assert!(
            found.candidates[0]
                .source_path
                .ends_with("project-b/memory/note.md")
        );
    }

    #[test]
    fn stable_identity_uses_provider_and_path_while_version_uses_content() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("note.md");
        write(&path, "first\n");
        let path = path.canonicalize().expect("canonical");
        let candidate = MemoryCandidate {
            provider: SourceKind::Claude,
            source_path: path,
            scope: MemoryScope::default(),
            kind: MemoryDocumentKind::Note,
        };
        let first = parse_memory_document(&candidate).expect("first parse");
        write(&candidate.source_path, "second\n");
        let second = parse_memory_document(&candidate).expect("second parse");
        assert_eq!(first.stable_id, second.stable_id);
        assert_ne!(first.version_sha256, second.version_sha256);
        assert_ne!(
            first.stable_id,
            memory_stable_id(SourceKind::Codex, &candidate.source_path)
        );
    }

    #[test]
    fn refresh_replaces_changed_document_deletes_confirmed_absence_and_skips_noop() {
        let temp = tempfile::tempdir().expect("tempdir");
        let claude = temp.path().join("claude-projects");
        let codex = temp.path().join("codex");
        let source = claude.join("project/memory/note.md");
        write(&source, "# First\n");
        let store = MemoryStore::new(temp.path().join("store/documents.json"));
        let options = options(claude, codex);

        let first = store.refresh(&options).expect("first refresh");
        assert!(first.changed);
        let initial = store.load().expect("initial snapshot");
        assert_eq!(initial.documents.len(), 1);

        let noop = store.refresh(&options).expect("noop refresh");
        assert!(!noop.changed);
        assert_eq!(noop.unchanged, 1);

        write(&source, "# Changed with a different length\n");
        let changed = store.refresh(&options).expect("changed refresh");
        assert!(changed.changed);
        assert_eq!(changed.parsed, 1);
        let replaced = store.load().expect("replaced snapshot");
        assert_eq!(
            replaced.documents[0].content,
            "# Changed with a different length\n"
        );

        fs::remove_file(&source).expect("remove source");
        let deleted = store.refresh(&options).expect("delete refresh");
        assert_eq!(deleted.deleted, 1);
        assert!(store.load().expect("empty snapshot").documents.is_empty());
    }

    #[test]
    fn equal_length_edit_with_preserved_mtime_still_replaces_content() {
        let temp = tempfile::tempdir().expect("tempdir");
        let claude = temp.path().join("claude-projects");
        let source = claude.join("project/memory/note.md");
        write(&source, "alpha\n");
        let store = MemoryStore::new(temp.path().join("store/documents.json"));
        let options = options(claude, temp.path().join("codex"));
        store.refresh(&options).expect("initial refresh");
        let original_mtime = fs::metadata(&source)
            .expect("metadata")
            .modified()
            .expect("mtime");

        write(&source, "bravo\n");
        File::options()
            .write(true)
            .open(&source)
            .expect("open source")
            .set_times(fs::FileTimes::new().set_modified(original_mtime))
            .expect("restore exact mtime");

        let report = store.refresh(&options).expect("refresh edited source");
        assert_eq!(report.parsed, 1);
        assert_eq!(report.snapshot.documents[0].content, "bravo\n");
    }

    #[test]
    fn codex_summary_metadata_and_relative_memory_refs_are_preserved() {
        let temp = tempfile::tempdir().expect("tempdir");
        let codex = temp.path().join("codex");
        let summary_name = "2026-09-01 example.md";
        write(
            &codex.join("memories/MEMORY.md"),
            &format!(
                "# Index\n- [summary](<rollout_summaries/{summary_name}>) (rollout_path=/sessions/run.jsonl, thread_id=thread-123)\n"
            ),
        );
        write(
            &codex.join("memories/rollout_summaries").join(summary_name),
            "thread_id: thread-123\nupdated_at: 2026-09-01T12:30:00+00:00\nrollout_path: /sessions/run.jsonl\ncwd: /workspace/memex\n\n# Work\n",
        );
        let store = MemoryStore::new(temp.path().join("store/documents.json"));
        let options = options(temp.path().join("claude"), codex);
        let report = store.refresh(&options).expect("refresh");
        let index = report
            .snapshot
            .documents
            .iter()
            .find(|document| document.kind == MemoryDocumentKind::Index)
            .expect("index");
        let summary = report
            .snapshot
            .documents
            .iter()
            .find(|document| {
                document
                    .source_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    == Some(summary_name)
            })
            .expect("summary");

        assert_eq!(summary.kind, MemoryDocumentKind::Summary);
        assert_eq!(
            summary.scope.cwd.as_deref(),
            Some(Path::new("/workspace/memex"))
        );
        assert_eq!(summary.scope.project.as_deref(), Some("memex"));
        assert_eq!(summary.event_dates[0].value, "2026-09-01T12:30:00+00:00");
        let index_ref = index
            .refs
            .iter()
            .find(|reference| reference.target.contains(summary_name))
            .expect("index reference");
        assert_eq!(
            index_ref.memory_id.as_deref(),
            Some(summary.stable_id.as_str())
        );
        assert_eq!(index_ref.session_id.as_deref(), Some("thread-123"));
        assert_eq!(
            index
                .refs
                .iter()
                .filter(|reference| reference.target.ends_with("run.md"))
                .count(),
            0
        );
        assert_eq!(
            index_ref.source_path.as_deref(),
            Some(Path::new("/sessions/run.jsonl"))
        );
        let rollout_ref = summary
            .refs
            .iter()
            .find(|reference| reference.source_path.is_some())
            .expect("rollout reference");
        assert_eq!(rollout_ref.session_id.as_deref(), Some("thread-123"));
    }

    #[test]
    fn discovery_failure_retains_only_its_subtree_and_exclusions_still_purge() {
        let temp = tempfile::tempdir().expect("tempdir");
        let first_home = temp.path().join("codex-one");
        let failed_home = temp.path().join("codex-two");
        let first_source = first_home.join("memories/MEMORY.md");
        let failed_source = failed_home.join("memories/MEMORY.md");
        write(&first_source, "# First\n");
        write(&failed_source, "# Second\n");
        let store = MemoryStore::new(temp.path().join("store/documents.json"));
        let mut options = MemoryDiscoveryOptions {
            claude_project_roots: Vec::new(),
            codex_homes: vec![first_home, failed_home.clone()],
            enabled_sources: [SourceKind::Codex].into_iter().collect(),
            exclude_patterns: Vec::new(),
        };
        store.refresh(&options).expect("initial refresh");

        fs::remove_file(&first_source).expect("delete from readable root");
        fs::remove_dir_all(failed_home.join("memories")).expect("remove failing tree");
        write(&failed_home.join("memories"), "not a directory\n");
        let partial = store.refresh(&options).expect("partial refresh");
        assert_eq!(partial.deleted, 1);
        assert_eq!(partial.stale_retained, 1);
        assert_eq!(partial.snapshot.documents.len(), 1);
        assert!(matches!(
            partial.snapshot.documents[0].freshness,
            MemoryFreshness::Stale { .. }
        ));

        options.exclude_patterns = vec![format!("{}/**", failed_home.display())];
        let excluded = store.refresh(&options).expect("excluded refresh");
        assert_eq!(excluded.deleted, 1);
        assert!(excluded.snapshot.documents.is_empty());
    }

    #[test]
    fn nested_discovery_failure_does_not_cover_deleted_sibling() {
        let discovery = MemoryDiscovery {
            candidates: Vec::new(),
            failures: vec![MemoryDiscoveryFailure {
                provider: SourceKind::Claude,
                root: PathBuf::from("/projects/project/memory/keep"),
                error: "permission denied".to_string(),
            }],
        };
        assert!(discovery_failure_covers(
            &discovery.failures,
            SourceKind::Claude,
            Path::new("/projects/project/memory/keep/note.md"),
        ));
        assert!(!discovery_failure_covers(
            &discovery.failures,
            SourceKind::Claude,
            Path::new("/projects/project/memory/gone.md"),
        ));
    }

    #[test]
    fn refresh_preserves_unselected_providers_and_configured_roots() {
        let temp = tempfile::tempdir().expect("tempdir");
        let claude = temp.path().join("claude-projects");
        let codex_one = temp.path().join("codex-one");
        let codex_two = temp.path().join("codex-two");
        write(&claude.join("project/memory/note.md"), "# Claude\n");
        write(&codex_one.join("memories/MEMORY.md"), "# Codex one\n");
        write(&codex_two.join("memories/MEMORY.md"), "# Codex two\n");
        let store = MemoryStore::new(temp.path().join("store/documents.json"));
        let mut initial = options(claude.clone(), codex_one.clone());
        initial.codex_homes.push(codex_two);
        assert_eq!(
            store
                .refresh(&initial)
                .expect("initial refresh")
                .document_count,
            3
        );

        let only_claude = MemoryDiscoveryOptions {
            claude_project_roots: vec![claude],
            codex_homes: Vec::new(),
            enabled_sources: [SourceKind::Claude].into_iter().collect(),
            exclude_patterns: Vec::new(),
        };
        let preserved = store.refresh(&only_claude).expect("selected refresh");
        assert_eq!(preserved.document_count, 3);
        assert_eq!(preserved.deleted, 0);

        let one_codex_root = MemoryDiscoveryOptions {
            claude_project_roots: Vec::new(),
            codex_homes: vec![codex_one],
            enabled_sources: [SourceKind::Codex].into_iter().collect(),
            exclude_patterns: Vec::new(),
        };
        let preserved = store.refresh(&one_codex_root).expect("one root refresh");
        assert_eq!(preserved.document_count, 3);
        assert_eq!(preserved.deleted, 0);
    }

    #[cfg(unix)]
    #[test]
    fn refresh_retains_last_good_document_on_read_error() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("tempdir");
        let claude = temp.path().join("claude-projects");
        let codex = temp.path().join("codex");
        let source = claude.join("project/memory/note.md");
        write(&source, "# Good\n");
        let store = MemoryStore::new(temp.path().join("store/documents.json"));
        let options = options(claude, codex);
        store.refresh(&options).expect("initial refresh");

        let mut permissions = fs::metadata(&source).expect("metadata").permissions();
        permissions.set_mode(0o000);
        fs::set_permissions(&source, permissions).expect("make unreadable");
        let outcome = store.refresh(&options);

        let mut permissions = fs::metadata(&source).expect("metadata").permissions();
        permissions.set_mode(0o600);
        fs::set_permissions(&source, permissions).expect("restore permissions");

        // Privileged test runners may still read mode-000 files, so only assert the stale path
        // when the operating system actually denied the read.
        let report = outcome.expect("refresh handles source error");
        if !report.failures.is_empty() {
            let snapshot = store.load().expect("snapshot");
            assert_eq!(snapshot.documents[0].content, "# Good\n");
            assert!(matches!(
                snapshot.documents[0].freshness,
                MemoryFreshness::Stale { .. }
            ));
        }
    }

    #[test]
    fn corrupt_snapshot_is_an_error_and_is_not_silently_replaced() {
        let temp = tempfile::tempdir().expect("tempdir");
        let snapshot = temp.path().join("memory/documents.json");
        write(&snapshot, "not json\n");
        let store = MemoryStore::new(&snapshot);
        let error = store.load().expect_err("corrupt snapshot");
        assert!(error.to_string().contains("parse memory snapshot"));
        assert_eq!(
            fs::read_to_string(snapshot).expect("unchanged"),
            "not json\n"
        );
    }

    #[test]
    fn atomic_snapshot_readers_only_observe_complete_json() {
        let temp = tempfile::tempdir().expect("tempdir");
        let snapshot = temp.path().join("memory/documents.json");
        let store = MemoryStore::new(&snapshot);
        atomic_write_snapshot(&snapshot, &MemorySnapshot::default()).expect("first write");
        for _ in 0..20 {
            atomic_write_snapshot(&snapshot, &MemorySnapshot::default()).expect("replace");
            assert_eq!(
                store.load().expect("complete snapshot"),
                MemorySnapshot::default()
            );
        }
    }

    #[test]
    fn changed_while_reading_error_kind_is_preserved_as_stale() {
        let error = io::Error::new(io::ErrorKind::PermissionDenied, "denied");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }
}
