//! Search and bounded retrieval over the immutable memory document snapshot.
//!
//! Memory search deliberately does not discover or refresh source documents. Callers that mutate
//! the snapshot must do so explicitly through [`crate::memory::MemoryStore`] and then rebuild the
//! section vectors with [`embed_memory`].

use crate::config::{Paths, UserConfig};
use crate::embed::{EmbedRuntimeConfig, EmbedderHandle, ModelChoice};
use crate::memory::{
    MemoryDocument, MemoryDocumentKind, MemoryEventDate, MemoryFreshness, MemoryRef,
    MemorySnapshot, MemoryStore, reparse_memory_document,
};
use crate::read_budget::{ContentContinuation, ContentPage, ReadField};
use crate::types::SourceKind;
use crate::vector::VectorIndex;
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use tantivy::collector::TopDocs;
use tantivy::query::{AllQuery, Query, QueryParser};
use tantivy::schema::{INDEXED, STORED, Schema, TEXT, Value};
use tantivy::{Index, ReloadPolicy, TantivyDocument};

const DEFAULT_SEARCH_LIMIT: usize = 20;
const DEFAULT_MAX_PER_DOCUMENT: usize = 2;
const MAX_SEARCH_LIMIT: usize = 500;
const MAX_QUERY_VIEWS: usize = 8;
const SEARCH_SNIPPET_CHARS: usize = 600;
const RRF_K: f32 = 60.0;
const SECTION_REF_PREFIX: &str = "msec1";
const MEMORY_VECTOR_DIR: &str = "vectors";
pub const DEFAULT_MEMORY_READ_CHARS: usize = 16_000;
pub const MAX_MEMORY_READ_CHARS: usize = 64_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemorySearchMode {
    #[default]
    Lexical,
    Semantic,
    Hybrid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemorySearchOptions {
    pub query: String,
    /// Include complete bounded section text only for explicit full/text projections.
    #[serde(default)]
    pub include_text: bool,
    #[serde(default)]
    pub additional_queries: Vec<String>,
    #[serde(default)]
    pub mode: MemorySearchMode,
    pub project: Option<String>,
    pub cwd: Option<PathBuf>,
    pub source: Option<SourceKind>,
    /// Inclusive lower bound over source modification time, in Unix milliseconds.
    pub since: Option<u64>,
    /// Inclusive upper bound over source modification time, in Unix milliseconds.
    pub until: Option<u64>,
    pub min_score: Option<f32>,
    #[serde(default = "default_recency_weight")]
    pub recency_weight: f32,
    #[serde(default = "default_recency_half_life_days")]
    pub recency_half_life_days: f32,
    #[serde(default = "default_search_limit")]
    pub limit: usize,
    #[serde(default = "default_max_per_document")]
    pub max_per_document: usize,
    /// Order all eligible ranked sections by source mtime (newest first) before applying document
    /// diversity and the result limit. Scores remain the relevance scores for the selected hits.
    #[serde(default)]
    pub sort_by_timestamp: bool,
}

impl Default for MemorySearchOptions {
    fn default() -> Self {
        Self {
            query: String::new(),
            include_text: false,
            additional_queries: Vec::new(),
            mode: MemorySearchMode::Lexical,
            project: None,
            cwd: None,
            source: None,
            since: None,
            until: None,
            min_score: None,
            recency_weight: 1.0,
            recency_half_life_days: 30.0,
            limit: DEFAULT_SEARCH_LIMIT,
            max_per_document: DEFAULT_MAX_PER_DOCUMENT,
            sort_by_timestamp: false,
        }
    }
}

impl MemorySearchOptions {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0 || self.limit > MAX_SEARCH_LIMIT {
            bail!("memory search limit must be between 1 and {MAX_SEARCH_LIMIT}");
        }
        if self.max_per_document == 0 || self.max_per_document > MAX_SEARCH_LIMIT {
            bail!("max_per_document must be between 1 and {MAX_SEARCH_LIMIT}");
        }
        normalized_query_views(self)?;
        if self
            .since
            .zip(self.until)
            .is_some_and(|(since, until)| since > until)
        {
            bail!("memory search since must be less than or equal to until");
        }
        if self
            .min_score
            .is_some_and(|score| !score.is_finite() || score < 0.0)
        {
            bail!("memory search min_score must be finite and non-negative");
        }
        if !self.recency_weight.is_finite() || self.recency_weight < 0.0 {
            bail!("memory search recency_weight must be finite and non-negative");
        }
        if !self.recency_half_life_days.is_finite() || self.recency_half_life_days <= 0.0 {
            bail!("memory search recency_half_life_days must be finite and greater than zero");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemorySearchHit {
    pub score: f32,
    pub memory_id: String,
    pub content_version: String,
    pub section_ref: String,
    pub section_id: String,
    pub title: Option<String>,
    pub heading: Option<String>,
    pub document_kind: MemoryDocumentKind,
    pub project: Option<String>,
    pub cwd: Option<PathBuf>,
    pub source: SourceKind,
    pub source_path: PathBuf,
    pub mtime_ms: u64,
    pub event_dates: Vec<MemoryEventDate>,
    pub start_line: u32,
    pub end_line: u32,
    pub snippet: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    pub refs: Vec<MemoryRef>,
    pub freshness: MemoryFreshness,
    pub changed_since_search: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryReadRequest {
    pub memory_id: String,
    pub section_ref: Option<String>,
    /// Content version returned by search. When it no longer matches, the reader returns a
    /// bounded page of the current full document rather than applying a stale section selector.
    pub content_version: Option<String>,
    #[serde(default)]
    pub offset_chars: usize,
    #[serde(default = "default_memory_read_chars")]
    pub max_chars: usize,
}

impl MemoryReadRequest {
    pub fn validate(&self) -> Result<()> {
        if self.memory_id.trim().is_empty() {
            bail!("memory_id must not be empty");
        }
        if self.max_chars == 0 || self.max_chars > MAX_MEMORY_READ_CHARS {
            bail!("memory read max_chars must be between 1 and {MAX_MEMORY_READ_CHARS}");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryReadValue {
    pub memory_id: String,
    pub content_version: String,
    pub section_ref: Option<String>,
    pub section_id: Option<String>,
    pub title: Option<String>,
    pub heading: Option<String>,
    pub document_kind: MemoryDocumentKind,
    pub project: Option<String>,
    pub cwd: Option<PathBuf>,
    pub source: SourceKind,
    pub source_path: PathBuf,
    pub mtime_ms: u64,
    pub event_dates: Vec<MemoryEventDate>,
    pub start_line: Option<u32>,
    pub end_line: Option<u32>,
    pub text: String,
    pub refs: Vec<MemoryRef>,
    pub freshness: MemoryFreshness,
    pub changed_since_search: bool,
    /// Actual character offset applied. This is reset to zero when the requested content version
    /// is stale and the response falls back to the current full document.
    pub offset_chars: usize,
    pub content: ContentPage,
    pub next_offset_chars: Option<usize>,
}

#[derive(Clone, Copy)]
struct SectionCandidate {
    document: usize,
    section: usize,
    vector_id: u64,
}

struct LexicalFields {
    key: tantivy::schema::Field,
    text: tantivy::schema::Field,
    heading: tantivy::schema::Field,
    title: tantivy::schema::Field,
}

struct LexicalMemoryIndex {
    index: Index,
    fields: LexicalFields,
}

pub fn search_memory(paths: &Paths, options: &MemorySearchOptions) -> Result<Vec<MemorySearchHit>> {
    options.validate()?;
    let queries = normalized_query_views(options)?;
    let store = MemoryStore::new(memory_snapshot_path(paths));
    let snapshot = store.load()?;
    let candidates = eligible_sections(&snapshot, options)?;
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let lexical = if matches!(
        options.mode,
        MemorySearchMode::Lexical | MemorySearchMode::Hybrid
    ) {
        Some(LexicalMemoryIndex::build(&snapshot, &candidates)?)
    } else {
        None
    };
    let mut vector = None;
    let mut embedder = None;
    if matches!(
        options.mode,
        MemorySearchMode::Semantic | MemorySearchMode::Hybrid
    ) {
        let snapshot_key = snapshot_fingerprint(&snapshot);
        let vector_path = memory_vector_path(paths, &snapshot_key);
        let loaded = VectorIndex::open(&vector_path).with_context(|| {
            format!(
                "memory vectors for snapshot {snapshot_key} are unavailable; run `memex index embed`"
            )
        })?;
        validate_vector_inventory(&loaded, &all_searchable_sections(&snapshot)?)?;
        let config = UserConfig::load(paths)?;
        let model = match loaded.model() {
            Some(model) => ModelChoice::parse(model)?,
            None => config.resolve_model(None)?,
        };
        let runtime = config.resolve_embed_runtime()?;
        embedder = Some(EmbedderHandle::with_model_and_runtime(model, &runtime)?);
        vector = Some(loaded);
    }

    let mut query_rankings = Vec::with_capacity(queries.len());
    for query in &queries {
        let lexical_ranking = match &lexical {
            Some(index) => Some(index.search(query, candidates.len())?),
            None => None,
        };
        let semantic_ranking = match (&vector, &mut embedder) {
            (Some(index), Some(embedder)) => {
                Some(semantic_ranking(index, embedder, query, &candidates)?)
            }
            _ => None,
        };
        let ranking = match options.mode {
            MemorySearchMode::Lexical => lexical_ranking.expect("lexical index initialized"),
            MemorySearchMode::Semantic => semantic_ranking.expect("semantic index initialized"),
            MemorySearchMode::Hybrid => fuse_rankings(&[
                lexical_ranking.expect("lexical index initialized"),
                semantic_ranking.expect("semantic index initialized"),
            ]),
        };
        query_rankings.push(ranking);
    }
    let mut ranked = if query_rankings.len() == 1 {
        query_rankings.pop().expect("one ranking")
    } else {
        fuse_rankings(&query_rankings)
    };

    let by_id = candidates
        .iter()
        .map(|candidate| (candidate.vector_id, *candidate))
        .collect::<HashMap<_, _>>();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64;
    for (id, score) in &mut ranked {
        let candidate = by_id
            .get(id)
            .expect("ranked memory section has a candidate");
        *score = apply_recency(
            *score,
            snapshot.documents[candidate.document].mtime_ms,
            now_ms,
            options.recency_weight,
            options.recency_half_life_days,
        );
    }
    if let Some(min_score) = options.min_score {
        ranked.retain(|(_, score)| *score >= min_score);
    }
    if options.sort_by_timestamp {
        ranked.sort_by(|left, right| {
            let left_candidate = by_id
                .get(&left.0)
                .expect("ranked memory section has a candidate");
            let right_candidate = by_id
                .get(&right.0)
                .expect("ranked memory section has a candidate");
            snapshot.documents[right_candidate.document]
                .mtime_ms
                .cmp(&snapshot.documents[left_candidate.document].mtime_ms)
                .then_with(|| right.1.total_cmp(&left.1))
                .then_with(|| left.0.cmp(&right.0))
        });
    } else {
        ranked.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
    }
    let mut document_counts = HashMap::<&str, usize>::new();
    let mut hits = Vec::with_capacity(options.limit);
    for (vector_id, score) in ranked {
        let Some(candidate) = by_id.get(&vector_id) else {
            continue;
        };
        let document = &snapshot.documents[candidate.document];
        let count = document_counts
            .entry(document.stable_id.as_str())
            .or_default();
        if *count >= options.max_per_document {
            continue;
        }
        *count += 1;
        let section = &document.sections[candidate.section];
        let (freshness, source_changed) = source_freshness(document);
        hits.push(MemorySearchHit {
            score,
            memory_id: document.stable_id.clone(),
            content_version: document.version_sha256.clone(),
            section_ref: make_section_ref(&document.version_sha256, &section.id),
            section_id: section.id.clone(),
            title: document.title.clone(),
            heading: section.heading.clone(),
            document_kind: document.kind,
            project: document.scope.project.clone(),
            cwd: document.scope.cwd.clone(),
            source: document.provider,
            source_path: document.source_path.clone(),
            mtime_ms: document.mtime_ms,
            event_dates: document.event_dates.clone(),
            start_line: section.start_line,
            end_line: section.end_line,
            snippet: make_snippet(&section.content, &queries),
            text: options.include_text.then(|| section.content.clone()),
            refs: refs_for_lines(document, section.start_line, section.end_line),
            freshness,
            changed_since_search: source_changed,
        });
        if hits.len() == options.limit {
            break;
        }
    }
    Ok(hits)
}

pub fn read_memory(paths: &Paths, request: &MemoryReadRequest) -> Result<MemoryReadValue> {
    request.validate()?;
    let store = MemoryStore::new(memory_snapshot_path(paths));
    let snapshot_document = store
        .get(&request.memory_id)?
        .ok_or_else(|| anyhow!("memory document '{}' not found", request.memory_id))?;
    // An indexed snapshot is a fallback for transient read failures, not a
    // permanent archive after confirmed source deletion. Do not follow a newly
    // substituted symlink or disclose cached text for a removed document.
    match fs::symlink_metadata(&snapshot_document.source_path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            bail!("memory source is no longer a regular file; refresh the index");
        }
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            bail!("memory source no longer exists; refresh the index");
        }
        _ => {}
    }
    if let Ok(canonical) = fs::canonicalize(&snapshot_document.source_path)
        && canonical != snapshot_document.source_path
    {
        bail!("memory source path identity changed; refresh the index");
    }
    let (snapshot_freshness, source_changed) = source_freshness(&snapshot_document);
    let (document, freshness, reparsed_current) = if source_changed {
        match reparse_memory_document(&snapshot_document) {
            Ok(current) => {
                let freshness = current.freshness.clone();
                (current, freshness, true)
            }
            Err(error)
                if error.chain().any(|cause| {
                    cause.downcast_ref::<std::io::Error>().is_some_and(|error| {
                        matches!(
                            error.kind(),
                            std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                        )
                    })
                }) =>
            {
                return Err(error.context("memory source disappeared while reading"));
            }
            Err(error) => (
                snapshot_document,
                MemoryFreshness::Stale {
                    error: format!(
                        "memory source changed but current content could not be parsed: {error:#}"
                    ),
                },
                false,
            ),
        }
    } else {
        (snapshot_document, snapshot_freshness, false)
    };
    let parsed_ref = request
        .section_ref
        .as_deref()
        .map(parse_section_ref)
        .transpose()?;
    let request_version_changed = request
        .content_version
        .as_deref()
        .is_some_and(|version| version != document.version_sha256.as_str())
        || parsed_ref
            .as_ref()
            .is_some_and(|(version, _)| version != &document.version_sha256);
    let has_version_evidence = request.content_version.is_some() || parsed_ref.is_some();
    let unverified_live_change = source_changed && (!reparsed_current || !has_version_evidence);
    let changed_since_search = request_version_changed || unverified_live_change;

    // A section reference is meaningful only for the content version that created it. Falling
    // back to the current full document prevents an old section ID or ordinal from selecting an
    // unrelated section after the source changes.
    let selector_stale = changed_since_search;
    let selected = if selector_stale {
        None
    } else if let Some((_, section_id)) = parsed_ref.as_ref() {
        Some(document.section(section_id).ok_or_else(|| {
            anyhow!(
                "memory section '{}' not found",
                request.section_ref.as_deref().unwrap_or_default()
            )
        })?)
    } else {
        None
    };
    let full_text = selected
        .map(|section| section.content.as_str())
        .unwrap_or(document.content.as_str());
    let offset_chars = if selector_stale {
        0
    } else {
        request.offset_chars
    };
    let total_chars = full_text.chars().count();
    if offset_chars > total_chars {
        bail!(
            "offset_chars {} is past the end of memory content ({} chars)",
            offset_chars,
            total_chars
        );
    }
    let returned_chars = request
        .max_chars
        .min(total_chars.saturating_sub(offset_chars));
    let text = char_range(full_text, offset_chars, returned_chars);
    let next_offset_chars =
        (offset_chars + returned_chars < total_chars).then_some(offset_chars + returned_chars);
    let content = ContentPage {
        returned_chars,
        total_chars,
        truncated: next_offset_chars.is_some(),
        continuations: next_offset_chars
            .map(|offset_chars| {
                vec![ContentContinuation {
                    field: ReadField::Text,
                    offset_chars,
                    total_chars,
                }]
            })
            .unwrap_or_default(),
    };
    let (section_ref, section_id, heading, start_line, end_line, refs) = match selected {
        Some(section) => (
            Some(make_section_ref(&document.version_sha256, &section.id)),
            Some(section.id.clone()),
            section.heading.clone(),
            Some(section.start_line),
            Some(section.end_line),
            refs_for_lines(&document, section.start_line, section.end_line),
        ),
        None => (None, None, None, None, None, document.refs.clone()),
    };

    Ok(MemoryReadValue {
        memory_id: document.stable_id,
        content_version: document.version_sha256,
        section_ref,
        section_id,
        title: document.title,
        heading,
        document_kind: document.kind,
        project: document.scope.project,
        cwd: document.scope.cwd,
        source: document.provider,
        source_path: document.source_path,
        mtime_ms: document.mtime_ms,
        event_dates: document.event_dates,
        start_line,
        end_line,
        text,
        refs,
        freshness,
        changed_since_search,
        offset_chars,
        content,
        next_offset_chars,
    })
}

/// Build and atomically publish section vectors for the current memory snapshot.
///
/// The snapshot fingerprint is part of the vector directory name, so a search can never silently
/// combine vectors from an older document version with the current snapshot.
pub fn embed_memory(
    paths: &Paths,
    model: ModelChoice,
    runtime: &EmbedRuntimeConfig,
) -> Result<usize> {
    let snapshot = MemoryStore::new(memory_snapshot_path(paths)).load()?;
    let candidates = all_searchable_sections(&snapshot)?;
    if candidates.is_empty() {
        return Ok(0);
    }
    let snapshot_key = snapshot_fingerprint(&snapshot);
    let vector_path = memory_vector_path(paths, &snapshot_key);
    if let Ok(existing) = VectorIndex::open(&vector_path)
        && existing.model() == Some(model.as_str())
        && validate_vector_inventory(&existing, &candidates).is_ok()
    {
        return Ok(0);
    }
    let mut embedder = EmbedderHandle::with_model_and_runtime(model, runtime)?;
    let mut vector =
        VectorIndex::empty_replacement(&vector_path, embedder.dims, Some(model.as_str()))?;
    for batch in candidates.chunks(64) {
        let texts = batch
            .iter()
            .map(|candidate| {
                snapshot.documents[candidate.document].sections[candidate.section]
                    .content
                    .as_str()
            })
            .collect::<Vec<_>>();
        let embeddings = embedder.embed_texts(&texts)?;
        if embeddings.len() != batch.len() {
            bail!(
                "memory embedder returned {} vectors for {} sections",
                embeddings.len(),
                batch.len()
            );
        }
        for (candidate, embedding) in batch.iter().zip(embeddings) {
            vector.add(candidate.vector_id, &embedding)?;
        }
    }
    vector.save()?;
    Ok(candidates.len())
}

/// Remove generated vector snapshots other than the one matching the current document snapshot.
///
/// The caller must hold the index lease and ensure no search readers are active. Unexpected files,
/// symlinks, and directory names are rejected instead of being deleted.
pub fn gc_memory_vectors(paths: &Paths, dry_run: bool) -> Result<usize> {
    let snapshot = MemoryStore::new(memory_snapshot_path(paths)).load()?;
    let current = snapshot_fingerprint(&snapshot);
    let root = paths.root.join("memory").join(MEMORY_VECTOR_DIR);
    let metadata = match fs::symlink_metadata(&root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        bail!(
            "memory vector root {} is not a regular directory",
            root.display()
        );
    }
    let mut obsolete = Vec::new();
    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow!("memory vector directory name is not valid UTF-8"))?;
        if file_type.is_symlink()
            || !file_type.is_dir()
            || name.len() != 64
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!(
                "unexpected entry in memory vector root: {}",
                entry.path().display()
            );
        }
        if name != current {
            obsolete.push(entry.path());
        }
    }
    obsolete.sort();
    if !dry_run {
        for path in &obsolete {
            fs::remove_dir_all(path)
                .with_context(|| format!("remove obsolete memory vectors {}", path.display()))?;
        }
    }
    Ok(obsolete.len())
}

impl LexicalMemoryIndex {
    fn build(snapshot: &MemorySnapshot, candidates: &[SectionCandidate]) -> Result<Self> {
        let mut schema = Schema::builder();
        let key = schema.add_u64_field("key", INDEXED | STORED);
        let text = schema.add_text_field("text", TEXT);
        let heading = schema.add_text_field("heading", TEXT);
        let title = schema.add_text_field("title", TEXT);
        let index = Index::create_in_ram(schema.build());
        let mut writer = index.writer(15_000_000)?;
        for candidate in candidates {
            let document = &snapshot.documents[candidate.document];
            let section = &document.sections[candidate.section];
            let mut row = TantivyDocument::default();
            row.add_u64(key, candidate.vector_id);
            row.add_text(text, &section.content);
            if let Some(value) = section.heading.as_deref() {
                row.add_text(heading, value);
            }
            if let Some(value) = document.title.as_deref() {
                row.add_text(title, value);
            }
            writer.add_document(row)?;
        }
        writer.commit()?;
        Ok(Self {
            index,
            fields: LexicalFields {
                key,
                text,
                heading,
                title,
            },
        })
    }

    fn search(&self, query: &str, limit: usize) -> Result<Vec<(u64, f32)>> {
        let reader = self
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let searcher = reader.searcher();
        let parsed: Box<dyn Query> = if query == "*" {
            Box::new(AllQuery)
        } else {
            let mut parser = QueryParser::for_index(
                &self.index,
                vec![self.fields.text, self.fields.heading, self.fields.title],
            );
            parser.set_field_boost(self.fields.heading, 1.5);
            parser.set_field_boost(self.fields.title, 1.25);
            parser.parse_query(query)?
        };
        let rows = searcher.search(&parsed, &TopDocs::with_limit(limit))?;
        rows.into_iter()
            .map(|(score, address)| {
                let row = searcher.doc::<TantivyDocument>(address)?;
                let key = row
                    .get_first(self.fields.key)
                    .and_then(|value| value.as_u64())
                    .ok_or_else(|| anyhow!("memory lexical index row is missing its key"))?;
                Ok((key, score))
            })
            .collect()
    }
}

fn semantic_ranking(
    vector: &VectorIndex,
    embedder: &mut EmbedderHandle,
    query: &str,
    candidates: &[SectionCandidate],
) -> Result<Vec<(u64, f32)>> {
    if query.trim().is_empty() {
        bail!("semantic memory search requires a non-empty query");
    }
    let embedding = embedder
        .embed_texts(&[query])?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("memory query embedding is missing"))?;
    let eligible = candidates
        .iter()
        .map(|candidate| candidate.vector_id)
        .collect::<HashSet<_>>();
    let rows =
        vector.search_filtered(
            &embedding,
            candidates.len(),
            |id| Ok(eligible.contains(&id)),
        )?;
    Ok(rows
        .into_iter()
        .map(|(id, distance)| (id, 1.0 / (1.0 + distance)))
        .collect())
}

fn fuse_rankings(rankings: &[Vec<(u64, f32)>]) -> Vec<(u64, f32)> {
    let mut scores = HashMap::<u64, f32>::new();
    for ranking in rankings {
        for (rank, (id, _)) in ranking.iter().enumerate() {
            *scores.entry(*id).or_default() += 1.0 / (RRF_K + rank as f32 + 1.0);
        }
    }
    let mut fused = scores.into_iter().collect::<Vec<_>>();
    fused.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    fused
}

fn eligible_sections(
    snapshot: &MemorySnapshot,
    options: &MemorySearchOptions,
) -> Result<Vec<SectionCandidate>> {
    section_candidates(snapshot, |document| {
        options
            .project
            .as_ref()
            .is_none_or(|project| document.scope.project.as_ref() == Some(project))
            && options
                .cwd
                .as_ref()
                .is_none_or(|cwd| document.scope.cwd.as_ref() == Some(cwd))
            && options
                .source
                .is_none_or(|source| document.provider == source)
            && options.since.is_none_or(|since| document.mtime_ms >= since)
            && options.until.is_none_or(|until| document.mtime_ms <= until)
    })
}

fn normalized_query_views(options: &MemorySearchOptions) -> Result<Vec<&str>> {
    let mut seen = HashSet::new();
    let queries = std::iter::once(options.query.as_str())
        .chain(options.additional_queries.iter().map(String::as_str))
        .map(str::trim)
        .filter(|query| !query.is_empty() && seen.insert(*query))
        .collect::<Vec<_>>();
    if queries.is_empty() {
        bail!("at least one non-empty memory search query is required");
    }
    if queries.len() > MAX_QUERY_VIEWS {
        bail!("memory search supports at most {MAX_QUERY_VIEWS} query views");
    }
    Ok(queries)
}

fn all_searchable_sections(snapshot: &MemorySnapshot) -> Result<Vec<SectionCandidate>> {
    section_candidates(snapshot, |_| true)
}

fn section_candidates(
    snapshot: &MemorySnapshot,
    include_document: impl Fn(&MemoryDocument) -> bool,
) -> Result<Vec<SectionCandidate>> {
    let mut ids = HashMap::<u64, (String, String)>::new();
    let mut candidates = Vec::new();
    for (document_index, document) in snapshot.documents.iter().enumerate() {
        if !include_document(document) {
            continue;
        }
        for (section_index, section) in document.sections.iter().enumerate() {
            if section.content.trim().is_empty() {
                continue;
            }
            let vector_id = section_vector_id(document, &section.id);
            if let Some((other_document, other_section)) =
                ids.insert(vector_id, (document.stable_id.clone(), section.id.clone()))
            {
                bail!(
                    "memory section vector ID collision between {other_document}/{other_section} and {}/{}",
                    document.stable_id,
                    section.id
                );
            }
            candidates.push(SectionCandidate {
                document: document_index,
                section: section_index,
                vector_id,
            });
        }
    }
    Ok(candidates)
}

fn validate_vector_inventory(vector: &VectorIndex, candidates: &[SectionCandidate]) -> Result<()> {
    if vector.len() != candidates.len()
        || candidates
            .iter()
            .any(|candidate| !vector.contains(candidate.vector_id))
    {
        bail!(
            "memory vector inventory does not match the current snapshot; run `memex index embed`"
        );
    }
    Ok(())
}

fn memory_snapshot_path(paths: &Paths) -> PathBuf {
    paths.root.join("memory").join("documents.json")
}

fn memory_vector_path(paths: &Paths, snapshot_key: &str) -> PathBuf {
    paths
        .root
        .join("memory")
        .join(MEMORY_VECTOR_DIR)
        .join(snapshot_key)
}

fn snapshot_fingerprint(snapshot: &MemorySnapshot) -> String {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, &snapshot.schema_version.to_string());
    let mut identities = snapshot
        .documents
        .iter()
        .flat_map(|document| {
            document.sections.iter().map(move |section| {
                (
                    document.stable_id.as_str(),
                    document.version_sha256.as_str(),
                    section.id.as_str(),
                )
            })
        })
        .collect::<Vec<_>>();
    identities.sort_unstable();
    for (document, version, section) in identities {
        hash_field(&mut hasher, document);
        hash_field(&mut hasher, version);
        hash_field(&mut hasher, section);
    }
    format!("{:x}", hasher.finalize())
}

fn section_vector_id(document: &MemoryDocument, section_id: &str) -> u64 {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, &document.stable_id);
    hash_field(&mut hasher, &document.version_sha256);
    hash_field(&mut hasher, section_id);
    let digest = hasher.finalize();
    u64::from_le_bytes(digest[..8].try_into().expect("SHA-256 has eight bytes"))
}

fn hash_field(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

fn make_section_ref(version: &str, section_id: &str) -> String {
    format!("{SECTION_REF_PREFIX}:{version}:{section_id}")
}

fn parse_section_ref(section_ref: &str) -> Result<(String, String)> {
    let mut parts = section_ref.splitn(3, ':');
    let prefix = parts.next();
    let version = parts.next();
    let section = parts.next();
    match (prefix, version, section) {
        (Some(SECTION_REF_PREFIX), Some(version), Some(section))
            if !version.is_empty() && !section.is_empty() =>
        {
            Ok((version.to_string(), section.to_string()))
        }
        _ => bail!("invalid memory section_ref"),
    }
}

fn refs_for_lines(document: &MemoryDocument, start: u32, end: u32) -> Vec<MemoryRef> {
    document
        .refs
        .iter()
        .filter(|reference| reference.line >= start && reference.line <= end)
        .cloned()
        .collect()
}

fn source_freshness(document: &MemoryDocument) -> (MemoryFreshness, bool) {
    match fs::symlink_metadata(&document.source_path) {
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return (
                MemoryFreshness::Stale {
                    error: format!(
                        "memory source {} is not a regular non-symlink file",
                        document.source_path.display()
                    ),
                },
                true,
            );
        }
        Err(error) => {
            return (
                MemoryFreshness::Stale {
                    error: format!(
                        "cannot inspect memory source {}: {error}",
                        document.source_path.display()
                    ),
                },
                true,
            );
        }
    }
    let canonical = match fs::canonicalize(&document.source_path) {
        Ok(path) => path,
        Err(error) => {
            return (
                MemoryFreshness::Stale {
                    error: format!(
                        "cannot resolve memory source {}: {error}",
                        document.source_path.display()
                    ),
                },
                true,
            );
        }
    };
    if canonical != document.source_path {
        return (
            MemoryFreshness::Stale {
                error: format!(
                    "memory source path identity changed from {} to {}",
                    document.source_path.display(),
                    canonical.display()
                ),
            },
            true,
        );
    }
    let bytes = match fs::read(&canonical) {
        Ok(bytes) => bytes,
        Err(error) => {
            return (
                MemoryFreshness::Stale {
                    error: format!(
                        "cannot read memory source {}: {error}",
                        document.source_path.display()
                    ),
                },
                true,
            );
        }
    };
    let current_version = format!("{:x}", Sha256::digest(&bytes));
    if current_version == document.version_sha256 {
        (MemoryFreshness::Fresh, false)
    } else {
        (
            MemoryFreshness::Stale {
                error: "memory source changed after the snapshot was created".to_string(),
            },
            true,
        )
    }
}

fn make_snippet(text: &str, queries: &[&str]) -> String {
    let start_byte = queries
        .iter()
        .flat_map(|query| query.split_whitespace())
        .filter(|term| term.chars().count() >= 2)
        .filter_map(|term| find_term(text, term))
        .min()
        .unwrap_or(0);
    let start_char = text[..start_byte].chars().count().saturating_sub(120);
    char_range(text, start_char, SEARCH_SNIPPET_CHARS)
}

fn find_term(text: &str, term: &str) -> Option<usize> {
    text.find(term).or_else(|| {
        term.is_ascii().then(|| {
            text.as_bytes()
                .windows(term.len())
                .position(|window| window.eq_ignore_ascii_case(term.as_bytes()))
        })?
    })
}

fn char_range(value: &str, offset: usize, count: usize) -> String {
    value.chars().skip(offset).take(count).collect()
}

fn apply_recency(score: f32, ts: u64, now_ms: u64, weight: f32, half_life_days: f32) -> f32 {
    if score <= 0.0 || weight <= 0.0 || ts == 0 {
        return score;
    }
    let age_ms = now_ms.saturating_sub(ts);
    let age_days = age_ms as f32 / (1000.0 * 60.0 * 60.0 * 24.0);
    let decay = (-std::f32::consts::LN_2 * age_days / half_life_days).exp();
    score * (1.0 + weight * decay)
}

fn default_search_limit() -> usize {
    DEFAULT_SEARCH_LIMIT
}

fn default_max_per_document() -> usize {
    DEFAULT_MAX_PER_DOCUMENT
}

fn default_memory_read_chars() -> usize {
    DEFAULT_MEMORY_READ_CHARS
}

fn default_recency_weight() -> f32 {
    1.0
}

fn default_recency_half_life_days() -> f32 {
    30.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{MemoryScope, MemorySection};
    use tempfile::TempDir;

    fn document(
        source_path: PathBuf,
        stable_id: &str,
        project: &str,
        mtime_ms: u64,
        sections: &[(&str, &str)],
    ) -> MemoryDocument {
        let content = sections
            .iter()
            .map(|(_, content)| *content)
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&source_path, &content).unwrap();
        let source_path = fs::canonicalize(source_path).unwrap();
        let version_sha256 = format!("{:x}", Sha256::digest(content.as_bytes()));
        MemoryDocument {
            provider: SourceKind::Codex,
            stable_id: stable_id.to_string(),
            version_sha256,
            source_path,
            scope: MemoryScope {
                project: Some(project.to_string()),
                cwd: Some(PathBuf::from(format!("/work/{project}"))),
            },
            kind: MemoryDocumentKind::Note,
            mtime_ms,
            event_dates: Vec::new(),
            title: Some(format!("{project} notes")),
            content,
            sections: sections
                .iter()
                .enumerate()
                .map(|(index, (id, content))| MemorySection {
                    id: (*id).to_string(),
                    heading: Some((*id).to_string()),
                    level: 2,
                    start_line: index as u32 + 1,
                    end_line: index as u32 + 1,
                    content: (*content).to_string(),
                })
                .collect(),
            refs: Vec::new(),
            freshness: MemoryFreshness::Fresh,
        }
    }

    fn write_snapshot(paths: &Paths, documents: Vec<MemoryDocument>) {
        let path = memory_snapshot_path(paths);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            path,
            serde_json::to_vec_pretty(&MemorySnapshot {
                schema_version: 1,
                documents,
            })
            .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn explicit_text_projection_includes_section_but_default_stays_compact() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().join("index"))).unwrap();
        let doc = document(
            tmp.path().join("note.md"),
            "test-id",
            "project",
            1,
            &[("section", "precise source text")],
        );
        write_snapshot(&paths, vec![doc]);
        let mut options = MemorySearchOptions {
            query: "precise".into(),
            ..Default::default()
        };
        assert!(search_memory(&paths, &options).unwrap()[0].text.is_none());
        options.include_text = true;
        assert_eq!(
            search_memory(&paths, &options).unwrap()[0].text.as_deref(),
            Some("precise source text")
        );
    }

    #[test]
    fn deleted_memory_source_is_not_served_as_a_cached_document() {
        let tmp = TempDir::new().unwrap();
        let paths = Paths::new(Some(tmp.path().join("index"))).unwrap();
        let doc = document(
            tmp.path().join("note.md"),
            "test-id",
            "project",
            1,
            &[("section", "content that was deleted")],
        );
        let request = MemoryReadRequest {
            memory_id: doc.stable_id.clone(),
            section_ref: None,
            content_version: Some(doc.version_sha256.clone()),
            offset_chars: 0,
            max_chars: 16000,
        };
        let source_path = doc.source_path.clone();
        write_snapshot(&paths, vec![doc]);
        fs::remove_file(source_path).unwrap();
        assert!(
            read_memory(&paths, &request)
                .unwrap_err()
                .to_string()
                .contains("no longer exists")
        );
    }

    #[test]
    fn lexical_search_filters_mtime_and_enforces_document_diversity() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().join("store"))).unwrap();
        write_snapshot(
            &paths,
            vec![
                document(
                    temp.path().join("one.md"),
                    "one",
                    "alpha",
                    100,
                    &[("a", "rust cache design"), ("b", "rust cache operations")],
                ),
                document(
                    temp.path().join("two.md"),
                    "two",
                    "alpha",
                    200,
                    &[("a", "rust cache verification")],
                ),
            ],
        );
        let hits = search_memory(
            &paths,
            &MemorySearchOptions {
                query: "rust cache".to_string(),
                since: Some(100),
                until: Some(200),
                limit: 2,
                max_per_document: 1,
                sort_by_timestamp: true,
                ..MemorySearchOptions::default()
            },
        )
        .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].memory_id, "two");
        assert_ne!(hits[0].memory_id, hits[1].memory_id);
        assert!(hits.iter().all(|hit| !hit.changed_since_search));
    }

    #[test]
    fn query_views_drop_blank_primary_and_deduplicate_trimmed_additions() {
        let options = MemorySearchOptions {
            query: "   ".to_string(),
            additional_queries: vec![" useful ".to_string(), String::new(), "useful".to_string()],
            mode: MemorySearchMode::Semantic,
            ..MemorySearchOptions::default()
        };
        assert_eq!(normalized_query_views(&options).unwrap(), ["useful"]);
        options.validate().unwrap();

        let empty = MemorySearchOptions {
            query: " \n ".to_string(),
            additional_queries: vec!["\t".to_string()],
            ..MemorySearchOptions::default()
        };
        assert!(
            empty
                .validate()
                .unwrap_err()
                .to_string()
                .contains("at least one non-empty")
        );
    }

    #[test]
    fn lexical_star_explicitly_browses_memory_sections() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().join("store"))).unwrap();
        write_snapshot(
            &paths,
            vec![document(
                temp.path().join("one.md"),
                "one",
                "alpha",
                100,
                &[("section", "browseable content")],
            )],
        );
        let hits = search_memory(
            &paths,
            &MemorySearchOptions {
                query: "*".to_string(),
                recency_weight: 0.0,
                ..MemorySearchOptions::default()
            },
        )
        .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].memory_id, "one");
    }

    #[test]
    fn semantic_search_without_current_vectors_is_an_explicit_error() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().join("store"))).unwrap();
        write_snapshot(
            &paths,
            vec![document(
                temp.path().join("one.md"),
                "one",
                "alpha",
                100,
                &[("a", "semantic content")],
            )],
        );
        let error = search_memory(
            &paths,
            &MemorySearchOptions {
                query: "meaning".to_string(),
                mode: MemorySearchMode::Semantic,
                ..MemorySearchOptions::default()
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("memory vectors"));
    }

    #[test]
    fn read_is_unicode_bounded_and_returns_a_continuation() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().join("store"))).unwrap();
        let doc = document(
            temp.path().join("one.md"),
            "one",
            "alpha",
            100,
            &[("unicode", "a🦀bc")],
        );
        let section_ref = make_section_ref(&doc.version_sha256, "unicode");
        write_snapshot(&paths, vec![doc]);
        let value = read_memory(
            &paths,
            &MemoryReadRequest {
                memory_id: "one".to_string(),
                section_ref: Some(section_ref),
                content_version: None,
                offset_chars: 1,
                max_chars: 2,
            },
        )
        .unwrap();
        assert_eq!(value.text, "🦀b");
        assert_eq!(value.content.returned_chars, 2);
        assert_eq!(value.next_offset_chars, Some(3));
    }

    #[test]
    fn stale_section_ref_falls_back_to_current_document() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().join("store"))).unwrap();
        let doc = document(
            temp.path().join("one.md"),
            "one",
            "alpha",
            100,
            &[("new", "current full content")],
        );
        write_snapshot(&paths, vec![doc]);
        let value = read_memory(
            &paths,
            &MemoryReadRequest {
                memory_id: "one".to_string(),
                section_ref: Some(make_section_ref("old-version", "old")),
                content_version: Some("old-version".to_string()),
                offset_chars: 0,
                max_chars: 64,
            },
        )
        .unwrap();
        assert!(value.changed_since_search);
        assert_eq!(value.section_ref, None);
        assert_eq!(value.text, "current full content");
    }

    #[test]
    #[cfg(unix)]
    fn read_rejects_replaced_parent_directory_symlink() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().join("store"))).unwrap();
        let parent = temp.path().join("indexed");
        fs::create_dir(&parent).unwrap();
        let doc = document(
            parent.join("MEMORY.md"),
            "one",
            "alpha",
            100,
            &[("old", "original")],
        );
        write_snapshot(&paths, vec![doc.clone()]);
        let outside = temp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        fs::write(outside.join("MEMORY.md"), "outside replacement").unwrap();
        fs::rename(&parent, temp.path().join("original-directory")).unwrap();
        std::os::unix::fs::symlink(&outside, &parent).unwrap();

        let error = read_memory(
            &paths,
            &MemoryReadRequest {
                memory_id: "one".to_string(),
                section_ref: None,
                content_version: None,
                offset_chars: 0,
                max_chars: 64,
            },
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("path identity changed"),
            "{error:#}"
        );
        assert!(
            reparse_memory_document(&doc)
                .unwrap_err()
                .to_string()
                .contains("path identity changed")
        );
    }

    #[test]
    fn source_change_reparses_current_document_and_resets_offset() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().join("store"))).unwrap();
        let mut doc = document(
            temp.path().join("one.md"),
            "one",
            "alpha",
            100,
            &[("old", "old content")],
        );
        doc.stable_id = crate::memory::memory_stable_id(doc.provider, &doc.source_path);
        doc.freshness = MemoryFreshness::Stale {
            error: "previous transient read failure".to_string(),
        };
        let memory_id = doc.stable_id.clone();
        let old_version = doc.version_sha256.clone();
        let old_ref = make_section_ref(&old_version, "old");
        let source_path = doc.source_path.clone();
        write_snapshot(&paths, vec![doc]);
        fs::write(&source_path, "# Current\n\nnew content").unwrap();

        let value = read_memory(
            &paths,
            &MemoryReadRequest {
                memory_id,
                section_ref: Some(old_ref),
                content_version: Some(old_version),
                offset_chars: 8,
                max_chars: 10,
            },
        )
        .unwrap();
        assert!(value.changed_since_search);
        assert_eq!(value.offset_chars, 0);
        assert_eq!(value.section_ref, None);
        assert_eq!(value.text, "# Current\n");
        let next_offset = value.next_offset_chars.unwrap();
        let current_version = value.content_version.clone();

        let continuation = read_memory(
            &paths,
            &MemoryReadRequest {
                memory_id: value.memory_id,
                section_ref: None,
                content_version: Some(current_version),
                offset_chars: next_offset,
                max_chars: 10,
            },
        )
        .unwrap();
        assert!(!continuation.changed_since_search);
        assert_eq!(continuation.offset_chars, next_offset);
        assert_eq!(continuation.text, "\nnew conte");
    }

    #[test]
    fn existing_complete_vector_snapshot_skips_model_initialization() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().join("store"))).unwrap();
        let doc = document(
            temp.path().join("one.md"),
            "one",
            "alpha",
            100,
            &[("section", "vector content")],
        );
        write_snapshot(&paths, vec![doc]);
        let snapshot = MemoryStore::new(memory_snapshot_path(&paths))
            .load()
            .unwrap();
        let candidates = all_searchable_sections(&snapshot).unwrap();
        let vector_path = memory_vector_path(&paths, &snapshot_fingerprint(&snapshot));
        let mut vector = VectorIndex::empty_replacement(&vector_path, 1, Some("potion")).unwrap();
        vector.add(candidates[0].vector_id, &[1.0]).unwrap();
        vector.save().unwrap();

        assert_eq!(
            embed_memory(&paths, ModelChoice::Potion, &EmbedRuntimeConfig::default()).unwrap(),
            0
        );
    }

    #[test]
    fn vector_gc_preserves_current_snapshot_and_removes_only_old_generated_dirs() {
        let temp = TempDir::new().unwrap();
        let paths = Paths::new(Some(temp.path().join("store"))).unwrap();
        write_snapshot(&paths, Vec::new());
        let snapshot = MemoryStore::new(memory_snapshot_path(&paths))
            .load()
            .unwrap();
        let current = snapshot_fingerprint(&snapshot);
        let old = if current == "0".repeat(64) {
            "1".repeat(64)
        } else {
            "0".repeat(64)
        };
        let root = paths.root.join("memory").join(MEMORY_VECTOR_DIR);
        fs::create_dir_all(root.join(&current)).unwrap();
        fs::create_dir_all(root.join(&old)).unwrap();

        assert_eq!(gc_memory_vectors(&paths, true).unwrap(), 1);
        assert!(root.join(&old).is_dir());
        assert_eq!(gc_memory_vectors(&paths, false).unwrap(), 1);
        assert!(root.join(&current).is_dir());
        assert!(!root.join(&old).exists());
    }
}
