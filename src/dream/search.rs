//! Lexical search over the dreams of the current generation. The corpus is small and
//! replaced wholesale by each run, so the index is built in memory per query.

use super::model::{Citation, CitationTarget, Dream, DreamStatus, clip_chars};
use super::store::DreamStore;
use crate::config::Paths;
use crate::machine::LOCAL_MACHINE_ID;
use crate::types::SourceKind;
use anyhow::{Result, anyhow, bail};
use serde::Serialize;
use tantivy::collector::TopDocs;
use tantivy::query::{AllQuery, Query, QueryParser};
use tantivy::schema::{
    INDEXED, IndexRecordOption, STORED, Schema, TextFieldIndexing, TextOptions, Value,
};
use tantivy::{Index, ReloadPolicy, TantivyDocument};

const MAX_SEARCH_LIMIT: usize = 500;
const SNIPPET_CHARS: usize = 600;
/// Citations returned inline with a hit; `memex dream show` lists them all.
const HIT_CITATIONS: usize = 5;

#[derive(Clone, Debug, Default)]
pub struct DreamSearchOptions {
    pub query: String,
    /// Dreams about this project, plus dreams that hold across projects: a standing
    /// preference applies inside every project scope.
    pub project: Option<String>,
    /// Only dreams with at least one citation from this agent.
    pub source: Option<SourceKind>,
    /// Inclusive bounds over the newest evidence of a dream, in Unix milliseconds.
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub limit: usize,
    pub sort_by_timestamp: bool,
    /// Also return superseded and retracted dreams.
    pub include_inactive: bool,
}

/// A citation as search returns it: enough to read the source with `memex show`.
#[derive(Clone, Debug, Serialize)]
pub struct CitationRef {
    pub machine: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub source: String,
    pub ts: u64,
    pub state: String,
    pub excerpt: String,
}

impl From<&Citation> for CitationRef {
    fn from(citation: &Citation) -> Self {
        let (record_id, memory_id, session_id, source) = match &citation.target {
            CitationTarget::Record {
                rid,
                session_id,
                source,
                ..
            } => (Some(rid.clone()), None, Some(session_id.clone()), *source),
            CitationTarget::Memory {
                memory_id, source, ..
            } => (None, Some(memory_id.clone()), None, *source),
        };
        Self {
            machine: citation.machine.clone(),
            record_id,
            memory_id,
            session_id,
            source: source.label().to_string(),
            ts: citation.ts,
            state: citation.state.label().to_string(),
            excerpt: citation.excerpt.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct DreamSearchHit {
    /// Always `dream`: a model's conclusion, not a source record.
    pub kind: &'static str,
    pub machine: String,
    pub score: f32,
    /// Timestamp of the newest evidence.
    pub ts: u64,
    pub dream_id: String,
    pub dream_kind: String,
    pub status: String,
    /// How much of the evidence can still be read: supported, partial, unverified, lost.
    pub evidence: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub snippet: String,
    pub generation: String,
    pub reinforced: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<String>,
    pub citation_count: usize,
    pub citations: Vec<CitationRef>,
}

fn hit(dream: &Dream, generation: &str, score: f32) -> DreamSearchHit {
    // Newest readable evidence first: it is what a reader should open.
    let mut citations = dream.citations.iter().collect::<Vec<_>>();
    citations.sort_by_key(|citation| (!citation.state.supports(), std::cmp::Reverse(citation.ts)));
    DreamSearchHit {
        kind: "dream",
        machine: LOCAL_MACHINE_ID.to_string(),
        score,
        ts: dream.last_evidence_ts(),
        dream_id: dream.id.clone(),
        dream_kind: dream.kind.label().to_string(),
        status: dream.status.label().to_string(),
        evidence: dream.evidence_health().label().to_string(),
        project: dream.project.clone(),
        snippet: clip_chars(&dream.text, SNIPPET_CHARS),
        generation: generation.to_string(),
        reinforced: dream.reinforced,
        supersedes: dream.supersedes.clone(),
        superseded_by: dream.superseded_by.clone(),
        citation_count: dream.citations.len(),
        citations: citations
            .into_iter()
            .take(HIT_CITATIONS)
            .map(CitationRef::from)
            .collect(),
    }
}

pub fn search_dreams(paths: &Paths, options: &DreamSearchOptions) -> Result<Vec<DreamSearchHit>> {
    if options.limit == 0 || options.limit > MAX_SEARCH_LIMIT {
        bail!("dream search limit must be between 1 and {MAX_SEARCH_LIMIT}");
    }
    let Some(generation) = DreamStore::new(paths).current()? else {
        return Ok(Vec::new());
    };
    let eligible = generation
        .dreams
        .iter()
        .filter(|dream| options.include_inactive || dream.status == DreamStatus::Active)
        .filter(|dream| {
            options.project.as_deref().is_none_or(|project| {
                dream
                    .project
                    .as_deref()
                    .is_none_or(|scope| scope == project)
            })
        })
        .filter(|dream| {
            options.source.is_none_or(|source| {
                dream
                    .citations
                    .iter()
                    .any(|citation| citation.source() == source)
            })
        })
        .filter(|dream| {
            let ts = dream.last_evidence_ts();
            options.since.is_none_or(|since| ts >= since)
                && options.until.is_none_or(|until| ts <= until)
        })
        .collect::<Vec<_>>();
    if eligible.is_empty() {
        return Ok(Vec::new());
    }

    let mut schema = Schema::builder();
    let key = schema.add_u64_field("key", INDEXED | STORED);
    let stemmed = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("en_stem")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    let text = schema.add_text_field("text", stemmed.clone());
    let labels = schema.add_text_field("labels", stemmed);
    let index = Index::create_in_ram(schema.build());
    let mut writer = index.writer(15_000_000)?;
    for (position, dream) in eligible.iter().enumerate() {
        let mut row = TantivyDocument::default();
        row.add_u64(key, position as u64);
        row.add_text(text, &dream.text);
        row.add_text(
            labels,
            format!(
                "{} {}",
                dream.kind.label().replace('_', " "),
                dream.project.as_deref().unwrap_or_default()
            ),
        );
        writer.add_document(row)?;
    }
    writer.commit()?;
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();
    let query: Box<dyn Query> = if matches!(options.query.trim(), "" | "*") {
        Box::new(AllQuery)
    } else {
        let parser = QueryParser::for_index(&index, vec![text, labels]);
        crate::index::parse_text_query(&parser, &options.query)?
    };
    let mut hits = searcher
        .search(&query, &TopDocs::with_limit(eligible.len()))?
        .into_iter()
        .map(|(score, address)| {
            let row = searcher.doc::<TantivyDocument>(address)?;
            let position = row
                .get_first(key)
                .and_then(|value| value.as_u64())
                .ok_or_else(|| anyhow!("dream search row is missing its key"))?;
            Ok(hit(eligible[position as usize], &generation.id, score))
        })
        .collect::<Result<Vec<_>>>()?;
    if options.sort_by_timestamp {
        hits.sort_by(|left, right| {
            right
                .ts
                .cmp(&left.ts)
                .then_with(|| left.dream_id.cmp(&right.dream_id))
        });
    } else {
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| right.ts.cmp(&left.ts))
                .then_with(|| left.dream_id.cmp(&right.dream_id))
        });
    }
    hits.truncate(options.limit);
    Ok(hits)
}
