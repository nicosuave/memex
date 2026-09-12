//! Context selection metadata, kept separate from stored text and tool payloads.
//!
//! Exact indexed strings and numeric fast fields avoid session-wide stored-document reads on
//! current indexes. A unique anchor adds one stored read. Selection still traverses segment term
//! dictionaries and matching session metadata;
//! it is a hydration bound, not a constant-time context lookup. Older projections without
//! canonical IDs or numeric fast fields retain the stored-record compatibility fallback.

use super::*;
use crate::retrieval::{ContextSelector, canonical_record_id};
use std::collections::HashMap;
use tantivy::collector::DocSetCollector;
use tantivy::{DocAddress, DocSet, Searcher, TERMINATED};

#[cfg(test)]
thread_local! {
    pub(crate) static STORED_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(crate) struct ContextEntry {
    pub id: String,
    pub record: Record,
    address: DocAddress,
}

pub(crate) struct ContextReader<'a> {
    fields: &'a IndexFields,
    searcher: Searcher,
}

impl<'a> ContextReader<'a> {
    pub fn new(index: &'a SearchIndex) -> Result<Self> {
        Ok(Self {
            fields: &index.fields,
            searcher: index.reader()?.searcher(),
        })
    }

    pub fn candidates(&self, selector: &ContextSelector) -> Result<Vec<ContextEntry>> {
        let (term, session, source) = match selector {
            ContextSelector::RecordId {
                id,
                session_id,
                source,
            } => (
                self.fields
                    .canonical_record_id
                    .map(|field| Term::from_field_text(field, id)),
                session_id,
                source,
            ),
            ContextSelector::DocId {
                id,
                session_id,
                source,
            } => (
                Some(Term::from_field_u64(self.fields.doc_id, *id)),
                session_id,
                source,
            ),
            ContextSelector::EventId {
                id,
                session_id,
                source,
            } => (
                Some(Term::from_field_text(self.fields.event_id, id)),
                session_id,
                source,
            ),
        };
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        if let Some(term) = term {
            clauses.push((
                Occur::Must,
                Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
            ));
        }
        if let Some(session) = session {
            clauses.push((
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_text(self.fields.session_id, session),
                    IndexRecordOption::Basic,
                )),
            ));
        }
        if let Some(source) = source
            && let Some(query) = exact_source_query(self.fields, *source)
        {
            clauses.push((Occur::Must, query));
        }
        let query: Box<dyn Query> = if clauses.is_empty() {
            Box::new(AllQuery)
        } else {
            Box::new(BooleanQuery::new(clauses))
        };
        Ok(self
            .entries(query.as_ref(), None, true)?
            .into_iter()
            .filter(|entry| {
                session
                    .as_deref()
                    .is_none_or(|session| entry.record.session_id == session)
                    && source.is_none_or(|source| entry.record.source == source)
                    && match selector {
                        ContextSelector::RecordId { id, .. } => entry.id == *id,
                        ContextSelector::DocId { id, .. } => entry.record.doc_id == *id,
                        ContextSelector::EventId { id, .. } => {
                            entry.record.links.event_id.as_deref() == Some(id.as_str())
                        }
                    }
            })
            .collect())
    }

    pub fn session(
        &self,
        anchor: &ContextEntry,
        expand_interactions: bool,
    ) -> Result<Vec<ContextEntry>> {
        let record = &anchor.record;
        let query = session_query(
            self.fields,
            &record.session_id,
            Some(&record.source_path),
            Some(record.source),
            None,
        );
        Ok(self
            .entries(&query, Some(record), expand_interactions)?
            .into_iter()
            // Legacy stored projections may infer source from the path. Preserve the
            // in-memory isolation check even when the indexed source filter is unavailable.
            .filter(|entry| entry.record.source == record.source)
            .collect())
    }

    pub fn hydrate(&self, entry: ContextEntry) -> Result<Record> {
        self.read_record(entry.address)
    }

    fn entries(
        &self,
        query: &dyn Query,
        scope: Option<&Record>,
        interactions: bool,
    ) -> Result<Vec<ContextEntry>> {
        let mut addresses = self
            .searcher
            .search(query, &DocSetCollector)?
            .into_iter()
            .collect::<Vec<_>>();
        addresses.sort();
        // Resolving one exact candidate has a fixed one-record hydration cost and avoids
        // walking a segment dictionary just to recover its source/session/path.
        if scope.is_none() && addresses.len() == 1 {
            return Ok(vec![self.stored_entry(addresses[0])?]);
        }
        let mut entries = Vec::with_capacity(addresses.len());
        for (ordinal, segment) in self.searcher.segment_readers().iter().enumerate() {
            let matching = addresses
                .iter()
                .filter(|address| address.segment_ord == ordinal as u32)
                .copied()
                .collect::<Vec<_>>();
            if matching.is_empty() {
                continue;
            }
            let numeric = (
                segment.fast_fields().u64("doc_id"),
                segment.fast_fields().u64("ts"),
                segment.fast_fields().u64("turn_id"),
            );
            let (Some(canonical), (Ok(doc_ids), Ok(timestamps), Ok(turns))) =
                (self.fields.canonical_record_id, numeric)
            else {
                for address in matching {
                    entries.push(self.stored_entry(address)?);
                }
                continue;
            };
            let positions = matching
                .iter()
                .enumerate()
                .map(|(position, address)| (address.doc_id, position))
                .collect::<HashMap<_, _>>();
            let mut docs = vec![TantivyDocument::default(); matching.len()];
            // Only exact metadata fields: never traverse the text/tool payload indexes.
            let mut fields = vec![Some(canonical)];
            if scope.is_none() {
                fields.extend([
                    Some(self.fields.session_id),
                    Some(self.fields.source_path),
                    self.fields.source,
                ]);
            }
            if interactions {
                fields.extend([
                    Some(self.fields.role),
                    Some(self.fields.event_id),
                    Some(self.fields.parent_tool_use_id),
                ]);
            }
            for field in fields.into_iter().flatten() {
                let inverted = segment.inverted_index(field)?;
                let mut terms = inverted.terms().stream()?;
                while terms.advance() {
                    let mut postings = inverted
                        .read_postings_from_terminfo(terms.value(), IndexRecordOption::Basic)?;
                    if postings.doc() < matching[0].doc_id {
                        postings.seek(matching[0].doc_id);
                    }
                    while postings.doc() != TERMINATED
                        && postings.doc() <= matching[matching.len() - 1].doc_id
                    {
                        if let Some(position) = positions.get(&postings.doc()) {
                            docs[*position].add_text(field, std::str::from_utf8(terms.key())?);
                        }
                        postings.advance();
                    }
                }
            }
            for (address, mut doc) in matching.into_iter().zip(docs) {
                let Some(id) = doc
                    .get_first(canonical)
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
                else {
                    entries.push(self.stored_entry(address)?);
                    continue;
                };
                doc.add_u64(
                    self.fields.doc_id,
                    doc_ids.first(address.doc_id).unwrap_or(0),
                );
                doc.add_u64(
                    self.fields.ts,
                    timestamps.first(address.doc_id).unwrap_or(0),
                );
                doc.add_u64(
                    self.fields.turn_id,
                    turns.first(address.doc_id).unwrap_or(0),
                );
                let mut record = record_from_doc(self.fields, &doc);
                if let Some(scope) = scope {
                    // The query fixes the exact path, so source inference is identical even
                    // on schemas without an indexed source field.
                    record.session_id.clone_from(&scope.session_id);
                    record.source_path.clone_from(&scope.source_path);
                    record.source = scope.source;
                }
                entries.push(ContextEntry {
                    id,
                    record,
                    address,
                });
            }
        }
        Ok(entries)
    }

    fn stored_entry(&self, address: DocAddress) -> Result<ContextEntry> {
        let mut record = self.read_record(address)?;
        let id = canonical_record_id(&record);
        // Legacy compatibility may require reading payloads, but do not retain them for the
        // whole session. Selected records are read again from this same snapshot.
        record.text.clear();
        record.text.shrink_to_fit();
        record.tool_input = None;
        record.tool_output = None;
        record.links.source_content = None;
        Ok(ContextEntry {
            id,
            record,
            address,
        })
    }

    fn read_record(&self, address: DocAddress) -> Result<Record> {
        #[cfg(test)]
        STORED_READS.with(|reads| reads.set(reads.get() + 1));
        let doc = self.searcher.doc::<TantivyDocument>(address)?;
        Ok(record_from_doc(self.fields, &doc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retrieval::{ContextOptions, context_records};

    #[test]
    fn legacy_context_preserves_windows_without_rebuilding() {
        for canonical in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let mut schema =
                serde_json::to_value(build_schema_with_canonical_record_id(canonical).unwrap())
                    .unwrap();
            // Exercise both missing canonical identity and pre-fast-doc-ID projections.
            if canonical {
                for field in schema.as_array_mut().unwrap() {
                    if field["name"] == "doc_id" {
                        field["options"]["fast"] = false.into();
                    }
                }
            }
            let schema: Schema = serde_json::from_value(schema).unwrap();
            drop(Index::create_in_dir(tmp.path(), schema).unwrap());
            let index = SearchIndex::open_or_create(tmp.path()).unwrap();
            let mut writer = index.writer().unwrap();
            let mut records = Vec::new();
            for turn in 1..=4 {
                let record = Record {
                    source: crate::types::SourceKind::Codex,
                    doc_id: turn as u64,
                    ts: turn as u64,
                    turn_id: turn,
                    project: "project".into(),
                    session_id: "session".into(),
                    role: "assistant".into(),
                    text: format!("record {turn}"),
                    source_path: "session.jsonl".into(),
                    tool_name: None,
                    tool_input: Some("input".into()),
                    tool_output: Some("output".into()),
                    links: RecordLinks::default(),
                };
                index.add_record(&mut writer, &record).unwrap();
                records.push(record);
            }
            writer.commit().unwrap();
            let selector = ContextSelector::record_id(canonical_record_id(&records[1])).with_scope(
                Some("session".into()),
                Some(crate::types::SourceKind::Codex),
            );
            let result = context_records(
                &index,
                &selector,
                ContextOptions {
                    before: 0,
                    after: 1,
                    expand_interactions: false,
                },
            )
            .unwrap();
            assert_eq!(result.records.len(), 2);
            for (actual, expected) in result.records.iter().zip(&records[1..3]) {
                assert_eq!(
                    serde_json::to_value(&actual.record).unwrap(),
                    serde_json::to_value(expected).unwrap()
                );
            }
            assert_eq!(result.records[0].distance, 0);
            assert_eq!(result.records[1].distance, 1);
            assert_eq!(index.fields.canonical_record_id.is_some(), canonical);
        }
    }
}
