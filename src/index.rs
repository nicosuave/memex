use crate::types::Record;
use anyhow::{Result, anyhow};
use std::ops::Bound;
use tantivy::collector::TopDocs;
use tantivy::query::{AllQuery, BooleanQuery, Occur, Query, RangeQuery, TermQuery};
use tantivy::schema::Value;
use tantivy::schema::{
    FAST, Field, INDEXED, IndexRecordOption, STORED, STRING, Schema, SchemaBuilder, TEXT,
    TextFieldIndexing, TextOptions,
};
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument, Term};

#[derive(Clone)]
pub struct IndexFields {
    pub doc_id: Field,
    pub ts: Field,
    pub project: Field,
    pub session_id: Field,
    pub turn_id: Field,
    pub role: Field,
    pub text: Field,
    pub source: Option<Field>,
    pub tool_name: Field,
    pub tool_input: Field,
    pub tool_output: Field,
    pub source_path: Field,
}

#[derive(Clone)]
pub struct SearchIndex {
    pub index: Index,
    pub fields: IndexFields,
}

#[derive(Debug, Clone)]
pub struct QueryOptions {
    pub query: String,
    pub project: Option<String>,
    pub role: Option<String>,
    pub tool: Option<String>,
    pub session_id: Option<String>,
    pub source: Option<crate::types::SourceFilter>,
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub limit: usize,
}

impl SearchIndex {
    pub fn open_or_create(dir: &std::path::Path) -> Result<Self> {
        let meta_path = dir.join("meta.json");
        if meta_path.exists() {
            let index = Index::open_in_dir(dir)?;
            let fields = load_fields(index.schema())?;
            Ok(Self { index, fields })
        } else {
            let schema = build_schema()?;
            let index = Index::create_in_dir(dir, schema.clone())?;
            let fields = load_fields(schema)?;
            Ok(Self { index, fields })
        }
    }

    pub fn writer(&self) -> Result<IndexWriter> {
        Ok(self.index.writer(256_000_000)?)
    }

    pub fn reader(&self) -> Result<IndexReader> {
        Ok(self.index.reader()?)
    }

    pub fn delete_by_source_path(&self, writer: &mut IndexWriter, path: &str) {
        let term = Term::from_field_text(self.fields.source_path, path);
        writer.delete_term(term);
    }

    pub fn add_record(&self, writer: &mut IndexWriter, record: &Record) -> Result<()> {
        let mut doc = TantivyDocument::default();
        doc.add_u64(self.fields.doc_id, record.doc_id);
        doc.add_u64(self.fields.ts, record.ts);
        doc.add_text(self.fields.project, &record.project);
        doc.add_text(self.fields.session_id, &record.session_id);
        doc.add_u64(self.fields.turn_id, record.turn_id as u64);
        doc.add_text(self.fields.role, &record.role);
        doc.add_text(self.fields.text, &record.text);
        if let Some(field) = self.fields.source {
            doc.add_text(field, record.source.label());
        }
        if let Some(tool_name) = &record.tool_name {
            doc.add_text(self.fields.tool_name, tool_name);
        }
        if let Some(tool_input) = &record.tool_input {
            doc.add_text(self.fields.tool_input, tool_input);
        }
        if let Some(tool_output) = &record.tool_output {
            doc.add_text(self.fields.tool_output, tool_output);
        }
        doc.add_text(self.fields.source_path, &record.source_path);
        writer.add_document(doc)?;
        Ok(())
    }

    pub fn get_by_doc_id(&self, doc_id: u64) -> Result<Option<Record>> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let term = Term::from_field_u64(self.fields.doc_id, doc_id);
        let query = TermQuery::new(term, IndexRecordOption::Basic);
        let top = searcher.search(&query, &TopDocs::with_limit(1))?;
        let Some((_, addr)) = top.first() else {
            return Ok(None);
        };
        let doc = searcher.doc::<TantivyDocument>(*addr)?;
        Ok(Some(record_from_doc(&self.fields, &doc)))
    }

    pub fn search(&self, options: &QueryOptions) -> Result<Vec<(f32, Record)>> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let query = build_query(&self.fields, options, &self.index)?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(options.limit))?;
        let mut results = Vec::with_capacity(top_docs.len());
        for (score, addr) in top_docs {
            let doc = searcher.doc::<TantivyDocument>(addr)?;
            results.push((score, record_from_doc(&self.fields, &doc)));
        }
        Ok(results)
    }

    pub fn records_by_session_id(&self, session_id: &str) -> Result<Vec<Record>> {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        let term = Term::from_field_text(self.fields.session_id, session_id);
        let query = TermQuery::new(term, IndexRecordOption::Basic);
        let limit = searcher.num_docs() as usize;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;
        let mut records = Vec::with_capacity(top_docs.len());
        for (_score, addr) in top_docs {
            let doc = searcher.doc::<TantivyDocument>(addr)?;
            records.push(record_from_doc(&self.fields, &doc));
        }
        Ok(records)
    }

    pub fn doc_count(&self) -> Result<usize> {
        let reader = self.reader()?;
        Ok(reader.searcher().num_docs() as usize)
    }

    pub fn for_each_record<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(Record) -> Result<()>,
    {
        let reader = self.reader()?;
        let searcher = reader.searcher();
        for segment_reader in searcher.segment_readers() {
            let store = segment_reader.get_store_reader(0)?;
            for doc in store.iter::<TantivyDocument>(segment_reader.alive_bitset()) {
                let doc = doc?;
                let record = record_from_doc(&self.fields, &doc);
                f(record)?;
            }
        }
        Ok(())
    }
}

fn build_schema() -> Result<Schema> {
    let mut builder = SchemaBuilder::default();

    builder.add_u64_field("doc_id", INDEXED | STORED | FAST);
    builder.add_u64_field("ts", INDEXED | STORED | FAST);
    builder.add_text_field("project", STRING | STORED);
    builder.add_text_field("session_id", STRING | STORED);
    builder.add_u64_field("turn_id", INDEXED | STORED | FAST);
    builder.add_text_field("role", STRING | STORED);
    builder.add_text_field("source", STRING | STORED);

    let text_indexing = TextFieldIndexing::default()
        .set_tokenizer("default")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);
    let text_options = TextOptions::default()
        .set_indexing_options(text_indexing)
        .set_stored();
    builder.add_text_field("text", text_options);

    builder.add_text_field("tool_name", STRING | STORED);
    builder.add_text_field("tool_input", TEXT | STORED);
    builder.add_text_field("tool_output", TEXT | STORED);
    builder.add_text_field("source_path", STRING | STORED);

    Ok(builder.build())
}

fn load_fields(schema: Schema) -> Result<IndexFields> {
    let get = |name: &str| {
        schema
            .get_field(name)
            .map_err(|_| anyhow!(format!("missing field {name}")))
    };
    Ok(IndexFields {
        doc_id: get("doc_id")?,
        ts: get("ts")?,
        project: get("project")?,
        session_id: get("session_id")?,
        turn_id: get("turn_id")?,
        role: get("role")?,
        text: get("text")?,
        source: schema.get_field("source").ok(),
        tool_name: get("tool_name")?,
        tool_input: get("tool_input")?,
        tool_output: get("tool_output")?,
        source_path: get("source_path")?,
    })
}

fn build_query(
    fields: &IndexFields,
    options: &QueryOptions,
    index: &Index,
) -> Result<Box<dyn Query>> {
    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

    if options.query.trim().is_empty() {
        clauses.push((Occur::Must, Box::new(AllQuery)));
    } else {
        let parser = tantivy::query::QueryParser::for_index(index, vec![fields.text]);
        let text_query = parser.parse_query(&options.query)?;
        clauses.push((Occur::Must, text_query));
    }

    if let Some(project) = &options.project {
        let term = Term::from_field_text(fields.project, project);
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }

    if let Some(role) = &options.role {
        let term = Term::from_field_text(fields.role, role);
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }

    if let Some(tool) = &options.tool {
        let term = Term::from_field_text(fields.tool_name, tool);
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }

    if let Some(source) = options.source
        && let Some(field) = fields.source
    {
        let term = Term::from_field_text(field, source.as_str());
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }

    if let Some(session_id) = &options.session_id {
        let term = Term::from_field_text(fields.session_id, session_id);
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }

    if options.since.is_some() || options.until.is_some() {
        let start = options.since.unwrap_or(0);
        let end = options.until.unwrap_or(u64::MAX);
        let range = RangeQuery::new_u64_bounds(
            "ts".to_string(),
            Bound::Included(start),
            Bound::Included(end),
        );
        clauses.push((Occur::Must, Box::new(range)));
    }

    Ok(Box::new(BooleanQuery::new(clauses)))
}

fn record_from_doc(fields: &IndexFields, doc: &TantivyDocument) -> Record {
    let get_str = |field: Field| -> Option<String> {
        doc.get_first(field)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    };

    let get_u64 =
        |field: Field| -> u64 { doc.get_first(field).and_then(|v| v.as_u64()).unwrap_or(0) };

    let source_path = get_str(fields.source_path).unwrap_or_default();
    let source = crate::types::SourceKind::from_path(&source_path);
    Record {
        source,
        doc_id: get_u64(fields.doc_id),
        ts: get_u64(fields.ts),
        project: get_str(fields.project).unwrap_or_default(),
        session_id: get_str(fields.session_id).unwrap_or_default(),
        turn_id: get_u64(fields.turn_id) as u32,
        role: get_str(fields.role).unwrap_or_default(),
        text: get_str(fields.text).unwrap_or_default(),
        tool_name: get_str(fields.tool_name),
        tool_input: get_str(fields.tool_input),
        tool_output: get_str(fields.tool_output),
        source_path,
    }
}
