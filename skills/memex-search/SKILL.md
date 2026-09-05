---
name: memex-search
description: Discover prior agent work across sessions, projects, providers, and machines. Use for historical investigations and analogous solutions, or as a fallback when native conversation and history tools cannot recover the needed evidence.
allowed-tools: Bash(memex:*)
---

# Memex Search

Use memex as an episodic retrieval system, not as a one-shot search box.

The goal is to recover the smallest set of source-grounded records or trajectories that actually answer the user's question. Search iteratively when needed, but stop once the evidence is sufficient.

## Core Rules

For recovery within the current active task, use native notes/history and the task worklog
first. Read known Codex or ChatGPT conversations with native conversation tools when
available. Use Memex when those sources are unavailable or insufficient, and for discovery
across prior sessions, projects, providers, or machines.

1. **Retrieve adaptively.** A known session ID needs no search. An exact filename or error may need one lexical query. An ambiguous historical question may need several query views and one or two reformulation rounds.
2. **Search for evidence, not an answer-shaped snippet.** Search rank is only a candidate generator. Inspect the record or trajectory before making claims.
3. **Prefer exact anchors when they exist.** Paths, symbols, commands, error strings, PR numbers, URLs, model names, table names, and quoted user phrasing usually beat semantic search.
4. **Use multiple independent query views for ambiguous requests.** Do not stuff every synonym into one giant query.
5. **Reformulate from retrieved evidence.** A first-pass hit often exposes the exact filename, command, error text, or terminology needed for the second pass.
6. **Preserve chronology and interaction boundaries.** When the question is about what happened or how something was fixed, reconstruct the relevant sequence; do not summarize isolated top-ranked messages.
7. **Treat outcome evidence asymmetrically.** Prefer tool results and explicit user confirmation over the assistant claiming that its own work succeeded.
8. **Do not confuse retrieval failure with absence.** If a reasonable search fails, say you did not find it. Do not conclude that it never happened.
9. **Do not dump transcripts into context reflexively.** Retrieve nuclei first; load a full session only when the question requires trajectory-level context.
10. **Do not repeatedly re-index during one task.** Refresh once only when freshness matters.

These rules intentionally mirror the strongest recurring ideas in adaptive and iterative retrieval work: choose retrieval depth based on query complexity, explicitly rewrite/decompose ambiguous queries, and interleave reasoning with search rather than committing to a single initial query.

## Step 0: Classify the Retrieval Task

Choose the cheapest strategy that can answer the request.

| User intent | First move |
| --- | --- |
| Has a session ID | Read the first bounded page with `memex session <id>` |
| "recent sessions", "resume", "the session from this repo" | `memex sessions` |
| Exact path/symbol/error/command/PR | lexical `memex search` |
| Fuzzy concept or remembered topic | hybrid search if vectors exist |
| "what did we decide" | search candidate sessions, then reconstruct decision context |
| "how did we fix/debug X" | search failures/outcomes, then reconstruct the recovery sequence |
| "find similar prior work" | search both topic and mechanism/task shape |
| Cross-session synthesis | diversify by session before hydrating transcripts |
| Latest/recent history | refresh once if necessary, then use date filters / timestamp sort |

### Retrieval budget

Use this as a default, not a rigid quota:

- **Tier 0 — direct:** known session/doc ID; no discovery search.
- **Tier 1 — simple:** one navigation or lexical query, then inspect one record/session.
- **Tier 2 — ambiguous:** 2-3 distinct query views, deduplicated by session, then inspect the best 1-3 sessions.
- **Tier 3 — multi-hop/synthesis:** decompose the question, search each information need, and inspect enough independent sessions to cover them.

Stop after at most **two reformulation rounds** unless the user explicitly asks for exhaustive research.

## Step 1: Build a Retrieval Packet

Before searching, silently extract:

- **Target:** what fact, decision, episode, solution, chronology, or artifact is needed?
- **Scope:** current repo, named project, all projects, source, machine, and time window.
- **Hard anchors:** exact identifiers likely to occur verbatim.
- **Concepts:** semantic topic when wording may differ from the transcript.
- **Mechanism/task shape:** what kind of work happened, independent of topic words.
- **Evidence requirement:** what would count as enough evidence to answer?

For example, "did we ever solve the stale Tantivy vector IDs after parser upgrades?" contains:

- hard anchors: `Tantivy`, `vector`, `parser`
- concept: stale semantic-search state after reparsing
- mechanism: invalidation/rebuild on parser-version migration
- evidence needed: prior diagnosis plus implemented/reported resolution

Do not expose this packet unless it helps explain a complex search.

## Step 2: Scope Before Searching When Scope Is Cheap

### Current repository / "work we did here"

Scope search directly to the current working directory or repository:

```bash
memex search "query" --cwd . --unique-session --limit 20
```

Use session metadata first when the goal is navigation, resumption, or discovering recent work in the repository:

```bash
memex sessions --cwd . --limit 30 --json-array
```

Add `--since`, `--source`, or `--project` when the user supplied them. Use the returned project/git-root metadata to avoid searching unrelated repositories.

### Recent / resume-oriented requests

```bash
memex sessions --limit 20 --json-array
memex sessions --cwd . --limit 10 --json-array
memex sessions --source codex --since 2026-08-01 --limit 20 --json-array
```

`memex sessions` returns `resume_cmd`; use it when the user's goal is navigation or resumption rather than factual retrieval.

### Freshness

`memex search` may auto-index depending on config; `memex sessions` does not. If the user explicitly asks about work from the last few minutes/hours and the index appears stale, refresh once:

```bash
memex index
```

Do not refresh again in the same retrieval task.

## Step 3: Choose Search Modes Deliberately

### Lexical: exact anchors

Use normal `memex search` for:

- filenames and paths
- function/type/table names
- exact error strings
- shell commands or flags
- PR/issue numbers
- URLs
- unusual proper nouns or quoted phrasing

```bash
memex search "immutable index generations" --unique-session --limit 20
memex search "source_tool_use_id" --project memex --unique-session --limit 20
```

### Hybrid: concept + wording uncertainty

Hybrid combines BM25 and vectors with reciprocal-rank fusion. Prefer it when some terms should match literally but the remembered wording may differ:

```bash
memex search "remote immutable indexes object storage" --hybrid --unique-session --limit 20
```

If memex warns that vectors are unavailable and falls back to lexical retrieval, continue if lexical evidence is adequate. Mention `memex embed` only when semantic recall is materially important.

### Semantic: low lexical overlap

Use `--semantic` selectively for abstract similarity or analogous work with few trustworthy literal anchors:

```bash
memex search "avoiding repeated context reconstruction across agent sessions" --semantic --unique-session --limit 20
```

Do not use semantic search for exact identifiers when lexical search is clearly better.

### Hypothesis query: last-resort semantic expansion

If the user's wording is too abstract and the first pass is weak, formulate one short description of what a relevant historical episode would likely be about and use it **only as a semantic/hybrid query**. Generated terms are retrieval probes, not evidence.

## Step 4: Use Query Views, Not Keyword Soup

For Tier 2/3 requests, issue a small set of distinct searches. Good query views are:

1. **Anchor view** — exact nouns, identifiers, errors, paths.
2. **Concept view** — what the user means if wording changed.
3. **Mechanism view** — the implementation/reasoning pattern rather than surface topic.
4. **Outcome/recovery view** — success, failure, fix, rollback, approval, or correction when the question depends on outcome.
5. **Contrast view** — a disambiguator when two projects/tools/approaches are easily confused.

Example: user asks, "what did we figure out about moving old memex indexes to S3?"

```bash
memex search "S3 index" --project memex --unique-session --limit 15
memex search "remote object storage old index generations" --project memex --hybrid --unique-session --limit 15
memex search "immutable generation upload local cache remote" --project memex --hybrid --unique-session --limit 15
```

Do **not** replace those with one brittle query containing every possible synonym.

### Decompose compound questions

If the request has independently answerable parts, search them separately. For example:

- architecture we considered
- why we rejected/accepted it
- what implementation work remains

This is especially important for "compare what we thought then vs now" and other multi-hop historical questions.

## Step 5: Diversify at the Session Level

During discovery, default to one hit per session:

```bash
memex search "query" \
  --unique-session \
  --limit 20
```

Search already returns a compact projection by default: `machine`, `score`, `ts`,
`doc_id`, `record_id`, `project`, `role`, `session_id`, `source`, `source_path`,
`snippet`, and `matches`. Lexical snippets center a literal match; semantic-only results
fall back to a compact prefix. Use `--fields` only when a different explicit projection
helps, and `--full` only when discovery itself requires all stored fields.

Use `memex search "query" --format toon` for TOON-encoded agent input. It preserves
search fields in a `results` array and works with `--fields` or `--full`. JSONL remains
the default for scripts; `--format json` returns a JSON array. Do not combine explicit
`--format` with `--json-array` or `-v`.

Use `--top-n-per-session 2` when two nuclei per session are helpful.

Rank candidate sessions using more than raw score:

1. exact anchor match
2. repository/time/source fit
3. evidence role (`tool_result` or explicit user statement can dominate assistant narration)
4. agreement across multiple query views
5. mechanism/task-shape similarity
6. recency, but only when recency matters to the question

Do not let ten near-duplicate hits from one session crowd out the rest of the corpus.

## Step 6: Hydrate Context Progressively

### Inspect one exact record

```bash
memex show <doc_id>
memex show --record-id <record_id> --machine <machine_id>
```

The default read budget is 16,000 Unicode content characters across `text`,
`tool_input`, and `tool_output` together. Metadata and JSON wire bytes do not consume it.
Inspect the returned `content` object: `returned_chars`, `total_chars`, `truncated`, and
`continuations`. Continue only the needed field with the reported offset:

```bash
memex show --record-id <record_id> --machine <machine_id> \
  --field tool-output --offset-chars <offset_chars>
```

Field values are `text`, `tool-input`, and `tool-output`; continuation JSON names them
`text`, `tool_input`, and `tool_output`. `--offset-chars` is a Unicode character offset,
not a byte offset. Use `--max-chars N` when a different bound is justified.

### Drill inside a candidate session

Use terms discovered in the first hit:

```bash
memex search "<exact filename/error/decision term>" \
  --session <session_id> \
  --sort ts \
  --limit 50 \
  --fields ts,doc_id,role,text,event_id,parent_event_id,parent_tool_use_id,source_tool_use_id
```

This is often cheaper than immediately loading a giant transcript.

### Read a trajectory progressively when the question requires sequence

```bash
memex session <session_id>
```

The default page contains at most 50 records and shares one 16,000-character content
budget across them. JSONL ends with a `type: "page"` object carrying `offset`, `total`,
and `next_offset`. Resume later records with `--offset <next_offset>`; if a returned
record itself is truncated, use its `record_id` and `content.continuations` with
`memex show` before advancing when that field matters.

Continue far enough to recover:

- decision evolution
- failure -> changed action -> result
- user correction and subsequent recovery
- tool-call/result ownership
- a complete handoff/resume summary

Use `memex session <id> --full` only when the complete unbounded transcript is required.
Adding `--limit` keeps full-content mode record-bounded. All read-command `--full` modes
conflict with `--max-chars`.

### Fetch bounded context around a result

When results expose `event_id`, `parent_event_id`, `logical_parent_event_id`, `parent_tool_use_id`, `source_tool_use_id`, or `source_tool_assistant_uuid`, use them to distinguish actual tool interactions and thread/subagent relationships from nearby but unrelated text.

Fetch a bounded neighborhood around a stable record, local document, or native event ID:

```bash
memex context --record-id <record_id> --before 5 --after 5
memex context --event-id <event_id> --session <session_id> --before 5 --after 5 --expand-interactions
memex context --doc-id <doc_id> --before 5 --after 5
```

Pass `--machine <machine_id>` for a remote anchor. Context uses the same shared read budget
and per-record content continuations as session reads. Its envelope includes `offset`,
`total`, and `next_offset`; use `--offset <next_offset>` to continue through the selected
neighborhood. Bounded output uses `order: "anchor_first"`: the anchor comes first, followed
by the remaining records in chronological order. `--full` uses `order: "chronological"`;
keep the same mode when continuing by offset.

Use `--expand-interactions` when directly owned tool calls/results matter. It does not
follow general parent or conversation ancestry. Expansion has a hard cap of 100 additional
records; if the command reports that cap, narrow `--before`/`--after` or disable expansion.
Canonical record IDs use an exact field in new indexes and a stored-record fallback scan
in old indexes until rebuilt. Bounded remote reads require updated peers; on an older
peer, upgrade it. Legacy document-ID `show`, `session`, and `hydrate` may explicitly use
`--full` if unbounded content is appropriate; remote context/stable-ID reads need an
updated peer in either mode.
Document/event anchors are indexed, and neighborhoods are
scoped by session, source, and source path.

## Step 7: Reformulate From Evidence

After inspecting the first useful hit, extract exact corpus language:

- file/path names
- function/type names
- command flags
- error strings
- model/provider names
- PR/issue references
- the user's own phrasing
- names of rejected/selected alternatives

Use those for a second pass if the answer is still incomplete.

### If results are too broad

Tighten in this order:

1. add an exact anchor
2. add `--project`, time, role, or tool constraints
3. switch conceptual query to lexical/hybrid with discovered terminology
4. use `--session` to drill into a candidate

### If results are too sparse

Broaden conservatively:

1. rewrite the query in corpus-like language
2. switch lexical -> hybrid/semantic
3. remove role/tool/source filters
4. widen the time range
5. drop project scope only if cross-project evidence is acceptable

Do not immediately search the entire corpus with a generic word.

## Step 8: Apply Evidence Standards by Question Type

### "What did we decide about X?"

A search hit mentioning X is not enough. Recover the decision plus enough surrounding context to distinguish:

- proposal
- rejected option
- tentative plan
- explicit user choice
- implemented choice

Prefer later implementation or user confirmation when it contradicts an earlier proposal.

### "How did we fix X?"

Look for a trajectory:

1. failure/error state
2. changed hypothesis/action
3. tool or code result
4. explicit success evidence if available

Do not infer success from an assistant saying "fixed".

Useful discovery searches:

```bash
memex search "<exact error>" --role tool_result --unique-session --limit 20
memex search "<problem concept> fix workaround resolved" --hybrid --unique-session --limit 20
```

### "Have I asked/worked on X before?"

Use `--unique-session` and multiple query views if wording may vary. Report the sessions you found. Unless the queries are highly exhaustive, phrase the conclusion as "I found N sessions" rather than claiming a complete lifetime count.

### "Find analogous prior work"

Topic similarity is insufficient. Search both:

- surface subject
- mechanism/task shape

For example, an S3-backed immutable-index design may be more analogous to another local-cache/remote-generation design than to every session that merely mentions S3.

### "What happened in that session?"

Once the session is identified, use `memex session`. Reconstruct chronology from the transcript rather than synthesizing from global search hits.

### "Latest/recent"

Refresh at most once if needed, constrain by time, and prefer timestamp ordering:

```bash
memex search "<topic>" --since <timestamp> --sort ts --limit 30
```

### Cross-session synthesis

Use at least enough independent sessions to cover the requested variants/time periods. Preserve disagreements instead of flattening them into one invented consensus.

## Step 9: Stop and Report Uncertainty

Stop searching when the evidence requirement from the retrieval packet is satisfied.

Typical stopping conditions:

- **Simple fact:** one direct, unambiguous source record.
- **Decision:** decision context plus later confirmation/implementation if relevant.
- **Fix/recovery:** failure and recovery sequence with observable success evidence.
- **Analogy:** one or more genuinely mechanism-similar prior episodes.
- **Synthesis:** sufficient independent sessions to cover the requested scope without obvious missing branch/time period.

If evidence conflicts, report the conflict with timestamps/context. Prefer newer **verified** evidence over older verified evidence; do not blindly prefer newer assistant narration.

If two reformulation rounds still produce nothing useful, say what you searched and that you did not find reliable evidence.

## Output Discipline

When answering from retrieved history:

- distinguish what the **user said**, what an **assistant proposed**, and what a **tool/result demonstrated**
- cite session IDs or timestamps when useful for disambiguation
- mention uncertainty when an outcome is only narrated rather than externally verified
- preserve exact identifiers that matter to resumption
- do not expose irrelevant private transcript content
- do not fabricate missing turns to make the history coherent

## Command Reference

### Search

```bash
memex search "query" --limit 20
```

Useful filters and controls:

- `--project <name>`
- `--role <user|assistant|tool_use|tool_result>`
- `--tool <tool_name>`
- `--session <session_id>`
- `--source claude|codex|cursor|opencode|pi|omp|openclaw|copilot|grok|hermes|jcode|muse`
- `--since <iso|unix>` / `--until <iso|unix>`
- `--machine <id>` (repeatable)
- `--query <query>` (repeatable additional query view, fused with the positional query)
- `--cwd <path>`
- `--semantic`
- `--hybrid`
- `--top-n-per-session <n>` / `--unique-session`
- `--sort score|ts`
- `--min-score <float>`
- `--recency-weight <float>`
- `--recency-half-life-days <float>`
- `--json-array`
- `--trace`
- `--fields machine,score,ts,doc_id,record_id,session_id,project,role,source,snippet,event_id,parent_event_id`

Hermes currently contributes primarily usage data; do not assume `--source hermes` implies searchable Hermes transcripts are available.

### Session navigation

```bash
memex sessions --limit 20
memex sessions --cwd . --limit 20
memex sessions --project <name> --since <date> --json-array
```

### Open results and fetch surrounding context

```bash
memex show <doc_id>
memex show --record-id <record_id> --machine <machine_id>
memex show --record-id <record_id> --machine <machine_id> --field text --offset-chars <n>
memex context --record-id <record_id> --machine <machine_id> --before 5 --after 5 --expand-interactions
memex context --record-id <record_id> --machine <machine_id> --offset <next_offset>
memex session <session_id>
memex session <session_id> --machine <machine_id> --offset <next_offset>
memex hydrate requests.jsonl
```

`memex hydrate` accepts JSONL requests and fetches bounded session pages in batches. Its
response envelope keeps `offset`, `total`, and `next_offset`, and each record carries
content metadata. A single 16,000-character budget is shared across requests in input
order. Use `--max-chars N` to resize it or `--full` for complete content; those flags
conflict. Use hydrate when several federated search results need context; do not use it
for a single local hit.

### Retrieval evaluation

```bash
memex search "query" --trace
memex eval-retrieval <dataset.jsonl> --k 20
```

Tracing records retrieval metadata without transcript contents. Evaluation reports recall, MRR, nDCG, and session diversity against JSONL relevance cases.

### Index freshness / embeddings

```bash
memex index
memex embed
memex index-service status
```

Embeddings are optional. If semantic/hybrid retrieval degrades to lexical but the lexical evidence is sufficient, do not turn the user's historical lookup into an embedding-maintenance task.

### Index privacy / scope controls

Relevant indexing controls include:

```bash
memex index --include-reasoning
memex index --exclude '<glob>'
memex index --embeddings --model <minilm|bge|nomic|gemma|potion>
```

Agent subprocess transcripts are always indexed; filter them at query time.

Plaintext reasoning is excluded by default; encrypted/redacted reasoning remains excluded.

## Native Retrieval Capabilities

Memex provides native support for:

1. bounded context around a record, document, or event, with optional interaction expansion
2. multiple query views fused with reciprocal-rank fusion and session-level diversification
3. direct working-directory/repository scoping with `memex search --cwd`
4. machine-aware, character-bounded `show`, `session`, and `context` reads with explicit continuations
5. bounded JSONL batch fetching with one shared content budget across requested pages
6. stable canonical record IDs and interaction neighborhoods
7. metadata-only retrieval tracing and JSONL relevance evaluation

Prefer progressive context fetching and explicit query reformulation over treating one global top-k search as sufficient.
