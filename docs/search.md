# Indexing, search, and reading

[Back to Memex](../README.md)

## Indexing and source discovery

Index (incremental):
```
memex index
```

The main search, indexing, and maintenance commands are organized as follows:

| Area | Commands |
| --- | --- |
| Index maintenance | `memex index`, `memex index rebuild`, `memex index gc`, `memex index embed`, `memex index stats` |
| Search and reading | `memex search`, `memex sessions`, `memex session`, `memex show`, `memex context` |
| Batch session reads | `memex session batch [requests.jsonl]` |
| Background processes | `memex daemon run`, `memex daemon enable`, `memex daemon restart`, `memex daemon status`, `memex daemon disable` |
| Browser UI | `memex web serve`, `memex web open` (`memex web` also serves) |
| Retrieval diagnostics | `memex debug eval-retrieval DATASET` |

## Search quality evaluation

Conversation relevance search favors matching user/assistant text over matching
tool records. Ordinary unquoted multi-word queries also reward conversational
records that cover the whole query, before the candidate limit is applied. Partial
matches and tool evidence remain eligible. Explicit role/tool filters bypass these
preferences; quoted phrases, Boolean operators and field syntax keep their match
semantics. This applies to shared lexical retrieval used by CLI, MCP, native app, TUI and web search.

For plain queries with four or more terms, conversational matches containing the
whole phrase come first, even when the phrase is inside a long message. Phrase
matching uses the index tokenizer (including case folding and stemming), not byte
equality. Four terms is an intent heuristic for remembered passages; shorter
keyword searches retain BM25 ordering. Other matches keep the existing relevance
bonuses and remain eligible after phrase matches. This priority is applied before
the result limit and preserved when local results are grouped into conversations.
Fallback scores remain on the BM25 scale; phrase scores receive an additive
bonus based on the strongest fallback match, independent of the result limit.
The TUI uses relevance ordering with or without configured machines. Explicit CLI
recency boosts, hybrid search, multi-query fusion and multi-machine rank fusion
can change the final order; they do not promise global phrase-first ordering.

All five surfaces use the same relevance ranking and conversation selection.
Metadata filters constrain matches without contributing relevance points; adding
a redundant project, source, or session filter does not change scores. Conversation
text receives an explicit scoring preference, while strong tool evidence can still
outrank weak conversational matches. `min_score` applies to the resulting scores.
Recency boosting is disabled by default; `--recency-weight` remains an explicit
option. Equal lexical scores sort by timestamp descending, then document ID
ascending, before truncation. Federated ties also use machine identity.
Conversation views keep the first ranked record for each machine/source/session/
source-path identity and deepen retrieval until enough conversations are found
or candidates are exhausted. CLI record mode still returns individual records;
use `--unique-session` to compare it with conversation views. Explicit newest/
oldest sorts and different corpora, filters, or machine selections can differ.

`debug eval-retrieval` evaluates the actual CLI result pipeline, including query
fusion, filters, recency, conversation diversity and compact snippets. Evaluation
never refreshes the index. The default is local lexical search with recency disabled
for reproducibility, 20 returned results, and a metric cutoff of 10.

```sh
memex debug eval-retrieval cases.jsonl --root ~/.memex
memex debug eval-retrieval cases.jsonl --root ~/.memex --unique-session
memex debug eval-retrieval cases.jsonl --root ~/.memex --recency-weight 1
```

Each JSONL case specifies `id`, `query` (or `queries` for multiple views), optional
`cwd`, optional `filters` (`project`, `source`, `role`, `session`, `since`, `until`),
and a `relevant` array. Judgments identify a record by machine, source, session,
source path, document ID and optionally stable `record_id`; they carry a graded
`relevance` and optional verbatim `evidence` spans. Use grades 0–3 for noise,
related context, useful evidence and direct answers. An empty relevant array is
a deliberately judged no-answer query, not an unjudged query.

Reports contain ranked record references and the **actual rendered snippets** for
each query. They measure MRR@k, graded nDCG@k, known-positive recall@k and @20,
success among the first five distinct conversations, expected evidence visible in
snippets at k, and elapsed search time. No-answer accuracy is reported separately;
positive ranking metrics are undefined for those cases. Aggregate means exclude
undefined metrics. Recall is relative to the judged set, not exhaustive corpus
recall; these metrics do not infer relevance from clicks or BM25 scores.

To evaluate grouping and snippets in the terminal or browser interface, use
`--surface tui` or `--surface web`. These wrappers call the existing UI search
functions. Their datasets must contain one query per case and only project/source
filters; unsupported CLI filters or tuning fail explicitly. Compare surfaces on
the same supported subset. TUI evaluation uses its configured machine selection
and repository grouping; web and CLI evaluation are local. Partial TUI federation
failures invalidate the evaluation rather than quietly reduce its result set.
Federated TUI reports record the selected machines, but baseline comparison is
rejected until remote snapshot identities can be recorded too.

Use `--records records.jsonl` instead of `--root` to build an isolated temporary
index from portable Memex records. Judged record references and evidence must
resolve in that corpus. Working-directory scopes require an existing root's
analytics metadata and are rejected with `--records`; use project/session scopes
for portable fixtures.
Snapshot analytics uses only supplied record facts, without opening source files
or discovering repositories on the host. Each record may include `repo_project`
to preserve repository grouping; missing repository facts remain Unfiled.
The temporary index is removed when the run finishes.

```sh
memex debug eval-retrieval cases.jsonl --records records.jsonl > baseline.json
memex debug eval-retrieval cases.jsonl --records records.jsonl --baseline baseline.json
```

Baseline comparisons fail on **individual query** quality regressions. They
require matching dataset/corpus hashes, cutoff and search configuration (including
live index revision for an existing root). Reports flag local index changes during
the run, and baseline comparisons reject such runs. Semantic/hybrid reports also
identify the vector snapshot; evaluation fails if that snapshot changes or disappears
around a search, and baselines require the same snapshot. Latency is reported but not used as a
machine-dependent CI gate. Reports contain queries, paths and snippets: keep
private evaluations outside tracked files.

The checked-in [regression corpus](../tests/fixtures/retrieval-quality/README.md)
uses wholly synthetic queries and passages. CI checks lexical CLI,
TUI and web baselines, including known weak cases. It does not establish absolute
quality over the user's full history. Review per-case diagnostics before updating
a baseline; do not lower it just to make CI pass.

Semantic/hybrid comparisons use `--mode semantic|hybrid` against a prepared root.
They require nonempty vectors and a resolvable stored model; missing vectors cannot
silently produce a lexical score labelled semantic. Keep corpus, stored model,
model revision, embedding runtime and filters fixed when comparing modes. Reranking
and query-expansion experiments need their own explicit configurations; neither
is enabled by this evaluator.

Index all supported sources by default. Use repeatable `--only-source <source>` or
`--exclude-source <source>` options to select providers, and `--claude-path <path>`
to use a non-default Claude projects directory. Index sources are `claude`, `codex`,
`cursor`, `opencode`, `pi`, `omp`, `openclaw`, `copilot`, `grok`, `hermes`, `jcode`,
`muse`, `antigravity`, `bob`, `zcode`, and `kilocode`.
Hermes transcripts and usage are read from `state.db` under `~/.hermes` and its
named profiles; `HERMES_PROFILE_ROOTS` (comma-separated paths) selects alternate
stores. Plaintext reasoning is indexed only with `--include-reasoning`.
Bob tasks are read from `~/.bob/db/bob.db` (override with `MEMEX_BOB_DB`, a comma-separated
list of database paths with any file name, `~/` expanded); each task is indexed under the
virtual source path `<db>/<task_id>`, and sub-agent runs embedded in a task appear as their own
sessions. A database that cannot be read is skipped with a warning and its indexed tasks are kept.
ZCode sessions are read from `~/.zcode/cli/db/db.sqlite`, the store its SSH-attached
agent runtimes also write on remote hosts; `ZCODE_HOME` (comma-separated state roots)
adds extra stores, such as a synced copy from another machine.
KiloCode CLI sessions are read from `~/.local/share/kilo/kilo.db`
(`$XDG_DATA_HOME/kilo/kilo.db` when set); `KILO_DATA_DIR` (comma-separated state
roots) adds extra stores. Subagent sessions are indexed under their own session id
with the parent recorded, and token usage comes from the per-request counters each
assistant message carries.

## Agent memories

Indexing also discovers Claude project memories (`projects/*/memory/**/*.md`) and
Codex's `memories/MEMORY.md`, `memory_summary.md`, and `rollout_summaries/*.md`.
Discovery follows the existing Claude roots, `--claude-path`, Codex homes, provider
selection, and path exclusions. Raw memory intermediates, generated skills, and
unrelated project storage are not memory sources. Memex never edits these files.

Memories are documents, not sessions. They have a stable document ID, a content
version, and sections for retrieval. A changed file replaces all its indexed
sections atomically; deleted sections and files disappear on the next scan.
Renames are treated as removal and addition. If a source cannot be read, Memex
keeps the last good copy and marks its freshness instead of treating an error as
a deletion. Memory snapshots are stored under the Memex data root's `memory/`
directory, separately from conversation records and session analytics.

For recall of prior decisions, preferences, project conventions, or previous work,
use `--content all` to search memories and conversations together. Use
`--content memories` to inspect saved notes specifically. Search without
`--content` remains conversation-only. Provider selection remains independent:
`--source claude` selects the provider, not the content type. Memory results carry
document/section references, source paths, scope, freshness, and content versions.
For scoped memories, `--project` uses the repository name across Git worktrees;
`--cwd` selects the exact checkout. Each memory keeps its own source path and ID.
Their timestamp filters use file modification time; dates explicitly recorded
inside a note are separate metadata and do not imply that its claims are current.

Memory retrieval uses the existing `search` and `show` interfaces. Session listing,
session reads, `session batch`, and conversation `context` keep their established
meaning. A memory read uses a returned document ID rather than an arbitrary file
path. If the version changed since search, the read identifies that change and
returns bounded current document content rather than applying an obsolete section
position to the new file.

The existing daemon handles updates; there is no second memory service. MCP uses
the same search/read implementation, with structured provenance and bounded
content. Retrieved notes are historical evidence, not instructions for the
consuming agent. Explicit links can provide supporting documents or conversations;
conflicting notes remain separately attributable rather than being silently merged.

```bash
memex search "deployment decision" --content all
memex search "deployment decision" --content memories --source codex
memex show --memory-id <memory_id> --section <section_ref> --content-version <content_version> --machine <machine>
```

## Output formats

`search`, `sessions`, `session`, `session batch`, `show`, `context`, and `usage`
support `--format jsonl|json|text`; search also supports `toon`. Search, session
listings, transcript pages, and batch reads keep JSONL as their default. `show` and
`context` default to one JSON object, while `usage` defaults to text.

## Source formats and indexing behavior

Modern OpenCode sessions stored in `opencode*.db` under
`~/.local/share/opencode` are discovered automatically, alongside OpenCode's
legacy JSON storage. To use one or more alternate data roots, set
`OPENCODE_DATA_DIR` to a comma-separated list of directories before running
the installed `memex` binary.

OpenCode SQLite support includes legacy `message`/`part` storage and v2
`session_message` projections with either `session_v2` or `session` metadata
(the latter tested against upstream commit `5a833585`). When a v2 projection
exists, it is authoritative for transcripts, session inventory, and usage.
Frozen legacy rows are not merged back into it: missing rows may have been
reverted or deleted. Incomplete migrations without an explicit ownership
marker are therefore not reconstructed from legacy tables. A readable v2
database also supersedes frozen legacy JSON under the same data root.

V2 incremental scans track message count and maximum sequence/update time,
plus the durable per-session event revision when `event_sequence` is available.
Without that revision, same-count replacements or edits that leave both maxima
unchanged require an index rebuild; direct middle-row deletions are detected.

The default scan indexes Pi sessions from `~/.pi/agent/sessions` and Oh My Pi sessions
separately from `~/.omp/agent/sessions` plus named profile session directories.

Plaintext reasoning is excluded by default because it is usually low-value search noise. Opt
in with `memex index --include-reasoning`; reasoning records remain BM25-only. Encrypted and
redacted payloads, along with reasoning signature fields, are always excluded.

Antigravity discovers conversations under `~/.gemini/antigravity-cli`,
`~/.gemini/antigravity-ide`, and `~/.gemini/antigravity` (override the parent with
`ANTIGRAVITY_HOME`). It prefers full transcript logs, then transcript logs,
conversation SQLite databases, and finally overview logs. Encrypted `.pb`
trajectories are not decrypted. Coverage depends on which projection is available;
token usage is not yet emitted.

## Search results

Search (JSONL default):
```
memex search "your query" --limit 20
```

Search output is compact by default. Each hit contains `machine`, `score`, `ts`,
`doc_id`, `record_id`, `project`, `role`, `session_id`, `source`, `source_path`,
`snippet`, and `matches`. Lexical snippets center the earliest literal match;
semantic-only hits use a compact prefix. Use `--fields` for an explicit projection or
`--full` to restore every legacy search field, including full record text and linkage
metadata.

Agent-facing TOON output is available for search:

```sh
memex search "your query" --format toon
```

It returns one TOON document with a `results` array and preserves the same values as
JSON output, including custom `--fields` and `--full`. `--format jsonl` is the default;
`--format json` returns one JSON array, `--format text` is human-readable, and
`--format json --pretty` pretty-prints that array.

TUI:
```
memex tui
```

Drag over visible text to select it; releasing the mouse sends the selection to
your clipboard. Selection stays within the pane where the drag began. Normal
clicks, scrolling, and dragging the split divider continue to work without a mode
switch. Copying requires OSC 52 clipboard writes to be enabled in your terminal
(and multiplexer, if used). Memex cannot confirm whether the terminal accepted
the clipboard write. You can also use native terminal selection by holding Shift
while dragging in Ghostty and most xterm-style terminals, or Option in iTerm2.

Notes:
- Embeddings are disabled by default. Pass `--embeddings` to generate them during indexing.
- Searches run an incremental index refresh by default (configurable).
- Index updates are copy-on-write generations. A writer builds a private generation and atomically
  publishes it when complete; searches keep using the previous immutable generation until then.
- Concurrent searches coalesce stale auto-index work: one process refreshes while other lexical
  searches query the last committed index. Semantic and hybrid searches wait for vector writes to
  finish. Explicit `index`, `index rebuild`, `index embed`, and analytics backfill operations wait
  up to 30 seconds for another index mutation to finish and report its holder on timeout.

## Reading transcripts and records

Bounded transcript page:
```
memex session <session_id>
```

Single bounded record:
```
memex show <doc_id>
memex show --record-id <record_id> --machine <machine_id>
```

Read commands return at most 16,000 Unicode content characters by default. The budget
counts `text`, `tool_input`, and `tool_output` together; JSON metadata and wire bytes do
not count. Each record includes content metadata shaped like:

```json
{"returned_chars":16000,"total_chars":24000,"truncated":true,"continuations":[{"field":"tool_output","offset_chars":12000,"total_chars":20000}]}
```

Continue a truncated field from its reported Unicode character offset:

```sh
memex show --record-id <record_id> --machine <machine_id> \
  --field tool-output --offset-chars 12000
```

`--field` accepts `text`, `tool-input`, or `tool-output`. Use `--max-chars N` to
choose another shared budget. `--full` disables the content budget and conflicts with
`--max-chars`.

`memex session` returns 50 records by default and shares the 16,000-character budget
across them. Its JSONL stream ends with a `{"type":"page",...}` object containing
`offset`, `total`, and `next_offset`. Use `next_offset` with `--offset` to read later
records, and use `memex show` with a record's continuation metadata to finish a field
that was truncated within a record. `memex session --full` preserves the unbounded
transcript behavior; adding `--limit` bounds its record count. For stream commands,
`--format json` wraps the same entries in an array; for `session`, this includes
its final page marker. Use
`--format json --pretty` for indented arrays. `show` and `context` also accept
`--pretty` with their default single-object JSON output.

Human output:
```
memex search "your query" --format text
```
## Search modes

| Need | Command |
| --- | --- |
| Exact terms | `search "exact term"` |
| Fuzzy concepts | `search "concept" --mode semantic` |
| Mixed | `search "term concept" --mode hybrid` |

Lexical matching stems English words, so `migration` also finds `migrations` and `migrated`.
Indexes built before stemming keep matching whole words until `memex index rebuild`; memory
search stems immediately. The same rebuild stops indexing the `tool_input` and `tool_output`
fields, which are stored for display only; their content is already searchable through `text`.
The one exception is a Codex turn-lifecycle record, whose stored payload is the raw event
envelope. Its identifiers remain searchable through the `event_id` field.
## Common filters

- `--project <name>`
- `--role <user|assistant|tool_use|tool_result>`
- `--tool <tool_name>`
- `--session <session_id>`
- `--source claude|codex|cursor|opencode|pi|omp|openclaw|copilot|grok|hermes|jcode|muse|antigravity|bob|zcode|kilocode`
- `--since <iso|unix>` / `--until <iso|unix>`
- `--limit <n>`
- `--min-score <float>`
- `--sort score|ts`
- `--top-n-per-session <n>`
- `--unique-session`
- `--fields score,ts,doc_id,record_id,session_id,snippet`
- `--full` (all legacy search fields; conflicts with `--fields`)
- `--mode lexical|semantic|hybrid`
- `--format jsonl|json|text|toon`
- `--pretty` (pretty-print JSON output)

Default JSONL search output uses the compact fields documented above. Full or explicit
projections can also include tree/linkage metadata:
`event_id`, `parent_event_id`, `logical_parent_event_id`,
`parent_session_id`, `thread_source`, `conversation_kind`,
`parent_tool_use_id`, `source_tool_use_id`, and
`source_tool_assistant_uuid`.
