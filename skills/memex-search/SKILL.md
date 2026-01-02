---
name: memex-search
description: Search, filter, and retrieve Claude/Codex history indexed by the memex CLI. Use when the user wants to index history, run lexical/semantic/hybrid search, fetch full transcripts, or produce LLM-friendly JSON output for RAG.
allowed-tools: Bash(memex:*)
---

# Memex Search

Use this skill to index local history and retrieve results in a structured, LLM-friendly way.

## Indexing

- Build or update the index (incremental):
  - `memex index`
- Continuous index (polling):
  - `memex index --watch --watch-interval 30`
- Full rebuild (clears index):
  - `memex reindex`
- Embeddings are on by default.
- Disable embeddings:
  - `memex index --no-embeddings`
- Backfill embeddings only:
  - `memex embed`
- Common flags:
  - `--source <path>` for Claude logs
  - `--include-agents` to include agent transcripts
  - `--codex/--no-codex` to include or skip Codex logs
  - `--model <minilm|bge|nomic|gemma|potion>` to select embedding model
  - `--root <path>` to change data root (default: `~/.memex`)

## Search (LLM default JSON)

Run a search; output is JSON lines by default.

```
memex search "query" --limit 20
```

Each JSON line includes:
- `doc_id`, `ts` (ISO), `session_id`, `project`, `role`, `source_path`
- `text` (full record text)
- `snippet` (trimmed single-line summary)
- `matches` (offsets + before/after context)
- `score` (ranked score)

### Mode decision table

| Need | Command |
| --- | --- |
| Exact terms | `search "exact term"` |
| Fuzzy concepts | `search "concept" --semantic` |
| Mixed | `search "term concept" --hybrid` |

### Filters

- `--project <name>`
- `--role <user|assistant|tool_use|tool_result>`
- `--tool <tool_name>`
- `--session <session_id>` (search inside a transcript)
- `--source claude|codex`
- `--since <iso|unix>` / `--until <iso|unix>`
- `--limit <n>`
- `--min-score <float>`

### Grouping / dedupe

- `--top-n-per-session <n>` (top n per session)
- `--unique-session` (same as top-k per session = 1)
- `--sort score|ts` (default score)

### Output shape

- JSONL default (one JSON per line)
- `--json-array` for a single JSON array
- `--fields score,ts,doc_id,session_id,snippet` to reduce output
- `-v/--verbose` for human output

### Background index service (macOS launchd)

```
memex index-service enable
memex index-service disable
```

### Narrow first (fastest reducers)

1) Global search with `--limit`
2) Reduce with `--project` and `--since/--until`
3) Optionally `--top-n-per-session` or `--unique-session`
4) `memex session <id>` for full context

### Practical narrowing tips

- Start with exact terms (quoted) before hybrid if results are noisy.
- Use `--unique-session` to collapse PR-link spam fast.
- Use `--min-score` to prune low-signal hits.
- Use `--sort ts` when you want a timeline view.
- Use `--role assistant` for narrative outcomes; `--role tool_result` for command errors.
- For a specific session, prefer `search "<term>" --session <id> --sort ts --limit 50` to jump to outcomes.

## Config

Create `~/.memex/config.toml` (or `<root>/config.toml` if you use `--root`):

```toml
embeddings = true
auto_index_on_search = true
model = "potion"  # minilm, bge, nomic, gemma, potion
scan_cache_ttl = 3600  # seconds (default 1 hour)
index_service_watch = false  # true = continuous background watch
index_service_interval = 3600  # seconds (ignored when watch = true)
index_service_watch_interval = 30  # seconds
index_service_label = "com.memex.index"
index_service_stdout = "/path/to/memex-index.log"
index_service_stderr = "/path/to/memex-index.err.log"
index_service_plist = "/path/to/com.memex.index.plist"
```

`auto_index_on_search` runs an incremental index update before each search.
`scan_cache_ttl` sets the maximum scan staleness for auto-indexing.
`index-service` reads config defaults (mode, interval, label, log paths). Flags override.

Recommended when embeddings are on (especially non-`potion` models): run the
background index service or `index --watch`, and consider setting
`auto_index_on_search = false` to keep searches fast.

### Semantic and Hybrid

- Semantic: `--semantic`
- Hybrid (BM25 + vectors, RRF): `--hybrid`
- Recency tuning:
  - `--recency-weight <float>`
  - `--recency-half-life-days <float>`

## Fetch Full Context

- One record:
  - `memex show <doc_id>`
- Full transcript:
  - `memex session <session_id>`

Both commands return JSON by default.

## Human Output

Use `-v/--verbose` for human-readable output:

- `memex search "query" -v`
- `memex show <doc_id> -v`
- `memex session <session_id> -v`

## Recommended LLM Flow

1) `memex search "query" --limit 20`
2) Pick hits using `matches` or `snippet`
3) `memex show <doc_id>` or `memex session <session_id>`
4) Refine with `--session`, `--role`, or time filters
