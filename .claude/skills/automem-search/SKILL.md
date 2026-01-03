---
name: automem-search
description: Search, filter, and retrieve Claude/Codex history indexed by the automem CLI. Use when the user wants to index history, run lexical/semantic/hybrid search, fetch full transcripts, or produce LLM-friendly JSON output for RAG.
---

# Automem Search

Use this skill to index local history and retrieve results in a structured, LLM-friendly way.

## Indexing

- Build or update the index (incremental):
  - `./target/debug/automem index`
- Full rebuild (clears index):
  - `./target/debug/automem reindex`
- Embeddings are on by default.
- Disable embeddings:
  - `./target/debug/automem index --no-embeddings`
- Backfill embeddings only:
  - `./target/debug/automem embed`
- Common flags:
  - `--source <path>` for Claude logs
  - `--include-agents` to include agent transcripts
  - `--codex/--no-codex` to include or skip Codex logs
  - `--root <path>` to change data root (default: `~/.automem`)

## Search (LLM default JSON)

Run a search; output is JSON lines by default.

```
./target/debug/automem search "query" --limit 20
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
- `--unique-session` (same as top‑k per session = 1)
- `--sort score|ts` (default score)

### Output shape

- JSONL default (one JSON per line)
- `--json-array` for a single JSON array
- `--fields score,ts,doc_id,session_id,snippet` to reduce output
- `-v/--verbose` for human output

### Narrow first (fastest reducers)

1) Global search with `--limit`
2) Reduce with `--project` and `--since/--until`
3) Optionally `--top-n-per-session` or `--unique-session`
4) `./target/debug/automem session <id>` for full context

### Practical narrowing tips

- Start with exact terms (quoted) before hybrid if results are noisy.
- Use `--unique-session` to collapse PR‑link spam fast.
- Use `--min-score` to prune low-signal hits.
- Use `--sort ts` when you want a timeline view.
- Use `--role assistant` for narrative outcomes; `--role tool_result` for command errors.
- For a specific session, prefer `search "<term>" --session <id> --sort ts --limit 50` to jump to outcomes.

## Config

Create `~/.automem/config.toml` (or `<root>/config.toml` if you use `--root`):

```toml
embeddings = true
auto_index_on_search = true
```

`auto_index_on_search` runs an incremental index update before each search.

### Semantic and Hybrid

- Semantic: `--semantic`
- Hybrid (BM25 + vectors, RRF): `--hybrid`
- Recency tuning:
  - `--recency-weight <float>`
  - `--recency-half-life-days <float>`

## Fetch Full Context

- One record:
  - `./target/debug/automem show <doc_id>`
- Full transcript:
  - `./target/debug/automem session <session_id>`

Both commands return JSON by default.

## Human Output

Use `-v/--verbose` for human-readable output:

- `./target/debug/automem search "query" -v`
- `./target/debug/automem show <doc_id> -v`
- `./target/debug/automem session <session_id> -v`

## Recommended LLM Flow

1) `./target/debug/automem search "query" --limit 20`
2) Pick hits using `matches` or `snippet`
3) `./target/debug/automem show <doc_id>` or `./target/debug/automem session <session_id>`
4) Refine with `--session`, `--role`, or time filters
