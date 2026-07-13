---
name: memex-search
description: Search, filter, and retrieve Claude/Codex/Cursor/OpenCode/Pi/Copilot history indexed by the memex CLI. Use when you want to search history, run lexical/semantic/hybrid search, fetch full transcripts, or produce LLM-friendly JSON output.
---

# Memex Search

Use this skill to index local history and retrieve results in a structured way.

## Indexing

- Build or update the index (incremental):
  - `memex index`
- Continuous index:
  - `memex index-service enable --continuous`
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
  - `--opencode/--no-opencode` to include or skip OpenCode logs
  - `--pi/--no-pi` to include or skip Pi logs
  - `--copilot/--no-copilot` to include or skip GitHub Copilot CLI logs
  - `--model <minilm|bge|nomic|gemma|potion>` to select embedding model
  - `--root <path>` to change data root (default: `~/.memex`)

## Search (LLM default JSON)

Run a search; output is JSON lines by default.

```
memex search "query" --limit 20
```

Each JSON line includes:
- `doc_id`, `ts` (ISO), `session_id`, `project`, `role`, `source`, `source_path`
- `text` (full record text)
- `snippet` (trimmed single-line summary)
- `matches` (offsets + before/after context)
- `score` (ranked score)
- tree/linkage fields when available: `event_id`, `parent_event_id`, `logical_parent_event_id`, `parent_session_id`, `thread_source`, `conversation_kind`, `parent_tool_use_id`, `source_tool_use_id`, `source_tool_assistant_uuid`

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
- `--source claude|codex|cursor|opencode|pi|copilot`
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
- `--fields score,ts,doc_id,session_id,snippet,event_id,parent_event_id` to reduce output
- `-v/--verbose` for human output

### Background index service

```
memex index-service enable
memex index-service enable --continuous
memex index-service disable
```

- Use `memex index-service enable` to install the background indexer. It runs via launchd on macOS and systemd user services on Linux.
- Default mode is periodic indexing, typically every 3600 seconds. Use `--interval <seconds>` to override.
- Use `memex index-service enable --continuous` for a long-lived process that watches more frequently; use `--poll-interval <seconds>` to tune continuous mode.
- The service inherits indexing flags, so pass source and embedding options at install time when needed, e.g. `memex index-service enable --include-agents --embeddings`.
- On successful enable, memex writes `auto_index_on_search = false` to config when that setting is absent, so searches do not duplicate daemon work. Explicit user config is preserved.
- macOS writes `~/.memex/index-service.plist`, `~/.memex/index-service.log`, and `~/.memex/index-service.err.log`. Linux writes systemd user units under `~/.config/systemd/user/`.
- Use `memex index-service disable` to unload and remove the service.

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
index_service_mode = "interval"  # interval or continuous
index_service_interval = 3600  # seconds (ignored when mode = "continuous")
index_service_poll_interval = 30  # seconds
```

`auto_index_on_search` runs an incremental index update before each search.
`scan_cache_ttl` sets the maximum scan staleness for auto-indexing.
`index-service` reads config defaults (mode, interval, log paths). Flags override.
Service logs and the plist live under `~/.memex` by default.

Recommended when embeddings are on (especially non-`potion` models): run the
background index service or `index --watch`, and consider setting
`auto_index_on_search = false` to keep searches fast.

### Semantic and Hybrid

- Semantic: `--semantic`
- Hybrid (BM25 + vectors, RRF): `--hybrid`
- If the vector index is unavailable, memex warns on stderr and falls back to lexical search. Treat this as degraded retrieval and mention `memex embed` as the recovery step when useful.
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

## Recommended Flow

1) `memex search "query" --limit 20`
2) Pick hits using `matches` or `snippet`
3) `memex show <doc_id>` or `memex session <session_id>`
4) Refine with `--session`, `--role`, or time filters
