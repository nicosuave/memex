# memex

Fast local history search for Claude and Codex logs.

## Build

```
cargo build
```

Binary:
```
./target/debug/memex
```

## Install Skill

Install the memex-search skill so Claude/Codex can use it automatically:

```bash
# Install for Claude Code only (default)
./target/debug/memex skill-install

# Install for both Claude and Codex
./target/debug/memex skill-install --codex

# Install for Codex only
./target/debug/memex skill-install --no-claude --codex
```

Restart Claude/Codex to load the skill.

## Quickstart

Index (incremental):
```
./target/debug/memex index
```

Search (JSONL default):
```
./target/debug/memex search "your query" --limit 20
```

Notes:
- Embeddings are enabled by default.
- Searches run an incremental reindex by default (configurable).

Full transcript:
```
./target/debug/memex session <session_id>
```

Single record:
```
./target/debug/memex show <doc_id>
```

Human output:
```
./target/debug/memex search "your query" -v
```

## Search modes

| Need | Command |
| --- | --- |
| Exact terms | `search "exact term"` |
| Fuzzy concepts | `search "concept" --semantic` |
| Mixed | `search "term concept" --hybrid` |

## Common filters

- `--project <name>`
- `--role <user|assistant|tool_use|tool_result>`
- `--tool <tool_name>`
- `--session <session_id>`
- `--source claude|codex`
- `--since <iso|unix>` / `--until <iso|unix>`
- `--limit <n>`
- `--min-score <float>`
- `--sort score|ts`
- `--top-n-per-session <n>`
- `--unique-session`
- `--fields score,ts,doc_id,session_id,snippet`
- `--json-array`

## Embeddings

Disable:
```
./target/debug/memex index --no-embeddings
```

## Embedding model

Select via `--model` flag or `MEMEX_MODEL` env var:

| Model | Dims | Speed | Quality |
|-------|------|-------|---------|
| minilm | 384 | Fastest | Good |
| bge | 384 | Fast | Better |
| nomic | 768 | Moderate | Good |
| gemma | 768 | Slowest | Best (default) |

```
./target/debug/memex index --model minilm
# or
MEMEX_MODEL=minilm ./target/debug/memex index
```

## Config (optional)

Create `~/.memex/config.toml` (or `<root>/config.toml` if you use `--root`):

```toml
embeddings = true
auto_index_on_search = true
model = "gemma"  # minilm, bge, nomic, gemma
```

The skill definition is bundled in `skills/memex-search/SKILL.md`.
