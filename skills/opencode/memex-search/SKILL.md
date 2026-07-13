---
name: memex-search
description: Search, filter, and retrieve Opencode history via memex CLI. Use for context resumption, finding past code/decisions, and self-correction based on history.
---

# Memex for Opencode

`memex` is the primary memory retrieval tool. Use it to access historical sessions and indexed code interactions.

## Usage Patterns

- **Context Retrieval:** "What did we discuss in the last session regarding the API?"
  - `memex search "API discussion" --sort ts --limit 10`
- **Code Discovery:** "Find the specific function implementation from last week."
  - `memex search "function implementation" --hybrid`
- **Session Identification:** "Which session covered the database migration?"
  - `memex search "database migration" --unique-session`

## Search Modes

### Semantic vs Exact

| Need                     | Flag         | Example                                 |
| ------------------------ | ------------ | --------------------------------------- |
| Exact terms, IDs, errors | (default)    | `memex search "Error: 500"`             |
| Concepts, intent         | `--semantic` | `memex search "auth flow" --semantic`   |
| Mixed specific + fuzzy   | `--hybrid`   | `memex search "user_id logic" --hybrid` |

If the vector index is unavailable, memex warns on stderr and falls back to lexical search. Treat this as degraded retrieval and mention `memex embed` as the recovery step when useful.

## Background Index Service

- Use `memex index-service enable` to install the background indexer. It runs via launchd on macOS and systemd user services on Linux.
- Use `memex index-service enable --continuous` for a long-lived watcher; add `--poll-interval <seconds>` to tune continuous mode.
- Default mode is periodic indexing, typically every 3600 seconds; add `--interval <seconds>` to tune interval mode.
- The service inherits indexing flags, so pass source and embedding options at install time when needed, e.g. `memex index-service enable --opencode --embeddings`.
- On successful enable, memex writes `auto_index_on_search = false` to config when that setting is absent, so searches do not duplicate daemon work. Explicit user config is preserved.
- macOS writes `~/.memex/index-service.plist`, `~/.memex/index-service.log`, and `~/.memex/index-service.err.log`. Linux writes systemd user units under `~/.config/systemd/user/`.
- Use `memex index-service disable` to unload and remove the service.

## Session Context

Use `--session {session_id}` to isolate a specific interaction thread.

1. **Find Session ID:**
   - `memex search "topic" --unique-session`
2. **Narrow Search:**
   - `memex search "detail" --session <session_id>`
3. **Fetch Transcript:**
   - `memex session <session_id>`

## Output Parsing

Output is JSONL (JSON Lines). Each line is a valid JSON object.

**Schema:**

- `doc_id`: Unique record ID.
- `session_id`: Conversation thread ID.
- `ts`: ISO 8601 timestamp.
- `source`: Agent source name.
- `source_path`: Transcript path.
- `role`: `user`, `assistant`, `tool_use`, or `tool_result`.
- `text`: Content payload.
- `score`: Search relevance (float).
- `event_id`, `parent_session_id`, `conversation_kind`: Fork/tree metadata when available.

**Interpretation:**

- **Filtering:** Discard results below a relevance threshold (e.g., `score < 0.5`) unless specific.
- **Ordering:** Sort by `ts` for timeline reconstruction.
- **Grouping:** Aggregate by `session_id` to view conversation turns.
- **Forks:** Use `parent_session_id` to connect forked Opencode sessions back to the parent thread.
