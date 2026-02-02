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
- `role`: `user`, `assistant`, `system`.
- `text`: Content payload.
- `score`: Search relevance (float).

**Interpretation:**

- **Filtering:** Discard results below a relevance threshold (e.g., `score < 0.5`) unless specific.
- **Ordering:** Sort by `ts` for timeline reconstruction.
- **Grouping:** Aggregate by `session_id` to view conversation turns.
