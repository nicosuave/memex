---
name: memex-search
description: Discover prior agent work across sessions, projects, providers, and machines. Use for historical investigations and analogous solutions, or as a fallback when native conversation and history tools cannot recover the needed evidence.
allowed-tools: Bash(memex:*)
---

# Memex Search

Recover the smallest set of source-grounded records that answers the question.
For the current task, use native notes/history and the worklog first. Read known
Codex or ChatGPT conversations with native conversation tools when available.
Use Memex for broader discovery or when those sources are unavailable or insufficient.

## Choose the retrieval depth

Silently identify the target fact or episode, repository/source/machine/time scope,
exact anchors, and what evidence would be sufficient. For analogous work, also
identify the mechanism or task shape; topic similarity alone is insufficient.

| Request | First move |
| --- | --- |
| Known record or session | Read it directly; skip discovery |
| Recent work or resumption | `memex sessions --cwd . --limit 20 --json-array`; use its `resume_cmd` |
| Exact path, symbol, error, command, PR, URL, or quoted phrase | Lexical search |
| Uncertain wording with some literal anchors | `--hybrid` |
| Abstract similarity with few literal anchors | `--semantic` |
| Decision, fix, or session narrative | Find an anchor, then reconstruct its surrounding sequence |
| Cross-session comparison | Decompose the information needs and diversify by session |

For a simple lookup, start with one query and one record. For an ambiguous request,
use 2–3 distinct query views and inspect the best 1–3 sessions. For synthesis,
cover each requested variant or time period. These are starting budgets, not quotas.
Stop after two reformulation rounds unless the user requests exhaustive research.

## Search and refine

```bash
memex search "exact anchor" --cwd . --unique-session --limit 20 --format toon
memex search "remembered concept" --hybrid --project <project> --unique-session --format toon
memex search "anchor" --query "another view" --unique-session --format toon
```

Scope by the user's repository, project, machine, source, or dates when known.
Use `memex search --help` for supported filters, sources, ranking controls, and syntax.
For recent history, use `--since <timestamp> --sort ts`. Search may auto-index;
`sessions` does not. If freshness matters and the index appears stale, run
`memex index` once, never repeatedly during the same lookup.

For ambiguous questions, separate anchor, concept, mechanism, outcome/recovery,
and disambiguating views rather than combining every synonym into one query.
Repeated `--query` values are fused with the positional query. Search independently
answerable parts separately. A hypothetical episode description may help as a
last-resort semantic/hybrid query, but generated terms are probes, never evidence.

Default to `--unique-session`; use `--top-n-per-session 2` when two hits per session
help. Select candidates by exact anchors, scope fit, evidence role, agreement across
query views, and mechanism similarity—not score alone. Recency matters only when
relevant to the question. Tool results and explicit user statements can outweigh
assistant narration.

After the first useful hit, reuse its exact paths, symbols, errors, commands,
identifiers, user phrasing, or selected/rejected alternatives:

- Too broad: add an exact anchor, tighten project/time/role/tool filters, then drill
  into the candidate with `--session <id> --sort ts`.
- Too sparse: use corpus terminology, try hybrid/semantic, relax role/tool/source
  filters, then widen time. Drop project scope only when cross-project evidence fits.
- If vectors are unavailable, continue with lexical results when adequate. Mention
  `memex embed` only when semantic recall matters; keep maintenance out of the lookup.

Search returns compact references and excerpts around literal matches; semantic-only
hits use a prefix. Use `--fields` for a custom projection and `--full` only when all
stored fields are needed. **Default to `--format toon` for agent-consumed search
results.** It preserves the selected values in a TOON `results` array. Use JSONL
(the CLI default) for scripts, or `--format json` when a JSON array is required.
Explicit `--format` conflicts with `--json-array` and `-v`.

## Read progressively

Inspect source records before making claims. Preserve the returned machine and
record/session identifiers when opening federated results.

```bash
memex show --record-id <record_id> --machine <machine_id>
memex context --record-id <record_id> --machine <machine_id> --before 5 --after 5
memex session <session_id> --machine <machine_id>
```

`show` also accepts a positional document ID. `context` accepts `--doc-id`, or
`--event-id` with `--session`/`--source` to disambiguate native IDs. Inspect linkage
metadata when tool ownership or thread/subagent relationships matter; nearby text
alone does not establish a relationship. `--expand-interactions` follows directly
owned tool calls/results, not conversation ancestry. It errors above 100 added
records; narrow the window or disable expansion if that cap is reached.

Read commands share a default 16,000 Unicode-character budget across `text`,
`tool_input`, and `tool_output`; metadata and JSON wire bytes are excluded.
Inspect each record's `content.truncated` and `content.continuations`:

```bash
memex show --record-id <record_id> --machine <machine_id> \
  --field tool-output --offset-chars <offset_chars>
memex session <session_id> --machine <machine_id> --offset <next_offset>
memex context --record-id <record_id> --machine <machine_id> --offset <next_offset>
```

- Field offsets count Unicode characters, not bytes. Fields are `text`, `tool-input`,
  and `tool-output`; continuation metadata uses `text`, `tool_input`, `tool_output`.
- Session pages default to at most 50 records. Their JSONL ends with `type: "page"`
  and `offset`, `total`, `next_offset`. Context returns these pagination fields too.
- `next_offset` resumes later records. Finish any relevant truncated field with
  `show` before moving on; page offsets do not recover omitted field content.
- Bounded context returns the anchor first, then remaining records chronologically.
  `--full` uses chronological order throughout. Keep the same mode across pages.
- `--max-chars N` changes the budget; `--full` disables it and conflicts with that
  flag. Use a complete transcript only when the question requires it; `--limit`
  still bounds the record count in full session reads.
- For several session pages, use `memex hydrate requests.jsonl`; consult
  `memex hydrate --help` for the request schema. One budget is shared in input order,
  with per-record continuations and per-request page offsets. Avoid batching one hit.

For sequence-dependent questions, read far enough to recover decisions, corrections,
changed actions, results, and tool-call ownership. A focused search inside a known
session can locate the relevant interval before paging through it.

Older indexes remain readable but stable-ID lookup may scan until rebuilt; current
indexes use exact IDs and session/source/path scope. Bounded remote reads need updated
peers. Legacy document-ID `show`, `session`, and `hydrate` may use `--full` when
unbounded content is appropriate; remote context/stable-ID reads need an updated peer
in either mode. Do not substitute an unbounded read without considering its scope.

## Decide when evidence is sufficient

| Question | Required evidence / stopping condition |
| --- | --- |
| Simple fact | One direct, unambiguous source record |
| What did we decide? | Distinguish proposal, rejected option, tentative plan, user choice, and implementation; check later confirmation when relevant |
| How did we fix it? | Failure → changed hypothesis/action → tool/code result → observable success when available; “fixed” in assistant prose is insufficient |
| Have we done this before? | Report sessions found, not a complete lifetime count without exhaustive coverage |
| Analogous work | Recover mechanism-similar episodes, not merely shared topic words |
| What happened in a session? | Reconstruct chronology from the transcript, including corrections and recovery |
| Cross-session synthesis | Cover requested variants/time periods and retain disagreements |

Stop when that evidence is sufficient. Prefer newer verified evidence when it
supersedes older evidence, not simply newer assistant narration. Report conflicts
with timestamps/context. If two reformulations still fail, state what you searched
and that you did not find reliable evidence; retrieval failure does not prove absence.

In the answer, distinguish user statements, assistant proposals, and demonstrated
results. Cite session IDs or timestamps where useful, preserve exact resumption
identifiers, and flag outcomes supported only by narration. Do not invent missing
turns or expose irrelevant private transcript content.

## Updates

Use `--non-interactive` when invoking Memex from an agent, especially in a PTY.
Update notices and stale-skill warnings still appear on stderr; searches never prompt
or update anything. When updating is authorized, run `memex update --yes` to upgrade
Memex and refresh existing skills. `memex skill status` inspects differing copies;
`memex skill update` refreshes just the skills. Updates replace local skill edits,
leave missing copies uninstalled, and require restarting the agent to load changes.

## Specialized tasks

- For retrieval debugging or relevance evaluation, use `memex search --help` for
  `--trace` and `memex eval-retrieval --help`. Traces omit transcript contents;
  relevance evaluation reports recall, MRR, nDCG, and session diversity.
- For indexing, privacy, or embedding configuration, inspect `memex index --help`
  and `memex index-service status`. Agent subprocesses are indexed and filtered at
  query time. Plaintext reasoning is excluded by default; encrypted/redacted
  reasoning remains excluded. Check `--exclude`, `--include-reasoning`, and
  `--embeddings --model` only when that configuration is in scope.
- Hermes primarily contributes usage data; source support alone does not establish
  that searchable transcripts are available.
