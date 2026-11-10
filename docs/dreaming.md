# Dreaming

[Back to Memex](../README.md)

A **dream** is one statement a model concluded from your agent history, stored with the
transcript records it rests on:

```
- Use bun, never npm, in the shop repository. `drm_3f9a…` (evidence supported)
  - 2026-11-09 claude user on local · `memex show --record-id rid1_… --machine local`
    > always use bun in this repo, never npm.
```

A **dream run** (`memex dream`) is the offline pass that writes them. It reads your recent
sessions and your saved Claude and Codex memories and proposes a small set of dreams:
corrections you keep giving, mistakes that recur, memories that recent work contradicts,
standing preferences, project facts, and lessons nobody wrote down.

Each agent's own memory consolidation sees only that agent. Memex indexes every agent on
every machine you configured, so a run can notice the same correction given to Claude Code
on Monday and Codex on Thursday, or a Claude memory that says `npm` while ten Codex sessions
use `bun`.

Dreaming is opt-in. It adds a model call, and with it cost and nondeterminism, to a tool
that is otherwise a deterministic index, and it sends transcript excerpts to that model.
Until you turn it on in `config.toml`, no run starts, the daemon schedules none, and search
returns no dreams:

```toml
[dream]
enabled = true
```

With it off, `memex dream` and `--content dreams` stop with a message naming this key, and
`--content all` returns what it did before. `memex dream --print-prompt` works either way,
because it sends nothing: it shows what a run would send. Turning it off later leaves the
store in place and readable with `memex dream show`; its dreams stop appearing in search.

```bash
memex dream --dry-run     # judge new sessions and show proposed dreams; write nothing
memex dream               # publish a new generation of dreams
memex dream show          # read the current dreams with their citations
memex search "package manager" --content all
```

## What a run reads and where it sends it

A run reads, from the index:

- user and assistant turns of primary sessions active since the last run (the first run
  looks back `lookback_days`). Tool calls, tool output, reasoning, and subagent sessions are
  not read;
- the sections of saved Claude and Codex memory files on this machine;
- the dreams already in the store.

It sends that text to the executor: the `claude` or `codex` CLI you already use, or a
command you configure. Memex holds no API keys. Text is passed through the same credential
redaction as other content leaving the machine (`[redaction] egress`, on by default; see
[secret redaction](configuration.md#secret-redaction)).

A run reads across agents, so a Codex session can be sent to Claude and the reverse. Use
`--source` to keep a run to one agent's history, `--project` to keep it to one project, and
`--print-prompt` to see exactly what would be sent.

The executor runs with no tools, in an empty working directory, without saving a session:

| Executor | Invocation |
| --- | --- |
| `claude` | `claude --print --safe-mode --no-session-persistence --tools ""` |
| `codex` | `codex exec --sandbox read-only --ephemeral --skip-git-repo-check` |
| `[dream] command` | your argv; prompt on stdin, answer on stdout |

`--safe-mode` keeps your own `CLAUDE.md`, hooks, skills, and MCP servers out of the run.
Codex has no switch that removes its shell tool, so it runs in its read-only sandbox instead.
The executor's environment has `MEMEX_DREAM=1`.

Sessions are read newest first, up to `max_sessions` and `max_prompt_chars`. Sessions beyond
the budget are skipped and counted in the report; they are not carried to the next run.
Raise the limits, or run `memex dream --since <timestamp>` to revisit a period.

The index is read as it stands. With the daemon running it is current; otherwise run
`memex index` first.

## What a run may write

The executor answers with proposed operations; memex decides what is written.

- **Every dream cites its evidence.** The executor cites short handles (`r12`, `m3`) for the
  records and memory sections it was shown. Memex maps them back to real identities. A handle
  it did not offer is dropped, and an operation left without evidence is rejected and listed
  in the report.
- **A dream needs a conversation record.** A saved memory alone is not new ground truth, so
  a dream citing only memories is rejected. A `contradiction` may instead cite two or more
  sources of any kind, since the disagreement is the finding.
- **A dream is never evidence for a dream.** Existing dreams cannot be cited, and
  transcripts of dream runs are never read back, so a run cannot build on its own earlier
  conclusions.
- **Nothing disappears by omission.** A dream the executor does not mention carries over
  unchanged. `revise` and `retract` need new record evidence, and the old dream stays in the
  store as `superseded` or `retracted`.
- **Reinforcing needs something new.** A dream is reinforced only by a record it does not
  already cite.
- **Excerpts come from the source.** The quote the executor gives only chooses where the
  excerpt starts; the text is cut from the indexed record.

Operations are `add`, `reinforce`, `revise`, and `retract`. A dream's kind is one of
`correction`, `repeated_mistake`, `contradiction`, `preference`, `project_fact`, and
`lesson`.

Memex never writes Claude or Codex memory files. Both agents consolidate their own memories,
and a second writer would fight them.

## The dream store

Dreams live under `<root>/dreams/` (`~/.memex/dreams/` by default):

```
dreams/
  CURRENT                      # ID of the current generation
  generations/<id>/dreams.json # the dreams: the record of truth
  generations/<id>/DREAMS.md   # the same dreams, rendered
  generations/<id>/diff.md     # what this generation changed
  generations/<id>/response.txt# the executor's raw answer
```

Every run publishes a new generation next to the earlier ones and switches `CURRENT`
atomically. An earlier generation is never modified, so any generation can be read again and
compared:

```bash
memex dream status                       # store, recent generations, schedule gate
memex dream diff                         # what the current generation changed
memex dream show --generation <id>       # the dreams as of an earlier generation
memex dream show <dream_id>              # one dream with every citation, as JSON
```

Generations are not pruned.

## Citations

A citation is more than a record ID:

| Field | Purpose |
| --- | --- |
| `machine` | Where the source lives |
| `rid`, `rid_version` | Canonical record ID, as `memex show --record-id` takes it, and its scheme |
| `source`, `session_id`, `source_path` | Scope for looking the record up |
| `event_id` | The agent's own event ID, when it has one |
| `role`, `ts` | Who said it and when |
| `text_sha256` | Hash of the whitespace-normalized text (first 8 KiB) |
| `excerpt` | Up to 240 characters of the redacted text, kept inline |

Record IDs survive index rebuilds, so most citations resolve directly. They can still stop
resolving: Claude Code deletes transcripts after its retention period, and memex mirrors the
file system, so a deleted transcript's records leave the index. A record ID also changes when
a transcript without native event IDs is moved, or when a parser starts reading an identity
it did not read before. The excerpt and hash exist for those cases.

`memex dream lint` finds each source again, cheapest exact match first:

1. the record ID within its session;
2. the native event ID, which survives an ID scheme change and a moved file;
3. identical text within the session, by hash;
4. a search for the excerpt, in the session and then everywhere.

| State | Meaning |
| --- | --- |
| `resolved` | Found by ID, event ID, or identical text |
| `remapped` | Found only by excerpt search: plausible, not exact |
| `unverified` | The machine holding the source could not be reached |
| `evidence_lost` | The source is gone; the inline excerpt is what remains |
| `source_revised` | A cited memory file changed and no longer contains the excerpt |

A citation found under a new ID is rewritten to it, with the old ID kept as `rid_prev`, so
the next lookup is the direct one. Lost evidence is marked, never dropped: the dream stays,
shows `evidence: lost`, and is offered to later runs flagged as such. An unreachable machine
is not evidence of loss. An edited memory that no longer contains the cited passage is
`source_revised` rather than lost, because you may have corrected the note on purpose.

Lint publishes a generation only when a citation changed. Every run also lints the existing
dreams before it reads anything new. Memory citations are checked on the machine that wrote
them.

## Reading dreams

Once dreaming is enabled, dreams come back wherever recall already happens. `--content all`, the search the bundled
skill tells agents to use for prior decisions, preferences, and conventions, returns
matching dreams beside the conversations and memories they were drawn from:

```bash
memex search "package manager" --content all
memex search "package manager" --content all --cwd .
```

An agent that asks memex what was decided gets the consolidated answer and its sources in
one result, without knowing a dream store exists. Each corpus is ranked on its own and
merged by reciprocal rank, at most five dreams per search, so dreams lead a page without
filling it. MCP `search` with `content: "all"` behaves the same.

To read dreams alone:

```bash
memex search "package manager" --content dreams
memex search "*" --content dreams --project memex --sort ts
memex dream show
```

Each hit has `kind: "dream"`, a `dream_id`, a `dream_kind`, `status`, `evidence`
(`supported`, `partial`, `unverified`, or `lost`), `project`, `snippet`, and up to five
`citations`, each with the `machine` and `record_id` (or `memory_id`) to pass to
`memex show`.

A dream is a model's conclusion, not a source. Read a citation before relying on one.

Scoping:

- `--project` returns that project's dreams and the dreams that hold across projects; a
  standing preference applies inside every project. With `--content all`, `--cwd` scopes
  dreams by the repository it names.
- `--source` keeps dreams with at least one citation from that agent.
- `--since` and `--until` bound a dream's newest evidence.
- `--role`, `--tool`, `--session`, `--tag`, and a narrowed `--origin` describe conversation
  records. With `--content all` they leave dreams (and memories) out; with `--content
  dreams` they are an error.

Dreams are matched by keyword in every mode: `--content all --mode hybrid` still returns
them, ranked lexically, and `--content dreams` accepts only lexical mode. They are read from
this machine's dream store, so they appear when `local` is among the searched machines.
Search without `--content` is conversation-only, as before. Superseded and retracted dreams
are not returned by search; `memex dream show` lists them.

Nothing is placed in an agent's context unasked: memex has no session-start injection and
writes no agent memory or instruction file.

## Scheduling

```toml
[dream]
enabled = true
auto = true
```

Scheduling is a second opt-in on top of `enabled`. With both set, the daemon starts `memex dream run` when all of these hold:

- at least `min_new_sessions` primary sessions were active since the last run;
- no session has been active for `idle_minutes`;
- `min_interval_hours` have passed since the last run;
- an hour has passed since a run that failed.

The gate is activity and idleness, not a clock time. `memex dream status` shows whether it
is open and why. The run happens in a child process, so a slow executor does not delay
indexing. One run happens at a time per store.

## Configuration

```toml
[dream]
enabled = false            # master switch; nothing below applies until true
auto = false               # let the daemon start dream runs (default false)
executor = "claude"        # "claude" or "codex"; unset picks whichever is on PATH
# command = ["my-llm", "--flag"]   # custom executor; replaces executor
# model = "opus"           # passed to the claude or codex CLI
timeout_secs = 900         # longest executor run
min_new_sessions = 5       # gate: new sessions since the last run
min_interval_hours = 24    # gate: time between daemon-started runs
idle_minutes = 30          # gate: quiet period before starting
lookback_days = 7          # how far back the first run reads
max_sessions = 40          # sessions per run, newest first
max_prompt_chars = 240000  # prompt size budget
machines = ["local"]       # machines whose sessions are read (default: this one)
```

`--executor`, `--model`, `--max-sessions`, and `--machine` override these for one run.

## Limits

- Dreams are only as good as the executor's reading. The rules above bound what can be
  written and keep every dream checkable; they do not make a dream true.
- Sessions on other machines are read when listed in `machines`; saved memories are read
  from this machine only.
- Dreams are matched by keyword only and read from this machine's store, in every search
  mode.
- Dreams reach an agent when it searches. Nothing is injected at session start.
- Memex does not write dreams back into Claude or Codex memory.
