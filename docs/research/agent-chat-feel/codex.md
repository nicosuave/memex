# OpenAI Codex: app-server protocol and TUI smoothness techniques

Source: `github.com/openai/codex`, shallow clone at SHA `caae161dd0c82f2e04e57e6f9d738fafbce503ba` (commit dated 2026-11-09, "Keep thread storage available when diagnostic logs fail (#60466)").
Clone path: `/private/tmp/claude-501/-Volumes-Dev-Code-memex--claude-worktrees-agent-app-ux-patterns-13a76b/f8deb1e4-a002-4452-a81e-c8ef8a51001b/scratchpad/repos/codex`.
All `path:line` references are relative to the repo root at that SHA. Nothing from the clone was built or executed.

Labels: **VERIFIED** = read in source at the SHA above. **UNVERIFIED** = public web claim, URL given. **INFERENCE** = my reading of how verified pieces combine.

Caveat on drift: this tree is much newer than most public write-ups of Codex. Several commonly quoted constants (for example a 50 ms commit tick) are no longer what the code does. Everything below is what the code says today.

---

## 1. TL;DR

1. **Streaming is a two-region model, not a typewriter.** Markdown source is committed only at newline boundaries into an append-only "stable" region; everything that can still change shape (the unterminated line, an open table, an open fence, open display math) lives in a separate mutable "tail" cell that is re-rendered in place. Committed output is never reflowed mid-stream. VERIFIED (`codex-rs/tui/src/streaming/controller.rs:1-43`, `codex-rs/tui/src/markdown_stream.rs:82-96`).
2. **Commit pacing is a two-gear queue with hysteresis.** Smooth = 1 line per tick; catch-up = drain the whole backlog in one tick. Enter catch-up at 8 queued lines or 120 ms oldest-line age; exit at <=2 lines and <=40 ms held for 250 ms; 250 ms re-entry hold unless severe (64 lines or 300 ms). Tick = 8.33 ms (120 Hz). VERIFIED (`codex-rs/tui/src/streaming/chunking.rs:85-116`, `codex-rs/tui/src/app.rs:477`).
3. **The status shimmer explicitly copies the desktop app's timing**: 600 ms start delay, a 1.0 s sweep, once every 4.0 s, raised-cosine band at least 6 columns wide, base text at 50% fg/bg, band peak at 100% fg, repainted every 32 ms. It rests 75% of the time. VERIFIED (`codex-rs/tui/src/summary_shimmer.rs:1-60`, `codex-rs/tui/src/status_indicator_widget.rs:308-319`).
4. **Optimistic send is protocol-backed.** The client mints a UUID `clientUserMessageId`, renders the prompt before the RPC is sent, and the server echoes it back as `UserMessage.clientId` so the client can dedupe and reconcile. VERIFIED (`codex-rs/app-server-protocol/src/protocol/v2/turn.rs:181`, `.../v2/item.rs:344-348`, `codex-rs/tui/src/chatwidget/input_submission.rs:480,525-542`).
5. **`turn/start` is atomically "start or steer".** If a turn is active the same call steers it and returns the active turn id; `turn/steer` is the strict variant with an `expectedTurnId` precondition. This removes the client-side race between "is it running?" and "send". VERIFIED (`codex-rs/app-server/src/request_processors/turn_processor.rs:687-738`, `.../v2/turn.rs:321-349`).
6. **Resume is a snapshot plus cursors, not an event replay.** `thread/resume` with `excludeTurns` + `initialTurnsPage` returns metadata, one summary page of recent turns, and backwards cursors; older history pages in via `thread/turns/list` and `thread/items/list`, which are explicitly un-serialized (concurrent) reads. Heavy tool output can be deferred per item. VERIFIED (`.../v2/thread.rs:419-487,1817-1985`, `.../protocol/common.rs:911-931`).
7. **Deltas are droppable; completion is authoritative.** The in-process transport drops ordinary notifications when the client queue is full and only guarantees a small must-deliver set (`turn/completed`, queue changes, etc.). The TUI therefore consolidates each message from `item/completed` text, not from accumulated deltas. VERIFIED (`codex-rs/app-server/src/in_process.rs:114-133,792-812`, `codex-rs/tui/src/chatwidget/streaming.rs:293-295`).
8. **One persistent server, cheap threads.** Threads stay loaded for 60 s after the last subscriber leaves (`thread_unload_delay_secs`), and the TUI keeps a per-thread event store (session + turns + bounded event buffer, capacity 32768) so switching threads is a local replay. VERIFIED (`codex-rs/config/src/config_toml.rs:351-354`, `codex-rs/core/src/config/mod.rs:4212-4213`, `codex-rs/tui/src/app/thread_events.rs:98-119`, `codex-rs/tui/src/app.rs:324`).

---

## 2. App-server protocol model and responsiveness affordances

### 2.1 Shape

JSON-RPC, three primitives: **Thread** (conversation), **Turn** (one unit of agent work), **Item** (one unit of input or output inside a turn). VERIFIED (`codex-rs/app-server-protocol/src/protocol/v2/thread_data.rs:205-295` Thread, `:387-411` Turn; `.../v2/item.rs:280-291` ThreadItem, `:341-540` ThreadItemPayload).

- `Thread` carries `id` (UUIDv7), `sessionId`, `forkedFromId`, `parentThreadId`, `preview`, `ephemeral`, `historyMode` (`legacy` | `paginated`), `status`, `cwd`, `gitInfo`, `name`, and `turns` (populated only on resume/fork/read). VERIFIED (`thread_data.rs:205-295`).
- `ThreadStatus` = `notLoaded | idle | systemError | active { activeFlags: [waitingOnApproval | waitingOnUserInput] }`. A sidebar can show "needs you" without loading the thread. VERIFIED (`.../v2/thread.rs:1729-1746`).
- `Turn` carries `id`, `rootTurnId`, `items`, `itemsView` (`notLoaded | summary | full`), `status` (`completed | interrupted | failed | inProgress`), `error`, `startedAt`, `completedAt`, `durationMs`. VERIFIED (`thread_data.rs:387-433`, `.../v2/turn.rs:34-39`).
- `ThreadItemPayload` variants: `UserMessage{id, clientId, content}`, `HookPrompt`, `AgentMessage{id, text, phase, questions, ...}`, `FunctionCallOutput`, `Plan{id, text}`, `Reasoning{id, summary[], content[]}`, `CommandExecution{id, command, cwd, processId, status, commandActions[], aggregatedOutput, exitCode, durationMs}`, `FileChange{id, changes[], status}`, `McpToolCall{id, server, tool, status, arguments, result, error, durationMs}`, `DynamicToolCall`, `CollabAgentToolCall`, `SubAgentActivity`, `WebSearch`, `ImageView`, `Sleep`, `ImageGeneration`, `EnteredReviewMode`, `ExitedReviewMode`, `ContextCompaction`. Every variant has a stable `id`. Items also carry `startedAtMs` / `completedAtMs`. VERIFIED (`.../v2/item.rs:280-291,341-540`).

### 2.2 Methods (client to server)

VERIFIED from the request table in `codex-rs/app-server-protocol/src/protocol/common.rs`:

| Area | Methods | Lines |
|---|---|---|
| Lifecycle | `initialize`, `thread/start`, `thread/resume`, `thread/fork`, `thread/archive`, `thread/unarchive`, `thread/delete`, `thread/unsubscribe`, `thread/name/set`, `thread/metadata/update`, `thread/settings/update` | 523-771 |
| History | `thread/list`, `thread/loaded/list`, `thread/read`, `thread/turns/list`, `thread/items/list`, `thread/items/read` (exp.), `thread/search` (exp.), `thread/revert`, `thread/compact/start` | 809-931 |
| Turns | `turn/start`, `turn/steer`, `turn/interrupt`, `turn/settings/update` (exp.), `review/start` | 1107-1176 |
| Server-side queue (exp.) | `thread/queue/add`, `/list`, `/update`, `/delete`, `/reorder`, `/start` | 667-701 |
| Goals | `thread/goal/set`, `/get`, `/clear` | 651-665 |
| Misc | `model/list`, `account/rateLimits/read`, `command/exec*`, `fuzzyFileSearch*`, `thread/shellCommand`, `thread/backgroundTerminals/*` | 781-808, 1178, 1430, 1473-1492, 1595-1613 |

Each request declares a **serialization scope**: per-thread (`thread_id(params.thread_id)`), global (`global("projects")`), or `None`. History reads are annotated "Explicitly concurrent: this primarily reads append-only rollout storage", so paging history never queues behind a running turn on the same thread. VERIFIED (`common.rs:911-931`).

### 2.3 Notifications (server to client)

VERIFIED (`common.rs:2045-2161`):

- Thread: `thread/started`, `thread/status/changed`, `thread/archived`, `thread/unarchived`, `thread/deleted`, `thread/closed`, `thread/reverted`, `thread/name/updated`, `thread/settings/updated`, `thread/queue/changed`, `thread/goal/updated`, `thread/compacted`, `thread/tokenUsage/updated`.
- Turn: `turn/started`, `turn/completed`, `turn/diff/updated` (whole aggregated unified diff for the turn), `turn/plan/updated` (`explanation` + `plan[{step, status: pending|inProgress|completed}]`).
- Item lifecycle: `item/started`, `item/completed`.
- Item deltas, all keyed by `threadId` + `turnId` + `itemId`: `item/agentMessage/delta`, `item/plan/delta`, `item/reasoning/summaryTextDelta` (+ `summaryIndex`), `item/reasoning/summaryPartAdded`, `item/reasoning/textDelta` (+ `contentIndex`), `item/commandExecution/outputDelta`, `item/commandExecution/terminalInteraction`, `item/fileChange/patchUpdated` (full `changes[]`, not a text delta), `item/mcpToolCall/progress`. VERIFIED (`.../v2/item.rs:1594-1690`).
- Requests resolved elsewhere: `serverRequest/resolved {threadId, requestId}` so a second client can dismiss a stale approval sheet. VERIFIED (`.../v2/notification.rs:74-77`).
- Account: `account/rateLimits/updated`.

Lifecycle payloads: `ItemStartedNotification {item, threadId, turnId, startedAtMs}`; `ItemCompletedNotification {item, threadId, turnId, contentTokenCount, completedAtMs}`. The **same full item shape** is sent at start (in-progress status) and at completion (final status), so a client can render a cell from `item/started` and replace it by id on `item/completed`. VERIFIED (`.../v2/item.rs:1452-1478,1549-1579`).

The plan item documents its own reconciliation rule: "The completed plan item is authoritative and may not match the concatenation of `PlanDelta` text." VERIFIED (`.../v2/item.rs:381-386,1604-1611`).

### 2.4 Server-to-client requests (approvals, questions)

VERIFIED (`common.rs:1888-1927`): `item/commandExecution/requestApproval`, `item/fileChange/requestApproval`, `item/permissions/requestApproval`, `item/tool/requestUserInput` (experimental), `mcpServer/elicitation/request`, `item/tool/call` (client-executed dynamic tools).

- Command approval params carry `threadId`, `turnId`, `itemId` (the id of the already-started `CommandExecution` item), `startedAtMs`, optional `approvalId`, `reason`, `command`, `cwd`, `commandActions`, `proposedExecpolicyAmendment`, and `availableDecisions` (the ordered buttons the client may show). VERIFIED (`.../v2/item.rs:1710-1771`).
- `item/tool/requestUserInput` carries `questions[{id, header, question, isOther, isSecret, options[{label, description}]}]` and `isBlocking`; the response maps question id to `answers[]`. VERIFIED (`.../v2/item.rs:1885-1964`).
- On resume of a running thread the server replays still-pending server requests to the newly attached connection after sending the resume response. VERIFIED (`codex-rs/app-server/src/request_processors/thread_lifecycle.rs:1283-1285`).

### 2.5 Turn start, steer, interrupt

`turn/start` params: `threadId`, `clientUserMessageId`, `input[]`, plus sticky overrides (`cwd`, `model`, `effort`, `approvalPolicy`, `sandboxPolicy`, `collaborationMode`, ...) that apply to "this turn and subsequent turns". VERIFIED (`.../v2/turn.rs:174-307`).

The handler calls `thread.start_or_steer_turn(...)` and maps the result to `Started` or `Steered`; either way it returns immediately with a skeletal `Turn {status: inProgress, items: [], itemsView: notLoaded}`. VERIFIED (`turn_processor.rs:687-770`):

```rust
TurnInputSubmission::Started { turn_id, root_turn_id } => (turn_id, root_turn_id, true),
TurnInputSubmission::Steered { turn_id, root_turn_id } => (turn_id, root_turn_id, false),
```

Several params are documented as "Ignored when this request steers an already-active turn" (`turnTrigger`, `parentTurnId`, `rootTurnId`), confirming that steering through `turn/start` is intended. VERIFIED (`.../v2/turn.rs:183-201`).

`turn/steer` params: `threadId`, `clientUserMessageId`, `input[]`, and required `expectedTurnId` ("The request fails when it does not match the currently active turn"). Typed failures: no active turn, turn id mismatch, `ActiveTurnNotSteerable` for review and compact turns, output-schema mismatch. VERIFIED (`.../v2/turn.rs:321-356`, `turn_processor.rs:1098-1215`).

`turn/interrupt` params: `threadId`, `turnId`. The response is **withheld until the abort is observed** (`pending_interrupts` is answered when `TurnAborted` arrives), so "interrupt acknowledged" means "the turn has actually stopped". An empty `turnId` is a "startup interrupt" answered immediately. VERIFIED (`turn_processor.rs:1473-1535`).

Steer delivery timing: the TUI labels admitted steers "Messages to be submitted after next tool call", and its interrupt path notes "The server has already discarded pending input by the time the interrupted turn reaches the UI". So a steer is injected at the next tool/result boundary, and an interrupt drops un-injected steers server-side. VERIFIED as client-side statements (`codex-rs/tui/src/bottom_pane/pending_input_preview.rs:99`, `codex-rs/tui/src/chatwidget/input_restore.rs:466-468`); the core injection point itself was not traced.

### 2.6 Server-side durable queue (experimental)

`thread/queue/add {threadId, input, clientUserMessageId}` returns a `QueuedSubmission {id, input, clientUserMessageId, ...}`; `update`, `delete`, `reorder {queuedSubmissionIds[]}`, `list` (paged), and `start {queuedSubmissionId?}` round it out, with `thread/queue/changed` as the fan-out. The backing crate describes itself as "Durable, storage-neutral user-message queue and idle dispatch", and `start` "Starts the selected queued message only when its thread is idle." Metadata is "retained with this input, including across reconnect and restoration". VERIFIED (`.../v2/thread.rs:938-1048`, `codex-rs/ext/queue/src/lib.rs:1`, `codex-rs/ext/queue/src/service.rs:399-400`).

Relevance to Memex: Codex is moving the follow-up queue out of the client and into the runtime so it survives client restarts and is shared across clients. Memex already has a durable queue; the notable design details are client-supplied message ids on queued entries, explicit reorder, and "start this one now" as a first-class operation.

### 2.7 Thread start, resume, fork, list, archive

- `thread/start`: all fields optional (`model`, `cwd`, `approvalPolicy`, `sandbox`, `config`, `baseInstructions`, `developerInstructions`, `ephemeral`, `historyMode`, ...). Returns `thread` plus resolved `model`, `cwd`, `approvalPolicy`, `sandbox`. VERIFIED (`.../v2/thread.rs:62-200`).
- `thread/resume`: `excludeTurns: true` returns "only thread metadata and live-resume state"; `initialTurnsPage {limit, sortDirection (default desc), itemsView (default summary)}` embeds the first page "so clients can bootstrap recent turns without a second request". The response adds `turnsBackwardsCursor` and `itemsBackwardsCursor`. "Full-history hydration is deprecated for paginated threads." VERIFIED (`.../v2/thread.rs:419-487,500-519`).
- Resuming a **running** thread merges persisted turns with the live active turn (`merge_turn_history_with_active_turn`), then sends a connection-scoped token-usage replay, a goal snapshot, and pending server requests, in that order after the response. VERIFIED (`thread_lifecycle.rs:1092-1108,1257-1285`, `codex-rs/app-server/src/request_processors/token_usage_replay.rs:1-34`).
- `thread/fork`: `lastTurnId` (inclusive) or `beforeTurnId` (exclusive) pick the fork point; `ephemeral`; `excludeTurns`; `attachToSource` (hidden attached fork). VERIFIED (`.../v2/thread.rs:544-647`).
- `thread/list`: cursor + `limit`, `sortKey`, `sortDirection`, `archived`, `cwd`, `searchTerm`, `sourceKinds`, `parentThreadId`, and `useStateDbOnly` ("return from the state DB without scanning JSONL rollouts"), which is the fast path for a sidebar. Response has `nextCursor` and `backwardsCursor`. VERIFIED (`.../v2/thread.rs:1437-1522,1611-1624`).
- `thread/turns/list`: cursor, `limit`, direction (default desc), `itemsView` (default summary). `backwardsCursor` re-includes the anchor turn "to catch updates to that turn". VERIFIED (`.../v2/thread.rs:1817-1846`).
- `thread/items/list`: optional `turnId`, opaque cursor or an exclusive `{itemId}` anchor, `deferOutput {commandExecution, mcpToolCall}` which omits large successful outputs and flags the entry `outputDeferred`; `thread/items/read` fetches up to 100 items by id later. VERIFIED (`.../v2/thread.rs:1851-1985`).
- `thread/unsubscribe` returns `notLoaded | notSubscribed | unsubscribed`; an unsubscribed thread with no activity is unloaded after `thread_unload_delay_secs` (default 60). VERIFIED (`.../v2/thread.rs:740-758`, `config_toml.rs:351-354`, `core/src/config/mod.rs:4212-4213`).

### 2.8 Review mode, token usage, rate limits

- `review/start {threadId, target: uncommittedChanges | baseBranch{branch} | commit{sha,title} | custom{instructions}, delivery}` returns `{turn, reviewThreadId}`; the turn is bracketed by `EnteredReviewMode` / `ExitedReviewMode` items; review turns are not steerable. VERIFIED (`.../v2/review.rs:20-62`, `.../v2/item.rs:525-534`, `turn_processor.rs:1161-1170`).
- `thread/tokenUsage/updated {threadId, turnId, tokenUsage{total, last, modelContextWindow}}` with breakdown `totalTokens, inputTokens, cachedInputTokens, cacheWriteInputTokens, outputTokens, reasoningOutputTokens`. VERIFIED (`.../v2/thread.rs:2059-2134`).
- `account/rateLimits/updated` with `RateLimitSnapshot {primary, secondary: {usedPercent, windowDurationMins, resetsAt}, credits, planType, ...}`. VERIFIED (`.../v2/account.rs:721-733,809-814`).

### 2.9 Connection-level affordances

- `initialize.capabilities.optOutNotificationMethods`: exact method names to suppress per connection. A list view can opt out of deltas entirely. VERIFIED (`codex-rs/app-server-protocol/src/protocol/v1.rs:66-69`).
- `experimentalApi` opt-in gates experimental methods and fields. VERIFIED (`v1.rs:55-57`).
- Two delivery classes on the in-process transport: must-deliver (`turn/completed`, `thread/queue/changed`, `thread/settings/updated`, process output/exit, async-delivery agent messages) versus everything else, which is `try_send` and dropped with "dropping in-process server notification (queue full)". A `Lagged {skipped}` marker tells the consumer it fell behind. VERIFIED (`codex-rs/app-server/src/in_process.rs:114-133,181-190,792-812`).
- The client facade keeps the consumer event queue unbounded "so unread notifications cannot prevent request responses from being delivered". VERIFIED (`codex-rs/app-server-client/src/lib.rs:14-21`).
- Transports: stdio JSONL (default) and websocket (experimental). UNVERIFIED in source this pass (https://learn.chatgpt.com/docs/app-server, reached via redirect from https://developers.openai.com/codex/app-server).

### 2.10 Comparison with ACP

App-server column is VERIFIED above. The ACP column is from my prior knowledge of the published ACP spec (agentclientprotocol.com) and was **not re-read in this assignment**; treat it as UNVERIFIED and check it against whichever ACP schema version Memex pins.

| Capability | Codex app-server | ACP (as I understand the stable spec) |
|---|---|---|
| Conversation unit | Thread, Turn, Item, all with stable ids | Session; a prompt turn is the lifetime of one `session/prompt` request; no first-class turn id |
| Send | `turn/start` returns immediately with a turn id; progress via notifications; `turn/completed` ends it | `session/prompt` stays open and resolves with `stopReason` |
| Client-supplied message id | `clientUserMessageId` echoed as `UserMessage.clientId` | None in stable spec; client must match optimistic bubbles heuristically or simply never re-render the echo |
| Mid-turn steer | `turn/steer` (strict) and start-or-steer `turn/start` | None; second prompt must wait or cancel first |
| Interrupt | `turn/interrupt`, response withheld until aborted | `session/cancel` notification; prompt resolves `cancelled` |
| Assistant text | `item/started` then `item/agentMessage/delta{itemId}` then `item/completed` with authoritative full text | `session/update` `agent_message_chunk` content blocks; no item boundary or final-text event |
| Reasoning | Separate item with `summaryIndex` / `contentIndex` parts | `agent_thought_chunk` |
| Tool calls | Typed items (`CommandExecution`, `FileChange`, `McpToolCall`, ...) with typed status, parsed `commandActions`, durations | Generic `tool_call` / `tool_call_update` with `toolCallId`, `kind`, `status`, content |
| Command output streaming | `item/commandExecution/outputDelta` by item id | Via client-hosted `terminal/*` or tool-call content updates |
| Turn-level diff | `turn/diff/updated` aggregated unified diff | Per-tool-call diff content only |
| Plan | `turn/plan/updated` (checklist) and streamed `Plan` item (proposal) | `plan` session update (entries with status and priority) |
| Approvals | Typed server requests bound to `itemId`, with `availableDecisions`; `serverRequest/resolved` fan-out | `session/request_permission` with options, bound to a tool call |
| Structured questions | `item/tool/requestUserInput` (exp.) | No stable equivalent |
| Resume | Snapshot + cursors; live active turn merged; pending requests replayed | `session/load` replays the conversation as `session/update` notifications (event replay) |
| History paging | `thread/turns/list`, `thread/items/list`, deferred outputs | None in stable spec |
| List / fork / archive | `thread/list`, `thread/fork` at a turn, `thread/archive` | Not in stable spec (list/fork/resume exist only as unstable drafts) |
| Review mode | `review/start` with typed targets | None |
| Queue | Experimental durable `thread/queue/*` | None; client-owned |
| Token usage / rate limits | `thread/tokenUsage/updated`, `account/rateLimits/updated` | Not in stable spec |
| Multi-client | Subscriptions per connection, opt-out lists, unload delay | Single client per agent process |

INFERENCE: the gaps that matter most for "feel" are (a) no client message id, (b) no authoritative end-of-item text, (c) replay-style load. A client on ACP has to synthesize all three locally: keep its own id for the optimistic bubble and never re-insert an echoed user chunk; treat the accumulated chunk buffer as the final text at turn end; and persist its own transcript snapshot so reopening a session paints from local storage before (or instead of) `session/load` replay.

### 2.11 Public claims about the closed-source desktop app

- UNVERIFIED: the app server "is the interface Codex uses to power rich clients (for example, the Codex VS Code extension)"; `turn/steer` "append[s] user input to the currently in-flight turn without creating a new turn" and "doesn't emit a new `turn/started` notification". https://learn.chatgpt.com/docs/app-server
- UNVERIFIED: the desktop app has a General setting "Follow-up behavior" choosing whether a message sent while Codex works steers the current run or queues for the next; Enter steers and Tab queues. Third-party summaries: https://www.memorylake.ai/en/blogs/codex-steer-vs-queue and https://codex-pocket.github.io/codex-pocket-docs/en/docs/next/usage/composer
- UNVERIFIED and conflicting: a forum user reports that in the app a steered message appears to apply only after the current turn finishes. https://community.openai.com/t/i-find-this-new-queued-follow-up-inputs-feature-a-little-frustrating/1398627
- UNVERIFIED: the app manages threads in local projects or git worktrees. https://developers.openai.com/codex/app/features (redirects to https://learn.chatgpt.com/docs/features; the page I could fetch was navigation only, so I have no behavioral detail on worktree threads).
- VERIFIED but indirect: the TUI source says its status shimmer timings "Match the desktop app's thinking and reasoning header timings" and describes "the desktop's cadenced sweep" (`codex-rs/tui/src/summary_shimmer.rs:1,24`). That is OpenAI's own statement, in open source, about the closed app's shimmer: 600 ms delay, 1 s sweep, 4 s interval.

I found nothing public and specific about optimistic sends in the desktop app.

---

## 3. TUI streaming pipeline, with constants

### 3.1 Stages

1. **Collect** (`MarkdownStreamCollector`, `codex-rs/tui/src/markdown_stream.rs:30-121`). `push_delta` appends raw text. `commit_complete_source()` returns the newly completed byte range up to the last `\n`, or `None` if the delta had no newline. `pending_source()` is the unterminated remainder. The collector does not parse markdown. VERIFIED.
2. **Render incrementally** (`StreamingRender`, `codex-rs/tui/src/streaming/render.rs:1-41`). "Completed top-level blocks are retained while the final block stays mutable, avoiding repeated rendering of the stable prefix as newline-bearing deltas arrive." Only the last top-level block is re-rendered per commit. Full recompute happens on width or render-mode change, or when reference-link definitions make the prefix unsafe to retain. VERIFIED.
3. **Partition** (`StreamCore`, `codex-rs/tui/src/streaming/controller.rs:86-106`). Rendered lines split into stable (`[0, enqueued_stable_len)`) and tail. Invariant: `emitted_stable_len <= enqueued_stable_len <= render.lines.len()`, and "committed source is append-only until `reset()`". VERIFIED (`controller.rs:36-43`).
4. **Hold back what can still reshape** (`active_tail_budget_lines`, `controller.rs:510-528`; `table_holdback.rs:1-30`). A pipe-table header (pending or confirmed by its delimiter row) keeps everything from the header onward in the tail, because "adding a new row can change every column's width and reshape all prior rows". Open markdown/mermaid fences and unclosed display math are held the same way. Pre-table prose still flows to stable. VERIFIED.
5. **Preview the unterminated line** (`ProsePreview`, `codex-rs/tui/src/streaming/prose_preview.rs:1-17`). "Bounded, disposable previews of unterminated prose. Preview lines never enter scrollback." Capped at `MAX_PREVIEW_BYTES = 8192` with a leading `…`. It refuses to advance on lines that might be structural (contain `|`, start with space/tab/`>`, or start a fence), and in rich mode withholds an unfinished link destination until it closes. VERIFIED (`prose_preview.rs:17,44-58,66-76,112-114`).
6. **Queue** (`StreamState`, `codex-rs/tui/src/streaming/mod.rs:31-107`). Newly stable lines are pushed to a FIFO with a shared `enqueued_at` timestamp. VERIFIED.
7. **Drain on a tick** (`run_commit_tick`, `codex-rs/tui/src/streaming/commit_tick.rs:71-100`), governed by `AdaptiveChunkingPolicy`. Drained lines become an `AgentMessageCell` appended to history. VERIFIED.
8. **Show the tail** (`sync_active_stream_tail`, `codex-rs/tui/src/chatwidget/streaming.rs:774-834`). The tail (mutable lines + prose preview) is wrapped in a `StreamingAgentTailCell` and placed in the single `active_cell` slot. It is replaced only if not equal to the current one, and each replacement bumps a revision used as a layout cache key. VERIFIED.
9. **Finalize** (`finalize_remaining`, `controller.rs:212-225`; `flush_answer_stream`, `chatwidget/streaming.rs:92-171`). Re-render the full source once, emit only lines beyond `emitted_stable_len`, then send `ConsolidateAgentMessage` to replace the run of streamed line-cells with one source-backed `AgentMarkdownCell` that can re-wrap on resize. VERIFIED.

### 3.2 Constants

| Constant | Value | Where |
|---|---|---|
| `MIN_FRAME_INTERVAL` | 8,333,334 ns (120 FPS cap) | `codex-rs/tui/src/tui/frame_rate_limiter.rs:13` |
| `TARGET_FRAME_INTERVAL` | = `MIN_FRAME_INTERVAL` | `codex-rs/tui/src/tui.rs:102` |
| `COMMIT_ANIMATION_TICK` | = `TARGET_FRAME_INTERVAL` (8.33 ms) | `codex-rs/tui/src/app.rs:477` |
| Tick missed-tick behavior | `MissedTickBehavior::Delay` | `codex-rs/tui/src/app/event_dispatch.rs:1182` |
| `ENTER_QUEUE_DEPTH_LINES` | 8 | `codex-rs/tui/src/streaming/chunking.rs:85` |
| `ENTER_OLDEST_AGE` | 120 ms | `chunking.rs:90` |
| `EXIT_QUEUE_DEPTH_LINES` | 2 | `chunking.rs:95` |
| `EXIT_OLDEST_AGE` | 40 ms | `chunking.rs:100` |
| `EXIT_HOLD` | 250 ms | `chunking.rs:103` |
| `REENTER_CATCH_UP_HOLD` | 250 ms | `chunking.rs:108` |
| `SEVERE_QUEUE_DEPTH_LINES` | 64 | `chunking.rs:113` |
| `SEVERE_OLDEST_AGE` | 300 ms | `chunking.rs:116` |
| `MAX_PREVIEW_BYTES` | 8192 | `codex-rs/tui/src/streaming/prose_preview.rs:17` |
| Live command output cap | 50 lines, 1 MiB | `codex-rs/tui/src/exec_cell/live_output.rs:5-6` |
| Thread event buffer | 32768 events | `codex-rs/tui/src/app.rs:324` |

Surprise: `COMMIT_ANIMATION_TICK` is 8.33 ms, so smooth mode commits up to 120 lines per second. Older descriptions of a 50 ms tick do not match this tree. The doc comment still says the interval "controls perceived typing speed for non-backlogged output" (`app.rs:473-476`).

### 3.3 Commit policy (exact)

VERIFIED (`chunking.rs:180-294`):

```text
decide(snapshot{queued_lines, oldest_age}, now):
  if queued_lines == 0: mode = Smooth; plan = Single
  Smooth  -> enter CatchUp if (queued >= 8 OR oldest >= 120ms)
             unless (now - last_exit < 250ms AND NOT severe)
  CatchUp -> if (queued <= 2 AND oldest <= 40ms) held for 250ms: mode = Smooth
             else reset the hold timer
  severe  := queued >= 64 OR oldest >= 300ms
  plan    := Smooth ? Single : Batch(queued_lines)
```

- Catch-up drains **everything** in one tick: "`CatchUp` drains all currently queued lines in a tick" (`chunking.rs:42-43`), test `catch_up_batch_drains_current_backlog` asserts `Batch(512)` (`chunking.rs:355-362`).
- Entry needs either signal; exit needs both: "this prevents oscillation when one signal is still under load" (`chunking.rs:274-283`).
- The snapshot sums depth and takes the max oldest age across the answer stream and the plan stream (`commit_tick.rs:102-127`).
- Three tick scopes: `AnyMode` (periodic), `CatchUpOnly` (opportunistic, run right after every delta that enqueued something), `Replay` (drain all, used when re-showing a thread) (`commit_tick.rs:30-38,83-93`).
- Each enqueuing delta does two things: starts the interval if not running, and runs a `CatchUpOnly` tick immediately, so a burst never waits for the next timer fire (`chatwidget/streaming.rs:756-759`).
- When all controllers go idle the interval is dropped (`StopCommitAnimation`), so there is no timer while nothing streams (`chatwidget/streaming.rs:671-674`, `event_dispatch.rs:1186-1188`).

Tests: `chunking.rs:296-457` (9 unit tests covering every threshold and both holds), `controller.rs:922-2204` (resize, table holdback, partial-drain invariants), `streaming/render_tests.rs`, `streaming/controller_preview_tests.rs`, snapshots in `codex-rs/tui/src/streaming/snapshots/`.

### 3.4 How committed output avoids reflow

- Tail is taken from `enqueued_stable_len`, not `emitted_stable_len`: "If callers were to derive the tail from `emitted_stable_len` instead, queued-but-not-yet-emitted lines could reappear in the active cell and duplicate content on screen." VERIFIED (`controller.rs:264-277`).
- If a structural rewrite moves the stable boundary **backwards** into lines that are queued but not yet emitted, the queue is rebuilt from the new render; already-emitted lines are never touched (`target.max(self.emitted_stable_len)`). VERIFIED (`controller.rs:450-486`).
- Resize mid-stream: re-render once at the new width and rebuild the queue from the emitted line count, deliberately avoiding byte-level remapping; final correctness comes from consolidation into a source-backed cell. VERIFIED (`controller.rs:28-34,294-331`).
- Finalization decides whether a visible reflow is needed: only when the stream needed it or when the authoritative completed text differs from what was streamed (`ConsolidationScrollbackReflow::Required` vs `IfResizeReflowRan`). VERIFIED (`chatwidget/streaming.rs:105-134`).

### 3.5 Ordering and flicker control around the stream

- **Deferred lifecycle events.** While an answer stream is active, approvals, user-input requests, `item/started`, and `item/completed` are pushed to an `InterruptManager` FIFO and flushed when the stream finishes. "once anything is queued due to an active write cycle, continue queueing until the queue is flushed to avoid reordering (e.g., ExecEnd before ExecBegin)". VERIFIED (`codex-rs/tui/src/chatwidget/streaming.rs:693-718`, `codex-rs/tui/src/chatwidget/interrupts.rs:1-30`).
- **One progress affordance at a time.** The status row is hidden as soon as stream lines are committed or a tail exists, and restored only when "commentary completion is pending, the turn is still running, and all stream queues have drained". "This gate prevents flicker while normal output is still actively streaming". VERIFIED (`chatwidget/streaming.rs:185-207,663-666,781`).
- **In-place tool cells.** A running tool lives in the single mutable `active_cell`; events are routed by call id; completion mutates the same cell (bullet turns green or red, "Running" becomes "Ran", "Exploring" becomes "Explored") and it is committed to history only when the next visible cell arrives. Consecutive read/list/search commands coalesce into one "Exploring" group. VERIFIED (`codex-rs/tui/src/exec_cell/model.rs:1-8`, `codex-rs/tui/src/exec_cell/render.rs:318-335,465-481`, `codex-rs/tui/src/chatwidget.rs:1230-1304`).
- **Empty cells do not break groups.** `take_history_insertion_prefix` only flushes the active cell if the incoming cell "renders visible lines". VERIFIED (`chatwidget.rs:1289-1303`).
- **Live output is bounded.** Streaming command output keeps a 50-line / 1 MiB window; the final `aggregatedOutput` "replaces any streamed preview". VERIFIED (`exec_cell/model.rs:22-26`, `exec_cell/live_output.rs:5-6`).

### 3.6 Frame scheduling and redraw coalescing

- `FrameRequester` is a cloneable handle; `schedule_frame()` and `schedule_frame_in(dur)` send an `Instant` to one scheduler task. The scheduler keeps the **earliest** requested deadline, clamps it to at least `last_emit + 8.33 ms`, and emits one draw notification on a capacity-1 broadcast channel. N requests before the deadline produce one draw. VERIFIED (`codex-rs/tui/src/tui/frame_requester.rs:49-56,96-127`, `frame_rate_limiter.rs:23-36`, `codex-rs/tui/src/tui.rs:773`).
- Animations are **pull-based**: a widget that is animating schedules its own next frame from inside `render()` (status row 32 ms or 1000 ms; spinner 100 ms). When the widget stops being drawn the animation stops with no teardown. VERIFIED (`status_indicator_widget.rs:308-319`, `codex-rs/tui/src/motion.rs:57-69`).
- Incoming thread events are drained in a loop bounded by a one-frame deadline (`Instant::now() + TARGET_FRAME_INTERVAL`), so a flood of deltas is applied in batches between draws instead of one draw per delta or one giant blocking batch. VERIFIED (`codex-rs/tui/src/app/thread_routing.rs:1728-1760`).
- Tests: `frame_requester.rs:149-400` (immediate, delayed, coalescing, rate limit), `frame_rate_limiter.rs:44-61`.

### 3.7 Paste burst handling

Terminal-specific (terminals without bracketed paste deliver a paste as fast key events). `PASTE_BURST_MIN_CHARS = 3`, `PASTE_BURST_CHAR_INTERVAL = 8 ms`, `PASTE_ENTER_SUPPRESS_WINDOW = 120 ms`, `PASTE_BURST_ACTIVE_IDLE_TIMEOUT = 8 ms` (60 ms on Windows). The first fast ASCII char is held up to 8 ms so it is not drawn and then yanked back ("flicker suppression"); Enter inside the window inserts a newline instead of submitting. VERIFIED (`codex-rs/tui/src/bottom_pane/paste_burst.rs:1-12,150-162`). Not transferable to AppKit (NSTextView gets real paste events); the transferable idea is only "never let a paste's trailing newline submit".

---

## 4. Shimmer and status indicator: exact algorithms

There are two shimmers. The status row uses the newer one.

### 4.1 Status-row shimmer (`summary_shimmer`), the one that matches the desktop app

VERIFIED (`codex-rs/tui/src/summary_shimmer.rs:24-60`):

```rust
const START_DELAY: Duration = Duration::from_millis(600);
const SWEEP_SECONDS: f64 = 1.0;
const INTERVAL_SECONDS: f64 = 4.0;
// ...
let half_width = (width * 0.1).max(3.0);
let sweep = (elapsed.saturating_sub(START_DELAY).as_secs_f64() % INTERVAL_SECONDS).min(SWEEP_SECONDS);
let position = sweep / SWEEP_SECONDS * (width + 2.0 * half_width) - half_width;
// per grapheme, center = column + glyph_width / 2:
let distance = ((center - position).abs() / half_width).min(1.0);
let intensity = 0.5 * (1.0 + (PI * distance).cos());
let alpha = (0.5 + 0.5 * intensity) as f32;
let style = Style::default().fg(rgb_color(blend(fg, bg, alpha)));
```

with `blend(fg, bg, a) = fg*a + bg*(1-a)` per channel (`codex-rs/tui/src/color.rs:7-12`).

In words:

- Units are terminal columns. `width` = display width of the label. Band half-width `h = max(0.1 * width, 3)`, so the band is at least 6 columns wide ("so short labels do not flash one letter at a time").
- Time: `elapsed` is measured from when the **header text last changed** (`header_started_at`); setting the same text again does not restart it (`status_indicator_widget.rs:125-130`, test at `:336-350`).
- Phase: wait 600 ms; then each 4.0 s cycle spends 1.0 s sweeping and 3.0 s parked. `sweep = min((elapsed - 0.6) mod 4.0, 1.0)`.
- Band center moves linearly from `-h` (fully off the left edge) to `width + h` (fully off the right edge) during the 1.0 s sweep, then stays off the right edge. Speed = `(width + 2h)` columns per second; for "Working" that is 13 col/s.
- Per glyph: `d = min(|center - pos| / h, 1)`; `intensity = 0.5 * (1 + cos(pi * d))` (raised cosine, 1 at the center, 0 at the band edge); `alpha = 0.5 + 0.5 * intensity`.
- Color: `fg * alpha + bg * (1 - alpha)`. Resting text is a 50/50 mix of terminal foreground and background; the band peak is pure foreground. "Only brightness changes."
- Repaint cadence: 32 ms while shimmer or progress effect is on; 1000 ms otherwise (just the timer) (`status_indicator_widget.rs:309-318`).
- Fallbacks: reduced motion returns plain text; no truecolor or unknown palette returns static dim text, "instead of a stepped animation" (`summary_shimmer.rs:34-41`).
- Reduced motion is read from the OS once at launch: `NSWorkspace.accessibilityDisplayShouldReduceMotion` on macOS (`codex-rs/tui/src/system_motion.rs:22-30`).
- Tests: `codex-rs/tui/src/summary_shimmer_tests.rs:7-45` (snapshot of brightness levels at 0, 850, 1100, 1350, 3600, 5100 ms in dark and light themes, including wide and combining graphemes) and `:47-80` (sampled every 32 ms across two sweeps: whenever any glyph exceeds 220, at least two glyphs exceed 160; no glyph changes by more than 25 levels between frames). Snapshot: `codex-rs/tui/src/snapshots/codex_tui__status_indicator_widget__summary_shimmer__tests__short_and_long_labels_sweep_smoothly_in_both_themes.snap`.

### 4.2 Classic shimmer (`shimmer_spans`), still used for "Loading earlier messages…", hook status, login banners, and the activity bullet

VERIFIED (`codex-rs/tui/src/shimmer.rs:21-69`):

```rust
let padding = 10usize;
let period = chars.len() + padding * 2;
let sweep_seconds = 2.0f32;
let pos_f = (elapsed_since_start().as_secs_f32() % sweep_seconds) / sweep_seconds * (period as f32);
let pos = pos_f as usize;
let band_half_width = 5.0;
// per char i: dist = |(i + padding) - pos|
let t = if dist <= band_half_width { 0.5 * (1.0 + (PI * dist / band_half_width).cos()) } else { 0.0 };
let (r, g, b) = blend(highlight_color, base_color, highlight * 0.9);  // + BOLD
```

- Continuous loop, period 2.0 s, no rest. Units are characters. The band travels across `len + 20` character slots (10 of padding each side), so it is off-screen part of each cycle.
- Position is truncated to an integer, so the band advances in whole-character steps (for a 7-char label: 27 slots in 2 s, one step every ~74 ms).
- Band half-width 5 chars, raised cosine.
- Clock is **process start**, shared by every shimmer on screen, so all shimmering labels are phase-locked (`shimmer.rs:14-19`).
- Color: `base_color = default_fg` (fallback 128 gray), `highlight_color = default_bg` (fallback 255 white), mix weight `0.9 * t`, plus bold. Note the oddity: on a terminal that reports its colors, the "highlight" is the **background** color, so the band is a dip toward the background rather than a brightening. VERIFIED (`shimmer.rs:39-40,52-62`).
- Non-truecolor fallback: `t < 0.2` dim, `< 0.6` normal, else bold (`shimmer.rs:71-80`).
- Activity bullet: first span of `shimmer_spans("•")` on truecolor; otherwise blink `•` / dim `◦` every 600 ms (`motion.rs:99-113`).
- Spinner glyphs: `⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏` at 100 ms per frame, with an optional show-delay during which nothing is drawn; reduced motion shows static `◌` (`motion.rs:51-70`).

### 4.3 Status row layout and content

VERIFIED (`codex-rs/tui/src/status_indicator_widget.rs:228-296`). One line, left to right:

```
[activity bullet] [shimmered header] (Ns • esc to interrupt) · inline message · hook status
  └ details line(s), dim, wrapped, max 3 lines then "…"
```

- Header defaults to `"Working"` (`:100`). During reasoning it becomes the latest non-empty reasoning summary line with heading `#` and `**bold**` markers stripped; if none, it falls back to "Working" (`codex-rs/tui/src/chatwidget/streaming.rs:16-31,49-68`). This is the "rotating header": it changes only when the model emits a new summary heading, and each change restarts the 600 ms delay.
- Elapsed format: `0s`, `59s`, `1m 00s`, `59m 59s`, `1h 00m 00s` (`:77-90`, test `:352-364`).
- The timer is pause-aware and owned separately from the row (`StatusTimer`: `elapsed_running` + `last_resume_at`, `pause_at` / `resume_at`), so it stops while an approval modal is open and survives the row being hidden and re-shown (`codex-rs/tui/src/status_indicator_widget/timer.rs:7-57`).
- "Keep optional context after elapsed/interrupt text so that core interrupt affordances stay in a fixed visual location" (`:268-271`). Variable-length content goes at the end so the timer and the interrupt hint never jump.
- Shimmer and progress effects are independently switchable under one master `animations` switch (`:236-239`; snapshot `..._effects_tests__shimmer_and_progress_are_independent_and_obey_master_switch.snap`).
- Tests and snapshots: `status_indicator_widget.rs:324-589`, `status_indicator_widget/effects_tests.rs`, `status_indicator_widget/timer_tests.rs`, `codex-rs/tui/src/snapshots/codex_tui__status_indicator_widget__tests__*.snap`.

### 4.4 Reimplementation in Swift

Mapping: 1 terminal column is roughly one average glyph advance. For a proportional font, replace "columns" with points and express the band in terms of text width.

Parameters to carry over unchanged: `startDelay = 0.6`, `sweep = 1.0`, `interval = 4.0`, `halfWidth = max(0.1 * textWidth, 3 * avgGlyphAdvance)`, base alpha 0.5, peak alpha 1.0, raised-cosine profile, restart only when the label string changes, disable under Reduce Motion.

**Option A: Core Animation gradient mask (recommended; zero per-frame main-thread work).**

- Two stacked `CATextLayer`s (or one `NSTextField` layer duplicated): `base` at 50% opacity, `highlight` at 100% opacity, same string and frame.
- Mask `highlight` with a `CAGradientLayer` of width `2 * halfWidth`, horizontal, whose alpha stops approximate the raised cosine: locations `[0, .125, .25, .375, .5, .625, .75, .875, 1]`, alphas `[0, .146, .5, .854, 1, .854, .5, .146, 0]`.
- Animate the mask's `position.x` from `-halfWidth` to `textWidth + halfWidth`, linear timing, duration 1.0 s, inside a `CAAnimationGroup` of duration 4.0 s with `repeatCount = .infinity` and `beginTime = CACurrentMediaTime() + 0.6`. The group's idle 3 s leaves the mask parked off the right edge (set the model value there).
- On label change: replace the string, remove and re-add the animation (this reproduces `header_started_at` reset). On same-string updates: do nothing.
- `base` at 50% opacity over the window background equals `blend(fg, bg, 0.5)` only on an opaque background; on vibrancy, prefer an explicit secondary label color for `base`.

**Option B: per-glyph color (exact port; use only if you need per-grapheme fidelity).**

```swift
func shimmerAlpha(glyphCenter x: CGFloat, textWidth w: CGFloat, avgAdvance a: CGFloat,
                  elapsed t: TimeInterval) -> CGFloat {
    let h = max(0.1 * w, 3 * a)
    let sweep = min(max(t - 0.6, 0).truncatingRemainder(dividingBy: 4.0), 1.0)
    let pos = CGFloat(sweep) * (w + 2 * h) - h
    let d = min(abs(x - pos) / h, 1)
    return 0.5 + 0.25 * (1 + cos(.pi * d))   // 0.5 at rest, 1.0 at band center
}
```

Drive it from a `CADisplayLink` / `TimelineView(.animation(minimumInterval: 1/30))` (Codex uses 32 ms), and only while `sweep < 1.0`; schedule a one-shot wake at the next cycle start for the 3 s rest instead of ticking through it.

Either way, follow Codex's rule that the animation is owned by the thing being drawn: when the row is not visible, nothing ticks.

---

## 5. Queue, steer, and interrupt semantics (TUI)

### 5.1 Keys

VERIFIED (`codex-rs/tui/src/keymap.rs:1865,1876-1882`, `codex-rs/tui/src/bottom_pane/chat_composer.rs:3647-3655`):

| Key (default, all remappable) | Idle | Turn running |
|---|---|---|
| Enter (`composer.submit`) | Start a turn | **Steer** the running turn |
| Tab (`composer.queue`) | Submit normally | **Queue** as a follow-up for after the turn |
| Shift+Left or Alt+Up (`chat.edit_queued_message`) | n/a | Pop the **last queued** message back into the composer for editing |
| Esc (`chat.interrupt_turn`) | First Esc shows a hint; second enters backtrack | Interrupt; with pending steers, "interrupt and send immediately" |

Key auto-repeat is ignored for submit and queue keys, so holding Enter cannot double-send (`chat_composer.rs:3642-3646`).

### 5.2 Three visible buckets above the composer

VERIFIED (`codex-rs/tui/src/bottom_pane/pending_input_preview.rs:14-31,97-177`, `codex-rs/tui/src/chatwidget/input_queue.rs:23-56`):

1. **Pending steers**: "Messages to be submitted after next tool call (press esc to interrupt and send immediately)". Already sent to the server, not yet committed into history.
2. **Rejected steers**: "Messages to be submitted at end of turn". Steers the server refused because the active turn is not steerable (review, compact); retried first when the turn ends.
3. **Queued follow-up inputs**, italic, with the hint "shift+left edit last queued message".

Each entry is previewed to at most 3 wrapped lines then `…` (`PREVIEW_LINE_LIMIT = 3`, `pending_input_preview.rs:38`).

### 5.3 Optimistic send and reconciliation

VERIFIED (`codex-rs/tui/src/chatwidget/input_submission.rs:257,478-617`, `codex-rs/tui/src/chatwidget/input_receipts.rs:1-127`):

- Every submission mints `client_user_message_id = Uuid::new_v4()`.
- **Idle**: `render_in_history = true`. The user cell is inserted into the transcript **before** the op is submitted ("Queue the optimistic prompt first so the user's input is visible while that happens", `input_submission.rs:525-542`), `user_turn_pending_start` is set, and the status header is forced to "Working" immediately (`codex-rs/tui/src/chatwidget/input_flow.rs:68-81`). `last_rendered_user_message_client_id` records the id so the server's echoed `UserMessage` item is not rendered twice.
- **Turn running**: `render_in_history = false`. The message becomes a `PendingSteer {client_id, admitted_turn_id: None, ...}` shown in bucket 1; it is not put in the transcript yet.
- Receipts (`UserMessageAdmission`): `Started(turn_id)` moves a pending steer into history now (the turn had ended in the meantime, so the "steer" became a new turn); `Steered(turn_id)` stamps `admitted_turn_id`; `Queued` moves it to rejected steers; `Duplicate` holds it as `Unconfirmed` and blocks auto-send until history confirms ("Ambiguous delivery stays out of automatic submission until history confirms it").
- When the committed `UserMessage` item arrives with a matching `clientId`, `take_pending_steer(client_id)` removes the preview entry and the message appears in the transcript at its true position in the turn.
- On rejection the draft is **restored to the composer** with an error line (`restore_rejected_user_message`, `input_receipts.rs:120-127`). Text is never lost.

### 5.4 Queue drain

VERIFIED (`codex-rs/tui/src/chatwidget/input_flow.rs:250-300`): `maybe_send_next_queued_input` runs when the turn ends or a modal closes. It sends **one** queued message per idle transition (the loop breaks after a successful plain submit) and refuses while the session is unconfigured, a fork is in progress, submissions are paused, rate-limit recovery is pending, or any message is unconfirmed. Slash commands queued with Tab are parsed at dequeue time, not at queue time (`QueuedInputAction::ParseSlash`).

Messages typed before the session is configured are queued and sent once it is ready, and the first composer is "editable and bottom-anchored while startup work continues" (`codex-rs/tui/src/startup_draft.rs:1-3`, `codex-rs/tui/src/startup_orchestration.rs:1-4`, `input_flow.rs:155`).

### 5.5 Interrupt

VERIFIED (`codex-rs/tui/src/chatwidget/interaction.rs:296-321`, `codex-rs/tui/src/chatwidget/input_restore.rs:432-520`):

- **Esc with pending steers**: sets `submit_pending_steers_after_interrupt`, sends interrupt. When the abort arrives, history gets "Model interrupted to submit steer instructions." and all pending steers are **merged into one message and resubmitted as a fresh turn** (`merge_user_messages_with_history_record`).
- **Esc with no pending steers**: history gets "■ Conversation interrupted - use /feedback if something went wrong". Then pending steers, rejected steers, queued follow-ups, **and the current composer draft** are merged into the composer (`drain_pending_messages_for_restore`, with image placeholders renumbered and text-element byte ranges rebased). The queue is **not** auto-sent after a plain interrupt.
- During `/review`, Esc with pending steers shows "Steer messages aren't supported during /review. Press Ctrl+C now to cancel the review." instead of interrupting (`interaction.rs:294-306`).
- Turn termination "settles background activity without opening queued prompts" (`interrupts.rs:1-3`): approvals deferred behind a stream are dropped, not popped open after the user hit Esc.
- An in-flight answer stream is flushed (not discarded) on termination: "Preserve received answer and plan source before ordinary turn termination" (`chatwidget/streaming.rs:70-86`).

### 5.6 Backtrack (edit a previous message)

VERIFIED (`codex-rs/tui/src/app_backtrack.rs:1-21`, `codex-rs/tui/src/bottom_pane/footer.rs:218-224`):

- Idle, empty composer: first Esc "primes" and shows a hint; a second Esc opens transcript browsing with the latest user prompt highlighted.
- Left/Right choose prompts; Esc restores the origin; Enter "requests a revert before the selected prompt and reopens it for editing" (thread reverted via `thread/revert`, prompt text restored into the composer).
- While a task is running, Esc never enters the hint state (`esc_hint_mode` returns the current mode), so interrupt always wins.
- Escape precedence: dismiss shortcut help, then editing, then backtrack, then interrupt (`chat_composer.rs:21`).

---

## 6. Ranked recommendations for a native Swift/AppKit client

Ranked by expected effect on perceived quality per unit of work. Each cites the verified source it is derived from; the Swift mapping is my INFERENCE.

1. **Split every streaming message into an append-only stable region and one mutable tail.** Commit source at newline boundaries only. Render committed blocks once and cache their layout (attributed string + measured height); re-render only the last top-level block. Keep tables, open fences, and open display math in the tail until their block closes. In AppKit terms: committed blocks are immutable rows or immutable text-storage ranges whose heights never change; the tail is one view at the bottom whose height may change. This is what removes mid-stream reflow. (`streaming/controller.rs:1-43`, `streaming/render.rs:1-41`, `streaming/table_holdback.rs:1-11`)
2. **Show the unterminated line as a disposable preview.** Do not wait for the newline to show text, and do not commit it either. Withhold ambiguous structure (a line containing `|`, a line starting with a fence or indent, an unfinished link destination) until it resolves. This gives token-by-token liveness without ever showing markup that then changes meaning. (`streaming/prose_preview.rs:1-17,44-76`)
3. **Pace commits with the two-gear policy and copy the constants.** One line per display tick in smooth mode; drain everything when depth >= 8 or oldest age >= 120 ms; leave catch-up only after 250 ms below (2 lines, 40 ms); 250 ms re-entry hold unless (64 lines, 300 ms). Run an opportunistic catch-up-only tick on every delta. Stop the timer when the queue is empty. Use a `CADisplayLink`-driven tick; at 60 Hz smooth mode is 60 lines/s, which is fine. (`streaming/chunking.rs:85-116,180-294`, `chatwidget/streaming.rs:756-759`)
4. **Mint a client message id and render the user bubble before the RPC.** Track `pendingStart` until the runtime confirms; flip the status row to "Working" in the same runloop turn as the keypress. Dedupe the echoed user message by id (on ACP, where there is no echo id, simply never insert user chunks for a prompt you originated). On rejection, put the text back in the composer. (`chatwidget/input_submission.rs:480,525-542,612-615`, `chatwidget/input_receipts.rs:120-127`, `chatwidget/input_flow.rs:68-81`)
5. **Treat end-of-item text as authoritative and reconcile once.** At completion, compare the final text with what was streamed; if equal, just swap the run of streamed fragments for one source-backed cell with no visual change; if different, re-render that one message. Never let dropped or coalesced deltas truncate the transcript. If Memex ever applies backpressure to its own ACP event pipe, drop deltas, never lifecycle events. (`chatwidget/streaming.rs:105-134,293-295`, `app-server/src/in_process.rs:114-133`)
6. **Use the desktop-matching shimmer and make it rest.** 600 ms delay, 1 s sweep, 4 s interval, band >= 6 glyph widths or 20% of the label, base 50% and peak 100% of the label color, restart only on label change, off under Reduce Motion. Implement as a Core Animation gradient mask so it costs nothing on the main thread. (`summary_shimmer.rs:1-60`; Swift sketch in section 4.4)
7. **Exactly one progress affordance, in a fixed place, and no flapping.** Hide the status row the moment assistant text is visibly streaming; restore it only when the stream queue is idle and the turn is still running. Put the elapsed timer and the interrupt hint before any variable-length text. Keep the timer pause-aware (stop it while an approval or question is open). Drive the header from the latest reasoning summary heading, falling back to "Working". (`chatwidget/streaming.rs:185-207`, `status_indicator_widget.rs:258-272`, `status_indicator_widget/timer.rs:27-57`)
8. **Mutate tool rows in place, keyed by tool-call id.** Create the row on start (shimmering bullet, present-tense verb), update the same row on completion (colored bullet, past-tense verb, duration), and coalesce consecutive read/list/search calls into one "Exploring / Explored" group. Bound live command output to a tail window (50 lines) and replace it with the final output on completion. Never insert-then-remove. (`exec_cell/model.rs:1-8`, `exec_cell/render.rs:318-335,465-481`, `exec_cell/live_output.rs:5-6`)
9. **Defer approvals, questions, and tool rows that arrive while assistant text is mid-stream, in FIFO order, until the stream flushes; and drop deferred prompts if the user interrupts.** This prevents a sheet from appearing over half-rendered text and prevents out-of-order rows. (`chatwidget/streaming.rs:693-718`, `chatwidget/interrupts.rs:1-3`)
10. **Make steer and queue two explicit actions with three visible buckets.** Enter steers ("will be delivered after the next tool call"), a second key queues, and refused steers get their own "at end of turn" bucket. Offer "interrupt and send now" on Esc when steers are pending, merging them into one new turn. On a plain interrupt, do not auto-send the queue: merge queue plus draft back into the composer. Send one queued message per idle transition. Let the user pop the last queued message back for editing. (`bottom_pane/pending_input_preview.rs:97-177`, `chatwidget/interaction.rs:308-321`, `chatwidget/input_restore.rs:432-520`, `chatwidget/input_flow.rs:250-300`)
11. **Coalesce redraws through one scheduler and batch event application per frame.** Any component may request a frame "now" or "in N ms"; one scheduler keeps the earliest deadline and fires once. Apply incoming events in a loop bounded by a one-frame time budget, then draw. In AppKit: accumulate model mutations and apply one `NSTableView`/diffable snapshot update per display-link tick; never `reloadData` per delta. (`tui/frame_requester.rs:96-127`, `app/thread_routing.rs:1728-1760`)
12. **Instant session switching from a per-session snapshot plus bounded event buffer; instant open from a summary page.** Keep, per live session, the last materialized transcript model plus events received while it was not frontmost, and replay locally on switch. For cold opens, paint the newest page of turns in summary form first, then page older turns and deferred tool outputs on scroll ("Loading earlier messages…" with a shimmer). Keep sessions warm for a grace period after they lose focus (Codex uses 60 s). (`app/thread_events.rs:98-119`, `app.rs:324`, `v2/thread.rs:419-487,1851-1985`, `config_toml.rs:351-354`)
13. **Let the composer accept input before the runtime is ready.** Show an editable, bottom-anchored composer immediately on new chat; if the user submits before the session exists, hold that one submission and dispatch it when the session is configured. (`startup_draft.rs:1-3`, `startup_orchestration.rs:1-4`, `chatwidget/input_flow.rs:155`)
14. **Withhold the interrupt acknowledgement until the turn has really stopped,** and show a distinct, calm "interrupted" marker. Preserve whatever text had streamed. (`turn_processor.rs:1473-1535`, `chatwidget/streaming.rs:70-86`, `chatwidget/input_restore.rs:455-463`)
15. **Test the feel, not just the logic.** Codex unit-tests the pacing thresholds with injected clocks, snapshot-tests shimmer brightness at fixed timestamps in both themes, and asserts a maximum frame-to-frame brightness delta (<= 25/255 at 32 ms) so the animation cannot regress into flashing. The same style works in XCTest with an injected clock. (`streaming/chunking.rs:296-457`, `summary_shimmer_tests.rs:7-80`, `tui/frame_requester.rs:149-400`)

### Things not worth copying

- Paste-burst detection (terminal-only problem).
- The classic 2 s continuous shimmer with integer stepping and background-colored "highlight"; the summary shimmer supersedes it.
- A fixed 120 Hz commit timer independent of the display; use the display link.

### Open questions this research did not settle

- Exactly where in core a steer is injected (I verified the client-facing statement "after next tool call", not the core code path).
- How the closed-source desktop app orders optimistic bubbles, handles worktree-backed threads, or uses `thread/queue/*`; nothing public and specific was found.
- The ACP column of the comparison table needs checking against the ACP schema version Memex pins.
