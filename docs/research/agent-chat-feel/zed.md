# Zed agent panel / ACP thread: implementation-level UX techniques

Source: `github.com/zed-industries/zed` shallow clone at
`f0d896f2c18a543335eb155f729d36b624695591` (HEAD of `main`, committed 2026-11-09).
Clone path: `/private/tmp/claude-501/-Volumes-Dev-Code-memex--claude-worktrees-agent-app-ux-patterns-13a76b/f8deb1e4-a002-4452-a81e-c8ef8a51001b/scratchpad/repos/zed`

Labels: **VERIFIED** = read in source at that SHA (`path:line`). **UNVERIFIED** = from a PR/issue
description or web result, URL given; not confirmed in source (shallow clone has no history).
All paths are relative to the repo root. Nothing from the clone was built or run.

Crate map (line counts at this SHA): `crates/acp_thread/src/acp_thread.rs` (26k, model),
`crates/agent_ui/src/conversation_view.rs` (21k, per-thread container + event routing),
`crates/agent_ui/src/conversation_view/thread_view.rs` (16k, transcript + composer rendering),
`crates/agent_ui/src/agent_panel.rs` (15k, thread switching/retention),
`crates/agent_servers/src/acp/{v1,v2}.rs` (wire), `crates/markdown/src/markdown.rs` (9.5k),
`crates/gpui/src/elements/list.rs` (3.9k, virtualized list).

---

## 1. TL;DR

1. Zed's "feel" comes from five structural choices, not from decoration:
   a typewriter reveal buffer between the wire and the markdown view; markdown parse and syntax
   highlight entirely off the main thread with latest-wins coalescing and "keep old render until
   new parse lands"; a virtualized list with a first-class follow-tail state machine and
   provisional measurements; lazy materialization of everything off-screen; and warm
   connections/sessions/views so switching never touches the agent.
2. There is **no shimmer in Zed**. In-progress affordances are a one-glyph braille spinner at the
   tail of the list, a 2 s sine-cubic alpha "breathing" on labels, a 2 s rotating circle icon, and
   one staggered skeleton-bar loader for pending diffs. Tool calls carry no per-row spinner.
3. Optimism is selective. The user bubble is pushed locally before `session/prompt` and marked
   `is_optimistic`; cancel flips all pending tool calls to Canceled and goes Idle before the agent
   confirms. But there is no optimistic transcript while a session is being created: that shows a
   pulsating "Loading…" label. They reduce exposure to that state by sharing one connection per
   agent and reusing a parked empty draft thread, not by a proactive session pool.
4. Anti-flash is done with thresholds: "Sending…" appears only after 1 s unsettled, the stopwatch
   after 30 s, token counts after 250 tokens, terminal elapsed after 10 s.
5. Their jank history is almost entirely scroll: `splice` on streamed entries reset the scroll
   position (fixed with `remeasure_items`), follow-tail snapped wrongly (rewritten as a state
   machine in the list), scrollbar drag fought streaming growth (height frozen during drag),
   per-frame tree-sitter highlighting made streaming janky (moved to parse time + LRU), and opening
   long threads cost hundreds of MiB to GiBs (lazy markdown + lazy diffs).

---

## 2. Architecture of the send -> stream -> render loop

### Layers

- **Model**: `AcpThread` owns `entries: Vec<AgentThreadEntry>` (UserMessage, AssistantMessage with
  Message/Thought chunks, ToolCall, SessionMessage, Elicitation, ContextCompaction), plans,
  permission requests, submissions, and the streaming reveal buffer. VERIFIED
  `crates/acp_thread/src/acp_thread.rs:565`, `:3740-3792`.
- **Events**: the model emits coarse, index-addressed events: `NewEntry`, `EntryUpdated(usize)`,
  `EntriesRemoved(Range)`, `StatusChanged`, `SubmissionUpdated`, `Stopped{..}`, `Error`, ...
  VERIFIED `acp_thread.rs:3912-3950`.
- **View state**: `EntryViewState` mirrors entries with per-entry view objects (an editor per user
  message, lazily built diff/terminal views per tool call, expansion sets). VERIFIED
  `crates/agent_ui/src/entry_view_state.rs:56-70`, `:296-470`.
- **List**: one `gpui::ListState` per thread view; item count = entries (+1 for the generating
  indicator). VERIFIED `crates/agent_ui/src/conversation_view.rs:1595-1628`,
  `thread_view.rs:7408-7438`, `:8644-8659`.
- **Both agent kinds share this path.** The native Zed agent implements the same
  `AgentConnection` trait and pushes into `AcpThread` with the same calls
  (`push_assistant_content_block`, `update_tool_call`). VERIFIED `crates/agent/src/agent.rs:2796-2811`,
  `:2899`. So everything below applies to external ACP agents and the native agent alike.
- **Wire**: JSON-RPC IO is decoded on a background task and handed to the main thread through an
  unbounded foreground work queue. VERIFIED `crates/agent_servers/src/acp/runtime.rs:57`,
  `crates/agent_servers/src/acp.rs:238-400`.

### Sequence at t=0 on Enter (idle thread, "Completion" transport = classic ACP v1 prompt)

1. `ThreadView::send` — if the composer is empty and something is queued, fast-track the queue;
   if a turn is generating, enqueue instead; otherwise `send_impl`. VERIFIED
   `thread_view.rs:2190-2281`.
2. `send_impl` starts resolving composer contents (mentions -> content blocks, async), clears
   error/feedback/editing state, resumes the queue. The composer is cleared **only after contents
   resolve**, and `is_loading_contents` blocks a double send. VERIFIED `thread_view.rs:2344-2384`,
   `:2401-2410`.
3. `send_content` (after contents resolve, same foreground task): starts the turn timer
   (`start_turn`), collapses an expanded composer, calls `list_state.scroll_to_end()`, sets a
   provisional title from the first line of the prompt (truncated to 200 chars) if the thread has
   none, then calls `thread.send(contents)`. VERIFIED `thread_view.rs:2414-2501`.
4. `AcpThread::submit` registers a `SubmissionRecord` (state `Pending`) and, for the Completion
   transport, `send_inner`: validates content, **pre-builds the rendered user message block**, mints
   a client-side user message id, then `run_turn`. VERIFIED `acp_thread.rs:7406-7421`,
   `:7491-7540`.
5. `run_turn` (synchronous part): requests cancellation of any previous turn, bumps `turn_id`,
   records `RunningTurn{first_entry_index}`, sets foreground state Running and emits
   `StatusChanged` — so the generating indicator row is spliced into the list in the same update
   as the send. VERIFIED `acp_thread.rs:7590-7629`; indicator splice `thread_view.rs:8644-8659`.
6. The spawned turn task, after `cancel_task` resolves (immediately when nothing was running),
   **pushes the user entry with `is_optimistic: true`, then issues the prompt request**. VERIFIED
   `acp_thread.rs:7513-7539`. Source comment at `acp_thread.rs:5038`: "We optimistically add the
   full user prompt before calling `prompt`."
7. `NewEntry` -> `ConversationView::handle_thread_event` builds the entry's view state and does
   `list_state.splice_focusable(index..index, ..)`. VERIFIED `conversation_view.rs:1988-2008`.
8. Agent echoes of the user message (`UserMessageChunk`) are dropped when they match the local
   optimistic message; the protocol id is adopted onto the local entry. VERIFIED
   `acp_thread.rs:5032-5063`. `upsert_local_user_message` later flips `is_optimistic = false` and
   updates the content in place. VERIFIED `acp_thread.rs:7457-7480`.
9. `AgentMessageChunk`/`AgentThoughtChunk` -> `push_assistant_content_block_with_message_id`. The
   first text chunk of a message creates the entry + a deferred `Markdown` entity; every later text
   chunk for the same trailing markdown is appended to the retained source blocks and metered into
   the view by the reveal buffer (section 4.1). Each chunk emits `EntryUpdated(ix)`. VERIFIED
   `acp_thread.rs:5764-5869`, `:5871-5958`.
10. `EntryUpdated(ix)` -> `sync_entry` + `list_state.remeasure_items(ix..ix+1)` (never `splice`) +
    auto-expand of the streaming thought + indicator sync. VERIFIED `conversation_view.rs:2010-2029`.
11. `ToolCall`/`ToolCallUpdate` -> upsert/patch a `ToolCall` entry in place (label markdown,
    status, content, diffs, terminals). VERIFIED `acp_thread.rs:5090-5095`, `:6280-6560`.
12. Prompt response -> `flush_streaming_text`, state Idle, `Stopped{..}`; the view removes the
    indicator row, and if still following tail calls `scroll_to_end()` once more. VERIFIED
    `acp_thread.rs:7631-7745`, `conversation_view.rs:2065-2098`.
13. `Stopped` also drives the queue: the next queued message is auto-sent unless paused or the
    user is focused in the first queued editor. VERIFIED
    `crates/agent_ui/src/conversation_view/message_queue.rs:159-186`, `thread_view.rs:3082-3110`.

Stale-turn safety: every completion handler compares `turn_id` / `current_submission` /
`turn_generation` before touching state, so a superseded turn's late response cannot clobber the
new turn's UI. VERIFIED `acp_thread.rs:7640-7672`, `thread_view.rs:2520-2530`. A regression test
exists for exactly this: `test_stale_stop_does_not_disable_follow_tail_during_regenerate`.
VERIFIED `conversation_view.rs:15306`.

### State representation of the interaction features

| Feature | Representation | Where |
|---|---|---|
| Pending send | `SubmissionState::{Pending, Accepted{echoed}, Disconnected{receipt}, Completed, Failed, Cancelled}` per submission, plus `is_optimistic` on the user entry | VERIFIED `crates/acp_thread/src/submission.rs:111-125`, `acp_thread.rs:335-342` |
| Queue | `MessageQueue{entries: VecDeque<QueueEntry>, processing_state: AutoProcess/Paused/AbsorbingCancel, can_fast_track}`; each entry has a stable id and its own read-only editor. In-memory per view; snapshotted only into reconnect recovery | VERIFIED `message_queue.rs:8-48`; recovery `thread_view.rs:80-93`, `:956-1016` |
| Steering | Per-entry `steer` flag; only the front entry matters; sets `end_turn_at_next_boundary` on the native thread. External agents always wait for turn end | VERIFIED `message_queue.rs:12-15`, `thread_view.rs:3054-3069`, `crates/agent/src/thread.rs:2417` |
| Send-now / interrupt-and-send | `thread.cancel()` awaited, then normal send; queue enters `AbsorbingCancel` so the cancel's own Stopped event does not double-send the next entry | VERIFIED `thread_view.rs:2555-2606`, `:3112-3185`, `message_queue.rs:24-27`, `:143-197` |
| Cancel | Local-first: tool calls in Pending/WaitingForConfirmation/InProgress -> `Canceled`, permission requests resolved, state Idle + `StatusChanged` emitted before the agent's response settles. User stop also pauses the queue | VERIFIED `acp_thread.rs:7853-7877`, `:7968-8004`; `thread_view.rs:2727-2734` |
| Edit-and-resend | Every user message is a real editor; focusing sets `editing_message`; Enter -> `regenerate`: keep earlier edits, `thread.rewind(client_id)` (agent `truncate`, then truncate entries, kill removed terminals, reject action-log edits), then `send_impl` from that editor. Not optimistic: waits for the agent's truncate | VERIFIED `thread_view.rs:1616-1641`, `:2805-2879`; `acp_thread.rs:8019-8062` |
| Checkpoint/restore | No snapshot checkpoints at this SHA. Restore = rewind-to-user-message (above) + action-log Keep All / Reject All / Undo Last Reject on the edits bar | VERIFIED `thread_view.rs:3825-3866`, `acp_thread.rs:4375` |
| Send failure rollback | The user bubble is **not** removed on generic error. Pending tool calls are canceled, `had_error` set, error callout shown. Exception: a model **refusal** with no completed tool output truncates entries back to and including the user message (`EntriesRemoved`). For receipt transports, failed/cancelled submissions are listed as recoverable cards and can be restored into an empty composer | VERIFIED `acp_thread.rs:7746-7758`, `:7925-7945`; `thread_view.rs:1859-1895`, `:1936-1975` |
| Queued-send failure | If the pre-send cancel fails, the entry is re-inserted at its original position and the queue is paused | VERIFIED `message_queue.rs:121-129`, `thread_view.rs:3163-3172` |
| Structured questions | `Elicitation` entries with `ElicitationStatus`, owned by an `ElicitationStore`; cancel sweeps outstanding ones | VERIFIED `acp_thread.rs:578-612`, `:8006-8015` |
| Plans | `plans: HashMap<PlanIdentity, Plan>` + `visible_plan`; completed plan entries cleared at the start of each new turn; rendered in the activity bar above the composer, not in the transcript | VERIFIED `acp_thread.rs:3754-3756`, `:7602`; `thread_view.rs:4858-5120` |

---

## 3. Techniques: what / where / why it feels good

All VERIFIED at the SHA unless marked.

| # | Technique | Where | Why it feels good |
|---|---|---|---|
| 1 | **Typewriter reveal buffer.** Text chunks append to retained source immediately; a 16 ms timer reveals a byte budget into the markdown view, sized so the pending backlog drains in ~200 ms | `acp_thread.rs:3849-3879`, `:5925-5958`, `:5984-6011`; cursor `:3802-3828` | Decouples visual cadence from network burstiness. Big chunks do not pop in; small ones do not stutter. Authoritative text is never behind |
| 2 | **Flush on every structural boundary.** `flush_streaming_text` before pushing any entry, on target switch (thought -> message), on cancel, on rewind, on turn end and error | `acp_thread.rs:5794`, `:5942`, `:6014`, `:7483`, `:7676`, `:7750`, `:7823`, `:8028` | The reveal never lags behind a tool call or the end of the turn; copy/export always has full text because source is separate from revealed text |
| 3 | **Markdown parse fully off-main with latest-wins coalescing.** One background parse in flight; further appends only set `should_reparse`; old parsed tree stays on screen until the new one swaps in | `crates/markdown/src/markdown.rs:1246-1274`, `:1286-1451`; "keep existing content visible" `:1088` | Main thread never parses. No blank frame between parses. Under fast streaming, parses naturally batch |
| 4 | **Syntax highlighting at parse time, not render time**, memoized in a content-keyed LRU (4 MiB budget, entries capped at 1/8 of it) so unchanged code blocks are free on each streaming reparse | `markdown.rs:1401`, `:1711-1800`; `crates/language_core/src/highlight_cache.rs:14-17`, `:83-90`; staleness check `markdown.rs:4251` | Removes the dominant per-frame cost during streaming and scrolling of code-heavy answers |
| 5 | **In-place markdown updates on reconcile.** `update_markdown_in_place`: if new text has the old as prefix, append the suffix; only otherwise reset. `replace_blocks` reuses existing render blocks/images by position and source equality | `acp_thread.rs:8713-8721`, `:2433-2565` | Upserts/replays/echo reconciliation do not tear down views, lose selection, or flash |
| 6 | **Follow-tail as a list state machine**: `FollowState::Tail{is_following}`. Layout snaps to end while following; any upward scroll stops it; returning within 1 px of the bottom resumes it | `crates/gpui/src/elements/list.rs:97-146`, `:563-565`, `:903-905`, `:1053-1066`, `:1228-1238`; enabled `conversation_view.rs:1596` | Stick-to-bottom without a timer or a "near bottom" heuristic racing with streaming. User intent always wins |
| 7 | **`remeasure_items(range)` instead of `splice` for content growth.** Keeps the item, its focus handle, and its previous height as a `size_hint`; preserves the scroll anchor's in-item offset | `list.rs:404-462`; used at `conversation_view.rs:2017` | Streaming into an entry never moves what the user is reading and never collapses total height to "unknown" |
| 8 | **Provisional measurements** via `set_item_measurement_readiness`: an item whose markdown is still parsing is measured but kept "Unmeasured with hint"; the list holds the scroll intent (`pending`, `provisional`, `pending_end`) and retries | `list.rs:647-652`, `:231-254`, `:1087-1128`, `:1157-1193`; wired `conversation_view.rs:1597-1608`; predicate `acp_thread.rs:1039-1068` | Restored scroll position and "scroll to end" survive async height changes instead of landing in the wrong place |
| 9 | **Lazy markdown.** All message/tool markdown is created with `defer_parsing: true`; parse starts on first layout | `acp_thread.rs:2880-2899`, `:1757`, `:8697`; `markdown.rs:535-537`, `:1233-1238`, `:2682-2683` | Opening a long thread parses only what is near the viewport |
| 10 | **Lazy tool-call views.** On restore (`sync_entry_initial` / `ToolCallSync::Restore`) diff editors and terminal views are built only for visible/expanded cards; expanding materializes | `entry_view_state.rs:44-54`, `:274-294`, `:385-386`; `conversation_view.rs:1610-1618`; `thread_view.rs:1576-1585` | Thread open cost is independent of tool-call count |
| 11 | **Virtualized list with 2048 px overdraw**, top-aligned | `conversation_view.rs:1595`; `list.rs:1079-1128`, `:1195-1218` | Fast scroll without pop-in; only on-screen + overdraw items are built |
| 12 | **Retained thread views.** Switching away parks the whole `ConversationView` in `retained_threads`; switching back re-activates the same entity. Running threads and non-reloadable ones are never evicted; idle reloadable ones are LRU-capped (`max_idle_retained_threads`, default 5) | `agent_panel.rs:4228-4334`, `:4502-4552`; default `assets/settings/default.json:1195` | Thread switching is a pointer swap: no reload, no reparse, scroll/expansion state intact |
| 13 | **Reusable parked draft.** The panel keeps one empty `draft_thread` (its session is created when the draft view is built, on demand); "new thread" re-activates it if it still exists and matches the agent. It is released once it gets its first message, so the next new thread pays one `session/new` on the shared connection. Native connection is requested at panel construction | `agent_panel.rs:2974-3070`, `:3080-3092`, `:1580`, `:2958-2972`; session creation `conversation_view.rs:1441-1446` | Returning to "new chat" is usually a pointer swap with typed text intact. Not a proactive warm pool |
| 14 | **One shared connection per agent per project**, exposed as a `Shared<Task>` so concurrent views await the same connect | `crates/agent_ui/src/agent_connection_store.rs:16-45`, `:186-205` | Second and later threads for an agent skip process spawn + initialize |
| 15 | **Lazy auth.** No auth check up front; `AuthRequired` is detected from the `session/new`/load error and only then is the auth UI shown | `conversation_view.rs:1453-1461`; `crates/agent_servers/src/acp.rs:738-739` | Happy path has zero auth round trips |
| 16 | **Delayed-reveal thresholds** (anti-flash) | 1 s: `thread_view.rs:76`, `:1897-1934`; 30 s / 250 tokens: `conversation_view.rs:107-108`, `thread_view.rs:8663-8684`; 10 s: `crates/agent_ui/src/ui/terminal_tool_header.rs:7` | Fast operations show no status at all |
| 17 | **One generating indicator as a virtual trailing list row**, spliced in/out with status | `thread_view.rs:8644-8659`, `:7426-7430` | The "working" affordance lives where new content will appear and participates in follow-tail; no overlay, no layout jump in the composer |
| 18 | **Spinner isolated in its own tiny view** so animation frames re-render only that view; `request_animation_frame` notifies only the current view | `thread_view.rs:285-323`; `crates/gpui/src/window.rs:2935-2945` | A 60 Hz spinner does not re-render the transcript |
| 19 | **Top-down layout, no layout shift on first send.** Empty thread: composer is full-height (`EditorMode::Full`); after first message it becomes auto-height at the bottom; content streams down from the first prompt | `thread_view.rs:8612-8641`; `ListAlignment::Top` `conversation_view.rs:1595` | The prompt you typed stays put; the answer grows beneath it |
| 20 | **Collapsed-by-default tool output**, expansion is an opt-in set keyed by tool-call id; edit and terminal cards auto-expand by setting; a pending permission forces content visible | `entry_view_state.rs:96-116`; `thread_view.rs:1596-1615`; defaults `assets/settings/default.json:1391`, `:1395` | Transcript height changes mostly at the tail; status transitions do not reflow old rows |
| 21 | **Thinking auto-expand/auto-collapse** with user-override tracking, optional height-capped "Preview" mode with internal auto-scroll and a top fade | `entry_view_state.rs:151-256`; `thread_view.rs:8815-8919` | Live thought is visible while it streams and gets out of the way when the answer starts, unless the user touched it |
| 22 | **Scrollbar drag freezes content height**; dragging to the frozen end re-engages follow | `list.rs:742-756`, `:1389-1416` | The thumb does not run away from the cursor while content streams |
| 23 | **Explicit pause of follow-tail for user-initiated growth** (expanding a compaction summary, zooming a diagram) | `list.rs:628-645`; `thread_view.rs:5255-5282`, `:13239` | Expanding something at the bottom does not yank you to its end |
| 24 | **Focused edits are not overwritten.** A user-message editor is refreshed from the model only when not focused (if editable) and only when `source_version` changed | `entry_view_state.rs:325-343` | Reconciliation never eats a draft edit or resets the caret |
| 25 | **Scroll position persisted per thread**, saved via a throttled, coalesced task from the scroll handler; restored before first layout | `thread_view.rs:1344-1365`, `:1376-1388`; `conversation_view.rs:1620-1628`; `acp_thread.rs:3786-3787` | Reopening a thread lands where you left it |
| 26 | **Draft prompt persistence** with 250 ms debounce, restored when the draft view is rebuilt | `conversation_view.rs:110`; `agent_panel.rs:4570-4576` | Unsent text survives window close |
| 27 | **Thread metadata updates decoupled from streaming**: only structural events mark the sidebar metadata dirty; `EntryUpdated` does not | `conversation_view.rs:769-799`, `:1958-1961` | The thread list does not churn or re-sort on every token |
| 28 | **Session updates that arrive before the `session/new`/load response are staged** (bounded: 1024 updates / 16 MiB) and applied in order afterwards | `crates/agent_servers/src/acp/session_updates.rs:10-11`, `:52-78` | No lost or reordered early chunks; no unbounded memory on a misbehaving agent |
| 29 | **Background compressed persistence** for native threads: JSON + zstd in SQLite, written from a pending-save slot (latest wins) | `crates/agent/src/db.rs:516-590`; `crates/agent/src/agent.rs:1970-2025` | Saves never block typing or streaming |
| 30 | **Floating "awaiting permission" row** with scroll-to arrows when the pending tool call is off-screen; capped height (`max_h_40`) | `thread_view.rs:4709-4806`, `:10265-10272` | The blocking action is always reachable without hunting |

---

## 4. Animation and in-progress affordances (concrete numbers)

### 4.1 Streaming text reveal (the main "smoothness" mechanism)

VERIFIED `acp_thread.rs:3857-3862`, `:5936-5947`, `:5987-5989`.

- Tick interval: `TASK_UPDATE_MS = 16` (timer on the background executor, applied on the main thread).
- Target: `REVEAL_TARGET = 200.0` ms to reveal all pending text.
- Budget: `bytes_per_tick = ceil(pending_bytes / 200 * 16)` = 8% of the backlog per tick,
  recomputed **when a chunk arrives** (from the total pending), not every tick. Between chunks the
  rate is constant, so a burst drains linearly in about 13 ticks (~208 ms).
- Reveal is by bytes snapped up to a UTF-8 char boundary (`ceil_char_boundary`), not by word or
  grapheme. VERIFIED `acp_thread.rs:3817`.
- No easing, no fade-in of new characters, no cursor glyph.
- The first chunk of a new message/thought bypasses the buffer (it creates the markdown entity
  with full text); buffering applies to subsequent chunks. VERIFIED `acp_thread.rs:5776-5790`,
  `:5845`.
- Cost model: each reveal calls `Markdown::append`, which copies the whole source string and
  requests a parse (`markdown.rs:1019-1022`). Because parses coalesce (technique 3), the effective
  reparse rate is "as fast as the background parser finishes", not 60 Hz. The element tree for a
  markdown block is still rebuilt from parse events on every layout (`markdown.rs:2675-2760`);
  there is no per-block element cache — the caching is at the highlight level only.
- Provenance: a fork's PR text attributes the buffer to upstream PR #51499. UNVERIFIED
  (https://github.com/helixml/zed/pull/35).

### 4.2 Spinners and labels

| Affordance | Implementation | Numbers | Where |
|---|---|---|---|
| Generating indicator (tail of list) | `SpinnerLabel` variant `Dots`: text label cycling 10 braille frames | 1000 ms loop, linear, `frame = floor(delta*10) % 10`; label size Small, color Muted; row padding `py_2`, `px` 22 px | VERIFIED `crates/ui/src/components/label/spinner_label.rs:41-58`, `:166-181`; `thread_view.rs:8692-8721` |
| Awaiting confirmation | `SpinnerLabel` variant `Sand` (35 frames) + `LoadingLabel("Awaiting Confirmation")` | Sand: 2000 ms loop. LoadingLabel: 1 s typewriter reveal of the label text, then a 1 s repeating cycle of "", ".", "..", "..." at quarter boundaries; `min_w(8rem)` reserved so the dots do not shift layout | VERIFIED `spinner_label.rs:45-57`; `crates/ui/src/components/label/loading_label.rs:84-113`; `thread_view.rs:8698-8712` |
| Blocked on a terminal command | No spinner in the indicator row (the terminal card has its own) | - | VERIFIED `thread_view.rs:8713-8714` |
| Elapsed / token stats in indicator | Hidden unless `show_turn_stats` (default false); elapsed only after 30 s; tokens only after 250; arrow up while tools run, down while generating; a 1 s timer re-notifies the view | - | VERIFIED `thread_view.rs:8662-8690`, `:1726-1733`; `conversation_view.rs:107-108`; `default.json:1423` |
| "Editing N files…", connecting "Loading…" | Label alpha animated with `pulsating_between(0.3, 0.7)` | 2 s loop | VERIFIED `thread_view.rs:5310-5328`; `conversation_view.rs:3970-3989` |
| Sidebar: running thread | `LoadCircle` icon, `with_rotate_animation(2)` | 2 s per revolution, linear, `repeat_synced` (phase-locked across the app) | VERIFIED `crates/ui/src/components/ai/thread_item.rs:342-350`; `crates/ui/src/traits/animation_ext.rs:15-41` |
| Sidebar: title being generated | Title label alpha `pulsating_between(0.4, 0.8)` | 2 s loop | VERIFIED `thread_item.rs:361-372` |
| Plan entry in progress | Status icon rotates | 2 s per revolution | VERIFIED `thread_view.rs:5074-5077` |
| Terminal tool running | `LoadCircle` rotating + Stop button; elapsed shown after 10 s | 2 s per revolution | VERIFIED `terminal_tool_header.rs:7`, `:192-200` |
| Subagent running | `SpinnerLabel::new()` (Dots) on the card | 1 s loop | VERIFIED `thread_view.rs:12350` |
| Pending native edit with no content yet | Skeleton: 5 rounded bars (h 4 px; widths 3/4, 1/4, 2/4, 3/5, 2/5), each fading opacity | 2 s loop; per-bar phase offset `0.15 * n`; active window 0.7 of the loop; triangle wave through `ease_in_out`; opacity 0.1 -> 1.0 | VERIFIED `thread_view.rs:11539-11578` |
| Tool call rows | **No spinner per row.** Icon by kind; status shown only for terminal states (dashed border + X for failed/canceled, pause icon, Stop button if cancellable) | - | VERIFIED `thread_view.rs:10114-10213`, `:10246`, `:11643-11657` |

### 4.3 Easing helpers (gpui)

VERIFIED `crates/gpui/src/elements/animation.rs`.

- `pulsating_between(min, max)`: `t = sin(2*pi*delta)`, `breath = (t^3 + t)/2`,
  `alpha = min + ((breath+1)/2) * (max-min)` — a sine/cubic "breathing" curve (`:526-540`).
- `ease_in_out` (quadratic) `:500-507`; `ease_out_quint` = `1-(1-d)^5` `:510-512`; `bounce` `:515-523`.
- `Animation::repeat_synced()` phase-locks all instances to one app-wide epoch (`:50-54`, `:403-406`).
- `Animation::with_max_fps(fps)` schedules re-render every `1/fps` s instead of every frame
  (`:69-72`, `:437-450`).
- Reduce-motion: all `with_animation` users render a static state when `cx.reduce_motion()` is set
  (end state for one-shots, start state for loops) (`:391-398`).
- UI durations: `AnimationDuration::{Instant = 50, Fast = 150, Slow = 300}` ms; `animate_in` uses
  150 ms `ease_out_quint`, opacity 0.4 -> 1.0, 40 px travel. VERIFIED
  `crates/ui/src/styles/animation.rs:7-12`, `:53-63`. Not used in the transcript itself (no
  `animate_in` call in `conversation_view*`/`agent_panel.rs`).

### 4.4 What is deliberately absent

- No shimmer/gradient-sweep text anywhere in `agent_ui`, `ui`, `acp_thread` (grep for
  `shimmer` returns nothing). VERIFIED by search at the SHA.
- No enter/exit animation for transcript rows; rows appear at final size.
- No floating "jump to bottom" button. Bottom is reached by keyboard action, context menu
  ("Scroll to Bottom"), dragging the scrollbar to the end, or scrolling down — all of which
  re-engage follow. VERIFIED `thread_view.rs:8443-8450`, `:8966-8973`; `list.rs:1228-1238`.
  There is a per-turn "scroll to most recent user prompt" button in the turn controls
  (`thread_view.rs:8135`).

---

## 5. Jank they hit and how they fixed it

| Problem | Fix | Evidence |
|---|---|---|
| Scrollbar flicker / scroll snapping while streaming, caused by `list_state.splice` on updated entries | Added `ListState::remeasure_items` and use it for `EntryUpdated`; refactored follow-tail into the list | Code VERIFIED `list.rs:404-462`, `conversation_view.rs:2017`. History UNVERIFIED: https://github.com/zed-industries/zed/pull/53101 ("so as to not remeasure the entire list's content on every new streamed token"); earlier commit "Fix scrollbar flicker when streaming agent2 response (#36606)" https://git.secluded.site/zed/commit/d0fb6120d9583fd46b17aed9d2b9a5b08e302f7e |
| Layout shift between typing the first prompt and submitting; bottom-up growth | Top-down generation + `follow_tail` in the GPUI list | Code VERIFIED `conversation_view.rs:1595-1596`, `thread_view.rs:8612-8641`. Motivation UNVERIFIED: https://github.com/zed-industries/zed/pull/52440 ("minimize as much as possible layout shift between writing your first prompt and actually submitting it") |
| Follow-tail not re-engaging after scrollbar drag | Scrollbar drag keeps `Tail` mode; dragging to (frozen) end resumes following | Code VERIFIED `list.rs:1403-1416`. UNVERIFIED: https://github.com/zed-industries/zed/pull/54570 |
| Thumb runs away while dragging during streaming | `scrollbar_drag_start_height` freezes the height used for mapping | VERIFIED `list.rs:742-756`, `:1392-1397` |
| Thread history list jumping to top on every streamed chunk; metadata rewritten per chunk | Preserve history scroll position; update metadata only on structural events | Code VERIFIED `conversation_view.rs:769-799`. UNVERIFIED: https://github.com/zed-industries/zed/pull/54365 |
| Per-frame tree-sitter highlight of every visible code block made streaming janky; ~50% CPU while agent output scrolls | Highlight in the background parse; content-keyed LRU | Code VERIFIED `markdown.rs:1401`, `:1711-1800`, `highlight_cache.rs:14-17`. UNVERIFIED: https://github.com/zed-industries/zed/pull/63138, issue https://github.com/zed-industries/zed/issues/57349 |
| Opening threads materialized all markdown (≈295 MiB with two large threads in a debug profile) | `defer_parsing` + measurement readiness so restored scroll survives async parse | Code VERIFIED (techniques 8, 9). Figures UNVERIFIED: https://github.com/zed-industries/zed/pull/65470 (parsed events 213.0 -> 11.0 MiB) |
| Opening tool-heavy threads built a diff editor, buffer and syntax tree per historical edit (multi-GiB spikes) | Build diffs/terminals on first view | Code VERIFIED (technique 10). Figures UNVERIFIED: https://github.com/zed-industries/zed/pull/65444 |
| Huge auto-height editors inside the list laid out every line (~50% of frame time) | Editor lays out only lines intersecting the content mask | UNVERIFIED (not re-read in source): https://github.com/zed-industries/zed/pull/44995 |
| Permission row flicker: floating row grew until the list had zero height, visibility query oscillated per frame | `item_is_above/below_viewport` answer from last layout bounds even at zero height; floating layout capped with internal scroll | Code VERIFIED `thread_view.rs:10265-10272`, `list.rs:813-850`. UNVERIFIED: https://github.com/zed-industries/zed/pull/58689 |
| Expanding a compaction summary at the tail auto-scrolled to its end | `pause_following_tail()` before expanding | VERIFIED `thread_view.rs:5255-5282`. UNVERIFIED: https://github.com/zed-industries/zed/pull/62210 |
| Queue double-send after "Send Now" (cancel's Stopped event re-triggered auto-send); index-based queue state drifting | `AbsorbingCancel` state; stable `QueueEntryId`s in a dedicated module | VERIFIED `message_queue.rs:20-28`, `:168-186`. UNVERIFIED: https://github.com/zed-industries/zed/pull/59310 |
| Stale turn completion disabling follow-tail / clobbering a regenerate | Generation checks on `turn_id`, `activity_generation`, `current_submission` | VERIFIED `conversation_view.rs:2077-2081`, `:15306`; `acp_thread.rs:7640-7672` |
| Reveal target invalidated when an entry is removed and its index reused before the next tick | Target holds the markdown entity and re-validates identity each tick | VERIFIED `acp_thread.rs:3837-3846` (comment at `:3844`) |
| Double-borrow when reading scroll position inside the list's scroll handler | `cx.defer` out of the handler | VERIFIED `thread_view.rs:1351-1364` |
| Truncated/choppy text before tool calls in a downstream fork, attributed to reveal-buffer baseline after flush | Fork proposes draining the whole buffer each tick | UNVERIFIED, fork only: https://github.com/helixml/zed/pull/35. Upstream at this SHA flushes on every boundary (technique 2) |

Remaining rough edges visible in source (VERIFIED, my reading):

- `Markdown::append` is O(total length) per reveal tick (string copy) and each parse is a full
  reparse of the block (`markdown.rs:1019-1022`, `:1308`). Long single messages get progressively
  more expensive; they rely on coalescing and the highlight LRU rather than incremental parsing.
- The element tree of a visible markdown block is rebuilt on each layout (`markdown.rs:2675+`).
- Creating a thread for an agent whose connection/session is not warm shows a centered pulsating
  "Loading…" label, not an optimistic composer+transcript (`conversation_view.rs:3970-3989`,
  `:1548-1556`).
- Send is gated on async content resolution; the composer is cleared and the bubble appears only
  after that resolves (`thread_view.rs:2367-2381`). With slow mention resolution there is a
  visible gap with only `is_loading_contents` as feedback.
- Edit-and-resend waits for the agent's `truncate` before any UI change (`thread_view.rs:2858-2860`).

---

## 6. Ranked recommendations for a native Swift/AppKit ACP client

Ranked by expected impact on perceived quality per unit of work.

1. **Put a reveal buffer between the ACP stream and the text view.** Keep authoritative text in
   the model; reveal on a display-linked tick with `budget = ceil(pending * tick / 200ms)`,
   recomputed on chunk arrival; snap to grapheme boundaries (better than Zed's byte/char snap);
   flush synchronously before any structural mutation (new entry, tool call, cancel, turn end,
   rewind). (Zed: `acp_thread.rs:3849-3879`, `:5925-6011`.)
2. **Never parse or highlight on the main thread, and never blank between parses.** One parse in
   flight per block, a dirty flag for latest-wins, swap the attributed result in atomically, keep
   the previous render until then. With swift-markdown: parse + build `NSAttributedString` on a
   background actor; cache highlighted code blocks keyed by (language, content hash) in a
   byte-budgeted LRU so each streaming reparse only pays for the trailing block. Going one step
   further than Zed — splitting a message into stable closed blocks plus one open tail block —
   removes the O(n) reparse Zed still has.
3. **Make follow-tail an explicit state machine owned by the scroll container**, not a
   "distance from bottom" check at append time: `following` / `detached`; user scroll-up or any
   programmatic scroll-to-item detaches; arriving within 1 px of the bottom (by any means)
   re-attaches; while following, pin to the end during layout. Expose `pauseFollowing()` for
   user-initiated expansion near the tail. (Zed: `list.rs:97-146`, `:1053-1066`, `:1228-1238`,
   `:628-645`.)
4. **Distinguish "row content changed" from "row inserted/removed".** For content changes,
   invalidate only that row's height, keep its previous height as the estimate, and preserve the
   anchor row + in-row offset. In AppKit: `noteHeightOfRows(withIndexesChanged:)` inside a
   zero-duration animation context with explicit anchor restoration, rather than `reloadData` /
   insert-remove. (Zed: `list.rs:404-462`.)
5. **Lazy everything off-screen**: do not parse markdown, build diff views, or attach terminal
   views for rows that have never been laid out or expanded; treat heights of rows with pending
   async work as provisional and retry the scroll intent (restored offset / scroll-to-end) until
   they settle. This is what makes old threads open instantly. (Zed: `acp_thread.rs:2880-2899`,
   `entry_view_state.rs:44-54`, `list.rs:647-652`.)
6. **Keep sessions and views warm.** One process/connection per agent shared by all sessions,
   requested at app/panel start; a reusable empty draft so "new chat" is a focus change (Zed
   creates it on demand; a proactive spare session created right after the previous draft is
   consumed would go further);
   park whole thread views (model + view state + scroll) on switch-away, never evict running
   ones, LRU-cap idle reloadable ones (Zed default 5). (Zed: `agent_connection_store.rs:186-205`,
   `agent_panel.rs:2974-3070`, `:4228-4334`.) Improve on Zed by showing the composer immediately
   and queuing the first send while the session is still being created, instead of a loading
   label.
7. **Optimistic local state with identity-based reconciliation.** Push the user bubble with a
   client id before the prompt request; drop/merge the agent's echo by id + content; update text
   in place (prefix -> append, else replace) so reconciliation never recreates a view; guard every
   async completion with a turn/generation id. On cancel, mark pending tool calls canceled and go
   idle locally, then let the agent catch up. Do not overwrite a field the user is focused in.
   (Zed: `acp_thread.rs:5032-5063`, `:7513-7539`, `:8713-8721`, `:7853-7877`;
   `entry_view_state.rs:325-343`.)
8. **Anti-flash thresholds instead of instant status.** Show "Sending…"/"Waiting…" only after
   ~1 s unsettled; elapsed time only after tens of seconds; reserve width for animated labels so
   they do not shift neighbors. (Zed: `thread_view.rs:76`, `:1897-1934`; `conversation_view.rs:107-108`.)
9. **One in-progress affordance at the tail, as a real row**, plus quiet per-kind icons on tool
   rows; no per-row spinners. Host any continuously animating element in its own layer/view so it
   does not invalidate the transcript (in AppKit, a `CALayer` animation costs nothing on the main
   thread — cheaper than Zed's per-frame re-render). If shimmer is the house style, apply it to
   that single tail row and to skeleton placeholders only. Zed's numbers for reference: 1 s
   10-frame spinner, 2 s breathing alpha between 0.3-0.7 or 0.4-0.8, 2 s rotation, skeleton bars
   2 s with 0.15 stagger. Honor Reduce Motion globally.
10. **Top-down layout with a composer that does not move on first send**; collapsed-by-default
    tool output keyed by tool-call id; auto-expand/auto-collapse live thinking with a
    "user touched it" override. (Zed: `thread_view.rs:8612-8641`; `entry_view_state.rs:96-256`.)
11. **Queue as a small explicit state machine** (`autoProcess` / `paused` / `absorbingCancel`)
    with stable ids; user stop pauses it; any send or enqueue resumes it; do not auto-send while
    the user is editing the front entry; on failed dispatch, reinsert at original position and
    pause. Memex's durable queue should keep these same transitions. (Zed: `message_queue.rs`.)
12. **Freeze scrollbar geometry during a knob drag** and treat reaching the frozen end as
    re-attach. (Zed: `list.rs:742-756`, `:1389-1416`.)
13. **Decouple sidebar/thread-list updates from token streaming** (only structural events), and
    persist scroll offset and draft text with throttled/debounced background writes (Zed: 250 ms
    draft debounce). (Zed: `conversation_view.rs:769-799`, `:110`.)
14. **Bound early-update staging**: buffer `session/update` notifications that arrive before the
    `session/new`/`session/load` response and apply them in order, with a hard cap. (Zed:
    `session_updates.rs:10-11`, `:52-78`.)

---

## Method notes

- Source read directly from the clone at the SHA above; ~25 targeted file regions across
  `acp_thread`, `agent_ui`, `agent_servers`, `agent`, `markdown`, `gpui`, `ui`, `language_core`.
- PR/issue context via `gh pr view` / `gh search prs` against `zed-industries/zed` and two web
  searches; GitHub search rate limit was hit before issue searches completed, so section 5's
  history is PR-description-based and marked UNVERIFIED where not confirmed in code.
- Not examined in depth: `crates/agent_servers/src/acp/v2.rs` receipt-transport details, the
  `sidebar` crate, `message_editor.rs` internals, `acp_thread/src/diff.rs` streaming
  diff reveal, and native-agent streaming edit sessions (`crates/agent/src/tools/edit_session*`).
