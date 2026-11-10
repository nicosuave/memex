# Agent runtime internals vs. the Memex snapshot-polling integration

Read-only code reading of the runtime pinned by Memex PR 237 (`sidequery/mono` @ `e610a6f585d9c8c1fbca025a5a8aa47c11e650f2`, per `pr237/apps/macos/runtime-source.json`). Nothing was built, run, or measured: every cost statement below is derived from code structure, not from a profile.

## Scope note: two extra crates were needed

The Rust side of every `conversation.*`, `thread.*`, `provider_operation.*` and `command.dispatch` request is not in `sq-acp` or `sq-ui`. `sq-acp-ffi-runtime` depends on `sidequery-agent-runtime = { path = "../../../agent-runtime" }` (`packages/sq-acp/rust/sq-acp-ffi-runtime/Cargo.toml`), which in turn depends on `agent-history`. A local checkout at `/Volumes/Dev/Code/sidequery-mono` contains the pinned commit, so I extracted `packages/agent-runtime` and `packages/agent-history` from that exact revision with `git archive` into `runtime/packages/` (the checkout itself was not touched). I confirmed the already-extracted `AgentConversationService.swift` and `ChatViewModel.swift` are byte-identical to the pinned revision. Without those two crates, questions 1 and 5 could not be answered from source.

## Citation key

All paths are relative to `runtime/`. Aliases used below:

| Alias | Path |
|---|---|
| SVC | `packages/sq-acp/Sources/SQACPHost/AgentConversationService.swift` |
| CREATE | `packages/sq-acp/Sources/SQACPHost/AgentConversationCreation.swift` |
| CLAUDE | `packages/sq-acp/Sources/SQACPHost/ClaudeAgentSDKTransport.swift` |
| CODEX | `packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift` |
| CODEXVER | `packages/sq-acp/Sources/SQACPHost/CodexRuntimeCompatibility.swift` |
| XPORT | `packages/sq-acp/Sources/SQACPHost/AgentSessionTransport.swift` |
| NATIVE | `packages/sq-acp/Sources/SQACPHost/AgentNativeEvent.swift` |
| PROJ | `packages/sq-acp/Sources/SQACPHost/AgentThreadProjection.swift` |
| ARCHIVE | `packages/sq-acp/Sources/SQACPHost/AgentACPEventArchive.swift` |
| CVM | `packages/sq-acp/Sources/SQACPHost/ChatViewModel.swift` |
| RTCLIENT | `packages/sq-acp/Sources/SQACP/AgentRuntimeClient.swift` |
| ACPSWIFT | `packages/sq-acp/Sources/SQACP/AcpClient.swift` |
| HOST | `packages/sq-acp/Tools/ClaudeAgentSDKHost/src/index.ts` |
| STREAM | `packages/sq-acp/Tools/ClaudeAgentSDKHost/src/stream-content.ts` |
| ACPRS | `packages/sq-acp/rust/sidequery-acp/src/client.rs` |
| FFI | `packages/sq-acp/rust/sq-acp-ffi-runtime/src/lib.rs` |
| PROTO | `packages/agent-runtime/src/protocol.rs` |
| CONV | `packages/agent-runtime/src/conversations.rs` |
| STORE | `packages/agent-runtime/src/store.rs` |
| LIVE | `packages/agent-history/src/live.rs` |
| HSTORE | `packages/agent-history/src/store.rs` |
| STREAMVIEW | `packages/sq-ui/Sources/SQACPUI/AcpMessageStreamView.swift` |
| MDVIEW | `packages/sq-ui/Sources/SQMarkdownUI/AcpMarkdownMessageView.swift` |
| MDSEG | `packages/sq-ui/Sources/SQMarkdownUI/AcpMarkdownSegments.swift` |
| MDPARA | `packages/sq-ui/Sources/SQMarkdownUI/AcpParagraphTextView.swift` |
| INDICATORS | `packages/sq-ui/Sources/SQACPUI/AcpActivityIndicators.swift` |

Memex-side references are written as `pr237/...` (relative to the scratchpad).

## Verdict on the working hypothesis

Partly confirmed, partly refuted, and the bigger problem is somewhere the hypothesis did not point.

- Confirmed: Memex adds a fixed 80 ms delay to every update including the first token, and each update re-reads and re-projects the whole conversation (`pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:189-204, 234-270`).
- Refuted: Memex is not what throws streaming granularity away. Per-token deltas reach `AgentConversationService`, and the service itself turns each one into a full-item snapshot plus a full-conversation JSON view before any subscriber sees it (SVC:1813-1861, CONV:378-385, LIVE:383-455). The service's public surface has no delta channel at all; the transports that carry deltas are `internal` (XPORT:5, XPORT:111, NATIVE:216, CLAUDE:31, CODEX:37).
- The larger cost is inside the runtime: at least two full-conversation clone + serialize + FFI + parse + re-serialize cycles per token on one serial queue, independent of Memex. Memex's `publish()` then repeats that work a third time and shares the same serial queue with event ingestion.

So "consume deltas instead of publish()" is not available to Memex today without a runtime change. What Memex can do alone is stop duplicating work and stop delaying the first frame (see the last section).

---

## 1. Event flow and granularity

### 1.1 What arrives from each provider

**Claude (Bun helper + Claude Agent SDK 0.3.270)**

- `query()` is created with `includePartialMessages: true`, `forwardSubagentText: true`, `settingSources: ["user","project","local"]`, `extraArgs: { "replay-user-messages": null }`, `persistSession: true`, streaming input (`prompt: queue`, an `AsyncMessageQueue`) (HOST:960-997).
- Text and thinking are forwarded one event per SDK delta: `content_block_delta` `text_delta` becomes `assistantDelta`, `thinking_delta` becomes `reasoningDelta`, keyed `"<api message id>:<kind>:<block index>"` (STREAM:61-79). Each is one JSON line on stdout (HOST:292-306, 549-556).
- Tool-call input is not streamed. `input_json_delta` is ignored (STREAM:73-75), and once a `tool_use` block starts, all later text/thinking in the same API message is deferred until the consolidated assistant message (STREAM:51-57). Tool cards are emitted only from the consolidated message (HOST:611-631), and tool results from the following user message (HOST:638-666). Between a tool block starting and its input finishing, the helper emits nothing.
- Subagent (child) text is not streamed into the root chat (HOST:541-547).
- The Swift transport parses each line on its own IO queue and re-emits one `AgentNativeEvent` per line on the callback queue (CLAUDE:934-944, 1084-1103, 1219-1232). No coalescing.

**Codex (`codex app-server --stdio`)**

- `item/agentMessage/delta` becomes `.assistantDelta` per notification; `item/reasoning/summaryTextDelta` and `item/reasoning/textDelta` become `.reasoningDelta` (CODEX:1944-1959). `item/completed` re-sends the full text as a snapshot (CODEX:1980-1996).
- Tools: `item/started` / `item/completed` map to `toolStarted` / `toolUpdated` (CODEX:1997-2006). Command and file-change output deltas arrive as `toolUpdated` with the delta in `details` and `rawOutputJSON: nil` (CODEX:2007-2019).
- That streamed command output is dropped by the service: `apply(.toolCallUpdated)` only publishes a result when `rawOutput` is non-nil and only uses `content` for plans (SVC:1627-1668). Each output delta still costs a full tool-invocation snapshot publish with no visible change (SVC:1637-1640).

**Generic ACP (Rust `sidequery-acp`)**

- `AgentMessageChunk`, `AgentThoughtChunk`, `UserMessageChunk`, `ToolCall`, `ToolCallUpdate` are each forwarded as one JSON string (ACPRS:2058-2125). They go through `mpsc::channel(128)` with `try_send`, so a chunk is silently dropped if the channel is full (ACPRS:254, 2081, 2101, 2118); the drain side only does an async dispatch so this should be rare, but it is unguarded.
- The C callback hops each chunk onto the Swift callback queue individually (ACPSWIFT:130-140), then `AcpAgentSessionTransport` wraps it as `.acpChunk` (XPORT:317-319).
- ACP chunks carry no message ids, so the service invents connection-scoped ephemeral ids per segment (SVC:1817-1820).

### 1.2 How it is stored and re-exposed

All three transports are constructed with `callbackQueue: self.queue`, the service's single serial queue (SVC:55, 504-512, 519-526, 547-550), and every event is processed there (SVC:652-662).

For each text delta, `receive` → `apply` → `message(append: true)`:

1. The delta is concatenated into an in-memory per-item string, `c.texts[itemID]` (SVC:1821-1822).
2. A snapshot of the **whole item text so far** is built (SVC:1823-1827) and sent as `{"type":"snapshot", ...}` to `conversation.apply` (SVC:1829-1845, 1850-1855). The overlay protocol has a real `TextDelta` update (LIVE:100-105, 347-378) but the service never uses it.
3. In Rust, `conversation.apply` applies the update and then returns `read_result`, the complete conversation view (CONV:378-385, 588-590).
4. `view()` projects every live item, sorts them, serializes every evidence record of every persisted entity to a JSON string in two separate passes, clones the filtered set into `presentation`, and clones the entire `persisted` vector again (LIVE:383-455, specifically 427-429, 436-438, 444, 449). So the response carries the full history roughly twice (`persisted` and `presentation`) plus `ephemeral`.
5. That value is converted with `json!` / `to_value`, then `to_string`, copied into a `CString` (PROTO:50-67, FFI:939-940), copied into a Swift `String` (RTCLIENT:82), and fully parsed by `JSONSerialization` (SVC:115-133).
6. `publish` runs `reconcile` (for Claude this loops over every persisted entity, SVC:1876-1908) and serializes the whole thing again with `.sortedKeys`, then dispatches the string to each subscriber (SVC:1856-1861).
7. After the projected events, `receive` always calls `state(c)`, which is another `conversation.apply` and another full publish even when `running` and `pending_interactions` are unchanged (SVC:1519, 1846-1849).

Net: **at least two** full-conversation round trips per token, each O(history size), with the streaming item's full text re-sent each time (so also quadratic in the length of the message being streamed). Tool updates cost three to five (invocation snapshot, activity metadata, MCP metadata, result snapshot, state: SVC:1609-1668).

Nothing is persisted per delta for Claude/Codex; the overlay is in-memory (LIVE:157-174), which matches the stated design ("No event-sourcing of every token delta", `packages/agent-runtime/ARCHITECTURE.md:129-131, 431`). The exception is source-less ACP sessions, which fsync every chunk (see section 5).

### 1.3 What triggers a `service.subscribe` callback

Every call to `publish` or `publishError`. There is no coalescing, debounce, or timer in the service other than the optional watch timer and the mutation timeout (SVC:252-270, 822). Sources:

- Subscribe itself: an initial `conversation.list` payload (SVC:375-388).
- Every overlay write: each item snapshot and each `state()` (SVC:1829-1855). This is the per-delta path.
- Claude `persistedMessageItems` (a full `conversation.read`, SVC:1305-1314).
- Turn end: `conversation.refresh` (SVC:1591).
- `addSource`, `refresh`, `reload`, `bindItems`, `connect`, `disconnect`, `close`, `relocateWorkspace`, watch polls (SVC:264-271, 306, 334, 354, 361, 371, 676, 697, 710, 761).
- Errors via `publishError` (SVC:1939-1941).

The callback payload is already the reconciled full-conversation JSON envelope; the runtime README describes rendering directly from it (`packages/sq-acp/README.md:26-29`). Memex discards the payload and re-reads (`pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:156-161, 236`).

### 1.4 `AgentConversationService.read(sessionID:)`

`queue.sync { json(reconcile(request("conversation.read"))) }` (SVC:310-312).

- It is an in-memory read: no SQLite and no transcript re-parse. `conversation.read` returns `read_result` from the overlay map (CONV:246-249, 581-590).
- It is still O(total history) every call: the `view()` clone-and-serialize work in 1.2 step 4, Rust serialization, Swift parse, Claude reconcile loop, Swift re-serialization. Memex then decodes that string a further time into `RawTranscriptJSON` (`pr237/apps/macos/Sources/Memex/ConversationProjection.swift:10-14`).
- It runs on the service's serial queue via `queue.sync`, so it waits behind every queued delta and blocks delta processing while it runs.

### 1.5 `thread.snapshot` and `provider_operation.list`

Path: `AgentRuntimeClient.requestJSON` takes a per-client lock (RTCLIENT:62-84), calls the C function, which takes a **process-global** mutex over all runtime handles and holds it for the whole request (FFI:931-942), then `RuntimeProtocol::handle_json` (PROTO:50-68).

- `thread.snapshot` → `RuntimeStore::snapshot` → `load_snapshot` (PROTO:141-146, STORE:426-428, 2180-2190): SQLite reads of the thread row, provider settings, lineage, all turns, all messages with context refs, activities and pending requests (STORE:2188-2270, 2351-2360, 2401-2408). In the Memex integration the store only holds runtime-dispatched commands and lifecycle events (the service ingests `turn.*`, `approval.*`, `user_input.*`, `provider.runtime.bound`; SVC:639-645, 1073-1076, 1582, 1596-1608, 1695, 1711), so cost grows with the number of prompts sent through Memex, not with transcript size.
- `provider_operation.list` → one id query then one `load_provider_operation` per row (PROTO:203-209, STORE:848-887). With `includeTerminal: true` that is every operation ever issued for the thread.
- The database is opened in WAL mode with a 5 s busy timeout and no `synchronous` pragma (STORE:262-267), so SQLite's compiled default applies to every commit. Reads do not fsync.

`commandStatuses` runs both (`includeTerminal: true` plus a snapshot) on the service queue (SVC:423-450), and `settings` runs `commandStatusesOnQueue` again (SVC:405-411). One Memex `publish()` therefore performs 1 `conversation.read`, 3 `thread.snapshot`, 3 `provider_operation.list`, and about 8 `queue.sync` hops (`pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:234-260`).

### 1.6 Is there a delta or event-stream API a client could consume?

Not for text. What exists:

| Candidate | What it is | Usable by Memex for token streaming? |
|---|---|---|
| `subscribe` payload | Full conversation envelope per change (SVC:1856-1861) | Saves the re-read, but it is still a full snapshot |
| `event.read` (`afterSequence`, `limit`) and `thread.sync` (cursor, `resetRequired`) | A real cursor API over the SQLite event log (PROTO:153-159, 210-217; STORE:460-480, 889-962) | No: only lifecycle/command events are in the log; deltas are never ingested |
| `LiveUpdate::TextDelta` | Sequenced text deltas in the overlay protocol (LIVE:100-105) | No: unused by the service, and `conversation.apply` returns the full view regardless (CONV:378-385) |
| `AgentSessionTransport` event handler with `AgentNativeEvent.assistantDelta` etc. | The actual delta stream; what `ChatViewModel` consumes (CVM:3411-3415, 6351-6358) | No: protocol, transports and event enums are `internal` to `SQACPHost` (XPORT:5, 111; NATIVE:216; CLAUDE:31; CODEX:37), and `AgentConversationCreation.transport` is internal (CREATE:12) |
| `AgentThreadEvent` | Public enum with `assistantMessageDelta`, `reasoningDelta`, `toolCallStarted/Updated` (PROJ:634) | Type is public, but nothing public emits it for a service connection |
| `ChatViewModel` | Public `ObservableObject` that applies deltas in place (CVM:190, 352) | Yes, but it is a different integration: it owns one transport per provider and bypasses the service's durable command ledger and overlay |
| `AcpClient` chunk callback | Public raw chunk stream for ACP only (ACPSWIFT:164-171) | Only by bypassing the service for ACP providers |

No `AsyncStream` exists anywhere in the service.

---

## 2. The runtime's own UI

### 2.1 How `ChatViewModel` consumes streaming

- It owns transports directly, one per provider id, reused across that provider's sessions (`agentClients[provider]`, CVM:3329-3337, 3411-3416). It does not use `AgentConversationService`.
- Delta application is in place: `.assistantDelta` → `appendStreamChunk` → `existing.content + chunk` on the matching message id, or append a new message (CVM:6533-6540, 9264-9296). No re-read of history.
- No throttling or coalescing: every chunk goes through `withProviderState` → `publishVisibleState`, which compares the full `messages` array and reassigns the `@Published` property (CVM:8908-8930). That is O(message count) per delta on the callback queue (default main). There are no timers in the streaming path; the only `asyncAfter` uses are history refresh and the load watchdog (CVM:2778-2788, 3184, 3226).
- Optimistic user message: appended and `isAwaiting` set before any provider hop, with an explicit comment that spawn + init + new session takes seconds (CVM:2149-2163). `projectLocalUserEcho` mirrors it into the thread projection (CVM:2194-2197, 2222-2226).
- Send before ready: the prompt is parked and auto-sent when the session opens (`pendingPromptAfterSessionOpen`, CVM:2165-2170, 5954-6002), when capabilities arrive (`pendingPromptAwaitingCapabilities`, CVM:9336-9341, 9300-9311), or when the agent process finishes starting (`startAgentIfNeeded(then:)`, CVM:3328-3336). The user never waits on readiness to press Enter.
- Queue: while a turn is in flight or history is loading, prompts go to a local queue capped at 25 and are dispatched automatically at turn end (CVM:857, 2132-2146, 9162-9204).
- Steer: native steering when the transport supports it, with the user message shown immediately (CVM:2087-2099, 6894-6928). Otherwise cancel-then-send via `pendingSteerPrompts` (CVM:2110-2129, 3697-3706, 9098-9160). A rejected steer falls back into the queue (CVM:6930-6973).
- Cancel: sends the cancel and clears local awaiting/streaming/turn state immediately, marking running tools cancelled, without waiting for acknowledgement (CVM:3643-3695).
- First-token indicator: `isAwaitingAssistant` is set at send and cleared on the first stream chunk (CVM:2205, 2221, 9217-9219).
- Session switch: optimistic swap to the target session with a cached history preview, rollback on failure, 45 s watchdog (CVM:3102, 3160, 3184, 3246-3300, 6385-6393).
- Agent start runs on a global queue, not main (CVM:3403).

### 2.2 `AcpMessageStreamView`

- `ScrollView` + `LazyVStack` + `ScrollViewReader`, rows identified by message id (STREAMVIEW:184-201). Not a `List`.
- Follow-tail logic is a small state machine fed by the content frame through a `PreferenceKey`: it distinguishes "content grew" from "reader scrolled", treats within 48 pt of the bottom as near-tail, and only pauses following when the frame moved without growing or moved toward the top (STREAMVIEW:12-37, 230-247).
- Tail scrolls are coalesced through `.task(id: scrollRequest)` with a `Task.yield()` and performed with animations disabled (STREAMVIEW:261-270). Triggers: last message id, reasoning tick, spinner visibility, content growth (STREAMVIEW:242-260).
- `scrollPosition(id:anchor: .top)` keeps the reader's place when older pages are prepended, and prepending pauses tail-following (STREAMVIEW:239-240, 250-257).
- One trailing indicator row chooses between loading-history, "thinking" (awaiting first token, no tools running) and "running" (STREAMVIEW:203-217).

### 2.3 `AcpMarkdownMessageView`

- Re-parses the entire message on every body evaluation: `AcpMarkdownSegments.parse(content)` then per-line `AttributedString(markdown:)` (MDVIEW:14-30, 60-61). No incremental parse and no cache keyed on a stable prefix.
- The segment parser is streaming-tolerant in one respect: an unterminated code fence is rendered as a code block to end of text, so code appears as code while it streams (MDSEG:24-33). Tables are extracted from runs of `|` lines (MDSEG:39-56).
- On macOS 26, text segments over 1024 bytes containing a newline use a TextKit-backed `NSViewRepresentable` to avoid repeated SwiftUI shaping (MDVIEW:93-104, MDPARA:5-8, 32-42).
- No streaming cursor, token fade-in, or typewriter smoothing.

### 2.4 Feel techniques: who has what

Runtime UI has, Memex integration lacks:

1. Delta applied in place with no history re-read and no fixed delay (CVM:9264-9296) vs. 80 ms + full re-read + full re-projection.
2. Send-before-ready with automatic dispatch (CVM:2165-2170, 3328-3336, 9336-9341). Memex's first send awaits the whole connect and then requires `canSend` (`pr237/apps/macos/Sources/Memex/LiveConversation.swift:613-616`).
3. Provider process reuse across conversations of the same provider (CVM:3329-3337). Memex spawns per conversation.
4. Immediate local cancel (CVM:3666-3693). Memex waits up to 10 s polling for the acknowledgement (`pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:331-348`).
5. Instant cached history preview on session switch (CVM:3102, 6385-6393).
6. An explicit "awaiting first token" state distinct from "running" (STREAMVIEW:208-213).
7. Growth-aware follow-tail with coalesced, non-animated scrolls (STREAMVIEW:12-37, 261-270). I did not audit Memex's `TranscriptController` scroll logic in depth, so treat this as "worth comparing", not as a confirmed gap.

Memex has, runtime UI lacks:

1. Durable send intent: draft, pending prompt and queue persisted before crossing the provider boundary, with `notSent` / `uncertain` phases and no automatic resend (`pr237/apps/macos/Sources/Memex/LiveConversation.swift:199-224, 1031-1056`).
2. Durable command receipts reconciled after restart (`pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:25-60`; `LiveConversation.swift:660-674`). `ChatViewModel` calls `client.sendAcknowledged` directly with no ledger (CVM:9366-9380).
3. Coalescing of update notifications (80 ms) and all runtime calls off the main actor. `ChatViewModel` publishes every delta on the callback queue.
4. An AppKit `NSTableView` transcript with incremental row insertion and targeted height invalidation (`pr237/apps/macos/Sources/Memex/TranscriptController.swift:626-628`) and a 120-record render window (`LiveConversation.swift:134, 341-354`).
5. Queue hold, edit, reorder, promote and transfer; ownership ("open elsewhere") probing; per-turn checkpoint hooks (`LiveConversation.swift:254-294, 517-525, 777-878`).

Shared: Memex already uses `AcpComposerView`, `AcpSendButton`/`AcpStopButton`, the selector menus, `AcpStructuredUserInputView` and `AcpShimmerText` (`pr237/apps/macos/Sources/Memex/ConversationComposer.swift:58-103`, `ConversationPendingView.swift:55`). It does not use `ChatViewModel`, `AcpMessageStreamView` or `AcpMarkdownMessageView`.

---

## 3. Startup latency

### 3.1 Processes

- **Claude**: two processes per live conversation. The transport spawns the helper (CLAUDE:153-206), a Bun single-file executable (`bun build --compile`, `packages/sq-acp/Tools/ClaudeAgentSDKHost/package.json`). The helper's `query()` then makes the SDK spawn the `claude` CLI at `pathToClaudeCodeExecutable` (HOST:970, 997). The helper holds one `active` session (plus `retainedSessions`), so it cannot multiplex conversations (HOST:916-927, 1007). Changing effort, plan mode, or to/from bypass permissions spawns a replacement CLI (HOST:1110-1129, 1405-1466).
- **Codex**: `codex --version` is run synchronously first, waiting up to 2 s plus 0.2 s (CODEX:192-194; CODEXVER:62-92), then `codex app-server --stdio` is spawned (CODEX:204-271; SVC:499). One app-server per conversation; the transport tracks a single `currentThreadId` and drops notifications for other threads (CODEX:1885-1898).
- **ACP**: one agent process per `AcpClient`, with its own OS thread (ACPRS:307, 367), started through a freshly created Tokio runtime that blocks the caller (FFI:265-267).

Nothing is pooled or shared across conversations in `AgentConversationService`: each `connect*` call builds a new transport (SVC:504-512, 519-526, 547-550). Memex additionally opens a separate SQLite database and archive per conversation (`pr237/apps/macos/Sources/Memex/InAppConversationTarget.swift:95`).

### 3.2 New conversation: `AgentConversationCreation`

`create` is fully synchronous on the calling thread: `transport.start()`, `transport.newSession(cwd:)`, then an `NSCondition` wait of up to 45 s for `sessionOpened` (CREATE:72-94, 153-164). Events seen meanwhile are buffered and replayed at adoption (CREATE:116-123, 166-174).

- Claude:
  - `start()` spawns the helper and blocks on a semaphore for the `initialize` response, up to 12 s (CLAUDE:212-253). `initialize` itself is cheap; it starts MCP app discovery in the background with a 4 s per-server timeout and responds immediately (HOST:139, 1156-1166).
  - `session/new` → `startSession` constructs the query (CLI spawn begins) and, **if a permission mode was supplied, awaits `runtime.setPermissionMode`** before emitting `sessionOpened` (HOST:997-1029). Memex always supplies one (default `"auto"`: `pr237/apps/macos/Sources/Memex/ConversationPromptLibrary.swift:135-140`), so a new Claude chat blocks until the CLI has booted far enough to answer a control request. That includes whatever the CLI does at startup with user/project/local settings, plugins and MCP servers (HOST:976, 994-995); the CLI's own startup cost is not visible in this source.
- Codex: version probe, spawn, `initialize` (12 s cap), `initialized`, then `model/list`, account read and `collaborationMode/list` fire without being awaited (CODEX:278-339). `thread/start` response → `sessionOpened` (CODEX:467-498).
- ACP: `start()` blocks until the process is spawned and ACP `initialize` returns (ACPRS:230-264, 463-478, 597). I found no timeout on that wait. Then `session/new`.

`connectCreated` after that is cheap: register, connect, replay buffered events, and the buffered `sessionOpened` flips `ready` (SVC:555-576, 664-668, 1295-1303).

### 3.3 Resume: `connectClaude` / `connectCodex` / `connectACP`

Before any process starts, Memex does (`pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:131-161`):

1. `AgentRuntimeClient(databaseURL:)`: open SQLite, WAL, migrate (STORE:52-56, 262-267).
2. `AgentConversationService.init` → `conversation.open`: open the archive (SQLite with `synchronous = FULL`, directory fsyncs; HSTORE:57-82), load every session's entities, re-validate captures (CONV:141-173, 178-229).
3. `addSource`: read the entire transcript file, SHA-256 it, capture the new byte window, re-normalize **all** observations for that source if anything changed, publish, reload entities (CONV:256-280, 628-719, 832-895). Returns the full conversation JSON, which Memex parses only to find a source id.
4. `subscribe`: serializes `conversation.list` as the initial payload (SVC:375-388).

Then `connect` (SVC:578-686), entirely inside `queue.sync`:

- `conversation.read` (full view, only to read the key), `conversation.connect` (full view again), `thread.list` (a snapshot per thread), possibly `thread.create`, and a `provider.runtime.bound` ingest (SVC:591-645). Each write is its own immediate transaction with projection verification (see section 5).
- `transport.start()` **inside the queue.sync** (SVC:670): helper/app-server spawn plus the blocking `initialize` wait (12 s cap), plus the Codex version probe.
- `load(c)` → `transport.loadSession` (SVC:672-675, 936-940), which is asynchronous.

What is loaded or replayed:

- Claude: the helper reads the whole saved session with `getSessionMessages` and emits the first `historyPage` **before** creating the query (HOST:885-902). It then spawns the CLI with `resume` and awaits `setPermissionMode` before `sessionLoaded` (HOST:962-1029). `session/resume` additionally awaits `discoverSavedChildren` before responding (HOST:1171-1178).
- Codex: `thread/resume` with `excludeTurns: true` and `initialTurnsPage` of 20 newest turns (CODEX:165, 522-538, 870-872), with fallbacks to `thread/turns/list` and `thread/read` (CODEX:751-799). Live notifications are buffered until the initial page is published (CODEX:1893-1896).
- ACP: `session/load` after capabilities (SVC:1401-1411); replay chunks are suppressed for transcript-backed sessions and retained as observed items for source-less ones (SVC:1477-1514).

### 3.4 What gates "ready"

`supportedActions` returns `[]` until `connection.ready` and no mutation is in progress (SVC:390-397). So `actions.contains(.prompt)` needs both:

- `c.actions` populated: from the `.capabilities` event, emitted by `start()` right after `initialize` (CLAUDE:254; CODEX:200, 319; SVC:1371-1379), or for ACP from the capabilities chunk (SVC:1401-1407).
- `c.ready = true`:
  - Claude/Codex resume: on the initial `historyPage`, after `seedHistory` (SVC:1316-1324).
  - New conversation: on `sessionOpened` while `newlyCreated` (SVC:1295-1303).
  - ACP: on the projected `sessionOpened` (SVC:1534-1541).

Consequence for Claude resume: ready is reached when the helper has read history, **before** the `claude` CLI has finished starting. The first prompt after ready is then acknowledged only once the CLI is up (section 4), so CLI cold start shows up as "sent but not accepted" time rather than as "loading" time.

### 3.5 Timeouts

| What | Limit | Where |
|---|---|---|
| Helper / app-server `initialize` | 12 s | CLAUDE:246, CODEX:309 |
| `codex --version` | 2 s + 0.2 s | CODEXVER:84, 89 |
| Creation wait for `sessionOpened` | 45 s | CREATE:73, 84 |
| MCP app discovery (non-blocking) | 4 s per server | HOST:139, 209 |
| Memex ready watchdog | 45 s | `pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:227-230` |
| History mutation | 45 s | SVC:770, 822 |
| Provider operation lease | 30 s default | PROTO:167 |
| ACP user-input submit | 5 s, blocking | ACPRS:1565 |

### 3.6 Can anything be pre-warmed?

There is no pre-warm API, but two existing seams work without runtime changes:

- An un-adopted `AgentConversationCreation` holds a fully started provider with an open session and no prompt sent; its `deinit` stops the transport if it is never adopted (CREATE:4-5, 29). The cwd is fixed at creation (CREATE:83). I could not verify from this source whether Codex writes its rollout file at `thread/start` (Memex requires a transcript path from it: `pr237/apps/macos/Sources/Memex/NewConversationRuntime.swift:136-140`), so discarded pre-warmed Codex threads may leave files behind.
- Connecting an existing conversation earlier than first send. Memex currently defers connect to the first explicit send by design (`pr237/apps/macos/Sources/Memex/LiveConversation.swift:613-615`).

---

## 4. Prompt dispatch and acknowledgement

### 4.1 The durable model

`perform` (SVC:844-918) inside `queue.sync`:

1. Guards: connection ready, no mutation, action supported, `verifyCurrent` (a `thread.snapshot`) (SVC:850-857, 952-960).
2. For prompt/steer: `commandStatusesOnQueue` (a `provider_operation.list` with terminal rows plus a `thread.snapshot`) to refuse while a model/config change is pending (SVC:862-869), then attachment validation.
3. `command.dispatch` with `type` `thread.turn.start` (runtime turn id `turn-<commandID>`) or `thread.turn.steer` (current turn id) (SVC:875-880, 911-912). In Rust this is one immediate transaction: idempotency lookup by `commandId`, decide, append event(s), project and verify, enqueue an `agent_provider_operations` row as `pending`, store the receipt with the whole outcome JSON (STORE:270-359, 2568-2592).
   - A repeat of the same `commandId` with identical JSON returns the stored outcome with `replayed: true`; different JSON is an idempotency conflict (STORE:276-298). Memex uses `replayed == false` to mark prompts it originated (`pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:349-352`).
4. `requireAccepted`: the receipt status must be `accepted` (SVC:913, 972-976). This is local acceptance by the runtime's state machine, not the provider.
5. `deliver` (SVC:988-1194): `provider_operation.claim` (status `leased`, STORE:585), `provider_operation.start_delivery` (status `dispatching`, STORE:629), then the transport call. For a prompt it sets `c.turnID` and **`c.running = true` immediately** (SVC:1018-1020) and calls `sendAcknowledged` / `steerAcknowledged`, which only writes a JSON line to the child's stdin (SVC:1109-1115; CLAUDE:920-931).
6. `state(c)` publishes `running: true` (SVC:915).

Operation status lifecycle: `pending` → `leased` → `dispatching` → `completed` or `failed` (STORE:585, 629, 701, 788-817, 2592). `commandStatuses` joins operations to turns and to in-memory checkpoints to expose `status`, `error`, `runtimeTurnID`, `nativeTurnID`, `nativeMessageID` (SVC:427-450).

### 4.2 How long `perform` blocks

It does not wait for the provider. It blocks for: the wait to get onto the serial queue (behind every queued delta), roughly three immediate SQLite write transactions (dispatch, claim, start_delivery), about four snapshot/operation reads, one stdin write and one overlay publish. Memex then runs a full `publish()` before returning (`pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:353`).

### 4.3 When a delivery becomes completed

- **Claude**: the helper holds the `turn/send` request open (HOST:1351-1357) and answers it when the CLI echoes the user message uuid (`replay-user-messages`), or at the `result` carrying `user_message_uuid` (HOST:641, 675-680, 714-715). The response carries `turnId` and `userMessageUUID`. The transport turns that into a checkpoint and then the completion (CLAUDE:552-566). On the service queue: checkpoint stored and `turn.attached` ingested (SVC:1062-1080); completion ingests `turn.started` and calls `provider_operation.complete` (SVC:1021-1030).
- **Codex**: completion is the `turn/start` RPC response containing `turn.id`; the checkpoint carries the native turn id and no message id (CODEX:1025-1061).
- **ACP**: there is no acknowledgement. The operation is parked in `pendingPromptDelivery` and completed only at `promptDone` / turn finish (SVC:1095-1099, 1566-1570). It stays `dispatching` for the whole turn.

Failures: a definite rejection calls `provider_operation.fail` and resets the turn; an ambiguous failure (transport died, `ambiguous_delivery:`) deliberately leaves the operation `dispatching` so nothing auto-replays (SVC:1031-1052; CLAUDE:586-597).

Memex's `isAccepted` is `status == "completed"` plus a native turn id or native message id (`pr237/apps/macos/Sources/Memex/ConversationControls.swift:60-65`), which matches the Claude and Codex completion points above and can never be true mid-turn for ACP.

### 4.4 Steering per provider

- **Claude**: true mid-turn injection. `turn/steer` pushes another user message onto the same streaming-input queue with priority `"next"`, which the comment describes as cooperative same-query steering consumed at the next loop boundary, as opposed to `"now"` which would interrupt (HOST:1345-1356). It must target the current turn id (HOST:1332-1340; CLAUDE:545-550). If the turn completes before the steer is echoed, the steer fails as `ambiguous_delivery` and the query is closed so it cannot become a new turn (HOST:717-726, 750-758).
- **Codex**: native `turn/steer` RPC with `expectedTurnId` (CODEX:1076-1112).
- **ACP**: not offered. The service's ACP action set is prompt, cancel, approval, user input (SVC:1407), and the transport's `steer` is just another `send` (XPORT:347-349). Memex falls back to interrupt-and-restart.
- The service requires an active runtime turn for steer (SVC:876-877).

### 4.5 Cancel acknowledgement

- Dispatching `thread.turn.cancel` marks the turn `cancel_requested` in the store (STORE:1851-1855).
- Claude: `turn/interrupt` → helper awaits `query.interrupt()` then responds (HOST:1387-1392; CLAUDE:620-625) → `provider_operation.complete` (SVC:1123-1125, 1207-1209). The turn itself ends later, when the SDK `result` arrives with an aborted terminal reason and the helper emits `turnCompleted` with `cancelled` (HOST:729, 742-745).
- Codex: `turn/interrupt` with thread and turn id (CODEX:1140-1149). If the native turn id is not known yet (Stop pressed between send and the `turn/start` response), the transport throws `noActiveTurn` and the service fails the operation (CODEX:1143; SVC:1126-1131). Memex surfaces that as "The agent rejected Stop." (`pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:339-341`).
- ACP: a notification only; the operation is completed immediately and `promptDone` is the real proof (SVC:1118-1121, 1182).
- `perform(.cancel)` also has to get onto the serial queue first, so during heavy streaming Stop waits behind the delta backlog.

### 4.6 Earliest "provider accepted the prompt and a turn has started"

In time order, with what each actually proves:

1. `perform` returns with receipt `accepted`; `state.running` is already `true` in the next published view (SVC:913-916, 1020). Proves: durably queued locally and written to the child's stdin. Not provider acceptance.
2. Claude only: `turnStarted` from the helper, emitted synchronously when it receives `turn/send` (HOST:1341-1344), which the service records as `turn.attached` then `turn.started` (SVC:1363-1365, 1555-1559). Proves: the helper accepted it. The turn id is one the helper generated, not the CLI's.
3. **The prompt RPC response**: Claude `turn/send` response (CLI echoed the message) or Codex `turn/start` response (turn id assigned). This is the earliest real provider acceptance. Publicly observable as the `commandStatuses` entry for the command id reaching `completed` with a `nativeTurnID` (and for Claude a `nativeMessageID`) (SVC:442-448), or as the thread snapshot's turn gaining `nativeTurnId`.
4. Codex `turn/started` notification (CODEX:1904-1921).
5. First ephemeral assistant, reasoning or tool item.

For ACP the earliest provider evidence is the first session-update chunk; there is nothing between 1 and 5.

Each of 2 to 4 triggers a subscriber callback immediately (via `state(c)`, SVC:1055, 1077), but the callback payload does not include command status, so a client has to ask `commandStatuses`.

---

## 5. Other things that hurt or help perceived speed

Hurts, inside the runtime:

1. **O(history) work per token, twice** (section 1.2). On a long resumed conversation this is the dominant cost and it grows with the conversation. Because events are queued with `queue.async` and never dropped or merged (SVC:655), a slow queue means the UI falls progressively further behind real time during a long answer rather than skipping frames.
2. **One serial queue for everything**: event ingestion, every public getter (`queue.sync` in `read`, `supportedActions`, `settings`, `commandStatuses`, `children`, `mcpAppTransportIdentity`, `supportsConversationMutation`: SVC:158-163, 310-316, 390-425), and `perform`. Memex's `publish()` and the delta stream block each other.
3. **`conversation.refresh` re-reads and re-normalizes the whole transcript**: full `fs::read`, SHA-256 of the prefix and of the whole file, all batch descriptors, all blobs for the source, full normalization, publish, reload (CONV:628-719, 832-895). The service does this at every turn end (SVC:1591). Memex also calls it from its 1 s tick whenever the provider's transcript file changed, which during a turn is continuously (`pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:206-220`), on the same serial queue.
4. **fsync per ACP chunk**: for source-less ACP sessions every received chunk is appended to an archive and `synchronize()`d before it is applied, with a new `ISO8601DateFormatter` each time (SVC:647-651, 1239; ARCHIVE:39-54). Memex's configured-provider path is source-less (`pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:140-151, 165-167`).
5. **Process-global mutex over all runtime handles**, held for the full duration of each request including the serialization of large views (FFI:931-942). All conversations in the app serialize through it even though each has its own database.
6. **Every runtime write is verified by double projection**: each command/event append loads the thread snapshot, projects in a portable kernel, projects in SQL, reloads and compares (STORE:1646-1681), then loads the snapshot again for the outcome and stores the whole outcome JSON in the receipt (STORE:335-356, 417). Several of these run per prompt and per turn boundary, each in its own immediate transaction. The archive database is explicitly `synchronous = FULL` and blob writes `sync_all` (HSTORE:81, 900).
7. **Blocking calls where async would do**: `transport.start()` waits on a semaphore for up to 12 s inside `queue.sync` (SVC:670; CLAUDE:246; CODEX:309); creation blocks on a condition for up to 45 s (CREATE:153-164); ACP start blocks on a new Tokio runtime (FFI:265-267). Memex calls these from an actor and from `Task.detached`, i.e. on Swift's cooperative pool.
8. **No tool-input streaming for Claude** and deferred text after a tool block (STREAM:51-57, 73-75): a long tool input (large write or edit) produces a silent gap with no event to hang an indicator on.
9. **Wasted publishes**: Codex command-output deltas (SVC:1637-1640), unconditional `state()` after every event (SVC:1519).

Hurts, on the Memex side (found while tracing the call pattern; outside the runtime but directly relevant):

10. `changed()` always waits 80 ms before the first publish, so first-token latency includes a fixed 80 ms plus a full publish (`pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:194-198`).
11. `publish()` repeats control-plane reads that only change at command or turn boundaries: 3 thread snapshots and 3 operation lists per call (section 1.5).
12. `ConversationProjection` rebuilds every record on every publish, including pretty-printing every entity's JSON for `rawJSON` and JSON-encoding evidence for joins (`pr237/apps/macos/Sources/Memex/ConversationProjection.swift:115-158, 402-403`), followed by a whole-snapshot equality check on the main actor (`pr237/apps/macos/Sources/Memex/LiveConversation.swift:475`).
13. For configured ACP providers, every ready publish atomically rewrites the entire conversation JSON to disk (`pr237/apps/macos/Sources/Memex/InAppAgentRuntime.swift:262-264`; `ConfiguredConversationHistory.swift:27-32`).
14. `send()` awaits the ownership probe before it creates the optimistic pending prompt, and `perform` awaits the `beforePrompt` hook and a draft flush before the runtime call (`pr237/apps/macos/Sources/Memex/LiveConversation.swift:596-603, 1026-1041`). The Enter-to-visible-feedback path therefore includes async work.

Helps:

- Deltas are genuinely per-token for text and reasoning on all three paths, with stable item ids for Claude and Codex (STREAM:77-79; CODEX:1944-1959), so a delta channel would be straightforward to add.
- The overlay already models sequenced deltas and item retirement, so persisted and live content do not double-render (LIVE:88-107, 235-282).
- Commands are idempotent by id with durable receipts and no automatic replay of ambiguous deliveries (STORE:276-298; SVC:1031-1033), which is what makes optimistic UI safe to reconcile.
- Readiness for resume does not wait for the Claude CLI (section 3.4), and Codex resume pages history (20 turns) instead of loading everything (CODEX:165, 536-537).
- `state.running` flips at dispatch, so a working indicator keyed on it is effectively optimistic already (SVC:1020, 915).

---

## What Memex could change

Ordered by expected effect on feel. "Runtime API" names what each would call; "Runtime change" says whether the pinned runtime needs modifying.

### A. Possible now, no runtime change

| # | Change | Runtime API used | Runtime change |
|---|---|---|---|
| A1 | Render from the subscribe payload instead of calling `service.read` on every flush. The payload is already the reconciled envelope for the session (SVC:1856-1861); keep only the newest one per flush. It is a `String`, so it crosses actors safely. Removes one of the three full-conversation passes. | `subscribe` callback argument | No |
| A2 | Split content from control. Fetch `thread.snapshot`, `provider_operation.list`, `commandStatuses`, `settings`, `children`, mutation support only when `state.running` / `pending_interactions` change, after a `perform`, and on the slow tick; not per streaming frame. | same calls, fewer of them | No |
| A3 | Re-project only what changed. Cache rendered records for `presentation` keyed by a cheap fingerprint (entity count plus last entity id/revision), and re-render only `ephemeral` items on streaming payloads. Hand the main actor changed record ids instead of relying on whole-array equality. | payload fields `presentation`, `ephemeral` | No |
| A4 | Make the throttle leading-edge: publish the first change immediately, then at most once per frame interval while changes keep arriving. Removes the fixed 80 ms from first token and from every state transition. | none | No |
| A5 | Stop calling `service.refresh` from the 1 s tick while a turn is running; the service refreshes at turn end itself (SVC:1591). Keep the tick for idle external-writer detection. | `refresh` (less often) | No |
| A6 | Pre-warm new chats: build an `AgentConversationCreation` when the new-chat composer opens with a known directory, hold it un-adopted, `connectCreated` on first send, drop it on dismiss (its `deinit` stops the provider). Check Codex rollout-file litter first. | `AgentConversationCreation.claude/.codex/.acp`, `connectCreated` | No |
| A7 | Pre-warm resumes: connect on open/focus/first keystroke rather than first send, with an idle disconnect. This claims the writer lease earlier, so it interacts with the "open elsewhere" design and needs an owner decision. | `connectClaude/Codex/ACP` | No |
| A8 | Key the indicator on four explicit stages: (1) Enter pressed, local and synchronous, before the ownership probe and draft flush; (2) `perform` returned with an accepted receipt; (3) `commandStatuses` shows `completed` with a native turn id, which is the provider acceptance point for Claude and Codex; (4) first ephemeral assistant, reasoning or tool item. For ACP skip 3. Reconcile the optimistic bubble on stage 3 (already done via `isAccepted`) and treat Claude "accepted" latency after resume as CLI startup. | `perform` result, `commandStatuses`, payload `ephemeral` | No |
| A9 | Make Stop feel immediate: reflect "stopping" locally at once and treat the acknowledgement as reconciliation, and handle the Codex "no native turn yet" failure by retrying once the turn id is known instead of surfacing a rejection. | `perform(.cancel)`, `commandStatuses` | No |
| A10 | For configured ACP providers, write `acp-observed-view.json` at turn end or idle, not on every publish. | none | No |
| A11 | Keep blocking runtime calls off the Swift cooperative pool (dedicated dispatch queue or thread for connect/create). | none | No |

A1 to A5 remove Memex's own duplication and delay, but each frame still costs one full-conversation parse, and the runtime's two-per-token cycles remain. On long conversations that floor is in the runtime.

### B. Needs a runtime change

| # | Change | Where | Why Memex cannot do it alone |
|---|---|---|---|
| B1 | A typed per-connection delta subscription on `AgentConversationService`, emitting the already-public `AgentThreadEvent` values (or item id + appended text + sequence) from `receive` (SVC:1518) independently of the overlay round trip. Memex would then append to the live row directly and use snapshots only for reconciliation at item/turn boundaries. | SVC, PROJ:634 | Transports and native events are internal (XPORT:111, NATIVE:216) |
| B2 | Stop returning the full view from `conversation.apply` (acknowledge only, or return just the changed item), and send `text_delta` updates instead of full-item snapshots. | CONV:378-385, SVC:1813-1845, LIVE:100-105 | Rust protocol and service internals |
| B3 | Coalesce in the service: skip `state()` when unchanged (SVC:1519), merge publishes per short interval, skip no-op tool snapshots (SVC:1637-1640). | SVC | Service internals |
| B4 | Make `view()` cheap: cache `presentation`, stop serializing evidence on every call, expose a persisted-revision counter so clients can skip unchanged history. | LIVE:383-455 | Rust |
| B5 | Batch or defer the fsync in `AgentACPEventArchive`. | ARCHIVE:39-54 | Service internals; it is a durability decision for the runtime owner |
| B6 | Startup: do not hold `sessionOpened` on `setPermissionMode` (or pass the mode only through `options`), cache the `codex --version` result per executable, move `transport.start()` out of `queue.sync`, and offer an explicit warm-process API. | HOST:1008-1018, CODEX:192-194, SVC:670 | Helper and transport internals |
| B7 | Forward Claude tool-input progress (at least a "tool call started" event at `content_block_start`) so the UI can show activity during long tool inputs. | STREAM:51-57 | Helper |
| B8 | Expose streamed command output for Codex through the service. | SVC:1627-1668 | Service internals |
| B9 | Replace the process-global registry mutex with per-handle locking. | FFI:931-942 | Rust FFI |

### C. Alternative worth naming, not recommending blind

Use the public `ChatViewModel` for live rendering. It already applies deltas in place and has send-before-ready, queueing, steering and immediate cancel (section 2.1). The cost is giving up the service's durable command ledger, receipts and overlay reconciliation that Memex's delivery-safety behaviour is built on, and adopting its one-transport-per-provider model. It also publishes every delta uncoalesced with an O(message count) array compare (CVM:8930). B1 gets the streaming benefit without that trade.

## Not verified

- No timings were taken. Relative costs are inferred from code paths; a profile of one long resumed conversation would confirm the ranking in section 5.
- Behaviour inside the Claude Agent SDK / `claude` CLI and the Codex app-server is not in this source: exactly when the CLI echoes a user message, how long CLI startup takes, and whether Codex writes a rollout file at `thread/start`.
- SQLite's effective `synchronous` level for `runtime.sqlite` is whatever the bundled build defaults to; the code sets none (STORE:262-267).
- I read `ChatViewModel.swift` selectively (send, stream, steer, cancel, start, publish paths), not all 9,493 lines, and only skimmed `AcpChatSidebarView` / `AcpComposerView`.
- Memex's `TranscriptController` was only skimmed, so the scroll-behaviour comparison in 2.4 is a prompt to compare, not a finding.
