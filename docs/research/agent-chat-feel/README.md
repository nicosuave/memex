# Agent chat feel: PR 237 review and research synthesis

Date: 2026-11-09. Subject: [nicosuave/memex#237](https://github.com/nicosuave/memex/pull/237) at `a278a9f`, with its pinned runtime `sidequery/mono@e610a6f`.

Goal being reviewed against: optimistic UI, few spinners, shimmer for in-progress states, no jank, very fast.

## Evidence basis

- Nothing was built or run. Every PR finding is from reading source. The app's real latencies are unmeasured.
- One local measurement: `lsof -nP -w -Fpa -- <claude transcript>` took 340-450 ms over three runs on this machine.
- External research was done by 13 subagents reading cloned source at recorded SHAs. Their reports label each claim verified, reported or unverified; numbers quoted from other projects' PRs are reported, not reproduced.
- Two more subagents read the pinned runtime and the PR's execution-host and web half.

## How PR 237 works

### Layers

| Layer | What it is | Where |
| --- | --- | --- |
| Provider process | Claude: a Bun helper plus the `claude` CLI. Codex: `codex app-server`. Configured `acp:` providers: a Rust ACP client. One process set per conversation, nothing pooled. | runtime `sq-acp` |
| Runtime service | `AgentConversationService`. Receives per-token deltas, converts each into a full-item snapshot, and returns the whole conversation view from `conversation.apply`. Commands and receipts are durable in SQLite. | runtime `sq-acp`, `agent-runtime` |
| Memex adapter | `NativeConversationRuntime` actor. On any notification it waits 80 ms, then re-reads the whole conversation, makes three SQLite-backed requests, re-projects every record and emits a full `ConversationSnapshot`. A 1 Hz tick also stats the JSONL transcript and refreshes it. | `InAppAgentRuntime.swift:189-270` |
| Conversation state | `LiveConversation`, a main-actor state machine: pending prompt, durable queue with hold, delivery uncertainty, generation tokens. | `LiveConversation.swift` |
| UI | `ReaderView` hosts an `NSTableView` transcript, a pending-prompt view above the composer, and the runtime's `AcpComposerView`. | `Reader.swift`, `TranscriptController.swift` |

Only configured `acp:` providers use ACP. Claude goes through the Claude Agent SDK and Codex through app-server.

### What happens on Enter

Existing conversation, not yet connected (the first send after browsing history):

1. `lsof` ownership probe, awaited before anything changes on screen (`LiveConversation.swift:597`).
2. Composer clears and the prompt appears as a bubble above the composer.
3. Connect: import the JSONL into the runtime archive, spawn the provider, wait for ready.
4. The transcript is replaced by the live snapshot (`Reader.swift:119-147`).
5. Pre-turn git checkpoint (`Store.swift:184`, `WorkspaceCheckpoints.swift:169-200`).
6. Flush `drafts.json`, one file for all sessions (`LiveConversation.swift:1040`).
7. `runtime.perform`: about three SQLite transactions and a stdin write.
8. The provider's RPC response marks the delivery accepted; the echo arrives; the bubble is removed and a table row appears.

Already connected: step 1 is skipped (`canProbeOwnership`), so the bubble appears immediately, then steps 5-8.

During steps 3-7 `preparingPrompt` is true, so `canSubmit` and `canQueue` are both false and a second Enter does nothing (`LiveConversation.swift:301-306`).

New chat from Home (`StoreNewConversation.swift:83-137`) is a serial chain: flush draft, prepare workspace, create the native session, apply model and settings with an acknowledgement wait each, capture attachments, reveal, then the send path above. The reader shows `PendingConversationStartView`, a prompt bubble with a spinner and a phase label, and there is no sidebar row until creation finishes.

### Streaming

Per token the runtime builds the full conversation view at least twice. Memex then adds 80 ms and a full re-read. On the main actor each snapshot regroups all records, re-parses the streaming message's markdown in full, re-measures that row and jumps to the end (`TranscriptController.swift:540-637`, `:810-972`).

Result: text arrives in blocks at 12 Hz or slower, and per-update cost grows with conversation length.

### What is already strong

- Delivery safety: client command IDs, durable receipts, an explicit uncertain state, and no automatic resend.
- Durable drafts and a durable queue with edit, reorder, cancel, promote and hold-on-stop. This is ahead of Zed (in-memory queue) and every terminal agent.
- The transcript container. `NSTableView` with explicit row heights and a height cache is what the native research recommends. The incremental path re-measures only changed rows, and disclosure toggles anchor on the clicked header.
- Generation tokens guard every async continuation.
- Reduce Motion is honoured for the working label.

## Gaps against the best apps

### 1. Enter to pixels

| | Memex PR 237 | Best practice | Source |
| --- | --- | --- | --- |
| Where the sent message renders | Separate bubble above the composer, then a different table row on echo | A transcript row at t=0, keyed by a client-minted ID that the echo reuses | OpenCode, T3 Code, Codex `clientUserMessageId`, Codevisor |
| Work before the composer clears | `lsof` on a first send | None | all |
| Working indicator | After `perform` starts | Same frame as the row, cleared by comparing server state to a local snapshot | T3 Code, OpenCode, Cline |
| Second Enter during dispatch | Ignored | Queued | Orca, OpenCode |

The runtime already carries what reconciliation needs: Memex mints the command ID, and deliveries return `nativeTurnID` and `nativeMessageID`.

### 2. Streaming

| | Memex PR 237 | Best practice | Source |
| --- | --- | --- | --- |
| Transport | Full snapshot, 80 ms trailing delay | Deltas coalesced per frame or per 50-60 ms, leading edge | OpenCode 16 ms, Paseo 60 ms, T3 50 ms |
| Markdown | Full reparse on main | Tail-only reparse, off main, settled blocks never re-laid out | Streamdown, Cline, OpenCode worker, Zed background parse |
| Cadence | Raw 80 ms blocks | Paced reveal that drains a backlog in 150-300 ms, with a short fade | Zed 200 ms, Paseo 150 ms, Codevisor 150 ms fade, Codex two-gear |
| Unterminated markdown | Not handled specially | Speculatively closed while streaming, real parse at completion | Streamdown `remend`, Lakr233 MarkdownView |
| Follow state | Within 60 pt of the end | From user input: wheel, keys, scroller drag | Roo, Kilo, OpenCode, Zed |

T3 Code takes the opposite route: no token streaming at all, finished paragraphs at most every 400 ms with a 600 ms fade. That is a legitimate alternative. I recommend paced token reveal instead, because Claude's text after a tool call arrives as one consolidated burst and a reveal buffer smooths that too.

Paseo is the only project with a before-and-after number: p95 gap between visible updates from 383 ms to 17 ms (reported in its own docs).

### 3. Cold start

- One process set per conversation and connect-on-first-send means every first send pays provider start. Research puts Claude CLI start at about 2-3 s (reported).
- Create blocks on `initialize` and then on session open, and for Claude it waits on `setPermissionMode`, which waits on CLI boot.
- Best practice: one long-lived adapter per agent (Zed, CodexMonitor, OpenCode), a spare session created before the user types (Zed), a warm pool with a short TTL (Acpira, 5 min), and attaching on intent rather than on Enter.
- Constraint: each live Claude CLI used 206-678 MB here, so about four idle sessions is the ceiling. Spares must match exactly and expire on auth or settings change.

### 4. New chat

- Best practice is a composer-first pending chat: the real chat view exists at t=0, the first message shows as held, and setup progress is an inline checklist with retry (Orca, OpenCode). T3 Code commits the run as `preparing` and provisions afterwards, keeping one view instance across the draft-to-thread change.
- Worktrees: Orca prepares one speculatively when a repo is selected. `git worktree add --no-checkout` measured 40 ms against 2.3-3.0 s for a 15.5k-file checkout (subagent measurement).

### 5. History to live swap

Connecting swaps to a second `NativeTranscript` with a different record-ID namespace, so scroll position and layout caches are lost. The fix is one transcript that paints from the local index and reconciles when the provider attaches. The protocol research confirms a client-owned transcript is required anyway, since ACP `session/load` cannot restore instantly.

### 6. Affordances

- `AcpShimmerText` sweeps a fixed 80 pt band from -100 to +100 pt whatever the text width, as a SwiftUI animation on the main thread. Codevisor and FlowDown use a `CAGradientLayer` mask sized to the text and phase-locked to wall clock.
- About 37 `ProgressView` uses across 23 files. The perceived-performance research justifies a spinner in three cases only: inside a button for a non-optimistic external action, on an attachment of unknown size, and a system toolbar activity mark.
- Indicators need a 200 ms show delay and a 300 ms minimum display. Status labels need a debounce (Codevisor 0.3 s).
- Sidebar working state is a static `circle.dotted` symbol.
- Model and settings pickers wait for native acknowledgement and block sending while pending.

### 7. Remote host and web surface

- No push anywhere. Both the browser and the macOS remote runtime poll a full snapshot once a second.
- Web send is not optimistic, one `busy` flag disables every control, a failed send jams the composer, there is no stick-to-bottom, and the whole transcript re-parses on each keystroke and poll.
- The macOS remote viewer fails on one bad poll. Responses over 32 MiB fail with no paging.

### 8. Interaction conventions

Five of six terminal agents deliver Enter-while-busy at the next tool boundary. Memex defaults to queue-after-turn with steer as a preference. The runtime supports true mid-turn injection for Claude and Codex, so boundary delivery could be the default, with after-turn and send-now on modifier keys.

## Recommended order

1. Instrument first: `enter_to_indicator`, `enter_to_prompt_written`, `enter_to_first_token` split by warm, cold and resume, `open_to_paint`, and per-publish cost. Nothing below can be verified without these.
2. Optimistic transcript row and immediate working row. Run the ownership probe concurrently and let a second Enter queue during dispatch. Memex only.
3. Streaming, no runtime change: render from the subscribe payload, leading-edge throttle, stop mid-turn refresh, read control-plane state only at command and turn boundaries. Then a per-frame pump, tail-only off-main markdown, and paced reveal.
4. Pre-warm through an un-adopted creation, connect on intent, and keep a small LRU of live sessions.
5. Composer-first new chat with a sidebar row at t=0 and a speculative `--no-checkout` worktree.
6. Affordance pass: Core Animation shimmer, spinner audit, anti-flash thresholds, input-driven follow.
7. Runtime changes: a typed delta subscription, `conversation.apply` not returning the full view, service-side coalescing, and not gating session open on `setPermissionMode`.
8. Push transport for the execution host and web surface.

Steps 2-6 do not require weakening the delivery-safety model. Optimism applies to showing the user's intent; "accepted", "stopped" and "worktree created" still render only on confirmation.

## Implemented on `claude/agent-app-ux-patterns-13a76b`

Based on PR 237 head `a278a9f`. Verified by build and unit tests only; the app has not been run and no provider
prompt has been sent, so every item below is unverified live.

| Step | Done | Where |
| --- | --- | --- |
| 1 Instrument | Signposts and debug log for each send step, Enter to dispatch, accepted and first output, and publish cost | `ChatSignposts.swift`, `LiveConversation.swift`, `InAppAgentRuntime.swift` |
| 2 Optimistic send | Composer clears and prompt shows before the ownership probe; new-turn prompt is a transcript row until its native echo; follow-up can be queued during dispatch; "Sending…" label only after 2 s | `LiveConversation.swift`, `ConversationPendingView.swift`, `Reader.swift` |
| 3 Streaming | Leading-edge adaptive publish (50-250 ms); content from the subscription payload; control-plane reads at boundaries only; mid-turn source refresh every 5 s. Tail-only markdown, healing of unterminated markdown, paced reveal (about 200 ms drain), input-driven follow with "Jump to latest" | `InAppAgentRuntime.swift`, `StreamingMarkdown.swift`, `StreamingReveal.swift`, `TranscriptFollow.swift`, `TranscriptController.swift` |
| 5 New chat (partial) | Pending page renders the prompt through the real transcript view with a shimmer status line. Still a separate page with no sidebar row, and creation is still serial | `PendingConversationStartView.swift` |
| 5 History to live | One `NativeTranscript` for both sources, so connecting reloads in place. Record IDs still differ between sources, so the reload lands at the end rather than preserving a mid-history position | `Reader.swift` |
| 6 Affordances | Core Animation `ShimmerText`; `DelayedProgress` (200 ms show delay); sidebar working pulse; 28 of 29 spinner sites replaced or removed | `ShimmerText.swift`, `DelayedProgress.swift`, 17 view files |
| 8 Web (partial) | Optimistic send, per-action busy state, host-side queue while running, memoized rows, stick-to-bottom, no blanking on switch, 250 ms adaptive polling, CSS shimmer. Transport is still polling | `web/src/Execution*.tsx`, `execution.ts` |

Not done, and why:

- Step 4, pre-warm and connect on intent: it changes the documented rule that browsing does not launch a provider and
  costs 200-600 MB per idle Claude CLI. Needs an owner decision.
- Speculative worktrees: creates worktrees the user did not ask for and needs TTL cleanup.
- Step 7, runtime delta subscription and service-side coalescing: changes in `sidequery/mono`.
- Push transport for the execution host.
- Moving the pre-turn checkpoint off the send path: unmeasured, and it must precede the agent's first write.
- Off-main markdown parsing: the renderers are main-actor bound; tail-only rendering made it unnecessary for now
  (subagent measurement: 2.3 ms per update against 63.6 ms on a 21,000-character reply).
- Three send levels on modifier keys, and boundary delivery as the Enter default.

## Open questions

- Whether the pre-turn checkpoint can move off the send path. It must capture state before the agent's first write, so it may only be safe to run concurrently with connect. Its real cost is unmeasured.
- Whether out-of-order snapshots can occur. Each runtime callback hops to the main actor in its own unstructured `Task` (`LiveConversation.swift:471`), which is ordered in practice but not guaranteed.
- `docs/chat-implementation.md` in the PR states that runtime-enabled packaging at the current pin is unverified.

## Reports

| Report | Scope |
| --- | --- |
| [runtime-internals.md](runtime-internals.md) | Pinned runtime: event flow, startup, dispatch |
| [pr237-host-and-web.md](pr237-host-and-web.md) | PR 237 execution host and web surface |
| [zed.md](zed.md) | Zed agent panel and ACP thread |
| [t3code.md](t3code.md) | T3 Code |
| [opencode.md](opencode.md) | OpenCode app and TUI |
| [codex.md](codex.md) | Codex app-server protocol and TUI |
| [acp-protocol.md](acp-protocol.md) | ACP v1 and v2, Claude and Codex adapters |
| [acp-clients.md](acp-clients.md) | ACP client survey: Codevisor, Acpira, agent-shell, Toad |
| [orchestrators.md](orchestrators.md) | Orca, Paseo, Paradev, xum and others |
| [ide-agents.md](ide-agents.md) | Cline, Roo, Kilo, Continue, Goose |
| [terminal-agents.md](terminal-agents.md) | Claude Code, pi, Crush, Gemini CLI, Amp |
| [swift-native.md](swift-native.md) | Native macOS transcript, markdown and shimmer |
| [web-chat-libs.md](web-chat-libs.md) | AI SDK, AI Elements, Streamdown, assistant-ui |
| [perceived-performance.md](perceived-performance.md) | Latency budget, affordance rules, anti-patterns |
| [fast-start.md](fast-start.md) | Session start and resume latency |
