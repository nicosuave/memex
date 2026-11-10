# T3 Code: implementation-level techniques behind "feels fast"

Source: `github.com/pingdotgg/t3code`, shallow clone at
`ee59ab22a819acb0544d85cd6cca0e4aa8c7c444` (2026-11-09, "fix(mobile): the composer and search bars
move with their iOS columns (#33543)"). Local path:
`/private/tmp/claude-501/-Volumes-Dev-Code-memex--claude-worktrees-agent-app-ux-patterns-13a76b/f8deb1e4-a002-4452-a81e-c8ef8a51001b/scratchpad/repos/t3code`.
Nothing from the repo was built, installed, or run.

Labels: **VERIFIED** = read in source at that SHA (`path:line`). **UNVERIFIED** = taken from a PR
or issue body via `gh` (URL given); numbers there are the authors' own measurements.

Scope note: the repo is far larger than "a GUI wrapper" now (31,894 files; `Orchestrator.ts` alone
is 14,675 lines; web, Electron desktop, React Native mobile, relay). I read the web send/stream/render
path, the shared `client-runtime` state, the server streaming pipeline and launch path, and the
CSS. I did not audit the mobile app, the Electron shell startup, or the relay.

---

## 1. TL;DR

1. **They deliberately do not stream tokens.** The default (`paragraph`) delivers only *finished
   Markdown blocks*, at most one emit per message per 400 ms; the old token mode was retired and is
   coerced to `paragraph` on load. This single product decision removes most streaming jank
   (half-word repaints, re-flowing partial Markdown, unstable code fences) and is why the rest of
   the pipeline can be cheap. VERIFIED `apps/server/src/orchestration-v2/assistantStreaming.ts:15-110`,
   `packages/contracts/src/settings.ts:1106`, `apps/server/src/serverSettings.ts:352-364`.
2. **Everything on Enter is local and synchronous**: client-generated `threadId` (at draft
   creation) and `messageId` (at send), an optimistic row keyed by that same id, a local
   "dispatch snapshot" that turns on the Working row and timer, the composer cleared, the new turn
   anchored near the top. Only then is one idempotent RPC sent. VERIFIED
   `apps/web/src/components/chat/useComposerSend.ts:784,1043-1131,1232`.
3. **No flash on reconcile** because (a) optimistic and server rows share an id/list key,
   (b) the optimistic row is dropped *during the same render* in which the server row becomes
   visible (derive-in-render, no effect/timer), and (c) the same `ChatView` instance survives the
   draft -> thread route swap. VERIFIED `apps/web/src/components/ChatView.tsx:632-646`,
   `apps/web/src/components/ThreadRouteView.tsx:28-33,170-186`.
4. **Streaming cost is bounded at four layers**: adapter 50 ms full-text coalescer -> server
   paragraph filter (400 ms) -> per-subscriber 50 ms "latest per entity" coalescer with a byte
   budget -> client applies each transport batch as one state update; then "quiet" atoms hide
   text-only churn from everything except the one streaming row, and Markdown is split so only the
   last block re-parses.
5. **Animations are duty-cycled, not per-vsync**: shimmer is `2.2s steps(30)`, skeleton and status
   pulses are stepped opacity holds, all paused by an IntersectionObserver + `visibilitychange`
   through a CSS variable. Their AGENTS.md states the rule: "No continuously repainting
   animations; they peg the GPU on high-refresh displays." VERIFIED `apps/web/src/index.css:295-346,551-656`,
   `apps/web/src/lib/visibleAnimation.ts:10-47`, `AGENTS.md:207`.
6. **Worktree creation never blocks first paint**: the launch command commits thread + message +
   a run in status `preparing` (`defer_start`), acks, and provisions the worktree in a forked
   background fiber that reports `prepared-run.progress`. VERIFIED
   `apps/server/src/orchestration-v2/ThreadLaunchService.ts:789-833,998`.
7. **Thread switching is cache-driven, not prewarmed**: one unkeyed `ChatView` + one unkeyed
   virtual list, remembered scroll positions, a rendered-Markdown element cache, a highlight
   cache, a 5-minute in-memory snapshot+cursor, IndexedDB snapshots, bounded server snapshots.
   They explicitly *rejected* hover prewarm of thread detail for heap/server-load reasons.

---

## 2. Architecture of the send -> stream -> render loop

### 2.1 Pieces

| Layer | What it is | Evidence |
|---|---|---|
| Server | Node WebSocket server, Effect-TS. Event-sourced "orchestration v2": typed RPC -> command -> `Orchestrator` (pure decisions, serialized) -> `EventSink` commits events + projections + command receipt + outbox effects in **one SQLite transaction** -> `EffectWorker` runs side effects (start provider turn, checkpoint) and feeds results back as commands. A command ack means "intent committed", not "provider started". | VERIFIED `docs/internals/overview.md:45-62`, `AGENTS.md:179` |
| Persistence | SQLite via `node:sqlite`, WAL, `synchronous = NORMAL` (`statev2.sqlite`). Event log keeps roughly a day for replay; projections are the source of truth. | VERIFIED `apps/server/src/persistence/Sqlite.ts:25,28`, `docs/internals/overview.md:61-73` |
| Transport | Effect RPC over WebSocket, JSON serialization, `perMessageDeflate: true` with context takeover so small frames compress well. Streams are pull-based with ACK (the next batch is requested after the client acks). | VERIFIED `apps/server/src/ws.ts:3572-3607`, `apps/server/src/server.ts:370-374`, `apps/server/src/orchestration-v2/LiveStreamBudget.ts:175-184` |
| Providers | One `ProviderAdapterV2` service per provider instance: `getCapabilities`, `planSelectionTransition`, `openSession` -> `ProviderAdapterV2SessionRuntime` (`events` stream, `ensureThread`, `resumeThread`, `startTurn`, `steerTurn`, `interruptTurn`, `respondToRuntimeRequest`, `readThreadSnapshot`, `rollbackThread`, `forkThread`, optional `compactThread`/`injectHistory`/`unloadThread`). | VERIFIED `packages/provider-core/src/server/ProviderAdapter.ts:617-725` |
| Normalized events | One union: `provider_session.updated`, `provider_thread.updated`, `provider_turn.updated`, `node.updated`, `subagent.updated`, `message.updated`, `turn_item.updated`, `background_work.updated`, `command_output.delta`, `runtime_request.updated`, `plan.updated`, `turn.terminal`, ... Entities are **whole-entity replacements** (full text each time), not deltas. | VERIFIED `packages/provider-core/src/server/ProviderAdapter.ts:83-191`, `apps/server/src/orchestration-v2/ThreadLiveEventCoalescer.ts:50-51` |
| Concrete adapters | Claude = `@anthropic-ai/claude-agent-sdk` (`ClaudeAdapterV2.ts`, 12,444 lines); Codex = own `effect-codex-app-server` client (`CodexAdapterV2.ts`); ACP = `packages/effect-acp` + `packages/provider-acp` (used by Grok, Kiro, "fx", and an ACP registry); Cursor and OpenCode have their own SDK-based packages. | VERIFIED `apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts:67`, `apps/server/package.json:39,100`, `packages/provider-{grok,kiro,fx,acp-registry}/package.json` |
| Client state | `packages/client-runtime` (shared by web and RN): Effect `Atom` families (`effect/reactivity` + `@effect/atom-react`), not TanStack Query. Zustand is used only for local UI stores (composer drafts, persisted). Routing is TanStack Router. React Compiler is mandatory: lint bans `useMemo`/`useCallback`, a test compiles every file and fails on any compiler bailout. | VERIFIED `apps/web/package.json`, `apps/web/src/composerDraftStore.ts:2499`, `docs/internals/web-ui.md:57-76` |
| Web UI | React + Vite + Tailwind v4; timeline virtualized with `@legendapp/list` 3.7.2; Markdown via unified/remark (own renderer, not `react-markdown`'s component); code + diffs via `@pierre/diffs` with the WASM "highlights" lexer and a worker pool. | VERIFIED `pnpm-workspace.yaml:57`, `apps/web/src/components/chat/markdownRenderer.ts:46-108`, `apps/web/src/lib/syntaxHighlighting.ts:17-22`, `apps/web/src/components/DiffWorkerPoolProvider.tsx:20-70` |
| Desktop | Electron wrapping the web app and bundling the server. Adds one behaviour relevant here: every running thread's detail stream is kept mounted. | VERIFIED `apps/web/src/state/threads.ts:82-133`, `docs/internals/connection-runtime.md:113-120` |

### 2.2 The streaming pipeline, stage by stage

```
provider delta
  -> adapter ProviderTextDeltaCoalescer (50 ms; emits FULL text so far)        [server]
  -> makeAssistantStreamingFilter: only finished blocks, >=400 ms apart         [server]
  -> Orchestrator/EventSink: committed event (whole-entity replacement)        [server, SQLite]
  -> per-subscriber ThreadLiveEventCoalescer (50 ms, latest per entity,        [server]
     512 pending cap, 1,000 items / 8 MiB retained budget -> else "resume")
  -> RPC stream chunk (ACK-gated, permessage-deflate)                          [wire]
  -> Stream.runForEachArray(applyItems): one SubscriptionRef.modify per batch  [client]
  -> quiet atoms: structure readers do not see text-only changes               [client]
  -> the one streaming row reads assistantMessageTextAtom                      [client]
  -> ChatMarkdown: split into blocks, only the last block re-parses            [client]
  -> new block fades in (opacity 600 ms, @starting-style)                      [CSS]
```

- Adapter coalescer: buffer per `(turnId,itemId)`, first append schedules one flush after
  `flushIntervalMs`, later appends just mark dirty; `complete` flushes synchronously. Claude 50 ms,
  Codex assistant 50 ms, Codex plan 400 ms, ACP 50 ms ("Emit first chunks and segment-close events
  immediately; coalesce later chunks into full-text snapshots. This avoids quadratic output").
  VERIFIED `apps/server/src/orchestration-v2/Adapters/ProviderTextDeltaCoalescer.ts:99-130`,
  `ClaudeAdapterV2.ts:7860`, `CodexAdapterV2.ts:214-216`, `packages/provider-acp/src/server/adapter.ts:147-151`.
- Paragraph filter: `splitBufferedAssistantText` finds the last safe boundary (blank line outside a
  fence, a closed fence, a new list item, a top-level heading; a section title is held until
  content follows). Streaming updates are dropped when `now - previous.at < 400` or when nothing
  new is "ready". Mode `turn` drops all streaming updates. VERIFIED
  `apps/server/src/orchestration-v2/assistantStreaming.ts:15-69,96-101`.
- Live coalescer: assistant/reasoning text and *running* tool items
  (`command_execution`, `file_change`, `file_search`, `web_search`, `dynamic_tool`) are
  coalescible; any other event flushes the window first so ordering and resume cursors stay
  correct. VERIFIED `apps/server/src/orchestration-v2/ThreadLiveEventCoalescer.ts:18-48,139-194`.
- Raw command output is **never** an orchestration event. Adapters send it to a
  `CommandOutputHub` (bounded tail per running command, throttled per subscriber) and a client
  subscribes only while that command row is expanded. VERIFIED
  `docs/internals/performance-regressions.md:45-51`.
- Client batch apply:

  ```ts
  // packages/client-runtime/src/state/threads.ts:592-605 (abridged)
  const result = yield* SubscriptionRef.modify(state, (current) => {
    let result: EventApplyResult = { _tag: "noop" };
    for (const item of fresh) { /* apply each event to `current` */ }
    // A batch that changed nothing keeps its state object, so readers do not re-render.
    return [result, result._tag === "noop" ? current : { ...current, sequence }];
  });
  ```
  The stream is consumed with `Stream.runForEachArray(applyItems)` (`threads.ts:1093`), so a
  transport chunk of N events produces one store write. There is no client-side rAF throttle; the
  throttling is upstream.
- Persisting the thread cache is kept off the streaming path: `shouldPersistThread` returns false
  while any run is `preparing|starting|running`, and writes go through `Queue.sliding(1)`.
  VERIFIED `threads.ts:113-125,305,453-466`.

### 2.3 t=0 sequence when the user presses Enter

**New thread (a "draft")**

Before Enter (when "New thread" was pressed): `draftId = newDraftId(); threadId = newThreadId()`
are generated client-side, stored in the persisted Zustand draft store, and the router navigates to
`/draft/$draftId`. No server round trip creates anything. VERIFIED
`apps/web/src/hooks/useHandleNewThread.ts:365-366,395-421`. (One caveat: if the project has no
default env mode, it awaits a project-file read first, skipped when the environment is offline -
`useHandleNewThread.ts:178-190,369`.)

On Enter, in order, all before the first `await` that touches the server
(`apps/web/src/components/chat/useComposerSend.ts`):

| Step | What | Line |
|---|---|---|
| 1 | Snapshot the composer (text, images, files, contexts) for rollback | 721-741 |
| 2 | `messageIdForSend = newMessageId()`, `messageCreatedAt = new Date().toISOString()` | 784-785 |
| 3 | Kick off attachment uploads in the background ("Uploads finish after the message shows as sending; the turn starts once they land") | 838-851 |
| 4 | `beginLocalDispatch({ preparingWorktree, submissionIntent })` -> `isSendBusy`, `localDispatchStartedAt`; this is what turns on the Working row and its timer | 1043-1049 |
| 5 | `anchorNewTurn(threadKey, messageId)`: follow mode on, hide the scroll-to-end pill, arm the "pin this user message near the top" anchor | 1080-1085 |
| 6 | Build `optimisticMessage` with `id: messageIdForSend` and push it | 1086-1109 |
| 7 | `markPromotedDraftThreadByRef`, clear composer content, reset cursor | 1124-1131 |
| 8 | One RPC: `startThreadTurn({ threadId, message:{messageId,...}, bootstrap:{createThread, prepareWorktree?, runSetupScript?}, titleSeed, ... })` | 1198-1256 |

Server side: `startThreadTurn` with a bootstrap becomes a single `launchThread` request carrying
the client `commandId`, `threadId`, `messageId`, and a `workspaceStrategy`
(`packages/client-runtime/src/operations/commands.ts:686-733`). The launch service dispatches
`message.dispatch` with `dispatchMode: { type: "defer_start", workspaceStrategy }`
(`ThreadLaunchService.ts:981-998`), which creates the run in status `preparing`
(`Orchestrator.ts:6757`), and forks worktree provisioning into a background scope
(`ThreadLaunchService.ts:820-832`). Title generation and branch renaming also run forked
(`ThreadLaunchService.ts:490-569`).

Route handoff: the `_chat` layout owns `ChatView`; both `/draft/$draftId` and
`/$environmentId/$threadId` route components render `null`
(`apps/web/src/routes/_chat.draft.$draftId.tsx:3-6`). A store-level subscription
(`subscribeDraftThreadPromotion`) watches server shells: when the reserved thread id appears it
marks the draft "promoting"; once the server "owns" the send (message persisted, or run
started/stopped) *and any hero-to-docked composer transition has finished*, it finalizes, which
records a redirect (`apps/web/src/composerDraftStore.ts:4493-4509,4517-4592`). `ThreadRouteView`
renders the server-route `ChatView` with `key={draftId}` in the same slot and emits
`<Navigate replace>`, so React keeps the instance through the URL change
(`ThreadRouteView.tsx:61-76,170-186,196-203`).

**Existing thread**

Same steps 1-7. Step 8 is `message.dispatch` with `start_immediately`, `queue_after_active`, or a
`deliveryIntent` of steer; delivery is resolved from authoritative state inside the server's
per-thread dispatch lock, so the client does not fetch a projection first
(`commands.ts:735-785`, `docs/internals/performance-regressions.md:56-60`). If a run is active
and the message will queue, the optimistic message carries `inputIntent: "queued_turn"` and is
shown in the queue control rather than the timeline (`useComposerSend.ts:1096-1100`,
`apps/web/src/session-logic.ts:705-715`, `apps/web/src/components/chat/QueuedRunsControl.tsx:127-129`).

**Reconciliation without a flash**

```tsx
// apps/web/src/components/ChatView.tsx:635-646 (abridged)
// Acknowledged messages leave the timeline in the same render the server's copy enters it.
const optimisticState = reconcileOptimisticUserMessages(
  heldOptimisticUserMessages, threadId, serverAcknowledgedUserMessageIds);
if (optimisticState !== heldOptimisticUserMessages) {
  optimisticBlobPreviews.handOff(optimisticState);
  setHeldOptimisticUserMessages(optimisticState);
}
```

- "Acknowledged" means the server's `user_message` **turn item is visible**, not merely that the
  message exists in the projection ("Projection messages can precede visible turn items by one
  event; evict optimistic rows only when visible") - `apps/web/src/components/ChatView.logic.ts:1011-1021`.
- Timeline entries dedupe by message id and the list key is the id, so the DOM row is the same
  node before and after (`session-logic.ts:685-715`, `MessagesTimeline.tsx:1392-1394`).
- Image attachments: the optimistic row shows `blob:` previews; on ack they move to a "handoff"
  set and are revoked only after the server URLs have finished preloading via `new Image()`
  (`apps/web/src/components/chat/optimisticUserMessages.ts:9-46,133-165`).
- The Working indicator's local state is cleared by *comparing server state to a snapshot taken at
  send* (latest run id/requestedAt/startedAt/completedAt, runtime status, latest user message id),
  not by the RPC resolving (`ChatView.logic.ts:987-1079`,
  `apps/web/src/components/chat/useLocalDispatchState.ts:26-36`).
- History: this used to be an effect + `setTimeout(0)` and cost an extra commit per send.
  UNVERIFIED https://github.com/pingdotgg/t3code/pull/25710

**Failure rollback** (`useComposerSend.ts:1329-1400`): restore the draft thread record (new
`threadId` if the server deleted the bootstrap thread), remove the optimistic row by id, restore
the composer from the send snapshot (text, images, files, terminal contexts, cursor at end) - but
only if the user has not already typed something new - set a thread error, `resetLocalDispatch()`,
and if the user navigated away show a toast with an "Open draft" action. Commands are idempotent
by `commandId` with durable receipts, so a retry does not double-send
(`docs/internals/overview.md:50-53`).

---

## 3. Techniques: what / where / why it feels good

All VERIFIED at the SHA unless marked.

### Optimistic UI and instant navigation

| # | What | Where | Why it feels good |
|---|---|---|---|
| O1 | Thread id is minted when the draft opens; message id at send; both travel to the server and become the durable ids | `hooks/useHandleNewThread.ts:365-366`; `chat/useComposerSend.ts:784,1237`; `client-runtime/src/operations/commands.ts:714-732` | No id remap on ack, so list keys, scroll anchors, drafts and URLs never change identity |
| O2 | "New thread" is a local draft + route push; server learns of it only at first send | `useHandleNewThread.ts:395-421` | Zero-latency new chat |
| O3 | One RPC creates thread + message + run + (optional) worktree; ack = intent committed | `commands.ts:686-733`; `ThreadLaunchService.ts:981-998` | One round trip; no multi-step waterfall with partial-failure states |
| O4 | Same view instance across draft -> thread (layout owns the view; route components are `null`; handoff key) | `ThreadRouteView.tsx:28-33,61-76,170-186`; `routes/_chat.draft.$draftId.tsx:3` | No remount, no lost composer focus, no scroll reset at the moment the reply starts |
| O5 | Promotion waits for the hero->docked composer transition before swapping routes | `composerDraftStore.ts:4575-4583` | The route swap never interrupts an in-flight animation |
| O6 | Optimistic rows reconciled during render against *visible* server turn items | `ChatView.tsx:632-646`; `ChatView.logic.ts:1011-1021`; `optimisticUserMessages.ts:28-46` | No frame with zero or two copies |
| O7 | Local dispatch snapshot drives Working row/timer; acknowledged by state comparison | `useLocalDispatchState.ts:11-66`; `ChatView.logic.ts:987-1079`; `ChatView.tsx:1436-1445` | The "Working for 0s" row and Stop button appear in the same frame as Enter |
| O8 | Blob preview handoff waits for server image preload | `optimisticUserMessages.ts:133-180` | Attached images never blink when the URL changes |
| O9 | Rollback restores a full send snapshot unless the user typed again | `useComposerSend.ts:1340-1363` | Failure never loses input or clobbers new input |
| O10 | Queued messages are optimistic too, shown in the queue control, deduped by acknowledged message ids | `QueuedRunsControl.tsx:107,127-129` | Queue/steer feels as instant as send |

### Streaming

| # | What | Where | Why |
|---|---|---|---|
| S1 | Paragraph-gated delivery: only finished blocks, min 400 ms apart; token mode retired | `assistantStreaming.ts:15-110`; `contracts/src/settings.ts:1106,1351-1353`; `serverSettings.ts:352-364` | No half-word/half-fence repaints; ~1 layout change per paragraph |
| S2 | 50 ms adapter coalescer emitting full-text snapshots, first chunk immediate | `Adapters/ProviderTextDeltaCoalescer.ts:99-130`; `provider-acp/src/server/adapter.ts:147-151` | Bounded event rate regardless of provider chattiness; avoids O(n^2) output |
| S3 | Per-subscriber "latest wins per entity" 50 ms window + ACK-gated delivery + retained byte budget; overflow => client resumes from last sequence | `ThreadLiveEventCoalescer.ts:18-19,52-77`; `LiveStreamBudget.ts:18-19,67-84` | A slow client or network never builds an unbounded backlog; it skips to the latest state |
| S4 | One store write per transport batch; no-op batches keep object identity | `client-runtime/src/state/threads.ts:592-605,1093` | No render per event |
| S5 | "Quiet" atoms: thread structure, visible items, and shell hold their previous object while only streamed text / timestamps change; only the streaming row subscribes to its own text | `client-runtime/src/state/threadDetail.ts:281-305,354-378`; `threadShell.ts:422`; `apps/web/src/state/entities.ts:156-163,254` | Chat chrome, sidebar, timeline container do not re-render during a stream |
| S6 | Replacement-index hint (WeakMap keyed by previous+next arrays) so derived lists patch one index instead of rescanning | `client-runtime/src/state/orchestrationV2Projection.ts:23-51`; `threadDetail.ts:173` | O(1) derivation per streamed update on long threads |
| S7 | Incremental timeline entries and rows derived from the previous projection | `hooks/useIncrementalTimelineEntries.ts:20-37`; `chat/useTimelineRows.ts:38-60`; `session-logic.ts:859-935` | Row objects keep identity -> memoized rows skip |
| S8 | Streaming Markdown split at block boundaries; each block its own memoized renderer; only the last block re-parses; bails to whole-text when cross-block syntax exists | `chat/markdownStreamBlocks.ts:1-47`; `ChatMarkdown.tsx:1699-1726,1739-1746,1786-1800` | Cost per update is constant in reply length |
| S9 | Rendered-element LRU keyed by source text (500k chars, about 10 MB), skipped for the still-growing block | `chat/markdownRenderer.ts:22-44,85-108` | Thread switch and virtualized remounts reuse React elements, no re-parse |
| S10 | Incremental Markdown parser: caches the parsed prefix up to the last closed fence, parses only the suffix | `markdown-incremental.ts:33-80` | Long replies with big code blocks stay cheap in the fallback path |
| S11 | Stream tokenizer for code: completed line nodes keep identity while the last line changes; finished blocks cached as HTML (500 entries / 50 MB) | `lib/incrementalHighlighting.ts:57-107`; `chat/MarkdownCodeBlock.tsx:32-37,283-309` | Code blocks do not re-highlight or re-create DOM per update |
| S12 | Code block reserves its space with an `invisible` `<pre>` Suspense fallback until highlighted | `ChatMarkdown.tsx:1604-1617` | No unstyled-then-styled flash and no height jump |
| S13 | WASM "highlights" lexer preferred over Shiki ("10-40x faster ... never backtrack"); both themes loaded so appearance switch needs no new highlighter | `lib/syntaxHighlighting.ts:17-34` | No long tasks from regex highlighting |
| S14 | Diffs render in a worker pool sized `clamp(cores/2, 2, 6)`, AST LRU 240, 30 s idle TTL, 1,000-char tokenize cap | `components/DiffWorkerPoolProvider.tsx:20-25,64,69` | Diff panel never blocks the main thread |
| S15 | Thread cache persistence deferred until the run settles | `threads.ts:113-125,453-466` | No IndexedDB encode per streamed update |

### Timeline, scroll, and layout stability

| # | What | Where | Why |
|---|---|---|---|
| T1 | `@legendapp/list` with `estimatedItemSize={90}`, `maintainVisibleContentPosition {data,size}`, `maintainScrollAtEndThreshold={1}`, and `[overflow-anchor:none]` (the list owns anchoring, not the browser) | `chat/MessagesTimeline.tsx:820-823,1288-1332` | One owner for scroll correction; no fights with native scroll anchoring |
| T2 | Follow-the-end is `animated: true` only while working, not reduced-motion, and not while a newly shown thread settles; otherwise instant | `MessagesTimeline.tsx:192-205,1305-1315` | Live edge "glides" during a run, but thread open snaps with no visible scroll |
| T3 | Item type buckets by `log2(text.length/1000)`; very long messages get a unique type | `MessagesTimeline.tsx:1396-1404` | Size estimates for unmeasured rows are not polluted by outliers (they cite nested-update loops) |
| T4 | New-turn anchoring: the just-sent user message is pinned 24 px from the top with reserved "anchored end space", armed before the run exists; released when tool/reasoning activity needs the end | `chat/timelineScrollAnchoring.ts:38`; `chat/useTimelineFollow.ts:412-427,497-531`; `packages/shared/src/chatList.ts:12-42`; `MessagesTimeline.tsx:807-819` | The reply grows downward from a fixed point instead of pushing the prompt around |
| T5 | Follow state is a small state machine driven by *input events* (wheel/touch/scrollbar pointer/keys) through one viewport signal; programmatic scrolls take a "hold" so they are not mistaken for the user | `chat/timelineViewport.ts:36-61,70-131`; `useTimelineFollow.ts:94-131` | Distinguishes "user scrolled up" from "content shrank and the browser clamped" |
| T6 | Scroll-to-bottom pill: showing is debounced 150 ms, hiding is immediate | `useTimelineFollow.ts:65-68` | No pill flash during thread switches or initial settle |
| T7 | Per-thread remembered position (row id + offset within row + disclosure state), LRU of 100, restored in `useLayoutEffect` before paint | `timelineScrollAnchoring.ts:94-130`; `MessagesTimeline.tsx:711-740,886-913` | Switching back lands exactly where you were, including expanded tool groups |
| T8 | Content-size reactions coalesced to one rAF, keyed by the laid-out data identity; lane width measured only on viewport/setting change ("measuring per content frame would force a layout on every chunk") | `MessagesTimeline.tsx:956-958,1114-1133` | No forced layout per streamed chunk |
| T9 | First and last message rows always mounted (screen-reader heading navigation); playing audio / fullscreen rows pinned via `alwaysRender` | `MessagesTimeline.tsx:774-794` | Virtualization without breaking a11y or media |
| T10 | Empty/loading thread renders a blank surface with `data-timeline-loading`, plus a one-line "Loading messages..." / "Syncing messages..." status that fades in over 150 ms; no skeleton, no spinner | `MessagesTimeline.tsx:1249-1256`; `chat/ThreadNow.tsx:18-25,70-77`; `threadSync.ts:5-27` | Nothing to flash for the common sub-100 ms case |
| T11 | Delayed status helper: show only after 400 ms, then hold at least 400 ms | `packages/client-runtime/src/delayedStatus.ts:1-60` | Statuses never flicker in and out |

### Speed (startup, switching, sessions)

| # | What | Where | Why |
|---|---|---|---|
| P1 | Warm resume: cached snapshot seeds `afterSequence`, so reopening replays only missed events; resume falls back to a snapshot when the replay would exceed 128 events / 1 MiB | `threads.ts:252-268,1057-1074`; `docs/internals/performance-regressions.md:31-34` | Thread switch is a delta, not a download |
| P2 | In-memory resume cache (state + cursor) with 5-minute idle TTL; live subscription itself has idle TTL 0 | `threads.ts:1122-1150`; `threadRetention.ts:1` | Back navigation is instant without holding sockets open |
| P3 | Cold open bounded to about 1 MiB / 10 user turns / 500 rows; older history paged over HTTP and merged "without disturbing the live scroll window"; tool bodies and command output omitted at the wire | `docs/internals/performance-regressions.md:15-44` | First paint cost independent of thread length |
| P4 | Cold open over the socket when the server pages snapshots (skips an HTTP round trip), HTTP as fallback to tell "missing" from "failed" | `threads.ts:989-1056` | One fewer round trip on open |
| P5 | Client cache in IndexedDB; cached projections readable offline and never overwrite newer live data | `apps/web/src/connection/storage.ts:132-140`; `docs/internals/connection-runtime.md:73-76` | App paints real content before the socket is up |
| P6 | Desktop only: every running thread's detail stream is kept mounted, so opening a running thread needs no replay | `apps/web/src/state/threads.ts:82-133` | The threads you are most likely to open are already live |
| P7 | No hover prewarm of thread detail; sidebar rows lease live status only within 160 px of the viewport | `apps/web/src/components/Sidebar.logic.ts:76-99` | Explicit decision: "Each prewarm retains growing thread detail and a live subscription, multiplying heap and server load" |
| P8 | Provider sessions (processes) are reused and released after 30 minutes idle (pinned up to 4 h by background work) | `apps/server/src/orchestration-v2/ProviderSessionManager.ts:59-60` | Second and later turns skip process spawn. I found **no** provider prewarm on draft open (searched `prewarm|warmUp|warmup` in `apps/server/src`: no hits) |
| P9 | Worktree provisioning in a forked fiber after ack, with progress events and cancellation by run interrupt | `ThreadLaunchService.ts:386-464,789-833` | Thread appears and is interactive while git works |
| P10 | Shell (sidebar) snapshot contains only active rows, the newest 25 settled threads, and no idle subagent children; resume sends deltas | `docs/internals/connection-runtime.md:78-92`; `docs/internals/performance-regressions.md:52-55` | Startup cost independent of history size |
| P11 | Per-streaming-batch code paths are deliberately untraced (`Effect.fnUntraced`) | `threads.ts:504-505,664`; `ThreadLiveEventCoalescer.ts:100` | Observability does not tax the hot path |
| P12 | Handlers in huge long-lived components are plain functions wrapped by `useStableCallback` only where they leave the component, to stop V8 closure scopes from retaining old renders | `docs/internals/web-ui.md:41-55` | Memory stays flat over long sessions |

---

## 4. Animation and in-progress affordances (exact numbers)

All from `apps/web/src/index.css` at the SHA unless noted.

### 4.1 Working / thinking shimmer (the "focus sweep")

Used by the Working row (worktree setup, compaction), live tool/thinking rows, the worktree setup
card and the compaction divider. Structure: base label in muted color; an absolutely positioned
overlay containing the *same label* in `text-foreground`, revealed through a moving mask.

```css
/* index.css:586-600 */
@keyframes live-activity-focus         { from { transform: translateX(0) } to { transform: translateX(100%) } }
@keyframes live-activity-focus-counter { from { transform: translateX(0) } to { transform: translateX(-100%) } }
```

```css
/* index.css:604-636 (abridged) */
@utility live-activity-focus {
  --live-activity-focus-width: 4.5rem;
  left: calc(-1 * var(--live-activity-focus-width));
  width: calc(100% + 2 * var(--live-activity-focus-width));
  mask-image: linear-gradient(to right, transparent 0, rgb(0 0 0 / 12%) 0.675rem,
    rgb(0 0 0 / 55%) 1.575rem, black 2.25rem, rgb(0 0 0 / 55%) 2.925rem,
    rgb(0 0 0 / 12%) 3.825rem, transparent 4.5rem, transparent 100%);
  animation: live-activity-focus 2.2s steps(30) infinite;
  animation-play-state: var(--visible-animation-state, paused);
  will-change: var(--visible-animation-will-change, auto);
}
```

- Duration **2.2 s**, timing **`steps(30)`** (about 13.6 position updates per second on any
  refresh rate), infinite. The outer span translates +100% while the inner counter span translates
  -100% (same timing), so the text stays still and only the 4.5 rem soft-edged mask window moves.
  Transform-only, so it stays on the compositor.
- Mask profile: 0 -> 12% at 0.675 rem -> 55% at 1.575 rem -> 100% at 2.25 rem (center) -> mirrored.
- Reduced motion or forced colors: `animation: none; opacity: 0` (overlay disappears, base label
  stays). `index.css:638-642`.
- Markup: `apps/web/src/components/chat/TimelineRowParts.tsx:44-55`; callers
  `chat/TimelineWorkGroupRows.tsx:278-310,389-426`, `chat/MessagesTimeline.tsx:1554-1559`.
- Why steps: PR #18191 moved it from `linear` to `steps(30)` after measuring 45-53 fps of drawing
  for one word on a 60 Hz display. UNVERIFIED https://github.com/pingdotgg/t3code/pull/18191

### 4.2 Running tool label shine (text-clip variant)

```css
/* index.css:551-578 (abridged) */
@keyframes live-tool-shine {
  from { background-position: -4.5rem 0, 0 0; }
  to   { background-position: calc(100% + 4.5rem) 0, 0 0; }
}
@utility live-tool-shine {
  @media (prefers-reduced-motion: no-preference) and (forced-colors: none) {
    color: transparent;
    background-image:
      linear-gradient(to right, transparent, var(--contrast-foreground), transparent),
      linear-gradient(var(--contrast-secondary-label), var(--contrast-secondary-label));
    background-size: 4.5rem 100%, 100% 100%;
    background-repeat: no-repeat;
    background-clip: text;
    animation: live-tool-shine 2.2s steps(30) infinite;
    animation-play-state: var(--visible-animation-state, paused);
  }
}
```
Same 2.2 s / `steps(30)` rhythm so both shimmers stay in phase. Applied at
`chat/TimelineWorkGroupRows.tsx:478`.

### 4.3 Offscreen / background pausing

`observeVisibleAnimation` is a ref callback. One shared `IntersectionObserver`, one
`visibilitychange` listener, one `prefers-reduced-motion` listener; it writes
`--visible-animation-state: running|paused` and `--visible-animation-will-change: transform|auto`
on the element. Default is `paused`, so an animation that never gets observed never runs.
`apps/web/src/lib/visibleAnimation.ts:10-18,24-47`. Used by spinners too
(`components/ui/spinner.tsx:32`, `index.css:580-584`).

### 4.4 Skeleton and status pulses

```css
/* index.css:295-313 */ --animate-skeleton: skeleton 2.4s infinite;   /* :223 */
@keyframes skeleton {
  0%, 42%  { opacity: 1;    animation-timing-function: steps(4); }
  50%, 92% { opacity: 0.55; animation-timing-function: steps(4); }
  100%     { opacity: 1; }
}
```
Their comment: one opacity pulse per *container*, stepped, "which on a 120Hz display is the
difference between ~14 and ~288 updates". Skeletons are rare in the chat path (only "Loading work
history" inside an expanded deferred tool group, `MessagesTimeline.tsx:1604-1618`).

```css
/* index.css:332-346 */ --animate-status-pulse: status-pulse 2s infinite;   /* :226 */
@keyframes status-pulse {
  0%, 40%  { opacity: 1;   animation-timing-function: steps(6); }
  50%, 90% { opacity: 0.5; animation-timing-function: steps(6); }
  100%     { opacity: 1; }
}
/* index.css:347-360 */ --animate-status-ping: status-ping 2s infinite;      /* :227 */
@keyframes status-ping {
  0%        { opacity: 0.9; scale: 0.75; animation-timing-function: steps(8); }
  40%, 100% { opacity: 0;   scale: 2; }
}
```
Sidebar "working" dots use `motion-safe:animate-status-pulse`
(`components/ThreadStatusIndicators.tsx:563,627,649`).

### 4.5 Streamed block fade-in

```css
/* index.css:2208-2218 */
@media (prefers-reduced-motion: no-preference) {
  .chat-markdown[data-streaming] > *,
  .chat-markdown[data-streaming] .chat-markdown-shiki,
  .chat-markdown[data-streaming] .chat-markdown-mermaid {
    transition: opacity 600ms ease-out;
    @starting-style { opacity: 0; }
  }
}
```
Opacity only, one shot, no layout change. `@starting-style` fires only on insertion and the rule
is gated on `data-streaming`, so opening a finished thread never replays it. This is what makes
paragraph-at-a-time delivery read as smooth rather than chunky.

### 4.6 Timers

- "Working for 12s", subagent elapsed, etc. are written **directly to `textContent`** by a ref
  callback subscribed to a shared tick; React never re-renders for a tick.
  `apps/web/src/lib/tickTextContent.ts:8-18`, `chat/AgentElapsed.tsx:41-48`.
- The tick is one `setTimeout` chain aligned to wall-clock second boundaries
  (`periodMs - (Date.now() % periodMs)`) "so every subscriber updates in the same frame instead of
  each drifting on its own phase"; it stops when nobody subscribes.
  `packages/client-runtime/src/clock.ts:15-65`.
- `tabular-nums` on every timer so digits do not shift width
  (`chat/TimelineWorkGroupRows.tsx:296`, `AgentElapsed.tsx:47`).
- Provider turn duration is recorded separately from run finalization so a late checkpoint/diff
  does not extend the displayed duration (`docs/internals/overview.md:79-86`).

### 4.7 Other small timings

| Thing | Value | Where |
|---|---|---|
| Thread status line appear | `transition-opacity duration-150 ease-out starting:opacity-0` | `chat/ThreadNow.tsx:24` |
| Scroll-to-end pill show debounce | 150 ms (hide immediate) | `chat/useTimelineFollow.ts:68` |
| Delayed status show / min visible | 400 ms / 400 ms | `client-runtime/src/delayedStatus.ts:1-2` |
| Anchor offset from top for a new turn | 24 px web (16 px shared default) | `chat/timelineScrollAnchoring.ts:38`; `packages/shared/src/chatList.ts:1` |
| Settings search target pulse | `650ms ease-in-out 2` | `index.css:935` |
| Preview loading bar | `5.3s cubic-bezier(0.1, 0.5, 0.2, 1) forwards` | `index.css:2404` |
| Stash pulse | 1400 ms (hold 1.2 s then 200 ms ease; count uses `step-start`) | `index.css:228-232` |

### 4.8 Work log, plans, questions, queue

- Tool calls are grouped into work groups with a live header row; a long group is its own nested
  virtual list capped at `min(18rem, 50dvh)` plus the measured expanded content
  (`chat/TimelineWorkGroupRows.tsx:255-271`). Large groups can be *deferred*: the row arrives
  without bodies and loads them on expand, with a retry affordance
  (`MessagesTimeline.tsx:1565-1602`, `docs/internals/performance-regressions.md:15-21`).
- Completed turns fold ("turn-fold" rows); expanding preserves scroll position via a work-group
  scroll anchor (`MessagesTimeline.tsx:890`, PR #29166 UNVERIFIED
  https://github.com/pingdotgg/t3code/pull/29166).
- Questions: the composer is replaced by a pending-input panel; single-select answers are applied
  optimistically in local state before the server confirms
  (`chat/ComposerPendingUserInputPanel.tsx:88-113`). `runtime-request.respond` commits the
  resolution and the resulting user message in one transaction and is idempotent
  (`docs/internals/providers.md:124-129`).
- Queue/steer: queued messages are server-side runs in status `queued`; the queue control merges
  server queued runs with optimistic `queued_turn` messages, and each row can be promoted to a
  steer or sent now (`chat/QueuedRunsControl.tsx:99-129,297`; server
  `Orchestrator.ts:600,5201-5203`).

---

## 5. Jank they hit and how they fixed it

Code state VERIFIED where a path is given; the narrative and measurements are UNVERIFIED (PR
bodies, URL pattern `https://github.com/pingdotgg/t3code/pull/<n>`).

| Symptom | Root cause | Fix | Evidence |
|---|---|---|---|
| "Responses take too long to appear" vs. token mode's "half-word repaints" | Buffered whole-message delivery, or one event per token | Paragraph streaming (finished blocks, 400 ms), 600 ms block fade, glide-to-end, hidden-until-highlighted code | PR #11062; `assistantStreaming.ts`; `index.css:2203-2218` |
| A long reply got slower with every delta | Whole reply re-parsed and every element re-rendered per update | Block splitting + memoized per-block renderer. PR reports script time 14.8 s -> 4.4 s on a 50 KB / 500-update bench, and one paragraph render per delta (465 -> 30) | PR #27417; `markdownStreamBlocks.ts` |
| Every visible row re-rendered per delta | A Map built before hook calls and captured after them; React Compiler cannot memoize across a hook call, so the shared row context got a new identity each render | Build the value right before its consumer; a render-count test now asserts 0 history-row renders while streaming (210 before) | PR #27420; `docs/internals/web-ui.md:66-70` |
| ChatView re-rendered once per delta | It subscribed to the live projection | `quietThreadAtom` / `quietVisibleTurnItemsAtom` | PR #23117; `threadDetail.ts:281-305` |
| ~5 React commits per delta in the timeline | Visible items changed identity each delta, list ran extra layout commits | `timelineVisibleTurnItemsAtom` + per-row `assistantMessageTextAtom`; commits per delta 5 -> 2 | PR #27419; `threadDetail.ts:354-378` |
| 1,942 renders in 12 s of chat chrome while streaming | Server bumps shell `updatedAt`/counts several times a second | `quietThreadShellAtom` ignores those fields while a run is active | PR #20860; `threadShell.ts:422` |
| Thinking shimmer drew 45-53 fps for one word | `linear` timing | `steps(30)` | PR #18191; `index.css:634,647` |
| New worktree thread flickered (gray block, Working rows dropped, two jumps) | An effect keyed on `draftId` reset the in-flight send when the draft promoted; send counted as acknowledged from the summary before detail loaded; setup card changed layout mid-flight | Reset local send state only on thread change; keep send in progress while either detail or summary reports a starting run; keep the running layout until the turn starts | PR #15019; `ChatView.logic.ts:1023-1079` |
| Extra commit per send; previews lingering | Optimistic removal in an effect with `setTimeout(0)` | Derive-in-render reconciliation | PR #25710; `ChatView.tsx:632-646` |
| Thread switch forced a full layout | Active sidebar row called `scrollIntoView` mid-commit; composer measured wrap from a MutationObserver; parse cache off for streaming messages | Skip reveal when the row has focus; measure at most once per frame; cache finished blocks | PR #27823; `ChatMarkdown.tsx:1798-1799` |
| Scroll-to-bottom pill flicker / jumping | `isAtEnd=false` events while initial scroll settles | 150 ms show debounce, immediate hide | PRs #1692, #26375; `useTimelineFollow.ts:65-68` |
| 2,552-line patch + custom follow/repin/reflow code on Legend List 3.3.5 | Library gaps | Upgrade to 3.7.2 (`isFollowingEnd`, `anchorVisibleContent`, `moveBefore` row moves), delete workarounds (+488/-3,878) | PR #28125; remaining patch is RN-only: `patches/@legendapp__list@3.7.2.patch` |
| Fade animation restarted when the list reordered rows | DOM reorder re-inserted nodes | List uses `moveBefore` | PR #11062, #28125 |
| Superseded full-prefix updates on the wire | Bursts within the coalescing window | Drop superseded updates per entity. PR reports serialized bytes -49.9% at 25 ms cadence, -79.8% at 10 ms | PR #24267; `ThreadLiveEventCoalescer.ts:52-77` |
| Cache writes during streaming | Snapshot persisted per update | Persist after the run settles; sliding queue | PR #12109; `threads.ts:453-466` |
| Mobile screen froze with looping shimmer | Reanimated commits each frame to the shadow tree and starves React commits | RN `Animated` with native driver for on-screen loops | `docs/internals/performance-regressions.md:134-145` |

**Open rough edges (GitHub issues, UNVERIFIED, `https://github.com/pingdotgg/t3code/issues/<n>`)**

- #20366 chat snaps back to the bottom while a turn runs when scrolled up (a regression; the
  follow state machine is clearly still the hardest part).
- #24120 streaming thinking and plan text rebuilds the whole chat timeline about 20 times a
  second (the quiet-atom treatment covers assistant text, not reasoning; PR #27419 says
  "Reasoning items are unchanged").
- #23365 paragraph streaming stores every full reasoning snapshot as a permanent event
  (reported: 93k rows, 2.2 GB in one day on OpenCode). This is the cost of whole-entity
  replacement events in an event store.
- #28175 paragraph mode withholds the final paragraph while a turn stays open on background tool
  calls (the hold-until-boundary rule has a tail-latency failure mode).
- #28996 baseline checkpoint capture blocks the provider turn; slow workspaces wait 30 s per turn.
- #22350 iPhone: scrolling up a long thread jerks backwards (LegendList `scrollBy` corrections
  applied twice during momentum).
- #12689 Windows desktop typing lag even in empty chats; #23041 Windows conversation pane blank
  until switching away and back.
- #25692 zooming can break scroll-follow; #12943 text selection jumps between messages while
  scrolling (virtualization recycling).
- #7766 flickering sidebar; #30197 working threads flicker in and out during a remote server
  update.

---

## 6. Ranked recommendations

Ranking is by expected effect on perceived quality per unit of work. "Native" = Swift 6.2 /
SwiftUI + AppKit ACP client; "Web" = the React surface.

### 6.1 Native Swift/AppKit ACP client

1. **Gate assistant text on finished Markdown blocks, and coalesce at the ACP boundary.**
   Port `splitBufferedAssistantText` (about 55 lines of line-scanning, no parser) and the
   two-stage timing: 50 ms accumulate at the adapter, >=400 ms between visible updates, flush
   immediately on completion. If the owner wants a "live" feel beyond T3's, show finished blocks
   as immutable laid-out fragments and render only the unfinished tail as plain text (no Markdown
   re-parse) - still one stable prefix, one mutable tail. Either way the transcript's layout
   changes about once per paragraph instead of 20-60 times a second, which removes most
   `NSTextView`/TextKit re-layout and scroll-stutter risk. Watch T3's known failure mode (#28175):
   flush the held tail when the model pauses on a tool call, not only on a block boundary.
2. **Mint ids on the client and send one launch command.** Session id when the new-chat view
   opens; message id at Enter; a single idempotent "create session + first prompt + workspace
   strategy" command with a client command id. Never remap an id after ack. Keep the *same*
   transcript view/controller instance across "draft" -> "live session"; in SwiftUI that means the
   view identity must not be derived from "has server session".
3. **Reconcile optimistic rows inside the same state transaction that applies the server row.**
   Key both by message id. Evict the optimistic entry in the reducer that makes the server's user
   item *visible in the transcript* (not when the RPC returns, not in a follow-up `Task`/
   `DispatchQueue.main.async`). That is the whole "no flash" trick.
4. **Drive "working" from a local dispatch snapshot.** At Enter record
   `(startedAt, latestRunId, latestUserMessageId, runtimeStatus)`; show the Working row, timer and
   Stop immediately; clear it only when observed server state differs from the snapshot. Do not
   reset it on view-identity changes (their worktree flicker bug, PR #15019).
5. **Split observation into structure vs. streamed text.** One `@Observable` per message for its
   text; the transcript's row list, header, sidebar row and composer observe only structural
   state (which items exist, statuses, counts crossing zero). Ignore `updatedAt`-style fields
   while a run is active. Add a debug render counter and assert "0 history-row updates per
   streamed delta" in a test, as they do.
6. **Duty-cycle every looping animation and pause it when not visible.** Shimmer: 2.2 s sweep,
   30 discrete steps, 4.5 rem soft mask window over a foreground-colored copy of the label. In
   Core Animation: a `CAGradientLayer` mask with a `CAKeyframeAnimation` on `locations` or
   `position`, `calculationMode = .discrete` with 30 keyframes (or a 15 Hz display link), so a
   120 Hz ProMotion display is not asked for 120 frames a second. Pause on
   `NSWindow.occlusionState`, app deactivation, row scrolled out, and Reduce Motion (hide the
   overlay, keep the label). Status dots: 2 s cycle, opacity 1 <-> 0.5 with long holds.
7. **Take worktree creation off the ack path.** Commit session + message + run in a `preparing`
   state, ack, then provision in the background with progress events (`worktree` -> `setup
   script`) and cancellation tied to run interrupt. Show the user message at once with a
   "Setting up worktree..." shimmer label in the Working row. Run title generation and branch
   naming concurrently and patch them in.
8. **Make timers free.** One wall-clock-aligned 1 Hz tick shared by all elapsed labels, writing
   straight to the text layer / `stringValue` (no SwiftUI invalidation above the label),
   monospaced digits.
9. **Thread switch from caches, not prewarm.** Reuse one transcript view; remember per-session
   scroll position as (row id, offset within row, disclosure state) in an LRU (they keep 100);
   restore before first paint. Cache parsed/laid-out Markdown per block keyed by source text
   (they budget about 10 MB) and highlighted code (500 entries / 50 MB). Keep a 5-minute
   in-memory snapshot + event cursor per session and replay only the delta; bound cold opens to
   roughly the last 10 user turns / 1 MiB and page older history upward without touching the live
   end. Keep *running* sessions' streams subscribed in the background (their desktop keep-alive);
   do not prewarm on hover.
10. **Scroll: one owner, input-driven follow.** A three-state machine (following end /
    anchoring new turn / free scrolling) that changes state on user input events (wheel, scroller
    drag, keys), never on raw offset changes; wrap programmatic scrolls in a "hold". On send, pin
    the new user message about 24 pt from the top with reserved space below, release when tool
    activity needs the end. Animate follow-to-end only while a run is active; snap on session
    open. Debounce showing the jump-to-end button by 150 ms, hide immediately.
11. **Keep tool output out of the transcript event stream.** Status + bounded ids in the
    transcript; live output on a separate, throttled, subscribe-while-expanded channel with a
    bounded tail. Defer large tool-group bodies until expansion.
12. **Statuses: 400 ms show delay, 400 ms minimum visible; loading = blank surface + one fading
    status line.** No skeleton or spinner on the session-open path.
13. **Do not persist while streaming.** Snapshot to disk when the run settles, through a
    latest-wins single-slot queue.
14. **Reuse agent processes with an idle timeout** (theirs: 30 min, pinned up to 4 h by background
    work). T3 does *not* prewarm a provider session when a draft opens; if "fast first token"
    matters more to Memex than idle resource use, prewarming the ACP process on composer focus is
    a place to beat them, but it must not trigger MCP startup, hooks, or login as a side effect
    (their rule: `docs/internals/providers.md:52-57`).

### 6.2 React web/mobile surface

1. Copy the Markdown pipeline as-is: `splitMarkdownBlocks` + per-block `memo` + element LRU
   (cache every block except the growing last one) + `data-streaming` / `@starting-style`
   600 ms opacity fade + invisible-`<pre>` Suspense fallback for code.
2. Use `@legendapp/list` >= 3.7.2 with their props: `maintainVisibleContentPosition {data,size}`,
   `maintainScrollAtEnd` with `animated` only while working, `maintainScrollAtEndThreshold={1}`,
   `overflow-anchor: none`, `getItemType` bucketing by text length, `anchoredEndSpace` for the
   new-turn pin, `alwaysRender` for first/last message.
3. Split selectors/atoms into quiet structure vs. per-row text; apply each transport batch as one
   store write and return the same object for no-op batches.
4. Optimistic rows keyed by client message id, reconciled during render; local dispatch snapshot
   for the working state; one layout-owned chat view across draft and thread routes.
5. Use the stepped keyframes in section 4 verbatim, with the `--visible-animation-state`
   IntersectionObserver pause and `prefers-reduced-motion` fallbacks.
6. Timers via `textContent` on a shared aligned tick.
7. Turn on React Compiler and add (a) a test that fails on any compiler bailout and (b)
   render-count assertions for the stream scenario. Their biggest regressions were silent memo
   losses.
8. Persist drafts locally (Zustand `persist`), thread snapshots in IndexedDB after settle, and
   resume with `afterSequence`.
9. If a server sits between the web surface and the agent: coalesce per subscriber with
   latest-wins and an ACK-gated byte budget, and enable `permessage-deflate` with context
   takeover.

### 6.3 Things not to copy blindly

- **Whole-entity replacement events persisted to the event store.** Simple and resume-safe, but it
  produced #23365 (GBs of reasoning snapshots). If Memex persists a transcript log, store deltas
  or only the final text for streaming items and keep full-text snapshots for the wire only.
- **Paragraph gating with no tail.** It trades first-visible-text latency for stability (their own
  PR reports first paragraph at about 3 s). For "fast first token" consider the stable-prefix +
  plain-text-tail hybrid in 6.1 item 1.
- **Scale of the timeline code.** `MessagesTimeline.tsx` + `.logic.ts` + `useTimelineFollow.ts`
  are about 4,900 lines and still have an open snap-to-bottom regression (#20366). Budget real
  time for scroll-follow; it is the least "solved" area even for them.

---

## Method

- `git clone --depth 1`; read-only inspection with `grep`/`sed`/file reads; no install, build or
  execution of repo code. `AGENTS.md` in the repo was treated as documentation, not instructions.
- PR/issue history through `gh pr list --search ... in:title`, `gh pr view`, `gh issue list` on
  `pingdotgg/t3code`. Shallow clone means commit history was not inspected locally.
- Not verified: any runtime behaviour or measurement (all performance numbers are from PR
  bodies); mobile (`apps/mobile`) internals; Electron startup path; relay/T3 Connect.
