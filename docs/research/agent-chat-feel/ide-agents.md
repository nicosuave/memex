# IDE agent chat UIs: Cline, Roo Code, Kilo Code, Continue, Goose desktop

Research date 2026-11-09. Read-only source study of shallow clones plus `gh` PR/issue history.

## Sources and labels

| Project | Repo | SHA (depth 1) | Notes |
|---|---|---|---|
| Cline | `cline/cline` | `1e8ae47d60051c5e9a9bd0eed311ef209fdeb347` | VS Code webview now lives at `apps/vscode/webview-ui`; host is an "SDK" bridge. A Tauri desktop app also exists (`apps/examples/desktop-app`, `sdk/packages/ui`). |
| Roo Code | `RooCodeInc/Roo-Code` | `b867ec9145750d0ae1ff7f02d35406e9bf2a0b16` | Repo is archived (last push 2026-05-15). Final state of a mature codebase. |
| Kilo Code | `Kilo-Org/kilocode` | `b6a49d01c10d7e26c42a8a14b3441b7a1856c04c` | No longer a Roo fork: OpenCode-based monorepo, SolidJS webview, `virtua` virtualizer. Differs materially, so covered. |
| Continue | `continuedev/continue` | `5522c6f44ca0ac3528b37244818fbfa39b5af470` | GUI in `gui/`. |
| Goose | `aaif-goose/goose` (redirect from `block/goose`) | `f56ba84dc7fe453d7076bb057d90b56e6371cc7b` | Desktop in `ui/desktop`, now ACP-driven (`ui/desktop/src/acp`). |

Clones: `/private/tmp/claude-501/-Volumes-Dev-Code-memex--claude-worktrees-agent-app-ux-patterns-13a76b/f8deb1e4-a002-4452-a81e-c8ef8a51001b/scratchpad/repos/ide-{cline,roo,kilo,continue,goose}`.

Labels: **VERIFIED** = I read the cited `path:line` at the SHA above, or read the PR/issue via `gh`. **UNVERIFIED** = claim taken from a PR/issue description or third-party analysis that I did not confirm in code. Paths are relative to each repo root.

---

## 1. Cline

### 1.1 State pipeline

- Two channels over gRPC-style streams: full state snapshots (`subscribeToState`, JSON blob) and single-message partial updates (`subscribeToPartialMessage`). VERIFIED `apps/vscode/webview-ui/src/context/ExtensionStateContext.tsx:444,620`.
- Both channels feed one pure "convergent replica" reducer keyed by three host-stamped values: `ts` (identity), `seq` (freshness), `epoch` (conversation fence). Snapshots in the same epoch are merged per message and can never shrink the transcript; stale snapshots are dropped by `stateVersion`. VERIFIED `apps/vscode/webview-ui/src/components/chat/chat-view/messageReducer.ts:3-15,104-147,168-215`.

  ```ts
  //   - ts    : message identity / merge key
  //   - seq   : freshness within an epoch (higher seq = newer copy of the same ts)
  //   - epoch : conversation/replica fence (newer epoch replaces; older is dropped)
  ```
- Turn phase is a separate, seq-gated value (`TurnState {phase, anchorTs, seq}`), set explicitly by the host at lifecycle points instead of inferred from the message tail; a late snapshot carrying `idle` cannot revert `streaming`. VERIFIED `messageReducer.ts:77-85`, `apps/vscode/src/sdk/turn-state-tracker.ts:15-33`, `ExtensionStateContext.tsx:466-471`.
- A no-op reducer result returns the previous React state object (no rerender). VERIFIED `ExtensionStateContext.tsx:636-641`.
- Host coalesces full-state posts with a 50 ms trailing debounce plus in-flight/queued folding, because each post rebuilds the whole `ExtensionState` including task history. VERIFIED `apps/vscode/src/sdk/state-post-debouncer.ts:1-8,28-61`, `apps/vscode/src/sdk/SdkController.ts:236`.
- Per-event messages go through the partial stream; full state is pushed only on `ended`/`done`/`error`. VERIFIED `apps/vscode/src/sdk/webview-grpc-bridge.ts:69-88`.

### 1.2 Transcript rendering

- `react-virtuoso` with: `atBottomThreshold={10}`, `increaseViewportBy={{ top: 3_000, bottom: Number.MAX_SAFE_INTEGER }}`, `initialTopMostItemIndex={length - 1}`, `key={task.ts}`, `overflowAnchor: "none"`, footer reduced to a 1px spacer. No `followOutput`; follow is hand-rolled. VERIFIED `chat-view/components/layout/MessagesArea.tsx:194-266`. The infinite bottom overscan means everything below the viewport is always mounted (trade-off: Roo removed exactly this for memory, see 2.5).
- Follow logic (`useScrollBehavior`): a single `disableAutoScrollRef`. Set by wheel-up inside the container (`deltaY < 0`, passive listener) and by user row expansion; cleared when Virtuoso reports at-bottom. Triggers to re-pin: grouped row count change (smooth scroll, then instant settle at 40 ms and 70 ms), last-row height change (500 ms trailing settle), tail `ts` change, turn phase entering `streaming`, queued prompt added. VERIFIED `chat-view/hooks/useScrollBehavior.ts:134-155,216-261,270-314,322-331`, `MessagesArea.tsx:128-165`, `ChatView.tsx:357-384`.
- Row height observation: each `ChatRow` measures itself with `useSize`; only the last row reports, and the first measurement is skipped ("we already scroll for that"). Rows are `memo(..., deepEqual)`. VERIFIED `ChatRow.tsx:103-133`.
- Expand/collapse rules: expanding never scrolls and disengages follow; collapsing re-pins only if at bottom or the row is last/second-to-last; programmatic expansions pass `preserveAutoScroll`. VERIFIED `useScrollBehavior.ts:216-261`.
- Grouping: browser-session messages become one row; consecutive low-stakes tools (read/list/search) plus their reasoning and the `api_req_started` that produced them collapse into one tool group; checkpoints after such groups are dropped. `isApiReqAbsorbable` is evaluated at render time specifically to avoid a transient frame where the API-request row appears before grouping absorbs it. VERIFIED `chat-view/utils/messageUtils.ts:17-28,201,430-441,525-532`.
- Sticky "last user message" header is absolutely positioned "to avoid layout shifts". VERIFIED `MessagesArea.tsx:204-215`.

### 1.3 Streaming markdown, code, diffs

- Markdown is split into top-level blocks with `marked.lexer`, and each block is a `memo` keyed by index with a string-equality comparator, so only the growing tail block re-parses. VERIFIED `apps/vscode/webview-ui/src/components/common/MarkdownBlock.tsx:20-27,29,102-112`.
- Streaming diff row: line-number column is always reserved "to prevent layout shift during streaming"; inner scroller auto-follows while streaming with a programmatic-scroll flag cleared on next animation frame so its own scroll events are not read as user intent. VERIFIED `components/chat/DiffEditRow.tsx:73-100,236`.
- Command output: bounded-height box (`max-h-[75px]` collapsed, `200px` expanded) with its own bottom pin, expand notch only above 5 lines. VERIFIED `components/chat/CommandOutputRow.tsx:26-39,97-109`.
- Reasoning: capped at `max-h-[150px]` inner scroller that auto-scrolls, with top/bottom fade gradients instead of growing the row. VERIFIED `components/chat/ThinkingRow.tsx:40-47,89-118`.

### 1.4 In-progress affordances

- Shimmer, not spinners: gradient text clip animated via `background-position`, 5 s linear infinite. VERIFIED `apps/vscode/webview-ui/src/theme.css:131-139`, `ThinkingRow.tsx:72-77`.

  ```css
  --animate-shimmer: shimmer 5s infinite linear;
  @keyframes shimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }
  ```
- Activity text types in at 15-30 ms/char, then switches to shimmer. VERIFIED `components/chat/TypewriterText.tsx:4-35`, `RequestStartRow.tsx:229`.
- One "Thinking..." loader, rendered as an in-list synthetic row (not a footer), governed by `useThinkingLoaderRow`: authoritative on `turnState.phase === "streaming"`; hidden while a visible row is partial; shown below tool groups; suppressed for completion rows. VERIFIED `chat-view/hooks/useThinkingLoaderRow.ts:45-85`.
- Anti-flash rule: a 500 ms grace applies only to hidden-to-visible transitions triggered by the tail finishing streaming; it never hides an already-visible loader and is skipped when the finalized tail is reasoning. Visibility is derived during render, not in an effect, so the first frame is correct. VERIFIED `useThinkingLoaderRow.ts:14,171-215`.
- Empty-list fast path: on a brand new task the loader is painted as a plain element over the mounted-but-empty Virtuoso, because a cold virtualized list takes several frames to measure its first item. VERIFIED `MessagesArea.tsx:108-113,218-239`.

### 1.5 Interaction flow

- Optimistic send: an optimistic user bubble (`pendingUserMessage`, anchored by `afterTs`) is appended client-side and retired when a matching user-authored row arrives; an optimistic loader marker (`pendingResponse` with the observed `turnState.seq`) is retired when a fresher TurnState arrives; both roll back on RPC failure. VERIFIED `chat-view/utils/pendingResponse.ts:32-56`, `chat-view/hooks/useMessageHandlers.ts:165-197`, `ChatView.tsx:88-101`.
- Queue + steering: messages sent mid-stream become `QueuedPrompt` items shown in a footer card above the composer ("N queued, M steering"), each with a "Steer" badge and a per-item cancel with a local `cancellingIds` disabled state. Queueing re-engages bottom pinning because the footer grew without adding a row. VERIFIED `chat-view/components/layout/QueuedPrompts.tsx:18-28,41-54,78-96`, `ChatView.tsx:357-384`.
- Cancel is guarded by an in-flight ref to stop double sends. VERIFIED `useMessageHandlers.ts:550-565`.
- Approval buttons: one table maps each ask state to `{primaryText, secondaryText, primaryAction, secondaryAction, sendingDisabled, enableButtons}`. VERIFIED `chat-view/shared/buttonConfig.ts:15-22,28-60`.

### 1.6 Instructive fixes

1. [#11436](https://github.com/cline/cline/pull/11436) restore pin-to-bottom. VERIFIED (PR body). Scroll code was byte-identical; the data flow changed. Pinning had exactly two triggers (row count change, last-row height growth) and new content paths hit neither (synthetic loader row, tool merged into an existing group at constant length). Lesson: follow must be driven by rendered content geometry, not by enumerated data events.
2. [#12631](https://github.com/cline/cline/pull/12631) loader flash at turn end. VERIFIED (PR body + code). Two channels with different latency (fast partial stream, slow state post) produced a ~100 ms window where the loader's rule was true. Fixed with a targeted 500 ms grace.
3. [#12750](https://github.com/cline/cline/pull/12750) the grace itself caused disappear/reappear/disappear around tool calls; restricted to hidden-to-visible and skipped for reasoning tails. VERIFIED.
4. [#12746](https://github.com/cline/cline/pull/12746) show thinking immediately: four stacked delays (host did not post phase promptly, no optimistic marker, cold virtualizer measurement, effect-owned visibility costing one blank paint). VERIFIED.
5. [#11873](https://github.com/cline/cline/pull/11873) command output un-pinned the chat: programmatic auto-expand reused the user-expansion path, which sets "user scrolled away". VERIFIED.
6. [#9300](https://github.com/cline/cline/pull/9300) stabilise tool-group text and stop shimmer restarting from remount churn. VERIFIED (PR body).
7. [#9266](https://github.com/cline/cline/pull/9266) keep the Focus Chain placeholder mounted so the header area does not collapse and re-expand. VERIFIED (PR body).
8. [#8232](https://github.com/cline/cline/pull/8232) scroll-to-top needs a delayed second scroll (300 ms) under virtualization. VERIFIED (PR body).
9. [#12767](https://github.com/cline/cline/pull/12767) auto-scroll when queueing a message. VERIFIED (code at `ChatView.tsx:357-384`).
10. [#8264](https://github.com/cline/cline/pull/8264) "Chat Streaming UI" refactor (+3206/-1856). UNVERIFIED beyond title/size; body only links #8219.

### 1.7 Remaining complaints

- [#14450](https://github.com/cline/cline/issues/14450) (open, desktop app): chat flashes and scroll jumps during agent operations. VERIFIED (issue body).
- [#4780](https://github.com/cline/cline/issues/4780) "bouncy scroll" investigation: Virtuoso range instability when scrolling up in long tasks, scrollHeight oscillating (22847 -> 22037 -> 22847), whack-a-mole with `increaseViewportBy`. VERIFIED (issue body); root cause analysis itself UNVERIFIED.
- [#3434](https://github.com/cline/cline/issues/3434) "Fix buggy scrolling in chatview" (10 thumbs-up). VERIFIED (title/reactions only).
- Structural smell still in code: timer ladders (10 ms debounce, 40/70/50/500 ms settles) and smooth scroll used for follow. VERIFIED `useScrollBehavior.ts:134-155,278-314`.

---

## 2. Roo Code

### 2.1 State pipeline

- Canonical `partial: true` pattern. On the host, a partial `say`/`ask` of the same type as the last message mutates it in place and sends only that message (`messageUpdated`); a new message appends and posts state. VERIFIED `src/core/task/Task.ts:1731-1756,1160-1164`.
- Webview applies `messageUpdated` by replacing one array slot found by `ts` and drops unknown `ts` with a warning. VERIFIED `webview-ui/src/context/ExtensionStateContext.tsx:337-358`.
- Three state-post variants reduce payload and races: full; without `taskHistory` (used on every message add, history is updated by `taskHistoryItemUpdated`); without `clineMessages` (settings/mode changes, so a stale async snapshot cannot overwrite streamed messages). VERIFIED `src/core/webview/ClineProvider.ts:1827-1866`, `Task.ts:1143-1152`.
- `clineMessagesSeq` monotonic counter; webview ignores a snapshot's `clineMessages` unless its seq is strictly greater. VERIFIED `ClineProvider.ts:1830-1831`, `ExtensionStateContext.tsx:151-164`.
- Stable row keys. VERIFIED `Task.ts:1277-1285`:

  ```ts
  // Bug for the history books:
  // In the webview we use the ts as the chatrow key for the virtuoso list.
  // Since we would update this ts right at the end of streaming, it would
  // cause the view to flicker. ... The lesson here is if you see flickering
  // when rendering lists, it's likely because the key prop is not stable.
  ```
- Token usage emits are debounced with `maxWait` (throttle-like). VERIFIED `Task.ts:520-525`.

### 2.2 Transcript rendering

- Virtuoso: `increaseViewportBy={{ top: 3_000, bottom: 1000 }}`, `followOutput={followOutputCallback}`, `atBottomStateChange`, `atBottomThreshold={10}`, `key={task.ts}`; no footer component. VERIFIED `webview-ui/src/components/chat/ChatView.tsx:1631-1641`.
- Follow is an explicit three-phase state machine: `HYDRATING_PINNED_TO_BOTTOM`, `ANCHORED_FOLLOWING`, `USER_BROWSING_HISTORY`. VERIFIED `webview-ui/src/hooks/useScrollLifecycle.ts:1-26`.
  - `followOutput` returns `"auto"` (instant) unless browsing. `:301-303`.
  - Only user input disengages follow: wheel-up, PageUp/Home/ArrowUp outside editable targets, pointer drag whose `scrollTop` decreases on the same scroller element that received `pointerdown` (prevents nested scrollers from disengaging), and row expansion. `:350-358,365-433,439-471`.
  - A not-at-bottom report while following and streaming is treated as drift and re-pinned, not as user intent. `:335-339`.
  - Task switch opens a 600 ms hydration window: one immediate `scrollToIndex("LAST")`, transient not-at-bottom ignored, one retry with a 160 ms window, then stay anchored rather than downgrade. `:19-20,175-213,315-318`.
  - `isAtBottom` is a ref, not state, to avoid stale closures and callback identity churn that rerendered rows. `:94`, and [#11385](https://github.com/RooCodeInc/Roo-Code/pull/11385).
  - Composer height change re-pins only when at bottom and not browsing. VERIFIED `ChatView.tsx:1776-1780`.
- Grouping: `combineApiRequests(combineCommandSequences(messages))`; consecutive read-file, list-files, and edit-file asks are batched into synthetic rows via `batchConsecutive`. VERIFIED `ChatView.tsx:124,1230-1232`.
- "Ever visible" LRU (max 100, 5 min TTL): once a message has been shown it keeps being shown even if the filter would now hide it, to stop rows popping out. VERIFIED `ChatView.tsx:155-160,975-992,1028-1031`.
- Rows `memo(..., deepEqual)` with `useSize` height reporting for the last row only. VERIFIED `ChatRow.tsx:133-164`.

### 2.3 Streaming markdown and code

- `MarkdownBlock` is memoized; code blocks render a plain React fallback immediately and swap in Shiki output asynchronously, guarded by a mounted ref. VERIFIED `webview-ui/src/components/common/MarkdownBlock.tsx:206`, `common/CodeBlock.tsx:168,195-270`.
- Mermaid rendering is debounced. VERIFIED `common/MermaidBlock.tsx:4,105`.
- [#6266](https://github.com/RooCodeInc/Roo-Code/pull/6266): scrollbar flicker with streaming tables/code fixed by removing negative margins, table `width:auto` in a horizontal-scroll wrapper, memoizing the `components` object, min-height on `pre`. VERIFIED (PR body); individual CSS changes UNVERIFIED in current tree.

### 2.4 Affordances and interaction

- Still spinner-based: API request and tool rows use a scaled `VSCodeProgressRing`. VERIFIED `components/chat/ProgressIndicator.tsx:3-16`.
- Reasoning block: lightbulb, "Thinking", elapsed seconds ticking at 1 Hz only while `isLast && isStreaming`, collapsible. VERIFIED `components/chat/ReasoningBlock.tsx:23-41,52-57`.
- Message queue: sends are queued when busy, streaming, the queue is non-empty (preserves order during drain), or a command is running; input clears immediately; queue renders above the composer with inline edit and remove; host drains on idle/ask. VERIFIED `ChatView.tsx:592-615,1734-1748`, `components/chat/QueuedMessages.tsx:18-45`, `src/core/task/Task.ts:1385,1411-1412,4603-4606`.
- Follow-up suggestions with a visible auto-approve countdown that cancels on typing or selection. VERIFIED `components/chat/FollowUpSuggest.tsx:31-83,122-134`.
- Primary/secondary button model plus scroll-to-bottom and jump-to-latest-checkpoint controls sharing the fixed-height 36 px action bar. VERIFIED `ChatView.tsx:141-142,1548,1643-1665`.
- Task header: context window progress, todo list. VERIFIED `components/chat/TaskHeader.tsx:315,426`.
- Cancel without flicker: the cancelled task is rehydrated in place rather than popping the task stack (which flashed the home view). VERIFIED (PR body) [#9037](https://github.com/RooCodeInc/Roo-Code/pull/9037).

### 2.5 Instructive fixes

1. [#11483](https://github.com/RooCodeInc/Roo-Code/pull/11483) redesign rehydration scroll lifecycle (+1048/-126): replaced "ad-hoc settle loops" and "race-prone sticky toggles" with the phase model above. VERIFIED (PR + code).
2. [#11385](https://github.com/RooCodeInc/Roo-Code/pull/11385): one source of truth for at-bottom (Virtuoso callback), removed a duplicate raw scroll listener, state -> ref. VERIFIED (PR body).
3. [#8999](https://github.com/RooCodeInc/Roo-Code/pull/8999): let Virtuoso own bottom-follow via `followOutput` instead of after-render debounced scrolls. VERIFIED (PR body).
4. [#7064](https://github.com/RooCodeInc/Roo-Code/pull/7064): a 500-message slice shifted every index when a message arrived and made the viewport jump to mid-conversation; cap removed. VERIFIED (PR body + comment at `ChatView.tsx:955-956`).
5. [#6780](https://github.com/RooCodeInc/Roo-Code/pull/6780): `scrollToIndex` during streaming caused jitter with code blocks; reverted to `scrollTo(MAX_SAFE_INTEGER)`. VERIFIED (PR body). Note the current hook uses `scrollToIndex("LAST")` again together with `followOutput`.
6. [#6697](https://github.com/RooCodeInc/Roo-Code/pull/6697): `increaseViewportBy.bottom = MAX_SAFE_INTEGER` mounted every row below the viewport; changed to 1000 px. VERIFIED (PR body + current config).
7. [#4209](https://github.com/RooCodeInc/Roo-Code/pull/4209): Virtuoso `Footer` spacer kept rerendering; removed. VERIFIED (PR body).
8. [#2318](https://github.com/RooCodeInc/Roo-Code/pull/2318): `combineApiRequests` O(n^2) -> O(n). VERIFIED (PR body).
9. [#5237](https://github.com/RooCodeInc/Roo-Code/pull/5237): bottom action bar restructured so it does not shift with scroll state. VERIFIED (PR body).
10. [#9565](https://github.com/RooCodeInc/Roo-Code/pull/9565): React Compiler enabled for the webview. VERIFIED (PR body); measured benefit UNVERIFIED.

### 2.6 Remaining complaints

- [#10769](https://github.com/RooCodeInc/Roo-Code/issues/10769) (open) todo list updates item by item, jittering N times; [#11986](https://github.com/RooCodeInc/Roo-Code/issues/11986) (open) same symptom. VERIFIED (issue body / title).
- [#5601](https://github.com/RooCodeInc/Roo-Code/issues/5601), [#2270](https://github.com/RooCodeInc/Roo-Code/issues/2270) grey/blank webview on huge histories. VERIFIED (titles/reactions only).

---

## 3. Kilo Code (where it differs from Roo)

Kilo's webview is SolidJS on an OpenCode core, virtualized with `virtua`. Its scroll code encodes the most recent thinking of the five.

- Follow hook `createAutoScroll`: pins with a plain `scrollTop = scrollHeight` assignment ("bypasses any CSS `scroll-behavior: smooth`"); "immediate updates to avoid visible catch-up animations". VERIFIED `packages/kilo-ui/src/hooks/create-auto-scroll.tsx:44-61`.
- Pin before paint. VERIFIED `create-auto-scroll.tsx:164-182`:

  ```ts
  // Content mutations are pinned while they are still queued, before the frame
  // lays out and paints. A ResizeObserver entry arrives after that layout, so
  // waiting for it lets the browser paint one frame with the new content hanging
  // below the viewport, which reads as the transcript twitching as it streams.
  ```
- Intent detection is input-based: a scroll event with no recent pointer/key/touch input is treated as a virtualizer or layout correction and re-pinned rather than treated as the user leaving. 300 ms grace after real input; upward wheel pauses immediately; a finished text selection pauses follow; viewport resize (composer growth) re-pins unless a drag is in progress; 300 ms settle after the working state ends. VERIFIED `create-auto-scroll.tsx:10,93-101,107-143,184-194,212-228`, `packages/kilo-ui/src/hooks/scroll-user-activity.ts:9-17`.
- The live turn is not virtualized. Rows are partitioned into `virtual` (completed history) and `direct` (the whole live turn, plain DOM), with a single handoff when the next turn starts. VERIFIED `packages/kilo-vscode/webview-ui/src/context/transcript-rows.ts:267-291`:

  ```ts
  // The whole live turn renders directly. Handing each finished step to the
  // virtualizer mid-turn mounted those rows at the 260px estimate while their
  // real height (a collapsed tool row is ~28px) was only known once measured;
  // the correction fought the bottom pin and rows near the range edge blinked
  // in and out of the DOM. One handoff happens when the next turn starts.
  ```
- On handoff, measure and re-pin in the same task (microtask) so no frame paints at the estimate. VERIFIED `packages/kilo-vscode/webview-ui/src/components/chat/MessageList.tsx:495-515`.
- Stable keys and structural sharing: the virtualizer is fed string keys, not row objects, because rebuilt objects remounted every row at the estimate; unchanged rows keep their previous object identity. VERIFIED `MessageList.tsx:487-492`, `transcript-rows.ts:259-264`.
- Virtual container clips vertically so only measured sizes set scroll height (prevents a mount/unmount oscillation). VERIFIED `MessageList.tsx:94-103`.
- Measurement cache per session. VERIFIED `MessageList.tsx:832-835,960`.
- Remaining complaint: [#13296](https://github.com/Kilo-Org/kilocode/issues/13296) (open) bottom edge snaps during streamed bullet lists in reasoning; reporter's proposed fix is a per-frame pin plus a grow-only `min-height` on the streaming block. VERIFIED (issue exists); its root-cause analysis is UNVERIFIED and predates the code above. Earlier: [#10836](https://github.com/Kilo-Org/kilocode/issues/10836), [#3310](https://github.com/Kilo-Org/kilocode/issues/3310) (titles only).

---

## 4. Continue

### 4.1 State pipeline

- Redux Toolkit. Stream chunks arrive as individual `postMessage`s, are buffered, and yielded as an array every 50 ms; each array is one `streamUpdate` dispatch (one reducer pass, one render). VERIFIED `gui/src/context/IdeMessenger.tsx:185-232`, `gui/src/redux/thunks/streamNormalInput.ts:199-225`, `gui/src/redux/slices/sessionSlice.ts:524`.
- Abort posts an `abort` message tied to the stream's `messageId`. VERIFIED `IdeMessenger.tsx:214-217`.

### 4.2 Transcript rendering

- No virtualization: `history.map` into a scrolling div, one error boundary per item. The last item gets `minHeight: 200px` (reserves space so a new response does not start at zero height). VERIFIED `gui/src/pages/gui/Chat.tsx:387-413`.
- Follow hook is ~60 lines: `ResizeObserver` on the container and its immediate children sets `scrollTop = scrollHeight` unless `userHasScrolled`; that flag is "not at bottom within 1 px" from the scroll event and resets only when a new user message is added. VERIFIED `gui/src/pages/gui/useAutoScroll.ts:8-60`.
- Smooth scrolling was added and reverted the same day. VERIFIED [#5290](https://github.com/continuedev/continue/pull/5290) / [#5408](https://github.com/continuedev/continue/pull/5408) (PR metadata; reason UNVERIFIED).

### 4.3 Markdown

- `StyledMarkdownPreview` is `memo`'d and uses `useRemark` with KaTeX and a highlight plugin; mermaid fences go to a separate block. VERIFIED `gui/src/components/StyledMarkdownPreview/index.tsx:188,227-264,354`.
- Anti-pattern worth noting: every preview selects the whole `state.session.history`, so `memo` does not shield old messages from stream updates. VERIFIED `index.tsx:198-221`.

### 4.4 Affordances and interaction

- Text ellipsis animations, no shimmer: "Generating..." in the toolbar under the input with Stop and its shortcut; "Thinking..." 600 ms dot cycle (o1 only). VERIFIED `gui/src/components/mainInput/Lump/LumpToolbar/GeneratingIndicator.tsx:3-16`, `StreamingToolbar.tsx:9-29`, `gui/src/components/StepContainer/ThinkingIndicator.tsx:15-41`.
- Reasoning pill: "Thinking" while in progress, "Thought for Ns" after. VERIFIED `gui/src/components/mainInput/belowMainInput/ThinkingBlockPeek.tsx:38-79`.
- Response actions row is a fixed `h-7` container kept mounted during streaming, dimmed with `pointer-events-none`. VERIFIED `gui/src/components/StepContainer/StepContainer.tsx:37-39,107-110`.
- Tool approval lives in the toolbar attached to the composer with Reject / Accept and keyboard hints on the first pending call; rejecting the last one refocuses the editor. VERIFIED `gui/src/components/mainInput/Lump/LumpToolbar/PendingToolCallToolbar.tsx:26-80`.
- Optimistic submit: `submitEditorAndInitAtIndex` writes the user message and an empty assistant placeholder synchronously; edit-and-resend truncates history at that index. VERIFIED `gui/src/redux/slices/sessionSlice.ts:357-372`.
- No message queue found (grep of `gui/src` for "queue"). VERIFIED (negative result at this SHA).

### 4.5 Instructive fixes

1. [#7677](https://github.com/continuedev/continue/pull/7677): removing the reserved space for response actions made the row collapse during streaming and pop back; fixed by always rendering it dimmed. VERIFIED (PR + code).
2. [#7748](https://github.com/continuedev/continue/pull/7748): memoized input box, toolbar and editor to stop rerender per keystroke; added a `useRenderDebug` hook and rerender-flash CSS. VERIFIED (PR body).
3. [#8222](https://github.com/continuedev/continue/pull/8222): thinking dots kept animating after cancel; `inProgress` tied to `isStreaming`. VERIFIED (PR body).
4. [#2395](https://github.com/continuedev/continue/pull/2395): removed an unnecessary scroll delay. VERIFIED (title/diff size only).
5. [#7860](https://github.com/continuedev/continue/pull/7860): CLI (Ink) only, not the GUI: split history at paragraph boundaries into memoized static chunks to reduce terminal flicker. VERIFIED (PR file list).

### 4.6 Remaining complaints

- [#8085](https://github.com/continuedev/continue/issues/8085) JetBrains sidebar freezes (17 thumbs-up). VERIFIED (title/reactions only). No virtualization means cost grows with session length; UNVERIFIED as the cause.

---

## 5. Goose desktop (Electron + React, ACP)

### 5.1 State pipeline

- External store keyed by session id; ACP `session/update` notifications go through an adapter that returns typed changes; each `notify` builds a snapshot and calls listeners. VERIFIED `ui/desktop/src/acp/chatSessionStore.ts:122-138,195-205,455-474`.
- Every snapshot deep-clones the message list (`cloneMessages`), so rows rely on deep-equality memo. VERIFIED `chatSessionStore.ts:762-785`, `ui/desktop/src/components/MessageList.tsx:155`.
- Session-load replay fast path: while `LoadingConversation`, replayed messages accumulate in the adapter, listeners get the frozen snapshot, and the conversation is materialized once in `finishSessionLoad`. VERIFIED `chatSessionStore.ts:219-232,460-470`.
- Prompt attempts have ids; completion and cancellation are applied only if the attempt is still current. VERIFIED `ui/desktop/src/acp/chatSessionController.ts:187-225`.

### 5.2 Transcript rendering

- No virtualization. Above 220 visible messages it shows the first 20 and last 200 with an "N messages hidden / Show all" divider; Cmd/Ctrl-F reveals all so find-in-page works. VERIFIED `MessageList.tsx:50-52,197-234,244-259`.
- Rows `memo(MessageRowComponent, isEqual)`; per-row tool context derived once per message array. VERIFIED `MessageList.tsx:155,236`.
- Scroll area: follow state from the scroll event with a 200 px bottom threshold; pin on children change (in `requestAnimationFrame`) and on a content `ResizeObserver` for late growth (images, highlighting); 100 ms "actively scrolling" guard so pins do not fight the user; all programmatic scrolls are instant. VERIFIED `ui/desktop/src/components/ui/scroll-area.tsx:50,62-76,107-151,154-203`.

  ```ts
  // Jump instantly rather than animating: a smooth programmatic scroll
  // emits intermediate scroll events that handleScroll mistakes for a
  // manual scroll-up, which disables auto-follow mid-animation.
  ```

### 5.3 Streaming markdown

- Streaming text is throttled before it reaches the markdown renderer: 50 ms cooldown, 250 ms once the message exceeds 16,000 chars; leading edge renders immediately, trailing edge always flushes; disabled when the message has tool requests or images. VERIFIED `ui/desktop/src/hooks/useThrottledStreamingText.ts:3-55`, `ui/desktop/src/components/GooseMessage.tsx:29-30,70-80`.
- `MarkdownContent`, `CodeBlock`, `MarkdownCode` are memoized; Prism highlighter element is `useMemo`'d. VERIFIED `ui/desktop/src/components/MarkdownContent.tsx:137,171-207,227,270`.

### 5.4 Affordances and interaction

- One status line, absolutely positioned at bottom-left of the transcript (out of flow, cannot shift layout), text keyed on chat state ("goose is thinking...", "working on it...", "waiting...", "compacting...") or an agent-supplied progress message, with small animated icons. VERIFIED `ui/desktop/src/components/BaseChat.tsx:530-534`, `ui/desktop/src/components/LoadingGoose.tsx:43-80`.
- Tool cards: 8 px status dot on the tool icon (grey pending, yellow `animate-pulse` loading, green, red). VERIFIED `ui/desktop/src/components/ToolCallStatusIndicator.tsx:24-45`.
- Thinking: collapsible, waits for the user setting before rendering so it does not flash a collapsed block. VERIFIED `ui/desktop/src/components/ThinkingContent.tsx:29-34`.
- Optimistic cancel with rollback: Stop sets state to Idle immediately, saves a restore snapshot, fires the ACP cancel in the background; the pending cancellation can be cleared or restored. VERIFIED `chatSessionStore.ts:330-390`, `chatSessionController.ts:227-240`.
- Optimistic steer messages are appended locally with a pending id set and discarded on cancel/finish if unconfirmed. VERIFIED `chatSessionStore.ts:255-270,730-738`.
- Message queue component with pause-on-interrupt, "Send now", inline edit, drag to reorder, clear all. VERIFIED `ui/desktop/src/components/MessageQueue.tsx:9-86` (strings only; behaviour not traced).

### 5.5 Instructive fixes

1. [#11583](https://github.com/aaif-goose/goose/pull/11583) long chat performance: throttle markdown, stop historical rows rerendering, derive tool context once. Reported test: 401 messages + 1,500 chunks at 5 ms went from unresponsive to usable. VERIFIED (PR body + code); benchmark UNVERIFIED.
2. [#10665](https://github.com/aaif-goose/goose/pull/10665) O(n^2) cloning during session load; reported 30,000-notification replay 188.5 s -> 4.3 s. VERIFIED (PR body + code); timing UNVERIFIED. Virtualization was attempted in the same PR and reverted.
3. [#10501](https://github.com/aaif-goose/goose/pull/10501) smooth programmatic scroll was misread as user scroll-up; switched to instant and added the ResizeObserver re-pin. VERIFIED (PR + code).
4. [#4257](https://github.com/aaif-goose/goose/pull/4257) one owner for scroll-to-bottom ("no longer fighting various scroll to bottom mechanisms"). VERIFIED (PR body).
5. [#4504](https://github.com/aaif-goose/goose/pull/4504), [#4923](https://github.com/aaif-goose/goose/pull/4923), [#5738](https://github.com/aaif-goose/goose/pull/5738): successive retuning of near-bottom detection, ending with the threshold raised from 100 to 200 px because fast streaming outran it. VERIFIED (PR bodies). This is the cost of inferring follow state from position alone.
6. [#8214](https://github.com/aaif-goose/goose/pull/8214): TUI only (`ui/text`), height tracking for streaming plus tool boxes. VERIFIED (file list).

### 5.6 Remaining complaints

- [#11538](https://github.com/aaif-goose/goose/issues/11538) UI froze after long responses with tool calls (fixed by #11583). VERIFIED.
- [#11926](https://github.com/aaif-goose/goose/issues/11926) (open) tool cards dominate a turn; request to collapse them into a compact summary. VERIFIED.
- [#12197](https://github.com/aaif-goose/goose/issues/12197), [#11348](https://github.com/aaif-goose/goose/issues/11348) large sessions load oldest-first and look stuck at the top. VERIFIED (titles only).

---

## 6. Cross-project synthesis

### 6.1 What every project converged on

| Concern | Cline | Roo | Kilo | Continue | Goose |
|---|---|---|---|---|---|
| Incremental per-message updates | yes (seq/epoch reducer) | yes (`messageUpdated` + seq) | yes | 50 ms chunk batches | per ACP notification |
| Coalescing interval | 50 ms state post | token usage only | n/a | 50 ms | 50 ms / 250 ms render throttle |
| Virtualization | Virtuoso, all-below mounted | Virtuoso, 3000/1000 px | virtua for history, live turn direct | none | none (20 + 200 cap) |
| Follow scroll style | smooth + instant settle timers | instant (`followOutput: "auto"`) | instant `scrollTop` | instant | instant |
| User-intent signal | wheel-up, expansion | wheel, keys, pointer drag, expansion | any recent input + selection | position only | position + 100 ms guard |
| Working indicator | in-list shimmer row | spinner ring | (not studied) | text ellipsis in toolbar | out-of-flow status line |

### 6.2 Hard-won lessons on scroll and streaming jank

1. **Follow state must come from user input, not from scroll position.** Every project that inferred "user scrolled away" from position alone ended up retuning thresholds (Goose 100 -> 200 px, #5738) or mis-reading its own programmatic scrolls (Goose #10501). Roo and Kilo ended at explicit input signals (wheel/keys/pointer) and treat any other position change as drift to correct (`useScrollLifecycle.ts:335-339`, `create-auto-scroll.tsx:126-140`).
2. **Never animate the follow scroll.** Smooth follow produces catch-up lag and intermediate events. Goose, Roo, Kilo and Continue all pin instantly; Cline still uses smooth plus a ladder of settle timers and has the most open scroll complaints.
3. **Pin in the same frame as the content change.** Kilo pins from a MutationObserver before layout; ResizeObserver-only pinning paints one frame with content below the viewport (`create-auto-scroll.tsx:164-182`). Cline #12746 found the same class of bug for the loader (effect-owned visibility cost one blank paint).
4. **Do not virtualize the growing tail.** Estimated heights for streaming rows fight the bottom pin (Kilo `transcript-rows.ts:279-283`; Cline #4780; Roo #6780). Kilo renders the whole live turn outside the virtualizer and hands it over once.
5. **Row identity must be stable across partial -> final.** Roo's "bug for the history books" (`Task.ts:1277-1285`); Kilo feeds keys not objects (`MessageList.tsx:487-492`); Roo #7064 shows index shifting from slicing is the same bug.
6. **Two update channels need ordering metadata.** Cline's seq/epoch reducer and Roo's `clineMessagesSeq` both exist because a slow full snapshot overwrote fresher streamed messages. Turn phase needs its own monotonic guard (Cline `applyTurnState`).
7. **Programmatic layout changes must not be classified as user intent.** Cline #11873 (auto-expand of command output disabled follow), Roo #11483 (nested scroller deltas), Goose #10501.
8. **Enumerated scroll triggers rot.** Cline #11436: scroll code unchanged, data flow changed, pinning broke. Observe rendered size instead.
9. **Reserve space for anything that appears later.** Response actions (Continue #7677), diff line-number gutter (Cline `DiffEditRow.tsx:236`), header placeholder (Cline #9266), out-of-flow status (Goose `BaseChat.tsx:530`), fixed-height action bar (Roo #5237), bounded-height inner scrollers for reasoning/command output (Cline).
10. **Indicator flicker is a state-machine problem.** One indicator, authoritative phase, optimistic show at send, asymmetric debounce (delay show only when ambiguous, never hide a visible one) (`useThinkingLoaderRow.ts:171-215`).
11. **Throttle the expensive consumer, not the data.** Goose keeps state exact and throttles only the markdown input (50/250 ms); Cline memoizes per markdown block so only the tail re-parses.
12. **Bulk loads are a separate path.** Goose replay fast path (#10665); Roo hydration window (#11483); Cline empty-list fast path (#12746).

### 6.3 Ranked recommendations for Memex

Ordered by expected impact on perceived smoothness. "AppKit" refers to the native transcript; "Web" to the React surface.

1. **Model follow as an explicit state machine driven by user input.** States: hydrating, following, browsing. AppKit: use `NSScrollView.willStartLiveScrollNotification` / `didLiveScrollNotification` / `didEndLiveScrollNotification` (user-initiated only) plus `scrollWheel` direction and key equivalents to enter browsing; treat `boundsDidChange` without a live scroll as drift and re-pin. Web: copy Roo's `useScrollLifecycle` or Kilo's `createAutoScroll`. Re-enter following on explicit scroll-to-bottom, on send, and when the user reaches the bottom.
2. **Instant, same-pass bottom pin.** AppKit: after applying a streaming delta, update the row height and set the clip view origin inside one `CATransaction` with actions disabled / `NSAnimationContext` duration 0, before the next display, not via `DispatchQueue.main.async`. Web: pin from a MutationObserver or `useLayoutEffect`, `scrollTop = scrollHeight`, no `behavior: "smooth"`.
3. **Keep the live turn out of estimated-height virtualization.** AppKit: the streaming turn should be one or a few rows whose height is always measured, updated with `noteHeightOfRows(withIndexesChanged:)` with animation off; never `reloadData` during streaming; never let rows above the viewport re-measure while following. Web: virtualize completed turns only (Kilo partition) or skip virtualization and cap (Goose 20 + 200) until proven necessary.
4. **Stable ids from first partial to final, plus an ordered reducer.** Each transcript item needs an id that does not change at completion, a per-item sequence, and a session epoch; snapshot merges never truncate within an epoch (Cline `messageReducer.ts`). ACP tool-call ids and message ids should map directly.
5. **Authoritative turn phase with its own sequence, and optimistic markers that retire on acknowledgement.** Optimistic user bubble + optimistic "working" marker at send; retire by ack sequence; roll back on failure (Cline `pendingResponse.ts`). Optimistic cancel with a restore snapshot (Goose `startPromptCancellation`).
6. **One working indicator with asymmetric debounce and continuous shimmer.** In-flow last row or out-of-flow status line, never both. Do not recreate the view when its label changes (shimmer restarts on remount, Cline #9300). AppKit: a single reused layer with a `CAGradientLayer` mask animation; keep it alive across reasoning -> tool -> text handoffs.
7. **Coalesce deltas at 16-50 ms and throttle markdown harder for large messages.** Apply ACP chunks to the model immediately; schedule one layout per display tick; above ~16k characters drop to ~250 ms for the full re-layout (Goose). Cache rendered markdown per top-level block and only re-render the tail block (Cline `MarkdownBlock.tsx:20-27,110-113`). Show unhighlighted code first, swap highlight asynchronously without changing line metrics (Roo `CodeBlock.tsx:195-270`).
8. **Reserve geometry.** Fixed-height action rows kept mounted during streaming; diff gutter width fixed from the first line; reasoning and command output in bounded-height inner scrollers with their own follow; queue and approval UI attached to the composer, outside the transcript; when the composer or queue grows, re-pin if following (Cline `ChatView.tsx:357-384`, Roo `ChatView.tsx:1776-1780`).
9. **Group low-value rows at the data layer and decide grouping before first paint.** Consecutive read/search/list calls into one row, API-request bookkeeping absorbed (Cline `groupLowStakesTools`, Roo `batchConsecutive`). Avoid frames where a row appears then gets absorbed (Cline `isApiReqAbsorbable`). Batch multi-item updates such as todo lists into one layout (Roo #10769).
10. **Separate bulk-load path.** On opening a session, apply the replay without per-item notifications, lay out once, land at the bottom, and suppress follow-state changes for a short hydration window (Goose #10665, Roo #11483). Paint the first row or loader without waiting for list measurement (Cline #12746).
11. **Expansion rules.** User expansion disengages follow and never scrolls; collapse re-pins only if at bottom; programmatic expansion is tagged and keeps follow (Cline `useScrollBehavior.ts:216-261`).
12. **Instrument.** A rerender/relayout flash overlay in debug builds (Continue #7748) and unit tests for the follow state machine and indicator visibility (Cline `useThinkingLoaderRow.test.tsx`, Kilo `create-auto-scroll.test.tsx` at 1,072 lines) are what stopped the regressions in these projects.

### 6.4 Gaps in this study

- Cline's Tauri desktop transcript (`sdk/packages/ui/components/message-scroller.tsx`, shadcn Message Scroller primitive) was only skimmed; PRs [#13028](https://github.com/cline/cline/pull/13028), [#13315](https://github.com/cline/cline/pull/13315), [#14948](https://github.com/cline/cline/pull/14948) read by title/body only.
- Checkpoint restore, edit-and-resend and history-list reopening were located but not traced end to end in Cline or Roo.
- Kilo's affordances (indicators, queue UI) were not studied; only scroll and virtualization.
- No runtime measurement was performed; all performance numbers are quoted from PR descriptions.
