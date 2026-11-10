# Perceived performance, optimistic architecture, and a feel spec for an agent chat app

Research date: 2026-11-09. Scope: principles and closed-source benchmarks for reviewing the Memex live agent chat PR (native macOS Swift/SwiftUI + AppKit, secondary React web surface). No code was read or changed.

## Evidence labels

- **VERIFIED**: I read the primary source in this session and it says this.
- **VERIFIED-2ND**: I read a secondary source (summary, reverse-engineering writeup, third-party transcription) that says this; the primary was not read.
- **UNVERIFIED**: search-result excerpt only, observational product behavior, or widely repeated practitioner knowledge not traced to a primary source here.
- **INFERENCE**: my synthesis from the evidence; not a sourced claim.

Sources are paraphrased, not quoted. Numbers and constants are reported as facts from the cited page.

Section 5 is the deliverable for reviewers. Sections 1-4 are the evidence it rests on.

---

## 1. Perceived-performance evidence

### 1.1 Response-time thresholds

| Threshold | Meaning | Source | Label |
|---|---|---|---|
| ~16 ms (8.3 ms at 120 Hz) | One display frame. Motion tracking is where users are most sensitive; RAIL budgets 10 ms of app work per frame at 60 Hz because the browser needs ~6 ms. | https://web.dev/articles/rail | VERIFIED |
| 50 ms | RAIL: handle an input event within 50 ms so the visible response lands inside 100 ms; anything longer needs explicit feedback. | https://web.dev/articles/rail | VERIFIED |
| 100 ms | Limit for "the system reacted instantly"; no feedback needed beyond the result itself. Traces to Miller 1968 and Card, Robertson, Mackinlay 1991. | https://www.nngroup.com/articles/response-times-3-important-limits/ | VERIFIED |
| 100 ms (native) | Apple: a delay under 100 ms in a discrete interaction is rarely noticeable; a few hundred ms reads as unresponsive. Apple's pipeline diagram budgets event delivery under 10 ms, rendering 8-33 ms, display update 8-16 ms, and notes event delivery plus post-main-thread rendering add 10-50 ms even in normal operation. | https://developer.apple.com/documentation/xcode/understanding-hangs-in-your-app | VERIFIED |
| 200 ms | INP "good" boundary (75th percentile); 200-500 ms needs improvement; above 500 ms poor. INP measures time until the next paint is unblocked, not until the work finishes. | https://web.dev/articles/inp | VERIFIED |
| 250 ms | Apple's tools start reporting a main-run-loop hang at 250 ms by default (Instruments allows lower). | same Apple doc | VERIFIED |
| 400 ms | Doherty threshold: productivity rises when system response is under ~400 ms (Doherty and Thadani, IBM Systems Journal, 1982; prior norm 2 s). | https://lawsofux.com/doherty-threshold/ | VERIFIED-2ND |
| 1 s | Limit for uninterrupted flow of thought. Delay is noticed but no special feedback is needed between 0.1 and 1 s. RAIL: past 1 s users lose focus on the task. | NN/g and web.dev RAIL above | VERIFIED |
| 2-10 s | NN/g: range where a looped indicator is acceptable. | https://www.nngroup.com/articles/progress-indicators/ | VERIFIED |
| 10 s | Limit of attention. Beyond it, show percent-done or an explicit duration estimate and a way to interrupt; users will want to switch tasks. | NN/g response-times and progress-indicators | VERIFIED |

Typing is stricter than the 100 ms rule. Pavel Fatin's editor-latency study argues that the usual 100 ms assumption is too optimistic for typing, measured good editors at roughly 1-5 ms of software latency and bad ones at 45-200 ms, and notes that keyboard (avg ~14 ms) and display (~12 ms) hardware already consume part of the budget. https://pavelfatin.com/typing-with-pleasure/ (VERIFIED). INFERENCE: the composer's own contribution to keypress-to-glyph should be one frame, because hardware already spends 20-30 ms.

Superhuman operationalizes the 100 ms rule as a product bar: every interaction under 100 ms, aiming under 50 ms, attributed to Paul Buchheit's Gmail rule. https://blog.superhuman.com/superhuman-is-built-for-speed/ (VERIFIED). Their measurement post recommends starting the clock at the event's own timestamp (so a blocked main thread is counted), ending at the frame, and tracking the fraction of events under 100 ms (plus a 50 ms bucket) instead of a p90, because a percentile near the line says little about experience and the 50 ms bucket caught a regression invisible at 100 ms. https://blog.superhuman.com/performance-metrics-for-blazingly-fast-web-apps/ (VERIFIED).

### 1.2 Spinners vs skeletons vs shimmer vs progress text

- **Spinners call attention to waiting.** Luke Wroblewski (2013): after adding spinners to Polar's web views, users complained the app felt slower; the indicator made people watch the wait instead of the progress. He replaced them with skeleton screens that fill in incrementally, and recommended folding loading into the transition. https://www.lukew.com/ff/entry.asp?1797 (VERIFIED).
- **NN/g thresholds.** Under ~1 s, no indicator at all: a looped animation that flashes is distracting and can make users anxious about what they missed. 2-10 s: looped indicator or skeleton. 10 s or more: percent-done. Skeletons suit full-page loads, spinners suit a single module, and a skeleton is wrong for a process such as an upload or conversion. Frame-only skeletons (app chrome with no content placeholders) are not recommended. https://www.nngroup.com/articles/skeleton-screens/ and https://www.nngroup.com/articles/progress-indicators/ (VERIFIED). NN/g also cites a University of Nebraska-Lincoln result that people who saw an animated progress indicator waited about three times longer than those who saw none (VERIFIED as NN/g's report of it; the study itself not read).
- **The empirical case for skeletons is mixed.** Mejtoft, Långström, Söderström (ECCE 2018, doi 10.1145/3232078.3232086) found the skeleton variant scored somewhat higher on perceived speed and ease of navigation than the spinner variant, while spinner users were faster at first-visit finding; a secondary source says the differences were not significant (UNVERIFIED, search excerpt). Viget's 2017 test (136 participants) is widely reported to have found skeletons produced the longest perceived wait versus spinner and blank (UNVERIFIED: Viget's page returned HTTP 429; figures come from secondary summaries). INFERENCE: skeletons are not a reliable perceived-speed win. Their defensible benefits are layout stability and orientation. The best loading state is none, which means having the data locally.
- **Shimmer origin.** Facebook's Shimmer library was built to show loading status in Paper and is described as an unobtrusive loading indicator, implemented with a CALayer mask and Core Animation timing. https://github.com/facebookarchive/Shimmer (VERIFIED). NN/g cautions that shimmer and pulse animations can distract or cause accessibility problems (VERIFIED, skeleton-screens article).
- **Shimmer has a real frame cost if done on the main thread.** The coder/mux project found its "Thinking" shimmer, animated via background-position on the main thread, dropped frames exactly when streaming had the main thread busy; the fix was a compositor-only translate over a dimmed copy of the text. https://github.com/coder/mux/pull/986 (VERIFIED). INFERENCE for Memex: drive shimmer with a Core Animation layer animation (render-server side), never with per-frame SwiftUI state invalidation; on web use transform/opacity only.
- **Progress text and operational transparency.** OpenAI's latency guide ranks streaming first (cuts waiting to about a second or less), then showing steps (the more real progress shown, the better), then loading states, and frames the goal as watching progress instead of waiting. https://developers.openai.com/api/docs/guides/latency-optimization (VERIFIED). Buell and Norton's "labor illusion" (Management Science 2011) found people can prefer a service that visibly shows its work over an instant one returning identical results (UNVERIFIED: abstract-level via search excerpt, https://ideas.repec.org/a/inm/ormnsc/v57y2011i9p1564-1579.html). Apple HIG: avoid vague labels such as "loading" because they add little; describe the task when a description helps. https://developer.apple.com/design/human-interface-guidelines/progress-indicators (VERIFIED).
- **Progress-bar perception.** Harrison, Yeo, Hudson (CHI 2010) showed visual treatments of a progress bar change perceived duration relative to a plain bar (VERIFIED that the paper exists and tested this, https://www.chrisharrison.net/index.php/Research/ProgressBars2; the often-cited "about 11% faster" figure is UNVERIFIED here). Apple HIG also recommends evening out the pace of a determinate indicator so people trust it (VERIFIED, progress-indicators).
- **Apple's position.** Show something as soon as possible because an empty view reads as broken; prefer placeholder content; let people keep doing other things while content loads; prefer determinate indicators; keep indicators moving; do not switch between circular and bar styles; a spinner is appropriate for background operations or constrained space such as next to a button or in a text field. https://developer.apple.com/design/human-interface-guidelines/loading and /progress-indicators (VERIFIED).

### 1.3 Delayed-indicator and minimum-display rules

| Rule | Delay before showing | Minimum visible | Source | Label |
|---|---|---|---|---|
| Vercel Web Interface Guidelines | ~150-300 ms | ~300-500 ms | https://vercel.com/design/guidelines | VERIFIED |
| Android ContentLoadingProgressBar | 500 ms (MIN_DELAY_MS) | 500 ms (MIN_SHOW_TIME_MS) | https://raw.githubusercontent.com/androidx/androidx/androidx-main/core/core/src/main/java/androidx/core/widget/ContentLoadingProgressBar.java | VERIFIED |
| spin-delay (React) | 500 ms default | 200 ms default | https://github.com/smeijer/spin-delay | VERIFIED |
| React Suspense | Fallback appearance throttled with FALLBACK_THROTTLE_MS = 300, explicitly to minimize jank as content streams in | n/a | https://github.com/facebook/react/blob/main/packages/react-reconciler/src/ReactFiberWorkLoop.js | VERIFIED |
| NN/g | No looped indicator under ~1 s | n/a | https://www.nngroup.com/articles/progress-indicators/ | VERIFIED |

INFERENCE: the cross-platform consensus is a show delay between 150 and 500 ms and a minimum display of 200-500 ms. For a local-daemon app where most operations finish in tens of ms, use 200 ms delay and 300 ms minimum. The delay rule applies to operations that are usually fast. It does not apply to the post-send working indicator, where the wait is always longer than the delay and the indicator is the acknowledgement (see 1.5).

### 1.4 When optimistic UI is appropriate, and how failure avoids feeling like lying

- Instagram popularized it in 2011 (Mike Krieger, "Secrets to Lightning Fast Mobile Design": perform actions optimistically, pre-load adaptively, move data in the background). https://speakerdeck.com/mikeyk/secrets-to-lightning-fast-mobile-design (UNVERIFIED: slide text via search excerpt).
- Conditions, per Denys Mishunov in Smashing Magazine: success rate around 97-99% (error rate should not exceed 1-3%), a stable and predictable API, server response under ~2 s, and simple binary-like actions. Failure must be communicated within the ~2 s flow window, scaled to the stakes: a quiet revert for a like, clear feedback for anything that matters. Pre-validate on the client to push the error rate down. https://www.smashingmagazine.com/2016/11/true-lies-of-optimistic-user-interfaces/ (VERIFIED).
- Vercel guidelines: update immediately when success is likely, reconcile with the server, and on failure show an error and roll back or offer Undo. Destructive actions need confirmation or an Undo window. Submissions carry an idempotency key. https://vercel.com/design/guidelines (VERIFIED). Rauno Freiberg's guidelines say the same in one line: update locally, roll back on server error with feedback. https://interfaces.rauno.me/ (VERIFIED).
- React's `useOptimistic` converges optimistic and real state in the same render when the action completes, with no separate "clear" render, and on error falls back to the unchanged real value. https://react.dev/reference/react/useOptimistic (VERIFIED). TanStack Query documents a per-item failure pattern: keep the failed item visible, styled as failed, with a Retry button, because the submitted variables are retained. https://tanstack.com/query/latest/docs/framework/react/guides/optimistic-updates (VERIFIED).

INFERENCE, the honesty rule this report uses throughout: **render the user's intent optimistically; render the system's facts on confirmation.** "You sent this message", "you put this item second in the queue", "you renamed this chat", "you answered Allow" are true the instant the user acts, so they can be shown at full fidelity immediately. "The agent is running", "the worktree exists", "the branch was pushed", "the turn was cancelled" are claims about the system and should be shown as in-progress wording ("Starting", "Stopping") until confirmed. Optimism about intent never needs an apology; optimism about system facts is where interfaces get caught lying.

Failure design that follows from the sources:

1. A failed item stays where it was, in place, with its content intact, marked failed, with Retry and Edit. It never silently disappears and never becomes a toast that the user must map back to an item.
2. Failure surfaces within about 2 s when it is known, and a pending marker appears at about 2 s when the outcome is still unknown.
3. Retry is idempotent (same client id), so a "failed" send that actually arrived cannot duplicate. Stream's chat SDK handles the already-exists case by marking the message received instead of failed. https://getstream.io/chat/docs/sdk/react/state/writing-messages/message-operations/ (VERIFIED).
4. Reversible destructive actions get Undo instead of a confirmation dialog; irreversible ones are not optimistic.

### 1.5 Time-to-first-token perception in LLM UIs

- Streaming is the single most effective lever because it converts waiting into watching progress (OpenAI latency guide, VERIFIED above).
- **Latency masking by immediate acknowledgement.** INFERENCE from Nielsen/RAIL plus the OpenAI guide: the user's own message and a working indicator must land within 100 ms of Enter, independent of network. TTFT is then experienced as "the agent is thinking", not "did my message go through".
- **Faster is not uniformly better for perceived quality.** A controlled experiment varying time-to-first-token at 2, 9, and 20 s found interaction behavior robust to latency, and participants in the 2 s condition rated outputs as less thoughtful and useful than in the slower conditions; long waits sometimes tipped into frustration. Tan, Messerschmidt, Yin, Nov, arXiv 2604.06183, https://arxiv.org/abs/2604.06183 (VERIFIED from abstract). INFERENCE: this does not justify adding delay. It supports spending the unavoidable wait on legible, real work (thinking summaries, tool names) instead of a generic spinner.
- **Cadence smoothing.** Vercel AI SDK ships `smoothStream`, which buffers and releases text in word (default), line, or custom chunks with a default 10 ms inter-chunk delay, passes tool calls through immediately, and skips delays when the document is hidden. https://ai-sdk.dev/docs/reference/ai-sdk-core/smooth-stream (VERIFIED). Streamdown completes unterminated Markdown during streaming so partially arrived bold, links, and code fences do not render wrongly and then jump. https://streamdown.ai/docs (VERIFIED). LibreChat documents its smooth-streaming setting as visual only (UNVERIFIED, search excerpt).
- **Why even pacing feels faster than bursts.** UNVERIFIED as a measured effect; I found no controlled study. INFERENCE from adjacent evidence: RAIL says users are most sensitive to non-smooth motion; Apple recommends evening out progress pace to build confidence; React throttles fallback reveals to reduce jank. Bursty text produces irregular layout jumps and a stop-start reading rhythm, and each stall resets the user's "is it stuck?" clock. Smoothing trades a small bounded lag for steady motion. Recommended shape: coalesce to one layout per display frame, reveal at a rate proportional to backlog so lag stays bounded (about 100-250 ms), and flush quickly on completion so the end is never artificially delayed.
- **Scroll during streaming.** StackBlitz's `use-stick-to-bottom` exists because naive approaches fail: it sticks only while the user is at the bottom, lets a user scroll-up escape immediately, distinguishes user scroll from its own animation, handles content that shrinks as well as grows, and uses velocity-based springs because fixed easing does not suit continuously arriving content. https://github.com/stackblitz-labs/use-stick-to-bottom (VERIFIED).

---

## 2. Optimistic and local-first architectures with reconciliation

### 2.1 What each system does

**Linear sync engine** (VERIFIED-2ND via the reverse-engineering writeup https://github.com/wzhudev/reverse-linear-sync-engine and the talk summary https://www.fujimon.com/blog/linear-sync-engine; Linear's own post https://linear.app/blog/scaling-the-linear-sync-engine is a talk page without the detail):

- Every model has a client-generated UUID; the UI reads a normalized in-memory object pool.
- Property assignment updates the in-memory model immediately; `save()` produces a transaction.
- Transactions move through created, queued, executing, and completed-but-unsynced queues. Same-tick transactions batch into one GraphQL mutation. The queue is persisted to IndexedDB and replayed on restart.
- The server assigns a global monotonically increasing sync id. A transaction completes only when the delta with its sync id arrives, not when the HTTP response returns.
- When a server delta arrives for a model with a pending local update, the pending transaction's "original" is rebased to the server value and the in-memory model is reset to the local pending value (last writer wins), so the screen does not flicker back.
- A local creation whose UUID matches an incoming server model is cancelled, not duplicated.
- Server rejection calls the transaction's rollback. Undo/redo is built from transactions.
- Persisted local tables only ever contain server-confirmed data; pending state lives in the transaction log.

**Replicache and Zero (Rocicorp)** (VERIFIED: https://doc.replicache.dev/concepts/how-it-works, https://zero.rocicorp.dev/docs/writing-data):

- Mutators are named functions that run on the client immediately against the local store; results are speculative.
- Each mutation carries a per-client sequential id. Push sends pending mutations; the server re-runs the same named mutator authoritatively.
- Pull returns a patch plus the last mutation id the server has applied. The client drops pending mutations at or below that id, then rebases: rewind to the last server state, apply the patch, replay the still-pending mutations, and only then reveal the result to the app in one step.
- The design goal is exactly-once application on the server.
- Zero recommends client-generated random ids, exposes separate `.client` and `.server` promises per mutation, and reverts the optimistic effect if the server run fails.

**TanStack Query and TanStack DB** (VERIFIED: https://tanstack.com/query/latest/docs/framework/react/guides/optimistic-updates, https://tanstack.com/db/latest/docs/overview):

- Query offers two shapes. UI-level: render the mutation's pending variables as a ghost item; simplest when one place shows it; keeps the item with Retry on error. Cache-level: cancel in-flight fetches so they cannot overwrite the optimistic write, snapshot, write, roll back from the snapshot on error, invalidate on settle.
- TanStack DB keeps optimistic state separate from synced data; live queries read the overlay; the optimistic state is held until the mutation handler resolves and is rolled back if it throws.

**Figma multiplayer** (VERIFIED: https://www.figma.com/blog/how-figmas-multiplayer-technology-works/):

- Local property changes apply immediately. The server is authoritative and orders events; last write per property per object wins.
- To prevent flicker, the client discards incoming server values that conflict with its own unacknowledged change to the same property, because its local value is the best prediction of the final state.
- Object ids embed the client id so creation works offline with no collisions.

**Superhuman** (VERIFIED, links in 1.1): local store of mail, preloading and prerendering of likely-next threads, minimal animation, keyboard-first. The article does not use the phrase optimistic UI.

**Messaging apps' pending states:**

- WhatsApp: clock means not yet sent; one grey check sent; two delivered; two blue read. https://faq.whatsapp.com/665923838265756 (VERIFIED).
- Stream chat SDK (a documented reference implementation of the pattern): message appears immediately with status sending; the server copy replaces the local one unless something newer arrived; failure keeps the message in the list marked failed with a retry that resends exactly what failed. https://getstream.io/chat/docs/sdk/react/state/writing-messages/message-operations/ (VERIFIED).
- Slack: greyed text while sending and a client-generated message id for dedupe (UNVERIFIED; third-party system-design writeups only, no Slack engineering post found).
- iMessage: bubble appears immediately, progress in the header, red "Not Delivered" with tap-to-retry on failure (UNVERIFIED, observational).

### 2.2 The general pattern

1. **Client-generated ids.** Every user-created entity (message, queued item, session, plan revision, answer) gets its id on the client at the moment of intent. The server stores that id or maps to it. The id is the idempotency key.
2. **Pending-mutation log layered over confirmed state.** Confirmed state is whatever the server/daemon last told us. Pending mutations are an ordered log. What the view renders is `apply(pending, confirmed)`. Persist the log so a crash or relaunch does not lose a typed prompt.
3. **Idempotent server application.** The server dedupes by mutation or entity id. Retries and reconnect replays are always safe.
4. **Reconcile by id, never re-key.** When the confirmed record arrives, it merges into the existing view model with the same identity. Row identity in lists (SwiftUI `ForEach` id, `NSDiffableDataSource` item identifier, React `key`) is the client id for the entity's whole life. A server id is an attribute, not an identity.
5. **Suppress echo flicker.** While a local mutation to a field is unacknowledged, ignore conflicting server values for that field (Figma), or rebase pending over the new server state before revealing (Replicache, Linear). Apply server patch and pending replay atomically, in one render.
6. **Complete on the echo, not the response.** A mutation is done when the authoritative stream reflects it (Linear's sync id, Replicache's lastMutationID, Codex app-server's rule that interrupt completes via `turn/completed`, not the RPC reply).
7. **Explicit per-item failure with retry.** Failed items stay in place with content, error, Retry, Edit.
8. **Never block input on the network.** Input is a local operation. Connection state changes what the pending marker says, not whether the user can type or submit.

### 2.3 Mapping onto an agent chat

Columns: what the user sees inside one frame; the mutation and its key; how it reconciles; how it fails.

| Operation | Immediate local effect (same frame) | Mutation / idempotency key | Reconcile | Failure UX |
|---|---|---|---|---|
| Send prompt (agent idle) | Message appended to transcript at full fidelity under `clientMessageId`; composer cleared; focus stays in composer; working indicator appears; stop control replaces send. | `sendTurn{sessionId, clientMessageId, content}`; dedupe on `clientMessageId`. | Server echo of the user item merges into the same row by `clientMessageId`. Server turn/item ids attach as attributes. Working indicator becomes real status on first agent event. | Message stays, marked "Not sent" inline with Retry and Edit (Edit puts text back in composer). Working indicator removed. Never drop the text. |
| Send prompt before session/transport is ready | Same as above. Message is held in the pending log with reason "Starting session". | Same mutation, flushed when the session is ready. | Same. | If session start fails, the failure is shown on the session setup step; the message remains with Retry. |
| Create session / worktree | Sidebar row and empty transcript exist immediately under `clientSessionId`; composer focused and typeable; title is a placeholder or derived from the first prompt. System facts are worded as in progress ("Creating worktree"). | `createSession{clientSessionId, repo, base, mode}`; dedupe on `clientSessionId`. | Server session id, branch name, and worktree path attach to the same row and view. Selection, scroll, draft, and focus survive the id swap. | Setup step shows the failing step with its log and Retry. Draft and queued prompt preserved. Row is not removed automatically. |
| Queue a follow-up while running | Item appears in the queue under `clientQueueItemId` in the position chosen; composer cleared; focus kept. | `enqueue{clientQueueItemId, afterId or orderKey}`. | Confirmed order merges by id. Use fractional or list-relative order keys so concurrent drains do not reshuffle the visible list. | Item stays, marked failed, with Retry and Edit. |
| Reorder / edit / delete queued item | Drag tracks the pointer continuously; drop commits locally at once. Editing pauses the queue's drain for that item (Conductor does this). | `moveQueueItem{id, orderKey}`, `editQueueItem{id, content, rev}`, `deleteQueueItem{id}`. | If the server already dequeued the item (race with turn end), the server wins: the item animates into the transcript as a sent message, and an in-progress edit is preserved as a new draft. | Revert position with a brief inline note; never lose edited text. |
| Steer (inject into the running turn) | Message appears in the transcript under the active turn with a "steering" pending caption worded as intent ("Will be read after the current step"). | `steer{expectedTurnId, clientMessageId}`. Codex app-server's `turn/steer` requires `expectedTurnId` and fails if no turn is active. | Caption clears when the agent's stream shows the input was consumed. | If the turn ended first, convert in place to a normal queued or sent message, not an error. True failure: inline Retry. |
| Cancel / stop | Stop control switches to "Stopping" within a frame; shimmer and token reveal stop advancing; composer remains usable. Do not append "Cancelled" yet. | `interrupt{turnId}`; naturally idempotent. | The turn is marked interrupted when the stream says so (Codex: status arrives on `turn/completed`). | If no confirmation within ~2 s, keep "Stopping" with elapsed time; after ~10 s offer force-stop. If the turn completed before the interrupt landed, show it as completed. |
| Answer permission / structured question | Prompt collapses in place to a compact record of the chosen answer; the agent's status returns to working. | `resolveRequest{requestId, decision}`; dedupe on `requestId`. | Server confirmation (`serverRequest/resolved` in Codex's protocol) finalizes; it also clears prompts resolved elsewhere or invalidated by turn end. | Prompt re-expands with the previous selection intact and an inline error. Never silently treat as answered. |
| Edit plan | Keystrokes are local; plan shows the user's text immediately; no save indicator while healthy. | `updatePlan{planId, baseRev, content}` debounced, plus flush on blur and before approve. | Revision-checked. If the agent revised the plan concurrently, do not overwrite the user's buffer: show "Agent updated the plan" with a view-diff / take-theirs choice. | Inline "Not saved" with Retry; buffer preserved across session switch and relaunch. |
| Approve plan | Plan card switches to approved state; working indicator starts. | `approvePlan{planId, rev}`. | Stream confirms execution began. | Card returns to pending approval with error. |
| Rename chat | Title updates everywhere immediately. | `rename{sessionId, title, clientMutationId}`. | Suppress conflicting server titles (including auto-generated ones) while the local rename is unacknowledged; after a manual rename, auto-titling must not overwrite. | Revert with inline note. |
| Pin / unpin | Row moves to its new section in the same frame. | `setPinned{sessionId, pinned}`. | Merge by id. | Revert quietly; low stakes. |
| Archive | Row leaves the list immediately; selection moves to a neighbor; Undo available for several seconds. No confirmation dialog. | `archive{sessionId}`. | Merge by id. | Row returns with a note. |
| Delete session / remove worktree with uncommitted work | Not optimistic. Confirm, then show in-progress on the row. | `delete{sessionId}`. | Remove on confirmation. | Row stays with error. |
| Push / create PR / other external side effects | Not optimistic. Button shows in-button progress with its label. | Provider call with idempotency key where available. | Show result on confirmation. | Inline error next to the control. |

---

## 3. Motion and interaction craft

### 3.1 Durations

| Interaction | Duration | Source | Label |
|---|---|---|---|
| Any UI animation in a product (not marketing) | Under 300 ms | Emil Kowalski, https://emilkowal.ski/ui/great-animations and /ui/you-dont-need-animations | VERIFIED |
| Dropdown / select | ~180 ms feels responsive; 400 ms feels slow | https://emilkowal.ski/ui/7-practical-animation-tips | VERIFIED |
| Interaction that must feel immediate | 200 ms or less | Rauno Freiberg, https://interfaces.rauno.me/ | VERIFIED |
| Tooltip appear | ~125 ms ease-out; first tooltip delayed, subsequent ones instant with no animation | Emil 7 tips; Vercel guidelines | VERIFIED |
| Micro-interactions 100-150 ms; standard UI 150-250 ms; modals and drawers 200-300 ms; exits somewhat faster than entrances | Emil's course as summarized in the local `web-animation-design` skill | VERIFIED-2ND |
| Toast (Sonner) | 400 ms ease, deliberately slower than typical | https://emilkowal.ski/ui/building-a-toast-component | VERIFIED |
| Sheet / drawer (Vaul) | 500 ms with cubic-bezier(0.32, 0.72, 0, 1), chosen to match the iOS sheet | https://emilkowal.ski/ui/building-a-drawer-component | VERIFIED |
| Material 3 tokens | short 50/100/150/200 ms; medium 250/300/350/400 ms; long 450-600 ms; extra-long 700-1000 ms | material-web token source, https://github.com/material-components/material-web/blob/main/tokens/versions/v0_192/_md-sys-motion.scss | VERIFIED |

### 3.2 Easing and springs

- **ease-out for anything entering, exiting, or responding to the user**: it starts fast, so it reads as responsive. ease-in-out for things already on screen that move. `ease` for hover color changes. Avoid ease-in for UI; linear only for constant-rate indicators. Built-in CSS curves are weak; prefer stronger custom curves. (Emil, great-animations and 7 tips: VERIFIED for the rules; no numeric curves on those pages.)
- **Stronger ease-out curves** as distributed with Emil's course material: quad (0.25, 0.46, 0.45, 0.94); cubic (0.215, 0.61, 0.355, 1); quart (0.165, 0.84, 0.44, 1); quint (0.23, 1, 0.32, 1); expo (0.19, 1, 0.22, 1). ease-in-out cubic (0.645, 0.045, 0.355, 1). (VERIFIED-2ND, local skill files derived from animations.dev.)
- **Material 3 curves**: standard (0.2, 0, 0, 1); standard-decelerate (0, 0, 0, 1); standard-accelerate (0.3, 0, 1, 1); emphasized-decelerate (0.05, 0.7, 0.1, 1); emphasized-accelerate (0.3, 0, 0.8, 0.15). (VERIFIED, material-web source above.)
- **Springs (Apple)**: SwiftUI's default animation is a spring. Configure by duration and bounce. Bounce 0 is the versatile general-purpose default; ~0.15 gives a slightly brisker tail; ~0.3 is noticeably bouncy; be cautious above ~0.4. Springs are the only animation type that preserves velocity continuity when retargeted mid-flight or handed off from a gesture. WWDC23 "Animate with springs", https://developer.apple.com/videos/play/wwdc2023/10158/ (VERIFIED). SwiftUI's `.smooth` preset has no bounce and `.snappy` has a small bounce (VERIFIED from Apple's reference pages; their numeric default durations are not stated there).
- SwiftUI mapping (INFERENCE): `.timingCurve(0.23, 1, 0.32, 1, duration: 0.18)` for enter/exit of small surfaces; `.spring(duration: 0.25-0.35, bounce: 0)` or `.snappy` for anything draggable or interruptible; no bounce in a work tool except drag-release.

### 3.3 What should never animate

- **Keyboard-initiated actions.** They are repeated all day; animation makes them feel slow and disconnected from the keypress. (Emil, VERIFIED.)
- **High-frequency, low-novelty actions.** Command menus, context menus, adding or removing list items, trivial hover. Raycast has no open animation; macOS context menus appear with no motion. (Emil; Rauno Freiberg, https://rauno.me/craft/interaction-design and https://interfaces.rauno.me/: VERIFIED.) Apple HIG says the same: generally avoid adding motion to frequent interactions, and do not make people wait for an animation to finish before they can act. https://developer.apple.com/design/human-interface-guidelines/motion (VERIFIED).
- **Theme switches** must not trigger transitions. (Rauno, VERIFIED.)
- **Layout properties.** Animate transform and opacity only; never `transition: all`. (Vercel guidelines; Emil: VERIFIED.)

INFERENCE for the agent chat: send, session switch (click or shortcut), new chat, command palette, queue-by-Enter, answer-by-keyboard, and Esc-to-stop get no entrance animation. Mouse-initiated disclosure (tool card expand, queue drag reorder, plan panel open) may animate at 150-200 ms ease-out or a bounce-0 spring.

### 3.4 Interruptibility and press feedback

- Animations must be interruptible and retargetable. Prefer transitions or springs to keyframes, because keyframes cannot be redirected mid-flight and elements added in quick succession jump. (Emil toast article; Vercel guidelines: VERIFIED.)
- Press feedback: scale to about 0.97 on press, immediately. Do not scale to 0.8. Never animate in from scale 0; start around 0.9-0.95 with opacity. Popovers scale from their trigger (origin-aware). (Emil 7 tips; Rauno: VERIFIED.)
- Lightweight actions can fire during a gesture; destructive ones wait for release. (Rauno, VERIFIED.)
- Toggles take effect immediately with no confirmation. (Rauno, VERIFIED.)
- Looping animations pause when off-screen. (Rauno, VERIFIED.) Applies to shimmer in background sessions and scrolled-away rows.

### 3.5 Reduced motion

Apple HIG accessibility: when Reduce Motion is on, reduce automatic and repetitive animations including scaling and peripheral motion; tighten springs to remove bounce; replace x/y/z transitions with fades; avoid animating into and out of blurs; never use motion as the only carrier of information. https://developer.apple.com/design/human-interface-guidelines/accessibility and /motion (VERIFIED). Vercel: honor `prefers-reduced-motion` with a reduced variant (VERIFIED).

INFERENCE: shimmer is a repetitive automatic animation. Under Reduce Motion it becomes static secondary-colored text (optionally a slow opacity change), token reveal becomes plain append, and disclosure becomes a cross-fade or nothing.

### 3.6 macOS conventions that bear on feel

- A spinner belongs next to a specific control or in a constrained area, unlabelled; determinate wherever duration is known; consistent placement. (HIG progress-indicators, VERIFIED.)
- Vercel form guidance that transfers directly: Enter submits a single-field form; never block typing; keep submit enabled until the request starts; never pre-disable submit; inputs must not lose focus or value across a re-render. (VERIFIED.)
- The Browser Company reportedly moved Dia's Mac UI to AppKit only because SwiftUI use on Mac consistently regressed performance (UNVERIFIED: secondary reports, https://mjtsai.com/blog/?p=47898). Relevant as a risk flag for a SwiftUI transcript under streaming load, not as proof.

---

## 4. Closed-source benchmark teardown

Published engineering detail on client-side feel is thin for most of these products. Where nothing public exists, the row says so.

| Product | Behaviors that make it feel fast | Published detail and label |
|---|---|---|
| **ChatGPT web/desktop** | User message and a shimmering status appear immediately on send; status text changes to describe current reasoning or tool; composer stays typeable while a response streams; send control becomes stop; new chat is an immediate local view; text reveal is paced. | OpenAI's own latency guide prescribes streaming, showing steps, and loading states (VERIFIED, https://developers.openai.com/api/docs/guides/latency-optimization). All client behaviors listed are UNVERIFIED/observational; I found no OpenAI post on the shimmer or token pacing implementation. A developer writeup describes ChatGPT pinning the just-sent user message near the top and reserving space below for the reply via dynamic bottom padding (UNVERIFIED, https://medium.com/@disgcfrguy/the-scroll-problem-nobody-talks-about-when-building-ai-chat-interface-987c223cafc0). OpenAI community threads show the cost of getting scroll wrong: recurring complaints about scroll jumps and lost position (UNVERIFIED, https://community.openai.com/t/chatgpt-scrolling-badly-broken-after-recent-changes-to-thread-loading/1402561). |
| **Claude Code desktop** | Typing a correction and pressing Enter while Claude works sends it without stopping the running action; it is read when the current action completes. Esc or the stop button interrupts. New session via Cmd+N; sessions switch with Ctrl+Tab; optional per-session worktree under `.claude/worktrees/`. Ghost-text prompt suggestions accepted with Tab and never sent by a bare Enter. OS notification when a session finishes and you are not viewing it. Side chat that does not pollute the main thread. Three transcript verbosity modes. | VERIFIED, https://code.claude.com/docs/en/desktop. No published detail on rendering, optimistic handling, or streaming cadence. claude.ai behaviors (immediate message echo, animated mark while thinking, paced text) are UNVERIFIED/observational. |
| **Cursor** | Follow-ups can be queued while the agent runs, reordered, and started without waiting (1.2). Opt+Enter queues; Cmd+Enter interrupts and sends now; messages are delivered at the next suitable point, usually after a tool call, instead of at end of generation; default configurable (1.4). | VERIFIED, https://cursor.com/changelog/1-2 and https://cursor.com/changelog/1-4. Engineering: Tab completions ~100 ms faster and TTFT down 30% in 1.2 (VERIFIED, same page). Fast-apply at ~1000 tokens/s via speculative edits, motivated by slow edits breaking flow (VERIFIED, https://cursor.com/blog/instant-apply). Tab model server p50 from 475 ms to 260 ms (UNVERIFIED, search excerpt of https://cursor.com/blog/tab-update). Forum reports show queue failure modes worth avoiding: messages stuck queued, queued messages acted on early (UNVERIFIED, https://forum.cursor.com/t/messages-always-queueing-now/151759). |
| **OpenAI Codex app** | Many agents in parallel, each thread streaming independently; worktree, local, and cloud modes; follow-up while running can steer the active turn or queue for the next. | Protocol is public: thread/turn/item primitives; `turn/start` returns immediately with status in progress and no items; UI is driven by `item/started`, deltas, `item/completed`; each item type is its own UI element; `turn/steer` appends to the active turn and requires the expected turn id; `turn/interrupt` returns an empty success and the UI must update from `turn/completed` with status interrupted; approvals are server-initiated requests ordered as pending item, then request, then `serverRequest/resolved`, then final item, and are also cleared by turn start, completion, or interruption; history can be paged separately from resume (VERIFIED, https://learn.chatgpt.com/docs/app-server, redirected from developers.openai.com/codex/app-server). Thread, turn, and item ids are server-issued in this protocol, so a client must hold its own stable key for optimistic rows (VERIFIED for the id ownership; the consequence is INFERENCE). The engineering post "Unlocking the Codex harness" returned HTTP 403 (UNVERIFIED, https://openai.com/index/unlocking-the-codex-harness/). Enter-steers / Tab-queues in the CLI and a "Follow-up behavior" setting in the app are UNVERIFIED (third-party posts). |
| **Conductor** | Workspace equals a git worktree plus a setup script that starts as soon as the workspace is created; setup logs visible and stoppable. Queue redesigned: any queued message can be edited, deleted, or sent now; the queue pauses while the user edits; partial messages survive edits. "Instant archiving". Relaunch reopens the last visible workspace. | Queue redesign VERIFIED, https://www.conductor.build/changelog/0.54.0-new-queue-diff-diffs. "Instant Archiving" VERIFIED as a release title only (0.46.0). Drag-to-reorder queue, queued items explaining why they wait (setup or attachment processing), and a fixed bug where messages could not be sent during setup are UNVERIFIED (search excerpts of the changelog). Many releases titled performance improvements, including long-chat performance and CPU use (VERIFIED titles, https://www.conductor.build/changelog), which signals long transcripts are a known hot spot. |
| **Raycast** | Window appears with no open animation; results on first keystroke; everything keyboard-first. | v2 deep dive (VERIFIED, https://www.raycast.com/blog/a-technical-deep-dive-into-the-new-raycast): windows are ordered front while still invisible so the web view is not throttled; rendering is triggered just before showing and the window becomes visible only after the web view has drawn, with a stated goal of no flicker when views appear; popovers and tooltips are separate native windows; v1 kept AI Chat and Notes windows in memory after first use, v2 tears idle ones down and plans grace periods to keep them warm; resize uses implicit Core Animation with the content pre-drawn at the expanded size. No ms targets are published. "No open animation" is corroborated by Emil Kowalski and Rauno Freiberg (VERIFIED as their observation). |
| **Linear** | No spinners on navigation or mutation; edits apply instantly; works offline; undo everywhere. | Sync engine mechanics in 2.1 (VERIFIED-2ND). Linear's own blog page has no technical text. |
| **Superhuman** | 100 ms rule with a 50 ms aim; local mail store; preload and prerender likely-next threads; minimal animation; keyboard-first; measures share of interactions under 100 ms and under 50 ms. | VERIFIED, links in 1.1. |
| **Arc / Dia** | Dia positioned as rebuilt for responsiveness. | UNVERIFIED secondary: dropped SwiftUI and TCA on Mac for performance (https://mjtsai.com/blog/?p=47898). No published detail on chat streaming. |
| **Warp** | Terminal output and UI rendered on the GPU; input never waits on layout of scrollback. | Official post reports rendering above 144 FPS and an average redraw of 1.9 ms, using Rust and Metal with glyphs rasterized once and minimal draw calls (VERIFIED-2ND: read as an excerpt of https://www.warp.dev/blog/how-warp-works via search, page not fetched directly). |
| **Granola** | Notes are typeable instantly during a meeting; enhancement streams into place. | No public engineering source found. UNVERIFIED/observational only. |

What the benchmarks converge on (INFERENCE):

1. The send path is local: message, cleared composer, and a working indicator are all on screen before any network round trip.
2. Input is never disabled by agent state. While the agent runs, Enter either steers or queues; both are immediate local operations, and the choice is explicit and configurable (Cursor, Codex, Claude Code desktop, Conductor all ship both).
3. Queues are first-class, editable, reorderable, and say why an item is waiting.
4. The stream is the source of truth for system state. RPC replies are not used to flip UI state (Codex protocol makes this explicit for interrupt).
5. Setup work (worktree, scripts) runs in the background of an already-usable session and is shown as inline steps, not a modal.
6. Keyboard-initiated surfaces do not animate; windows are prewarmed and shown only once drawn.

---

## 5. Feel spec for an agent chat app (review criteria)

How to use this section: 5.1 gives numeric budgets a reviewer can measure; 5.2 says what may be on screen in each state; 5.3 lists layout rules; 5.4 is a pass/fail checklist to run against the PR. Targets are INFERENCE grounded in sections 1-4; the grounding is cited per row.

### 5.0 Operating rules

1. **Intent is instant, facts are confirmed.** Anything the user did renders in the same frame. Anything the system did renders when the event stream says so, and is worded as in progress until then.
2. **Input is never blocked by network, connection, setup, or agent state.**
3. **Identity is the client id, for life.**
4. **The default loading state is the previous content.** Indicators are a fallback for when there is nothing to show.
5. **The threshold ladder** for any wait:

| Elapsed | Show |
|---|---|
| 0-100 ms | The result only. |
| 100-200 ms | Still nothing new. Keep prior content; selection or pressed state already changed at 0 ms. |
| 200 ms-1 s | If prior content exists: keep it, optionally dim it. If nothing exists: skeleton matching final layout. Once shown, hold at least 300 ms. |
| 1-10 s | Shimmer status text that names the real activity. Stop/cancel reachable. Elapsed time from about 5 s. |
| Over 10 s | Step list or determinate progress, elapsed time, cancel, and the user can leave (background plus notification on completion). |

Grounding: Nielsen 0.1/1/10 s; RAIL; Vercel 150-300 ms delay and 300-500 ms minimum; React 300 ms throttle; NN/g no indicator under 1 s and percent-done past 10 s.

### 5.1 Latency budget table

"Target" is what p50 should hit; "ceiling" is the p95 line a reviewer should treat as a defect when exceeded. Measure from the input event's own timestamp to the frame that presents the change (Superhuman method; on macOS, `NSEvent.timestamp` to the committing `CATransaction`, recorded with signposts). Track the share of interactions under 50 ms and under 100 ms, not only percentiles.

| # | Interaction | Target | Ceiling | What is on screen at each point | Grounding |
|---|---|---|---|---|---|
| 1 | Keypress to glyph in composer | 1 frame (8-16 ms) of app time | 33 ms; never a 100 ms hitch | Character. Nothing else may run synchronously per keystroke: no transcript layout, no Markdown reparse, no draft write to disk on the main thread, no network. Must hold while a response is streaming. | Apple hang doc pipeline; Fatin; RAIL 50 ms |
| 2 | Mouse-down to pressed state | 1 frame | 50 ms | Pressed state (scale ~0.97 or highlight) on down, action on up. | Emil; RAIL |
| 3 | Enter to user message in transcript | 16-50 ms | 100 ms | Message at full fidelity, composer empty, focus still in composer, transcript scrolled to show it. All in one frame. No dependency on ack. | Nielsen 100 ms; Superhuman 50 ms aim |
| 4 | Enter to working indicator | Same frame as #3 | 100 ms | Shimmer status line in the slot where the reply will start; send control becomes stop. Shown immediately (not delay-gated) because TTFT always exceeds the delay window and this is the acknowledgement. | 1.5 latency masking; OpenAI guide |
| 5 | Enter to durable acceptance (ack) | 50-300 ms | 2 s | Under 2 s: nothing extra. At 2 s unacked: quiet per-message "Sending" caption in reserved space. On error or 10 s: inline "Not sent" with Retry and Edit. | Mishunov 2 s; WhatsApp clock; Stream SDK |
| 6 | First token (TTFT), app overhead | Provider TTFT + at most 50 ms | Provider TTFT + 100 ms | 0-1 s: generic shimmer ("Thinking"). From first agent event: specific real status (reasoning summary, tool name and target). From ~5 s: elapsed time. Past 10 s with no events: still shimmer plus elapsed; never a frozen screen. First text replaces the status in place with no height change. | OpenAI guide; arXiv 2604.06183; Nielsen 10 s |
| 7 | Token cadence while streaming | One layout per display frame; reveal lag 100-250 ms behind the wire | No frame over 2x display interval; no 250 ms main-thread hang | Evenly paced text; backlog-proportional catch-up; complete flush within ~150 ms of end of stream; tool events bypass the smoother. | AI SDK smoothStream; Apple 250 ms hang; mux PR |
| 8 | Session switch, session in memory or local store | 16-50 ms | 100 ms | Full last-known transcript at restored scroll position, composer with that session's draft, focus in composer. No animation, no skeleton, no blank frame. Live updates merge in afterwards. | Superhuman preload; Linear local pool; Emil no keyboard animation |
| 9 | Session switch, cold (not loaded) | First paint of header + cached tail within 100 ms | 1 s to complete | 0-200 ms: selection moves, header and whatever is cached. 200 ms+: skeleton rows matching message layout, minimum 300 ms. Never a centered spinner. Composer usable throughout. | 1.3 rules; HIG show something as soon as possible |
| 10 | New chat (Cmd+N or click) | 16-50 ms | 100 ms | Empty transcript, focused composer, sidebar row under a client id. No network, no worktree work on this path. No animation. | Raycast; Claude Code desktop Cmd+N |
| 11 | Create session with worktree | UI as #10; background setup as long as it takes | UI 100 ms | Session usable at once. Setup rendered inline as named steps with per-step completion (determinate when the step count is known); first prompt accepted and held with reason "Waiting for setup". Not modal. | Conductor; HIG let people do other things |
| 12 | Queue a follow-up (Enter while running) | 16-50 ms | 100 ms | Item in queue, composer cleared, focus kept. | Cursor, Conductor |
| 13 | Queue reorder by drag | Tracks pointer every frame; drop commits in 1 frame | 33 ms per frame | Live reordering under the pointer; neighbors move with an interruptible spring; no sync indicator. | RAIL animation; WWDC springs |
| 14 | Steer | 16-50 ms | 100 ms | Message in transcript under the active turn with intent caption. | Codex `turn/steer`; Claude Code desktop |
| 15 | Cancel (Esc or stop) acknowledgement | 16-50 ms | 100 ms | Control reads "Stopping", shimmer and reveal freeze, composer usable. Confirmed stop expected within 1 s; "Stopping" plus elapsed after 2 s; force-stop offered after 10 s. | Codex interrupt semantics; Nielsen |
| 16 | Answer permission or question | 16-50 ms | 100 ms | Prompt collapses to chosen-answer record; status returns to working. | 2.3 |
| 17 | Arrival of a permission or question prompt | Rendered within 100 ms of the event | 200 ms | Appears in a reserved or bottom-anchored region without moving the text being read and without taking keyboard focus from the composer. | 5.3 rules 12 and 13 |
| 18 | Plan edit keystroke | As #1 | As #1 | Text. Save is silent and debounced. | 2.3 |
| 19 | Rename, pin, archive | 16-50 ms | 100 ms | Final state immediately. Archive offers Undo. | Linear; Vercel Undo |
| 20 | Tool card expand or collapse | Starts in 1 frame; 150-200 ms ease-out or bounce-0 spring if mouse-initiated; instant if keyboard-initiated | 250 ms total | Card's header stays under the pointer; content below moves; nothing above moves. | Emil; Rauno; 5.3 rule 6 |
| 21 | Command palette / search open | 16-50 ms | 100 ms | Focused field, no animation; results from local index on first keystroke. | Raycast; Emil |
| 22 | Scroll through transcript | Every frame on time (8.3 ms at 120 Hz) | No dropped-frame runs while streaming | Content; no blank cells, no height pops. | RAIL; Warp |
| 23 | App launch to usable last session | Under 500 ms warm | 1 s | Last-known window state from local store, composer focused; live reconnect happens behind it with no indicator under 1 s. | Nielsen 1 s; Linear bootstrap |
| 24 | Reconnect after transport drop | n/a | n/a | Under 1 s: nothing. After: small text status in the session header ("Reconnecting"); composer stays enabled; sends go to the pending log; transcript stays. | Rule 2; HIG explain stalls |

### 5.2 State-by-state affordance table

Allowed affordances, in order of preference: **Nothing** (prior or final content only), **Shimmer text** (status wording with a moving highlight, in a reserved slot), **Skeleton** (layout-matched placeholder), **Determinate** (bar, ring, or n-of-m steps), **Spinner** (small indeterminate, attached to a control).

| State | Affordance | Notes |
|---|---|---|
| Prompt sent, ack pending under 2 s | Nothing | Message looks sent because the user did send it. |
| Prompt ack pending over 2 s | Text caption on the message | "Sending" in reserved space; no icon animation. |
| Prompt failed | Inline failed state | Retry, Edit; persists across relaunch. |
| Waiting for first agent event | Shimmer text | Generic wording only until a real event arrives. |
| Reasoning / thinking | Shimmer text | Real summary when available; elapsed from ~5 s. |
| Streaming answer text | Nothing | The text is the indicator. No caret blink loop needed; no spinner beside it. |
| Tool call running, under ~300 ms | Nothing, then the completed card | Do not flash a running state for fast tools; apply the 200 ms delay and 300 ms minimum to the running treatment. |
| Tool call running, 300 ms-10 s | Shimmer text on the card title | Title names tool and target. |
| Tool call running, over 10 s (tests, builds, installs) | Shimmer title + elapsed + live output tail; Determinate if a count exists | Stop control reachable. |
| Subagent / background task running | Shimmer text row with elapsed | Collapsed by default. |
| Context compaction | Shimmer text with elapsed | It is a real, nameable step. |
| Session connecting, under 1 s | Nothing | |
| Session connecting or reconnecting, over 1 s | Text status in header | Composer enabled. |
| Session / worktree creation | Determinate step list inline | Current step may shimmer; completed steps get a static check. |
| Setup script running | Step row with elapsed and expandable log | Not modal; cancellable. |
| Session switch, cached | Nothing | |
| Session switch, cold, over 200 ms | Skeleton | Layout-matched, 300 ms minimum. |
| Older history paging in on scroll-up | Skeleton rows of known or estimated height at the top | Anchor preserved; no spinner row that changes height on completion. |
| Sidebar session list, first ever load | Skeleton | Only when there is no cached list. |
| Sidebar session list, refresh | Nothing | Merge by id; keep stale rows. |
| Sidebar row for a session that is working | Small static-shape activity mark (pulsing dot or shimmer on status word) | Paused when off-screen and under Reduce Motion. |
| Sidebar row needing attention (question/permission) | Static badge | Never animated continuously. |
| Queued item awaiting its turn | Nothing | It is in a queue; that is its status. If blocked, a text reason. |
| Queue mutation syncing | Nothing | |
| Steering message not yet consumed | Text caption | |
| Cancel requested | Text on the control ("Stopping") | Shimmer elsewhere stops. |
| Permission / question awaiting user | Static prompt | It is the user's turn; nothing should animate. |
| Answer submitted, confirmation pending | Nothing | |
| Plan saving | Nothing | "Not saved" text only on failure. |
| Rename, pin, archive pending | Nothing | |
| Attachment upload or processing | Determinate ring or bar on the chip | Send stays enabled; message is held until ready with a reason. |
| Model or mode switch | Nothing | Applies to the next turn. |
| Push, create PR, merge, sign-in, other non-optimistic external action | **Spinner inside the button**, label kept, 200 ms delay, 300 ms minimum | The justified spinner case. |
| App update download, large clone, dependency install with known size | Determinate | |
| Search over transcripts | Nothing under 200 ms; then keep prior results dimmed | Never clear results while typing. |

**Where spinners remain justified** (everything else should be one of the other four):

1. Inside a button for a user-initiated, non-optimistic, indeterminate external action (push, create PR, sign in), with the label kept and the delay/minimum rule applied. Grounding: HIG says spinners suit a small area next to a control; Vercel says loading buttons keep their label.
2. An attachment or remote fetch whose size is unknown, on the chip itself.
3. A system-standard toolbar or status-bar background-activity indicator, if macOS convention calls for one.

**Never**: a spinner centered in a pane; a spinner that replaces content that was already on screen; a modal or sheet with a spinner; a spinner next to streaming text; a spinner for any optimistic mutation; a spinner that appears and disappears inside 300 ms.

### 5.3 Layout stability rules

1. **One identity per entity.** List identity is the client id from creation to deletion. Server ids arrive as attributes. Promotion from pending to confirmed is a property change on the same row, not a remove-and-insert. (Linear, Figma, Replicache.)
2. **Merge, never replace.** Reloads, reconnects, and resyncs diff into the existing collection by id. A fetch that returns the same items produces zero view changes. Selection, scroll offset, expanded states, and drafts survive.
3. **Atomic reconcile.** Server patch plus replay of still-pending mutations are applied in one update so no intermediate state is rendered. While a local change to a field is unacknowledged, conflicting server values for that field are not shown.
4. **Reserved slots for transient status.** The working indicator, per-message "Sending" caption, and "Stopping" text live in space that exists whether or not they are visible. First token replaces the status line in place. Showing or hiding status changes opacity, not height.
5. **Streaming appends only below.** New tokens may grow the last block downward. They must never change the height or position of anything above the insertion point. Late-arriving syntax highlighting, fonts, and images must not change line height or block height; reserve dimensions.
6. **Disclosure anchors at the control.** Expanding or collapsing a tool card, thinking block, or diff keeps the clicked header at the same screen position. If the card is above the viewport's anchor, compensate scroll offset in the same frame.
7. **Bottom pinning is conditional.** Follow the stream only when the user is within a small threshold of the bottom. Any upward user scroll releases the pin in that frame and it stays released until the user returns to the bottom or sends. When released, show a "jump to latest" control instead of moving the view. Programmatic scroll must be distinguishable from user scroll. (use-stick-to-bottom.)
8. **Send has one deterministic scroll outcome.** After Enter, the new message is visible and the view is pinned. Choose one model (stick-to-bottom, or sent-message-near-top with reserved space below) and apply it everywhere.
9. **Stable block type during streaming.** A block that will be a code fence, table, or list renders as that type from its first token, including when the closing syntax has not arrived. No paragraph-to-code-block flips. (Streamdown.)
10. **Skeletons match final geometry.** Same row heights, paddings, and line counts as the content they stand in for. (Vercel guidelines.)
11. **Composer growth does not move reading position.** When the composer grows, queue chips appear, or a prompt region opens, the transcript's visible content keeps its relationship to the pin (bottom-pinned stays bottom-pinned; released stays where the user left it).
12. **Focus is sticky.** Composer focus survives send, queue, session switch, new chat, answer, and reconnect. Popovers and sheets return focus to where it was. Re-renders never drop focus, selection, IME composition, or undo history in the composer or plan editor.
13. **Arriving prompts never capture in-flight input.** A permission or question that appears while the user is typing must not take first responder, and its default action must not be bound to a key the user may already be pressing (Enter). Require an explicit focus move or a modifier.
14. **Per-session draft persistence.** Each session's composer text, attachments, and caret survive session switch and relaunch.
15. **No list reflow from asynchronous metadata.** Auto-generated titles, branch names, token counts, and timestamps fill fixed-width or reserved areas, or cross-fade in place; they do not reorder or resize rows after first paint, except that an explicit recency sort may move a row once per new activity.
16. **Motion budget.** Keyboard-initiated actions do not animate. Mouse-initiated disclosure is 150-200 ms ease-out or a bounce-0 spring and interruptible. Only transform and opacity (or their Core Animation equivalents) animate; shimmer runs off the main thread, pauses off-screen, and is static under Reduce Motion.

### 5.4 Anti-pattern checklist for PR review

Each line is a defect if found. Tags: [B] blocks the "feels good" bar outright; [M] should be fixed before merge; [P] polish.

**Input and composer**

- [ ] [B] Composer or send is disabled while connecting, creating the session, running setup, reconnecting, or while the agent is working.
- [ ] [B] Enter does nothing, or text is lost, when pressed before the session or transport is ready.
- [ ] [B] Composer loses focus after send, after queueing, after answering a prompt, or after switching sessions.
- [ ] [B] Typing drops frames or lags while a response is streaming (per-keystroke work touches the transcript, or streaming updates invalidate the composer).
- [ ] [M] Composer text is cleared only after the server acks, or is restored late on failure instead of being preserved on the failed item.
- [ ] [M] Drafts are not per session, or do not survive relaunch.
- [ ] [M] IME composition, undo history, or caret position is broken by a re-render of the composer or plan editor.
- [ ] [M] A newly arrived permission or question prompt takes keyboard focus, or binds Enter while the user is typing.
- [ ] [P] Send button is pre-disabled on empty input in a way that also blocks attachments-only or slash-command sends.

**Identity and reconciliation**

- [ ] [B] A row or view is keyed by a server id that does not exist at creation time, so it is torn down and rebuilt when the id arrives (visible as flash, scroll reset, lost focus, replayed entrance animation, or collapsed tool cards).
- [ ] [B] Temporary id swap changes the selected session, navigation path, or window title, or drops the draft.
- [ ] [B] Transcript or sidebar is cleared and rebuilt on reload, reconnect, resume, or refetch.
- [ ] [M] Sent message appears twice (optimistic copy plus server echo) or flickers out and back.
- [ ] [M] Server echo of an older value overwrites a newer local edit (title flips back, queue order snaps back, plan text reverts).
- [ ] [M] Mutations have no client-generated id or idempotency key; retry or reconnect can duplicate a prompt or queue item.
- [ ] [M] UI state flips on the RPC reply instead of on the event stream (for example, marking a turn cancelled when the interrupt call returns).
- [ ] [M] Pending mutations live only in memory; a crash between Enter and ack loses the prompt.
- [ ] [M] Queue reorder racing with the agent dequeuing the item is unhandled (item vanishes, duplicates, or an in-progress edit is lost).
- [ ] [P] Auto-generated title overwrites a manual rename.

**Loading indicators**

- [ ] [B] Modal, sheet, or full-pane blocking progress for session creation, worktree creation, or setup scripts.
- [ ] [B] Centered spinner replaces a transcript or list that already had content.
- [ ] [M] Any indicator that can flash for less than 300 ms (no show delay, no minimum display).
- [ ] [M] Spinner used where the data is available locally (session switch, sidebar refresh, back navigation).
- [ ] [M] Spinner shown for an optimistic mutation (rename, pin, archive, queue edit, answer).
- [ ] [M] Generic "Loading" or "Working" text where a real activity name is available from the event stream.
- [ ] [M] A wait that can exceed 10 s with no elapsed time, no description of what is happening, or no cancel.
- [ ] [M] Skeleton whose geometry differs from the final content, causing a shift on swap.
- [ ] [P] Spinner and progress bar swapped for the same operation mid-flight.
- [ ] [P] Running-state treatment flashes on tool calls that finish in under 300 ms.

**Layout and scroll**

- [ ] [B] Scroll position jumps when a tool card, thinking block, or diff expands or collapses.
- [ ] [B] View is yanked to the bottom while the user has scrolled up to read.
- [ ] [B] Content above the streaming point shifts during streaming.
- [ ] [M] Working indicator appearing or disappearing changes transcript height (visible bounce at first token or at end of turn).
- [ ] [M] Partially streamed Markdown renders as the wrong block type and then reflows (unterminated code fence, table, list).
- [ ] [M] Late syntax highlighting, images, or fonts change block heights after first paint.
- [ ] [M] Scroll position is not restored per session on switch.
- [ ] [M] Loading older history changes what is under the user's eye.
- [ ] [M] Sidebar rows reorder or resize as metadata trickles in.
- [ ] [P] No "jump to latest" control when unpinned during streaming.

**Motion**

- [ ] [M] Entrance animation on keyboard-initiated actions: send, session switch, new chat, command palette, Esc.
- [ ] [M] Animation that must finish before the next input is accepted (non-interruptible).
- [ ] [M] Shimmer or other looping animation driven on the main thread per frame, or left running off-screen or in background sessions.
- [ ] [M] Reduce Motion not honored for shimmer, token fade-in, and disclosure.
- [ ] [P] Durations over 300 ms for product UI; ease-in or linear on enter/exit; bounce in non-gesture UI; scale-from-zero entrances.
- [ ] [P] No pressed state on mouse-down, or action fires on mouse-down for a destructive control.

**Streaming**

- [ ] [B] More than one layout pass per display frame while streaming, or a full transcript re-layout or full Markdown reparse per token.
- [ ] [M] Bursty reveal: tokens painted exactly as network chunks arrive, with visible stop-start on long answers.
- [ ] [M] Smoothing that adds unbounded lag, or delays the end of the response.
- [ ] [M] Tool and status events delayed by the text smoother.
- [ ] [M] Background sessions do full view work for every token.
- [ ] [P] Long transcripts are not virtualized or otherwise bounded.

**Honesty and failure**

- [ ] [B] A failed send disappears, or is reported only in a toast or log.
- [ ] [M] System facts asserted before confirmation ("Cancelled", "Worktree created", "Pushed", "Running").
- [ ] [M] Failed items lack Retry and Edit, or Retry can duplicate.
- [ ] [M] Stuck pending state with no timeout path (no change after 2 s, nothing actionable after 10 s).
- [ ] [M] Destructive, irreversible action applied optimistically, or reversible action gated by a confirmation dialog instead of Undo.
- [ ] [M] Concurrent agent revision silently overwrites the user's plan edits, or the reverse.
- [ ] [P] Queued items do not say why they are waiting when blocked by setup or attachments.

**Instrumentation**

- [ ] [M] No measurement exists for rows 1, 3, 4, 8, 10, and 15 of the budget table (signposts from event timestamp to presenting frame; share under 50 ms and under 100 ms).
- [ ] [M] No hang detection at a threshold tighter than 250 ms during streaming.
- [ ] [P] No fixture for a long transcript with many tool cards to test scroll and switch latency.

### 5.5 Reviewer quick tests (manual, about ten minutes)

1. With the network disabled or the daemon stopped, type and press Enter in a new chat. Expect: message visible, composer cleared and focused, a pending caption at about 2 s, a failed state with Retry later. Re-enable and Retry: exactly one message.
2. Press Cmd+N and start typing in the same motion. Expect: no dropped characters, no animation.
3. Create a worktree-backed session and send a prompt during setup. Expect: prompt accepted and held with a reason; no modal.
4. While a long answer streams: type in the composer (no lag), scroll up (view stays put), expand a tool card above the fold (header stays under the pointer), press Esc (control reads "Stopping" within a frame).
5. Switch among three sessions rapidly with the keyboard. Expect: each paints fully within one or two frames at its own scroll position with its own draft; no skeleton, no blank frame.
6. Queue three follow-ups, reorder them by drag, edit the second, delete the third, as the current turn ends. Expect: no loss, no duplication, no snap-back.
7. Trigger a permission prompt while typing a follow-up and keep typing through its arrival. Expect: keystrokes land in the composer; nothing is approved.
8. Record the screen at 120 fps during send and first token. Step through frames looking for: blank frames, a height bounce when status becomes text, a re-inserted user message, replayed entrance animations.
9. Turn on Reduce Motion. Expect: no shimmer movement, no slide or scale transitions.
10. Kill and relaunch mid-stream. Expect: last-known transcript and draft visible within a second; stream resumes by merge, not by reset.

---

## Source index

Thresholds and indicators
- https://www.nngroup.com/articles/response-times-3-important-limits/
- https://www.nngroup.com/articles/progress-indicators/
- https://www.nngroup.com/articles/skeleton-screens/
- https://web.dev/articles/rail
- https://web.dev/articles/inp
- https://lawsofux.com/doherty-threshold/
- https://developer.apple.com/documentation/xcode/understanding-hangs-in-your-app
- https://pavelfatin.com/typing-with-pleasure/
- https://www.lukew.com/ff/entry.asp?1797
- https://github.com/facebookarchive/Shimmer
- https://github.com/coder/mux/pull/986
- https://www.chrisharrison.net/index.php/Research/ProgressBars2
- https://raw.githubusercontent.com/androidx/androidx/androidx-main/core/core/src/main/java/androidx/core/widget/ContentLoadingProgressBar.java
- https://github.com/smeijer/spin-delay
- https://github.com/facebook/react/blob/main/packages/react-reconciler/src/ReactFiberWorkLoop.js
- https://www.viget.com/articles/a-bone-to-pick-with-skeleton-screens/ (not retrievable, HTTP 429)
- Mejtoft et al. 2018, doi 10.1145/3232078.3232086 (not read in full)

Optimistic UI and sync
- https://www.smashingmagazine.com/2016/11/true-lies-of-optimistic-user-interfaces/
- https://speakerdeck.com/mikeyk/secrets-to-lightning-fast-mobile-design
- https://react.dev/reference/react/useOptimistic
- https://tanstack.com/query/latest/docs/framework/react/guides/optimistic-updates
- https://tanstack.com/db/latest/docs/overview
- https://doc.replicache.dev/concepts/how-it-works
- https://zero.rocicorp.dev/docs/writing-data
- https://github.com/wzhudev/reverse-linear-sync-engine
- https://www.fujimon.com/blog/linear-sync-engine
- https://linear.app/blog/scaling-the-linear-sync-engine
- https://www.figma.com/blog/how-figmas-multiplayer-technology-works/
- https://blog.superhuman.com/superhuman-is-built-for-speed/
- https://blog.superhuman.com/performance-metrics-for-blazingly-fast-web-apps/
- https://faq.whatsapp.com/665923838265756
- https://getstream.io/chat/docs/sdk/react/state/writing-messages/message-operations/

LLM streaming
- https://developers.openai.com/api/docs/guides/latency-optimization
- https://arxiv.org/abs/2604.06183
- https://ai-sdk.dev/docs/reference/ai-sdk-core/smooth-stream
- https://streamdown.ai/docs
- https://github.com/stackblitz-labs/use-stick-to-bottom

Motion and craft
- https://emilkowal.ski/ui/great-animations
- https://emilkowal.ski/ui/7-practical-animation-tips
- https://emilkowal.ski/ui/you-dont-need-animations
- https://emilkowal.ski/ui/good-vs-great-animations
- https://emilkowal.ski/ui/building-a-toast-component
- https://emilkowal.ski/ui/building-a-drawer-component
- https://vercel.com/design/guidelines
- https://interfaces.rauno.me/
- https://rauno.me/craft/interaction-design
- https://developer.apple.com/design/human-interface-guidelines/loading
- https://developer.apple.com/design/human-interface-guidelines/progress-indicators
- https://developer.apple.com/design/human-interface-guidelines/motion
- https://developer.apple.com/design/human-interface-guidelines/accessibility
- https://developer.apple.com/videos/play/wwdc2023/10158/
- https://github.com/material-components/material-web/blob/main/tokens/versions/v0_192/_md-sys-motion.scss
- Local, secondary: /Users/nico/.claude/skills/web-animation-design/SKILL.md and /Users/nico/.claude/skills/emil-design-engineering/animations.md

Benchmarks
- https://code.claude.com/docs/en/desktop
- https://cursor.com/changelog/1-2
- https://cursor.com/changelog/1-4
- https://cursor.com/blog/instant-apply
- https://cursor.com/blog/tab-update (search excerpt only)
- https://learn.chatgpt.com/docs/app-server
- https://openai.com/index/unlocking-the-codex-harness/ (not retrievable, HTTP 403)
- https://www.conductor.build/changelog and https://www.conductor.build/changelog/0.54.0-new-queue-diff-diffs
- https://www.raycast.com/blog/a-technical-deep-dive-into-the-new-raycast
- https://www.warp.dev/blog/how-warp-works (search excerpt only)
- https://mjtsai.com/blog/?p=47898 (secondary, Dia)

## Gaps and caveats

- No primary engineering source was found for ChatGPT's, Claude's, or Granola's client-side rendering, shimmer, or token pacing. Those rows are observational.
- No controlled study was found showing even-paced token reveal is perceived as faster than bursty reveal. The recommendation rests on adjacent evidence and shipped practice.
- The skeleton-versus-spinner literature is small and conflicting; the recommendation here leans on layout stability and on avoiding loading states through local data, not on a perceived-speed claim for skeletons.
- Viget's study, OpenAI's Codex harness post, Cursor's tab-update post, and Warp's post were not read directly in this session.
- Budget numbers in 5.1 are targets derived from the cited thresholds, not measurements of Memex or of any benchmark app.
