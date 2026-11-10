# OpenCode: chat-flow UX techniques, verified in source

Repo: `github.com/sst/opencode` (redirects to `github.com/anomalyco/opencode`). Shallow clone at
`df54d0cddff3abe5d3d12692f306aca44cc3f13f` (2026-11-09, "refactor(core): give each warmed session one owned loop (#62972)").
Clone path: `/private/tmp/claude-501/-Volumes-Dev-Code-memex--claude-worktrees-agent-app-ux-patterns-13a76b/f8deb1e4-a002-4452-a81e-c8ef8a51001b/scratchpad/repos/opencode`.
All paths below are relative to `packages/` in that clone unless they start with another root.

Labels: **VERIFIED** = read in source at the SHA above (path:line). **UNVERIFIED** = from a PR/issue body or title via `gh`, not re-derived from code.

Layout has changed from what the assignment assumed. Current packages: `core` (runtime), `server` (HTTP+SSE), `schema`/`protocol` (wire contracts), `client` (generated API + the SolidJS sync store), `app` (SolidJS GUI), `session-ui` (transcript components), `ui` (design system), `desktop` (Electron shell, not Tauri), `tui` (opentui), `cli` (incl. ACP), `patchmark` (their own streaming markdown block parser).

---

## 1. TL;DR

- The data model is no longer "message + parts with part deltas". It is a per-session **durable event log** (`aggregateID`, `seq`, `version`) plus **ephemeral** stream fragments. `session.text.delta` is ephemeral; `session.text.ended` is durable and carries the full text, so a client that drops deltas self-heals at the boundary. V1 `message.part.*` events are explicitly kept out of the current protocol.
- Prompts go through a server-side **inbox** with `delivery: "steer" | "queue"`. Admission is idempotent per client-minted, time-ascending ID (`msg_` + 48-bit ms timestamp/counter + random). The client renders the row synchronously, tracks it in an `outbox` set, and the durable `session.inbox.enqueued` echo **upserts the same ID in place**. No diffing, no temp-ID swap.
- Optimism is layered and each layer has a bounded exit: handoff (prompt shown before the data layer admits it) 30 s send / 1 s echo / 10 s resync; optimistic "running" status owned by a tiny machine that only rolls back if the *latest* dispatched prompt fails; optimistic session creation under a client-minted session ID with prompt POSTs gated on the create promise.
- Streaming render path: SSE events batched in a **16 ms** timer and applied inside one Solid `batch`; text reveal is immediate unless the backlog exceeds **512 chars**, then paced at **24 ms** steps; markdown is projected into blocks **in a Web Worker**, only blocks from the first changed index are sent back, settled blocks keep their DOM, the single live block is reconciled by key with **per-word 180 ms opacity fade** for newly mounted words only; code highlighting is an incremental line tokenizer in the worker with a stable/unstable token split.
- Timeline: TanStack Virtual, semantic rows with stable keys, per-session measurement cache (16), **keep-alive of 16 mounted session views**, hidden cold mount that measures only the tail first (3 s reveal fallback), a fixed 92 px tail lane so "Working" never shifts rows, and an explicit follow state machine (following / returning / detached / anchored) that detaches only on reader-originated upward input.
- In-progress affordances are almost all shimmer: one `TextShimmer` component (1200 ms linear sweep, 220 ms crossfade in/out, compositor-only transforms + mask), reused for "Working", every running tool title, and the new-session status line. The dot-matrix spinner is reserved for "AI is working" and is phase-synced and paused offscreen.
- They treat jank as a measurable contract: a benchmark suite (`strobe`) asserts **zero** transcript requests on warm tab switch, **zero** `layout.shift` on tab switch, and tracks main-thread work per streamed delta. `packages/app/AGENTS.md` mandates benchmarking session/timeline changes against `origin/dev`.

---

## 2. Architecture of the send -> stream -> render loop

### 2.1 Server

- **Event feed** (VERIFIED `server/src/event-feed.ts:8,48-70,76-84`): one bus observer encodes each public event once (`data: <json>\n\n`) and offers it to every subscriber's dropping queue of capacity 4,096; a subscriber that overflows is removed and failed (`SubscriberOverflowError`) rather than back-pressuring the runtime.
- **SSE endpoint** (VERIFIED `server/src/handlers/event.ts:13-34`): first frame is always `server.connected`; a `: heartbeat` comment every 15 s; headers `Cache-Control: no-cache, no-transform`, `X-Accel-Buffering: no`. Optional `types` query filter.
- **Durable vs ephemeral** (VERIFIED `schema/src/event.ts:17-56`, `schema/src/session-event.ts:397-432`): durable events carry `{aggregateID, seq, version}`. `Text.Started` durable, `Text.Delta` ephemeral ("Stream fragments are live-only; Text.Ended is the replayable full-value boundary", line 408), `Text.Ended` durable with full `text`. Same for reasoning, tool input, compaction; `Tool.Progress` and `UsageUpdated` are ephemeral.
- **IDs** (VERIFIED `schema/src/identifier.ts:8-32`, `schema/src/session-message.ts:21-27`): `msg_` + 12 hex chars of `timestamp*0x1000 + counter` + 14 random base62 chars. Lexicographic order == creation order, which the client exploits (`item.id >= event.data.to` for revert truncation, `client/src/solid/data.ts:1231-1236`). Message IDs for server-originated rows are derived from the event ID (`evt_` -> `msg_`), so client and projector agree without a round trip.
- **Inbox** (VERIFIED `schema/src/session-inbox.ts:11,44-56`): items are `user | synthetic | compaction | move`, each with `delivery: "steer" | "queue"`. Events: `session.inbox.enqueued | delivered | cancelled | delivery.changed` (all durable, `session-event.ts:213-237`).

### 2.2 Client sync layer (`client/src/solid`)

- **Connection** (VERIFIED `client/src/solid/connection.ts`): events are pushed to an array and flushed on a `setTimeout` (default 10 ms; the app passes `flushInterval: 16`, `app/src/runtime/server/client.tsx:129`) inside one `batch()` (lines 65-74). Connect timeout 2 s, reconnect delay 1 s, idle watchdog 45 s (3x the heartbeat) (32-38, 84-92). On `visibilitychange` back to visible after >20 s of silence, or on `online`, it aborts the socket and reconnects immediately with no backoff (176-180, 211-229). `pagehide` stops, `pageshow` restarts.
- **Store** (VERIFIED `client/src/solid/data.ts:124-146`): one Solid store: `session.info`, `session.active` (idle/running), `session.message[sessionID]` (array), `messageCursor`, `pending` (inbox), `permission`, `form`. A side `Map<sessionID, Map<messageID, index>>` gives O(1) row lookup for every streamed event (321, 489-557).
- **Applying events** (VERIFIED `data.ts:704-1300`): a single `switch`. Streaming handlers mutate in place through `produce` on one assistant message: `text.delta` does `text.text += delta` (1049-1053), `text.ended` replaces with the authoritative full text (1054-1058). A delta whose target row is not loaded is dropped (507-516) because the durable `ended` will fix it.
- **Resync on reconnect** (VERIFIED `data.ts:706-743`): `server.connected` refetches the active-session snapshot; lifecycle events that arrive during hydration are recorded in `activeUpdates` and **override** the snapshot before `reconcile`.
- **Racing fetch vs stream** (VERIFIED `data.ts:1590-1629`, `1820-1857`): pending and message re-fetches merge in rows still in the `outbox` so an HTTP snapshot that predates an admission cannot wipe an optimistic row; inbox events that arrive while the fetch is in flight are replayed over the snapshot.
- **Refetch coalescing** (VERIFIED `data.ts:92-94,283-295`): `settleMs = 150` trailing window so N same-key events cost one refetch. `createSync()` (206-259) dedupes loads per key with an `invalidated` flag that chains exactly one reload.
- **Pagination** (VERIFIED `data.ts:92,1820-1830,1864-1928`): first transcript read is the newest 20 messages (40 in compact mode, `app/src/runtime/server/runtime.tsx:211`); `loadMore` prepends a page; `all: true` uses 200/page and "publishes once, not once per page" (1910).
- **Retention** (VERIFIED `client/src/solid/retention.ts`, `app/src/runtime/server/runtime.tsx:234-253`): transcripts stay loaded for open tabs + 3 most recently viewed roots; others are evicted and reload on open. Unacked outbox rows survive eviction (`data.ts:621-642`).
- **Memory** (VERIFIED `data.ts:177-195`, `runtime.tsx:217-219`): `omitToolFileData: true` strips base64 payloads from tool results before they enter the store; the stub URI is rebuilt from the mime "because any slice of the original URI is a V8 sliced string that keeps the whole payload alive".

### 2.3 t=0 sequence on Enter (active session, steer)

1. **Snapshot** (VERIFIED `app/src/composer/submit.ts:260-265,297-338`): `submit()` clones the prompt and context, serializes the composer, and mints `SessionMessage.ID.create()` (line 332). A pure machine (`composer/machine.ts`) decides the path; effects run in `submit.ts:82-233`.
2. **Handoff shown** (VERIFIED `submit.ts:527-540`, `session/handoff-machine.ts:97-110`): `dispatchPrompt` synchronously calls `session.handoff.set({message, selection})`. Comment: "A steer appears in the timeline before the composer clears". This covers the async gap before the data layer admits (image data-URL encoding, `@mention` existence checks, staged-revert commit). Arms a 30 s `sending` timer.
3. **Optimistic busy** (VERIFIED `submit.ts:545,579-586`, `composer/prompt-status-machine.ts:28-48`): `dispatched` on an idle session sets status `running` immediately -> the "Working" shimmer appears in the reserved tail lane.
4. **Local admission** (VERIFIED `client/src/solid/data.ts:1739-1783,445-458`): `session.prompt` adds the ID to `outbox`, then `admitLocal` writes the pending inbox row and the transcript row in one `batch`. File-URI attachments and skills are left off the optimistic row (server-loaded); inline data URLs render immediately (1763-1767).
5. **Send chain** (VERIFIED `data.ts:426-438`, `submit.ts:786-820`): the POST waits on `[gate, in-flight session create, previous send]`, so admissions keep submission order and a prompt to a still-creating session cannot 404. Agent/model switches run in `prepare` *after* local admission ("so a steer shows as pending while the selection commits"), and an unchanged model is skipped "so a send never waits on that request" (`submit.ts:745-756`). One automatic retry with the identical ID and payload (820).
6. **Composer clears** (VERIFIED `submit.ts:377-439`): returns the cleared state so a failed send restores "only while it is unchanged".
7. **Row pinned before paint** (VERIFIED `app/src/session/timeline/virtualizer.tsx:1085-1105`): a newly added `UserMessage` row, while following, measures the mounted tail and calls `scrollToEnd()` in a microtask: "The optimistic row can paint before ResizeObserver corrects the tail estimates."
8. **POST resolves** (VERIFIED `submit.ts:553-561`, `handoff-machine.ts:129-148`): status ownership returns to `server`; handoff moves to `awaiting-echo` with a 1 s timer (30 s if disconnected).
9. **Echo** (VERIFIED `data.ts:906-915`, `session/handoff.ts:72-81`): `session.inbox.enqueued` deletes the outbox entry and `admitLocal` upserts the same ID with the durable payload; handoff is cleared. If the echo is missed, the handoff machine invalidates and reloads session + inbox + messages, bounded at 10 s (`handoff.ts:41-56`, `handoff-machine.ts:162-170,224-242`).
10. **Delivery** (VERIFIED `data.ts:882-897`): `inbox.delivered` removes the pending row and moves the transcript row to the end with the server's timestamp, so a steer lands where the model actually saw it.
11. **Stream** (VERIFIED `data.ts:972-1005,1044-1053,1150-1151`): `execution.started` -> running; `step.started` appends the assistant message (ID supplied by the event); `text.started` pushes an empty part; `text.delta` appends. Each 16 ms batch -> `PacedMarkdown` -> worker projection -> block DOM update -> virtualizer resize batch -> stay pinned.
12. **Failure** (VERIFIED `data.ts:1777-1782`, `prompt-status-machine.ts:45-47`, `submit.ts:594-614,625,865-879`): rollback only "rows this call admitted and the server has not acknowledged"; status returns to idle only if the failed ID is the latest dispatched; the draft goes back only if the composer is still the object it was cleared to, with the same message ID re-armed under a payload fingerprint so an unedited resend is idempotent; toast.

### 2.4 New session at t=0

- VERIFIED `app/src/new-session/composer-adapter.ts:104-128`, `new-session/machine.ts:14-41,163-165,237-241`: mints `Session.ID.create()` client-side, then `tabs.prepareSession(draftID, {server, sessionId}, {message, selection, view})` swaps the draft tab for a **pending session tab** at once. Phases worktree (11 min bound) -> MCP (2 min) -> `session.create` (30 s) -> `session.promote` (inside `startTransition`). Any failure rolls the tab back to the draft.
- VERIFIED `app/src/session/route.tsx:150-185`: the pending tab renders the real session header, the user's message via the same `SessionUserMessage` component, and one `TextShimmer` status line. No spinner, no skeleton.
- VERIFIED `app/src/session/preparing-composer.tsx:10-72`: a live `PreparingComposer` lets the user type a follow-up while the session is being created; the draft and caret transfer into the promoted composer (`submit.ts:421-427`).
- VERIFIED `client/src/solid/data.ts:1634-1690`: `session.create` is itself optimistic (local record under the client ID, `sync.complete` on its empty collections so nothing fetches, rollback only if neither the echo nor the response acknowledged).
- "Held" sends (VERIFIED `composer/machine.ts:52-56`, `submit.ts:98-116,235-254`): Enter pressed while a cold Location's agents/models are still loading is not rejected. The prompt shows in the timeline immediately and sends once the catalog loads; an edit cancels a hold that is still in the composer.

### 2.5 Queue, steer, interrupt, revert

- Queue is **server-owned** (VERIFIED `app/src/session/composer/queue.ts:48-52`): the strip is derived from `data.session.pending.list()` filtered to `delivery === "queue"`. Steer-now and requeue are `inbox.update({delivery})` (VERIFIED `tui/src/routes/session/index.tsx:765-768`).
- Pending status is derived, not stored (VERIFIED `client/src/solid/data.ts:59-65,1575-1589`): `starting | steering | stranded | queued`, computed from inbox + last idle marker + active status.
- Queue mutations are serialized through one machine (VERIFIED `app/src/session/composer/queue-machine.ts:88-125`): `active` + `pending`, 60 s bound, a `gone` list hides rows the server confirmed removed before the SSE event lands (with expiry so a missed event cannot hide a row forever), and a `moved` map keeps a row's key stable across re-admission under a new inbox ID so reorder does not grow/shrink the strip.
- Interrupt (VERIFIED `app/src/session/composer/adapter.ts:56-60`): `session.interrupt({resume: true})`; stop on a held send just cancels the hold (`composer/machine.ts:205-206`).
- Revert (VERIFIED `app/src/session/revert-machine.ts:1-55`, `data.ts:1214-1238`): staged/cleared/committed events; an undelivered prompt is *withdrawn from the inbox* rather than rewound; the machine tracks its own echoes "in send order" so a late echo is not mistaken for a foreign change; 30 s bound; the Rewound toast can restore the pre-rewind draft.

---

## 3. Technique table

| # | What | Where (all VERIFIED at SHA) | Why it feels good |
|---|---|---|---|
| 1 | Client-minted ascending IDs + `outbox` set + upsert-by-ID echo | `client/src/solid/data.ts:389-394,445-458,906-915,1739-1783`; `schema/src/identifier.ts:16-31` | Row appears at t=0 and is never replaced or re-keyed, so no flash on reconcile. Retry is idempotent. |
| 2 | Rollback only unacked rows; restore draft only if composer untouched | `data.ts:1777-1782`; `app/src/composer/submit.ts:617-625` | Failure never eats text typed after the send and never deletes server state. |
| 3 | Optimistic "running" owned by a 2-state machine keyed on the latest prompt | `app/src/composer/prompt-status-machine.ts:1-48` | "Working" shows the same frame as the user row; an older prompt failing cannot hide a newer turn. |
| 4 | Handoff with bounded echo wait then resync | `app/src/session/handoff-machine.ts:3-10,97-176`; `session/handoff.ts:41-56` | Optimistic row can never stick as a ghost; 1 s echo budget, then authoritative reload. |
| 5 | Per-session send chain gated on in-flight create | `data.ts:402-438` | First prompt of a new session never races the create; order preserved without blocking the UI. |
| 6 | Ephemeral deltas + durable full-value boundary | `schema/src/session-event.ts:408-432`; `data.ts:1049-1058` | Dropped or reordered deltas never corrupt final text; replay is cheap. |
| 7 | 16 ms event flush in one reactive batch | `client/src/solid/connection.ts:65-74`; `app/src/runtime/server/client.tsx:129` | At most one render pass per frame regardless of token rate. |
| 8 | Backlog-only pacing (512-char threshold, 24 ms step, quarter-of-remainder slices up to 256 chars, snap to word boundary within 8 chars) | `session-ui/src/message/paced-text-machine.ts:1-73`; `message/message-content.tsx:170-222` | Normal token cadence is not delayed; a burst (reconnect, tab refocus) reveals smoothly instead of popping a wall of text. |
| 9 | Markdown block projection in a worker, diffed by block index | `session-ui/src/components/markdown-stream.ts:21-91`; `markdown.worker.ts:60-81`; `markdown-worker-protocol.ts:41-52` | Per-delta main-thread cost is O(live block), not O(answer). |
| 10 | Per-key coalescing lanes for worker requests (one active + newest queued) | `session-ui/src/components/markdown-worker-machine.ts:12-31,57-62` | A slow parse never builds a queue; stale snapshots are dropped before posting. Worker crash degrades to plain text with 2 s -> 60 s backoff. |
| 11 | Settled blocks keep their DOM; live -> settled only flips an attribute | `session-ui/src/components/markdown.tsx:1217-1227` | Text selection survives the end of streaming; no end-of-stream reflow flash. |
| 12 | Live block rendered as keyed nodes; only new words fade | `session-ui/src/components/markdown-solid.tsx:17-102`; `markdown.css:51-57,511-518` | Existing words are never re-created, so no flicker and the fade reads as typing. |
| 13 | Greedy wrapping on the growing block | `session-ui/src/components/markdown.css:73-76` | `text-wrap: pretty` would move settled words on every token. |
| 14 | Incremental code tokenizer, stable/unstable token split | `markdown.worker.ts:109-145`; `markdown.tsx:1299-1349` | Only the last line's spans are replaced; earlier highlighted lines are append-only DOM. |
| 15 | Per-session virtual measurement cache + disclosure state | `app/src/session/timeline/virtualizer.tsx:84-92,155-157,1224-1232` | Returning to a session lays out at final heights on the first frame. |
| 16 | Keep-alive cache of 16 mounted timeline views, each holding its own context copies while detached | `app/src/session/timeline/cache.ts:37-65,76-84,172` | Tab switch is a DOM attach, not a rebuild. Hidden views do not react to the selected tab's providers. |
| 17 | Hidden cold mount: tail first, then full range, reveal when measured + markdown ready + pinned; 3 s fallback | `app/src/session/timeline/cold-mount.ts:1-4,16-25,153-171`; `virtualizer.tsx:64,159,827-853,1187` | First paint of a cold session is already at the bottom with correct heights; no visible settle. |
| 18 | Batched row-size commits in a microtask | `virtualizer.tsx:403-473` | Avoids layout thrash from measure/render interleaving; one scroll write per batch. |
| 19 | Row `min-height` = cached size until content reports ready | `virtualizer.tsx:1108-1118` | Deferred row bodies cannot collapse and re-expand. |
| 20 | Reserved tail lane (92 px, 104 px touch) + fixed-height activity lane | `virtualizer.tsx:147-152`; `app/src/session/timeline/message-timeline.tsx:403-418` | "Working" and hints come and go without moving any row. |
| 21 | Follow state machine with gesture attribution | `app/src/session/timeline/machine.ts:1-11,22-30,209-235` | Auto-scroll never fights the reader; a virtualizer compensation is never mistaken for a scroll-up. |
| 22 | Anchor-to-end + follow-on-append + 80 px end threshold | `virtualizer.tsx:379-385` | Pinned growth is handled by the virtualizer, not by chasing `scrollTop` after layout. |
| 23 | Composer floats over the timeline; inset as a non-inherited CSS variable | `virtualizer.tsx:706-709,1212` | Composer growth does not restyle every row each frame. |
| 24 | Shimmer and spinner on the compositor only | `ui/src/typography/text-shimmer/text-shimmer.css:57-133`; `ui/src/components/spinner.tsx:16-20` | A running session at rest costs ~0 main-thread time per frame. |
| 25 | Spinner phase sync + pause offscreen | `ui/src/hooks/spin-phase.ts:3-55`; `ui/src/components/spinner.css:26-29` | Multiple spinners never look out of step; hidden ones cost nothing. |
| 26 | Tool title morphs active -> done text, animating only the differing suffix | `session-ui/src/components/tool-status-title.tsx:14-26,44-50`; `tool-status-title-machine.ts:1-28` | "Reading" -> "Read" changes without a width pop. |
| 27 | Deferred tool-body mounts, one per frame, bottom first | `session-ui/src/components/disclosure.ts:11-29` | Heavy default-open tool bodies near the viewport become interactive first; no long task on session open. |
| 28 | Hover-dwell prefetch (100 ms) and top-2 prefetch on Home | `app/src/shell/titlebar/tab-strip.tsx:276-286`; `app/src/home/sessions/controller.tsx:331-343,576-580` | Transcript is usually in memory before the click lands. |
| 29 | Smooth "jump to latest" glides only the last screen | `virtualizer.tsx:660-669` | A smooth scroll across history would stream every row through the virtualizer. |
| 30 | Panel resize holds row width until the spring lands | `virtualizer.tsx:254-269,1193-1200` | Text wraps once, not on every animation frame. |
| 31 | Draft writes batched with a 100 ms flush and exponential retry | `app/src/runtime/persistence/namespace-machine.ts:1-29` | Typing in a 44k-char draft does not hit storage per keystroke (bench expects ~25 encodes for 51 keystrokes, `app/perf/README.md`). |
| 32 | Lifecycle rule: one discriminated union, pure `transition`, attempt IDs, every async path ends in a state with a bounded fallback | `app/AGENTS.md` "State Machines" | This is what removes "stuck spinner" and "ghost row" classes of bug across the board. |
| 33 | Suspense guard rule | `app/AGENTS.md` "Suspense" | Their named root cause for flashes, blanked dialogs and reset scroll on first open. |

---

## 4. Animation and in-progress affordance details

### Text shimmer (VERIFIED `ui/src/typography/text-shimmer/text-shimmer.css`, `machine.ts`, `text-shimmer.tsx`)

- Custom properties: `--text-shimmer-step: 45ms`, `--text-shimmer-duration: 1200ms`, `--text-shimmer-swap: 220ms`, `--text-shimmer-spread: 5.2ch`, gutter = spread + 1ch, base color `var(--text-weak)`, peak `var(--text-strong)` (css:1-10).
- Structure: two stacked copies of the text in one grid cell (`grid-area: 1/1`): a base layer and a shimmer layer, crossfaded by `opacity` over 220 ms `ease-out` (css:26-55). Inside the shimmer layer, a "sweep" window with a `linear-gradient` mask (`transparent calc(50% - spread)`, `#000 50%`, `transparent calc(50% + spread)`) holds a third copy in the peak color (css:61-84).
- Keyframes (css:115-133), both `linear`, `infinite`, `fill-mode: both`, duration 1200 ms:

```css
@keyframes text-shimmer-window {
  0%   { translate: calc(-50% + var(--text-shimmer-spread) - var(--text-shimmer-gutter)) 0; }
  100% { translate: calc(50% - var(--text-shimmer-spread) + var(--text-shimmer-gutter)) 0; }
}
@keyframes text-shimmer-text {
  0%   { translate: calc(50% + var(--text-shimmer-gutter)) 0; }
  100% { translate: calc(-50% - var(--text-shimmer-gutter)) 0; }
}
```

  The window and the text inside it move by equal and opposite amounts, so the glyphs stay put while the window travels. Both are transforms, so the compositor runs them (css:57-60 comment).
- Phase offset across adjacent shimmers: `animation-delay: calc(step * index * -1)` (css:99); `ToolStatusTitle` passes the prefix length as `offset` so a split title sweeps as one string (`tool-status-title.tsx:150`).
- Lifecycle: `active -> fading -> inactive`; the sweep keeps running for the 220 ms fade "so the layer never freezes mid-fade" (`machine.ts:1-12`). Frozen singleton states because "every timeline row mounts a shimmer" (`machine.ts:19-26`).
- Reduced motion: no sweep, instant swap (css:135-152).
- TUI equivalent: `DURATION = 1200` ms, masked text renderable (VERIFIED `tui/src/component/shimmer-text.tsx:11,36-43`).

### Spinner "Soft Radar" (VERIFIED `ui/src/components/spinner-radar.ts:1-22`, `spinner.tsx`, `spinner.css`, `hooks/spin-phase.ts`)

- 5x5 dot grid, 2-unit dots on a 3-unit pitch in a 16-unit viewBox; lit head sweeps each ring clockwise leaving a trail; centre dot steady. Loop `1.8 s`, `linear`, scaled by `--spinner-time`. Unlit opacity `0.2`. Trail fades over 48% of the loop, sampled every 4%: `[1, 0.987, 0.934, 0.812, 0.599, 0.384, 0.238, 0.143, 0.081, 0.041, 0.017, 0.004, 0]` scaled by 0.55.
- Each dot is its own `<svg>` so opacity animates on the compositor ("the compositor runs the opacity animation of an SVG element but not of a shape inside one").
- All instances share a phase by setting every infinite animation's `startTime = 0` on the document timeline, re-pinned from a `ResizeObserver` (fires before paint) and on `animationstart`. An `IntersectionObserver` sets `data-offscreen` -> `animation-play-state: paused !important`.
- Usage rule (VERIFIED `app/AGENTS.md` "Loading Indicators"): this spinner means only "the AI is working"; everything else uses `Loader`. Never restyle one to look like another.

### "Working" row (VERIFIED `app/src/session/timeline/message-timeline.tsx:340-418`)

- `TextShimmer` with text "Working", 13 px, weight 530, in a lane with fixed `h-9` (36 px).
- Suppressed when something more specific is already live in the active turn: a Thinking row, a running compaction notice, a tool in `streaming`/`running`, or visible streaming text. Read/context groups deliberately do **not** suppress it "so each new read does not swap Working out for its short call" (382-383).
- When undelivered steers wait below the live work, status moves into an `Activity` row above them (401).

### Tool rows

- Title is `<TextShimmer text={title} active={pending()} />` (VERIFIED `session-ui/src/components/basic-tool.tsx:104`; same in `tools/tool-renderer.tsx:635,922,1686,1962,2191,2300,2667`).
- `ToolStatusTitle` (VERIFIED `tool-status-title.css:18-76`, `tool-status-title-machine.ts:1-2`): width transition `480ms cubic-bezier(0.22, 1, 0.36, 1)` (fallback of `--tool-motion-spring-ms` / `--tool-motion-ease`); opacity `240ms ease-out`, filter and transform `192ms ease-out`; the hidden text sits at `opacity: 0; filter: blur(0.9px); transform: translateY(0.03em)`. Both texts stay mounted for `TOOL_STATUS_ANIMATION_MS = 600`. Flow: `idle -> measuring` (pin old width one rAF) `-> animating` (set new width) `-> idle`. A zero-width (hidden/detached) title skips animation so it cannot clip.
- Tool state machine in data: `streaming` (input JSON deltas) -> `running` (with `progress` metadata) -> `completed | error` (VERIFIED `client/src/solid/data.ts:1059-1122`). If an execution ends with a tool still non-terminal, the client refetches the transcript (1191-1203).
- Collapsible expand: `slideDown`/`slideUp` keyframes exist but the `250ms ease-out` rule is commented out; chevron and hover use `0.15s ease` (VERIFIED `ui/src/components/collapsible.css:7,29,38,98-100,126-142`). Tool expansion is effectively instant.

### Streaming text

- New word / inline code: `animation: markdown-word-enter 180ms ease-out both` (opacity 0 -> 1 only, no transform) (VERIFIED `session-ui/src/components/markdown.css:55-57,511-518`). Applied only to nodes mounted after the first render and flagged `animate` (`markdown-solid.tsx:19-26,41-46`), and suppressed when a remount re-renders identical text (`markdown.tsx:1252-1256`, `rendered.raw !== block.raw`).

### Other motion numbers

- Jump-to-latest pill: in `150ms cubic-bezier(.2,.8,.2,1)`, out `110ms cubic-bezier(.4,0,1,1)`, 4 px translate (VERIFIED `virtualizer.tsx:1128-1132`).
- User-message meta labels (e.g. "Pending") grow in via `grid-template-columns 0fr -> 1fr`, `200ms ease-out`, and leave instantly; delivery swaps "Pending" for the timestamp with no transition (VERIFIED `session-ui/src/components/message-part.css:365-409`).
- Composer region height is a spring ("morph") with a 150 ms no-bounce fade; faces present within 250 ms of mount swap without a flight (VERIFIED `app/src/session/composer/spring-height-machine.ts:3-6,24-39`).
- Subagent working indicator collapses width/margin/opacity over 200 ms `ease-out` and cancels its own gap so unmount does not nudge the title (VERIFIED `app/src/session/header/session-working-indicator.tsx:32-37`).
- Question dock motion (blur 2 px / scale 0.97 crossfade, rolling counter) exists but is gated behind a tuning knob whose default is `false` (VERIFIED `app/src/session/requests/question-motion.ts:8-15`).
- Desktop window: created with the stored background color, stays hidden until content loaded and theme applied, `REVEAL_TIMEOUT_MS = 3000` (VERIFIED `desktop/src/main/windows/reveal-machine.ts:1-9`, `windows/early.ts:91-100`).

### Permission / question / elicitation prompts

- Rendered as **docks in the composer region**, not inline transcript rows or modals (VERIFIED file set `app/src/session/requests/session-{permission,question,elicitation,websearch}-dock.tsx`, each with a `*-machine.ts`). Their height changes go through the composer morph spring and the timeline inset spacer, so opening a dock does not jump a pinned timeline (`virtualizer.tsx:681-691`).
- Replies are optimistic-removal with resync on "already settled" (VERIFIED `client/src/solid/data.ts:369-379`).

---

## 5. Jank they hit and how they fixed it

| Symptom | Fix | Evidence |
|---|---|---|
| Long sessions: blank ranges, overlapping rows, reverse scroll, 22.8 FPS under stream on a throttled Mac | Replaced Virtua with TanStack Virtual, semantic rows with stable keys, measurement cache, end anchoring, worker highlighting, incremental markdown. Reported: hot tab switch 60.8 -> 15.9 ms, busy FPS 22.8 -> 51.4 | UNVERIFIED PR #32331 (numbers); resulting code VERIFIED (`virtualizer.tsx`, `markdown.worker.ts`) |
| "Working" painted two frames before the optimistic user row; row moved when the virtualizer re-measured | Set optimistic busy only after local admission; measure new user row + tail in a microtask before paint | UNVERIFIED PR #46723; VERIFIED `virtualizer.tsx:1085-1105`, `submit.ts:545` |
| Optimistic prompt stuck forever / re-appended as a ghost after a missed echo; phantom agent-switch notice | Handoff machine with send/echo/sync timeouts and connection-aware resync; owned per server, not module-global | UNVERIFIED PR #61932; VERIFIED `handoff-machine.ts` |
| Spinner + shimmer cost 862 ms main-thread per 5 s idle (restyle of 24 SVG rects + `background-position` animation) | One SVG per dot (opacity on compositor); shimmer as transform + mask. Reported main-thread busy 3.20% -> 0.53% | UNVERIFIED PR #61562 (numbers); VERIFIED `spinner.tsx:16-20`, `text-shimmer.css:57-60` |
| Half of JS allocations during a stream came from `DOMPurify.sanitize` rebuilding allow-lists; two extra HTML round trips per live update | `setConfig` once; read nodes from the decorated DOM directly. Reported 112.7 -> 78.7 MB per 600 deltas | UNVERIFIED PR #60549; VERIFIED `markdown-solid.tsx:66-68` |
| morphdom-based prose updates flickered and re-animated | Keyed Solid `reconcile` over a parsed node tree; fade only newly mounted words; do not replay fade on remount | UNVERIFIED PRs #44354, #44396; VERIFIED `markdown-solid.tsx`, `markdown.tsx:1252-1256` |
| Streamed text landed in bursts, then over-smoothed (char-by-char catch-up) | Pace only large backlogs; "normal 3-4-word provider fragments render as one update" | UNVERIFIED PRs #19404, #32331; VERIFIED `paced-text-machine.ts:3-4,55` |
| Switching back to a tab in another folder rebuilt the whole view (134 ms) | Each cached view holds its own Location/Data/Markdown context copies while detached (86 ms) | UNVERIFIED PR #59168; VERIFIED `cache.ts:27-65` |
| iOS: paragraph moved 544 px beyond the finger while row heights resolved | Defer scroll compensation during touch/momentum as a row translation, then transfer it into the native offset in one write | UNVERIFIED PR #48600; VERIFIED `virtualizer.tsx:476-492,594-608`, `machine.ts:182-186` |
| Under page zoom the furthest reachable offset sits up to 4 px short, so "at end" never became true | `ZOOM_SLACK = 4` plus a write-and-undo probe after the scroll settles | VERIFIED `machine.ts:5-7`, `virtualizer.tsx:713-727` |
| Chrome "ResizeObserver loop completed with undelivered notifications" every frame | Report content height from the virtualizer's total size instead of observing the element | VERIFIED `virtualizer.tsx:696-704` |
| Hard edge where the timeline met an opaque composer | Timeline scrolls under a floating composer; virtualizer viewport = measured height minus inset; spacer of inset height | UNVERIFIED PR #57141; VERIFIED `virtualizer.tsx:284-299,1212` |
| Flashes / reset scroll on first open | Named cause: a render-time read entering Suspense. Rule: guard on `resource.state`, show data you already have | VERIFIED `app/AGENTS.md` "Suspense" |
| Racy initial reads of a just-created session overwrote event-built state | Skip those reads; comment calls it a "Band-aid" until hydration is atomic | VERIFIED `client/src/solid/data.ts:752-755` |

### Known rough edges (open issues; titles only, UNVERIFIED)

- #56285 "desktop: fast scroll up from bottom snaps back down, older history loads one chunk at a time" (the 20-message page size and follow logic have a cost).
- #55699 "desktop: session scroll position is lost when switching sessions and back".
- #62304 "desktop: @ session list lags on every keystroke".
- #60143 "desktop: code blocks hide horizontal scrollbars and cannot wrap long lines".
- #61373 "app: mobile session drawer cannot scroll; touch drag reorders tabs instead".
- #59135 "Subagent-bound command: prompt to freshly-created child session can be silently dropped".
- #52435 "[FEATURE]: Copy pending steering messages"; #54572 "open a session at the start of the last message instead of scrolled to the bottom".
- In-source admission of a remaining race: `data.ts:752-755` (above). PR #32331 also concedes a "rare intermittent jump" on ~77,000 px expanded diffs settling above the viewport.

---

## 6. TUI and ACP notes

### TUI (opentui)

- VERIFIED `tui/src/app.tsx:251-256`: `createCliRenderer({ targetFps: 60, externalOutputMode: "passthrough", useKittyKeyboard: {} ... })`, rendered with `@opentui/solid`.
- Streaming markdown is a native renderable: `<markdown streaming={...} internalBlockMode="top-level" content={...}>` (VERIFIED `tui/src/routes/session/index.tsx:2224-2234`, `routes/session/group-view.tsx:393`). The block-incremental work happens inside opentui, not in app code.
- Same data layer and optimistic admission as the GUI (VERIFIED `tui/src/component/prompt/index.tsx:1341,1362` comments), including optimistic session creation on first prompt (UNVERIFIED PR #43687).
- Queue semantics: default submit is `steer`; a separate key path submits `queue` (VERIFIED `prompt/index.tsx:455,1099`). Shell mode, exit words and slash commands cannot be queued (1142). Pending items can be promoted/demoted with `inbox.update` (`routes/session/index.tsx:765-768`).
- A second "mini" runtime (`tui/src/mini/*`) writes finished content into terminal scrollback and keeps only a live footer.

### `opencode acp` (VERIFIED `cli/src/acp/agent.ts:68-127`, `service.ts:133-146`, `turn.ts:406-423`, `capabilities.ts:3-13`)

- Built on `@agentclientprotocol/sdk`; it is a thin translator over the same HTTP client (`OpenCodeClient`).
- Methods: `initialize`, `authenticate`, `session/new`, `session/load`, `session/list`, `session/delete`, `session/resume`, `session/close`, `session/fork`, `session/set_config_option`, `session/set_mode`, `session/prompt`, `session/cancel`.
- Capabilities returned: `loadSession: true`, `mcpCapabilities: {http: true, sse: false}`, `promptCapabilities: {embeddedContext: true, image: true}`, `sessionCapabilities: {additionalDirectories, close, delete, fork, list, resume}`.
- Extensions a Memex-style client can use:
  - `_meta.steering.supported: true` + request `_session/steering` `{sessionId, prompt}` -> `{outcome: "injected"}` or `{outcome: "promptRequired", reason: "noRunningTurn"}`. Comment: "Interim extension until ACP has native mid-turn steering." It posts with `delivery: "steer", resume: false` so a steer never starts a turn.
  - `_meta["opencode/child-session-updates"]` (client and agent) to receive subagent session updates.
  - Retry state via `session_info_update` `_meta` (`translate.ts:265-275`).
  - Client capability `session.compaction` and `elicitation.form` are honored.
- Replay on `session/load` emits `user_message_chunk` / `agent_message_chunk` with `messageId`, plus `tool_call` and `tool_call_update` (`replay.ts:46-113`). A session opened and closed without a prompt is deleted (`service.ts:107-118`).
- ACP exposes steer but **not** `delivery: "queue"`, inbox reorder/edit, or the durable event log. An ACP client must own its queue.

---

## 7. Ranked recommendations

Ranked by expected feel-per-effort for a native Swift/AppKit ACP client with a React web surface.

1. **Client-minted, time-ordered message IDs with upsert-by-ID reconciliation and an outbox set.** Insert the user row synchronously; never swap IDs; on echo replace payload in place; on failure remove only if still in the outbox. ACP `session/prompt` has no admission echo, so Memex's own runtime/queue must supply the ack (write-ahead to the durable queue = ack). Mirror `data.ts:1739-1783`. Both surfaces.
2. **Make "busy" optimistic and make it appear in the same frame as the user row.** Port `prompt-status-machine.ts` verbatim (it is 48 lines): optimistic until admitted, roll back only if the latest dispatched prompt fails. In SwiftUI/AppKit, apply the row insert and the status change in one transaction so they commit together.
3. **Reserve layout for transient status.** Fixed-height tail lane under the last row (they use 36 px lane inside a 92 px tail) so "Working", hints and the activity pill never change content height. In AppKit: a permanent footer supplementary view with constant height; toggle its content's alpha, not its presence.
4. **One shimmer primitive, used everywhere a spinner would go.** 1200 ms linear loop, 220 ms crossfade on start/stop, sweep kept alive through the fade-out. Native: `CAGradientLayer` as a mask (or a second text layer in the peak color masked by a moving gradient) animated with `CABasicAnimation` on `position`/`locations` so the render server drives it; never a `Timer`/`TimelineView` redraw. Share phase by deriving `beginTime` from a common epoch (`CACurrentMediaTime()` modulo duration) and remove the animation when the view leaves the window. React: copy the CSS (section 4).
5. **Stream text as: coalesce -> block-diff -> append.** (a) Coalesce transport events per display frame (16 ms) and apply in one transaction. (b) Split markdown into blocks; treat all but the last as immutable and cache their rendered form by content hash; re-render only the live tail block. With swift-markdown: keep the settled prefix's `NSAttributedString` per block, reparse only from the last block boundary, do parsing and highlighting off the main actor with a per-message "one active + newest queued" lane. (c) For code blocks, keep earlier lines' attributed runs and replace only the last line. (d) No pretty/balanced wrapping on the live block. (e) Pace only when the unrevealed backlog exceeds ~512 chars.
6. **Explicit follow state machine instead of "if near bottom, scroll".** States following / returning / detached / anchored; detach only on user-originated upward input (wheel delta, key, drag while a gesture is held); re-follow only on *arrival* at the end (0.5 px epsilon), not on resting near it; clicking with a text selection detaches. In AppKit, distinguish user scroll (`NSScrollView.willStartLiveScrollNotification`, scroll-wheel phases, momentum) from programmatic bounds changes. Pin to bottom by anchoring layout to the end, not by correcting after layout.
7. **Keep recent session views alive and cache row heights per session.** They keep 16 mounted views + 16 measurement snapshots and transcripts for open tabs + 3 recents. Native: retain the N most recent transcript view controllers (detached from the window, updates paused) and a `[RowID: CGFloat]` height cache keyed by stable row ID and width bucket, so `heightOfRow` answers without measuring. Target they assert in CI: zero transcript requests and zero layout shift on a warm switch.
8. **Cold open: render hidden, tail first, reveal when settled, with a hard timeout.** Measure the last screenful, pin to bottom, then expand; reveal on "all visible rows measured + markdown ready + at end" or after a bounded wait (theirs is 3 s; for a local native app use something much shorter). Never show a skeleton for a transcript that is already on disk.
9. **Optimistic new session.** Mint the session ID locally, navigate immediately to a real session view containing the user's message and a shimmer status line, keep a live composer for a follow-up while the agent process/worktree spins up, gate the first prompt on the create promise, roll back to the draft on failure. Also port the "held send": accept Enter while models/agents are still loading and send when ready.
10. **Bound every optimistic state.** Each has a send timeout, an echo/ack timeout that escalates to an authoritative resync, and connection-aware behavior (wait while disconnected, resync on reconnect). Carry an attempt ID in state and in every result; ignore stale results. This is the single biggest source of "no ghost rows, no stuck spinners" in their codebase.
11. **Prefetch on intent.** 100 ms hover dwell on a tab/sidebar row loads its transcript; prefetch the top two sessions on the home screen. Cancel on pointer exit. Cheap, and it is what makes their warm switch read as instant.
12. **Animate the smallest thing that changed.** Tool title active->done morphs only the differing suffix with a width transition (480 ms ease-out-quint-like curve) and a 240 ms crossfade; labels grow in but leave instantly; new words fade 180 ms opacity only. Avoid height animations on tool expansion (theirs is disabled).
13. **Server-owned queue with derived status and stable row keys across edits.** Memex already has a durable queue; adopt their `gone` (hide confirmed-removed rows until the event lands, with expiry) and `moved` (old ID -> new ID so a reordered/edited row keeps its key) tricks so the queue strip never flickers during mutations, and serialize all queue mutations through one active+pending lane.
14. **Put requests (permissions, questions) in a dock attached to the composer** and route height changes through the same inset the timeline already compensates for, so a prompt appearing never jumps a pinned transcript.
15. **Measure it.** Adopt count-style assertions that encode "should not happen": layout shifts on switch = 0, network/disk reads on warm switch = 0, draft encodes per keystroke batched, main-thread work per streamed delta flat as history grows, idle running session ~0% main thread.
16. **If Memex drives OpenCode over ACP:** advertise `opencode/child-session-updates`, use `_session/steering` for mid-turn steer (check `_meta.steering.supported`), and expect `promptRequired/noRunningTurn` when the turn already ended; fall back to a normal prompt.

### Caveats on transfer

- Their zero-reflow reconciliation relies on the server echoing the client's ID. Plain ACP agents do not; Memex must treat its own runtime as the admission authority.
- Several numbers (3 s cold reveal, 30 s send timeout, 20-message first page) are tuned for a remote HTTP server on possibly slow links. A local native app should shorten them; the structure is what transfers.
- The app is SolidJS; fine-grained in-place store mutation makes "append to one text part" nearly free. In React the equivalent needs an external store with per-block subscriptions (`useSyncExternalStore` + memoized block components keyed by block index and hash) to avoid re-rendering the message on every delta.
