# PR 237 review: execution host and web/mobile surface

Scope: execution host (Swift core + Rust gateway), macOS remote client, web surface. Read-only review of
`scratchpad/pr237` at HEAD `a278a9f`, diffed against merge base `d9052aaf`. Nothing was built or run; every
behavioural claim below is from reading source. Paths are relative to the PR checkout unless prefixed.

Path abbreviations:

- `Host/` = `apps/macos/Sources/MemexExecutionHostCore/`
- `App/` = `apps/macos/Sources/Memex/`
- `RT/` = `scratchpad/runtime/packages/` (a runtime checkout beside the PR; the PR pins
  `sidequery/mono@e610a6f` in `apps/macos/runtime-source.json:4`. I did not verify the checkout is that exact
  revision, so `RT/` citations are "as of that checkout".)

Confidence markers: unmarked = read directly in source. **[inferred]** = follows from the code but depends on
runtime/browser behaviour I did not execute.

---

## 1. How it works

### 1.1 Processes and trust boundaries

Three processes on the execution machine, plus viewers:

1. **`MemexExecutionHost`** (Swift executable, `apps/macos/Sources/MemexExecutionHost/main.swift:4-35`). Parses
   `--root` and repeated `--workspace` (`main.swift:7-18`), takes ownership of `<root>/state/execution` via
   `HostSocketServer` (`main.swift:21`), builds `ExecutionHost` with a `NativeExecutionProvider`
   (`main.swift:23-25`) and blocks in `server.run(host:)` (`main.swift:27`). Without SQACPHost it exits with
   `runtime_unavailable` (`main.swift:28-31`).
   - `NativeExecutionProvider` owns the Rust runtime client and `AgentConversationService` in-process
     (`Host/NativeExecutionProvider.swift:43-44`) and spawns the codex/claude provider through it
     (`:47-66`, `:68-91`). Viewers disconnecting never stops these objects (`:7-8`).
   - `ExecutionHost` keeps one durable `catalog.json` (workspaces, conversations, receipts, queue, schedules,
     runs, handoffs: `Host/ExecutionHost.swift:41-54`) rewritten in full on every save (`:797-810`).
2. **`memex web`** (Rust, tiny_http) is the only network-facing piece. It adds one route, `POST /api/control`
   (`src/web.rs:175-223`), that authenticates and relays one JSON envelope to the host's Unix socket
   (`src/execution_host.rs:98-148`).
3. **`memex-control`** (Rust stdio MCP, `src/bin/memex-control.rs:5-45`, `src/control_mcp.rs:57-91`) relays the
   same envelopes straight to the Unix socket with no token (same-user authority), optionally scoped to one
   conversation (`src/control_mcp.rs:162-247`), at most 4 concurrent calls (`src/control_mcp.rs:45`).

Viewers:

- **Browser** (`web/src/ExecutionSurface.tsx`) -> `fetch("/api/control")` (`web/src/execution.ts:49-51`).
- **macOS app, remote conversation** -> `ExecutionHostHTTPClient` posts to the same `/api/control`
  (`App/ExecutionHostConnection.swift:77-90`, `:117-124`), over HTTPS or an app-managed `ssh -N -L` loopback
  tunnel (`App/RemoteWorkspaceSSH.swift:24-33`). Local (non-hosted) conversations do not use the host at all;
  they run `InAppAgentRuntime` in-process.
- A reverse channel `desktop.sock` lets the host call the app for browser tools
  (`Host/ExecutionHost.swift:653-668`, `Host/HostSocketClient.swift:5-61`).

### 1.2 Authentication

- Host socket: no token. Authority is filesystem permission only: directory must be user-owned 0700 and not a
  symlink (`Host/ExecutionHost.swift:811-817`), socket chmod 0600 (`Host/HostSocketServer.swift:65`), exclusive
  `flock` on `owner.lock` (`:25-31`). Comment states the gateway owns bearer auth (`:4-9`).
- Pairing token: 32 random bytes hex (64 chars) written `O_EXCL` 0600 to `control-token`
  (`Host/HostSocketServer.swift:180-200`), reused across restarts (`:183-192`).
- Gateway: POST only (`src/web.rs:176-178`); same-origin or the single configured
  `MEMEX_CONTROL_PUBLIC_ORIGIN` (`src/web.rs:179-192`, `src/execution_host.rs:34-49`); exactly one
  `Authorization` header (`src/web.rs:193-197`); token re-read from disk and compared in constant time on every
  request (`src/execution_host.rs:52-88`). Cookies and retrieval tokens never authorize it
  (`src/web.rs:203-207`, test `src/web.rs:4108-4193`).
- Host identity: every method except `host.info` must carry the exact `hostId`
  (`Host/ExecutionHost.swift:139-143`).
- Browser stores `{token, hostId}` in `sessionStorage` (per tab) (`web/src/ExecutionSurface.tsx:11-17`, `:157`);
  requests use `credentials: "omit"`, `redirect: "error"` (`web/src/execution.ts:49`).
- macOS stores the token in Keychain (`App/ExecutionHostConnection.swift:16-41`), refuses redirects
  (`:43-48`), and for SSH proves via libproc that its own `ssh` child owns the local listener before every
  request (`App/RemoteWorkspaceSSH.swift:63-85`, `Host/ProcessSockets.swift:9-19`).

### 1.3 Wire protocol

- Envelope: `{id, method, params}` -> `{id, result | error:{code,message}}` (`Host/HostProtocol.swift:41-64`).
- Unix socket framing: one newline-terminated JSON request, one newline-terminated JSON response, then the
  server closes the connection (`Host/HostSocketServer.swift:124-129`, `:111`). A new socket connection per
  request on the Rust side (`src/execution_host.rs:131-138`). **Strict request/response. There is no push,
  no stream, no subscription anywhere in the protocol.**
- HTTP: one POST per envelope; body is the upstream JSON re-serialized (`src/web.rs:219-222`, `:2234-2243`).
  No gzip/br on responses (no `Content-Encoding` handling anywhere in `src/web.rs`).
- Mutations are idempotent by client `commandId`: the host stores a SHA-256 request digest and a receipt
  before executing (`Host/ExecutionHost.swift:220-255`); same id + same request returns the stored result,
  different request is `id_conflict` (`:229-236`).
- The only long-lived call is `conversation.wait` (long poll, `Host/ExecutionHost.swift:696-708`): default 20 s,
  max 30 s (`:697`), implemented as a **100 ms sleep loop that rebuilds the full snapshot each iteration**
  (`:700-707`). It is reachable from the MCP allow-list (`src/control_mcp.rs:134`) but **no client in this PR
  calls it** (repo-wide grep: only those two sites).

### 1.4 How state and streaming output reach viewers

Provider -> host (in-process): the provider subprocess talks to `AgentConversationService` inside the host.
The host never subscribes to service events; it only reads on demand.

Host read path, executed for every `conversation.read`:

1. `ExecutionHost.handle` takes the single host lock for the whole route (`Host/ExecutionHost.swift:131-137`).
2. `ExecutionHost.read` (`:670-694`) calls `provider.read`, then adds `conversation`, `queueHeld`,
   `handoffBlocked`, `queue`.
3. `NativeExecutionProvider.read` (`Host/NativeExecutionProvider.swift:117-161`):
   - `attachSource` -> `service.refresh(sessionID:)` unconditionally when the source is already attached
     (`:119`, `:322-329`). In the runtime checkout, refresh reads the entire native JSONL and SHA-256s the
     captured prefix every time (`RT/agent-runtime/src/conversations.rs:628-644`), then re-normalizes
     (`:718`). The local macOS runtime guards the same call behind a file-version check
     (`App/InAppAgentRuntime.swift:206-219`); the host does not.
   - `service.read` (full conversation view), `thread.snapshot`, command statuses, settings/controls, provider
     operations, children (`Host/NativeExecutionProvider.swift:120-158`).
4. Result is `{thread, presentation, deliveries, operations, children, ready, connected, running, actions,
   controls, warning}` (`:157-160`) plus host fields. `presentation` is the "lossless" service view and
   `thread` is the Rust runtime projection; the docs warn they are two renderings of the same transcript
   (`docs/execution-host.md:188`). The service view also carries `persisted`
   (`Host/NativeExecutionProvider.swift:106-107` reads it from the same call), so the transcript is in the
   payload up to three times.

**Payload is always a full snapshot. There is no delta, cursor, ETag, "unchanged" short-circuit or pagination
on `conversation.read`** (no such parameter in `Host/ExecutionHost.swift:179`, `:670-694`).

Serialization hops per read: service JSON string -> `HostValue` decode (`Host/NativeExecutionProvider.swift:338`)
-> `JSONEncoder` (`Host/HostSocketServer.swift:126`) -> `serde_json::from_slice`
(`src/execution_host.rs:147`) -> `serde_json::to_vec` (`src/web.rs:2235`) -> client parse.

(a) **macOS remote** (`App/RemoteConversationRuntime.swift`): **polling, 1 s sleep between polls**
(`:69-74`), so period = 1 s + round-trip + decode. Each poll: `conversation.read` (`:147`), then
`HostResponse` -> `HostValue` decode (`App/ExecutionHostConnection.swift:134`), re-encode and decode again
into `RawTranscriptJSON` (`App/RemoteConversationRuntime.swift:148`), project
(`:157-159`), `receive(.success(snapshot))` (`:170`). No coalescing, no sequence check.
For contrast, the local runtime is **push**: `service.subscribe` callback (`App/InAppAgentRuntime.swift:156-161`)
coalesced at **80 ms** (`:194-198`), with a 1 s tick only as a file-change/timeout backstop (`:181-186`).

(b) **Browser** (`web/src/ExecutionSurface.tsx:91-107`): **polling, `setTimeout(poll, 1000)` after each
response** (`:103`), full snapshot into React state every time (`:101`). Schedule runs poll every **5000 ms**
(`web/src/ExecutionSchedules.tsx:47`). Host/conversation/workspace/schedule lists are fetched only on mount and
after each mutation (`web/src/ExecutionSurface.tsx:72-89`, `:114`).

### 1.5 Constants

| Constant | Value | Where |
| --- | --- | --- |
| Web snapshot poll | 1000 ms after each response | `web/src/ExecutionSurface.tsx:103` |
| Web schedule-runs poll | 5000 ms after each response | `web/src/ExecutionSchedules.tsx:47` |
| Web fetch timeout | 65000 ms | `web/src/execution.ts:51` |
| Web attachment limit | 2 MiB each | `web/src/execution.ts:84` |
| Web question attachments | 1,048,576 bytes total, 10 files | `web/src/ExecutionQuestions.tsx:9-10` |
| macOS remote snapshot poll | 1 s sleep, then read | `App/RemoteConversationRuntime.swift:72-73` |
| macOS remote Stop confirmation | read every 100 ms, 10 s deadline | `App/RemoteConversationRuntime.swift:108`, `:119` |
| macOS HTTP timeout | 65 s request, +5 s resource | `App/ExecutionHostConnection.swift:62`, `:69-70` |
| macOS schedule observer | every 15 s, 15 s timeout | `App/HostScheduleObserver.swift:58`, `:102` |
| macOS local runtime (comparison) | push + 80 ms coalesce; 1 s tick | `App/InAppAgentRuntime.swift:196`, `:183` |
| SSH listener proof | up to 10 s, 100 ms steps, every request | `App/RemoteWorkspaceSSH.swift:63`, `:74` |
| SSH options | ConnectTimeout=10, ServerAliveInterval=15 x3 | `App/RemoteWorkspaceSSH.swift:27-28` |
| SSH pairing probe | 10 tries, 1 s timeout, 250 ms apart | `App/ExecutionHostConnection.swift:218-224` |
| Host socket concurrency | 8 (semaphore), listen backlog 16 | `Host/HostSocketServer.swift:15`, `:65` |
| Host socket I/O timeout | 65 s | `Host/HostSocketServer.swift:112` |
| Host scheduler tick | 1 s | `Host/HostSocketServer.swift:90` |
| `conversation.wait` | default 20 s, max 30 s, 0.1 s loop | `Host/ExecutionHost.swift:697`, `:706` |
| Request limit | 4 MiB (32 MiB handoff install/abort) | `Host/HostSocketServer.swift:139-146`, `src/execution_host.rs:8-20` |
| Response limit | 32 MiB | `Host/HostSocketServer.swift:127`, `src/execution_host.rs:12` |
| Prompt `text` limit | 1,048,576 bytes | `Host/ExecutionHost.swift:786` |
| Fork context | last 100 messages, 512,000 bytes | `Host/ExecutionHost.swift:347`, `:351` |
| Receipts | result <= 64 KiB, 7 days, 1000 | `Host/ExecutionHost.swift:823-825` |
| Finished runs / queue kept | 30 days and 500 / 200 | `Host/ExecutionHost.swift:826-828` |
| `schedule.runs` page | default 200, max 1000 | `Host/ExecutionHost.swift:829-830` |
| Managed-worktree verification cache | 3 s | `Host/ExecutionHost.swift:66` |
| Gateway workers / queue | 8 / 64 | `src/web.rs:23-24` |
| Gateway upstream timeout | 65 s read and write | `src/execution_host.rs:132-133` |
| Control MCP concurrency | 4 | `src/control_mcp.rs:45` |
| Desktop bridge client timeout | 25 s | `Host/HostSocketClient.swift:16` |

There is **no debounce or throttle anywhere in the web surface**, and no conversation page size.

---

## 2. Send path on the web surface

All mutations go through one function, `operate` (`web/src/ExecutionSurface.tsx:109-118`):

```
operationActive = true; setBusy(true); setError("")
await client.call(method, params, true)     // outbox write, then fetch
await refresh(client)                       // 4 parallel calls + worktree.list
finally: setOutbox(...); operationActive = false; setBusy(false)
```

`refresh` is `host.info`, `conversation.list`, `workspace.list`, `schedule.list` in parallel, then a serial
`worktree.list` (`:72-78`). Every mutation therefore costs 6 requests in 3 serial phases before `busy` clears,
plus a 7th because the new `schedules` array identity restarts the runs poll
(`web/src/ExecutionSchedules.tsx:37-51`).

### 2.1 Sending a prompt (`send`, `:126-138`)

At t=0 (click Send, or Cmd/Ctrl+Enter at `:283`):

- **Not optimistic.** Nothing is added to the transcript. `LiveMessages` renders only host snapshot entities
  (`:26-46`); there is no pending bubble.
- **Input is not cleared.** The textarea keeps the text until `operate` resolves, i.e. after the send
  round-trip *and* the refresh phases (`:130-136`). It is cleared only if the saved draft still equals what was
  sent (`:133-135`), so text typed meanwhile is preserved (good) but the sent text then also stays.
- **Visible feedback is only `disabled`.** `busy` disables Send/Queue/Steer (`:301-303`), model and config
  selects (`:144`, `:275`, `:278`), Stop, Resume, Fork (`:239-241`) and every sidebar action. No spinner, no
  label change, no shimmer.
- Before the fetch, the full request is written synchronously to `localStorage` as an outbox entry
  (`web/src/execution.ts:33-46`).

Server echo reconciliation: there is none to do, because there is no local echo. On a successful response the
outbox entry is removed (`web/src/execution.ts:60`). The user's message appears when a later 1 Hz poll returns
a snapshot containing the provider's own user-message entity. `deliveries` is used only to print failed
deliveries (`web/src/ExecutionSurface.tsx:248-249`).

Worst-case sequence the user sees: click -> buttons grey, text still in box -> (send RTT; host does two
full-catalog fsync saves around the provider call, `Host/ExecutionHost.swift:237-244`) -> (refresh phase 1) ->
(worktree.list, which runs git validation under the host lock, `Host/ExecutionHost.swift:164-170`, `:744-764`)
-> box clears, buttons re-enable -> up to 1 s + RTT later the message and "Working" appear.

Failure:

- Network error/timeout: `delivery_unknown` (`web/src/execution.ts:52-55`); red banner at the very top of
  `main` (`web/src/ExecutionSurface.tsx:153`); draft kept; the outbox entry stays and appears under
  "Unconfirmed commands" in the sidebar with Inspect receipt / Retry exact command / Mark inspected
  (`:209-231`). While any outbox entry exists for the chat, Send, Queue and Steer are disabled (`:140`,
  `:301-303`) and Cmd+Enter is ignored (`:283`).
- Host-returned error (validation, provider busy, etc.): thrown at `web/src/execution.ts:58` **before** the
  outbox removal at `:60`, so a definitive rejection is also left as an "unconfirmed command" and blocks the
  composer. Retrying it replays the stored receipt error forever (`Host/ExecutionHost.swift:229-236`); the only
  way out is "Mark inspected", which is a `window.confirm` (`web/src/ExecutionSurface.tsx:226-230`).
- A second action while one is in flight is silently dropped (`:110`).

### 2.2 Answering a question (`web/src/ExecutionQuestions.tsx:107-117`, `web/src/ExecutionSurface.tsx:258-261`)

Click Answer -> `setSending(true)` disables the whole fieldset (`ExecutionQuestions.tsx:70`, `:82`) -> `operate`
(global busy + refresh) -> on success the id is added to `submitted` and the card stays, disabled, with
"Answer sent; waiting for the provider to settle this question." (`:113`, `:121`) until a poll drops the
request from `pending_interactions` (`web/src/ExecutionSurface.tsx:141-142`). Not optimistic: the card is not
removed and no answer bubble is shown. On unknown delivery the card is disabled with "Answer delivery is
unconfirmed. Inspect or retry its exact command below." (`ExecutionQuestions.tsx:120`) although the retry
control lives in the sidebar. Answer drafts are React state only (`:53`) and the component is keyed by
conversation (`web/src/ExecutionSurface.tsx:258`), so they are lost on switch or reload.

Approvals: same shape. Option buttons call `operate("conversation.approval")` (`:256`); the card remains until
a poll removes it.

### 2.3 Stopping (`web/src/ExecutionSurface.tsx:240`)

Click Stop -> `operate("conversation.interrupt")` -> button disabled via `busy`, status text still "Working"
(`:238`) until a poll reports `running: false`. No "Stopping…" state. Host side holds the queue first, then
calls the provider (`Host/ExecutionHost.swift:374-378`). After the response the 5-call refresh still runs
before `busy` clears.

### 2.4 Creating a schedule (`web/src/ExecutionSchedules.tsx:63-69`, `:117`)

Click Save schedule -> `operate("schedule.upsert")` -> all buttons disabled -> after response + refresh the
`schedules` prop updates and the row appears; only then is the form reset (`:66-68`). No optimistic row; pause,
run-now, delete and mark-read are the same (`:80-82`, `:131`). `window.prompt` is used for editing a queued
prompt and `window.confirm` for cleanup (`web/src/ExecutionSurface.tsx:266`, `:202`).

### 2.5 Other mutations

- Create conversation (`:176-179`): the host spawns the provider process synchronously inside the request and
  under the host lock (`Host/ExecutionHost.swift:336`, `Host/NativeExecutionProvider.swift:63`); the UI shows
  only a disabled button for that whole time, then switches selection and shows "Loading conversation…".
- Model/config select (`:275-280`): controlled by snapshot value. Choosing an option fires `operate`; React
  re-renders with the old value, so the select snaps back and flips to the new value only when a later poll
  returns it. **[inferred]**

---

## 3. Rendering on the web surface

- **Structure.** One ~260-line component holds all state, including the draft (`web/src/ExecutionSurface.tsx:48-70`).
  `LiveMessages` is a plain (non-memo) child (`:26`).
- **Not virtualized.** Every visible entity is mapped to an `<article>` (`:36-45`). The history view in the
  same app already has a `@tanstack/react-virtual` list with anchoring and tail handling
  (`web/src/Transcript.tsx:9`, `:99-134`, `:257`); it is not reused.
- **Merge and keys.** Saved entities whose `native_message_id` is also live are dropped, then live entities are
  appended sorted by `source_order` (`:28-33`). Key is `item_id || entity_id || unknown-<index>` (`:40`).
- **Full re-render per poll and per keystroke.** Every poll stores a new snapshot object (`:101`); every
  keystroke sets `draft` in the same component (`:282`, `:120-124`). Both re-render `LiveMessages`, which passes
  a fresh `message={{...}}` literal to each `MessageContent` (`:42`), defeating its `memo`
  (`web/src/MessageContent.tsx:485`). `useMemo` saves the context/XML/JSON-block passes
  (`MessageContent.tsx:490-498`, `:297`) but `ReactMarkdown` itself re-runs for every message
  (`:300-305`), tool results re-run `formatJsonBlocks` unmemoized (`:503`), and every entity's "Source details"
  `<details>` re-runs `JSON.stringify(entity, null, 2)` and keeps that text in the DOM even when collapsed
  (`web/src/ExecutionSurface.tsx:43`). **[inferred: cost scales with transcript length; not measured]**
- **Markdown.** `react-markdown` + `remark-gfm` (`MessageContent.tsx:2-3`, `:300`), with context-envelope
  sections (`:20-111`), XML cards (`:411-459`), JSON pretty-printing (`:235-286`) and a tool-call field table
  (`:382-409`). No streaming-aware handling: the whole message is re-parsed on each update, so an unterminated
  fence or table re-flows when it closes. No syntax highlighting. Images load from any URL
  (`:288-294`).
- **Scroll.** The transcript is `min-h-40 flex-1 overflow-auto` (`web/src/ExecutionSurface.tsx:246`). There is no
  ref, no effect and no scroll code in any `Execution*.tsx` file. Opening a conversation leaves it at the top;
  streaming does not follow; nothing preserves position. `main` is also `overflow-auto` (`:146`), so on narrow
  screens there are nested scrollers.
- **Loading / empty states.**
  - Spinners: **0**. Skeletons/shimmer: **0**. CSS animations or transitions in `Execution*.tsx`: **0**.
  - "Loading…" text: **1**, the title "Loading conversation…" (`:237`).
  - Wrong-state flashes while loading: status reads "Disconnected" and a "Resume on host" button renders
    whenever `snapshot` is null (`:238-239`); the transcript is blank (`:250`); sidebar shows "Start the
    execution host with a registered workspace." until the first refresh returns (`:180`, initial `[]` at
    `:53`); the run inbox shows "No runs yet." before its first response
    (`web/src/ExecutionSchedules.tsx:125`, initial `[]` at `:30`); the schedules/worktree sections pop in when
    `info` arrives (`web/src/ExecutionSurface.tsx:183`, `:208`).
  - Empty states: "Create or choose a conversation…" (`:235`); "The host is ready for a prompt. Configure this
    conversation before sending." (`:35`), shown for any empty transcript including a disconnected one.
  - Busy states are a single global boolean rendered only as `disabled` (`:67`, `:111`, `:117`).
- **Responsive.** Two-column grid at `md` and up, single column below (`:165`); the sidebar stacks above the
  chat with a bottom border (`:166`); padding steps `p-3 md:p-5` / `p-3 md:p-4` (`:246`, `:252`); rows are
  `flex-wrap`. The textarea is 16 px on mobile (`web/src/components/ui/textarea.tsx:10`). Native `<select>`s
  inherit `text-sm`/`text-xs` (`web/src/ExecutionSurface.tsx:167`, `:275`), which triggers focus zoom on iOS
  Safari **[inferred]**. Tests assert only "no horizontal overflow at 390x844"
  (`web/tests/execution.spec.ts:124-133`, `web/tests/execution-questions.spec.ts:56`, `:63`).
- **Navigation state.** Only `?view=execution` is in the URL (`web/src/App.tsx:1013`, `:167`); the selected
  conversation is component state (`web/src/ExecutionSurface.tsx:57`) and the surface unmounts when leaving
  the view (`web/src/App.tsx:1334-1335`), so reload/back loses the selection.

---

## 4. macOS send through a remote host (`App/RemoteConversationRuntime.swift`)

The macOS UI layer is already optimistic for prompts (`App/LiveConversation.swift:596-620` sets
`pendingPrompt` and clears the draft before `runtime.perform` at `:1058`). What the remote runtime adds:

**Per send** (`perform`, `:83-126`), serial:

1. SSH only: transport check, two main-actor hops plus a libproc fd scan
   (`App/RemoteWorkspaceSSH.swift:79-85`, `App/ExecutionHostConnection.swift:102`).
2. Outbox file write + fsync (`App/ExecutionHostConnection.swift:104-116`).
3. Round trip 1: `conversation.send` (`App/RemoteConversationRuntime.swift:106`). Host: lock, receipt save
   (full catalog write + file fsync + directory fsync, `Host/ExecutionHost.swift:237-238`, `:797-810`),
   synchronous `provider.perform` (`:378`), second save (`:241-244`).
4. Round trip 2: `publish()` -> `conversation.read` full snapshot (`App/RemoteConversationRuntime.swift:122`,
   `:147`), triple-transcoded on the client (`App/ExecutionHostConnection.swift:134`,
   `App/RemoteConversationRuntime.swift:148`).

Local equivalent: one in-process `service.perform` (`App/InAppAgentRuntime.swift:327-330`) and an 80 ms pushed
publish (`:194-198`).

**Streaming after the send:** 1 s sleep between full-snapshot reads (`:69-74`). First token is visible on
average ~0.5 s + RTT + decode after it exists, and output arrives in >= 1 s steps. No change detection: an idle
open conversation still pulls and decodes the full snapshot every second, and relies on a deep `!=` on the main
actor to drop no-ops (`App/LiveConversation.swift:475`).

**Stop:** `conversation.interrupt`, then `conversation.read` every 100 ms until the delivery is `completed`, up
to 10 s (`App/RemoteConversationRuntime.swift:107-121`), then another `publish()` (`:122`). Each of those reads
is a full snapshot built under the host lock. Local polls an in-process status list at 50 ms
(`App/InAppAgentRuntime.swift:332-343`).

**Connect:** 3 to 5 serial round trips before the first snapshot: optional `conversation.list` (`:34-42`),
optional `conversation.import` (`:43-50`), `conversation.read` for identity (`:52`), `conversation.resume`
(`:67`, whose result is a full read per `Host/ExecutionHost.swift:368` and is discarded with `_ =`), then
`publish()` (`:68`). Three of them carry the whole snapshot. Resume starts the provider process synchronously
under the host lock (`Host/ExecutionHost.swift:367`).

**Reconnect:** none. Any error from a poll ends the loop (`:75-78`) and reports "Execution host connection
lost. Work remains on <name>." (`:173-175`), which `LiveConversation` treats as a connection failure: new
generation, `connected/ready = false`, queue held, error shown (`App/LiveConversation.swift:528-534`,
`:554-584`). One slow or dropped poll (sleep/wake, Wi-Fi change, a 65 s stall behind the host lock) tears down
the viewer although the host keeps working. A dead SSH tunnel is restarted inside the next request's transport
check (`App/RemoteWorkspaceSSH.swift:79-85`), but after the loop has exited there is no next request.

---

## 5. Feel problems, ranked by user impact

1. **Web send is not optimistic and the box clears late.** No pending bubble; text stays until send + a
   5-request refresh finish; the message shows only on a later poll.
   `web/src/ExecutionSurface.tsx:109-118`, `:126-138`, `:72-78`.
   Fix: on submit, clear the input and append a local pending bubble keyed by `commandId`; reconcile it against
   `deliveries[].commandId`/`nativeMessageId`; drop the blocking `refresh` from the send path.

2. **Streaming is 1 Hz full-snapshot polling on both remote surfaces.** Text lands in >= 1 s chunks, several
   lines at a time, versus 80 ms push locally.
   `web/src/ExecutionSurface.tsx:98-104`; `App/RemoteConversationRuntime.swift:69-74`;
   `Host/ExecutionHost.swift:670-694`; contrast `App/InAppAgentRuntime.swift:156-161`, `:194-198`.
   Fix: have the host subscribe to the service (the API exists: `RT/sq-acp/Sources/SQACPHost/AgentConversationService.swift:375-389`)
   and expose a stream (SSE from the gateway on a dedicated thread, or a real blocking `conversation.wait`
   woken by that subscription) that carries a revision and deltas. Interim: poll at 150-250 ms while `running`
   and return `{unchanged}` when the client's revision is current.

3. **Web re-parses the whole transcript on every keystroke and every poll; no virtualization.** Typing lag and
   scroll stutter grow with conversation length.
   `web/src/ExecutionSurface.tsx:26-46`, `:62`, `:101`, `:282`; `web/src/MessageContent.tsx:300-305`, `:485`, `:503`.
   Fix: move the composer into its own component, `memo` the list and rows with stable props, render "Source
   details" lazily, and reuse `VirtualMessages` from `web/src/Transcript.tsx:99`.

4. **No scroll-to-bottom or stick-to-bottom on web.** Opens at the top of history; streaming does not follow.
   `web/src/ExecutionSurface.tsx:246-251`.
   Fix: stick to the tail while the user is within a threshold of the bottom, with a "jump to latest" affordance
   (the history list already scrolls to total size at `web/src/Transcript.tsx:257`).

5. **Conversation switch flashes three states.** One frame of the previous transcript, then blank with
   "Loading conversation…", "Disconnected" and a live "Resume on host" button, then content.
   `web/src/ExecutionSurface.tsx:91-93`, `:237-239`, `:250`.
   Fix: key snapshots by conversation id, keep a per-conversation cache and show the last snapshot immediately
   (stale-while-revalidate); render connection state as unknown/shimmer until the first response.

6. **One global `busy` flag freezes every control during any request and its follow-up refresh; Send is
   disabled while the agent runs and Cmd+Enter then silently does nothing.**
   `web/src/ExecutionSurface.tsx:67`, `:109-118`, `:144`, `:283`, `:301-303`; `web/src/ExecutionSchedules.tsx:79-82`.
   Fix: per-action pending state; never disable the composer for unrelated work; make Enter while running
   queue (or steer) by default with visible feedback.

7. **A failed or rejected send jams the composer.** Any error, including a definitive host rejection, leaves an
   "unconfirmed command" that disables Send/Queue until a sidebar retry or a `window.confirm`; the error banner
   is at the top of the page, away from the composer, and never clears on its own.
   `web/src/execution.ts:56-60`; `web/src/ExecutionSurface.tsx:102`, `:140`, `:153`, `:209-231`, `:301-303`.
   Fix: remove the outbox entry on definitive host errors (`rejected` codes); show uncertain sends inline on the
   pending bubble with Retry/Discard; clear poll errors on the next success.

8. **macOS remote viewer dies on a single failed poll.** No retry or backoff; the user must reconnect manually
   while the host keeps running.
   `App/RemoteConversationRuntime.swift:69-79`, `:173-175`; `App/LiveConversation.swift:528-534`, `:554-584`.
   Fix: retry with backoff inside the runtime, keep the last snapshot visible with a quiet "reconnecting"
   indicator, and only surface failure after sustained loss.

9. **macOS remote Stop and send latency.** Stop reads full snapshots at 10 Hz for up to 10 s; send is two
   serial round trips; connect is 3 to 5 with a discarded full read.
   `App/RemoteConversationRuntime.swift:106-122`, `:34-68`.
   Fix: return delivery status and the fresh snapshot in the mutation response; use the `conversation.resume`
   result instead of re-reading; fold identity verification into resume.

10. **One host lock serializes everything, including slow work.** Provider spawn, git worktree operations,
    handoff export and double catalog fsyncs all hold the lock that every viewer's read needs, so all
    transcripts freeze while any of them runs.
    `Host/ExecutionHost.swift:131-137`, `:237-244`, `:284`, `:336`, `:797-810`.
    Fix: do process/git/disk work outside the lock; keep reads lock-light; batch receipt persistence.

11. **Each poll is expensive on the host and the wire.** Unconditional transcript re-read + hash per read,
    transcript in the payload up to three times, no compression, four JSON transcodes.
    `Host/NativeExecutionProvider.swift:106-107`, `:119`, `:157-160`, `:322-329`;
    `RT/agent-runtime/src/conversations.rs:628-644`; `docs/execution-host.md:188`; `src/web.rs:2234-2243`.
    Fix: refresh only on file-version change (as `App/InAppAgentRuntime.swift:206-219` does), send one
    representation per client, gzip, and pass bytes through the gateway without re-parsing.

12. **Settings selects snap back; question answers and approvals are not optimistic; busy disables the
    question fieldset and drops focus.**
    `web/src/ExecutionSurface.tsx:256`, `:259`, `:275-280`; `web/src/ExecutionQuestions.tsx:82`, `:107-121`.
    Fix: hold a local pending value per control; collapse an answered card immediately into a pending answer
    row.

13. **Draft persistence is synchronous `localStorage` on every keystroke, attachments included.**
    `web/src/ExecutionSurface.tsx:120-124`, `:282`; `web/src/execution.ts:77-79`.
    Fix: debounce (about 300 ms) and store attachments separately (IndexedDB).

14. **Selection and place are not durable.** Reload or back/forward loses the selected conversation; leaving
    the view unmounts it; switching conversations discards question drafts.
    `web/src/App.tsx:1013`, `:1334-1335`; `web/src/ExecutionSurface.tsx:57`, `:258`.
    Fix: put the conversation id in the URL and keep per-conversation UI state in a cache.

15. **Native blocking dialogs** for queue edit, cleanup and reconciliation.
    `web/src/ExecutionSurface.tsx:202`, `:227`, `:266`. Fix: inline editors and confirm popovers.

16. **Schedules:** no optimistic rows, "No runs yet." flash, and the runs poll restarts on every mutation
    anywhere. `web/src/ExecutionSchedules.tsx:37-51`, `:63-69`, `:125`.
    Fix: optimistic upsert/pause/delete; distinguish "not loaded" from "empty"; depend on a stable key.

17. **Mobile:** the whole sidebar stacks above the chat, sub-16 px selects zoom on iOS, and pairing means
    pasting a 64-hex token per tab.
    `web/src/ExecutionSurface.tsx:11-17`, `:163`, `:165-167`, `:275`.
    Fix: chat-first layout with the sidebar in a sheet; 16 px controls; pairing by link/QR. **[inferred]**

---

## 6. Correctness and robustness (concrete only)

- **Out-of-order snapshots on macOS remote.** `RemoteConversationRuntime` is an actor that awaits the network
  inside `publish()` (`App/RemoteConversationRuntime.swift:145-171`); the poll loop (`:73`) and the
  post-mutation `publish()` (`:122`) can be in flight together on separate connections and nothing compares
  `thread.snapshotSequence`. If the earlier read returns last, a stale snapshot is applied (echo or running
  state flips back) until the next poll. Fix: drop snapshots older than the last applied sequence, or serialize
  publishes.
- **Successful send reported as uncertain.** If `conversation.send` succeeds but the follow-up `publish()`
  throws (`:106`, `:122-125`), `perform` throws and `LiveConversation` marks the prompt uncertain with an error
  (`App/LiveConversation.swift:1064-1075`); it heals only when a later snapshot arrives, which the dead poll
  loop (section 4) may never deliver.
- **Long conversations become unreadable remotely.** Responses over 32 MiB fail
  (`Host/HostSocketServer.swift:127`) and the failure is rewritten to a generic `transport` error (`:131-132`).
  The message says "Read a smaller conversation page" but `conversation.read` has no paging. With the
  transcript carried up to three times, this is reachable well below 32 MiB of history. Web then shows a sticky
  error and "Loading conversation…" indefinitely; macOS connect fails.
- **Web `localStorage` quota.** A send stores the full request in the outbox while the identical draft is still
  stored (`web/src/execution.ts:45`, `:78`), so about 2 MiB of attachments (base64, twice) can exceed the
  roughly 5 M-character origin quota and the send fails before dispatch **[inferred]**. When `saveDraft` throws,
  `setDraft` is skipped and the keystroke is dropped (`web/src/ExecutionSurface.tsx:120-124`). "Mark inspected"
  copies whole requests to `memex-execution-resolved-v1:*` keys that are never pruned
  (`web/src/execution.ts:68-72`). "Unpair this tab" removes only the pairing key
  (`web/src/ExecutionSurface.tsx:151`); drafts, outbox and resolved prompts stay in `localStorage`.
- **Non-JSON gateway errors.** 403 and 405 are `text/plain` (`src/web.rs:176-192`) but the client always calls
  `response.json()` first (`web/src/execution.ts:56`), so the user sees a `SyntaxError` and a mutation stays in
  the outbox.
- **`conversation.wait` is a busy loop.** It rebuilds the full snapshot every 100 ms under the host lock, and
  returns immediately whenever the conversation is not running (`Host/ExecutionHost.swift:700-707`), so an
  idle long-poller spins. Each waiter also pins one of 8 host slots (`Host/HostSocketServer.swift:15`, `:109`)
  and, via HTTP, one of 8 gateway workers shared with the history UI (`src/web.rs:23`, `:93-121`). Not used by
  this PR's clients, but exposed to MCP callers (`src/control_mcp.rs:134`). It must be fixed before it is used
  as the push mechanism.
- **Gateway worker starvation.** Control requests block a gateway worker for the whole upstream call, up to
  65 s (`src/execution_host.rs:132-133`). A stalled host lock plus a few polling tabs can occupy all 8 workers
  and stall history search too **[inferred]**.
- **URLSession leak in the schedule observer.** Every request builds a new `ExecutionHostHTTPClient`, hence a
  new delegate-backed `URLSession` that is never invalidated (`App/HostScheduleObserver.swift:47`, `:95-99`;
  `App/ExecutionHostConnection.swift:71`, `:206-210`): two per paired host every 15 s. It also reads Keychain
  on the main actor each time (`App/ExecutionHostConnection.swift:208`).
- **Catalog rewrite cost.** Every mutation rewrites and fsyncs the whole catalog twice
  (`Host/ExecutionHost.swift:237-244`, `:797-810`); receipts can retain up to 1000 results of 64 KiB each
  (`:823-825`), so this grows with use.
- **Open question, not verified:** whether `thread.snapshotSequence` advances for streamed text that exists
  only in the ephemeral presentation. If not, `conversation.wait`'s change test (`Host/ExecutionHost.swift:705`)
  would miss token streaming and needs a presentation revision.
- **Untested behaviour.** The web tests stub `conversation.read` with an empty transcript
  (`web/tests/execution.spec.ts:71-78`), so message rendering, streaming, scroll, reconciliation and long
  transcripts have no coverage; the docs also state no real-provider smoke check was run
  (`docs/execution-host.md:220`).
