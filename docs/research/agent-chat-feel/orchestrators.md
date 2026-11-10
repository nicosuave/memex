# Open-source multi-session agent orchestrators: UX engineering study

Date: 2026-11-09. Scope: "Conductor-like" desktop orchestrators, excluding Zed, T3 Code, OpenCode, Codex, Cline/Roo/Continue/Goose.

Evidence labels:
- **VERIFIED** = read in source at the SHA listed in "Clones", cited as `path:line`.
- **VERIFIED (API)** = GitHub REST via `gh api` on 2026-11-09 (stars, push dates, issue titles/bodies).
- **UNVERIFIED** = vendor docs/changelog/README only, or inferred from file names without tracing behavior. URL or reason given.

Clones (all `git clone --depth 1`, read only, nothing built or run), under
`/private/tmp/claude-501/-Volumes-Dev-Code-memex--claude-worktrees-agent-app-ux-patterns-13a76b/f8deb1e4-a002-4452-a81e-c8ef8a51001b/scratchpad/repos/`:

| Dir | Repo | SHA |
|---|---|---|
| orch-orca | stablyai/orca | 5c5cd9344a9acc729108f91e90203ee63bd3ec2b |
| orch-paradev | ParadevAI/Paradev (GitHub redirect target of superset-sh/superset) | cedc22dec3ae5e59a762e2a177a27d6159b4c5dd |
| orch-paseo | getpaseo/paseo | ab64bebe14b8e30c576457292f7e3ec19ab21028 |
| orch-xum | coder/xum (renamed from coder/mux; README states the rename) | c0d42460a06fa8699b4b15e34a63e5d37434c3c0 |
| orch-vibe-kanban | BloopAI/vibe-kanban | ad41bda05844411f03dbe92d36f21a574e1ad9da |
| orch-emdash | generalaction/emdash | 9346e9130902a0eac126c1cc37aa10b125eea99b |
| orch-cmux | manaflow-ai/cmux | b614a9bca3648a7ab484f8b31190f473813f31a1 |
| orch-jean | coollabsio/jean | a5c7e7302644e7fc071c9818c4bd063f7dabe73f |
| orch-nimbalyst | nimbalyst/nimbalyst (successor of stravu/crystal) | 9a46322631c626f2912254aee65520ef3e436561 |
| orch-happy | slopus/happy | 04fd379a9e992fa2d03bd99f44322a53ead8d71a |
| orch-codexmonitor | Dimillian/CodexMonitor | dd61b9abd37de5ded86e82b9fe8a83fd49d46fa5 |

Naming note: several candidates were renamed. `superset-sh/superset` resolves to `ParadevAI/Paradev` (packages are still `@superset/*`); `coder/mux` to `coder/xum`; `getAsterisk/opcode` to `winfunc/opcode`; `AndyMik90/Auto-Claude` to `AndyMik90/Aperant`; `stravu/crystal` says "now Nimbalyst". I use the current names with the old one in parentheses.

---

## 1. TL;DR ranking

Ranked by UX engineering that is transferable to a native Swift app with instant new chat, instant switching, and clear background status.

1. **Orca (stablyai/orca)** — best new-session t=0 and fastest worktrees. It speculatively builds a locked, detached, fully checked-out worktree while the user is still in the composer, then turns it into the real workspace with `git worktree move` + `checkout -b` on submit. The pending chat pane takes the first message immediately and holds it ("Sends when the worktree is ready") with an inline step checklist and Retry.
2. **Paseo (getpaseo/paseo)** — best streaming feel and best-documented sync model. Leading+trailing 60 ms coalescer on the daemon, one store commit per frame on the client, backlog-proportional paced text reveal, epoch+seq cursors with a `current`/`behind` freshness check so switching between open chats needs no fetch. Measured before/after numbers are checked in.
3. **Paradev (ex-Superset)** — best protocol design decisions and optimistic plumbing. Client-minted workspace UUID as idempotency key, enqueue + settled-event creation, full-item-snapshot wire protocol with ephemeral deltas, one reducer for live and replay, an outbox with `clientId` echo reconciliation, steer with `expectedTurnId` compare-and-set. Its creating state is still a full-pane spinner, which is the weak spot.
4. **xum (ex-Mux, coder)** — best session-switch hydration and background-session economy. Only the active workspace holds a live transcript subscription; everything else gets a small activity snapshot. Cached rows paint immediately and a `since` replay patches them; a skeleton is used only for non-incremental catch-up and is bounded at 10 s. Worktree is reserved with `--no-checkout` and materialized in the background while the workspace is already open. Compositor-only text shimmer.

Worth mining but not in the top four:
- **cmux (manaflow)** — the only native Swift/AppKit app here; directly reusable sidebar and spinner patterns (section 3.6).
- **Emdash** — checked-in benchmark of copy-on-write worktree strategies on APFS (section 4).
- **Vibe Kanban** — the JSON-patch stream reference, but the project is sunsetting (README h1, VERIFIED `README.md:19-22`) and its transport has no cursor/replay (section 3.5).

---

## 2. Ecosystem table

Stars and last push are VERIFIED (API) on 2026-11-09. "Agent transport" is VERIFIED from manifests/source for cloned repos (first 11 rows) and UNVERIFIED (README keyword scan) for the rest.

| # | App (repo) | Stack | Stars / last push | Agent transport | Quality signal |
|---|---|---|---|---|---|
| 1 | Orca (stablyai/orca) | Electron + React + zustand, xterm, Monaco, Expo mobile | 120,374 / 2026-11-09, release v1.4.255 same day | Own ACP JSON-RPC peer (`src/main/acp/transport`), Claude Agent SDK, per-agent hooks posting to a loopback hook server, OSC 7501 status from PTY, JSONL transcript tailing for adoption (`src/main/agent-hooks/README.md:1-24`, `src/main/native-chat/transcript-*.ts`) | Very high velocity; design docs and perf-driven comments in tree; 8,268 open issues |
| 2 | Paradev (ex-Superset) | Electron + React, tRPC host-service, TanStack DB, pty-daemon, Expo mobile | 16,150 / 2026-11-09, desktop-v2.0.7 same day | Claude Agent SDK direct, `codex app-server` adapter, ACP adapters (`packages/chat-runtime/src/harness/{claude,codex,acp}`), PTY for terminal agents | Decision logs and measured perf plans in `plans/` |
| 3 | Paseo (getpaseo/paseo) | Node daemon + Expo (web/iOS/Android) + Electron wrapper, WebSocket, zod | 23,653 / 2026-11-09, v0.15.3 | Claude Agent SDK, Codex, ACP SDK, OpenCode SDK, Pi (`docs/agent-stream-performance.md:20`) | Best docs of the group; perf specs with recorded healthy numbers |
| 4 | xum (ex-Mux, coder) | Electron + React, oRPC, ghostty-web, own agent loop | 2,072 / 2026-11-09, nightly | Own provider loop (not a wrapper around Claude Code); ACP SDK present as dependency | Extremely defensive state machine code; 468 open issues |
| 5 | Vibe Kanban (BloopAI) | Rust (axum, sqlx) + React, Tauri shell | 28,631 / 2026-11-09 (last release 2026-04-24) | Rust executors per agent incl. `agent-client-protocol` crate; logs normalized to JSON-patch streams | Sunsetting; good normalized-log idea, weak replay |
| 6 | Emdash (generalaction) | Electron + React + MobX, better-sqlite3, Monaco | 6,229 / 2026-11-06, v1.2.11 | ACP (claude-agent-acp, codex-acp), Claude Agent SDK, node-pty | Checked-in CoW benchmark; past MobX deep-observe lag regression |
| 7 | cmux (manaflow-ai) | Native Swift/AppKit + libghostty, Rust TUI/daemon, web chat bundle | 28,739 / 2026-11-09 | Terminal-first: agents run in PTYs; status via hooks + notifications (`Sources/AgentHookDelivery*.swift`) | Native perf craft; 6,162 open issues, memory and latency complaints |
| 8 | Nimbalyst (ex-Crystal) | Electron + React, PGlite, jotai/zustand, iOS app | 2,050 / 2026-11-09, v0.82.8 | Claude Agent SDK, Codex SDK, OpenCode SDK, ACP | Active; broad scope (docs/trackers) |
| 9 | Happy (slopus/happy) | Swift/Expo clients, Node CLI + server, socket.io relay | 24,558 / 2026-11-09 | CLI wraps Claude Agent SDK and ACP; E2E relay to mobile | Remote-control focus; 1,105 open issues |
| 10 | Jean (coollabsio) | Tauri (Rust core) + React, ghostty-web/xterm | 1,403 / 2026-11-09, v1.0.14 | CLI stream-json per agent; one shared `codex app-server` (`jean-core/src/agent_browser/mod.rs:346`) | Small, active |
| 11 | CodexMonitor (Dimillian) | Tauri + React, portable-pty | 4,366 / 2026-03-26 (stale) | `codex app-server` events re-emitted as Tauri events (`src-tauri/src/event_sink.rs:18`) | Stalled since March |
| 12 | opcode (winfunc, ex-Claudia) | Tauri + React | 22,433 / 2026-11-06 | Claude Code CLI (UNVERIFIED) | Popular; single-agent GUI more than orchestrator |
| 13 | Claude Squad (smtg-ai) | Go TUI | 8,760 / 2026-08-20 | tmux panes + worktrees, PTY scraping (UNVERIFIED) | Slowing |
| 14 | Agent Deck (asheshgoplani) | Go TUI (Bubble Tea) | 1,238 / 2026-11-09 | tmux sessions, status from pane scraping (UNVERIFIED) | Active TUI |
| 15 | Aperant (ex-Auto-Claude) | Electron + kanban | 14,599 / 2026-06-14 | Claude SDK (UNVERIFIED) | Stale since June |
| 16 | 1Code (21st-dev) | Electron | 5,574 / 2026-03-06, archived | Claude Code/Codex (UNVERIFIED) | Archived |
| 17 | Sculptor (imbue-ai) | Python backend, containers | 246 / 2026-11-09 | Agents in Docker containers (UNVERIFIED) | Small OSS footprint |
| 18 | Omnara | Go platform, Expo | 3,372 / 2026-11-09 | Claude Agent SDK; pivoted to "managed agent platform" (UNVERIFIED) | Pivoted away from desktop |
| 19 | Constellagent | Electron, node-pty | 217 / 2026-05-05 | PTY terminals per agent (UNVERIFIED) | Stale |
| 20 | Commander (autohandai) | Tauri | 88 / 2026-06-09 | CLI agents (UNVERIFIED) | Stale |
| 21 | Chorus (meltylabs) | Tauri chat app | 776 / 2026-01-09 | Model APIs; not a coding orchestrator (UNVERIFIED) | Stale, off-category |
| 22 | Kanna (jakemor) | Web UI | 724 / 2026-11-06 | Claude Agent SDK + `codex app-server`, JSONL (UNVERIFIED) | Small, active |
| 23 | Craft Agents OSS | Electron | 7,320 / 2026-11-03 | Claude Agent SDK (UNVERIFIED) | Active |
| 24 | CodeLayer (humanlayer) | Tauri/Electron + Go daemon | 12,848 / 2026-11-05 | Claude Code sessions via daemon (UNVERIFIED) | Active |
| 25 | codeg (spacering-net) | Rust + web, desktop/self-host | 4,412 / 2026-11-06 | ACP (UNVERIFIED) | Active |
| 26 | Kandev (kdlbs) | Go + web, kanban | 1,106 / 2026-11-09 | ACP + `codex app-server` + PTY (UNVERIFIED) | Active |
| 27 | Maestro (RunMaestro) | Electron | 3,562 / 2026-11-09 | CLI agents, JSONL (UNVERIFIED) | Active |
| 28 | Parallel Code (johannesjo) | Electron | 1,083 / 2026-11-09 | CLI agents in worktrees (UNVERIFIED) | Active |
| 29 | HAPI (tiann) | TS + native clients | 5,387 / 2026-11-09 | Remote wrapper for CLI agents (UNVERIFIED) | Active |
| 30 | Catnip (wandb) | — | repo 404 on 2026-11-09 | — | Gone |
| C1 | Conductor (closed) | macOS app | n/a | Claude Code + Codex (UNVERIFIED) | See 3.7 |
| C2 | Cursor Agents Window (closed) | Electron | n/a | Own agent (UNVERIFIED) | See 3.7 |

Why these four for source study: they are the only ones where the new-session path, the stream transport, and switch hydration are each engineered deliberately and documented in-tree, and all four shipped a release within a week of this study.

---

## 3. Deep dives

### 3.1 Orca (stablyai/orca @ 5c5cd93)

#### t=0 sequence for "new chat in a new worktree" (VERIFIED)

| t | What happens | Where |
|---|---|---|
| before click | Selecting a git repo in the composer fires `prefetchWorktreeCreateBase(repoId, baseBranch)` in an effect. No user intent to create yet. | `src/renderer/src/hooks/composer-state/linked-item-lookup-effects.ts:74-78` |
| before click | Main process fetches the base ref and, in parallel, starts a **speculative prepared worktree**: `git worktree add --detach --no-checkout --lock --reason <id> <root>/.orca-preparing/<pid-uuid> <base>`; then materializes with `git -c checkout.workers=0 reset --hard <base>`. | `src/main/workspaces/creation/worktree-create-base-prefetch.ts:100-105,121-160`; `src/main/git/worktrees/creation/worktree-preparation-add.ts:64-79,95`; `.../worktree-create-preparation.ts:98-108`; `src/main/git/command-runner/checkout-workers-git-args.ts:8-9` |
| 0 ms (submit) | Renderer mints `creationId`, inserts a pending entry, selects it, opens the sidebar. No await before this. | `src/renderer/src/lib/workspace/creation/worktree-creation-flow.ts:94-113,65-92` |
| 0 ms | Sidebar shows an optimistic row: name + live progress label ("Checking out files… 42%") + hover Cancel. | `src/renderer/src/components/sidebar/worktree-list/rows/PendingWorktreeRow.tsx:40-112` |
| 0 ms | Main pane is the real chat UI: empty state + **live composer**, focused on the next frame. | `src/renderer/src/components/worktree-creation/PendingNativeChatCreation.tsx:57,166-177,231-246` |
| user sends | Message is held, rendered as a normal user row immediately; composer shows "Sends when the worktree is ready". Step checklist appears under it only after a send. | `PendingNativeChatCreation.tsx:87,103-107,113-137,190-229` |
| submit + n | Host claims the prepared checkout, verifies lock ownership, `reset --hard` only if the tip moved, then `git worktree move -f -f <prepared> <final>` and `git checkout --no-track -b <branch> <head>`, sets upstream config, unlocks. | `src/main/git/worktrees/creation/worktree-create-preparation.ts:142-262` |
| during | Phases `preparing → fetching → creating` are logged with timestamps; checklist rows show per-step elapsed time and git's own "Updating files" percent. | `src/renderer/src/store/slices/worktrees/create/pending-worktree-creation.ts:16-41`; `src/renderer/src/components/worktree-creation/worktree-setup-steps.ts:25-87`; `src/main/git/worktrees/creation/worktree-checkout-progress.ts:14-34` |
| failure | Same card turns the failed step red, prints the error, offers Retry/Dismiss. Retry reuses the captured request; if chat input was already handed off it retries only the agent launch. | `WorktreeSetupSteps.tsx:98-111`; `worktree-creation-flow.ts:154-183` |
| ready | Held draft and attachments move to the real session; pending entry removed. | `pending-worktree-creation.ts:94-121` (draft scope cleanup), `worktree-creation-structured-session.ts:67` |

Nothing in this path blocks first paint or first prompt. The only spinners are 14 px `Loader2` icons inside the step list and the sidebar row.

#### Technique table (VERIFIED unless noted)

| What | Where | Why it feels good |
|---|---|---|
| Speculative prepared-worktree pool: keyed by repo+root+base, cap 3, TTL 5 min, oldest-of-same-workspace evicted first | `src/main/workspaces/creation/preparation/worktree-create-preparation-pool.ts:45,107-127,160-182`; TTL `src/main/git/worktrees/creation/worktree-preparation-operation.ts:12` | Create cost is paid during composer think-time; submit is a rename plus a branch create |
| Burst re-arm: a replacement checkout is prepared only if the previous one was consumed within 5 min | `.../preparation/worktree-create-preparation-burst.ts:3-13` | Keeps disk cost bounded for one-off creates, stays instant for people spawning many |
| Lock reason carries pid + machine identity; every step re-verifies ownership; stale preparations are cleaned on next start | `worktree-create-preparation-pool.ts:221-225,252-256`; `worktree-preparation-add.ts:95-101` | Crash-safe; never deletes a checkout it does not own |
| Materialize via `reset --hard` instead of `checkout` so user post-checkout hooks are deferred to submission | `worktree-create-preparation.ts:98` | Speculation has no user-visible side effects |
| `-c checkout.workers=0` unless the user configured it | `checkout-workers-git-args.ts:8-22` | Parallel checkout on large repos for free |
| APFS clone of configured linked paths: probes `df` + `diskutil info -plist` once per device (5 s timeout), then `/bin/cp -c` (file) or `/bin/cp -n -c -R` (dir), atomic via temp + `link` | `src/main/ipc/worktree-apfs-clone.ts:15,53-76,94-109,138-185` | Dependencies arrive without a reinstall; falls back to plain copy off-APFS |
| Sparse worktree option | `src/main/git/worktrees/creation/worktree-sparse-add.ts:13-30` | Monorepo creates stay small |
| Pending creation is first-class state with its own draft scope, attachments, and chat option picks | `pending-worktree-creation.ts:53-75`; `PendingNativeChatCreation.tsx:143-147` | User can attach files and pick a model before the workspace exists |
| No-op phase re-emits are dropped before `set` | `pending-worktree-creation.ts:76-83` | Progress events do not re-render the panel |
| Status from two channels with fixed precedence: managed hooks win; OSC 7501 fills in; hooks silent for 2 min yield | `src/main/agent-hooks/README.md:1-24` | Background status stays right even for agents with no integration |
| One status stream per host, map updated in place, per-session subscribers, summaries retained across disconnect | `src/renderer/src/runtime/native-chat/status/structured-agent-session-status-feed.ts:1,28-44,56-58` | One session's change does not wake every sidebar row |
| Text shimmer via `background-clip:text` gradient with `prefers-reduced-motion` off-switch; `shimmer-steps-N` utility caps repaints for long-lived shimmers | `src/renderer/src/assets/text-shimmer.css:27-67,95-98`; used at `components/sidebar/workspace-card/surface/WorktreeCardStatusSlot.tsx:191` | Comment states the reason: `background-position` animates on the main thread |
| Tool-run and awaiting-input rows use `animate-pulse` with `motion-reduce:animate-none`, not spinners | `components/native-chat/tools/runs/NativeChatToolRun.tsx:212`; `prompts/NativeChatAwaitingInputRow.tsx:63` | In-progress is a text treatment |
| Shimmer placeholder rows sized to fill the viewport | `components/task-page/github/List.tsx:229` | No height jump when results land |

Not traced (UNVERIFIED, file names only): queued-message cards with reorder/edit (`components/native-chat/queue/*`), transcript windowing (`viewport/virtualization/*`), older-history paging and per-source retention while hidden (`viewport/history/use-native-chat-history-retention.ts:5`), the durable session journal with replay/rewind (`src/main/native-chat/agent-session-journal/{replay,context-epochs}`).

Where spinners remain: step list icons, sidebar pending row, fork action button, usage card.

---

### 3.2 Paseo (getpaseo/paseo @ ab64beb)

#### t=0 sequence (VERIFIED)

| t | What happens | Where |
|---|---|---|
| 0 ms | Submit moves a reducer from `draft` to `creating`, carrying a `CreateAttempt` with a client-minted `clientMessageId`, the text, timestamp, images, attachments, and a client-built `draftAgent` preview. | `packages/app/src/composer/draft/create-flow.ts:14-24,46-48,60-69` |
| 0 ms | The chat renders from the draft agent; the user message row already has its final identity. | `create-flow.ts:149-160`; identity rule `docs/timeline-sync.md:267-270,319-323` |
| RPC returns | `handoffCreatedAgentMessageSubmission` binds the optimistic message to the real agent id; lifecycle marked `sent`. | `create-flow.ts:187-200` |
| failure | State returns to `draft` with the error message; nothing typed is lost. | `create-flow.ts:49-54` |
| user navigates away mid-create | The create is issued in the background and the consumed draft is cleared only if no newer edit touched it (version check). | `packages/app/src/screens/new-workspace/background-handoff.ts:12-36,38-54` |
| worktree setup | Setup belongs to the workspace and is shown in a Setup panel from per-command snapshots; it never becomes chat rows. | `docs/agent-lifecycle.md:147` |

Paseo does not pre-create worktrees. Its strength is everything after the first byte.

#### Technique table (VERIFIED)

| What | Where | Why it feels good |
|---|---|---|
| Daemon coalescer, leading + trailing, at most one message per 60 ms per agent | `packages/server/src/server/agent/agent-stream-coalescer.ts:4`; `docs/agent-stream-performance.md:11,29` | First character is not delayed by the window; daemon loop is not flooded |
| Client reducer queue commits once per frame with a timer ceiling for hidden tabs | `docs/agent-stream-performance.md:36` | One store write per frame; background sessions still advance |
| Turn boundary flushes the queue before liveness changes | `docs/agent-stream-performance.md:37` | Avoids "finished turn with no reply, footer drops, double jump" |
| Paced reveal: arrival sets a target, reveal step = `ceil(backlog * elapsed / 150ms)`, min 1 char, capped at 60 Hz, elapsed clamped to 250 ms, grapheme-safe cut | `packages/app/src/agent-stream/text-reveal.ts:21,24,29,36-61,87-97` | Bursty arrival reads as smooth typing. Recorded result: frames that advanced text 6% → 87%, p95 gap between visible updates 383 ms → 17 ms (`docs/agent-stream-performance.md:62-67`) |
| Store holds full text; only the rendered slice is paced. First sight of any text is revealed whole. Leaving `streaming` snaps. | `docs/agent-stream-performance.md:31,34,35` | History, replay, and remounted virtual rows never animate; copy/scroll geometry stay consistent |
| Row memoization on item identity + layout identity | `docs/agent-stream-performance.md:38-45` | Measured 100–250 ms JS per tick before the fix on ~50 mounted rows |
| Streaming Markdown: split at block boundary, memoize the closed head, reparse only the tail | `docs/agent-stream-performance.md:51-53` | Per-frame parse cost does not grow with message length |
| Two delivery paths: live deltas for immediacy, paged fetch of projected items for correctness; gaps fetched `after` until `hasNewer:false` | `docs/timeline-sync.md:3-6,50-56` | Dropped frames self-heal without a full reload |
| Subscribe carries `{epoch, seq}`; daemon answers `current` or `behind` per chat | `docs/timeline-sync.md:109-116` | Switching between open chats needs no fetch (`:106`); resume of an unchanged chat shows nothing |
| "Unsettled turn" hold: while catch-up is owed, keep showing the running footer; clear it in the same store write as the rows | `docs/timeline-sync.md:126-143` | No flicker to "idle" and back when returning to a chat |
| Catch-up retry backoff 1 s → 30 s, reset on reconnect/visibility; user Retry owns a published `retrying` status | `docs/timeline-sync.md:75-84` | No idle request storm; button reflects reality |
| Item size budgets: tool calls 64 KiB, other items 256 KiB, with truncation markers | `docs/timeline-sync.md:23-33` | One huge diff cannot stall the socket |
| Presence is not delivery: heartbeat (visibility, focused agent, last activity) only routes notifications | `docs/timeline-sync.md:35-46` | Stale focus never drops rows |
| Notification plan: suppress if a present, visible client is focused on the target; else in-app to the most recently active present client; else push. Presence threshold 180 s. | `packages/server/src/server/agent-attention-policy.ts:3,42-76` | One notification, on the device you are using |
| Attention clears on focus for `finished`/`error`, never for `permission` | `packages/app/src/utils/agent-attention.ts:17-21` | Permission badge persists until answered |
| Fork/rewind anchor = timeline `epoch` + projected item `seqEnd` | `docs/timeline-sync.md:92-102` | Edit-and-fork works on items without provider ids |
| Terminal pipeline in a forked worker: headless xterm parse, 5 ms leading+trailing coalescer before IPC, binary WS frames, per-viewer replay, hidden terminals in the focused workspace keep streams | `docs/terminal-performance.md:8-16,24-25,34` | Terminal floods do not stall chat; tab switches do not resubscribe. Healthy echo p50 ~2.3 ms recorded (`:41`) |
| `.worktreeinclude` plugin: copy only files that match the include file AND are gitignored, using `COPYFILE_FICLONE` | `plugins/worktree-include/server/copy.ts:39-70` (clone flag at `:67`) | `.env` and caches arrive via CoW, tracked files untouched |

Known contention they document themselves: a single 250 KB `agent_stream` message delays terminal echo by ~100 ms (`docs/terminal-performance.md:51`).

---

### 3.3 Paradev (ex-Superset @ cedc22d)

#### t=0 sequence (VERIFIED)

| t | What happens | Where |
|---|---|---|
| 0 ms | Renderer mints the workspace UUID and upserts an optimistic row into the host's cached list (`worktreePath: ""`, name from input or "New workspace"). | `apps/desktop/src/renderer/stores/workspace-creates/useWorkspaceCreates.ts:252-258,310-342` |
| 0 ms | An empty pane layout is written for that id and the UI navigates to the optimistic id. | `useWorkspaceCreates.ts:395-400`; `.../useBranchPickerController.ts:92` |
| 0 ms | Subscribes to `workspace:create-settled` **before** calling `createEnqueued`; the mutation validates and returns immediately. | `useWorkspaceCreates.ts:141-174`; `packages/host-service/src/trpc/router/workspaces/workspaces.ts:1438-1497` |
| during | Pane shows `WorkspaceCreatingState`: a spinner, name, branch, two steps (Preparing, Starting), elapsed counter, "stuck" state at 30 s. | `.../v2-workspace/components/WorkspaceCreatingState/WorkspaceCreatingState.tsx:26-41,46-50` |
| host | Runs the real create in the background, broadcasts settle with terminals/agents. Setup scripts run in a server-created terminal; with `waitForSetupBeforeAgents` the sole agent launch is chained behind setup. | `workspaces.ts:1462-1493,1310-1320`; `docs/V2_WORKSPACE_SETUP_SCRIPTS.md:33-37` |
| settled | Pane layout rewritten with real terminal/agent panes; pending UI held until then so the user never lands in a briefly empty workspace. | `useWorkspaceCreates.ts:402-417,457-468` |
| lost event | 10 min backstop, then probe the row; if it exists keep it and read launched agents back. | `useWorkspaceCreates.ts:59-61,189-217` |
| failure | Optimistic row removed; a `failedWorkspaceCreates` record stores the full input snapshot and error, rendered as `WorkspaceCreateErrorState` at the same route. | `useWorkspaceCreates.ts:260-271,448-455`; `.../$workspaceId/layout.tsx:109-116,196-199` |

The prompt is accepted before the workspace exists (it travels in the create input), but the user cannot keep typing in the target chat while it builds. That is the gap versus Orca.

#### Technique table (VERIFIED)

| What | Where | Why it feels good |
|---|---|---|
| Client-minted id is the row id and the idempotency key; retry with the same id returns the existing row; lost insert race cleans up its folder | `packages/host-service/src/trpc/router/workspace-creation/procedures/create-session.ts:35-36,79-91,147-171` | Optimistic row and server row are the same entity; retries cannot duplicate |
| Enqueue + event instead of a long-held request | `workspaces.ts:1438-1447` | Stated reason: a minutes-long create pins one of Chromium's six sockets and exceeds a 30 s relay cap |
| AI naming is async and only relabels; folder never renames | `create-session.ts:93-104,174-182` | Name appears later without moving anything |
| Terminal panes are created optimistically; WebSocket open creates-or-attaches; initial command is queued behind an OSC 133 shell-ready gate with a timeout | `docs/V2_WORKSPACE_SETUP_SCRIPTS.md:5-31,39-56` | Pane mounts instantly; setup output replays on attach |
| Wire protocol decisions, each with a one-line reason: full item snapshots upserted by id (no patch semantics); one pure reducer for live and replay; deltas ephemeral; epoch-qualified cursors; approvals as transcript items; FIFO queue with visible `queued` tag plus explicit `steer` with `expectedTurnId` CAS; edit = fork, never truncate; optimistic sends reconciled by echoed `clientId` | `plans/20260804-chat-protocol-decisions.md:5-14` | Removes "history renders differently than live" and reorder/duplicate corruption as bug classes |
| Reducer: copy-on-write maps per batch; durable `item` upsert clears that item's live delta streams; epoch mismatch sets `pendingReset` instead of splicing | `packages/chat/src/core/reducer/reducer.ts:53-87,104-118,145-149,176-184` | One state build path; cheap batches |
| Stream client: reconnect with `since=<cursor>`, backoff 250 ms → 10 s, reset envelope nulls the cursor | `packages/chat/src/client/subscribeToSession/subscribeToSession.ts:41-42,101-105,109-117` | Resume is the normal path |
| Per-frame batching: envelopes buffered, committed in one `reduceMany` on rAF (16 ms timer fallback) | `packages/chat/src/react/useChatSession/useChatSession.ts:50-61,278-296` | One React commit per frame regardless of token rate |
| Outline seed + lazy bodies: mount from an outline, fetch omitted bodies in microtask-batched chunks of 100, 3 attempts with backoff | `useChatSession.ts:68-97,307-351` | Long sessions open fast |
| Snapshot LRU (5 sessions) and `prefetchChatSession` priming used by `warmTranscript` | `useChatSession/utils/snapshotCache/snapshotCache.ts:3-45`; `useChatSession.ts:99-114,221-223`; `.../AcpChatPane/utils/warmTranscript/warmTranscript.ts:18` | Switching back paints from memory on the first render |
| Outbox: `queued`/`inflight`/`failed`, 5 attempts, flush when connection opens, confirm by echoed `clientId`, retry/discard | `packages/chat/src/core/outbox/outbox.ts:3,34,55-97`; `useChatSession.ts:200-209,274-276` | Send is instant and survives disconnects; failure is visible and recoverable |
| Every command carries a `commandId`; queued prompts can be removed, edited, steered; `cancelTurn({pauseQueue})` | `packages/chat/src/client/sessionClient/sessionClient.ts:162-216` | Idempotent commands; queue is user-editable |
| Per-turn file changes and undo; optional worktree snapshot per prompt | `sessionClient.ts:78-89,48,169` | Checkpoint-style restore scoped to a turn |
| Sidebar status store: per-workspace external store, rows subscribe to their own entry, notifications flushed after commit | `.../DashboardSidebarWorkspaceStatusProvider.tsx:59-64,97-128` | Status change re-renders one row |
| Status priority fold with early exit at `permission`; acknowledging clears `review` only | `apps/desktop/src/shared/tabs-types.ts:36-41,61-76,82-90` | Workspace dot = most urgent pane; seen-ness is explicit |
| Optimistic model/mode pickers | `.../AgentComposer/hooks/useOptimisticSelections/useOptimisticSelections.ts:19` | Picker reflects the click immediately |

Measured lessons checked into `plans/20260717-host-service-git-worker-pool.md` (VERIFIED `:9-16,27-45`): capping watcher path collection at 128 unique paths then emitting a broad invalidation cut p99 event-loop delay from 87.2 ms to 13.6 ms; a worker hop for medium batches made it worse (47.4 ms); raising the git limiter from 4 to 6 did not help; one flooding terminal once took down the shared daemon socket for every terminal.

---

### 3.4 xum (ex-Mux, coder/xum @ c0d4246)

#### t=0 sequence (VERIFIED)

| t | What happens | Where |
|---|---|---|
| draft | Creation is a persisted draft with its own model/agent choices; send is frozen from submit until settle. | `src/browser/features/ChatInput/useCreationWorkspace.ts:353-358,442-448` |
| submit | For worktree runtime the name is not awaited (`canHandOff`): the backend names the workspace and renames its branch before the init hook. Other runtimes wait for the generated name. | `useCreationWorkspace.ts:228-233,600-608` |
| create | `git worktree add --no-checkout` reserves the directory and branch using local refs only; the origin fetch is deferred. Returns `pendingMaterialization`. | `src/node/worktree/WorktreeManager.ts:309-313,327-340,366-368` |
| return | Workspace is announced; client navigates to it immediately unless the user left the draft. | `useCreationWorkspace.ts:683-697` |
| 0 ms in workspace | A pending-send transcript row is shown "from the moment it opens until the backend persists the first message". | `useCreationWorkspace.ts:506-509` |
| background | Fetch, checkout, and init hook stream into an init card; init lines are applied in idle callbacks. | `WorktreeManager.ts:309-312,322-324`; `src/node/services/initStateManager.ts:85-90`; `src/browser/stores/WorkspaceStore.ts:1268` |
| agent | Tools await `waitForInit()`; the turn itself can start. | `initStateManager.ts:92-94` |
| failure after create | The failed send's draft is moved into the new workspace's composer so retry happens there, not as a duplicate workspace. | `useCreationWorkspace.ts:270-274` |

#### Technique table (VERIFIED)

| What | Where | Why it feels good |
|---|---|---|
| Live transcript subscription only for the active workspace (plus pinned side chats); background workspaces get `WorkspaceActivitySnapshot` (streaming flag, generation, recency, status) | `src/browser/stores/WorkspaceStore.ts:1146-1149,1166-1167`; `src/common/orpc/schemas/workspace.ts:437-460` | Cost of N background sessions is N tiny records, not N transcripts |
| Stream completion vs abort distinguished by whether `recency` advanced after `streaming=true` | `WorkspaceStore.ts:1172-1175` | "Done" notification only on real completion |
| Sidebar status precedence in four tiers, different sources for active vs inactive | `WorkspaceStore.ts:971-996` | No flapping between stale persisted and live status |
| Switch hydration: cached rows stay painted during a `since` replay; server verifies rows up to the cursor and swaps rows after it in one commit | `WorkspaceStore.ts:225-235` | Return to a session is instant and only the tail changes |
| Skeleton only for non-incremental catch-up, bounded at 10 s, then fall back to cached rows plus a dock shimmer | `WorkspaceStore.ts:744-750` | A hung backend cannot hide a readable conversation |
| Failed replay keeps cached rows as provisional, closes the mutation barrier, retries with backoff | `WorkspaceStore.ts:214-219` | Read stays available; writes cannot race stale state |
| Replay windowing: full replay sends only newest rows; older pages on demand; replay events arrive batched | `WorkspaceStore.ts:1074-1076` | Long histories open fast |
| Queue dispatch mode `tool-end` vs `turn-end`; refused queued messages are kept as "held inputs" | `src/common/orpc/schemas/stream.ts:739`; `WorkspaceStore.ts:205-207` | Steer at next tool boundary or wait for the turn, user's choice; nothing is silently dropped |
| Review panel refreshes from a per-workspace "file-modifying tool completed" timestamp | `WorkspaceStore.ts:1261-1266` | Diff updates when it can have changed, not on a poll |
| Text shimmer animates only `transform` on a masked duplicate, with `contain: paint` and a reduced-motion off switch | `src/browser/features/AIElements/Shimmer.tsx:16-22`; `src/browser/styles/transcript-content.css:486-545` | Compositor-only; comment cites per-frame paint cost of animating `background-position` in the streaming transcript |

Caveat: xum runs its own agent loop, so "tools wait for init while the model starts" is easier for it than for an ACP client whose agent process needs a populated cwd.

---

### 3.5 Vibe Kanban transport (BloopAI/vibe-kanban @ ad41bda), partial read

| What | Where | Assessment |
|---|---|---|
| Every executor's output is normalized into JSON Patch ops against `{entries: []}`; server keeps history in a `VecDeque` (100 MB cap) plus a `broadcast` channel (capacity 100,000) | `crates/utils/src/msg_store.rs:12-13,37-47,49-63` | Uniform normalized log model across agents is the good idea |
| New subscribers get full history, then live | `msg_store.rs:100-121` | No cursor; every reconnect replays everything |
| Lagging subscriber: messages are dropped and only logged | `msg_store.rs:110-116` | Client state can silently diverge with index-based patches |
| Client batches ops per animation frame, dedupes, applies with immer | `packages/web-core/src/shared/lib/streamJsonPatchEntries.ts:39-40,56-81` | Same per-frame batching as the others |
| Worktrees are disposable: `ensure_worktree_exists` recreates under a per-path lock | `crates/worktree-manager/src/worktree_manager.rs:86-116` | Lets idle workspaces drop their checkout |

Paradev considered and rejected patch semantics for this reason: "Duplicates/reordering can't corrupt state" with full item snapshots (`plans/20260804-chat-protocol-decisions.md:7`).

### 3.6 cmux (manaflow-ai/cmux @ b614a9b), native Swift patterns, partial read

| What | Where | Why it matters for a Swift app |
|---|---|---|
| One `@Observable` snapshot owner; the dictionary is `@ObservationIgnored` and only a `revision` counter is observed; `refresh` bumps the revision only if a snapshot changed | `Sources/Sidebar/SidebarRowSnapshotCache.swift:4-18,55-67` | Avoids CoW copies of the whole dictionary and whole-list invalidation per event |
| Sidebar is an AppKit `NSTableView` with cell models, row-height cache, and a mutation scheduler | `Sources/Sidebar/AppKitList/*` (file list) | They moved the hot list off SwiftUI `List` |
| Selection coalescer: leading edge applies immediately, clicks inside 100 ms replace the pending request, one trailing fire; press highlight stays instant | `Sources/Sidebar/AppKitList/SidebarSelectionCoalescer.swift:3-12,26-49` | Rapid session switching does not queue one full content swap per click |
| GPU spinner: only `transform.rotation.z` is animated; animation removed off-window, when occluded, or under Reduce Motion | `Sources/Sidebar/GPUSpinner.swift:4-6`; `GPUSpinnerNSView.swift:157,205,266` | Zero per-frame CPU for N background-session indicators |

### 3.7 Closed source (all UNVERIFIED, vendor pages only)

Conductor (https://www.conductor.build/docs/concepts/workspaces-and-branches, https://www.conductor.build/changelog):
- Workspaces are git worktrees; a new workspace "always starts from the latest remote commit"; the branch is renamed after the first chat.
- Changelog titles show the feature order: message queues (0.1.0, Jul 2025), checkpoints (0.19.0), mark workspaces unread (0.25.11), view setup script logs (0.31.0), rerun setup scripts (0.32.1), workspace status (0.35.0), new sidebar + rebuilt composer + Codex checkpoints (0.44.0), instant archiving (0.46.0), new queue (0.54.0).
- At least ten releases are titled for performance, including "We no longer burn your CPU (hopefully)" (0.7.0), two terminal performance releases, and "SDK Memory Leaks" (0.36.3). Entry bodies were not retrievable, so no mechanism is known.

Cursor (https://cursor.com/docs/configuration/worktrees):
- Agents Window runs each agent in its own worktree; setup via `.cursor/worktrees.json` (`setup-worktree`, `-unix`, `-windows`), output in a "Worktrees Setup" panel.
- Cap of 25 worktrees per machine with interval cleanup; creating past the cap triggers an immediate cleanup.
- Docs advise against symlinking dependencies into worktrees and recommend a fast package manager instead.

---

## 4. Fast-workspace techniques compared

| Technique | Who | Evidence | Cost / risk | Fit for native Swift |
|---|---|---|---|---|
| Speculative prepared worktree, claimed with `worktree move` + `checkout -b` | Orca | VERIFIED 3.1 | Up to 3 full checkouts on disk for 5 min; needs lock ownership and stale cleanup | Best available. Pure git CLI, no platform dependency |
| Reserve with `--no-checkout`, announce, materialize in background, defer origin fetch | xum; Paradev uses `--no-checkout` for sparse (`packages/host-service/src/trpc/router/workspace-creation/shared/sparse-checkout.ts:177`) | VERIFIED 3.4 | Workspace exists but is empty for a moment; consumers must wait on init | Good fallback when no prepared entry matches |
| Prefetch base ref when the composer selects a repo; share the fetch with create | Orca | VERIFIED `worktree-create-base-prefetch.ts:100-118` | One background fetch per repo selection | Cheap, do it |
| Parallel checkout `-c checkout.workers=0` | Orca | VERIFIED | None if the user has not set it | One flag |
| Kernel `clonefile(2)` on whole directories for ignored artifacts | Emdash (prototype only) | VERIFIED `apps/emdash-desktop/tooling/prototypes/cow-worktree/README.md:19-24,33-47`: baseline `worktree add` 0.6–1.6 s; full snapshot via `cp -cR` 58 s; via `clonefile(2)` 8.1 s; **hybrid `worktree add` + clonefile of `node_modules` only 4.3 s**; `cp -cR` ~12x slower than directory `clonefile` | Directory clone is not atomic against concurrent writers; Node cannot call it, Swift can | Strong fit: call `clonefile` directly from Swift |
| Per-entry CoW copy of user-named gitignored patterns with plain-copy fallback, staged + renamed for crash replay | Emdash | VERIFIED `packages/core/src/runtimes/workspace-registry/node/copy-artifacts.ts:36-45` | Glob cost scales with matched set; they call enumerating the whole ignored set an anti-pattern | Use for `.env` and small caches |
| `.worktreeinclude` (gitignore syntax) ∩ gitignored, cloned with `COPYFILE_FICLONE` | Paseo plugin | VERIFIED 3.2 | Per-file | Good convention to adopt; also requested on Vibe Kanban (#1947, VERIFIED API title) |
| `cp -c` / `cp -n -c -R` shell-out with APFS same-volume probe | Orca | VERIFIED 3.1 | Per-file userspace clone; Emdash measured this class as the slow engine | Works today, slower than `clonefile` on big trees |
| Sparse checkout presets | Orca, Paradev | VERIFIED | User must define cones | Optional |
| Disposable worktrees recreated on demand | Vibe Kanban | VERIFIED 3.5 | Recreate latency on reopen | Good for archived sessions |
| Setup script in a terminal with OSC 133 readiness gate; optional "agent waits for setup" | Paradev | VERIFIED 3.3 | Agent may start before deps exist unless gated | Matches Memex's terminal pane |
| Setup as a separate panel with per-command snapshots, never chat rows | Paseo | VERIFIED 3.2 | — | Keeps transcript clean |
| Worktree cap with interval cleanup | Cursor | UNVERIFIED 3.7 | — | Needed; most-reacted Vibe Kanban issue #765 asks for exactly this (VERIFIED API) |
| Pre-created idle pool independent of user intent | none found | searched all clones for pool/prewarm/reserve | — | Orca's intent-triggered preparation is the closest; nobody keeps a standing pool |

---

## 5. Anti-patterns (things that feel janky, with evidence)

| Anti-pattern | Evidence | Lesson |
|---|---|---|
| Full-pane "Creating workspace" spinner with elapsed timer | Paradev `WorkspaceCreatingState.tsx:26-50` (VERIFIED) | Give the user the real chat pane and composer during creation (Orca) |
| Removing explicit naming in favour of AI naming | Paradev #7168 (r15) and #7318 (r11): no way to type workspace/branch name on v2 (VERIFIED API) | Async auto-name is a default, not a replacement for a name field |
| Blocking Send on a preflight that can hang | xum #6055: Send disabled with no explanation while `listBranches` never settles (VERIFIED API) | Every preflight needs a timeout and a visible reason |
| Rendering whole long transcripts | Vibe Kanban #1140 (open): long conversations take seconds to open and make the page sluggish; asks for last-N + load more (VERIFIED API) | Window the transcript and page history from day one |
| Patch stream with no cursor and silent drop on lag | Vibe Kanban `msg_store.rs:100-121` (VERIFIED) | Use snapshot upserts + epoch/seq cursor |
| Deep-observing large trees in a reactive store | Emdash #2196: file tree in a deep MobX observable blocked the renderer in ~1.3 s chunks on every fs-watch event (VERIFIED API body) | Keep big, frequently replaced data out of deep observation; in Swift, do not put file trees in `@Observable` graphs that views read wholesale |
| Synchronous index maintenance on the UI path | Emdash #2882: UI locks up, FTS5 full scan on every reindex (VERIFIED API title) | Index off the main actor |
| Periodic main-thread ticks | Paradev #2861 (r18): rhythmic terminal input lag after an upgrade (VERIFIED API body) | No fixed-interval polling on the thread that echoes keystrokes |
| Unbounded watcher path sets | Paradev plan: 87 ms p99 loop delay until capped at 128 paths (VERIFIED `plans/20260717-host-service-git-worker-pool.md:35-45`) | Cap precise invalidation, then go broad |
| Shared transport for terminals and chat without backpressure | Paradev: one flooding terminal destroyed the shared socket (`plans/...git-worker-pool.md:11`); Paseo: 250 KB chat message delays terminal echo (`docs/terminal-performance.md:51`) (VERIFIED) | Separate channels or strict per-stream budgets |
| Two notification paths for the same event | cmux #2322 (r22): completion notifications arrive seconds to tens of seconds late, attributed to two delivery paths (VERIFIED API body) | One owner for "turn finished" |
| Memory growth with many terminals | cmux #4529, #2962 (70+ GB reports); Orca #38108 (GPU helper 20+ GB from an uncapped texture) (VERIFIED API titles) | Hibernate hidden terminals; cap GPU surfaces |
| Stale requests piling up | Paseo #4879: directory search requests not cancelled (VERIFIED API title) | Cancel superseded requests |
| Trailing-only coalescing | Paseo docs: adds a full window to the first character/keystroke (`docs/agent-stream-performance.md:29`, `docs/terminal-performance.md:24`) (VERIFIED) | Leading + trailing |
| Painting each delta as it lands | Paseo measured chars-per-frame CV 4.11 vs 1.86 paced (`docs/agent-stream-performance.md:62-67`) (VERIFIED) | Pace the reveal |
| Shimmer by animating `background-position` everywhere | xum and Orca both comment that it paints on the main thread (`Shimmer.tsx:19-21`, `text-shimmer.css:95`) (VERIFIED) | Animate transform/mask only, or step it |
| No worktree cleanup policy | Vibe Kanban #765 (r19) (VERIFIED API) | Ship retention + cap with the feature |
| Fixed 1 s retry on persistent refusal | Paseo docs: one request and log line per second on an idle app (`docs/timeline-sync.md:75-80`) (VERIFIED) | Exponential backoff, reset on real state change |
| Symlinking dependencies into worktrees | Cursor docs warn it "can cause issues in the main worktree" (UNVERIFIED 3.7) | Clone, do not symlink |

---

## 6. Ranked recommendations for a native Swift app

Each item names the source pattern. "Inference" marks steps I derived rather than read.

1. **Make new chat composer-first with a held first prompt.** Mint the session UUID on the client; insert the sidebar row and open the real chat view with a focused composer in the same runloop turn. Accept the first message immediately, render it as a normal user row, and hold it until the workspace and agent are ready. Show setup as an inline checklist card under that message (steps, per-step elapsed, git checkout percent, Retry/Dismiss on failure), never a modal or full-pane spinner. Source: Orca `PendingNativeChatCreation.tsx`, `worktree-setup-steps.ts`; idempotent id from Paradev `create-session.ts:35-36`.

2. **Prepare the worktree before the user submits.** When the new-chat composer has a repo and base selected, in the background: fetch the base; `git worktree add --detach --no-checkout --lock --reason <owner> <root>/.preparing/<id> <base>`; `git -c checkout.workers=0 reset --hard <base>`. On submit: verify the lock, `reset --hard` only if the tip moved, `git worktree move -f -f` to the final path, `git checkout --no-track -b <branch>`, unlock. Cap 3, TTL 5 min, re-arm only on bursts, clean stale preparations at launch. Source: Orca 3.1.

3. **Fallback path: reserve then materialize.** With no prepared entry, `git worktree add --no-checkout` using local refs, show the session at once, then fetch and check out with streamed progress. Source: xum `WorktreeManager.ts:309-368`. Inference: because `--no-checkout` creates the directory immediately, an ACP agent process can be spawned with that cwd in parallel with checkout, while the held first prompt (item 1) is released only after materialization.

4. **Clone ignored artifacts with `clonefile(2)` from Swift.** After checkout, clone user-named ignored directories (e.g. `node_modules`, `target`, `.venv`) and `.env`-class files from the main checkout. Honour a `.worktreeinclude` file. Use directory-level `clonefile`, not per-file `cp -c`; fall back to copy off-APFS or across volumes. Never symlink. Source: Emdash benchmark (4.3 s hybrid vs 58 s `cp -cR`), Paseo `copy.ts`, Orca volume probe.

5. **Wire model: full item snapshots, ephemeral deltas, epoch+seq cursor, one reducer.** Durable events are whole items upserted by id; token deltas are a separate ephemeral channel that a durable item supersedes. The same reducer builds state for live, replay, and cold open. On epoch mismatch refetch, never splice. Source: Paradev `plans/20260804-chat-protocol-decisions.md`, `reducer.ts`.

6. **Coalesce at the source, commit once per frame, pace the reveal.** Leading+trailing ~60 ms coalescing where agent events enter the app; one model commit per display frame with a timer ceiling when the window is hidden; flush the queue before applying turn-end. Reveal streaming text at `ceil(backlog * dt / 150 ms)` per frame, grapheme-safe, never for first-seen text, snap on completion. Source: Paseo 3.2. In Swift: a `CADisplayLink`-driven drain on the main actor; keep the full string in the model and pace only the rendered prefix.

7. **Session switching: paint from memory, then reconcile by cursor.** Keep an LRU of reduced session snapshots (Paradev keeps 5) plus scroll anchor and draft per session. On select, render the cached snapshot in the same frame; send `{epoch, seq}` and apply only the tail if `behind`. Skeleton only when there is no cache or the catch-up is non-incremental, with a hard deadline (xum: 10 s). Keep the "running" footer until the catch-up commit lands. Coalesce rapid selection clicks (leading immediate, 100 ms trailing). Sources: Paradev `snapshotCache.ts`, Paseo `timeline-sync.md:106-143`, xum `WorkspaceStore.ts:225-235,744-750`, cmux `SidebarSelectionCoalescer.swift`.

8. **Background sessions get a small activity record, not a transcript subscription.** One host-wide status stream: `{streaming, generation, recency, status, attention reason}` per session. Derive the sidebar state as a priority fold (`permission` > `working` > `review`/unread > idle); clear `review` on focus, never clear `permission` on focus. Distinguish completion from abort before notifying. Sources: xum activity snapshot, Paradev `tabs-types.ts`, Paseo `agent-attention.ts`, Orca status feed.

9. **Sidebar rendering: per-row snapshots behind a revision counter, AppKit table if needed.** Rows read an equatable snapshot; events bump a revision only when a snapshot changed. Source: cmux `SidebarRowSnapshotCache.swift`; same idea in Paradev's per-workspace external store.

10. **In-progress affordances: compositor-only shimmer, almost no spinners.** Text shimmer as a masked duplicate layer animated by transform (Core Animation on a mask layer), removed when off-window, occluded, or under Reduce Motion. Any remaining spinner animates only a layer rotation with the same removal rules. Use pulse/shimmer on the tool title for running tools; reserve spinners for step rows. Sources: xum `transcript-content.css:486-545`, cmux `GPUSpinner.swift`, Orca `shimmer-steps`.

11. **Outbox with visible states and explicit steer.** Sends get a `clientId`, render immediately as `queued`/`inflight`/`failed`, flush on reconnect, and are confirmed by the echoed id. Messages sent mid-turn queue FIFO with a visible tag and can be edited, removed, reordered, or promoted to steer; steer carries the expected turn id; offer dispatch at tool end or turn end. Edit of history forks. Sources: Paradev `outbox.ts`, `sessionClient.ts:162-216`; xum `queueDispatchMode`.

12. **Keep terminals and diffs off the chat path.** PTY parsing in a separate process with a 5 ms leading+trailing coalescer and binary frames; hidden terminals in the focused workspace stay attached, others detach or hibernate. Refresh the diff when a file-modifying tool completes; bound git concurrency (4 was the measured sweet spot) and cap watcher path sets. Sources: Paseo `terminal-performance.md`, xum `WorkspaceStore.ts:1261-1266`, Paradev worker-pool plan.

13. **Notification routing by presence, one owner.** Suppress when the user is looking at that session; otherwise deliver to the most recently active client; one code path for "turn finished". Sources: Paseo `agent-attention-policy.ts`; cmux #2322 as the counter-example.

14. **Ship worktree retention with the feature.** Cap count, age-based cleanup, lazy recreation for archived sessions, and keep a name field alongside auto-naming. Sources: Cursor docs (UNVERIFIED), Vibe Kanban #765 and `ensure_worktree_exists`, Paradev #7168.

---

## Gaps and limits of this study

- Orca's queue/steer, transcript virtualization, and journal replay were identified by file layout only, not traced.
- Vibe Kanban, Emdash, and cmux were read partially (transport, CoW, sidebar respectively).
- Rows 12–29 of the ecosystem table rest on README keyword scans and API metadata.
- Conductor and Cursor entries are vendor pages only; Conductor changelog bodies were not retrievable.
- No app was built or run, so every "feels good" claim is from code, in-tree measurements, or issue reports, not from observation.
