# ACP client ecosystem survey (excluding Zed, T3 Code, OpenCode, Codex)

Date: 2026-11-09. Scope: breadth survey of ACP clients, four source-level deep dives, cross-client pitfalls, recommendations for a native Swift client.

## Evidence labels

- **VERIFIED** `path:line @ repo` = I read that line in a shallow clone at the SHA listed below.
- **VERIFIED-DOC** = a project's own engineering doc states it at that path:line. The doc statement is verified; the underlying agent behaviour was observed by that project, not reproduced by me.
- **UNVERIFIED** (URL) = taken from a listing page, repo description, or issue title via the GitHub API. Issue bodies were not read.
- Stars and push dates: `gh api repos/<r>` on 2026-11-09.

Clone SHAs (all under `scratchpad/repos/acpclient-*`):

| Repo | SHA | Commit date |
| --- | --- | --- |
| 851-labs/codevisor | 9c6121346bff1efcab3acefc47d98cf05488858a | 2026-11-09 |
| hotic/acpira | 28d44ba8f3df25c14c0218355d6f8a61ffb379a2 | 2026-11-10 |
| batrachianai/toad | dd4f90e8b3700c3de80ad4b0eaa488ad0105e2c1 | 2026-05-26 |
| xenodium/agent-shell | 468ef219668a086eb19e374eb477106825734797 | 2026-11-09 |
| xenodium/acp.el | 2959da5fa8657da17a78ac6ceef303a50e3e69de | 2026-10-21 |
| vivy-company/aizen | 3ccde932e192acd249ede4ae522f7a9c19ba9140 | 2026-07-26 |
| wiedymi/swift-acp | 9498537769d1309b6519fbb87d0c22fcf9317f3e | 2026-07-22 |
| rebornix/Agmente | 87f224e7d5884d450f4d54cc1d72724416e6d750 | 2026-03-21 |
| getpaseo/paseo | ab64bebe14b8e30c576457292f7e3ec19ab21028 | 2026-11-09 |
| OpenSource03/harnss | dc1dfd8a33caa46a1eefcfe9e14697b27ac4c33d | 2026-06-19 |
| Textualize/textual (one file, tag v8.2.7) | `src/textual/widgets/_markdown.py`, saved as `scratchpad/research/textual_markdown_v8.2.7.py` | n/a |

Also cloned but only skimmed or grepped: sxhxliang/agent-studio, RAIT-09/obsidian-agent-client, olimorris/codecompanion.nvim, openclaw/acpx, smagnuso/hydra-acp, carlos-algms/agentic.nvim, formulahendry/acp-ui, marimo-team/use-acp, tidewave-ai/tidewave_app.

---

## 1. TL;DR ranking

Ranked by what a native macOS chat app can learn from them, not by popularity.

1. **Codevisor** (851-labs/codevisor, Swift/AppKit + TS server, 25 stars). The best-engineered native transcript I found, and the closest analogue to Memex. Pure Swift, no webview: virtualized AppKit transcript, off-main Markdown and TextKit preparation, anchor-preserving height commits, adaptive text reveal, a composer-to-bubble send animation, queue and steer, optimistic sends reconciled by id. It also documents its own perf numbers and where it still misses a frame.
2. **Acpira** (hotic/acpira, Rust engine + system WebView, 12 stars). The most rigorous ACP protocol layer. Warm process pool, host-side queue with steering, per-agent quirk modules, and three docs (`protocol-gotchas.md`, `agent-quirks*.md`, `ui-motion-layout.md`) that amount to a field guide to how Claude and Codex adapters actually behave on the wire.
3. **agent-shell** (xenodium/agent-shell, Emacs Lisp, 2,045 stars). The most battle-tested community client. Good at protocol robustness: messageId-based boundaries with a fallback heuristic, loud handling of out-of-turn updates, buffered restore, four restore verbosity levels, queue with merge, steering, and type-ahead while the agent boots.
4. **Toad** (batrachianai/toad, Python/Textual, 3,502 stars). Best streaming Markdown in a TUI, and the simplest readable reducer. Weak on queueing, resume UX and tests (zero test files in the repo).
5. **Aizen** (vivy-company/aizen, Swift/SwiftUI, 310 stars). A native macOS ACP client on `wiedymi/swift-acp`. Worth reading mainly as a catalogue of the hacks you fall into without an ordered event pipeline (100 ms sleeps, polling for readiness, cancel-then-send). Included as a fifth, shorter dive.

Honourable mentions, not examined in depth: Paseo (23.6k stars, large tested ACP provider layer), Agmente (Swift iOS, own ACPClient package and a UIKit chat list), hydra-acp (multi-client session daemon), acpx (headless stateful CLI).

One thing all five converge on independently: **coalesce streamed chunks on a short timer rather than rendering per chunk.** Aizen and agent-shell both chose 50 ms; Agmente throttles Markdown rows at 1/60 s; Codevisor uses a display-link frame driver; Textual's `MarkdownStream` batches whatever arrived while the previous render was in flight.

---

## 2. Ecosystem table

Source for membership: the official clients page (https://agentclientprotocol.com/get-started/clients, fetched 2026-11-09, roughly 150 entries) plus GitHub topic `agent-client-protocol` (555 repos). "Examined" says how far I went. Several names in the brief resolved to renamed repos; the current name is shown.

### Desktop GUI

| Client | Repo | Stack | Stars / last push | What it is | Quality signal | Examined |
| --- | --- | --- | --- | --- | --- | --- |
| Codevisor | github.com/851-labs/codevisor | Swift (AppKit/UIKit) app, TS/Effect server, per-harness adapters | 25 / 2026-11-09 | Native macOS + iOS app driving Claude Code, Codex, Pi and any ACP agent | 9,632 files, about 2,000 test files, documented perf traces, AGPL | Deep |
| Acpira | github.com/hotic/acpira | Rust engine, React in system WebView, VS Code / IntelliJ / iOS / Android shells | 12 / 2026-11-09 | "Native workbench for coding agents" built on ACP | 325 test files, per-agent quirk docs with adapter versions | Deep |
| Aizen | github.com/vivy-company/aizen | Swift/SwiftUI, swift-acp, libghostty | 310 / 2026-08-01 | macOS workspace app with ACP chat, worktrees, terminal | Clean layering, 50 test files, timing hacks in the session layer; open issue "Codex/CC ACP Agents are Terribly Slow" (issues/25) | Deep (short) |
| Paseo | github.com/getpaseo/paseo | TS daemon + React Native/Electron clients | 23,653 / 2026-11-09 | Orchestrates agents from desktop and mobile | 4,389-line `acp-agent.ts`, about 2,300 test files; open issue on missing ACP steering (issues/3964) | Skimmed |
| AionUi | github.com/iOfficeAI/AionUi | Electron/TS | 34,168 / 2026-11-05 | Multi-agent "cowork" desktop app | Very popular; 888 open issues, including "agent_message_chunk notifications are not rendered incrementally" (issues/4211) | Not read |
| Orca | github.com/stablyai/orca | TS | 120,374 / 2026-11-09 | Fleet runner for coding agents, desktop + mobile | Largest by stars; 8,272 open issues | Not read |
| Harnss | github.com/OpenSource03/harnss | Electron/React | 395 / 2026-11-08 | Desktop UI for Claude Code, Codex, any ACP agent | One ACP IPC file (`electron/src/ipc/acp-sessions.ts`), 51 test files | Skimmed |
| Agent Studio (AgentX) | github.com/sxhxliang/agent-studio | Rust, GPUI | 516 / 2026-06-29 | GPU-accelerated desktop ACP client | Native (non-web) rendering; 4 test files; quiet since June | Skimmed |
| Codeg | github.com/spacering-net/codeg (was xintaofei/codeg) | Rust/Tauri | 4,412 / 2026-11-06 | Multi-agent coding workspace aggregating sessions | 367 open issues | Not read |
| DeepChat | github.com/ThinkInAIXYZ/deepchat | Electron/TS | 6,388 / 2026-11-09 | General AI chat app with ACP agents | Mature product, ACP is one provider type | Not read |
| ACP UI | github.com/formulahendry/acp-ui | Vue + Tauri | 506 / 2026-05-25 | Cross-platform reference-style ACP client | No tests; quiet since May | Skimmed |
| gemini-cli-desktop | github.com/Piebald-AI/gemini-cli-desktop | TS/Tauri | 520 / 2026-10-24 | Desktop UI for Gemini CLI / Qwen Code over ACP | Agent-specific | Not read |
| Athas | github.com/athasdev/athas | Tauri (Rust + React) | 3,123 / 2026-11-06 | Lightweight code editor with ACP agents | Editor first | Not read |
| Tidewave | github.com/tidewave-ai/tidewave_app | Rust desktop shell + web UI | 90 / 2026-10-27 | Web-app-aware coding agent host; ACP for external agents | Small shell repo; main UI is closed | Skimmed |
| Sidequery | https://sidequery.dev (app closed), github.com/sidequery/duckdb-acp | n/a; Rust DuckDB extension | 59 / 2025-12-11 (extension) | Data workspace listed as an ACP client; DuckDB extension that calls ACP agents from SQL | Extension dormant since Dec 2025 | Not read |
| Coincide | github.com/apoorvdarshan/coincide | Rust + AppKit | 14 / 2026-11-09 | Native macOS app for several coding agents | New (created 2026-10-27) | Not read |
| Aureways / agenthesia / Volant | github.com/nullskymc/Aureways, github.com/anesthetised/agenthesia, github.com/mysticcoders/volant | Swift | 9 / 0 / 15 | Small native macOS ACP clients | Too new or small to assess | Not read |
| ACP Inspector | github.com/newioapp/acp-inspector | TS desktop | 13 / 2026-06-28 | JSON-RPC inspector for ACP agents | Useful as a debugging tool, not a chat client | Not read |

### TUI, CLI and shells

| Client | Repo | Stack | Stars / last push | What it is | Quality signal | Examined |
| --- | --- | --- | --- | --- | --- | --- |
| Toad | github.com/batrachianai/toad | Python, Textual | 3,502 / 2026-05-26 | Unified terminal UI for ACP agents | Excellent rendering; zero test files; no push since May | Deep |
| acpx | github.com/openclaw/acpx | TS | 3,423 / 2026-11-09 | Headless CLI for stateful ACP sessions (queueing, named sessions) | 280 test files | Grepped |
| hydra-acp | github.com/smagnuso/hydra-acp | TS | 47 / 2026-11-09 | Session daemon + multiplexer + TUI; many clients attach to one live session | 279 test files; marked experimental; implements `_session/steering` | Grepped |
| Nori CLI | github.com/tilework-tech/nori-cli | Rust | 193 / 2026-11-06 | Simple CLI front end for any agent | n/a | Not read |
| Intelligent Terminal | github.com/microsoft/intelligent-terminal | C++ | 2,198 / 2026-11-09 | Windows Terminal fork with native agent integration | Microsoft-maintained | Not read |
| Hash | github.com/tfcace/hash | Go | 33 / 2026-11-07 | POSIX shell with `??` to drive ACP agents | Novel form factor | Not read |

### Editors and IDE plugins

| Client | Repo | Stack | Stars / last push | What it is | Quality signal | Examined |
| --- | --- | --- | --- | --- | --- | --- |
| agent-shell | github.com/xenodium/agent-shell (+ xenodium/acp.el, 177 stars) | Emacs Lisp | 2,045 / 2026-11-09 | Emacs shell for ACP agents | 12k-line core, 30 test files, very active issue tracker with fixes cited in code comments | Deep |
| CodeCompanion.nvim | github.com/olimorris/codecompanion.nvim | Lua | 6,932 / 2026-11-09 | Neovim AI plugin with ACP adapters | 399 test files | Grepped |
| avante.nvim | github.com/avante-corp/avante.nvim (was yetone/) | Lua | 18,184 / 2026-11-09 | Cursor-style Neovim plugin; ACP providers | Popular; ACP is one of several back ends | Not read |
| agentic.nvim | github.com/carlos-algms/agentic.nvim | Lua | 658 / 2026-11-08 | ACP-only Neovim chat | 84 test files | Grepped |
| Obsidian Agent Client | github.com/RAIT-09/obsidian-agent-client | TS | 2,466 / 2026-10-15 | ACP chat inside Obsidian | 59 open issues, several on session restore (issues/274, 182, 427) | Grepped |
| Copilot for Obsidian | github.com/logancyang/obsidian-copilot | TS | 7,971 / 2026-11-09 | Obsidian assistant, now runs ACP agents | Large user base | Not read |
| JetBrains AI Assistant | https://www.jetbrains.com/help/ai-assistant/acp.html | Kotlin (closed) | n/a | ACP agents inside JetBrains IDEs | JetBrains co-maintains the ACP registry and the AIR `_meta` extensions that Claude/Codex adapters gate features on (VERIFIED-DOC, `docs/dev/protocol-gotchas.md:37,49 @ acpira`) | Docs only |
| vscode-acp | github.com/formulahendry/vscode-acp | TS | 410 / 2026-05-16 | Generic ACP client for VS Code | 65 open issues, quiet since May | Not read |
| Chrome ACP | github.com/Areo-Joe/chrome-acp | TS | 194 / 2026-11-09 | Browser side panel connecting to a local ACP agent | n/a | Not read |
| marimo | github.com/marimo-team/marimo (+ marimo-team/use-acp, 69 stars) | Python + React | 23,485 / 2026-11-09 | Notebook with an ACP agent panel; `use-acp` is its reusable React hook layer | `use-acp` is small (45 files, 2 tests) | Grepped |
| Qt Creator, Sublime, Pulsar, Unity | see clients page | various | small | Editor plugins | n/a | Not read |

### Mobile and remote

| Client | Repo | Stack | Stars / last push | What it is | Quality signal | Examined |
| --- | --- | --- | --- | --- | --- | --- |
| Agmente | github.com/rebornix/Agmente | Swift (iOS), own `ACPClient` package | 548 / 2026-05-31 | iOS client for ACP agents and Codex app-server | Custom `HighPerformanceChatListView`, height cache, Markdown rows throttled at 1/60 s (VERIFIED `Agmente/ChatRendering/HighPerformanceChatListView/MarkdownRows.swift:14 @ Agmente`); 42 test files | Skimmed |
| Happy | github.com/slopus/happy | Swift + TS | 24,558 / 2026-11-09 | Remote control for Claude, Codex, Grok | Listed as ACP client; 1,105 open issues | Not read |
| Happier | github.com/happier-dev/happier | TS | 2,186 / 2026-11-09 | Web/desktop/mobile orchestrator | n/a | Not read |
| Mobvibe, Ferngeist, AgentDeck, TermFold | see clients page | TS / Kotlin | 147 / 30 / 8 / 33 | Phone remotes | Small | Not read |

### Proxies, multiplexers, SDKs

| Project | Repo | Stars / last push | Role |
| --- | --- | --- | --- |
| symposium-acp | github.com/agentclientprotocol/symposium-acp | 36 / 2026-03-18 | Rust implementation of the ACP proxy-chain / conductor idea; quiet since March |
| Symposium | github.com/symposium-dev/symposium | 432 / 2026-11-09 | Rust-focused agent tooling built around that proxy design |
| Compozy | github.com/compozy/compozy | 2,860 / 2026-11-09 | "Operating system for agents", fronts several CLIs |
| Ngent | github.com/beyond5959/ngent | 33 / 2026-06-21 | Wraps ACP agents as a web service |
| Swift SDKs | github.com/wiedymi/swift-acp (32), github.com/aptove/swift-sdk (12), github.com/Cocoanetics/SwiftACP (3) | 2026-07 to 2026-11 | No official Swift SDK exists; these are community packages. Codevisor and Agmente each wrote their own (`ACPKit`, `ACPClient`) |

Not found or not a client: **Agentastic** (no repo under that name, not on the clients page), **Neovate Code** (github.com/neovateai/neovate-code, 1,560 stars, an agent rather than a client, last push 2026-03-24), **Goose** (github.com/aaif-goose/goose, 56,408 stars; primarily an agent; its use of ACP agents as providers was not verified here).

That is 40+ clients tabulated; 5 examined in source in depth and 9 more skimmed or grepped.

---

## 3. Deep dives

### 3.1 Codevisor (native Swift, the closest analogue)

Architecture: a local TS server owns the agent processes and a durable event log; Swift apps are clients of that server. The ACP hop is `packages/adapter-acp`. The Swift side is split into `ACPKit` (types), `TranscriptKit` (pure reducers and projections), `StreamMarkdown` (parser + views), `TranscriptSurface` (AppKit/UIKit hosts), `CodevisorCore/Session` (send queue, steering, optimistic state). The design note is `packages/swift/CodevisorUI/TRANSCRIPT.md`.

| What | Where (VERIFIED @ codevisor unless noted) | Why it feels good |
| --- | --- | --- |
| One agent process per session, closed on failure | `packages/adapter-acp/src/provider.ts:296-327` | Simple isolation. No cross-session head-of-line blocking. Cost: every new session pays spawn + initialize (they do not pool) |
| Explicit turn lifecycle events around `session/prompt` | `provider.ts:166-181` | The UI never infers "turn started/ended" from chunk gaps |
| Cancel emits a synthetic `ended/cancelled` turn event straight after sending `session/cancel` | `provider.ts:182-188` | Stop button resolves instantly instead of waiting for the agent's `cancelled` stop reason |
| Cancel also cancels pending questions first | `packages/adapter-acp/src/sdk-connection.ts:307-311` | Agent is never left waiting on an answer that will not come |
| Auth failure on `session/new` retries once through an advertised external auth method | `sdk-connection.ts:220-231` | User does not see an auth error for a recoverable state |
| Auth probe is `session/new` with a 10 s timeout, on a throwaway connection | `provider.ts:140, 409-429`; `sdk-connection.ts:146-175` | Sign-in state is known before the first send |
| Auth failure during a prompt becomes a typed stop (`authRequired`) with a human sentence | `sdk-connection.ts:272-274`; `provider.ts:152-155` | Renders as an inline call to action, not a red error |
| Steering via `_session/steering`, only when the agent advertises `_meta.steering.supported` and only while a prompt is in flight | `sdk-connection.ts:92-97, 286-306` | Mid-turn messages join the running turn where supported and fall back to queueing where not |
| Reducer: chunk with `messageId` appends to that message; without one, appends to the last entry if it is text, else opens a new text entry | `packages/swift/TranscriptKit/Sources/TranscriptKit/TranscriptTextReduction.swift:66-103` | Correct message boundaries with or without ids |
| In-place append that avoids a copy-on-write of the whole string | `TranscriptTextReduction.swift:105-114` (writes an empty placeholder into the enum slot, mutates, writes back) | O(chunk) per append instead of O(message) |
| Offset-addressed, generation-stamped text patches with overlap trimming | `TranscriptTextReduction.swift:123-162` | Replays and reconnects are idempotent: a patch that overlaps existing text only contributes its new tail |
| `tool_call_update` for an unknown id creates the row | `TranscriptToolReduction.swift:74-95` | Survives update-before-call |
| Tool upsert ignores stale revisions and merges omitted fields from the existing row | `TranscriptToolReduction.swift:41-51` | Adapters that resend only changed fields do not blank the title |
| Parent tool settling cascades to subagent children | `TranscriptToolReduction.swift:97-110` | No orphaned spinners under a finished parent |
| `user_message_chunk` is ignored by the live reducer | `TranscriptReducer.swift:31-32` | Echoes cannot duplicate the optimistic bubble |
| Optimistic user message: appended to settled history before delivery, id recorded in `pendingOptimisticUserMessageIDs` | `packages/swift/CodevisorCore/Sources/CodevisorCore/ViewModels/SessionModel+Prompting.swift:41-47` | Bubble appears on keypress |
| Reconciliation with server echo: match by id first, then by "pending optimistic id and same trimmed text"; merge attachments rather than replace | `ViewModels/RemoteUserMessage.swift:47-89` | No duplicate, no flicker when history arrives |
| Failed delivery removes the bubble from the transcript and holds the message as "undelivered" with a reason | `SessionModel+Prompting.swift:61-65, 82-94` | The text is never lost and never shown as sent |
| Queue vs steer decided at send time; queued prompts can be edited, reordered, deleted, or "sent now" | `Session/SessionController+Steering.swift:9-43`; `SessionController+Sending.swift:305-320` | The composer never disables during a turn |
| Send animation only for sends that land in the transcript, not for ones that land in the queue | `SessionController+Sending.swift:38-43` | Motion matches where the message actually went |
| Cancel UI: after the cancel RPC, poll up to 20 × 100 ms for the terminal event, then reconcile from the server, then show a specific error | `SessionModel+Prompting.swift:128-150`; `SessionModel.swift:12` | A stuck cancel is detected and explained |
| Stalled-turn watch: 300 s without provider activity triggers reconciliation | `ViewModels/SessionStalledTurnWatch.swift:6` | Avoids "working forever" |
| Reconnect notice appears only after 1 s of trouble and then stays at least 1 s | `ViewModels/SessionModel.swift:217`; `Session/DelayedConnectionNotice.swift:22-45` | No banner flash on blips |
| Streaming text reveal: 150 ms per-segment fade on a custom cubic curve (0.37, 0.55, 0.86, 0.88); queue lead adapts between 55 ms and 350 ms from smoothed inter-arrival gap and jitter | `packages/swift/StreamMarkdown/Sources/StreamMarkdown/Views/StreamingTextAnimationSpec.swift:4-28, 56-70` | Bursty input reads as steady output; fast streams are not artificially slowed |
| Emoji held 50 ms before reveal | `StreamingTextAnimationSpec.swift:5` | Multi-scalar emoji do not flash half-formed |
| Shimmer: `CAGradientLayer` band 33% of width, 1.2 s linear sweep, repeat forever, driven by Core Animation, not SwiftUI state | `packages/swift/CodevisorUI/Sources/CodevisorUI/DesignSystem/ShimmeringText.swift:37, 61, 95-101` | Zero main-thread work per frame |
| All shimmers phase-locked to wall clock (`beginTime` aligned to `now mod duration`) | `ShimmeringText.swift:102-103` | A row that remounts mid-sweep does not restart; multiple shimmers move together |
| Activity label debounce: a new label must persist 0.3 s before it replaces the shown one; a stale label is force-refreshed after 1.2 s; the first label is never blank ("Thinking") | `CodevisorUI/Transcript/WorkedActivityLabelSmoother.swift:41-42, 49-51, 78-83` | Rapid tool sequences do not strobe the status line |
| Send motion: bubble spring response 0.4 / damping 0.74 from scale 0.96, 100 ms crossfade, follower rows revealed after 260 ms with a 200 ms fade | `CodevisorUI/Transcript/SendMotion/TranscriptSendMotion.swift:6-18` | The sent bubble visibly travels from the composer |
| Attachment loading overlay delayed 150 ms | `StreamMarkdown/Views/AttachmentLoadingOverlay.swift:3` | Fast loads never show a spinner |
| Virtualized transcript with a persistent balanced tree for row geometry; height corrections are O(log R) and keep the old snapshot for anchor compensation | VERIFIED-DOC `packages/swift/CodevisorUI/TRANSCRIPT.md:92-104` | Height changes above the viewport do not move the reading position |
| Frame order: accept model changes, mount rows, commit measurements, advance text reveals, finish native work | VERIFIED-DOC `TRANSCRIPT.md:9-12` | One coherent commit per frame |
| Anchor rule: keep the first row that starts inside the viewport stationary on every height commit | VERIFIED-DOC `TRANSCRIPT.md:12-15` | No scroll jump while streaming above or at the fold |
| Incremental Markdown: reparse only the mutable top-level tail, reuse complete prefix blocks; fall back to a full parse when the source contains `[` (reference links) | VERIFIED-DOC `TRANSCRIPT.md:106-107, 111-115` | Cost per chunk is bounded by the last block in the common case |
| Large settled blocks get attributed text and TextKit layout prepared on a worker; the view adopts that layout | VERIFIED-DOC `TRANSCRIPT.md:61-67` | A 5,000-line code block cold-configures in about 17-19 ms instead of 3.1 s (their debug measurement, `TRANSCRIPT.md:172-175, 224`) |
| `LatestValuePreparationWorker`: one executing and one replaceable pending request per worker | VERIFIED-DOC `TRANSCRIPT.md:69-71` | Streaming never builds a backlog of obsolete layouts |
| Navigation is a presentation boundary: leaving settles streams; text that arrived while hidden returns opaque; only later arrivals animate | VERIFIED-DOC `TRANSCRIPT.md:83-90` | No replayed fade when switching back to a chat |
| Bottom-jump gate: a jump to the live edge completes only when the required rows are resolved and no measurements are pending | `TranscriptKit/Sources/TranscriptKit/TranscriptBottomJumpGate.swift:17-29` | No land-then-shift after "scroll to bottom" |
| Initial pin to bottom unless a non-bottom position is being restored | `TranscriptKit/Sources/TranscriptKit/TranscriptInitialBottomPin.swift:6-8` | Reopening a chat restores the exact anchor |
| Retained native transcript surfaces (6 detached on macOS) | VERIFIED-DOC `TRANSCRIPT.md:45-47` | Switching between recent chats is a 0.3 ms reconfiguration by their trace (`TRANSCRIPT.md:167`) |

Caveats they state themselves: cold configuration and pagination bursts still exceed 16.67 ms; a growing single block can go quadratic; their numbers are debug builds on one machine (`TRANSCRIPT.md:117-123, 259-265`).

### 3.2 Acpira (the protocol-rigor reference)

Architecture: a Rust "host" owns agent processes, a transcript store and a prompt queue; UIs are views over it. ACP code is under `rust/crates/acpira-host/src/acp/{transport,session,transcript,vendors,agents}`.

| What | Where (VERIFIED @ acpira unless noted) | Why it matters |
| --- | --- | --- |
| Warm process pool: one idle, already-initialized CLI per (agent, cwd, account); a new session takes it and only runs `session/new` | `rust/crates/acpira-host/src/acp/agents/pool.rs:1-2, 77-93` | Removes spawn + `initialize` from time-to-first-token |
| Warm slot expires after 5 minutes and is killed; a warm process that exits clears its slot | `pool.rs:16, 137-157, 159-171` | No zombie processes, no stale handles |
| Warm process answers any permission or elicitation with `cancelled` and ignores updates | `pool.rs:18-49` | An idle process cannot pop UI |
| Warming is triggered when the user is positioned to start a session with that agent | `rust/crates/acpira-host/src/session_manager/agents.rs:96-102` | Speculative, but scoped to the likely next action |
| A mid-turn `user_message_chunk` is dropped as the echo of the prompt just sent | `rust/crates/acpira-host/src/acp/session/updates.rs:155-158` | No duplicate user bubble (Grok echoes prompts: VERIFIED-DOC `docs/dev/agent-quirks.md:38`) |
| Host-side queue, in memory, in send order; "send now" cancels and lets the prompt's completion flush the queue; never send a concurrent prompt from the cancel path | `acp/session/queue.rs:1, 26-39, 187-203`; VERIFIED-DOC `docs/dev/protocol-gotchas.md:55` | Prompts never overlap, which several adapters mishandle |
| Steering through `_session/steering` with `idleBehavior: promptRequired`; three outcomes parsed (`injected`, `promptRequired`, `startedNewTurn`) | `acp/vendors/steering.rs:10-36` | The host knows whether the steer joined the turn, bounced, or started an unowned turn |
| Steering only for peers advertising `_meta.steering.supported` | `steering.rs:38-41` | No guessing |
| A steer that cannot land stays queued and never cancels the turn | VERIFIED-DOC `protocol-gotchas.md:56`; `queue.rs:126` | Steering is never destructive |
| No steering during a compaction; steers park until it ends | `queue.rs:32-34`; VERIFIED-DOC `protocol-gotchas.md:57` | A steer aborted Claude's compaction |
| On cancel, unanswered steers are re-queued for claude-agent-acp ≥ 0.86.0, which drops them | `steering.rs:43-50`; `queue.rs:426-431` | User input is not silently lost on Stop |
| Codex turn bracket read from `session_info_update._meta.codex.threadStatus` | `steering.rs:59-66` | Detects turns codex-acp starts on its own |
| Restore failures are classified (no restore path / gone / not resumable / locked / failed); "gone" falls back to `session/new` only when the transcript is empty, otherwise the session goes read-only | VERIFIED-DOC `agent-quirks.md:15` | Never silently continues a long chat on an empty native context |
| Restored sessions keep their persisted model/mode, not what the adapter answers on resume | VERIFIED-DOC `protocol-gotchas.md:83` | claude-agent-acp answers resume with defaults |
| New sessions replay remembered controls (model before effort) and hold the first prompt until those settle | VERIFIED-DOC `protocol-gotchas.md:82` | The first prompt never runs on the wrong model |
| Edit = fresh `session/new` plus retained transcript context, since ACP has no turn-addressed rewind | VERIFIED-DOC `protocol-gotchas.md:7` | Honest about what the peer still remembers |
| Empty `end_turn` becomes a retryable `empty_response` | VERIFIED-DOC `docs/dev/transcript-pipeline.md:95` | Some agents report failures as empty success |
| Session list for import uses `initialize` + paged `session/list` on a throwaway process, never `session/new` | VERIFIED-DOC `transcript-pipeline.md:40` | Several agents persist a row on every `session/new` |
| Motion rules: only the turn-level activity owns the spinner ("Orb"); running tool rows shimmer their verb | VERIFIED-DOC `docs/dev/ui-motion-layout.md:17` | One spinner per screen, shimmer everywhere else |
| Tool shimmers share a document-timeline origin so a remounting row does not restart the sweep | VERIFIED-DOC `ui-motion-layout.md:18` | Same idea as Codevisor's phase lock, reached independently |
| Restored history never animates | VERIFIED-DOC `ui-motion-layout.md:24` | Loading a session is instant-looking |
| Bottom follow releases only on an upward scroll within a short window of real wheel/touch/key input; re-arms only at the actual bottom, not a near-bottom zone; a new user turn re-arms and jumps | VERIFIED-DOC `ui-motion-layout.md:35` | Layout shrinkage cannot un-pin the view; readers are not snapped back |
| Sub-pixel drift fix: pad the scroller so the content end lands on a whole pixel while the tail is held | VERIFIED-DOC `ui-motion-layout.md:36` | Removes a 1 px jitter per height change |
| Auto-fold is suspended while the reader is scrolled up; a manual toggle beats automatic state | VERIFIED-DOC `ui-motion-layout.md:54` | Content never collapses under the reader |
| Turn duration is timed from first response, not from send | VERIFIED-DOC `ui-motion-layout.md:59` | Queue and startup time are not billed to the agent |

Their motion philosophy in one line (VERIFIED-DOC `ui-motion-layout.md:7`): keyboard-initiated actions get no animation, and things seen many times a day stay near-imperceptible.

### 3.3 agent-shell (protocol robustness in a mature community client)

| What | Where (VERIFIED @ agent-shell unless noted) | Why it matters |
| --- | --- | --- |
| Message boundary: use `messageId` when present; otherwise a new message starts whenever the previous rendered entry was not an agent chunk | `agent-shell.el:3449-3459` | Distinct messages never coalesce; older agents still work |
| 50 ms chunk coalescing; a different fragment flushes the pending batch first, preserving order | `agent-shell.el:322-329, 5645-5656` | Avoids re-rendering a growing Markdown body per chunk |
| Only tag the untagged tail of a streamed block | `agent-shell.el:5569-5582` | Touching the whole block per chunk caused full re-fontification on multi-megabyte blocks (their issue #757) |
| `user_message_chunk`: rendered during `session/load`; a silent no-op during a live `session/prompt`; flagged as an agent bug with the JSON payload when nothing is in flight | `agent-shell.el:3621-3690` | Three contexts, three behaviours; no duplicates and no silent drops |
| Any `session/update` after the turn ended is surfaced with a "report this to the agent maintainer" body | `agent-shell.el:2907-2928` | Makes agent bugs diagnosable instead of mysterious |
| `tool_call_update` upgrades the title when the update carries a better one | `agent-shell.el:3692-3708` (cites issues 182 and 309) | First `tool_call` often has a generic title |
| Re-render an open permission dialog when a later `tool_call_update` fills in `rawInput` | `agent-shell.el:3722-3735` (cites issue 617) | OpenCode sends the arguments after the permission request |
| Interrupt order: answer all pending permission requests `cancelled`, cancel pending elicitations, then send `session/cancel` | `agent-shell.el:2562-2582` | The agent is not left blocked on a dialog |
| Interrupt before a session id exists is refused with "Starting agent, please wait" rather than killing the client | `agent-shell.el:2583-2595` | Killing mid-bootstrap wedged the buffer |
| Prompt shown immediately, before bootstrap, so the user can type ahead | `agent-shell.el:5399-5410` | Perceived startup is zero |
| Restore verbosity: `minimal` (uses `session/resume`, no replay), `last`, `first-last`, `full` (use `session/load`); falls forward when the agent lacks one | `agent-shell.el:967-989` | Fast quiet resume by default, history when asked |
| During `session/load` all updates are buffered and grouped into prompt turns, then the chosen turns are replayed through the normal path | `agent-shell.el:3437-3442, 8345-8380` | Replay cannot interleave with live rendering; one code path renders both |
| `session/list` paging guards against a repeated cursor | `agent-shell.el:8721` | An agent bug cannot loop forever |
| Queue: prompts sent while busy queue; by default all pending prompts are merged into one turn, separated by blank lines | `agent-shell-prompt-queue.el:78-86, 88-128` | Follow-ups are usually refinements of the same instruction |
| Draft protection: when a queued prompt is auto-submitted, text the user is typing is set aside and restored | `agent-shell-prompt-queue.el:113-128` | Draft and queued prompt are never sent as one message |
| Steering is explicit and documented as non-additive (the agent may drop what it was doing) | `agent-shell-prompt-queue.el:285-300`; `agent-shell-experimental.el:174-211` | Sets expectations correctly |
| Per-agent `notification-adapter` hook to normalize quirks before core handling | `agent-shell.el:2950-2960` | Quirks live at the edge, not in the reducer |
| Shutdown waits a 5 s grace period before killing; pending requests fail with the sentinel event | `acp.el:262-263, 294-297 @ acp.el` | No hung callbacks after a crash |

### 3.4 Toad (streaming Markdown and a minimal reducer)

| What | Where (VERIFIED @ toad unless noted) | Why it matters |
| --- | --- | --- |
| `MarkdownStream`: fragments are pushed to a pending list; a single task joins whatever accumulated and appends once | `src/textual/widgets/_markdown.py:78-109 @ textual v8.2.7`; used at `src/toad/widgets/agent_response.py:75-77` | Natural back-pressure: render rate adapts to render cost, never a backlog |
| Append reparses only from the last top-level block's start line, updates the last existing block in place, mounts only new blocks, inside one batched update | `_markdown.py:1460-1501 @ textual v8.2.7` | Per-chunk cost is the last block, not the document |
| One process per session; agent started only after the first UI refresh | `src/toad/acp/agent.py:556-564`; `src/toad/widgets/conversation.py:1376-1391` | The window paints before any subprocess work |
| 10 MiB stdout line limit | `acp/agent.py:563` | Large tool payloads do not break the reader |
| `tool_call_update` before `tool_call` synthesizes a call titled "Tool call" | `acp/agent.py:310-335` | The comment says it plainly: agents do this |
| Permission request for an unknown tool id seeds the tool-call map | `acp/agent.py:394-405` | Permission can arrive before the tool call |
| Each `session/update` handled as its own task with a yield between lines | `acp/agent.py:576-629` | A slow handler does not block the reader loop |
| Block boundary: any tool call with status unset or `completed`, a permission request, or a user message resets the current response/thought widget | `conversation.py:934-945, 963-965, 996-998` | Simple boundary rule without ids |
| Plan update replaces the plan widget in place only if it is the last child | `conversation.py:967-985` | Plans do not stack up |
| Optimistic user bubble, scroll to end without animation, then a "Please wait..." loading row that is removed on first content | `conversation.py:816-819, 1511-1512` | Instant feedback; the placeholder never coexists with content |
| Throbber: one-row rainbow gradient bar, 15 fps, one cycle per second, shown while a busy counter is above zero | `src/toad/widgets/throbber.py:84, 97`; `conversation.py:926-928` | Single global activity signal, cheap |
| Cancel: Esc, then Esc again within 3 s; flashes success or "Agent declined to cancel" | `conversation.py:1720-1728` | Guards against accidental cancel |
| Stop reasons other than `end_turn` render a specific note (`max_tokens`, `max_turn_requests`, `refusal`) | `conversation.py:869-894` | The user learns why it stopped |
| Edit permissions get a full-screen diff review; everything else is an inline question | `conversation.py:1193-1229, 1241-1260` | Scales UI weight to risk |
| Transcript pruning by rendered height with low/high marks, then re-anchor | `conversation.py:1524-1578` | Bounded memory in long sessions |
| Resume refuses cleanly when the agent lacks `loadSession` | `acp/agent.py:675-683` | Explicit failure |

Gaps: no queue or steering (a submit during a turn simply starts another `session/prompt`, `conversation.py:796-844`); replayed `user_message_chunk`s are always rendered (`conversation.py:940-945`), so an agent that echoes the live prompt would duplicate it; chunk handlers only match `{type, text}` content, so non-text chunks are dropped (`acp/agent.py:280-298`); no test files.

### 3.5 Aizen (native Swift; read it for the traps)

| What | Where (VERIFIED @ aizen) | Assessment |
| --- | --- | --- |
| Agent chunks buffered and flushed every 50 ms; thoughts every 60 ms | `aizen/Features/Chat/Infrastructure/ACP/ChatAgentSession.swift:85, 89`; `ChatAgentSession+StreamingFinalization.swift:24-48, 98-110` | Good |
| Draft message updates go through a throttled in-place path instead of replacing the row | `aizen/Features/Chat/UI/Components/ChatMessageList+IncrementalSync.swift:93-107` | Good |
| Notifications decoded off-main but applied strictly in order by chaining tasks | `ChatAgentSessionNotifications.swift:31-56` | Good idea, heavy mechanism (one task per notification) |
| `tool_call_update` for an unknown id is buffered and applied when the call arrives | `ChatAgentSessionNotifications.swift:125-132` | Good |
| Plan updates applied only when changed | `ChatAgentSessionNotifications.swift:149-154` | Good |
| Spinner is a `CAShapeLayer` arc with a `CABasicAnimation` rotation; input border pulses opacity 0.55 to 1.0 and respects Reduce Motion | `UI/Components/AgentLoadingSpinner.swift:82-101`; `UI/Components/ChatInputBar+AnimatedBorder.swift:118-135` | Good: Core Animation, not SwiftUI timers |
| Scroll: tracks explicit user-scrolled-up intent; auto-scroll debounced 80 ms | `Application/ChatTimelineStore.swift:32-33, 129-141` | Reasonable |
| After `session/prompt` returns, sleeps 100 ms before clearing `isStreaming` because notifications are consumed on a separate stream | `ChatAgentSession+PromptLifecycle.swift:176-187` | **Trap.** Response and notifications must share one ordered queue |
| Message completion inferred from a 200 ms idle gap after the last chunk | `ChatAgentSession.swift:75`; `ChatAgentSession+StreamingFinalization.swift:116-143` | **Trap.** Timing-based boundaries |
| Send while busy = cancel, sleep 100 ms, send | `Application/ChatSessionStore+Messages.swift:45-50` | **Trap.** No queue; races the agent's cancel handling |
| First send starts the session, then polls readiness 100 × 100 ms; the user bubble is added only inside `sendMessage` after the session is ready | `ChatSessionStore+Messages.swift:57-80`; `ChatAgentSession+PromptLifecycle.swift:19-24, 162` | **Trap.** The user's first message is not optimistic |
| Resume: keeps its own persisted messages and suppresses replayed agent chunks by prefix-matching them against stored text, dropping all replayed tool calls, with a 1 s cleanup timer | `ChatAgentSession+ResumeReplay.swift:27-67`; `ChatAgentSessionNotifications.swift:74-81`; `ChatAgentSession+ResumeSupport.swift:51-57` | **Trap.** Fragile; diverges if stored text differs by a character |
| Timeouts: `initialize` 120 s, `session/new` 30 s | `ChatAgentSession+StartupSupport.swift:68`; `ChatAgentSession+Lifecycle.swift:81` | Sensible ceilings, but nothing shortens the wait |
| One process per chat session, LRU cap of 20 live sessions | `Application/ChatSessionRegistry.swift:16, 26` | Fine |

---

## 4. Cross-client pitfalls

Issue links are GitHub URLs returned by the search API; titles were read, bodies were not.

### Messages, replay, duplication

1. **Duplicate user message from echoed `user_message_chunk`.** Some agents echo the prompt during the live turn (Grok: VERIFIED-DOC `agent-quirks.md:38 @ acpira`). Clients that insert optimistically and also render the echo show it twice. Fixes seen: drop user chunks while a turn is running (`updates.rs:155-158 @ acpira`), ignore them in the live reducer (`TranscriptReducer.swift:31-32 @ codevisor`), render only while `session/load` is the active request (`agent-shell.el:3626-3634`). Zed fixed the same bug (UNVERIFIED, https://git.secluded.site/zed/commit/08e43aed1f6383609910af44d17d149487bbcb5f).
2. **Replay has no message boundaries or timestamps in v1.** OpenCode replays with `messageId` but no stop reason or usage (VERIFIED-DOC `agent-quirks.md:60 @ acpira`). claude-agent-acp replays carry no timestamps (https://github.com/agentclientprotocol/claude-agent-acp/issues/1307) and only from the last compaction (issues/1297); a compaction summary once replayed as a user message (issues/1535). The v2 "message updates" RFD proposes whole-message upserts keyed by an agent-owned `messageId` (draft; https://agentclientprotocol.com/rfds/v2/message-updates, fetched and summarized).
3. **`session/load` vs `session/resume`.** Load replays, resume does not. Agents support different subsets (Pi: load only; DSH: resume only: VERIFIED-DOC `agent-quirks.md:66, 82 @ acpira`). A client that owns its own transcript should prefer resume and never need replay; a client without one needs load. agent-shell exposes this as a setting (`agent-shell.el:967-989`).
4. **Resume resets session settings.** claude-agent-acp answers resume with its defaults (VERIFIED-DOC `protocol-gotchas.md:83 @ acpira`); related reports: https://github.com/RAIT-09/obsidian-agent-client/issues/274, https://github.com/agentclientprotocol/claude-agent-acp/issues/1487, https://github.com/zed-industries/codex-acp/issues/172.
5. **Never-prompted sessions cannot be resumed.** Claude Code writes no transcript until the first turn (`-32002`); codex-acp has "no rollout found"; Devin sweeps them (VERIFIED-DOC `agent-quirks-claude.md:16`, `agent-quirks-codex.md:11`, `agent-quirks.md:32 @ acpira`). Treat as "gone", and only fall back to a fresh session if the local transcript is empty.
6. **Session locked by another process.** codex "thread already has an active writer"; Devin `-32015` (VERIFIED-DOC `agent-quirks-codex.md:12`, `agent-quirks.md:33 @ acpira`). Relevant to Memex if the same Codex thread is open in the Codex app.

### Tool calls and ordering

7. **`tool_call_update` before `tool_call`.** Handled explicitly by Toad (`acp/agent.py:323-335`), Codevisor (`TranscriptToolReduction.swift:86-94`), Aizen (`ChatAgentSessionNotifications.swift:125-132`).
8. **Permission request before the tool's arguments exist.** OpenCode (https://github.com/xenodium/agent-shell/issues/616, issues/615; fix at `agent-shell.el:3722-3735`). Spec-level report: https://github.com/agentclientprotocol/agent-client-protocol/issues/1979.
9. **Partial updates.** Claude tool calls skip `in_progress` (pending, then updates with no status, then completed) and later updates resend only changed fields, so the title disappears unless merged (VERIFIED-DOC `agent-quirks-claude.md:15 @ acpira`). Codex opens with `in_progress` and can leave a call `pending` forever (VERIFIED-DOC `agent-quirks-codex.md:14 @ acpira`). Merge by id; never treat a missing field as cleared; settle open tools when the turn ends.
10. **Tool call ids are unique per agent session only.** A rebuilt session reuses ids (VERIFIED-DOC `protocol-gotchas.md:13 @ acpira`). Key rows by (turn, id).
11. **Updates after the turn ended.** Background work wakes the model after `end_turn`; Claude's autonomous cycles stream with no start mark (VERIFIED-DOC `agent-quirks-claude.md:25 @ acpira`); `session/new`'s deferred `available_commands_update` can straggle past an unrelated cancel (https://github.com/agentclientprotocol/claude-agent-acp/issues/1200). agent-shell surfaces these (`agent-shell.el:2907-2928`); Acpira runs them as detached turns.
12. **Closing reply not streamed after a tool call.** On claude-agent-acp 0.84.0 all chunks of the final reply arrived in the same millisecond at turn end (VERIFIED-DOC `agent-quirks-claude.md:13 @ acpira`). A text-reveal animation hides this; a naive renderer shows a wall of text popping in.

### Cancel, queue, steering

13. **Prompt response can beat its own notifications if they travel different paths.** Aizen's 100 ms sleep (`ChatAgentSession+PromptLifecycle.swift:180-187`). Process responses and notifications on one serial queue in wire order.
14. **Cancel races.** A Stop right after Send can put `session/cancel` on the wire before `session/prompt` (UNVERIFIED, https://github.com/FabioLeitao/zero-bridge/issues/105). codex-acp ignored cancel after `session/load` (https://github.com/zed-industries/codex-acp/issues/186). Gemini CLI threw on cancel (https://github.com/agentclientprotocol/agent-client-protocol/issues/171). Cancel one tool aborting all (https://github.com/xenodium/agent-shell/issues/936).
15. **Cancel must resolve pending permission requests first**, or the agent stays blocked (`agent-shell.el:2562-2582`; `sdk-connection.ts:307-311 @ codevisor`).
16. **Never overlap prompts.** Queue host-side; on "send now", cancel and let the cancelled prompt's completion flush the queue (VERIFIED-DOC `protocol-gotchas.md:55 @ acpira`).
17. **Steering is an extension with sharp edges.** `_session/steering` is implemented by both JetBrains-maintained adapters (`steering.rs:1-6 @ acpira`). Claude honours `idleBehavior: promptRequired`; codex-acp ignores it and starts a turn no prompt owns. Up to claude-agent-acp 0.85.x a Stop with pending steers settles only at a 30 s force floor; from 0.86.0 cancel drops unanswered steers silently (`steering.rs:43-50 @ acpira`). Paseo has an open issue for lacking it (https://github.com/getpaseo/paseo/issues/3964).
18. **Background subagents hold `session/prompt` open** on Claude (VERIFIED-DOC `agent-quirks-claude.md:24 @ acpira`). "Turn in flight" can last long after visible output ends; the UI needs a distinct "waiting on background work" state.

### Startup, auth, hangs

19. **`session/new` can be very slow or hang.** Kiro blocked about 66 s downloading a model (UNVERIFIED, https://github.com/kirodotdev/Kiro/issues/12118); Hermes hung on Windows (https://github.com/NousResearch/hermes-agent/issues/135977); Aizen has an open "terribly slow" report (https://github.com/vivy-company/aizen/issues/25). Mitigations seen: warm pool (Acpira), type-ahead before ready (agent-shell), generous but finite timeouts (Aizen 120 s / 30 s).
20. **Auth is not uniform.** claude-agent-acp advertises no `authMethods` unless the client advertises `clientCapabilities.auth.terminal`, then expects a terminal login rather than `authenticate` (VERIFIED-DOC `agent-quirks-claude.md:7 @ acpira`). codex-acp signs in through `authenticate` (`agent-quirks-codex.md:7`). Grok lets `session/new` succeed signed-out and fails the prompt with `-32603` (`agent-quirks.md:43`). Terminal auth retried on the old process in Zed (https://github.com/zed-industries/zed/issues/65342).
21. **Silent auth failure.** A stale `ANTHROPIC_API_KEY` makes Claude retry for over a minute while the adapter suppresses auth warnings (VERIFIED-DOC `agent-quirks-claude.md:45 @ acpira`).
22. **Turn stuck "working" forever.** No inactivity timeout in the protocol (UNVERIFIED, https://github.com/pingdotgg/t3code/issues/29211 and 30271; agent-shell https://github.com/xenodium/agent-shell/issues/511). Codevisor's 300 s stalled-turn watch and cancel reconciliation address this.
23. **Failures reported as success.** Kimi and Pi answer failed turns with an empty `end_turn`; Antigravity puts the failure in the last message chunk (VERIFIED-DOC `agent-quirks.md:17, 76 @ acpira`). The ACP SDK turns a thrown error into `-32603 "Internal error"` with the real text in `data.details` (VERIFIED-DOC `protocol-gotchas.md:117 @ acpira`). Always surface `error.data`.
24. **Listing sessions has side effects.** OpenCode, DSH and Pi persist a session on every `session/new`; codex-acp filters `session/list` by canonical cwd and can return empty pages with a cursor (VERIFIED-DOC `transcript-pipeline.md:40-41 @ acpira`). Repeated-cursor guard: `agent-shell.el:8721`.
25. **Orphaned child processes** under a long-lived adapter across repeated loads (https://github.com/agentclientprotocol/claude-agent-acp/issues/1011).
26. **Usage numbers are not comparable across agents.** claude-agent-acp reports `used: 0` in `usage_update` and seeds a 200k window (VERIFIED-DOC `agent-quirks-claude.md:10 @ acpira`); `PromptResponse.usage` semantics are contested (https://github.com/agentclientprotocol/agent-client-protocol/issues/1860).

### Rendering

27. **Per-chunk full re-render.** AionUi shows nothing until turn end (https://github.com/iOfficeAI/AionUi/issues/4211); agent-shell had cursor lag from large collapsed outputs (https://github.com/xenodium/agent-shell/issues/839) and whole-block re-fontification (#757, cited at `agent-shell.el:5579-5582`).
28. **Markdown syntax split across chunks.** Image syntax can straddle chunks (VERIFIED-DOC `transcript-pipeline.md:25 @ acpira`); reveal animation must not cut right after Markdown punctuation or a transient `**` flashes (VERIFIED-DOC `ui-motion-layout.md:22 @ acpira`).
29. **Second measurement pass on big code blocks.** Codevisor removed a separate `boundingRect` pass that took seconds once highlighting split the text into many runs (VERIFIED-DOC `TRANSCRIPT.md:125-128, 175-178 @ codevisor`).

---

## 5. Ranked recommendations for a native Swift ACP client

Ordered by expected effect on how the chat feels, with the cheapest wins first within each tier.

### Tier 1: correctness that users feel immediately

1. **One serial, wire-ordered event queue per connection** carrying both notifications and responses. No sleeps, no idle-gap heuristics for completion. (Counter-example: Aizen; pitfall 13.)
2. **Optimistic user bubble keyed by a client-generated id, inserted before any session work.** Reconcile later by id, then by "pending and same text"; merge rather than replace. On delivery failure, pull the bubble and restore the text as undelivered. (Codevisor `RemoteUserMessage.swift:47-89`, `SessionModel+Prompting.swift:41-43, 82-94`.)
3. **Ignore `user_message_chunk` while a prompt is in flight; render it only during a load you asked for.** (Pitfall 1.)
4. **Boundary rule:** `messageId` if present, else "append to the last entry if it is the same kind, otherwise open a new one". (Codevisor `TranscriptTextReduction.swift:66-103`; agent-shell `agent-shell.el:3449-3459`.)
5. **Tool rows are upserts by id that merge omitted fields**, create on update-before-call, and are force-settled when the turn ends or is cancelled. (Pitfalls 7, 9.)
6. **Own the transcript locally; prefer `session/resume` over `session/load`.** If load is the only option, buffer the replay and reconcile against local history by id rather than suppressing by text prefix. Restored history never animates.
7. **Cancel sequence:** resolve pending permission/question requests as cancelled, send `session/cancel`, flip the UI to stopped immediately, then verify the terminal event arrives and reconcile if it does not. (Codevisor `provider.ts:182-188`, `SessionModel+Prompting.swift:128-150`; agent-shell `agent-shell.el:2562-2582`.)

### Tier 2: speed

8. **Warm pool.** Keep one initialized process per (agent, cwd, account) with a TTL; a new chat takes it and only pays `session/new`. Refill after take. (Acpira `pool.rs`.) This is the single largest startup-latency trick I found; Codevisor, Aizen, Toad and agent-shell all pay spawn + initialize per session.
9. **Let the user type and send before the session exists.** Queue the first prompt behind session creation; apply remembered model/mode before releasing it. (agent-shell `agent-shell.el:5399-5410`; Acpira `protocol-gotchas.md:82`.)
10. **Coalesce chunks per frame or on a roughly 50 ms timer**, flush before any other kind of entry to preserve order, and append in place without copying the accumulated string. (Aizen, agent-shell, Codevisor `TranscriptTextReduction.swift:105-114`.)
11. **Incremental Markdown:** reparse only the mutable tail block, cache parsed prefix blocks, latest-value workers for heavy layout. (Textual `_markdown.py:1460-1501`; Codevisor `TRANSCRIPT.md:69-71, 106-107`.)
12. **Virtualize the transcript in AppKit**, cache measured heights, anchor on the first row inside the viewport when heights change, and retain a few recently used transcript surfaces for instant switching. (Codevisor `TRANSCRIPT.md:9-15, 45-47, 92-104`.)

### Tier 3: feel

13. **Shimmer over spinners.** One turn-level activity indicator; running tool rows shimmer their verb. Drive shimmer with a `CAGradientLayer` + `CABasicAnimation` (about 1.2 s linear, band about a third of the width) and phase-lock it to wall-clock time so remounts do not restart it. (Codevisor `ShimmeringText.swift:37-103`; Acpira `ui-motion-layout.md:17-18`.)
14. **Debounce status labels** (about 0.3 s settle, 1.2 s max staleness) so tool bursts do not strobe; never show an empty state, default to "Thinking". (Codevisor `WorkedActivityLabelSmoother.swift:41-83`.)
15. **Adaptive text reveal** with a short per-segment fade (150 ms) whose pacing tracks arrival rate. This also disguises agents that dump the whole final reply at once (pitfall 12). Do not replay it on navigation or on restored history.
16. **Delay every "slow" indicator** (150 ms for loads, 1 s for connection trouble) and give it a minimum visible time once shown. (Codevisor `AttachmentLoadingOverlay.swift:3`, `DelayedConnectionNotice.swift:22-45`.)
17. **Bottom follow based on user intent, not distance.** Release only on a real upward input, re-arm only at the true bottom, re-arm and jump on a new user turn, gate "jump to bottom" on pending measurements. (Acpira `ui-motion-layout.md:35`; Codevisor `TranscriptBottomJumpGate.swift`.)
18. **Queue and steer as first-class composer states.** Composer never disables during a turn. Queue host-side; steer only when `_meta.steering.supported`; a failed steer stays queued; re-queue unanswered steers on cancel for claude-agent-acp ≥ 0.86.0; animate the send differently depending on whether it landed in the transcript or the queue. (Acpira `queue.rs`, `steering.rs`; Codevisor `SessionController+Steering.swift`, `SessionController+Sending.swift:38-43`.)

### Tier 4: robustness

19. **Quirks at the edge.** A per-agent normalization layer before the reducer, with the adapter version recorded for each workaround. (agent-shell `agent-shell.el:2950-2960`; Acpira `vendors/`.)
20. **Surface protocol anomalies** (out-of-turn updates, unknown update kinds) in a debug channel with the payload. (agent-shell `agent-shell.el:2907-2946`.)
21. **Stalled-turn watchdog** and an explicit "waiting on background work" state. (Codevisor `SessionStalledTurnWatch.swift:6`; pitfall 18.)
22. **Classify restore and prompt errors** (auth, gone, locked, not resumable, failed, empty response) and always show `error.data`. (Pitfalls 5, 6, 23.)
23. **A deterministic stress harness**: seed N turns, push chunks on demand, record numeric frame traces. Codevisor's `TRANSCRIPT_STRESS` driver (`TRANSCRIPT.md:347-385`) is what let them find the code-block and stale-height bugs.

---

## Limits of this survey

- Only five clients were read in depth. Table rows marked "Not read" rest on repo metadata and the official listing.
- Acpira's and Codevisor's docs are the source for many behavioural claims about agents; those are their observations at stated adapter versions, and adapter behaviour changes quickly.
- No client was built or run. Nothing here is a measurement of perceived latency.
- JetBrains, Sidequery, Tidewave's main UI and Devin Desktop are closed source and were not inspected.
- Issue bodies were not read; links are pointers for follow-up, not confirmed root causes.
