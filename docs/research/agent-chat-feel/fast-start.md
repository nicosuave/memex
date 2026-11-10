# Fast start: agent session startup and resume latency for a native ACP client

Research date: 2026-11-09. Scope: everything between Enter and first token, plus session resume/reopen. Read-only research; no agent sessions were started, no prompts sent.

## Evidence legend

- **VERIFIED** = `path:line` at a recorded SHA, an official doc URL, or my own measurement (command given).
- **REPORTED** = a first-hand number from an issue/PR/README that I did not reproduce (treat as UNVERIFIED for strictness; URL given).
- **UNVERIFIED** = secondary source or inference (URL or reasoning given).

### Source snapshots (all under `/private/tmp/claude-501/-Volumes-Dev-Code-memex--claude-worktrees-agent-app-ux-patterns-13a76b/f8deb1e4-a002-4452-a81e-c8ef8a51001b/scratchpad/repos/`)

| Repo | Dir | SHA | Notes |
|---|---|---|---|
| zed-industries/claude-agent-acp (redirects to agentclientprotocol/claude-agent-acp) | `start-claude-agent-acp` | `2f7e386f3600658d905906f3b37301120b07605f` | v0.102.0, pins `@anthropic-ai/claude-agent-sdk` 0.3.321 |
| zed-industries/codex-acp | `start-codex-acp` | `296069e841634cd4bb9bc4515602d836e49231ec` | v0.16.0, codex crates at `rust-v0.137.0` |
| agentclientprotocol/agent-client-protocol | `start-acp-spec` | `ab82539032aeaa3926cebfb001007fb8a9ba58a2` | spec + RFDs |
| zed-industries/zed | `zed` (other researcher's clone) | `f0d896f2c18a543335eb155f729d36b624695591` | |
| pingdotgg/t3code | `t3code` (other researcher's clone) | `ee59ab22a819acb0544d85cd6cca0e4aa8c7c444` | |
| sst/opencode | `opencode` (other researcher's clone) | `df54d0cddff3abe5d3d12692f306aca44cc3f13f` | |
| openai/codex | `codex` (other researcher's clone) | `caae161dd0c82f2e04e57e6f9d738fafbce503ba` | |
| BloopAI/vibe-kanban | `start-vibe-kanban` | `ad41bda05844411f03dbe92d36f21a574e1ad9da` | |
| Dimillian/CodexMonitor | `start-codexmonitor` | `dd61b9abd37de5ded86e82b9fe8a83fd49d46fa5` | last commit 2026-03-26 |
| slopus/happy | `start-happy` | `04fd379a9e992fa2d03bd99f44322a53ead8d71a` | |
| generalaction/emdash | `start-emdash` | `9346e9130902a0eac126c1cc37aa10b125eea99b` | |
| superset-sh/superset | `start-superset` | `cedc22dec3ae5e59a762e2a177a27d6159b4c5dd` | |

Claude Code changelog snapshot: `.../scratchpad/research/cc-changelog.txt` (fetched from `anthropics/claude-code` `CHANGELOG.md` via `gh api`, top entry v2.1.319).

Measurement machine: Apple M5 Ultra, macOS 27.2, git 2.56.0, Claude Code 2.1.319 (native Mach-O, 322 MB), codex-cli 0.167.0 (native Mach-O, 271 MB).

---

## 1. Where the time goes

### 1.1 Budget summary (cold path, Enter to first token)

| Stage | Typical cost | Evidence |
|---|---|---|
| Local: persist prompt, create chat row, navigate, paint | should be < 16 ms; entirely under the client's control | design target |
| Spawn ACP adapter process | 1.4 ms `spawn()`; ~70 ms if the adapter is a Node script (`node -e 0` = 0.07 s warm, 0.12 s first run); ~0 ms extra for a bun-compiled/native binary | REPORTED (https://github.com/anthropics/claude-code/issues/13329); VERIFIED own (`/usr/bin/time -p node -e 0`) |
| ACP `initialize` | ~90 ms for claude-code-acp; no CLI is spawned and auth probing is explicitly not awaited | REPORTED (issue 13329); VERIFIED `src/acp-agent.ts:2945-2957` @2f7e386 ("`initialize` never waits on the CLI probe") |
| Workspace: `git worktree add` | 0.6-1.6 s (Emdash's repo); 2.3-3.0 s (my 15.5k-file bench); 38-42 ms with `--no-checkout` | REPORTED (`apps/emdash-desktop/tooling/prototypes/cow-worktree/README.md` @9346e91); VERIFIED own (section 1.6) |
| Workspace: setup script / dependency install | seconds to minutes; "deps need a full install (minutes)" | REPORTED (same Emdash README) |
| ACP `session/new` for Claude | ~2-3 s today, dominated by Claude CLI spawn + SDK initialize; was 11+ s, then ~6 s, then ~3 s over 2026 | REPORTED (https://github.com/agentclientprotocol/claude-agent-acp/issues/567); 1.9 s SDK init in a maintainer probe (https://github.com/agentclientprotocol/claude-agent-acp/pull/1089) |
| MCP servers named in `session/new` | 1.2-2.3 s each for common stdio/http servers; one pathological server 31 s; npx-launched servers tens of seconds | REPORTED (issue 13329); UNVERIFIED (https://longform.social/post/did:plc:tevykrhi4kibtsipzci76d76/2025-12-17-fixing-slow-zed-agent-startup-times) |
| `session/prompt` to first token | model TTFT; first request of a session pays prompt-cache write on a ~15-25k-token system+tools prefix unless the prefix is already cached | UNVERIFIED (third-party `/context` reports: system prompt 2.6-3.5k tokens, system tools 11.6-21k; https://dannywillems.github.io/2026/01/16/claude-code-context-command.html) |

Takeaway: with a default ACP flow (worktree, then spawn, then `session/new`, then prompt), a Claude chat pays roughly 3-6 s of pure startup before the model even receives the prompt, plus setup-script time. Everything in that 3-6 s can be moved off the Enter path.

### 1.2 Claude Code CLI / Agent SDK cold start

- The CLI is a native binary. Trivial invocations are fast: `claude --version` 0.01 s (0.11 s first run), `claude --help` 0.05 s. **VERIFIED own** (`/usr/bin/time -p claude --version`, three runs each). These hit fast paths and are a floor, not session init.
- Real session initialization (settings, plugins, auth, managed policy, MCP bring-up, hooks) is what costs seconds. The adapter's own comment: "The CLI start takes most of the load time" (**VERIFIED** `src/acp-agent.ts:3166-3170` @2f7e386).
- Maintainer statement that the adapter spawns one CLI per session: "unlike the CLI, we are spawning a process for every session because that is how Anthropics SDK API works" (**REPORTED**, benbrandt in issue 567).
- History of `session/new` with no MCP servers: 11+ s (JS-bundle SDK) -> ~6 s (adapter 0.30.0, native binary SDK) -> ~3 s (May 2026 builds). **REPORTED** (issue 567).
- A string-prompt `query()` re-spawns the CLI every call: 11.8-13.4 s each on Windows in Oct 2025; Anthropic's answer was "expected behavior when passing a string prompt. We recommend using streaming input to keep the process alive between turns." **REPORTED** (https://github.com/anthropics/claude-agent-sdk-typescript/issues/34).
- Startup work Anthropic has moved off the critical path (each is a changelog line, **VERIFIED** against `cc-changelog.txt`):
  - v2.1.84: REPL "renders immediately instead of blocking until all servers connect".
  - v2.1.89: `MCP_CONNECTION_NONBLOCKING=true` for `-p`, and `--mcp-config` connections bounded at 5 s "instead of blocking on the slowest server".
  - v2.1.117: concurrent connect of local and claude.ai MCP servers is default.
  - v2.1.144: "SDK/headless MCP startup: pre-wait now overlaps startup instead of blocking before the first turn (up to 2s faster with slow MCP servers)".
  - v2.1.181: fixed "~120ms per launch" regression waiting on managed-settings fetch.
  - v2.1.243: "sandbox and MCP bring-up no longer block the first frame".
  - v2.1.274: `CLAUDE_CODE_MCP_STARTUP_WAIT_MS` bounds how long the first non-interactive turn waits for connecting MCP servers (`0` = don't wait); v2.1.284 still waits up to 2 s for servers named by `--allowedTools`.
  - v2.1.274 (same release): "`--input-format stream-json` sessions: the first turn no longer waits up to 2s for still-connecting MCP servers whose tools tool search defers; they arrive on a later turn".
  - v2.1.281: managed-settings request cost "about 80 ms, 17+ seconds when the network is unreachable"; git reads and startup telemetry no longer precede first frame.
  - v2.1.311: Pro/Max startup skips two network requests within an hour of the last check.
- Residual network sensitivity: several of those lines show startup phases that touch the network (managed settings, account checks). On a bad network, warm processes are worth far more than on a good one.

### 1.3 What claude-agent-acp awaits

All **VERIFIED** in `src/acp-agent.ts` @2f7e386 unless noted.

- `initialize` (`:2945`): pure capability negotiation. `void this.probeCliAuthStatus()` runs in the background and later pushes `_auth/status_update`. No CLI spawn.
- `session/new` (`:3155` -> `createSession` `:11998`), awaited in order, with the adapter's own phase timer (`src/session-timing.ts`, logs `[session/create] ... phase=<name> durationMs= totalMs=`):
  1. `validate-cwd` (`:12047`, `fs.stat`; cwd must be absolute and exist, `validateCwd` at `:11946-11984`)
  2. `resume-transcript` (resume only, tail read)
  3. `settings` (`SettingsManager.initialize()`, `:12071`)
  4. `prepare-query`: builds options, then `query({ prompt: input, options })` (`:12487`) which "spawns the CLI at once"
  5. `sdk-initialize`: `await q.initializationResult()` (`:12501`) - the dominant cost
  6. `models`, `modes` (`sessionModes.initialize`), `agents` (`discoverCustomAgents(q)`), `ultracode`, `register` (`:12591-12842`)
- The session is a streaming-input `query()` fed by a `Pushable<SDKUserMessage>` (`:12066`, `:12487`), so the CLI stays alive across turns. One adapter process hosts many sessions (`this.sessions[...]`), but each session is its own CLI child.
- Options passed that affect init cost: `settingSources: ["user","project","local"]` (`:12273`), `mcpServers` merged from the request (`:12304-12307`), `includePartialMessages: true`, `promptSuggestions: true`, hooks.
- The adapter does not use the SDK's `startup()`/`prewarm()` (grep for `prewarm|startup\(|SpareProcess|WarmQuery|\.claim\(` in `src/` returns nothing). Pre-warming therefore has to be done by the ACP client via an early `session/new`.
- No idle reaper in the adapter: sessions live until `session/close`, a fingerprint change, or process exit (`teardownSession` callers at `:9513`, `:9521`, `:11918`). Lifetime policy is the client's job.

### 1.4 Codex CLI / app-server / codex-acp

- `codex --version` 0.00-0.03 s, `codex --help` 0.01 s. **VERIFIED own.**
- codex-acp is a single Rust process that embeds codex core; sessions are in-process threads: `thread_manager.start_thread(config)` and `self.sessions.insert(session_id, thread)` (**VERIFIED** `src/codex_agent.rs:554-604` @296069e). No per-session subprocess, so `session/new` cost is auth check + config build + thread start + model list. `list_models` uses `RefreshStrategy::OnlineIfUncached` (`src/thread.rs:247-254`), i.e. a network call when the model cache is cold.
- MCP startup in Codex is reported as events (`McpStartupUpdate`, `McpStartupComplete`, **VERIFIED** `src/thread.rs:1428-1431` @296069e) rather than blocking thread creation.
- Codex core pre-warms the model connection itself: "WebSocket prewarm is a v2-only `response.create` with `generate=false`; it waits for completion so the next request can reuse the same connection and `previous_response_id`" (**VERIFIED** `codex-rs/core/src/client.rs:17-25` @caae161). It is scheduled at session start (`core/src/session/session.rs:2202`) and can be re-scheduled for an idle thread (`CodexThread::prewarm`, `core/src/codex_thread.rs:293-299`; `prewarm_with_history` `:305`). App-server registers the creating client before `start_thread` because "Startup can prewarm the Responses socket before start_thread returns" (`app-server/src/request_processors/thread_processor.rs:1631`).
- Codex times its own prewarm with phases `startup_prewarm_create_turn_context`, `_build_tools`, `_build_prompt`, `_websocket_warmup`, `_resolve`, `_total`, plus metrics `STARTUP_PREWARM_DURATION_METRIC` and `STARTUP_PREWARM_AGE_AT_FIRST_TURN_METRIC` (**VERIFIED** `core/src/session/startup_prewarm.rs:27-28` and `record_startup_phase` call sites).

### 1.5 First-request overhead and prompt cache

- Cache lifetime is 5 minutes by default (1 hour at extra cost), refreshed on every hit, "measured from the start of the request". **VERIFIED** (https://platform.claude.com/docs/en/build-with-claude/prompt-caching).
- Official pre-warm: "Set `max_tokens: 0` in your request. The API reads your prompt into the model and writes the cache at any `cache_control` breakpoint, then returns immediately without generating any output." **VERIFIED** (same URL).
- "a cache entry only becomes available after the first response starts" - parallel first requests do not share a cold cache. **VERIFIED** (same URL).
- Pricing: 5-minute writes 1.25x input, 1-hour writes 2x, reads 0.1x (0.05x on Opus 5.5/Sonnet 5.5, 0.025x on Fable/Mythos 5.x). **VERIFIED** (same URL).
- An ACP client cannot issue a `max_tokens: 0` request through Claude Code; the CLI owns the API calls. Inference (UNVERIFIED): because the cache keys on prefix content, the shared system+tools prefix is usually already warm if any Claude Code session with the same version, tool set and model made a request in the last 5 minutes; the per-project suffix (CLAUDE.md, git status, MCP tool lists) is not.

### 1.6 Git worktree creation

My measurement (**VERIFIED own**; `git worktree add --detach <path> HEAD` etc. on `start-superset`, 15,528 tracked files, 426 MB with `.git`, APFS, scratch dir under `/private/tmp`; the clone is a `--filter=blob:none` partial clone with all HEAD blobs present, so treat absolute numbers as an upper-ish bound):

| Operation | Time |
|---|---|
| `git worktree add --detach` (full checkout) | 3.05 s, 2.34 s, 2.60 s |
| `git worktree add --detach --no-checkout` | 42 ms, 38 ms, 42 ms |
| `git checkout HEAD -- .` inside the no-checkout worktree | 2.89 s |
| `cp -cR` (userspace per-file clonefile) of a checked-out worktree | 4.66 s, 4.23 s |
| `git status --porcelain` in the git-written worktree | 0.62 s |
| first `git status --porcelain` in the `cp -cR` copy (index stat data stale) | 3.46 s |

Emdash's own benchmark (**REPORTED**, `apps/emdash-desktop/tooling/prototypes/cow-worktree/README.md` @9346e91, ~208k entries including node_modules): baseline `git worktree add` 0.6-1.6 s; full snapshot via `cp -cR` 58 s; via kernel-recursive `clonefile(2)` 8.1 s; hybrid `worktree add` + `clonefile` of node_modules only 4.3 s. Their conclusion: "`cp -cR` (per-file userspace clonefile) is ~12x slower than a kernel-recursive `clonefile(2)` on the directory".

Two consequences: (a) registering the worktree is ~40 ms, the checkout is the cost; (b) a naive `cp -c` copy is slower than git and leaves a stat-dirty index, so copy-on-write only wins with directory-level `clonefile(2)` plus an index stat rewrite (what T3 Code does, section 2g).

### 1.7 Memory per live agent process

- Eight live Claude Code CLI processes on this machine: 206, 245, 255, 349, 464, 483, 563, 678 MB RSS (median ~405 MB), ages 13 min to 1 h 40 min. `codex app-server`: 30 MB; a Codex host process: 63 MB. **VERIFIED own** (`ps -axo rss,etime,comm`). These are working sessions, not fresh idle ones; the 206 MB process is the closest to an idle floor.
- Community reports put a fresh session at ~343 MB and idle-for-days sessions at ~1 GB, with several leak regressions. **UNVERIFIED** (https://dev.to/alexey-pelykh/the-rss-illusion-63-gb-process-on-a-32-gb-machine-298n; https://github.com/anthropics/claude-code/issues/11315). macOS `footprint` can exceed RSS when pages are compressed.
- Planning number: budget 250-700 MB per live Claude session, ~30-60 MB for a whole Codex app-server/codex-acp process regardless of thread count.

---

## 2. Elimination techniques, verified in real apps

### (a) Long-lived agent processes shared across sessions

| App | Strategy | Evidence (VERIFIED unless noted) |
|---|---|---|
| Zed | One ACP connection per agent per project, cached and shared by all threads; `request_connection` returns the existing entry | `crates/agent_ui/src/agent_connection_store.rs:89-96,186-193` @f0d896f; sessions map `crates/agent_servers/src/acp.rs:240` |
| claude-agent-acp | One adapter process hosts many sessions; each session is a separate Claude CLI child kept alive by streaming input | `src/acp-agent.ts:12066,12487` @2f7e386; issue 567 (REPORTED) |
| codex-acp | One process; sessions are in-process threads | `src/codex_agent.rs:570-599` @296069e |
| T3 Code | `ProviderSessionManager` holds live sessions; Codex adapter multiplexes threads on one process (`supportsMultipleProviderThreadsPerSession: true`), Claude adapter does not (`false`) | `apps/server/src/orchestration-v2/Adapters/CodexAdapterV2.ts:247`, `ClaudeAdapterV2.ts:402` @ee59ab2 |
| CodexMonitor | One shared `codex app-server` reused across all workspaces (`take_live_shared_session`), threads multiplexed with `thread/start`/`thread/resume`; workspaces are connected at app launch | `src-tauri/src/shared/workspaces_core/connect.rs:36,76-78`; `src-tauri/src/backend/app_server.rs:434-448,749-764`; `src/features/workspaces/hooks/useWorkspaceRestore.ts:36-51` @dd61b9a |
| OpenCode | `opencode acp` starts one standalone server and the ACP agent is a thin HTTP client to it; T3's docs note OpenCode 2.x "serves every directory from one server per instance" | `packages/cli/src/commands/handlers/acp.ts:25-35` @df54d0c; `docs/internals/providers.md:21-24` @ee59ab2 |
| Happy | Persistent daemon (`happy daemon start`, detached, lock file, heartbeat) that spawns one CLI process per session; each session holds one streaming-input `query()` | `packages/happy-cli/src/daemon/CLAUDE.md:1-30`; `packages/happy-cli/src/claude/claudeRemote.ts:163-174` @04fd379 |
| Vibe Kanban | Counter-example: process per turn. Follow-ups spawn a new `claude ... --resume <id>`; Codex spawns `codex app-server` per run and forks the thread | `crates/executors/src/executors/claude.rs:360-375,620-632`; `codex.rs:453,613,640-655` @ad41bda |

Which adapters support multiple sessions per process: all of claude-agent-acp, codex-acp, and opencode acp. The difference is cost per session: Claude = one extra CLI process (seconds, hundreds of MB); Codex/OpenCode = an in-process object (tens of ms plus any network).

### (b) Pre-warming

- **Zed pre-creates the next session before the user types.** The agent panel always holds a `draft_thread`; `activate_draft` -> `ensure_draft` builds a `ConversationView`, which immediately requests the shared connection and calls `new_session` (**VERIFIED** `crates/agent_ui/src/agent_panel.rs:1130,2974-3004,3054,4689`; `crates/agent_ui/src/conversation_view.rs:1317,1340-1342,1441-1445` @f0d896f). Sending from the draft is then just `session/prompt`.
- **Zed applies defaults after creation, without blocking**: default mode goes out as a detached `SetSessionModeRequest` while the UI already shows it (`crates/agent_servers/src/acp/v1.rs:1389-1424`); default config options via `SetSessionConfigOptionRequest` (`v1.rs:226`). This is the pattern for "cwd is fixed at `session/new`, everything else can change afterwards".
- **Claude Agent SDK has first-class warm APIs** (**VERIFIED**, https://code.claude.com/docs/en/agent-sdk/typescript):
  - `startup({ options, initializeTimeoutMs }): Promise<WarmQuery>` - "Pre-warms the CLI subprocess by spawning it and completing the initialize handshake before a prompt is available... so the first `query()` call resolves without paying subprocess spawn and initialization cost inline."
  - `prewarm(): Promise<SpareProcess>` (alpha, SDK >= 0.3.282) - "Starts a Claude Code process as a spare before you know which session it will serve, so you can bind it to a session later with `claim()`... The session's working directory, its `SessionStart` hooks, its stdio MCP servers, and its CLAUDE.md and git context wait for the claim."
  - Claude Code itself uses this: "the idle worker that the agent view pre-warms for your next background session" (changelog v2.1.238, **VERIFIED**).
  - Not used by claude-agent-acp today (section 1.3). An upstream change that backs `session/new` with `prewarm().claim()` would remove the cwd constraint for spares.
- **Codex pre-warms the model transport at session start** (section 1.4).
- **Community warm pool for ACP**: "I mitigated the issue by spawning session in advance, keeping them in a pool. Every time a session is taken from the pool another one is started to replace it. But it only work if the session is spawned in the needed workspace." A second user found it incompatible with per-session worktrees because `session/new` needs a valid cwd. **REPORTED** (issue 567).
- **Constraints to design around**:
  - ACP: cwd "MUST be used for the session regardless of where the Agent subprocess was spawned" and is set at `session/new` (**VERIFIED** `docs/protocol/v1/session-setup.mdx:438-442` @ab82539).
  - claude-agent-acp rebuilds the CLI if `cwd`, `mcpServers`, `additionalDirectories`, system prompt, or rebuild-class `_meta` options change (`computeSessionFingerprint`, `src/acp-agent.ts:1840-1872`; teardown-and-recreate at `:11905-11919`). Mode and model do not rebuild.
  - Known hazards of pre-warmed workers in Claude Code's own history (**VERIFIED** changelog): "pre-warmed background workers failing with 'Could not resolve authentication method' when claimed after sitting idle" (v2.1.174); "background agents potentially reading another directory's project settings (`.mcp.json` approvals, trust) when dispatched onto a pre-warmed worker" (v2.1.172); "ignoring project-level settings `env` values... when dispatched onto a pre-warmed worker" (v2.1.169); telemetry double-counting unclaimed `prewarm()` processes (v2.1.319). Codex has open issues of the same family (https://github.com/openai/codex/issues/47792 prewarm racing history restore; https://github.com/openai/codex/issues/47111 "a prewarmed helper is already running").

### (c) Speculative work on hover / keydown / typing

- OpenCode prefetches the two most recent sessions' data as soon as the home list renders (**VERIFIED** `packages/app/src/home/sessions/controller.tsx:333-345` @df54d0c) and preloads the session route module ahead of navigation (`packages/app/src/shell/routes/routes.tsx:18,42-47`).
- T3 Code has an intent-prefetch hook for pull-request rows (`apps/web/src/components/pullRequest/usePullRequestIntentPrefetch.ts:24` @ee59ab2); I did not find an equivalent for agent session start.
- Zed's trigger is "panel shows a draft", which is earlier than keydown.
- I found no open-source app that starts worktree creation on first keystroke. It is a straightforward extension of the spare-worktree pool below and is listed as a recommendation, not a verified practice.

### (d) Deferring MCP servers / lazy tools

- Claude Code now connects MCP servers in the background and exposes per-server status `"connected" | "failed" | "needs-auth" | "pending" | "disabled"` (**VERIFIED**, `McpServerStatus` at https://code.claude.com/docs/en/agent-sdk/typescript), with changelog entries listed in 1.2.
- Tool search defers MCP tool schemas: "`alwaysLoad: false` to defer all of that server's tools behind tool search" (v2.1.287); first stream-json turn no longer waits for servers whose tools are deferred (v2.1.274) (**VERIFIED** changelog).
- `strictMcpConfig`: "Use only the servers passed in `mcpServers` and ignore project `.mcp.json`, user settings, plugin-provided MCP servers, and claude.ai connectors" (**VERIFIED**, same SDK doc). Reachable through the adapter's `_meta.claudeCode.options` pass-through (`...userProvidedOptions`, `src/acp-agent.ts:12293`).
- Servers can be added after the session exists: `setMcpServers()` "resolves after newly added stdio, HTTP, and SSE servers connect or fail, so tools from servers that connected are available on the next turn" (**VERIFIED**, SDK doc). The adapter kicks MCP OAuth in the background after the `session/new` response (`afterSetupResponse`, `src/acp-agent.ts:11382-11398`).
- Client-side rule that follows: never launch client-provided MCP servers through `npx`/`uvx` resolution at session time.

### (e) Streaming-input mode and persistent query reuse

- Official guidance: streaming input "allows the agent to operate as a long lived process" and supports "Queued messages: send multiple messages that process sequentially" (**VERIFIED** https://code.claude.com/docs/en/agent-sdk/streaming-vs-single-mode).
- claude-agent-acp, T3's Claude adapter and Happy all hold one streaming `query()` per session. Vibe Kanban does not and pays CLI startup on every follow-up.
- Startup flags/knobs that cut init: `strictMcpConfig`; narrower `settingSources`; `--bare` ("connect only the MCP servers named on the command line, send the model no system reminders, and start no background tasks", changelog v2.1.286) for utility sessions; `CLAUDE_CODE_MCP_STARTUP_WAIT_MS`. All **VERIFIED** as existing; their effect on a given user's setup needs measurement.

### (f) Prompt-cache warming

- Anthropic API: `max_tokens: 0` pre-warm (section 1.5). Not reachable through Claude Code/ACP.
- Codex: built in (`generate=false` prewarm).
- For Claude via ACP there is no verified client-side technique that does not spend a turn. Do not send a hidden "ping" prompt: it costs tokens, pollutes the transcript, and the 5-minute TTL makes it mostly waste.

### (g) Fast worktrees

| App | Technique | Evidence |
|---|---|---|
| T3 Code | `git worktree add --no-checkout`, then populate by `clonefile(2)` (FFI to libSystem, off the event loop) from a private cached checkout, rewrite the index stat data "so Git trusts the clones without re-reading them", move to the requested tree with `read-tree -m -u`, then run the skipped `post-checkout` hook; falls back to plain `git worktree add` on any failure. Keeps 3 cached checkouts per repo, rebuilds after 500 changed files | **VERIFIED** `apps/server/src/vcs/CheckoutCloner.ts:34-56`; `apps/server/src/vcs/worktreeCheckoutCache.ts:20-23,96-100,601-668` @ee59ab2 |
| Superset | `git worktree add --no-checkout`, set cone-mode sparse checkout, then check out; falls back to full checkout | **VERIFIED** `packages/host-service/src/trpc/router/workspace-creation/shared/sparse-checkout.ts:123-201` @cedc22d |
| Emdash | Fast foreground pipeline `inspect -> resolve-base -> add-worktree -> verify`; base ref fetched only when not locally resolvable; gitignored `preservePatterns` copied as a durable background step with `cp -c` / `--reflink=auto`; push and ref freshening are background steps | **VERIFIED** `agents/workflows/worktrees.md:16-34`; `packages/core/src/runtimes/workspace-registry/node/create-worktree.ts:155-172` @9346e91 |
| Vibe Kanban | Plain `git worktree add` via CLI under a per-path lock; worktrees are garbage-collected and lazily recreated (`ensure_worktree_exists`) | **VERIFIED** `crates/worktree-manager/src/worktree_manager.rs:88-119,303-335`; `crates/local-deployment/src/container.rs:276-284` @ad41bda |
| Codex app | Worktrees under `$CODEX_HOME/worktrees`, detached HEAD, created on submit; keeps the 15 most recent; optional setup script; `.worktreeinclude` for ignored files | **VERIFIED** (https://learn.chatgpt.com/docs/environments/git-worktrees) |
| Cursor | Separate checkout per agent; `.cursor/worktrees.json` setup commands; cap 25 per machine with interval cleanup; "We do not recommend symlinking dependencies into the worktree... Use a fast package manager such as `bun`, `pnpm`, or `uv`" | **VERIFIED** (https://cursor.com/docs/configuration/worktrees) |
| Conductor | Workspace from the repo base branch after fetching origin; tracked files via git checkout; setup script for everything else (closed source) | **VERIFIED** (https://www.conductor.build/docs/core/scripts); no copy-on-write claim found |

Important interaction with agent start: the Claude CLI loads project settings, CLAUDE.md, `.mcp.json` and git context when the session is created. Creating the session in a `--no-checkout` worktree before files exist would start the agent without them. So "register in 40 ms, check out in the background" is only safe if `session/new` waits for the checkout (or for a clonefile populate), or if the session is created with the SDK's deferred-cwd `prewarm()` path. This is my inference from `settingSources: ["user","project","local"]` (`src/acp-agent.ts:12273`) and the `prewarm()` doc sentence quoted above.

### (h) Accept and durably queue the prompt before the session exists

- T3 Code's design: `thread.message.send -> ConversationMessage(role=user) -> Run(status=queued) -> ... -> enqueue provider command`; provider session is ensured afterwards (**VERIFIED** `docs/orchestration-v2/feature-lifecycles.md:20-36` @ee59ab2). A first send that needs a workspace is dispatched as `defer_start`, creating the run in status `preparing` with the chosen `workspaceStrategy`; otherwise `starting` (`apps/server/src/orchestration-v2/Orchestrator.ts:6757,6771,8253`). Later sends queue behind a `preparing` run (`:6314`, `:6362`).
- ACP v1 has no idempotency key on `session/prompt`. The v2 prompt-lifecycle RFD changes that: the prompt response returns an agent `messageId` at insertion time and agents must accept prompts while work is active (**VERIFIED** `docs/rfds/v2/prompt.mdx:13-15,88` @ab82539). claude-agent-acp already advertises `_meta.claudeCode.promptQueueing: true` (`src/acp-agent.ts:3083-3086`).
- Happy shows the lazy variant: it awaits the first message before starting Claude at all (`claudeRemote.ts:94` then `:174`), i.e. durable intake first, zero pre-warm.

---

## 3. Resume and reopen

### 3.1 Protocol costs

- `session/load`: "The Agent MUST replay the entire conversation to the Client in the form of `session/update` notifications" and responds only when all entries have streamed (**VERIFIED** `docs/protocol/v1/session-setup.mdx:136,191` @ab82539).
- `session/resume`: "the Agent MUST NOT replay the conversation history... restores the session context, reconnects to the requested MCP servers, and returns once the session is ready" (`session-setup.mdx:258`).
- v2 proposes one `session/resume` with optional `replayFrom` cursor (`docs/rfds/v2/session-resume-replay.mdx:10-14`).
- Both still need a live agent session. For Claude that is the same CLI spawn as `session/new`.

### 3.2 Measured adapter behavior

- claude-agent-acp runs replay and CLI start concurrently on load: "Start the CLI of a loaded session, and replay the history while the CLI starts. The CLI start takes most of the load time" (**VERIFIED** `src/acp-agent.ts:3165-3233` @2f7e386).
- Maintainer measurement for a 69-message session: cold load "dropped from 20-29 seconds to 1.9 seconds. Transcript model lookup took 5 ms and replay took 14 ms; the remaining time was SDK initialization." **REPORTED** (PR 1089). So today replay is cheap for ordinary sessions and the ~2 s is process start.
- If the session is still live in the adapter and the fingerprint matches, `session/resume`/`session/load` return immediately without touching the CLI (**VERIFIED** `src/acp-agent.ts:11903-11913`); load additionally replays (`:3281-3285`).
- Claude Code `/resume` "up to 67% faster on 40MB+ sessions" (changelog v2.1.116, **VERIFIED**) - very large transcripts are where replay stops being free.
- codex-acp: `load_session`/`resume_session` both go through `restore_session(..., replay)`, resuming from the rollout in-process (**VERIFIED** `src/codex_agent.rs:607-700` @296069e).

### 3.3 How apps keep reopen instant

- **Zed**: keeps up to `max_idle_retained_threads` (default 5) idle threads fully alive in memory, evicting least-recently-updated ones, and never evicting threads that are running, have pending permissions/elicitations, unsent input or selections (**VERIFIED** `assets/settings/default.json:1192-1195`; `crates/agent_ui/src/agent_panel.rs:4305-4335`; `crates/acp_thread/src/acp_thread.rs:4630-4644` @f0d896f). Only threads whose agent supports `session/load` are evictable. On reopen of an evicted thread Zed prefers `session/load`, falling back to `session/resume` (`crates/agent_ui/src/conversation_view.rs:1410-1432`).
- **T3 Code**: the UI is rendered from its own event store/projections ("V2 always supports app-thread resumption from stored events/projections"), and provider processes are reaped and recreated independently: idle timeout 30 minutes, maximum pin 4 hours while "busy but silent", provider resume when work must continue (**VERIFIED** `apps/server/src/orchestration-v2/ProviderSessionManager.ts:59-61,443-444,1189,1256-1270`; `docs/orchestration-v2/feature-lifecycles.md:268-290` @ee59ab2).
- **CodexMonitor**: process stays up for the app's lifetime; threads are listed/resumed on demand (`thread/list`, `thread/resume`).
- **Codex app-server**: distinguishes loaded threads from cold ones ("A cold resume resolves the saved provider from configuration. A loaded thread uses its retained..." `codex-rs/app-server/README.md:356-361` @caae161) and can prewarm an idle thread again with history.
- **OpenCode**: prefetches the two most recent sessions on home (section 2c).

### 3.4 Recommendation for reopen

Render from a local transcript projection, attach lazily, and never replay to paint:

1. Paint from the local store immediately (Memex already indexes agent transcripts, which is exactly the projection T3 builds by hand).
2. Do not attach on open. Start attach (`session/resume`, no replay) on the first signal of intent to write: composer focus or first keystroke. If the user only reads, no process is ever spawned.
3. Use `session/load` only as a reconciliation tool when the local projection is known to be behind the agent's transcript (edited elsewhere, crash mid-turn). Diff by message id where available; otherwise by tail fingerprint.
4. Keep an LRU of live sessions sized by memory, not count (section 5.4).

---

## 4. Remote / SSH execution

- **Connection reuse.** Zed runs its own master: `ssh -N -o ControlPersist=no -o ControlMaster=yes -o ControlPath=<socket>` and multiplexes every later command over it; it also detects and reuses a user's existing ControlMaster via `ssh -G` (**VERIFIED** `crates/remote/src/transport/ssh.rs:314-336,723-797` @f0d896f). T3 Code and Emdash explicitly disable multiplexing on their tunnels (`ControlMaster=no`, `packages/ssh/src/tunnel.ts:1146-1150` @ee59ab2; `apps/emdash-desktop/src/core/services/ssh/node/transport/transports.ts:45` @9346e91) because their model is a persistent server on the host, not a command per action.
- **Persistent daemon on the host.** Zed starts a remote server that outlives the connection and reattaches with `proxy --identifier <id> --reconnect` (**VERIFIED** `crates/remote/src/transport/ssh.rs:652-678`; `crates/remote_server/src/server.rs:948-1005`). T3 Code runs its whole server in the environment: "The environment owns providers, execution, files, and durable state. Direct access, Tailscale, SSH, and T3 Connect change how the client reaches that server; they do not introduce another execution model" (`docs/internals/remote.md:3-7`). Happy's daemon is the same idea for mobile.
- **Why it matters for latency.** Without multiplexing, every remote spawn pays TCP + key exchange + auth (several round trips and often hundreds of ms to seconds); with a master it is one channel open. I did not measure this; it is standard OpenSSH behavior (UNVERIFIED, https://man.openbsd.org/ssh_config#ControlMaster).
- **Latency masking.** The same rules as local, but stricter: Enter is acknowledged and rendered from local state; worktree creation, `session/new` and replay all happen on the host next to the repo; the client only streams ACP frames. Reopen paints from the local projection and reconciles after attach.
- **Recommendation.** For each remote host keep (1) one ControlMaster socket owned by the app (reuse the user's if present), (2) one long-lived per-host helper started through it that owns the ACP adapter processes, the warm spare, the worktree pool, and a spool of undelivered ACP frames so a dropped SSH link does not kill the turn. Pre-open the master when a remote project is selected in the composer, not on Enter.

---

## 5. Recommended startup architecture for a native ACP client

Targets: Enter-to-working-indicator < 50 ms (p99), Enter-to-`session/prompt`-written < 100 ms on the warm path, Enter-to-first-token = model TTFT on the warm path.

### 5.1 Principles

1. **Nothing on the Enter path awaits a process, git, the network, or the agent.** Enter does a local durable write and a local state transition; the UI renders from that.
2. **One long-lived adapter process per (agent kind, execution host, environment fingerprint)**, started after first frame at app launch for the agents the user actually used recently, and `initialize`d immediately. This removes spawn + initialize (~0.1 s) and, for Codex/OpenCode, nearly all session cost.
3. **Keep exactly one warm spare session per likely target.** For Claude this removes the 2-3 s CLI start; for Codex it triggers its built-in transport prewarm.
4. **Decouple workspace from session by pooling workspaces too**, because cwd is fixed at `session/new`.
5. **The local store is the source of truth for the UI**; the agent session is an attachment that can be absent.

### 5.2 Session lifecycle state machine

States are per local chat (`ChatID`) plus an unowned spare. `S` = an ACP session id on a live adapter connection.

```
                 +-----------------------------------------------+
                 |                                               |
   (spare)   cold --spawn/session.new--> warming --ok--> warm-idle
                 ^                          |               |  claim(chat)
                 |                          | fail          v
                 |                          v            attached <------------------+
                 |                       failed             |  flush outbox          |
                 |                    (auth | spawn |       v                        |
                 |                     workspace)        prompting --turn end--------+
                 |                          |               |  cancel/close/evict
                 |                    retry / sign-in       v
                 |                          |            draining --done--> parked
                 +--------------------------+               ^                 |
                 ^                                          |                 | intent to write
                 +------------------ evict (LRU/idle/memory)+                 v
                                                                           warming (resume)
```

| State | Meaning | Holds | Enter while here |
|---|---|---|---|
| `cold` | No process, no session. Chat may or may not exist locally | nothing | enqueue, go `warming` |
| `warming` | Workspace and/or `session/new` (or `session/resume`) in flight. Sub-phases: `workspace`, `adapter`, `session`, `defaults` | in-flight tasks | enqueue; indicator shows current sub-phase |
| `warm-idle` | Spare: session exists, defaults applied, no owner, never prompted | 1 live session, maybe 1 spare worktree | claim -> `attached` -> flush |
| `attached` | Bound to a chat, idle, ready for `session/prompt` | 1 live session | send immediately |
| `prompting` | A turn is running | 1 live session | enqueue; steer or queue per agent capability |
| `draining` | Stop requested (cancel, close, eviction, app quit): waiting for the turn to settle and final updates to be persisted | 1 live session, bounded timer | enqueue; will be delivered after re-attach |
| `parked` | Session closed (`session/close`) or process gone; transcript and resume cursor are local | nothing live | enqueue, go `warming` (resume path) |
| `failed(reason)` | `authRequired`, `spawnFailed`, `workspaceFailed`, `sessionFailed` | error + retry policy | enqueue; composer stays usable; banner offers the fix |

Transition rules:

- `cold -> warming`: triggered by pre-warm policy (5.3) or by Enter.
- `warming -> warm-idle`: `session/new` returned and default mode/model/config were sent. Send those as non-blocking `session/set_mode` / `session/set_config_option` and show the intended values optimistically (Zed's approach).
- `warm-idle -> attached`: claim is a local, synchronous map update. Claim is valid only if the spare's fingerprint (agent, host, cwd, MCP set, system prompt, rebuild-class options) equals the chat's required fingerprint. Otherwise discard the spare (`session/close`) and go through `warming`.
- `attached -> prompting`: write `session/prompt`. Mark the outbox row `sending` before the write and `sent` when the first `session/update` for that turn or the prompt response arrives.
- `prompting -> attached`: prompt response with a stop reason, and all trailing updates applied.
- `attached -> draining -> parked`: eviction or user close. `draining` has a hard timeout (T3 uses a 15 s interrupt timeout and a 30 s scope-close timeout; `ProviderSessionManager.ts:61-63`). Never evict a session that is `prompting`, has a pending permission/elicitation, or has background work.
- `parked -> warming`: `session/resume` (no replay). Trigger on composer focus or first keystroke in that chat, not on open.
- Any state -> `failed`: keep the chat, the outbox and the composer fully usable. Recovery re-enters `warming`.
- App quit: `draining` for running turns only if the user opted into finishing; otherwise persist and exit. A running turn should be recoverable next launch via `parked -> warming`.

### 5.3 What to pre-warm, and when

| Trigger | Action | Cost | Notes |
|---|---|---|---|
| App launch, after first frame, at utility QoS | Spawn + `initialize` the adapter(s) for the last-used agent(s) | ~0.1 s CPU, 30-100 MB | Also surfaces auth state early via `_auth/status_update` |
| Home visible with a project+agent selected | Ensure one spare session for (agent, project, default MCP set). If the project defaults to worktrees, ensure one spare worktree first and create the spare session inside it | Claude: one CLI, ~250+ MB, ~2-3 s in the background | This is the main win |
| Composer focus or first keystroke, if no matching spare | Start `warming` now (debounce 100-150 ms after a picker change) | same | Typing a prompt takes longer than 3 s for most prompts, so this alone hides most cold starts |
| Project / agent / execution-host picker change | Retarget: keep the old spare if cheap (Codex), close it if expensive (Claude) after a 10 s grace | | Avoids thrash while the user flips pickers |
| Spare claimed | Replenish after the first token of the new turn, not before | | Do not compete with the turn the user is waiting on |
| Sidebar hover > 150 ms or keyboard selection of an old chat | Prefetch its local transcript page into memory. Do not attach | ms | Attach only on intent to write |
| Remote project selected | Open ControlMaster and the host helper | one handshake | |

Worktree handling:

- Keep a pool of 1 spare worktree per project that uses worktrees, with a neutral directory name (not derived from the task) and detached HEAD at the base ref. Create it with a clonefile-backed populate where available (T3's approach) or plain `git worktree add`; either way it happens before the user presses Enter.
- On claim: if the base ref moved, `git reset --hard <base>` in the clean spare (cost proportional to changed files), then create the task branch. Both are sub-100 ms on a fresh spare in the common case; if the base moved a lot, treat as a cache miss.
- Setup scripts and dependency installs run in the spare ahead of time when they are deterministic for the base ref; otherwise run them after claim in parallel with the first turn and tell the agent (or gate tool use) until they finish. This is a product decision; default to "run ahead in the spare".
- Never create the Claude session in a worktree that has not been populated (see the warning in 2g).
- Bound disk: 1 spare per active project, at most 3 spares total, delete spares unused for 24 h. Codex keeps 15 managed worktrees and Cursor 25; those are for used ones, not spares.

### 5.4 Pool sizes and eviction

| Pool | Size | Eviction |
|---|---|---|
| Adapter processes | 1 per (agent kind, host, env fingerprint) | Kill after 30 min with zero live sessions and zero spares; restart on demand |
| Spare sessions (Claude) | 1 for the current Home target; at most 2 overall | Close when the target changes (10 s grace), after 15 min unclaimed, on auth change, on adapter/CLI version change, on settings/MCP config change, or under memory pressure. Re-create rather than trust a stale spare (see the Claude Code pre-warm bugs in 2b) |
| Spare sessions (Codex/OpenCode) | 1 per recently used project, up to 3 | Same triggers; cheap enough to be generous |
| Live attached sessions | Memory-budgeted: default 4 idle Claude sessions (about 1-2.8 GB at 250-700 MB each) or `max(2, min(6, physicalRAM/8GB))`; unlimited for in-process agents within reason | LRU by last user interaction; idle timeout 30 min (T3's default); respond to `DispatchSource` memory-pressure warnings by parking all idle sessions immediately. Never evict `prompting`, pending-permission, or background-work sessions. Pinned chats are exempt up to a cap |
| Spare worktrees | 1 per active worktree project, max 3 | 24 h unused; base ref drift beyond a threshold |

Zed's default of 5 retained idle threads and T3's 30-minute idle release are the two verified reference points; the memory-based cap is my addition because Claude sessions are an order of magnitude heavier than Codex ones.

### 5.5 Durable prompt queue

On Enter, on the main actor, in this order:

1. Generate `chatID` (if new) and `turnID`. Insert an outbox row `{turnID, chatID, payload, attachments refs, target fingerprint, state: queued, createdAt}` and the user message into the local transcript in one SQLite transaction (WAL, `synchronous=NORMAL` is enough to survive app crash; the write is sub-millisecond for text; large attachments are already on disk and referenced).
2. Navigate to the chat and render the user bubble plus the working indicator from that local state. This is the < 50 ms deliverable and it does not depend on any state in 5.2.
3. Hand the chat to the lifecycle actor: claim a matching spare or start `warming`.

Flush rules:

- Flush strictly in order per chat when the session reaches `attached`.
- Before writing `session/prompt`, set `state: sending` and persist. After the first update or the response, set `sent`.
- If the user queues more prompts while `warming`, keep them as separate rows; deliver them one per turn unless the agent advertises queueing (claude-agent-acp: `_meta.claudeCode.promptQueueing`), in which case they can be forwarded as soon as the first is accepted.
- Edits and deletes of a `queued` row are local and free. Once `sending`, they become cancel + new turn.
- Crash recovery: rows in `queued` are flushed normally after re-attach. Rows in `sending` are ambiguous in ACP v1 (no idempotency key): reconcile against the agent's transcript (the local index of the agent's own transcript file, or a `session/load` replay) and mark `sent` if the user message is present; otherwise surface "Not delivered - Retry". Never auto-resend an ambiguous row.
- The indicator text should track the real sub-phase (`Preparing workspace`, `Starting agent`, `Connecting tools`, `Thinking`), which is cheap once the spans in 5.7 exist.

### 5.6 Failure handling without blocking the composer

| Failure | Detection | Behavior |
|---|---|---|
| Auth required | Adapter `_auth/status_update` push after `initialize` (claude-agent-acp), or `auth_required` error from `session/new` (Zed handles this at `conversation_view.rs:1451-1458`) | Detect at pre-warm time, before Enter. Show a non-modal banner on Home with the sign-in action. Composer still accepts and queues. After sign-in, discard spares and re-warm (the adapter itself refuses to reuse a pre-sign-in session; comment at `src/acp-agent.ts:12537-12545`) |
| Adapter spawn / `initialize` failure | Process exit, timeout (2 s soft, 10 s hard) | `failed(spawnFailed)` with stderr tail; exponential backoff 0.5 s -> 30 s; the chat exists and the prompt is queued; one-click retry |
| `session/new` failure or timeout | Error or > 15 s | Same, plus fall back to a fresh adapter process once (covers a wedged adapter) |
| Workspace failure (git, disk, setup script) | Non-zero exit | Offer "Run in project directory instead" and "Retry"; keep the prompt queued |
| MCP server slow/failed | Server status `pending`/`failed` | Never block the turn; show a per-server chip; do not hold the spare back |
| Spare went stale (auth rotated, settings/MCP changed, CLI updated) | Fingerprint mismatch or liveness probe failure at claim | Treat as a miss: close and go through `warming`. A claim must never produce a session with the wrong project settings |
| Session died mid-turn | Adapter reports closure / process exit | `parked` with the partial transcript kept; next intent to write resumes; any `sending` row follows the ambiguity rule |
| Remote link dropped | SSH master exit | Host helper keeps the turn running and spools frames; client shows "Reconnecting", replays the spool on reattach |

### 5.7 Telemetry: spans to time

Emit as `os_signpost` intervals plus one structured log line per chat start. Every span carries `agent`, `host` (local/remote id), `path` (`warm-claim` | `cold` | `resume`), `worktree` (none/pooled/created), `mcp_count`, and adapter/CLI versions.

Enter path (user-perceived):

- `enter_to_indicator` - keydown handler entry to the frame that shows the bubble + indicator (target p99 < 50 ms; alert at > 100 ms).
- `enter_to_durable` - outbox transaction commit.
- `enter_to_prompt_written` - `session/prompt` bytes written to the adapter stdin.
- `enter_to_first_update` - first `session/update` of the turn (any kind).
- `enter_to_first_token` - first `agent_message_chunk` / `agent_thought_chunk`. Split by `path`; the warm-claim distribution is the model's TTFT and is the baseline every other path is compared against.
- `claim_hit` (bool) and `claim_miss_reason` (`none`, `fingerprint`, `stale`, `auth`, `memory`, `not_ready`).

Startup path (mostly off the Enter path, but regressions here turn into misses):

- `adapter_spawn` (posix_spawn to first byte on stdout), `acp_initialize`.
- `session_new` total, and for claude-agent-acp parse its phase log lines (`validate-cwd`, `settings`, `prepare-query`, `sdk-initialize`, `models`, `modes`, `agents`, `register`) from stderr so the CLI-start share is visible.
- `session_defaults` (set_mode / set_config_option round trips).
- `mcp_ready` per server, from status updates.
- `workspace_register`, `workspace_populate` (clone or checkout), `workspace_setup_script`, `workspace_claim_fixup` (reset + branch).
- `spare_age_at_claim` and `spare_wasted` (closed unclaimed) - the cost side of pre-warming. Codex tracks the same pair (`STARTUP_PREWARM_AGE_AT_FIRST_TURN_METRIC`, `STARTUP_PREWARM_DURATION_METRIC`).

Resume path:

- `open_to_paint` (sidebar click to transcript painted from local store; target < 50 ms).
- `intent_to_attached` (`session/resume` round trip), `load_replay_duration` and `load_replay_messages` when a reconciliation load is used.
- `evictions` by reason, `live_sessions`, summed RSS of agent children (sampled once a minute).

Remote: `ssh_master_connect`, `host_helper_attach`, `rtt_ms`.

Regression gates worth automating: p95 `enter_to_indicator`; warm-claim hit rate; p50/p95 `enter_to_first_token` on the cold path minus the warm path (this difference is exactly the startup overhead the user feels).

### 5.8 Prioritized techniques

| # | Technique | Expected saving on Enter-to-first-token | Risk / cost | Evidence strength |
|---|---|---|---|---|
| 1 | Local-first Enter: durable outbox + optimistic chat + lifecycle actor | Makes indicator < 50 ms regardless of everything else; removes all perceived blocking | Ambiguous-delivery handling after crashes; needs the outbox state machine | T3 Code source (VERIFIED) |
| 2 | Long-lived adapter process per agent/host, spawned + initialized at launch | ~0.1 s (spawn + initialize); for Codex/OpenCode also most of session cost | Idle memory 30-100 MB; restart handling | Zed, CodexMonitor, OpenCode source (VERIFIED) |
| 3 | Spare session pre-created for the Home target (and on composer focus/first keystroke as fallback) | Claude: ~2-3 s (historically 6-11 s); more on slow networks or with MCP servers | 250-700 MB per Claude spare; stale auth/settings on a long-idle spare; wrong-project settings if the fingerprint check is sloppy; wasted spares | Zed draft thread (VERIFIED), SDK `startup()`/`prewarm()` docs (VERIFIED), issue 567 pool report (REPORTED) |
| 4 | Spare worktree pool, session pre-created inside the spare | 0.6-3 s checkout plus setup-script time (seconds to minutes) | Disk; base drift; setup scripts run for work that may never happen; cleanup bugs | Own bench + Emdash numbers; pooling itself is not verified in an open-source app (recommendation) |
| 5 | Fast populate: `worktree add --no-checkout` + `clonefile(2)` from a cached checkout + index stat rewrite | Cuts populate from seconds to roughly 0.1 ms per changed file plus a directory clone, per T3's comments | Significant complexity (filters, LFS, hooks, case-folding, sparse); macOS/APFS only; must fall back cleanly | T3 Code source (VERIFIED); my `cp -cR` bench shows the naive version is slower than git |
| 6 | Lazy attach on reopen: paint from local projection, `session/resume` on intent to write, LRU of live sessions | Reopen becomes local-only (< 50 ms); saves ~2 s per reopened Claude chat that the user only reads | Local projection can drift from the agent transcript; needs reconciliation path | T3 Code (VERIFIED), Zed LRU (VERIFIED), PR 1089 numbers (REPORTED) |
| 7 | Keep MCP off the critical path: pass only needed servers, no `npx` at session time, surface `pending` status, consider `strictMcpConfig` for speed-sensitive profiles | 1-2 s typical per slow server; tens of seconds in bad cases | Tools missing on the very first turn; `strictMcpConfig` hides the user's own servers | Changelog + SDK docs (VERIFIED), issue 13329 (REPORTED) |
| 8 | Apply mode/model/config after `session/new` without awaiting | One round trip each, tens of ms | Brief window where the session has the old mode; must roll back UI on error | Zed source (VERIFIED) |
| 9 | Remote: app-owned ControlMaster + per-host helper owning adapters, spares and a frame spool | Removes per-action handshakes; keeps turns alive across drops | A daemon to version and upgrade on the host | Zed, T3 Code source (VERIFIED) |
| 10 | Upstream: back claude-agent-acp `session/new` with SDK `prewarm().claim()` so a spare is cwd-agnostic | Makes #3 work without #4 for worktree users | Alpha SDK API; depends on maintainers | SDK docs (VERIFIED); not implemented in adapter (VERIFIED by grep) |
| 11 | Prompt-cache warming | Up to one cache-write worth of TTFT on the first turn | No clean path through Claude Code/ACP today; a hidden ping turn is not acceptable | Anthropic docs (VERIFIED) for the API; inapplicable via ACP |

Order of work: 1, 2, 3 give the bulk of the result and are independent of worktrees. 6 is the reopen equivalent of 1. 4 is needed only when worktrees are the default. 5 is an optimization of 4 and should wait for telemetry showing populate time matters.

### 5.9 Open questions the evidence does not settle

- Actual `sdk-initialize` time on the owner's machine and network with their plugin/MCP/hook set. The adapter already logs it; capture it before sizing pools.
- Whether a Claude spare created long before use still reads fresh project settings and credentials at first prompt. Claude Code's changelog shows this class of bug existed for its own pre-warmed workers; the conservative policy in 5.4 (short spare lifetime, invalidate on any config/auth change) is the mitigation.
- First-turn TTFT with a cold versus warm prompt cache for Claude Code's real prefix. Not measured here (no prompts were sent).
- Memory of a freshly initialized, never-prompted Claude CLI. My RSS figures are from working sessions.
