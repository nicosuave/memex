# ACP (Agent Client Protocol) and its adapters: what a zero-spinner client gets, and what it must paper over

Research date: 2026-11-09. Read-only research; nothing was built or run from the cloned repos.

## Sources and SHAs

All clones are shallow, under
`/private/tmp/claude-501/-Volumes-Dev-Code-memex--claude-worktrees-agent-app-ux-patterns-13a76b/f8deb1e4-a002-4452-a81e-c8ef8a51001b/scratchpad/repos/`.

| Alias | Repo | SHA | Version at SHA |
|---|---|---|---|
| SPEC | `agentclientprotocol/agent-client-protocol` (`acp-agent-client-protocol`) | `ab82539032aeaa3926cebfb001007fb8a9ba58a2` (2026-11-09) | schema v1 `1.30.0`, schema v2 `2.0.0-alpha.14` |
| CLAUDE | `agentclientprotocol/claude-agent-acp` (`acp-claude-agent-acp`) | `2f7e386f3600658d905906f3b37301120b07605f` (2026-11-09) | `0.102.0`; `@anthropic-ai/claude-agent-sdk 0.3.321`; `@agentclientprotocol/sdk 2.5.0` |
| CODEX | `agentclientprotocol/codex-acp` (`acp-codex-acp`) | `ab7a968e982a95d8a765c1e6c6682a520d4f7638` (2026-11-09) | `2.6.0`; `@openai/codex ^0.166.0` |
| TS-SDK | `agentclientprotocol/typescript-sdk` (`acp-typescript-sdk`) | `74c78331901d950ac39b347169a27b8cb4a04807` | `2.5.0` |
| RUST-SDK | `agentclientprotocol/rust-sdk` (`acp-rust-sdk`) | `e75d6f19fabb97d389fd471e89b093bbb50ba547` | `5.1.0` |

Claim labels: **VERIFIED** = read at the cited path:line at the SHA above (or via `gh` against the live repo on 2026-11-09). **UNVERIFIED** = inference or secondhand.

Naming note: the old `zed-industries/agent-client-protocol`, `zed-industries/claude-code-acp` and `zed-industries/codex-acp` all now live under the `agentclientprotocol` GitHub org. `codex-acp` is no longer the Rust adapter; it is a TypeScript adapter that drives the Codex **app-server** (VERIFIED: CODEX `README.md:7`, `package.json`).

---

## 1. TL;DR

1. **There are now two protocol versions.** v1 is "Latest" and what every shipping adapter speaks by default. **v2 was published as a Draft on 2026-07-20** and is the version that actually fixes the things a chat client cares about: `session/prompt` returns at *insertion* with an agent-generated `messageId` instead of at turn end, there is an explicit `state_update` (`running` / `requires_action` / `idle`), prompts may be sent while a turn is running, tool-call content and terminal bytes stream as chunks, and `session/load` folds into `session/resume` with an optional `replayFrom` cursor. v2 is explicitly "gate behind version negotiation AND feature flags; do not ship by default" (VERIFIED: SPEC `docs/announcements/acp-v2-draft.mdx`, `docs/docs.json` tags v1 "Latest", v2 "Draft"). Both the Claude and Codex adapters already serve v2 experimentally when the client asks for protocol version 2 (VERIFIED: CLAUDE `README.md:131-145`; CODEX `src/CodexAcpServer.ts` `initializeV2`).
2. **On v1 the protocol gives a client almost nothing for optimistic UI.** No turn-started signal, no ack that a prompt was accepted, no echo of the live user message, no id the client can use to reconcile its optimistic bubble with replayed history, no defined behavior for a second `session/prompt` while one is in flight, and no "replay finished" marker other than the `session/load` response. All of these are client inference on v1.
3. **Session creation is the dominant latency, and it is adapter-specific.** Claude: `session/new` spawns a fresh Claude Code CLI process *per session* and awaits SDK initialization plus several control round-trips before returning (reported ~3 s on current builds, with 10-20 s regressions in the issue history). Codex: one shared app-server process is started at `initialize`; `session/new` is a handful of RPCs (`thread/start`, skills refresh, model list) and does not wait for MCP servers. `session/resume` and `session/load` on the Claude adapter pay the same CLI cold start as `session/new`.
4. **Restore cannot be instant through the protocol.** `session/load` MUST replay the whole history as notifications and only then answer; the Claude adapter's answer lands 0.7-1.9 s after the replay is complete on an idle machine and 4-9 s cold (figures from the in-flight `replay_complete` RFD). `session/resume` skips replay but still pays process start. A client that wants instant switching must render from its own persisted transcript and treat ACP attach as a background reconciliation.
5. **Steering and queueing are not standardized, but both main adapters ship them.** Both expose a pre-standard `_session/steering` extension method (advertised as top-level `_meta.steering.supported` in the `initialize` response). The Claude adapter additionally accepts overlapping `session/prompt` requests and queues them (`agentCapabilities._meta.claudeCode.promptQueueing: true`); Codex serializes overlapping prompts through a per-session turn-start FIFO. Two competing RFD PRs (#1261 explicit `delivery: steer|queue`, #2255 implicit steering) are open against v2.
6. **`session/cancel` cancels queued prompts too.** On both adapters a cancel resolves every not-yet-started queued prompt as `cancelled`. A client with a durable queue must own the queue itself and feed prompts one at a time if it wants "stop current, keep the rest".
7. **Config (model, mode, effort) can change mid-session at any time; cwd cannot.** cwd is fixed at `session/new`; the Claude adapter tears down and respawns the CLI if a `resume`/`load` arrives with a different cwd or MCP set. Pre-warming is therefore only possible per-cwd, which conflicts with one-worktree-per-session unless the worktree exists first.
8. **Swift**: there is no official Swift SDK. Five community libraries exist; the most current is `aptove/swift-sdk` (Swift 6, port of the Kotlin SDK, pushed 2026-11-09), the most starred is `wiedymi/swift-acp` (Swift 5.9, used by Aizen, stale since July). None is clearly production-grade for v2. For a client that already has its own ACP runtime, the schema JSON (`schema/v1/schema.json`, `schema/v2/schema.json`) is the thing to track, not a library.

---

## 2. Protocol surface, with stability

Stability key: **S1** = in stable v1 schema (`schema/v1/schema.json`, `meta.json`). **U1** = only in `schema/v1/schema.unstable.json`. **S2-draft** = in the "stable" feature set of the v2 draft (`schema/v2/schema.json`), which is itself a Draft. **U2** = only in `schema/v2/schema.unstable.json`. Method lists are VERIFIED from SPEC `schema/v1/meta.json`, `schema/v1/meta.unstable.json`, `schema/v2/meta.json`, `schema/v2/meta.unstable.json`; enum variants are VERIFIED by `jq` over the four schema files.

### Methods

| Surface | v1 | v2 draft | Notes |
|---|---|---|---|
| `initialize` | S1 | S2-draft | v2 renames to role-agnostic `info` / `capabilities`; support markers become objects, not booleans (SPEC `docs/protocol/v2/migration.mdx:84-199`). |
| `authenticate` | S1 | replaced by `auth/login` (S2-draft) | Terminal auth methods are stable (RFD auth-methods, Completed 2026-08-20). |
| `logout` | S1 | `auth/logout` S2-draft | Completed 2026-05-22. |
| `auth/status`, `auth/status_update` | U1 | U2 | RFD get-auth-state, Draft. Both adapters ship a private `_auth/status_update` push instead. |
| `session/new` | S1 | S2-draft | Response carries `sessionId`, `modes`, `configOptions` (v1); `configOptions`, `availableCommands` (v2). |
| `session/load` | S1 (capability `loadSession`) | **removed**; use `session/resume` + `replayFrom: {type:"start"}` | v1: MUST replay entire conversation then respond. |
| `session/resume` | S1 (capability `sessionCapabilities.resume`) | S2-draft, **required** if `capabilities.session` | v1: MUST NOT replay. v2: optional `replayFrom` cursor; always sends a current `state_update` before responding. |
| `session/list` | S1 (capability `list`) | S2-draft, required | Cursor pagination, optional `cwd` filter, rows `{sessionId, cwd, title?, updatedAt?}`. |
| `session/close` | S1 (capability `close`) | S2-draft, required | Cancels work and frees resources; idempotent. |
| `session/delete` | S1 (capability `delete`) | S2-draft (capability) | Completed 2026-06-05. |
| `session/fork` | U1 | U2 | RFD session-fork, Draft. Shipped by Claude and Codex adapters. |
| `session/archive`, `session/unarchive` | U1 | U2 | RFD Draft (2026-10-28). |
| `session/status` | U1 | U2 | RFD Draft (2026-10-26): open / saved / unknown without side effects. |
| `session/set_title` | U1 | U2 | RFD Draft (2026-10-26). Codex adapter ships it. |
| `session/set_mode` | S1 | **removed** (modes become a config option with `category: "mode"`) | |
| `session/set_config_option` | S1 | S2-draft | Response is the complete config option list. |
| `session/set_model` | **removed** from v1 artifacts 2026-06-01 | n/a | Use config options with `category: "model"`. 17 of 41 registry agents still answer it (registry matrix). |
| `session/prompt` | S1: resolves at **turn end** with `stopReason` | S2-draft: resolves at **insertion** with `messageId` | The single biggest behavioral difference. |
| `session/cancel` (notification) | S1 | S2-draft | v2: also answers un-inserted prompts with `-32800`. |
| `$/cancel_request` (notification) | S1 | S2-draft | Completed 2026-06-29. Cancels any outstanding JSON-RPC request by id; optional to honor. |
| `tool_call/cancel`, `terminal/cancel` | n/a | S2-draft | Per-item cancel for background work, gated by `capabilities.cancel` on the item. |
| `session/request_permission` (agent->client request) | S1: bound to a `toolCall` | S2-draft: `title` required, optional `description`, extensible `subject` | Options: `allow_once`, `allow_always`, `reject_once`, `reject_always`. |
| `elicitation/create`, `elicitation/complete` | S1 | S2-draft | Completed 2026-07-24. Form mode and URL mode; client advertises each. This is the structured-question surface. |
| `fs/read_text_file`, `fs/write_text_file` | S1 | **removed** | v2 RFD client-filesystem-terminal-capabilities. |
| `terminal/create`, `output`, `wait_for_exit`, `kill`, `release` (client-side execution) | S1 | **removed**; replaced by agent-owned display-only terminals (`terminal_update`, `terminal_output_chunk`) | |
| `providers/list`, `providers/set`, `providers/disable` | U1 | U2 | RFD custom-llm-endpoint, Draft. |
| `mcp/message` (MCP over ACP) | U1 | U2 | RFD mcp-over-acp, Active. |
| `proxy/initialize`, `proxy/successor` | U1 | U2 | RFD proxy-chains, Draft. |
| Underscore-prefixed custom methods and `_meta` | S1 | S2-draft | Custom methods MUST start with `_`; no custom root fields; `_meta` reserved keys `traceparent`, `tracestate`, `baggage` (SPEC `docs/protocol/v1/extensibility.mdx:37-56`). |

### `session/update` variants

| Variant | v1 | v2 draft | Notes |
|---|---|---|---|
| `user_message_chunk` | S1 | S2-draft | v1: only guaranteed during load replay. v2: agent MUST report every inserted user message. |
| `user_message`, `agent_message`, `agent_thought` (whole-message upserts) | n/a | S2-draft | Upsert keyed by `(type, messageId)`; `content` replaces, omitted leaves unchanged, `null`/`[]` clears. |
| `agent_message_chunk`, `agent_thought_chunk` | S1 | S2-draft | `messageId` optional in v1 (stabilized 2026-06-05), required in v2. |
| `tool_call` | S1 | **removed** (merged into `tool_call_update` upsert) | |
| `tool_call_update` | S1 (patch) | S2-draft (upsert) | Statuses v1: `pending`, `in_progress`, `completed`, `failed`. v2 adds `paused`, `cancelled`. |
| `tool_call_content_chunk` | n/a | S2-draft | Appends one content item. |
| `terminal_update`, `terminal_output_chunk` | n/a | S2-draft | Agent-owned display terminal; base64 byte chunks. |
| `plan` (full replace) | S1 | removed | |
| `plan_update`, `plan_removed` | U1 | `plan_update` S2-draft, `plan_removed` U2 | RFD plan-operations (Draft); v2 plan-variants (Active). |
| `available_commands_update` | S1 | S2-draft | |
| `current_mode_update` | S1 | removed | |
| `config_option_update` | S1 | S2-draft | Completed (session-config-options). |
| `session_info_update` (`title`, `updatedAt`) | S1 | S2-draft | Completed. |
| `usage_update` (`used`, `size`, optional `cost`) | S1 | S2-draft | Completed 2026-06-05. Context-window state, not per-turn tokens. |
| `notice` | S1 (client capability `session.notices`) | S2-draft | Completed 2026-10-09. Live-only, not history. |
| `compaction_update`, `compaction_summary_chunk` | S1 (client capability `session.compaction`) | S2-draft | Completed 2026-10-06. |
| `state_update` | n/a | S2-draft | `running` / `requires_action` / `idle` (+ `stopReason`, `error`, `background[]`). |
| `subagent_update`, `session_message`, `session_message_chunk` | U1 | U2 | RFD subagents, Active. |

Other enums (VERIFIED by `jq`): `StopReason` v1 = `end_turn`, `max_tokens`, `max_turn_requests`, `refusal`, `cancelled`; v2 adds `error` and open `_`-prefixed values. `ToolKind` = `read`, `edit`, `delete`, `move`, `search`, `execute`, `think`, `fetch`, `switch_mode`, `other`. `ToolCallContent` = `content`, `diff`, `terminal` (+ `session` unstable). `PromptResponse` v1 stable has only `stopReason`; `usage` is unstable-only.

### Transport

Only **stdio** (newline-delimited JSON-RPC, no embedded newlines) is defined. Streamable HTTP + WebSocket is an Active RFD (`docs/rfds/streamable-http-websocket-transport.mdx`); a "durable & resumable HTTP/WS transport" RFD is an open PR (#2208). VERIFIED: SPEC `docs/protocol/v1/transports.mdx`.

---

## 3. RFD status

Status is VERIFIED from SPEC `docs/docs.json` navigation groups and `docs/rfds/updates.mdx` (dated lifecycle log); open-PR rows are VERIFIED with `gh pr list -R agentclientprotocol/agent-client-protocol` on 2026-11-09.

### Completed (in stable v1 and the v2 draft)

| RFD | Completed | UX relevance |
|---|---|---|
| message-id | 2026-06-05 | Agent-generated `messageId` on chunks. Explicitly **no** client-generated ids and no id on the v1 prompt request or response. |
| session-usage | 2026-06-05 | `usage_update` context-window meter and cumulative cost. |
| session-list | earlier | Session picker; `session_info_update`. |
| session-info-update | earlier | Title and `updatedAt` pushes. |
| session-resume | earlier | Attach without replay. |
| session-close | earlier | Free agent resources per session. |
| session-delete | 2026-06-05 | |
| session-config-options | earlier | Model/mode/effort selectors; supersedes modes and `set_model`. |
| model-config-category | 2026-06-24 | `model_config` category. |
| boolean-config-option | 2026-07-06 | Toggles; client opts in on v1 with `session.configOptions.boolean`. |
| request-cancellation | 2026-06-29 | `$/cancel_request`. |
| elicitation | 2026-07-24 | Structured questions (form) and URL flows. |
| auth-methods (terminal auth) | 2026-08-20 | |
| logout-method | 2026-05-22 | |
| additional-directories | 2026-06-01 | Extra workspace roots on new/load/resume. |
| tool-call-name | 2026-09-17 | Optional programmatic `name` on tool calls. |
| session-compaction | 2026-10-06 | |
| session-notices | 2026-10-09 | Toast/banner channel that is not conversation history. |
| client-system-prompt | 2026-11-06 | `systemPrompt` on new/load/resume (`append`, optional `override`). |
| meta-propagation, acp-agent-registry, rust-sdk-v1, introduce-rfd-process | earlier | |

### Active (maintainer focus)

| RFD | Notes |
|---|---|
| v2 overview and its 13 children: prompt, enum-variant-extension, client-filesystem-terminal-capabilities, plan-variants, tool-call-updates, message-updates, permission-requests, required-session-methods, session-resume-replay, diff-file-states, terminal-output, meta-patches, background-work | These define the v2 draft's breaking changes and are the scope of the v2 Preview. |
| streamable-http-websocket-transport | Remote transport; does not block v2. |
| subagents | Child sessions on the same connection. Shipped by both adapters behind client capability `subagents`. |
| mcp-over-acp | Client-supplied tools over the existing connection. |

### Draft (in unstable schema)

session-fork, proxy-chains, custom-llm-endpoint (providers), plan-operations, end-turn-token-usage, get-auth-state, session-status, session-set-title, session-archive.

### Removed

Agent Telemetry Export (2026-07-02; "use standard OTel env vars"), Next Edit Suggestions (2026-09-30), Delete in Diff (2026-10-29), unstable `session/set_model` (2026-06-01).

### In-flight RFD pull requests that matter for chat UX (all OPEN, none merged)

| PR | Title | What it would give a client |
|---|---|---|
| #1261 | steer and queue prompts sent while the Agent is working | v2-only `delivery: "steer" \| "queue"` field on `session/prompt` plus `session.prompt.delivery.{steer,queue}` capability. Replaced an earlier `session/inject` design. |
| #2255 | propose session steering | v2-only implicit steering: any prompt while not idle is inserted at the next safe point; queueing stays in the client. Competes with #1261. |
| #2043 | feat(unstable-v2): add session injection schema | Schema for `session/inject` / `revoke_inject` / `replace_inject`; superseded in spirit by #1261's current revision. |
| #2407 | propose a replay_complete signal for session load | One update after replay and before the response, so the client can show history before the agent finishes starting. Carries the measured Claude numbers quoted in section 4(e). |
| #2394 | define a session's last change, and how to catch up to it | Makes `updatedAt` mean "retained conversation changed", so a client with a persisted copy can skip re-replay. Notes both adapters currently bump it on a no-op reconnect. |
| #2753 | Read session text without activating the session | `session/read_text`: paginated history read with no lifecycle side effects. |
| #2323 | session list extensions (limit, order, row metadata, change hints) | |
| #2114 | session cursor RFD | Incremental replay cursor (v2 `replayFrom` currently only defines `start`). |
| #1321 | Session Rewind (truncate and edit history, v2) | Edit/retry a prior user message. |
| #533 | Multi-Client Session Attach | |
| #2485 | define per-turn, in-turn, subagent, and cumulative token usage | |
| #2404 | prompt_suggestions session update | Standardizes what the Claude adapter ships under `_meta`. |
| #2755 / #2756 / #2772 / #2773 | error classification; v2 failure entries | Typed errors and failure history. |
| #2208 | durable & resumable HTTP/WS transport | |
| #1983 | free-form feedback with tool-call permission response | "Reject, and tell it why". |
| #582 | dynamic MCP server updates in live sessions | |

---

## 4. UX consequences: what the protocol guarantees vs. what the client must infer

### (a) Optimistic user message, and reconciling with echoed `user_message_chunk`

**v1 guarantees**
- `session/load` "MUST replay the entire conversation" as `session/update`, including user messages as `user_message_chunk` (VERIFIED: SPEC `docs/protocol/v1/session-setup.mdx:136`, example at `:149-166`).
- `messageId` on chunks is optional ("The Agent MAY include an opaque, unique `messageId`"; same id = same message; changed id = new message) (VERIFIED: SPEC `docs/protocol/v1/prompt-turn.mdx:190-192`).
- The message-id RFD is explicit: "This RFD does not add a message ID to the `session/prompt` request or response. Clients do not create protocol message IDs." (VERIFIED: SPEC `docs/rfds/message-id.mdx`, "Proposed Structure").

**v1: client must infer**
- Whether the live user message is echoed at all. The spec never requires an echo during a live turn. The Claude adapter deliberately drops it: "The turn's own echo is then dropped from the feed (the client already shows it)" (VERIFIED: CLAUDE `src/acp-agent.ts:8170-8175`). Codex ignores `userMessage` items in live events (VERIFIED: CODEX `src/CodexEventHandler.ts:958-970`, `:1016-1022`).
- The identity of the optimistic bubble. `PromptRequest` has only `sessionId`, `prompt`, `_meta` and `PromptResponse` only `stopReason` (VERIFIED: `jq` on `schema/v1/schema.json`). The id the Claude adapter will later replay for that message is an adapter-minted SDK uuid the client never sees live (VERIFIED: CLAUDE `src/acp-agent.ts:4044-4046` mints `promptUuid`; `:14062-14089` `messageIdForGrouping` uses the SDK uuid for user messages). So on a later `session/load`, a v1 client can only match its stored optimistic message to the replayed one **by position and content**.
- That replayed content equals what was sent. Adapters transform prompts (mentions, embedded resources, slash commands). The Claude adapter strips local-command metadata and hides some meta messages on replay (VERIFIED: CLAUDE `src/acp-agent.ts:2302-2460`).

**v2 draft guarantees**
- "Acceptance means insertion." The prompt response returns the agent-generated `messageId`; the agent MUST also report the inserted message via `user_message` (full content) or `user_message_chunk` with that same id; updates may arrive before or after the response; "Clients must not infer the association from content or arrival order" (VERIFIED: SPEC `docs/protocol/v2/prompt-lifecycle.mdx:242-291`).
- On replay, "A retained user message inserted by `session/prompt` MUST use the ID returned in that prompt response, including after reconnecting or restarting the Agent" (VERIFIED: SPEC `docs/protocol/v2/session-setup.mdx:282-284`, under "Replay contents" at `:215`).

**v2: client must still handle**
- The window between send and response (the optimistic bubble has no protocol id until the response; correlation is by JSON-RPC request id).
- Loss: "If the response is lost, the submission's outcome remains uncertain ... a retry can create another submission" (same section). No idempotency key exists; client-generated correlation tokens were considered and rejected in the v2 prompt RFD FAQ (VERIFIED: SPEC `docs/rfds/v2/prompt.mdx:403-421` headings).
- Other entity ids (agent messages, tool calls) are *not* guaranteed stable across replay: "Clients MUST NOT rely on them to deduplicate replay against previously received history. A full replay is authoritative: Clients SHOULD rebuild" (VERIFIED: SPEC `docs/protocol/v2/session-setup.mdx:285-292`).

### (b) Knowing a turn has started before the first token

**v1 guarantees**: nothing. The lifecycle is request -> zero or more updates -> response with `stopReason` (VERIFIED: SPEC `docs/protocol/v1/prompt-turn.mdx:35-78`, `:452`). There is no ack, no "running" state, and no required first update.

**v1: client must infer**: "running" = "my `session/prompt` request is outstanding". "Thinking" between send and first chunk is purely local UI. Updates may also arrive outside any turn ("Session updates are not limited to active prompt turns", `prompt-turn.mdx:33`), so "an update arrived" does not imply "a turn is running".

**v2 draft guarantees**: when foreground work starts the agent MUST report `state_update: running` (or `requires_action`); when it ends, MUST report `idle` and SHOULD include `stopReason`; "Consecutive completed turns therefore have an idle boundary" (VERIFIED: SPEC `docs/protocol/v2/prompt-lifecycle.mdx:66-80`, `:929-990`).

**v2: client must still handle**: `running` "reports foreground work, not insertion. It may precede insertion, the prompt response, and user-message updates" (`:297`); the prompt response "has no fixed order relative to other session updates", even relative to the `idle` that ends the work (`:291`). So the client keeps two independent facts per session: (1) is my submission inserted, (2) what is the foreground state. Unknown states must render as generic busy, never as idle.

### (c) Steering / interrupting mid-turn, and queueing

**v1 guarantees**: none. The spec only says "Once a prompt turn completes, the Client may send another `session/prompt`" (VERIFIED: SPEC `docs/protocol/v1/prompt-turn.mdx:584`). A grep of the v1 docs for concurrent/in-flight/queue language finds nothing normative. Behavior of an overlapping prompt is undefined.

**v1: what adapters actually do (client must special-case per agent)**
- Claude: overlapping `session/prompt` is accepted and queued in a per-session `turnQueue`; each request resolves when *its* turn ends (VERIFIED: CLAUDE `src/acp-agent.ts:4098-4131`). Advertised as `agentCapabilities._meta.claudeCode.promptQueueing: true` (`:3083-3086`). Turn hand-off is driven by the CLI's user-message echo (`:8176-8254`).
- Claude and Codex: pre-standard **`_session/steering`** request, advertised as top-level `InitializeResponse._meta.steering.supported: true` (VERIFIED: CLAUDE `src/acp-agent.ts:710`, `:3148-3150`, `:4549-4625`; CODEX `docs/steering.md`, `src/CodexAcpServer.ts:955-957`). Outcomes: `injected`, `startedNewTurn`, `promptRequired` (opt-in via `_meta.steering.idleBehavior: "promptRequired"`), and on Codex `failed`. The steered message's output arrives as ordinary updates; the response is not a turn result.
- Claude steering semantics: the injected message is pushed at priority `now`, which **aborts the current generation** (or backgrounds a running foreground tool) and runs the steer as a second cycle within the same ACP turn; while a permission or elicitation is open it is delivered `later` instead so it does not cancel that request (VERIFIED: CLAUDE `src/acp-agent.ts:4520-4548`, `:4602-4603`). Steering is v1-only on the Claude adapter for now (CLAUDE `README.md:145`).
- Codex: overlapping v1 prompts wait in a per-session turn-start FIFO (VERIFIED: CODEX `src/CodexAcpServer.ts:439-453`, `:534`, `:4853-4877`); steering maps to app-server `turn/steer` (`src/CodexAcpClient.ts:1560`).
- Other agents: UNVERIFIED; assume neither queueing nor steering unless advertised.

**v2 draft guarantees**: "Clients MAY send `session/prompt` in any state ... The Agent either inserts the prompt right away or holds it and inserts it later. Held prompts are inserted in the order they were received. What inserting during active work means, such as steering the current turn or starting the next one, is up to the Agent. Agents MUST NOT reject a prompt only because foreground work is active." A held prompt's response stays pending and can be withdrawn with `$/cancel_request` (VERIFIED: SPEC `docs/protocol/v2/prompt-lifecycle.mdx:299-303`).

**v2: client must still infer**: whether a mid-turn prompt will steer or queue. That is exactly what open PRs #1261 and #2255 are fighting over. Until one lands, "steer vs queue" intent cannot be expressed in standard ACP.

### (d) Cancel latency and what may still arrive after cancel

**v1 guarantees**
- `session/cancel` is a notification (no response). Agent SHOULD stop model requests and tools "as soon as possible", then MUST answer the original `session/prompt` with `stopReason: cancelled`; it MUST NOT surface the abort as an error (VERIFIED: SPEC `docs/protocol/v1/prompt-turn.mdx:547-580`).
- The agent "MAY send `session/update` notifications with content or tool call updates after receiving the `session/cancel` notification, but it MUST ensure that it does so before responding to the `session/prompt` request." The client "SHOULD still accept tool call updates received after sending `session/cancel`" (`:578-580`).
- Client obligations: SHOULD preemptively mark all non-finished tool calls as cancelled locally; MUST answer all pending `session/request_permission` with outcome `cancelled` (`:561-563`).

**v1: client must infer**
- **No latency bound.** Confirmation is the prompt response, whenever it comes. The Claude adapter awaits the SDK `interrupt()` and settles the active turn at the interrupted `result` (or a backstop) (VERIFIED: CLAUDE `src/acp-agent.ts:8960-8965`, `:9014-9120`, `:9312`). UI must flip to "stopped" optimistically.
- v1 has no `cancelled` tool status (`pending`, `in_progress`, `completed`, `failed` only), so "cancelled" on a tool card is a client-local state, or arrives as `failed`.
- Stragglers do happen in practice: Claude adapter issue #1200 (OPEN) documents an `available_commands_update` arriving 70-120 ms after the cancelled prompt response, failing TCK check ACP-CANCEL-002 (VERIFIED via `gh issue view 1200 -R agentclientprotocol/claude-agent-acp`). Clients must route late updates by entity id and never by "which turn is active".
- **Cancel scope includes queued prompts** on both adapters: Claude resolves every queued, not-yet-active turn received before the cancel as `cancelled` immediately and drops them from the CLI queue (VERIFIED: CLAUDE `src/acp-agent.ts:9025-9041`, `:9106-9123`); a prompt that arrives *after* the cancel is kept. Codex drops queued v2 prompts and interrupts the running turn (VERIFIED: CODEX `src/CodexAcpServer.ts:5742-5774`).
- Background work survives: Claude background commands and (on the subagent RFD form) background subagents keep running after a parent cancel (VERIFIED: CLAUDE `README.md` "Subagent sessions"; `src/acp-agent.ts:12275-12285`).

**v2 draft guarantees**: cancel covers foreground work and un-inserted prompts received before it (those get `-32800`); the agent MUST send `state_update: idle, stopReason: cancelled` after all pending updates; work in the latest `background` list is not covered; the client's preemptive "cancelled" marks are a display hint that the agent's final statuses replace; per-item `tool_call/cancel` and `terminal/cancel` exist for background work (VERIFIED: SPEC `docs/protocol/v2/prompt-lifecycle.mdx:1023-1075`; `docs/protocol/v2/tool-calls.mdx:425-460`, `:584-604`).

### (e) Restoring a session instantly

**v1 guarantees**
- `session/load`: replay everything as notifications, then respond ("When all the conversation entries have been streamed to the Client, the Agent MUST respond") (VERIFIED: SPEC `docs/protocol/v1/session-setup.mdx:136`, `:191`). The response is the *only* end-of-replay signal.
- `session/resume`: "the Agent MUST NOT replay the conversation history ... restores the session context, reconnects to the requested MCP servers, and returns once the session is ready" (`:258`).
- `session/list`: rows with optional `title` and `updatedAt`; cursor pagination (VERIFIED: SPEC `docs/protocol/v1/session-list.mdx`).

**v1: client must infer / work around**
- When replay is "done enough to show". Per RFD PR #2407: "For claude-agent-acp the answer comes 0.7-1.9 s after the replay on an idle machine and 4-9 s on a cold start or a loaded machine" (VERIFIED quote via `gh pr view 2407`). The Claude adapter replays the transcript from disk *in parallel with* CLI startup precisely because "The CLI start takes most of the load time" (VERIFIED: CLAUDE `src/acp-agent.ts:3165-3228`). A private end-of-replay marker exists only for JetBrains AIR (`session_info_update._meta.jetbrains.air.loadReplayEnd`, CLAUDE PR #1361, merged 2026-10-19).
- Whether the session changed since last seen. `updatedAt` is unreliable for this: both adapters move it on a no-op reconnect (VERIFIED claim in RFD PR #2394 body; not independently reproduced).
- Dedup. v1 ids are optional and v2 says non-user entity ids are not stable across replay, so a replay must **replace** the session's entities, not merge.
- Replay fidelity and scale: codex-acp has open issues for large-session replay stalls and partial history (#539 "Large session loads stall in quadratic JSON-RPC line buffering", #516, #695) (VERIFIED titles via `gh issue list`).
- `session/resume` is not free: on the Claude adapter it is `getOrCreateSession` -> `createSession({resume})`, which spawns a CLI and awaits init exactly like `session/new`, unless the session is still live in the same adapter process with an identical fingerprint, in which case it returns immediately (VERIFIED: CLAUDE `src/acp-agent.ts:3269-3275`, `:11887-11944`).

**v2 draft**: one method. `session/resume` without `replayFrom` = attach, no replay; with `replayFrom: {type:"start"}` = full replay; either way the agent MUST send a current `state_update` (and SHOULD send `session_info_update`) before responding, and the response carries `configOptions` and `availableCommands`. Only the `start` cursor is defined, so there is still no incremental catch-up (VERIFIED: SPEC `docs/protocol/v2/session-setup.mdx:140-360`).

**Bottom line**: instant switching is impossible via ACP alone on either version. The client must own a durable local transcript and paint from it.

### (f) Tool call lifecycle, streaming output, diffs, terminals

**v1 guarantees**
- `tool_call` creates (`toolCallId`, `title` required; optional `name`, `kind`, `status`, `content`, `locations`, `rawInput`, `rawOutput`); `tool_call_update` patches only the fields present (VERIFIED: SPEC `docs/protocol/v1/tool-calls.mdx:12-135`; `jq` on `ToolCall`/`ToolCallUpdate`).
- Statuses and their meaning: `pending` = "input is either streaming or awaiting approval"; `in_progress`; `completed`; `failed` (`tool-calls.mdx:254-272`).
- Content types: `content` (any content block), `diff` (`path`, `oldText?`, `newText`: full-text replacement, no patch, no delete/move), `terminal` (`terminalId` of a **client-created** terminal via `terminal/create`).
- `locations` for "follow the agent".

**v1: client must infer**
- Streaming granularity of tool output. v1 has no append primitive: `tool_call_update.content` *replaces* the collection, so naive agents resend everything. Both adapters instead stream command output through a non-standard `_meta` channel on `tool_call_update`: `terminal_info`, `terminal_output` (or `terminal_output_delta`), `terminal_exit`, enabled when the client advertises `clientCapabilities._meta.terminal_output: true` (or `terminal_output_delta: true`) (VERIFIED: CLAUDE `src/tool-calls/client-capabilities.ts:20-45`, `src/tool-calls/renderer.ts:99-131`, `:407-469`; CODEX `src/AcpV2SessionUpdate.ts:23-37`, `:213-264`, `src/ToolCallReports.ts:9`).
- Whether the tool is created before its input is complete. Claude emits `tool_call` at `content_block_start` with empty input, then refines `rawInput`/title as the JSON input streams, at top-level-comma boundaries (VERIFIED: CLAUDE `src/acp-agent.ts:14766-14830`).
- Orphans. Nothing in v1 forces a terminal status when a turn ends by cancel or crash. Both adapters carry dedicated "close open tool calls" code (CLAUDE `src/acp-agent.ts:5537-5600`; CODEX `finishOutstandingToolCalls`, `failUnfinishedToolCalls` at `src/CodexAcpServer.ts:1812`, `:5620`), which is evidence that clients see dangling `in_progress` tools often enough to matter. The client should finalize any non-terminal tool call when the prompt response arrives.
- Sub-agent nesting: no stable field. Claude stamps `_meta.claudeCode.parentToolUseId` on updates of nested tool calls (VERIFIED: CLAUDE `src/acp-agent.ts:2056-2062`), or uses the unstable subagents RFD (`subagent_update` + child session ids) when the client advertises `clientCapabilities.subagents`.

**v2 draft**: single `tool_call_update` upsert (a chunk for an unseen id creates the entity; clients MUST NOT drop it); `tool_call_content_chunk` appends; display-only agent terminals with `terminal_update` and base64 `terminal_output_chunk`; statuses add `paused` and `cancelled`; structured diffs (add/delete/modify/move/copy, binary, optional `git_patch`); first update fixes timeline position, later updates patch in place "including after the session reports idle" (VERIFIED: SPEC `docs/protocol/v2/prompt-lifecycle.mdx:46-64`, `:726-800`; `docs/announcements/acp-v2-draft.mdx`).

### (g) Permission prompts without blocking the stream

**Guarantees (v1)**: `session/request_permission` is an ordinary agent->client JSON-RPC *request* carrying the tool call and options; the client answers `selected(optionId)` or `cancelled`. Nothing ties it to stream ordering, so notifications keep flowing while it is outstanding, and several can be open at once (the cancellation doc's own example shows a terminal request and a permission request concurrently) (VERIFIED: SPEC `docs/protocol/v1/tool-calls.mdx:136-252`; `docs/protocol/v1/cancellation.mdx:44-70`). On cancel the client MUST answer every pending one with `cancelled`.

**Client must infer (v1)**
- That the turn is "waiting on the user". There is no state for it; derive it as "at least one permission/elicitation request outstanding".
- Dismissal. The agent may withdraw a request with `$/cancel_request` (stable but optional); a client that ignores it leaves a dead prompt on screen.
- Where the prompt belongs in the timeline. The request references `toolCall.toolCallId`; the Claude adapter makes sure the tool call has been emitted first (`ensureToolCallEmitted`, CLAUDE `src/acp-agent.ts:10651`), but the spec does not require that ordering.
- A permission answer blocks only that tool, not the session: in the Claude adapter each `canUseTool` callback awaits its own request (CLAUDE `src/acp-agent.ts:10537`, `:10890`), so parallel tools and subagents continue.

**Structured questions**: `elicitation/create` (stable) is the general surface: form mode with a restricted JSON schema (string/number/integer/boolean/enum/multi-select) or URL mode; client advertises each mode; client MUST let the user review/modify before sending (VERIFIED: SPEC `docs/protocol/v1/elicitation.mdx:15-165`). The Claude adapter renders `AskUserQuestion` as a form elicitation and **disables the tool entirely** if the client does not advertise form elicitation (VERIFIED: CLAUDE `src/acp-agent.ts:12164-12175`, `:11207`). Codex "async questions" are non-blocking form elicitations whose answers are steered into the asking turn (CODEX `docs/async-questions.md`, `README.md`).

**v2 draft**: `requires_action` aggregate state (SHOULD, only when the whole foreground workflow is blocked); permission requests get their own required `title`, optional `description`, and an extensible `subject` (`tool_call`, `command`, or none); on cancel the agent SHOULD withdraw each with `$/cancel_request` (VERIFIED: SPEC `docs/protocol/v2/prompt-lifecycle.mdx:959-975`, `:1041`; `docs/rfds/v2/permission-requests.mdx`).

### (h) Session creation cost, warm sessions, and changing cwd/mode/model later

**Guarantees**
- `session/new` requires an absolute `cwd`, which "MUST be used for the session regardless of where the Agent subprocess was spawned" (VERIFIED: SPEC `docs/protocol/v1/session-setup.mdx:436-443`). There is no method to change a session's cwd. On load/resume the request cwd is expected to match the session's cwd (`:360`).
- Mode and config options (including model) "can be changed at any point during a session, whether the Agent is idle or generating a response"; `session/set_config_option` returns the complete option list (VERIFIED: SPEC `docs/protocol/v1/session-config-options.mdx:255`, `:304`; `docs/protocol/v1/session-modes.mdx:79`).
- One connection can hold many sessions; nothing forbids creating a session ahead of use.
- v2: "Agents SHOULD put initial state in the `session/new` response ... rather than sending notifications before it"; clients MAY buffer or drop notifications for a not-yet-known `sessionId` (VERIFIED: SPEC `docs/protocol/v2/session-setup.mdx:96-104`, "Initial session state").

**Client must infer**
- Cost (see section 5). It is not bounded or advertised.
- Whether an unprompted session persists. v2: agents "may not store a session that was never prompted" (SPEC `docs/protocol/v2/session-setup.mdx:47`). Claude: a never-prompted session has no stored conversation; `session/fork` of one fails (CLAUDE issue #1497, OPEN), and the adapter needs a special `recreateLiveSession` path for it (CLAUDE `src/acp-agent.ts:11932-11937`).
- Whether a warm session can be retargeted. On the Claude adapter a `resume`/`load` with different session-defining params (cwd, MCP servers, skills) is a **teardown and respawn** of the CLI (VERIFIED: CLAUDE `src/acp-agent.ts:11904-11919`). A user in CLAUDE issue #567 reports that a pool of pre-spawned sessions was ineffective for a one-worktree-per-session app for exactly this reason (VERIFIED via `gh issue view 567`).
- Config change latency. `set_config_option` is a request the adapter may service slowly: CLAUDE issue #886 (CLOSED) documents model switches taking 10-20 s in v0.59 due to a blocking context-window fetch. The picker must be optimistic and reconcile with the response and later `config_option_update`.
- MCP servers are fixed per session-setup call; live MCP changes are only an open RFD (#582).

### (i) Titles, usage, context-window indicators

**Guarantees (stable v1)**
- `session_info_update` with nullable `title` and `updatedAt`; `SessionInfo.title` in `session/list` (VERIFIED: SPEC `docs/protocol/v1/session-list.mdx:148-217`).
- `usage_update`: required `used` and `size` token counts for the *current session context*, optional `cost {amount, currency}` (VERIFIED: SPEC `docs/protocol/v1/prompt-turn.mdx:425-448`).

**Client must infer**
- *When* a title appears. Entirely agent-driven: Claude Code generates it "a few seconds in" after reading the first prompt and the adapter fetches and publishes it (VERIFIED: CLAUDE `src/session-titles.ts:1-20`). There is no stable way for the client to set a title (`session/set_title` is a Draft RFD, shipped by Codex; Claude uses a private `_session/rename`). Show a client-derived provisional title immediately and replace it.
- Per-turn token usage. Not stable: `PromptResponse.usage` is unstable-only (RFD end-turn-token-usage, Draft) and adapters disagree on semantics (CODEX issue #447: reports only the final request's usage).
- Model attribution and rate limits: only via adapter `_meta` (Claude attaches `_claude/model` and `_claude/rateLimit` to `usage_update._meta`; CLAUDE `src/acp-agent.ts:4990-5015`).
- Auto-compact threshold: not exposed (CLAUDE issue #1402, OPEN). Compaction itself is visible via `compaction_update` if the client advertises `session.compaction`.
- Usage is not guaranteed to be replayed on load (v2 says agents "need not replay" historical usage and MAY send a current one before responding).

---

## 5. Adapter behavior and startup/latency findings

### 5.1 Claude adapter (`@agentclientprotocol/claude-agent-acp` 0.102.0)

**Process model.** One adapter process (Node, or a standalone binary bundling the Claude CLI) serves many sessions. **Each session is its own Claude Code CLI child process**, created by the Agent SDK's `query()` with a streaming-input `Pushable` (VERIFIED: `src/acp-agent.ts:12066`, `:12487-12497`: "`query()` spawns the CLI at once").

**`initialize`.** Cheap. Returns `protocolVersion: 1` and capabilities without waiting on the CLI; an auth probe runs in the background and pushes `_auth/status_update` later (VERIFIED: `src/acp-agent.ts:2945-2957`, `:3080-3152`). Advertises: `loadSession`, `sessionCapabilities {additionalDirectories, close, delete, fork, list, resume, systemPrompt}`, `promptCapabilities {image, embeddedContext}`, `mcpCapabilities {http, sse}`, `auth.logout`, `providers`, plus `_meta.claudeCode.promptQueueing` and top-level `_meta.steering.supported`.

**`session/new` awaits, in order** (VERIFIED: `src/acp-agent.ts:11998-12866`; phase names are the adapter's own timing log labels from `src/session-timing.ts`):
1. `validate-cwd`: `fs.stat(cwd)`.
2. `settings`: `SettingsManager.initialize()` (reads user/project/local settings).
3. `prepare-query`: resolve CLI path, build options, call `query()` (process spawn).
4. `sdk-initialize`: `await q.initializationResult()` (the CLI cold start; the dominant cost).
5. `models`: `getAvailableModels(...)`.
6. `modes`: `sessionModes.initialize(...)`.
7. `agents`: `discoverCustomAgents(q)` (control request).
8. `ultracode`: `readUltracodeSettings(q)` (control request), then possibly `q.applyFlagSettings(...)`.
9. `register`.

After the response, `available_commands_update`, subagent states and MCP auth are kicked off from a `setTimeout(0)` (`:11382-11389`), so slash commands arrive *after* `session/new` returns and can straggle (issue #1200).

The adapter logs every phase as `[session/create] sessionId=... phase=... durationMs=... totalMs=...` on stderr, so a client can measure this without instrumentation.

**Latency evidence from the tracker** (VERIFIED with `gh issue view`):
- #567 (CLOSED, 2026-04): "Why does newSession take 11+ seconds?", on every request; a later comment reports "~3s connection overhead" on newer builds, and that a pre-spawned pool did not help a worktree-per-session app.
- #886 (CLOSED, 2026-07): `session/new` and model switches took 10-20 s in v0.59 after a blocking `fetchContextWindowSize()` was introduced.
- #1192 (CLOSED, 2026-09): a non-awaited background `getContextUsage()` after `session/new` made the CLI issue ~15 `count_tokens` requests, and **the client's first `session/prompt` or `set_config_option` waited behind it**.
- #880 (CLOSED): a failed model request during session init caused ~100 s of retries.
- #1269 (OPEN): the PostToolUse hook awaits `session/update` delivery; **a client that stops reading the agent's stdout outside a prompt stalls the session** (600 s per tool).

**Prompt path.** v1 `prompt()` resolves at turn end (`:3903-3927`). `startTurn` pushes the user message onto the streaming input and enqueues a `Turn`; the persistent consumer activates the turn when the CLI echoes the user message (the adapter passes `--replay-user-messages`, `:12331-12334`) and settles it on the CLI `result`/idle (`:4098-4131`, `:5109-5126`, `:8176-8254`). Before queueing it may await a provider update, a sign-out respawn, a subscription guard, and plan publication.

**Streaming granularity.** `includePartialMessages: true` is forced (`:12302`). Text and thinking arrive as per-delta `agent_message_chunk` / `agent_thought_chunk`; tool input streams as incremental `tool_call_update` refinements; `message_start`/`message_delta`/`message_stop`/`ping` emit nothing (`:14735-14870`). Assistant `messageId` is the Anthropic API message id, identical live and on replay; user `messageId` is the SDK uuid (`:14062-14089`). So for **assistant** messages a v1 client *can* reconcile live and replayed content by id on this adapter; for **user** messages it cannot, because the id is never sent live.

**Tool calls and plans.** `TodoWrite` and `Task*` tools become `plan` updates rather than tool calls (`:14109-14128`). Bash output streams via `_meta.terminal_*` when negotiated. Nested subagent tool calls carry `_meta.claudeCode.parentToolUseId`, or become child sessions under the subagents RFD.

**Cancel.** Marks the session cancelled, withdraws open permission/elicitation inputs, resolves queued turns as `cancelled` immediately, drops them from the CLI queue (`cancelAsyncMessage`), then awaits `query.interrupt()`; the active turn settles on the interrupted result or a backstop (`:8960-8965`, `:9014-9123`, `:9312`). `$/cancel_request` for the prompt request is routed to the same path (`:14873-14890`).

**Load / resume / list / fork.**
- `load`: reads the transcript from disk and replays it **while** the CLI starts, then waits for both (`:3165-3228`, `:3277-3295`).
- `resume`: same CLI start, no replay; instant only if already live with the same fingerprint (`:11904-11912`).
- `list`: Agent SDK `listSessions`, page size 1000, `updatedAt` from transcript mtime, title from the stored summary (`:2077`, `:3302-3335`).
- `fork` (unstable): copies the transcript to a new session id and starts it (`:3230-3260`).

**`_meta` extensions clients use** (all VERIFIED in `src/acp-agent.ts` unless noted):
- Request: `session/new._meta.claudeCode.options` (Agent SDK `Options` pass-through, with `cwd`, `includePartialMessages`, `permissionMode`, `canUseTool`, executable overridden; `:1886-1915`), `_meta.claudeCode.emitRawSDKMessages` (raw SDK messages as `_claude/sdkMessage` ext notifications), `_meta.systemPrompt`, `_meta.disableBuiltInTools`, `_meta.additionalRoots` (legacy).
- Client capabilities: `_meta.terminal_output`, `_meta.terminal_output_delta`, `_meta["terminal-auth"]`, `auth._meta.gateway`.
- Updates: `_meta.claudeCode.parentToolUseId`, `_meta.terminal_info` / `terminal_output` / `terminal_exit`, `usage_update._meta["_claude/model"]` and `["_claude/rateLimit"]`, `session_info_update._meta.promptSuggestions` (predicted next prompt; currently AIR-gated, issue #1395).
- Methods: `_session/steering`, `_session/async_task/stop`, `_session/rename`, `_session/archive`, `_session/list/subscribe`, `_session/batch_get`, `_auth/status_update`.
- A large JetBrains AIR extension family under `_meta.jetbrains.air` (`docs/air-extensions.md`).

**v2 mode.** Negotiated when the client requests protocol version >= 2; `CLAUDE_AGENT_ACP_PROTOCOL=v1|v2|auto` pins it. Serves the v2 session baseline, plans, notices, compaction, display terminals, permissions, elicitation, subagents. "Steering and the AIR extensions are v1 only for now." (VERIFIED: `README.md:131-145`.)

### 5.2 Codex adapter (`@agentclientprotocol/codex-acp` 2.6.0)

**Process model.** One adapter process, **one shared Codex app-server process** for all sessions; a session is a Codex thread. A supervisor restarts the app-server after a crash (VERIFIED: `README.md:7`; `src/app-server-recovery/`).

**`initialize`.** Awaits app-server startup and the app-server's own `initialize` (VERIFIED: `src/CodexAcpServer.ts:809-815`). So the process cold start is paid once, at connect, not per session. Advertises `loadSession`, `sessionCapabilities {resume, list, close, delete, fork, title, additionalDirectories, subagents, systemPrompt}`, `mcpCapabilities {http: true, sse: false}`, top-level `_meta.steering.supported`.

**`session/new` awaits** (VERIFIED: `src/CodexAcpServer.ts:2189-2212`, `:1282-1470`; `src/CodexAcpClient.ts:860-893`): authorization check (account read), `refreshSkills`, session config build, app-server `thread/start`, then `fetchAvailableModels()` (paginated `model/list`, `:1569-1581`, re-fetched per call). **It does not wait for MCP servers** unless the client passes `_meta.mcpStartupAwaitTimeoutMs` (VERIFIED: `docs/mcp-startup-await-timeout.md`); failures surface later as a synthetic failed `tool_call` titled `mcp__<server>__startup`. No per-session process spawn. No latency complaints about `session/new` were found in the codex-acp tracker (searched "slow OR latency OR startup OR seconds OR hang"); the latency issues there are about large-history replay.

**Prompt path.** v1 prompts acquire a per-session turn-start reservation and wait behind earlier ones (`:4853-4877`, `:439-453`). Turn start is app-server `turn/start`; the v1 response resolves on `turn/completed`.

**Streaming granularity.** `item/agentMessage/delta` -> `agent_message_chunk`; `item/reasoning/summaryTextDelta` and `textDelta` -> thought chunks; `item/started` / `item/completed` -> tool call create/finish; `item/commandExecution/outputDelta` -> streamed terminal output (via `_meta.terminal_*` on v1, `terminal_output_chunk` on v2); `item/plan/delta` and `turn/plan/updated` -> plan; `thread/tokenUsage/updated` -> `usage_update`; `thread/name/updated` -> `session_info_update` (VERIFIED: `src/CodexEventHandler.ts:577-724`, `:1358`). Live `userMessage` items are ignored on v1 (`:958-970`).

**Cancel.** Flags the active prompt, aborts outbound permission/elicitation requests synchronously, drops queued v2 prompts, then `turn/interrupt`; the prompt completes on the app-server's `turn/completed` (`:5742-5774`).

**Load / resume / list / fork / title.** `load` reattaches the thread, streams paged thread history as updates, then answers (`:1880-1958`); it also waits up to a timeout for an in-flight title generation to settle before answering. `resume` reattaches without replay. v2 `resume` with `replayFrom.type == "start"` shares the load path; other cursor types are rejected. `session/set_title` (unstable) is implemented on v1 and v2 (`docs/session-title.md`).

**Known problems relevant to feel** (VERIFIED titles via `gh issue list -R agentclientprotocol/codex-acp`): #539 large session loads stall in quadratic line buffering; #516 large sessions replay only partial history; #695 history replay sends screenshots as base64 markdown; #600 `session/prompt` never resolves when the app-server exits mid-turn; #672 a `send_message` after `session/resume` blocks the prompt for 10 minutes.

### 5.3 Other agents with native ACP

From the registry's generated capability matrix (VERIFIED: `agentclientprotocol/registry/.protocol-matrix/latest.md`, generated 2026-11-09, 41 agents probed, 40 initialize OK) and launch args in each `agent.json`:

| Agent | Launch | Advertised session capabilities |
|---|---|---|
| Gemini CLI 0.67.0 | `npx @google/gemini-cli --acp` | `loadSession` only |
| OpenCode 2.2.6 | `opencode acp` | `loadSession`, list, fork, resume |
| Cursor 2026.11.05 | `cursor-agent acp` | `loadSession`, list |
| Goose 1.57.0 | `goose acp` | `loadSession`, list |
| GitHub Copilot CLI 1.0.107 | `npx @github/copilot --acp` | `loadSession`, list |
| Claude adapter 0.101.0 | binary or npx | `loadSession`, list, fork, resume |
| Codex adapter 2.6.0 | npx | `loadSession`, list, fork, resume |

Across all 41: `session/list` works on 30, `session/resume` on 17, `session/fork` on 13. So **`session/load` is the only restore path a client can rely on broadly; `resume` is a minority feature.** Per-agent streaming, queueing and cancel behavior for these five was not read from source: UNVERIFIED.

---

## 6. SDK landscape

### Official SDKs (VERIFIED via `gh api .../releases` on 2026-11-09 and SPEC `docs/libraries/*.mdx`)

| SDK | Latest | Notes |
|---|---|---|
| Rust `agent-client-protocol` | v5.1.0 (2026-11-09) | Reference implementation; powers Zed. Workspace also ships `-http`, `-conductor` (proxy chains), `-rmcp`, `-polyfill`, a trace viewer, and v2 examples. 1.0 reached 2026-06-25. |
| TypeScript `@agentclientprotocol/sdk` | v2.5.0 (2026-11-09) | Used by both adapters. Entry points: default (stable v1), `/unstable`, `/experimental/v2`, `/experimental/v2/unstable`. Fluent `agent()` / `client()` API; old `AgentSideConnection` / `ClientSideConnection` deprecated. |
| Python `agent-client-protocol` | 1.0.0 (2026-11-09) | Pydantic models, async base classes. |
| Kotlin `kotlin-sdk` | v0.34.0 (2026-10-21) | JVM only; pre-1.0; 20 open issues. |
| Java `java-sdk` | v0.80.0 (2026-10-08) | Pre-1.0; Spring AI examples. |
| TCK `acp-tck` | n/a | Drives an agent over stdio and reports requirement pass/fail; useful for checking an agent, not a client. |

### Swift (no official SDK)

SPEC `docs/libraries/community.mdx` lists three Swift libraries; GitHub search finds more. Metadata VERIFIED via `gh api` on 2026-11-09; none were cloned or built, so API quality is UNVERIFIED.

| Library | Stars | Last push | Tools / platforms | License | Notes |
|---|---|---|---|---|---|
| `aptove/swift-sdk` | 12 | 2026-11-09 | Swift 6.0, macOS 12+/iOS 15+ | Apache-2.0 | Self-described port of the Kotlin SDK v0.33.0. Modules `ACPModel`, `ACP` (stdio), `ACPHTTP` (WebSocket). README mentions a low-level protocol actor "for V2 sessions". Release v0.2.3. Most actively maintained. |
| `wiedymi/swift-acp` | 32 | 2026-07-24 | Swift 5.9, macOS 12+/iOS 15+ | MIT | Built for Aizen (macOS worktree + agents app). Actor-based, `AsyncStream` updates, stable `session/list`, boolean config options, usage updates, fs/terminal delegates, `ACPRegistry` module. Stale since July, so predates the v2 draft and several stabilizations (elicitation, compaction, notices, system prompt). |
| `Cocoanetics/SwiftACP` | 3 | 2026-11-04 | Swift 6.1, macOS 14+/iOS 15+ | BSD-2-Clause | Client, server and a daemon client in one module; depends on the author's `JSONFoundation` and `SwiftMCP`; ships `acpx` CLI and `acpxd` daemon. |
| `Orcaryn/agent-client-protocol-swift` | 0 | 2026-07-24 | Swift 6.2, macOS 13+ | Apache-2.0 | "Type-safe `Codable` and `Sendable` models for stable ACP v1"; actor runtimes; `ACPProcess` uses `swift-subprocess`; redacted wire inspection. Built for Chapeta. No adoption signal. |
| `rebornix/acp-swift-sdk` | 7 | 2026-02-07 | n/a | none | Extracted from the Agmente iOS client; stale. |
| `RayZhao1998/acp-swift-sdk` | 4 | 2025-12-28 | n/a | n/a | Stale. |

Related Swift apps that show how others integrate (not libraries): `rebornix/Agmente` (iOS, 548 stars), `vivy-company/aizen`, `851-labs/codevisor`, `chenweigao/perch`, `SilverMarcs/Atrium`.

Assessment: none of these tracks the schema with generated types, and only one mentions v2. Given the spec's release cadence (schema v1 is at 1.30.0; v2 at alpha.14; three stabilizations in the last five weeks), a Swift client is better served by owning a thin JSON-RPC layer and decoding leniently against the published schema than by taking a dependency on a low-adoption community SDK. If a dependency is wanted, `aptove/swift-sdk` is the only one with both Swift 6 and current activity.

---

## 7. Ranked recommendations for a zero-spinner, optimistic client on ACP

Ordered by how much perceived latency or ambiguity each removes.

1. **Make the local store the source of truth for rendering; treat ACP attach as background reconciliation.** Persist every applied update (already necessary for a durable queue). Session switch paints from the store in the same frame. Then call `session/resume` (preferred, no replay) or `session/load` in the background. Rationale: 4(e); `load` is 0.7-9 s behind on Claude and can be partial or stall on Codex.
2. **Never block the composer on `session/new`.** Let the user type and submit into a local "pending session"; hold the first prompt in the durable queue and send it the moment `session/new` returns. Show the optimistic user bubble immediately. Cache `configOptions` / modes / `availableCommands` per (agent, cwd) from the last session and render pickers from the cache, reconciling when the response arrives. Rationale: 5.1 (process spawn + init per session; ~3 s typical, tail much worse).
3. **Pre-warm where the adapter makes it cheap, and only per-cwd.** Spawn the adapter process and run `initialize` at app launch or on project open (for Codex this also boots the app-server, moving the whole cold start off the first-message path). For Claude, a spare `session/new` for the project's cwd is a real win only if new chats reuse that cwd; with one worktree per session, create the worktree first and call `session/new` as early as the worktree path is known (for example when the "new chat" view opens), not at first send. Do not try to retarget a warm session to another cwd: the Claude adapter tears it down and respawns. Rationale: 4(h).
4. **Own the queue; send at most one `session/prompt` per session at a time on v1.** Do not rely on adapter-side queueing (Claude only, undefined elsewhere, and `session/cancel` wipes the adapter's queue). With a client-owned queue, "stop" cancels only the running turn and the rest of the queue survives, which is the behavior users expect from a durable queue. Rationale: 4(c), 4(d).
5. **Implement steering as a capability-gated extension, with a deterministic fallback.** If `initialize` returns `_meta.steering.supported`, send `_session/steering` with `_meta.steering.idleBehavior: "promptRequired"` so that a race with turn end returns the content to the client queue instead of silently starting a detached turn. Otherwise fall back to "cancel, then prompt". Render the steered message optimistically in the timeline at send time; on Claude its output will arrive as a second generation inside the same turn. Rationale: 4(c); 5.1.
6. **Drive all "busy" UI from a client-side state machine, and make cancel optimistic.** v1: `running` = request outstanding; `requires_action` = any permission/elicitation outstanding; `idle` = response received. On stop: flip to idle-looking UI immediately, mark non-terminal tool calls cancelled locally, answer every pending permission with `cancelled`, keep applying late updates by entity id until the prompt response arrives, then finalize any still-open tool calls. Write the state machine so that v2 `state_update` can replace the inferred inputs without changing the views. Rationale: 4(b), 4(d), 4(f).
7. **Key every entity by `(type, id)`, tolerate chunk-before-create, and never use arrival order across entities as identity.** This is required by v2 and harmless on v1. For v1 user messages, reconcile optimistic vs replayed by ordinal position within the session (nth user message), with a content check as a guard, because no id is available live. On replay, rebuild the session's entity set rather than merging by id. Rationale: 4(a), 4(e).
8. **Always drain the agent's stdout, and decode off the main actor.** Read continuously even with no prompt in flight (Claude issue #1269: an unread pipe stalls the agent for minutes). Coalesce text deltas to display refresh (Claude emits one notification per SSE delta; Codex per app-server delta) and apply them in batches so SwiftUI/AppKit does not relayout per token. Rationale: 5.1 streaming granularity.
9. **Advertise the capabilities that unlock better data.** `clientCapabilities._meta.terminal_output` (or `terminal_output_delta`) for live command output on both adapters; `elicitation.form` (otherwise Claude disables `AskUserQuestion`) and `elicitation.url`; `session.notices`; `session.compaction`; `session.configOptions.boolean`; `auth.terminal`. Consider `subagents` only if child-session UI exists. Rationale: 4(f), 4(g).
10. **Titles and meters: provisional first, authoritative later.** Set a client-derived title from the first prompt immediately; replace on `session_info_update`. Show the context meter from the last persisted `usage_update` on restore; do not expect per-turn token counts from stable ACP. Rationale: 4(i).
11. **Instrument from the adapter's own timing logs.** Capture adapter stderr and parse the Claude adapter's `[session/create|load] ... phase=... durationMs=...` lines to attribute new-chat latency without guessing. Track time from send to prompt request written, to first update, to first text delta.
12. **Plan for v2 behind a flag, per agent.** v2 removes most of the inference above (insertion ack + `messageId`, `state_update`, prompts while running, chunked tool output, resume with or without replay). Both primary adapters already negotiate it. Keep wire shapes separate and share the session model; do not enable by default until the draft settles and the steer/queue RFD (#1261 vs #2255) lands, since steering is currently v1-only on the Claude adapter. Watch #2407 (`replay_complete`), #2394 (`updatedAt` semantics) and #2753 (`session/read_text`), each of which would let the client drop a workaround listed here.

### Things the protocol cannot fix (client or product decisions)

- First-token latency after a prompt is model and CLI time; ACP adds nothing measurable there once the session exists. The avoidable cost is everything *before* the prompt is written to the pipe.
- A lost prompt response is unrecoverable ambiguity on both versions (no idempotency key). The durable queue should mark such a prompt "delivery unknown" and offer resend rather than auto-retry.
- Agent process death is not reported by any update (v2 says so explicitly); the client must watch the process and stdout EOF and fail outstanding requests itself.
